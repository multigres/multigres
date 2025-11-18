// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package coordinator

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/multigres/multigres/go/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// LoadQuorumRuleFromNode loads the active durability policy from a node via gRPC.
// This uses the GetDurabilityPolicy RPC to fetch the policy from the node's local database.
func (c *Coordinator) LoadQuorumRuleFromNode(ctx context.Context, node *Node, database string) (*clustermetadatapb.QuorumRule, error) {
	// Call GetDurabilityPolicy RPC
	resp, err := node.GetDurabilityPolicy(ctx)
	if err != nil {
		return nil, mterrors.Wrapf(err, "failed to get durability policy from node %s", node.ID.Name)
	}

	// Check if a policy was returned
	if resp.Policy == nil || resp.Policy.QuorumRule == nil {
		// No active policy found - return a default policy
		c.logger.WarnContext(ctx, "No active durability policy found, using default ANY_N with majority",
			"node", node.ID.Name,
			"database", database)
		return c.getDefaultQuorumRule(ctx, 0), nil
	}

	quorumRule := resp.Policy.QuorumRule

	c.logger.InfoContext(ctx, "Loaded durability policy from node",
		"node", node.ID.Name,
		"database", database,
		"quorum_type", quorumRule.QuorumType,
		"required_count", quorumRule.RequiredCount,
		"description", quorumRule.Description)

	return quorumRule, nil
}

// LoadQuorumRule loads the quorum rule using the following strategy:
// 1. If a PRIMARY node exists, load from it (most up-to-date)
// 2. Otherwise, load from all REPLICA nodes in parallel
// 3. Wait for n-1 responses
// 4. Return the rule with the highest version number
func (c *Coordinator) LoadQuorumRule(ctx context.Context, cohort []*Node, database string) (*clustermetadatapb.QuorumRule, error) {
	if len(cohort) == 0 {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "cohort is empty")
	}

	// Step 1: Find PRIMARY node
	var primaryNode *Node
	var replicaNodes []*Node
	for _, node := range cohort {
		switch node.pooler.Type {
		case clustermetadatapb.PoolerType_PRIMARY:
			primaryNode = node
		case clustermetadatapb.PoolerType_REPLICA:
			replicaNodes = append(replicaNodes, node)
		}
	}

	// If PRIMARY exists, load from it
	if primaryNode != nil {
		c.logger.InfoContext(ctx, "Loading durability policy from PRIMARY node",
			"node", primaryNode.ID.Name,
			"database", database)
		rule, err := c.LoadQuorumRuleFromNode(ctx, primaryNode, database)
		if err != nil {
			c.logger.WarnContext(ctx, "Failed to load policy from PRIMARY, falling back to REPLICAs",
				"node", primaryNode.ID.Name,
				"error", err)
			// Fall through to REPLICA strategy
		} else {
			return rule, nil
		}
	}

	// Step 2-4: Load from REPLICAs in parallel and select latest version
	if len(replicaNodes) == 0 {
		c.logger.WarnContext(ctx, "No REPLICA nodes available, using default policy")
		return c.getDefaultQuorumRule(ctx, len(cohort)), nil
	}

	c.logger.InfoContext(ctx, "Loading durability policy from REPLICA nodes in parallel",
		"replica_count", len(replicaNodes),
		"database", database)

	return c.loadFromReplicasInParallel(ctx, replicaNodes, database)
}

// loadFromReplicasInParallel loads policies from all REPLICA nodes in parallel,
// waits for n-1 responses, and returns the policy with the highest version.
func (c *Coordinator) loadFromReplicasInParallel(ctx context.Context, replicas []*Node, database string) (*clustermetadatapb.QuorumRule, error) {
	type result struct {
		node   *Node
		policy *clustermetadatapb.DurabilityPolicy
		rule   *clustermetadatapb.QuorumRule
		err    error
	}

	results := make(chan result, len(replicas))
	var wg sync.WaitGroup

	// Launch parallel queries
	for _, node := range replicas {
		wg.Add(1)
		go func(n *Node) {
			defer wg.Done()
			resp, err := n.GetDurabilityPolicy(ctx)
			if err != nil {
				results <- result{node: n, err: err}
				return
			}

			if resp.Policy == nil || resp.Policy.QuorumRule == nil {
				results <- result{
					node: n,
					err:  fmt.Errorf("no active policy found"),
				}
				return
			}

			results <- result{
				node:   n,
				policy: resp.Policy,
				rule:   resp.Policy.QuorumRule,
			}
		}(node)
	}

	// Close results channel when all goroutines complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results - wait for n-1 responses (majority)
	requiredResponses := len(replicas) - 1
	if requiredResponses < 1 {
		requiredResponses = 1 // At least get one response
	}

	var bestPolicy *clustermetadatapb.DurabilityPolicy
	var bestRule *clustermetadatapb.QuorumRule
	successCount := 0
	errorCount := 0

	// Collect all responses (channel will close when all goroutines complete)
	for res := range results {
		if res.err != nil {
			c.logger.WarnContext(ctx, "Failed to load policy from REPLICA",
				"node", res.node.ID.Name,
				"error", res.err)
			errorCount++
			continue
		}

		successCount++
		c.logger.InfoContext(ctx, "Loaded policy from REPLICA",
			"node", res.node.ID.Name,
			"version", res.policy.PolicyVersion)

		// Select policy with highest version
		if bestPolicy == nil || res.policy.PolicyVersion > bestPolicy.PolicyVersion {
			bestPolicy = res.policy
			bestRule = res.rule
		}
	}

	// Check if we got enough responses
	if successCount == 0 {
		c.logger.WarnContext(ctx, "Failed to load policy from all REPLICAs, using default",
			"replica_count", len(replicas),
			"errors", errorCount)
		return c.getDefaultQuorumRule(ctx, len(replicas)), nil
	}

	if successCount < requiredResponses {
		c.logger.WarnContext(ctx, "Got fewer responses than required, using best available",
			"success_count", successCount,
			"required", requiredResponses)
	}

	c.logger.InfoContext(ctx, "Selected durability policy",
		"policy_name", bestPolicy.PolicyName,
		"policy_version", bestPolicy.PolicyVersion,
		"quorum_type", bestRule.QuorumType,
		"required_count", bestRule.RequiredCount)

	return bestRule, nil
}

// getDefaultQuorumRule returns a default majority quorum rule.
// If cohortSize is provided and > 0, it calculates required_count as majority.
// Otherwise, it returns ANY_N with required_count=2 as a safe default.
func (c *Coordinator) getDefaultQuorumRule(ctx context.Context, cohortSize int) *clustermetadatapb.QuorumRule {
	if cohortSize > 0 {
		// Calculate majority
		requiredCount := cohortSize/2 + 1
		return &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: int32(requiredCount),
			Description:   fmt.Sprintf("Default majority quorum (%d of %d nodes)", requiredCount, cohortSize),
		}
	}

	// Safe fallback
	return &clustermetadatapb.QuorumRule{
		QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
		RequiredCount: 2,
		Description:   "Default ANY_N quorum (2 nodes)",
	}
}

// CreateDefaultPolicy creates a default durability policy in the given database.
// This is useful for bootstrapping new shards.
func (c *Coordinator) CreateDefaultPolicy(ctx context.Context, node *Node, database string, policyName string) error {
	// Connect to the node's postgres database
	dsn := fmt.Sprintf("host=%s port=%d dbname=%s user=postgres sslmode=disable",
		node.Hostname, node.Port, database)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return mterrors.Wrapf(err, "failed to connect to node %s", node.ID.Name)
	}
	defer db.Close()

	// Test the connection
	if err := db.PingContext(ctx); err != nil {
		return mterrors.Wrapf(err, "failed to ping node %s", node.ID.Name)
	}

	// Create default ANY_N policy with required_count = 2
	quorumRule := clustermetadatapb.QuorumRule{
		QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
		RequiredCount: 2,
		Description:   "Default ANY_N quorum (2 nodes)",
	}

	// Marshal to JSON
	quorumRuleJSON, err := protojson.Marshal(&quorumRule)
	if err != nil {
		return mterrors.Wrap(err, "failed to marshal quorum rule")
	}

	// Convert to standard JSON for PostgreSQL JSONB
	var jsonData interface{}
	if err := json.Unmarshal(quorumRuleJSON, &jsonData); err != nil {
		return mterrors.Wrap(err, "failed to parse quorum rule JSON")
	}

	jsonBytes, err := json.Marshal(jsonData)
	if err != nil {
		return mterrors.Wrap(err, "failed to re-marshal quorum rule")
	}

	// Insert the policy
	query := `
		INSERT INTO multigres.durability_policy (policy_name, policy_version, quorum_rule, is_active)
		VALUES ($1, 1, $2::jsonb, true)
		ON CONFLICT (policy_name, policy_version) DO NOTHING
	`

	_, err = db.ExecContext(ctx, query, policyName, string(jsonBytes))
	if err != nil {
		return mterrors.Wrapf(err, "failed to insert default policy into node %s", node.ID.Name)
	}

	c.logger.InfoContext(ctx, "Created default durability policy",
		"node", node.ID.Name,
		"database", database,
		"policy_name", policyName)

	return nil
}
