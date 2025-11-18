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

// LoadQuorumRule attempts to load the quorum rule from any available node in the cohort.
// It tries each node until it finds one that responds successfully.
func (c *Coordinator) LoadQuorumRule(ctx context.Context, cohort []*Node, database string) (*clustermetadatapb.QuorumRule, error) {
	if len(cohort) == 0 {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "cohort is empty")
	}

	var lastErr error
	for _, node := range cohort {
		rule, err := c.LoadQuorumRuleFromNode(ctx, node, database)
		if err != nil {
			c.logger.WarnContext(ctx, "Failed to load policy from node",
				"node", node.ID.Name,
				"error", err)
			lastErr = err
			continue
		}
		return rule, nil
	}

	// If all nodes failed, return default policy
	c.logger.WarnContext(ctx, "Failed to load policy from all nodes, using default",
		"cohort_size", len(cohort),
		"last_error", lastErr)

	return c.getDefaultQuorumRule(ctx, len(cohort)), nil
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
