// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package actions

import (
	"context"
	"fmt"
	"log/slog"

	_ "github.com/lib/pq"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/mterrors"
	"github.com/multigres/multigres/go/multiorch/coordinator"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// BootstrapShardAction handles bootstrap initialization of a new shard from scratch.
// This action assumes all nodes in the cohort are empty (uninitialized).
// It will:
// 1. Select the first reachable node as the primary
// 2. Initialize it as an empty primary with term=1
// 3. Create the durability policy in the database
// 4. Initialize remaining nodes as standbys
type BootstrapShardAction struct {
	topoStore topo.Store
	logger    *slog.Logger
}

// NewBootstrapShardAction creates a new bootstrap action
func NewBootstrapShardAction(topoStore topo.Store, logger *slog.Logger) *BootstrapShardAction {
	return &BootstrapShardAction{
		topoStore: topoStore,
		logger:    logger,
	}
}

// Execute performs bootstrap initialization for a new shard
func (a *BootstrapShardAction) Execute(ctx context.Context, shardID string, database string, cohort []*coordinator.Node) error {
	a.logger.InfoContext(ctx, "Executing bootstrap initialization",
		"shard", shardID,
		"database", database,
		"cohort_size", len(cohort))

	if len(cohort) == 0 {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "cohort is empty for shard %s", shardID)
	}

	// Step 1: Get the durability policy name from the Database proto in topology
	policyName, err := a.getDurabilityPolicyName(ctx, database)
	if err != nil {
		return mterrors.Wrap(err, "failed to get durability policy name from topology")
	}

	a.logger.InfoContext(ctx, "Using durability policy",
		"shard", shardID,
		"database", database,
		"policy_name", policyName)

	// Step 2: Select the first reachable node as the initial primary
	candidate, err := a.selectBootstrapCandidate(ctx, cohort)
	if err != nil {
		return mterrors.Wrap(err, "failed to select bootstrap candidate")
	}

	a.logger.InfoContext(ctx, "Selected bootstrap candidate",
		"shard", shardID,
		"database", database,
		"candidate", candidate.ID.Name)

	// Step 3: Initialize the candidate as an empty primary with term=1
	req := &multipoolermanagerdatapb.InitializeEmptyPrimaryRequest{
		ConsensusTerm: 1,
	}
	resp, err := candidate.RpcClient.InitializeEmptyPrimary(ctx, candidate.Pooler, req)
	if err != nil {
		return mterrors.Wrap(err, "failed to initialize empty primary")
	}

	if !resp.Success {
		return mterrors.Errorf(mtrpcpb.Code_INTERNAL,
			"failed to initialize empty primary on node %s: %s",
			candidate.ID.Name, resp.ErrorMessage)
	}

	a.logger.InfoContext(ctx, "Successfully initialized primary",
		"shard", shardID,
		"database", database,
		"primary", candidate.ID.Name,
		"backup_id", resp.BackupId)

	// Step 4: Create durability policy in the primary's database
	quorumRule, err := a.parsePolicy(policyName)
	if err != nil {
		return mterrors.Wrap(err, "failed to parse policy")
	}

	createPolicyReq := &multipoolermanagerdatapb.CreateDurabilityPolicyRequest{
		PolicyName: policyName,
		QuorumRule: quorumRule,
	}
	createPolicyResp, err := candidate.RpcClient.CreateDurabilityPolicy(ctx, candidate.Pooler, createPolicyReq)
	if err != nil {
		return mterrors.Wrap(err, "failed to create durability policy")
	}

	if !createPolicyResp.Success {
		return mterrors.Errorf(mtrpcpb.Code_INTERNAL,
			"failed to create durability policy: %s", createPolicyResp.ErrorMessage)
	}

	a.logger.InfoContext(ctx, "Successfully created durability policy",
		"shard", shardID,
		"database", database,
		"policy_name", policyName)

	// Step 5: Initialize remaining nodes as standbys
	standbys := make([]*coordinator.Node, 0, len(cohort)-1)
	for _, node := range cohort {
		if node.ID.Name != candidate.ID.Name {
			standbys = append(standbys, node)
		}
	}

	if err := a.initializeStandbys(ctx, shardID, candidate, standbys, resp.BackupId); err != nil {
		// Log but don't fail - we have a primary at least
		a.logger.WarnContext(ctx, "Failed to initialize some standbys",
			"shard", shardID,
			"database", database,
			"error", err)
	}

	a.logger.InfoContext(ctx, "Bootstrap initialization complete",
		"shard", shardID,
		"database", database,
		"primary", candidate.ID.Name,
		"standbys", len(standbys))

	return nil
}

// selectBootstrapCandidate selects the first healthy node as the bootstrap candidate
func (a *BootstrapShardAction) selectBootstrapCandidate(ctx context.Context, cohort []*coordinator.Node) (*coordinator.Node, error) {
	// For bootstrap, we just pick the first reachable node
	// In a production system, you might want to consider factors like:
	// - Node with fastest storage
	// - Node in preferred availability zone
	// - Node with most resources available

	for _, node := range cohort {
		req := &multipoolermanagerdatapb.InitializationStatusRequest{}
		status, err := node.RpcClient.InitializationStatus(ctx, node.Pooler, req)
		if err != nil {
			a.logger.WarnContext(ctx, "Node unreachable during candidate selection",
				"node", node.ID.Name,
				"error", err)
			continue
		}

		if !status.IsInitialized {
			a.logger.InfoContext(ctx, "Selected node as bootstrap candidate",
				"node", node.ID.Name)
			return node, nil
		}
	}

	return nil, mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE,
		"no suitable candidate found for bootstrap")
}

// initializeStandbys initializes multiple nodes as standbys of the given primary
func (a *BootstrapShardAction) initializeStandbys(ctx context.Context, shardID string, primary *coordinator.Node, standbys []*coordinator.Node, backupID string) error {
	if len(standbys) == 0 {
		return nil
	}

	a.logger.InfoContext(ctx, "Initializing standbys",
		"shard", shardID,
		"primary", primary.ID.Name,
		"standby_count", len(standbys),
		"backup_id", backupID)

	// Initialize all standbys in parallel
	// Use a simple error aggregation approach
	type result struct {
		node *coordinator.Node
		err  error
	}

	results := make(chan result, len(standbys))

	for _, standby := range standbys {
		go func(node *coordinator.Node) {
			// Set pooler type to REPLICA before initializing as standby
			changeTypeReq := &multipoolermanagerdatapb.ChangeTypeRequest{
				PoolerType: clustermetadatapb.PoolerType_REPLICA,
			}
			_, err := node.RpcClient.ChangeType(ctx, node.Pooler, changeTypeReq)
			if err != nil {
				results <- result{node: node, err: fmt.Errorf("failed to set pooler type: %w", err)}
				return
			}

			req := &multipoolermanagerdatapb.InitializeAsStandbyRequest{
				PrimaryHost:   primary.Hostname,
				PrimaryPort:   primary.Port,
				ConsensusTerm: 1,
				Force:         false,
				BackupId:      backupID,
			}
			resp, err := node.RpcClient.InitializeAsStandby(ctx, node.Pooler, req)
			if err != nil {
				results <- result{node: node, err: err}
				return
			}

			if !resp.Success {
				results <- result{
					node: node,
					err:  fmt.Errorf("initialization failed: %s", resp.ErrorMessage),
				}
				return
			}

			results <- result{node: node, err: nil}
		}(standby)
	}

	// Collect results
	var failedNodes []string
	for i := 0; i < len(standbys); i++ {
		res := <-results
		if res.err != nil {
			a.logger.WarnContext(ctx, "Failed to initialize standby",
				"shard", shardID,
				"node", res.node.ID.Name,
				"error", res.err)
			failedNodes = append(failedNodes, res.node.ID.Name)
		} else {
			a.logger.InfoContext(ctx, "Successfully initialized standby",
				"shard", shardID,
				"node", res.node.ID.Name)
		}
	}

	if len(failedNodes) > 0 {
		return mterrors.Errorf(mtrpcpb.Code_INTERNAL,
			"failed to initialize %d standbys: %v", len(failedNodes), failedNodes)
	}

	return nil
}

// getDurabilityPolicyName retrieves the durability policy name from the Database proto in topology
func (a *BootstrapShardAction) getDurabilityPolicyName(ctx context.Context, database string) (string, error) {
	db, err := a.topoStore.GetDatabase(ctx, database)
	if err != nil {
		return "", mterrors.Wrapf(err, "failed to get database %s from topology", database)
	}

	if db.DurabilityPolicy == "" {
		return "", mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"database %s has no durability_policy configured", database)
	}

	// Validate that the policy name is supported
	switch db.DurabilityPolicy {
	case "ANY_2", "MULTI_CELL_ANY_2":
		return db.DurabilityPolicy, nil
	default:
		return "", mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
			"unsupported durability policy: %s (must be ANY_2 or MULTI_CELL_ANY_2)", db.DurabilityPolicy)
	}
}

// parsePolicy converts a policy name into a QuorumRule
func (a *BootstrapShardAction) parsePolicy(policyName string) (*clustermetadatapb.QuorumRule, error) {
	switch policyName {
	case "ANY_2":
		return &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N,
			RequiredCount: 2,
			Description:   "Any 2 nodes must acknowledge",
		}, nil

	case "MULTI_CELL_ANY_2":
		return &clustermetadatapb.QuorumRule{
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_MULTI_CELL_ANY_N,
			RequiredCount: 2,
			Description:   "Any 2 nodes from different cells must acknowledge",
		}, nil

	default:
		return nil, mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
			"unsupported policy name: %s", policyName)
	}
}
