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
	"time"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/multiorch/recovery/types"
	"github.com/multigres/multigres/go/multiorch/store"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// Compile-time assertion that BootstrapShardAction implements types.RecoveryAction.
var _ types.RecoveryAction = (*BootstrapShardAction)(nil)

// BootstrapShardAction handles bootstrap initialization of a new shard from scratch.
// This action assumes all nodes in the cohort are empty (uninitialized).
// It will:
// 1. Acquire distributed lock for the shard
// 2. Re-verify all nodes are still uninitialized
// 3. Select the first reachable node as the primary
// 4. Initialize it as an empty primary with term=1
// 5. Create the durability policy in the database
// 6. Initialize remaining nodes as standbys
type BootstrapShardAction struct {
	rpcClient   rpcclient.MultiPoolerClient
	poolerStore *store.ProtoStore[string, *multiorchdatapb.PoolerHealthState]
	topoStore   topo.Store
	logger      *slog.Logger
}

// NewBootstrapShardAction creates a new bootstrap action
func NewBootstrapShardAction(
	rpcClient rpcclient.MultiPoolerClient,
	poolerStore *store.ProtoStore[string, *multiorchdatapb.PoolerHealthState],
	topoStore topo.Store,
	logger *slog.Logger,
) *BootstrapShardAction {
	return &BootstrapShardAction{
		rpcClient:   rpcClient,
		poolerStore: poolerStore,
		topoStore:   topoStore,
		logger:      logger,
	}
}

// Execute performs bootstrap initialization for a new shard
func (a *BootstrapShardAction) Execute(ctx context.Context, problem types.Problem) error {
	a.logger.InfoContext(ctx, "executing bootstrap shard action",
		"database", problem.Database,
		"tablegroup", problem.TableGroup,
		"shard", problem.Shard)

	// Step 1: Acquire distributed lock for this shard
	metadata := a.Metadata()
	lockPath := topo.RecoveryLockPath(problem.Database, problem.TableGroup, problem.Shard)
	a.logger.InfoContext(ctx, "acquiring recovery lock", "lock_path", lockPath)

	lockDesc, err := a.topoStore.LockShardForRecovery(
		ctx,
		problem.Database,
		problem.TableGroup,
		problem.Shard,
		"bootstrap recovery",
		metadata.GetLockTimeout(),
	)
	if err != nil {
		a.logger.InfoContext(ctx, "failed to acquire lock, another recovery may be in progress",
			"lock_path", lockPath,
			"error", err)
		return err
	}
	defer func() {
		// Use background context for unlock to ensure lock release even if ctx is cancelled
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if unlockErr := lockDesc.Unlock(unlockCtx); unlockErr != nil {
			a.logger.WarnContext(ctx, "failed to release recovery lock",
				"lock_path", lockPath,
				"error", unlockErr)
		}
	}()

	a.logger.InfoContext(ctx, "acquired recovery lock", "lock_path", lockPath)

	// Step 2: Fetch cohort from pooler store
	cohort := a.getCohort(problem.Database, problem.TableGroup, problem.Shard)
	if len(cohort) == 0 {
		return fmt.Errorf("no poolers found for shard %s/%s/%s",
			problem.Database, problem.TableGroup, problem.Shard)
	}

	// Step 3: Get the durability policy name from the Database proto in topology
	policyName, err := a.getDurabilityPolicyName(ctx, problem.Database)
	if err != nil {
		return mterrors.Wrap(err, "failed to get durability policy name from topology")
	}

	a.logger.InfoContext(ctx, "using durability policy",
		"shard", problem.Shard,
		"database", problem.Database,
		"policy_name", policyName)

	// Step 4: Revalidate that bootstrap is still needed after acquiring lock.
	// Make fresh RPC calls to verify all nodes are still uninitialized.
	// Don't rely on potentially stale store data.
	needsBootstrap, err := a.verifyBootstrapNeeded(ctx, cohort)
	if err != nil {
		return mterrors.Wrap(err, "failed to verify bootstrap needed")
	}
	if !needsBootstrap {
		a.logger.InfoContext(ctx, "shard no longer needs bootstrap, problem resolved by another recovery",
			"database", problem.Database,
			"tablegroup", problem.TableGroup,
			"shard", problem.Shard)
		return nil
	}

	a.logger.InfoContext(ctx, "verified shard still needs bootstrap, proceeding",
		"database", problem.Database,
		"tablegroup", problem.TableGroup,
		"shard", problem.Shard,
		"cohort_size", len(cohort))

	// Step 5: Select a bootstrap candidate
	candidate, err := a.selectBootstrapCandidate(ctx, cohort)
	if err != nil {
		return mterrors.Wrap(err, "failed to select bootstrap candidate")
	}

	a.logger.InfoContext(ctx, "selected bootstrap candidate",
		"shard", problem.Shard,
		"database", problem.Database,
		"candidate", candidate.MultiPooler.Id.Name)

	// Step 6: Set pooler type to PRIMARY before initializing
	changeTypeReq := &multipoolermanagerdatapb.ChangeTypeRequest{
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	_, err = a.rpcClient.ChangeType(ctx, candidate.MultiPooler, changeTypeReq)
	if err != nil {
		return mterrors.Wrap(err, "failed to set pooler type to PRIMARY")
	}

	// Step 7: Initialize the candidate as an empty primary with term=1
	req := &multipoolermanagerdatapb.InitializeEmptyPrimaryRequest{
		ConsensusTerm: 1,
	}
	resp, err := a.rpcClient.InitializeEmptyPrimary(ctx, candidate.MultiPooler, req)
	if err != nil {
		return mterrors.Wrap(err, "failed to initialize empty primary")
	}

	if !resp.Success {
		return mterrors.Errorf(mtrpcpb.Code_INTERNAL,
			"failed to initialize empty primary on node %s: %s",
			candidate.MultiPooler.Id.Name, resp.ErrorMessage)
	}

	a.logger.InfoContext(ctx, "successfully initialized primary",
		"shard", problem.Shard,
		"database", problem.Database,
		"primary", candidate.MultiPooler.Id.Name,
		"backup_id", resp.BackupId)

	// Step 8: Create durability policy in the primary's database
	quorumRule, err := a.parsePolicy(policyName)
	if err != nil {
		return mterrors.Wrap(err, "failed to parse policy")
	}

	createPolicyReq := &multipoolermanagerdatapb.CreateDurabilityPolicyRequest{
		PolicyName: policyName,
		QuorumRule: quorumRule,
	}
	createPolicyResp, err := a.rpcClient.CreateDurabilityPolicy(ctx, candidate.MultiPooler, createPolicyReq)
	if err != nil {
		return mterrors.Wrap(err, "failed to create durability policy")
	}

	if !createPolicyResp.Success {
		return mterrors.Errorf(mtrpcpb.Code_INTERNAL,
			"failed to create durability policy: %s", createPolicyResp.ErrorMessage)
	}

	a.logger.InfoContext(ctx, "successfully created durability policy",
		"shard", problem.Shard,
		"database", problem.Database,
		"policy_name", policyName)

	// Step 9: Initialize remaining nodes as standbys
	standbys := make([]*multiorchdatapb.PoolerHealthState, 0, len(cohort)-1)
	for _, pooler := range cohort {
		if pooler.MultiPooler.Id.Name != candidate.MultiPooler.Id.Name {
			standbys = append(standbys, pooler)
		}
	}

	if err := a.initializeStandbys(ctx, problem.Shard, candidate, standbys, resp.BackupId); err != nil {
		// Log but don't fail - we have a primary at least
		a.logger.WarnContext(ctx, "failed to initialize some standbys",
			"shard", problem.Shard,
			"database", problem.Database,
			"error", err)
	}

	a.logger.InfoContext(ctx, "bootstrap shard action completed successfully",
		"shard", problem.Shard,
		"database", problem.Database,
		"primary", candidate.MultiPooler.Id.Name,
		"standbys", len(standbys))

	return nil
}

// verifyBootstrapNeeded checks if bootstrap is still needed by making fresh RPC calls.
// Returns true if all reachable nodes are uninitialized, false if any node is initialized.
func (a *BootstrapShardAction) verifyBootstrapNeeded(ctx context.Context, cohort []*multiorchdatapb.PoolerHealthState) (bool, error) {
	for _, pooler := range cohort {
		req := &multipoolermanagerdatapb.InitializationStatusRequest{}
		resp, err := a.rpcClient.InitializationStatus(ctx, pooler.MultiPooler, req)
		if err != nil {
			// Node unreachable - can't determine state, continue checking others
			a.logger.WarnContext(ctx, "node unreachable during bootstrap recheck",
				"node", pooler.MultiPooler.Id.Name,
				"error", err)
			continue
		}

		if resp.IsInitialized {
			a.logger.InfoContext(ctx, "node is now initialized (fresh RPC check), skipping bootstrap",
				"node", pooler.MultiPooler.Id.Name,
				"role", resp.Role)
			return false, nil
		}
	}

	return true, nil
}

// selectBootstrapCandidate selects a healthy node as the bootstrap candidate.
// Prefers initialized nodes over uninitialized nodes to avoid data loss in mixed scenarios.
func (a *BootstrapShardAction) selectBootstrapCandidate(ctx context.Context, cohort []*multiorchdatapb.PoolerHealthState) (*multiorchdatapb.PoolerHealthState, error) {
	// For bootstrap in mixed initialization scenarios, prefer initialized nodes
	// to avoid data loss. This handles cases where some nodes have data and others don't.
	// Selection strategy:
	// 1. First pass: look for initialized nodes
	// 2. Second pass: if no initialized nodes, use first uninitialized node

	type nodeInfo struct {
		pooler      *multiorchdatapb.PoolerHealthState
		initialized bool
		reachable   bool
	}

	nodes := make([]nodeInfo, 0, len(cohort))

	for _, pooler := range cohort {
		req := &multipoolermanagerdatapb.InitializationStatusRequest{}
		status, err := a.rpcClient.InitializationStatus(ctx, pooler.MultiPooler, req)
		if err != nil {
			a.logger.WarnContext(ctx, "node unreachable during candidate selection",
				"node", pooler.MultiPooler.Id.Name,
				"error", err)
			nodes = append(nodes, nodeInfo{
				pooler:      pooler,
				initialized: false,
				reachable:   false,
			})
			continue
		}

		nodes = append(nodes, nodeInfo{
			pooler:      pooler,
			initialized: status.IsInitialized,
			reachable:   true,
		})
	}

	// First pass: prefer initialized nodes
	for _, node := range nodes {
		if node.reachable && node.initialized {
			a.logger.InfoContext(ctx, "selected initialized node as bootstrap candidate",
				"node", node.pooler.MultiPooler.Id.Name)
			return node.pooler, nil
		}
	}

	// Second pass: use first uninitialized but reachable node
	for _, node := range nodes {
		if node.reachable && !node.initialized {
			a.logger.InfoContext(ctx, "selected uninitialized node as bootstrap candidate",
				"node", node.pooler.MultiPooler.Id.Name)
			return node.pooler, nil
		}
	}

	return nil, mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE,
		"no suitable candidate found for bootstrap")
}

// initializeStandbys initializes multiple nodes as standbys of the given primary
func (a *BootstrapShardAction) initializeStandbys(ctx context.Context, shardID string, primary *multiorchdatapb.PoolerHealthState, standbys []*multiorchdatapb.PoolerHealthState, backupID string) error {
	if len(standbys) == 0 {
		return nil
	}

	a.logger.InfoContext(ctx, "initializing standbys",
		"shard", shardID,
		"primary", primary.MultiPooler.Id.Name,
		"standby_count", len(standbys),
		"backup_id", backupID)

	// Initialize all standbys in parallel
	type result struct {
		node *multiorchdatapb.PoolerHealthState
		err  error
	}

	results := make(chan result, len(standbys))

	for _, standby := range standbys {
		go func(node *multiorchdatapb.PoolerHealthState) {
			err := a.initializeSingleStandby(ctx, node, primary, backupID)
			results <- result{node: node, err: err}
		}(standby)
	}

	// Collect results
	var failedNodes []string
	for range standbys {
		res := <-results
		if res.err != nil {
			a.logger.WarnContext(ctx, "failed to initialize standby",
				"shard", shardID,
				"node", res.node.MultiPooler.Id.Name,
				"error", res.err)
			failedNodes = append(failedNodes, res.node.MultiPooler.Id.Name)
		} else {
			a.logger.InfoContext(ctx, "successfully initialized standby",
				"shard", shardID,
				"node", res.node.MultiPooler.Id.Name)
		}
	}

	if len(failedNodes) > 0 {
		return mterrors.Errorf(mtrpcpb.Code_INTERNAL,
			"failed to initialize %d standbys: %v", len(failedNodes), failedNodes)
	}

	return nil
}

// initializeSingleStandby initializes a single node as a standby of the given primary.
func (a *BootstrapShardAction) initializeSingleStandby(ctx context.Context, node *multiorchdatapb.PoolerHealthState, primary *multiorchdatapb.PoolerHealthState, backupID string) error {
	// Set pooler type to REPLICA before initializing as standby
	changeTypeReq := &multipoolermanagerdatapb.ChangeTypeRequest{
		PoolerType: clustermetadatapb.PoolerType_REPLICA,
	}
	_, err := a.rpcClient.ChangeType(ctx, node.MultiPooler, changeTypeReq)
	if err != nil {
		return fmt.Errorf("failed to set pooler type: %w", err)
	}

	req := &multipoolermanagerdatapb.InitializeAsStandbyRequest{
		PrimaryHost:   primary.MultiPooler.Hostname,
		PrimaryPort:   primary.MultiPooler.PortMap["pg"],
		ConsensusTerm: 1,
		Force:         false,
		BackupId:      backupID,
	}
	resp, err := a.rpcClient.InitializeAsStandby(ctx, node.MultiPooler, req)
	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("initialization failed: %s", resp.ErrorMessage)
	}

	return nil
}

// getCohort fetches all poolers in the shard from the pooler store.
func (a *BootstrapShardAction) getCohort(database, tablegroup, shard string) []*multiorchdatapb.PoolerHealthState {
	var cohort []*multiorchdatapb.PoolerHealthState

	a.poolerStore.Range(func(key string, pooler *multiorchdatapb.PoolerHealthState) bool {
		if pooler == nil || pooler.MultiPooler == nil || pooler.MultiPooler.Id == nil {
			return true // continue
		}

		if pooler.MultiPooler.Database == database &&
			pooler.MultiPooler.TableGroup == tablegroup &&
			pooler.MultiPooler.Shard == shard {
			cohort = append(cohort, pooler)
		}

		return true // continue
	})

	return cohort
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

// RecoveryAction interface implementation

func (a *BootstrapShardAction) RequiresHealthyPrimary() bool {
	return false
}

func (a *BootstrapShardAction) Metadata() types.RecoveryMetadata {
	return types.RecoveryMetadata{
		Name:        "BootstrapShard",
		Description: "Initialize empty shard with primary and standbys",
		Timeout:     60 * time.Second,
		LockTimeout: 15 * time.Second,
		Retryable:   false,
	}
}

func (a *BootstrapShardAction) Priority() types.Priority {
	return types.PriorityShardBootstrap
}
