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

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	commontypes "github.com/multigres/multigres/go/common/types"
	"github.com/multigres/multigres/go/multiorch/coordinator"
	"github.com/multigres/multigres/go/multiorch/recovery/types"
	"github.com/multigres/multigres/go/multiorch/store"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// Compile-time assertion that BootstrapShardAction implements types.RecoveryAction.
var _ types.RecoveryAction = (*BootstrapShardAction)(nil)

// DefaultStatusRPCTimeout is the default timeout for individual Status RPC calls.
const DefaultStatusRPCTimeout = 5 * time.Second

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
	rpcClient        rpcclient.MultiPoolerClient
	poolerStore      *store.PoolerHealthStore
	topoStore        topoclient.Store
	logger           *slog.Logger
	statusRPCTimeout time.Duration
	coordinator      *coordinator.Coordinator
}

// NewBootstrapShardAction creates a new bootstrap action with default settings.
func NewBootstrapShardAction(
	rpcClient rpcclient.MultiPoolerClient,
	poolerStore *store.PoolerHealthStore,
	topoStore topoclient.Store,
	coordinator *coordinator.Coordinator,
	logger *slog.Logger,
) *BootstrapShardAction {
	return &BootstrapShardAction{
		rpcClient:        rpcClient,
		poolerStore:      poolerStore,
		topoStore:        topoStore,
		logger:           logger,
		statusRPCTimeout: DefaultStatusRPCTimeout,
		coordinator:      coordinator,
	}
}

// WithStatusRPCTimeout sets the timeout for individual Status RPC calls.
// This is useful for testing with shorter timeouts.
func (a *BootstrapShardAction) WithStatusRPCTimeout(timeout time.Duration) *BootstrapShardAction {
	a.statusRPCTimeout = timeout
	return a
}

// Execute performs bootstrap initialization for a new shard
func (a *BootstrapShardAction) Execute(ctx context.Context, problem types.Problem) error {
	a.logger.InfoContext(ctx, "executing bootstrap shard action",
		"database", problem.ShardKey.Database,
		"tablegroup", problem.ShardKey.TableGroup,
		"shard", problem.ShardKey.Shard)

	// Acquire distributed lock for this shard
	a.logger.InfoContext(ctx, "acquiring recovery lock", "shard_key", problem.ShardKey.String())

	ctx, unlock, err := a.topoStore.LockShard(
		ctx,
		problem.ShardKey,
		"bootstrap recovery",
	)
	if err != nil {
		a.logger.InfoContext(ctx, "failed to acquire lock, another recovery may be in progress",
			"shard_key", problem.ShardKey.String(),
			"error", err)
		return err
	}
	defer func() {
		var unlockErr error
		unlock(&unlockErr)
		if unlockErr != nil {
			a.logger.WarnContext(ctx, "failed to release recovery lock",
				"shard_key", problem.ShardKey.String(),
				"error", unlockErr)
		}
	}()

	a.logger.InfoContext(ctx, "acquired recovery lock", "shard_key", problem.ShardKey.String())

	// Fetch cohort from pooler store
	cohort := a.getCohort(problem.ShardKey)
	if len(cohort) == 0 {
		return fmt.Errorf("no poolers found for shard %s", problem.ShardKey)
	}

	// Get the durability policy name from the Database proto in topology
	policyName, err := a.getDurabilityPolicyName(ctx, problem.ShardKey.Database)
	if err != nil {
		return mterrors.Wrap(err, "failed to get durability policy name from topology")
	}

	a.logger.InfoContext(ctx, "using durability policy",
		"shard_key", problem.ShardKey.String(),
		"policy_name", policyName)

	// Parse policy to get required count for quorum check
	quorumRule, err := a.parsePolicy(policyName)
	if err != nil {
		return mterrors.Wrap(err, "failed to parse policy")
	}

	// Check that enough poolers are reachable to satisfy quorum before attempting bootstrap
	reachableCount := a.countReachablePoolers(ctx, cohort)
	if reachableCount < int(quorumRule.RequiredCount) {
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"insufficient reachable poolers for bootstrap: have %d, need %d for quorum",
			reachableCount, quorumRule.RequiredCount)
	}

	a.logger.InfoContext(ctx, "quorum check passed",
		"shard_key", problem.ShardKey.String(),
		"reachable_poolers", reachableCount,
		"required_count", quorumRule.RequiredCount)

	// Revalidate that bootstrap is still needed after acquiring lock.
	// Make fresh RPC calls to verify all nodes are still uninitialized.
	// Don't rely on potentially stale store data.
	needsBootstrap, err := a.verifyBootstrapNeeded(ctx, cohort)
	if err != nil {
		return mterrors.Wrap(err, "failed to verify bootstrap needed")
	}
	if !needsBootstrap {
		a.logger.InfoContext(ctx, "shard no longer needs bootstrap, problem resolved by another recovery",
			"shard_key", problem.ShardKey.String())
		return nil
	}

	a.logger.InfoContext(ctx, "verified shard still needs bootstrap, proceeding",
		"shard_key", problem.ShardKey.String(),
		"cohort_size", len(cohort))

	// Select a bootstrap candidate
	candidate, err := a.selectBootstrapCandidate(ctx, cohort)
	if err != nil {
		return mterrors.Wrap(err, "failed to select bootstrap candidate")
	}

	a.logger.InfoContext(ctx, "selected bootstrap candidate",
		"shard_key", problem.ShardKey.String(),
		"candidate", candidate.MultiPooler.Id.Name)

	// Initialize the candidate as an empty primary with term=1
	// This now also sets the pooler type to PRIMARY and creates the durability policy
	req := &multipoolermanagerdatapb.InitializeEmptyPrimaryRequest{
		ConsensusTerm:        1,
		DurabilityPolicyName: policyName,
		DurabilityQuorumRule: quorumRule,
		CoordinatorId:        topoclient.ClusterIDString(a.coordinator.GetCoordinatorID()),
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

	a.logger.InfoContext(ctx, "successfully initialized primary with durability policy",
		"shard_key", problem.ShardKey.String(),
		"primary", candidate.MultiPooler.Id.Name,
		"backup_id", resp.BackupId,
		"policy_name", policyName)

	// Configure synchronous replication. We still add the standby nodes, even if they are not wired up yet,
	// because synchronous replication requires at least one standby configured.
	// MultiOrch will wire them up later, so including them during bootstrap is fine.

	standbys := make([]*multiorchdatapb.PoolerHealthState, 0, len(cohort)-1)
	for _, pooler := range cohort {
		if pooler.MultiPooler.Id.Name != candidate.MultiPooler.Id.Name {
			standbys = append(standbys, pooler)
		}
	}

	// Configure synchronous replication on primary based on durability policy.
	// This must be done AFTER standbys are initialized so they can receive WAL.
	// Only include successfully initialized standbys in the sync replication config.
	if err := a.configureSynchronousReplication(ctx, candidate, standbys, quorumRule); err != nil {
		return mterrors.Wrap(err, "failed to configure synchronous replication")
	}

	a.logger.InfoContext(ctx, "bootstrap shard action completed successfully",
		"shard_key", problem.ShardKey.String(),
		"primary", candidate.MultiPooler.Id.Name,
		"standbys", len(standbys),
		"total_standbys", len(standbys))

	return nil
}

// verifyBootstrapNeeded checks if bootstrap is still needed by making fresh RPC calls.
// Returns true if all reachable nodes are uninitialized, false if any node is initialized.
func (a *BootstrapShardAction) verifyBootstrapNeeded(ctx context.Context, cohort []*multiorchdatapb.PoolerHealthState) (bool, error) {
	for _, pooler := range cohort {
		req := &multipoolermanagerdatapb.StatusRequest{}
		resp, err := a.rpcClient.Status(ctx, pooler.MultiPooler, req)
		if err != nil {
			// Node unreachable - can't determine state, continue checking others
			a.logger.WarnContext(ctx, "node unreachable during bootstrap recheck",
				"node", pooler.MultiPooler.Id.Name,
				"error", err)
			continue
		}

		if resp.Status != nil && resp.Status.IsInitialized {
			a.logger.InfoContext(ctx, "node is now initialized (fresh RPC check), skipping bootstrap",
				"node", pooler.MultiPooler.Id.Name,
				"postgres_role", resp.Status.PostgresRole)
			return false, nil
		}
	}

	return true, nil
}

// selectBootstrapCandidate selects the first reachable node as the bootstrap candidate.
// Assumes the analyzer correctly determined that all nodes are uninitialized.
func (a *BootstrapShardAction) selectBootstrapCandidate(ctx context.Context, cohort []*multiorchdatapb.PoolerHealthState) (*multiorchdatapb.PoolerHealthState, error) {
	for _, pooler := range cohort {
		req := &multipoolermanagerdatapb.StatusRequest{}
		_, err := a.rpcClient.Status(ctx, pooler.MultiPooler, req)
		if err != nil {
			a.logger.WarnContext(ctx, "pooler unreachable during candidate selection",
				"pooler", pooler.MultiPooler.Id.Name,
				"error", err)
			continue
		}

		a.logger.InfoContext(ctx, "selected pooler as bootstrap candidate",
			"pooler", pooler.MultiPooler.Id.Name)
		return pooler, nil
	}

	return nil, mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE,
		"no reachable candidate found for bootstrap")
}

// configureSynchronousReplication configures synchronous replication on the primary
// based on the durability policy and available standbys.
func (a *BootstrapShardAction) configureSynchronousReplication(
	ctx context.Context,
	primary *multiorchdatapb.PoolerHealthState,
	standbys []*multiorchdatapb.PoolerHealthState,
	quorumRule *clustermetadatapb.QuorumRule,
) error {
	// Build sync replication config using shared function
	syncConfig, err := coordinator.BuildSyncReplicationConfig(a.logger, quorumRule, standbys, primary)
	if err != nil {
		return mterrors.Wrap(err, "failed to build sync replication config")
	}

	// If syncConfig is nil, sync replication is not needed (e.g., required_count=1)
	if syncConfig == nil {
		a.logger.InfoContext(ctx, "Skipping synchronous replication configuration",
			"reason", "not required by policy")
		return nil
	}

	// Configure synchronous replication on the primary
	_, err = a.rpcClient.ConfigureSynchronousReplication(ctx, primary.MultiPooler, syncConfig)
	if err != nil {
		return mterrors.Wrap(err, "failed to configure synchronous replication")
	}

	a.logger.InfoContext(ctx, "Configured synchronous replication",
		"primary", primary.MultiPooler.Id.Name,
		"num_sync", syncConfig.NumSync,
		"standby_count", len(syncConfig.StandbyIds))

	return nil
}

// countReachablePoolers counts how many poolers in the cohort are reachable via RPC.
func (a *BootstrapShardAction) countReachablePoolers(ctx context.Context, cohort []*multiorchdatapb.PoolerHealthState) int {
	count := 0
	for _, pooler := range cohort {
		// Use a per-RPC timeout to prevent a single slow/unresponsive pooler from
		// consuming the entire action timeout.
		rpcCtx, cancel := context.WithTimeout(ctx, a.statusRPCTimeout)
		req := &multipoolermanagerdatapb.StatusRequest{}
		_, err := a.rpcClient.Status(rpcCtx, pooler.MultiPooler, req)
		cancel()
		if err == nil {
			count++
		}
	}
	return count
}

// getCohort fetches all poolers in the shard from the pooler store.
func (a *BootstrapShardAction) getCohort(shardKey commontypes.ShardKey) []*multiorchdatapb.PoolerHealthState {
	var cohort []*multiorchdatapb.PoolerHealthState

	a.poolerStore.Range(func(key string, pooler *multiorchdatapb.PoolerHealthState) bool {
		if pooler == nil || pooler.MultiPooler == nil || pooler.MultiPooler.Id == nil {
			return true // continue
		}

		if pooler.MultiPooler.Database == shardKey.Database &&
			pooler.MultiPooler.TableGroup == shardKey.TableGroup &&
			pooler.MultiPooler.Shard == shardKey.Shard {
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
