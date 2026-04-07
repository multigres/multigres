// Copyright 2026 Supabase, Inc.
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

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	commontypes "github.com/multigres/multigres/go/common/types"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/consensus"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// Compile-time assertion that InitialCohortAction implements types.RecoveryAction.
var _ types.RecoveryAction = (*InitialCohortAction)(nil)

// InitialCohortAction handles Phase 2 of shard bootstrap: the first backup has already
// been created and all poolers have restored from it. This action:
//  1. Fetches the cohort from the pooler store
//  2. Re-verifies the shard still needs initial cohort establishment via fresh RPCs
//  3. Waits for enough initialized poolers to satisfy quorum
//  4. CAS-claims the initial cohort via topoStore.ClaimInitialCohort
//  5. Calls coordinator.AppointInitialLeader with the committed cohort
type InitialCohortAction struct {
	config      *config.Config
	coordinator *consensus.Coordinator
	poolerStore *store.PoolerStore
	rpcClient   rpcclient.MultiPoolerClient
	topoStore   topoclient.Store
	logger      *slog.Logger
}

// NewInitialCohortAction creates a new InitialCohortAction.
func NewInitialCohortAction(
	cfg *config.Config,
	coordinator *consensus.Coordinator,
	poolerStore *store.PoolerStore,
	rpcClient rpcclient.MultiPoolerClient,
	topoStore topoclient.Store,
	logger *slog.Logger,
) *InitialCohortAction {
	return &InitialCohortAction{
		config:      cfg,
		coordinator: coordinator,
		poolerStore: poolerStore,
		rpcClient:   rpcClient,
		topoStore:   topoStore,
		logger:      logger,
	}
}

// Execute performs the initial cohort establishment for a bootstrapped shard.
func (a *InitialCohortAction) Execute(ctx context.Context, problem types.Problem) error {
	a.logger.InfoContext(ctx, "executing initial cohort action",
		"database", problem.ShardKey.Database,
		"tablegroup", problem.ShardKey.TableGroup,
		"shard", problem.ShardKey.Shard)

	cohort := a.getCohort(problem.ShardKey)
	if len(cohort) == 0 {
		return fmt.Errorf("no poolers found for shard %s", problem.ShardKey)
	}

	// Load the bootstrap durability policy from topology (not from nodes) because poolers
	// are UNKNOWN type at this point — they have just restored from backup and are in
	// hot-standby mode.
	policy, err := a.coordinator.GetBootstrapPolicy(ctx, problem.ShardKey.Database)
	if err != nil {
		return mterrors.Wrap(err, "failed to load durability policy from topology")
	}

	// Verify via fresh RPCs that the shard still needs initial cohort establishment.
	initialCohortNeeded, initializedPoolers, err := a.verifyAndCollectInitialized(ctx, cohort)
	if err != nil {
		return mterrors.Wrap(err, "failed to verify initial cohort needed")
	}
	if !initialCohortNeeded {
		a.logger.InfoContext(ctx, "shard no longer needs initial cohort, already established",
			"shard_key", problem.ShardKey.String())
		return nil
	}

	// Ensure enough initialized poolers are available to satisfy the durability policy.
	if !commonconsensus.IsDurabilityPolicyAchievable(policy, len(initializedPoolers)) {
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"insufficient initialized poolers for initial cohort: have %d, need %d for quorum",
			len(initializedPoolers), policy.RequiredCount)
	}

	a.logger.InfoContext(ctx, "quorum of initialized poolers available",
		"shard_key", problem.ShardKey.String(),
		"initialized_count", len(initializedPoolers),
		"required_count", policy.RequiredCount)

	// Build the proposed cohort from initialized poolers.
	proposedIDs := make([]string, 0, len(initializedPoolers))
	for _, p := range initializedPoolers {
		proposedIDs = append(proposedIDs, p.MultiPooler.Id.Name)
	}

	// CAS-claim the initial cohort. This is idempotent: if another multiorch instance
	// already claimed the cohort, we get back the committed IDs instead of our proposal.
	committedIDs, err := a.topoStore.ClaimInitialCohort(ctx, problem.ShardKey, proposedIDs)
	if err != nil {
		return mterrors.Wrap(err, "failed to claim initial cohort")
	}

	a.logger.InfoContext(ctx, "initial cohort claimed",
		"shard_key", problem.ShardKey.String(),
		"committed_ids", committedIDs)

	// Build the committed cohort from the topology-committed IDs. Use the pooler store
	// entries so that each PoolerHealthState includes address/port information.
	committedCohort := a.buildCohortFromIDs(cohort, committedIDs)
	if len(committedCohort) == 0 {
		return mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE,
			"none of the committed cohort poolers are known to this multiorch instance: %v", committedIDs)
	}

	if err := a.coordinator.AppointInitialLeader(ctx, problem.ShardKey.Shard, committedCohort, problem.ShardKey.Database); err != nil {
		return mterrors.Wrap(err, "failed to appoint initial leader")
	}

	a.logger.InfoContext(ctx, "initial cohort action completed successfully",
		"shard_key", problem.ShardKey.String())
	return nil
}

// verifyAndCollectInitialized makes fresh Status RPCs to verify the shard still needs
// initial cohort establishment and collects all initialized poolers.
//
// Returns (false, nil, nil) if any pooler reports a cohort is already established.
// Returns (true, initializedList, nil) if no cohort is established.
func (a *InitialCohortAction) verifyAndCollectInitialized(ctx context.Context, cohort []*multiorchdatapb.PoolerHealthState) (bool, []*multiorchdatapb.PoolerHealthState, error) {
	var initialized []*multiorchdatapb.PoolerHealthState

	for _, pooler := range cohort {
		req := &multipoolermanagerdatapb.StatusRequest{}
		resp, err := a.rpcClient.Status(ctx, pooler.MultiPooler, req)
		if err != nil {
			a.logger.WarnContext(ctx, "pooler unreachable during initial cohort recheck",
				"pooler", pooler.MultiPooler.Id.Name,
				"error", err)
			continue
		}

		if resp.Status == nil {
			continue
		}

		// If any pooler already has cohort members, the cohort is established.
		if len(resp.Status.CohortMembers) > 0 {
			a.logger.InfoContext(ctx, "cohort already established (fresh RPC check), skipping",
				"pooler", pooler.MultiPooler.Id.Name,
				"cohort_size", len(resp.Status.CohortMembers))
			return false, nil, nil
		}

		if resp.Status.IsInitialized {
			initialized = append(initialized, pooler)
		}
	}

	return true, initialized, nil
}

// buildCohortFromIDs returns the subset of cohort poolers whose names are in committedIDs.
func (a *InitialCohortAction) buildCohortFromIDs(cohort []*multiorchdatapb.PoolerHealthState, committedIDs []string) []*multiorchdatapb.PoolerHealthState {
	idSet := make(map[string]struct{}, len(committedIDs))
	for _, id := range committedIDs {
		idSet[id] = struct{}{}
	}

	var result []*multiorchdatapb.PoolerHealthState
	for _, p := range cohort {
		if _, ok := idSet[p.MultiPooler.Id.Name]; ok {
			result = append(result, p)
		}
	}
	return result
}

// getCohort fetches all poolers in the shard from the pooler store.
func (a *InitialCohortAction) getCohort(shardKey commontypes.ShardKey) []*multiorchdatapb.PoolerHealthState {
	var cohort []*multiorchdatapb.PoolerHealthState

	a.poolerStore.Range(func(key string, pooler *multiorchdatapb.PoolerHealthState) bool {
		if pooler == nil || pooler.MultiPooler == nil || pooler.MultiPooler.Id == nil {
			return true
		}

		if pooler.MultiPooler.Database == shardKey.Database &&
			pooler.MultiPooler.TableGroup == shardKey.TableGroup &&
			pooler.MultiPooler.Shard == shardKey.Shard {
			cohort = append(cohort, pooler)
		}

		return true
	})

	return cohort
}

// RecoveryAction interface implementation

func (a *InitialCohortAction) RequiresHealthyPrimary() bool {
	return false
}

func (a *InitialCohortAction) Metadata() types.RecoveryMetadata {
	return types.RecoveryMetadata{
		Name:        "InitialCohort",
		Description: "Establish initial cohort and appoint first leader for a bootstrapped shard",
		Timeout:     60 * time.Second,
		LockTimeout: 15 * time.Second,
		Retryable:   true,
	}
}

func (a *InitialCohortAction) Priority() types.Priority {
	return types.PriorityShardBootstrap
}

func (a *InitialCohortAction) GracePeriod() *types.GracePeriodConfig {
	return nil
}
