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
	"log/slog"
	"time"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/topoclient"
	commontypes "github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// initialCohortCoordinator is the subset of consensus.Coordinator used by InitialCohortAction.
type initialCohortCoordinator interface {
	GetBootstrapPolicy(ctx context.Context, database string) (*clustermetadatapb.DurabilityPolicy, error)
	AppointInitialLeader(ctx context.Context, shardID string, cohort []*multiorchdatapb.PoolerHealthState, database string) error
}

// Compile-time assertion that InitialCohortAction implements types.RecoveryAction.
var _ types.RecoveryAction = (*InitialCohortAction)(nil)

// InitialCohortAction handles Phase 2 of shard bootstrap: the first backup has already
// been created and all poolers have restored from it. This action:
//  1. Reads the pooler store (already refreshed by the recovery loop recheck) to collect
//     initialized poolers and verify no cohort is established yet
//  2. Ensures enough initialized poolers are available to satisfy the durability policy
//  3. CAS-claims the initial cohort via topoStore.ClaimInitialCohort
//  4. Calls coordinator.AppointInitialLeader with the committed cohort
type InitialCohortAction struct {
	config      *config.Config
	coordinator initialCohortCoordinator
	poolerStore *store.PoolerStore
	topoStore   topoclient.Store
	logger      *slog.Logger
}

// NewInitialCohortAction creates a new InitialCohortAction.
func NewInitialCohortAction(
	cfg *config.Config,
	coordinator initialCohortCoordinator,
	poolerStore *store.PoolerStore,
	topoStore topoclient.Store,
	logger *slog.Logger,
) *InitialCohortAction {
	return &InitialCohortAction{
		config:      cfg,
		coordinator: coordinator,
		poolerStore: poolerStore,
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

	// The recovery loop force-polls all poolers before calling Execute, so the pooler
	// store holds fresh state. getInitializedPoolers reads that state: it returns nil
	// if the cohort is already established, or the list of initialized poolers otherwise.
	initializedPoolers, cohortEstablished := a.getInitializedPoolers(problem.ShardKey)
	if cohortEstablished {
		a.logger.InfoContext(ctx, "cohort already established, skipping",
			"shard_key", problem.ShardKey.String())
		return nil
	}
	if len(initializedPoolers) == 0 {
		return mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE,
			"no initialized poolers found for shard %s", problem.ShardKey)
	}

	// Load the bootstrap durability policy from topology (not from nodes) because poolers
	// are UNKNOWN type at this point — they have just restored from backup and are in
	// hot-standby mode.
	policy, err := a.coordinator.GetBootstrapPolicy(ctx, problem.ShardKey.Database)
	if err != nil {
		return mterrors.Wrap(err, "failed to load durability policy from topology")
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
	committedCohort := a.buildCohortFromIDs(initializedPoolers, committedIDs)

	// The cohort may have been committed to etcd by a different multiorch instance; we may not
	// yet have all committed poolers in our store. Retry next cycle if reachable poolers
	// are insufficient to satisfy the durability policy.
	if !commonconsensus.IsDurabilityPolicyAchievable(policy, len(committedCohort)) {
		return mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE,
			"insufficient initial cohort poolers reachable: have %d of %v, need %d for quorum",
			len(committedCohort), committedIDs, policy.RequiredCount)
	}

	if err := a.coordinator.AppointInitialLeader(ctx, problem.ShardKey.Shard, committedCohort, problem.ShardKey.Database); err != nil {
		return mterrors.Wrap(err, "failed to appoint initial leader")
	}

	a.logger.InfoContext(ctx, "initial cohort action completed successfully",
		"shard_key", problem.ShardKey.String())
	return nil
}

// getInitializedPoolers reads fresh pooler state from the store (already refreshed by the
// recovery loop before Execute is called). It returns the list of initialized poolers, plus
// a bool indicating whether the cohort is already established (any pooler has CohortMembers).
// If cohortEstablished is true the returned slice is nil and the caller should no-op.
func (a *InitialCohortAction) getInitializedPoolers(shardKey commontypes.ShardKey) (initialized []*multiorchdatapb.PoolerHealthState, cohortEstablished bool) {
	a.poolerStore.Range(func(_ string, pooler *multiorchdatapb.PoolerHealthState) bool {
		if pooler == nil || pooler.MultiPooler == nil || pooler.MultiPooler.Id == nil {
			return true
		}
		if pooler.MultiPooler.Database != shardKey.Database ||
			pooler.MultiPooler.TableGroup != shardKey.TableGroup ||
			pooler.MultiPooler.Shard != shardKey.Shard {
			return true
		}
		if len(pooler.CohortMembers) > 0 {
			cohortEstablished = true
			return false // stop iteration
		}
		if pooler.IsInitialized {
			initialized = append(initialized, pooler)
		}
		return true
	})
	return initialized, cohortEstablished
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
