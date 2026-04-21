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

// shardInitCoordinator is the subset of consensus.Coordinator used by ShardInitAction.
type shardInitCoordinator interface {
	GetBootstrapPolicy(ctx context.Context, database string) (*clustermetadatapb.DurabilityPolicy, error)
	AppointInitialLeader(ctx context.Context, shardID string, cohort []*multiorchdatapb.PoolerHealthState, database string) error
	GetCoordinatorID() *clustermetadatapb.ID
}

// Compile-time assertion that ShardInitAction implements types.RecoveryAction.
var _ types.RecoveryAction = (*ShardInitAction)(nil)

// ShardInitAction handles Phase 2 of shard bootstrap: the first backup has already
// been created and all poolers have restored from it. This action:
//  1. Reads the pooler store to collect initialized poolers and verify no cohort
//     is established yet
//  2. Ensures enough initialized poolers are available to satisfy the durability policy
//  3. Claims the exclusive right to initialize via topoStore.ClaimShardInitialization
//  4. Calls coordinator.AppointInitialLeader with the initialized poolers
type ShardInitAction struct {
	config      *config.Config
	coordinator shardInitCoordinator
	poolerStore *store.PoolerStore
	topoStore   topoclient.Store
	logger      *slog.Logger
}

// NewShardInitAction creates a new ShardInitAction.
func NewShardInitAction(
	cfg *config.Config,
	coordinator shardInitCoordinator,
	poolerStore *store.PoolerStore,
	topoStore topoclient.Store,
	logger *slog.Logger,
) *ShardInitAction {
	return &ShardInitAction{
		config:      cfg,
		coordinator: coordinator,
		poolerStore: poolerStore,
		topoStore:   topoStore,
		logger:      logger,
	}
}

// Execute performs the initial cohort establishment for a bootstrapped shard.
func (a *ShardInitAction) Execute(ctx context.Context, problem types.Problem) error {
	a.logger.InfoContext(ctx, "executing shard init action",
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
	policyProto, err := a.coordinator.GetBootstrapPolicy(ctx, problem.ShardKey.Database)
	if err != nil {
		return mterrors.Wrap(err, "failed to load durability policy from topology")
	}
	durabilityPolicy, err := commonconsensus.NewPolicyFromProto(policyProto)
	if err != nil {
		return mterrors.Wrap(err, "failed to parse durability policy")
	}

	// Ensure the initialized poolers we see could ever satisfy the durability policy.
	initializedIDs := make([]*clustermetadatapb.ID, len(initializedPoolers))
	for i, p := range initializedPoolers {
		initializedIDs[i] = p.MultiPooler.Id
	}
	if err := durabilityPolicy.CheckAchievable(initializedIDs); err != nil {
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"insufficient initialized poolers for initial cohort (have %d): %v", len(initializedPoolers), err)
	}

	a.logger.InfoContext(ctx, "quorum of initialized poolers available",
		"shard_key", problem.ShardKey.String(),
		"initialized_count", len(initializedPoolers),
		"policy", durabilityPolicy.Description())

	// Claim the exclusive right to initialize this shard and persist the cohort.
	// On crash-retry the committed cohort is returned so we reuse the same members.
	won, committedIDs, err := a.topoStore.ClaimShardInitialization(ctx, problem.ShardKey, a.coordinator.GetCoordinatorID(), initializedIDs)
	if err != nil {
		return mterrors.Wrap(err, "failed to claim shard initialization")
	}
	if !won {
		a.logger.InfoContext(ctx, "shard initialization claimed by another coordinator, skipping",
			"shard_key", problem.ShardKey.String())
		return nil
	}

	a.logger.InfoContext(ctx, "shard initialization claimed",
		"shard_key", problem.ShardKey.String(),
		"committed_cohort_size", len(committedIDs))

	// Resolve committed IDs to full PoolerHealthState entries from the pooler store.
	committedCohort := a.buildCohortFromIDs(initializedPoolers, committedIDs)
	committedCohortIDs := make([]*clustermetadatapb.ID, len(committedCohort))
	for i, p := range committedCohort {
		committedCohortIDs[i] = p.MultiPooler.Id
	}
	if err := durabilityPolicy.CheckAchievable(committedCohortIDs); err != nil {
		return mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE,
			"insufficient committed cohort poolers reachable (have %d of %d): %v",
			len(committedCohort), len(committedIDs), err)
	}

	if err := a.coordinator.AppointInitialLeader(ctx, problem.ShardKey.Shard, committedCohort, problem.ShardKey.Database); err != nil {
		return mterrors.Wrap(err, "failed to appoint initial leader")
	}

	a.logger.InfoContext(ctx, "shard init action completed successfully",
		"shard_key", problem.ShardKey.String())
	return nil
}

// getInitializedPoolers reads fresh pooler state from the store (already refreshed by the
// recovery loop before Execute is called). It returns the list of initialized poolers, plus
// a bool indicating whether the cohort is already established (any pooler has CohortMembers).
// If cohortEstablished is true the returned slice is nil and the caller should no-op.
func (a *ShardInitAction) getInitializedPoolers(shardKey commontypes.ShardKey) (initialized []*multiorchdatapb.PoolerHealthState, cohortEstablished bool) {
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
			initialized = nil
			return false // stop iteration
		}
		if pooler.IsInitialized {
			initialized = append(initialized, pooler)
		}
		return true
	})
	return initialized, cohortEstablished
}

// buildCohortFromIDs resolves committed cohort IDs to PoolerHealthState entries
// from the local pooler store. Returns only the poolers we can find locally.
func (a *ShardInitAction) buildCohortFromIDs(poolers []*multiorchdatapb.PoolerHealthState, committedIDs []*clustermetadatapb.ID) []*multiorchdatapb.PoolerHealthState {
	idSet := make(map[string]struct{}, len(committedIDs))
	for _, id := range committedIDs {
		idSet[topoclient.ClusterIDString(id)] = struct{}{}
	}

	var result []*multiorchdatapb.PoolerHealthState
	for _, p := range poolers {
		if _, ok := idSet[topoclient.ClusterIDString(p.MultiPooler.Id)]; ok {
			result = append(result, p)
		}
	}
	return result
}

// RecoveryAction interface implementation

func (a *ShardInitAction) RequiresHealthyPrimary() bool {
	return false
}

func (a *ShardInitAction) Metadata() types.RecoveryMetadata {
	return types.RecoveryMetadata{
		Name:        "ShardInit",
		Description: "Establish initial cohort and appoint first leader for a bootstrapped shard",
		Timeout:     60 * time.Second,
		LockTimeout: 15 * time.Second,
		Retryable:   true,
	}
}

func (a *ShardInitAction) Priority() types.Priority {
	return types.PriorityShardBootstrap
}

func (a *ShardInitAction) GracePeriod() *types.GracePeriodConfig {
	return nil
}
