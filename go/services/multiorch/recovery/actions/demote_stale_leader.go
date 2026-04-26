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

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/eventlog"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	commontypes "github.com/multigres/multigres/go/common/types"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"

	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// Compile-time assertion that DemoteStaleLeaderAction implements types.RecoveryAction.
var _ types.RecoveryAction = (*DemoteStaleLeaderAction)(nil)

// StaleLeaderDrainTimeout is a shorter drain timeout for stale primaries.
// Stale primaries that just came back online typically have no active connections,
// so we use a shorter timeout to speed up demotion.
const StaleLeaderDrainTimeout = 5 * time.Second

// DemoteStaleLeaderAction demotes a stale leader that was detected after failover.
// It uses the DemoteStalePrimary RPC with the correct primary's term to force the stale leader
// to accept the term and demote, preventing further writes.
type DemoteStaleLeaderAction struct {
	config      *config.Config
	rpcClient   rpcclient.MultiPoolerClient
	poolerStore *store.PoolerStore
	topoStore   topoclient.Store
	logger      *slog.Logger
}

// NewDemoteStaleLeaderAction creates a new action to demote a stale leader.
func NewDemoteStaleLeaderAction(
	cfg *config.Config,
	rpcClient rpcclient.MultiPoolerClient,
	poolerStore *store.PoolerStore,
	topoStore topoclient.Store,
	logger *slog.Logger,
) *DemoteStaleLeaderAction {
	return &DemoteStaleLeaderAction{
		config:      cfg,
		rpcClient:   rpcClient,
		poolerStore: poolerStore,
		topoStore:   topoStore,
		logger:      logger,
	}
}

func (a *DemoteStaleLeaderAction) Metadata() types.RecoveryMetadata {
	return types.RecoveryMetadata{
		Name:        "DemoteStaleLeader",
		Description: "Demote a stale leader that came back online after failover",
		Timeout:     60 * time.Second,
		LockTimeout: 15 * time.Second,
		Retryable:   true,
	}
}

func (a *DemoteStaleLeaderAction) Priority() types.Priority {
	return types.PriorityHigh
}

func (a *DemoteStaleLeaderAction) RequiresHealthyPrimary() bool {
	// We're demoting a primary, so we can't require a healthy primary
	return false
}

func (a *DemoteStaleLeaderAction) GracePeriod() *types.GracePeriodConfig {
	return &types.GracePeriodConfig{
		BaseDelay: a.config.GetLeaderFailoverGracePeriodBase(),
		MaxJitter: a.config.GetLeaderFailoverGracePeriodMaxJitter(),
	}
}

// Execute demotes the stale leader using the DemoteStalePrimary RPC with the correct primary's term.
// This is safer than BeginTerm because:
// 1. We use the correct primary's term (not a new term), avoiding term inconsistency
// 2. The stale leader accepts term >= its current term and demotes
// 3. Both primaries end up with the same term (no term inconsistency)
func (a *DemoteStaleLeaderAction) Execute(ctx context.Context, problem types.Problem) (retErr error) {
	poolerIDStr := topoclient.MultiPoolerIDString(problem.PoolerID)

	a.logger.InfoContext(ctx, "executing demote stale leader action",
		"shard_key", problem.ShardKey.String(),
		"stale_leader", poolerIDStr)

	// Get the stale leader from the store
	staleLeader, ok := a.poolerStore.Get(poolerIDStr)
	if !ok {
		return fmt.Errorf("stale leader %s not found in store", poolerIDStr)
	}

	// Check if postgres is running on the stale leader before attempting demote.
	// Demote requires postgres to be healthy. If postgres is not running yet,
	// we should skip this attempt and let the next recovery cycle retry once
	// postgres is ready. This avoids wasting time on RPCs that will fail.
	// if !stalePrimary.IsPostgresReady {
	// 	return mterrors.New(mtrpcpb.Code_UNAVAILABLE,
	// 		fmt.Sprintf("postgres not running on stale leader %s, skipping demote attempt", poolerIDStr))
	// }

	// Find the correct leader to use as rewind source
	correctLeader, correctLeaderTerm, err := a.findCorrectLeader(problem.ShardKey, poolerIDStr)
	if err != nil {
		return mterrors.Wrap(err, "failed to find correct leader")
	}

	a.logger.InfoContext(ctx, "demoting stale leader using DemoteStalePrimary RPC",
		"stale_leader", poolerIDStr,
		"correct_leader", correctLeader.MultiPooler.Id.Name,
		"correct_leader_term", correctLeaderTerm)

	eventlog.Emit(ctx, a.logger, eventlog.Started, eventlog.PrimaryDemotion{NodeName: poolerIDStr, Reason: "stale"})
	defer func() {
		if retErr == nil {
			eventlog.Emit(ctx, a.logger, eventlog.Success, eventlog.PrimaryDemotion{NodeName: poolerIDStr, Reason: "stale"})
		} else {
			eventlog.Emit(ctx, a.logger, eventlog.Failed, eventlog.PrimaryDemotion{NodeName: poolerIDStr, Reason: "stale"}, "error", retErr)
		}
	}()

	// Call DemoteStalePrimary RPC - this will:
	// 1. Stop postgres
	// 2. Run pg_rewind to sync with the correct leader's postgres
	// 3. Restart as standby
	// 4. Clear sync replication config
	// 5. Update topology to REPLICA
	demoteResp, err := a.rpcClient.DemoteStalePrimary(ctx, staleLeader.MultiPooler, &multipoolermanagerdatapb.DemoteStalePrimaryRequest{
		Source:        correctLeader.MultiPooler,
		ConsensusTerm: correctLeaderTerm,
		Force:         false,
	})
	if err != nil {
		return mterrors.Wrap(err, "DemoteStalePrimary RPC failed")
	}

	a.logger.InfoContext(ctx, "stale leader demoted successfully",
		"stale_leader", poolerIDStr,
		"rewind_performed", demoteResp.RewindPerformed,
		"lsn_position", demoteResp.LsnPosition)

	a.logger.InfoContext(ctx, "demote stale leader action completed",
		"shard_key", problem.ShardKey.String(),
		"demoted_leader", poolerIDStr)

	return nil
}

// findCorrectLeader finds the current leader in the shard and returns it along with its term.
// The correct leader is the one with the highest LeaderTerm.
func (a *DemoteStaleLeaderAction) findCorrectLeader(shardKey commontypes.ShardKey, stalePrimaryIDStr string) (*multiorchdatapb.PoolerHealthState, int64, error) {
	var correctLeader *multiorchdatapb.PoolerHealthState
	var maxLeaderTerm int64

	// Iterate through all poolers to find the current leader
	a.poolerStore.Range(func(key string, pooler *multiorchdatapb.PoolerHealthState) bool {
		if pooler == nil || pooler.MultiPooler == nil || pooler.MultiPooler.Id == nil {
			return true // continue
		}

		// Only consider poolers in the same shard
		if pooler.MultiPooler.Database != shardKey.Database ||
			pooler.MultiPooler.TableGroup != shardKey.TableGroup ||
			pooler.MultiPooler.Shard != shardKey.Shard {
			return true // continue
		}

		poolerIDStr := topoclient.MultiPoolerIDString(pooler.MultiPooler.Id)

		// Skip the stale leader
		if poolerIDStr == stalePrimaryIDStr {
			return true // continue
		}

		if !commonconsensus.IsLeader(pooler.GetConsensusStatus().GetConsensusStatus()) {
			return true // continue
		}

		leaderTerm := commonconsensus.LeaderTerm(pooler.GetConsensusStatus().GetConsensusStatus())

		if leaderTerm > maxLeaderTerm {
			maxLeaderTerm = leaderTerm
			correctLeader = pooler
		}

		return true // continue
	})

	if correctLeader == nil {
		return nil, 0, fmt.Errorf("no current leader found in shard %s", shardKey.String())
	}

	// Return consensus term for the RPC parameter
	consensusTerm := int64(0)
	if correctLeader.ConsensusStatus != nil {
		consensusTerm = correctLeader.ConsensusStatus.CurrentTerm
	}

	return correctLeader, consensusTerm, nil
}
