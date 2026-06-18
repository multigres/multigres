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

	"google.golang.org/protobuf/proto"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/eventlog"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	commontypes "github.com/multigres/multigres/go/common/types"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"

	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// Compile-time assertion that DemoteStaleLeaderAction implements types.RecoveryAction.
var _ types.RecoveryAction = (*DemoteStaleLeaderAction)(nil)

// StaleLeaderDrainTimeout is a shorter drain timeout for stale leaders.
// Stale leaders that just came back online typically have no active connections,
// so we use a shorter timeout to speed up demotion.
const StaleLeaderDrainTimeout = 5 * time.Second

// DemoteStaleLeaderAction demotes a stale leader that was detected after failover.
// It uses the SetPrimary RPC with the correct leader's rule to force the stale leader
// to accept the new leader, run pg_rewind, and restart as a standby.
//
// TODO: this action and FixReplicationAction (which reconnects orphan replicas)
// now both reduce to a single SetPrimary RPC against the misbehaving node.
// They should eventually share one underlying action — detection can stay split
// for metrics/monitoring, but the mutation path is identical.
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

func (a *DemoteStaleLeaderAction) RequiresHealthyLeader() bool {
	// We're demoting a stale leader, so we can't require a healthy leader
	return false
}

func (a *DemoteStaleLeaderAction) GracePeriod() *types.GracePeriodConfig {
	// The demote goes through SetPrimary, which is position-fenced: a
	// leader that's only momentarily-stale would see its own rule >= the
	// incoming rule and no-op without touching postgres. A spurious detection
	// costs an RPC, not a wrongful demote, so no grace period is needed.
	return nil
}

// Execute demotes the stale leader using SetPrimary with the correct leader's rule.
// This is safer than Recruit because we use the correct leader's existing rule rather than
// minting a new term, so both leaders end up agreeing on the same term and rule.
func (a *DemoteStaleLeaderAction) Execute(ctx context.Context, problem types.Problem) (retErr error) {
	poolerIDStr := topoclient.ComponentIDString(problem.PoolerID)

	a.logger.InfoContext(ctx, "executing demote stale leader action",
		"shard_key", commontypes.FormatShardKey(problem.ShardKey),
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

	// Identify the rewind target from cached consensus state: the leader named by
	// the highest known rule across the shard (the global consensus view, never a
	// node's local self-claim).
	members := a.poolerStore.FindShardMembers(problem.ShardKey)
	correctLeader, correctRule := members.Leader, members.HighestKnownRule
	if correctLeader == nil || correctRule == nil {
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"no consensus leader known for shard %s", commontypes.FormatShardKey(problem.ShardKey))
	}

	// If the supposedly-stale node is itself the highest known consensus leader,
	// it is not actually stale (e.g. a spurious or already-resolved detection):
	// there is nothing newer to rewind it toward. Do nothing rather than send a
	// self-targeted SetPrimary, which the pooler would ignore.
	if proto.Equal(correctLeader.GetMultiPooler().GetId(), problem.PoolerID) {
		a.logger.InfoContext(ctx, "stale leader is the current consensus leader, nothing to demote",
			"leader", correctLeader.MultiPooler.Id.Name,
			"shard_key", commontypes.FormatShardKey(problem.ShardKey))
		return nil
	}

	a.logger.InfoContext(ctx, "demoting stale leader via SetPrimary",
		"stale_leader", poolerIDStr,
		"correct_leader", correctLeader.MultiPooler.Id.Name,
		"correct_leader_rule", commonconsensus.FormatRuleNumber(correctRule.GetRuleNumber()))

	eventlog.Emit(ctx, a.logger, eventlog.Started, eventlog.PrimaryDemotion{NodeName: string(poolerIDStr), Reason: "stale"})
	defer func() {
		if retErr == nil {
			eventlog.Emit(ctx, a.logger, eventlog.Success, eventlog.PrimaryDemotion{NodeName: string(poolerIDStr), Reason: "stale"})
		} else {
			eventlog.Emit(ctx, a.logger, eventlog.Failed, eventlog.PrimaryDemotion{NodeName: string(poolerIDStr), Reason: "stale"}, "error", retErr)
		}
	}()

	// Route through SetPrimary on the stale leader. The pooler-side handler:
	// 1. Stops postgres
	// 2. Runs pg_rewind to sync with the correct leader's postgres
	// 3. Restarts as standby
	// 4. Clears sync replication config
	// 5. Updates topology to REPLICA
	setPrimaryReq := &consensusdatapb.SetPrimaryRequest{
		Leader: topoclient.PoolerAddressFor(correctLeader.MultiPooler),
		Rule:   correctRule,
	}
	if _, err := a.rpcClient.SetPrimary(ctx, staleLeader.MultiPooler, setPrimaryReq); err != nil {
		return mterrors.Wrap(err, "SetPrimary RPC failed")
	}
	a.logger.InfoContext(ctx, "stale leader demoted successfully via SetPrimary",
		"stale_leader", poolerIDStr)

	a.logger.InfoContext(ctx, "demote stale leader action completed",
		"shard_key", commontypes.FormatShardKey(problem.ShardKey),
		"demoted_leader", poolerIDStr)

	return nil
}
