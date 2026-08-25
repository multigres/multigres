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
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// Compile-time assertion that ReconnectRecruitAbandonedAction implements types.RecoveryAction.
var _ types.RecoveryAction = (*ReconnectRecruitAbandonedAction)(nil)

// ReconnectRecruitAbandonedAction reconnects a follower stranded by an abandoned
// recruit (ProblemReplicaRecruitAbandoned): its TermRevocation revokes the
// leader's committed rule, so it rejects a plain SetPrimary. The fix is a
// leader-led no-op rule advance — UpdateConsensusRule(ADVANCE) rewrites the rule
// with the same leader and cohort at a fresh leader_subterm, moving the
// committed decision past the revocation's outgoing_rule so IsRuleRevoked no
// longer holds — followed by a SetPrimary the follower now accepts.
//
// Idempotency: UpdateConsensusRule is compare-and-swap guarded on the expected
// outgoing rule, so concurrent orchestrators cannot double-advance; a loser sees
// the rule already advanced and its later SetPrimary is harmless.
type ReconnectRecruitAbandonedAction struct {
	config      *config.Config
	rpcClient   rpcclient.MultipoolerClient
	poolerStore *store.PoolerCache
	logger      *slog.Logger
}

// NewReconnectRecruitAbandonedAction creates a new reconnect action.
func NewReconnectRecruitAbandonedAction(
	cfg *config.Config,
	rpcClient rpcclient.MultipoolerClient,
	poolerStore *store.PoolerCache,
	logger *slog.Logger,
) *ReconnectRecruitAbandonedAction {
	return &ReconnectRecruitAbandonedAction{
		config:      cfg,
		rpcClient:   rpcClient,
		poolerStore: poolerStore,
		logger:      logger,
	}
}

// Execute advances the leader's rule and reconnects the stranded follower.
func (a *ReconnectRecruitAbandonedAction) Execute(ctx context.Context, rechecked types.RecheckedProblem) error {
	problem := rechecked.Problem
	a.logger.InfoContext(ctx, "executing reconnect recruit-abandoned action",
		"shard_key", problem.ShardKey.String(),
		"pooler", problem.PoolerID.Name,
		"problem_code", string(problem.Code))

	follower, err := store.FindPoolerByID(a.poolerStore, problem.PoolerID)
	if err != nil {
		return mterrors.Wrap(err, "failed to find stranded follower")
	}

	members := store.FindShardMembers(a.poolerStore, problem.ShardKey)
	leader := members.Leader
	if leader == nil || members.HighestKnownPosition == nil {
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"no consensus leader known for shard %s", problem.ShardKey)
	}

	revocation := follower.Health().GetConsensusStatus().GetTermRevocation()

	// Advance the rule only if the follower is still stranded — the highest known
	// rule may already outrank the revocation (its decision is high enough, even
	// mid-transition with an outstanding proposal, or a prior cycle / racing
	// orchestrator already advanced it), in which case SetPrimary is safe to send
	// as-is and no advance is needed.
	advanced := members.HighestKnownPosition
	if commonconsensus.IsRuleRevoked(advanced, revocation) {
		// Advancing rewrites the rule; the leader refuses that while a proposal is
		// undecided (it CAS-guards on the decided outgoing rule), so defer to a
		// later cycle rather than erroring.
		if !commonconsensus.IsRuleDecided(advanced) {
			return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
				"shard %s has an undecided proposal; cannot advance rule yet", problem.ShardKey)
		}
		req := &multipoolermanagerdatapb.UpdateConsensusRuleRequest{
			Operation:            multipoolermanagerdatapb.RuleOperation_RULE_OPERATION_ADVANCE,
			ExpectedOutgoingRule: members.HighestKnownPosition.GetDecision().GetRuleNumber(),
		}
		resp, err := a.rpcClient.UpdateConsensusRule(ctx, leader.Health().Multipooler, req)
		if err != nil {
			return mterrors.Wrap(err, "leader-led rule advance failed")
		}
		advanced = resp.GetCurrentPosition().GetPosition()
		if commonconsensus.IsRuleRevoked(advanced, revocation) {
			return mterrors.Errorf(mtrpcpb.Code_INTERNAL,
				"leader reported rule %s still short of the follower's revocation after advancing",
				commonconsensus.FormatRulePosition(advanced))
		}
		a.logger.InfoContext(ctx, "advanced leader rule to reconnect stranded follower",
			"leader", leader.Health().Multipooler.Id.Name,
			"follower", follower.Health().Multipooler.Id.Name)
	}

	// Relay the advanced decision to the follower. It no longer revokes this rule,
	// so it accepts SetPrimary and rejoins. RewindReady is relayed so a follower
	// that also needs a rewind defers it until the leader is checkpointed.
	setReq := &consensusdatapb.SetPrimaryRequest{
		ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{
			Position:    advanced,
			Primary:     topoclient.PoolerAddressFor(leader.Health().Multipooler),
			RewindReady: commonconsensus.ReplicationPrimaryOrNil(leader.Health().GetConsensusStatus()).GetRewindReady(),
		},
	}
	if _, err := a.rpcClient.SetPrimary(ctx, follower.Health().Multipooler, setReq); err != nil {
		return mterrors.Wrap(err, "SetPrimary to reconnect stranded follower failed")
	}

	a.logger.InfoContext(ctx, "reconnect recruit-abandoned action completed",
		"leader", leader.Health().Multipooler.Id.Name,
		"follower", follower.Health().Multipooler.Id.Name)
	return nil
}

// RequiresHealthyLeader reports that this action needs a healthy leader: it runs
// UpdateConsensusRule on the leader.
func (a *ReconnectRecruitAbandonedAction) RequiresHealthyLeader() bool {
	return true
}

func (a *ReconnectRecruitAbandonedAction) Metadata() types.RecoveryMetadata {
	return types.RecoveryMetadata{
		Name:        "ReconnectRecruitAbandoned",
		Description: "Advance the leader rule to reconnect a follower stranded by an abandoned recruit",
		// Not urgent (the follower just stays stranded a bit longer), and
		// LeaderWritesProgressing already gates against attempting this while
		// a real election looks to be in flight — so under normal conditions
		// both RPCs are fast. No reason to still and potentially block the
		// recovery loop over this.
		Timeout:     30 * time.Second,
		LockTimeout: 15 * time.Second,
		Retryable:   true,
	}
}

func (a *ReconnectRecruitAbandonedAction) GracePeriod() *types.GracePeriodConfig {
	return nil
}
