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
// recruit (ProblemReplicaRecruitAbandoned).
//
// The follower's TermRevocation revokes the leader's committed rule — a failover
// was started at a higher term, reached this follower, then never committed — so
// the follower rejects a plain SetPrimary at the current rule. The fix is a
// leader-led no-op rule advance: UpdateConsensusRule(ADVANCE) rewrites the rule
// with the same leader and cohort at a fresh leader_subterm, committed by the
// rest of the cohort. That moves the committed decision past the rule the
// revocation was authored to transition away from (its outgoing_rule), which
// defeats the revocation via the runaway-recruit override in IsRuleRevoked
// without changing the coordinator term or any pooler's revocation. The action
// then relays the advanced decision via SetPrimary, which the follower now
// accepts, and it rejoins as a streaming standby.
//
// Idempotency: UpdateConsensusRule is compare-and-swap guarded on the expected
// outgoing rule, so concurrent orchestrators cannot double-advance; a loser sees
// the rule already advanced and its later SetPrimary is harmless.
type ReconnectRecruitAbandonedAction struct {
	config      *config.Config
	rpcClient   rpcclient.MultipoolerClient
	poolerStore *store.PoolerCache
	logger      *slog.Logger

	// Polling parameters for waiting on the advanced rule to be decided.
	verifyMaxAttempts  int
	verifyPollInterval time.Duration
}

// NewReconnectRecruitAbandonedAction creates a new reconnect action.
func NewReconnectRecruitAbandonedAction(
	cfg *config.Config,
	rpcClient rpcclient.MultipoolerClient,
	poolerStore *store.PoolerCache,
	logger *slog.Logger,
) *ReconnectRecruitAbandonedAction {
	maxAttempts := DefaultVerifyMaxAttempts
	pollInterval := DefaultVerifyPollInterval
	if cfg != nil {
		if timeout := cfg.GetVerifyReplicationTimeout(); timeout > 0 {
			maxAttempts = max(int(timeout/DefaultVerifyPollInterval), 1)
		}
	}
	return &ReconnectRecruitAbandonedAction{
		config:             cfg,
		rpcClient:          rpcClient,
		poolerStore:        poolerStore,
		logger:             logger,
		verifyMaxAttempts:  maxAttempts,
		verifyPollInterval: pollInterval,
	}
}

// Execute advances the leader's rule and reconnects the stranded follower.
func (a *ReconnectRecruitAbandonedAction) Execute(ctx context.Context, problem types.Problem) error {
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
		if _, err := a.rpcClient.UpdateConsensusRule(ctx, leader.Health().Multipooler, req); err != nil {
			return mterrors.Wrap(err, "leader-led rule advance failed")
		}
		a.logger.InfoContext(ctx, "advanced leader rule to reconnect stranded follower",
			"leader", leader.Health().Multipooler.Id.Name,
			"follower", follower.Health().Multipooler.Id.Name)

		advanced, err = a.waitForRulePastRevocation(ctx, leader, revocation)
		if err != nil {
			return err
		}
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

// waitForRulePastRevocation polls the leader until its committed decision has
// advanced past the follower's revocation (the ADVANCE is decided by the
// cohort), returning that decided position for the reconnect SetPrimary. Until
// the advance is decided, the follower would still reject SetPrimary, so this
// avoids a silently-ignored reconnect.
func (a *ReconnectRecruitAbandonedAction) waitForRulePastRevocation(
	ctx context.Context,
	leader *store.Pooler,
	revocation *clustermetadatapb.TermRevocation,
) (*clustermetadatapb.RulePosition, error) {
	ticker := time.NewTicker(a.verifyPollInterval)
	defer ticker.Stop()

	var lastPos *clustermetadatapb.RulePosition
	for attempt := 1; attempt <= a.verifyMaxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return nil, mterrors.Wrap(ctx.Err(), "context cancelled while waiting for rule advance")
		case <-ticker.C:
		}

		resp, err := a.rpcClient.Status(ctx, leader.Health().Multipooler, &multipoolermanagerdatapb.StatusRequest{})
		if err != nil {
			return nil, mterrors.Wrap(err, "failed to read leader status while waiting for rule advance")
		}
		pos := resp.GetConsensusStatus().GetCurrentPosition().GetPosition()
		lastPos = pos
		if commonconsensus.IsRuleDecided(pos) && !commonconsensus.IsRuleRevoked(pos, revocation) {
			return pos, nil
		}
	}
	return nil, mterrors.Errorf(mtrpcpb.Code_DEADLINE_EXCEEDED,
		"leader rule did not advance past the follower's revocation after %d attempts (last position %s)",
		a.verifyMaxAttempts, commonconsensus.FormatRulePosition(lastPos))
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
		Timeout:     30 * time.Second,
		LockTimeout: 15 * time.Second,
		Retryable:   true,
	}
}

func (a *ReconnectRecruitAbandonedAction) GracePeriod() *types.GracePeriodConfig {
	return nil
}
