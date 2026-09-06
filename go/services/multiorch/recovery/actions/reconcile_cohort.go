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
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// Compile-time assertion that ReconcileCohortAction implements types.RecoveryAction.
var _ types.RecoveryAction = (*ReconcileCohortAction)(nil)

// ReconcileCohortAction applies a single cohort-membership change on the
// shard's leader.
//
// It handles two problem codes:
//   - ProblemPoolerNotInCohort: add the pooler via UpdateConsensusRule(ADD).
//   - ProblemCohortMemberIneligible: remove the pooler via UpdateConsensusRule(REMOVE).
//
// The action mutates exactly one cohort member per execution; multiple
// drifting members produce multiple problems and run separately.
//
// TODO: future work will likely cap the cohort size based on the durability
// policy and require a fitness heuristic to choose the best-qualified
// candidates among many eligible poolers. Today the action adds every
// eligible non-cohort pooler unconditionally.
type ReconcileCohortAction struct {
	config      *config.Config
	rpcClient   rpcclient.MultipoolerClient
	poolerStore *store.PoolerCache
	topoStore   topoclient.Store
	logger      *slog.Logger
}

// NewReconcileCohortAction creates a new cohort reconciliation action.
func NewReconcileCohortAction(
	cfg *config.Config,
	rpcClient rpcclient.MultipoolerClient,
	poolerStore *store.PoolerCache,
	topoStore topoclient.Store,
	logger *slog.Logger,
) *ReconcileCohortAction {
	return &ReconcileCohortAction{
		config:      cfg,
		rpcClient:   rpcClient,
		poolerStore: poolerStore,
		topoStore:   topoStore,
		logger:      logger,
	}
}

// Execute applies the cohort change on the shard leader. rechecked.HighestKnownRule
// is the exact position the engine's recheck just re-verified this problem
// against (recovery_loop.go's recheckProblem re-runs
// CohortMismatchAnalyzer.Analyze against it). Because that analyzer only
// emits ProblemCohortMemberIneligible when IsCohortMemberRemovalSafe already
// holds for this same rule — a pure, deterministic function of (rule,
// targetID) — a REMOVE problem surviving the recheck already proves removal
// is safe against the rule; Execute does not re-verify it. CAS-ing on the
// same rule (ExpectedOutgoingRule below) rather than a fresh, independent
// store read means the mutation can't apply against any rule other than the
// one just proven safe.
func (a *ReconcileCohortAction) Execute(ctx context.Context, rechecked types.RecheckedProblem) error {
	problem := rechecked.Problem
	rule := rechecked.HighestKnownRule
	a.logger.InfoContext(ctx, "executing reconcile cohort action",
		"shard_key", problem.ShardKey.String(),
		"pooler", problem.PoolerID.Name,
		"problem_code", string(problem.Code))

	var op multipoolermanagerdatapb.RuleOperation
	switch problem.Code {
	case types.ProblemPoolerNotInCohort:
		op = multipoolermanagerdatapb.RuleOperation_RULE_OPERATION_COHORT_ADD
	case types.ProblemCohortMemberIneligible:
		op = multipoolermanagerdatapb.RuleOperation_RULE_OPERATION_COHORT_REMOVE
	default:
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
			"unsupported problem code for reconcile cohort: %s", problem.Code)
	}

	if rule == nil {
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"no consensus rule known for shard %s", problem.ShardKey)
	}
	// A cohort change is leader-led: it's meaningless to compute one from an
	// outstanding, not-yet-decided proposal (no cohort is settled to add to
	// or remove from yet). Require a decided rule rather than relying on
	// LeaderWritesProgressing below to reject this incidentally.
	//
	// TODO: allow non-promotion rule changes to ride along with an
	// outstanding proposal via propagation, instead of always waiting for
	// decision.
	if !commonconsensus.IsRuleDecided(rule) {
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"shard %s has an undecided rule proposal; cohort reconciliation requires a decided rule", problem.ShardKey)
	}
	decidedRule := rule.GetDecision()

	// For ADD we need the pooler to be live in the cache (the cohort grows
	// only if we have a healthy replica) and we use it afterward to clear the
	// joining member's archive. For REMOVE the pooler may already be gone
	// from the cache (the whole point of "cohort member is no longer
	// tracked"), so we operate on the problem's raw ID directly.
	var targetID *clustermetadatapb.ID
	var target *store.Pooler
	if op == multipoolermanagerdatapb.RuleOperation_RULE_OPERATION_COHORT_ADD {
		t, err := store.FindPoolerByID(a.poolerStore, problem.PoolerID)
		if err != nil {
			return mterrors.Wrap(err, "failed to find target pooler")
		}
		target = t
		targetID = target.Health().Multipooler.Id
	} else {
		targetID = problem.PoolerID
	}

	// The leader named by rule may not be the pooler we happen to have
	// freshest connectivity/liveness data for — look it up by the ID the
	// rule itself names, rather than independently re-deriving "the leader"
	// from the cache the way the rule's own content already settles.
	leader, err := store.FindPoolerByID(a.poolerStore, decidedRule.GetLeaderId())
	if err != nil {
		return mterrors.Wrap(err, "failed to find leader named by consensus rule")
	}

	if !store.LeaderWritesProgressing(leader, rule, time.Now(), store.DefaultLeaderWriteFreshness) {
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"leader for shard %s does not look able to commit writes right now", problem.ShardKey)
	}

	// TODO: batch multiple cohort changes into a single UpdateConsensusRule
	// call. The proto already accepts repeated standby_ids; the analyzer emits
	// one Problem per pooler and the recovery engine dispatches one action per
	// problem, so each cycle currently fires N separate UpdateConsensusRule
	// RPCs (each triggering its own rule_history write and reload) even though
	// the underlying RPC could apply them in one shot. Coalescing same-shard,
	// same-operation problems would cut RPC fanout and history churn.
	req := &multipoolermanagerdatapb.UpdateConsensusRuleRequest{
		Operation:            op,
		StandbyIds:           []*clustermetadatapb.ID{targetID},
		ExpectedOutgoingRule: rule.GetDecision().GetRuleNumber(),
	}

	if _, err := a.rpcClient.UpdateConsensusRule(ctx, leader.Health().Multipooler, req); err != nil {
		return mterrors.Wrap(err, "UpdateConsensusRule failed")
	}

	// A member that joins an already-established cohort out-of-band (provisioned
	// after a failover, added here rather than through the promotion-time Recruit
	// wave) never received Recruit's synchronous restore_command clear. The ADD
	// above only amends the leader's rule + synchronous_standby_names; it runs on
	// the leader and cannot touch the joining member's restore_command. Left set,
	// a restart-as-standby can resolve recovery_target_timeline=latest through the
	// archive to a divergent timeline and FATAL at startup. Drive the member-side
	// clear synchronously by re-issuing SetPrimary carrying the post-ADD rule: the
	// member now sees itself named in that rule and clears restore_command before
	// the monitor's ~one-tick backstop would. Best-effort — the ADD (the action's
	// contract) already succeeded, and the monitor backstop still covers a failure
	// here — so a member-side hiccup does not fail cohort reconciliation.
	if op == multipoolermanagerdatapb.RuleOperation_RULE_OPERATION_COHORT_ADD && target != nil {
		a.clearJoiningMemberArchive(ctx, leader, target)
	}

	a.logger.InfoContext(ctx, "reconcile cohort action completed",
		"target", targetID.Name,
		"primary", leader.Health().Multipooler.Id.Name,
		"operation", op.String())
	return nil
}

// clearJoiningMemberArchive re-issues SetPrimary to a pooler just added to the
// cohort so it clears restore_command synchronously (see the caller for why).
//
// It re-reads the leader's status to obtain the post-ADD rule — the rule that
// now names the member — because the member-side clear keys off cohort
// membership as asserted by the rule this SetPrimary delivers. The cached
// pre-ADD rule would not name the member, so relaying it would not trigger the
// clear. Failures are logged and swallowed: this is a best-effort hardening step
// layered on top of the pooler's own monitor backstop.
func (a *ReconcileCohortAction) clearJoiningMemberArchive(ctx context.Context, leader, target *store.Pooler) {
	statusResp, err := a.rpcClient.Status(ctx, leader.Health().Multipooler, &multipoolermanagerdatapb.StatusRequest{})
	if err != nil {
		a.logger.WarnContext(ctx, "reconcile cohort: could not read leader status to clear joining member's archive; relying on monitor backstop",
			"target", target.Health().Multipooler.Id.Name, "error", err)
		return
	}
	// The leader's own rule store reflects the ADD synchronously (UpdateConsensusRule
	// commits before returning), so HighestKnownRule here is the post-ADD rule.
	postAddRule := commonconsensus.HighestKnownRule([]*clustermetadatapb.ConsensusStatus{statusResp.GetConsensusStatus()})
	if postAddRule == nil {
		a.logger.WarnContext(ctx, "reconcile cohort: leader reported no rule after ADD; relying on monitor backstop",
			"target", target.Health().Multipooler.Id.Name)
		return
	}
	setPrimaryReq := &consensusdatapb.SetPrimaryRequest{
		ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{
			Position:    postAddRule,
			Primary:     topoclient.PoolerAddressFor(leader.Health().Multipooler),
			RewindReady: commonconsensus.ReplicationPrimaryOrNil(statusResp.GetConsensusStatus()).GetRewindReady(),
		},
	}
	if _, err := a.rpcClient.SetPrimary(ctx, target.Health().Multipooler, setPrimaryReq); err != nil {
		a.logger.WarnContext(ctx, "reconcile cohort: SetPrimary to clear joining member's archive failed; relying on monitor backstop",
			"target", target.Health().Multipooler.Id.Name, "error", err)
	}
}

// RecoveryAction interface implementation

func (a *ReconcileCohortAction) RequiresHealthyLeader() bool {
	return true // UpdateConsensusRule must run on a healthy primary.
}

func (a *ReconcileCohortAction) Metadata() types.RecoveryMetadata {
	return types.RecoveryMetadata{
		Name:        "ReconcileCohort",
		Description: "Add or remove a single cohort member on the shard leader",
		Timeout:     30 * time.Second,
		LockTimeout: 15 * time.Second,
		Retryable:   true,
	}
}

func (a *ReconcileCohortAction) GracePeriod() *types.GracePeriodConfig {
	return nil
}
