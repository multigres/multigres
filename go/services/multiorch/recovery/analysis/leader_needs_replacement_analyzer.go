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

package analysis

import (
	"errors"
	"fmt"
	"time"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// LeaderNeedsReplacementAnalyzer judges a shard's leader/durability situation and
// emits at most one shard-level problem per cycle. It reasons on two axes:
//
//   - Does the leader need replacing, and why? Either healthy (empty cause) or one
//     of LeaderResigned / LeaderUnhealthy / LeaderUnreachableByCohort, chosen by
//     the first-hand vs observer-derived evidence principle (see the leader problem
//     docs in the types package). Observer-derived causes are quorum-gated; first-
//     hand ones are not.
//   - Could a failover succeed? Only if a durability-sufficient set of reachable,
//     initialized poolers is available to recruit a replacement.
//
// Crossing the leader verdict with failover feasibility (non-actionable outcomes are
// alert-only):
//   - replace + feasible   → the cause code (actionable → AppointLeader).
//   - replace + infeasible → ShardStuck, or NoHealthyCohortMembers when blind.
//   - healthy              → no problem, or ShardAtRisk if losing the leader would strand the shard.
//   - inconclusive         → LeaderHealthUnknown, or the infeasible codes above when we also can't recruit.
//
// "Feasible" is CheckSufficientRecruitment: a strict majority of the outgoing
// cohort reachable (unique rule number) with the remainder unable to satisfy the
// policy (revocation). For ShardAtRisk we run it excluding the current leader — the
// question is whether we could recover if the leader were lost.
//
// TODO(LeaderStuck): a "leader reachable but quorum-commit not advancing" cause is
// not yet detected — it needs a quorum-commit signal (per-replica lag is not
// quorum-safe).
//
// TODO(pooler-reported health): this analyzer reasons about postgres running/ready
// directly (leaderPostgresReady/Running). Directionally it should trust a pooler's
// self-reported fitness — the pooler knows it is e.g. mid-restart and still fit —
// with backstops for when a pooler is wrong, rather than second-guessing postgres
// state here.
type LeaderNeedsReplacementAnalyzer struct {
	factory *RecoveryActionFactory
}

func (a *LeaderNeedsReplacementAnalyzer) Name() types.CheckName {
	return "LeaderNeedsReplacement"
}

func (a *LeaderNeedsReplacementAnalyzer) RecoveryAction() types.RecoveryAction {
	return a.factory.NewAppointLeaderAction()
}

// followerConfiguredForLeader reports whether the follower's primary_conninfo targets
// this leader's postgres (host:port) — the shared "this follower is trying to follow
// THIS leader" test. A follower pointed at a different primary (or none) indicates a
// deeper problem (misconfig/split-brain) and is neither streaming from nor cut off
// from this leader.
func followerConfiguredForLeader(follower *store.Pooler, primaryHost string, primaryPort int32) bool {
	connInfo := follower.Health().GetStatus().GetReplicationStatus().GetPrimaryConnInfo()
	return connInfo.GetHost() != "" && connInfo.GetHost() == primaryHost && connInfo.GetPort() == primaryPort
}

// followerStreamingFromLeader reports whether a single follower is actively streaming
// from the leader's postgres: configured for this leader, has received WAL, the WAL
// receiver is in streaming state, and keepalives are fresh (within
// wal_receiver_status_interval × multiplier, falling back to the default threshold,
// and never older than wal_receiver_timeout).
func followerStreamingFromLeader(sa *ShardAnalysis, replica *store.Pooler, primaryHost string, primaryPort int32) bool {
	if !followerConfiguredForLeader(replica, primaryHost, primaryPort) {
		return false
	}
	rs := replica.Health().GetStatus().GetReplicationStatus()
	if rs.LastReceiveLsn == "" || rs.WalReceiverStatus != "streaming" {
		return false
	}
	if ts := rs.LastMsgReceiveTime; ts != nil {
		threshold := defaultReplicationHeartbeatStalenessThreshold
		delay := sa.Now.Sub(ts.AsTime())
		if d := rs.WalReceiverTimeout; d != nil && delay > d.AsDuration() {
			return false
		}
		if d := rs.WalReceiverStatusInterval; d != nil && d.AsDuration() > 0 {
			threshold = replicationHeartbeatStalenessMultiplier * d.AsDuration()
		}
		if delay > threshold {
			return false
		}
	}
	return true
}

// leaderObservedLive reports whether the orchestrator holds a recent, valid
// observation of the leader's pooler — the freshness-aware liveness basis for
// failover detection. It deliberately keys off observation age (sa.Now vs the
// leader's last snapshot, bounded by sa.Policy.LeaderLivenessFreshness) rather
// than whether a particular health stream is currently connected, so a brief
// stream interruption does not read as a dead leader while a genuinely stalled
// stream does.
func leaderObservedLive(sa *ShardAnalysis) bool {
	if sa.Leader == nil {
		return false
	}
	return observationFresh(sa.Leader, sa.Now, sa.Policy.LeaderLivenessFreshness)
}

// leaderHasResigned reports whether the leader has voluntarily signalled it
// should be replaced — cohort-eligibility INELIGIBLE or a term-matched
// REQUESTING_DEMOTION — read from its self-reported AvailabilityStatus.
func leaderHasResigned(sa *ShardAnalysis) bool {
	return sa.Leader != nil && types.LeaderNeedsReplacement(sa.Leader.Health())
}

// leaderShutdownTombstoned reports whether the shard's leader has been observed in
// LIFECYCLE_SHUTDOWN. That lifecycle is written to topology at the END of a graceful
// shutdown (after the drain), so acting on it does not preempt the drain — and it is
// durable, so it still fires when the ephemeral REQUESTING_DEMOTION health broadcast is
// lost. A SHUTDOWN pooler is tombstoned and evicted from the live cache (absent from
// sa.Leader), so we match it by ID against the cache's tombstone set; the leaderID still
// comes from the shard rule, so we can act with no cached leader. STOPPING is
// deliberately NOT consulted: it is observability-only and precedes the drain.
func leaderShutdownTombstoned(sa *ShardAnalysis, leaderID *clustermetadatapb.ID) bool {
	if leaderID == nil {
		return false
	}
	_, ok := sa.TombstoneIDs[topoclient.ComponentIDString(leaderID)]
	return ok
}

// leaderPostgresReady reports the leader's last-snapshot pg_isready result.
func leaderPostgresReady(sa *ShardAnalysis) bool {
	return sa.Leader != nil && sa.Leader.Health().GetStatus().GetPostgresReady()
}

// leaderServing reports whether the leader is a healthy, currently-serving
// primary suitable to drive a leader-led change (cohort reconcile, replica
// re-pointing): a recent observation (within the policy's leader-change
// freshness), postgres accepting connections, and not resigned. This is the Q3
// gate — not latency-sensitive, so requiring freshness merely defers a
// non-urgent change when our view of the leader is stale.
func leaderServing(sa *ShardAnalysis) bool {
	if sa.Leader == nil {
		return false
	}
	return observationFresh(sa.Leader, sa.Now, sa.Policy.LeaderChangeFreshness) &&
		leaderPostgresReady(sa) &&
		!leaderHasResigned(sa)
}

// leaderPostgresRunning reports whether the leader's last snapshot shows its
// postgres process alive (may be true even when pg_isready fails, e.g. SIGSTOP).
func leaderPostgresRunning(sa *ShardAnalysis) bool {
	return sa.Leader != nil && sa.Leader.Health().GetStatus().GetPostgresRunning()
}

// leaderLastPostgresReadyTime returns when the leader's postgres last reported
// ready per its snapshots, or the zero time if never observed ready.
func leaderLastPostgresReadyTime(sa *ShardAnalysis) time.Time {
	if sa.Leader == nil {
		return time.Time{}
	}
	if ts := sa.Leader.Health().GetLastPostgresReadyTime(); ts != nil {
		return ts.AsTime()
	}
	return time.Time{}
}

// leaderPromoting reports whether the leader's last snapshot shows pg_promote()
// in progress (postgres in the PROMOTING state).
func leaderPromoting(sa *ShardAnalysis) bool {
	return sa.Leader != nil &&
		sa.Leader.Health().GetStatus().GetPostgresStatus() == multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_PROMOTING
}

// inPromotionGrace reports whether failover should be briefly suppressed because
// the leader is mid-promotion: a freshly-created leadership rule needs a moment
// for followers to reconnect and start streaming before "are followers vouching?"
// is meaningful. The grace holds while the leader reports promoting (postgres
// still running) AND the rule is younger than ConnectReplicasToNewLeaderGrace. The
// rule-age bound is the point — a leader that claims to be promoting forever but
// never gains followers cannot make progress, so once the grace lapses we stop
// honoring the claim and let normal detection fail it over.
//
// TODO: remove the PROMOTING-status coupling entirely — have promoteLocked
// RecordTermPrimary the proposed rule *before* WAL catch-up, so orch reads the
// pooler self-asserting leadership and suppresses without a PROMOTING flag; the
// postgres monitor self-resigns if the promotion turns out unbacked/stuck (rule
// absent → resign → LeaderUnspecified → re-recruit). Event-driven; the north star.
func inPromotionGrace(sa *ShardAnalysis) bool {
	if !leaderPromoting(sa) || !leaderObservedLive(sa) || !leaderPostgresRunning(sa) {
		return false
	}
	ruleAge := sa.Now.Sub(commonconsensus.PossiblyUndecidedRule(sa.HighestPosition).GetCreationTime().AsTime())
	return ruleAge < sa.Policy.ConnectReplicasToNewLeaderGrace
}

func (a *LeaderNeedsReplacementAnalyzer) Analyze(sa *ShardAnalysis) ([]types.Problem, error) {
	if a.factory == nil {
		return nil, errors.New("recovery action factory not initialized")
	}

	undecidedRule := commonconsensus.PossiblyUndecidedRule(sa.HighestPosition)
	leaderID := undecidedRule.GetLeaderId()
	cohort := undecidedRule.GetCohortMembers()

	// No rule at all yet, or a rule naming neither a leader nor a cohort — the
	// initial, unbootstrapped state. ShardNeedsInitialization owns that, so do
	// nothing here; there's no established policy to read yet either.
	if leaderID == nil && len(cohort) == 0 {
		return nil, nil
	}

	policy, err := commonconsensus.NewPolicyFromProto(undecidedRule.GetDurabilityPolicy())
	if err != nil {
		return nil, mterrors.Wrap(err, "leader-needs-replacement: durability policy unavailable")
	}

	// A non-empty cohort with no designated leader needs one recruited.
	if leaderID == nil {
		return a.emitFailover(sa, nil, policy, cohort, types.ProblemLeaderUnspecified,
			fmt.Sprintf("Shard %s has cohort members but no designated leader", sa.ShardKey)), nil
	}

	// Suppress failover briefly while the leader is mid-promotion (see
	// inPromotionGrace).
	if inPromotionGrace(sa) {
		a.factory.Logger().Info("primary promotion in progress within grace, suppressing failover",
			"shard_key", sa.ShardKey.String(),
			"promoting_primary", topoclient.ComponentIDString(leaderID),
			"rule_age", sa.Now.Sub(undecidedRule.GetCreationTime().AsTime()))
		return nil, nil
	}

	// Judge the leader: healthy, must-replace (with a cause), or inconclusive.
	leaderLive := leaderObservedLive(sa)
	cause, description, inconclusive := a.leaderReplacementCause(sa, cohort, leaderID, leaderLive, policy)

	switch {
	case inconclusive:
		// We can neither confirm the leader healthy nor conclusively convict it.
		// Never fail over; surface only a blind spot we cannot act through.
		return a.emitInconclusive(sa, leaderID, policy, cohort), nil
	case cause == "":
		// Healthy leader — but warn if losing it now would strand the shard.
		return a.atRiskProblemIfDegraded(sa, policy, cohort, leaderID), nil
	default:
		// Must replace: gate on whether a failover could actually succeed.
		return a.emitFailover(sa, leaderID, policy, cohort, cause, description), nil
	}
}

// atRiskProblemIfDegraded returns a ShardAtRisk warning when a healthy leader
// could not be recovered from if lost BECAUSE cohort members are currently
// unreachable — a genuine degradation — and nil otherwise. It deliberately does
// NOT warn when the cohort is merely at its policy floor (e.g. 2 members under
// AtLeast(2)): that is the operator's chosen posture, not an anomaly, and would
// otherwise fire forever. The distinguisher: recovery is infeasible now but WOULD
// be feasible if every cohort member were reachable, i.e. standbys are missing.
func (a *LeaderNeedsReplacementAnalyzer) atRiskProblemIfDegraded(sa *ShardAnalysis, policy commonconsensus.DurabilityPolicy, cohort []*clustermetadatapb.ID, leaderID *clustermetadatapb.ID) []types.Problem {
	recoverableIfLeaderLost := recruitmentFeasible(policy, cohort, reachableCohort(sa, cohort, leaderID))
	recoverableIfFullyReachable := recruitmentFeasible(policy, cohort, cohortWithout(cohort, leaderID))
	if !recoverableIfLeaderLost && recoverableIfFullyReachable {
		return a.atRiskProblem(sa, leaderID,
			fmt.Sprintf("Shard %s could not recover if its leader were lost: cohort members are unreachable", sa.ShardKey))
	}
	return nil
}

// recruitmentFeasible reports whether a failover could establish a new term from
// the reachable subset: a strict majority of the outgoing cohort reachable, with
// the unreachable remainder unable to satisfy the durability policy. Thin
// readable wrapper over CheckSufficientRecruitment's error return.
func recruitmentFeasible(policy commonconsensus.DurabilityPolicy, cohort, reachable []*clustermetadatapb.ID) bool {
	return commonconsensus.CheckSufficientRecruitment(policy, cohort, reachable) == nil
}

// emitFailover applies the feasibility gate to a leader that must be replaced. A
// safe failover needs *sufficient recruitment*: reach a strict majority of the
// outgoing cohort (so the new rule number is unique) and leave the un-reachable
// remainder unable to satisfy the durability policy (so the outgoing rule is
// revoked). If that is impossible the failover can't proceed, and we split on why:
//   - No fresh, usable observation of any shard pooler → NoHealthyCohortMembers: orch
//     is blind and cannot trust its (stale-derived) view of the leader, so it does
//     not convict it. Often transient; clears when fresh health returns.
//   - Some poolers are reachable but not a sufficient quorum → ShardStuck: a
//     confident verdict that progress is halted and a human must intervene.
//
// Both are alert-only. Otherwise the cause is actionable via AppointLeader. The old
// leader is not excluded from the reachable set — even an unhealthy-but-reachable
// leader can still participate in the recruit that establishes the new term.
func (a *LeaderNeedsReplacementAnalyzer) emitFailover(sa *ShardAnalysis, leaderID *clustermetadatapb.ID, policy commonconsensus.DurabilityPolicy, cohort []*clustermetadatapb.ID, cause types.ProblemCode, description string) []types.Problem {
	if !recruitmentFeasible(policy, cohort, reachableCohort(sa, cohort, nil)) {
		return a.blindOrStuck(sa, leaderID, cohort,
			fmt.Sprintf("Shard %s needs a new leader (%s) but cannot reach a sufficient recruitment quorum", sa.ShardKey, cause))
	}
	return a.shardProblem(sa, leaderID, cause, types.PriorityEmergency, a.factory.NewAppointLeaderAction(), description)
}

// emitInconclusive handles a leader we can neither confirm healthy nor convict. It
// never fails over; it emits an alert-only problem describing why we can't tell:
// blind (no cohort health) → NoHealthyCohortMembers; a must-replace leader with no
// recruitment quorum → ShardStuck; otherwise a recoverable-but-unconfirmed cohort →
// LeaderHealthUnknown (a warning: the leader may be fine, we just lack conclusive
// evidence). Transient at cold start; persistent means orch has lost sight of the
// leader with an ambiguous cohort.
//
// TODO(propagation): the progress axis will further split LeaderHealthUnknown into
// ShardWritesBlockedOnPropagation when a quorum is catching up but not yet current.
func (a *LeaderNeedsReplacementAnalyzer) emitInconclusive(sa *ShardAnalysis, leaderID *clustermetadatapb.ID, policy commonconsensus.DurabilityPolicy, cohort []*clustermetadatapb.ID) []types.Problem {
	if recruitmentFeasible(policy, cohort, reachableCohort(sa, cohort, nil)) {
		return a.shardProblem(sa, leaderID, types.ProblemLeaderHealthUnknown, types.PriorityNormal, a.factory.NewAlertOnlyAction(),
			fmt.Sprintf("Shard %s leader health is unknown: orch cannot confirm it is serving a quorum nor that its cohort is cut off from it", sa.ShardKey))
	}
	return a.blindOrStuck(sa, leaderID, cohort,
		fmt.Sprintf("Shard %s cannot confirm leader progress and cannot reach a sufficient recruitment quorum", sa.ShardKey))
}

// blindOrStuck returns the alert-only problem for an infeasible failover: no usable
// health of any cohort member → NoHealthyCohortMembers (blind); otherwise a sub-quorum
// cohort → ShardStuck (with stuckDescription). Shared by emitFailover/emitInconclusive.
func (a *LeaderNeedsReplacementAnalyzer) blindOrStuck(sa *ShardAnalysis, leaderID *clustermetadatapb.ID, cohort []*clustermetadatapb.ID, stuckDescription string) []types.Problem {
	if !hasUsableShardHealth(sa, cohort) {
		return a.shardProblem(sa, leaderID, types.ProblemNoHealthyCohortMembers, types.PriorityEmergency, a.factory.NewAlertOnlyAction(),
			fmt.Sprintf("Shard %s has no healthy cohort members: no initialized pooler has a fresh, valid health report, so the leader cannot be judged", sa.ShardKey))
	}
	return a.shardProblem(sa, leaderID, types.ProblemShardStuck, types.PriorityEmergency, a.factory.NewAlertOnlyAction(), stuckDescription)
}

// atRiskProblem builds the ShardAtRisk warning. It is ScopePooler (anchored to the
// healthy leader) and PriorityNormal — deliberately NOT shard-wide/emergency — so
// it does not suppress the replica recoveries (standby adds) that resolve the risk.
func (a *LeaderNeedsReplacementAnalyzer) atRiskProblem(sa *ShardAnalysis, leaderID *clustermetadatapb.ID, description string) []types.Problem {
	return []types.Problem{{
		Code:           types.ProblemShardAtRisk,
		CheckName:      a.Name(),
		PoolerID:       leaderID,
		ShardKey:       sa.ShardKey,
		Description:    description,
		Priority:       types.PriorityNormal,
		Scope:          types.ScopePooler,
		DetectedAt:     time.Now(),
		RecoveryAction: a.factory.NewAlertOnlyAction(),
	}}
}

// leaderReplacementCause returns one of three verdicts: healthy (cause=="",
// inconclusive==false), replace (cause!=""), or inconclusive (cause=="",
// inconclusive==true — can neither confirm healthy nor conclusively convict; the
// caller must NOT treat it as healthy). The cause follows the first-hand vs
// observer-derived principle (see the leader problem docs in the types package).
func (a *LeaderNeedsReplacementAnalyzer) leaderReplacementCause(
	sa *ShardAnalysis,
	cohort []*clustermetadatapb.ID,
	leaderID *clustermetadatapb.ID,
	leaderLive bool,
	policy commonconsensus.DurabilityPolicy,
) (cause types.ProblemCode, description string, inconclusive bool) {
	// First-hand, authoritative intent to step down — act immediately, bypassing
	// progress/liveness signals. leaderHasResigned is the fast path (the leader's
	// REQUESTING_DEMOTION/INELIGIBLE health broadcast); the SHUTDOWN tombstone is the
	// durable fallback for when that ephemeral broadcast is lost.
	// TODO: first-hand causes still wait the shared failover grace; they could skip it
	// (intent doesn't flap; multi-orch safety is the Recruit CAS), cutting the write outage.
	if leaderHasResigned(sa) || leaderShutdownTombstoned(sa, leaderID) {
		return types.ProblemLeaderResigned,
			fmt.Sprintf("Leader for shard %s is stepping down", sa.ShardKey), false
	}

	// Healthy and serving as a postgres primary — no replacement needed.
	//
	// TODO(LeaderStuck): a live, postgres-ready leader can still fail to make durable
	// progress. Check the quorum-commit watermark (K-th-highest follower position from
	// the receive-position advance signal): crossing the prior shard frontier ⇒
	// healthy; rising only toward a known frontier ⇒ propagation (no failover); flat ⇒
	// LeaderStuck.
	if leaderLive && leaderPostgresReady(sa) {
		return "", "", false
	}

	if leaderLive {
		// First-hand: we observe the leader and its own postgres is not ready.
		// Anti-flap: treat as healthy while the process is alive and postgres
		// responded within the response window; once it lapses, a wedged postgres
		// must not block failover forever. (Interim guard, replaced by the LSN
		// progress signal when LeaderStuck lands.)
		if leaderPostgresRunning(sa) {
			threshold := a.factory.Config().GetLeaderPostgresResponseThreshold()
			lastReady := leaderLastPostgresReadyTime(sa)
			if !lastReady.IsZero() && time.Since(lastReady) <= threshold {
				return "", "", false
			}
		}
		return types.ProblemLeaderUnhealthy,
			fmt.Sprintf("Leader for shard %s is reachable but its postgres is unhealthy", sa.ShardKey), false
	}

	// Observer-derived: we hold no fresh healthy observation of the leader, so we
	// judge it through its cohort's evidence — three outcomes:
	vouching, cutOff := a.classifyCohortReachability(sa, cohort, leaderID)

	// NEGATIVE — followers conclusively cut off are sufficient to revoke the term: the
	// members not cut off (including the leader) can no longer satisfy the policy.
	// "Cut off" is an observation, not a TermRevocation. Mirrors the recruitment-
	// feasibility gate: same conclusive revocation to detect the failure as to act.
	if revocationSufficient(policy, cohort, cutOff) {
		return types.ProblemLeaderUnreachableByCohort,
			fmt.Sprintf("Leader for shard %s is unreachable by a durability-sufficient set of its cohort", sa.ShardKey), false
	}

	// POSITIVE — a durability-sufficient set streaming from the leader proves it alive
	// (you cannot stream from a dead primary) and serving a quorum → healthy. One
	// streaming follower plus the leader itself can be the quorum. A future LeaderStuck
	// check strengthens this to require the commit watermark to advance.
	if policy.SatisfiedBy(vouching) == nil {
		return "", "", false
	}

	// NEITHER — inconclusive, NOT healthy: we may simply not have looked long enough
	// (a freshly (re)started orch, or followers mid-reconnect).
	return "", "", true
}

// revocationSufficient reports whether a set of cohort members leaving the leader is
// enough to revoke its term: the members NOT in that set can no longer independently
// satisfy the durability policy. Dual of CheckSufficientRecruitment's revocation
// check — "sufficient to revoke" means the complement cannot form a quorum, NOT that
// the set itself is a quorum (those coincide only for strict-majority policies).
func revocationSufficient(policy commonconsensus.DurabilityPolicy, cohort, cutOff []*clustermetadatapb.ID) bool {
	cutKeys := make(map[topoclient.ComponentID]struct{}, len(cutOff))
	for _, m := range cutOff {
		cutKeys[topoclient.ComponentIDString(m)] = struct{}{}
	}
	remaining := make([]*clustermetadatapb.ID, 0, len(cohort))
	for _, m := range cohort {
		if _, ok := cutKeys[topoclient.ComponentIDString(m)]; !ok {
			remaining = append(remaining, m)
		}
	}
	return policy.SatisfiedBy(remaining) != nil
}

// followerLeaderRelation answers "is this follower connected to the leader?" from a
// follower's fresh health — one of four conclusions.
type followerLeaderRelation int

const (
	// relationUnaware: we can't conclude anything. No fresh observation, the follower's
	// highest-known rule doesn't name this leader, or (past the grace) it isn't even
	// configured to follow it.
	relationUnaware followerLeaderRelation = iota
	// relationAdapting: it learned of this leader only within the connect grace, so its
	// not-yet-connected state is inconclusive — give it time.
	relationAdapting
	// relationConnected: actively streaming from the leader → proves it alive.
	relationConnected
	// relationCutOff: wants to be connected (configured, past the grace) yet is not
	// streaming → conclusive evidence it cannot reach the leader.
	relationCutOff
)

// classifyFollowerToLeader answers "is this follower connected to the leader?".
// RecruitBlockedUntil is deliberately not consulted: a blocked recruit is still
// pointed at the leader and can report it unreachable — recruitability is a
// feasibility concern (reachableCohort), not detection.
func (a *LeaderNeedsReplacementAnalyzer) classifyFollowerToLeader(sa *ShardAnalysis, pa *store.Pooler, leaderID *clustermetadatapb.ID, primaryHost string, primaryPort int32) followerLeaderRelation {
	if !observationFresh(pa, sa.Now, sa.Policy.FollowerStreamFreshness) {
		return relationUnaware
	}
	if followerStreamingFromLeader(sa, pa, primaryHost, primaryPort) {
		return relationConnected
	}
	// Not streaming. Does it know it should be following THIS leader? Its highest-known
	// rule may come from its replication primary, not only its own WAL position.
	rule := commonconsensus.PossiblyUndecidedRule(
		commonconsensus.HighestKnownRule([]*clustermetadatapb.ConsensusStatus{pa.Health().GetConsensusStatus()}))
	if !commonconsensus.RuleNamesLeader(rule, leaderID) {
		return relationUnaware
	}
	if created := rule.GetCreationTime(); created == nil || sa.Now.Sub(created.AsTime()) <= sa.Policy.ConnectReplicasToNewLeaderGrace {
		return relationAdapting
	}
	if !followerConfiguredForLeader(pa, primaryHost, primaryPort) {
		return relationUnaware // knows the leader, had time, but isn't pointed at it
	}
	return relationCutOff
}

// classifyCohortReachability sorts cohort followers by their relation to the leader,
// collecting the two conclusive sets: `vouching` (connected → proves the leader alive)
// and `cutOff` (conclusively unable to reach it). Adapting/unaware followers land in
// neither — their silence is not evidence. The leader is appended to `vouching` once
// any follower streams from it (you cannot stream from a dead primary), so it counts
// toward the vouching quorum but never toward the cut-off set.
func (a *LeaderNeedsReplacementAnalyzer) classifyCohortReachability(sa *ShardAnalysis, cohort []*clustermetadatapb.ID, leaderID *clustermetadatapb.ID) (vouching, cutOff []*clustermetadatapb.ID) {
	if sa.Leader == nil {
		// No leader identity to check followers against — no evidence either way.
		return nil, nil
	}
	primaryHost := sa.Leader.Health().GetMultipooler().GetHostname()
	primaryPort := sa.Leader.Health().GetMultipooler().GetPortMap()["postgres"]
	leaderKey := topoclient.ComponentIDString(leaderID)

	byID := make(map[topoclient.ComponentID]*store.Pooler, len(sa.Analyses))
	for _, pa := range sa.Analyses {
		if pa != nil {
			byID[topoclient.ComponentIDString(poolerID(pa))] = pa
		}
	}

	for _, member := range cohort {
		if topoclient.ComponentIDString(member) == leaderKey {
			// Skip the leader: a live leader would have been judged by its own health
			// above, so here we provisionally assume it alive and let the cohort's
			// evidence confirm (vouch) or revoke that. It vouches for itself below.
			continue
		}
		pa, ok := byID[topoclient.ComponentIDString(member)]
		if !ok {
			continue
		}
		switch a.classifyFollowerToLeader(sa, pa, leaderID, primaryHost, primaryPort) {
		case relationConnected:
			vouching = append(vouching, member)
		case relationCutOff:
			cutOff = append(cutOff, member)
		case relationAdapting, relationUnaware:
			// no conclusive evidence either way
		}
	}
	if len(vouching) > 0 {
		vouching = append(vouching, leaderID)
	}
	return vouching, cutOff
}

// cohortWithout returns the cohort members other than exclude (all of them,
// regardless of reachability) — used to ask what recruitment would be possible if
// every member were reachable.
func cohortWithout(cohort []*clustermetadatapb.ID, exclude *clustermetadatapb.ID) []*clustermetadatapb.ID {
	excludeKey := topoclient.ComponentIDString(exclude)
	out := make([]*clustermetadatapb.ID, 0, len(cohort))
	for _, m := range cohort {
		if topoclient.ComponentIDString(m) != excludeKey {
			out = append(out, m)
		}
	}
	return out
}

// reachableCohort returns the outgoing-cohort members that are currently
// recruitable — the set we could use to establish a new rule. Recruitment
// forms the new term from the *outgoing cohort*, so membership is the rule's
// cohort intersected with recruitable poolers (CheckSufficientRecruitment
// also requires recruited ⊆ cohort). If exclude is non-nil that member is
// omitted — used to ask "could we recover if the leader were lost?" for the
// ShardAtRisk check.
func reachableCohort(sa *ShardAnalysis, cohort []*clustermetadatapb.ID, exclude *clustermetadatapb.ID) []*clustermetadatapb.ID {
	byID := make(map[topoclient.ComponentID]*store.Pooler, len(sa.Analyses)+1)
	for _, pa := range sa.Analyses {
		if pa != nil {
			byID[topoclient.ComponentIDString(poolerID(pa))] = pa
		}
	}
	// The leader's rider lives on sa.Leader, not necessarily in Analyses.
	if sa.Leader != nil {
		byID[topoclient.ComponentIDString(poolerID(sa.Leader))] = sa.Leader
	}

	excludeKey := topoclient.ComponentIDString(exclude)
	var recruited []*clustermetadatapb.ID
	for _, m := range cohort {
		if exclude != nil && topoclient.ComponentIDString(m) == excludeKey {
			continue
		}
		pa, ok := byID[topoclient.ComponentIDString(m)]
		if !ok {
			continue
		}
		if recruitable(pa, sa.Now, sa.Policy.ObservationFreshness) {
			recruited = append(recruited, m)
		}
	}
	return recruited
}

// hasUsableShardHealth reports whether orch has at least one fresh, valid,
// initialized observation of a shard pooler to reason from. Without one, orch is
// blind: its view of the rule/leader comes only from stale health, so it must not
// convict the leader (see emitFailover, which reports NoHealthyCohortMembers then).
//
// This is exactly reachableCohort being non-empty: the leader is itself a cohort
// member (the rule's CohortMembers includes it, which is why emitFailover recruits
// with exclude=nil), so a fresh leader already counts here.
func hasUsableShardHealth(sa *ShardAnalysis, cohort []*clustermetadatapb.ID) bool {
	return len(reachableCohort(sa, cohort, nil)) > 0
}

// shardProblem builds the single shard-scoped problem this analyzer emits.
func (a *LeaderNeedsReplacementAnalyzer) shardProblem(sa *ShardAnalysis, leaderID *clustermetadatapb.ID, code types.ProblemCode, priority types.Priority, action types.RecoveryAction, description string) []types.Problem {
	return []types.Problem{{
		Code:           code,
		CheckName:      a.Name(),
		PoolerID:       leaderID,
		ShardKey:       sa.ShardKey,
		Description:    description,
		Priority:       priority,
		Scope:          types.ScopeShard,
		DetectedAt:     time.Now(),
		RecoveryAction: action,
	}}
}
