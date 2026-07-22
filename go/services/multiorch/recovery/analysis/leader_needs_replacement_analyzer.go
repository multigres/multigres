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
// Crossing the two axes:
//   - needs replacement + feasible   → the cause code (actionable → AppointLeader).
//   - needs replacement + infeasible → ShardStuck (critical, alert-only).
//   - healthy + feasible             → no problem.
//   - healthy + infeasible-without-leader → ShardAtRisk (non-blocking warning).
//
// "Feasible" is CheckSufficientRecruitment: a strict majority of the outgoing
// cohort reachable (unique rule number) with the remainder unable to satisfy the
// policy (revocation). For ShardAtRisk we run it excluding the current leader — the
// question is whether we could recover if the leader were lost.
//
// TODO(LeaderStuck): a "leader reachable but quorum-commit not advancing" cause is
// not yet detected — it needs a quorum-commit signal (per-replica lag is not
// quorum-safe). See the failover detection redesign note.
//
// TODO(pooler-reported health): this analyzer reasons about postgres running/ready
// directly (leaderPostgresReady/Running). Directionally it should trust a pooler's
// self-reported fitness — the pooler knows it is e.g. mid-restart and still fit —
// with backstops for when a pooler is wrong, rather than second-guessing postgres
// state here. See the failover detection redesign note.
type LeaderNeedsReplacementAnalyzer struct {
	factory *RecoveryActionFactory
}

func (a *LeaderNeedsReplacementAnalyzer) Name() types.CheckName {
	return "LeaderNeedsReplacement"
}

func (a *LeaderNeedsReplacementAnalyzer) RecoveryAction() types.RecoveryAction {
	return a.factory.NewAppointLeaderAction()
}

// followerStreamingFromLeader reports whether a single follower is actively
// streaming from the leader's postgres: its primary_conninfo targets the leader,
// it has received WAL, the WAL receiver is in streaming state, and keepalives are
// fresh (within wal_receiver_status_interval × multiplier, falling back to the
// default threshold, and never older than wal_receiver_timeout).
func followerStreamingFromLeader(sa *ShardAnalysis, replica *store.Pooler, primaryHost string, primaryPort int32) bool {
	rs := replica.Health().GetStatus().GetReplicationStatus()
	if rs == nil {
		return false
	}
	connInfo := rs.PrimaryConnInfo
	if connInfo == nil || connInfo.Host == "" {
		return false
	}
	// Wrong primary indicates a deeper problem (misconfig/split-brain); return
	// false to avoid letting it vouch for the current leader.
	if connInfo.Host != primaryHost || connInfo.Port != primaryPort {
		return false
	}
	if rs.LastReceiveLsn == "" {
		return false
	}
	if rs.WalReceiverStatus != "streaming" {
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

func (a *LeaderNeedsReplacementAnalyzer) Analyze(sa *ShardAnalysis) ([]types.Problem, error) {
	if a.factory == nil {
		return nil, errors.New("recovery action factory not initialized")
	}

	undecidedRule := commonconsensus.PossiblyUndecidedRule(sa.HighestPosition)
	leaderID := undecidedRule.GetLeaderId()
	cohort := undecidedRule.GetCohortMembers()

	policy, err := commonconsensus.NewPolicyFromProto(sa.BootstrapDurabilityPolicy)
	if err != nil {
		return nil, mterrors.Wrap(err, "leader-needs-replacement: durability policy unavailable")
	}

	// The rule names no leader. An empty cohort is the initial, unbootstrapped rule
	// — ShardNeedsInitialization owns that, so do nothing here. A non-empty cohort
	// with no designated leader needs one recruited.
	if leaderID == nil {
		if len(cohort) == 0 {
			return nil, nil
		}
		return a.emitFailover(sa, nil, policy, cohort, types.ProblemLeaderUnspecified,
			fmt.Sprintf("Shard %s has cohort members but no designated leader", sa.ShardKey)), nil
	}

	// Suppress during a promotion, but only briefly: a freshly-created leadership
	// rule needs a moment for followers to reconnect and start streaming before
	// "are followers vouching?" is meaningful. Grant that grace while the leader
	// reports it is promoting (postgres still running) AND the rule is younger than
	// ConnectReplicasToNewLeaderGrace. The rule-age bound is the point — a leader that claims to be
	// promoting forever but never gains followers cannot make progress, so once the
	// grace lapses we stop honoring the claim and let normal detection fail it over.
	//
	// TODO: remove the PROMOTING-status coupling entirely — have promoteLocked
	// RecordTermPrimary the proposed rule *before* WAL catch-up, so orch reads the
	// pooler self-asserting leadership and suppresses without a PROMOTING flag; the
	// postgres monitor self-resigns if the promotion turns out unbacked/stuck (rule
	// absent → resign → LeaderUnspecified → re-recruit). Event-driven; the north star
	// (see the failover detection redesign note).
	leaderLive := leaderObservedLive(sa)
	ruleAge := sa.Now.Sub(undecidedRule.GetCreationTime().AsTime())
	if leaderPromoting(sa) && leaderLive && leaderPostgresRunning(sa) && ruleAge < sa.Policy.ConnectReplicasToNewLeaderGrace {
		a.factory.Logger().Info("primary promotion in progress within grace, suppressing failover",
			"shard_key", sa.ShardKey.String(),
			"promoting_primary", topoclient.ComponentIDString(leaderID),
			"rule_age", ruleAge)
		return nil, nil
	}

	// Axis 1: does the leader need replacing, and why? Empty cause means healthy.
	cause, description := a.leaderReplacementCause(sa, cohort, leaderID, leaderLive, policy)

	if cause == "" {
		// Leader healthy. Warn (ShardAtRisk) if losing it now could not be recovered
		// from BECAUSE cohort members are currently unreachable — a genuine
		// degradation. We do NOT warn when the cohort is simply at its policy floor
		// (e.g. 2 members under AtLeast(2)); that is the operator's chosen posture,
		// not an anomaly, and would otherwise fire forever. The distinguisher: could
		// we recover if every cohort member were reachable? If yes but we currently
		// cannot, standbys are missing. Emitted non-blocking (see atRiskProblem) so
		// it does not suppress the replica recoveries that add standbys.
		recoverableIfLeaderLost := commonconsensus.CheckSufficientRecruitment(policy, cohort, reachableCohort(sa, cohort, leaderID)) == nil
		recoverableIfFullyReachable := commonconsensus.CheckSufficientRecruitment(policy, cohort, cohortWithout(cohort, leaderID)) == nil
		if !recoverableIfLeaderLost && recoverableIfFullyReachable {
			return a.atRiskProblem(sa, leaderID,
				fmt.Sprintf("Shard %s could not recover if its leader were lost: cohort members are unreachable", sa.ShardKey)), nil
		}
		return nil, nil
	}

	// Axis 2: could a failover succeed? Emit ShardStuck if not, else the cause.
	return a.emitFailover(sa, leaderID, policy, cohort, cause, description), nil
}

// emitFailover applies the feasibility gate to a leader that must be replaced. A
// safe failover needs *sufficient recruitment*: reach a strict majority of the
// outgoing cohort (so the new rule number is unique) and leave the un-reachable
// remainder unable to satisfy the durability policy (so the outgoing rule is
// revoked). If that is impossible the shard is stuck and only a human can help
// (ShardStuck, alert-only); otherwise the cause is actionable via AppointLeader.
// The old leader is not excluded here — even an unhealthy-but-reachable leader can
// still participate in the recruit that establishes the new term.
func (a *LeaderNeedsReplacementAnalyzer) emitFailover(sa *ShardAnalysis, leaderID *clustermetadatapb.ID, policy commonconsensus.DurabilityPolicy, cohort []*clustermetadatapb.ID, cause types.ProblemCode, description string) []types.Problem {
	if commonconsensus.CheckSufficientRecruitment(policy, cohort, reachableCohort(sa, cohort, nil)) != nil {
		return a.shardProblem(sa, leaderID, types.ProblemShardStuck, a.factory.NewAlertOnlyAction(),
			fmt.Sprintf("Shard %s needs a new leader (%s) but cannot reach a sufficient recruitment quorum", sa.ShardKey, cause))
	}
	return a.shardProblem(sa, leaderID, cause, a.factory.NewAppointLeaderAction(), description)
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

// leaderReplacementCause judges whether the shard's leader must be replaced and,
// if so, returns the cause code and a human description; an empty cause means the
// leader is healthy and progressing. The cause is chosen by the evidence, on the
// first-hand vs observer-derived principle (see the leader problem docs in the
// types package).
func (a *LeaderNeedsReplacementAnalyzer) leaderReplacementCause(
	sa *ShardAnalysis,
	cohort []*clustermetadatapb.ID,
	leaderID *clustermetadatapb.ID,
	leaderLive bool,
	policy commonconsensus.DurabilityPolicy,
) (types.ProblemCode, string) {
	// Resigned: first-hand, authoritative intent to step down. Act immediately.
	if leaderHasResigned(sa) {
		return types.ProblemLeaderResigned,
			fmt.Sprintf("Leader for shard %s has requested demotion", sa.ShardKey)
	}

	// Healthy and serving as a postgres primary — no replacement needed.
	//
	// TODO(LeaderStuck): a leader can be observed live and postgres-ready yet not
	// actually making durable progress (quorum-commit position not advancing). Add
	// that cause here: treat the leader as making progress iff ANY cohort replica
	// shows a fresh quorum-ack'd commit watermark advancing (one witness proves the
	// leader commits AND replicates); if none do, the leader is stuck. This needs a
	// quorum-commit watermark in the heartbeat row — per-replica replay lag is not
	// quorum-safe (standbys replay WAL ahead of the sync-quorum ack). See the
	// failover detection redesign note.
	if leaderLive && leaderPostgresReady(sa) {
		return "", ""
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
				return "", ""
			}
		}
		return types.ProblemLeaderUnhealthy,
			fmt.Sprintf("Leader for shard %s is reachable but its postgres is unhealthy", sa.ShardKey)
	}

	// Observer-derived: we hold no fresh healthy observation of the leader. Suppress
	// while a durability-sufficient set of the cohort still streams from it (it can
	// still maintain quorum); otherwise it is unreachable by its cohort. We gate on
	// the *vouching* set being insufficient, not the disconnected set being
	// sufficient — those differ (e.g. 4 members / need 3 / 2 still connected).
	if policy.SatisfiedBy(a.vouchingForLeader(sa, cohort, leaderID)) == nil {
		return "", ""
	}
	return types.ProblemLeaderUnreachableByCohort,
		fmt.Sprintf("Leader for shard %s is unreachable by a durability-sufficient set of its cohort", sa.ShardKey)
}

// vouchingForLeader returns the cohort members that currently vouch for the
// leader making progress: a fresh follower actively streaming from it, plus the
// leader itself when observed live and postgres-ready. A durability-sufficient
// vouching set means the leader can still maintain quorum, so failover is
// suppressed. Cohort members we have no fresh observation for are omitted —
// absence of evidence neither vouches nor convicts.
func (a *LeaderNeedsReplacementAnalyzer) vouchingForLeader(sa *ShardAnalysis, cohort []*clustermetadatapb.ID, leaderID *clustermetadatapb.ID) []*clustermetadatapb.ID {
	if sa.Leader == nil {
		return nil
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

	var vouching []*clustermetadatapb.ID
	for _, member := range cohort {
		if topoclient.ComponentIDString(member) == leaderKey {
			continue
		}
		pa, ok := byID[topoclient.ComponentIDString(member)]
		if !ok || !observationFresh(pa, sa.Now, sa.Policy.FollowerStreamFreshness) {
			continue
		}
		if followerStreamingFromLeader(sa, pa, primaryHost, primaryPort) {
			vouching = append(vouching, member)
		}
	}
	// The leader vouches for itself if we directly observe it healthy, OR if any
	// follower is actively streaming from it — you cannot stream from a dead
	// primary, so a single streaming follower proves the leader is alive even when
	// we cannot reach the leader directly.
	if (leaderObservedLive(sa) && leaderPostgresReady(sa)) || len(vouching) > 0 {
		vouching = append(vouching, leaderID)
	}
	return vouching
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

// reachableCohort returns the outgoing-cohort members we currently have a fresh,
// valid, initialized observation for — the set we could recruit to establish a new
// rule. Recruitment forms the new term from the *outgoing cohort*, so membership is
// the rule's cohort intersected with poolers we can reach (CheckSufficientRecruitment
// also requires recruited ⊆ cohort). If exclude is non-nil that member is omitted —
// used to ask "could we recover if the leader were lost?" for the ShardAtRisk check.
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
		// observationFresh, not IsLastCheckValid: the latter flips false the instant a
		// health stream drops, so a momentary blip would wrongly drop a reachable
		// member from the recruitment set (and could falsely trip ShardStuck/AtRisk).
		// Freshness ages out only when the observation is genuinely stale.
		if ok && observationFresh(pa, sa.Now, sa.Policy.LeaderLivenessFreshness) && pa.IsInitialized() {
			recruited = append(recruited, m)
		}
	}
	return recruited
}

// shardProblem builds the single shard-scoped problem this analyzer emits.
func (a *LeaderNeedsReplacementAnalyzer) shardProblem(sa *ShardAnalysis, leaderID *clustermetadatapb.ID, code types.ProblemCode, action types.RecoveryAction, description string) []types.Problem {
	return []types.Problem{{
		Code:           code,
		CheckName:      a.Name(),
		PoolerID:       leaderID,
		ShardKey:       sa.ShardKey,
		Description:    description,
		Priority:       types.PriorityEmergency,
		Scope:          types.ScopeShard,
		DetectedAt:     time.Now(),
		RecoveryAction: action,
	}}
}
