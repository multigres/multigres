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
	"github.com/multigres/multigres/go/common/topoclient"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// LeaderNeedsReplacementAnalyzer detects when a leader exists in the shard but is unhealthy/unreachable.
// It operates at the shard level: if any initialized follower observes the leader as dead,
// one shard-scoped problem is emitted.
//
// TODO: split this analyzer into distinct conditions in a future PR. Today a
// single reachability + postgres-response heuristic conflates failure modes that
// have different evidence and different correct responses, which is what made the
// "pooler dead but postgres healthy" suppression bug easy to miss. The conditions:
//
//   - Leader unhealthy: the leader is reachable and directly reports trouble (e.g.
//     postgres unresponsive / process down). We have a first-hand signal here.
//   - Unreachable leader, replicas lost hope: we can't reach the leader, but if we
//     can show that no quorum of replicas has a live connection (or hope of one)
//     to the leader, failover may be worth attempting.
//   - No heartbeats reaching WAL: even when replicas can still connect to a leader
//     we can't reach directly, an absence of newly written heartbeats is suspicious
//     — it suggests either the unreachable leader's pooler is down (so queries can't
//     be served) or the pooler is up but writes are blocked (e.g. a read-only disk).
//     Suppress only when the leader is reachable and the read-only/no-write state appears
//     intentional; otherwise this warrants failover.
type LeaderNeedsReplacementAnalyzer struct {
	factory *RecoveryActionFactory
}

func (a *LeaderNeedsReplacementAnalyzer) Name() types.CheckName {
	return "LeaderNeedsReplacement"
}

func (a *LeaderNeedsReplacementAnalyzer) RecoveryAction() types.RecoveryAction {
	return a.factory.NewAppointLeaderAction()
}

// replicasStreamingFromLeader reports whether every follower we currently have
// fresh evidence for is actively streaming from the leader's postgres. It is the
// suppress-side evidence for failover: if all observable followers are streaming
// with fresh WAL heartbeats, the leader's postgres is alive even when its own
// pooler is unreachable.
//
// Followers we have never heard from, or whose snapshot is stale, are ignored —
// absence of evidence is not evidence of disconnection — so the decision rests
// only on followers we currently have data for. Returns false when there are no
// such followers, so failover is not suppressed on missing data.
func replicasStreamingFromLeader(sa *ShardAnalysis) bool {
	if sa.Leader == nil {
		return false
	}
	primaryHost := sa.Leader.Health().GetMultiPooler().GetHostname()
	primaryPort := sa.Leader.Health().GetMultiPooler().GetPortMap()["postgres"]
	leaderKey := topoclient.ComponentIDString(sa.HighestShardRule.GetLeaderId())

	replicaCount := 0
	connectedCount := 0
	for _, pa := range sa.Analyses {
		if pa == nil {
			continue
		}
		// Skip the leader itself and any node that self-claims leadership.
		if topoclient.ComponentIDString(poolerID(pa)) == leaderKey || commonconsensus.SelfConsensusRole(pa.Health().GetConsensusStatus()) == commonconsensus.ConsensusRoleLeader {
			continue
		}
		// Ignore followers with no fresh observation (never reported, or a stale
		// snapshot). A snapshot we haven't refreshed recently is absence of
		// evidence, so it neither vouches for a live connection nor counts as
		// disconnected — the decision rests on followers we currently see.
		if !observationFresh(pa, sa.Now, sa.Policy.FollowerStreamFreshness) {
			continue
		}
		replicaCount++
		if followerStreamingFromLeader(sa, pa, primaryHost, primaryPort) {
			connectedCount++
		}
	}
	return replicaCount > 0 && connectedCount == replicaCount
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
// failover detection (Q1). It deliberately keys off observation age
// (sa.Now vs the leader's last snapshot, bounded by
// sa.Policy.LeaderLivenessFreshness) rather than whether a particular health
// stream is currently connected, so a brief stream interruption does not read
// as a dead leader while a genuinely stalled stream does.
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

// hasInitializedReplica reports whether the shard has at least one non-leader,
// reachable, initialized pooler — a postgres standby that could witness the
// leader. Used to avoid false-positive failover when no standby has joined yet.
func hasInitializedReplica(sa *ShardAnalysis) bool {
	for _, pa := range sa.Analyses {
		if commonconsensus.SelfConsensusRole(pa.Health().GetConsensusStatus()) != commonconsensus.ConsensusRoleLeader &&
			pa.Health().IsLastCheckValid && pa.IsInitialized() {
			return true
		}
	}
	return false
}

func (a *LeaderNeedsReplacementAnalyzer) Analyze(sa *ShardAnalysis) ([]types.Problem, error) {
	if a.factory == nil {
		return nil, errors.New("recovery action factory not initialized")
	}

	// No known leader yet (no consensus rule names one) — nothing to fail over.
	if sa.HighestShardRule.GetLeaderId() == nil {
		return nil, nil
	}

	// A resigned leader (voluntary INELIGIBLE / REQUESTING_DEMOTION) needs
	// replacement immediately, independent of liveness or follower streaming —
	// emit and stop before the liveness-suppression logic below. Distinct problem
	// code so dashboards can tell a voluntary demotion from a detected failure.
	if leaderHasResigned(sa) {
		return []types.Problem{{
			Code:           types.ProblemLeaderResigned,
			CheckName:      a.Name(),
			PoolerID:       sa.HighestShardRule.GetLeaderId(),
			ShardKey:       sa.ShardKey,
			Description:    fmt.Sprintf("Leader for shard %s has requested demotion", sa.ShardKey),
			Priority:       types.PriorityEmergency,
			Scope:          types.ScopeShard,
			DetectedAt:     time.Now(),
			RecoveryAction: a.factory.NewAppointLeaderAction(),
		}}, nil
	}

	// Q1 liveness: do we hold a recent, valid observation of the leader? This is
	// judged here (not read from a pre-baked generator verdict) and is
	// freshness-aware via leaderObservedLive, so a brief health-stream
	// interruption does not by itself read as a dead leader.
	leaderLive := leaderObservedLive(sa)

	// Leader is recently observed and serving as a postgres primary — no problem.
	// (Resignation was already handled above.)
	if leaderLive && leaderPostgresReady(sa) {
		return nil, nil
	}

	// No initialized replica to confirm the leader is dead — skip to avoid false positives
	// when the shard has no postgres standby that has joined the cluster yet.
	if !hasInitializedReplica(sa) {
		return nil, nil
	}

	// Suppress failover during a known pg_promote() window. The multipooler explicitly
	// signals promotion is in progress (leader postgres in PROMOTING state). The conditions:
	//   - leaderPromoting: the leader's snapshot flags pg_promote() is running
	//   - leaderLive: we hold a recent, valid observation, so the flag is current (not stale)
	//   - leaderPostgresRunning: postgres process is still alive
	// If postgres crashes during promotion, leaderPostgresRunning is false and we fall through.
	// If the multipooler crashes or its observation ages out, leaderLive is false and we fall through.
	if leaderPromoting(sa) && leaderLive && leaderPostgresRunning(sa) {
		a.factory.Logger().Info("primary promotion in progress, suppressing LeaderIsDead",
			"shard_key", sa.ShardKey.String(),
			"promoting_primary", topoclient.ComponentIDString(sa.HighestShardRule.GetLeaderId()))
		return nil, nil
	}

	// At this point, the leader is not observed live. This can happen in three cases:
	//
	// 1. Leader pooler is unreachable (e.g. pooler process crashed).
	//    Postgres may still be running; followers can still receive WAL.
	//
	// 2. Leader pooler is reachable and Postgres process is alive yet
	//    unresponsive, pg_isready fails but the process exists (e.g. SIGSTOP or
	//    overloaded). Followers remain connected until TCP keepalive times out.
	//
	// 3. Leader pooler is reachable but Postgres process is dead: this means
	//    pg_isready can be assumed to fail and the process is gone (e.g.
	//    SIGKILL). Followers may still appear connected for ~30s via TCP
	//    keepalive even though Postgres is dead.
	//
	// For cases 1 and 2, we check if ALL followers are still connected to the
	// leader's postgres. If they are, postgres is still running (or recovering)
	// and we suppress failover, but only if the leader's postgres responded
	// recently enough. This prevents suppressing indefinitely when followers are
	// observing stale connections while postgres is unresponsive.
	//
	// For case 3, we must NOT suppress: the pooler reports the process is dead,
	// so followers' apparent connections are stale (TCP keepalive hasn't fired
	// yet). Suppressing would delay failover by up to the TCP keepalive
	// interval (~30s).

	if replicasStreamingFromLeader(sa) {
		// Reaching here means every replica is actively streaming from the leader's
		// postgres with fresh WAL heartbeats (followerStreamingFromLeader verifies
		// LastMsgReceiveTime against the receiver timeout), which is direct proof the
		// leader's postgres is alive right now. If postgres dies, those heartbeats go
		// stale, replicasStreamingFromLeader becomes false, and we fall through to failover.
		leaderPGRunning := leaderPostgresRunning(sa)

		// Case 1: the leader pooler is unreachable (e.g. its process crashed) while
		// postgres keeps serving. We cannot observe the leader's postgres directly —
		// a dead pooler reports nothing, so LeaderPostgresReady/LastPostgresReadyTime
		// are unavailable — but the replicas' fresh streaming proves it is alive, so
		// suppress failover.
		if !leaderLive {
			a.factory.Logger().Warn("leader pooler unreachable but replicas still streaming from its postgres, suppressing failover",
				"shard_key", sa.ShardKey.String(),
				"leader_pooler_id", topoclient.ComponentIDString(sa.HighestShardRule.GetLeaderId()),
				"leader_postgres_running", leaderPGRunning)
			return nil, nil
		}

		// The leader pooler is reachable, so we can trust the direct postgres signal
		// it reports.
		threshold := a.factory.Config().GetLeaderPostgresResponseThreshold()
		lastReadyTime := leaderLastPostgresReadyTime(sa)
		primaryPostgresUnresponsive := !leaderPostgresReady(sa) &&
			(lastReadyTime.IsZero() || time.Since(lastReadyTime) > threshold)

		// Case 2: postgres process is alive but possibly unresponsive (pg_isready
		// fails while the process exists). Suppress while it responded recently; once
		// the window closes, allow failover so a wedged postgres cannot block it forever.
		if leaderPGRunning && !primaryPostgresUnresponsive {
			a.factory.Logger().Warn("leader postgres reachable and responsive, replicas connected, suppressing failover",
				"shard_key", sa.ShardKey.String(),
				"leader_pooler_id", topoclient.ComponentIDString(sa.HighestShardRule.GetLeaderId()),
				"leader_postgres_ready", leaderPostgresReady(sa),
				"last_postgres_ready_time", lastReadyTime,
				"threshold", threshold)
			return nil, nil
		}
		if leaderPGRunning && primaryPostgresUnresponsive {
			a.factory.Logger().Warn("leader postgres process alive but unresponsive beyond threshold, allowing failover",
				"shard_key", sa.ShardKey.String(),
				"leader_pooler_id", topoclient.ComponentIDString(sa.HighestShardRule.GetLeaderId()),
				"leader_postgres_ready", leaderPostgresReady(sa),
				"last_postgres_ready_time", lastReadyTime,
				"threshold", threshold)
		}

		// Case 3: pooler is reachable but reports the postgres process is dead.
		// This happens after SIGKILL: the process is gone but followers still show as
		// connected (TCP keepalive has not fired yet). Do not suppress.
		if !leaderPGRunning {
			a.factory.Logger().Warn("leader pooler reachable but postgres process is dead, replicas still connected (stale connections)",
				"shard_key", sa.ShardKey.String(),
				"leader_pooler_id", topoclient.ComponentIDString(sa.HighestShardRule.GetLeaderId()),
				"leader_postgres_ready", leaderPostgresReady(sa),
				"leader_postgres_running", leaderPGRunning,
			)
		}
	}

	// Leader is dead — emit one shard-level problem.
	return []types.Problem{{
		Code:           types.ProblemLeaderIsDead,
		CheckName:      a.Name(),
		PoolerID:       sa.HighestShardRule.GetLeaderId(),
		ShardKey:       sa.ShardKey,
		Description:    fmt.Sprintf("Leader for shard %s is dead/unreachable", sa.ShardKey),
		Priority:       types.PriorityEmergency,
		Scope:          types.ScopeShard,
		DetectedAt:     time.Now(),
		RecoveryAction: a.factory.NewAppointLeaderAction(),
	}}, nil
}
