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

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// LeaderIsDeadAnalyzer detects when a leader exists in the shard but is unhealthy/unreachable.
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
type LeaderIsDeadAnalyzer struct {
	factory *RecoveryActionFactory
}

func (a *LeaderIsDeadAnalyzer) Name() types.CheckName {
	return "LeaderIsDead"
}

func (a *LeaderIsDeadAnalyzer) ProblemCode() types.ProblemCode {
	return types.ProblemLeaderIsDead
}

func (a *LeaderIsDeadAnalyzer) RecoveryAction() types.RecoveryAction {
	return a.factory.NewAppointLeaderAction()
}

func (a *LeaderIsDeadAnalyzer) Analyze(sa *ShardAnalysis) ([]types.Problem, error) {
	if a.factory == nil {
		return nil, errors.New("recovery action factory not initialized")
	}

	// No known leader yet (no consensus rule names one) — nothing to fail over.
	if sa.HighestShardRule.GetLeaderId() == nil {
		return nil, nil
	}

	// Leader is fully reachable — no problem.
	if sa.LeaderReachable {
		return nil, nil
	}

	// No initialized replica to confirm the leader is dead — skip to avoid false positives
	// when the shard has no postgres standby that has joined the cluster yet.
	if !sa.HasInitializedReplica {
		return nil, nil
	}

	// Suppress failover during a known pg_promote() window. The multipooler explicitly
	// signals promotion is in progress via PromotingPrimaryID. The conditions:
	//   - PromotingPrimaryID != nil: multipooler has flagged pg_promote() is running
	//   - LeaderPoolerReachable: stream is live, so the flag is current (not stale)
	//   - LeaderPostgresRunning: postgres process is still alive
	// If postgres crashes during promotion, LeaderPostgresRunning=false and we fall through.
	// If the multipooler crashes, LeaderPoolerReachable=false and we fall through.
	if sa.PromotingPrimaryID != nil && sa.LeaderPoolerReachable && sa.LeaderPostgresRunning {
		a.factory.Logger().Info("primary promotion in progress, suppressing LeaderIsDead",
			"shard_key", sa.ShardKey.String(),
			"promoting_primary", topoclient.ComponentIDString(sa.PromotingPrimaryID))
		return nil, nil
	}

	// At this point, LeaderReachable is false. This can happen in three cases:
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

	if sa.ReplicasConnectedToLeader {
		// Reaching here means every replica is actively streaming from the leader's
		// postgres with fresh WAL heartbeats (isFollowerConnectedToLeader verifies
		// LastMsgReceiveTime against the receiver timeout), which is direct proof the
		// leader's postgres is alive right now. If postgres dies, those heartbeats go
		// stale, ReplicasConnectedToLeader becomes false, and we fall through to failover.

		// Case 1: the leader pooler is unreachable (e.g. its process crashed) while
		// postgres keeps serving. We cannot observe the leader's postgres directly —
		// a dead pooler reports nothing, so LeaderPostgresReady/LastPostgresReadyTime
		// are unavailable — but the replicas' fresh streaming proves it is alive, so
		// suppress failover.
		if !sa.LeaderPoolerReachable {
			a.factory.Logger().Warn("leader pooler unreachable but replicas still streaming from its postgres, suppressing failover",
				"shard_key", sa.ShardKey.String(),
				"leader_pooler_id", topoclient.ComponentIDString(sa.HighestShardRule.GetLeaderId()),
				"leader_postgres_running", sa.LeaderPostgresRunning)
			return nil, nil
		}

		// The leader pooler is reachable, so we can trust the direct postgres signal
		// it reports.
		threshold := a.factory.Config().GetLeaderPostgresResponseThreshold()
		lastReadyTime := sa.LeaderLastPostgresReadyTime
		primaryPostgresUnresponsive := !sa.LeaderPostgresReady &&
			(lastReadyTime.IsZero() || time.Since(lastReadyTime) > threshold)

		// Case 2: postgres process is alive but possibly unresponsive (pg_isready
		// fails while the process exists). Suppress while it responded recently; once
		// the window closes, allow failover so a wedged postgres cannot block it forever.
		if sa.LeaderPostgresRunning && !primaryPostgresUnresponsive {
			a.factory.Logger().Warn("leader postgres reachable and responsive, replicas connected, suppressing failover",
				"shard_key", sa.ShardKey.String(),
				"leader_pooler_id", topoclient.ComponentIDString(sa.HighestShardRule.GetLeaderId()),
				"leader_postgres_ready", sa.LeaderPostgresReady,
				"last_postgres_ready_time", lastReadyTime,
				"threshold", threshold)
			return nil, nil
		}
		if sa.LeaderPostgresRunning && primaryPostgresUnresponsive {
			a.factory.Logger().Warn("leader postgres process alive but unresponsive beyond threshold, allowing failover",
				"shard_key", sa.ShardKey.String(),
				"leader_pooler_id", topoclient.ComponentIDString(sa.HighestShardRule.GetLeaderId()),
				"leader_postgres_ready", sa.LeaderPostgresReady,
				"last_postgres_ready_time", lastReadyTime,
				"threshold", threshold)
		}

		// Case 3: pooler is reachable but reports the postgres process is dead.
		// This happens after SIGKILL: the process is gone but followers still show as
		// connected (TCP keepalive has not fired yet). Do not suppress.
		if !sa.LeaderPostgresRunning {
			a.factory.Logger().Warn("leader pooler reachable but postgres process is dead, replicas still connected (stale connections)",
				"shard_key", sa.ShardKey.String(),
				"leader_pooler_id", topoclient.ComponentIDString(sa.HighestShardRule.GetLeaderId()),
				"leader_postgres_ready", sa.LeaderPostgresReady,
				"leader_postgres_running", sa.LeaderPostgresRunning,
			)
		}
	}

	// Leader is dead — emit one shard-level problem.
	return []types.Problem{{
		Code:           types.ProblemLeaderIsDead,
		CheckName:      "LeaderIsDead",
		PoolerID:       sa.HighestShardRule.GetLeaderId(),
		ShardKey:       sa.ShardKey,
		Description:    fmt.Sprintf("Leader for shard %s is dead/unreachable", sa.ShardKey),
		Priority:       types.PriorityEmergency,
		Scope:          types.ScopeShard,
		DetectedAt:     time.Now(),
		RecoveryAction: a.factory.NewAppointLeaderAction(),
	}}, nil
}
