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

	// No leader known — nothing to declare dead.
	if sa.HighestTermDiscoveredLeaderID == nil {
		return nil, nil
	}

	// Leader is fully reachable — no problem.
	if sa.LeaderReachable {
		return nil, nil
	}

	// No initialized follower to confirm the leader is dead — skip to avoid false positives
	// when the shard has no follower that has joined the cohort yet.
	if !sa.HasInitializedReplica {
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

	if sa.FollowersConnectedToLeader && !sa.LeaderHasResigned {
		threshold := a.factory.Config().GetLeaderPostgresResponseThreshold()
		lastReadyTime := sa.LeaderLastPostgresReadyTime
		primaryPostgresUnresponsive := !sa.LeaderPostgresReady &&
			(lastReadyTime.IsZero() || time.Since(lastReadyTime) > threshold)

		// Cases 1 and 2: followers are connected and the leader pooler is down
		// OR the postgres process is alive (but possibly unresponsive). Suppress
		// failover if postgres responded recently (within threshold).
		if (!sa.LeaderPoolerReachable || sa.LeaderPostgresRunning) && !primaryPostgresUnresponsive {
			a.factory.Logger().Warn("leader not fully reachable but replicas still connected to postgres (within threshold)",
				"shard_key", sa.ShardKey.String(),
				"leader_pooler_id", topoclient.MultiPoolerIDString(sa.HighestTermDiscoveredLeaderID),
				"leader_pooler_reachable", sa.LeaderPoolerReachable,
				"leader_postgres_ready", sa.LeaderPostgresReady,
				"leader_postgres_running", sa.LeaderPostgresRunning,
				"last_postgres_ready_time", lastReadyTime,
				"threshold", threshold)
			return nil, nil
		}

		// Cases 1 and 2: postgres timestamp expired or unset — suppression window closed, allowing failover.
		if (!sa.LeaderPoolerReachable || sa.LeaderPostgresRunning) && primaryPostgresUnresponsive {
			a.factory.Logger().Warn("leader not fully reachable, postgres timestamp expired or unset, allowing failover",
				"shard_key", sa.ShardKey.String(),
				"leader_pooler_id", topoclient.MultiPoolerIDString(sa.HighestTermDiscoveredLeaderID),
				"leader_pooler_reachable", sa.LeaderPoolerReachable,
				"leader_postgres_ready", sa.LeaderPostgresReady,
				"leader_postgres_running", sa.LeaderPostgresRunning,
				"last_postgres_ready_time", lastReadyTime,
				"threshold", threshold)
		}

		// Case 3: pooler is reachable but reports postgres process is dead.
		// This happens after SIGKILL: the process is gone but followers still show as connected.
		if sa.LeaderPoolerReachable && !sa.LeaderPostgresRunning {
			a.factory.Logger().Warn("leader pooler reachable but postgres process is dead, replicas still connected (stale connections)",
				"shard_key", sa.ShardKey.String(),
				"leader_pooler_id", topoclient.MultiPoolerIDString(sa.HighestTermDiscoveredLeaderID),
				"leader_postgres_ready", sa.LeaderPostgresReady,
				"leader_postgres_running", sa.LeaderPostgresRunning,
			)
		}
	}

	// Primary is dead — emit one shard-level problem.
	return []types.Problem{{
		Code:           types.ProblemLeaderIsDead,
		CheckName:      "LeaderIsDead",
		PoolerID:       sa.HighestTermDiscoveredLeaderID,
		ShardKey:       sa.ShardKey,
		Description:    fmt.Sprintf("Leader for shard %s is dead/unreachable", sa.ShardKey),
		Priority:       types.PriorityEmergency,
		Scope:          types.ScopeShard,
		DetectedAt:     time.Now(),
		RecoveryAction: a.factory.NewAppointLeaderAction(),
	}}, nil
}
