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

package analysis

import (
	"errors"
	"fmt"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// PrimaryIsDeadAnalyzer detects when a primary exists in topology but is unhealthy/unreachable.
// It operates at the shard level: if any initialized replica observes the primary as dead,
// one shard-scoped problem (PoolerID nil) is emitted.
type PrimaryIsDeadAnalyzer struct {
	factory *RecoveryActionFactory
}

func (a *PrimaryIsDeadAnalyzer) Name() types.CheckName {
	return "PrimaryIsDead"
}

func (a *PrimaryIsDeadAnalyzer) ProblemCode() types.ProblemCode {
	return types.ProblemPrimaryIsDead
}

func (a *PrimaryIsDeadAnalyzer) RecoveryAction() types.RecoveryAction {
	return a.factory.NewAppointLeaderAction()
}

func (a *PrimaryIsDeadAnalyzer) Analyze(sa *ShardAnalysis) ([]types.Problem, error) {
	if a.factory == nil {
		return nil, errors.New("recovery action factory not initialized")
	}

	// No primary known — nothing to declare dead.
	if sa.HighestTermDiscoveredPrimaryID == nil {
		return nil, nil
	}

	// Primary is fully reachable — no problem.
	if sa.PrimaryReachable {
		return nil, nil
	}

	// Primary is gracefully stopping — ShutdownPrimary handles the election.
	if sa.PrimaryIsStopping {
		return nil, nil
	}

	// No initialized replica to confirm the primary is dead — skip to avoid false positives
	// when the shard has no replica that has joined the cohort yet.
	if !sa.HasInitializedReplica {
		return nil, nil
	}

	// At this point, PrimaryReachable is false. This can happen in three cases:
	//
	// 1. Primary pooler is unreachable (e.g. pooler process crashed).
	//    Postgres may still be running; replicas can still receive WAL.
	//
	// 2. Primary pooler is reachable and Postgres process is alive yet
	//    unresponsive, pg_isready fails but the process exists (e.g. SIGSTOP or
	//    overloaded). Replicas remain connected until TCP keepalive times out.
	//
	// 3. Primary pooler is reachable but Postgres process is dead: this means
	//    pg_isready can be assumed to fail and the process is gone (e.g.
	//    SIGKILL). Replicas may still appear connected for ~30s via TCP
	//    keepalive even though Postgres is dead.
	//
	// For cases 1 and 2, we check if ALL replicas are still connected to the
	// primary Postgres. If they are, Postgres is still running (or recovering)
	// and we suppress failover, but only if the primary postgres responded
	// recently enough. This prevents suppressing indefinitely when replicas are
	// observing stale connections while postgres is unresponsive.
	//
	// For case 3, we must NOT suppress: the pooler reports the process is dead,
	// so replicas' apparent connections are stale (TCP keepalive hasn't fired
	// yet). Suppressing would delay failover by up to the TCP keepalive
	// interval (~30s).

	// Suppress while the topology primary is explicitly mid-promotion.
	// Three guards are all required:
	//   - PromotingPrimaryID != nil: multipooler has flagged pg_promote() is running
	//   - PrimaryPoolerReachable: stream is live, so the flag is current (not stale)
	//   - PrimaryPostgresRunning: postgres process is still alive
	// If postgres crashes during promotion, PrimaryPostgresRunning=false and we fall through.
	// If the multipooler crashes, PrimaryPoolerReachable=false and we fall through.
	if sa.PromotingPrimaryID != nil && sa.PrimaryPoolerReachable && sa.PrimaryPostgresRunning {
		a.factory.Logger().Info("primary promotion in progress, suppressing PrimaryIsDead",
			"shard_key", sa.ShardKey.String(),
			"promoting_primary", topoclient.MultiPoolerIDString(sa.PromotingPrimaryID))
		return nil, nil
	}

	if sa.ReplicasConnectedToPrimary && !sa.PrimaryHasResigned {
		threshold := a.factory.Config().GetPrimaryPostgresResponseThreshold()
		lastReadyTime := sa.PrimaryLastPostgresReadyTime
		primaryPostgresUnresponsive := !sa.PrimaryPostgresReady &&
			(lastReadyTime.IsZero() || time.Since(lastReadyTime) > threshold)

		// Cases 1 and 2: replicas are connected and the primary pooler is down
		// OR the postgres process is alive (but possibly unresponsive). Suppress
		// failover if postgres responded recently (within threshold).
		if (!sa.PrimaryPoolerReachable || sa.PrimaryPostgresRunning) && !primaryPostgresUnresponsive {
			a.factory.Logger().Warn("primary not fully reachable but replicas still connected to postgres (within threshold)",
				"shard_key", sa.ShardKey.String(),
				"primary_pooler_id", topoclient.MultiPoolerIDString(sa.HighestTermDiscoveredPrimaryID),
				"primary_pooler_reachable", sa.PrimaryPoolerReachable,
				"primary_postgres_ready", sa.PrimaryPostgresReady,
				"primary_postgres_running", sa.PrimaryPostgresRunning,
				"last_postgres_ready_time", lastReadyTime,
				"threshold", threshold)
			return nil, nil
		}

		// Cases 1 and 2: postgres timestamp expired or unset — suppression window closed, allowing failover.
		if (!sa.PrimaryPoolerReachable || sa.PrimaryPostgresRunning) && primaryPostgresUnresponsive {
			a.factory.Logger().Warn("primary not fully reachable, postgres timestamp expired or unset, allowing failover",
				"shard_key", sa.ShardKey.String(),
				"primary_pooler_id", topoclient.MultiPoolerIDString(sa.HighestTermDiscoveredPrimaryID),
				"primary_pooler_reachable", sa.PrimaryPoolerReachable,
				"primary_postgres_ready", sa.PrimaryPostgresReady,
				"primary_postgres_running", sa.PrimaryPostgresRunning,
				"last_postgres_ready_time", lastReadyTime,
				"threshold", threshold)
		}

		// Case 3: pooler is reachable but reports postgres process is dead.
		// This happens after SIGKILL: the process is gone but replicas still show as connected.
		if sa.PrimaryPoolerReachable && !sa.PrimaryPostgresRunning {
			a.factory.Logger().Warn("primary pooler reachable but postgres process is dead, replicas still connected (stale connections)",
				"shard_key", sa.ShardKey.String(),
				"primary_pooler_id", topoclient.MultiPoolerIDString(sa.HighestTermDiscoveredPrimaryID),
				"primary_postgres_ready", sa.PrimaryPostgresReady,
				"primary_postgres_running", sa.PrimaryPostgresRunning,
			)
		}
	}

	// Primary is dead — emit one shard-level problem.
	return []types.Problem{{
		Code:           types.ProblemPrimaryIsDead,
		CheckName:      "PrimaryIsDead",
		PoolerID:       sa.HighestTermDiscoveredPrimaryID,
		ShardKey:       sa.ShardKey,
		Description:    fmt.Sprintf("Primary for shard %s is dead/unreachable", sa.ShardKey),
		Priority:       types.PriorityEmergency,
		Scope:          types.ScopeShard,
		DetectedAt:     time.Now(),
		RecoveryAction: a.factory.NewAppointLeaderAction(),
	}}, nil
}
