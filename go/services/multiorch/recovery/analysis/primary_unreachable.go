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
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// primaryUnreachableState represents the failure mode when the primary is unreachable.
type primaryUnreachableState int

const (
	// primaryOK means the primary is reachable or replicas confirm it's alive.
	primaryOK primaryUnreachableState = iota
	// primaryDeadAllReplicasReachable means primary is unreachable and ALL replicas
	// are reachable but none is connected to the primary.
	primaryDeadAllReplicasReachable
	// primaryDeadSomeReplicasReachable means primary is unreachable and SOME replicas
	// are reachable (but not all), and none of the reachable ones is connected.
	primaryDeadSomeReplicasReachable
	// primaryDeadNoReplicasReachable means primary is unreachable and NO replicas
	// are reachable either.
	primaryDeadNoReplicasReachable
)

// checkPrimaryUnreachable evaluates common preconditions for all primary-is-dead
// analyzers and returns the failure mode. Returns primaryOK if the analysis does not
// apply (wrong pooler type, primary reachable, replicas still connected, etc).
func checkPrimaryUnreachable(factory *RecoveryActionFactory, analysis *store.ReplicationAnalysis) primaryUnreachableState {
	// Only analyze replicas (primaries can't report themselves as dead)
	if analysis.IsPrimary {
		return primaryOK
	}

	// Skip if replica is not initialized (ShardNeedsBootstrap handles that)
	if !analysis.IsInitialized {
		return primaryOK
	}

	// Skip if no primary exists in topology
	if analysis.PrimaryPoolerID == nil {
		return primaryOK
	}

	// Primary is fully reachable (pooler up AND Postgres running)
	if analysis.PrimaryReachable {
		return primaryOK
	}

	// Primary pooler down but Postgres still running.
	// ReplicasConnectedToPrimary is true only when ALL replicas are:
	// streaming WAL, connected to the correct primary, and have healthy heartbeat.
	// This means the primary is alive — only the pooler process needs restart.
	if !analysis.PrimaryPoolerReachable && analysis.ReplicasConnectedToPrimary {
		factory.Logger().Warn("primary pooler unreachable but postgres still running (replicas connected with healthy heartbeat)",
			"shard_key", analysis.ShardKey.String(),
			"primary_pooler_id", topoclient.MultiPoolerIDString(analysis.PrimaryPoolerID),
			"action", "operator should restart pooler process")
		return primaryOK
	}

	// Primary is unreachable — determine failure mode based on replica visibility.
	switch {
	case analysis.CountReachableReplicasInShard == 0:
		return primaryDeadNoReplicasReachable
	case analysis.CountReachableReplicasInShard < analysis.CountReplicasInShard:
		return primaryDeadSomeReplicasReachable
	default:
		return primaryDeadAllReplicasReachable
	}
}
