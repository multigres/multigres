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
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// PrimaryIsDeadAnalyzer detects when a primary exists in topology but is unhealthy/unreachable.
// This is a per-pooler analyzer that returns a shard-wide problem.
// The recovery loop's filterAndPrioritize() will deduplicate multiple instances.
type PrimaryIsDeadAnalyzer struct {
	factory *RecoveryActionFactory
}

func (a *PrimaryIsDeadAnalyzer) Name() types.CheckName {
	return "PrimaryIsDead"
}

func (a *PrimaryIsDeadAnalyzer) Analyze(poolerAnalysis *store.ReplicationAnalysis) ([]types.Problem, error) {
	if a.factory == nil {
		return nil, errors.New("recovery action factory not initialized")
	}

	// Only analyze replicas (primaries can't report themselves as dead)
	if poolerAnalysis.IsPrimary {
		return nil, nil
	}

	// Skip if replica is not initialized (ShardNeedsBootstrap handles that)
	if !poolerAnalysis.IsInitialized {
		return nil, nil
	}

	// Skip if no primary exists in topology
	if poolerAnalysis.PrimaryPoolerID == nil {
		return nil, nil
	}

	// Early return if primary is fully reachable (pooler up AND Postgres running)
	if poolerAnalysis.PrimaryReachable {
		return nil, nil
	}

	// At this point, PrimaryReachable is false. This can happen in two cases:
	// 1. Primary pooler is reachable but Postgres is down -> FAILOVER
	// 2. Primary pooler is unreachable -> need to check if Postgres is still running
	//
	// For case 2, we check if ALL replicas are still connected to the primary Postgres.
	// If they are, Postgres is still running and only the pooler process is down.
	// In this case, we do NOT trigger failover - the operator should restart the pooler.
	if !poolerAnalysis.PrimaryPoolerReachable && poolerAnalysis.ReplicasConnectedToPrimary {
		// Primary pooler is down but Postgres is still running (replicas are connected).
		// Do not trigger failover - operator should restart the pooler process.
		a.factory.Logger().Warn("primary pooler unreachable but postgres still running",
			"shard_key", poolerAnalysis.ShardKey.String(),
			"primary_pooler_id", topoclient.MultiPoolerIDString(poolerAnalysis.PrimaryPoolerID),
			"action", "operator should restart pooler process")
		return nil, nil
	}

	return []types.Problem{{
		Code:           types.ProblemPrimaryIsDead,
		CheckName:      "PrimaryIsDead",
		PoolerID:       poolerAnalysis.PoolerID,
		ShardKey:       poolerAnalysis.ShardKey,
		Description:    fmt.Sprintf("Primary for shard %s is dead/unreachable", poolerAnalysis.ShardKey),
		Priority:       types.PriorityEmergency,
		Scope:          types.ScopeShard,
		DetectedAt:     time.Now(),
		RecoveryAction: a.factory.NewAppointLeaderAction(),
	}}, nil
}
