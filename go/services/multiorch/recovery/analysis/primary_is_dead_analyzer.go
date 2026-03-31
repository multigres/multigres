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

	// No initialized replica to confirm the primary is dead — skip to avoid false positives
	// when the shard has no replica that has joined the cohort yet.
	if !sa.HasInitializedReplica {
		return nil, nil
	}

	// Primary pooler is down but Postgres is still running (all replicas still connected).
	// Do not trigger failover — the operator should restart the pooler process.
	if !sa.PrimaryPoolerReachable && sa.ReplicasConnectedToPrimary {
		a.factory.Logger().Warn("primary pooler unreachable but postgres still running",
			"shard_key", sa.ShardKey.String(),
			"primary_pooler_id", topoclient.MultiPoolerIDString(sa.HighestTermDiscoveredPrimaryID),
			"action", "operator should restart pooler process")
		return nil, nil
	}

	// Primary is dead — emit one shard-level problem.
	return []types.Problem{{
		Code:           types.ProblemPrimaryIsDead,
		CheckName:      "PrimaryIsDead",
		PoolerID:       nil,
		ShardKey:       sa.ShardKey,
		Description:    fmt.Sprintf("Primary for shard %s is dead/unreachable", sa.ShardKey),
		Priority:       types.PriorityEmergency,
		Scope:          types.ScopeShard,
		DetectedAt:     time.Now(),
		RecoveryAction: a.factory.NewAppointLeaderAction(),
	}}, nil
}
