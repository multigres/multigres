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

	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// PrimaryIsDeadAndSomeReplicasAnalyzer detects when a primary is unreachable,
// SOME but not all replica poolers are reachable, and none of the reachable
// replicas is connected to the primary. Triggers failover with partial visibility.
type PrimaryIsDeadAndSomeReplicasAnalyzer struct {
	factory *RecoveryActionFactory
}

func (a *PrimaryIsDeadAndSomeReplicasAnalyzer) Name() types.CheckName {
	return "PrimaryIsDeadAndSomeReplicas"
}

func (a *PrimaryIsDeadAndSomeReplicasAnalyzer) ProblemCode() types.ProblemCode {
	return types.ProblemPrimaryIsDeadAndSomeReplicas
}

func (a *PrimaryIsDeadAndSomeReplicasAnalyzer) RecoveryAction() types.RecoveryAction {
	return a.factory.NewAppointLeaderAction()
}

func (a *PrimaryIsDeadAndSomeReplicasAnalyzer) Analyze(poolerAnalysis *store.ReplicationAnalysis) (*types.Problem, error) {
	if a.factory == nil {
		return nil, errors.New("recovery action factory not initialized")
	}

	if !checkPrimaryUnreachable(poolerAnalysis) {
		return nil, nil
	}

	// If there are no reachable replicas, this analyzer can't make a decision.
	if poolerAnalysis.CountReachableReplicaPoolersInShard == 0 {
		return nil, nil
	}

	// All replicas are reachable, so this analyzer can't make a decision.
	if poolerAnalysis.CountReachableReplicaPoolersInShard == poolerAnalysis.CountReplicaPoolersInShard {
		return nil, nil
	}

	// If any reachable replica is still connected to the primary, the primary
	// may be alive — don't failover with only partial visibility.
	if poolerAnalysis.CountReplicasConfirmingPrimaryAliveInShard > 0 {
		return nil, nil
	}

	return &types.Problem{
		Code:      types.ProblemPrimaryIsDeadAndSomeReplicas,
		CheckName: "PrimaryIsDeadAndSomeReplicas",
		PoolerID:  poolerAnalysis.PoolerID,
		ShardKey:  poolerAnalysis.ShardKey,
		Description: fmt.Sprintf("Primary for shard %s is dead/unreachable; some replicas unreachable and none of the reachable ones is connected (%d/%d reachable)",
			poolerAnalysis.ShardKey, poolerAnalysis.CountReachableReplicaPoolersInShard, poolerAnalysis.CountReplicaPoolersInShard),
		Priority:       types.PriorityCritical,
		Scope:          types.ScopeShard,
		DetectedAt:     time.Now(),
		RecoveryAction: a.factory.NewAppointLeaderAction(),
	}, nil
}
