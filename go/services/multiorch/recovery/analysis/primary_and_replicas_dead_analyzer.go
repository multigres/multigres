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

// PrimaryAndReplicasDeadAnalyzer detects when a primary is unreachable and NO replicas
// are reachable. Non-actionable — no viable failover candidate exists.
type PrimaryAndReplicasDeadAnalyzer struct {
	factory *RecoveryActionFactory
}

func (a *PrimaryAndReplicasDeadAnalyzer) Name() types.CheckName {
	return "PrimaryAndReplicasDead"
}

func (a *PrimaryAndReplicasDeadAnalyzer) ProblemCode() types.ProblemCode {
	return types.ProblemPrimaryAndReplicasDead
}

func (a *PrimaryAndReplicasDeadAnalyzer) RecoveryAction() types.RecoveryAction {
	return &types.NoOpAction{}
}

func (a *PrimaryAndReplicasDeadAnalyzer) Analyze(poolerAnalysis *store.ReplicationAnalysis) (*types.Problem, error) {
	if a.factory == nil {
		return nil, errors.New("recovery action factory not initialized")
	}

	if checkPrimaryUnreachable(a.factory, poolerAnalysis) != primaryDeadNoReplicasReachable {
		return nil, nil
	}

	return &types.Problem{
		Code:      types.ProblemPrimaryAndReplicasDead,
		CheckName: "PrimaryAndReplicasDead",
		PoolerID:  poolerAnalysis.PoolerID,
		ShardKey:  poolerAnalysis.ShardKey,
		Description: fmt.Sprintf("Primary for shard %s is unreachable and none of its replicas is reachable (%d total)",
			poolerAnalysis.ShardKey, poolerAnalysis.CountReplicasInShard),
		Priority:       types.PriorityEmergency,
		Scope:          types.ScopeShard,
		DetectedAt:     time.Now(),
		RecoveryAction: &types.NoOpAction{},
	}, nil
}
