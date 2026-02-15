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

// PrimaryAndReplicasDeadAnalyzer detects when a primary is unreachable and NO replica
// poolers are reachable. This most likely means multiorch itself is network-partitioned
// from the entire cluster, not that every node is actually down. Non-actionable: there
// are no viable failover candidates, and the other primary-is-dead analyzers will
// trigger the appropriate recovery if any replica becomes reachable. This analyzer
// exists for observability — it surfaces the problem so operators can investigate
// the network partition.
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

	// Cannot use checkPrimaryUnreachable: it requires IsInitialized, but
	// unreachable replicas have IsInitialized=false (IsLastCheckValid=false).
	// In this analyzer's target scenario (all replicas unreachable), every
	// analysis would be rejected.
	if poolerAnalysis.IsPrimary {
		return nil, nil
	}
	if poolerAnalysis.PrimaryPoolerID == nil {
		return nil, nil
	}
	if poolerAnalysis.PrimaryReachable {
		return nil, nil
	}

	// No replica poolers reachable — non-actionable, no viable failover candidate.
	if poolerAnalysis.CountReachableReplicaPoolersInShard != 0 {
		return nil, nil
	}

	return &types.Problem{
		Code:      types.ProblemPrimaryAndReplicasDead,
		CheckName: "PrimaryAndReplicasDead",
		PoolerID:  poolerAnalysis.PoolerID,
		ShardKey:  poolerAnalysis.ShardKey,
		Description: fmt.Sprintf("Primary for shard %s is unreachable and no replica poolers are reachable (%d total); likely multiorch network partition — no failover candidates available",
			poolerAnalysis.ShardKey, poolerAnalysis.CountReplicaPoolersInShard),
		Priority:       types.PriorityNormal,
		Scope:          types.ScopeShard,
		DetectedAt:     time.Now(),
		RecoveryAction: &types.NoOpAction{},
	}, nil
}
