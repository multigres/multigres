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

	"github.com/multigres/multigres/go/multiorch/recovery/types"
	"github.com/multigres/multigres/go/multiorch/store"
)

// ReplicaTimelineDivergedAnalyzer detects when a replica has a diverged timeline
// that cannot be resolved by normal replication and requires pg_rewind.
type ReplicaTimelineDivergedAnalyzer struct {
	factory *RecoveryActionFactory
}

func (a *ReplicaTimelineDivergedAnalyzer) Name() types.CheckName {
	return "ReplicaTimelineDiverged"
}

func (a *ReplicaTimelineDivergedAnalyzer) Analyze(poolerAnalysis *store.ReplicationAnalysis) ([]types.Problem, error) {
	if a.factory == nil {
		return nil, errors.New("recovery action factory not initialized")
	}

	// Only analyze replicas
	if poolerAnalysis.IsPrimary {
		return nil, nil
	}

	// Skip if replica is not initialized
	if !poolerAnalysis.IsInitialized {
		return nil, nil
	}

	// Skip if primary is unreachable (can't run pg_rewind anyway)
	if poolerAnalysis.PrimaryPoolerID != nil && !poolerAnalysis.PrimaryReachable {
		return nil, nil
	}

	// Skip if no divergence detected
	if !poolerAnalysis.TimelineDiverged {
		return nil, nil
	}

	return []types.Problem{{
		Code:           types.ProblemReplicaTimelineDiverged,
		CheckName:      "ReplicaTimelineDiverged",
		PoolerID:       poolerAnalysis.PoolerID,
		ShardKey:       poolerAnalysis.ShardKey,
		Description:    fmt.Sprintf("Replica %s has diverged timeline requiring pg_rewind", poolerAnalysis.PoolerID.Name),
		Priority:       types.PriorityHigh,
		Scope:          types.ScopePooler,
		DetectedAt:     time.Now(),
		RecoveryAction: a.factory.NewFixReplicationAction(),
	}}, nil
}
