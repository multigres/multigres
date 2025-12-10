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

// ReplicaNotReplicatingAnalyzer detects when a replica has no replication configured.
// This happens when primary_conninfo is not set or replication is stopped.
type ReplicaNotReplicatingAnalyzer struct {
	factory *RecoveryActionFactory
}

func (a *ReplicaNotReplicatingAnalyzer) Name() types.CheckName {
	return "ReplicaNotReplicating"
}

func (a *ReplicaNotReplicatingAnalyzer) Analyze(poolerAnalysis *store.ReplicationAnalysis) ([]types.Problem, error) {
	if a.factory == nil {
		return nil, errors.New("recovery action factory not initialized")
	}

	// Only analyze replicas
	if poolerAnalysis.IsPrimary {
		return nil, nil
	}

	// Skip if replica is not initialized (ShardNeedsBootstrap handles that)
	if !poolerAnalysis.IsInitialized {
		return nil, nil
	}

	// Skip if primary is unreachable (PrimaryIsDead handles that)
	if poolerAnalysis.PrimaryPoolerID != nil && !poolerAnalysis.PrimaryReachable {
		return nil, nil
	}

	// Check if replication is not configured or stopped
	if !a.needsReplicationFix(poolerAnalysis) {
		return nil, nil
	}

	return []types.Problem{{
		Code:           types.ProblemReplicaNotReplicating,
		CheckName:      "ReplicaNotReplicating",
		PoolerID:       poolerAnalysis.PoolerID,
		ShardKey:       poolerAnalysis.ShardKey,
		Description:    fmt.Sprintf("Replica %s has no replication configured", poolerAnalysis.PoolerID.Name),
		Priority:       types.PriorityHigh,
		Scope:          types.ScopePooler,
		DetectedAt:     time.Now(),
		RecoveryAction: a.factory.NewFixReplicationAction(),
	}}, nil
}

// needsReplicationFix returns true if replication is not configured or stopped.
func (a *ReplicaNotReplicatingAnalyzer) needsReplicationFix(analysis *store.ReplicationAnalysis) bool {
	// No primary_conninfo configured
	if analysis.PrimaryConnInfoHost == "" {
		return true
	}

	// Replication explicitly stopped
	if analysis.ReplicationStopped {
		return true
	}

	return false
}
