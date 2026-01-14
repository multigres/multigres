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

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// ReplicaNotInStandbyListAnalyzer detects when a replica is not in the primary's
// synchronous_standby_names list. This can happen if:
// - The replica was added but never registered with the primary
// - The standby list was cleared/modified manually
// - There was a failure during the fix replication process
type ReplicaNotInStandbyListAnalyzer struct {
	factory *RecoveryActionFactory
}

func (a *ReplicaNotInStandbyListAnalyzer) Name() types.CheckName {
	return "ReplicaNotInStandbyList"
}

func (a *ReplicaNotInStandbyListAnalyzer) Analyze(poolerAnalysis *store.ReplicationAnalysis) ([]types.Problem, error) {
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

	// Skip if primary is unreachable (can't update standby list anyway)
	if poolerAnalysis.PrimaryPoolerID != nil && !poolerAnalysis.PrimaryReachable {
		return nil, nil
	}

	// Skip if replication is not configured (ReplicaNotReplicating handles that)
	if poolerAnalysis.PrimaryConnInfoHost == "" {
		return nil, nil
	}

	// Only check replicas with PoolerType_REPLICA (not UNKNOWN or other types)
	if poolerAnalysis.PoolerType != clustermetadatapb.PoolerType_REPLICA {
		return nil, nil
	}

	// Check if replica is already in the standby list
	if poolerAnalysis.IsInPrimaryStandbyList {
		return nil, nil
	}

	return []types.Problem{{
		Code:           types.ProblemReplicaNotInStandbyList,
		CheckName:      "ReplicaNotInStandbyList",
		PoolerID:       poolerAnalysis.PoolerID,
		ShardKey:       poolerAnalysis.ShardKey,
		Description:    fmt.Sprintf("Replica %s is not in primary's synchronous standby list", poolerAnalysis.PoolerID.Name),
		Priority:       types.PriorityNormal,
		Scope:          types.ScopePooler,
		DetectedAt:     time.Now(),
		RecoveryAction: a.factory.NewFixReplicationAction(),
	}}, nil
}
