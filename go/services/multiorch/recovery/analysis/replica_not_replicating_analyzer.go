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

	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// ReplicaNotReplicatingAnalyzer detects when a replica has no replication configured.
// This happens when primary_conninfo is not set or replication is stopped.
type ReplicaNotReplicatingAnalyzer struct {
	factory *RecoveryActionFactory
}

func (a *ReplicaNotReplicatingAnalyzer) Name() types.CheckName {
	return "ReplicaNotReplicating"
}

func (a *ReplicaNotReplicatingAnalyzer) ProblemCode() types.ProblemCode {
	return types.ProblemReplicaNotReplicating
}

func (a *ReplicaNotReplicatingAnalyzer) RecoveryAction() types.RecoveryAction {
	return a.factory.NewFixReplicationAction()
}

func (a *ReplicaNotReplicatingAnalyzer) Analyze(sa *ShardAnalysis) ([]types.Problem, error) {
	return analyzeAllPoolers(sa, a.analyzePooler)
}

func (a *ReplicaNotReplicatingAnalyzer) analyzePooler(sa *ShardAnalysis, poolerAnalysis *PoolerAnalysis) (*types.Problem, error) {
	if a.factory == nil {
		return nil, errors.New("recovery action factory not initialized")
	}

	// Only analyze replicas
	if poolerAnalysis.NamesSelfAsLeader {
		return nil, nil
	}

	// Skip if replica is not initialized (ShardNeedsInitialization handles that)
	if !poolerAnalysis.IsInitialized {
		return nil, nil
	}

	// Skip unless we know where to point the replica: the shard must have a known
	// consensus leader (HighestShardRule) whose host/port we actually have (Leader
	// health present). A leader we have no address for is not actionable.
	//
	// TODO(temporary): we also require the leader to be reachable because today's
	// FixReplication still runs pg_rewind against the leader, which needs it live.
	// Once rewind is separated from SetPrimary (SetPrimary just delivers the
	// leader's rule + address), leader reachability no longer matters here — an
	// unreachable-but-known leader is still the official term leader worth telling
	// replicas about, and only knowing where to point them matters.
	if sa.Leader == nil || sa.Leader.Health().GetMultiPooler().GetHostname() == "" || !sa.LeaderReachable {
		return nil, nil
	}

	// Check if replication is not configured or stopped
	if !a.needsReplicationFix(poolerAnalysis) {
		return nil, nil
	}

	return &types.Problem{
		Code:           types.ProblemReplicaNotReplicating,
		CheckName:      "ReplicaNotReplicating",
		PoolerID:       poolerAnalysis.PoolerID,
		ShardKey:       poolerAnalysis.ShardKey,
		Description:    fmt.Sprintf("Replica %s has no replication configured", poolerAnalysis.PoolerID.Name),
		Priority:       types.PriorityHigh,
		Scope:          types.ScopePooler,
		DetectedAt:     time.Now(),
		RecoveryAction: a.factory.NewFixReplicationAction(),
	}, nil
}

// needsReplicationFix returns true if replication is not configured or stopped.
func (a *ReplicaNotReplicatingAnalyzer) needsReplicationFix(analysis *PoolerAnalysis) bool {
	// No primary_conninfo configured
	if analysis.PrimaryConnInfoHost == "" {
		return true
	}

	// Replication not running (e.g. WAL replay paused)
	if !analysis.WalReplayNotPaused {
		return true
	}

	return false
}
