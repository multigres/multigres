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

// PrimaryRevokedAnalyzer detects when the primary is reachable but all replicas
// have disconnected from it.
//
// This catches a rare but important edge case:
// 1. The primary fails.
// 2. multiorch recruits replicas but fails to appoint a new leader.
// 3. The old primary comes back online. It is alive, but it has been revoked.
// Other analyzers can short-circuit when the old primary is alive, so we need an
// explicit check for the "alive but revoked" state.
//
// This also helps us test one of the core revocation mechanisms in our consensus
// algorithm: revoke a primary by revoking all of its followers.
type PrimaryRevokedAnalyzer struct {
	factory *RecoveryActionFactory
}

func (a *PrimaryRevokedAnalyzer) Name() types.CheckName {
	return "PrimaryRevoked"
}

func (a *PrimaryRevokedAnalyzer) ProblemCode() types.ProblemCode {
	return types.ProblemPrimaryRevoked
}

func (a *PrimaryRevokedAnalyzer) RecoveryAction() types.RecoveryAction {
	return a.factory.NewAppointLeaderAction()
}

func (a *PrimaryRevokedAnalyzer) Analyze(poolerAnalysis *store.ReplicationAnalysis) (*types.Problem, error) {
	if a.factory == nil {
		return nil, errors.New("recovery action factory not initialized")
	}

	// Only analyze replicas (primaries can't detect their own revocation)
	if poolerAnalysis.IsPrimary {
		return nil, nil
	}

	if !poolerAnalysis.IsInitialized {
		return nil, nil
	}

	// Primary must exist and be reachable — distinguishes from PrimaryIsDead
	if poolerAnalysis.PrimaryPoolerID == nil || !poolerAnalysis.PrimaryReachable {
		return nil, nil
	}

	// Must have replicas to failover to
	if poolerAnalysis.CountReplicaPoolersInShard == 0 {
		return nil, nil
	}

	// Full visibility required: all replica poolers must be reachable
	if poolerAnalysis.CountReachableReplicaPoolersInShard != poolerAnalysis.CountReplicaPoolersInShard {
		return nil, nil
	}

	// Both conditions must hold to confirm a revoked primary:
	// 1. This replica's consensus term exceeds the primary's promotion term,
	//    proving the replica received a BeginTerm REVOKE with a higher term.
	if poolerAnalysis.ConsensusTerm <= poolerAnalysis.PrimaryTerm {
		return nil, nil
	}

	// 2. No replicas are still replicating from the primary.
	if poolerAnalysis.CountReplicasConfirmingPrimaryAliveInShard > 0 {
		return nil, nil
	}

	return &types.Problem{
		Code:      types.ProblemPrimaryRevoked,
		CheckName: "PrimaryRevoked",
		PoolerID:  poolerAnalysis.PoolerID,
		ShardKey:  poolerAnalysis.ShardKey,
		Description: fmt.Sprintf("Primary for shard %s is reachable but all replicas have disconnected (revoked)",
			poolerAnalysis.ShardKey),
		Priority:       types.PriorityEmergency,
		Scope:          types.ScopeShard,
		DetectedAt:     time.Now(),
		RecoveryAction: a.factory.NewAppointLeaderAction(),
	}, nil
}
