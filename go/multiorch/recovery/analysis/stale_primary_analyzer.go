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

// StalePrimaryAnalyzer detects when THIS node is a stale primary that came back online
// after failover. This happens when an old primary restarts without being properly demoted.
// The analyzer identifies the stale primary (lower consensus term) and triggers demotion.
//
// Note: This is NOT true split-brain. True split-brain means both primaries can accept
// writes. In this scenario, the new primary cannot accept writes because it cannot
// recruit standbys while the stale primary exists.
type StalePrimaryAnalyzer struct {
	factory *RecoveryActionFactory
}

func (a *StalePrimaryAnalyzer) Name() types.CheckName {
	return "StalePrimary"
}

func (a *StalePrimaryAnalyzer) Analyze(poolerAnalysis *store.ReplicationAnalysis) ([]types.Problem, error) {
	if a.factory == nil {
		return nil, errors.New("recovery action factory not initialized")
	}

	// Only analyze primaries - stale primary is detected from the PRIMARY's perspective
	if !poolerAnalysis.IsPrimary {
		return nil, nil
	}

	// Skip if not initialized
	if !poolerAnalysis.IsInitialized {
		return nil, nil
	}

	// Skip if no other primary detected
	if poolerAnalysis.OtherPrimaryInShard == nil {
		return nil, nil
	}

	// Multiple primaries detected! Determine which one is stale.
	// The one with the lower consensus term is stale and should be demoted.
	// If terms are equal, we demote THIS node to be safe (let the other one win).
	isThisNodeStale := poolerAnalysis.ConsensusTerm <= poolerAnalysis.OtherPrimaryTerm

	if !isThisNodeStale {
		// The other node is stale, not this one.
		// We'll let the other node's analysis handle its own demotion.
		// This prevents both nodes trying to demote the other simultaneously.
		return nil, nil
	}

	// This node is the stale primary - it needs to be demoted
	return []types.Problem{{
		Code:      types.ProblemStalePrimary,
		CheckName: "StalePrimary",
		PoolerID:  poolerAnalysis.PoolerID,
		ShardKey:  poolerAnalysis.ShardKey,
		Description: fmt.Sprintf("Stale primary detected: %s (term %d) is stale, other primary %s has term %d",
			poolerAnalysis.PoolerID.Name,
			poolerAnalysis.ConsensusTerm,
			poolerAnalysis.OtherPrimaryInShard.Name,
			poolerAnalysis.OtherPrimaryTerm),
		Priority:       types.PriorityEmergency,
		Scope:          types.ScopeShard,
		DetectedAt:     time.Now(),
		RecoveryAction: a.factory.NewDemoteStalePrimaryAction(),
	}}, nil
}
