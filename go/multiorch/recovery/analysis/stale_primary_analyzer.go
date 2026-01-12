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

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// StalePrimaryAnalyzer detects stale primaries that came back online after failover.
// This happens when an old primary restarts without being properly demoted.
// The analyzer identifies the stale primary (lower consensus term) and triggers demotion.
//
// The analyzer detects stale primaries from both perspectives:
// - When THIS node is stale (lower term): reports itself for demotion
// - When OTHER primary is stale (this node has higher term): reports the other for demotion
//
// This allows the correct primary to proactively demote stale primaries, which is important
// because the stale primary may not be running multiorch yet or may not be healthy enough
// to detect and demote itself.
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
	// If terms are equal, neither node demotes - this is an unusual state that
	// should be resolved through normal failover or manual intervention.

	if poolerAnalysis.ConsensusTerm == poolerAnalysis.OtherPrimaryTerm {
		// Terms are equal - unusual state, skip
		return nil, nil
	}

	// Determine which primary is stale
	var stalePrimaryID *clustermetadatapb.ID
	var stalePrimaryTerm int64
	var correctPrimaryName string
	var correctPrimaryTerm int64

	if poolerAnalysis.ConsensusTerm < poolerAnalysis.OtherPrimaryTerm {
		// This node is stale
		stalePrimaryID = poolerAnalysis.PoolerID
		stalePrimaryTerm = poolerAnalysis.ConsensusTerm
		correctPrimaryName = poolerAnalysis.OtherPrimaryInShard.Name
		correctPrimaryTerm = poolerAnalysis.OtherPrimaryTerm
	} else {
		// Other node is stale
		stalePrimaryID = poolerAnalysis.OtherPrimaryInShard
		stalePrimaryTerm = poolerAnalysis.OtherPrimaryTerm
		correctPrimaryName = poolerAnalysis.PoolerID.Name
		correctPrimaryTerm = poolerAnalysis.ConsensusTerm
	}

	// Report the stale primary for demotion
	return []types.Problem{{
		Code:      types.ProblemStalePrimary,
		CheckName: "StalePrimary",
		PoolerID:  stalePrimaryID,
		ShardKey:  poolerAnalysis.ShardKey,
		Description: fmt.Sprintf("Stale primary detected: %s (term %d) is stale, correct primary %s has term %d",
			stalePrimaryID.Name,
			stalePrimaryTerm,
			correctPrimaryName,
			correctPrimaryTerm),
		Priority:       types.PriorityEmergency,
		Scope:          types.ScopeShard,
		DetectedAt:     time.Now(),
		RecoveryAction: a.factory.NewDemoteStalePrimaryAction(),
	}}, nil
}
