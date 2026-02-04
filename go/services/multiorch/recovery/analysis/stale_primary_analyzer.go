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

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// StalePrimaryAnalyzer detects stale primaries that came back online after failover.
// This happens when an old primary restarts without being properly demoted.
// The analyzer identifies the stale primary (lower PrimaryTerm) and triggers demotion.
//
// The analyzer detects stale primaries from both perspectives:
// - When THIS pooler is stale (lower PrimaryTerm): reports itself for demotion
// - When OTHER primary is stale (this pooler has higher PrimaryTerm): reports the other for demotion
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

func (a *StalePrimaryAnalyzer) ProblemCode() types.ProblemCode {
	return types.ProblemStalePrimary
}

func (a *StalePrimaryAnalyzer) RecoveryAction() types.RecoveryAction {
	return a.factory.NewDemoteStalePrimaryAction()
}

func (a *StalePrimaryAnalyzer) Analyze(poolerAnalysis *store.ReplicationAnalysis) (*types.Problem, error) {
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

	// Skip if no other primaries detected
	if len(poolerAnalysis.OtherPrimariesInShard) == 0 {
		return nil, nil
	}

	// Skip if no most advanced primary identified (tie or all PrimaryTerm=0)
	if poolerAnalysis.MostAdvancedPrimary == nil {
		return nil, nil
	}

	// Multiple primaries detected! Determine which one is stale.
	// The primary with the highest PrimaryTerm is the most advanced (correct) primary.
	// All others should be demoted.

	thisPoolerIDStr := topoclient.MultiPoolerIDString(poolerAnalysis.PoolerID)
	mostAdvancedIDStr := topoclient.MultiPoolerIDString(poolerAnalysis.MostAdvancedPrimary.ID)

	var stalePrimaryID *clustermetadatapb.ID
	var stalePrimaryTerm int64
	var mostAdvancedPrimaryName string
	var mostAdvancedPrimaryTerm int64

	if thisPoolerIDStr == mostAdvancedIDStr {
		// This pooler is the most advanced - demote first other primary
		stalePrimaryID = poolerAnalysis.OtherPrimariesInShard[0].ID
		stalePrimaryTerm = poolerAnalysis.OtherPrimariesInShard[0].PrimaryTerm
		mostAdvancedPrimaryName = poolerAnalysis.PoolerID.Name
		mostAdvancedPrimaryTerm = poolerAnalysis.MostAdvancedPrimary.PrimaryTerm
	} else {
		// This pooler is stale
		stalePrimaryID = poolerAnalysis.PoolerID
		stalePrimaryTerm = poolerAnalysis.PrimaryTerm
		mostAdvancedPrimaryName = poolerAnalysis.MostAdvancedPrimary.ID.Name
		mostAdvancedPrimaryTerm = poolerAnalysis.MostAdvancedPrimary.PrimaryTerm
	}

	// Report the stale primary for demotion
	return &types.Problem{
		Code:      types.ProblemStalePrimary,
		CheckName: "StalePrimary",
		PoolerID:  stalePrimaryID,
		ShardKey:  poolerAnalysis.ShardKey,
		Description: fmt.Sprintf("Stale primary detected: %s (stale_primary_term %d) is stale, most advanced primary %s (most_advanced_primary_term %d)",
			stalePrimaryID.Name,
			stalePrimaryTerm,
			mostAdvancedPrimaryName,
			mostAdvancedPrimaryTerm),
		Priority:       types.PriorityEmergency,
		Scope:          types.ScopeShard,
		DetectedAt:     time.Now(),
		RecoveryAction: a.factory.NewDemoteStalePrimaryAction(),
	}, nil
}
