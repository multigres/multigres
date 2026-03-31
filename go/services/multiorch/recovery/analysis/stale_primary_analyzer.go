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
	"slices"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// StalePrimaryAnalyzer detects stale primaries that came back online after failover.
// This happens when an old primary restarts without being properly demoted.
//
// The analyzer operates at the shard level: when multiple primaries are detected,
// it reports all of them except the highest-term primary as stale. Problems are
// sorted most-stale-first with descending priorities so the recovery system addresses
// the most out-of-date primary first.
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

func (a *StalePrimaryAnalyzer) Analyze(sa *ShardAnalysis) ([]types.Problem, error) {
	if a.factory == nil {
		return nil, errors.New("recovery action factory not initialized")
	}

	// Need multiple primaries to detect staleness.
	if len(sa.Primaries) <= 1 {
		return nil, nil
	}

	// A tie in PrimaryTerm indicates a consensus bug — skip automatic demotion.
	if sa.HighestTermPrimary == nil {
		return nil, nil
	}

	// Collect stale primaries: all in sa.Primaries that are not the highest-term primary
	// and have a valid (non-zero) PrimaryTerm.
	mostAdvancedIDStr := topoclient.MultiPoolerIDString(sa.HighestTermPrimary.PoolerID)
	var stalePrimaries []*PoolerAnalysis
	for _, p := range sa.Primaries {
		if topoclient.MultiPoolerIDString(p.PoolerID) == mostAdvancedIDStr {
			continue
		}
		if p.PrimaryTerm == 0 {
			continue
		}
		stalePrimaries = append(stalePrimaries, p)
	}

	if len(stalePrimaries) == 0 {
		return nil, nil
	}

	// Sort most stale first (lowest PrimaryTerm first) so the recovery system
	// processes the most out-of-date primary at highest priority.
	slices.SortFunc(stalePrimaries, comparePrimaryTimeline)

	// Assign descending priorities so the most stale primary (lowest PrimaryTerm,
	// sorted first) gets PriorityEmergency, the next gets PriorityEmergency-1, etc.
	problems := make([]types.Problem, 0, len(stalePrimaries))
	for i, stale := range stalePrimaries {
		problems = append(problems, types.Problem{
			Code:      types.ProblemStalePrimary,
			CheckName: "StalePrimary",
			PoolerID:  stale.PoolerID,
			ShardKey:  sa.ShardKey,
			Description: fmt.Sprintf("Stale primary detected: %s (stale_primary_term %d) is stale, most advanced primary %s (most_advanced_primary_term %d)",
				stale.PoolerID.Name,
				stale.PrimaryTerm,
				sa.HighestTermPrimary.PoolerID.Name,
				sa.HighestTermPrimary.PrimaryTerm),
			Priority:       types.PriorityEmergency - types.Priority(i),
			Scope:          types.ScopeShard,
			DetectedAt:     time.Now(),
			RecoveryAction: a.factory.NewDemoteStalePrimaryAction(),
		})
	}
	return problems, nil
}
