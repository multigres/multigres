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

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
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
	if sa.HighestTermReachablePrimary == nil {
		return nil, nil
	}

	// Collect stale primaries: every topology-PRIMARY pooler that is not the
	// highest-term primary is stale. This includes poolers whose own rule has
	// caught up (PrimaryTerm == 0 because the rule now names a different
	// primary) — exactly the post-emergency-demotion state we need to repair.
	mostAdvancedIDStr := topoclient.MultiPoolerIDString(sa.HighestTermReachablePrimary.PoolerID)
	var stalePrimaries []*PoolerAnalysis
	for _, p := range sa.Primaries {
		if topoclient.MultiPoolerIDString(p.PoolerID) == mostAdvancedIDStr {
			continue
		}
		stalePrimaries = append(stalePrimaries, p)
	}

	if len(stalePrimaries) == 0 {
		return nil, nil
	}

	// Sort most stale first (lowest rule coordinator term first) so the
	// recovery system processes the most out-of-date primary at highest
	// priority.
	slices.SortFunc(stalePrimaries, comparePrimaryTimeline)

	// Assign descending priorities so the most stale primary (sorted first)
	// gets PriorityEmergency, the next gets PriorityEmergency-1, etc.
	problems := make([]types.Problem, 0, len(stalePrimaries))
	for i, stale := range stalePrimaries {
		problems = append(problems, types.Problem{
			Code:      types.ProblemStalePrimary,
			CheckName: "StalePrimary",
			PoolerID:  stale.PoolerID,
			ShardKey:  sa.ShardKey,
			Description: fmt.Sprintf("Stale primary detected: %s (stale_primary_term %d) is stale, most advanced primary %s (most_advanced_primary_term %d)",
				stale.PoolerID.Name,
				commonconsensus.PrimaryTerm(stale.ConsensusStatus),
				sa.HighestTermReachablePrimary.PoolerID.Name,
				commonconsensus.PrimaryTerm(sa.HighestTermReachablePrimary.ConsensusStatus)),
			Priority:       types.PriorityEmergency - types.Priority(i),
			Scope:          types.ScopeShard,
			DetectedAt:     time.Now(),
			RecoveryAction: a.factory.NewDemoteStalePrimaryAction(),
		})
	}
	return problems, nil
}
