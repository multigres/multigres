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
	"slices"
	"time"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// StaleLeaderAnalyzer detects stale leaders that came back online after failover.
// This happens when an old primary restarts without being properly demoted.
//
// The analyzer operates at the shard level: when multiple leaders are detected,
// it reports all of them except the highest-term leader as stale. Problems are
// sorted most-stale-first with descending priorities so the recovery system addresses
// the most out-of-date primary first.
//
// Note: This is NOT true split-brain. True split-brain means both primaries can accept
// writes. In this scenario, the new primary cannot accept writes because it cannot
// recruit standbys while the stale leader exists.
type StaleLeaderAnalyzer struct {
	factory *RecoveryActionFactory
}

func (a *StaleLeaderAnalyzer) Name() types.CheckName {
	return "StaleLeader"
}

func (a *StaleLeaderAnalyzer) ProblemCode() types.ProblemCode {
	return types.ProblemStaleLeader
}

func (a *StaleLeaderAnalyzer) RecoveryAction() types.RecoveryAction {
	return a.factory.NewDemoteStaleLeaderAction()
}

func (a *StaleLeaderAnalyzer) Analyze(sa *ShardAnalysis) ([]types.Problem, error) {
	if a.factory == nil {
		return nil, errors.New("recovery action factory not initialized")
	}

	// Need multiple leaders to detect staleness.
	if len(sa.Leaders) <= 1 {
		return nil, nil
	}

	// A tie in LeaderTerm indicates a consensus bug — skip automatic demotion.
	if sa.HighestTermReachableLeader == nil {
		return nil, nil
	}

	// Collect stale leaders: every topology-PRIMARY pooler that is not the
	// highest-term leader is stale. This includes poolers whose own rule has
	// caught up (LeaderTerm == 0 because the rule now names a different
	// leader) — exactly the post-emergency-demotion state we need to repair.
	mostAdvancedIDStr := topoclient.MultiPoolerIDString(sa.HighestTermReachableLeader.PoolerID)
	var staleLeaders []*PoolerAnalysis
	for _, p := range sa.Leaders {
		if topoclient.MultiPoolerIDString(p.PoolerID) == mostAdvancedIDStr {
			continue
		}
		staleLeaders = append(staleLeaders, p)
	}

	if len(staleLeaders) == 0 {
		return nil, nil
	}

	// Sort most stale first (lowest rule coordinator term first) so the
	// recovery system processes the most out-of-date leader at highest
	// priority.
	slices.SortFunc(staleLeaders, compareLeaderTimeline)

	// Assign descending priorities so the most stale leader (sorted first)
	// gets PriorityEmergency, the next gets PriorityEmergency-1, etc.
	problems := make([]types.Problem, 0, len(staleLeaders))
	for i, stale := range staleLeaders {
		problems = append(problems, types.Problem{
			Code:      types.ProblemStaleLeader,
			CheckName: "StaleLeader",
			PoolerID:  stale.PoolerID,
			ShardKey:  sa.ShardKey,
			Description: fmt.Sprintf("Stale leader detected: %s (stale_leader_term %d) is stale, most advanced leader %s (most_advanced_leader_term %d)",
				stale.PoolerID.Name,
				commonconsensus.LeaderTerm(stale.ConsensusStatus),
				sa.HighestTermReachableLeader.PoolerID.Name,
				commonconsensus.LeaderTerm(sa.HighestTermReachableLeader.ConsensusStatus)),
			Priority:       types.PriorityEmergency - types.Priority(i),
			Scope:          types.ScopeShard,
			DetectedAt:     time.Now(),
			RecoveryAction: a.factory.NewDemoteStaleLeaderAction(),
		})
	}
	return problems, nil
}
