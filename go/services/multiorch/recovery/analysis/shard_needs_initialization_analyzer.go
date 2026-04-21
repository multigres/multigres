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

	"github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// ShardNeedsInitializationAnalyzer detects Phase 2 of shard bootstrap: the shard has
// been initialized (first backup created and a 0-member cohort record written) but
// its initial cohort has not yet been established by multiorch.
//
// Fires when the shard has at least one reachable, initialized pooler, no primary,
// and no pooler in the shard has cohort members. This is a shard-level problem:
// ShardInitAction needs to see all initialized poolers to form a quorum.
type ShardNeedsInitializationAnalyzer struct {
	factory *RecoveryActionFactory
}

func (a *ShardNeedsInitializationAnalyzer) Name() types.CheckName {
	return "ShardNeedsInitialization"
}

func (a *ShardNeedsInitializationAnalyzer) ProblemCode() types.ProblemCode {
	return types.ProblemShardNeedsInitialization
}

func (a *ShardNeedsInitializationAnalyzer) RecoveryAction() types.RecoveryAction {
	return a.factory.NewShardInitAction()
}

func (a *ShardNeedsInitializationAnalyzer) Analyze(sa *ShardAnalysis) ([]types.Problem, error) {
	if a.factory == nil {
		return nil, errors.New("recovery action factory not initialized")
	}

	// Without a policy we can't determine the quorum requirement — skip.
	if sa.BootstrapDurabilityPolicy == nil {
		return nil, nil
	}

	for _, pa := range sa.Analyses {
		// If any pooler has cohort members, the cohort is already established.
		if len(pa.CohortMembers) > 0 {
			return nil, nil
		}
	}

	// Not enough initialized poolers to satisfy the durability policy yet.
	if !consensus.IsDurabilityPolicyAchievable(sa.BootstrapDurabilityPolicy, sa.NumInitialized) {
		return nil, nil
	}

	return []types.Problem{{
		Code:           types.ProblemShardNeedsInitialization,
		CheckName:      "ShardNeedsInitialization",
		ShardKey:       sa.ShardKey,
		Description:    fmt.Sprintf("Shard %s needs initialization", sa.ShardKey),
		Priority:       types.PriorityShardBootstrap,
		Scope:          types.ScopeShard,
		DetectedAt:     time.Now(),
		RecoveryAction: a.factory.NewShardInitAction(),
	}}, nil
}
