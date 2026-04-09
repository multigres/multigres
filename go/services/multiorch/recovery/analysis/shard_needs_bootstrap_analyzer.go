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

// ShardNeedsBootstrapAnalyzer detects when all nodes in a shard are uninitialized.
// It operates at the shard level: if any reachable replica has no data directory, is
// uninitialized, and reports no primary in topology, one shard-scoped problem
// (PoolerID nil) is emitted.
type ShardNeedsBootstrapAnalyzer struct {
	factory *RecoveryActionFactory
}

func (a *ShardNeedsBootstrapAnalyzer) Name() types.CheckName {
	return "ShardNeedsBootstrap"
}

func (a *ShardNeedsBootstrapAnalyzer) ProblemCode() types.ProblemCode {
	return types.ProblemShardNeedsBootstrap
}

func (a *ShardNeedsBootstrapAnalyzer) RecoveryAction() types.RecoveryAction {
	return a.factory.NewBootstrapShardAction()
}

func (a *ShardNeedsBootstrapAnalyzer) Analyze(sa *ShardAnalysis) ([]types.Problem, error) {
	if a.factory == nil {
		return nil, errors.New("recovery action factory not initialized")
	}

	for _, pa := range sa.Analyses {
		// Skip unreachable nodes - we can't determine their true initialization state.
		// An unreachable node might be perfectly initialized but just temporarily down.
		// PrimaryIsDead analyzer will handle dead primaries.
		if !pa.LastCheckValid {
			continue
		}

		// Skip primary nodes. A dead primary should be handled by PrimaryIsDead
		// (detected via sa.HighestTermDiscoveredPrimaryID + sa.PrimaryReachable), not by
		// ShardNeedsBootstrap. When a primary's postgres dies it appears as
		// IsPrimary=true, IsInitialized=false — skipping it avoids a false positive.
		if pa.IsPrimary {
			continue
		}

		// Skip if node has a data directory - it was initialized at some point.
		// HasDataDirectory (presence of PG_VERSION file) is the canonical signal for
		// "was ever initialized", regardless of pooler type.
		// This allows poolers to start as REPLICA type while still detecting
		// fresh poolers that need bootstrap.
		if pa.HasDataDirectory {
			continue
		}

		// Skip if initialized.
		if pa.IsInitialized {
			continue
		}

		// Only trigger if no primary exists in the shard topology.
		if sa.HighestTermDiscoveredPrimaryID != nil {
			continue
		}

		// Uninitialized replica with no primary — emit one shard-level problem.
		return []types.Problem{{
			Code:           types.ProblemShardNeedsBootstrap,
			CheckName:      "ShardNeedsBootstrap",
			PoolerID:       nil,
			ShardKey:       sa.ShardKey,
			Description:    fmt.Sprintf("Shard %s has no initialized nodes and needs bootstrap", sa.ShardKey),
			Priority:       types.PriorityShardBootstrap,
			Scope:          types.ScopeShard,
			DetectedAt:     time.Now(),
			RecoveryAction: a.factory.NewBootstrapShardAction(),
		}}, nil
	}

	return nil, nil
}
