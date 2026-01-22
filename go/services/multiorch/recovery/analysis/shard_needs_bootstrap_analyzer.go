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
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// ShardNeedsBootstrapAnalyzer detects when all nodes in a shard are uninitialized.
// This is a per-pooler analyzer that returns a shard-wide problem.
// The recovery loop's filterAndPrioritize() will deduplicate multiple instances.
type ShardNeedsBootstrapAnalyzer struct {
	factory *RecoveryActionFactory
}

func (a *ShardNeedsBootstrapAnalyzer) Name() types.CheckName {
	return "ShardNeedsBootstrap"
}

func (a *ShardNeedsBootstrapAnalyzer) Analyze(poolerAnalysis *store.ReplicationAnalysis) ([]types.Problem, error) {
	// Skip unreachable nodes - we can't determine their true initialization state.
	// An unreachable node might be perfectly initialized but just temporarily down.
	// PrimaryIsDead analyzer will handle dead primaries.
	if !poolerAnalysis.LastCheckValid {
		return nil, nil
	}

	// Skip primary nodes - they don't have a PrimaryPoolerID by design (they ARE the primary).
	// A dead primary should be handled by PrimaryIsDead (detected by replicas), not by
	// ShardNeedsBootstrap. When a primary's postgres dies, it appears as:
	// - IsPrimary = true (it's the primary)
	// - IsInitialized = false (postgres down, can't get LSN)
	// - PrimaryPoolerID = nil (it's the primary itself)
	// This would incorrectly trigger ShardNeedsBootstrap if we don't skip it.
	// Always skip primary nodes regardless of initialization state - if a primary's postgres
	// crashes, PrimaryIsDead will handle it (detected by replicas).
	if poolerAnalysis.IsPrimary {
		return nil, nil
	}

	// Skip if node has a data directory - it was initialized at some point.
	// HasDataDirectory (presence of PG_VERSION file) is the canonical signal for
	// "was ever initialized", regardless of pooler type.
	// This allows poolers to start as REPLICA type while still detecting
	// fresh poolers that need bootstrap.
	if poolerAnalysis.HasDataDirectory {
		return nil, nil
	}

	// Only analyze if this pooler is uninitialized
	if poolerAnalysis.IsInitialized {
		return nil, nil
	}

	// If this pooler is uninitialized AND there's no primary in the shard,
	// then the whole shard likely needs bootstrap
	if poolerAnalysis.PrimaryPoolerID == nil {
		if a.factory == nil {
			return nil, errors.New("recovery action factory not initialized")
		}

		return []types.Problem{{
			Code:           types.ProblemShardNeedsBootstrap,
			CheckName:      "ShardNeedsBootstrap",
			PoolerID:       poolerAnalysis.PoolerID,
			ShardKey:       poolerAnalysis.ShardKey,
			Description:    fmt.Sprintf("Shard %s has no initialized nodes and needs bootstrap", poolerAnalysis.ShardKey),
			Priority:       types.PriorityShardBootstrap,
			Scope:          types.ScopeShard,
			DetectedAt:     time.Now(),
			RecoveryAction: a.factory.NewBootstrapShardAction(),
		}}, nil
	}

	return nil, nil
}
