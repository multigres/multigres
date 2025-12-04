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
	"fmt"
	"time"

	"github.com/multigres/multigres/go/multiorch/recovery/types"
	"github.com/multigres/multigres/go/multiorch/store"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// ShardNeedsBootstrapAnalyzer detects when all nodes in a shard are uninitialized.
// This is a per-pooler analyzer that returns a shard-wide problem.
// The recovery loop's filterAndPrioritize() will deduplicate multiple instances.
type ShardNeedsBootstrapAnalyzer struct{}

func (a *ShardNeedsBootstrapAnalyzer) Name() types.CheckName {
	return "ShardNeedsBootstrap"
}

func (a *ShardNeedsBootstrapAnalyzer) Analyze(poolerAnalysis *store.ReplicationAnalysis) []types.Problem {
	// Skip unreachable nodes - we can't determine their true initialization state.
	// An unreachable node might be perfectly initialized but just temporarily down.
	// Other analyzers (PrimaryIsDead, ShardHasNoPrimary) will handle dead primaries.
	if poolerAnalysis.IsUnreachable {
		return nil
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
		return nil
	}

	// Skip if node is registered as a REPLICA - it was initialized at some point.
	// Even if IsInitialized is false (e.g., due to failed health check when primary died),
	// a node that was ever a REPLICA should not trigger bootstrap.
	if poolerAnalysis.PoolerType == clustermetadatapb.PoolerType_REPLICA {
		return nil
	}

	// Only analyze if this pooler is uninitialized
	if poolerAnalysis.IsInitialized {
		return nil
	}

	// If this pooler is uninitialized AND there's no primary in the shard,
	// then the whole shard likely needs bootstrap
	if poolerAnalysis.PrimaryPoolerID == nil {
		factory := GetRecoveryActionFactory()
		if factory == nil {
			// Factory not initialized yet, skip recovery action
			return nil
		}

		return []types.Problem{{
			Code:       types.ProblemShardNeedsBootstrap,
			CheckName:  "ShardNeedsBootstrap",
			PoolerID:   poolerAnalysis.PoolerID,
			Database:   poolerAnalysis.Database,
			TableGroup: poolerAnalysis.TableGroup,
			Shard:      poolerAnalysis.Shard,
			Description: fmt.Sprintf("Shard %s/%s/%s has no initialized nodes and needs bootstrap",
				poolerAnalysis.Database, poolerAnalysis.TableGroup, poolerAnalysis.Shard),
			Priority:       types.PriorityShardBootstrap,
			Scope:          types.ScopeShard,
			DetectedAt:     time.Now(),
			RecoveryAction: factory.NewBootstrapShardAction(),
		}}
	}

	return nil
}
