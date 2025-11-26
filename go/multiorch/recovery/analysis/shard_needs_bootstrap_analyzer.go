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

	"github.com/multigres/multigres/go/multiorch/store"
)

// ShardNeedsBootstrapAnalyzer detects when all nodes in a shard are uninitialized.
// This is a per-pooler analyzer that returns a shard-wide problem.
// The recovery loop's filterAndPrioritize() will deduplicate multiple instances.
type ShardNeedsBootstrapAnalyzer struct{}

func (a *ShardNeedsBootstrapAnalyzer) Name() CheckName {
	return "ShardNeedsBootstrap"
}

func (a *ShardNeedsBootstrapAnalyzer) Analyze(poolerAnalysis *store.ReplicationAnalysis) []Problem {
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

		return []Problem{{
			Code:       ProblemShardNeedsBootstrap,
			CheckName:  "ShardNeedsBootstrap",
			PoolerID:   poolerAnalysis.PoolerID,
			Database:   poolerAnalysis.Database,
			TableGroup: poolerAnalysis.TableGroup,
			Shard:      poolerAnalysis.Shard,
			Description: fmt.Sprintf("Shard %s/%s/%s has no initialized nodes and needs bootstrap",
				poolerAnalysis.Database, poolerAnalysis.TableGroup, poolerAnalysis.Shard),
			Priority:       PriorityShardBootstrap,
			Scope:          ScopeShard,
			DetectedAt:     time.Now(),
			RecoveryAction: factory.NewBootstrapRecoveryAction(),
		}}
	}

	return nil
}
