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
	"log/slog"
	"time"

	"github.com/multigres/multigres/go/multiorch/store"
)

// ShardHasNoPrimaryAnalyzer detects when nodes are initialized but no primary exists.
// This is a per-pooler analyzer that returns a shard-wide problem.
// The recovery loop's filterAndPrioritize() will deduplicate multiple instances.
type ShardHasNoPrimaryAnalyzer struct{}

func (a *ShardHasNoPrimaryAnalyzer) Name() CheckName {
	return "ShardHasNoPrimary"
}

func (a *ShardHasNoPrimaryAnalyzer) Analyze(poolerAnalysis *store.ReplicationAnalysis) []Problem {
	// Only analyze replicas (primaries can't detect missing primary)
	if poolerAnalysis.IsPrimary {
		return nil
	}

	// If this replica is initialized but has no primary, the shard needs leader appointment
	if poolerAnalysis.IsInitialized && poolerAnalysis.PrimaryPoolerID == nil {
		return []Problem{{
			Code:       ProblemShardHasNoPrimary,
			CheckName:  "ShardHasNoPrimary",
			PoolerID:   poolerAnalysis.PoolerID,
			Database:   poolerAnalysis.Database,
			TableGroup: poolerAnalysis.TableGroup,
			Shard:      poolerAnalysis.Shard,
			Description: fmt.Sprintf("Shard %s/%s/%s has initialized nodes but no primary",
				poolerAnalysis.Database, poolerAnalysis.TableGroup, poolerAnalysis.Shard),
			Priority:       PriorityShardBootstrap,
			Scope:          ScopeShard,
			DetectedAt:     time.Now(),
			RecoveryAction: NewAppointLeaderRecoveryAction(slog.Default()),
		}}
	}

	return nil
}
