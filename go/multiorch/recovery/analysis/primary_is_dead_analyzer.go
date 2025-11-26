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

// PrimaryIsDeadAnalyzer detects when a primary exists in topology but is unhealthy/unreachable.
// This differs from ShardHasNoPrimaryAnalyzer which detects when NO primary exists in topology.
// This is a per-pooler analyzer that returns a shard-wide problem.
// The recovery loop's filterAndPrioritize() will deduplicate multiple instances.
type PrimaryIsDeadAnalyzer struct{}

func (a *PrimaryIsDeadAnalyzer) Name() CheckName {
	return "PrimaryIsDead"
}

func (a *PrimaryIsDeadAnalyzer) Analyze(poolerAnalysis *store.ReplicationAnalysis) []Problem {
	// Only analyze replicas (primaries can't report themselves as dead)
	if poolerAnalysis.IsPrimary {
		return nil
	}

	// Skip if replica is not initialized (ShardNeedsBootstrap handles that)
	if !poolerAnalysis.IsInitialized {
		return nil
	}

	// Only trigger if:
	// 1. A primary exists in topology (PrimaryPoolerID is set)
	// 2. BUT the primary is unreachable (PrimaryReachable is false)
	//
	// This is the key difference from ShardHasNoPrimary which checks PrimaryPoolerID == nil
	if poolerAnalysis.PrimaryPoolerID != nil && !poolerAnalysis.PrimaryReachable {
		factory := GetRecoveryActionFactory()
		if factory == nil {
			// Factory not initialized yet, skip recovery action
			return nil
		}

		return []Problem{{
			Code:       ProblemPrimaryIsDead,
			CheckName:  "PrimaryIsDead",
			PoolerID:   poolerAnalysis.PoolerID,
			Database:   poolerAnalysis.Database,
			TableGroup: poolerAnalysis.TableGroup,
			Shard:      poolerAnalysis.Shard,
			Description: fmt.Sprintf("Primary for shard %s/%s/%s is dead/unreachable",
				poolerAnalysis.Database, poolerAnalysis.TableGroup, poolerAnalysis.Shard),
			Priority:       PriorityEmergency,
			Scope:          ScopeShard,
			DetectedAt:     time.Now(),
			RecoveryAction: factory.NewAppointLeaderRecoveryAction(),
		}}
	}

	return nil
}
