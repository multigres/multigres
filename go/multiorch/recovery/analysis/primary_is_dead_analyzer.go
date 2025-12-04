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

	"github.com/multigres/multigres/go/multiorch/recovery/types"
	"github.com/multigres/multigres/go/multiorch/store"
)

// PrimaryIsDeadAnalyzer detects when a primary exists in topology but is unhealthy/unreachable.
// This is a per-pooler analyzer that returns a shard-wide problem.
// The recovery loop's filterAndPrioritize() will deduplicate multiple instances.
type PrimaryIsDeadAnalyzer struct{}

func (a *PrimaryIsDeadAnalyzer) Name() types.CheckName {
	return "PrimaryIsDead"
}

func (a *PrimaryIsDeadAnalyzer) Analyze(poolerAnalysis *store.ReplicationAnalysis) ([]types.Problem, error) {
	// Only analyze replicas (primaries can't report themselves as dead)
	if poolerAnalysis.IsPrimary {
		return nil, nil
	}

	// Skip if replica is not initialized (ShardNeedsBootstrap handles that)
	if !poolerAnalysis.IsInitialized {
		return nil, nil
	}

	// Early return if primary is reachable - no problem to report
	if poolerAnalysis.PrimaryPoolerID != nil && poolerAnalysis.PrimaryReachable {
		return nil, nil
	}

	// At this point:
	// - This is an initialized replica
	// - Either no primary exists (PrimaryPoolerID == nil) or primary is unreachable
	// Only trigger if a primary exists but is unreachable.
	if poolerAnalysis.PrimaryPoolerID == nil {
		return nil, nil
	}

	factory := GetRecoveryActionFactory()
	if factory == nil {
		return nil, errors.New("recovery action factory not initialized")
	}

	return []types.Problem{{
		Code:           types.ProblemPrimaryIsDead,
		CheckName:      "PrimaryIsDead",
		PoolerID:       poolerAnalysis.PoolerID,
		ShardKey:       poolerAnalysis.ShardKey,
		Description:    fmt.Sprintf("Primary for shard %s is dead/unreachable", poolerAnalysis.ShardKey),
		Priority:       types.PriorityEmergency,
		Scope:          types.ScopeShard,
		DetectedAt:     time.Now(),
		RecoveryAction: factory.NewAppointLeaderAction(),
	}}, nil
}
