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

package actions

import (
	"context"
	"log/slog"

	"github.com/multigres/multigres/go/common/rpcclient"

	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// InitializationScenario represents the different ways a shard can be initialized
type InitializationScenario int

const (
	// ScenarioUnknown indicates we couldn't determine the scenario
	ScenarioUnknown InitializationScenario = iota

	// ScenarioBootstrap: All nodes are empty (no data directories or no initialized schemas)
	// Action: Initialize one node as primary, then initialize others as standbys
	ScenarioBootstrap

	// ScenarioRepair: Some nodes have data but no current primary
	// Action: Use coordinator.AppointLeader to elect the most advanced node
	ScenarioRepair

	// ScenarioReelect: All nodes are initialized but no current primary
	// Action: Use coordinator.AppointLeader to re-elect a leader
	ScenarioReelect
)

// String returns the string representation of the scenario
func (s InitializationScenario) String() string {
	switch s {
	case ScenarioBootstrap:
		return "Bootstrap"
	case ScenarioRepair:
		return "Repair"
	case ScenarioReelect:
		return "Reelect"
	default:
		return "Unknown"
	}
}

// GatherInitializationStatus queries all nodes for their initialization status.
// This is a helper function that multiorch can use to collect node status
// before determining which initialization action to use.
func GatherInitializationStatus(ctx context.Context, rpcClient rpcclient.MultiPoolerClient, cohort []*multiorchdatapb.PoolerHealthState, logger *slog.Logger) ([]*multipoolermanagerdatapb.InitializationStatusResponse, error) {
	statuses := make([]*multipoolermanagerdatapb.InitializationStatusResponse, len(cohort))

	for i, pooler := range cohort {
		req := &multipoolermanagerdatapb.InitializationStatusRequest{}
		status, err := rpcClient.InitializationStatus(ctx, pooler.MultiPooler, req)
		if err != nil {
			if logger != nil {
				logger.WarnContext(ctx, "Failed to get initialization status from node",
					"node", pooler.MultiPooler.Id.Name,
					"error", err)
			}
			// Continue with nil status - we'll handle missing statuses in DetermineScenario
			statuses[i] = nil
			continue
		}
		statuses[i] = status
	}

	return statuses, nil
}

// DetermineScenario analyzes initialization statuses to determine the appropriate scenario.
// This is a helper function that multiorch can use to decide which initialization
// action to call (Bootstrap, Repair, or Reelect).
func DetermineScenario(statuses []*multipoolermanagerdatapb.InitializationStatusResponse, logger *slog.Logger) InitializationScenario {
	var initializedCount int
	var emptyCount int
	var unavailableCount int

	for _, status := range statuses {
		if status == nil {
			unavailableCount++
			continue
		}

		if status.IsInitialized {
			initializedCount++
		} else {
			emptyCount++
		}
	}

	if logger != nil {
		logger.Debug("Analyzing initialization statuses",
			"initialized", initializedCount,
			"empty", emptyCount,
			"unavailable", unavailableCount)
	}

	// All nodes are empty (or unavailable) → Bootstrap
	if initializedCount == 0 && emptyCount > 0 {
		return ScenarioBootstrap
	}

	// All reachable nodes are initialized → Reelect
	if initializedCount > 0 && emptyCount == 0 {
		return ScenarioReelect
	}

	// Mix of initialized and empty nodes → Repair
	if initializedCount > 0 && emptyCount > 0 {
		return ScenarioRepair
	}

	// All nodes unavailable or unknown state
	return ScenarioUnknown
}
