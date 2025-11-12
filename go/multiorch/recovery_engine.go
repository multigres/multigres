// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package multiorch

import (
	"context"
	"log/slog"
	"time"

	"github.com/multigres/multigres/go/clustermetadata/topo"
)

// RecoveryEngine orchestrates health checking and automated recovery for Multigres poolers.
//
// The RecoveryEngine provides high availability for Multigres
// by continuously monitoring pooler health and automatically
// recovering from failures.
//
// # Architecture
//
// The RecoveryEngine runs three main loops operating at different intervals:
//
//	┌──────────────────────────────────────────────────────────────────┐
//	│                        RecoveryEngine                            │
//	├──────────────────────────────────────────────────────────────────┤
//	│                                                                  │
//	│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐   │
//	│  │ Healthcheck Loop│  │  Recovery Loop  │  │ Maintenance Loop│   │
//	│  │  (TODO: 5s)     │  │   (TODO: 1s)    │  │  (every 1m/15s) │   │
//	│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘   │
//	│           │                    │                    │            │
//	│           └────────────────────┼────────────────────┘            │
//	│                                ▼                                 │
//	│                        (TODO: State Store)                       │
//	└──────────────────────────────────────────────────────────────────┘
//
// # Loop Details
//
// Maintenance Loop:
//
//   - Cluster Metadata Refresh:
//
//   - Queries all cells in the topology to find multipoolers that are part of the shards_to_watch
//
//   - Reads database information for the shards being watched (this will contain durability policy information)
//
//   - Bookkeeping Tasks:
//
//   - Forget unseen poolers (remove stale entries)
//
//   - Clean up stale data from the in memory state store.
//
// Healthcheck Loop:
//   - Poll each pooler for health status
//   - Update in-memory state store with current status
//
// Recovery Loop (TODO):
//   - Analyze pooler state for problems
//   - Execute recovery actions for detected issues
//   - Coordinate failovers via consensus protocol
//
// # Configuration
//
// The RecoveryEngine requires:
//   - shard_watch_targets: List of database/tablegroup/shard targets to monitor
//   - bookkeeping_interval: How often to run cleanup tasks (default: 1m)
//   - cluster_metadata_refresh_interval: How often to refresh from topology (default: 15s)
//
// Example:
//
//	engine := NewRecoveryEngine(
//	    "zone1",                          // cell
//	    topoStore,                        // topology service
//	    logger,                           // structured logger
//	    []string{"postgres"},             // watch entire database
//	    1*time.Minute,                    // bookkeeping interval
//	    15*time.Second,                   // metadata refresh interval
//	)
//	engine.Start()
type RecoveryEngine struct {
	cell              string
	ts                topo.Store
	logger            *slog.Logger
	shardWatchTargets []string

	bookkeepingInterval            time.Duration
	clusterMetadataRefreshInterval time.Duration

	// Context for shutting down loops
	ctx    context.Context
	cancel context.CancelFunc
}

// NewRecoveryEngine creates a new RecoveryEngine instance.
func NewRecoveryEngine(
	cell string,
	ts topo.Store,
	logger *slog.Logger,
	shardWatchTargets []string,
	bookkeepingInterval time.Duration,
	clusterMetadataRefreshInterval time.Duration,
) *RecoveryEngine {
	ctx, cancel := context.WithCancel(context.Background())
	return &RecoveryEngine{
		cell:                           cell,
		ts:                             ts,
		logger:                         logger,
		shardWatchTargets:              shardWatchTargets,
		bookkeepingInterval:            bookkeepingInterval,
		clusterMetadataRefreshInterval: clusterMetadataRefreshInterval,
		ctx:                            ctx,
		cancel:                         cancel,
	}
}

// Start initializes and starts the RecoveryEngine loops.
func (re *RecoveryEngine) Start() error {
	re.logger.Info("starting recovery engine",
		"cell", re.cell,
		"watch_targets", re.shardWatchTargets,
		"bookkeeping_interval", re.bookkeepingInterval,
		"cluster_metadata_refresh_interval", re.clusterMetadataRefreshInterval,
	)

	// Start maintenance loop (cluster metadata refresh + bookkeeping)
	go re.runMaintenanceLoop()

	re.logger.Info("recovery engine started successfully")
	return nil
}

// Stop gracefully shuts down the RecoveryEngine.
func (re *RecoveryEngine) Stop() {
	re.logger.Info("stopping recovery engine")
	re.cancel()
}

// runMaintenanceLoop runs the cluster metadata refresh and bookkeeping tasks.
func (re *RecoveryEngine) runMaintenanceLoop() {
	bookkeepingTicker := time.NewTicker(re.bookkeepingInterval)
	defer bookkeepingTicker.Stop()

	metadataTicker := time.NewTicker(re.clusterMetadataRefreshInterval)
	defer metadataTicker.Stop()

	re.logger.Info("maintenance loop started")

	// Do initial metadata refresh
	re.refreshClusterMetadata()

	for {
		select {
		case <-re.ctx.Done():
			re.logger.Info("maintenance loop stopped")
			return

		case <-metadataTicker.C:
			re.refreshClusterMetadata()

		case <-bookkeepingTicker.C:
			re.runBookkeeping()
		}
	}
}

// refreshClusterMetadata queries the topology service for pooler updates.
func (re *RecoveryEngine) refreshClusterMetadata() {
	re.logger.Debug("refreshing cluster metadata")

	ctx, cancel := context.WithTimeout(re.ctx, topo.RemoteOperationTimeout)
	defer cancel()

	// Get all cells
	cells, err := re.ts.GetCellNames(ctx)
	if err != nil {
		re.logger.Error("failed to get cell names", "error", err)
		return
	}

	// For each cell, count poolers (simple for now)
	totalPoolers := 0
	for _, cell := range cells {
		poolers, err := re.ts.GetMultiPoolersByCell(ctx, cell, nil)
		if err != nil {
			re.logger.Error("failed to get poolers", "cell", cell, "error", err)
			continue
		}
		totalPoolers += len(poolers)
		re.logger.Debug("discovered poolers", "cell", cell, "count", len(poolers))
	}

	re.logger.Info("cluster metadata refresh complete",
		"cells", len(cells),
		"total_poolers", totalPoolers,
	)
}

// runBookkeeping performs periodic bookkeeping tasks.
func (re *RecoveryEngine) runBookkeeping() {
	re.logger.Debug("running bookkeeping tasks")
	// TODO: Add actual bookkeeping tasks in future PRs
	// - Forget unseen poolers
	// - Expire old recovery history
	// - Clean up stale data
}
