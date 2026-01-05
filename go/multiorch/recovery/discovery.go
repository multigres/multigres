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

package recovery

import (
	"context"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	commontypes "github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

// refreshClusterMetadata queries the topology service for pooler updates.
func (re *Engine) refreshClusterMetadata() {
	startTime := time.Now()
	defer func() {
		re.metrics.clusterMetadataRefreshDuration.Record(re.shutdownCtx, time.Since(startTime).Seconds())
	}()

	re.logger.Debug("refreshing cluster metadata")

	// Create a timeout context for this refresh operation
	// Use the configured timeout, but respect parent context cancellation
	ctx, cancel := context.WithTimeout(re.shutdownCtx, re.config.GetClusterMetadataRefreshTimeout())
	defer cancel()

	// Get all cells
	cells, err := re.ts.GetCellNames(ctx)
	if err != nil {
		re.logger.Error("failed to get cell names", "error", err)
		return
	}

	// Get current targets (protected by mutex)
	re.mu.Lock()
	targets := re.shardWatchTargets
	re.mu.Unlock()

	// Query poolers for each watch target
	totalPoolers := 0
	for _, target := range targets {
		// Check for context cancellation (e.g., shutdown in progress)
		select {
		case <-ctx.Done():
			re.logger.Info("cluster metadata refresh cancelled")
			return
		default:
		}

		count, err := re.refreshPoolersForTarget(ctx, target.Database, target.TableGroup, target.Shard, nil /* poolersToIgnore */)
		if err != nil {
			re.logger.Error("failed to refresh poolers for target",
				"target", target.String(),
				"error", err,
			)
			continue
		}

		totalPoolers += count
		re.logger.Debug("refreshed poolers for target", "target", target.String(), "count", count)
	}

	// Info level log is too spammy
	re.logger.Debug("cluster metadata refresh complete",
		"cells", len(cells),
		"total_poolers", totalPoolers,
	)
}

// refreshPoolersForTarget refreshes poolers matching a specific target from topology
// and updates the pooler store. This is the common implementation used by both
// the main discovery loop and shard-specific refresh operations.
//
// poolersToIgnore is a list of pooler IDs (e.g., "cell1/multipooler/pooler1") to skip.
// This is useful when a pooler is known to be dead/unreachable.
//
// Returns the total count of poolers discovered/updated.
func (re *Engine) refreshPoolersForTarget(ctx context.Context, database, tablegroup, shard string, poolersToIgnore []string) (int, error) {
	// Build ignore map for O(1) lookup
	ignoreMap := make(map[string]bool, len(poolersToIgnore))
	for _, id := range poolersToIgnore {
		ignoreMap[id] = true
	}

	// Create filter for topo query
	opt := &topoclient.GetMultiPoolersByCellOptions{
		DatabaseShard: &topoclient.DatabaseShard{
			Database:   database,
			TableGroup: tablegroup,
			Shard:      shard,
		},
	}

	// Get cells from topology
	cells, err := re.ts.GetCellNames(ctx)
	if err != nil {
		return 0, err
	}

	discoveredCount := 0
	for _, cell := range cells {
		// Get poolers for this cell, filtered by target at the topo level
		poolers, err := re.ts.GetMultiPoolersByCell(ctx, cell, opt)
		if err != nil {
			re.logger.WarnContext(ctx, "failed to get poolers for cell",
				"cell", cell,
				"database", database,
				"tablegroup", tablegroup,
				"shard", shard,
				"error", err,
			)
			continue
		}

		// Process each pooler
		for _, pooler := range poolers {
			if pooler == nil || pooler.MultiPooler == nil {
				continue
			}

			poolerID := topoclient.MultiPoolerIDString(pooler.Id)

			// Skip ignored poolers
			if ignoreMap[poolerID] {
				re.logger.DebugContext(ctx, "skipping ignored pooler",
					"pooler_id", poolerID,
				)
				continue
			}

			// Check if we already know about this pooler
			if existing, ok := re.poolerStore.Get(poolerID); ok {
				// Update the pooler metadata in case topology changed
				// but preserve all timestamps and computed fields.
				// ProtoStore.Set() clones on write, so we can mutate existing safely.
				existing.MultiPooler = pooler.MultiPooler
				re.poolerStore.Set(poolerID, existing)
			} else {
				// New pooler - we've discovered it in the topology, but we haven't
				// performed a health check yet. The health check loop will update
				// LastSeen, LastCheckAttempted, LastCheckSuccessful, and IsUpToDate.
				poolerInfo := &multiorchdatapb.PoolerHealthState{
					MultiPooler: pooler.MultiPooler,
					IsUpToDate:  false, // Not yet health checked
				}
				re.poolerStore.Set(poolerID, poolerInfo)

				// Queue health check for this newly discovered pooler
				re.healthCheckQueue.Push(poolerID)
			}

			discoveredCount++
		}
	}

	return discoveredCount, nil
}

// refreshShardMetadata refreshes cluster metadata for a specific shard only.
// This is used during recovery validation to get fresh data without scanning the entire cluster.
//
// poolersToIgnore is a list of pooler IDs (e.g., "cell1/multipooler/pooler1") to skip.
// This is useful when a pooler is known to be dead/unreachable.
func (re *Engine) refreshShardMetadata(ctx context.Context, shardKey commontypes.ShardKey, poolersToIgnore []string) error {
	re.logger.DebugContext(ctx, "refreshing shard metadata",
		"shard_key", shardKey.String(),
		"ignore_count", len(poolersToIgnore),
	)

	count, err := re.refreshPoolersForTarget(ctx, shardKey.Database, shardKey.TableGroup, shardKey.Shard, poolersToIgnore)
	if err != nil {
		return err
	}

	re.logger.DebugContext(ctx, "shard metadata refresh complete",
		"shard_key", shardKey.String(),
		"poolers_refreshed", count,
	)

	return nil
}

// forceHealthCheckShardPoolers forces a health check re-poll for all poolers in a shard.
// This is used after shard-wide recoveries to ensure all pooler state is up-to-date.
//
// poolersToIgnore is a list of pooler IDs to skip (e.g., a dead primary).
func (re *Engine) forceHealthCheckShardPoolers(ctx context.Context, shardKey commontypes.ShardKey, poolersToIgnore []string) {
	re.logger.DebugContext(ctx, "force refreshing all poolers in shard",
		"shard_key", shardKey.String(),
		"ignore_count", len(poolersToIgnore),
	)

	// Build ignore map for O(1) lookup
	ignoreMap := make(map[string]bool, len(poolersToIgnore))
	for _, id := range poolersToIgnore {
		ignoreMap[id] = true
	}

	// Collect poolers to poll (can't poll inside Range due to lock contention)
	type poolerToPoll struct {
		id     *clustermetadatapb.ID
		health *multiorchdatapb.PoolerHealthState
	}
	var poolersToPoll []poolerToPoll

	re.poolerStore.Range(func(poolerID string, poolerHealth *multiorchdatapb.PoolerHealthState) bool {
		if poolerHealth == nil || poolerHealth.MultiPooler == nil || poolerHealth.MultiPooler.Id == nil {
			return true
		}

		// Check if this pooler is in the target shard
		if poolerHealth.MultiPooler.Database != shardKey.Database ||
			poolerHealth.MultiPooler.TableGroup != shardKey.TableGroup ||
			poolerHealth.MultiPooler.Shard != shardKey.Shard {
			return true // continue
		}

		// Skip ignored poolers
		if ignoreMap[poolerID] {
			return true // continue
		}

		// Collect this pooler for polling (already a clone from Range)
		poolersToPoll = append(poolersToPoll, poolerToPoll{
			id:     poolerHealth.MultiPooler.Id,
			health: poolerHealth,
		})
		return true // continue
	})

	// Poll the collected poolers (outside the Range lock)
	polledCount := 0
	for _, p := range poolersToPoll {
		pollCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		re.pollPooler(pollCtx, p.id, p.health, true /* forceDiscovery */)
		cancel()
		polledCount++
	}

	re.logger.DebugContext(ctx, "shard pooler force refresh complete",
		"shard_key", shardKey.String(),
		"poolers_polled", polledCount,
	)
}
