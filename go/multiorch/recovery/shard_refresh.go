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

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/multiorch/store"
)

// RefreshShardMetadata refreshes cluster metadata for a specific shard only.
// This is used during recovery validation to get fresh data without scanning the entire cluster.
//
// poolersToIgnore is a list of pooler IDs (e.g., "cell1/multipooler/pooler1") to skip.
// This is useful when a pooler is known to be dead/unreachable.
func (re *Engine) RefreshShardMetadata(ctx context.Context, database, tablegroup, shard string, poolersToIgnore []string) error {
	re.logger.DebugContext(ctx, "refreshing shard metadata",
		"database", database,
		"tablegroup", tablegroup,
		"shard", shard,
		"ignore_count", len(poolersToIgnore),
	)

	// Build ignore map for O(1) lookup
	ignoreMap := make(map[string]bool, len(poolersToIgnore))
	for _, id := range poolersToIgnore {
		ignoreMap[id] = true
	}

	// Create filter to only get poolers for this specific shard from topo
	filter := &topo.GetMultiPoolersByCellOptions{
		DatabaseShard: &topo.DatabaseShard{
			Database:   database,
			TableGroup: tablegroup,
			Shard:      shard,
		},
	}

	// Get cells from topology
	cells, err := re.ts.GetCellNames(ctx)
	if err != nil {
		return err
	}

	discoveredCount := 0
	for _, cell := range cells {
		// Get poolers for this cell, filtered by shard at the topo level
		poolers, err := re.ts.GetMultiPoolersByCell(ctx, cell, filter)
		if err != nil {
			re.logger.WarnContext(ctx, "failed to get poolers for cell during shard refresh",
				"cell", cell,
				"error", err,
			)
			continue
		}

		// All poolers returned are already filtered to our shard by topo
		for _, pooler := range poolers {
			if pooler == nil || pooler.MultiPooler == nil {
				continue
			}

			poolerID := topo.MultiPoolerIDString(pooler.Id)

			// Skip ignored poolers
			if ignoreMap[poolerID] {
				re.logger.DebugContext(ctx, "skipping ignored pooler",
					"pooler_id", poolerID,
				)
				continue
			}

			// Add to store (this ensures we have the latest topo data)
			// Check if pooler already exists to preserve health check data
			if existing, ok := re.poolerStore.Get(poolerID); ok {
				updated := &store.PoolerHealth{
					MultiPooler:         pooler.MultiPooler,
					LastCheckAttempted:  existing.LastCheckAttempted,
					LastCheckSuccessful: existing.LastCheckSuccessful,
					LastSeen:            existing.LastSeen,
					IsUpToDate:          existing.IsUpToDate,
					IsLastCheckValid:    existing.IsLastCheckValid,
				}
				re.poolerStore.Set(poolerID, updated)
			} else {
				// New pooler discovered during shard refresh
				poolerInfo := &store.PoolerHealth{
					MultiPooler: pooler.MultiPooler,
					IsUpToDate:  false, // Not yet health checked
				}
				re.poolerStore.Set(poolerID, poolerInfo)
			}
			discoveredCount++
		}
	}

	re.logger.DebugContext(ctx, "shard metadata refresh complete",
		"database", database,
		"tablegroup", tablegroup,
		"shard", shard,
		"poolers_refreshed", discoveredCount,
	)

	return nil
}

// ForceRefreshShardPoolers forces a health check re-poll for all poolers in a shard.
// This is used after cluster-wide recoveries to ensure all pooler state is up-to-date.
//
// poolersToIgnore is a list of pooler IDs to skip (e.g., a dead primary).
func (re *Engine) ForceRefreshShardPoolers(ctx context.Context, database, tablegroup, shard string, poolersToIgnore []string) {
	re.logger.DebugContext(ctx, "force refreshing all poolers in shard",
		"database", database,
		"tablegroup", tablegroup,
		"shard", shard,
		"ignore_count", len(poolersToIgnore),
	)

	// Build ignore map for O(1) lookup
	ignoreMap := make(map[string]bool, len(poolersToIgnore))
	for _, id := range poolersToIgnore {
		ignoreMap[id] = true
	}

	polledCount := 0

	// Iterate over store to find poolers in this shard
	re.poolerStore.Range(func(poolerID string, poolerHealth *store.PoolerHealth) bool {
		if poolerHealth == nil || poolerHealth.MultiPooler == nil {
			return true
		}

		mp := poolerHealth.MultiPooler

		// Check if this pooler is in the target shard
		if mp.Database != database ||
			mp.TableGroup != tablegroup ||
			mp.Shard != shard {
			return true // continue
		}

		// Skip ignored poolers
		if ignoreMap[poolerID] {
			return true // continue
		}

		// Force poll this pooler
		pollCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		re.pollPooler(pollCtx, mp.Id, poolerHealth, true /* forceDiscovery */)
		cancel()

		polledCount++
		return true // continue
	})

	re.logger.DebugContext(ctx, "shard pooler force refresh complete",
		"database", database,
		"tablegroup", tablegroup,
		"shard", shard,
		"poolers_polled", polledCount,
	)
}
