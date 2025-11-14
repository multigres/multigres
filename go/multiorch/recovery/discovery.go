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

// refreshClusterMetadata queries the topology service for pooler updates.
func (re *Engine) refreshClusterMetadata() {
	startTime := time.Now()
	defer func() {
		latency := time.Since(startTime).Seconds()
		if re.refreshLatency != nil {
			re.refreshLatency.Record(re.ctx, latency)
		}
	}()

	re.logger.Debug("refreshing cluster metadata")

	// Create a timeout context for this refresh operation
	// Use the configured timeout, but respect parent context cancellation
	ctx, cancel := context.WithTimeout(re.ctx, re.config.GetClusterMetadataRefreshTimeout())
	defer cancel()

	// Get all cells
	cells, err := re.ts.GetCellNames(ctx)
	if err != nil {
		re.logger.Error("failed to get cell names", "error", err)
		return
	}

	// For each cell, query poolers matching our shard watch targets
	totalPoolers := 0
	for _, cell := range cells {
		// Check for context cancellation (e.g., shutdown in progress)
		select {
		case <-ctx.Done():
			re.logger.Info("cluster metadata refresh cancelled")
			return
		default:
		}

		cellPoolers := 0

		// Get current targets (protected by mutex)
		re.mu.Lock()
		targets := re.shardWatchTargets
		re.mu.Unlock()

		// Query poolers for each watch target
		for _, target := range targets {
			opt := &topo.GetMultiPoolersByCellOptions{
				DatabaseShard: &topo.DatabaseShard{
					Database:   target.Database,
					TableGroup: target.TableGroup, // empty = all tablegroups
					Shard:      target.Shard,      // empty = all shards
				},
			}

			poolers, err := re.ts.GetMultiPoolersByCell(ctx, cell, opt)
			if err != nil {
				re.logger.Error("failed to get poolers",
					"cell", cell,
					"target", target.String(),
					"error", err,
				)
				continue
			}

			// Store each pooler in the state store
			for _, pooler := range poolers {
				key := topo.MultiPoolerIDString(pooler.Id)

				// Check if we already know about this pooler
				if existing, ok := re.poolerStore.Get(key); ok {
					// Update the MultiPooler record in case topology changed
					// but preserve all timestamps and computed fields
					updated := &store.PoolerHealth{
						MultiPooler:         pooler.MultiPooler,
						LastCheckAttempted:  existing.LastCheckAttempted,
						LastCheckSuccessful: existing.LastCheckSuccessful,
						LastSeen:            existing.LastSeen,
						IsUpToDate:          existing.IsUpToDate,
						IsLastCheckValid:    existing.IsLastCheckValid,
					}
					re.poolerStore.Set(key, updated)
				} else {
					// New pooler - we've discovered it in the topology, but we haven't
					// performed a health check yet. The health check loop will update
					// LastSeen, LastCheckAttempted, LastCheckSuccessful, and IsUpToDate.
					poolerInfo := &store.PoolerHealth{
						MultiPooler: pooler.MultiPooler,
						IsUpToDate:  false, // Not yet health checked
					}
					re.poolerStore.Set(key, poolerInfo)
				}
			}

			cellPoolers += len(poolers)
		}

		totalPoolers += cellPoolers
		re.logger.Debug("discovered poolers", "cell", cell, "count", cellPoolers)
	}

	re.logger.Info("cluster metadata refresh complete",
		"cells", len(cells),
		"total_poolers", totalPoolers,
	)

	// Queue all discovered poolers for health checking
	re.queuePoolersHealthCheck()
}
