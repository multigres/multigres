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

	commontypes "github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

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
