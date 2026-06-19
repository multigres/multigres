// Copyright 2026 Supabase, Inc.
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
	"log/slog"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/poolerwatch"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

const (
	// shutdownGracePeriod is how long the cache retains a ghost record of a
	// SHUTDOWN pooler so future etcd-cleanup logic can find it.
	shutdownGracePeriod = 4 * time.Hour
	// vanishedGracePeriod is how long an unexpectedly-deleted entry stays
	// visible to reads, so accidental etcd deletes can self-heal.
	vanishedGracePeriod = 4 * time.Hour
)

// newPoolerCache builds the orchestrator's pooler cache (without binding
// hooks — the caller passes hooks to cache.Start).
func newPoolerCache(
	ctx context.Context,
	topoStore topoclient.Store,
	targets func() []config.WatchTarget,
	logger *slog.Logger,
) *store.PoolerCache {
	matchesAnyTarget := func(p *clustermetadatapb.MultiPooler) bool {
		for _, t := range targets() {
			if t.MatchesShard(p.GetShardKey().GetDatabase(), p.GetShardKey().GetTableGroup(), p.GetShardKey().GetShard()) {
				return true
			}
		}
		return false
	}

	return poolerwatch.New(ctx, poolerwatch.Config[*store.Pooler]{
		Source:        topoStore,
		Filter:        matchesAnyTarget,
		ShutdownGrace: shutdownGracePeriod,
		VanishedGrace: vanishedGracePeriod,
		Logger:        logger,
	})
}

// poolerCacheHooks builds the hook set for the orchestrator's pooler
// cache. Bound at cache.Start (when both the cache and the
// HealthStream-stream-spawner are fully constructed).
//
// OnLive spawns the per-pooler health stream via HealthStream.spawnStream
// and stashes the handle on the rider; OnGone cancels via the handle. No
// parallel registry — the cache is the single source of truth for
// "everything we track about this pooler".
func poolerCacheHooks(ctx context.Context, cache *store.PoolerCache, streams *HealthStream, logger *slog.Logger) poolerwatch.Hooks[*store.Pooler] {
	return poolerwatch.Hooks[*store.Pooler]{
		OnLive: func(p *clustermetadatapb.MultiPooler, _ *store.Pooler) *store.Pooler {
			logger.InfoContext(ctx, "pooler discovered live",
				"pooler_id", topoclient.ComponentIDString(p.Id),
				"database", p.GetShardKey().GetDatabase(),
				"tablegroup", p.GetShardKey().GetTableGroup(),
				"shard", p.GetShardKey().GetShard(),
				"leader", p.GetSelfLeadership().GetLeaderId() != nil,
			)
			return &store.Pooler{
				PoolerHealthState: &multiorchdatapb.PoolerHealthState{
					MultiPooler: p,
					IsUpToDate:  false,
				},
				Stream: streams.spawnStream(cache, topoclient.ComponentIDString(p.Id)),
			}
		},

		OnUpdate: func(_, curr *clustermetadatapb.MultiPooler, rider *store.Pooler) {
			// Atomic pointer swap; safe to do outside the cache lock.
			rider.MultiPooler = curr
		},

		OnGone: func(p *clustermetadatapb.MultiPooler, rider *store.Pooler, reason poolerwatch.GoneReason) {
			if rider.Stream != nil {
				rider.Stream.Cancel()
			}
			switch reason {
			case poolerwatch.GoneShutdown:
				logger.InfoContext(ctx, "pooler entered SHUTDOWN lifecycle", "pooler_id", topoclient.ComponentIDString(p.Id))
			case poolerwatch.GoneVanished:
				logger.WarnContext(ctx, "pooler topology entry vanished after grace period", "pooler_id", topoclient.ComponentIDString(p.Id))
			case poolerwatch.GoneCacheShutdown:
				logger.DebugContext(ctx, "pooler released because cache is shutting down", "pooler_id", topoclient.ComponentIDString(p.Id))
			}
		},
	}
}
