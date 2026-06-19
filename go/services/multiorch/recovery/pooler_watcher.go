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

// newPoolerCache builds the orchestrator's pooler cache. The cache is
// lifecycle-aware and dispatches start/stop callbacks for per-pooler health
// streams as poolers enter and leave the topology.
//
// onPoolerLive fires when a pooler enters the Live state — either first
// discovery or a restart after a prior SHUTDOWN.
//
// onPoolerGone is the single terminal callback. It fires when the cache
// stops tracking the pooler: lifecycle SHUTDOWN (immediate), NoNode after
// vanish grace, or cache shutdown. Callers typically use this to tear down
// per-pooler resources such as health streams.
//
// TODO: collapse the HealthStream registry into the rider. Today,
// health_stream.go keeps its own ID→streamEntry map in parallel with this
// cache. If PoolerHealthState owned the per-pooler stream state directly,
// OnLive could spawn the stream and stash it on the rider, OnGone could
// cancel via the rider, and the separate registry (plus the chicken-and-egg
// healthStream/cache closure in engine.go) would disappear.
func newPoolerCache(
	ctx context.Context,
	topoStore topoclient.Store,
	targets func() []config.WatchTarget,
	onPoolerLive func(id *clustermetadatapb.ID),
	onPoolerGone func(id *clustermetadatapb.ID),
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
		Source: topoStore,
		Filter: matchesAnyTarget,
		Hooks: poolerwatch.Hooks[*store.Pooler]{
			OnLive: func(p *clustermetadatapb.MultiPooler, _ *store.Pooler) *store.Pooler {
				if onPoolerLive != nil {
					onPoolerLive(p.Id)
				}
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
				}
			},

			OnUpdate: func(_, curr *clustermetadatapb.MultiPooler, rider *store.Pooler) {
				// Atomic pointer swap; safe to do outside the cache lock.
				rider.MultiPooler = curr
			},

			OnGone: func(p *clustermetadatapb.MultiPooler, _ *store.Pooler, reason poolerwatch.GoneReason) {
				if onPoolerGone != nil {
					onPoolerGone(p.Id)
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
		},
		ShutdownGrace: shutdownGracePeriod,
		VanishedGrace: vanishedGracePeriod,
		Logger:        logger,
	})
}
