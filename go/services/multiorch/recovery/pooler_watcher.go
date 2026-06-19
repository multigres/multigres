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
)

const (
	// shutdownGracePeriod is how long the cache retains a ghost record of a
	// SHUTDOWN pooler so future etcd-cleanup logic can find it.
	shutdownGracePeriod = 4 * time.Hour
	// vanishedGracePeriod is how long an unexpectedly-deleted entry stays
	// visible to reads, so accidental etcd deletes can self-heal.
	vanishedGracePeriod = 4 * time.Hour
)

// PoolerWatcher wires the orchestrator's per-pooler health-stream callbacks
// to a topology cache. Its sole job is "start a health stream when a pooler
// goes live; stop it when the pooler goes away."
type PoolerWatcher struct {
	cache  *poolerwatch.PoolerCache[*multiorchdatapb.PoolerHealthState]
	logger *slog.Logger
}

// NewPoolerWatcher creates a new PoolerWatcher.
//
// onPoolerLive fires when a pooler enters the Live state — either first
// discovery or a restart after a prior SHUTDOWN.
//
// onPoolerGone is the single terminal callback. It fires when the cache
// stops tracking the pooler: lifecycle SHUTDOWN (immediate), NoNode after
// vanish grace, or explicit eviction (Forget). Callers typically use this
// to tear down per-pooler resources such as health streams.
func NewPoolerWatcher(
	ctx context.Context,
	topoStore topoclient.Store,
	targets func() []config.WatchTarget,
	onPoolerLive func(id *clustermetadatapb.ID),
	onPoolerGone func(id *clustermetadatapb.ID),
	logger *slog.Logger,
) *PoolerWatcher {
	pw := &PoolerWatcher{logger: logger}

	matchesAnyTarget := func(p *clustermetadatapb.MultiPooler) bool {
		for _, t := range targets() {
			if t.MatchesShard(p.GetShardKey().GetDatabase(), p.GetShardKey().GetTableGroup(), p.GetShardKey().GetShard()) {
				return true
			}
		}
		return false
	}

	pw.cache = poolerwatch.New(ctx, poolerwatch.Config[*multiorchdatapb.PoolerHealthState]{
		Source: topoStore,
		Filter: matchesAnyTarget,
		Hooks: poolerwatch.Hooks[*multiorchdatapb.PoolerHealthState]{
			OnLive: func(p *clustermetadatapb.MultiPooler, _ *multiorchdatapb.PoolerHealthState) *multiorchdatapb.PoolerHealthState {
				if onPoolerLive != nil {
					onPoolerLive(p.Id)
				}
				logger.Info("pooler discovered live",
					"pooler_id", topoclient.ComponentIDString(p.Id),
					"database", p.GetShardKey().GetDatabase(),
					"tablegroup", p.GetShardKey().GetTableGroup(),
					"shard", p.GetShardKey().GetShard(),
					"leader", p.GetSelfLeadership().GetLeaderId() != nil,
				)
				return &multiorchdatapb.PoolerHealthState{
					MultiPooler: p,
					IsUpToDate:  false,
				}
			},

			OnUpdate: func(_, curr *clustermetadatapb.MultiPooler, rider *multiorchdatapb.PoolerHealthState) {
				// Atomic pointer swap; safe to do outside the cache lock.
				rider.MultiPooler = curr
			},

			OnGone: func(p *clustermetadatapb.MultiPooler, _ *multiorchdatapb.PoolerHealthState, reason poolerwatch.GoneReason) {
				if onPoolerGone != nil {
					onPoolerGone(p.Id)
				}
				switch reason {
				case poolerwatch.GoneShutdown:
					logger.Info("pooler entered SHUTDOWN lifecycle", "pooler_id", topoclient.ComponentIDString(p.Id))
				case poolerwatch.GoneVanished:
					logger.Warn("pooler topology entry vanished after grace period", "pooler_id", topoclient.ComponentIDString(p.Id))
				case poolerwatch.GoneCacheShutdown:
					logger.Debug("pooler released because cache is shutting down", "pooler_id", topoclient.ComponentIDString(p.Id))
				}
			},
		},
		ShutdownGrace: shutdownGracePeriod,
		VanishedGrace: vanishedGracePeriod,
		Logger:        logger,
	})

	return pw
}

// Cache returns the underlying pooler cache. PoolerStore is built on it.
func (pw *PoolerWatcher) Cache() *poolerwatch.PoolerCache[*multiorchdatapb.PoolerHealthState] {
	return pw.cache
}

// Start launches the underlying cache (watch + sweeper).
func (pw *PoolerWatcher) Start() {
	pw.logger.Info("starting pooler watcher")
	pw.cache.Start()
}

// Stop shuts down the underlying cache.
func (pw *PoolerWatcher) Stop() {
	pw.cache.Shutdown()
	pw.logger.Info("pooler watcher stopped")
}

// Sync blocks until every event observed by the cache at the time of the
// call has been processed by this watcher's hooks.
func (pw *PoolerWatcher) Sync(ctx context.Context) error {
	return pw.cache.Sync(ctx)
}
