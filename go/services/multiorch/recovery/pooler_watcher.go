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

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// PoolerWatcher watches the topology cache for changes and keeps the pooler store
// up-to-date. It uses topoclient.PoolerCache internally, which provides automatic
// reconnect handling, proto.Equal suppression for spurious updates on reconnect,
// and reconciliation when a cell's snapshot changes.
//
// When a pooler event arrives, it is filtered in-memory against the engine's
// WatchTargets before the pooler store is updated.
type PoolerWatcher struct {
	cache   *topoclient.PoolerCache
	targets func() []config.WatchTarget // live accessor, same as Engine.shardWatchTargets
	store   *store.PoolerStore
	queue   *Queue
	logger  *slog.Logger

	unsub func()
}

// NewPoolerWatcher creates a new PoolerWatcher.
// targets is a function that returns the current WatchTargets (consulted on every event).
func NewPoolerWatcher(
	ctx context.Context,
	topoStore topoclient.Store,
	targets func() []config.WatchTarget,
	poolerStore *store.PoolerStore,
	queue *Queue,
	logger *slog.Logger,
) *PoolerWatcher {
	return &PoolerWatcher{
		cache:   topoclient.NewPoolerCache(ctx, topoStore, logger),
		targets: targets,
		store:   poolerStore,
		queue:   queue,
		logger:  logger,
	}
}

// Start subscribes to the cache and begins watching for topology changes.
// Existing poolers in the cache are replayed synchronously before Start returns.
func (pw *PoolerWatcher) Start() {
	pw.logger.Info("starting pooler watcher")
	pw.unsub = pw.cache.Subscribe(pw.onCacheChange)
	pw.cache.Start()
}

// Stop unsubscribes and waits for all background goroutines to finish.
func (pw *PoolerWatcher) Stop() {
	pw.unsub()
	pw.cache.Stop()
	pw.logger.Info("pooler watcher stopped")
}

// onCacheChange handles a change notification from the topology cache.
func (pw *PoolerWatcher) onCacheChange(pooler *clustermetadatapb.MultiPooler, removed bool) {
	if removed {
		// Deletions are not removed from the store; bookkeeping handles stale entry removal.
		pw.logger.Debug("pooler deleted from topology", "pooler_id", topoclient.MultiPoolerIDString(pooler.Id))
		return
	}
	pw.onPoolerUpserted(pooler)
}

// onPoolerUpserted handles a pooler add or update event.
func (pw *PoolerWatcher) onPoolerUpserted(pooler *clustermetadatapb.MultiPooler) {
	if !pw.matchesAnyTarget(pooler) {
		return
	}

	poolerID := topoclient.MultiPoolerIDString(pooler.Id)

	if existing, ok := pw.store.Get(poolerID); ok {
		// Update the MultiPooler metadata but preserve all health-check timestamps.
		existing.MultiPooler = pooler
		pw.store.Set(poolerID, existing)
		pw.logger.Debug("pooler metadata updated from topology", "pooler_id", poolerID)
	} else {
		// New pooler — add to store and queue for immediate health check.
		pw.store.Set(poolerID, &multiorchdatapb.PoolerHealthState{
			MultiPooler: pooler,
			IsUpToDate:  false,
		})
		pw.queue.Push(poolerID)
		pw.logger.Info("new pooler discovered via watcher",
			"pooler_id", poolerID,
			"database", pooler.Database,
			"tablegroup", pooler.TableGroup,
			"shard", pooler.Shard,
			"type", pooler.Type.String(),
		)
	}
}

// matchesAnyTarget returns true if the pooler matches at least one of the
// configured WatchTargets.
func (pw *PoolerWatcher) matchesAnyTarget(pooler *clustermetadatapb.MultiPooler) bool {
	for _, target := range pw.targets() {
		if target.MatchesShard(pooler.Database, pooler.TableGroup, pooler.Shard) {
			return true
		}
	}
	return false
}
