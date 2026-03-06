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
	"sync"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// PoolerWatcher watches etcd for topology changes and keeps the pooler store
// up-to-date. It uses WatchAllPoolersWithRetry to automatically discover cells
// and watch poolers within each cell.
//
// When a pooler event arrives, it is filtered in-memory against the engine's
// WatchTargets before the pooler store is updated.
type PoolerWatcher struct {
	topoStore topoclient.Store
	targets   func() []config.WatchTarget // live accessor, same as Engine.shardWatchTargets
	store     *store.PoolerHealthStore
	queue     *Queue
	logger    *slog.Logger

	// Control
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewPoolerWatcher creates a new PoolerWatcher.
// targets is a function that returns the current WatchTargets (consulted on every event).
func NewPoolerWatcher(
	ctx context.Context,
	topoStore topoclient.Store,
	targets func() []config.WatchTarget,
	poolerStore *store.PoolerHealthStore,
	queue *Queue,
	logger *slog.Logger,
) *PoolerWatcher {
	watchCtx, cancel := context.WithCancel(ctx)
	return &PoolerWatcher{
		topoStore: topoStore,
		targets:   targets,
		store:     poolerStore,
		queue:     queue,
		logger:    logger,
		ctx:       watchCtx,
		cancel:    cancel,
	}
}

// Start launches the pooler watcher goroutine.
func (pw *PoolerWatcher) Start() {
	pw.wg.Go(func() {
		pw.logger.Info("starting pooler watcher")
		topoclient.WatchAllPoolersWithRetry(pw.ctx, pw.topoStore, pw.logger,
			pw.onInitialCell,
			pw.onPoolerUpserted,
			func(poolerID string) {
				// Deletions are not removed from the store; bookkeeping handles stale entry removal.
				pw.logger.Debug("pooler deleted from topology", "pooler_id", poolerID)
			},
			func(cell string) {
				// Cell removal is handled by bookkeeping; no immediate action needed.
				pw.logger.Debug("cell removed from topology", "cell", cell)
			},
		)
		pw.logger.Info("pooler watcher shutting down")
	})
}

// Stop cancels the watcher and waits for all goroutines to finish.
func (pw *PoolerWatcher) Stop() {
	pw.cancel()
	pw.wg.Wait()
}

// onInitialCell processes the initial pooler snapshot for a cell (also called on reconnect).
// When poolers is nil, the cell was removed; there is nothing to add to the store.
func (pw *PoolerWatcher) onInitialCell(_ string, poolers []*clustermetadatapb.MultiPooler) {
	for _, pooler := range poolers {
		pw.onPoolerUpserted(pooler)
	}
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
