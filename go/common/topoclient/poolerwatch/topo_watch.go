// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package poolerwatch

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// CellStatus holds discovery status for a single cell. Populated by
// PoolerCache from its own state.
type CellStatus struct {
	Cell         string
	LastActivity time.Time // time of the most recent watch event from this cell
	Poolers      []*clustermetadatapb.Multipooler
}

// topoWatchHandlers receive topology events from a topoWatch. All callbacks
// run synchronously on the per-cell watcher goroutines. For a given cell,
// callbacks observe events in the order the topology emitted them. Across
// cells, callbacks may run concurrently — callers handle their own
// serialization if needed.
type topoWatchHandlers struct {
	// OnSnapshot fires whenever a per-cell watcher starts or reconnects.
	// poolers is the complete current state of the cell. The handler is
	// responsible for reconciling: anything not in poolers that the handler
	// thinks is in this cell should be treated as deleted.
	OnSnapshot func(cell string, poolers []*clustermetadatapb.Multipooler)

	// OnUpsert fires for a single-pooler upsert event observed by the watch.
	OnUpsert func(pooler *clustermetadatapb.Multipooler)

	// OnDelete fires for a single-pooler delete event observed by the watch.
	OnDelete func(id topoclient.ComponentID)

	// OnCellRemoved fires when a cell is removed from topology. It is
	// guaranteed to be called only after all OnSnapshot/OnUpsert/OnDelete
	// callbacks for that cell have completed.
	OnCellRemoved func(cell string)
}

// topoWatch is a thin etcd-watch primitive. It owns the per-cell watcher
// goroutines and the retry/backoff logic, but holds NO pooler state: it
// dispatches events as they arrive and forgets them. The consumer
// (PoolerCache) is the single source of truth for the topology view.
type topoWatch struct {
	store    topoclient.ConnProvider
	logger   *slog.Logger
	handlers topoWatchHandlers

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// broadcaster lets Sync(ctx) ask every per-cell watcher goroutine to
	// drain its in-flight events before returning.
	broadcaster *cellSyncBroadcaster
}

// newTopoWatch creates a new topoWatch. Call Start to begin watching.
func newTopoWatch(ctx context.Context, store topoclient.ConnProvider, logger *slog.Logger, handlers topoWatchHandlers) *topoWatch {
	wctx, cancel := context.WithCancel(ctx)
	return &topoWatch{
		store:       store,
		logger:      logger,
		handlers:    handlers,
		ctx:         wctx,
		cancel:      cancel,
		broadcaster: newCellSyncBroadcaster(),
	}
}

// Start launches the per-cell watcher goroutines.
func (c *topoWatch) Start() {
	c.wg.Go(func() {
		watchPoolersAcrossCells(c.ctx, c.store, c.logger, c.broadcaster,
			c.handlers.OnSnapshot,
			c.handlers.OnUpsert,
			c.handlers.OnDelete,
			c.handlers.OnCellRemoved,
		)
	})
}

// Stop cancels the watch and waits for all background goroutines to exit.
func (c *topoWatch) Stop() {
	c.cancel()
	c.wg.Wait()
}

// sync blocks until every per-cell watcher has drained the events it had
// already observed at the time of the call. Because handlers run
// synchronously on the watcher goroutines, by the time sync returns the
// handlers have observed and processed those events.
//
// Test-only: the sole caller is poolerwatch.SyncForTest. Lowercase to
// match that constraint — production code reacts to events as they
// arrive rather than polling for state.
func (c *topoWatch) sync(ctx context.Context) error {
	return c.broadcaster.syncAll(ctx)
}
