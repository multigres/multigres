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
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/store"
	"github.com/multigres/multigres/go/tools/retry"
)

// PoolerWatcher watches etcd for topology changes and keeps the pooler store
// up-to-date. It mirrors the two-tier approach used by multigateway:
//
//  1. A global watcher monitors the cells/ directory to detect cells appearing
//     or disappearing.
//  2. For each cell, a per-cell watcher monitors the poolers/ directory.
//
// When a pooler event arrives, it is filtered in-memory against the engine's
// WatchTargets before the pooler store is updated.
type PoolerWatcher struct {
	topoStore topoclient.Store
	targets   func() []config.WatchTarget // live accessor, same as Engine.shardWatchTargets
	store     *store.PoolerStore
	queue     *Queue
	logger    *slog.Logger

	// Control
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// State: per-cell watchers (protected by mu)
	mu           sync.Mutex
	cellWatchers map[string]*cellPoolerWatcher
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
	watchCtx, cancel := context.WithCancel(ctx)
	return &PoolerWatcher{
		topoStore:    topoStore,
		targets:      targets,
		store:        poolerStore,
		queue:        queue,
		logger:       logger,
		ctx:          watchCtx,
		cancel:       cancel,
		cellWatchers: make(map[string]*cellPoolerWatcher),
	}
}

// Start launches the global cell-watcher goroutine.
func (pw *PoolerWatcher) Start() {
	pw.wg.Go(func() {
		pw.logger.Info("starting pooler watcher")

		r := retry.New(100*time.Millisecond, 30*time.Second)
		for attempt, err := range r.Attempts(pw.ctx) {
			if err != nil {
				pw.logger.Info("pooler watcher shutting down")
				return
			}
			if attempt > 0 {
				pw.logger.Info("restarting pooler watcher cell watch")
			}
			pw.watchCells(r)
		}
	})
}

// Stop cancels the watcher and waits for all goroutines to finish.
func (pw *PoolerWatcher) Stop() {
	pw.cancel()

	pw.mu.Lock()
	for _, w := range pw.cellWatchers {
		w.stop()
	}
	pw.mu.Unlock()

	pw.wg.Wait()
}

// Sync blocks until all events that were in-flight at the time of the call have
// been processed by every active cell watcher. It is intended for use in tests
// to replace time.Sleep calls after topology mutations.
func (pw *PoolerWatcher) Sync(ctx context.Context) error {
	pw.mu.Lock()
	watchers := make([]*cellPoolerWatcher, 0, len(pw.cellWatchers))
	for _, w := range pw.cellWatchers {
		watchers = append(watchers, w)
	}
	pw.mu.Unlock()

	for _, w := range watchers {
		if err := w.sync(ctx); err != nil {
			return err
		}
	}
	return nil
}

// watchCells establishes a WatchRecursive on the global cells/ directory.
// It starts per-cell watchers as cells appear and stops them when cells disappear.
func (pw *PoolerWatcher) watchCells(r *retry.Retry) {
	conn, err := pw.topoStore.ConnForCell(pw.ctx, topoclient.GlobalCell)
	if err != nil {
		pw.logger.Error("failed to get global topo connection for cell watch", "error", err)
		return
	}

	initial, changes, err := conn.WatchRecursive(pw.ctx, topoclient.CellsPath)
	if err != nil {
		pw.logger.Error("failed to start watch on cells directory", "error", err)
		return
	}

	// Process existing cells
	pw.processInitialCells(initial)

	// Reset backoff after 30s of stable watching
	resetTimer := time.AfterFunc(30*time.Second, r.Reset)
	defer resetTimer.Stop()

	for {
		select {
		case <-pw.ctx.Done():
			return
		case event, ok := <-changes:
			if !ok {
				pw.logger.Info("cell watch channel closed, will reconnect")
				return
			}
			pw.processCellEvent(event)
		}
	}
}

func (pw *PoolerWatcher) processInitialCells(initial []*topoclient.WatchDataRecursive) {
	pw.mu.Lock()
	defer pw.mu.Unlock()

	for _, event := range initial {
		if event.Err != nil {
			continue
		}
		cell := extractCellNameFromPath(event.Path)
		if cell == "" {
			continue
		}
		if _, exists := pw.cellWatchers[cell]; !exists {
			pw.startCellWatcher(cell)
		}
	}

	cells := make([]string, 0, len(pw.cellWatchers))
	for cell := range pw.cellWatchers {
		cells = append(cells, cell)
	}
	pw.logger.Info("initial cell discovery completed", "cells", cells)
}

func (pw *PoolerWatcher) processCellEvent(event *topoclient.WatchDataRecursive) {
	cell := extractCellNameFromPath(event.Path)
	if cell == "" {
		return
	}

	if event.Err != nil {
		if errors.Is(event.Err, &topoclient.TopoError{Code: topoclient.NoNode}) {
			pw.mu.Lock()
			if w, exists := pw.cellWatchers[cell]; exists {
				pw.logger.Info("cell removed, stopping pooler watcher", "cell", cell)
				w.stop()
				delete(pw.cellWatchers, cell)
			}
			pw.mu.Unlock()
		} else {
			pw.logger.Warn("cell watch error", "error", event.Err, "path", event.Path)
		}
		return
	}

	pw.mu.Lock()
	if _, exists := pw.cellWatchers[cell]; !exists {
		pw.logger.Info("new cell discovered, starting pooler watcher", "cell", cell)
		pw.startCellWatcher(cell)
	}
	pw.mu.Unlock()
}

// startCellWatcher starts a per-cell pooler watcher. Caller must hold pw.mu.
func (pw *PoolerWatcher) startCellWatcher(cell string) {
	w := newCellPoolerWatcher(pw.ctx, pw.topoStore, cell, pw.targets, pw.store, pw.queue, pw.logger)
	pw.cellWatchers[cell] = w
	w.start()
}

// extractCellNameFromPath extracts the cell name from a cells/ watch path.
// Handles both relative paths (memorytopo: "cells/zone1/Cell") and
// absolute paths with root prefix (etcd: "/multigres/global/cells/zone1/Cell").
func extractCellNameFromPath(watchPath string) string {
	_, after, found := strings.Cut(watchPath, topoclient.CellsPath+"/")
	if !found {
		return ""
	}
	cell, _, _ := strings.Cut(after, "/")
	return cell
}

// ---------------------------------------------------------------------------
// cellPoolerWatcher: per-cell pooler directory watcher
// ---------------------------------------------------------------------------

type cellPoolerWatcher struct {
	topoStore topoclient.Store
	cell      string
	targets   func() []config.WatchTarget
	store     *store.PoolerStore
	queue     *Queue
	logger    *slog.Logger

	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	syncChan chan chan struct{} // for Sync(); closed by the watchPoolers select loop
}

func newCellPoolerWatcher(
	ctx context.Context,
	topoStore topoclient.Store,
	cell string,
	targets func() []config.WatchTarget,
	poolerStore *store.PoolerStore,
	queue *Queue,
	logger *slog.Logger,
) *cellPoolerWatcher {
	watchCtx, cancel := context.WithCancel(ctx)
	return &cellPoolerWatcher{
		topoStore: topoStore,
		cell:      cell,
		targets:   targets,
		store:     poolerStore,
		queue:     queue,
		logger:    logger.With("cell", cell),
		ctx:       watchCtx,
		cancel:    cancel,
		syncChan:  make(chan chan struct{}, 1),
	}
}

func (cw *cellPoolerWatcher) start() {
	cw.wg.Go(func() {
		r := retry.New(100*time.Millisecond, 30*time.Second)
		for attempt, err := range r.Attempts(cw.ctx) {
			if err != nil {
				cw.logger.Info("cell pooler watcher shutting down")
				return
			}
			if attempt > 0 {
				cw.logger.Info("restarting cell pooler watcher")
			}
			cw.watchPoolers(r)
		}
	})
}

func (cw *cellPoolerWatcher) stop() {
	cw.cancel()
	cw.wg.Wait()
}

func (cw *cellPoolerWatcher) watchPoolers(r *retry.Retry) {
	conn, err := cw.topoStore.ConnForCell(cw.ctx, cw.cell)
	if err != nil {
		cw.logger.Error("failed to get cell topo connection", "error", err)
		return
	}

	initial, changes, err := conn.WatchRecursive(cw.ctx, topoclient.PoolersPath)
	if err != nil {
		cw.logger.Error("failed to start watch on poolers directory", "error", err)
		return
	}

	// Process the initial set of poolers
	for _, wd := range initial {
		cw.handlePoolerEvent(wd)
	}

	// Reset backoff after 30s of stable watching
	resetTimer := time.AfterFunc(30*time.Second, r.Reset)
	defer resetTimer.Stop()

	for {
		select {
		case <-cw.ctx.Done():
			return
		case event, ok := <-changes:
			if !ok {
				cw.logger.Info("pooler watch channel closed, will reconnect")
				return
			}
			cw.handlePoolerEvent(event)
		case done := <-cw.syncChan:
			close(done)
		}
	}
}

// sync blocks until all events enqueued before this call have been processed,
// or until ctx is cancelled. It is intended for use in tests.
//
// It works by sending a sentinel channel into the same select loop that processes
// watch events. Since the select loop is sequential, closing the sentinel is
// guaranteed to happen only after all previously-enqueued events have been handled.
func (cw *cellPoolerWatcher) sync(ctx context.Context) error {
	done := make(chan struct{})
	select {
	case cw.syncChan <- done:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// handlePoolerEvent processes a single watch event for a pooler file.
func (cw *cellPoolerWatcher) handlePoolerEvent(wd *topoclient.WatchDataRecursive) {
	if wd.Err != nil {
		// Deletions (NoNode) are intentionally ignored here: the pooler store
		// entry is left in place so that ongoing health checks can continue until
		// bookkeeping removes it after the configured unseen threshold.
		if !errors.Is(wd.Err, &topoclient.TopoError{Code: topoclient.NoNode}) {
			cw.logger.Warn("watch error on pooler path", "error", wd.Err, "path", wd.Path)
		}
		return
	}

	// Only handle files named "Pooler"
	if !strings.HasSuffix(wd.Path, "/"+topoclient.PoolerFile) {
		return
	}
	if wd.Contents == nil {
		return
	}

	pooler := &clustermetadatapb.MultiPooler{}
	if err := proto.Unmarshal(wd.Contents, pooler); err != nil {
		cw.logger.Warn("failed to unmarshal pooler", "path", wd.Path, "error", err)
		return
	}
	if pooler.Id == nil {
		return
	}

	// Apply WatchTarget filter (in-memory, same semantics as old GetMultiPoolersByCell options)
	if !cw.matchesAnyTarget(pooler) {
		return
	}

	poolerID := topoclient.MultiPoolerIDString(pooler.Id)

	if existing, ok := cw.store.Get(poolerID); ok {
		// Update the MultiPooler metadata but preserve all health-check timestamps.
		existing.MultiPooler = pooler
		cw.store.Set(poolerID, existing)
		cw.logger.Debug("pooler metadata updated from topology", "pooler_id", poolerID)
	} else {
		// New pooler — add to store and queue for immediate health check.
		cw.store.Set(poolerID, &multiorchdatapb.PoolerHealthState{
			MultiPooler: pooler,
			IsUpToDate:  false,
		})
		cw.queue.Push(poolerID)
		cw.logger.Info("new pooler discovered via watcher",
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
func (cw *cellPoolerWatcher) matchesAnyTarget(pooler *clustermetadatapb.MultiPooler) bool {
	for _, target := range cw.targets() {
		if target.MatchesShard(pooler.Database, pooler.TableGroup, pooler.Shard) {
			return true
		}
	}
	return false
}
