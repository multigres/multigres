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

	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/store"
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
	store     *store.PoolerHealthStore
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
	poolerStore *store.PoolerHealthStore,
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
		topoclient.WatchPathWithRetry(pw.ctx, pw.topoStore, topoclient.GlobalCell, topoclient.CellsPath, pw.logger,
			pw.processInitialCells, pw.watchCells)
		pw.logger.Info("pooler watcher shutting down")
	})
}

// Stop cancels the watcher and waits for all goroutines to finish.
func (pw *PoolerWatcher) Stop() {
	pw.cancel()

	pw.mu.Lock()
	for _, w := range pw.cellWatchers {
		w.stopWatcher()
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
		if err := w.syncWatcher(ctx); err != nil {
			return err
		}
	}
	return nil
}

// processInitialCells starts a per-cell watcher for each cell present in the initial snapshot.
func (pw *PoolerWatcher) processInitialCells(initial []*topoclient.WatchDataRecursive) {
	pw.mu.Lock()
	defer pw.mu.Unlock()

	for _, event := range initial {
		if event.Err != nil {
			continue
		}
		cell := topoclient.ExtractCellFromPath(event.Path)
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

// watchCells processes cell-level events until the channel closes or the context is done.
func (pw *PoolerWatcher) watchCells(changes <-chan *topoclient.WatchDataRecursive) {
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

// processCellEvent handles a single event from the cells watch. A NoNode error
// signals that the cell was removed; any other value means the cell is new or updated.
func (pw *PoolerWatcher) processCellEvent(event *topoclient.WatchDataRecursive) {
	cell := topoclient.ExtractCellFromPath(event.Path)
	if cell == "" {
		return
	}

	if event.Err != nil {
		if errors.Is(event.Err, &topoclient.TopoError{Code: topoclient.NoNode}) {
			pw.mu.Lock()
			if w, exists := pw.cellWatchers[cell]; exists {
				pw.logger.Info("cell removed, stopping pooler watcher", "cell", cell)
				w.stopWatcher()
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
	w.startWatcher()
}

// ---------------------------------------------------------------------------
// cellPoolerWatcher: per-cell pooler directory watcher
// ---------------------------------------------------------------------------

type cellPoolerWatcher struct {
	topoStore topoclient.Store
	cell      string
	targets   func() []config.WatchTarget
	store     *store.PoolerHealthStore
	queue     *Queue
	logger    *slog.Logger

	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	syncChan chan chan struct{} // for Sync(); closed by the watchPoolers select loop
}

// newCellPoolerWatcher creates a cellPoolerWatcher for the given cell.
func newCellPoolerWatcher(
	ctx context.Context,
	topoStore topoclient.Store,
	cell string,
	targets func() []config.WatchTarget,
	poolerStore *store.PoolerHealthStore,
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

// startWatcher launches the pooler-directory watcher goroutine for this cell.
func (cw *cellPoolerWatcher) startWatcher() {
	cw.wg.Go(func() {
		topoclient.WatchPathWithRetry(cw.ctx, cw.topoStore, cw.cell, topoclient.PoolersPath, cw.logger,
			cw.processInitialPoolers, cw.watchPoolers)
	})
}

// stopWatcher cancels the cell watcher and waits for its goroutine to finish.
func (cw *cellPoolerWatcher) stopWatcher() {
	cw.cancel()
	cw.wg.Wait()
}

// syncWatcher blocks until all events enqueued before this call have been processed,
// or until ctx is cancelled. It is intended for use in tests.
//
// It works by sending a sentinel channel into the same select loop that processes
// watch events. Since the select loop is sequential, closing the sentinel is
// guaranteed to happen only after all previously-enqueued events have been handled.
func (cw *cellPoolerWatcher) syncWatcher(ctx context.Context) error {
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

// processInitialPoolers feeds each entry in the initial pooler snapshot through handlePoolerEvent.
func (cw *cellPoolerWatcher) processInitialPoolers(initial []*topoclient.WatchDataRecursive) {
	for _, wd := range initial {
		cw.handlePoolerEvent(wd)
	}
}

// watchPoolers processes pooler events until the channel closes or the context is done.
// It also handles syncChan signals used by syncWatcher() for test synchronisation.
func (cw *cellPoolerWatcher) watchPoolers(changes <-chan *topoclient.WatchDataRecursive) {
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

// handlePoolerEvent processes a single watch event for a pooler file.
func (cw *cellPoolerWatcher) handlePoolerEvent(wd *topoclient.WatchDataRecursive) {
	if wd.Err != nil {
		// A deletion arrives as a NoNode error on the specific path.
		if errors.Is(wd.Err, &topoclient.TopoError{Code: topoclient.NoNode}) {
			if strings.HasSuffix(wd.Path, "/"+topoclient.PoolerFile) {
				poolerID := cw.extractPoolerID(wd.Path)
				if poolerID == "" {
					return
				}
				if cw.store.Delete(poolerID) {
					cw.logger.Info("pooler removed from topology, deleted from store", "pooler_id", poolerID)
				} else {
					cw.logger.Debug("pooler removed from topology, not in store (filtered or unknown)", "pooler_id", poolerID)
				}
			}
		} else {
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

// extractPoolerID extracts the pooler ID string from a watch path.
// The path format is: "...poolers/{pooler_name}/Pooler"
func (cw *cellPoolerWatcher) extractPoolerID(watchPath string) string {
	_, after, found := strings.Cut(watchPath, topoclient.PoolersPath+"/")
	if !found {
		return ""
	}
	// Drop the trailing "/Pooler"
	name := strings.TrimSuffix(after, "/"+topoclient.PoolerFile)
	if name == after {
		return "" // suffix was not present
	}
	return name
}
