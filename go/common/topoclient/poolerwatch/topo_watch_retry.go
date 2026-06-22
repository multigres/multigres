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
	"errors"
	"log/slog"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// watchPoolersWithRetry watches the poolers directory for a single cell, delivering
// typed events. It handles retry/reconnect internally.
//
// onInitial is called with all poolers present in the initial snapshot (including on reconnect).
// onUpserted is called when a pooler is added or its metadata changes.
// onDeleted is called with the pooler ID string when a pooler is removed from the topology.
//
// watchPoolersWithRetry returns when ctx is cancelled.
//
// syncReq is an optional channel for in-band sync requests. When a function is
// received on syncReq, the watch loop first drains every event currently
// available in `changes` (so onUpserted/onDeleted have fired for them) and then
// invokes the received function. This lets callers build a barrier that waits
// for the watch loop to "catch up" to a known point. Pass nil to disable.
func watchPoolersWithRetry(
	ctx context.Context,
	store topoclient.ConnProvider,
	cell string,
	logger *slog.Logger,
	syncReq <-chan func(),
	onInitial func([]*clustermetadatapb.MultiPooler),
	onUpserted func(*clustermetadatapb.MultiPooler),
	onDeleted func(poolerID topoclient.ComponentID),
) {
	processOne := func(wd *topoclient.WatchDataRecursive) {
		pooler, poolerID, isDelete, ok := parsePoolerWatchEntry(wd, logger)
		if !ok {
			return
		}
		if isDelete {
			onDeleted(poolerID)
		} else {
			onUpserted(pooler)
		}
	}
	drainPending := func(changes <-chan *topoclient.WatchDataRecursive) bool {
		for {
			select {
			case wd, ok := <-changes:
				if !ok {
					return false
				}
				processOne(wd)
			default:
				return true
			}
		}
	}

	topoclient.WatchPathWithRetry(ctx, store, cell, topoclient.PoolersPath, logger,
		func(initial []*topoclient.WatchDataRecursive) {
			poolers := make([]*clustermetadatapb.MultiPooler, 0, len(initial))
			for _, wd := range initial {
				pooler, _, isDelete, ok := parsePoolerWatchEntry(wd, logger)
				if !ok || isDelete {
					continue
				}
				poolers = append(poolers, pooler)
			}
			onInitial(poolers)
		},
		func(changes <-chan *topoclient.WatchDataRecursive) {
			for {
				select {
				case <-ctx.Done():
					return
				case wd, ok := <-changes:
					if !ok {
						return
					}
					processOne(wd)
				case fn := <-syncReq:
					// Drain everything currently in changes before acking.
					// processOne does not block, so the drain is bounded by
					// what was already buffered at the time syncReq fired.
					if !drainPending(changes) {
						// changes closed mid-drain; ack so the caller doesn't
						// hang, then loop will exit via the next iteration.
						fn()
						return
					}
					fn()
				}
			}
		},
	)
}

// watchAllPoolersWithRetry discovers cells automatically and watches poolers in each cell.
// It starts a per-cell pooler watcher when a cell is discovered, and cancels it when the
// cell is removed. The function handles all retry/reconnect logic internally.
//
// onInitial is called with the cell name and the pooler snapshot for that cell whenever a
// per-cell watcher starts (including on reconnect after a disconnect). The snapshot is
// always a non-nil slice (possibly empty).
// onUpserted is called when a pooler is added or its metadata changes.
// onDeleted is called with the pooler ID string when a pooler is removed.
// onCellRemoved is called when a cell is removed from the topology. It is guaranteed to
// be called only after all onInitial/onUpserted/onDeleted callbacks for that cell have
// completed, so callers can safely clean up per-cell state.
//
// cellSyncBroadcaster coordinates in-band sync barriers across the per-cell
// watcher goroutines started by watchAllPoolersWithRetry. Callers create one
// (newCellSyncBroadcaster), pass it to watchAllPoolersWithRetry, and call
// syncAll() to wait for every active cell watcher to drain its pending events.
//
// Internally each cell watcher owns a syncReq channel that its select loop
// reads from with drain-first semantics: on receipt of a sync function, the
// watcher first drains every event already buffered in `changes` (firing
// onUpserted/onDeleted for each) and then invokes the function as ack. By the
// time syncAll returns, every cell watcher has observed and emitted every
// event that was in flight when syncAll was called.
type cellSyncBroadcaster struct {
	mu       sync.Mutex
	syncReqs map[string]chan func()
}

func newCellSyncBroadcaster() *cellSyncBroadcaster {
	return &cellSyncBroadcaster{syncReqs: make(map[string]chan func())}
}

func (b *cellSyncBroadcaster) register(cell string) chan func() {
	ch := make(chan func(), 1)
	b.mu.Lock()
	b.syncReqs[cell] = ch
	b.mu.Unlock()
	return ch
}

func (b *cellSyncBroadcaster) deregister(cell string) {
	b.mu.Lock()
	delete(b.syncReqs, cell)
	b.mu.Unlock()
}

// cellCount reports the number of currently-registered cell watchers. Test-only.
func (b *cellSyncBroadcaster) cellCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.syncReqs)
}

// syncAll sends a sync function to each currently-registered cell watcher and
// waits for every one to ack (or ctx to cancel). Watchers registered AFTER
// syncAll snapshots the set do not participate in this call.
func (b *cellSyncBroadcaster) syncAll(ctx context.Context) error {
	b.mu.Lock()
	chans := make([]chan func(), 0, len(b.syncReqs))
	for _, ch := range b.syncReqs {
		chans = append(chans, ch)
	}
	b.mu.Unlock()

	if len(chans) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	wg.Add(len(chans))
	for _, ch := range chans {
		select {
		case ch <- wg.Done:
		case <-ctx.Done():
			// Early return: the WaitGroup is local-scope and nobody is
			// Wait()ing on it after this point. Any wg.Done already sent
			// to a watcher's channel will be invoked on this abandoned
			// WaitGroup — harmless no-ops.
			return ctx.Err()
		}
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// watchAllPoolersWithRetry returns when ctx is cancelled. If broadcaster is
// non-nil, it is populated with sync channels as cells start/stop, enabling
// cross-goroutine sync barriers (see cellSyncBroadcaster.syncAll).
func watchAllPoolersWithRetry(
	ctx context.Context,
	store topoclient.ConnProvider,
	logger *slog.Logger,
	broadcaster *cellSyncBroadcaster,
	onInitial func(cell string, poolers []*clustermetadatapb.MultiPooler),
	onUpserted func(*clustermetadatapb.MultiPooler),
	onDeleted func(poolerID topoclient.ComponentID),
	onCellRemoved func(cell string),
) {
	type cellEntry struct {
		cancel context.CancelFunc
		done   chan struct{} // closed when the per-cell goroutine exits
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	cellEntries := make(map[string]*cellEntry)

	startCell := func(cell string) {
		mu.Lock()
		defer mu.Unlock()
		if _, exists := cellEntries[cell]; exists {
			return
		}
		cellCtx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		cellEntries[cell] = &cellEntry{cancel: cancel, done: done}
		var syncReq <-chan func()
		if broadcaster != nil {
			syncReq = broadcaster.register(cell)
		}
		wg.Go(func() {
			defer close(done)
			if broadcaster != nil {
				defer broadcaster.deregister(cell)
			}
			watchPoolersWithRetry(cellCtx, store, cell, logger.With("cell", cell),
				syncReq,
				func(poolers []*clustermetadatapb.MultiPooler) { onInitial(cell, poolers) },
				onUpserted,
				onDeleted,
			)
		})
	}

	stopCell := func(cell string) {
		mu.Lock()
		entry, exists := cellEntries[cell]
		if exists {
			entry.cancel()
			delete(cellEntries, cell)
		}
		mu.Unlock()

		if exists {
			// Wait for the goroutine to fully exit before signalling removal.
			// This prevents onInitial(cell, poolers) from arriving after onCellRemoved(cell),
			// and prevents startCell from launching a new watcher for the same cell
			// before the old one is gone.
			<-entry.done
			onCellRemoved(cell)
		}
	}

	topoclient.WatchCellsWithRetry(ctx, store, logger,
		func(cells []string) {
			for _, cell := range cells {
				startCell(cell)
			}
		},
		startCell,
		stopCell,
	)

	wg.Wait()
}

// parsePoolerWatchEntry parses a single WatchDataRecursive event from the poolers directory.
//
// Returns:
//   - (pooler, "", false, true)  for an upsert event
//   - (nil, poolerID, true, true) for a deletion event
//   - (nil, "", false, false)     when the entry should be ignored (wrong path, unmarshal error, etc.)
func parsePoolerWatchEntry(wd *topoclient.WatchDataRecursive, logger *slog.Logger) (
	pooler *clustermetadatapb.MultiPooler, poolerID topoclient.ComponentID, isDelete bool, ok bool,
) {
	if !strings.HasSuffix(wd.Path, "/"+topoclient.PoolerFile) {
		return nil, "", false, false
	}

	if wd.Err != nil {
		if errors.Is(wd.Err, &topoclient.TopoError{Code: topoclient.NoNode}) {
			id := extractPoolerIDFromPath(wd.Path)
			if id == "" {
				return nil, "", false, false
			}
			return nil, id, true, true
		}
		logger.Warn("watch error on pooler path", "error", wd.Err, "path", wd.Path)
		return nil, "", false, false
	}

	if wd.Contents == nil {
		return nil, "", false, false
	}

	p := &clustermetadatapb.MultiPooler{}
	if err := proto.Unmarshal(wd.Contents, p); err != nil {
		logger.Warn("failed to unmarshal pooler", "path", wd.Path, "error", err)
		return nil, "", false, false
	}
	if p.Id == nil {
		return nil, "", false, false
	}
	return p, "", false, true
}

// extractPoolerIDFromPath extracts the pooler ID from a poolers-directory watch path.
// The path format is: "[prefix/]poolers/{poolerID}/Pooler"
func extractPoolerIDFromPath(watchPath string) topoclient.ComponentID {
	_, after, found := strings.Cut(watchPath, topoclient.PoolersPath+"/")
	if !found {
		return ""
	}
	name := strings.TrimSuffix(after, "/"+topoclient.PoolerFile)
	if name == after {
		return ""
	}
	return topoclient.ComponentID(name)
}
