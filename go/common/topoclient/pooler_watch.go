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

package topoclient

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"

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
func watchPoolersWithRetry(
	ctx context.Context,
	store ConnProvider,
	cell string,
	logger *slog.Logger,
	onInitial func([]*clustermetadatapb.MultiPooler),
	onUpserted func(*clustermetadatapb.MultiPooler),
	onDeleted func(poolerID string),
) {
	watchPathWithRetry(ctx, store, cell, PoolersPath, logger,
		func(initial []*WatchDataRecursive) {
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
		func(changes <-chan *WatchDataRecursive) {
			for {
				select {
				case <-ctx.Done():
					return
				case wd, ok := <-changes:
					if !ok {
						return
					}
					pooler, poolerID, isDelete, ok := parsePoolerWatchEntry(wd, logger)
					if !ok {
						continue
					}
					if isDelete {
						onDeleted(poolerID)
					} else {
						onUpserted(pooler)
					}
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
// watchAllPoolersWithRetry returns when ctx is cancelled.
func watchAllPoolersWithRetry(
	ctx context.Context,
	store ConnProvider,
	logger *slog.Logger,
	onInitial func(cell string, poolers []*clustermetadatapb.MultiPooler),
	onUpserted func(*clustermetadatapb.MultiPooler),
	onDeleted func(poolerID string),
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
		wg.Go(func() {
			defer close(done)
			watchPoolersWithRetry(cellCtx, store, cell, logger.With("cell", cell),
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

	watchCellsWithRetry(ctx, store, logger,
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
func parsePoolerWatchEntry(wd *WatchDataRecursive, logger *slog.Logger) (
	pooler *clustermetadatapb.MultiPooler, poolerID string, isDelete bool, ok bool,
) {
	if !strings.HasSuffix(wd.Path, "/"+PoolerFile) {
		return nil, "", false, false
	}

	if wd.Err != nil {
		if errors.Is(wd.Err, &TopoError{Code: NoNode}) {
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
func extractPoolerIDFromPath(watchPath string) string {
	_, after, found := strings.Cut(watchPath, PoolersPath+"/")
	if !found {
		return ""
	}
	name := strings.TrimSuffix(after, "/"+PoolerFile)
	if name == after {
		return ""
	}
	return name
}
