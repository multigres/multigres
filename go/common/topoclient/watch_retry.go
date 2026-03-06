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
	"time"

	"github.com/multigres/multigres/go/tools/retry"
)

// watchPathWithRetry establishes a WatchRecursive watch in an exponential-backoff retry loop.
//
// For each attempt it:
//  1. Calls store.ConnForCell(ctx, cell) to obtain a topo connection.
//  2. Calls conn.WatchRecursive(ctx, path) to get the initial snapshot and change channel.
//  3. Calls onInitial(initial) with the initial snapshot.
//  4. Resets the backoff after 30 seconds of stable watching.
//  5. Calls watchFn(changes) to process ongoing events.
//
// When watchFn returns (channel closed or ctx cancelled), the loop retries with backoff.
// watchPathWithRetry returns when ctx is cancelled.
//
// watchFn is responsible for its own select loop. It should return when changes is closed
// or ctx is done.
func watchPathWithRetry(
	ctx context.Context,
	store ConnProvider,
	cell string,
	path string,
	logger *slog.Logger,
	onInitial func([]*WatchDataRecursive),
	watchFn func(changes <-chan *WatchDataRecursive),
) {
	r := retry.New(100*time.Millisecond, 30*time.Second)
	for attempt, err := range r.Attempts(ctx) {
		if err != nil {
			return // ctx cancelled
		}
		if attempt > 0 {
			logger.InfoContext(ctx, "watch reconnecting", "cell", cell, "path", path)
		}

		conn, err := store.ConnForCell(ctx, cell)
		if err != nil {
			logger.ErrorContext(ctx, "failed to get topo connection", "cell", cell, "error", err)
			continue
		}

		initial, changes, err := conn.WatchRecursive(ctx, path)
		if err != nil {
			logger.ErrorContext(ctx, "failed to start recursive watch", "cell", cell, "path", path, "error", err)
			continue
		}

		onInitial(initial)

		resetTimer := time.AfterFunc(30*time.Second, r.Reset)
		watchFn(changes)
		resetTimer.Stop()
	}
}

// watchCellsWithRetry watches the global cells directory and delivers typed cell events.
// It handles retry/reconnect internally.
//
// onInitial is called with the names of all cells present in the initial snapshot.
// onCellAdded is called when a new cell appears.
// onCellRemoved is called when a cell is deleted from the topology.
//
// watchCellsWithRetry returns when ctx is cancelled.
func watchCellsWithRetry(
	ctx context.Context,
	store ConnProvider,
	logger *slog.Logger,
	onInitial func(cells []string),
	onCellAdded func(cell string),
	onCellRemoved func(cell string),
) {
	watchPathWithRetry(ctx, store, GlobalCell, CellsPath, logger,
		func(initial []*WatchDataRecursive) {
			var cells []string
			for _, wd := range initial {
				if wd.Err != nil {
					continue
				}
				if cell := extractCellFromPath(wd.Path); cell != "" {
					cells = append(cells, cell)
				}
			}
			onInitial(cells)
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
					cell := extractCellFromPath(wd.Path)
					if cell == "" {
						continue
					}
					if wd.Err != nil {
						if errors.Is(wd.Err, &TopoError{Code: NoNode}) {
							onCellRemoved(cell)
						} else {
							logger.WarnContext(ctx, "cell watch error", "error", wd.Err, "path", wd.Path)
						}
						continue
					}
					onCellAdded(cell)
				}
			}
		},
	)
}

// extractCellFromPath extracts the cell name from a cells/ watch path.
// Handles both relative (memorytopo: "cells/zone1/Cell") and absolute
// (etcd: "/multigres/global/cells/zone1/Cell") paths.
// Returns "" if the path does not contain a recognisable cell segment.
func extractCellFromPath(watchPath string) string {
	_, after, found := strings.Cut(watchPath, CellsPath+"/")
	if !found {
		return ""
	}
	cell, _, _ := strings.Cut(after, "/")
	return cell
}
