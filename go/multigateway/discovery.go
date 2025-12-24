// Copyright 2025 Supabase, Inc.
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

package multigateway

import (
	"context"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/tools/retry"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"

	"google.golang.org/protobuf/proto"
)

// CellPoolerDiscovery is a discovery service that watches for multipoolers
// in a single cell using topology watches and maintains a list of available poolers.
type CellPoolerDiscovery struct {
	// Configuration
	topoStore topoclient.Store
	cell      string // The cell this watcher is monitoring
	logger    *slog.Logger

	// Control
	ctx        context.Context
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup

	// State
	mu          sync.Mutex
	poolers     map[string]*topoclient.MultiPoolerInfo // pooler ID -> pooler info
	lastRefresh time.Time
}

// NewCellPoolerDiscovery creates a new pooler discovery service for a single cell.
func NewCellPoolerDiscovery(ctx context.Context, topoStore topoclient.Store, cell string, logger *slog.Logger) *CellPoolerDiscovery {
	discoveryCtx, cancel := context.WithCancel(ctx)

	return &CellPoolerDiscovery{
		topoStore:  topoStore,
		cell:       cell,
		logger:     logger.With("cell", cell),
		ctx:        discoveryCtx,
		cancelFunc: cancel,
		poolers:    make(map[string]*topoclient.MultiPoolerInfo),
	}
}

// Start begins the discovery process using topology watch.
func (pd *CellPoolerDiscovery) Start() {
	pd.wg.Go(func() {
		pd.logger.Info("Starting pooler discovery with topology watch", "cell", pd.cell)

		r := retry.New(100*time.Millisecond, 30*time.Second)
		for attempt, err := range r.Attempts(pd.ctx) {
			if err != nil {
				// Context cancelled
				pd.logger.Info("Pooler discovery shutting down")
				return
			}

			if attempt > 0 {
				pd.logger.Info("Restarting pooler discovery with topology watch", "cell", pd.cell)
			}

			// Establish watch and process changes
			func() {
				// Get connection for the cell
				conn, err := pd.topoStore.ConnForCell(pd.ctx, pd.cell)
				if err != nil {
					pd.logger.Error("Failed to get connection for cell", "cell", pd.cell, "error", err)
					return
				}

				// Start watching the poolers directory
				poolersPath := "poolers" // This matches the PoolersPath constant from store.go
				initial, changes, err := conn.WatchRecursive(pd.ctx, poolersPath)
				if err != nil {
					pd.logger.Error("Failed to start recursive watch on poolers", "path", poolersPath, "error", err)
					return
				}

				// Process initial values
				pd.processInitialPoolers(initial)

				// Reset backoff after watch has been stable for 30s
				resetTimer := time.AfterFunc(30*time.Second, func() {
					r.Reset()
				})
				defer resetTimer.Stop()

				// Process changes as they come in
				for {
					select {
					case <-pd.ctx.Done():
						return
					case watchData, ok := <-changes:
						if !ok {
							pd.logger.Info("Watch channel closed, will reconnect")
							return
						}

						if watchData.Err != nil {
							pd.logger.Error("Watch error received", "error", watchData.Err)
							// Continue watching despite the error
							continue
						}

						pd.processPoolerChange(watchData)
					}
				}
			}()
		}
	})
}

// Stop stops the discovery service.
func (pd *CellPoolerDiscovery) Stop() {
	pd.cancelFunc()
	pd.wg.Wait()
}

// processInitialPoolers processes the initial set of poolers from the watch
func (pd *CellPoolerDiscovery) processInitialPoolers(initial []*topoclient.WatchDataRecursive) {
	pd.mu.Lock()
	defer pd.mu.Unlock()

	// Clear existing poolers
	pd.poolers = make(map[string]*topoclient.MultiPoolerInfo)

	// Process initial pooler data
	for _, watchData := range initial {
		if watchData.Err != nil {
			pd.logger.Warn("Error in initial watch data", "path", watchData.Path, "error", watchData.Err)
			continue
		}

		// Parse the pooler from watch data
		pooler, err := pd.parsePoolerFromWatchData(watchData)
		if err != nil {
			pd.logger.Warn("Failed to parse pooler from initial data", "path", watchData.Path, "error", err)
			continue
		}

		if pooler != nil {
			poolerID := topoclient.MultiPoolerIDString(pooler.Id)
			pd.poolers[poolerID] = pooler
			pd.logger.Info("Initial pooler discovered",
				"id", poolerID,
				"hostname", pooler.Hostname,
				"addr", pooler.Addr(),
				"database", pooler.Database,
				"shard", pooler.Shard,
				"type", pooler.Type.String())
		}
	}

	pd.lastRefresh = time.Now()
	pd.logger.Info("Initial pooler discovery completed",
		"cell", pd.cell,
		"pooler_count", len(pd.poolers))
}

// processPoolerChange processes a single pooler change from the watch
func (pd *CellPoolerDiscovery) processPoolerChange(watchData *topoclient.WatchDataRecursive) {
	pd.mu.Lock()
	defer pd.mu.Unlock()

	// Parse the pooler from watch data
	pooler, err := pd.parsePoolerFromWatchData(watchData)
	if err != nil {
		pd.logger.Warn("Failed to parse pooler from change data", "path", watchData.Path, "error", err)
		return
	}

	if pooler == nil {
		// This might be a deletion or non-pooler file
		pd.logger.Debug("Skipping non-pooler file or deletion", "path", watchData.Path)
		return
	}

	// Add or update the pooler
	poolerID := topoclient.MultiPoolerIDString(pooler.Id)

	// Check if this is a new pooler
	_, existed := pd.poolers[poolerID]
	pd.poolers[poolerID] = pooler
	pd.lastRefresh = time.Now()

	if !existed {
		pd.logger.Info("New pooler discovered",
			"id", poolerID,
			"hostname", pooler.Hostname,
			"addr", pooler.Addr(),
			"tableGroup", pooler.TableGroup,
			"database", pooler.Database,
			"shard", pooler.Shard,
			"type", pooler.Type.String())
	} else {
		pd.logger.Info("Pooler updated",
			"id", poolerID,
			"hostname", pooler.Hostname,
			"addr", pooler.Addr(),
			"tableGroup", pooler.TableGroup,
			"database", pooler.Database,
			"shard", pooler.Shard,
			"type", pooler.Type.String())
	}
}

// GetPoolersForAdmin returns a list of all discovered poolers in this cell.
// This is intended for admin/status pages, not the hot query path.
// Poolers are sorted by name for consistent display order.
func (pd *CellPoolerDiscovery) GetPoolersForAdmin() []*clustermetadatapb.MultiPooler {
	pd.mu.Lock()
	defer pd.mu.Unlock()

	// Collect and sort pooler IDs for consistent ordering
	poolerIDs := make([]string, 0, len(pd.poolers))
	for id := range pd.poolers {
		poolerIDs = append(poolerIDs, id)
	}
	sort.Strings(poolerIDs)

	poolers := make([]*clustermetadatapb.MultiPooler, 0, len(pd.poolers))
	for _, id := range poolerIDs {
		pooler := pd.poolers[id]
		poolers = append(poolers, proto.Clone(pooler.MultiPooler).(*clustermetadatapb.MultiPooler))
	}
	return poolers
}

// GetPooler returns a pooler matching the target specification.
// Target specifies the tablegroup, shard, and pooler type to route to.
// Returns nil if no matching pooler is found.
//
// Filtering logic:
// - TableGroup: Required, must match exactly
// - PoolerType: If not specified (UNKNOWN), defaults to PRIMARY
// - Shard: If empty, matches any shard; otherwise must match exactly
func (pd *CellPoolerDiscovery) GetPooler(target *query.Target) *clustermetadatapb.MultiPooler {
	pd.mu.Lock()
	defer pd.mu.Unlock()

	// Default to PRIMARY if not specified
	targetType := target.PoolerType
	if targetType == clustermetadatapb.PoolerType_UNKNOWN {
		targetType = clustermetadatapb.PoolerType_PRIMARY
	}

	// Debug: Log all discovered poolers
	pd.logger.Debug("GetPooler called - listing all discovered poolers",
		"target_tablegroup", target.TableGroup,
		"target_shard", target.Shard,
		"target_pooler_type", targetType.String(),
		"total_poolers", len(pd.poolers))
	for i, pooler := range pd.poolers {
		pd.logger.Debug("discovered pooler",
			"index", i,
			"pooler_id", topoclient.MultiPoolerIDString(pooler.Id),
			"tablegroup", pooler.TableGroup,
			"shard", pooler.Shard,
			"type", pooler.Type.String())
	}

	// Find matching pooler
	for _, pooler := range pd.poolers {
		// TableGroup must match
		if pooler.TableGroup != target.TableGroup {
			continue
		}

		// PoolerType must match
		if pooler.Type != targetType {
			continue
		}

		// Shard must match if specified
		if target.Shard != "" && pooler.Shard != target.Shard {
			continue
		}

		// Found a match!
		pd.logger.Debug("selected pooler for target",
			"pooler_id", topoclient.MultiPoolerIDString(pooler.Id),
			"pooler_type", pooler.Type.String(),
			"tablegroup", pooler.TableGroup,
			"shard", pooler.Shard)
		return proto.Clone(pooler.MultiPooler).(*clustermetadatapb.MultiPooler)
	}

	pd.logger.Warn("no matching pooler found",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", targetType.String())
	return nil
}

// LastRefresh returns the timestamp of the last successful refresh.
func (pd *CellPoolerDiscovery) LastRefresh() time.Time {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	return pd.lastRefresh
}

// PoolerCount returns the current number of discovered poolers.
func (pd *CellPoolerDiscovery) PoolerCount() int {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	return len(pd.poolers)
}

// parsePoolerFromWatchData parses a MultiPooler from watch data
func (pd *CellPoolerDiscovery) parsePoolerFromWatchData(watchData *topoclient.WatchDataRecursive) (*topoclient.MultiPoolerInfo, error) {
	// Only process files that end with "Pooler" (the actual pooler data files)
	if !strings.HasSuffix(watchData.Path, "/Pooler") {
		return nil, nil // Not a pooler file, skip
	}

	// If Contents is nil, this might be a deletion
	if watchData.Contents == nil {
		return nil, nil
	}

	// Parse the protobuf data
	pooler := &clustermetadatapb.MultiPooler{}
	if err := proto.Unmarshal(watchData.Contents, pooler); err != nil {
		return nil, err
	}

	return &topoclient.MultiPoolerInfo{
		MultiPooler: pooler,
	}, nil
}

// Cell returns the cell this discovery is watching.
func (pd *CellPoolerDiscovery) Cell() string {
	return pd.cell
}

// GlobalPoolerDiscovery orchestrates multiple CellPoolerDiscovery instances,
// one per cell. It watches for cell changes from global etcd and creates/removes
// cell watchers as cells appear/disappear.
type GlobalPoolerDiscovery struct {
	// Configuration
	topoStore topoclient.Store
	localCell string // The cell this multigateway is running in (for cell affinity)
	logger    *slog.Logger

	// Control
	ctx        context.Context
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup

	// State
	mu           sync.Mutex
	cellWatchers map[string]*CellPoolerDiscovery // cell name -> cell watcher
}

// NewGlobalPoolerDiscovery creates a new global pooler discovery service.
// The localCell parameter indicates which cell this multigateway is running in,
// which will be used for cell affinity when selecting poolers.
func NewGlobalPoolerDiscovery(ctx context.Context, topoStore topoclient.Store, localCell string, logger *slog.Logger) *GlobalPoolerDiscovery {
	discoveryCtx, cancel := context.WithCancel(ctx)

	return &GlobalPoolerDiscovery{
		topoStore:    topoStore,
		localCell:    localCell,
		logger:       logger,
		ctx:          discoveryCtx,
		cancelFunc:   cancel,
		cellWatchers: make(map[string]*CellPoolerDiscovery),
	}
}

// Start begins the discovery process by watching for cells and starting
// a CellPoolerDiscovery for each cell.
func (gd *GlobalPoolerDiscovery) Start() {
	gd.wg.Go(func() {
		gd.logger.Info("Starting global pooler discovery")

		r := retry.New(100*time.Millisecond, 30*time.Second)
		for attempt, err := range r.Attempts(gd.ctx) {
			if err != nil {
				// Context cancelled
				gd.logger.Info("Global pooler discovery shutting down")
				return
			}

			if attempt > 0 {
				gd.logger.Info("Restarting global pooler discovery")
			}

			// Watch for cells and manage cell watchers
			func() {
				// Get initial list of cells
				cells, err := gd.topoStore.GetCellNames(gd.ctx)
				if err != nil {
					gd.logger.Error("Failed to get cell names", "error", err)
					return
				}

				gd.logger.Info("Discovered cells", "cells", cells)

				// Start watchers for each cell
				gd.mu.Lock()
				for _, cell := range cells {
					if _, exists := gd.cellWatchers[cell]; !exists {
						gd.startCellWatcher(cell)
					}
				}
				gd.mu.Unlock()

				// Reset backoff after stable for 30s
				resetTimer := time.AfterFunc(30*time.Second, func() {
					r.Reset()
				})
				defer resetTimer.Stop()

				// TODO: Watch for cell changes (cells being added/removed)
				// For now, just wait for context cancellation
				<-gd.ctx.Done()
			}()
		}
	})
}

// startCellWatcher starts a CellPoolerDiscovery for the given cell.
// Caller must hold gd.mu.
func (gd *GlobalPoolerDiscovery) startCellWatcher(cell string) {
	gd.logger.Info("Starting cell watcher", "cell", cell)

	cellWatcher := NewCellPoolerDiscovery(gd.ctx, gd.topoStore, cell, gd.logger)
	gd.cellWatchers[cell] = cellWatcher
	cellWatcher.Start()
}

// Stop stops the global discovery service and all cell watchers.
func (gd *GlobalPoolerDiscovery) Stop() {
	gd.cancelFunc()

	// Stop all cell watchers
	gd.mu.Lock()
	for _, watcher := range gd.cellWatchers {
		watcher.Stop()
	}
	gd.mu.Unlock()

	gd.wg.Wait()
}

// GetPooler returns a pooler matching the target specification.
// It searches across all cells, preferring the local cell for replicas.
// For primaries, it will return a primary from any cell.
func (gd *GlobalPoolerDiscovery) GetPooler(target *query.Target) *clustermetadatapb.MultiPooler {
	gd.mu.Lock()
	defer gd.mu.Unlock()

	// Default to PRIMARY if not specified
	targetType := target.PoolerType
	if targetType == clustermetadatapb.PoolerType_UNKNOWN {
		targetType = clustermetadatapb.PoolerType_PRIMARY
	}

	// For replicas, try local cell first
	if targetType != clustermetadatapb.PoolerType_PRIMARY {
		if localWatcher, exists := gd.cellWatchers[gd.localCell]; exists {
			if pooler := localWatcher.GetPooler(target); pooler != nil {
				return pooler
			}
		}
	}

	// Search all cells
	for _, watcher := range gd.cellWatchers {
		if pooler := watcher.GetPooler(target); pooler != nil {
			return pooler
		}
	}

	gd.logger.Warn("No matching pooler found across all cells",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", targetType.String())
	return nil
}

// PoolerCount returns the total number of discovered poolers across all cells.
func (gd *GlobalPoolerDiscovery) PoolerCount() int {
	gd.mu.Lock()
	defer gd.mu.Unlock()

	count := 0
	for _, watcher := range gd.cellWatchers {
		count += watcher.PoolerCount()
	}
	return count
}

// CellStatusInfo contains status information for a single cell's discovery.
type CellStatusInfo struct {
	Cell        string
	LastRefresh time.Time
	Poolers     []*clustermetadatapb.MultiPooler
}

// GetCellStatusesForAdmin returns status information for each cell.
// This is intended for admin/status pages, not the hot query path.
// Cells are sorted alphabetically for consistent display order.
func (gd *GlobalPoolerDiscovery) GetCellStatusesForAdmin() []CellStatusInfo {
	gd.mu.Lock()
	defer gd.mu.Unlock()

	// Collect and sort cell names for consistent ordering
	cellNames := make([]string, 0, len(gd.cellWatchers))
	for cellName := range gd.cellWatchers {
		cellNames = append(cellNames, cellName)
	}
	sort.Strings(cellNames)

	statuses := make([]CellStatusInfo, 0, len(gd.cellWatchers))
	for _, cellName := range cellNames {
		watcher := gd.cellWatchers[cellName]
		statuses = append(statuses, CellStatusInfo{
			Cell:        watcher.Cell(),
			LastRefresh: watcher.LastRefresh(),
			Poolers:     watcher.GetPoolersForAdmin(),
		})
	}
	return statuses
}
