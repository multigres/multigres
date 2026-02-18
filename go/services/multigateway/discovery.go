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
	"errors"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
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

	// Callbacks for notifying about pooler changes (may be nil)
	onPoolerChanged func(pooler *clustermetadatapb.MultiPooler)
	onPoolerRemoved func(pooler *clustermetadatapb.MultiPooler)

	// Control
	ctx        context.Context
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup

	// State (protected by mu)
	// Lock order: acquire this AFTER GlobalPoolerDiscovery.mu (never before)
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

						// Process the change - this handles both updates and deletions.
						// Deletions come as events with Err set to NoNode error.
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

	// Save old poolers to detect removals (for watch reconnection)
	oldPoolers := pd.poolers
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
			pd.logger.Debug("Initial pooler discovered",
				"id", poolerID,
				"hostname", pooler.Hostname,
				"addr", pooler.Addr(),
				"database", pooler.Database,
				"shard", pooler.Shard,
				"type", pooler.Type.String())
		}
	}

	// Notify about removed poolers (existed before but not in new state)
	if pd.onPoolerRemoved != nil {
		for poolerID, oldPooler := range oldPoolers {
			if _, exists := pd.poolers[poolerID]; !exists {
				pd.onPoolerRemoved(oldPooler.MultiPooler)
			}
		}
	}

	// Notify about all current poolers
	if pd.onPoolerChanged != nil {
		for _, pooler := range pd.poolers {
			pd.onPoolerChanged(pooler.MultiPooler)
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

	// Check if this is an error event
	if watchData.Err != nil {
		// Check if it's a deletion event (NoNode error) for a pooler path.
		// TopoError is returned as a value type, so we need to use errors.Is with a pointer.
		if errors.Is(watchData.Err, &topoclient.TopoError{Code: topoclient.NoNode}) {
			if strings.HasSuffix(watchData.Path, "/Pooler") {
				poolerID := pd.extractPoolerIDFromPath(watchData.Path)
				if poolerID != "" {
					if oldPooler, existed := pd.poolers[poolerID]; existed {
						delete(pd.poolers, poolerID)
						pd.lastRefresh = time.Now()
						pd.logger.Info("Pooler removed",
							"id", poolerID,
							"path", watchData.Path)
						if pd.onPoolerRemoved != nil {
							pd.onPoolerRemoved(oldPooler.MultiPooler)
						}
					}
				}
			}
		} else {
			// Log other watch errors (non-deletion errors)
			pd.logger.Warn("Watch error received", "error", watchData.Err, "path", watchData.Path)
		}
		return
	}

	// Parse the pooler from watch data
	pooler, err := pd.parsePoolerFromWatchData(watchData)
	if err != nil {
		pd.logger.Warn("Failed to parse pooler from change data", "path", watchData.Path, "error", err)
		return
	}

	if pooler == nil {
		// This is a non-pooler file, skip
		pd.logger.Debug("Skipping non-pooler file", "path", watchData.Path)
		return
	}

	poolerID := topoclient.MultiPoolerIDString(pooler.Id)

	// If this is a PRIMARY, evict any other PRIMARY for the same TableGroup/Shard.
	// This handles the failover case where a new PRIMARY comes up but the old
	// crashed PRIMARY's record is still present.
	if pooler.Type == clustermetadatapb.PoolerType_PRIMARY {
		pd.evictConflictingPrimary(poolerID, pooler.TableGroup, pooler.Shard)
	}

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

	if pd.onPoolerChanged != nil {
		pd.onPoolerChanged(pooler.MultiPooler)
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

// extractPoolerIDFromPath extracts the pooler ID from a watch path.
// The path format is: "poolers/{pooler_id}/Pooler"
func (pd *CellPoolerDiscovery) extractPoolerIDFromPath(path string) string {
	// Expected format: "poolers/{pooler_id}/Pooler"
	parts := strings.Split(path, "/")
	if len(parts) >= 3 && parts[0] == "poolers" && parts[len(parts)-1] == "Pooler" {
		// The pooler ID is the middle part(s) - everything between "poolers/" and "/Pooler"
		return strings.Join(parts[1:len(parts)-1], "/")
	}
	return ""
}

// evictConflictingPrimary removes any existing PRIMARY pooler for the same
// TableGroup/Shard that has a different pooler ID. This handles the failover
// case where a new PRIMARY comes up but the old crashed PRIMARY's record is
// still in the discovery cache.
// Caller must hold pd.mu.
func (pd *CellPoolerDiscovery) evictConflictingPrimary(newPoolerID, tableGroup, shard string) {
	for poolerID, pooler := range pd.poolers {
		// Skip if it's the same pooler (update case)
		if poolerID == newPoolerID {
			continue
		}
		// Check if this is a PRIMARY for the same TableGroup/Shard
		if pooler.Type == clustermetadatapb.PoolerType_PRIMARY &&
			pooler.TableGroup == tableGroup &&
			pooler.Shard == shard {
			delete(pd.poolers, poolerID)
			pd.logger.Info("Evicted stale PRIMARY pooler",
				"evicted_id", poolerID,
				"new_primary_id", newPoolerID,
				"tableGroup", tableGroup,
				"shard", shard)
			if pd.onPoolerRemoved != nil {
				pd.onPoolerRemoved(pooler.MultiPooler)
			}
		}
	}
}

// Cell returns the cell this discovery is watching.
func (pd *CellPoolerDiscovery) Cell() string {
	return pd.cell
}

// PoolerChangeListener receives notifications about pooler discovery changes.
// Implementations can use this to maintain connections to discovered poolers.
type PoolerChangeListener interface {
	// OnPoolerChanged is called when a pooler is added or updated.
	// For new poolers, this creates a connection. For existing poolers,
	// this may recreate the connection if the address changed.
	OnPoolerChanged(pooler *clustermetadatapb.MultiPooler)
	// OnPoolerRemoved is called when a pooler is removed from discovery.
	OnPoolerRemoved(pooler *clustermetadatapb.MultiPooler)
}

// poolerNotification represents a notification to be delivered to listeners.
type poolerNotification struct {
	pooler         *clustermetadatapb.MultiPooler
	isRemoval      bool
	targetListener PoolerChangeListener // nil = broadcast to all

	// Special: listener registration
	isListenerRegistration bool
	newListener            PoolerChangeListener
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

	// State (protected by mu)
	// Lock order: acquire this BEFORE CellPoolerDiscovery.mu
	mu              sync.Mutex
	cellWatchers    map[string]*CellPoolerDiscovery // cell name -> cell watcher
	lastCellRefresh time.Time                       // when cells were last discovered/refreshed

	// Listeners for pooler changes (protected by listenersMu)
	// Lock order: can acquire notificationsMu while holding this
	listenersMu sync.Mutex
	listeners   []PoolerChangeListener

	// Notification queue (protected by notificationsMu)
	// LOCK ORDERING: This is the INNERMOST lock - never acquire other locks while holding it.
	// It CAN be acquired while holding mu, listenersMu, or CellPoolerDiscovery.mu.
	notificationsMu sync.Mutex
	notifications   []poolerNotification
	notifySignal    chan struct{} // buffered(1) to wake up processor goroutine
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
		notifySignal: make(chan struct{}, 1),
	}
}

// Start begins the discovery process by watching the cells directory in the
// global topology. When cells are created or removed, it starts or stops
// the corresponding CellPoolerDiscovery watchers.
func (gd *GlobalPoolerDiscovery) Start() {
	// Start notification processor goroutine
	gd.wg.Go(gd.processNotifications)

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
				conn, err := gd.topoStore.ConnForCell(gd.ctx, topoclient.GlobalCell)
				if err != nil {
					gd.logger.Error("Failed to get global topo connection", "error", err)
					return
				}

				initial, changes, err := conn.WatchRecursive(gd.ctx, topoclient.CellsPath)
				if err != nil {
					gd.logger.Error("Failed to start watch on cells", "error", err)
					return
				}

				// Process initial cells
				gd.processInitialCells(initial)

				// Reset backoff after watch has been stable for 30s
				resetTimer := time.AfterFunc(30*time.Second, func() {
					r.Reset()
				})
				defer resetTimer.Stop()

				// Watch for cell changes (additions/removals)
				for {
					select {
					case <-gd.ctx.Done():
						return
					case event, ok := <-changes:
						if !ok {
							gd.logger.Info("Cell watch channel closed, will reconnect")
							return
						}
						gd.processCellChange(event)
					}
				}
			}()
		}
	})
}

// processInitialCells processes the initial set of cells from the watch.
func (gd *GlobalPoolerDiscovery) processInitialCells(initial []*topoclient.WatchDataRecursive) {
	gd.mu.Lock()
	defer gd.mu.Unlock()

	for _, event := range initial {
		if event.Err != nil {
			continue
		}
		cell := extractCellFromPath(event.Path)
		if cell == "" {
			continue
		}
		if _, exists := gd.cellWatchers[cell]; !exists {
			gd.startCellWatcher(cell)
		}
	}

	// Log discovered cells
	cells := make([]string, 0, len(gd.cellWatchers))
	for cell := range gd.cellWatchers {
		cells = append(cells, cell)
	}
	gd.lastCellRefresh = time.Now()
	gd.logger.Info("Initial cell discovery completed", "cells", cells)
}

// processCellChange handles a single cell change event from the watch.
func (gd *GlobalPoolerDiscovery) processCellChange(event *topoclient.WatchDataRecursive) {
	cell := extractCellFromPath(event.Path)
	if cell == "" {
		return
	}

	// Deletion: NoNode error signals the cell was removed
	if event.Err != nil {
		if errors.Is(event.Err, &topoclient.TopoError{Code: topoclient.NoNode}) {
			gd.mu.Lock()
			if watcher, exists := gd.cellWatchers[cell]; exists {
				gd.logger.Info("Cell removed, stopping watcher", "cell", cell)
				watcher.Stop()
				delete(gd.cellWatchers, cell)
				gd.lastCellRefresh = time.Now()
			}
			gd.mu.Unlock()
		} else {
			gd.logger.Warn("Cell watch error received", "error", event.Err, "path", event.Path)
		}
		return
	}

	// Addition: start a watcher if we don't have one
	gd.mu.Lock()
	if _, exists := gd.cellWatchers[cell]; !exists {
		gd.logger.Info("New cell discovered", "cell", cell)
		gd.startCellWatcher(cell)
		gd.lastCellRefresh = time.Now()
	}
	gd.mu.Unlock()
}

// extractCellFromPath extracts the cell name from a cells watch path.
// The path format is: "cells/<cellname>/Cell"
func extractCellFromPath(path string) string {
	parts := strings.Split(path, "/")
	if len(parts) >= 2 && parts[0] == topoclient.CellsPath {
		return parts[1]
	}
	return ""
}

// startCellWatcher starts a CellPoolerDiscovery for the given cell.
// Caller must hold gd.mu.
func (gd *GlobalPoolerDiscovery) startCellWatcher(cell string) {
	gd.logger.Info("Starting cell watcher", "cell", cell)

	cellWatcher := NewCellPoolerDiscovery(gd.ctx, gd.topoStore, cell, gd.logger)
	cellWatcher.onPoolerChanged = gd.notifyPoolerChanged
	cellWatcher.onPoolerRemoved = gd.notifyPoolerRemoved
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

// processNotifications runs in a background goroutine and processes queued notifications.
func (gd *GlobalPoolerDiscovery) processNotifications() {
	for {
		select {
		case <-gd.ctx.Done():
			return

		case <-gd.notifySignal:
			// Drain and process all pending notifications
			for {
				gd.notificationsMu.Lock()
				if len(gd.notifications) == 0 {
					gd.notificationsMu.Unlock()
					break // Buffer empty, go back to waiting
				}

				pending := gd.notifications
				gd.notifications = nil
				gd.notificationsMu.Unlock()

				// Process notifications
				for _, notif := range pending {
					if notif.isListenerRegistration {
						// Add listener in serial order with notifications
						gd.listenersMu.Lock()
						gd.listeners = append(gd.listeners, notif.newListener)
						gd.listenersMu.Unlock()
						continue
					}

					if notif.targetListener != nil {
						// Targeted replay - only call specific listener
						if notif.isRemoval {
							notif.targetListener.OnPoolerRemoved(notif.pooler)
						} else {
							notif.targetListener.OnPoolerChanged(notif.pooler)
						}
					} else {
						// Broadcast - re-read listeners to include recently-registered ones
						gd.listenersMu.Lock()
						currentListeners := gd.listeners
						gd.listenersMu.Unlock()

						for _, listener := range currentListeners {
							if notif.isRemoval {
								listener.OnPoolerRemoved(notif.pooler)
							} else {
								listener.OnPoolerChanged(notif.pooler)
							}
						}
					}
				}
			}
		}
	}
}

// queueNotification adds a notification to the queue and signals the processor.
func (gd *GlobalPoolerDiscovery) queueNotification(notif poolerNotification) {
	gd.notificationsMu.Lock()
	gd.notifications = append(gd.notifications, notif)
	gd.notificationsMu.Unlock()

	// Wake up processor (non-blocking - buffered channel)
	select {
	case gd.notifySignal <- struct{}{}:
	default: // Already signaled, processor will drain buffer
	}
}

// RegisterListener adds a listener for pooler change notifications.
// The listener will immediately receive OnPoolerChanged for all currently
// known poolers, then continue to receive updates as poolers change.
func (gd *GlobalPoolerDiscovery) RegisterListener(listener PoolerChangeListener) {
	// Collect current state and queue registration+replay atomically
	// while holding gd.mu to prevent poolers from changing between
	// collection and queuing
	gd.mu.Lock()

	// Collect replay notifications
	var replay []poolerNotification
	for _, watcher := range gd.cellWatchers {
		watcher.mu.Lock()
		for _, pooler := range watcher.poolers {
			replay = append(replay, poolerNotification{
				pooler:         pooler.MultiPooler,
				targetListener: listener,
			})
		}
		watcher.mu.Unlock()
	}

	// Queue: (1) add listener, (2) replay state
	// This must happen while still holding gd.mu to ensure atomicity
	gd.notificationsMu.Lock()
	gd.notifications = append(gd.notifications, poolerNotification{
		isListenerRegistration: true,
		newListener:            listener,
	})
	gd.notifications = append(gd.notifications, replay...)
	gd.notificationsMu.Unlock()

	gd.mu.Unlock()

	// Signal processor outside locks
	select {
	case gd.notifySignal <- struct{}{}:
	default:
	}
}

// notifyPoolerChanged notifies all listeners that a pooler was added or updated.
func (gd *GlobalPoolerDiscovery) notifyPoolerChanged(pooler *clustermetadatapb.MultiPooler) {
	gd.queueNotification(poolerNotification{
		pooler:         pooler,
		targetListener: nil, // Broadcast to all
	})
}

// notifyPoolerRemoved notifies all listeners that a pooler was removed.
func (gd *GlobalPoolerDiscovery) notifyPoolerRemoved(pooler *clustermetadatapb.MultiPooler) {
	gd.queueNotification(poolerNotification{
		pooler:         pooler,
		isRemoval:      true,
		targetListener: nil, // Broadcast to all
	})
}

// PoolerCount returns the total number of discovered poolers across all cells.
// LastCellRefresh returns when cells were last discovered or refreshed.
// Returns zero time if initial cell discovery has not completed yet.
func (gd *GlobalPoolerDiscovery) LastCellRefresh() time.Time {
	gd.mu.Lock()
	defer gd.mu.Unlock()
	return gd.lastCellRefresh
}

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
