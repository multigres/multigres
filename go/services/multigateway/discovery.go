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
	"sync"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"

	"google.golang.org/protobuf/proto"
)

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

// GlobalPoolerDiscovery discovers multipoolers across all cells and notifies
// registered listeners of changes. It uses WatchAllPoolersWithRetry internally
// to manage cell discovery and per-cell pooler watching.
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
	mu              sync.Mutex
	poolers         map[string]*clustermetadatapb.MultiPooler // poolerID -> pooler, all cells
	cellLastRefresh map[string]time.Time                      // cell -> time of last initial snapshot

	// Listeners for pooler changes (protected by listenersMu)
	// Lock order: can acquire notificationsMu while holding this
	listenersMu sync.Mutex
	listeners   []PoolerChangeListener

	// Notification queue (protected by notificationsMu)
	// LOCK ORDERING: This is the INNERMOST lock - never acquire other locks while holding it.
	// It CAN be acquired while holding mu, listenersMu.
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
		topoStore:       topoStore,
		localCell:       localCell,
		logger:          logger,
		ctx:             discoveryCtx,
		cancelFunc:      cancel,
		poolers:         make(map[string]*clustermetadatapb.MultiPooler),
		cellLastRefresh: make(map[string]time.Time),
		notifySignal:    make(chan struct{}, 1),
	}
}

// Start begins the discovery process.
func (gd *GlobalPoolerDiscovery) Start() {
	gd.wg.Go(gd.processNotifications)

	gd.wg.Go(func() {
		gd.logger.Info("Starting global pooler discovery")
		topoclient.WatchAllPoolersWithRetry(gd.ctx, gd.topoStore, gd.logger,
			gd.onInitialCell, gd.onPoolerUpserted, gd.onPoolerDeleted, gd.onCellRemoved,
		)
		gd.logger.Info("Global pooler discovery shutting down")
	})
}

// Stop stops the discovery service.
func (gd *GlobalPoolerDiscovery) Stop() {
	gd.cancelFunc()
	gd.wg.Wait()
}

// onInitialCell reconciles the in-memory pooler set for a cell against the
// snapshot delivered on each (re)connection. It removes stale entries for the
// cell, adds the new snapshot, and notifies listeners.
func (gd *GlobalPoolerDiscovery) onInitialCell(cell string, poolers []*clustermetadatapb.MultiPooler) {
	gd.mu.Lock()

	newCellPoolers := make(map[string]*clustermetadatapb.MultiPooler, len(poolers))
	for _, p := range poolers {
		newCellPoolers[topoclient.MultiPoolerIDString(p.Id)] = p
	}

	// Detect and remove stale entries for this cell
	var removed []*clustermetadatapb.MultiPooler
	for id, p := range gd.poolers {
		if p.Id.Cell == cell {
			if _, stillPresent := newCellPoolers[id]; !stillPresent {
				delete(gd.poolers, id)
				removed = append(removed, p)
			}
		}
	}

	// Add new snapshot entries; only notify for poolers whose data actually changed.
	var changed []*clustermetadatapb.MultiPooler
	for id, p := range newCellPoolers {
		if existing, ok := gd.poolers[id]; !ok || !proto.Equal(existing, p) {
			changed = append(changed, p)
		}
		gd.poolers[id] = p
	}

	gd.cellLastRefresh[cell] = time.Now()
	gd.mu.Unlock()

	// Notify outside lock
	for _, p := range removed {
		gd.notifyPoolerRemoved(p)
	}
	for _, p := range changed {
		gd.notifyPoolerChanged(p)
	}

	gd.logger.Info("Initial pooler discovery completed", "cell", cell, "pooler_count", len(newCellPoolers))
}

// onCellRemoved handles cell removal by evicting all poolers for that cell and
// clearing its refresh record.
func (gd *GlobalPoolerDiscovery) onCellRemoved(cell string) {
	gd.mu.Lock()

	var removed []*clustermetadatapb.MultiPooler
	for id, p := range gd.poolers {
		if p.Id.Cell == cell {
			delete(gd.poolers, id)
			removed = append(removed, p)
		}
	}
	delete(gd.cellLastRefresh, cell)

	gd.mu.Unlock()

	for _, p := range removed {
		gd.notifyPoolerRemoved(p)
	}
}

// onPoolerUpserted handles a pooler add or update event.
func (gd *GlobalPoolerDiscovery) onPoolerUpserted(pooler *clustermetadatapb.MultiPooler) {
	poolerID := topoclient.MultiPoolerIDString(pooler.Id)

	gd.mu.Lock()

	// If this is a PRIMARY, evict any other PRIMARY for the same TableGroup/Shard.
	// This handles failover where a new PRIMARY comes up before the old crashed
	// PRIMARY's record is removed.
	var evicted []*clustermetadatapb.MultiPooler
	if pooler.Type == clustermetadatapb.PoolerType_PRIMARY {
		for id, p := range gd.poolers {
			if id != poolerID &&
				p.Type == clustermetadatapb.PoolerType_PRIMARY &&
				p.TableGroup == pooler.TableGroup &&
				p.Shard == pooler.Shard {
				delete(gd.poolers, id)
				evicted = append(evicted, p)
				gd.logger.Info("Evicted stale PRIMARY pooler",
					"evicted_id", id,
					"new_primary_id", poolerID,
					"tableGroup", pooler.TableGroup,
					"shard", pooler.Shard)
			}
		}
	}

	_, existed := gd.poolers[poolerID]
	gd.poolers[poolerID] = pooler
	gd.mu.Unlock()

	for _, p := range evicted {
		gd.notifyPoolerRemoved(p)
	}

	if !existed {
		gd.logger.Info("New pooler discovered",
			"id", poolerID,
			"hostname", pooler.Hostname,
			"tableGroup", pooler.TableGroup,
			"database", pooler.Database,
			"shard", pooler.Shard,
			"type", pooler.Type.String())
	} else {
		gd.logger.Info("Pooler updated",
			"id", poolerID,
			"hostname", pooler.Hostname,
			"tableGroup", pooler.TableGroup,
			"database", pooler.Database,
			"shard", pooler.Shard,
			"type", pooler.Type.String())
	}

	gd.notifyPoolerChanged(pooler)
}

// onPoolerDeleted handles a pooler deletion event.
func (gd *GlobalPoolerDiscovery) onPoolerDeleted(poolerID string) {
	gd.mu.Lock()
	p, existed := gd.poolers[poolerID]
	if existed {
		delete(gd.poolers, poolerID)
	}
	gd.mu.Unlock()

	if existed {
		gd.logger.Info("Pooler removed", "id", poolerID)
		gd.notifyPoolerRemoved(p)
	}
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
	// Collect current state and queue registration+replay atomically while holding gd.mu
	// to prevent poolers from changing between collection and queuing.
	gd.mu.Lock()

	var replay []poolerNotification
	for _, pooler := range gd.poolers {
		replay = append(replay, poolerNotification{
			pooler:         pooler,
			targetListener: listener,
		})
	}

	// Queue: (1) add listener, (2) replay state
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

// LastCellRefresh returns the time of the most recent initial snapshot delivery
// across all cells. Returns zero time if no cell has been discovered yet.
func (gd *GlobalPoolerDiscovery) LastCellRefresh() time.Time {
	gd.mu.Lock()
	defer gd.mu.Unlock()

	var latest time.Time
	for _, t := range gd.cellLastRefresh {
		if t.After(latest) {
			latest = t
		}
	}
	return latest
}

// PoolerCount returns the total number of discovered poolers across all cells.
func (gd *GlobalPoolerDiscovery) PoolerCount() int {
	gd.mu.Lock()
	defer gd.mu.Unlock()
	return len(gd.poolers)
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

	// Group poolers by cell
	byCell := make(map[string][]*clustermetadatapb.MultiPooler)
	for _, p := range gd.poolers {
		cell := p.Id.Cell
		byCell[cell] = append(byCell[cell], proto.Clone(p).(*clustermetadatapb.MultiPooler))
	}

	// Include cells with no poolers but a recorded refresh time
	for cell := range gd.cellLastRefresh {
		if _, exists := byCell[cell]; !exists {
			byCell[cell] = nil
		}
	}

	// Sort cell names for consistent ordering
	cellNames := make([]string, 0, len(byCell))
	for cell := range byCell {
		cellNames = append(cellNames, cell)
	}
	sort.Strings(cellNames)

	statuses := make([]CellStatusInfo, 0, len(cellNames))
	for _, cell := range cellNames {
		poolers := byCell[cell]
		sort.Slice(poolers, func(i, j int) bool {
			return topoclient.MultiPoolerIDString(poolers[i].Id) < topoclient.MultiPoolerIDString(poolers[j].Id)
		})
		statuses = append(statuses, CellStatusInfo{
			Cell:        cell,
			LastRefresh: gd.cellLastRefresh[cell],
			Poolers:     poolers,
		})
	}
	return statuses
}
