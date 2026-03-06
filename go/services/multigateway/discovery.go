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

// TODO: Delete GlobalPoolerDiscovery and use topoclient.PoolerCache directly throughout
// the gateway. The PoolerChangeListener interface can be replaced with a plain func adapter.

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
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

// GlobalPoolerDiscovery is a thin wrapper around topoclient.PoolerCache that adds
// a typed PoolerChangeListener fanout.
//
// All state management (pooler map, cell refresh tracking, reconnect reconciliation,
// proto.Equal suppression) is handled by the underlying PoolerCache.
type GlobalPoolerDiscovery struct {
	cache     *topoclient.PoolerCache
	localCell string
	logger    *slog.Logger

	listenersMu sync.Mutex
	listeners   []PoolerChangeListener

	unsub func()
}

// NewGlobalPoolerDiscovery creates a new global pooler discovery service.
// The localCell parameter indicates which cell this multigateway is running in,
// which will be used for cell affinity when selecting poolers.
func NewGlobalPoolerDiscovery(ctx context.Context, topoStore topoclient.Store, localCell string, logger *slog.Logger) *GlobalPoolerDiscovery {
	return &GlobalPoolerDiscovery{
		cache:     topoclient.NewPoolerCache(ctx, topoStore, logger),
		localCell: localCell,
		logger:    logger,
	}
}

// Start begins the discovery process.
func (gd *GlobalPoolerDiscovery) Start() {
	gd.unsub = gd.cache.Subscribe(gd.onCacheChange)
	gd.cache.Start()
}

// Stop stops the discovery service.
func (gd *GlobalPoolerDiscovery) Stop() {
	gd.unsub()
	gd.cache.Stop()
}

// onCacheChange is called by the cache's delivery goroutine for all pooler events
// and forwards them to registered listeners.
func (gd *GlobalPoolerDiscovery) onCacheChange(pooler *clustermetadatapb.MultiPooler, removed bool) {
	gd.listenersMu.Lock()
	ls := gd.listeners
	gd.listenersMu.Unlock()

	for _, l := range ls {
		if removed {
			l.OnPoolerRemoved(pooler)
		} else {
			l.OnPoolerChanged(pooler)
		}
	}
}

// RegisterListener adds a listener for pooler change notifications.
// The listener will receive OnPoolerChanged for all currently known poolers,
// then continue to receive updates as poolers change.
//
// The snapshot replay is serialized with concurrent cache events: holding the lock
// blocks onCacheChange from running while we snapshot, ensuring the replayed state
// is consistent with the cache. OnPoolerChanged is assumed to be idempotent —
// a duplicate notification may be delivered if a live event races the replay.
func (gd *GlobalPoolerDiscovery) RegisterListener(listener PoolerChangeListener) {
	// Hold the lock while appending and snapshotting so that concurrent
	// onCacheChange calls are serialized with this registration.
	gd.listenersMu.Lock()
	gd.listeners = append(gd.listeners, listener)
	snapshot := gd.cache.All()
	gd.listenersMu.Unlock()

	for _, p := range snapshot {
		listener.OnPoolerChanged(p)
	}
}

// LastCellRefresh returns the time of the most recent watch event across all cells.
// Returns zero time if no cell has been discovered yet.
func (gd *GlobalPoolerDiscovery) LastCellRefresh() time.Time {
	var latest time.Time
	for _, s := range gd.cache.CellStatuses() {
		if s.LastActivity.After(latest) {
			latest = s.LastActivity
		}
	}
	return latest
}

// PoolerCount returns the total number of discovered poolers across all cells.
func (gd *GlobalPoolerDiscovery) PoolerCount() int {
	return gd.cache.Count()
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
	raw := gd.cache.CellStatuses()
	out := make([]CellStatusInfo, len(raw))
	for i, s := range raw {
		out[i] = CellStatusInfo{
			Cell:        s.Cell,
			LastRefresh: s.LastActivity,
			Poolers:     s.Poolers,
		}
	}
	return out
}
