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
	"sort"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// CellStatus holds discovery status for a single cell.
type CellStatus struct {
	Cell         string
	LastActivity time.Time // time of the most recent watch event from this cell
	Poolers      []*clustermetadatapb.MultiPooler
}

// topoWatch maintains a typesafe in-memory copy of all poolers discovered across all
// cells via watchAllPoolersWithRetry. Reads never touch the network.
//
// Subscriptions are delivered serially by a background goroutine outside any lock, so
// handlers may safely call back into the cache. Delivery order is guaranteed: for any
// pooler, events are emitted in the order they were received from the topology watch.
//
// Per-cell activity timestamps are updated whenever any watch event (upsert, delete, or
// initial snapshot) arrives from a cell. Enabling etcd ProgressNotify on the underlying
// WatchRecursive calls would allow updating these timestamps even for quiet cells,
// providing a stronger freshness guarantee — that is a planned follow-up.
type topoWatch struct {
	store  topoclient.ConnProvider
	logger *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// mu protects poolers, byCell, and cellLastActivity.
	// It may be acquired while holding notifMu.
	// It must NOT be held while calling notify.
	mu               sync.Mutex
	poolers          map[topoclient.ComponentID]*clustermetadatapb.MultiPooler            // poolerID → pooler
	byCell           map[string]map[topoclient.ComponentID]*clustermetadatapb.MultiPooler // cell → poolerID → pooler
	cellLastActivity map[string]time.Time

	// Notification queue (protected by notifMu).
	// LOCK ORDERING: notifMu is the innermost lock — never acquire mu while holding notifMu.
	// notifMu CAN be acquired while holding mu (to enqueue under the state snapshot).
	notifMu    sync.Mutex
	notifQueue []cacheNotif
	notifCh    chan struct{} // buffered(1), wakes delivery goroutine

	// broadcaster lets Sync(ctx) ask every per-cell watcher goroutine to drain
	// its in-flight `changes` events before the delivery sentinel is enqueued,
	// closing the race where a sentinel could be appended ahead of a
	// not-yet-read watch event.
	broadcaster *cellSyncBroadcaster
}

// cacheNotif is a pending notification in the delivery queue.
//
// For pooler events, prev and curr encode the transition:
//   - prev == nil, curr != nil → insert (first-time discovery)
//   - prev != nil, curr != nil → update (proto.Equal already suppressed)
//   - prev != nil, curr == nil → delete
//
// prev is captured atomically with the cache mutation that produced this event,
// so subscribers receive an accurate transition pair in strict FIFO order.
type cacheNotif struct {
	prev *clustermetadatapb.MultiPooler
	curr *clustermetadatapb.MultiPooler

	// Subscription management (mutually exclusive with pooler notifications).
	isSubscribe   bool
	isUnsubscribe bool
	sub           *cacheSubscription

	// isReplay marks a targeted replay notification delivered only to sub
	// (used when a new subscriber catches up to the current state).
	isReplay bool

	// isSync marks a sentinel used by Sync(); the delivery goroutine closes
	// syncDone when it reaches this entry, after dispatching every event ahead
	// of it. Mutually exclusive with all other variants.
	isSync   bool
	syncDone chan struct{}
}

// ChangeFn receives a pooler-state transition observed by the cache.
//
//   - prev == nil → first-time insert (curr is the new pooler)
//   - curr == nil → deletion (prev is the last-known pooler)
//   - both set    → update (the cache already suppresses proto.Equal no-ops)
//
// Callbacks are invoked synchronously on topoWatch's single delivery goroutine
// in strict FIFO order. Slow callbacks delay subsequent events for all subscribers.
type ChangeFn func(prev, curr *clustermetadatapb.MultiPooler)

// cacheSubscription is a registered change handler.
type cacheSubscription struct {
	fn ChangeFn
}

// newTopoWatch creates a new topoWatch. Call Start to begin watching.
func newTopoWatch(ctx context.Context, store topoclient.ConnProvider, logger *slog.Logger) *topoWatch {
	cacheCtx, cancel := context.WithCancel(ctx)
	return &topoWatch{
		store:            store,
		logger:           logger,
		ctx:              cacheCtx,
		cancel:           cancel,
		poolers:          make(map[topoclient.ComponentID]*clustermetadatapb.MultiPooler),
		byCell:           make(map[string]map[topoclient.ComponentID]*clustermetadatapb.MultiPooler),
		cellLastActivity: make(map[string]time.Time),
		notifCh:          make(chan struct{}, 1),
		broadcaster:      newCellSyncBroadcaster(),
	}
}

// Start launches the watch and notification delivery goroutines.
func (c *topoWatch) Start() {
	c.wg.Go(c.deliverNotifications)
	c.wg.Go(func() {
		watchAllPoolersWithRetry(c.ctx, c.store, c.logger, c.broadcaster,
			c.onInitialCell,
			c.onUpserted,
			c.onDeleted,
			c.onCellRemoved,
		)
	})
}

// Stop cancels the watch and waits for all background goroutines to exit.
func (c *topoWatch) Stop() {
	c.cancel()
	c.wg.Wait()
}

// Get returns the pooler with the given ID (as returned by ComponentIDString).
func (c *topoWatch) Get(id topoclient.ComponentID) (*clustermetadatapb.MultiPooler, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, ok := c.poolers[id]
	return p, ok
}

// All returns a snapshot of all currently known poolers.
func (c *topoWatch) All() []*clustermetadatapb.MultiPooler {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*clustermetadatapb.MultiPooler, 0, len(c.poolers))
	for _, p := range c.poolers {
		out = append(out, p)
	}
	return out
}

// AllForCell returns a snapshot of all poolers in the given cell.
func (c *topoWatch) AllForCell(cell string) []*clustermetadatapb.MultiPooler {
	c.mu.Lock()
	defer c.mu.Unlock()
	cellMap := c.byCell[cell]
	out := make([]*clustermetadatapb.MultiPooler, 0, len(cellMap))
	for _, p := range cellMap {
		out = append(out, p)
	}
	return out
}

// Count returns the number of currently known poolers.
func (c *topoWatch) Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.poolers)
}

// CellStatuses returns per-cell status sorted alphabetically by cell name.
// Intended for admin/status pages, not the hot query path.
func (c *topoWatch) CellStatuses() []CellStatus {
	c.mu.Lock()
	defer c.mu.Unlock()

	cellSet := make(map[string]struct{}, len(c.byCell))
	for cell := range c.byCell {
		cellSet[cell] = struct{}{}
	}
	for cell := range c.cellLastActivity {
		cellSet[cell] = struct{}{}
	}

	cellNames := make([]string, 0, len(cellSet))
	for cell := range cellSet {
		cellNames = append(cellNames, cell)
	}
	sort.Strings(cellNames)

	statuses := make([]CellStatus, 0, len(cellNames))
	for _, cell := range cellNames {
		var poolers []*clustermetadatapb.MultiPooler
		for _, p := range c.byCell[cell] {
			poolers = append(poolers, proto.Clone(p).(*clustermetadatapb.MultiPooler))
		}
		sort.Slice(poolers, func(i, j int) bool {
			return topoclient.ComponentIDString(poolers[i].Id) < topoclient.ComponentIDString(poolers[j].Id)
		})
		statuses = append(statuses, CellStatus{
			Cell:         cell,
			LastActivity: c.cellLastActivity[cell],
			Poolers:      poolers,
		})
	}
	return statuses
}

// Subscribe registers fn to be called on every pooler change or removal.
// Before returning, it enqueues targeted replay notifications so fn receives the current
// state before any subsequent broadcasts. No callbacks are made under any lock.
//
// The returned function unsubscribes fn and is safe to call from any goroutine.
func (c *topoWatch) Subscribe(fn ChangeFn) func() {
	sub := &cacheSubscription{fn: fn}

	// Hold mu while collecting the replay snapshot and enqueueing both the subscription
	// registration and replay notifications. This prevents a concurrent upsert from
	// appearing between the snapshot and the replay in the delivery queue.
	//
	// Replay events are delivered with prev == nil so subscribers see them as
	// first-time inserts, which matches the semantics from their point of view.
	c.mu.Lock()
	replay := make([]cacheNotif, 0, len(c.poolers))
	for _, p := range c.poolers {
		replay = append(replay, cacheNotif{curr: p, sub: sub, isReplay: true})
	}
	c.notifMu.Lock()
	c.notifQueue = append(c.notifQueue, cacheNotif{isSubscribe: true, sub: sub})
	c.notifQueue = append(c.notifQueue, replay...)
	c.notifMu.Unlock()
	c.mu.Unlock()

	c.wake()

	var once sync.Once
	return func() {
		once.Do(func() {
			c.notifMu.Lock()
			c.notifQueue = append(c.notifQueue, cacheNotif{isUnsubscribe: true, sub: sub})
			c.notifMu.Unlock()
			c.wake()
		})
	}
}

// Sync blocks until every event that was already observable from the topology
// at the time of the call has been dispatched to all current subscribers.
//
// It works in two phases:
//
//  1. Broadcast a sync request to every per-cell watcher goroutine. Each
//     watcher drains its `changes` channel (firing onUpserted/onDeleted for
//     every event already buffered there) before acking. This closes the race
//     where a topology write has been delivered to the watch channel but the
//     watcher hasn't yet read it.
//
//  2. Append a sync sentinel to the delivery queue and wait for it to drain.
//     Because phase 1 guarantees that every notification triggered by
//     pre-call topology state is already in the queue ahead of the sentinel,
//     this delivers exactly those notifications before returning.
//
// Sync is intended for use in tests to replace time.Sleep barriers after
// topology mutations. Production code should generally subscribe and react.
func (c *topoWatch) Sync(ctx context.Context) error {
	if err := c.broadcaster.syncAll(ctx); err != nil {
		return err
	}

	done := make(chan struct{})
	c.notifMu.Lock()
	c.notifQueue = append(c.notifQueue, cacheNotif{isSync: true, syncDone: done})
	c.notifMu.Unlock()
	c.wake()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// wake signals the delivery goroutine without blocking.
func (c *topoWatch) wake() {
	select {
	case c.notifCh <- struct{}{}:
	default:
	}
}

// deliverNotifications runs in a background goroutine, processing queued notifications
// in order. It maintains its own subs list to avoid any lock during delivery.
func (c *topoWatch) deliverNotifications() {
	var subs []*cacheSubscription
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.notifCh:
			for {
				c.notifMu.Lock()
				if len(c.notifQueue) == 0 {
					c.notifMu.Unlock()
					break
				}
				pending := c.notifQueue
				c.notifQueue = nil
				c.notifMu.Unlock()

				for _, n := range pending {
					switch {
					case n.isSubscribe:
						subs = append(subs, n.sub)
					case n.isUnsubscribe:
						for i, s := range subs {
							if s == n.sub {
								subs = append(subs[:i], subs[i+1:]...)
								break
							}
						}
					case n.isReplay:
						// Targeted replay: only delivered to the registering subscriber.
						n.sub.fn(n.prev, n.curr)
					case n.isSync:
						close(n.syncDone)
					default:
						// Broadcast to all current subscribers.
						for _, s := range subs {
							s.fn(n.prev, n.curr)
						}
					}
				}
			}
		}
	}
}

// addToCell inserts a pooler into the byCell secondary index. Must hold mu.
func (c *topoWatch) addToCell(id topoclient.ComponentID, p *clustermetadatapb.MultiPooler) {
	cell := p.Id.Cell
	if c.byCell[cell] == nil {
		c.byCell[cell] = make(map[topoclient.ComponentID]*clustermetadatapb.MultiPooler)
	}
	c.byCell[cell][id] = p
}

// removeFromCell removes a pooler from the byCell secondary index. Must hold mu.
func (c *topoWatch) removeFromCell(id topoclient.ComponentID, cell string) {
	delete(c.byCell[cell], id)
	if len(c.byCell[cell]) == 0 {
		delete(c.byCell, cell)
	}
}

// onInitialCell reconciles the pooler map for a cell against the new snapshot.
func (c *topoWatch) onInitialCell(cell string, poolers []*clustermetadatapb.MultiPooler) {
	newCellPoolers := make(map[topoclient.ComponentID]*clustermetadatapb.MultiPooler, len(poolers))
	for _, p := range poolers {
		newCellPoolers[topoclient.ComponentIDString(p.Id)] = p
	}

	c.mu.Lock()

	var pendingNotifs []cacheNotif

	for id, p := range c.poolers {
		if p.Id.Cell == cell {
			if _, stillPresent := newCellPoolers[id]; !stillPresent {
				delete(c.poolers, id)
				c.removeFromCell(id, cell)
				pendingNotifs = append(pendingNotifs, cacheNotif{prev: p, curr: nil})
			}
		}
	}
	for id, p := range newCellPoolers {
		existing, ok := c.poolers[id]
		if !ok || !proto.Equal(existing, p) {
			// existing may be nil here (insert); that's what we want for prev.
			pendingNotifs = append(pendingNotifs, cacheNotif{prev: existing, curr: p})
		}
		c.poolers[id] = p
		c.addToCell(id, p)
	}
	c.cellLastActivity[cell] = time.Now()

	c.notifMu.Lock()
	c.notifQueue = append(c.notifQueue, pendingNotifs...)
	c.notifMu.Unlock()

	c.mu.Unlock()
	c.wake()
}

// onUpserted handles a pooler add or update event.
func (c *topoWatch) onUpserted(pooler *clustermetadatapb.MultiPooler) {
	id := topoclient.ComponentIDString(pooler.Id)

	c.mu.Lock()
	existing, exists := c.poolers[id]
	if exists && proto.Equal(existing, pooler) {
		c.mu.Unlock()
		return // unchanged: suppress spurious notification
	}
	// existing is nil here on insert, which is exactly what we want for prev.
	c.poolers[id] = pooler
	c.addToCell(id, pooler)
	c.cellLastActivity[pooler.Id.Cell] = time.Now()
	c.notifMu.Lock()
	c.notifQueue = append(c.notifQueue, cacheNotif{prev: existing, curr: pooler})
	c.notifMu.Unlock()
	c.mu.Unlock()

	c.wake()
}

// onDeleted handles a pooler deletion event.
func (c *topoWatch) onDeleted(poolerID topoclient.ComponentID) {
	c.mu.Lock()
	p, existed := c.poolers[poolerID]
	if !existed {
		c.mu.Unlock()
		return
	}
	delete(c.poolers, poolerID)
	c.removeFromCell(poolerID, p.Id.Cell)
	c.cellLastActivity[p.Id.Cell] = time.Now()
	c.notifMu.Lock()
	c.notifQueue = append(c.notifQueue, cacheNotif{prev: p, curr: nil})
	c.notifMu.Unlock()
	c.mu.Unlock()

	c.wake()
}

// onCellRemoved handles cell removal by evicting all poolers for that cell.
func (c *topoWatch) onCellRemoved(cell string) {
	c.mu.Lock()
	var removed []*clustermetadatapb.MultiPooler
	for id, p := range c.byCell[cell] {
		delete(c.poolers, id)
		removed = append(removed, p)
	}
	delete(c.byCell, cell)
	delete(c.cellLastActivity, cell)

	c.notifMu.Lock()
	for _, p := range removed {
		c.notifQueue = append(c.notifQueue, cacheNotif{prev: p, curr: nil})
	}
	c.notifMu.Unlock()
	c.mu.Unlock()

	c.wake()
}
