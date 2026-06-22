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

// Package poolerwatch provides a lifecycle-aware cache mirroring topology
// pooler discovery, with per-pooler rider state retained across configurable
// grace periods.
//
// poolerwatch sits logically above topoclient: topoclient is the "talk to
// etcd" layer; poolerwatch applies policy on top.
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
	"github.com/multigres/multigres/go/tools/timer"
)

// State is the cache's view of a pooler's lifecycle. Only Live and MissingFromTopo
// entries are returned by read methods (Get, GetByShard, GetByCell). SHUTDOWN
// poolers are not entries at all — they are tracked separately as "tombstones"
// (see Tombstones) so external cleanup can hard-delete their topology records.
type State int

const (
	// StateLive — the pooler is in topology with a non-SHUTDOWN lifecycle.
	StateLive State = iota
	// StateMissingFromTopo — the pooler's topology entry has been deleted (NoNode)
	// without a prior SHUTDOWN. The entry is retained, visible to reads,
	// for Config.MissingFromTopoGrace before OnGone fires and it is removed.
	StateMissingFromTopo
)

func (s State) String() string {
	switch s {
	case StateLive:
		return "live"
	case StateMissingFromTopo:
		return "missing-from-topo"
	default:
		return "unknown"
	}
}

// GoneReason explains why the cache stopped tracking a pooler.
type GoneReason int

const (
	// GoneShutdown — the pooler's lifecycle was SHUTDOWN at the moment it was
	// removed (immediately on transition with grace=0, or at grace expiry).
	GoneShutdown GoneReason = iota
	// GoneMissingFromTopo — the pooler's topology entry was deleted (NoNode) and the
	// missing-from-topology grace period expired without recovery (or grace=0 took effect
	// immediately).
	GoneMissingFromTopo
	// GoneCacheShutdown — the cache itself was shut down while this entry was
	// still being tracked. Lets callers tear down their riders symmetrically
	// with OnLive on graceful service exit.
	GoneCacheShutdown
)

func (r GoneReason) String() string {
	switch r {
	case GoneShutdown:
		return "shutdown"
	case GoneMissingFromTopo:
		return "missing-from-topo"
	case GoneCacheShutdown:
		return "cache-shutdown"
	default:
		return "unknown"
	}
}

// Entry is a read snapshot of a single pooler's cache state. Returned by
// value so callers cannot accidentally mutate cache internals.
type Entry[T any] struct {
	Pooler     *clustermetadatapb.MultiPooler // latest observed proto; treat as immutable
	Rider      T                              // caller-supplied state
	State      State
	LastChange time.Time // wall-clock time of the last proto change or state transition
}

// Hooks define caller behavior for lifecycle transitions on cache entries.
//
// All hooks for a given pooler are invoked synchronously by the cache and
// observe events in topology order. Hooks may safely call back into the
// cache's read methods. Slow hooks delay subsequent events for that pooler.
//
// MissingFromTopo (NoNode grace) is invisible at the hook level: no hook fires
// when a Live entry enters grace, nor while it stays in grace. Hooks fire
// only on Live-relevant transitions (OnLive, OnUpdate) and at the moment
// the cache permanently lets the entry go (OnGone). Callers that want to
// distinguish "currently in missing-from-topo grace" from "live" can read Entry.State.
//
// SHUTDOWN is different: OnGone fires immediately at the transition, the
// rider is released, and the entry leaves the read-visible map. A "tombstone"
// pooler ID is retained separately (see Tombstones) so that future cleanup
// logic (etcd hard-delete) can find and remove the topology entry.
type Hooks[T any] struct {
	// OnLive fires when a pooler enters the Live state — either first
	// discovery (prevRider is the zero value of T) or recovery from
	// MissingFromTopo within the grace window (prevRider is whatever was attached
	// when the entry departed Live). The returned value becomes the new
	// rider; returning prevRider unchanged preserves it.
	OnLive func(pooler *clustermetadatapb.MultiPooler, prevRider T) T

	// OnUpdate fires when the pooler's proto changes while the entry stays
	// Live. proto.Equal no-ops are suppressed.
	OnUpdate func(prev, curr *clustermetadatapb.MultiPooler, rider T)

	// OnGone fires once, terminally, when the cache stops tracking the
	// pooler — either lifecycle SHUTDOWN observed (subject to ShutdownGrace)
	// or topology entry gone (subject to MissingFromTopoGrace). After return, the
	// entry is no longer in the cache.
	OnGone func(pooler *clustermetadatapb.MultiPooler, rider T, reason GoneReason)
}

// Config configures a PoolerCache. Hooks are NOT part of Config —
// they are supplied at Start, which lets callers construct the cache,
// then construct anything that wants to reference the cache (LB,
// HealthStream, etc.), then bind hooks that close over those.
type Config[T any] struct {
	// Source is the topology backing store. The cache subscribes to it on
	// Start to receive upserts and deletions. Required.
	Source topoclient.Store

	// Filter, if non-nil, restricts which poolers the cache tracks. It is
	// called on every observed event; poolers returning false are dropped at
	// the door. Deletions for unknown IDs are no-ops, so filtered-out
	// poolers cost nothing extra.
	//
	// Filter is called once per event and may consult live external state
	// (e.g., a configurable WatchTargets list). It must not block.
	Filter func(*clustermetadatapb.MultiPooler) bool

	// ShutdownGrace is how long a tombstone is retained after lifecycle
	// transitions to LIFECYCLE_SHUTDOWN. Zero means the tombstone is
	// removed at the next sweep with no extra retention window.
	ShutdownGrace time.Duration

	// MissingFromTopoGrace is how long an entry is retained after the topology
	// record disappears (NoNode) without prior SHUTDOWN. Generous values
	// protect against accidental etcd deletes.
	MissingFromTopoGrace time.Duration

	// SweepInterval is how often the background goroutine scans for entries
	// whose grace deadline has passed and invokes OnGone. Zero defaults
	// to 30s. Ignored if Start is not called (tests can call Sweep manually).
	SweepInterval time.Duration

	// Logger is used for diagnostic messages from the cache itself.
	Logger *slog.Logger

	// now allows tests to inject a deterministic clock. Defaults to time.Now.
	now func() time.Time
}

// shardKey identifies a pooler shard for the byShard secondary index.
type shardKey struct {
	database   string
	tableGroup string
	shard      string
}

// internalEntry holds a single pooler's cache state.
type internalEntry[T any] struct {
	id           topoclient.ComponentID
	pooler       *clustermetadatapb.MultiPooler
	rider        T
	state        State
	lastChange   time.Time
	disposeAfter time.Time // zero iff state == StateLive
}

// Tombstone is a record of a pooler that was observed in LIFECYCLE_SHUTDOWN
// and has been soft-deleted from the read-visible cache. The rider has
// already been released through OnGone. Tombstones are retained so external
// cleanup logic can find them and hard-delete the topology record.
type Tombstone struct {
	ID         *clustermetadatapb.ID
	ShutdownAt time.Time // wall-clock time when the cache first observed SHUTDOWN
}

// tombstoneEntry is the internal companion to Tombstone.
type tombstoneEntry struct {
	id           topoclient.ComponentID
	poolerID     *clustermetadatapb.ID
	shutdownAt   time.Time
	disposeAfter time.Time
}

// PoolerCache maintains a per-pooler rider with lifecycle-aware retention.
// See package doc for full semantics.
type PoolerCache[T any] struct {
	config Config[T]
	hooks  Hooks[T] // bound at Start; zero value if Start has not run
	ctx    context.Context

	mu      sync.Mutex
	entries map[topoclient.ComponentID]*internalEntry[T]
	byCell  map[string]map[topoclient.ComponentID]*internalEntry[T]
	byShard map[shardKey]map[topoclient.ComponentID]*internalEntry[T]
	// tombstones holds SHUTDOWN poolers whose rider has been released. They are
	// invisible to Get/GetByShard/GetByCell; surface via Tombstones() for
	// future etcd-cleanup callers.
	tombstones map[topoclient.ComponentID]*tombstoneEntry
	closed     bool

	sweeper *timer.PeriodicRunner

	// topoSource is the optional underlying watch (created in New if
	// config.Source is provided). Owned and shut down by this cache.
	topoSource *topoWatch

	// cellLastActivity records the wall-clock time of the most recent watch
	// event observed for each cell. Surfaced via CellStatuses for admin
	// pages. Updated under mu.
	cellLastActivity map[string]time.Time

	// Shutdown is one-shot. Concurrent callers (including the ctx-watcher
	// goroutine spawned in New) all block on shutdownDone until disposal
	// hooks have fully run.
	shutdownOnce sync.Once
	shutdownDone chan struct{}
}

// New constructs a PoolerCache.
//
// If config.Source is set, the cache also owns an internal topology watch:
// Start launches both the watch and the disposal sweeper, and Shutdown tears
// both down. Tests that drive events directly via the package-internal
// apply* helpers can omit config.Source.
//
// ctx scopes the cache's lifetime. When ctx is cancelled, Shutdown runs
// automatically — stopping the sweeper and the watch and disposing every
// remaining entry. Callers may still call Shutdown explicitly to wait for
// disposal to finish; concurrent and repeat calls are safe.
func New[T any](ctx context.Context, config Config[T]) *PoolerCache[T] {
	if config.Logger == nil {
		panic("poolerwatch: Config.Logger is required")
	}
	if config.now == nil {
		config.now = time.Now
	}
	interval := config.SweepInterval
	if interval == 0 {
		interval = 30 * time.Second
	}
	c := &PoolerCache[T]{
		config:           config,
		ctx:              ctx,
		entries:          make(map[topoclient.ComponentID]*internalEntry[T]),
		byCell:           make(map[string]map[topoclient.ComponentID]*internalEntry[T]),
		byShard:          make(map[shardKey]map[topoclient.ComponentID]*internalEntry[T]),
		tombstones:       make(map[topoclient.ComponentID]*tombstoneEntry),
		cellLastActivity: make(map[string]time.Time),
		sweeper:          timer.NewPeriodicRunner(ctx, interval),
		shutdownDone:     make(chan struct{}),
	}
	if config.Source != nil {
		c.topoSource = newTopoWatch(ctx, config.Source, config.Logger, topoWatchHandlers{
			OnSnapshot:    c.reconcileCell,
			OnUpsert:      c.applyUpsert,
			OnDelete:      c.applyDelete,
			OnCellRemoved: c.onCellRemoved,
		})
	}
	// Auto-shutdown on parent context cancellation. The goroutine exits as
	// soon as Shutdown has finished, regardless of who called it first.
	go func() {
		select {
		case <-ctx.Done():
			c.Shutdown()
		case <-c.shutdownDone:
		}
	}()
	return c
}

// Start binds hooks and launches the background sweeper that disposes
// expired entries. If config.Source was provided, also starts the
// topology watch that drives upserts and deletes. Hooks may safely
// reference the cache itself, since by the time hooks fire the cache
// is fully constructed.
//
// Must be called exactly once. Tests that drive events via the
// package-internal apply helpers also call Start (with their test
// hooks) so the cache's hooks field is populated.
func (c *PoolerCache[T]) Start(hooks Hooks[T]) {
	c.hooks = hooks
	c.sweeper.Start(func(context.Context) { c.sweep() }, nil)
	if c.topoSource != nil {
		c.topoSource.Start()
	}
}

// Sync blocks until every event already enqueued by the underlying topology
// watch has been observed by this cache. Returns immediately if no source
// is configured.
func (c *PoolerCache[T]) Sync(ctx context.Context) error {
	if c.topoSource == nil {
		return nil
	}
	return c.topoSource.Sync(ctx)
}

// Shutdown stops the background sweeper and disposes every remaining entry.
// After Shutdown returns, every rider has been passed through OnGone,
// subsequent reads return zero/false, and apply* events are ignored.
//
// Safe to call multiple times and from multiple goroutines. Concurrent and
// repeat callers all block until disposal has finished — the work runs once.
// Shutdown is also called automatically when the context passed to New is
// cancelled.
func (c *PoolerCache[T]) Shutdown() {
	c.shutdownOnce.Do(func() {
		defer close(c.shutdownDone)
		// Stop the watch first so no new events arrive while we tear down.
		if c.topoSource != nil {
			c.topoSource.Stop()
		}
		c.sweeper.Stop()
		c.mu.Lock()
		c.closed = true
		entries := c.entries
		c.entries = nil
		c.byCell = nil
		c.byShard = nil
		c.tombstones = nil
		c.mu.Unlock()
		hooks := c.hooks
		if hooks.OnGone == nil {
			return
		}
		for _, e := range entries {
			hooks.OnGone(e.pooler, e.rider, goneReasonFor(e.state))
		}
	})
	<-c.shutdownDone
}

// goneReasonFor maps an entry's current state to the GoneReason passed to
// OnGone when the entry is finally removed. Only called for entries still
// in the read-visible map (i.e., StateLive or StateMissingFromTopo).
func goneReasonFor(s State) GoneReason {
	switch s {
	case StateMissingFromTopo:
		return GoneMissingFromTopo
	default:
		// State == StateLive happens at cache shutdown only.
		return GoneCacheShutdown
	}
}

// Get returns the entry for a single pooler ID.
func (c *PoolerCache[T]) Get(id topoclient.ComponentID) (Entry[T], bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[id]
	if !ok {
		return Entry[T]{}, false
	}
	return entrySnapshot(e), true
}

// GetRider is a convenience that returns just the rider for a pooler ID,
// equivalent to Get(id).Rider but without the wrapper struct.
func (c *PoolerCache[T]) GetRider(id topoclient.ComponentID) (T, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[id]
	if !ok {
		var zero T
		return zero, false
	}
	return e.rider, true
}

// GetByShard returns every entry in the given (database, tableGroup, shard).
// Order is unspecified. Returns all states; callers can filter by State.
func (c *PoolerCache[T]) GetByShard(database, tableGroup, shard string) []Entry[T] {
	key := shardKey{database: database, tableGroup: tableGroup, shard: shard}
	c.mu.Lock()
	defer c.mu.Unlock()
	bucket := c.byShard[key]
	out := make([]Entry[T], 0, len(bucket))
	for _, e := range bucket {
		out = append(out, entrySnapshot(e))
	}
	return out
}

// All returns every read-visible entry (Live or MissingFromTopo). Tombstones are not
// included. Intended for cross-shard scans like metric collection or
// bookkeeping; for ordinary lookups prefer Get / GetByShard.
func (c *PoolerCache[T]) All() []Entry[T] {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Entry[T], 0, len(c.entries))
	for _, e := range c.entries {
		out = append(out, entrySnapshot(e))
	}
	return out
}

// Len returns the number of entries currently tracked, including those
// in MissingFromTopo state awaiting eviction. Tombstones (post-SHUTDOWN) are
// not counted — they leave the entries map at OnGone time.
func (c *PoolerCache[T]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// CellStatuses returns per-cell status sorted alphabetically by cell name.
// Reflects the raw topology view: every observed pooler, including
// SHUTDOWN (i.e. tombstones). Intended for admin/status pages, not the hot
// query path.
func (c *PoolerCache[T]) CellStatuses() []CellStatus {
	c.mu.Lock()
	defer c.mu.Unlock()

	cellSet := make(map[string]struct{}, len(c.byCell))
	for cell := range c.byCell {
		cellSet[cell] = struct{}{}
	}
	for cell := range c.cellLastActivity {
		cellSet[cell] = struct{}{}
	}
	for _, g := range c.tombstones {
		cellSet[g.poolerID.GetCell()] = struct{}{}
	}

	cellNames := make([]string, 0, len(cellSet))
	for cell := range cellSet {
		cellNames = append(cellNames, cell)
	}
	sort.Strings(cellNames)

	statuses := make([]CellStatus, 0, len(cellNames))
	for _, cell := range cellNames {
		var poolers []*clustermetadatapb.MultiPooler
		for _, e := range c.byCell[cell] {
			poolers = append(poolers, proto.Clone(e.pooler).(*clustermetadatapb.MultiPooler))
		}
		// Tombstones (SHUTDOWN poolers) are part of the cell's view too — operators
		// want to see them. They carry no full proto, only an ID.
		for _, g := range c.tombstones {
			if g.poolerID.GetCell() != cell {
				continue
			}
			poolers = append(poolers, &clustermetadatapb.MultiPooler{
				Id: g.poolerID,
				LifecycleStatus: &clustermetadatapb.PoolerLifecycle{
					Status: clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN,
				},
			})
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

// reconcileCell handles a per-cell topology snapshot from the underlying
// topoWatch. The snapshot is the complete current state of the cell; any
// entry the cache holds for this cell that's missing from the snapshot is
// treated as deleted. Called on every per-cell watcher (re)connect.
func (c *PoolerCache[T]) reconcileCell(cell string, poolers []*clustermetadatapb.MultiPooler) {
	seen := make(map[topoclient.ComponentID]struct{}, len(poolers))
	for _, p := range poolers {
		seen[topoclient.ComponentIDString(p.Id)] = struct{}{}
	}

	// Snapshot the IDs currently in this cell so we can compare without
	// holding the lock during the apply* calls (which take it themselves).
	c.mu.Lock()
	c.cellLastActivity[cell] = c.config.now()
	var existing []topoclient.ComponentID
	for id := range c.byCell[cell] {
		existing = append(existing, id)
	}
	c.mu.Unlock()

	// Drop anything in this cell that's no longer present.
	for _, id := range existing {
		if _, ok := seen[id]; !ok {
			c.applyDelete(id)
		}
	}
	// Upsert everything present (applyUpsert suppresses proto.Equal no-ops).
	for _, p := range poolers {
		c.applyUpsert(p)
	}
}

// onCellRemoved handles a cell-removed event from the underlying topoWatch.
// Every entry in the cache for that cell is deleted, and the cell's
// LastActivity is forgotten.
func (c *PoolerCache[T]) onCellRemoved(cell string) {
	c.mu.Lock()
	var existing []topoclient.ComponentID
	for id := range c.byCell[cell] {
		existing = append(existing, id)
	}
	delete(c.cellLastActivity, cell)
	c.mu.Unlock()

	for _, id := range existing {
		c.applyDelete(id)
	}
}

// DoUpdate atomically reads the rider for id, calls fn to compute the new
// rider, and writes it back. If no entry exists, fn is not called. The
// cache lock is held for the duration of fn, so fn must not block, call
// back into the cache, or perform IO.
//
// DoUpdate does not clone the rider. Callers that need defensive copies
// (e.g. for proto values shared across goroutines) should wrap the cache
// with their own clone-on-read/write layer.
func (c *PoolerCache[T]) DoUpdate(id topoclient.ComponentID, fn func(curr T) T) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[id]
	if !ok {
		return
	}
	e.rider = fn(e.rider)
}

// sweep scans for entries and tombstones whose grace deadline has passed:
//   - MissingFromTopo entries fire OnGone(MissingFromTopo) and are removed.
//   - Tombstones (post-SHUTDOWN) are removed silently — OnGone already fired at
//     the moment of SHUTDOWN.
//
// Called automatically by the background goroutine when Start was invoked;
// tests can call it directly.
func (c *PoolerCache[T]) sweep() {
	now := c.config.now()
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	var due []*internalEntry[T]
	for _, e := range c.entries {
		if e.state == StateMissingFromTopo && !now.Before(e.disposeAfter) {
			due = append(due, e)
		}
	}
	for _, e := range due {
		delete(c.entries, e.id)
		c.indexRemove(e)
	}
	for id, g := range c.tombstones {
		if !now.Before(g.disposeAfter) {
			delete(c.tombstones, id)
		}
	}
	hooks := c.hooks
	c.mu.Unlock()

	if hooks.OnGone == nil {
		return
	}
	for _, e := range due {
		hooks.OnGone(e.pooler, e.rider, goneReasonFor(e.state))
	}
}

func entrySnapshot[T any](e *internalEntry[T]) Entry[T] {
	return Entry[T]{
		Pooler:     e.pooler,
		Rider:      e.rider,
		State:      e.state,
		LastChange: e.lastChange,
	}
}

func shardKeyFor(p *clustermetadatapb.MultiPooler) shardKey {
	sk := p.GetShardKey()
	return shardKey{
		database:   sk.GetDatabase(),
		tableGroup: sk.GetTableGroup(),
		shard:      sk.GetShard(),
	}
}

func (c *PoolerCache[T]) indexInsert(e *internalEntry[T]) {
	cell := e.pooler.GetId().GetCell()
	if cell != "" {
		bucket, ok := c.byCell[cell]
		if !ok {
			bucket = make(map[topoclient.ComponentID]*internalEntry[T])
			c.byCell[cell] = bucket
		}
		bucket[e.id] = e
	}
	sk := shardKeyFor(e.pooler)
	bucket, ok := c.byShard[sk]
	if !ok {
		bucket = make(map[topoclient.ComponentID]*internalEntry[T])
		c.byShard[sk] = bucket
	}
	bucket[e.id] = e
}

func (c *PoolerCache[T]) indexRemove(e *internalEntry[T]) {
	cell := e.pooler.GetId().GetCell()
	if bucket := c.byCell[cell]; bucket != nil {
		delete(bucket, e.id)
		if len(bucket) == 0 {
			delete(c.byCell, cell)
		}
	}
	sk := shardKeyFor(e.pooler)
	if bucket := c.byShard[sk]; bucket != nil {
		delete(bucket, e.id)
		if len(bucket) == 0 {
			delete(c.byShard, sk)
		}
	}
}

func (c *PoolerCache[T]) indexUpdate(e *internalEntry[T], prevPooler *clustermetadatapb.MultiPooler) {
	prevCell := prevPooler.GetId().GetCell()
	prevSK := shardKeyFor(prevPooler)
	newCell := e.pooler.GetId().GetCell()
	newSK := shardKeyFor(e.pooler)
	if prevCell == newCell && prevSK == newSK {
		return
	}
	// Remove from old buckets using the prev pooler's coordinates.
	saved := e.pooler
	e.pooler = prevPooler
	c.indexRemove(e)
	e.pooler = saved
	c.indexInsert(e)
}

// applyUpsert ingests an upsert event from a topology source. Until this
// package owns its own watch loop, callers wire it to an external watch.
//
// Events are silently dropped if Config.Filter is set and returns false for
// this pooler — filtered poolers never enter the cache, so subsequent reads
// (Get, GetByShard, GetByCell, Count) ignore them.
//
// Lifecycle transitions:
//   - First sight, Live: OnLive(p, zero) fires; entry enters StateLive.
//   - First sight, SHUTDOWN: recorded as a tombstone so external cleanup can
//     hard-delete; no OnLive (orch's "SHUTDOWN = dead from my POV" stance).
//   - Live → Live (proto diff): OnUpdate fires.
//   - Live → SHUTDOWN: OnGone(Shutdown) fires immediately; the entry is
//     removed from the read-visible map and a tombstone is retained for
//     ShutdownGrace so external cleanup can find it.
//   - MissingFromTopo → Live (recovery): OnUpdate fires (rider was preserved through
//     grace; the pooler was never "gone" from caller's POV).
//   - MissingFromTopo → SHUTDOWN: OnGone(Shutdown) fires (first time for this entry);
//     entry removed, tombstone retained.
//   - Tombstone → Live (restart-from-shutdown): OnLive(p, zero) fires fresh —
//     the previous rider was already released through OnGone; the tombstone
//     is dropped.
//   - Tombstone → SHUTDOWN: tombstone timestamp refreshed; no hooks.
func (c *PoolerCache[T]) applyUpsert(pooler *clustermetadatapb.MultiPooler) {
	if c.config.Filter != nil && !c.config.Filter(pooler) {
		return
	}
	c.upsert(pooler)
}

// upsert is applyUpsert without the filter check. Tests call this via
// SeedForTest to inject state without having to know the cache's filter
// configuration.
func (c *PoolerCache[T]) upsert(pooler *clustermetadatapb.MultiPooler) {
	id := topoclient.ComponentIDString(pooler.Id)
	now := c.config.now()
	isShutdown := pooler.GetLifecycleStatus().GetStatus() == clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	if cell := pooler.GetId().GetCell(); cell != "" {
		c.cellLastActivity[cell] = now
	}

	// --- Existing entry (Live or MissingFromTopo) ---
	if e, exists := c.entries[id]; exists {
		prevPooler := e.pooler
		prevState := e.state
		rider := e.rider
		unchanged := proto.Equal(prevPooler, pooler)
		e.pooler = pooler
		c.indexUpdate(e, prevPooler)

		if isShutdown {
			// Live/MissingFromTopo → Shutdown: remove from read-visible map, fire
			// OnGone, and retain a tombstone for future cleanup.
			delete(c.entries, e.id)
			c.indexRemove(e)
			c.addTombstoneLocked(id, pooler.Id, now)
			hooks := c.hooks
			c.mu.Unlock()
			if hooks.OnGone != nil {
				hooks.OnGone(pooler, rider, GoneShutdown)
			}
			return
		}

		// Not shutdown.
		if prevState == StateLive {
			if !unchanged {
				e.lastChange = now
			}
			hooks := c.hooks
			c.mu.Unlock()
			if !unchanged && hooks.OnUpdate != nil {
				hooks.OnUpdate(prevPooler, pooler, rider)
			}
			return
		}
		// prevState == StateMissingFromTopo: the topology entry came back inside
		// the grace window. MissingFromTopo is a topology-layer state ("etcd
		// said NoNode"), not a health signal — the pooler process itself
		// may have been reachable the whole time, which is exactly why
		// we keep the rider and its resources (e.g. orch's per-pooler
		// health stream) running through the window. No hook fires on
		// the Live → MissingFromTopo transition under grace > 0, so the rider
		// is the same instance we had before, with state intact.
		//
		// On recovery we treat it as a proto-update: OnUpdate fires iff
		// the proto changed. An unchanged recovery (transient etcd blip
		// that resolves with no real diff) is silent on purpose — the
		// entry was never observably "gone" from the caller's POV.
		// OnLive is intentionally NOT fired.
		//
		// Invariant: rider exists ⇔ its per-pooler resources are
		// running. A MissingFromTopo entry whose grace deadline passes is
		// swept (OnGone fires, resources cancelled); a subsequent
		// reappearance comes through the "truly first sight" path and
		// gets a fresh rider with fresh resources. So gone-past-grace-
		// then-reappear restarts cleanly; gone-within-grace-then-reappear
		// continues seamlessly.
		e.state = StateLive
		e.lastChange = now
		e.disposeAfter = time.Time{}
		hooks := c.hooks
		c.mu.Unlock()
		if !unchanged && hooks.OnUpdate != nil {
			hooks.OnUpdate(prevPooler, pooler, rider)
		}
		return
	}

	// --- Not an entry. Maybe a tombstone (restart from SHUTDOWN) or truly new. ---
	if g, ok := c.tombstones[id]; ok {
		if isShutdown {
			// Refresh the tombstone timestamp; otherwise no-op.
			g.shutdownAt = now
			c.mu.Unlock()
			return
		}
		// Tombstone → Live: restart-from-shutdown. Drop the tombstone; treat as a
		// fresh discovery (the previous rider was released through OnGone).
		delete(c.tombstones, id)
		hooks := c.hooks
		// Drop c.mu around OnLive: hooks may take their own locks or block
		// on IO; holding the cache lock through that would invite deadlock
		// (sync.Mutex isn't reentrant) and starve other readers. The
		// watch goroutine is single-threaded per cell, so no concurrent
		// event for this pooler can race the OnLive callback. Shutdown
		// can land during the window — handled by the c.closed check
		// after we reacquire.
		c.mu.Unlock()
		var zero T
		var rider T
		if hooks.OnLive != nil {
			rider = hooks.OnLive(pooler, zero)
		}
		c.mu.Lock()
		if c.closed {
			// Shutdown completed during OnLive. Dispose the fresh rider
			// directly via OnGone(GoneCacheShutdown) — it never made it
			// into the entries map for Shutdown's iteration to find.
			c.mu.Unlock()
			if hooks.OnGone != nil {
				hooks.OnGone(pooler, rider, GoneCacheShutdown)
			}
			return
		}
		ne := &internalEntry[T]{
			id:         id,
			pooler:     pooler,
			rider:      rider,
			state:      StateLive,
			lastChange: now,
		}
		c.entries[id] = ne
		c.indexInsert(ne)
		c.mu.Unlock()
		return
	}

	// Truly first sight.
	if isShutdown {
		// Cold-discovered SHUTDOWN: record as a tombstone so future cleanup can
		// find it. No OnLive, no rider, no OnGone.
		c.addTombstoneLocked(id, pooler.Id, now)
		c.mu.Unlock()
		return
	}
	hooks := c.hooks
	// First-sight Live: same release/reacquire pattern as the tombstone
	// restart path above — drop c.mu around OnLive (no reentrancy /
	// deadlock risk), then reacquire to insert the entry. If Shutdown
	// lands during the window, the c.closed check disposes the fresh
	// rider directly.
	c.mu.Unlock()
	var zero T
	var rider T
	if hooks.OnLive != nil {
		rider = hooks.OnLive(pooler, zero)
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		if hooks.OnGone != nil {
			hooks.OnGone(pooler, rider, GoneCacheShutdown)
		}
		return
	}
	ne := &internalEntry[T]{
		id:         id,
		pooler:     pooler,
		rider:      rider,
		state:      StateLive,
		lastChange: now,
	}
	c.entries[id] = ne
	c.indexInsert(ne)
	c.mu.Unlock()
}

// addTombstoneLocked records a SHUTDOWN pooler as a tombstone. Caller holds c.mu.
func (c *PoolerCache[T]) addTombstoneLocked(id topoclient.ComponentID, poolerID *clustermetadatapb.ID, now time.Time) {
	c.tombstones[id] = &tombstoneEntry{
		id:           id,
		poolerID:     poolerID,
		shutdownAt:   now,
		disposeAfter: now.Add(c.config.ShutdownGrace),
	}
}

// applyDelete ingests a topology deletion (NoNode) event for the given
// pooler ID.
//
//   - If the pooler is currently a read-visible entry (Live or MissingFromTopo),
//     it transitions to StateMissingFromTopo. The entry stays visible to reads
//     for MissingFromTopoGrace; if the pooler returns, the rider is preserved.
//     If grace expires (Sweep), OnGone(MissingFromTopo) fires. MissingFromTopoGrace=0
//     fires OnGone immediately and removes the entry.
//   - If the pooler is a tombstone (we observed its SHUTDOWN earlier), the
//     deletion confirms cleanup happened: the tombstone is removed silently.
//   - Unknown ID: no-op.
//
// deleteImmediate evicts an entry now, bypassing MissingFromTopoGrace. Test-only
// (via DeleteForTest); applyDelete is the production path that honors
// grace. Caller must not hold c.mu.
func (c *PoolerCache[T]) deleteImmediate(id topoclient.ComponentID) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	e, ok := c.entries[id]
	if !ok {
		c.mu.Unlock()
		return
	}
	delete(c.entries, e.id)
	c.indexRemove(e)
	hooks := c.hooks
	pooler := e.pooler
	rider := e.rider
	c.mu.Unlock()
	if hooks.OnGone != nil {
		hooks.OnGone(pooler, rider, GoneMissingFromTopo)
	}
}

func (c *PoolerCache[T]) applyDelete(id topoclient.ComponentID) {
	now := c.config.now()
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}

	if e, ok := c.entries[id]; ok {
		if cell := e.pooler.GetId().GetCell(); cell != "" {
			c.cellLastActivity[cell] = now
		}
		switch e.state {
		case StateLive:
			e.state = StateMissingFromTopo
			e.lastChange = now
			e.disposeAfter = now.Add(c.config.MissingFromTopoGrace)
			hooks := c.hooks
			pooler := e.pooler
			rider := e.rider
			removeNow := c.config.MissingFromTopoGrace == 0
			if removeNow {
				delete(c.entries, e.id)
				c.indexRemove(e)
			}
			c.mu.Unlock()
			if removeNow && hooks.OnGone != nil {
				hooks.OnGone(pooler, rider, GoneMissingFromTopo)
			}
		case StateMissingFromTopo:
			c.mu.Unlock()
		}
		return
	}

	if _, ok := c.tombstones[id]; ok {
		delete(c.tombstones, id)
		c.mu.Unlock()
		return
	}

	c.mu.Unlock()
}

// Tombstones returns a snapshot of poolers observed in SHUTDOWN whose topology
// records have not yet been hard-deleted (or have not yet been observed as
// deleted by this cache). Intended for an external cleanup loop that
// removes the topology entries.
//
// The returned Tombstone.ID values share storage with the cache; callers
// must treat them as read-only (the cleanup loop only uses them as lookup
// keys, which is safe).
func (c *PoolerCache[T]) Tombstones() []Tombstone {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Tombstone, 0, len(c.tombstones))
	for _, g := range c.tombstones {
		out = append(out, Tombstone{ID: g.poolerID, ShutdownAt: g.shutdownAt})
	}
	return out
}
