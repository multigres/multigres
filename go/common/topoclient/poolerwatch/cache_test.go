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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// fakeClock allows tests to advance time deterministically.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{t: time.Unix(1_700_000_000, 0)} }

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.t
}

func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	f.t = f.t.Add(d)
	f.mu.Unlock()
}

// hookRecorder captures hook calls so tests can assert on event sequences.
type hookRecorder struct {
	mu     sync.Mutex
	events []string
}

func (r *hookRecorder) record(s string) {
	r.mu.Lock()
	r.events = append(r.events, s)
	r.mu.Unlock()
}

func (r *hookRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.events))
	copy(out, r.events)
	return out
}

type testRider struct{ id int64 }

var riderCounter atomic.Int64

func newRider() *testRider { return &testRider{id: riderCounter.Add(1)} }

func pool(cell, name, db, tg, shard string, lifecycle clustermetadatapb.PoolerLifecycleStatus) *clustermetadatapb.MultiPooler {
	return &clustermetadatapb.MultiPooler{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      cell,
			Name:      name,
		},
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   db,
			TableGroup: tg,
			Shard:      shard,
		},
		LifecycleStatus: &clustermetadatapb.PoolerLifecycle{Status: lifecycle},
		Hostname:        name + ".local",
	}
}

func silentLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

// newTestCache builds a PoolerCache wired to a fake clock with the given
// grace periods. The hookRecorder captures every hook invocation in a
// human-readable form.
func newTestCache(t *testing.T, clk *fakeClock, shutdownGrace, missingGracePeriod time.Duration) (*PoolerCache[*testRider], *hookRecorder) {
	t.Helper()
	rec := &hookRecorder{}
	cfg := Config[*testRider]{
		ShutdownGrace:      shutdownGrace,
		MissingGracePeriod: missingGracePeriod,
		Logger:             silentLogger(),
		now:                clk.Now,
	}
	cache := New(t.Context(), cfg)
	cache.hooks = Hooks[*testRider]{
		OnLive: func(p *clustermetadatapb.MultiPooler, prev *testRider) *testRider {
			if prev != nil {
				rec.record("resume:" + p.Id.Name)
				return prev
			}
			rec.record("live:" + p.Id.Name)
			return newRider()
		},
		OnUpdate: func(_, curr *clustermetadatapb.MultiPooler, _ *testRider) {
			rec.record("update:" + curr.Id.Name)
		},
		OnGone: func(p *clustermetadatapb.MultiPooler, _ *testRider, r GoneReason) {
			rec.record("gone-" + r.String() + ":" + p.Id.Name)
		},
	}
	return cache, rec
}

func componentID(cell, name string) topoclient.ComponentID {
	return topoclient.ComponentIDString(&clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      cell,
		Name:      name,
	})
}

// --------------------------------------------------------------------------
// Tests
// --------------------------------------------------------------------------

func TestCache_FirstDiscoveryFiresOnLive(t *testing.T) {
	clk := newFakeClock()
	c, rec := newTestCache(t, clk, time.Hour, time.Hour)
	defer c.Shutdown()

	p := pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE)
	c.applyUpsert(p)

	assert.Equal(t, []string{"live:p1"}, rec.snapshot())
	entry, ok := c.Get(componentID("zone1", "p1"))
	require.True(t, ok)
	assert.Equal(t, StateLive, entry.State)
	assert.NotNil(t, entry.Rider)
}

func TestCache_ProtoUpdateWhileLiveFiresOnUpdate(t *testing.T) {
	clk := newFakeClock()
	c, rec := newTestCache(t, clk, time.Hour, time.Hour)
	defer c.Shutdown()

	p := pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE)
	c.applyUpsert(p)
	clk.Advance(1 * time.Second)
	p2 := pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE)
	p2.Hostname = "newhost.local"
	c.applyUpsert(p2)

	assert.Equal(t, []string{"live:p1", "update:p1"}, rec.snapshot())
}

func TestCache_DuplicateUpsertIsSuppressed(t *testing.T) {
	clk := newFakeClock()
	c, rec := newTestCache(t, clk, time.Hour, time.Hour)
	defer c.Shutdown()

	p := pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE)
	c.applyUpsert(p)
	c.applyUpsert(p) // proto.Equal — should suppress OnUpdate

	assert.Equal(t, []string{"live:p1"}, rec.snapshot())
}

// TestCache_ShutdownFiresOnGoneImmediately verifies the asymmetric SHUTDOWN
// semantics: OnGone fires AT the transition, regardless of grace. The
// entry is then invisible to reads.
func TestCache_ShutdownFiresOnGoneImmediately(t *testing.T) {
	clk := newFakeClock()
	c, rec := newTestCache(t, clk, time.Hour, time.Hour) // ShutdownGrace is for tombstone retention only
	defer c.Shutdown()

	c.applyUpsert(pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE))
	c.applyUpsert(pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN))

	// OnGone fired immediately, even though ShutdownGrace > 0.
	assert.Equal(t, []string{"live:p1", "gone-shutdown:p1"}, rec.snapshot())

	// Entry is invisible to normal reads.
	_, ok := c.Get(componentID("zone1", "p1"))
	assert.False(t, ok, "shut-down pooler must not appear in Get")

	// Tombstone is retained for cleanup.
	tombstones := c.Tombstones()
	require.Len(t, tombstones, 1)
	assert.Equal(t, "p1", tombstones[0].ID.Name)
}

func TestCache_ColdShutdownDiscoveryIgnoresWithoutOnLive(t *testing.T) {
	clk := newFakeClock()
	c, rec := newTestCache(t, clk, time.Hour, time.Hour)
	defer c.Shutdown()

	c.applyUpsert(pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN))

	assert.Empty(t, rec.snapshot(), "cold-shutdown discovery must fire no hooks")
	_, ok := c.Get(componentID("zone1", "p1"))
	assert.False(t, ok, "cold-shutdown pooler must not be visible")
	require.Len(t, c.Tombstones(), 1, "cold-shutdown pooler must be tracked as a tombstone")
}

func TestCache_TombstonesExpireSilentlyOnSweep(t *testing.T) {
	clk := newFakeClock()
	c, rec := newTestCache(t, clk, time.Minute, time.Hour)
	defer c.Shutdown()

	c.applyUpsert(pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE))
	c.applyUpsert(pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN))

	clk.Advance(2 * time.Minute) // past ShutdownGrace
	c.sweep()

	// No new hook fires when the tombstone expires — OnGone already happened.
	assert.Equal(t, []string{"live:p1", "gone-shutdown:p1"}, rec.snapshot())
	assert.Empty(t, c.Tombstones(), "tombstone must be evicted after grace")
}

// TestCache_MissingFromTopoIsSilentUntilGrace verifies that NoNode does NOT fire
// OnGone immediately. The entry stays visible to reads for MissingGracePeriod.
func TestCache_MissingFromTopoIsSilentUntilGrace(t *testing.T) {
	clk := newFakeClock()
	c, rec := newTestCache(t, clk, time.Hour, time.Hour)
	defer c.Shutdown()

	c.applyUpsert(pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE))
	c.applyDelete(componentID("zone1", "p1"))

	// No hook fires on topology removal; the entry remains visible (as MissingFromTopo).
	assert.Equal(t, []string{"live:p1"}, rec.snapshot())
	entry, ok := c.Get(componentID("zone1", "p1"))
	require.True(t, ok, "pooler missing from topology stays visible to reads during grace")
	assert.Equal(t, StateMissingFromTopo, entry.State)

	// Past grace, sweep fires OnGone(MissingFromTopo) and removes the entry.
	clk.Advance(2 * time.Hour)
	c.sweep()
	assert.Equal(t, []string{"live:p1", "gone-missing-from-topo:p1"}, rec.snapshot())
	_, ok = c.Get(componentID("zone1", "p1"))
	assert.False(t, ok)
}

func TestCache_MissingFromTopoRecoveryWithinGracePreservesRider(t *testing.T) {
	clk := newFakeClock()
	c, rec := newTestCache(t, clk, time.Hour, time.Hour)
	defer c.Shutdown()

	c.applyUpsert(pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE))
	r1, _ := c.Get(componentID("zone1", "p1"))
	originalRider := r1.Rider

	c.applyDelete(componentID("zone1", "p1"))
	clk.Advance(10 * time.Minute) // within grace
	updated := pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE)
	updated.Hostname = "recovered.local"
	c.applyUpsert(updated)

	// Recovery looks like an OnUpdate: no OnGone fired, rider preserved.
	assert.Equal(t, []string{"live:p1", "update:p1"}, rec.snapshot())
	r2, ok := c.Get(componentID("zone1", "p1"))
	require.True(t, ok)
	assert.Equal(t, StateLive, r2.State)
	assert.Same(t, originalRider, r2.Rider, "rider preserved across missing-from-topo recovery")
}

func TestCache_RestartFromShutdownIsFreshOnLive(t *testing.T) {
	clk := newFakeClock()
	c, rec := newTestCache(t, clk, time.Hour, time.Hour)
	defer c.Shutdown()

	c.applyUpsert(pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE))
	r1, _ := c.Get(componentID("zone1", "p1"))
	rider1 := r1.Rider

	c.applyUpsert(pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN))
	clk.Advance(5 * time.Minute) // within tombstone retention
	c.applyUpsert(pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE))

	// Restart-from-shutdown is a fresh discovery: OnLive(prev=nil) fires.
	assert.Equal(t, []string{"live:p1", "gone-shutdown:p1", "live:p1"}, rec.snapshot())
	r2, _ := c.Get(componentID("zone1", "p1"))
	assert.NotSame(t, rider1, r2.Rider, "restart-from-shutdown must allocate a fresh rider")
}

func TestCache_MissingFromTopoToShutdownFiresOnGone(t *testing.T) {
	clk := newFakeClock()
	c, rec := newTestCache(t, clk, time.Hour, time.Hour)
	defer c.Shutdown()

	c.applyUpsert(pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE))
	c.applyDelete(componentID("zone1", "p1"))
	// Entry is now MissingFromTopo. Pooler reappears as SHUTDOWN — first time OnGone
	// fires for this entry.
	c.applyUpsert(pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN))

	assert.Equal(t, []string{"live:p1", "gone-shutdown:p1"}, rec.snapshot())
	_, ok := c.Get(componentID("zone1", "p1"))
	assert.False(t, ok)
	require.Len(t, c.Tombstones(), 1)
}

func TestCache_DeleteOfTombstoneRemovesItSilently(t *testing.T) {
	clk := newFakeClock()
	c, rec := newTestCache(t, clk, time.Hour, time.Hour)
	defer c.Shutdown()

	c.applyUpsert(pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE))
	c.applyUpsert(pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN))
	require.Len(t, c.Tombstones(), 1)

	// Topology cleanup happens (external etcd delete).
	c.applyDelete(componentID("zone1", "p1"))

	assert.Equal(t, []string{"live:p1", "gone-shutdown:p1"}, rec.snapshot(),
		"hard-deletion of a tombstone must not produce any hook")
	assert.Empty(t, c.Tombstones())
}

func TestCache_ZeroMissingGracePeriodFiresOnGoneInline(t *testing.T) {
	clk := newFakeClock()
	c, rec := newTestCache(t, clk, time.Hour, 0)
	defer c.Shutdown()

	c.applyUpsert(pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE))
	c.applyDelete(componentID("zone1", "p1"))

	assert.Equal(t, []string{"live:p1", "gone-missing-from-topo:p1"}, rec.snapshot())
	_, ok := c.Get(componentID("zone1", "p1"))
	assert.False(t, ok)
}

func TestCache_GetByShardAndCellExcludeShutdownPoolers(t *testing.T) {
	clk := newFakeClock()
	c, _ := newTestCache(t, clk, time.Hour, time.Hour)
	defer c.Shutdown()

	c.applyUpsert(pool("zone1", "alive", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE))
	c.applyUpsert(pool("zone1", "dead", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE))
	c.applyUpsert(pool("zone1", "dead", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN))

	shard := c.GetByShard("db", "tg", "0")
	require.Len(t, shard, 1, "shutdown pooler must not appear in shard reads")
	assert.Equal(t, "alive", shard[0].Pooler.Id.Name)
}

func TestCache_ShutdownDisposesEverythingRemaining(t *testing.T) {
	clk := newFakeClock()
	c, rec := newTestCache(t, clk, time.Hour, time.Hour)

	c.applyUpsert(pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE))
	c.applyUpsert(pool("zone1", "p2", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE))
	c.applyDelete(componentID("zone1", "p2"))

	// Now: p1 is Live, p2 is MissingFromTopo. Both should fire OnGone on Shutdown.
	c.Shutdown()

	got := rec.snapshot()
	// Count OnGone events; reasons reflect their state at shutdown.
	cacheShutdown, missingFromTopo := 0, 0
	for _, e := range got {
		switch e {
		case "gone-cache-shutdown:p1":
			cacheShutdown++
		case "gone-missing-from-topo:p2":
			missingFromTopo++
		}
	}
	assert.Equal(t, 1, cacheShutdown, "Live entry must fire OnGone(CacheShutdown), got: %v", got)
	assert.Equal(t, 1, missingFromTopo, "MissingFromTopo entry must fire OnGone(MissingFromTopo), got: %v", got)
}

func TestCache_ContextCancellationTriggersShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	clk := newFakeClock()
	rec := &hookRecorder{}
	cfg := Config[*testRider]{
		ShutdownGrace:      time.Hour,
		MissingGracePeriod: time.Hour,
		Logger:             silentLogger(),
		now:                clk.Now,
	}
	c := New(ctx, cfg)
	c.hooks = Hooks[*testRider]{
		OnLive: func(p *clustermetadatapb.MultiPooler, _ *testRider) *testRider {
			rec.record("live:" + p.Id.Name)
			return newRider()
		},
		OnGone: func(p *clustermetadatapb.MultiPooler, _ *testRider, r GoneReason) {
			rec.record("gone-" + r.String() + ":" + p.Id.Name)
		},
	}
	c.applyUpsert(pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE))

	cancel()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		events := rec.snapshot()
		if len(events) >= 2 && events[1] == "gone-cache-shutdown:p1" {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected ctx-cancel to fire OnGone(CacheShutdown); got events: %v", rec.snapshot())
}

func TestCache_RepeatShutdownIsIdempotent(t *testing.T) {
	clk := newFakeClock()
	c, rec := newTestCache(t, clk, time.Hour, time.Hour)
	c.applyUpsert(pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE))

	c.Shutdown()
	c.Shutdown()
	c.Shutdown()

	disposes := 0
	for _, e := range rec.snapshot() {
		if e == "gone-cache-shutdown:p1" {
			disposes++
		}
	}
	assert.Equal(t, 1, disposes, "OnGone must fire exactly once across multiple Shutdown calls")
}

func TestCache_ConcurrentShutdownAllBlockUntilDone(t *testing.T) {
	clk := newFakeClock()
	disposeStart := make(chan struct{})
	disposeRelease := make(chan struct{})
	cfg := Config[*testRider]{
		ShutdownGrace:      time.Hour,
		MissingGracePeriod: time.Hour,
		Logger:             silentLogger(),
		now:                clk.Now,
	}
	c := New(t.Context(), cfg)
	c.hooks = Hooks[*testRider]{
		OnLive: func(*clustermetadatapb.MultiPooler, *testRider) *testRider { return newRider() },
		OnGone: func(*clustermetadatapb.MultiPooler, *testRider, GoneReason) {
			close(disposeStart)
			<-disposeRelease
		},
	}
	c.applyUpsert(pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE))

	firstDone := make(chan struct{})
	go func() {
		c.Shutdown()
		close(firstDone)
	}()
	<-disposeStart

	secondDone := make(chan struct{})
	go func() {
		c.Shutdown()
		close(secondDone)
	}()

	select {
	case <-secondDone:
		t.Fatal("second Shutdown returned before first finished")
	case <-time.After(50 * time.Millisecond):
	}

	close(disposeRelease)
	<-firstDone
	<-secondDone
}

func TestCache_FilterDropsNonMatchingPoolers(t *testing.T) {
	clk := newFakeClock()
	rec := &hookRecorder{}
	cfg := Config[*testRider]{
		Filter: func(p *clustermetadatapb.MultiPooler) bool {
			return p.GetShardKey().GetDatabase() == "mydb"
		},
		ShutdownGrace:      time.Hour,
		MissingGracePeriod: time.Hour,
		Logger:             silentLogger(),
		now:                clk.Now,
	}
	c := New(t.Context(), cfg)
	c.hooks = Hooks[*testRider]{
		OnLive: func(p *clustermetadatapb.MultiPooler, _ *testRider) *testRider {
			rec.record("live:" + p.Id.Name)
			return newRider()
		},
	}
	defer c.Shutdown()

	c.applyUpsert(pool("zone1", "kept", "mydb", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE))
	c.applyUpsert(pool("zone1", "dropped", "otherdb", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE))

	assert.Equal(t, []string{"live:kept"}, rec.snapshot())
	assert.Equal(t, 1, c.Len())
}

// TestCache_ReconcileCellEvictsMissingPoolers verifies that a per-cell
// snapshot from the underlying topoWatch (the (re)connect path) evicts any
// entry the cache holds for that cell that's missing from the snapshot. This
// is the core contract that justifies topoWatch holding no pooler state.
func TestCache_ReconcileCellEvictsMissingPoolers(t *testing.T) {
	clk := newFakeClock()
	c, rec := newTestCache(t, clk, time.Hour, 0 /* MissingGracePeriod=0 so eviction is immediate */)
	defer c.Shutdown()

	// Seed two poolers in zone1 and one in zone2 via the watcher dispatch path.
	c.reconcileCell("zone1", []*clustermetadatapb.MultiPooler{
		pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE),
		pool("zone1", "p2", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE),
	})
	c.reconcileCell("zone2", []*clustermetadatapb.MultiPooler{
		pool("zone2", "q1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE),
	})
	require.Equal(t, []string{"live:p1", "live:p2", "live:q1"}, rec.snapshot())

	// A reconnect snapshot drops p2 and adds p3, leaving p1.
	c.reconcileCell("zone1", []*clustermetadatapb.MultiPooler{
		pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE),
		pool("zone1", "p3", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE),
	})

	// p2 evicted (missing from topology with grace=0), p3 went live; p1 stayed (proto.Equal suppresses dup); zone2 untouched.
	events := rec.snapshot()
	assert.Contains(t, events, "gone-missing-from-topo:p2", "p2 must be evicted by reconcile")
	assert.Contains(t, events, "live:p3", "p3 must be added by reconcile")
	_, p1Ok := c.Get(componentID("zone1", "p1"))
	_, p3Ok := c.Get(componentID("zone1", "p3"))
	_, q1Ok := c.Get(componentID("zone2", "q1"))
	assert.True(t, p1Ok)
	assert.True(t, p3Ok)
	assert.True(t, q1Ok, "cell zone2 must be untouched by zone1 reconcile")
}

// --------------------------------------------------------------------------
// LastReachedTimestamp / MissingGracePeriod: an entry whose topology record
// disappears (NoNode) should NOT be evicted as long as the rider keeps
// reporting fresh contact, because the operational risk we're guarding
// against is "etcd contents lost; pooler still up and responding."
// --------------------------------------------------------------------------

// reachableRider is a testRider with a configurable lastReached timestamp,
// used by the tests below to drive the LastReachedTimestamp predicate.
type reachableRider struct {
	testRider
	lastReached time.Time
}

// newReachCache builds a cache like newTestCache but installs a non-nil
// LastReachedTimestamp predicate that reads each rider's lastReached field.
func newReachCache(t *testing.T, clk *fakeClock, missingGracePeriod time.Duration) (*PoolerCache[*reachableRider], *hookRecorder) {
	t.Helper()
	rec := &hookRecorder{}
	cache := New(t.Context(), Config[*reachableRider]{
		MissingGracePeriod: missingGracePeriod,
		LastReachedTimestamp: func(r *reachableRider) time.Time {
			if r == nil {
				return time.Time{}
			}
			return r.lastReached
		},
		Logger: silentLogger(),
		now:    clk.Now,
	})
	cache.hooks = Hooks[*reachableRider]{
		OnLive: func(p *clustermetadatapb.MultiPooler, prev *reachableRider) *reachableRider {
			if prev != nil {
				rec.record("resume:" + p.Id.Name)
				return prev
			}
			rec.record("live:" + p.Id.Name)
			return &reachableRider{testRider: testRider{id: riderCounter.Add(1)}}
		},
		OnGone: func(p *clustermetadatapb.MultiPooler, _ *reachableRider, r GoneReason) {
			rec.record("gone-" + r.String() + ":" + p.Id.Name)
		},
	}
	return cache, rec
}

// setLastReached writes the rider's lastReached field so the predicate
// returns the given time on subsequent sweeps. Mirrors what production
// would do via DoUpdate, but stays in-test by reaching through GetRider.
func setLastReached(t *testing.T, c *PoolerCache[*reachableRider], id topoclient.ComponentID, when time.Time) {
	t.Helper()
	c.DoUpdate(id, func(r *reachableRider) *reachableRider {
		if r == nil {
			return nil
		}
		r.lastReached = when
		return r
	})
}

// TestCache_LastReachedSlidesDeadlineForward: an entry whose topology
// record is gone but whose rider keeps reporting fresh contact must not
// be evicted on the NoNode-anchored disposeAfter — the deadline should
// slide to lastReached + grace.
func TestCache_LastReachedSlidesDeadlineForward(t *testing.T) {
	clk := newFakeClock()
	c, rec := newReachCache(t, clk, time.Hour)
	defer c.Shutdown()

	id := componentID("zone1", "p1")
	c.applyUpsert(pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE))
	setLastReached(t, c, id, clk.Now())

	// Topology record disappears. disposeAfter = now + 1h.
	c.applyDelete(id)

	// 30 minutes in: pooler is still being reached. Bump lastReached
	// to "now" so the sliding deadline becomes t0+90m.
	clk.Advance(30 * time.Minute)
	setLastReached(t, c, id, clk.Now())

	// Advance to t0+75m: past the original disposeAfter (t0+60m) but
	// inside the slid deadline (t0+90m). Sweep must NOT evict.
	clk.Advance(45 * time.Minute)
	c.sweep()
	assert.Equal(t, []string{"live:p1"}, rec.snapshot(), "fresh contact must defer eviction past disposeAfter")
	_, stillThere := c.Get(id)
	assert.True(t, stillThere, "entry must survive sweep while lastReached is fresh")

	// Pooler stops being reached. Advance past the slid deadline of
	// t0+90m (lastReached t0+30m + grace 60m). Sweep must now evict.
	clk.Advance(30 * time.Minute)
	c.sweep()
	assert.Equal(t, []string{"live:p1", "gone-missing-from-topo:p1"}, rec.snapshot(),
		"once lastReached is stale by more than grace, sweep must evict")
}

// TestCache_LastReachedZeroDoesNotSlideDeadline: a rider that has never
// reported contact (zero timestamp) should not gain any extension —
// the entry must evict at the original disposeAfter.
func TestCache_LastReachedZeroDoesNotSlideDeadline(t *testing.T) {
	clk := newFakeClock()
	c, rec := newReachCache(t, clk, time.Hour)
	defer c.Shutdown()

	id := componentID("zone1", "p1")
	c.applyUpsert(pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE))
	// Deliberately do NOT set lastReached; rider's zero value stands.

	c.applyDelete(id)
	clk.Advance(time.Hour + time.Second)
	c.sweep()

	assert.Equal(t, []string{"live:p1", "gone-missing-from-topo:p1"}, rec.snapshot(),
		"zero lastReached must fall back to the disposeAfter deadline")
}

// TestCache_LastReachedDoesNotApplyToLiveEntries: the predicate is only
// consulted for StateMissingFromTopo entries. A Live entry whose
// lastReached is stale must NOT be evicted.
func TestCache_LastReachedDoesNotApplyToLiveEntries(t *testing.T) {
	clk := newFakeClock()
	c, rec := newReachCache(t, clk, time.Hour)
	defer c.Shutdown()

	id := componentID("zone1", "p1")
	c.applyUpsert(pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE))
	// Set lastReached to long ago — would force eviction if the predicate
	// applied to Live entries.
	setLastReached(t, c, id, clk.Now().Add(-100*time.Hour))

	clk.Advance(2 * time.Hour)
	c.sweep()
	assert.Equal(t, []string{"live:p1"}, rec.snapshot(),
		"Live entries are not subject to the lastReached deadline")
}

// TestCache_LastReachedNilPredicateMatchesOldBehavior: with no
// LastReachedTimestamp configured the cache must behave exactly as it
// did before — eviction strictly at NoNode + grace.
func TestCache_LastReachedNilPredicateMatchesOldBehavior(t *testing.T) {
	clk := newFakeClock()
	// Use the original newTestCache helper (no LastReachedTimestamp).
	c, rec := newTestCache(t, clk, time.Hour, time.Hour)
	defer c.Shutdown()

	id := componentID("zone1", "p1")
	c.applyUpsert(pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE))
	c.applyDelete(id)

	// One hour exactly hits disposeAfter; sweep evicts on the original
	// deadline because no predicate is configured.
	clk.Advance(time.Hour + time.Second)
	c.sweep()
	assert.Equal(t, []string{"live:p1", "gone-missing-from-topo:p1"}, rec.snapshot())
}

// --------------------------------------------------------------------------
// Stringer methods on State and GoneReason: tiny but worth pinning since
// they appear in log lines and metric labels.
// --------------------------------------------------------------------------

func TestState_String(t *testing.T) {
	assert.Equal(t, "live", StateLive.String())
	assert.Equal(t, "missing-from-topo", StateMissingFromTopo.String())
	assert.Equal(t, "unknown", State(99).String())
}

func TestGoneReason_String(t *testing.T) {
	assert.Equal(t, "shutdown", GoneShutdown.String())
	assert.Equal(t, "missing-from-topo", GoneMissingFromTopo.String())
	assert.Equal(t, "cache-shutdown", GoneCacheShutdown.String())
	assert.Equal(t, "unknown", GoneReason(99).String())
}

// --------------------------------------------------------------------------
// End-to-end through Start: prior tests bypass Start by driving applyUpsert
// directly. This one exercises the real Start path — sweeper goroutine,
// topoSource subscription, Sync barrier, and the read-side methods Get,
// GetRider, All, CellStatuses — against an in-memory topology. Doubles as
// the cell-removal hook coverage (onCellRemoved fires when memorytopo's
// DeleteCell is called).
// --------------------------------------------------------------------------

func TestCache_StartIntegratesWithMemoryTopo(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ts, factory := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	rec := &hookRecorder{}
	cache := New(ctx, Config[*testRider]{
		Source:             ts,
		ShutdownGrace:      time.Hour,
		MissingGracePeriod: time.Hour,
		Logger:             silentLogger(),
	})
	t.Cleanup(func() { cache.Shutdown() })

	cache.Start(Hooks[*testRider]{
		OnLive: func(p *clustermetadatapb.MultiPooler, _ *testRider) *testRider {
			rec.record("live:" + p.Id.Name)
			return newRider()
		},
		OnUpdate: func(_, curr *clustermetadatapb.MultiPooler, _ *testRider) {
			rec.record("update:" + curr.Id.Name)
		},
		OnGone: func(p *clustermetadatapb.MultiPooler, _ *testRider, r GoneReason) {
			rec.record("gone-" + r.String() + ":" + p.Id.Name)
		},
	})

	// Wait for the zone1 watcher goroutine to register with the broadcaster
	// before issuing any Sync. Without this, an early Sync can return
	// immediately because no per-cell sync channels are registered yet, and
	// subsequent assertions race the watcher's initial snapshot.
	require.Eventually(t, func() bool {
		return cache.topoSource.broadcaster.cellCount() >= 1
	}, 2*time.Second, 5*time.Millisecond, "zone1 watcher must register")

	// Sync against an empty cell: should return cleanly with no events.
	require.NoError(t, SyncForTest(t, cache, ctx))
	assert.Empty(t, rec.snapshot())
	assert.Empty(t, cache.All())

	// Create a pooler. After Sync, OnLive must have fired and the entry
	// must be visible via Get / GetRider / All / CellStatuses.
	p1 := &clustermetadatapb.MultiPooler{
		Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "p1"},
		ShardKey: &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
		Hostname: "p1.local",
	}
	require.NoError(t, ts.CreateMultiPooler(ctx, p1))
	require.NoError(t, SyncForTest(t, cache, ctx))

	assert.Equal(t, []string{"live:p1"}, rec.snapshot())

	id := topoclient.ComponentIDString(p1.Id)
	rider, ok := cache.GetRider(id)
	require.True(t, ok, "GetRider must surface a Live entry")
	require.NotNil(t, rider)

	all := cache.All()
	require.Len(t, all, 1)
	assert.Equal(t, StateLive, all[0].State)

	statuses := cache.CellStatuses()
	require.Len(t, statuses, 1)
	assert.Equal(t, "zone1", statuses[0].Cell)
	require.Len(t, statuses[0].Poolers, 1)
	assert.Equal(t, "p1", statuses[0].Poolers[0].Id.Name)

	// Add a second cell with its own pooler; Sync waits for the new
	// per-cell watcher to drain its initial snapshot.
	require.NoError(t, factory.AddCell(ctx, ts, "zone2"))
	p2 := &clustermetadatapb.MultiPooler{
		Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone2", Name: "p2"},
		ShardKey: &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
		Hostname: "p2.local",
	}
	require.NoError(t, ts.CreateMultiPooler(ctx, p2))
	// AddCell triggers async per-cell watcher registration; broadcaster.cellCount
	// is the deterministic signal that the new watcher is ready to participate
	// in Sync. Without this, Sync may run before zone2's watcher registers and
	// the p2 event is missed.
	require.Eventually(t, func() bool {
		return cache.topoSource.broadcaster.cellCount() >= 2
	}, 2*time.Second, 5*time.Millisecond)
	require.NoError(t, SyncForTest(t, cache, ctx))

	// OnLive fired for both poolers; CellStatuses surfaces both cells.
	require.Contains(t, rec.snapshot(), "live:p2")
	statuses = cache.CellStatuses()
	require.Len(t, statuses, 2)
	cells := []string{statuses[0].Cell, statuses[1].Cell}
	assert.Contains(t, cells, "zone1")
	assert.Contains(t, cells, "zone2")

	// Delete the cell — onCellRemoved fires and OnGone(GoneMissingFromTopo)
	// is queued for the entries in that cell (sweeper would evict them after
	// grace; we don't wait, just confirm the cell event arrived).
	beforeCells := cache.topoSource.broadcaster.cellCount()
	require.NoError(t, ts.DeleteCell(ctx, "zone2", true /*force*/))
	require.Eventually(t, func() bool {
		return cache.topoSource.broadcaster.cellCount() < beforeCells
	}, 2*time.Second, 5*time.Millisecond, "onCellRemoved must deregister the zone2 watcher")
}
