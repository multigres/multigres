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
func newTestCache(t *testing.T, clk *fakeClock, shutdownGrace, vanishedGrace time.Duration) (*PoolerCache[*testRider], *hookRecorder) {
	t.Helper()
	rec := &hookRecorder{}
	cfg := Config[*testRider]{
		Hooks: Hooks[*testRider]{
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
		},
		ShutdownGrace: shutdownGrace,
		VanishedGrace: vanishedGrace,
		Logger:        silentLogger(),
		now:           clk.Now,
	}
	return New(t.Context(), cfg), rec
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
	c, rec := newTestCache(t, clk, time.Hour, time.Hour) // ShutdownGrace is for ghost retention only
	defer c.Shutdown()

	c.applyUpsert(pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE))
	c.applyUpsert(pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN))

	// OnGone fired immediately, even though ShutdownGrace > 0.
	assert.Equal(t, []string{"live:p1", "gone-shutdown:p1"}, rec.snapshot())

	// Entry is invisible to normal reads.
	_, ok := c.Get(componentID("zone1", "p1"))
	assert.False(t, ok, "shut-down pooler must not appear in Get")

	// Ghost is retained for cleanup.
	ghosts := c.Ghosts()
	require.Len(t, ghosts, 1)
	assert.Equal(t, "p1", ghosts[0].ID.Name)
}

func TestCache_ColdShutdownDiscoveryIgnoresWithoutOnLive(t *testing.T) {
	clk := newFakeClock()
	c, rec := newTestCache(t, clk, time.Hour, time.Hour)
	defer c.Shutdown()

	c.applyUpsert(pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN))

	assert.Empty(t, rec.snapshot(), "cold-shutdown discovery must fire no hooks")
	_, ok := c.Get(componentID("zone1", "p1"))
	assert.False(t, ok, "cold-shutdown pooler must not be visible")
	require.Len(t, c.Ghosts(), 1, "cold-shutdown pooler must be tracked as a ghost")
}

func TestCache_GhostsExpireSilentlyOnSweep(t *testing.T) {
	clk := newFakeClock()
	c, rec := newTestCache(t, clk, time.Minute, time.Hour)
	defer c.Shutdown()

	c.applyUpsert(pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE))
	c.applyUpsert(pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN))

	clk.Advance(2 * time.Minute) // past ShutdownGrace
	c.sweep()

	// No new hook fires when the ghost expires — OnGone already happened.
	assert.Equal(t, []string{"live:p1", "gone-shutdown:p1"}, rec.snapshot())
	assert.Empty(t, c.Ghosts(), "ghost must be evicted after grace")
}

// TestCache_VanishedIsSilentUntilGrace verifies that NoNode does NOT fire
// OnGone immediately. The entry stays visible to reads for VanishedGrace.
func TestCache_VanishedIsSilentUntilGrace(t *testing.T) {
	clk := newFakeClock()
	c, rec := newTestCache(t, clk, time.Hour, time.Hour)
	defer c.Shutdown()

	c.applyUpsert(pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE))
	c.applyDelete(componentID("zone1", "p1"))

	// No hook fires on vanish; the entry remains visible (as Vanished).
	assert.Equal(t, []string{"live:p1"}, rec.snapshot())
	entry, ok := c.Get(componentID("zone1", "p1"))
	require.True(t, ok, "vanished pooler stays visible to reads during grace")
	assert.Equal(t, StateVanished, entry.State)

	// Past grace, sweep fires OnGone(Vanished) and removes the entry.
	clk.Advance(2 * time.Hour)
	c.sweep()
	assert.Equal(t, []string{"live:p1", "gone-vanished:p1"}, rec.snapshot())
	_, ok = c.Get(componentID("zone1", "p1"))
	assert.False(t, ok)
}

func TestCache_VanishedRecoveryWithinGracePreservesRider(t *testing.T) {
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
	assert.Same(t, originalRider, r2.Rider, "rider preserved across vanish-recovery")
}

func TestCache_RestartFromShutdownIsFreshOnLive(t *testing.T) {
	clk := newFakeClock()
	c, rec := newTestCache(t, clk, time.Hour, time.Hour)
	defer c.Shutdown()

	c.applyUpsert(pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE))
	r1, _ := c.Get(componentID("zone1", "p1"))
	rider1 := r1.Rider

	c.applyUpsert(pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN))
	clk.Advance(5 * time.Minute) // within ghost retention
	c.applyUpsert(pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE))

	// Restart-from-shutdown is a fresh discovery: OnLive(prev=nil) fires.
	assert.Equal(t, []string{"live:p1", "gone-shutdown:p1", "live:p1"}, rec.snapshot())
	r2, _ := c.Get(componentID("zone1", "p1"))
	assert.NotSame(t, rider1, r2.Rider, "restart-from-shutdown must allocate a fresh rider")
}

func TestCache_VanishedToShutdownFiresOnGone(t *testing.T) {
	clk := newFakeClock()
	c, rec := newTestCache(t, clk, time.Hour, time.Hour)
	defer c.Shutdown()

	c.applyUpsert(pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE))
	c.applyDelete(componentID("zone1", "p1"))
	// Entry is now Vanished. Pooler reappears as SHUTDOWN — first time OnGone
	// fires for this entry.
	c.applyUpsert(pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN))

	assert.Equal(t, []string{"live:p1", "gone-shutdown:p1"}, rec.snapshot())
	_, ok := c.Get(componentID("zone1", "p1"))
	assert.False(t, ok)
	require.Len(t, c.Ghosts(), 1)
}

func TestCache_DeleteOfGhostRemovesItSilently(t *testing.T) {
	clk := newFakeClock()
	c, rec := newTestCache(t, clk, time.Hour, time.Hour)
	defer c.Shutdown()

	c.applyUpsert(pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE))
	c.applyUpsert(pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN))
	require.Len(t, c.Ghosts(), 1)

	// Topology cleanup happens (external etcd delete).
	c.applyDelete(componentID("zone1", "p1"))

	assert.Equal(t, []string{"live:p1", "gone-shutdown:p1"}, rec.snapshot(),
		"hard-deletion of a ghost must not produce any hook")
	assert.Empty(t, c.Ghosts())
}

func TestCache_ZeroVanishedGraceFiresOnGoneInline(t *testing.T) {
	clk := newFakeClock()
	c, rec := newTestCache(t, clk, time.Hour, 0)
	defer c.Shutdown()

	c.applyUpsert(pool("zone1", "p1", "db", "tg", "0", clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE))
	c.applyDelete(componentID("zone1", "p1"))

	assert.Equal(t, []string{"live:p1", "gone-vanished:p1"}, rec.snapshot())
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

	// Now: p1 is Live, p2 is Vanished. Both should fire OnGone on Shutdown.
	c.Shutdown()

	got := rec.snapshot()
	// Count OnGone events; reasons reflect their state at shutdown.
	cacheShutdown, vanished := 0, 0
	for _, e := range got {
		switch e {
		case "gone-cache-shutdown:p1":
			cacheShutdown++
		case "gone-vanished:p2":
			vanished++
		}
	}
	assert.Equal(t, 1, cacheShutdown, "Live entry must fire OnGone(CacheShutdown), got: %v", got)
	assert.Equal(t, 1, vanished, "Vanished entry must fire OnGone(Vanished), got: %v", got)
}

func TestCache_ContextCancellationTriggersShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	clk := newFakeClock()
	rec := &hookRecorder{}
	cfg := Config[*testRider]{
		Hooks: Hooks[*testRider]{
			OnLive: func(p *clustermetadatapb.MultiPooler, _ *testRider) *testRider {
				rec.record("live:" + p.Id.Name)
				return newRider()
			},
			OnGone: func(p *clustermetadatapb.MultiPooler, _ *testRider, r GoneReason) {
				rec.record("gone-" + r.String() + ":" + p.Id.Name)
			},
		},
		ShutdownGrace: time.Hour,
		VanishedGrace: time.Hour,
		Logger:        silentLogger(),
		now:           clk.Now,
	}
	c := New(ctx, cfg)
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
		Hooks: Hooks[*testRider]{
			OnLive: func(*clustermetadatapb.MultiPooler, *testRider) *testRider { return newRider() },
			OnGone: func(*clustermetadatapb.MultiPooler, *testRider, GoneReason) {
				close(disposeStart)
				<-disposeRelease
			},
		},
		ShutdownGrace: time.Hour,
		VanishedGrace: time.Hour,
		Logger:        silentLogger(),
		now:           clk.Now,
	}
	c := New(t.Context(), cfg)
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
		Hooks: Hooks[*testRider]{
			OnLive: func(p *clustermetadatapb.MultiPooler, _ *testRider) *testRider {
				rec.record("live:" + p.Id.Name)
				return newRider()
			},
		},
		ShutdownGrace: time.Hour,
		VanishedGrace: time.Hour,
		Logger:        silentLogger(),
		now:           clk.Now,
	}
	c := New(t.Context(), cfg)
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
	c, rec := newTestCache(t, clk, time.Hour, 0 /* VanishedGrace=0 so eviction is immediate */)
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

	// p2 vanished, p3 went live; p1 stayed (proto.Equal suppresses dup); zone2 untouched.
	events := rec.snapshot()
	assert.Contains(t, events, "gone-vanished:p2", "p2 must be evicted by reconcile")
	assert.Contains(t, events, "live:p3", "p3 must be added by reconcile")
	_, p1Ok := c.Get(componentID("zone1", "p1"))
	_, p3Ok := c.Get(componentID("zone1", "p3"))
	_, q1Ok := c.Get(componentID("zone2", "q1"))
	assert.True(t, p1Ok)
	assert.True(t, p3Ok)
	assert.True(t, q1Ok, "cell zone2 must be untouched by zone1 reconcile")
}
