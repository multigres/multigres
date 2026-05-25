// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package recovery

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// newTestPoolerWatcher creates a PoolerWatcher backed by memorytopo for testing.
// onPoolerStopped and onPoolerDeleted default to nil; tests that need to
// observe those events call NewPoolerWatcher directly (both branches in
// handlePoolerEvent nil-check before invoking).
func newTestPoolerWatcher(
	ctx context.Context,
	ts topoclient.Store,
	targets []config.WatchTarget,
	poolerStore *store.PoolerStore,
	onNewPooler func(*clustermetadata.ID),
	logger *slog.Logger,
) *PoolerWatcher {
	return NewPoolerWatcher(
		ctx,
		ts,
		func() []config.WatchTarget { return targets },
		poolerStore,
		onNewPooler,
		nil, /* onPoolerStopped */
		nil, /* onPoolerDeleted */
		logger,
	)
}

// newCallbackTracker returns an onNewPooler callback and a Len function for tests
// that need to assert how many new-pooler notifications were fired.
func newCallbackTracker() (func(*clustermetadata.ID), func() int) {
	var mu sync.Mutex
	var ids []*clustermetadata.ID
	onNew := func(id *clustermetadata.ID) {
		mu.Lock()
		ids = append(ids, id)
		mu.Unlock()
	}
	count := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(ids)
	}
	return onNew, count
}

// waitForCondition polls fn until it returns true or the timeout elapses.
func waitForCondition(t *testing.T, timeout time.Duration, fn func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func TestPoolerWatcher_InitialDiscovery(t *testing.T) {
	ctx := t.Context()

	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	// Pre-populate topology before watcher starts
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id: &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"},
		ShardKey: &clustermetadata.ShardKey{
			Database:   "mydb",
			TableGroup: "tg1",
			Shard:      "0",
		},
		Type:     clustermetadata.PoolerType_PRIMARY,
		Hostname: "host1",
	}))
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id: &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler2"},
		ShardKey: &clustermetadata.ShardKey{
			Database:   "mydb",
			TableGroup: "tg1",
			Shard:      "0",
		},
		Type:     clustermetadata.PoolerType_REPLICA,
		Hostname: "host2",
	}))

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	poolerStore := store.NewPoolerStore(nil, logger)
	onNew, countNew := newCallbackTracker()

	targets := []config.WatchTarget{{Database: "mydb"}}
	watcher := newTestPoolerWatcher(ctx, ts, targets, poolerStore, onNew, logger)
	watcher.Start()
	defer watcher.Stop()

	// Both poolers should be discovered
	ok := waitForCondition(t, 5*time.Second, func() bool {
		return poolerStore.Len() == 2
	})
	require.True(t, ok, "expected 2 poolers to be discovered, got %d", poolerStore.Len())

	p1, exists := poolerStore.Get(poolerKey("zone1", "pooler1"))
	require.True(t, exists)
	assert.Equal(t, "host1", p1.MultiPooler.Hostname)
	assert.False(t, p1.IsUpToDate, "new pooler should not be marked up-to-date")

	// Both should trigger onNewPooler
	ok = waitForCondition(t, 5*time.Second, func() bool { return countNew() == 2 })
	assert.True(t, ok, "expected 2 onNewPooler callbacks, got %d", countNew())
}

func TestPoolerWatcher_NewPoolerAddedAfterStart(t *testing.T) {
	ctx := t.Context()

	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	poolerStore := store.NewPoolerStore(nil, logger)
	onNew, countNew := newCallbackTracker()

	targets := []config.WatchTarget{{Database: "mydb"}}
	watcher := newTestPoolerWatcher(ctx, ts, targets, poolerStore, onNew, logger)
	watcher.Start()
	defer watcher.Stop()

	// Sync to confirm watcher started and processed initial (empty) topology
	require.NoError(t, watcher.Sync(ctx))
	assert.Equal(t, 0, poolerStore.Len())

	// Add a pooler after the watcher has started
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id: &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"},
		ShardKey: &clustermetadata.ShardKey{
			Database:   "mydb",
			TableGroup: "tg1",
			Shard:      "0",
		},
		Type:     clustermetadata.PoolerType_PRIMARY,
		Hostname: "host1",
	}))

	ok := waitForCondition(t, 5*time.Second, func() bool {
		return poolerStore.Len() == 1
	})
	require.True(t, ok, "expected pooler to be discovered after add")

	p1, exists := poolerStore.Get(poolerKey("zone1", "pooler1"))
	require.True(t, exists)
	assert.Equal(t, "host1", p1.MultiPooler.Hostname)

	ok = waitForCondition(t, 5*time.Second, func() bool { return countNew() == 1 })
	assert.True(t, ok, "expected 1 onNewPooler callback, got %d", countNew())
}

func TestPoolerWatcher_PoolerMetadataUpdate(t *testing.T) {
	ctx := t.Context()

	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id: &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"},
		ShardKey: &clustermetadata.ShardKey{
			Database:   "mydb",
			TableGroup: "tg1",
			Shard:      "0",
		},
		Type:     clustermetadata.PoolerType_PRIMARY,
		Hostname: "host1",
	}))

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	poolerStore := store.NewPoolerStore(nil, logger)
	onNew, countNew := newCallbackTracker()

	targets := []config.WatchTarget{{Database: "mydb"}}
	watcher := newTestPoolerWatcher(ctx, ts, targets, poolerStore, onNew, logger)
	watcher.Start()
	defer watcher.Stop()

	// Wait for initial discovery
	ok := waitForCondition(t, 5*time.Second, func() bool {
		return poolerStore.Len() == 1
	})
	require.True(t, ok)

	// Wait for the initial onNewPooler callback
	ok = waitForCondition(t, 5*time.Second, func() bool { return countNew() == 1 })
	require.True(t, ok, "new pooler should trigger callback once on discovery")
	callbacksAfterDiscovery := countNew()

	// Simulate a health-check populating some state
	pid := poolerKey("zone1", "pooler1")
	existing, _ := poolerStore.Get(pid)
	existing.IsUpToDate = true
	existing.IsLastCheckValid = true
	poolerStore.Set(pid, existing)

	// Update the pooler metadata in topology (e.g., hostname change)
	retrieved, err := ts.GetMultiPooler(ctx, &clustermetadata.ID{
		Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1",
	})
	require.NoError(t, err)
	retrieved.MultiPooler.Hostname = "host2"
	require.NoError(t, ts.UpdateMultiPooler(ctx, retrieved))

	// Wait for the update to propagate
	ok = waitForCondition(t, 5*time.Second, func() bool {
		p, exists := poolerStore.Get(pid)
		return exists && p.MultiPooler.Hostname == "host2"
	})
	require.True(t, ok, "expected hostname to be updated to host2")

	// Health-check state should be preserved
	updated, exists := poolerStore.Get(pid)
	require.True(t, exists)
	assert.True(t, updated.IsUpToDate, "IsUpToDate should be preserved")
	assert.True(t, updated.IsLastCheckValid, "IsLastCheckValid should be preserved")

	// An update to an existing pooler should NOT trigger another onNewPooler callback
	require.NoError(t, watcher.Sync(ctx))
	assert.Equal(t, callbacksAfterDiscovery, countNew(), "existing pooler should not re-trigger callback on metadata update")
}

func TestPoolerWatcher_WatchTargetFiltering(t *testing.T) {
	ctx := t.Context()

	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	// Add poolers in different databases/tablegroups
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id: &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "watched"},
		ShardKey: &clustermetadata.ShardKey{
			Database:   "mydb",
			TableGroup: "tg1",
			Shard:      "0",
		},
	}))
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id: &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "other-db"},
		ShardKey: &clustermetadata.ShardKey{
			Database:   "otherdb",
			TableGroup: "tg1",
			Shard:      "0",
		},
	}))
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id: &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "other-tg"},
		ShardKey: &clustermetadata.ShardKey{
			Database:   "mydb",
			TableGroup: "tg2",
			Shard:      "0",
		},
	}))

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	poolerStore := store.NewPoolerStore(nil, logger)

	// Only watch mydb/tg1
	targets := []config.WatchTarget{{Database: "mydb", TableGroup: "tg1"}}
	watcher := newTestPoolerWatcher(ctx, ts, targets, poolerStore, func(*clustermetadata.ID) {}, logger)
	watcher.Start()
	defer watcher.Stop()

	ok := waitForCondition(t, 5*time.Second, func() bool {
		return poolerStore.Len() >= 1
	})
	require.True(t, ok)

	// Sync to ensure all events (including filtered ones) have been processed
	require.NoError(t, watcher.Sync(ctx))
	assert.Equal(t, 1, poolerStore.Len(), "only the watched pooler should be in the store")
	_, exists := poolerStore.Get(poolerKey("zone1", "watched"))
	assert.True(t, exists)
	_, exists = poolerStore.Get(poolerKey("zone1", "other-db"))
	assert.False(t, exists, "pooler in other database should be filtered out")
	_, exists = poolerStore.Get(poolerKey("zone1", "other-tg"))
	assert.False(t, exists, "pooler in other tablegroup should be filtered out")
}

func TestPoolerWatcher_NewCellDiscovered(t *testing.T) {
	ctx := t.Context()

	// Start with only zone1
	ts, factory := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	poolerStore := store.NewPoolerStore(nil, logger)

	targets := []config.WatchTarget{{Database: "mydb"}}
	watcher := newTestPoolerWatcher(ctx, ts, targets, poolerStore, func(*clustermetadata.ID) {}, logger)
	watcher.Start()
	defer watcher.Stop()

	// Add a pooler in zone1
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id: &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"},
		ShardKey: &clustermetadata.ShardKey{
			Database:   "mydb",
			TableGroup: "tg1",
			Shard:      "0",
		},
	}))

	ok := waitForCondition(t, 5*time.Second, func() bool {
		return poolerStore.Len() == 1
	})
	require.True(t, ok, "expected zone1 pooler to be discovered")

	// Add zone2 cell and a pooler in it
	require.NoError(t, factory.AddCell(ctx, ts, "zone2"))
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id: &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone2", Name: "pooler2"},
		ShardKey: &clustermetadata.ShardKey{
			Database:   "mydb",
			TableGroup: "tg1",
			Shard:      "0",
		},
	}))

	ok = waitForCondition(t, 5*time.Second, func() bool {
		return poolerStore.Len() == 2
	})
	require.True(t, ok, "expected zone2 pooler to be discovered after new cell added")

	_, exists := poolerStore.Get(poolerKey("zone2", "pooler2"))
	assert.True(t, exists)
}

// TestPoolerWatcher_PoolerDeletedFromTopology verifies that deleting a
// pooler from topology evicts its store entry and fires the
// onDeletedPooler callback with the right ID. This is the authoritative
// signal that a pooler has gone away (typically because its OnClose
// unregisterFunc ran during graceful shutdown); callers wire the callback
// to per-pooler cleanup such as HealthStream.Stop.
// TestPoolerWatcher_PoolerDeletedFromTopology pins the new NoNode contract:
// the watcher logs the event and invokes onPoolerDeleted (the reserved hook)
// if it was wired, but it does NOT evict the in-memory cache and does NOT
// fire onPoolerStopped. The intent is that an accidental external deletion
// of a still-running pooler's entry leaves the orchestrator's view of that
// pooler — including its open health stream — intact.
func TestPoolerWatcher_PoolerDeletedFromTopology(t *testing.T) {
	ctx := t.Context()

	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	poolerID := &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"}
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id: poolerID,
		ShardKey: &clustermetadata.ShardKey{
			Database:   "mydb",
			TableGroup: "tg1",
			Shard:      "0",
		},
		LifecycleStatus: &clustermetadata.PoolerLifecycle{
			Status: clustermetadata.PoolerLifecycleStatus_LIFECYCLE_ACTIVE,
		},
	}))

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	poolerStore := store.NewPoolerStore(nil, logger)

	var stoppedIDs []*clustermetadata.ID
	var stoppedMu sync.Mutex
	onStopped := func(id *clustermetadata.ID) {
		stoppedMu.Lock()
		defer stoppedMu.Unlock()
		stoppedIDs = append(stoppedIDs, id)
	}

	var deletedIDs []*clustermetadata.ID
	var deletedMu sync.Mutex
	onDeleted := func(id *clustermetadata.ID) {
		deletedMu.Lock()
		defer deletedMu.Unlock()
		deletedIDs = append(deletedIDs, id)
	}

	targets := []config.WatchTarget{{Database: "mydb"}}
	watcher := NewPoolerWatcher(ctx, ts, func() []config.WatchTarget { return targets },
		poolerStore, func(*clustermetadata.ID) {}, onStopped, onDeleted, logger)
	watcher.Start()
	defer watcher.Stop()

	require.True(t, waitForCondition(t, 5*time.Second, func() bool {
		return poolerStore.Len() == 1
	}))

	require.NoError(t, ts.UnregisterMultiPooler(ctx, poolerID))
	require.NoError(t, watcher.Sync(ctx))

	// Cache entry must survive: NoNode is a no-op on the orchestrator side.
	assert.Equal(t, 1, poolerStore.Len(), "NoNode must not evict the cache")
	_, ok := poolerStore.Get(poolerKey("zone1", "pooler1"))
	assert.True(t, ok, "deleted pooler should still be cached")

	// onPoolerDeleted fires; onPoolerStopped does not.
	deletedMu.Lock()
	defer deletedMu.Unlock()
	require.Len(t, deletedIDs, 1, "onPoolerDeleted should fire exactly once for a single NoNode event")
	assert.Equal(t, poolerID.Name, deletedIDs[0].Name)
	assert.Equal(t, poolerID.Cell, deletedIDs[0].Cell)

	stoppedMu.Lock()
	defer stoppedMu.Unlock()
	assert.Empty(t, stoppedIDs, "onPoolerStopped must not fire on a NoNode event")
}

// TestPoolerWatcher_PoolerEntersShutdownLifecycle pins the new lifecycle-
// SHUTDOWN contract: when a tracked pooler's topology entry transitions to
// LifecycleStatus=LIFECYCLE_SHUTDOWN, the watcher invokes onPoolerStopped
// (so the caller can tear down per-pooler resources like the health stream)
// but deliberately does NOT evict the cache. Analyzers need the cached
// PoolerHealthState to fire failover (e.g. LeaderIsDeadAnalyzer); the 4 h
// bookkeeping eviction handles eventual cleanup. onPoolerDeleted must not
// fire — the topology entry is still present, just in its terminal state.
func TestPoolerWatcher_PoolerEntersShutdownLifecycle(t *testing.T) {
	ctx := t.Context()

	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	poolerID := &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"}
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id: poolerID,
		ShardKey: &clustermetadata.ShardKey{
			Database:   "mydb",
			TableGroup: "tg1",
			Shard:      "0",
		},
		LifecycleStatus: &clustermetadata.PoolerLifecycle{
			Status: clustermetadata.PoolerLifecycleStatus_LIFECYCLE_ACTIVE,
		},
	}))

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	poolerStore := store.NewPoolerStore(nil, logger)

	var stoppedIDs []*clustermetadata.ID
	var stoppedMu sync.Mutex
	onStopped := func(id *clustermetadata.ID) {
		stoppedMu.Lock()
		defer stoppedMu.Unlock()
		stoppedIDs = append(stoppedIDs, id)
	}

	var deletedIDs []*clustermetadata.ID
	var deletedMu sync.Mutex
	onDeleted := func(id *clustermetadata.ID) {
		deletedMu.Lock()
		defer deletedMu.Unlock()
		deletedIDs = append(deletedIDs, id)
	}

	targets := []config.WatchTarget{{Database: "mydb"}}
	watcher := NewPoolerWatcher(ctx, ts, func() []config.WatchTarget { return targets },
		poolerStore, func(*clustermetadata.ID) {}, onStopped, onDeleted, logger)
	watcher.Start()
	defer watcher.Stop()

	require.True(t, waitForCondition(t, 5*time.Second, func() bool {
		return poolerStore.Len() == 1
	}))

	// Transition ACTIVE -> SHUTDOWN via read-modify-write.
	_, err := ts.UpdateMultiPoolerFields(ctx, poolerID, func(mp *clustermetadata.MultiPooler) error {
		mp.LifecycleStatus = &clustermetadata.PoolerLifecycle{
			Status: clustermetadata.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN,
			Reason: "pooler shutdown",
		}
		return nil
	})
	require.NoError(t, err)

	require.NoError(t, watcher.Sync(ctx))

	// Cache entry must survive: the SHUTDOWN transition is a signal, not a
	// reason to evict. Analyzers still need to see the entry.
	assert.Equal(t, 1, poolerStore.Len(), "SHUTDOWN transition must not evict the cache")
	cached, ok := poolerStore.Get(poolerKey("zone1", "pooler1"))
	require.True(t, ok, "cached entry should still be present")
	assert.Equal(t,
		clustermetadata.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN,
		cached.MultiPooler.GetLifecycleStatus().GetStatus(),
		"cached MultiPooler should reflect the SHUTDOWN transition")

	// onPoolerStopped fires; onPoolerDeleted does not.
	stoppedMu.Lock()
	defer stoppedMu.Unlock()
	require.Len(t, stoppedIDs, 1, "onPoolerStopped should fire exactly once for a SHUTDOWN transition")
	assert.Equal(t, poolerID.Name, stoppedIDs[0].Name)
	assert.Equal(t, poolerID.Cell, stoppedIDs[0].Cell)

	deletedMu.Lock()
	defer deletedMu.Unlock()
	assert.Empty(t, deletedIDs, "onPoolerDeleted must not fire on a lifecycle transition")
}

// TestPoolerWatcher_RestartAfterShutdownFiresOnNewPooler verifies the
// re-entry path: a pooler that transitioned to SHUTDOWN and then comes back
// up (writes STARTING/ACTIVE via RegisterMultiPooler(allowUpdate=true))
// re-triggers onNewPooler so the orchestrator restarts its health stream.
// The cache entry is retained across SHUTDOWN, so the watcher detects the
// SHUTDOWN→non-SHUTDOWN lifecycle transition and explicitly re-fires the
// discovery callback.
func TestPoolerWatcher_RestartAfterShutdownFiresOnNewPooler(t *testing.T) {
	ctx := t.Context()

	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	poolerID := &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"}
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id: poolerID,
		ShardKey: &clustermetadata.ShardKey{
			Database:   "mydb",
			TableGroup: "tg1",
			Shard:      "0",
		},
		LifecycleStatus: &clustermetadata.PoolerLifecycle{
			Status: clustermetadata.PoolerLifecycleStatus_LIFECYCLE_ACTIVE,
		},
	}))

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	poolerStore := store.NewPoolerStore(nil, logger)
	onNew, countNew := newCallbackTracker()

	targets := []config.WatchTarget{{Database: "mydb"}}
	watcher := NewPoolerWatcher(ctx, ts, func() []config.WatchTarget { return targets },
		poolerStore, onNew, func(*clustermetadata.ID) {} /* onPoolerStopped */, nil, logger)
	watcher.Start()
	defer watcher.Stop()

	// Initial discovery fires onNewPooler once.
	require.True(t, waitForCondition(t, 5*time.Second, func() bool { return countNew() == 1 }),
		"new pooler should trigger onNewPooler on discovery")

	// Transition ACTIVE -> SHUTDOWN (cache stays; onPoolerStopped fires).
	_, err := ts.UpdateMultiPoolerFields(ctx, poolerID, func(mp *clustermetadata.MultiPooler) error {
		mp.LifecycleStatus = &clustermetadata.PoolerLifecycle{
			Status: clustermetadata.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN,
		}
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, watcher.Sync(ctx))
	require.Equal(t, 1, poolerStore.Len(), "cache must be retained across SHUTDOWN")

	// Pooler comes back: lifecycle transitions back to ACTIVE.
	_, err = ts.UpdateMultiPoolerFields(ctx, poolerID, func(mp *clustermetadata.MultiPooler) error {
		mp.LifecycleStatus = &clustermetadata.PoolerLifecycle{
			Status: clustermetadata.PoolerLifecycleStatus_LIFECYCLE_ACTIVE,
		}
		return nil
	})
	require.NoError(t, err)

	require.True(t, waitForCondition(t, 5*time.Second, func() bool { return countNew() == 2 }),
		"restart after SHUTDOWN must re-fire onNewPooler so the health stream restarts")
}

// TestPoolerWatcher_ColdStartShutdownIgnored verifies that an already-SHUTDOWN
// pooler discovered for the first time (e.g. orchestrator restart while the
// entry is still in topology) is cached but triggers no callbacks. The cache
// entry lets the watcher detect a future SHUTDOWN→non-SHUTDOWN transition
// and fire onNewPooler then, but no health stream is opened immediately —
// there's nothing live to monitor.
func TestPoolerWatcher_ColdStartShutdownIgnored(t *testing.T) {
	ctx := t.Context()

	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	// Pre-existing SHUTDOWN entry.
	poolerID := &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"}
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id: poolerID,
		ShardKey: &clustermetadata.ShardKey{
			Database:   "mydb",
			TableGroup: "tg1",
			Shard:      "0",
		},
		LifecycleStatus: &clustermetadata.PoolerLifecycle{
			Status: clustermetadata.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN,
		},
	}))

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	poolerStore := store.NewPoolerStore(nil, logger)
	onNew, countNew := newCallbackTracker()

	var stoppedCalls, deletedCalls int
	var cbMu sync.Mutex
	onStopped := func(*clustermetadata.ID) { cbMu.Lock(); stoppedCalls++; cbMu.Unlock() }
	onDeleted := func(*clustermetadata.ID) { cbMu.Lock(); deletedCalls++; cbMu.Unlock() }

	targets := []config.WatchTarget{{Database: "mydb"}}
	watcher := NewPoolerWatcher(ctx, ts, func() []config.WatchTarget { return targets },
		poolerStore, onNew, onStopped, onDeleted, logger)
	watcher.Start()
	defer watcher.Stop()

	// Wait for the watcher to process the initial pooler event, then drain
	// any remaining events.
	require.True(t, waitForCondition(t, 5*time.Second, func() bool {
		return poolerStore.Len() == 1
	}), "watcher should cache the SHUTDOWN entry")
	require.NoError(t, watcher.Sync(ctx))

	cached, ok := poolerStore.Get(poolerKey("zone1", "pooler1"))
	require.True(t, ok, "cached entry should be present")
	assert.Equal(t,
		clustermetadata.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN,
		cached.MultiPooler.GetLifecycleStatus().GetStatus(),
		"cached MultiPooler lifecycle should reflect SHUTDOWN")
	assert.Equal(t, 0, countNew(), "onNewPooler must not fire for an already-SHUTDOWN pooler")

	cbMu.Lock()
	defer cbMu.Unlock()
	assert.Equal(t, 0, stoppedCalls, "onPoolerStopped must not fire for cold-start SHUTDOWN")
	assert.Equal(t, 0, deletedCalls, "onPoolerDeleted must not fire for cold-start SHUTDOWN")
}
