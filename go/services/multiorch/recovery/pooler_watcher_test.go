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

// newTestPoolerCache creates a PoolerCache backed by memorytopo for testing.
// onPoolerStopped and onPoolerDeleted default to nil; tests that need to
// observe those events call newPoolerCache directly (both branches in
// handlePoolerEvent nil-check before invoking).
func newTestPoolerCache(
	ctx context.Context,
	ts topoclient.Store,
	targets []config.WatchTarget,
	onNewPooler func(*clustermetadata.ID),
	logger *slog.Logger,
) *store.PoolerCache {
	return newPoolerCache(
		ctx,
		ts,
		func() []config.WatchTarget { return targets },
		onNewPooler,
		nil, /* onPoolerGone */
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
	onNew, countNew := newCallbackTracker()

	targets := []config.WatchTarget{{Database: "mydb"}}
	poolerStore := newTestPoolerCache(ctx, ts, targets, onNew, logger)
	poolerStore.Start()
	defer poolerStore.Shutdown()

	// Both poolers should be discovered
	ok := waitForCondition(t, 5*time.Second, func() bool {
		return poolerStore.Len() == 2
	})
	require.True(t, ok, "expected 2 poolers to be discovered, got %d", poolerStore.Len())

	p1, exists := poolerStore.GetRider(poolerKey("zone1", "pooler1"))
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
	onNew, countNew := newCallbackTracker()

	targets := []config.WatchTarget{{Database: "mydb"}}
	poolerStore := newTestPoolerCache(ctx, ts, targets, onNew, logger)
	poolerStore.Start()
	defer poolerStore.Shutdown()

	// Sync to confirm watcher started and processed initial (empty) topology
	require.NoError(t, poolerStore.Sync(ctx))
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

	p1, exists := poolerStore.GetRider(poolerKey("zone1", "pooler1"))
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
	onNew, countNew := newCallbackTracker()

	targets := []config.WatchTarget{{Database: "mydb"}}
	poolerStore := newTestPoolerCache(ctx, ts, targets, onNew, logger)
	poolerStore.Start()
	defer poolerStore.Shutdown()

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
	existing, _ := poolerStore.GetRider(pid)
	existing.IsUpToDate = true
	existing.IsLastCheckValid = true
	store.SeedCache(t, poolerStore, existing)

	// Update the pooler metadata in topology (e.g., hostname change)
	retrieved, err := ts.GetMultiPooler(ctx, &clustermetadata.ID{
		Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1",
	})
	require.NoError(t, err)
	retrieved.MultiPooler.Hostname = "host2"
	require.NoError(t, ts.UpdateMultiPooler(ctx, retrieved))

	// Wait for the update to propagate
	ok = waitForCondition(t, 5*time.Second, func() bool {
		p, exists := poolerStore.GetRider(pid)
		return exists && p.MultiPooler.Hostname == "host2"
	})
	require.True(t, ok, "expected hostname to be updated to host2")

	// Health-check state should be preserved
	updated, exists := poolerStore.GetRider(pid)
	require.True(t, exists)
	assert.True(t, updated.IsUpToDate, "IsUpToDate should be preserved")
	assert.True(t, updated.IsLastCheckValid, "IsLastCheckValid should be preserved")

	// An update to an existing pooler should NOT trigger another onNewPooler callback
	require.NoError(t, poolerStore.Sync(ctx))
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

	// Only watch mydb/tg1
	targets := []config.WatchTarget{{Database: "mydb", TableGroup: "tg1"}}
	poolerStore := newTestPoolerCache(ctx, ts, targets, func(*clustermetadata.ID) {}, logger)
	poolerStore.Start()
	defer poolerStore.Shutdown()

	ok := waitForCondition(t, 5*time.Second, func() bool {
		return poolerStore.Len() >= 1
	})
	require.True(t, ok)

	// Sync to ensure all events (including filtered ones) have been processed
	require.NoError(t, poolerStore.Sync(ctx))
	assert.Equal(t, 1, poolerStore.Len(), "only the watched pooler should be in the store")
	_, exists := poolerStore.GetRider(poolerKey("zone1", "watched"))
	assert.True(t, exists)
	_, exists = poolerStore.GetRider(poolerKey("zone1", "other-db"))
	assert.False(t, exists, "pooler in other database should be filtered out")
	_, exists = poolerStore.GetRider(poolerKey("zone1", "other-tg"))
	assert.False(t, exists, "pooler in other tablegroup should be filtered out")
}

func TestPoolerWatcher_NewCellDiscovered(t *testing.T) {
	ctx := t.Context()

	// Start with only zone1
	ts, factory := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	targets := []config.WatchTarget{{Database: "mydb"}}
	poolerStore := newTestPoolerCache(ctx, ts, targets, func(*clustermetadata.ID) {}, logger)
	poolerStore.Start()
	defer poolerStore.Shutdown()

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

	_, exists := poolerStore.GetRider(poolerKey("zone2", "pooler2"))
	assert.True(t, exists)
}

// TestPoolerWatcher_PoolerDeletedFromTopology verifies that deleting a
// pooler from topology evicts its store entry and fires the
// onDeletedPooler callback with the right ID. This is the authoritative
// signal that a pooler has gone away (typically because its OnClose
// unregisterFunc ran during graceful shutdown); callers wire the callback
// to per-pooler cleanup such as HealthStream.Stop.
// TestPoolerWatcher_PoolerDeletedFromTopology pins the NoNode contract:
// NoNode is presumed accidental. The watcher does NOT evict the cache and
// does NOT fire onPoolerGone immediately. The entry remains visible to
// analyzers during the vanish grace window so accidental etcd deletes can
// self-heal if the pooler reappears. onPoolerGone fires only at grace
// expiry (hours away in this configuration).
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

	var goneIDs []*clustermetadata.ID
	var goneMu sync.Mutex
	onGone := func(id *clustermetadata.ID) {
		goneMu.Lock()
		defer goneMu.Unlock()
		goneIDs = append(goneIDs, id)
	}

	targets := []config.WatchTarget{{Database: "mydb"}}
	poolerStore := newPoolerCache(ctx, ts, func() []config.WatchTarget { return targets },
		func(*clustermetadata.ID) {}, onGone, logger)
	poolerStore.Start()
	defer poolerStore.Shutdown()

	require.True(t, waitForCondition(t, 5*time.Second, func() bool {
		return poolerStore.Len() == 1
	}))

	require.NoError(t, ts.UnregisterMultiPooler(ctx, poolerID))
	require.NoError(t, poolerStore.Sync(ctx))

	// Entry remains cached so analyzers see it during vanish grace.
	assert.Equal(t, 1, poolerStore.Len(), "NoNode must leave the entry visible during grace")
	_, ok := poolerStore.GetRider(poolerKey("zone1", "pooler1"))
	assert.True(t, ok, "vanished pooler should still be cached during grace")

	// onPoolerGone must not fire immediately — only after grace expiry.
	goneMu.Lock()
	defer goneMu.Unlock()
	assert.Empty(t, goneIDs, "onPoolerGone must not fire on NoNode until grace expires")
}

// TestPoolerWatcher_PoolerEntersShutdownLifecycle pins the SHUTDOWN contract:
// from orch's perspective SHUTDOWN is dead. The watcher invokes onPoolerGone
// immediately AND evicts the store entry — no soft-delete intermediate state
// at the orch level. The cache layer below retains a ghost record for future
// etcd-cleanup, but that is invisible to orch's store.
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

	var goneIDs []*clustermetadata.ID
	var goneMu sync.Mutex
	onGone := func(id *clustermetadata.ID) {
		goneMu.Lock()
		defer goneMu.Unlock()
		goneIDs = append(goneIDs, id)
	}

	targets := []config.WatchTarget{{Database: "mydb"}}
	poolerStore := newPoolerCache(ctx, ts, func() []config.WatchTarget { return targets },
		func(*clustermetadata.ID) {}, onGone, logger)
	poolerStore.Start()
	defer poolerStore.Shutdown()

	require.True(t, waitForCondition(t, 5*time.Second, func() bool {
		return poolerStore.Len() == 1
	}))

	// Transition ACTIVE -> SHUTDOWN.
	_, err := ts.UpdateMultiPoolerFields(ctx, poolerID, func(mp *clustermetadata.MultiPooler) error {
		mp.LifecycleStatus = &clustermetadata.PoolerLifecycle{
			Status: clustermetadata.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN,
			Reason: "pooler shutdown",
		}
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, poolerStore.Sync(ctx))

	// Store entry must be evicted: SHUTDOWN means gone.
	assert.Equal(t, 0, poolerStore.Len(), "SHUTDOWN must evict the orch store")
	_, ok := poolerStore.GetRider(poolerKey("zone1", "pooler1"))
	assert.False(t, ok, "store entry must be gone after SHUTDOWN")

	// onPoolerGone fires exactly once.
	goneMu.Lock()
	defer goneMu.Unlock()
	require.Len(t, goneIDs, 1, "onPoolerGone should fire exactly once for SHUTDOWN")
	assert.Equal(t, poolerID.Name, goneIDs[0].Name)
	assert.Equal(t, poolerID.Cell, goneIDs[0].Cell)
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
	onNew, countNew := newCallbackTracker()

	targets := []config.WatchTarget{{Database: "mydb"}}
	poolerStore := newPoolerCache(ctx, ts, func() []config.WatchTarget { return targets },
		onNew, nil /* onPoolerGone */, logger)
	poolerStore.Start()
	defer poolerStore.Shutdown()

	// Initial discovery fires onPoolerLive once.
	require.True(t, waitForCondition(t, 5*time.Second, func() bool { return countNew() == 1 }),
		"new pooler should trigger onPoolerLive on discovery")

	// Transition ACTIVE -> SHUTDOWN evicts the store entry.
	_, err := ts.UpdateMultiPoolerFields(ctx, poolerID, func(mp *clustermetadata.MultiPooler) error {
		mp.LifecycleStatus = &clustermetadata.PoolerLifecycle{
			Status: clustermetadata.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN,
		}
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, poolerStore.Sync(ctx))
	require.Equal(t, 0, poolerStore.Len(), "SHUTDOWN must evict the store")

	// Pooler comes back: lifecycle transitions back to ACTIVE. The cache
	// recognizes restart-from-ghost and re-fires OnLive → onPoolerLive.
	_, err = ts.UpdateMultiPoolerFields(ctx, poolerID, func(mp *clustermetadata.MultiPooler) error {
		mp.LifecycleStatus = &clustermetadata.PoolerLifecycle{
			Status: clustermetadata.PoolerLifecycleStatus_LIFECYCLE_ACTIVE,
		}
		return nil
	})
	require.NoError(t, err)

	require.True(t, waitForCondition(t, 5*time.Second, func() bool { return countNew() == 2 }),
		"restart after SHUTDOWN must re-fire onPoolerLive so the health stream restarts")
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
	onNew, countNew := newCallbackTracker()

	var goneCalls int
	var cbMu sync.Mutex
	onGone := func(*clustermetadata.ID) { cbMu.Lock(); goneCalls++; cbMu.Unlock() }

	targets := []config.WatchTarget{{Database: "mydb"}}
	poolerStore := newPoolerCache(ctx, ts, func() []config.WatchTarget { return targets },
		onNew, onGone, logger)
	poolerStore.Start()
	defer poolerStore.Shutdown()

	// Give the watcher time to process the initial SHUTDOWN entry; it should
	// reach a steady state with the store empty (cold-discovered SHUTDOWN
	// poolers are tracked as ghosts in the cache, not as store entries).
	require.NoError(t, poolerStore.Sync(ctx))

	assert.Equal(t, 0, poolerStore.Len(), "cold-discovered SHUTDOWN must not populate the orch store")
	assert.Equal(t, 0, countNew(), "onPoolerLive must not fire for cold-discovered SHUTDOWN")

	cbMu.Lock()
	defer cbMu.Unlock()
	assert.Equal(t, 0, goneCalls, "onPoolerGone must not fire for cold-discovered SHUTDOWN")
}
