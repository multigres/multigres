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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// newTestPoolerCache creates a PoolerCache backed by memorytopo for testing.
// It constructs a HealthStream wired to a fake rpc client so the cache's
// OnLive hook can spawn (and OnGone can cancel) per-pooler stream goroutines
// without requiring the test to drive the stream itself. Tests that need to
// observe stream-spawn events should construct the HealthStream themselves
// and pass it to newPoolerCache directly.
func newTestPoolerCache(
	ctx context.Context,
	ts topoclient.Store,
	targets []config.WatchTarget,
	logger *slog.Logger,
) *store.PoolerCache {
	hs := store.NewHealthStreamFactory(ctx, rpcclient.NewFakeClient(), logger)
	cache := newPoolerCache(
		ctx,
		ts,
		func() []config.WatchTarget { return targets },
		logger,
	)
	cache.Start(poolerCacheHooks(ctx, cache, hs, logger))
	return cache
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

	targets := []config.WatchTarget{{Database: "mydb"}}
	poolerStore := newTestPoolerCache(ctx, ts, targets, logger)
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

	// OnLive must have run for each discovered pooler — the cache rider's
	// Stream handle (installed by the OnLive hook via HealthStream.spawnStream)
	// is the observable proof.
	assert.NotNil(t, p1.HealthStream, "OnLive should have spawned a stream for pooler1")
	p2, exists := poolerStore.GetRider(poolerKey("zone1", "pooler2"))
	require.True(t, exists)
	assert.NotNil(t, p2.HealthStream, "OnLive should have spawned a stream for pooler2")
}

func TestPoolerWatcher_NewPoolerAddedAfterStart(t *testing.T) {
	ctx := t.Context()

	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	targets := []config.WatchTarget{{Database: "mydb"}}
	poolerStore := newTestPoolerCache(ctx, ts, targets, logger)
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
	assert.NotNil(t, p1.HealthStream, "OnLive should have spawned a stream on discovery")
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

	targets := []config.WatchTarget{{Database: "mydb"}}
	poolerStore := newTestPoolerCache(ctx, ts, targets, logger)
	defer poolerStore.Shutdown()

	// Wait for initial discovery
	ok := waitForCondition(t, 5*time.Second, func() bool {
		return poolerStore.Len() == 1
	})
	require.True(t, ok)

	// Capture the StreamHandle installed by OnLive so we can confirm a metadata
	// update doesn't trigger a second OnLive (which would replace the handle).
	pid := poolerKey("zone1", "pooler1")
	existing, _ := poolerStore.GetRider(pid)
	require.NotNil(t, existing.HealthStream, "OnLive should have spawned a stream on discovery")
	originalStream := existing.HealthStream

	// Simulate a health-check populating some state
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

	// An update to an existing pooler must NOT re-fire OnLive — that would
	// install a fresh StreamHandle and replace the original one.
	require.NoError(t, poolerStore.Sync(ctx))
	assert.Same(t, originalStream, updated.HealthStream,
		"existing pooler should not re-trigger OnLive on metadata update")
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
	poolerStore := newTestPoolerCache(ctx, ts, targets, logger)
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
	poolerStore := newTestPoolerCache(ctx, ts, targets, logger)
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

// TestPoolerWatcher_PoolerDeletedFromTopology pins the NoNode contract:
// NoNode is presumed accidental. The watcher does NOT evict the cache and
// does NOT fire OnGone immediately. The entry remains visible to analyzers
// during the vanish grace window so accidental etcd deletes can self-heal
// if the pooler reappears. OnGone fires only at grace expiry (hours away in
// this configuration), so the rider's StreamHandle stays uncancelled.
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

	targets := []config.WatchTarget{{Database: "mydb"}}
	poolerStore := newTestPoolerCache(ctx, ts, targets, logger)
	defer poolerStore.Shutdown()

	require.True(t, waitForCondition(t, 5*time.Second, func() bool {
		return poolerStore.Len() == 1
	}))

	require.NoError(t, ts.UnregisterMultiPooler(ctx, poolerID))
	require.NoError(t, poolerStore.Sync(ctx))

	// Entry remains cached so analyzers see it during vanish grace.
	assert.Equal(t, 1, poolerStore.Len(), "NoNode must leave the entry visible during grace")
	rider, ok := poolerStore.GetRider(poolerKey("zone1", "pooler1"))
	assert.True(t, ok, "vanished pooler should still be cached during grace")
	// OnGone must not have fired: the StreamHandle installed by OnLive is
	// still attached to the rider.
	assert.NotNil(t, rider.HealthStream, "stream handle must persist while entry is in vanish grace")
}

// TestPoolerWatcher_PoolerEntersShutdownLifecycle pins the SHUTDOWN contract:
// from orch's perspective SHUTDOWN is dead. The watcher fires OnGone
// immediately AND evicts the store entry — no soft-delete intermediate state
// at the orch level. The cache layer below retains a ghost record for future
// etcd-cleanup, but that is invisible to orch's store. OnGone is responsible
// for cancelling the per-pooler StreamHandle.
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

	targets := []config.WatchTarget{{Database: "mydb"}}
	poolerStore := newTestPoolerCache(ctx, ts, targets, logger)
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
}

// TestPoolerWatcher_RestartAfterShutdownFiresOnLive verifies the re-entry
// path: a pooler that transitioned to SHUTDOWN and then comes back up
// (writes STARTING/ACTIVE via RegisterMultiPooler(allowUpdate=true))
// re-triggers OnLive so the orchestrator restarts its health stream. The
// cache entry is retained as a ghost across SHUTDOWN, so the watcher
// detects the SHUTDOWN→non-SHUTDOWN lifecycle transition and explicitly
// re-fires OnLive — which in turn spawns a fresh per-pooler stream handle.
func TestPoolerWatcher_RestartAfterShutdownFiresOnLive(t *testing.T) {
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

	targets := []config.WatchTarget{{Database: "mydb"}}
	poolerStore := newTestPoolerCache(ctx, ts, targets, logger)
	defer poolerStore.Shutdown()

	// Initial discovery installs a stream handle on the rider.
	require.True(t, waitForCondition(t, 5*time.Second, func() bool { return poolerStore.Len() == 1 }),
		"new pooler should be discovered")
	first, ok := poolerStore.GetRider(poolerKey("zone1", "pooler1"))
	require.True(t, ok)
	require.NotNil(t, first.HealthStream, "OnLive should have spawned a stream on initial discovery")
	originalStream := first.HealthStream

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
	// recognizes restart-from-ghost and re-fires OnLive, which spawns a
	// fresh StreamHandle (distinct from the one OnGone just cancelled).
	_, err = ts.UpdateMultiPoolerFields(ctx, poolerID, func(mp *clustermetadata.MultiPooler) error {
		mp.LifecycleStatus = &clustermetadata.PoolerLifecycle{
			Status: clustermetadata.PoolerLifecycleStatus_LIFECYCLE_ACTIVE,
		}
		return nil
	})
	require.NoError(t, err)

	require.True(t, waitForCondition(t, 5*time.Second, func() bool {
		p, ok := poolerStore.GetRider(poolerKey("zone1", "pooler1"))
		return ok && p.HealthStream != nil && p.HealthStream != originalStream
	}), "restart after SHUTDOWN must re-fire OnLive and install a fresh StreamHandle")
}

// TestPoolerWatcher_ColdStartShutdownIgnored verifies that an already-SHUTDOWN
// pooler discovered for the first time (e.g. orchestrator restart while the
// entry is still in topology) is tracked as a ghost in the cache but never
// becomes a live store entry. The ghost lets the watcher detect a future
// SHUTDOWN→non-SHUTDOWN transition and fire OnLive then, but no health stream
// is opened immediately — there's nothing live to monitor.
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

	targets := []config.WatchTarget{{Database: "mydb"}}
	poolerStore := newTestPoolerCache(ctx, ts, targets, logger)
	defer poolerStore.Shutdown()

	// Give the watcher time to process the initial SHUTDOWN entry; it should
	// reach a steady state with the store empty (cold-discovered SHUTDOWN
	// poolers are tracked as ghosts in the cache, not as store entries).
	require.NoError(t, poolerStore.Sync(ctx))

	assert.Equal(t, 0, poolerStore.Len(), "cold-discovered SHUTDOWN must not populate the orch store")
	// No rider for the SHUTDOWN entry, so no stream handle was spawned —
	// OnLive did not run for the cold-discovered SHUTDOWN.
	_, ok := poolerStore.GetRider(poolerKey("zone1", "pooler1"))
	assert.False(t, ok, "cold-discovered SHUTDOWN must not have a rider in the store")
}
