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

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// newTestPoolerWatcher creates a PoolerWatcher backed by memorytopo for testing.
func newTestPoolerWatcher(
	ctx context.Context,
	ts topoclient.Store,
	targets []config.WatchTarget,
	poolerStore *store.PoolerStore,
	queue *Queue,
	logger *slog.Logger,
) *PoolerWatcher {
	return NewPoolerWatcher(
		ctx,
		ts,
		func() []config.WatchTarget { return targets },
		poolerStore,
		queue,
		logger,
	)
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
		Id:         &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"},
		Database:   "mydb",
		TableGroup: "tg1",
		Shard:      "0",
		Type:       clustermetadata.PoolerType_PRIMARY,
		Hostname:   "host1",
	}))
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:         &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler2"},
		Database:   "mydb",
		TableGroup: "tg1",
		Shard:      "0",
		Type:       clustermetadata.PoolerType_REPLICA,
		Hostname:   "host2",
	}))

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	poolerStore := store.NewPoolerStore(nil, logger)
	cfg := config.NewTestConfig()
	queue := NewQueue(logger, cfg)

	targets := []config.WatchTarget{{Database: "mydb"}}
	watcher := newTestPoolerWatcher(ctx, ts, targets, poolerStore, queue, logger)
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

	// Both should be queued for health check
	assert.Equal(t, 2, queue.QueueLen())
}

func TestPoolerWatcher_NewPoolerAddedAfterStart(t *testing.T) {
	ctx := t.Context()

	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	poolerStore := store.NewPoolerStore(nil, logger)
	cfg := config.NewTestConfig()
	queue := NewQueue(logger, cfg)

	targets := []config.WatchTarget{{Database: "mydb"}}
	watcher := newTestPoolerWatcher(ctx, ts, targets, poolerStore, queue, logger)
	watcher.Start()
	defer watcher.Stop()

	// Sync to confirm watcher started and processed initial (empty) topology
	require.NoError(t, watcher.Sync(ctx))
	assert.Equal(t, 0, poolerStore.Len())

	// Add a pooler after the watcher has started
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:         &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"},
		Database:   "mydb",
		TableGroup: "tg1",
		Shard:      "0",
		Type:       clustermetadata.PoolerType_PRIMARY,
		Hostname:   "host1",
	}))

	ok := waitForCondition(t, 5*time.Second, func() bool {
		return poolerStore.Len() == 1
	})
	require.True(t, ok, "expected pooler to be discovered after add")

	p1, exists := poolerStore.Get(poolerKey("zone1", "pooler1"))
	require.True(t, exists)
	assert.Equal(t, "host1", p1.MultiPooler.Hostname)
	assert.Equal(t, 1, queue.QueueLen())
}

func TestPoolerWatcher_PoolerMetadataUpdate(t *testing.T) {
	ctx := t.Context()

	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:         &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"},
		Database:   "mydb",
		TableGroup: "tg1",
		Shard:      "0",
		Type:       clustermetadata.PoolerType_PRIMARY,
		Hostname:   "host1",
	}))

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	poolerStore := store.NewPoolerStore(nil, logger)
	cfg := config.NewTestConfig()
	queue := NewQueue(logger, cfg)

	targets := []config.WatchTarget{{Database: "mydb"}}
	watcher := newTestPoolerWatcher(ctx, ts, targets, poolerStore, queue, logger)
	watcher.Start()
	defer watcher.Stop()

	// Wait for initial discovery
	ok := waitForCondition(t, 5*time.Second, func() bool {
		return poolerStore.Len() == 1
	})
	require.True(t, ok)

	// Record queue length after initial discovery (the new pooler was queued once)
	queueLenAfterDiscovery := queue.QueueLen()
	assert.Equal(t, 1, queueLenAfterDiscovery, "new pooler should be queued once on discovery")

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

	// An update to an existing pooler should NOT add more entries to the queue
	assert.Equal(t, queueLenAfterDiscovery, queue.QueueLen(), "existing pooler should not be re-queued on metadata update")
}

func TestPoolerWatcher_WatchTargetFiltering(t *testing.T) {
	ctx := t.Context()

	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	// Add poolers in different databases/tablegroups
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:         &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "watched"},
		Database:   "mydb",
		TableGroup: "tg1",
		Shard:      "0",
	}))
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:         &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "other-db"},
		Database:   "otherdb",
		TableGroup: "tg1",
		Shard:      "0",
	}))
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:         &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "other-tg"},
		Database:   "mydb",
		TableGroup: "tg2",
		Shard:      "0",
	}))

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	poolerStore := store.NewPoolerStore(nil, logger)
	cfg := config.NewTestConfig()
	queue := NewQueue(logger, cfg)

	// Only watch mydb/tg1
	targets := []config.WatchTarget{{Database: "mydb", TableGroup: "tg1"}}
	watcher := newTestPoolerWatcher(ctx, ts, targets, poolerStore, queue, logger)
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
	cfg := config.NewTestConfig()
	queue := NewQueue(logger, cfg)

	targets := []config.WatchTarget{{Database: "mydb"}}
	watcher := newTestPoolerWatcher(ctx, ts, targets, poolerStore, queue, logger)
	watcher.Start()
	defer watcher.Stop()

	// Add a pooler in zone1
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:         &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"},
		Database:   "mydb",
		TableGroup: "tg1",
		Shard:      "0",
	}))

	ok := waitForCondition(t, 5*time.Second, func() bool {
		return poolerStore.Len() == 1
	})
	require.True(t, ok, "expected zone1 pooler to be discovered")

	// Add zone2 cell and a pooler in it
	require.NoError(t, factory.AddCell(ctx, ts, "zone2"))
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:         &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone2", Name: "pooler2"},
		Database:   "mydb",
		TableGroup: "tg1",
		Shard:      "0",
	}))

	ok = waitForCondition(t, 5*time.Second, func() bool {
		return poolerStore.Len() == 2
	})
	require.True(t, ok, "expected zone2 pooler to be discovered after new cell added")

	_, exists := poolerStore.Get(poolerKey("zone2", "pooler2"))
	assert.True(t, exists)
}

// TestPoolerWatcher_PoolerDeletedFromTopology verifies that deleting a pooler from topology
// does NOT immediately remove it from the store. Removal is deferred to bookkeeping so
// that health-check state is preserved across transient topology blips (e.g. rolling
// restarts). The store entry will be cleaned up by forgetLongUnseenInstances.
func TestPoolerWatcher_PoolerDeletedFromTopology(t *testing.T) {
	ctx := t.Context()

	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:         &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"},
		Database:   "mydb",
		TableGroup: "tg1",
		Shard:      "0",
	}))

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	poolerStore := store.NewPoolerStore(nil, logger)
	cfg := config.NewTestConfig()
	queue := NewQueue(logger, cfg)

	targets := []config.WatchTarget{{Database: "mydb"}}
	watcher := newTestPoolerWatcher(ctx, ts, targets, poolerStore, queue, logger)
	watcher.Start()
	defer watcher.Stop()

	// Wait for discovery
	ok := waitForCondition(t, 5*time.Second, func() bool {
		return poolerStore.Len() == 1
	})
	require.True(t, ok)

	// Delete the pooler from topology
	require.NoError(t, ts.UnregisterMultiPooler(ctx, &clustermetadata.ID{
		Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1",
	}))

	// The pooler should remain in the store after the deletion event is processed;
	// bookkeeping (forgetLongUnseenInstances) is responsible for eventual removal.
	require.NoError(t, watcher.Sync(ctx))
	assert.Equal(t, 1, poolerStore.Len(), "deleted pooler should remain in store until bookkeeping removes it")
}
