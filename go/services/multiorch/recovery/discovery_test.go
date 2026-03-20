// Copyright 2025 Supabase, Inc.
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
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	commontypes "github.com/multigres/multigres/go/common/types"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// poolerKey creates the store key for a pooler
func poolerKey(cell, name string) string {
	return topoclient.MultiPoolerIDString(&clustermetadata.ID{
		Component: clustermetadata.ID_MULTIPOOLER,
		Cell:      cell,
		Name:      name,
	})
}

// startWatcher starts a PoolerWatcher and registers Stop() as a cleanup function.
func startWatcher(t *testing.T, pw *PoolerWatcher) {
	t.Helper()
	pw.Start()
	t.Cleanup(pw.Stop)
}

// startEngine starts an Engine and registers Stop() as a cleanup function.
func startEngine(t *testing.T, engine *Engine) {
	t.Helper()
	require.NoError(t, engine.Start())
	t.Cleanup(engine.Stop)
}

func TestDiscovery_DatabaseLevelWatch(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	cfg := config.NewTestConfig(config.WithCell("zone1"))
	engine := NewEngine(
		ts,
		slog.Default(),
		cfg,
		[]config.WatchTarget{{Database: "mydb"}},
		&rpcclient.FakeClient{},
		newTestCoordinator(ts, &rpcclient.FakeClient{}, "zone1"),
	)
	startEngine(t, engine)

	poolerStoreIs := func(val int) func() bool {
		return func() bool { return engine.poolerStore.Len() == val }
	}

	// Initial state: 2 poolers in different tablegroups
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"},
		Database: "mydb", TableGroup: "tg1", Shard: "0",
	}))
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler2"},
		Database: "mydb", TableGroup: "tg2", Shard: "0",
	}))

	// First watch event - should discover both
	require.True(t, waitForCondition(t, 5*time.Second, poolerStoreIs(2)), "timed out waiting for 2 poolers")
	require.Equal(t, 2, engine.poolerStore.Len())

	_, ok := engine.poolerStore.Get(poolerKey("zone1", "pooler1"))
	require.True(t, ok, "pooler1 should be discovered")
	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler2"))
	require.True(t, ok, "pooler2 should be discovered")

	// Add new tablegroup with pooler
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler3"},
		Database: "mydb", TableGroup: "tg3", Shard: "0",
	}))

	// Second watch event - should discover new tablegroup's pooler
	require.True(t, waitForCondition(t, 5*time.Second, poolerStoreIs(3)), "timed out waiting for 3 poolers")
	require.Equal(t, 3, engine.poolerStore.Len())

	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler3"))
	require.True(t, ok, "pooler3 in new tablegroup should be discovered")

	// Add new shard in existing tablegroup
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler4"},
		Database: "mydb", TableGroup: "tg1", Shard: "1",
	}))

	// Third watch event - should discover new shard's pooler
	require.True(t, waitForCondition(t, 5*time.Second, poolerStoreIs(4)), "timed out waiting for 4 poolers")
	require.Equal(t, 4, engine.poolerStore.Len())

	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler4"))
	require.True(t, ok, "pooler4 in new shard should be discovered")

	// Remove a pooler from topology
	require.NoError(t, ts.UnregisterMultiPooler(ctx, &clustermetadata.ID{
		Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler2",
	}))

	// Fourth watch event - removed pooler still in store (bookkeeping removes it later)
	require.NoError(t, engine.poolerWatcher.Sync(ctx))
	require.Equal(t, 4, engine.poolerStore.Len(), "store should still contain all poolers, bookkeeping handles removal")
}

func TestDiscovery_TablegroupLevelWatch(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	cfg := config.NewTestConfig(config.WithCell("zone1"))
	engine := NewEngine(
		ts,
		slog.Default(),
		cfg,
		[]config.WatchTarget{{Database: "mydb", TableGroup: "tg1"}},
		&rpcclient.FakeClient{},
		newTestCoordinator(ts, &rpcclient.FakeClient{}, "zone1"),
	)
	startEngine(t, engine)

	poolerStoreIs := func(val int) func() bool {
		return func() bool { return engine.poolerStore.Len() == val }
	}

	poolerStoreAtLeast := func(val int) func() bool {
		return func() bool { return engine.poolerStore.Len() >= val }
	}

	// Initial state: poolers in tg1 and tg2
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"},
		Database: "mydb", TableGroup: "tg1", Shard: "0",
	}))
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler2"},
		Database: "mydb", TableGroup: "tg2", Shard: "0",
	}))

	// First watch event - should only discover tg1
	require.True(t, waitForCondition(t, 5*time.Second, poolerStoreAtLeast(1)), "timed out waiting for first pooler")
	require.NoError(t, engine.poolerWatcher.Sync(ctx))
	require.Equal(t, 1, engine.poolerStore.Len())

	_, ok := engine.poolerStore.Get(poolerKey("zone1", "pooler1"))
	require.True(t, ok, "pooler1 in tg1 should be discovered")
	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler2"))
	require.False(t, ok, "pooler2 in tg2 should NOT be discovered")

	// Add new shard in tg1
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler3"},
		Database: "mydb", TableGroup: "tg1", Shard: "1",
	}))

	// Second watch event - should discover new shard in tg1
	require.True(t, waitForCondition(t, 5*time.Second, poolerStoreIs(2)), "timed out waiting for 2 poolers")
	require.Equal(t, 2, engine.poolerStore.Len())

	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler3"))
	require.True(t, ok, "pooler3 in new shard of tg1 should be discovered")

	// Add new tablegroup tg3 with pooler
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler4"},
		Database: "mydb", TableGroup: "tg3", Shard: "0",
	}))

	// Third watch event - should NOT discover new tablegroup
	require.NoError(t, engine.poolerWatcher.Sync(ctx))
	require.Equal(t, 2, engine.poolerStore.Len())

	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler4"))
	require.False(t, ok, "pooler4 in tg3 should NOT be discovered (only watching tg1)")
}

func TestDiscovery_ShardLevelWatch(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	cfg := config.NewTestConfig(config.WithCell("zone1"))
	engine := NewEngine(
		ts,
		slog.Default(),
		cfg,
		[]config.WatchTarget{{Database: "mydb", TableGroup: "tg1", Shard: "0"}},
		&rpcclient.FakeClient{},
		newTestCoordinator(ts, &rpcclient.FakeClient{}, "zone1"),
	)
	startEngine(t, engine)

	poolerStoreIs := func(val int) func() bool {
		return func() bool { return engine.poolerStore.Len() == val }
	}

	poolerStoreAtLeast := func(val int) func() bool {
		return func() bool { return engine.poolerStore.Len() >= val }
	}

	// Initial state: poolers in different shards and tablegroups
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"},
		Database: "mydb", TableGroup: "tg1", Shard: "0",
	}))
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler2"},
		Database: "mydb", TableGroup: "tg1", Shard: "1",
	}))
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler3"},
		Database: "mydb", TableGroup: "tg2", Shard: "0",
	}))

	// First watch event - should only discover tg1/shard0
	require.True(t, waitForCondition(t, 5*time.Second, poolerStoreAtLeast(1)), "timed out waiting for first pooler")
	require.NoError(t, engine.poolerWatcher.Sync(ctx))
	require.Equal(t, 1, engine.poolerStore.Len())

	_, ok := engine.poolerStore.Get(poolerKey("zone1", "pooler1"))
	require.True(t, ok, "pooler1 in tg1/shard0 should be discovered")
	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler2"))
	require.False(t, ok, "pooler2 in tg1/shard1 should NOT be discovered")
	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler3"))
	require.False(t, ok, "pooler3 in tg2/shard0 should NOT be discovered")

	// Add another pooler to the watched shard
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler4"},
		Database: "mydb", TableGroup: "tg1", Shard: "0",
	}))

	// Second watch event - should discover new pooler in same shard
	require.True(t, waitForCondition(t, 5*time.Second, poolerStoreIs(2)), "timed out waiting for 2 poolers")
	require.Equal(t, 2, engine.poolerStore.Len())

	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler4"))
	require.True(t, ok, "pooler4 in tg1/shard0 should be discovered")

	// Add new shard in same tablegroup
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler5"},
		Database: "mydb", TableGroup: "tg1", Shard: "2",
	}))

	// Third watch event - should NOT discover new shard
	require.NoError(t, engine.poolerWatcher.Sync(ctx))
	require.Equal(t, 2, engine.poolerStore.Len())

	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler5"))
	require.False(t, ok, "pooler5 in tg1/shard2 should NOT be discovered (only watching shard0)")

	// Add new tablegroup
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler6"},
		Database: "mydb", TableGroup: "tg3", Shard: "0",
	}))

	// Fourth watch event - should NOT discover new tablegroup
	require.NoError(t, engine.poolerWatcher.Sync(ctx))
	require.Equal(t, 2, engine.poolerStore.Len())

	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler6"))
	require.False(t, ok, "pooler6 in tg3 should NOT be discovered (only watching tg1/shard0)")
}

func TestDiscovery_PreservesTimestamps(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	cfg := config.NewTestConfig(config.WithCell("zone1"))
	engine := NewEngine(
		ts,
		slog.Default(),
		cfg,
		[]config.WatchTarget{{Database: "mydb"}},
		&rpcclient.FakeClient{},
		newTestCoordinator(ts, &rpcclient.FakeClient{}, "zone1"),
	)
	startEngine(t, engine)

	poolerStoreDiscovered := func(val int) func() bool {
		return func() bool {
			_, ok := engine.poolerStore.Get(poolerKey("zone1", "pooler1"))
			return ok
		}
	}

	// Add initial pooler
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"},
		Database: "mydb", TableGroup: "tg1", Shard: "0",
		Hostname: "host1",
	}))

	// First watch event - discover pooler
	require.True(t, waitForCondition(t, 5*time.Second, poolerStoreDiscovered(1)), "expected pooler1 to be discovered")

	poolerInfo, ok := engine.poolerStore.Get(poolerKey("zone1", "pooler1"))
	require.True(t, ok)
	require.Equal(t, "host1", poolerInfo.MultiPooler.Hostname)
	require.Nil(t, poolerInfo.LastSeen, "LastSeen should be nil (not yet health checked successfully)")
	// Note: IsUpToDate may already be true here - health workers run concurrently and set it
	// to true even on a failed check (FakeClient returns an error but IsUpToDate is still set).

	// Simulate health check by updating timestamps
	now := timestamppb.Now()
	poolerInfo.LastSeen = now
	poolerInfo.LastCheckAttempted = now
	poolerInfo.LastCheckSuccessful = now
	poolerInfo.IsUpToDate = true
	poolerInfo.IsLastCheckValid = true
	engine.poolerStore.Set(poolerKey("zone1", "pooler1"), poolerInfo)

	// Update topology record (hostname changed)
	retrieved, err := ts.GetMultiPooler(ctx, &clustermetadata.ID{
		Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1",
	})
	require.NoError(t, err)
	retrieved.MultiPooler.Hostname = "host2"
	require.NoError(t, ts.UpdateMultiPooler(ctx, retrieved))

	// Watch event - should update MultiPooler but preserve timestamps
	require.True(t, waitForCondition(t, 5*time.Second, func() bool {
		info, ok := engine.poolerStore.Get(poolerKey("zone1", "pooler1"))
		return ok && info.MultiPooler.Hostname == "host2"
	}), "expected hostname to be updated to host2")

	updatedInfo, ok := engine.poolerStore.Get(poolerKey("zone1", "pooler1"))
	require.True(t, ok)

	// MultiPooler record should be updated
	require.Equal(t, "host2", updatedInfo.MultiPooler.Hostname, "hostname should be updated")

	// Timestamps and computed fields should be preserved (exact equality)
	require.True(t, now.AsTime().Equal(updatedInfo.LastSeen.AsTime()), "LastSeen should be preserved")
	require.True(t, now.AsTime().Equal(updatedInfo.LastCheckAttempted.AsTime()), "LastCheckAttempted should be preserved")
	require.True(t, now.AsTime().Equal(updatedInfo.LastCheckSuccessful.AsTime()), "LastCheckSuccessful should be preserved")
	require.True(t, updatedInfo.IsUpToDate, "IsUpToDate should be preserved")
	require.True(t, updatedInfo.IsLastCheckValid, "IsLastCheckValid should be preserved")
}

func TestDiscovery_MultipleWatchTargets(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	cfg := config.NewTestConfig(config.WithCell("zone1"))
	engine := NewEngine(
		ts,
		slog.Default(),
		cfg,
		[]config.WatchTarget{
			{Database: "db1"},                                // Watch entire database
			{Database: "db2", TableGroup: "tg1"},             // Watch specific tablegroup
			{Database: "db3", TableGroup: "tg1", Shard: "0"}, // Watch specific shard
		},
		&rpcclient.FakeClient{},
		newTestCoordinator(ts, &rpcclient.FakeClient{}, "zone1"),
	)
	startEngine(t, engine)

	poolerStoreIs := func(val int) func() bool {
		return func() bool { return engine.poolerStore.Len() == val }
	}

	// Add poolers for different watch targets
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"},
		Database: "db1", TableGroup: "tg1", Shard: "0",
	}))
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler2"},
		Database: "db1", TableGroup: "tg2", Shard: "1", // Should be discovered (db1 watch)
	}))
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler3"},
		Database: "db2", TableGroup: "tg1", Shard: "0", // Should be discovered (db2/tg1 watch)
	}))
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler4"},
		Database: "db2", TableGroup: "tg2", Shard: "0", // Should NOT be discovered
	}))
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler5"},
		Database: "db3", TableGroup: "tg1", Shard: "0", // Should be discovered (db3/tg1/0 watch)
	}))
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler6"},
		Database: "db3", TableGroup: "tg1", Shard: "1", // Should NOT be discovered
	}))

	// Wait for expected 4 poolers: pooler1, pooler2, pooler3, pooler5
	require.True(t, waitForCondition(t, 5*time.Second, poolerStoreIs(4)), "expected 4 poolers in store after all poolers written")

	// Sync to ensure all events (including filtered ones) have been processed
	require.NoError(t, engine.poolerWatcher.Sync(ctx))
	assert.Equal(t, 4, engine.poolerStore.Len(), "should discover exactly 4 poolers")

	_, ok := engine.poolerStore.Get(poolerKey("zone1", "pooler1"))
	require.True(t, ok)
	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler2"))
	require.True(t, ok)
	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler3"))
	require.True(t, ok)
	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler4"))
	require.False(t, ok, "pooler4 should NOT be discovered (wrong tablegroup)")
	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler5"))
	require.True(t, ok)
	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler6"))
	require.False(t, ok, "pooler6 should NOT be discovered (wrong shard)")
}

func TestDiscovery_EmptyTopology(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	cfg := config.NewTestConfig(config.WithCell("zone1"))
	engine := NewEngine(
		ts,
		slog.Default(),
		cfg,
		[]config.WatchTarget{{Database: "mydb"}},
		&rpcclient.FakeClient{},
		newTestCoordinator(ts, &rpcclient.FakeClient{}, "zone1"),
	)
	startEngine(t, engine)

	poolerStoreIs := func(val int) func() bool {
		return func() bool { return engine.poolerStore.Len() == val }
	}

	// Sync to confirm watcher started and processed initial (empty) topology
	require.NoError(t, engine.poolerWatcher.Sync(ctx))
	assert.Equal(t, 0, engine.poolerStore.Len(), "store should be empty with empty topology")

	// Add a pooler
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"},
		Database: "mydb", TableGroup: "tg1", Shard: "0",
	}))

	require.True(t, waitForCondition(t, 5*time.Second, poolerStoreIs(1)), "expected 1 pooler after adding to topology")
}

// TestPoolerWatcher_DirectDiscovery tests PoolerWatcher in isolation, without going
// through Engine. This verifies the watcher's own store-update and filtering logic
// independently of the rest of the Engine.
func TestPoolerWatcher_DirectDiscovery(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	cfg := config.NewTestConfig(config.WithCell("zone1"))
	poolerStore := store.NewPoolerStore(nil, slog.Default())
	queue := NewQueue(slog.Default(), cfg)
	watchTargets := []config.WatchTarget{{Database: "mydb", TableGroup: "tg1"}}
	poolerWatcher := NewPoolerWatcher(ctx, ts, func() []config.WatchTarget { return watchTargets }, poolerStore, queue, slog.Default())
	startWatcher(t, poolerWatcher)

	poolerStoreAtLeast := func(val int) func() bool {
		return func() bool { return poolerStore.Len() >= val }
	}

	poolerStoreIs := func(val int) func() bool {
		return func() bool { return poolerStore.Len() == val }
	}

	// Add a matching pooler and a non-matching pooler simultaneously
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"},
		Database: "mydb", TableGroup: "tg1", Shard: "0",
	}))
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler2"},
		Database: "mydb", TableGroup: "tg2", Shard: "0", // filtered out
	}))

	require.True(t, waitForCondition(t, 5*time.Second, poolerStoreAtLeast(1)), "expected at least 1 pooler in store")
	require.NoError(t, poolerWatcher.Sync(ctx))
	assert.Equal(t, 1, poolerStore.Len(), "only tg1 pooler should be in the watcher's store")

	_, ok := poolerStore.Get(poolerKey("zone1", "pooler1"))
	require.True(t, ok, "pooler1 in tg1 should be discovered")
	_, ok = poolerStore.Get(poolerKey("zone1", "pooler2"))
	require.False(t, ok, "pooler2 in tg2 should be filtered out by watcher")

	// Verify a new pooler discovered via watcher is queued for immediate health check
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler3"},
		Database: "mydb", TableGroup: "tg1", Shard: "1",
	}))

	require.True(t, waitForCondition(t, 5*time.Second, poolerStoreIs(2)), "expected pooler3 to be discovered")
	require.True(t, waitForCondition(t, 5*time.Second, func() bool { return queue.QueueLen() > 0 }), "new pooler should be queued for health check")
}

// TestForceHealthCheckShardPoolers_ForcesPolls tests that forceHealthCheckShardPoolers
// forces re-polls for all poolers in the specified shard.
func TestForceHealthCheckShardPoolers_ForcesPolls(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := config.NewTestConfig(config.WithCell("cell1"))

	// Create in-memory topology
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")

	engine := NewEngine(
		ts,
		logger,
		cfg,
		[]config.WatchTarget{{Database: "db1", TableGroup: "tg1"}},
		&rpcclient.FakeClient{},
		newTestCoordinator(ts, &rpcclient.FakeClient{}, "zone1"),
	)

	// Add poolers to the store (simulating already discovered poolers)
	mp1 := &clustermetadata.MultiPooler{
		Id: &clustermetadata.ID{
			Component: clustermetadata.ID_MULTIPOOLER,
			Cell:      "cell1",
			Name:      "pooler1",
		},
		Database:   "db1",
		TableGroup: "tg1",
		Shard:      "0",
		Type:       clustermetadata.PoolerType_PRIMARY,
		Hostname:   "host1",
	}
	existingHealth := &multiorchdatapb.PoolerHealthState{
		MultiPooler: mp1,
		IsUpToDate:  false,
	}
	engine.poolerStore.Set(poolerKey("cell1", "pooler1"), existingHealth)

	mp2 := &clustermetadata.MultiPooler{
		Id: &clustermetadata.ID{
			Component: clustermetadata.ID_MULTIPOOLER,
			Cell:      "cell1",
			Name:      "pooler2",
		},
		Database:   "db1",
		TableGroup: "tg1",
		Shard:      "0",
		Type:       clustermetadata.PoolerType_REPLICA,
		Hostname:   "host2",
	}
	existingHealth = &multiorchdatapb.PoolerHealthState{
		MultiPooler: mp2,
		IsUpToDate:  false,
	}
	engine.poolerStore.Set(poolerKey("cell1", "pooler2"), existingHealth)

	// Add a pooler in a different shard (should be ignored)
	mp3 := &clustermetadata.MultiPooler{
		Id: &clustermetadata.ID{
			Component: clustermetadata.ID_MULTIPOOLER,
			Cell:      "cell1",
			Name:      "pooler3",
		},
		Database:   "db1",
		TableGroup: "tg1",
		Shard:      "1",
		Type:       clustermetadata.PoolerType_PRIMARY,
		Hostname:   "host3",
	}
	existingHealth = &multiorchdatapb.PoolerHealthState{
		MultiPooler: mp3,
		IsUpToDate:  false,
	}
	engine.poolerStore.Set(poolerKey("cell1", "pooler3"), existingHealth)

	// Force health check for shard 0
	engine.forceHealthCheckShardPoolers(ctx, commontypes.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "0"}, nil)

	// Verify all shard 0 poolers had their LastCheckAttempted updated
	p1, ok := engine.poolerStore.Get(poolerKey("cell1", "pooler1"))
	require.True(t, ok)
	assert.NotNil(t, p1.LastCheckAttempted, "pooler1 should have been polled")

	p2, ok := engine.poolerStore.Get(poolerKey("cell1", "pooler2"))
	require.True(t, ok)
	assert.NotNil(t, p2.LastCheckAttempted, "pooler2 should have been polled")

	// Verify pooler in different shard was NOT polled
	p3, ok := engine.poolerStore.Get(poolerKey("cell1", "pooler3"))
	require.True(t, ok)
	assert.Nil(t, p3.LastCheckAttempted, "pooler3 (different shard) should not have been polled")
}

// TestForceHealthCheckShardPoolers_RespectsIgnoreList tests that
// forceHealthCheckShardPoolers respects the poolersToIgnore list.
func TestForceHealthCheckShardPoolers_RespectsIgnoreList(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := config.NewTestConfig(config.WithCell("cell1"))

	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")

	engine := NewEngine(
		ts,
		logger,
		cfg,
		[]config.WatchTarget{{Database: "db1", TableGroup: "tg1"}},
		&rpcclient.FakeClient{},
		newTestCoordinator(ts, &rpcclient.FakeClient{}, "zone1"),
	)

	// Add poolers to the store
	mp1 := &clustermetadata.MultiPooler{
		Id: &clustermetadata.ID{
			Component: clustermetadata.ID_MULTIPOOLER,
			Cell:      "cell1",
			Name:      "dead-primary",
		},
		Database:   "db1",
		TableGroup: "tg1",
		Shard:      "0",
		Type:       clustermetadata.PoolerType_PRIMARY,
		Hostname:   "host1",
	}
	existingHealth := &multiorchdatapb.PoolerHealthState{
		MultiPooler: mp1,
	}
	engine.poolerStore.Set(poolerKey("cell1", "dead-primary"), existingHealth)

	mp2 := &clustermetadata.MultiPooler{
		Id: &clustermetadata.ID{
			Component: clustermetadata.ID_MULTIPOOLER,
			Cell:      "cell1",
			Name:      "healthy-replica",
		},
		Database:   "db1",
		TableGroup: "tg1",
		Shard:      "0",
		Type:       clustermetadata.PoolerType_REPLICA,
		Hostname:   "host2",
	}
	existingHealth = &multiorchdatapb.PoolerHealthState{
		MultiPooler: mp2,
	}
	engine.poolerStore.Set(poolerKey("cell1", "healthy-replica"), existingHealth)

	// Force health check, but ignore the dead primary
	poolersToIgnore := []string{poolerKey("cell1", "dead-primary")}
	engine.forceHealthCheckShardPoolers(ctx, commontypes.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "0"}, poolersToIgnore)

	// Verify only the healthy replica was polled
	pDead, ok := engine.poolerStore.Get(poolerKey("cell1", "dead-primary"))
	require.True(t, ok)
	assert.Nil(t, pDead.LastCheckAttempted, "dead primary should not have been polled")

	pHealthy, ok := engine.poolerStore.Get(poolerKey("cell1", "healthy-replica"))
	require.True(t, ok)
	assert.NotNil(t, pHealthy.LastCheckAttempted, "healthy replica should have been polled")
}
