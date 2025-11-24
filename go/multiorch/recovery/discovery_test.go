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

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/clustermetadata/topo/memorytopo"
	"github.com/multigres/multigres/go/multiorch/config"
	"github.com/multigres/multigres/go/multiorch/store"
	"github.com/multigres/multigres/go/multipooler/rpcclient"
	"github.com/multigres/multigres/go/pb/clustermetadata"
)

// poolerKey creates the store key for a pooler
func poolerKey(cell, name string) string {
	return topo.MultiPoolerIDString(&clustermetadata.ID{
		Component: clustermetadata.ID_MULTIPOOLER,
		Cell:      cell,
		Name:      name,
	})
}

func TestDiscovery_DatabaseLevelWatch(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	cfg := config.NewTestConfig(
		config.WithCell("zone1"),
		config.WithClusterMetadataRefreshTimeout(5*time.Second),
	)

	engine := NewEngine(
		ts,
		slog.Default(),
		cfg,
		[]config.WatchTarget{{Database: "mydb"}},
		&rpcclient.FakeClient{},
	)

	// Initial state: 2 poolers in different tablegroups
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"},
		Database: "mydb", TableGroup: "tg1", Shard: "0",
	}))
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler2"},
		Database: "mydb", TableGroup: "tg2", Shard: "0",
	}))

	// First refresh - should discover both
	engine.refreshClusterMetadata()
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

	// Second refresh - should discover new tablegroup's pooler
	engine.refreshClusterMetadata()
	require.Equal(t, 3, engine.poolerStore.Len())

	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler3"))
	require.True(t, ok, "pooler3 in new tablegroup should be discovered")

	// Add new shard in existing tablegroup
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler4"},
		Database: "mydb", TableGroup: "tg1", Shard: "1",
	}))

	// Third refresh - should discover new shard's pooler
	engine.refreshClusterMetadata()
	require.Equal(t, 4, engine.poolerStore.Len())

	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler4"))
	require.True(t, ok, "pooler4 in new shard should be discovered")

	// Remove a pooler from topology
	require.NoError(t, ts.UnregisterMultiPooler(ctx, &clustermetadata.ID{
		Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler2",
	}))

	// Fourth refresh - removed pooler still in store (bookkeeping removes it later)
	engine.refreshClusterMetadata()
	require.Equal(t, 4, engine.poolerStore.Len(), "store should still contain all poolers, bookkeeping handles removal")
}

func TestDiscovery_TablegroupLevelWatch(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	cfg := config.NewTestConfig(
		config.WithCell("zone1"),
		config.WithClusterMetadataRefreshTimeout(5*time.Second),
	)

	engine := NewEngine(
		ts,
		slog.Default(),
		cfg,
		[]config.WatchTarget{{Database: "mydb", TableGroup: "tg1"}},
		&rpcclient.FakeClient{},
	)

	// Initial state: poolers in tg1 and tg2
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"},
		Database: "mydb", TableGroup: "tg1", Shard: "0",
	}))
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler2"},
		Database: "mydb", TableGroup: "tg2", Shard: "0",
	}))

	// First refresh - should only discover tg1
	engine.refreshClusterMetadata()
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

	// Second refresh - should discover new shard in tg1
	engine.refreshClusterMetadata()
	require.Equal(t, 2, engine.poolerStore.Len())

	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler3"))
	require.True(t, ok, "pooler3 in new shard of tg1 should be discovered")

	// Add new tablegroup tg3 with pooler
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler4"},
		Database: "mydb", TableGroup: "tg3", Shard: "0",
	}))

	// Third refresh - should NOT discover new tablegroup
	engine.refreshClusterMetadata()
	require.Equal(t, 2, engine.poolerStore.Len())

	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler4"))
	require.False(t, ok, "pooler4 in tg3 should NOT be discovered (only watching tg1)")
}

func TestDiscovery_ShardLevelWatch(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	cfg := config.NewTestConfig(
		config.WithCell("zone1"),
		config.WithClusterMetadataRefreshTimeout(5*time.Second),
	)

	engine := NewEngine(
		ts,
		slog.Default(),
		cfg,
		[]config.WatchTarget{{Database: "mydb", TableGroup: "tg1", Shard: "0"}},
		&rpcclient.FakeClient{},
	)

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

	// First refresh - should only discover tg1/shard0
	engine.refreshClusterMetadata()
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

	// Second refresh - should discover new pooler in same shard
	engine.refreshClusterMetadata()
	require.Equal(t, 2, engine.poolerStore.Len())

	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler4"))
	require.True(t, ok, "pooler4 in tg1/shard0 should be discovered")

	// Add new shard in same tablegroup
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler5"},
		Database: "mydb", TableGroup: "tg1", Shard: "2",
	}))

	// Third refresh - should NOT discover new shard
	engine.refreshClusterMetadata()
	require.Equal(t, 2, engine.poolerStore.Len())

	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler5"))
	require.False(t, ok, "pooler5 in tg1/shard2 should NOT be discovered (only watching shard0)")

	// Add new tablegroup
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler6"},
		Database: "mydb", TableGroup: "tg3", Shard: "0",
	}))

	// Fourth refresh - should NOT discover new tablegroup
	engine.refreshClusterMetadata()
	require.Equal(t, 2, engine.poolerStore.Len())

	_, ok = engine.poolerStore.Get(poolerKey("zone1", "pooler6"))
	require.False(t, ok, "pooler6 in tg3 should NOT be discovered (only watching tg1/shard0)")
}

func TestDiscovery_PreservesTimestamps(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	cfg := config.NewTestConfig(
		config.WithCell("zone1"),
		config.WithClusterMetadataRefreshTimeout(5*time.Second),
	)

	engine := NewEngine(
		ts,
		slog.Default(),
		cfg,
		[]config.WatchTarget{{Database: "mydb"}},
		&rpcclient.FakeClient{},
	)

	// Add initial pooler
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"},
		Database: "mydb", TableGroup: "tg1", Shard: "0",
		Hostname: "host1",
	}))

	// First refresh - discover pooler
	engine.refreshClusterMetadata()
	require.Equal(t, 1, engine.poolerStore.Len())

	poolerInfo, ok := engine.poolerStore.Get(poolerKey("zone1", "pooler1"))
	require.True(t, ok)
	require.Equal(t, "host1", poolerInfo.Hostname)
	require.True(t, poolerInfo.LastSeen.IsZero(), "LastSeen should be zero (not yet health checked)")
	require.False(t, poolerInfo.IsUpToDate, "IsUpToDate should be false")

	// Simulate health check by updating timestamps
	now := time.Now()
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

	// Second refresh - should update MultiPooler but preserve timestamps
	engine.refreshClusterMetadata()
	require.Equal(t, 1, engine.poolerStore.Len())

	updatedInfo, ok := engine.poolerStore.Get(poolerKey("zone1", "pooler1"))
	require.True(t, ok)

	// MultiPooler record should be updated
	require.Equal(t, "host2", updatedInfo.Hostname, "hostname should be updated")

	// Timestamps and computed fields should be preserved
	require.Equal(t, now, updatedInfo.LastSeen, "LastSeen should be preserved")
	require.Equal(t, now, updatedInfo.LastCheckAttempted, "LastCheckAttempted should be preserved")
	require.Equal(t, now, updatedInfo.LastCheckSuccessful, "LastCheckSuccessful should be preserved")
	require.True(t, updatedInfo.IsUpToDate, "IsUpToDate should be preserved")
	require.True(t, updatedInfo.IsLastCheckValid, "IsLastCheckValid should be preserved")
}

func TestDiscovery_MultipleWatchTargets(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer ts.Close()

	cfg := config.NewTestConfig(
		config.WithCell("zone1"),
		config.WithClusterMetadataRefreshTimeout(5*time.Second),
	)

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
	)

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

	engine.refreshClusterMetadata()

	// Should discover: pooler1, pooler2, pooler3, pooler5 (4 total)
	require.Equal(t, 4, engine.poolerStore.Len())

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

	cfg := config.NewTestConfig(
		config.WithCell("zone1"),
		config.WithClusterMetadataRefreshTimeout(5*time.Second),
	)

	engine := NewEngine(
		ts,
		slog.Default(),
		cfg,
		[]config.WatchTarget{{Database: "mydb"}},
		&rpcclient.FakeClient{},
	)

	// Refresh with empty topology
	engine.refreshClusterMetadata()
	require.Equal(t, 0, engine.poolerStore.Len())

	// Add some poolers
	require.NoError(t, ts.CreateMultiPooler(ctx, &clustermetadata.MultiPooler{
		Id:       &clustermetadata.ID{Component: clustermetadata.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"},
		Database: "mydb", TableGroup: "tg1", Shard: "0",
	}))
	engine.refreshClusterMetadata()
	require.Equal(t, 1, engine.poolerStore.Len())
}

// TestRefreshPoolersForTarget_BasicRefresh tests that refreshPoolersForTarget
// correctly discovers and adds poolers to the store.
func TestRefreshPoolersForTarget_BasicRefresh(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := config.NewTestConfig(config.WithCell("cell1"))

	// Create in-memory topology
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")

	// Add a multipooler to topology
	mp := &clustermetadata.MultiPooler{
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
	err := ts.CreateMultiPooler(ctx, mp)
	require.NoError(t, err)

	engine := NewEngine(
		ts,
		logger,
		cfg,
		[]config.WatchTarget{{Database: "db1", TableGroup: "tg1", Shard: "0"}},
		&rpcclient.FakeClient{},
	)

	// Refresh poolers for the target
	count, err := engine.refreshPoolersForTarget(ctx, "db1", "tg1", "0", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "should discover 1 pooler")

	// Verify pooler was added to store
	poolerID := poolerKey("cell1", "pooler1")
	ph, ok := engine.poolerStore.Get(poolerID)
	require.True(t, ok, "pooler should be in store")
	assert.Equal(t, "db1", ph.Database)
	assert.Equal(t, "tg1", ph.TableGroup)
	assert.Equal(t, "0", ph.Shard)
	assert.False(t, ph.IsUpToDate, "new pooler should not be up-to-date yet")

	// Verify pooler was queued for health check
	assert.Equal(t, 1, engine.healthCheckQueue.QueueLen(), "pooler should be queued for health check")
}

// TestRefreshPoolersForTarget_PreservesHealthCheckData tests that refreshing
// existing poolers preserves their health check data.
func TestRefreshPoolersForTarget_PreservesHealthCheckData(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := config.NewTestConfig(config.WithCell("cell1"))

	// Create in-memory topology
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")

	// Add a multipooler to topology
	mp := &clustermetadata.MultiPooler{
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
	err := ts.CreateMultiPooler(ctx, mp)
	require.NoError(t, err)

	engine := NewEngine(
		ts,
		logger,
		cfg,
		[]config.WatchTarget{{Database: "db1", TableGroup: "tg1", Shard: "0"}},
		&rpcclient.FakeClient{},
	)

	// Pre-populate store with existing health check data
	poolerID := poolerKey("cell1", "pooler1")
	lastCheck := time.Now().Add(-5 * time.Minute)
	lastSuccess := time.Now().Add(-6 * time.Minute)
	lastSeen := time.Now().Add(-7 * time.Minute)

	existingHealth := store.NewPoolerHealthFromMultiPooler(mp)
	existingHealth.LastCheckAttempted = lastCheck
	existingHealth.LastCheckSuccessful = lastSuccess
	existingHealth.LastSeen = lastSeen
	existingHealth.IsUpToDate = true
	existingHealth.IsLastCheckValid = true
	engine.poolerStore.Set(poolerID, existingHealth)

	// Refresh poolers (should update MultiPooler but preserve timestamps)
	count, err := engine.refreshPoolersForTarget(ctx, "db1", "tg1", "0", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Verify health check data was preserved
	ph, ok := engine.poolerStore.Get(poolerID)
	require.True(t, ok)
	assert.Equal(t, lastCheck, ph.LastCheckAttempted, "should preserve LastCheckAttempted")
	assert.Equal(t, lastSuccess, ph.LastCheckSuccessful, "should preserve LastCheckSuccessful")
	assert.Equal(t, lastSeen, ph.LastSeen, "should preserve LastSeen")
	assert.True(t, ph.IsUpToDate, "should preserve IsUpToDate")
	assert.True(t, ph.IsLastCheckValid, "should preserve IsLastCheckValid")

	// Should not re-queue existing pooler
	assert.Equal(t, 0, engine.healthCheckQueue.QueueLen(), "should not re-queue existing pooler")
}

// TestRefreshPoolersForTarget_IgnoresPoolers tests that poolers in the ignore
// list are skipped during refresh.
func TestRefreshPoolersForTarget_IgnoresPoolers(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := config.NewTestConfig(config.WithCell("cell1"))

	// Create in-memory topology
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")

	// Add two multipoolers to topology
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
	err := ts.CreateMultiPooler(ctx, mp1)
	require.NoError(t, err)

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
	err = ts.CreateMultiPooler(ctx, mp2)
	require.NoError(t, err)

	engine := NewEngine(
		ts,
		logger,
		cfg,
		[]config.WatchTarget{{Database: "db1", TableGroup: "tg1", Shard: "0"}},
		&rpcclient.FakeClient{},
	)

	// Refresh poolers, ignoring pooler1
	poolersToIgnore := []string{poolerKey("cell1", "pooler1")}
	count, err := engine.refreshPoolersForTarget(ctx, "db1", "tg1", "0", poolersToIgnore)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "should discover only 1 pooler (pooler2)")

	// Verify only pooler2 was added
	_, ok := engine.poolerStore.Get(poolerKey("cell1", "pooler1"))
	assert.False(t, ok, "pooler1 should be ignored")

	ph2, ok := engine.poolerStore.Get(poolerKey("cell1", "pooler2"))
	require.True(t, ok, "pooler2 should be in store")
	assert.Equal(t, "pooler2", ph2.ID.Name)
}

// TestRefreshPoolersForTarget_FiltersToShard tests that refreshPoolersForTarget
// only returns poolers matching the specified shard.
func TestRefreshPoolersForTarget_FiltersToShard(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := config.NewTestConfig(config.WithCell("cell1"))

	// Create in-memory topology
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")

	// Add poolers in different shards
	mp1 := &clustermetadata.MultiPooler{
		Id: &clustermetadata.ID{
			Component: clustermetadata.ID_MULTIPOOLER,
			Cell:      "cell1",
			Name:      "pooler-shard0",
		},
		Database:   "db1",
		TableGroup: "tg1",
		Shard:      "0",
		Type:       clustermetadata.PoolerType_PRIMARY,
		Hostname:   "host1",
	}
	err := ts.CreateMultiPooler(ctx, mp1)
	require.NoError(t, err)

	mp2 := &clustermetadata.MultiPooler{
		Id: &clustermetadata.ID{
			Component: clustermetadata.ID_MULTIPOOLER,
			Cell:      "cell1",
			Name:      "pooler-shard1",
		},
		Database:   "db1",
		TableGroup: "tg1",
		Shard:      "1",
		Type:       clustermetadata.PoolerType_PRIMARY,
		Hostname:   "host2",
	}
	err = ts.CreateMultiPooler(ctx, mp2)
	require.NoError(t, err)

	engine := NewEngine(
		ts,
		logger,
		cfg,
		[]config.WatchTarget{{Database: "db1", TableGroup: "tg1", Shard: "0"}},
		&rpcclient.FakeClient{},
	)

	// Refresh only shard 0
	count, err := engine.refreshPoolersForTarget(ctx, "db1", "tg1", "0", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "should discover only poolers in shard 0")

	// Verify only shard 0 pooler was added
	_, ok := engine.poolerStore.Get(poolerKey("cell1", "pooler-shard0"))
	assert.True(t, ok, "shard 0 pooler should be in store")

	_, ok = engine.poolerStore.Get(poolerKey("cell1", "pooler-shard1"))
	assert.False(t, ok, "shard 1 pooler should not be in store")
}

// TestRefreshShardMetadata_Success tests the wrapper function for shard refresh.
func TestRefreshShardMetadata_Success(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := config.NewTestConfig(config.WithCell("cell1"))

	// Create in-memory topology
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")

	// Add a multipooler to topology
	mp := &clustermetadata.MultiPooler{
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
	err := ts.CreateMultiPooler(ctx, mp)
	require.NoError(t, err)

	engine := NewEngine(
		ts,
		logger,
		cfg,
		[]config.WatchTarget{{Database: "db1", TableGroup: "tg1", Shard: "0"}},
		&rpcclient.FakeClient{},
	)

	// Refresh shard metadata
	err = engine.refreshShardMetadata(ctx, "db1", "tg1", "0", nil)
	require.NoError(t, err)

	// Verify pooler was added
	_, ok := engine.poolerStore.Get(poolerKey("cell1", "pooler1"))
	assert.True(t, ok, "pooler should be in store after shard refresh")
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
	existingHealth := store.NewPoolerHealthFromMultiPooler(mp1)
	existingHealth.IsUpToDate = false
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
	existingHealth = store.NewPoolerHealthFromMultiPooler(mp2)
	existingHealth.IsUpToDate = false
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
	existingHealth = store.NewPoolerHealthFromMultiPooler(mp3)
	existingHealth.IsUpToDate = false
	engine.poolerStore.Set(poolerKey("cell1", "pooler3"), existingHealth)

	// Force health check for shard 0
	engine.forceHealthCheckShardPoolers(ctx, "db1", "tg1", "0", nil)

	// Verify all shard 0 poolers had their LastCheckAttempted updated
	p1, ok := engine.poolerStore.Get(poolerKey("cell1", "pooler1"))
	require.True(t, ok)
	assert.False(t, p1.LastCheckAttempted.IsZero(), "pooler1 should have been polled")

	p2, ok := engine.poolerStore.Get(poolerKey("cell1", "pooler2"))
	require.True(t, ok)
	assert.False(t, p2.LastCheckAttempted.IsZero(), "pooler2 should have been polled")

	// Verify pooler in different shard was NOT polled
	p3, ok := engine.poolerStore.Get(poolerKey("cell1", "pooler3"))
	require.True(t, ok)
	assert.True(t, p3.LastCheckAttempted.IsZero(), "pooler3 (different shard) should not have been polled")
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
	existingHealth := store.NewPoolerHealthFromMultiPooler(mp1)
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
	existingHealth = store.NewPoolerHealthFromMultiPooler(mp2)
	engine.poolerStore.Set(poolerKey("cell1", "healthy-replica"), existingHealth)

	// Force health check, but ignore the dead primary
	poolersToIgnore := []string{poolerKey("cell1", "dead-primary")}
	engine.forceHealthCheckShardPoolers(ctx, "db1", "tg1", "0", poolersToIgnore)

	// Verify only the healthy replica was polled
	pDead, ok := engine.poolerStore.Get(poolerKey("cell1", "dead-primary"))
	require.True(t, ok)
	assert.True(t, pDead.LastCheckAttempted.IsZero(), "dead primary should not have been polled")

	pHealthy, ok := engine.poolerStore.Get(poolerKey("cell1", "healthy-replica"))
	require.True(t, ok)
	assert.False(t, pHealthy.LastCheckAttempted.IsZero(), "healthy replica should have been polled")
}
