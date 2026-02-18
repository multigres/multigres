// Copyright 2025 Supabase, Inc.
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

package multigateway

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// Test configuration constants
const (
	// testTimeout is the maximum time to wait for async operations in tests.
	// Longer timeout for CI environments where scheduling may be slower.
	testTimeout = 5 * time.Second

	// testPollInterval is how frequently to check conditions in Eventually assertions.
	// Shorter interval for faster test feedback.
	testPollInterval = 10 * time.Millisecond
)

// waitForPoolerCount waits for the CellPoolerDiscovery to reach the expected pooler count.
// It fails the test if the timeout is exceeded.
func waitForPoolerCount(t *testing.T, pd *CellPoolerDiscovery, expected int) {
	t.Helper()
	require.Eventually(t, func() bool {
		return pd.PoolerCount() == expected
	}, testTimeout, testPollInterval,
		"Expected %d poolers, but got %d", expected, pd.PoolerCount())
}

// waitForCondition waits for an arbitrary condition to become true.
// It fails the test if the timeout is exceeded.
func waitForCondition(t *testing.T, condition func() bool, msgAndArgs ...any) {
	t.Helper()
	require.Eventually(t, condition, testTimeout, testPollInterval, msgAndArgs...)
}

// Helper function to create a pooler protobuf message
func createTestPooler(name, cell, hostname, database, shard string, poolerType clustermetadatapb.PoolerType) *clustermetadatapb.MultiPooler {
	return &clustermetadatapb.MultiPooler{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      cell,
			Name:      name,
		},
		Hostname:   hostname,
		Database:   database,
		Shard:      shard,
		Type:       poolerType,
		TableGroup: constants.DefaultTableGroup,
		PortMap: map[string]int32{
			"grpc": 5432,
		},
	}
}

// Integration tests - these use the public API (Start/Stop/GetPoolers)

func TestNewCellPoolerDiscovery(t *testing.T) {
	ctx := context.Background()
	store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer store.Close()
	logger := slog.Default()

	pd := NewCellPoolerDiscovery(ctx, store, "test-cell", logger)

	assert.NotNil(t, pd)
	assert.Equal(t, "test-cell", pd.Cell())
	assert.Equal(t, store, pd.topoStore)
	assert.NotNil(t, pd.poolers)
	assert.Empty(t, pd.poolers)
}

func TestPoolerDiscovery_StartStop(t *testing.T) {
	ctx := context.Background()
	store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer store.Close()
	logger := slog.Default()

	// Set up initial poolers BEFORE starting discovery
	pooler1 := createTestPooler("pooler1", "test-cell", "host1", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, store.CreateMultiPooler(ctx, pooler1))

	pd := NewCellPoolerDiscovery(ctx, store, "test-cell", logger)

	// Start discovery - should pick up existing pooler
	pd.Start()

	// Wait for initial discovery to complete
	waitForPoolerCount(t, pd, 1)

	// Send a change by adding a new pooler
	pooler2 := createTestPooler("pooler2", "test-cell", "host2", "db2", "shard2", clustermetadatapb.PoolerType_REPLICA)
	require.NoError(t, store.CreateMultiPooler(ctx, pooler2))

	// Wait for change to be processed
	waitForPoolerCount(t, pd, 2)

	// Stop discovery
	pd.Stop()

	// Verify discovery stopped cleanly
	assert.Equal(t, 2, pd.PoolerCount())
}

func TestPoolerDiscovery_MultiplePoolerUpdates(t *testing.T) {
	ctx := context.Background()
	store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer store.Close()
	logger := slog.Default()

	pd := NewCellPoolerDiscovery(ctx, store, "test-cell", logger)

	// Start with two initial poolers
	pooler1 := createTestPooler("pooler1", "test-cell", "host1", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	pooler2 := createTestPooler("pooler2", "test-cell", "host2", "db2", "shard2", clustermetadatapb.PoolerType_REPLICA)
	require.NoError(t, store.CreateMultiPooler(ctx, pooler1))
	require.NoError(t, store.CreateMultiPooler(ctx, pooler2))

	pd.Start()
	defer pd.Stop()

	// Wait for initial discovery
	waitForPoolerCount(t, pd, 2)

	// Update an existing pooler
	pooler1Info, err := store.GetMultiPooler(ctx, pooler1.Id)
	require.NoError(t, err)
	pooler1Info.Hostname = "host1-updated"
	require.NoError(t, store.UpdateMultiPooler(ctx, pooler1Info))

	// Verify the update is reflected
	waitForCondition(t, func() bool {
		poolers := pd.GetPoolersForAdmin()
		for _, p := range poolers {
			if p.Id.Name == "pooler1" && p.Hostname == "host1-updated" {
				return true
			}
		}
		return false
	}, "Expected pooler1 to be updated to host1-updated")

	// Count should remain 2
	assert.Equal(t, 2, pd.PoolerCount())
}

func TestPoolerDiscovery_EmptyInitialState(t *testing.T) {
	ctx := context.Background()
	store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer store.Close()
	logger := slog.Default()

	pd := NewCellPoolerDiscovery(ctx, store, "test-cell", logger)

	// Start with no poolers
	pd.Start()
	defer pd.Stop()

	require.Eventually(t, func() bool {
		return !pd.LastRefresh().IsZero()
	}, testTimeout, testPollInterval,
		"Expected lastRefresh to be set after starting discovery")

	// After initial discovery completes, count should still be 0
	assert.Equal(t, 0, pd.PoolerCount())

	// Now add a pooler via watch
	pooler1 := createTestPooler("pooler1", "test-cell", "host1", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, store.CreateMultiPooler(ctx, pooler1))

	waitForPoolerCount(t, pd, 1)
}

func TestPoolerDiscovery_VerifyPoolerDetails(t *testing.T) {
	ctx := context.Background()
	store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer store.Close()
	logger := slog.Default()

	pd := NewCellPoolerDiscovery(ctx, store, "test-cell", logger)

	// Create poolers with specific details
	pooler1 := createTestPooler("pooler1", "test-cell", "primary.example.com", "mydb", "shard-01", clustermetadatapb.PoolerType_PRIMARY)
	pooler2 := createTestPooler("pooler2", "test-cell", "replica.example.com", "mydb", "shard-02", clustermetadatapb.PoolerType_REPLICA)

	require.NoError(t, store.CreateMultiPooler(ctx, pooler1))
	require.NoError(t, store.CreateMultiPooler(ctx, pooler2))

	pd.Start()
	defer pd.Stop()

	waitForPoolerCount(t, pd, 2)

	// Verify all pooler details are correctly populated
	poolers := pd.GetPoolersForAdmin()
	poolerMap := make(map[string]*clustermetadatapb.MultiPooler)
	for _, p := range poolers {
		poolerMap[p.Id.Name] = p
	}

	// Verify pooler1
	require.Contains(t, poolerMap, "pooler1")
	assert.Equal(t, "primary.example.com", poolerMap["pooler1"].Hostname)
	assert.Equal(t, "mydb", poolerMap["pooler1"].Database)
	assert.Equal(t, "shard-01", poolerMap["pooler1"].Shard)
	assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, poolerMap["pooler1"].Type)

	// Verify pooler2
	require.Contains(t, poolerMap, "pooler2")
	assert.Equal(t, "replica.example.com", poolerMap["pooler2"].Hostname)
	assert.Equal(t, "mydb", poolerMap["pooler2"].Database)
	assert.Equal(t, "shard-02", poolerMap["pooler2"].Shard)
	assert.Equal(t, clustermetadatapb.PoolerType_REPLICA, poolerMap["pooler2"].Type)
}

func TestPoolerDiscovery_GetPoolers_ThreadSafe(t *testing.T) {
	ctx := context.Background()
	store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer store.Close()
	logger := slog.Default()

	pd := NewCellPoolerDiscovery(ctx, store, "test-cell", logger)

	pooler1 := createTestPooler("pooler1", "test-cell", "host1", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, store.CreateMultiPooler(ctx, pooler1))

	pd.Start()
	defer pd.Stop()

	waitForPoolerCount(t, pd, 1)

	// Verify GetPoolers returns a copy (not the internal map)
	poolers1 := pd.GetPoolersForAdmin()
	poolers2 := pd.GetPoolersForAdmin()

	// Modifying one shouldn't affect the other
	assert.NotSame(t, poolers1[0], poolers2[0])

	// But they should have the same data
	assert.Equal(t, poolers1[0].Hostname, poolers2[0].Hostname)
}

func TestPoolerDiscovery_InvalidDataHandling(t *testing.T) {
	ctx := context.Background()
	store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer store.Close()
	logger := slog.Default()

	pd := NewCellPoolerDiscovery(ctx, store, "test-cell", logger)

	// Get connection to inject data directly
	conn, err := store.ConnForCell(ctx, "test-cell")
	require.NoError(t, err)

	// Set up initial data with a mix of valid and invalid poolers
	pooler1 := createTestPooler("pooler1", "test-cell", "host1", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	pooler2 := createTestPooler("pooler2", "test-cell", "host2", "db2", "shard2", clustermetadatapb.PoolerType_REPLICA)

	// Create valid poolers
	require.NoError(t, store.CreateMultiPooler(ctx, pooler1))
	require.NoError(t, store.CreateMultiPooler(ctx, pooler2))

	// Verify we can read pooler1 data directly to validate the path
	pooler1Path := "poolers/" + topoclient.MultiPoolerIDString(pooler1.Id) + "/Pooler"
	pooler1Data, _, err := conn.Get(ctx, pooler1Path)
	require.NoError(t, err, "Should be able to read valid pooler data")
	require.NotEmpty(t, pooler1Data, "Valid pooler data should not be empty")

	// Inject corrupted data directly - should be logged and ignored
	_, err = conn.Create(ctx, "poolers/pooler3-test-cell-pooler3/Pooler", []byte("corrupted data"))
	require.NoError(t, err)

	// Inject invalid path data - should be silently ignored
	_, err = conn.Create(ctx, "poolers/something/else", []byte("invalid"))
	require.NoError(t, err)

	pd.Start()
	defer pd.Stop()

	// Should discover only the valid poolers
	waitForPoolerCount(t, pd, 2)

	poolers := pd.GetPoolersForAdmin()
	poolerMap := make(map[string]*clustermetadatapb.MultiPooler)
	for _, p := range poolers {
		poolerMap[p.Id.Name] = p
	}

	// Verify only valid poolers were discovered
	assert.Contains(t, poolerMap, "pooler1")
	assert.Contains(t, poolerMap, "pooler2")
	assert.NotContains(t, poolerMap, "pooler3")
	assert.Equal(t, "host1", poolerMap["pooler1"].Hostname)
	assert.Equal(t, "host2", poolerMap["pooler2"].Hostname)
}

// Internal unit tests - these test internal methods directly for edge cases

func TestPoolerDiscovery_LastRefresh(t *testing.T) {
	ctx := context.Background()
	store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer store.Close()
	logger := slog.Default()

	pd := NewCellPoolerDiscovery(ctx, store, "test-cell", logger)

	// Initially should be zero
	assert.True(t, pd.LastRefresh().IsZero())

	// Create a pooler and start discovery
	pooler1 := createTestPooler("pooler1", "test-cell", "host1", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, store.CreateMultiPooler(ctx, pooler1))

	before := time.Now()
	pd.Start()
	defer pd.Stop()

	// Wait for initial discovery
	waitForPoolerCount(t, pd, 1)
	after := time.Now()

	lastRefresh := pd.LastRefresh()
	assert.False(t, lastRefresh.IsZero())
	assert.True(t, lastRefresh.After(before) || lastRefresh.Equal(before))
	assert.True(t, lastRefresh.Before(after) || lastRefresh.Equal(after))
}

func TestPoolerDiscovery_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer store.Close()
	logger := slog.Default()

	pd := NewCellPoolerDiscovery(ctx, store, "test-cell", logger)

	// Set up initial poolers
	pooler1 := createTestPooler("pooler1", "test-cell", "host1", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, store.CreateMultiPooler(ctx, pooler1))

	// Start discovery
	pd.Start()

	// Wait for initial discovery
	waitForPoolerCount(t, pd, 1)

	// Cancel context
	cancel()

	// Stop should complete quickly
	done := make(chan struct{})
	go func() {
		pd.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(testTimeout):
		t.Fatal("Stop() took too long after context cancellation")
	}
}

// TestPoolerDiscovery_ReconnectsAfterWatchClosed tests that discovery reconnects
// when the watch channel is closed (e.g., due to etcd compaction).
func TestPoolerDiscovery_ReconnectsAfterWatchClosed(t *testing.T) {
	ctx := context.Background()
	store, factory := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer store.Close()
	logger := slog.Default()

	// Create initial pooler
	pooler1 := createTestPooler("pooler1", "test-cell", "host1", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, store.CreateMultiPooler(ctx, pooler1))

	// Start discovery
	pd := NewCellPoolerDiscovery(ctx, store, "test-cell", logger)
	pd.Start()
	defer pd.Stop()

	// Verify discovery sees initial pooler
	waitForPoolerCount(t, pd, 1)
	poolers := pd.GetPoolersForAdmin()
	require.Len(t, poolers, 1)
	assert.Equal(t, "pooler1", poolers[0].Id.Name)

	// Simulate watch channel closure (like etcd compaction)
	// This closes all watch channels for the "poolers" path
	factory.CloseWatches("test-cell", "poolers")

	// Give discovery a moment to detect the closure
	time.Sleep(100 * time.Millisecond)

	// Add a new pooler after watch closure
	pooler2 := createTestPooler("pooler2", "test-cell", "host2", "db2", "shard2", clustermetadatapb.PoolerType_REPLICA)
	require.NoError(t, store.CreateMultiPooler(ctx, pooler2))

	// Discovery should reconnect and see the new pooler
	waitForCondition(t, func() bool {
		return pd.PoolerCount() == 2
	}, "Discovery should reconnect and see 2 poolers after watch closure")

	// Verify both poolers are present
	poolers = pd.GetPoolersForAdmin()
	require.Len(t, poolers, 2)
	names := []string{poolers[0].Id.Name, poolers[1].Id.Name}
	assert.Contains(t, names, "pooler1")
	assert.Contains(t, names, "pooler2")
}

// GlobalPoolerDiscovery tests

// waitForGlobalPoolerCount waits for the GlobalPoolerDiscovery to reach the expected pooler count.
func waitForGlobalPoolerCount(t *testing.T, gd *GlobalPoolerDiscovery, expected int) {
	t.Helper()
	require.Eventually(t, func() bool {
		return gd.PoolerCount() == expected
	}, testTimeout, testPollInterval,
		"Expected %d poolers, but got %d", expected, gd.PoolerCount())
}

func TestNewGlobalPoolerDiscovery(t *testing.T) {
	ctx := context.Background()
	store, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer store.Close()
	logger := slog.Default()

	gd := NewGlobalPoolerDiscovery(ctx, store, "zone1", logger)

	assert.NotNil(t, gd)
	assert.Equal(t, "zone1", gd.localCell)
	assert.Equal(t, store, gd.topoStore)
	assert.NotNil(t, gd.cellWatchers)
	assert.Empty(t, gd.cellWatchers)
}

func TestGlobalPoolerDiscovery_MultiCell(t *testing.T) {
	ctx := context.Background()
	// Create a store with multiple cells
	store, _ := memorytopo.NewServerAndFactory(ctx, "zone1", "zone2")
	defer store.Close()
	logger := slog.Default()

	// Create poolers in different cells
	pooler1 := createTestPooler("pooler1", "zone1", "host1", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	pooler2 := createTestPooler("pooler2", "zone2", "host2", "db2", "shard1", clustermetadatapb.PoolerType_REPLICA)
	require.NoError(t, store.CreateMultiPooler(ctx, pooler1))
	require.NoError(t, store.CreateMultiPooler(ctx, pooler2))

	// Start global discovery
	gd := NewGlobalPoolerDiscovery(ctx, store, "zone1", logger)
	gd.Start()
	defer gd.Stop()

	// Should discover poolers from both cells
	waitForGlobalPoolerCount(t, gd, 2)
	assert.Equal(t, 2, gd.PoolerCount())
}

func TestGlobalPoolerDiscovery_GetCellStatusesForAdmin(t *testing.T) {
	ctx := context.Background()
	store, _ := memorytopo.NewServerAndFactory(ctx, "zone1", "zone2")
	defer store.Close()
	logger := slog.Default()

	// Create poolers in different cells
	pooler1 := createTestPooler("pooler1", "zone1", "host1", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	pooler2 := createTestPooler("pooler2", "zone2", "host2", "db2", "shard1", clustermetadatapb.PoolerType_REPLICA)
	require.NoError(t, store.CreateMultiPooler(ctx, pooler1))
	require.NoError(t, store.CreateMultiPooler(ctx, pooler2))

	// Start global discovery
	gd := NewGlobalPoolerDiscovery(ctx, store, "zone1", logger)
	gd.Start()
	defer gd.Stop()

	waitForGlobalPoolerCount(t, gd, 2)

	// Get cell statuses
	statuses := gd.GetCellStatusesForAdmin()
	require.Len(t, statuses, 2)

	// Find each cell's status
	var zone1Status, zone2Status *CellStatusInfo
	for i := range statuses {
		switch statuses[i].Cell {
		case "zone1":
			zone1Status = &statuses[i]
		case "zone2":
			zone2Status = &statuses[i]
		}
	}

	require.NotNil(t, zone1Status, "Should have zone1 status")
	require.NotNil(t, zone2Status, "Should have zone2 status")
	assert.Len(t, zone1Status.Poolers, 1)
	assert.Len(t, zone2Status.Poolers, 1)
	assert.Equal(t, "pooler1", zone1Status.Poolers[0].Id.Name)
	assert.Equal(t, "pooler2", zone2Status.Poolers[0].Id.Name)
}

// TestPoolerDiscovery_PoolerDeletion tests that poolers are removed from
// discovery when they are deleted from the topology store.
func TestPoolerDiscovery_PoolerDeletion(t *testing.T) {
	ctx := context.Background()
	store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer store.Close()
	logger := slog.Default()

	pd := NewCellPoolerDiscovery(ctx, store, "test-cell", logger)

	// Create two poolers
	pooler1 := createTestPooler("pooler1", "test-cell", "host1", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	pooler2 := createTestPooler("pooler2", "test-cell", "host2", "db1", "shard1", clustermetadatapb.PoolerType_REPLICA)
	require.NoError(t, store.CreateMultiPooler(ctx, pooler1))
	require.NoError(t, store.CreateMultiPooler(ctx, pooler2))

	pd.Start()
	defer pd.Stop()

	// Wait for initial discovery
	waitForPoolerCount(t, pd, 2)

	// Delete pooler1
	require.NoError(t, store.UnregisterMultiPooler(ctx, pooler1.Id))

	// Wait for deletion to be processed
	waitForPoolerCount(t, pd, 1)

	// Verify only pooler2 remains
	poolers := pd.GetPoolersForAdmin()
	require.Len(t, poolers, 1)
	assert.Equal(t, "pooler2", poolers[0].Id.Name)
}

// TestPoolerDiscovery_NewPrimaryEvictsOldPrimary tests that when a new PRIMARY
// pooler is discovered for the same TableGroup/Shard, any existing PRIMARY for
// that TableGroup/Shard is evicted from the discovery cache.
// This handles the failover case where a new PRIMARY comes up but the old
// crashed PRIMARY's record hasn't been cleaned up yet.
func TestPoolerDiscovery_NewPrimaryEvictsOldPrimary(t *testing.T) {
	ctx := context.Background()
	store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer store.Close()
	logger := slog.Default()

	pd := NewCellPoolerDiscovery(ctx, store, "test-cell", logger)

	// Create old primary (simulating a crashed primary that hasn't been cleaned up)
	oldPrimary := createTestPooler("old-primary", "test-cell", "old-host", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, store.CreateMultiPooler(ctx, oldPrimary))

	pd.Start()
	defer pd.Stop()

	// Wait for initial discovery
	waitForPoolerCount(t, pd, 1)

	// Verify old primary is discovered
	poolers := pd.GetPoolersForAdmin()
	require.Len(t, poolers, 1)
	assert.Equal(t, "old-primary", poolers[0].Id.Name)

	// Now add a new primary for the same tablegroup/shard (simulating failover)
	newPrimary := createTestPooler("new-primary", "test-cell", "new-host", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, store.CreateMultiPooler(ctx, newPrimary))

	// Wait for the new primary to be discovered and old one evicted
	// The count should still be 1 because old primary gets evicted
	waitForCondition(t, func() bool {
		poolers := pd.GetPoolersForAdmin()
		if len(poolers) != 1 {
			return false
		}
		return poolers[0].Id.Name == "new-primary"
	}, "Expected new-primary to replace old-primary")

	// Verify only new primary is present
	poolers = pd.GetPoolersForAdmin()
	require.Len(t, poolers, 1)
	assert.Equal(t, "new-primary", poolers[0].Id.Name)
	assert.Equal(t, "new-host", poolers[0].Hostname)
}

// TestPoolerDiscovery_MultipleShardsPrimaryEviction tests that PRIMARY eviction
// only affects poolers with the same TableGroup/Shard combination.
func TestPoolerDiscovery_MultipleShardsPrimaryEviction(t *testing.T) {
	ctx := context.Background()
	store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer store.Close()
	logger := slog.Default()

	pd := NewCellPoolerDiscovery(ctx, store, "test-cell", logger)

	// Create primaries for different shards
	primary1 := createTestPooler("primary-shard1", "test-cell", "host1", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	primary2 := createTestPooler("primary-shard2", "test-cell", "host2", "db1", "shard2", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, store.CreateMultiPooler(ctx, primary1))
	require.NoError(t, store.CreateMultiPooler(ctx, primary2))

	pd.Start()
	defer pd.Stop()

	// Wait for initial discovery
	waitForPoolerCount(t, pd, 2)

	// Add new primary only for shard1
	newPrimary := createTestPooler("new-primary-shard1", "test-cell", "new-host", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, store.CreateMultiPooler(ctx, newPrimary))

	// Wait for the new primary to be discovered and old shard1 primary evicted
	waitForCondition(t, func() bool {
		poolers := pd.GetPoolersForAdmin()
		if len(poolers) != 2 {
			return false
		}
		names := make(map[string]bool)
		for _, p := range poolers {
			names[p.Id.Name] = true
		}
		// Should have new-primary-shard1 and primary-shard2, NOT primary-shard1
		return names["new-primary-shard1"] && names["primary-shard2"] && !names["primary-shard1"]
	}, "Expected new-primary-shard1 and primary-shard2 to be present")

	// Verify the correct primaries are present
	poolers := pd.GetPoolersForAdmin()
	require.Len(t, poolers, 2)
	names := make(map[string]bool)
	for _, p := range poolers {
		names[p.Id.Name] = true
	}
	assert.True(t, names["new-primary-shard1"], "Should have new-primary-shard1")
	assert.True(t, names["primary-shard2"], "Should have primary-shard2")
	assert.False(t, names["primary-shard1"], "Should NOT have old primary-shard1")
}

// TestPoolerDiscovery_ReplicaDoesNotEvictPrimary tests that adding a REPLICA
// does not evict an existing PRIMARY.
func TestPoolerDiscovery_ReplicaDoesNotEvictPrimary(t *testing.T) {
	ctx := context.Background()
	store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer store.Close()
	logger := slog.Default()

	pd := NewCellPoolerDiscovery(ctx, store, "test-cell", logger)

	// Create a primary
	primary := createTestPooler("primary", "test-cell", "primary-host", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, store.CreateMultiPooler(ctx, primary))

	pd.Start()
	defer pd.Stop()

	// Wait for initial discovery
	waitForPoolerCount(t, pd, 1)

	// Add a replica for the same shard
	replica := createTestPooler("replica", "test-cell", "replica-host", "db1", "shard1", clustermetadatapb.PoolerType_REPLICA)
	require.NoError(t, store.CreateMultiPooler(ctx, replica))

	// Wait for replica to be discovered
	waitForPoolerCount(t, pd, 2)

	// Verify both are present
	poolers := pd.GetPoolersForAdmin()
	require.Len(t, poolers, 2)
	names := make(map[string]bool)
	for _, p := range poolers {
		names[p.Id.Name] = true
	}
	assert.True(t, names["primary"], "Primary should still be present")
	assert.True(t, names["replica"], "Replica should be present")
}

// TestPoolerDiscovery_EvictionCallsOnPoolerRemoved verifies that onPoolerRemoved
// is called when a conflicting primary is evicted during failover.
func TestPoolerDiscovery_EvictionCallsOnPoolerRemoved(t *testing.T) {
	ctx := context.Background()
	store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer store.Close()
	logger := slog.Default()

	pd := NewCellPoolerDiscovery(ctx, store, "test-cell", logger)

	// Track callback invocations
	listener := &mockPoolerListener{}
	pd.onPoolerChanged = listener.OnPoolerChanged
	pd.onPoolerRemoved = listener.OnPoolerRemoved

	// Create old primary
	oldPrimary := createTestPooler("old-primary", "test-cell", "old-host", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, store.CreateMultiPooler(ctx, oldPrimary))

	pd.Start()
	defer pd.Stop()

	// Wait for initial discovery
	waitForPoolerCount(t, pd, 1)

	// Clear the tracked callbacks (initial discovery triggers OnPoolerChanged)
	listener.mu.Lock()
	listener.changedPoolers = nil
	listener.removedPoolers = nil
	listener.mu.Unlock()

	// Add new primary for the same tablegroup/shard (triggers eviction)
	newPrimary := createTestPooler("new-primary", "test-cell", "new-host", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, store.CreateMultiPooler(ctx, newPrimary))

	// Wait for the new primary to be discovered and old one evicted
	waitForCondition(t, func() bool {
		poolers := pd.GetPoolersForAdmin()
		if len(poolers) != 1 {
			return false
		}
		return poolers[0].Id.Name == "new-primary"
	}, "Expected new-primary to replace old-primary")

	// Verify OnPoolerRemoved was called for the evicted old primary
	removedPoolers := listener.getRemovedPoolers()
	require.Len(t, removedPoolers, 1, "OnPoolerRemoved should be called for evicted primary")
	assert.Equal(t, topoclient.MultiPoolerIDString(oldPrimary.Id), removedPoolers[0])

	// Verify OnPoolerChanged was called for the new primary
	changedPoolers := listener.getChangedPoolers()
	require.Contains(t, changedPoolers, topoclient.MultiPoolerIDString(newPrimary.Id))
}

// TestGlobalPoolerDiscovery_WatchDiscoversNewCells tests that the watch-based
// cell discovery detects cells added after the gateway has started. This is the
// core regression test for the bug where discovery blocked forever on
// <-ctx.Done() and never discovered new cells.
func TestGlobalPoolerDiscovery_WatchDiscoversNewCells(t *testing.T) {
	ctx := context.Background()
	// Start with NO cells — watch returns empty initial set
	store, factory := memorytopo.NewServerAndFactory(ctx)
	defer store.Close()

	gd := NewGlobalPoolerDiscovery(ctx, store, "zone1", slog.Default())
	gd.Start()
	defer gd.Stop()

	// Initially no poolers — watch sees empty cells directory
	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, 0, gd.PoolerCount(), "Should have 0 poolers with no cells")

	// Dynamically add a cell (simulates multiorch registering cells after gateway start)
	require.NoError(t, factory.AddCell(ctx, store, "zone1"))

	// Create a pooler in the new cell
	pooler := createTestPooler("pooler1", "zone1", "host1", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, store.CreateMultiPooler(ctx, pooler))

	// Watch should fire an event for the new cell, start a watcher, and discover the pooler
	waitForGlobalPoolerCount(t, gd, 1)
	assert.Equal(t, 1, gd.PoolerCount())
}

// TestExtractCellFromPath tests the path parsing logic for cell watch events.
func TestExtractCellFromPath(t *testing.T) {
	tests := []struct {
		path     string
		expected string
	}{
		{"cells/zone1/Cell", "zone1"},
		{"cells/us-east-1/Cell", "us-east-1"},
		{"cells/zone1", "zone1"},
		{"cells/", ""},
		{"cells", ""},
		{"other/zone1/Cell", ""},
		{"", ""},
	}

	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			result := extractCellFromPath(tc.path)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// TestCellPoolerDiscovery_ExtractPoolerIDFromPath tests the path parsing logic.
func TestCellPoolerDiscovery_ExtractPoolerIDFromPath(t *testing.T) {
	ctx := context.Background()
	store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer store.Close()
	logger := slog.Default()

	pd := NewCellPoolerDiscovery(ctx, store, "test-cell", logger)

	tests := []struct {
		path     string
		expected string
	}{
		{"poolers/multipooler-cell1-pooler1/Pooler", "multipooler-cell1-pooler1"},
		{"poolers/some-complex-id/Pooler", "some-complex-id"},
		{"poolers/id/Pooler", "id"},
		{"poolers/Pooler", ""}, // Missing ID segment
		{"other/path/Pooler", ""},
		{"poolers/id/other", ""},
		{"", ""},
	}

	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			result := pd.extractPoolerIDFromPath(tc.path)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// mockPoolerListener records pooler change events for testing.
type mockPoolerListener struct {
	mu             sync.Mutex
	changedPoolers []string
	removedPoolers []string
}

func (m *mockPoolerListener) OnPoolerChanged(pooler *clustermetadatapb.MultiPooler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.changedPoolers = append(m.changedPoolers, topoclient.MultiPoolerIDString(pooler.Id))
}

func (m *mockPoolerListener) OnPoolerRemoved(pooler *clustermetadatapb.MultiPooler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.removedPoolers = append(m.removedPoolers, topoclient.MultiPoolerIDString(pooler.Id))
}

func (m *mockPoolerListener) getChangedPoolers() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]string, len(m.changedPoolers))
	copy(result, m.changedPoolers)
	return result
}

func (m *mockPoolerListener) getRemovedPoolers() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]string, len(m.removedPoolers))
	copy(result, m.removedPoolers)
	return result
}

func TestGlobalPoolerDiscovery_RegisterListener_ReplaysExistingPoolers(t *testing.T) {
	ctx := context.Background()
	store, _ := memorytopo.NewServerAndFactory(ctx, "zone1", "zone2")
	defer store.Close()
	logger := slog.Default()

	// Create poolers in different cells BEFORE starting discovery
	pooler1 := createTestPooler("pooler1", "zone1", "host1", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	pooler2 := createTestPooler("pooler2", "zone2", "host2", "db2", "shard1", clustermetadatapb.PoolerType_REPLICA)
	require.NoError(t, store.CreateMultiPooler(ctx, pooler1))
	require.NoError(t, store.CreateMultiPooler(ctx, pooler2))

	// Start global discovery
	gd := NewGlobalPoolerDiscovery(ctx, store, "zone1", logger)
	gd.Start()
	defer gd.Stop()

	// Wait for poolers to be discovered
	waitForGlobalPoolerCount(t, gd, 2)

	// Now register a listener AFTER poolers are already discovered
	listener := &mockPoolerListener{}
	gd.RegisterListener(listener)

	// Wait for replay notifications to be processed
	waitForCondition(t, func() bool {
		return len(listener.getChangedPoolers()) == 2
	}, "Listener should receive replay of 2 existing poolers")

	// The listener should have received OnPoolerChanged for all existing poolers
	changedPoolers := listener.getChangedPoolers()
	require.Len(t, changedPoolers, 2, "Listener should receive replay of existing poolers")

	// Verify both poolers were replayed (order may vary)
	assert.Contains(t, changedPoolers, topoclient.MultiPoolerIDString(pooler1.Id))
	assert.Contains(t, changedPoolers, topoclient.MultiPoolerIDString(pooler2.Id))

	// No poolers should have been removed
	removedPoolers := listener.getRemovedPoolers()
	assert.Empty(t, removedPoolers)
}

func TestGlobalPoolerDiscovery_RegisterListener_ReceivesSubsequentChanges(t *testing.T) {
	ctx := context.Background()
	store, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer store.Close()
	logger := slog.Default()

	// Start with one pooler
	pooler1 := createTestPooler("pooler1", "zone1", "host1", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, store.CreateMultiPooler(ctx, pooler1))

	// Start global discovery
	gd := NewGlobalPoolerDiscovery(ctx, store, "zone1", logger)
	gd.Start()
	defer gd.Stop()

	waitForGlobalPoolerCount(t, gd, 1)

	// Register listener
	listener := &mockPoolerListener{}
	gd.RegisterListener(listener)

	// Wait for replay notification to be processed
	waitForCondition(t, func() bool {
		return len(listener.getChangedPoolers()) == 1
	}, "Listener should receive replay of 1 existing pooler")

	// Should have replayed pooler1
	changedPoolers := listener.getChangedPoolers()
	require.Len(t, changedPoolers, 1)

	// Now add another pooler
	pooler2 := createTestPooler("pooler2", "zone1", "host2", "db2", "shard2", clustermetadatapb.PoolerType_REPLICA)
	require.NoError(t, store.CreateMultiPooler(ctx, pooler2))

	// Wait for listener to receive the new pooler
	waitForCondition(t, func() bool {
		return len(listener.getChangedPoolers()) == 2
	}, "Listener should receive new pooler after registration")

	changedPoolers = listener.getChangedPoolers()
	assert.Contains(t, changedPoolers, topoclient.MultiPoolerIDString(pooler2.Id))
}
