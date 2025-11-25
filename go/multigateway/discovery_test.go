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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/clustermetadata/topo"
	"github.com/multigres/multigres/go/common/clustermetadata/topo/memorytopo"
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

// waitForPoolerCount waits for the PoolerDiscovery to reach the expected pooler count.
// It fails the test if the timeout is exceeded.
func waitForPoolerCount(t *testing.T, pd *PoolerDiscovery, expected int) {
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
		TableGroup: "default",
		PortMap: map[string]int32{
			"grpc": 5432,
		},
	}
}

// Integration tests - these use the public API (Start/Stop/GetPoolers)

func TestNewPoolerDiscovery(t *testing.T) {
	ctx := context.Background()
	store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer store.Close()
	logger := slog.Default()

	pd := NewPoolerDiscovery(ctx, store, "test-cell", logger)

	assert.NotNil(t, pd)
	assert.Equal(t, "test-cell", pd.cell)
	assert.Equal(t, store, pd.topoStore)
	assert.Equal(t, logger, pd.logger)
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

	pd := NewPoolerDiscovery(ctx, store, "test-cell", logger)

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

	pd := NewPoolerDiscovery(ctx, store, "test-cell", logger)

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
		poolers := pd.GetPoolers()
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

	pd := NewPoolerDiscovery(ctx, store, "test-cell", logger)

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

	pd := NewPoolerDiscovery(ctx, store, "test-cell", logger)

	// Create poolers with specific details
	pooler1 := createTestPooler("pooler1", "test-cell", "primary.example.com", "mydb", "shard-01", clustermetadatapb.PoolerType_PRIMARY)
	pooler2 := createTestPooler("pooler2", "test-cell", "replica.example.com", "mydb", "shard-02", clustermetadatapb.PoolerType_REPLICA)

	require.NoError(t, store.CreateMultiPooler(ctx, pooler1))
	require.NoError(t, store.CreateMultiPooler(ctx, pooler2))

	pd.Start()
	defer pd.Stop()

	waitForPoolerCount(t, pd, 2)

	// Verify all pooler details are correctly populated
	poolers := pd.GetPoolers()
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

	pd := NewPoolerDiscovery(ctx, store, "test-cell", logger)

	pooler1 := createTestPooler("pooler1", "test-cell", "host1", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, store.CreateMultiPooler(ctx, pooler1))

	pd.Start()
	defer pd.Stop()

	waitForPoolerCount(t, pd, 1)

	// Verify GetPoolers returns a copy (not the internal map)
	poolers1 := pd.GetPoolers()
	poolers2 := pd.GetPoolers()

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

	pd := NewPoolerDiscovery(ctx, store, "test-cell", logger)

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
	pooler1Path := "poolers/" + topo.MultiPoolerIDString(pooler1.Id) + "/Pooler"
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

	poolers := pd.GetPoolers()
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

	pd := NewPoolerDiscovery(ctx, store, "test-cell", logger)

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

	pd := NewPoolerDiscovery(ctx, store, "test-cell", logger)

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
	pd := NewPoolerDiscovery(ctx, store, "test-cell", logger)
	pd.Start()
	defer pd.Stop()

	// Verify discovery sees initial pooler
	waitForPoolerCount(t, pd, 1)
	poolers := pd.GetPoolers()
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
	poolers = pd.GetPoolers()
	require.Len(t, poolers, 2)
	names := []string{poolers[0].Id.Name, poolers[1].Id.Name}
	assert.Contains(t, names, "pooler1")
	assert.Contains(t, names, "pooler2")
}
