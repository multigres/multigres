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
	"sync/atomic"
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

// waitForGlobalPoolerCount waits for the GlobalPoolerDiscovery to reach the expected pooler count.
func waitForGlobalPoolerCount(t *testing.T, gd *GlobalPoolerDiscovery, expected int) {
	t.Helper()
	require.Eventually(t, func() bool {
		return gd.PoolerCount() == expected
	}, testTimeout, testPollInterval,
		"Expected %d poolers, but got %d", expected, gd.PoolerCount())
}

// waitForCondition waits for an arbitrary condition to become true.
// It fails the test if the timeout is exceeded.
func waitForCondition(t *testing.T, condition func() bool, msgAndArgs ...any) {
	t.Helper()
	require.Eventually(t, condition, testTimeout, testPollInterval, msgAndArgs...)
}

// getAllPoolers is a test helper that returns all poolers from a GlobalPoolerDiscovery as a flat slice.
func getAllPoolers(gd *GlobalPoolerDiscovery) []*clustermetadatapb.MultiPooler {
	var poolers []*clustermetadatapb.MultiPooler
	for _, status := range gd.GetCellStatusesForAdmin() {
		poolers = append(poolers, status.Poolers...)
	}
	return poolers
}

// countingTopoStore wraps a topoclient.Store to count WatchRecursive() calls for testing
type countingTopoStore struct {
	topoclient.Store
	watchCalls atomic.Int32
}

func newCountingTopoStore(store topoclient.Store) *countingTopoStore {
	return &countingTopoStore{Store: store}
}

func (s *countingTopoStore) ConnForCell(ctx context.Context, cell string) (topoclient.Conn, error) {
	conn, err := s.Store.ConnForCell(ctx, cell)
	if err != nil {
		return nil, err
	}
	return &countingTopoConn{Conn: conn, store: s}, nil
}

func (s *countingTopoStore) WatchCallCount() int {
	return int(s.watchCalls.Load())
}

// countingTopoConn wraps a topoclient.Conn to intercept Watch calls
type countingTopoConn struct {
	topoclient.Conn
	store *countingTopoStore
}

func (c *countingTopoConn) Watch(ctx context.Context, filePath string) (*topoclient.WatchData, <-chan *topoclient.WatchData, error) {
	c.store.watchCalls.Add(1)
	return c.Conn.Watch(ctx, filePath)
}

func (c *countingTopoConn) WatchRecursive(ctx context.Context, dirPath string) ([]*topoclient.WatchDataRecursive, <-chan *topoclient.WatchDataRecursive, error) {
	c.store.watchCalls.Add(1)
	return c.Conn.WatchRecursive(ctx, dirPath)
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

// GlobalPoolerDiscovery tests

func TestNewGlobalPoolerDiscovery(t *testing.T) {
	ctx := t.Context()
	store, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer store.Close()
	logger := slog.Default()

	gd := NewGlobalPoolerDiscovery(ctx, store, "zone1", logger)

	assert.NotNil(t, gd)
	assert.Equal(t, "zone1", gd.localCell)
	assert.Equal(t, 0, gd.PoolerCount())
}

func TestPoolerDiscovery_StartStop(t *testing.T) {
	ctx := t.Context()
	store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer store.Close()
	logger := slog.Default()

	// Set up initial poolers BEFORE starting discovery
	pooler1 := createTestPooler("pooler1", "test-cell", "host1", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, store.CreateMultiPooler(ctx, pooler1))

	gd := NewGlobalPoolerDiscovery(ctx, store, "test-cell", logger)

	// Start discovery - should pick up existing pooler
	gd.Start()

	// Wait for initial discovery to complete
	waitForGlobalPoolerCount(t, gd, 1)

	// Send a change by adding a new pooler
	pooler2 := createTestPooler("pooler2", "test-cell", "host2", "db2", "shard2", clustermetadatapb.PoolerType_REPLICA)
	require.NoError(t, store.CreateMultiPooler(ctx, pooler2))

	// Wait for change to be processed
	waitForGlobalPoolerCount(t, gd, 2)

	// Stop discovery
	gd.Stop()

	// Verify discovery stopped cleanly
	assert.Equal(t, 2, gd.PoolerCount())
}

func TestPoolerDiscovery_MultiplePoolerUpdates(t *testing.T) {
	ctx := t.Context()
	store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer store.Close()
	logger := slog.Default()

	gd := NewGlobalPoolerDiscovery(ctx, store, "test-cell", logger)

	// Start with two initial poolers
	pooler1 := createTestPooler("pooler1", "test-cell", "host1", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	pooler2 := createTestPooler("pooler2", "test-cell", "host2", "db2", "shard2", clustermetadatapb.PoolerType_REPLICA)
	require.NoError(t, store.CreateMultiPooler(ctx, pooler1))
	require.NoError(t, store.CreateMultiPooler(ctx, pooler2))

	gd.Start()
	defer gd.Stop()

	// Wait for initial discovery
	waitForGlobalPoolerCount(t, gd, 2)

	// Update an existing pooler
	pooler1Info, err := store.GetMultiPooler(ctx, pooler1.Id)
	require.NoError(t, err)
	pooler1Info.Hostname = "host1-updated"
	require.NoError(t, store.UpdateMultiPooler(ctx, pooler1Info))

	// Verify the update is reflected
	waitForCondition(t, func() bool {
		poolers := getAllPoolers(gd)
		for _, p := range poolers {
			if p.Id.Name == "pooler1" && p.Hostname == "host1-updated" {
				return true
			}
		}
		return false
	}, "Expected pooler1 to be updated to host1-updated")

	// Count should remain 2
	assert.Equal(t, 2, gd.PoolerCount())
}

func TestPoolerDiscovery_EmptyInitialState(t *testing.T) {
	ctx := t.Context()
	store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer store.Close()
	logger := slog.Default()

	gd := NewGlobalPoolerDiscovery(ctx, store, "test-cell", logger)

	// Start with no poolers
	gd.Start()
	defer gd.Stop()

	require.Eventually(t, func() bool {
		return !gd.LastCellRefresh().IsZero()
	}, testTimeout, testPollInterval,
		"Expected LastCellRefresh to be set after starting discovery")

	// After initial discovery completes, count should still be 0
	assert.Equal(t, 0, gd.PoolerCount())

	// Now add a pooler via watch
	pooler1 := createTestPooler("pooler1", "test-cell", "host1", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, store.CreateMultiPooler(ctx, pooler1))

	waitForGlobalPoolerCount(t, gd, 1)
}

func TestPoolerDiscovery_VerifyPoolerDetails(t *testing.T) {
	ctx := t.Context()
	store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer store.Close()
	logger := slog.Default()

	gd := NewGlobalPoolerDiscovery(ctx, store, "test-cell", logger)

	// Create poolers with specific details
	pooler1 := createTestPooler("pooler1", "test-cell", "primary.example.com", "mydb", "shard-01", clustermetadatapb.PoolerType_PRIMARY)
	pooler2 := createTestPooler("pooler2", "test-cell", "replica.example.com", "mydb", "shard-02", clustermetadatapb.PoolerType_REPLICA)

	require.NoError(t, store.CreateMultiPooler(ctx, pooler1))
	require.NoError(t, store.CreateMultiPooler(ctx, pooler2))

	gd.Start()
	defer gd.Stop()

	waitForGlobalPoolerCount(t, gd, 2)

	// Verify all pooler details are correctly populated
	poolers := getAllPoolers(gd)
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
	ctx := t.Context()
	store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer store.Close()
	logger := slog.Default()

	gd := NewGlobalPoolerDiscovery(ctx, store, "test-cell", logger)

	pooler1 := createTestPooler("pooler1", "test-cell", "host1", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, store.CreateMultiPooler(ctx, pooler1))

	gd.Start()
	defer gd.Stop()

	waitForGlobalPoolerCount(t, gd, 1)

	// Verify GetCellStatusesForAdmin returns independent copies (proto.Clone)
	statuses1 := gd.GetCellStatusesForAdmin()
	statuses2 := gd.GetCellStatusesForAdmin()

	require.Len(t, statuses1, 1)
	require.Len(t, statuses1[0].Poolers, 1)
	require.Len(t, statuses2, 1)
	require.Len(t, statuses2[0].Poolers, 1)

	// Different pointer (proto.Clone), same data
	assert.NotSame(t, statuses1[0].Poolers[0], statuses2[0].Poolers[0])
	assert.Equal(t, statuses1[0].Poolers[0].Hostname, statuses2[0].Poolers[0].Hostname)
}

func TestPoolerDiscovery_InvalidDataHandling(t *testing.T) {
	ctx := t.Context()
	store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer store.Close()
	logger := slog.Default()

	gd := NewGlobalPoolerDiscovery(ctx, store, "test-cell", logger)

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

	gd.Start()
	defer gd.Stop()

	// Should discover only the valid poolers
	waitForGlobalPoolerCount(t, gd, 2)

	poolers := getAllPoolers(gd)
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

func TestPoolerDiscovery_LastRefresh(t *testing.T) {
	ctx := t.Context()
	store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer store.Close()
	logger := slog.Default()

	gd := NewGlobalPoolerDiscovery(ctx, store, "test-cell", logger)

	// Initially should be zero
	assert.True(t, gd.LastCellRefresh().IsZero())

	// Create a pooler and start discovery
	pooler1 := createTestPooler("pooler1", "test-cell", "host1", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, store.CreateMultiPooler(ctx, pooler1))

	before := time.Now()
	gd.Start()
	defer gd.Stop()

	// Wait for initial discovery
	waitForGlobalPoolerCount(t, gd, 1)
	after := time.Now()

	lastRefresh := gd.LastCellRefresh()
	assert.False(t, lastRefresh.IsZero())
	assert.True(t, lastRefresh.After(before) || lastRefresh.Equal(before))
	assert.True(t, lastRefresh.Before(after) || lastRefresh.Equal(after))
}

func TestPoolerDiscovery_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer store.Close()
	logger := slog.Default()

	gd := NewGlobalPoolerDiscovery(ctx, store, "test-cell", logger)

	// Set up initial poolers
	pooler1 := createTestPooler("pooler1", "test-cell", "host1", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, store.CreateMultiPooler(ctx, pooler1))

	// Start discovery
	gd.Start()

	// Wait for initial discovery
	waitForGlobalPoolerCount(t, gd, 1)

	// Cancel context
	cancel()

	// Stop should complete quickly
	done := make(chan struct{})
	go func() {
		gd.Stop()
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
	ctx := t.Context()
	store, factory := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer store.Close()

	// Wrap store to count watch calls
	countingStore := newCountingTopoStore(store)

	logger := slog.Default()

	// Create initial pooler
	pooler1 := createTestPooler("pooler1", "test-cell", "host1", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, countingStore.CreateMultiPooler(ctx, pooler1))

	// Start discovery with counting store
	gd := NewGlobalPoolerDiscovery(ctx, countingStore, "test-cell", logger)
	gd.Start()
	defer gd.Stop()

	// Verify discovery sees initial pooler.
	// GlobalPoolerDiscovery establishes 2 watches: cells (global) + poolers (test-cell).
	waitForGlobalPoolerCount(t, gd, 1)
	require.Eventually(t, func() bool {
		return countingStore.WatchCallCount() >= 2
	}, testTimeout, testPollInterval,
		"Initial watches should be established (cells + poolers)")

	poolers := getAllPoolers(gd)
	require.Len(t, poolers, 1)
	assert.Equal(t, "pooler1", poolers[0].Id.Name)

	// Simulate watch channel closure (like etcd compaction)
	// This closes all watch channels for the "poolers" path in test-cell
	factory.CloseWatches("test-cell", "poolers")

	// Wait for discovery to detect closure and reconnect (one additional watch call)
	require.Eventually(t, func() bool {
		return countingStore.WatchCallCount() >= 3
	}, testTimeout, testPollInterval,
		"Discovery should reconnect after watch closure")

	// Add a new pooler to verify the reconnected watch is working
	pooler2 := createTestPooler("pooler2", "test-cell", "host2", "db2", "shard2", clustermetadatapb.PoolerType_REPLICA)
	require.NoError(t, countingStore.CreateMultiPooler(ctx, pooler2))

	// Discovery should see the new pooler through the reconnected watch
	waitForCondition(t, func() bool {
		return gd.PoolerCount() == 2
	}, "Discovery should see 2 poolers after reconnection")

	// Verify both poolers are present
	poolers = getAllPoolers(gd)
	require.Len(t, poolers, 2)
	names := []string{poolers[0].Id.Name, poolers[1].Id.Name}
	assert.Contains(t, names, "pooler1")
	assert.Contains(t, names, "pooler2")
}

func TestGlobalPoolerDiscovery_MultiCell(t *testing.T) {
	ctx := t.Context()
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

// TestGlobalPoolerDiscovery_PoolerAddedAfterStart tests that a pooler created
// after global discovery is already running gets picked up through the full
// path: GlobalPoolerDiscovery → WatchAllPoolersWithRetry → watch event.
func TestGlobalPoolerDiscovery_PoolerAddedAfterStart(t *testing.T) {
	ctx := context.Background()
	store, _ := memorytopo.NewServerAndFactory(ctx, "zone1")
	defer store.Close()

	gd := NewGlobalPoolerDiscovery(ctx, store, "zone1", slog.Default())
	gd.Start()
	defer gd.Stop()

	// Wait for cell discovery with no poolers
	waitForGlobalPoolerCount(t, gd, 0)

	// Add a pooler after discovery is running
	pooler1 := createTestPooler("pooler1", "zone1", "host1", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, store.CreateMultiPooler(ctx, pooler1))

	// Should be picked up by the cell watcher
	waitForGlobalPoolerCount(t, gd, 1)

	// Add a second pooler
	pooler2 := createTestPooler("pooler2", "zone1", "host2", "db2", "shard2", clustermetadatapb.PoolerType_REPLICA)
	require.NoError(t, store.CreateMultiPooler(ctx, pooler2))

	waitForGlobalPoolerCount(t, gd, 2)
}

func TestGlobalPoolerDiscovery_GetCellStatusesForAdmin(t *testing.T) {
	ctx := t.Context()
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
	ctx := t.Context()
	store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer store.Close()
	logger := slog.Default()

	gd := NewGlobalPoolerDiscovery(ctx, store, "test-cell", logger)

	// Create two poolers
	pooler1 := createTestPooler("pooler1", "test-cell", "host1", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	pooler2 := createTestPooler("pooler2", "test-cell", "host2", "db1", "shard1", clustermetadatapb.PoolerType_REPLICA)
	require.NoError(t, store.CreateMultiPooler(ctx, pooler1))
	require.NoError(t, store.CreateMultiPooler(ctx, pooler2))

	gd.Start()
	defer gd.Stop()

	// Wait for initial discovery
	waitForGlobalPoolerCount(t, gd, 2)

	// Delete pooler1
	require.NoError(t, store.UnregisterMultiPooler(ctx, pooler1.Id))

	// Wait for deletion to be processed
	waitForGlobalPoolerCount(t, gd, 1)

	// Verify only pooler2 remains
	poolers := getAllPoolers(gd)
	require.Len(t, poolers, 1)
	assert.Equal(t, "pooler2", poolers[0].Id.Name)
}

// TestPoolerDiscovery_ReplicaDoesNotEvictPrimary tests that adding a REPLICA
// does not evict an existing PRIMARY.
func TestPoolerDiscovery_ReplicaDoesNotEvictPrimary(t *testing.T) {
	ctx := t.Context()
	store, _ := memorytopo.NewServerAndFactory(ctx, "test-cell")
	defer store.Close()
	logger := slog.Default()

	gd := NewGlobalPoolerDiscovery(ctx, store, "test-cell", logger)

	// Create a primary
	primary := createTestPooler("primary", "test-cell", "primary-host", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, store.CreateMultiPooler(ctx, primary))

	gd.Start()
	defer gd.Stop()

	// Wait for initial discovery
	waitForGlobalPoolerCount(t, gd, 1)

	// Add a replica for the same shard
	replica := createTestPooler("replica", "test-cell", "replica-host", "db1", "shard1", clustermetadatapb.PoolerType_REPLICA)
	require.NoError(t, store.CreateMultiPooler(ctx, replica))

	// Wait for replica to be discovered
	waitForGlobalPoolerCount(t, gd, 2)

	// Verify both are present
	poolers := getAllPoolers(gd)
	require.Len(t, poolers, 2)
	names := make(map[string]bool)
	for _, p := range poolers {
		names[p.Id.Name] = true
	}
	assert.True(t, names["primary"], "Primary should still be present")
	assert.True(t, names["replica"], "Replica should be present")
}

// TestGlobalPoolerDiscovery_WatchDiscoversNewCells tests that the watch-based
// cell discovery detects cells added after the gateway has started.
func TestGlobalPoolerDiscovery_WatchDiscoversNewCells(t *testing.T) {
	ctx := context.Background()
	// Start with NO cells — watch returns empty initial set
	store, factory := memorytopo.NewServerAndFactory(ctx)
	defer store.Close()

	gd := NewGlobalPoolerDiscovery(ctx, store, "zone1", slog.Default())
	gd.Start()
	defer gd.Stop()

	// Dynamically add a cell (simulates multiorch registering cells after gateway start)
	require.NoError(t, factory.AddCell(ctx, store, "zone1"))

	// Create a pooler in the new cell
	pooler := createTestPooler("pooler1", "zone1", "host1", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	require.NoError(t, store.CreateMultiPooler(ctx, pooler))

	// Watch should fire an event for the new cell, start a watcher, and discover the pooler
	waitForGlobalPoolerCount(t, gd, 1)
	assert.Equal(t, 1, gd.PoolerCount())
}

// TestGlobalPoolerDiscovery_CellRemovalStopsWatcher tests that when a cell is
// deleted from the topology, its poolers are no longer reported.
func TestGlobalPoolerDiscovery_CellRemovalStopsWatcher(t *testing.T) {
	ctx := context.Background()
	store, _ := memorytopo.NewServerAndFactory(ctx, "zone1", "zone2")
	defer store.Close()

	// Create a pooler in each cell
	pooler1 := createTestPooler("pooler1", "zone1", "host1", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	pooler2 := createTestPooler("pooler2", "zone2", "host2", "db2", "shard2", clustermetadatapb.PoolerType_REPLICA)
	require.NoError(t, store.CreateMultiPooler(ctx, pooler1))
	require.NoError(t, store.CreateMultiPooler(ctx, pooler2))

	gd := NewGlobalPoolerDiscovery(ctx, store, "zone1", slog.Default())
	gd.Start()
	defer gd.Stop()

	// Both cells discovered with one pooler each
	waitForGlobalPoolerCount(t, gd, 2)
	assert.Len(t, gd.GetCellStatusesForAdmin(), 2)

	// Delete zone2
	require.NoError(t, store.DeleteCell(ctx, "zone2", true))

	// zone2's watcher should stop, dropping its pooler
	waitForGlobalPoolerCount(t, gd, 1)

	// Only zone1 should remain
	statuses := gd.GetCellStatusesForAdmin()
	require.Len(t, statuses, 1)
	assert.Equal(t, "zone1", statuses[0].Cell)
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
	ctx := t.Context()
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
	ctx := t.Context()
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
