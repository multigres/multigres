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
	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// mockConn implements topo.Conn for testing
type mockConn struct {
	cell            string
	watchChanges    chan *topo.WatchDataRecursive
	initialPoolers  []*topo.WatchDataRecursive
	closed          bool
	mu              sync.Mutex
	watchStarted    bool
	shouldFailWatch bool
}

func newMockConn(cell string) *mockConn {
	return &mockConn{
		cell:         cell,
		watchChanges: make(chan *topo.WatchDataRecursive, 10),
	}
}

func (m *mockConn) setInitialPoolers(poolers []*topo.WatchDataRecursive) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.initialPoolers = poolers
}

func (m *mockConn) sendChange(change *topo.WatchDataRecursive) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed && m.watchStarted {
		m.watchChanges <- change
	}
}

func (m *mockConn) setShouldFailWatch(fail bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.shouldFailWatch = fail
}

func (m *mockConn) closeWatch() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		m.closed = true
		close(m.watchChanges)
	}
}

func (m *mockConn) WatchRecursive(ctx context.Context, path string) ([]*topo.WatchDataRecursive, <-chan *topo.WatchDataRecursive, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.shouldFailWatch {
		return nil, nil, topo.TopoError{
			Code:    topo.NoNode,
			Message: "watch failed",
		}
	}

	m.watchStarted = true
	return m.initialPoolers, m.watchChanges, nil
}

func (m *mockConn) Close() error {
	m.closeWatch()
	return nil
}

// Implement other required methods as no-ops
func (m *mockConn) ListDir(ctx context.Context, dirPath string, full bool) ([]topo.DirEntry, error) {
	return nil, nil
}

func (m *mockConn) Create(ctx context.Context, filePath string, contents []byte) (topo.Version, error) {
	return nil, nil
}

func (m *mockConn) Update(ctx context.Context, filePath string, contents []byte, version topo.Version) (topo.Version, error) {
	return nil, nil
}

func (m *mockConn) Get(ctx context.Context, filePath string) ([]byte, topo.Version, error) {
	return nil, nil, nil
}

func (m *mockConn) GetVersion(ctx context.Context, filePath string, version int64) ([]byte, error) {
	return nil, nil
}

func (m *mockConn) List(ctx context.Context, filePathPrefix string) ([]topo.KVInfo, error) {
	return nil, nil
}

func (m *mockConn) Delete(ctx context.Context, filePath string, version topo.Version) error {
	return nil
}

func (m *mockConn) Lock(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	return nil, nil
}

func (m *mockConn) LockWithTTL(ctx context.Context, dirPath, contents string, ttl time.Duration) (topo.LockDescriptor, error) {
	return nil, nil
}

func (m *mockConn) LockName(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	return nil, nil
}

func (m *mockConn) TryLock(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	return nil, nil
}

func (m *mockConn) Watch(ctx context.Context, filePath string) (current *topo.WatchData, changes <-chan *topo.WatchData, err error) {
	return nil, nil, nil
}

// mockStore implements topo.Store for testing
type mockStore struct {
	cell string
	conn *mockConn
	mu   sync.Mutex
}

func newMockStore(cell string) *mockStore {
	return &mockStore{
		cell: cell,
		conn: newMockConn(cell),
	}
}

func (s *mockStore) ConnForCell(ctx context.Context, cell string) (topo.Conn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn, nil
}

func (s *mockStore) Close() error {
	return s.conn.Close()
}

// Implement required GlobalStore methods as no-ops
func (s *mockStore) GetCellNames(ctx context.Context) ([]string, error) {
	return []string{s.cell}, nil
}

func (s *mockStore) GetCell(ctx context.Context, cell string) (*clustermetadatapb.Cell, error) {
	return nil, nil
}

func (s *mockStore) CreateCell(ctx context.Context, cell string, ci *clustermetadatapb.Cell) error {
	return nil
}

func (s *mockStore) UpdateCell(ctx context.Context, cell string, ci *clustermetadatapb.Cell) error {
	return nil
}

func (s *mockStore) UpdateCellFields(ctx context.Context, cell string, update func(*clustermetadatapb.Cell) error) error {
	return nil
}

func (s *mockStore) DeleteCell(ctx context.Context, cell string, force bool) error {
	return nil
}

// Implement required CellStore methods as no-ops
func (s *mockStore) GetMultiPooler(ctx context.Context, id *clustermetadatapb.ID) (*topo.MultiPoolerInfo, error) {
	return nil, nil
}

func (s *mockStore) GetMultiPoolerIDsByCell(ctx context.Context, cell string) ([]*clustermetadatapb.ID, error) {
	return nil, nil
}

func (s *mockStore) GetMultiPoolersByCell(ctx context.Context, cellName string, opt *topo.GetMultiPoolersByCellOptions) ([]*topo.MultiPoolerInfo, error) {
	return nil, nil
}

func (s *mockStore) CreateMultiPooler(ctx context.Context, multipooler *clustermetadatapb.MultiPooler) error {
	return nil
}

func (s *mockStore) UpdateMultiPooler(ctx context.Context, mpi *topo.MultiPoolerInfo) error {
	return nil
}

func (s *mockStore) UpdateMultiPoolerFields(ctx context.Context, id *clustermetadatapb.ID, update func(*clustermetadatapb.MultiPooler) error) (*clustermetadatapb.MultiPooler, error) {
	return nil, nil
}

func (s *mockStore) UnregisterMultiPooler(ctx context.Context, id *clustermetadatapb.ID) error {
	return nil
}

func (s *mockStore) RegisterMultiPooler(ctx context.Context, multipooler *clustermetadatapb.MultiPooler, allowUpdate bool) error {
	return nil
}

func (s *mockStore) GetMultiGateway(ctx context.Context, id *clustermetadatapb.ID) (*topo.MultiGatewayInfo, error) {
	return nil, nil
}

func (s *mockStore) GetMultiGatewayIDsByCell(ctx context.Context, cell string) ([]*clustermetadatapb.ID, error) {
	return nil, nil
}

func (s *mockStore) GetMultiGatewaysByCell(ctx context.Context, cellName string) ([]*topo.MultiGatewayInfo, error) {
	return nil, nil
}

func (s *mockStore) CreateMultiGateway(ctx context.Context, multigateway *clustermetadatapb.MultiGateway) error {
	return nil
}

func (s *mockStore) UpdateMultiGateway(ctx context.Context, mgi *topo.MultiGatewayInfo) error {
	return nil
}

func (s *mockStore) UpdateMultiGatewayFields(ctx context.Context, id *clustermetadatapb.ID, update func(*clustermetadatapb.MultiGateway) error) (*clustermetadatapb.MultiGateway, error) {
	return nil, nil
}

func (s *mockStore) UnregisterMultiGateway(ctx context.Context, id *clustermetadatapb.ID) error {
	return nil
}

func (s *mockStore) RegisterMultiGateway(ctx context.Context, multigateway *clustermetadatapb.MultiGateway, allowUpdate bool) error {
	return nil
}

func (s *mockStore) GetMultiOrch(ctx context.Context, id *clustermetadatapb.ID) (*topo.MultiOrchInfo, error) {
	return nil, nil
}

func (s *mockStore) GetMultiOrchIDsByCell(ctx context.Context, cell string) ([]*clustermetadatapb.ID, error) {
	return nil, nil
}

func (s *mockStore) GetMultiOrchsByCell(ctx context.Context, cellName string) ([]*topo.MultiOrchInfo, error) {
	return nil, nil
}

func (s *mockStore) CreateMultiOrch(ctx context.Context, multiorch *clustermetadatapb.MultiOrch) error {
	return nil
}

func (s *mockStore) UpdateMultiOrch(ctx context.Context, moi *topo.MultiOrchInfo) error {
	return nil
}

func (s *mockStore) UpdateMultiOrchFields(ctx context.Context, id *clustermetadatapb.ID, update func(*clustermetadatapb.MultiOrch) error) (*clustermetadatapb.MultiOrch, error) {
	return nil, nil
}

func (s *mockStore) UnregisterMultiOrch(ctx context.Context, id *clustermetadatapb.ID) error {
	return nil
}

func (s *mockStore) RegisterMultiOrch(ctx context.Context, multiorch *clustermetadatapb.MultiOrch, allowUpdate bool) error {
	return nil
}

func (s *mockStore) GetDatabase(ctx context.Context, dbName string) (*clustermetadatapb.Database, error) {
	return nil, nil
}

func (s *mockStore) GetDatabaseNames(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (s *mockStore) CreateDatabase(ctx context.Context, database string, db *clustermetadatapb.Database) error {
	return nil
}

func (s *mockStore) UpdateDatabase(ctx context.Context, database string, update func(*clustermetadatapb.Database) error) (*clustermetadatapb.Database, error) {
	return nil, nil
}

func (s *mockStore) UpdateDatabaseFields(ctx context.Context, database string, update func(*clustermetadatapb.Database) error) error {
	return nil
}

func (s *mockStore) DeleteDatabase(ctx context.Context, dbName string, force bool) error {
	return nil
}

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
func waitForCondition(t *testing.T, condition func() bool, msgAndArgs ...interface{}) {
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

// Helper function to create WatchDataRecursive for a pooler
func createPoolerWatchData(pooler *clustermetadatapb.MultiPooler) *topo.WatchDataRecursive {
	data, _ := proto.Marshal(pooler)
	poolerID := topo.MultiPoolerIDString(pooler.Id)
	return &topo.WatchDataRecursive{
		Path: "poolers/" + poolerID + "/Pooler",
		WatchData: topo.WatchData{
			Contents: data,
		},
	}
}

// Integration tests - these use the public API (Start/Stop/GetPoolers)

func TestNewPoolerDiscovery(t *testing.T) {
	ctx := context.Background()
	store := newMockStore("test-cell")
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
	store := newMockStore("test-cell")
	logger := slog.Default()

	pd := NewPoolerDiscovery(ctx, store, "test-cell", logger)

	// Set up initial poolers
	pooler1 := createTestPooler("pooler1", "test-cell", "host1", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	store.conn.setInitialPoolers([]*topo.WatchDataRecursive{createPoolerWatchData(pooler1)})

	// Start discovery
	pd.Start()

	// Wait for initial discovery to complete
	waitForPoolerCount(t, pd, 1)

	// Send a change
	pooler2 := createTestPooler("pooler2", "test-cell", "host2", "db2", "shard2", clustermetadatapb.PoolerType_REPLICA)
	store.conn.sendChange(createPoolerWatchData(pooler2))

	// Wait for change to be processed
	waitForPoolerCount(t, pd, 2)

	// Stop discovery
	pd.Stop()

	// Verify discovery stopped cleanly
	assert.Equal(t, 2, pd.PoolerCount())
}

func TestPoolerDiscovery_WatchChannelClosed(t *testing.T) {
	t.Skip("Skipping known bug: pooler discovery doesn't reconnect after a network error")

	ctx := context.Background()
	store := newMockStore("test-cell")
	logger := slog.Default()

	pd := NewPoolerDiscovery(ctx, store, "test-cell", logger)

	// Set up initial poolers
	pooler1 := createTestPooler("pooler1", "test-cell", "host1", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	store.conn.setInitialPoolers([]*topo.WatchDataRecursive{createPoolerWatchData(pooler1)})

	// Start discovery
	pd.Start()

	// Wait for initial discovery
	waitForPoolerCount(t, pd, 1)

	assert.Equal(t, 1, pd.PoolerCount(), "Should have initial pooler")
	initialPoolers := pd.GetPoolers()
	assert.Equal(t, "host1", initialPoolers[0].Hostname)

	// Close the watch channel to simulate connection loss
	store.conn.closeWatch()
	store.conn.setShouldFailWatch(true)

	// Wait a bit for the watch to notice the closure
	time.Sleep(100 * time.Millisecond)

	// Old values should still be cached
	assert.Equal(t, 1, pd.PoolerCount(), "Should still have cached pooler after connection loss")
	cachedPoolers := pd.GetPoolers()
	assert.Equal(t, "host1", cachedPoolers[0].Hostname, "Cached values should remain")

	// Now set up new poolers for after reconnection (update existing + add new)
	pooler1Updated := createTestPooler("pooler1", "test-cell", "host1-reconnected", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	pooler2 := createTestPooler("pooler2", "test-cell", "host2", "db2", "shard2", clustermetadatapb.PoolerType_REPLICA)

	// Create a new mock connection with the updated poolers
	newConn := newMockConn("test-cell")
	newConn.setInitialPoolers([]*topo.WatchDataRecursive{
		createPoolerWatchData(pooler1Updated),
		createPoolerWatchData(pooler2),
	})

	// Replace the connection so the next reconnection attempt gets new data
	store.mu.Lock()
	store.conn = newConn
	store.mu.Unlock()

	// Wait for reconnection and new data to be picked up
	waitForPoolerCount(t, pd, 2)

	// Verify the new values are picked up
	reconnectedPoolers := pd.GetPoolers()
	assert.Len(t, reconnectedPoolers, 2, "Should have both poolers after reconnection")

	poolerMap := make(map[string]*clustermetadatapb.MultiPooler)
	for _, p := range reconnectedPoolers {
		poolerMap[p.Id.Name] = p
	}

	assert.Contains(t, poolerMap, "pooler1", "Should have pooler1")
	assert.Equal(t, "host1-reconnected", poolerMap["pooler1"].Hostname, "Pooler1 should be updated")

	assert.Contains(t, poolerMap, "pooler2", "Should have pooler2")
	assert.Equal(t, "host2", poolerMap["pooler2"].Hostname, "Pooler2 should be present")

	// Stop should complete cleanly
	pd.Stop()
}

func TestPoolerDiscovery_MultiplePoolerUpdates(t *testing.T) {
	ctx := context.Background()
	store := newMockStore("test-cell")
	logger := slog.Default()

	pd := NewPoolerDiscovery(ctx, store, "test-cell", logger)

	// Start with two initial poolers
	pooler1 := createTestPooler("pooler1", "test-cell", "host1", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	pooler2 := createTestPooler("pooler2", "test-cell", "host2", "db2", "shard2", clustermetadatapb.PoolerType_REPLICA)
	store.conn.setInitialPoolers([]*topo.WatchDataRecursive{
		createPoolerWatchData(pooler1),
		createPoolerWatchData(pooler2),
	})

	pd.Start()
	defer pd.Stop()

	// Wait for initial discovery
	waitForPoolerCount(t, pd, 2)

	// Update an existing pooler
	pooler1Updated := createTestPooler("pooler1", "test-cell", "host1-updated", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	store.conn.sendChange(createPoolerWatchData(pooler1Updated))

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
	store := newMockStore("test-cell")
	logger := slog.Default()

	pd := NewPoolerDiscovery(ctx, store, "test-cell", logger)

	// Start with no poolers
	store.conn.setInitialPoolers([]*topo.WatchDataRecursive{})

	pd.Start()
	defer pd.Stop()

	// Give it a moment to process (should remain at 0)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 0, pd.PoolerCount())

	// Now add a pooler via watch
	pooler1 := createTestPooler("pooler1", "test-cell", "host1", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	store.conn.sendChange(createPoolerWatchData(pooler1))

	waitForPoolerCount(t, pd, 1)
}

func TestPoolerDiscovery_VerifyPoolerDetails(t *testing.T) {
	ctx := context.Background()
	store := newMockStore("test-cell")
	logger := slog.Default()

	pd := NewPoolerDiscovery(ctx, store, "test-cell", logger)

	// Create poolers with specific details
	pooler1 := createTestPooler("pooler1", "test-cell", "primary.example.com", "mydb", "shard-01", clustermetadatapb.PoolerType_PRIMARY)
	pooler2 := createTestPooler("pooler2", "test-cell", "replica.example.com", "mydb", "shard-02", clustermetadatapb.PoolerType_REPLICA)

	store.conn.setInitialPoolers([]*topo.WatchDataRecursive{
		createPoolerWatchData(pooler1),
		createPoolerWatchData(pooler2),
	})

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
	store := newMockStore("test-cell")
	logger := slog.Default()

	pd := NewPoolerDiscovery(ctx, store, "test-cell", logger)

	pooler1 := createTestPooler("pooler1", "test-cell", "host1", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	store.conn.setInitialPoolers([]*topo.WatchDataRecursive{createPoolerWatchData(pooler1)})

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
	store := newMockStore("test-cell")
	logger := slog.Default()

	pd := NewPoolerDiscovery(ctx, store, "test-cell", logger)

	// Set up initial data with a mix of valid and invalid poolers
	pooler1 := createTestPooler("pooler1", "test-cell", "host1", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	pooler2 := createTestPooler("pooler2", "test-cell", "host2", "db2", "shard2", clustermetadatapb.PoolerType_REPLICA)

	store.conn.setInitialPoolers([]*topo.WatchDataRecursive{
		createPoolerWatchData(pooler1),
		// Invalid data (not a pooler path) - should be silently ignored
		{
			Path: "poolers/something/else",
			WatchData: topo.WatchData{
				Contents: []byte("invalid"),
			},
		},
		createPoolerWatchData(pooler2),
		// Corrupted protobuf data - should be logged and ignored
		{
			Path: "poolers/pooler3-test-cell-pooler3/Pooler",
			WatchData: topo.WatchData{
				Contents: []byte("corrupted data"),
			},
		},
	})

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

func TestPoolerDiscovery_ProcessInitialPoolers_WithErrors(t *testing.T) {
	ctx := context.Background()
	store := newMockStore("test-cell")
	logger := slog.Default()

	pd := NewPoolerDiscovery(ctx, store, "test-cell", logger)

	// Create mix of valid and invalid watch data
	pooler1 := createTestPooler("pooler1", "test-cell", "host1", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)

	initial := []*topo.WatchDataRecursive{
		createPoolerWatchData(pooler1),
		// Invalid data (not a pooler path)
		{
			Path: "poolers/something/else",
			WatchData: topo.WatchData{
				Contents: []byte("invalid"),
			},
		},
		// Corrupted protobuf data
		{
			Path: "poolers/pooler2-test-cell-pooler2/Pooler",
			WatchData: topo.WatchData{
				Contents: []byte("corrupted data"),
			},
		},
	}

	pd.processInitialPoolers(initial)

	// Only valid pooler should be added
	assert.Equal(t, 1, pd.PoolerCount())

	poolers := pd.GetPoolers()
	assert.Len(t, poolers, 1)
	assert.Equal(t, "host1", poolers[0].Hostname)
}

func TestPoolerDiscovery_LastRefresh(t *testing.T) {
	ctx := context.Background()
	store := newMockStore("test-cell")
	logger := slog.Default()

	pd := NewPoolerDiscovery(ctx, store, "test-cell", logger)

	// Initially should be zero
	assert.True(t, pd.LastRefresh().IsZero())

	// After processing initial poolers
	pooler1 := createTestPooler("pooler1", "test-cell", "host1", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	before := time.Now()
	pd.processInitialPoolers([]*topo.WatchDataRecursive{createPoolerWatchData(pooler1)})
	after := time.Now()

	lastRefresh := pd.LastRefresh()
	assert.False(t, lastRefresh.IsZero())
	assert.True(t, lastRefresh.After(before) || lastRefresh.Equal(before))
	assert.True(t, lastRefresh.Before(after) || lastRefresh.Equal(after))
}

func TestPoolerDiscovery_ParsePoolerFromWatchData(t *testing.T) {
	ctx := context.Background()
	store := newMockStore("test-cell")
	logger := slog.Default()

	pd := NewPoolerDiscovery(ctx, store, "test-cell", logger)

	tests := []struct {
		name        string
		watchData   *topo.WatchDataRecursive
		expectNil   bool
		expectError bool
	}{
		{
			name: "valid pooler",
			watchData: createPoolerWatchData(
				createTestPooler("pooler1", "test-cell", "host1", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY),
			),
			expectNil:   false,
			expectError: false,
		},
		{
			name: "non-pooler path",
			watchData: &topo.WatchDataRecursive{
				Path: "poolers/something/else",
				WatchData: topo.WatchData{
					Contents: []byte("data"),
				},
			},
			expectNil:   true,
			expectError: false,
		},
		{
			name: "nil contents",
			watchData: &topo.WatchDataRecursive{
				Path: "poolers/pooler1-test-cell-pooler1/Pooler",
				WatchData: topo.WatchData{
					Contents: nil,
				},
			},
			expectNil:   true,
			expectError: false,
		},
		{
			name: "corrupted protobuf",
			watchData: &topo.WatchDataRecursive{
				Path: "poolers/pooler1-test-cell-pooler1/Pooler",
				WatchData: topo.WatchData{
					Contents: []byte("invalid protobuf data"),
				},
			},
			expectNil:   false,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pooler, err := pd.parsePoolerFromWatchData(tt.watchData)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			if tt.expectNil {
				assert.Nil(t, pooler)
			} else if !tt.expectError {
				assert.NotNil(t, pooler)
			}
		})
	}
}

func TestPoolerDiscovery_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := newMockStore("test-cell")
	logger := slog.Default()

	pd := NewPoolerDiscovery(ctx, store, "test-cell", logger)

	// Set up initial poolers
	pooler1 := createTestPooler("pooler1", "test-cell", "host1", "db1", "shard1", clustermetadatapb.PoolerType_PRIMARY)
	store.conn.setInitialPoolers([]*topo.WatchDataRecursive{createPoolerWatchData(pooler1)})

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
