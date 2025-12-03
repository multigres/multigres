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

package topoclient

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/tools/viperutil"
)

func TestRegisterFactory(t *testing.T) {
	// Save and restore the original factories map
	originalFactories := factories
	defer func() { factories = originalFactories }()

	factories = make(map[string]Factory)

	factory := newMockFactory()
	RegisterFactory("test", factory)

	registered := factories["test"]
	assert.Equal(t, factory, registered, "Registered factory should match")
}

func TestGetAvailableImplementations(t *testing.T) {
	// Save and restore the original factories map
	originalFactories := factories
	defer func() { factories = originalFactories }()

	factories = make(map[string]Factory)

	// Register multiple factories
	RegisterFactory("etcd", newMockFactory())
	RegisterFactory("memory", newMockFactory())
	RegisterFactory("alpha", newMockFactory())

	implementations := GetAvailableImplementations()

	// Should be sorted alphabetically
	expected := []string{"alpha", "etcd", "memory"}
	assert.Equal(t, expected, implementations, "Implementations should be sorted")
}

func TestNewWithFactory(t *testing.T) {
	factory := newMockFactory()
	root := "/test/root"
	serverAddrs := []string{"localhost:2181", "localhost:2182"}

	ts := NewWithFactory(factory, root, serverAddrs)
	require.NotNil(t, ts, "Store should not be nil")

	// Verify factory was called for global topology
	assert.GreaterOrEqual(t, factory.getCreateCount(), int32(1), "Factory should be called at least once")

	// Clean up
	ts.Close()
}

func TestOpenServer(t *testing.T) {
	// Save and restore the original factories map
	originalFactories := factories
	defer func() { factories = originalFactories }()

	factories = make(map[string]Factory)

	t.Run("Success with registered implementation", func(t *testing.T) {
		factory := newMockFactory()
		RegisterFactory("test-impl", factory)

		ts, err := OpenServer("test-impl", "/test", []string{"localhost:2181"})
		require.NoError(t, err, "OpenServer should succeed")
		require.NotNil(t, ts, "Store should not be nil")

		ts.Close()
	})

	t.Run("Error with unregistered implementation", func(t *testing.T) {
		ts, err := OpenServer("nonexistent", "/test", []string{"localhost:2181"})
		assert.Error(t, err, "OpenServer should fail with unregistered implementation")
		assert.Nil(t, ts, "Store should be nil")
		assert.Contains(t, err.Error(), "not found", "Error should mention not found")
	})

	t.Run("Error with no registered implementations", func(t *testing.T) {
		factories = make(map[string]Factory)

		ts, err := OpenServer("anything", "/test", []string{"localhost:2181"})
		assert.Error(t, err, "OpenServer should fail with no implementations")
		assert.Nil(t, ts, "Store should be nil")
		assert.Contains(t, err.Error(), "no topology implementations registered", "Error should mention no implementations")
	})
}

func TestConnForCell_GlobalCell(t *testing.T) {
	factory := newMockFactory()
	ts := NewWithFactory(factory, "/test", []string{"localhost:2181"})
	defer ts.Close()

	ctx := context.Background()

	// Global cell should return the global connection
	conn, err := ts.ConnForCell(ctx, GlobalCell)
	require.NoError(t, err, "ConnForCell should succeed for global cell")
	require.NotNil(t, conn, "Connection should not be nil")

	// Should not create additional connections
	initialCount := factory.getCreateCount()
	conn2, err := ts.ConnForCell(ctx, GlobalCell)
	require.NoError(t, err)
	assert.Equal(t, conn, conn2, "Should return same global connection")
	assert.Equal(t, initialCount, factory.getCreateCount(), "Should not create new connections")
}

func TestConnForCell_NewCell(t *testing.T) {
	factory := newMockFactory()
	ts := NewWithFactory(factory, "/test", []string{"localhost:2181"})
	require.NotNil(t, ts)
	defer ts.Close()

	ctx := context.Background()

	// Create a cell in the global topology first
	cellInfo := &clustermetadatapb.Cell{
		ServerAddresses: []string{"cell1:2181"},
		Root:            "/cell1",
	}
	err := ts.CreateCell(ctx, "cell1", cellInfo)
	require.NoError(t, err, "CreateCell should succeed")

	initialCount := factory.getCreateCount()

	// Get connection for the cell
	conn, err := ts.ConnForCell(ctx, "cell1")
	require.NoError(t, err, "ConnForCell should succeed for new cell")
	require.NotNil(t, conn, "Connection should not be nil")

	// Verify factory was called to create cell connection
	assert.Greater(t, factory.getCreateCount(), initialCount, "Should create new connection for cell")
}

func TestConnForCell_CachedConnection(t *testing.T) {
	factory := newMockFactory()
	ts := NewWithFactory(factory, "/test", []string{"localhost:2181"})
	require.NotNil(t, ts)
	defer ts.Close()

	ctx := context.Background()

	// Create a cell
	cellInfo := &clustermetadatapb.Cell{
		ServerAddresses: []string{"cell1:2181"},
		Root:            "/cell1",
	}
	err := ts.CreateCell(ctx, "cell1", cellInfo)
	require.NoError(t, err)

	// Get connection twice
	conn1, err := ts.ConnForCell(ctx, "cell1")
	require.NoError(t, err)

	initialCallCount := factory.getCreateCount()

	conn2, err := ts.ConnForCell(ctx, "cell1")
	require.NoError(t, err)

	// Should return the same connection (cached)
	assert.Equal(t, conn1, conn2, "Should return cached connection")

	// Should not create new connection
	assert.Equal(t, initialCallCount, factory.getCreateCount(), "Should not create new connection")
}

func TestConnForCell_UpdatedCellConfig(t *testing.T) {
	factory := newMockFactory()
	ts := NewWithFactory(factory, "/test", []string{"localhost:2181"})
	require.NotNil(t, ts)
	defer ts.Close()

	ctx := context.Background()

	// Create a cell
	cellInfo := &clustermetadatapb.Cell{
		ServerAddresses: []string{"cell1:2181"},
		Root:            "/cell1",
	}
	err := ts.CreateCell(ctx, "cell1", cellInfo)
	require.NoError(t, err)

	// Get connection
	conn1, err := ts.ConnForCell(ctx, "cell1")
	require.NoError(t, err)
	initialCallCount := factory.getCreateCount()

	// Update cell configuration
	err = ts.UpdateCellFields(ctx, "cell1", func(c *clustermetadatapb.Cell) error {
		c.ServerAddresses = []string{"cell1-new:2181"}
		c.Root = "/cell1-new"
		return nil
	})
	require.NoError(t, err)

	// Get connection again - should create new one due to config change
	conn2, err := ts.ConnForCell(ctx, "cell1")
	require.NoError(t, err)

	// Should be different connection
	assert.NotEqual(t, conn1, conn2, "Should create new connection after config change")

	// Should have created new connection
	assert.Greater(t, factory.getCreateCount(), initialCallCount, "Should create new connection")
}

func TestConnForCell_NonexistentCell(t *testing.T) {
	factory := newMockFactory()
	ts := NewWithFactory(factory, "/test", []string{"localhost:2181"})
	require.NotNil(t, ts)
	defer ts.Close()

	ctx := context.Background()

	// Try to get connection for nonexistent cell
	conn, err := ts.ConnForCell(ctx, "nonexistent")
	assert.Error(t, err, "ConnForCell should fail for nonexistent cell")
	assert.Nil(t, conn, "Connection should be nil")
}

func TestConnForCell_CanceledContext(t *testing.T) {
	factory := newMockFactory()
	ts := NewWithFactory(factory, "/test", []string{"localhost:2181"})
	require.NotNil(t, ts)
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Should fail with context error
	conn, err := ts.ConnForCell(ctx, "cell1")
	assert.Error(t, err, "ConnForCell should fail with canceled context")
	assert.Nil(t, conn, "Connection should be nil")
	assert.Equal(t, context.Canceled, err, "Error should be context.Canceled")
}

func TestStoreClose(t *testing.T) {
	factory := newMockFactory()
	ts := NewWithFactory(factory, "/test", []string{"localhost:2181"})

	ctx := context.Background()

	// Create some cells
	cellInfo := &clustermetadatapb.Cell{
		ServerAddresses: []string{"cell1:2181"},
		Root:            "/cell1",
	}
	err := ts.CreateCell(ctx, "cell1", cellInfo)
	require.NoError(t, err)

	// Get connections to cache them
	_, err = ts.ConnForCell(ctx, "cell1")
	require.NoError(t, err)

	// Close the store
	err = ts.Close()
	assert.NoError(t, err, "Close should succeed")

	// Verify we can call Close again without error
	err = ts.Close()
	assert.NoError(t, err, "Second Close should not fail")
}

func TestStoreClose_VerifiesConnectionsClosed(t *testing.T) {
	factory := newMockFactory()
	ts := NewWithFactory(factory, "/test", []string{"localhost:2181"})

	ctx := context.Background()

	// Create a cell and get its connection
	cellInfo := &clustermetadatapb.Cell{
		ServerAddresses: []string{"cell1:2181"},
		Root:            "/cell1",
	}
	err := ts.CreateCell(ctx, "cell1", cellInfo)
	require.NoError(t, err)

	conn, err := ts.ConnForCell(ctx, "cell1")
	require.NoError(t, err)

	// Close the store
	err = ts.Close()
	assert.NoError(t, err)

	// Try to use the connection - should fail or be closed
	if wrapperConn, ok := conn.(*WrapperConn); ok {
		_, _, err := wrapperConn.Get(ctx, "/test")
		// After close, we expect an error
		assert.Error(t, err, "Connection should fail after store is closed")
	}
}

func TestCellConnStruct(t *testing.T) {
	// Test that cellConn stores both Cell config and connection
	cellInfo := &clustermetadatapb.Cell{
		ServerAddresses: []string{"test:2181"},
		Root:            "/test",
	}

	conn := newMockConn(1)

	cc := cellConn{
		Cell: cellInfo,
		conn: conn,
	}

	assert.Equal(t, cellInfo, cc.Cell, "Cell should match")
	assert.Equal(t, conn, cc.conn, "Connection should match")
}

func TestConnForCell_ProtoEquality(t *testing.T) {
	factory := newMockFactory()
	ts := NewWithFactory(factory, "/test", []string{"localhost:2181"})
	require.NotNil(t, ts)
	defer ts.Close()

	ctx := context.Background()

	// Create a cell
	cellInfo := &clustermetadatapb.Cell{
		ServerAddresses: []string{"cell1:2181"},
		Root:            "/cell1",
	}
	err := ts.CreateCell(ctx, "cell1", cellInfo)
	require.NoError(t, err)

	// Get connection
	conn1, err := ts.ConnForCell(ctx, "cell1")
	require.NoError(t, err)

	// Update with same values (proto.Equal should return true)
	err = ts.UpdateCellFields(ctx, "cell1", func(c *clustermetadatapb.Cell) error {
		// No changes
		return nil
	})
	require.NoError(t, err)

	initialCallCount := factory.getCreateCount()

	// Get connection again - should return cached connection
	conn2, err := ts.ConnForCell(ctx, "cell1")
	require.NoError(t, err)

	assert.Equal(t, conn1, conn2, "Should return same connection for equal proto")
	assert.Equal(t, initialCallCount, factory.getCreateCount(), "Should not create new connection")
}

func TestConnForCell_ConcurrentAccess(t *testing.T) {
	factory := newMockFactory()
	ts := NewWithFactory(factory, "/test", []string{"localhost:2181"})
	require.NotNil(t, ts)
	defer ts.Close()

	ctx := context.Background()

	// Create a cell
	cellInfo := &clustermetadatapb.Cell{
		ServerAddresses: []string{"cell1:2181"},
		Root:            "/cell1",
	}
	err := ts.CreateCell(ctx, "cell1", cellInfo)
	require.NoError(t, err)

	// Concurrently access the same cell
	const numGoroutines = 10
	var wg sync.WaitGroup
	conns := make([]Conn, numGoroutines)
	errors := make([]error, numGoroutines)

	wg.Add(numGoroutines)
	for i := range numGoroutines {
		go func(idx int) {
			defer wg.Done()
			conns[idx], errors[idx] = ts.ConnForCell(ctx, "cell1")
		}(i)
	}
	wg.Wait()

	// All should succeed
	for i := range numGoroutines {
		assert.NoError(t, errors[i], "Concurrent ConnForCell should succeed")
		assert.NotNil(t, conns[i], "Connection should not be nil")
	}

	// All should get the same connection (or very few if there was a race)
	firstConn := conns[0]
	sameCount := 0
	for i := 1; i < numGoroutines; i++ {
		if conns[i] == firstConn {
			sameCount++
		}
	}
	// Most should be the same (allowing for some race conditions during initialization)
	assert.Equal(t, sameCount, numGoroutines-1, "Connections should be the same cached instance")
}

func TestConstants(t *testing.T) {
	// Verify constants are set correctly
	assert.Equal(t, "global", GlobalCell)
	assert.Equal(t, "Cell", CellFile)
	assert.Equal(t, "Database", DatabaseFile)
	assert.Equal(t, "Gateway", GatewayFile)
	assert.Equal(t, "Pooler", PoolerFile)
	assert.Equal(t, "Orch", OrchFile)
	assert.Equal(t, "databases", DatabasesPath)
	assert.Equal(t, "cells", CellsPath)
	assert.Equal(t, "gateways", GatewaysPath)
	assert.Equal(t, "poolers", PoolersPath)
	assert.Equal(t, "orchs", OrchsPath)
}

func TestOpen_ValidConfiguration(t *testing.T) {
	factories = make(map[string]Factory)
	factory := newMockFactory()
	RegisterFactory("test-impl", factory)
	reg := viperutil.NewRegistry()
	cfg := NewTopoConfig(reg)
	cfg.implementation.Set("test-impl")
	cfg.globalServerAddresses.Set([]string{"localhost:2181"})
	cfg.globalRoot.Set("/test")

	// Call Open - should succeed
	ts := cfg.Open()
	require.NotNil(t, ts, "Store should not be nil")

	// Verify factory was called
	assert.GreaterOrEqual(t, factory.getCreateCount(), int32(1), "Factory should be called")

	// Clean up
	ts.Close()
}

// Note: We cannot easily test the error cases of Open() because they call os.Exit(1)
// Those cases are:
// - len(topoGlobalServerAddresses) == 0
// - topoGlobalRoot == ""
// - topoImplementation == ""
// - OpenServer returns an error
// These would require mocking os.Exit or running in a subprocess, which is beyond
// the scope of unit tests. The logic is tested indirectly through OpenServer tests.

func TestStatus_InitiallyEmpty(t *testing.T) {
	factory := newMockFactory()
	ts := NewWithFactory(factory, "/test", []string{"localhost:2181"})
	require.NotNil(t, ts)
	defer ts.Close()

	status := ts.Status()
	require.NotNil(t, status, "Status should return a map")

	// Global cell should have empty status (no error)
	globalStatus, ok := status[GlobalCell]
	assert.True(t, ok, "Status should contain global cell")
	assert.Empty(t, globalStatus, "Global cell should have empty status initially")
}

func TestStatus_GlobalCellError(t *testing.T) {
	factory := newMockFactory()
	factory.setShouldFail(true)

	ts := NewWithFactory(factory, "/test", []string{"localhost:2181"})
	require.NotNil(t, ts)
	defer ts.Close()

	// Wait for the alarm to be called with error
	require.Eventually(t, func() bool {
		status := ts.Status()
		globalStatus, ok := status[GlobalCell]
		return ok && globalStatus != ""
	}, 100*time.Millisecond, 5*time.Millisecond, "Global cell should have error status")

	status := ts.Status()
	globalStatus := status[GlobalCell]
	assert.NotEmpty(t, globalStatus, "Global cell should have error status")
	assert.Contains(t, globalStatus, "factory error", "Status should contain error message")
}

func TestStatus_GlobalCellRecovery(t *testing.T) {
	factory := newMockFactory()
	factory.setShouldFail(true)

	ts := NewWithFactory(factory, "/test", []string{"localhost:2181"})
	require.NotNil(t, ts)
	defer ts.Close()

	// Wait for error status
	require.Eventually(t, func() bool {
		status := ts.Status()
		globalStatus, ok := status[GlobalCell]
		return ok && globalStatus != ""
	}, 100*time.Millisecond, 5*time.Millisecond, "Global cell should have error status")

	// Allow connection to succeed
	factory.setShouldFail(false)

	// Wait for status to be cleared (empty string)
	require.Eventually(t, func() bool {
		status := ts.Status()
		globalStatus, ok := status[GlobalCell]
		return ok && globalStatus == ""
	}, 2*time.Second, 10*time.Millisecond, "Global cell status should be cleared after recovery")

	status := ts.Status()
	globalStatus := status[GlobalCell]
	assert.Empty(t, globalStatus, "Global cell should have empty status after recovery")
}

func TestStatus_CellConnection(t *testing.T) {
	factory := newMockFactory()
	ts := NewWithFactory(factory, "/test", []string{"localhost:2181"})
	require.NotNil(t, ts)
	defer ts.Close()

	ctx := context.Background()

	// Create a cell
	cellInfo := &clustermetadatapb.Cell{
		ServerAddresses: []string{"cell1:2181"},
		Root:            "/cell1",
	}
	err := ts.CreateCell(ctx, "cell1", cellInfo)
	require.NoError(t, err)

	// Get connection for the cell
	_, err = ts.ConnForCell(ctx, "cell1")
	require.NoError(t, err)

	// Check status includes the cell
	status := ts.Status()
	cell1Status, ok := status["cell1"]
	assert.True(t, ok, "Status should contain cell1")
	assert.Empty(t, cell1Status, "Cell1 should have empty status (no error)")
}

func TestStatus_MultipleCells(t *testing.T) {
	factory := newMockFactory()
	ts := NewWithFactory(factory, "/test", []string{"localhost:2181"})
	require.NotNil(t, ts)
	defer ts.Close()

	ctx := context.Background()

	// Create multiple cells
	for i := 1; i < 4; i++ {
		cellName := fmt.Sprintf("cell%d", i)
		cellInfo := &clustermetadatapb.Cell{
			ServerAddresses: []string{fmt.Sprintf("%s:2181", cellName)},
			Root:            fmt.Sprintf("/%s", cellName),
		}
		err := ts.CreateCell(ctx, cellName, cellInfo)
		require.NoError(t, err)

		_, err = ts.ConnForCell(ctx, cellName)
		require.NoError(t, err)
	}

	// Check status includes all cells
	status := ts.Status()
	assert.Len(t, status, 4, "Status should contain global + 3 cells")

	for i := 1; i < 4; i++ {
		cellName := fmt.Sprintf("cell%d", i)
		cellStatus, ok := status[cellName]
		assert.True(t, ok, "Status should contain %s", cellName)
		assert.Empty(t, cellStatus, "%s should have empty status", cellName)
	}
}

func TestStatus_CellConnectionError(t *testing.T) {
	// Create a factory that fails for specific cell
	factory := &mockFactoryWithSelectiveFailure{
		failForCell: "cell1",
	}

	ts := NewWithFactory(factory, "/test", []string{"localhost:2181"})
	require.NotNil(t, ts)
	defer ts.Close()

	ctx := context.Background()

	// Create a cell
	cellInfo := &clustermetadatapb.Cell{
		ServerAddresses: []string{"cell1:2181"},
		Root:            "/cell1",
	}
	err := ts.CreateCell(ctx, "cell1", cellInfo)
	require.NoError(t, err)

	// Try to connect - will fail and trigger retry
	_, err = ts.ConnForCell(ctx, "cell1")
	require.NoError(t, err) // ConnForCell doesn't fail, just starts retry

	// Wait for error status to be set
	require.Eventually(t, func() bool {
		status := ts.Status()
		cell1Status, ok := status["cell1"]
		return ok && cell1Status != ""
	}, 100*time.Millisecond, 5*time.Millisecond, "Cell1 should have error status")

	status := ts.Status()
	cell1Status := status["cell1"]
	assert.NotEmpty(t, cell1Status, "Cell1 should have error status")
	assert.Contains(t, cell1Status, "factory error", "Status should contain error message")
}

func TestStatus_ReturnsCopy(t *testing.T) {
	factory := newMockFactory()
	ts := NewWithFactory(factory, "/test", []string{"localhost:2181"})
	require.NotNil(t, ts)
	defer ts.Close()

	ctx := context.Background()

	// Get initial status
	status1 := ts.Status()

	// Create a cell
	cellInfo := &clustermetadatapb.Cell{
		ServerAddresses: []string{"cell1:2181"},
		Root:            "/cell1",
	}
	err := ts.CreateCell(ctx, "cell1", cellInfo)
	require.NoError(t, err)

	_, err = ts.ConnForCell(ctx, "cell1")
	require.NoError(t, err)

	// Get new status
	status2 := ts.Status()

	// Verify status1 wasn't modified (it's a copy)
	assert.Len(t, status1, 1, "Original status should only have global cell")
	assert.Len(t, status2, 2, "New status should have global + cell1")

	// Modifying the returned map shouldn't affect the internal state
	status2["fake-cell"] = "fake error"

	status3 := ts.Status()
	_, hasFake := status3["fake-cell"]
	assert.False(t, hasFake, "Internal status should not be affected by external modifications")
}
