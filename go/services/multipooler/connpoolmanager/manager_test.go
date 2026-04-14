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

package connpoolmanager

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/fakepgserver"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multipooler/pools/reserved"
	"github.com/multigres/multigres/go/tools/viperutil"
)

// newTestManager creates a Manager configured for testing with the given fake server.
// No password configuration is needed since fakepgserver uses trust authentication,
// which simulates the production behavior of Unix socket trust auth.
func newTestManager(t *testing.T, server *fakepgserver.Server) *Manager {
	t.Helper()

	reg := viperutil.NewRegistry()
	config := NewConfig(reg)

	manager := config.NewManager(slog.Default())
	manager.Open(context.Background(), &ConnectionConfig{
		SocketFile: server.ClientConfig().SocketFile,
		Host:       server.ClientConfig().Host,
		Port:       server.ClientConfig().Port,
		Database:   server.ClientConfig().Database,
	})

	return manager
}

func TestManager_Open(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	manager := newTestManager(t, server)
	defer manager.Close()

	// Verify manager is open and admin pool is created.
	require.NotNil(t, manager.adminPool)
	require.NotNil(t, manager.settingsCache)
	assert.Equal(t, 0, manager.UserPoolCount())
	assert.False(t, manager.IsClosed())
}

func TestManager_Close(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	manager := newTestManager(t, server)

	ctx := context.Background()

	// Get a connection to create a user pool.
	conn, err := manager.GetRegularConn(ctx, "testuser")
	require.NoError(t, err)
	conn.Recycle()

	// Verify user pool exists.
	assert.Equal(t, 1, manager.UserPoolCount())

	// Close the manager.
	manager.Close()

	// Verify closed state.
	assert.True(t, manager.IsClosed())
	assert.Equal(t, 0, manager.UserPoolCount())
	assert.Nil(t, manager.adminPool)
}

func TestManager_Close_Idempotent(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	manager := newTestManager(t, server)

	// Double close should not panic.
	manager.Close()
	manager.Close()

	assert.True(t, manager.IsClosed())
}

func TestManager_GetAdminConn(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	manager := newTestManager(t, server)
	defer manager.Close()

	ctx := context.Background()

	// Get an admin connection.
	conn, err := manager.GetAdminConn(ctx)
	require.NoError(t, err)
	require.NotNil(t, conn)

	// Verify connection is working.
	assert.False(t, conn.Conn.IsClosed())

	conn.Recycle()
}

func TestManager_GetRegularConn(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	manager := newTestManager(t, server)
	defer manager.Close()

	ctx := context.Background()

	// Get a regular connection for a user.
	conn, err := manager.GetRegularConn(ctx, "testuser")
	require.NoError(t, err)
	require.NotNil(t, conn)

	// Verify connection is working.
	assert.False(t, conn.Conn.IsClosed())

	conn.Recycle()

	// Verify user pool was created.
	assert.True(t, manager.HasUserPool("testuser"))
}

func TestManager_GetRegularConn_EmptyUser(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	manager := newTestManager(t, server)
	defer manager.Close()

	ctx := context.Background()

	// Empty user should error.
	_, err := manager.GetRegularConn(ctx, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "user cannot be empty")
}

func TestManager_GetRegularConn_ClosedManager(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	manager := newTestManager(t, server)
	manager.Close()

	ctx := context.Background()

	// Closed manager should error.
	_, err := manager.GetRegularConn(ctx, "testuser")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "manager is closed")
}

func TestManager_GetRegularConnWithSettings(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	// Accept SET and RESET commands.
	server.AddQueryPattern(`SELECT pg_catalog\.set_config\(.+\)`, &sqltypes.Result{})
	server.AddQueryPattern(`RESET .+`, &sqltypes.Result{})

	manager := newTestManager(t, server)
	defer manager.Close()

	ctx := context.Background()

	settings := map[string]string{
		"search_path": "public",
	}

	// Get a connection with settings.
	conn, err := manager.GetRegularConnWithSettings(ctx, settings, "testuser")
	require.NoError(t, err)
	require.NotNil(t, conn)

	// Verify settings were applied.
	assert.NotNil(t, conn.Conn.Settings())

	conn.Recycle()

	// Verify SET was called.
	assert.Greater(t, server.GetPatternCalledNum(`SELECT pg_catalog\.set_config\(.+\)`), 0)
}

func TestManager_NewReservedConn(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	manager := newTestManager(t, server)
	defer manager.Close()

	ctx := context.Background()

	// Create a reserved connection.
	conn, err := manager.NewReservedConn(ctx, nil, "testuser")
	require.NoError(t, err)
	require.NotNil(t, conn)

	// Verify connection has a unique ID.
	assert.Greater(t, conn.ConnID(), int64(0))

	conn.Release(reserved.ReleaseCommit) // ReleaseCommit
}

func TestManager_NewReservedConn_WithSettings(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	// Accept SET commands.
	server.AddQueryPattern(`SELECT pg_catalog\.set_config\(.+\)`, &sqltypes.Result{})

	manager := newTestManager(t, server)
	defer manager.Close()

	ctx := context.Background()

	settings := map[string]string{
		"timezone": "UTC",
	}

	// Create a reserved connection with settings.
	conn, err := manager.NewReservedConn(ctx, settings, "testuser")
	require.NoError(t, err)
	require.NotNil(t, conn)

	conn.Release(reserved.ReleaseCommit)

	// Verify SET was called.
	assert.Greater(t, server.GetPatternCalledNum(`SELECT pg_catalog\.set_config\(.+\)`), 0)
}

func TestManager_GetReservedConn(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	manager := newTestManager(t, server)
	defer manager.Close()

	ctx := context.Background()

	// Create a reserved connection.
	conn, err := manager.NewReservedConn(ctx, nil, "testuser")
	require.NoError(t, err)
	connID := conn.ConnID()

	// Retrieve by ID.
	retrieved, ok := manager.GetReservedConn(connID, "testuser")
	require.True(t, ok)
	assert.Equal(t, connID, retrieved.ConnID())

	conn.Release(reserved.ReleaseCommit)
}

func TestManager_GetReservedConn_NotFound(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	manager := newTestManager(t, server)
	defer manager.Close()

	// Non-existent connection ID.
	_, ok := manager.GetReservedConn(999999, "testuser")
	assert.False(t, ok)
}

func TestManager_GetReservedConn_UserNotFound(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	manager := newTestManager(t, server)
	defer manager.Close()

	// No user pools exist yet.
	_, ok := manager.GetReservedConn(1, "unknownuser")
	assert.False(t, ok)
}

func TestManager_Stats(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	manager := newTestManager(t, server)
	defer manager.Close()

	ctx := context.Background()

	// Initial stats - no user pools.
	stats := manager.Stats()
	assert.NotNil(t, stats.Admin)
	assert.Empty(t, stats.UserPools)

	// Create some user pools.
	conn1, err := manager.GetRegularConn(ctx, "user1")
	require.NoError(t, err)
	conn1.Recycle()

	conn2, err := manager.GetRegularConn(ctx, "user2")
	require.NoError(t, err)
	conn2.Recycle()

	// Verify stats include user pools.
	stats = manager.Stats()
	assert.Len(t, stats.UserPools, 2)
	assert.Contains(t, stats.UserPools, "user1")
	assert.Contains(t, stats.UserPools, "user2")
}

func TestManager_UserPoolReuse(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	manager := newTestManager(t, server)
	defer manager.Close()

	ctx := context.Background()

	// Get connections for the same user multiple times.
	conn1, err := manager.GetRegularConn(ctx, "testuser")
	require.NoError(t, err)
	conn1.Recycle()

	conn2, err := manager.GetRegularConn(ctx, "testuser")
	require.NoError(t, err)
	conn2.Recycle()

	conn3, err := manager.GetRegularConn(ctx, "testuser")
	require.NoError(t, err)
	conn3.Recycle()

	// Only one user pool should exist.
	assert.Equal(t, 1, manager.UserPoolCount())
}

func TestManager_ConcurrentUserPoolCreation(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	manager := newTestManager(t, server)
	defer manager.Close()

	ctx := context.Background()
	numGoroutines := 10

	var wg sync.WaitGroup
	errs := make(chan error, numGoroutines)

	// Concurrently create connections for the same user.
	for range numGoroutines {
		wg.Go(func() {
			conn, err := manager.GetRegularConn(ctx, "concurrent-user")
			if err != nil {
				errs <- err
				return
			}
			conn.Recycle()
		})
	}

	wg.Wait()
	close(errs)

	// Collect any errors.
	for err := range errs {
		t.Errorf("unexpected error: %v", err)
	}

	// Only one user pool should exist.
	assert.Equal(t, 1, manager.UserPoolCount())
}

func TestManager_ConcurrentDifferentUsers(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	manager := newTestManager(t, server)
	defer manager.Close()

	ctx := context.Background()
	numUsers := 5

	var wg sync.WaitGroup

	// Concurrently create connections for different users.
	for i := range numUsers {
		wg.Add(1)
		go func(userNum int) {
			defer wg.Done()
			user := "user" + string(rune('A'+userNum))
			conn, err := manager.GetRegularConn(ctx, user)
			if err != nil {
				t.Errorf("unexpected error for %s: %v", user, err)
				return
			}
			conn.Recycle()
		}(i)
	}

	wg.Wait()

	// All user pools should exist.
	assert.Equal(t, numUsers, manager.UserPoolCount())
}

func TestManager_SettingsCacheIntegration(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	// Accept SET commands.
	server.AddQueryPattern(`SELECT pg_catalog\.set_config\(.+\)`, &sqltypes.Result{})

	manager := newTestManager(t, server)
	defer manager.Close()

	ctx := context.Background()

	settings := map[string]string{
		"search_path": "public",
	}

	// Get connections with the same settings multiple times.
	conn1, err := manager.GetRegularConnWithSettings(ctx, settings, "user1")
	require.NoError(t, err)
	settings1 := conn1.Conn.Settings()
	conn1.Recycle()

	conn2, err := manager.GetRegularConnWithSettings(ctx, settings, "user2")
	require.NoError(t, err)
	settings2 := conn2.Conn.Settings()
	conn2.Recycle()

	// Settings should be the same pointer (from cache).
	assert.Same(t, settings1, settings2)
}

// --- ApplySettingsToConn tests ---

func TestManager_ApplySettingsToConn(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	// Accept SET commands.
	server.AddQueryPattern(`SELECT pg_catalog\.set_config\(.+\)`, &sqltypes.Result{})

	manager := newTestManager(t, server)
	defer manager.Close()

	ctx := context.Background()

	// Create a reserved connection with initial settings.
	initialSettings := map[string]string{"search_path": "public"}
	conn, err := manager.NewReservedConn(ctx, initialSettings, "testuser")
	require.NoError(t, err)
	defer conn.Release(reserved.ReleaseCommit)

	// Record how many SET calls have been made so far.
	setsBefore := server.GetPatternCalledNum(`SELECT pg_catalog\.set_config\(.+\)`)

	// Apply different settings — should trigger SET commands.
	newSettings := map[string]string{"search_path": "public", "statement_timeout": "200ms"}
	err = manager.ApplySettingsToConn(ctx, conn.Conn(), newSettings)
	require.NoError(t, err)

	// Verify SET was called again.
	setsAfter := server.GetPatternCalledNum(`SELECT pg_catalog\.set_config\(.+\)`)
	assert.Greater(t, setsAfter, setsBefore, "SET should have been called for new settings")
}

func TestManager_ApplySettingsToConn_SameSettings(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	// Accept SET commands.
	server.AddQueryPattern(`SELECT pg_catalog\.set_config\(.+\)`, &sqltypes.Result{})

	manager := newTestManager(t, server)
	defer manager.Close()

	ctx := context.Background()

	settings := map[string]string{"search_path": "public"}
	conn, err := manager.NewReservedConn(ctx, settings, "testuser")
	require.NoError(t, err)
	defer conn.Release(reserved.ReleaseCommit)

	// Record SET calls.
	setsBefore := server.GetPatternCalledNum(`SELECT pg_catalog\.set_config\(.+\)`)

	// Apply the same settings — should be a no-op (pointer equality via cache).
	err = manager.ApplySettingsToConn(ctx, conn.Conn(), settings)
	require.NoError(t, err)

	setsAfter := server.GetPatternCalledNum(`SELECT pg_catalog\.set_config\(.+\)`)
	assert.Equal(t, setsBefore, setsAfter, "no SET should have been called for same settings")
}

func TestManager_ApplySettingsToConn_NilSettings(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	manager := newTestManager(t, server)
	defer manager.Close()

	ctx := context.Background()

	conn, err := manager.NewReservedConn(ctx, nil, "testuser")
	require.NoError(t, err)
	defer conn.Release(reserved.ReleaseCommit)

	// Apply nil settings — should be a no-op.
	err = manager.ApplySettingsToConn(ctx, conn.Conn(), nil)
	require.NoError(t, err)
}

func TestManager_ApplySettingsToConn_RemovedSettings(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	// Accept SET, individual RESET, and combined RESET+SET commands.
	server.AddQueryPattern(`SELECT pg_catalog\.set_config\(.+\)`, &sqltypes.Result{})
	server.AddQueryPattern(`RESET search_path`, &sqltypes.Result{})
	server.AddQueryPattern(`RESET search_path; SELECT pg_catalog\.set_config\(.+\)`, &sqltypes.Result{})

	manager := newTestManager(t, server)
	defer manager.Close()

	ctx := context.Background()

	// Create a reserved connection with two settings.
	initialSettings := map[string]string{"search_path": "public", "work_mem": "256MB"}
	conn, err := manager.NewReservedConn(ctx, initialSettings, "testuser")
	require.NoError(t, err)
	defer conn.Release(reserved.ReleaseCommit)

	// Record calls so far.
	combinedBefore := server.GetPatternCalledNum(`RESET search_path; SELECT pg_catalog\.set_config\(.+\)`)

	// Apply new settings with search_path removed.
	newSettings := map[string]string{"work_mem": "256MB"}
	err = manager.ApplySettingsToConn(ctx, conn.Conn(), newSettings)
	require.NoError(t, err)

	// Verify combined RESET+SET was called for the removed setting.
	combinedAfter := server.GetPatternCalledNum(`RESET search_path; SELECT pg_catalog\.set_config\(.+\)`)
	assert.Greater(t, combinedAfter, combinedBefore, "combined RESET+SET should have been called for removed setting")
}

// BenchmarkManager_GetUserPool_HotPath benchmarks the lock-free hot path
// for getting an existing user's pool. This is the most common operation
// and should be very fast (atomic load + map lookup).
func BenchmarkManager_GetUserPool_HotPath(b *testing.B) {
	server := fakepgserver.New(b)
	defer server.Close()
	server.SetNeverFail(true)

	reg := viperutil.NewRegistry()
	config := NewConfig(reg)
	manager := config.NewManager(slog.Default())

	ctx := context.Background()
	manager.Open(ctx, &ConnectionConfig{
		Host:     "127.0.0.1",
		Port:     server.ClientConfig().Port,
		Database: "testdb",
	})
	defer manager.Close()

	// Create the user pool first (cold path)
	conn, err := manager.GetRegularConn(ctx, "benchuser")
	if err != nil {
		b.Fatalf("failed to create user pool: %v", err)
	}
	conn.Recycle()

	// Benchmark the hot path (existing user)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// This should only do: atomic load + map lookup
			if !manager.HasUserPool("benchuser") {
				b.Fatal("user pool should exist")
			}
		}
	})
}

// BenchmarkManager_GetRegularConn_ExistingUser benchmarks getting a connection
// for an existing user. This includes the hot path pool lookup plus connection
// acquisition from the pool.
func BenchmarkManager_GetRegularConn_ExistingUser(b *testing.B) {
	server := fakepgserver.New(b)
	defer server.Close()
	server.SetNeverFail(true)

	reg := viperutil.NewRegistry()
	config := NewConfig(reg)
	manager := config.NewManager(slog.Default())

	ctx := context.Background()
	manager.Open(ctx, &ConnectionConfig{
		Host:     "127.0.0.1",
		Port:     server.ClientConfig().Port,
		Database: "testdb",
	})
	defer manager.Close()

	// Create the user pool first
	conn, err := manager.GetRegularConn(ctx, "benchuser")
	if err != nil {
		b.Fatalf("failed to create user pool: %v", err)
	}
	conn.Recycle()

	// Benchmark getting connections for existing user
	b.ResetTimer()
	for b.Loop() {
		conn, err := manager.GetRegularConn(ctx, "benchuser")
		if err != nil {
			b.Fatalf("failed to get connection: %v", err)
		}
		conn.Recycle()
	}
}
