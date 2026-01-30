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

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/fakepgserver"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/multipooler/pools/reserved"
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
	require.NotNil(t, manager.userPools)
	assert.False(t, manager.closed)
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
	assert.Len(t, manager.userPools, 1)

	// Close the manager.
	manager.Close()

	// Verify closed state.
	assert.True(t, manager.closed)
	assert.Nil(t, manager.userPools)
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

	assert.True(t, manager.closed)
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
	manager.mu.Lock()
	_, ok := manager.userPools["testuser"]
	manager.mu.Unlock()
	assert.True(t, ok)
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
	server.AddQueryPattern(`SET SESSION .+ = .+`, &sqltypes.Result{})
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
	assert.Greater(t, server.GetPatternCalledNum(`SET SESSION .+ = .+`), 0)
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
	assert.Greater(t, conn.ConnID, int64(0))

	conn.Release(reserved.ReleaseCommit) // ReleaseCommit
}

func TestManager_NewReservedConn_WithSettings(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	// Accept SET commands.
	server.AddQueryPattern(`SET SESSION .+ = .+`, &sqltypes.Result{})

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
	assert.Greater(t, server.GetPatternCalledNum(`SET SESSION .+ = .+`), 0)
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
	connID := conn.ConnID

	// Retrieve by ID.
	retrieved, ok := manager.GetReservedConn(connID, "testuser")
	require.True(t, ok)
	assert.Equal(t, connID, retrieved.ConnID)

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
	manager.mu.Lock()
	numPools := len(manager.userPools)
	manager.mu.Unlock()
	assert.Equal(t, 1, numPools)
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
	manager.mu.Lock()
	numPools := len(manager.userPools)
	manager.mu.Unlock()
	assert.Equal(t, 1, numPools)
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
	manager.mu.Lock()
	numPools := len(manager.userPools)
	manager.mu.Unlock()
	assert.Equal(t, numUsers, numPools)
}

func TestManager_SettingsCacheIntegration(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	// Accept SET commands.
	server.AddQueryPattern(`SET SESSION .+ = .+`, &sqltypes.Result{})

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

// --- InternalUser tests ---

func TestManager_InternalUser_Default(t *testing.T) {
	reg := viperutil.NewRegistry()
	config := NewConfig(reg)
	manager := config.NewManager(slog.Default())

	// Manager should return the default internal user from config
	assert.Equal(t, "postgres", manager.InternalUser())
}

func TestManager_InternalUser_CustomValue(t *testing.T) {
	reg := viperutil.NewRegistry()
	config := NewConfig(reg)

	// Register flags
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	config.RegisterFlags(fs)

	// Parse a custom value
	err := fs.Parse([]string{"--connpool-internal-user", "replication_user"})
	require.NoError(t, err)

	// Bind to viper (this is what happens in the real application)
	v := viper.New()
	err = v.BindPFlags(fs)
	require.NoError(t, err)

	manager := config.NewManager(slog.Default())

	// Manager should delegate to config and return the configured value
	// The viperutil binding picks up the flag value correctly
	assert.Equal(t, "replication_user", manager.InternalUser(),
		"Manager should return the custom internal user set via flag")
}

func TestManager_InternalUser_DelegatestoConfig(t *testing.T) {
	// Create a config with default values
	reg := viperutil.NewRegistry()
	config := NewConfig(reg)

	// Create manager from config
	manager := config.NewManager(slog.Default())

	// Verify Manager.InternalUser() delegates to Config.InternalUser()
	assert.Equal(t, config.InternalUser(), manager.InternalUser(),
		"Manager.InternalUser() should return the same value as Config.InternalUser()")
}
