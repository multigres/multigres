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
	"fmt"
	"log/slog"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/fakepgserver"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multipooler/pools/connpool"
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

// TestWithReopenRetry_RetriesOnReopen covers the race in which
// reopenConnections (triggered by MonitorPostgres after a postgres restart)
// closes the user pool while a connection acquisition is already in-flight.
// The helper must notice the generation bump and retry against the fresh pool
// rather than surfacing the transient ErrPoolClosed to the caller.
func TestWithReopenRetry_RetriesOnReopen(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	manager := newTestManager(t, server)
	defer manager.Close()

	calls := 0
	result, err := withReopenRetry(manager, "testuser", nil, nil, func(_ *UserPool) (int, error) {
		calls++
		if calls == 1 {
			// Simulate reopenConnections running mid-flight: the pool we
			// were handed has been closed, but a new one has already been
			// installed with a bumped generation.
			manager.generation.Add(1)
			return 0, connpool.ErrPoolClosed
		}
		return 42, nil
	})

	require.NoError(t, err)
	assert.Equal(t, 42, result)
	assert.Equal(t, 2, calls, "should have retried once after the reopen")
}

// TestWithReopenRetry_SurfacesGenuineClose covers the non-reopen case: the
// manager is actually shutting down, so ErrPoolClosed must be returned to the
// caller instead of being retried into an infinite loop.
func TestWithReopenRetry_SurfacesGenuineClose(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	manager := newTestManager(t, server)
	defer manager.Close()

	calls := 0
	_, err := withReopenRetry(manager, "testuser", nil, nil, func(_ *UserPool) (int, error) {
		calls++
		// No generation bump — manager hasn't been reopened.
		return 0, connpool.ErrPoolClosed
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, connpool.ErrPoolClosed)
	assert.Equal(t, 1, calls, "should not retry when generation did not advance")
}

// TestWithReopenRetry_OnlyRetriesOnce ensures the helper stops after one retry
// even if the retry also fails with ErrPoolClosed (e.g. if a second reopen
// races with the retry). Without a cap this could loop forever.
func TestWithReopenRetry_OnlyRetriesOnce(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	manager := newTestManager(t, server)
	defer manager.Close()

	calls := 0
	_, err := withReopenRetry(manager, "testuser", nil, nil, func(_ *UserPool) (int, error) {
		calls++
		manager.generation.Add(1)
		return 0, connpool.ErrPoolClosed
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, connpool.ErrPoolClosed)
	assert.Equal(t, 2, calls, "should retry exactly once, not loop")
}

// TestWithReopenRetry_EvictsStalePoolOnAuthError verifies the stale-key
// self-heal path: when op fails with a class-28 SQLSTATE (SCRAM auth
// failure against stale cached keys), withReopenRetry must evict the
// user pool and retry once against a freshly-built replacement so the
// session's known-current keys get a chance against pg_authid.
func TestWithReopenRetry_EvictsStalePoolOnAuthError(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	manager := newTestManager(t, server)
	defer manager.Close()

	authErr := &mterrors.PgDiagnostic{MessageType: 'E', Severity: "FATAL", Code: "28P01", Message: "password authentication failed"}

	var seenPools []*UserPool
	calls := 0
	result, err := withReopenRetry(manager, "testuser", nil, nil, func(pool *UserPool) (int, error) {
		calls++
		seenPools = append(seenPools, pool)
		if calls == 1 {
			return 0, fmt.Errorf("dial failed: %w", authErr)
		}
		return 42, nil
	})

	require.NoError(t, err)
	assert.Equal(t, 42, result)
	assert.Equal(t, 2, calls, "should retry exactly once after auth failure")
	require.Len(t, seenPools, 2)
	assert.NotSame(t, seenPools[0], seenPools[1], "retry must run against a freshly-built pool, not the evicted one")

	snap := manager.userPoolsSnapshot.Load()
	require.NotNil(t, snap, "snapshot must be loaded after retry")
	assert.True(t, manager.HasUserPool("testuser"), "user pool must be present in fresh snapshot")
	assert.NotSame(t, seenPools[0], (*snap)["testuser"], "the original stale pool must no longer be in the snapshot")
}

// TestWithReopenRetry_SurfacesPersistentAuthError ensures that if the retry
// also fails with an auth error — e.g. because the retrier itself is holding
// old-password keys — we surface the clean 28xxx error and stop retrying.
// Without a retry cap this would loop forever.
func TestWithReopenRetry_SurfacesPersistentAuthError(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	manager := newTestManager(t, server)
	defer manager.Close()

	authErr := &mterrors.PgDiagnostic{MessageType: 'E', Severity: "FATAL", Code: "28P01", Message: "password authentication failed"}

	calls := 0
	_, err := withReopenRetry(manager, "testuser", nil, nil, func(_ *UserPool) (int, error) {
		calls++
		return 0, fmt.Errorf("dial failed: %w", authErr)
	})

	require.Error(t, err)
	assert.True(t, mterrors.IsAuthenticationError(err), "auth error must propagate with class-28 SQLSTATE intact")
	assert.Equal(t, 2, calls, "should retry exactly once, not loop")
}

// TestEvictUserPool_RemovesFromSnapshotAndCloses verifies the direct
// eviction primitive: removes the pool from the snapshot, closes it, and
// returns true. A subsequent acquire must build a fresh pool.
func TestEvictUserPool_RemovesFromSnapshotAndCloses(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	manager := newTestManager(t, server)
	defer manager.Close()

	ctx := context.Background()

	// Warm up: create a user pool.
	stale, err := manager.getOrCreateUserPool("testuser", nil, nil)
	require.NoError(t, err)
	require.True(t, manager.HasUserPool("testuser"))

	evicted := manager.evictUserPool("testuser", stale)
	assert.True(t, evicted, "eviction of the current pool should return true")
	assert.False(t, manager.HasUserPool("testuser"), "pool must be removed from snapshot")

	// Subsequent acquire must produce a different pool instance.
	fresh, err := manager.getOrCreateUserPool("testuser", nil, nil)
	require.NoError(t, err)
	assert.NotSame(t, stale, fresh, "post-eviction acquire must build a fresh pool")

	// Real acquisition must still work against the fresh pool.
	conn, err := manager.GetRegularConn(ctx, "testuser")
	require.NoError(t, err)
	conn.Recycle()
}

// TestEvictUserPool_NoOpOnRacingEviction verifies that evicting a stale
// reference that no longer matches the snapshot (because a concurrent call
// already swapped it) is a no-op and returns false — preventing callers
// from clobbering a fresh pool built by a racing goroutine.
func TestEvictUserPool_NoOpOnRacingEviction(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	manager := newTestManager(t, server)
	defer manager.Close()

	stale, err := manager.getOrCreateUserPool("testuser", nil, nil)
	require.NoError(t, err)

	// Racing goroutine already evicted + recreated.
	require.True(t, manager.evictUserPool("testuser", stale))
	fresh, err := manager.getOrCreateUserPool("testuser", nil, nil)
	require.NoError(t, err)
	require.NotSame(t, stale, fresh)

	// Late caller still holding the stale reference must not clobber fresh.
	assert.False(t, manager.evictUserPool("testuser", stale), "stale reference must not evict the fresh pool")
	assert.True(t, manager.HasUserPool("testuser"), "fresh pool must remain in the snapshot")
}

// TestManager_NewReservedConn_SurvivesReopen exercises the end-to-end path:
// close+reopen the manager between calls and verify subsequent reserved-conn
// acquisition still works. This mirrors the production crash-recovery path
// where reopenConnections swaps the pools after postgres auto-restart.
func TestManager_NewReservedConn_SurvivesReopen(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	manager := newTestManager(t, server)
	defer manager.Close()

	ctx := context.Background()

	// Warm up: create a user pool.
	c1, err := manager.NewReservedConn(ctx, nil, "testuser")
	require.NoError(t, err)
	c1.Release(reserved.ReleaseCommit)

	// Simulate reopenConnections: close then reopen against the same fake server.
	manager.Close()
	manager.Open(ctx, &ConnectionConfig{
		SocketFile: server.ClientConfig().SocketFile,
		Host:       server.ClientConfig().Host,
		Port:       server.ClientConfig().Port,
		Database:   server.ClientConfig().Database,
	})

	// Reserved-conn acquisition must succeed against the fresh pool.
	c2, err := manager.NewReservedConn(ctx, nil, "testuser")
	require.NoError(t, err)
	require.NotNil(t, c2)
	c2.Release(reserved.ReleaseCommit)
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

// --- SCRAM passthrough ---

// TestBuildUserClientConfig_KeysApplied verifies that when both keys are
// supplied, the resulting client.Config carries them through to the dial.
func TestBuildUserClientConfig_KeysApplied(t *testing.T) {
	reg := viperutil.NewRegistry()
	config := NewConfig(reg)
	manager := config.NewManager(slog.Default())
	manager.Open(context.Background(), &ConnectionConfig{Database: "db"})
	defer manager.Close()

	ck, sk := bytes32(1), bytes32(2)
	cfg := manager.buildUserClientConfig("alice", ck, sk)

	assert.Equal(t, ck, cfg.ScramClientKey)
	assert.Equal(t, sk, cfg.ScramServerKey)
	assert.Empty(t, cfg.Password)
	assert.Equal(t, "alice", cfg.User)
}

// TestBuildUserClientConfig_NilKeys_FallsBack ensures that an
// authenticated-but-keyless session (not expected in practice, but possible
// during rollout) does not crash and falls back to the empty-password path.
// An empty-password dial only succeeds against the pg_hba admin-user trust
// exception; any other user will fail authentication at PostgreSQL.
func TestBuildUserClientConfig_NilKeys_FallsBack(t *testing.T) {
	reg := viperutil.NewRegistry()
	config := NewConfig(reg)
	manager := config.NewManager(slog.Default())
	manager.Open(context.Background(), &ConnectionConfig{Database: "db"})
	defer manager.Close()

	cfg := manager.buildUserClientConfig("alice", nil, nil)

	assert.Nil(t, cfg.ScramClientKey)
	assert.Nil(t, cfg.ScramServerKey)
	assert.Empty(t, cfg.Password)
}

// bytes32 returns a deterministic 32-byte slice for key testing.
func bytes32(seed byte) []byte {
	out := make([]byte, 32)
	for i := range out {
		out[i] = seed
	}
	return out
}
