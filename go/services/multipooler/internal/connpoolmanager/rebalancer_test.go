// Copyright 2026 Supabase, Inc.
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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/fakepgserver"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/connpool"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/regular"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/reserved"
	"github.com/multigres/multigres/go/tools/viperutil"
)

// newTestManagerForRebalancer creates a Manager configured for rebalancer testing.
// Uses default config values since viperutil bindings happen at NewConfig() time.
// Default inactiveTimeout is 5 minutes, so pools won't be GC'd during most tests.
func newTestManagerForRebalancer(t *testing.T, server *fakepgserver.Server) *Manager {
	t.Helper()

	reg := viperutil.NewRegistry()
	config := NewConfig(reg)
	resolveTestPgPassword(t, config)

	manager := config.NewManager(slog.Default())
	manager.Open(context.Background(), &ConnectionConfig{
		SocketFile: server.ClientConfig().SocketFile,
		Host:       server.ClientConfig().Host,
		Port:       server.ClientConfig().Port,
		Database:   server.ClientConfig().Database,
	})

	return manager
}

func TestManager_RebalancerStartsAndStops(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	manager := newTestManagerForRebalancer(t, server)

	// Verify allocators were created
	require.NotNil(t, manager.regularAllocator)
	require.NotNil(t, manager.reservedAllocator)

	// Close should stop the rebalancer cleanly
	manager.Close()

	// Verify closed state
	assert.True(t, manager.IsClosed())
}

func TestManager_RebalanceWithNoUsers(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	manager := newTestManagerForRebalancer(t, server)
	defer manager.Close()

	// Manually trigger a rebalance with no users - should not panic
	manager.rebalance(context.Background())

	assert.Equal(t, 0, manager.UserPoolCount())
}

func TestManager_RebalanceWithUsers(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	manager := newTestManagerForRebalancer(t, server)
	defer manager.Close()

	ctx := context.Background()

	// Create some user pools
	conn1, err := manager.GetRegularConn(ctx, "user1", nil, nil)
	require.NoError(t, err)
	conn1.Recycle()

	conn2, err := manager.GetRegularConn(ctx, "user2", nil, nil)
	require.NoError(t, err)
	conn2.Recycle()

	// Manually trigger a rebalance
	manager.rebalance(ctx)

	// Verify both users still have pools
	assert.Equal(t, 2, manager.UserPoolCount())
	assert.True(t, manager.HasUserPool("user1"))
	assert.True(t, manager.HasUserPool("user2"))
}

func TestManager_GarbageCollectInactivePools_ManualTest(t *testing.T) {
	// This test manually manipulates lastActivity to test GC logic
	// without relying on timing.
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	manager := newTestManagerForRebalancer(t, server)
	defer manager.Close()

	ctx := context.Background()

	// Create a user pool
	conn, err := manager.GetRegularConn(ctx, "inactive-user", nil, nil)
	require.NoError(t, err)
	conn.Recycle()

	assert.Equal(t, 1, manager.UserPoolCount())

	// Manually set lastActivity to a time far in the past
	// Default inactive timeout is 5 minutes
	pools := manager.userPoolsSnapshot.Load()
	pool := (*pools)["inactive-user"]
	// Set activity to 10 minutes ago
	pool.lastActivity.Store(time.Now().Add(-10 * time.Minute).UnixNano())

	// Trigger garbage collection
	manager.garbageCollectInactivePools(ctx)

	// Pool should have been removed
	assert.Equal(t, 0, manager.UserPoolCount())
	assert.False(t, manager.HasUserPool("inactive-user"))
}

func TestManager_GarbageCollectPreservesActivePool(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	manager := newTestManagerForRebalancer(t, server)
	defer manager.Close()

	ctx := context.Background()

	// Create a user pool
	conn, err := manager.GetRegularConn(ctx, "active-user", nil, nil)
	require.NoError(t, err)
	conn.Recycle()

	// Trigger garbage collection
	manager.garbageCollectInactivePools(ctx)

	// Pool should still exist (activity is recent)
	assert.Equal(t, 1, manager.UserPoolCount())
	assert.True(t, manager.HasUserPool("active-user"))
}

func TestManager_GarbageCollectMixedPools(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	manager := newTestManagerForRebalancer(t, server)
	defer manager.Close()

	ctx := context.Background()

	// Create two user pools
	conn1, err := manager.GetRegularConn(ctx, "user1", nil, nil)
	require.NoError(t, err)
	conn1.Recycle()

	conn2, err := manager.GetRegularConn(ctx, "user2", nil, nil)
	require.NoError(t, err)
	conn2.Recycle()

	assert.Equal(t, 2, manager.UserPoolCount())

	// Set user2 as inactive (10 minutes ago)
	pools := manager.userPoolsSnapshot.Load()
	(*pools)["user2"].lastActivity.Store(time.Now().Add(-10 * time.Minute).UnixNano())

	// Keep user1 active
	(*pools)["user1"].lastActivity.Store(time.Now().UnixNano())

	// Trigger garbage collection
	manager.garbageCollectInactivePools(ctx)

	// Only user1 should remain
	assert.Equal(t, 1, manager.UserPoolCount())
	assert.True(t, manager.HasUserPool("user1"))
	assert.False(t, manager.HasUserPool("user2"))
}

func TestManager_RebalancerLoop(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	manager := newTestManagerForRebalancer(t, server)
	defer manager.Close()

	ctx := context.Background()

	// Create a user pool
	conn, err := manager.GetRegularConn(ctx, "testuser", nil, nil)
	require.NoError(t, err)
	conn.Recycle()

	// Wait for at least one rebalance cycle (default is 10s, but we'll just wait briefly)
	// The rebalancer runs in the background; we just want to verify it doesn't crash.
	time.Sleep(50 * time.Millisecond)

	// Pool should still exist (rebalancer ran without errors)
	assert.Equal(t, 1, manager.UserPoolCount())
}

func TestManager_AllocatorCapacities(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	manager := newTestManagerForRebalancer(t, server)
	defer manager.Close()

	// Default: globalCapacity=100, reservedRatio=0.2
	// Regular: 80% of 100 = 80
	// Reserved: 20% of 100 = 20
	assert.Equal(t, int64(80), manager.regularAllocator.Capacity())
	assert.Equal(t, int64(20), manager.reservedAllocator.Capacity())
}

func TestManager_DemandTrackersCreated(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	manager := newTestManagerForRebalancer(t, server)
	defer manager.Close()

	ctx := context.Background()

	// Create a user pool
	conn, err := manager.GetRegularConn(ctx, "testuser", nil, nil)
	require.NoError(t, err)
	conn.Recycle()

	// Get the pool and verify stats include demand fields
	pools := manager.userPoolsSnapshot.Load()
	pool := (*pools)["testuser"]
	require.NotNil(t, pool)

	// Demand trackers should be created since default config has valid durations
	stats := pool.Stats()
	assert.Equal(t, "testuser", stats.Username)
	// LastActivity should be set
	assert.Greater(t, stats.LastActivity, int64(0))
}

func TestUserPool_GetConnUpdatesActivity(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestUserPool(t, server)
	defer pool.Close()

	// Get initial activity
	initial := pool.LastActivity()
	assert.Greater(t, initial, int64(0))

	// Wait a bit and get a connection (which should touch activity)
	time.Sleep(10 * time.Millisecond)
	conn, err := pool.GetRegularConn(context.Background())
	require.NoError(t, err)
	conn.Recycle()

	// Activity should be updated by GetRegularConn
	updated := pool.LastActivity()
	assert.Greater(t, updated, initial)
}

func TestUserPool_StatsIncludesDemand(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	ctx := context.Background()
	config := &UserPoolConfig{
		ClientConfig:              server.ClientConfig(),
		AdminPool:                 nil,
		RegularPoolConfig:         &connpool.Config{Capacity: 4, MaxIdleCount: 4},
		ReservedPoolConfig:        &connpool.Config{Capacity: 4, MaxIdleCount: 4},
		ReservedInactivityTimeout: 5 * time.Second,
		DemandWindow:              100 * time.Millisecond,
		RebalanceInterval:         50 * time.Millisecond,
	}

	pool, err := NewUserPool(ctx, config)
	require.NoError(t, err)
	defer pool.Close()

	// Get stats - should include demand fields
	stats := pool.Stats()
	assert.Equal(t, server.ClientConfig().User, stats.Username)
	assert.GreaterOrEqual(t, stats.RegularDemand, int64(0))
	assert.GreaterOrEqual(t, stats.ReservedDemand, int64(0))
	assert.Greater(t, stats.LastActivity, int64(0))
}

// A pool holding a checked-out reserved conn (e.g. the pubsub LISTEN conn,
// which never touches lastActivity) must survive GC until the conn is released.
func TestManager_GarbageCollectSkipsPoolWithHeldReservedConn(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	manager := newTestManagerForRebalancer(t, server)
	defer manager.Close()

	ctx := context.Background()

	rc, err := manager.NewReservedConn(ctx, nil, "listener", nil, nil)
	require.NoError(t, err)
	rc.SetInactivityTimeout(0)

	pool := (*manager.userPoolsSnapshot.Load())["listener"]
	pool.lastActivity.Store(time.Now().Add(-10 * time.Minute).UnixNano())

	manager.garbageCollectInactivePools(ctx)
	assert.True(t, manager.HasUserPool("listener"), "pool with held reserved conn must not be GC'd")
	assert.False(t, rc.IsClosed(), "held reserved conn must not be closed by GC")

	rc.Release(reserved.ReleaseRollback, nil)
	pool.lastActivity.Store(time.Now().Add(-10 * time.Minute).UnixNano())

	manager.garbageCollectInactivePools(ctx)
	assert.False(t, manager.HasUserPool("listener"), "idle pool must be GC'd once the conn is released")
}

// A pool with a borrowed regular conn (statement in flight) must survive GC:
// closing it would block the rebalancer on PoolCloseTimeout waiting for drain.
func TestManager_GarbageCollectSkipsPoolWithBorrowedRegularConn(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	manager := newTestManagerForRebalancer(t, server)
	defer manager.Close()

	ctx := context.Background()

	conn, err := manager.GetRegularConn(ctx, "busy", nil, nil)
	require.NoError(t, err)

	pool := (*manager.userPoolsSnapshot.Load())["busy"]
	pool.lastActivity.Store(time.Now().Add(-10 * time.Minute).UnixNano())

	manager.garbageCollectInactivePools(ctx)
	assert.True(t, manager.HasUserPool("busy"))

	conn.Recycle()
	pool.lastActivity.Store(time.Now().Add(-10 * time.Minute).UnixNano())

	manager.garbageCollectInactivePools(ctx)
	assert.False(t, manager.HasUserPool("busy"))
}

// A reserved conn holds a borrowed slot from before validation until release.
// GC must (a) count that borrowed-but-unregistered slot as checked out and
// (b) never block behind, or race, an acquisition in flight.
func TestManager_GarbageCollectSkipsPoolDuringReservedSetup(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	manager := newTestManagerForRebalancer(t, server)
	defer manager.Close()

	ctx := context.Background()

	// Prime the pool so the validate hook can reach it through the snapshot.
	c, err := manager.GetRegularConn(ctx, "setup", nil, nil)
	require.NoError(t, err)
	c.Recycle()
	pool := (*manager.userPoolsSnapshot.Load())["setup"]

	backdate := func() { pool.lastActivity.Store(time.Now().Add(-10 * time.Minute).UnixNano()) }

	// validate runs after the underlying borrow and before registration in
	// reserved.Pool.active — exactly the setup window the GC used to miss.
	validated := false
	validate := func(context.Context, *regular.Conn) error {
		validated = true
		assert.True(t, pool.HasCheckedOutConns(), "borrowed-but-unregistered reserved conn must count as checked out")
		backdate()

		done := make(chan struct{})
		go func() {
			manager.garbageCollectInactivePools(ctx)
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("GC blocked behind an in-flight acquisition")
		}
		assert.True(t, manager.HasUserPool("setup"), "GC must skip a pool with an acquisition in flight")
		assert.False(t, pool.IsClosing())
		return nil
	}
	rc, err := manager.NewReservedConn(ctx, nil, "setup", nil, nil, reserved.WithValidate(validate))
	require.NoError(t, err)
	require.True(t, validated)
	assert.False(t, rc.IsClosed(), "conn acquired during GC must stay open")

	// Once released and idle again, the pool is collected as usual.
	rc.Release(reserved.ReleaseRollback, nil)
	backdate()
	manager.garbageCollectInactivePools(ctx)
	assert.False(t, manager.HasUserPool("setup"))
	assert.True(t, pool.IsClosing())

	// A subsequent acquisition builds a fresh pool rather than hitting the old one.
	c2, err := manager.GetRegularConn(ctx, "setup", nil, nil)
	require.NoError(t, err)
	c2.Recycle()
	assert.True(t, manager.HasUserPool("setup"))
	assert.NotSame(t, pool, (*manager.userPoolsSnapshot.Load())["setup"])
}

func TestUserPool_TryMarkInactive(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	manager := newTestManagerForRebalancer(t, server)
	defer manager.Close()

	ctx := context.Background()
	future := time.Now().Add(time.Hour).UnixNano() // every pool is "stale" against this cutoff

	c, err := manager.GetRegularConn(ctx, "u", nil, nil)
	require.NoError(t, err)
	pool := (*manager.userPoolsSnapshot.Load())["u"]

	assert.False(t, pool.tryMarkInactive(future), "must refuse while a conn is borrowed")
	c.Recycle()
	assert.False(t, pool.tryMarkInactive(time.Now().Add(-time.Hour).UnixNano()), "must refuse when recently active")

	// An acquisition in flight holds the read lock; GC must give up, not wait.
	pool.acqMu.RLock()
	assert.False(t, pool.tryMarkInactive(future), "must refuse while an acquisition is in flight")
	pool.acqMu.RUnlock()

	require.True(t, pool.tryMarkInactive(future), "idle pool must be claimable")
	assert.True(t, pool.IsClosing())
	assert.False(t, pool.tryMarkInactive(future), "a claimed pool must not be claimed twice")

	// Every acquire path refuses a claimed pool so the caller re-looks-up.
	_, err = pool.GetRegularConn(ctx)
	assert.ErrorIs(t, err, connpool.ErrPoolClosed)
	_, err = pool.GetRegularConnWithSettings(ctx, nil)
	assert.ErrorIs(t, err, connpool.ErrPoolClosed)
	_, err = pool.NewReservedConn(ctx, nil)
	assert.ErrorIs(t, err, connpool.ErrPoolClosed)
	_, err = pool.NewLogicalReplicationConn(ctx)
	assert.ErrorIs(t, err, connpool.ErrPoolClosed)
}
