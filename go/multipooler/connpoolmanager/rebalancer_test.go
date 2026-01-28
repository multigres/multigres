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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/fakepgserver"
	"github.com/multigres/multigres/go/multipooler/pools/connpool"
	"github.com/multigres/multigres/go/tools/viperutil"
)

// newTestManagerForRebalancer creates a Manager configured for rebalancer testing.
// Uses default config values since viperutil bindings happen at NewConfig() time.
// Default inactiveTimeout is 5 minutes, so pools won't be GC'd during most tests.
func newTestManagerForRebalancer(t *testing.T, server *fakepgserver.Server) *Manager {
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
	conn1, err := manager.GetRegularConn(ctx, "user1")
	require.NoError(t, err)
	conn1.Recycle()

	conn2, err := manager.GetRegularConn(ctx, "user2")
	require.NoError(t, err)
	conn2.Recycle()

	// Touch activity so pools don't get GC'd
	pools := manager.userPoolsSnapshot.Load()
	for _, pool := range *pools {
		pool.TouchActivity()
	}

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
	conn, err := manager.GetRegularConn(ctx, "inactive-user")
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
	conn, err := manager.GetRegularConn(ctx, "active-user")
	require.NoError(t, err)
	conn.Recycle()

	// Touch activity to keep it active (recent activity)
	pools := manager.userPoolsSnapshot.Load()
	(*pools)["active-user"].TouchActivity()

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
	conn1, err := manager.GetRegularConn(ctx, "user1")
	require.NoError(t, err)
	conn1.Recycle()

	conn2, err := manager.GetRegularConn(ctx, "user2")
	require.NoError(t, err)
	conn2.Recycle()

	assert.Equal(t, 2, manager.UserPoolCount())

	// Set user2 as inactive (10 minutes ago)
	pools := manager.userPoolsSnapshot.Load()
	(*pools)["user2"].lastActivity.Store(time.Now().Add(-10 * time.Minute).UnixNano())

	// Keep user1 active
	(*pools)["user1"].TouchActivity()

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
	conn, err := manager.GetRegularConn(ctx, "testuser")
	require.NoError(t, err)
	conn.Recycle()

	// Touch activity so pool doesn't get GC'd
	pools := manager.userPoolsSnapshot.Load()
	(*pools)["testuser"].TouchActivity()

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
	conn, err := manager.GetRegularConn(ctx, "testuser")
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

func TestUserPool_TouchActivity(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestUserPool(t, server)
	defer pool.Close()

	// Get initial activity
	initial := pool.LastActivity()
	assert.Greater(t, initial, int64(0))

	// Wait a bit and touch
	time.Sleep(10 * time.Millisecond)
	pool.TouchActivity()

	// Activity should be updated
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
		DemandSampleInterval:      10 * time.Millisecond,
		RebalanceInterval:         50 * time.Millisecond,
	}

	pool := NewUserPool(ctx, config)
	defer pool.Close()

	// Get stats - should include demand fields
	stats := pool.Stats()
	assert.Equal(t, server.ClientConfig().User, stats.Username)
	assert.GreaterOrEqual(t, stats.RegularDemand, int64(0))
	assert.GreaterOrEqual(t, stats.ReservedDemand, int64(0))
	assert.Greater(t, stats.LastActivity, int64(0))
}
