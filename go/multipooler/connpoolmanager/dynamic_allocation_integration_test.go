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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/fakepgserver"
	"github.com/multigres/multigres/go/multipooler/pools/regular"
	"github.com/multigres/multigres/go/tools/viperutil"
)

// DynamicAllocationTestConfig holds test-specific configuration overrides.
type DynamicAllocationTestConfig struct {
	GlobalCapacity     int64
	ReservedRatio      float64
	RebalanceInterval  time.Duration
	DemandWindow       time.Duration // Sliding window for peak demand tracking
	InactiveTimeout    time.Duration
	MinCapacityPerUser int64 // 0 means use default (10), set to 1 for strict demand-based allocation
}

// newTestManagerWithConfig creates a Manager with custom configuration for testing.
func newTestManagerWithConfig(t *testing.T, server *fakepgserver.Server, testCfg *DynamicAllocationTestConfig) *Manager {
	t.Helper()

	reg := viperutil.NewRegistry()
	config := NewConfig(reg)

	// Apply custom configuration using Value.Set()
	if testCfg != nil {
		if testCfg.GlobalCapacity > 0 {
			config.globalCapacity.Set(testCfg.GlobalCapacity)
		}
		if testCfg.ReservedRatio > 0 {
			config.reservedRatio.Set(testCfg.ReservedRatio)
		}
		if testCfg.RebalanceInterval > 0 {
			config.rebalanceInterval.Set(testCfg.RebalanceInterval)
		}
		if testCfg.DemandWindow > 0 {
			config.demandWindow.Set(testCfg.DemandWindow)
		}
		if testCfg.InactiveTimeout > 0 {
			config.inactiveTimeout.Set(testCfg.InactiveTimeout)
		}
		// Use 1 for tests that need strict demand-based allocation (the original behavior).
		// Otherwise minCapacityPerUser defaults to 10 which ensures burst capacity for light users.
		if testCfg.MinCapacityPerUser > 0 {
			config.minCapacityPerUser.Set(testCfg.MinCapacityPerUser)
		}
	}

	manager := config.NewManager(slog.Default())
	manager.Open(context.Background(), &ConnectionConfig{
		SocketFile: server.ClientConfig().SocketFile,
		Host:       server.ClientConfig().Host,
		Port:       server.ClientConfig().Port,
		Database:   server.ClientConfig().Database,
	})

	return manager
}

// TestDynamicAllocation_DifferentWorkloadPatterns tests that the rebalancer
// correctly allocates capacity based on different usage patterns using max-min fairness.
// Users wanting less than fair share get exactly what they want; remainder is split among others.
func TestDynamicAllocation_DifferentWorkloadPatterns(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	// Configure: 12 global * 0.75 = 9 regular connections for 3 users
	// Fair share = 9/3 = 3 each
	// User A wants 5 (more than fair share)
	// User B wants 1 (less than fair share)
	// User C wants 5 (more than fair share)
	//
	// Max-min fairness:
	// 1. B satisfied with 1, leaving 8 for A and C
	// 2. A and C split 8 evenly = 4 each
	// Result: A=4, B=1, C=4
	manager := newTestManagerWithConfig(t, server, &DynamicAllocationTestConfig{
		GlobalCapacity:     12,
		ReservedRatio:      0.25,
		RebalanceInterval:  50 * time.Millisecond,
		DemandWindow:       150 * time.Millisecond, // 3 buckets = 150ms / 50ms rebalance interval
		MinCapacityPerUser: 1,                      // Use strict demand-based allocation for this test
	})
	defer manager.Close()

	ctx := t.Context()

	// Demands designed to test max-min fairness:
	// B wants less than fair share (1 < 3), A and C want more (5 > 3)
	demands := map[string]int{
		"userA": 5, // Wants more than fair share
		"userB": 1, // Wants less than fair share
		"userC": 5, // Wants more than fair share
	}

	// Acquire connections concurrently - each request in its own goroutine
	// so all 11 requests (5+1+5) happen simultaneously, creating true concurrent demand
	var wg sync.WaitGroup
	connsByUser := make(map[string][]regular.PooledConn)
	var mu sync.Mutex
	ready := make(chan struct{})

	for user, demand := range demands {
		for range demand {
			wg.Add(1)
			go func(user string) {
				defer wg.Done()
				<-ready // Wait for signal to start

				conn, err := manager.GetRegularConn(ctx, user)
				if err == nil {
					mu.Lock()
					connsByUser[user] = append(connsByUser[user], conn)
					mu.Unlock()
				}
			}(user)
		}
	}

	// Start all goroutines simultaneously
	close(ready)

	// Wait for rebalancer to converge on expected allocations.
	// Use Eventually instead of fixed sleep to handle slow CI machines.
	var capA, capB, capC int64
	require.Eventually(t, func() bool {
		stats := manager.Stats()
		if len(stats.UserPools) != 3 {
			return false
		}
		capA = stats.UserPools["userA"].Regular.Capacity
		capB = stats.UserPools["userB"].Regular.Capacity
		capC = stats.UserPools["userC"].Regular.Capacity
		totalCap := capA + capB + capC
		return totalCap == 9
	}, 5*time.Second, 50*time.Millisecond, "rebalancer should converge on total capacity = 9")

	t.Logf("Capacity: 9 regular connections")
	t.Logf("Allocations: A=%d, B=%d, C=%d (total=%d)", capA, capB, capC, capA+capB+capC)

	// Max-min fairness with capacity=9, demands A=5, B=1, C=5:
	// B gets exactly 1 (satisfied), A and C split remaining 8 = 4 each
	assert.Equal(t, int64(1), capB, "userB (demand=1) should get exactly 1")
	assert.Equal(t, int64(4), capA, "userA (demand=5) should get 4 (half of remaining 8)")
	assert.Equal(t, int64(4), capC, "userC (demand=5) should get 4 (half of remaining 8)")

	// Release all connections
	mu.Lock()
	for _, conns := range connsByUser {
		for _, conn := range conns {
			conn.Recycle()
		}
	}
	mu.Unlock()
}

// TestDynamicAllocation_UserArrivalDuringLoad tests that new users
// get connections without starving existing users.
func TestDynamicAllocation_UserArrivalDuringLoad(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	// 15 * 0.8 = 12 regular capacity
	manager := newTestManagerWithConfig(t, server, &DynamicAllocationTestConfig{
		GlobalCapacity:    15,
		ReservedRatio:     0.2,
		RebalanceInterval: 50 * time.Millisecond,
		DemandWindow:      150 * time.Millisecond, // 3 buckets = 150ms / 50ms rebalance interval
	})
	defer manager.Close()

	ctx := t.Context()
	// Start with 2 users under heavy load (each wanting 5 connections)
	var wg sync.WaitGroup
	var mu sync.Mutex
	conns := make(map[string][]regular.PooledConn)
	ready := make(chan struct{})
	release := make(chan struct{})

	// User1 and User2 each request 10 connections concurrently
	for _, user := range []string{"user1", "user2"} {
		for i := range 10 {
			shouldRelease := i > 3
			wg.Add(1)
			go func(u string, shouldRelease bool) {
				defer wg.Done()
				<-ready
				conn, err := manager.GetRegularConn(ctx, u)
				if err == nil {
					if !shouldRelease {
						mu.Lock()
						conns[u] = append(conns[u], conn)
						mu.Unlock()
					}
				}
				if !shouldRelease {
					return
				}
				<-release
				conn.Recycle()
			}(user, shouldRelease)
		}
	}
	close(ready)

	// Wait for rebalancer to converge on expected capacity split.
	// Use Eventually instead of fixed sleep to handle slow CI machines.
	require.Eventually(t, func() bool {
		stats := manager.Stats()
		if len(stats.UserPools) != 2 {
			return false
		}
		for _, poolStats := range stats.UserPools {
			if poolStats.Regular.Capacity != 6 {
				return false
			}
		}
		return true
	}, 5*time.Second, 50*time.Millisecond, "both users should have half of total capacity (6)")
	assert.Equal(t, 2, manager.UserPoolCount())

	// Now add a 3rd user during load
	for range 4 {
		wg.Go(func() {
			conn, err := manager.GetRegularConn(ctx, "user3")
			if err == nil {
				mu.Lock()
				conns["user3"] = append(conns["user3"], conn)
				mu.Unlock()
			}
		})
	}

	// They should get connections just as they are released from the other users
	close(release)

	// Wait for rebalancer to redistribute and include the new user
	require.Eventually(t, func() bool {
		return manager.UserPoolCount() == 3
	}, 5*time.Second, 50*time.Millisecond, "user3 should have a pool")

	// All users should have capacity
	stats := manager.Stats()
	for user, poolStats := range stats.UserPools {
		assert.GreaterOrEqual(t, poolStats.Regular.Capacity, int64(1),
			"user %s should have capacity after new user arrived", user)
	}

	// User3 should have acquired at least 1 connection
	mu.Lock()
	user3Conns := len(conns["user3"])
	mu.Unlock()
	assert.GreaterOrEqual(t, user3Conns, 1, "user3 should have acquired at least 1 connection")

	// Cleanup
	mu.Lock()
	for _, userConns := range conns {
		for _, conn := range userConns {
			conn.Recycle()
		}
	}
	mu.Unlock()
	// Make sure all go routines have returned.
	wg.Wait()
}

// TestDynamicAllocation_UserDepartureDuringLoad tests that when users
// become inactive, their pools are garbage collected and capacity is
// reallocated to remaining users.
func TestDynamicAllocation_UserDepartureDuringLoad(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	manager := newTestManagerWithConfig(t, server, &DynamicAllocationTestConfig{
		GlobalCapacity:    20,
		ReservedRatio:     0.2,
		RebalanceInterval: 50 * time.Millisecond,
		DemandWindow:      150 * time.Millisecond, // 3 buckets = 150ms / 50ms rebalance interval
		InactiveTimeout:   100 * time.Millisecond, // Short timeout for testing
	})
	defer manager.Close()

	ctx := context.Background()

	// Create 3 users
	conn1, err := manager.GetRegularConn(ctx, "user1")
	require.NoError(t, err)
	conn1.Recycle()

	conn2, err := manager.GetRegularConn(ctx, "user2")
	require.NoError(t, err)
	conn2.Recycle()

	conn3, err := manager.GetRegularConn(ctx, "user3")
	require.NoError(t, err)
	conn3.Recycle()

	assert.Equal(t, 3, manager.UserPoolCount())

	// Keep user1 and user2 active while user3 goes inactive.
	// We need to keep them active even during the GC wait period.
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				conn, _ := manager.GetRegularConn(ctx, "user1")
				if conn != nil {
					conn.Recycle()
				}
				conn, _ = manager.GetRegularConn(ctx, "user2")
				if conn != nil {
					conn.Recycle()
				}
				time.Sleep(30 * time.Millisecond)
			}
		}
	}()

	// Wait for GC to run and remove user3 (inactive timeout + GC cycle)
	require.Eventually(t, func() bool {
		return manager.UserPoolCount() == 2
	}, 5*time.Second, 50*time.Millisecond, "user3 should be garbage collected")
	close(done)

	assert.True(t, manager.HasUserPool("user1"))
	assert.True(t, manager.HasUserPool("user2"))
	assert.False(t, manager.HasUserPool("user3"))
}

// TestDynamicAllocation_CapacityExhaustion tests fair distribution
// when total demand exceeds capacity.
func TestDynamicAllocation_CapacityExhaustion(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	// Very low capacity to force exhaustion: 10 * 0.8 = 8 regular
	// 5 users each wanting 3 connections = 15 total demand, only 8 available
	manager := newTestManagerWithConfig(t, server, &DynamicAllocationTestConfig{
		GlobalCapacity:    10,
		ReservedRatio:     0.2,
		RebalanceInterval: 50 * time.Millisecond,
		DemandWindow:      150 * time.Millisecond, // 3 buckets = 150ms / 50ms rebalance interval
	})
	defer manager.Close()

	ctx := t.Context()

	// 5 users each requesting 3 connections concurrently (total demand = 15)
	users := []string{"user1", "user2", "user3", "user4", "user5"}
	var wg sync.WaitGroup
	var mu sync.Mutex
	conns := make(map[string][]regular.PooledConn)
	ready := make(chan struct{})

	for _, user := range users {
		for range 3 {
			wg.Add(1)
			go func(u string) {
				defer wg.Done()
				<-ready
				conn, err := manager.GetRegularConn(ctx, u)
				if err == nil {
					mu.Lock()
					conns[u] = append(conns[u], conn)
					mu.Unlock()
				}
			}(user)
		}
	}
	close(ready)
	wg.Wait()

	// Wait for rebalancer to converge on capacity constraints.
	// Without rebalancing, initial capacity would be 8 per user (40 total),
	// so we must wait for the rebalancer to reduce it to <= 8 total.
	var totalCapacity int64
	require.Eventually(t, func() bool {
		stats := manager.Stats()
		if len(stats.UserPools) != 5 {
			return false
		}
		totalCapacity = 0
		for _, poolStats := range stats.UserPools {
			totalCapacity += poolStats.Regular.Capacity
		}
		return totalCapacity <= 8
	}, 5*time.Second, 50*time.Millisecond, "total allocation should converge to <= regular capacity (8)")

	// Each user should get at least 1 connection (minimum guarantee)
	stats := manager.Stats()
	for user, poolStats := range stats.UserPools {
		assert.GreaterOrEqual(t, poolStats.Regular.Capacity, int64(1),
			"user %s should have minimum 1 connection", user)
	}

	t.Logf("Total demand: 15, Capacity: 8, Allocated: %d", totalCapacity)

	// Cleanup
	mu.Lock()
	for _, userConns := range conns {
		for _, conn := range userConns {
			conn.Recycle()
		}
	}
	mu.Unlock()
}

// TestDynamicAllocation_CapacityRecovery tests that remaining users
// get more capacity when other users leave.
func TestDynamicAllocation_CapacityRecovery(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	// 20 * 0.8 = 16 regular capacity
	manager := newTestManagerWithConfig(t, server, &DynamicAllocationTestConfig{
		GlobalCapacity:    20,
		ReservedRatio:     0.2,
		RebalanceInterval: 50 * time.Millisecond,
		DemandWindow:      150 * time.Millisecond, // 3 buckets = 150ms / 50ms rebalance interval
		InactiveTimeout:   100 * time.Millisecond,
	})
	defer manager.Close()

	ctx := t.Context()

	// Start activity goroutines for user1 that run throughout the test
	// Use 8 goroutines to create demand of ~8 connections
	done := make(chan struct{})
	var user1Capacity int64
	var capacityMu sync.Mutex

	for range 8 {
		go func() {
			for {
				select {
				case <-done:
					return
				default:
					conn, _ := manager.GetRegularConn(ctx, "user1")
					if conn != nil {
						time.Sleep(50 * time.Millisecond) // Hold connection briefly
						conn.Recycle()
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
		}()
	}

	// Wait for user1 pool to be created and rebalanced
	time.Sleep(100 * time.Millisecond)

	// Now create users 2, 3, 4 with concurrent load
	var wg sync.WaitGroup
	var mu sync.Mutex
	conns := make(map[string][]regular.PooledConn)
	ready := make(chan struct{})

	for _, user := range []string{"user2", "user3", "user4"} {
		for range 5 {
			wg.Add(1)
			go func(u string) {
				defer wg.Done()
				<-ready
				conn, err := manager.GetRegularConn(ctx, u)
				if err == nil {
					mu.Lock()
					conns[u] = append(conns[u], conn)
					mu.Unlock()
				}
			}(user)
		}
	}
	close(ready)
	wg.Wait()

	// Wait for rebalancer to reduce user1's capacity (now shared with 3 other users).
	// With 16 regular capacity split 4 ways, user1 should get ~4.
	// Use Eventually instead of fixed sleep to handle slow CI machines where the
	// rebalancer goroutine might not be scheduled promptly.
	require.Eventually(t, func() bool {
		stats := manager.Stats()
		capacityMu.Lock()
		user1Capacity = stats.UserPools["user1"].Regular.Capacity
		capacityMu.Unlock()
		return user1Capacity < 16
	}, 5*time.Second, 50*time.Millisecond, "user1 capacity should decrease when shared with other users")
	t.Logf("user1 capacity with 4 users: %d", user1Capacity)

	// Release connections for users 2, 3, 4 (they go inactive)
	mu.Lock()
	for _, user := range []string{"user2", "user3", "user4"} {
		for _, conn := range conns[user] {
			conn.Recycle()
		}
	}
	mu.Unlock()

	// Wait for GC to remove inactive users
	require.Eventually(t, func() bool {
		return manager.UserPoolCount() == 1
	}, 5*time.Second, 50*time.Millisecond, "inactive users should be garbage collected")

	// Wait for rebalancer to recover capacity for the remaining user.
	// user1 should now get all available capacity (16) since it's the only user.
	var finalUser1Capacity int64
	require.Eventually(t, func() bool {
		stats := manager.Stats()
		finalUser1Capacity = stats.UserPools["user1"].Regular.Capacity
		return finalUser1Capacity > user1Capacity
	}, 5*time.Second, 50*time.Millisecond, "remaining user should get more capacity after others leave")
	t.Logf("user1 capacity alone: %d (was %d with 4 users)", finalUser1Capacity, user1Capacity)

	close(done)
}

// TestDynamicAllocation_GracefulDegradation tests that the system remains
// responsive even when heavily oversubscribed.
func TestDynamicAllocation_GracefulDegradation(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	// Low capacity but enough for minimum 1 per user
	// 15 * 0.8 = 12 regular capacity for 10 users each wanting 3 = 30 demand
	manager := newTestManagerWithConfig(t, server, &DynamicAllocationTestConfig{
		GlobalCapacity:    15,
		ReservedRatio:     0.2,
		RebalanceInterval: 50 * time.Millisecond,
		DemandWindow:      150 * time.Millisecond, // 3 buckets = 150ms / 50ms rebalance interval
	})
	defer manager.Close()

	ctx := t.Context()

	// Create 10 users each requesting 3 connections (total demand = 30)
	numUsers := 10
	users := make([]string, numUsers)
	for i := range numUsers {
		users[i] = "user" + string(rune('A'+i))
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	conns := make(map[string][]regular.PooledConn)
	ready := make(chan struct{})

	// All users request 3 connections concurrently
	for _, user := range users {
		for range 3 {
			wg.Add(1)
			go func(u string) {
				defer wg.Done()
				<-ready
				conn, err := manager.GetRegularConn(ctx, u)
				if err == nil {
					mu.Lock()
					conns[u] = append(conns[u], conn)
					mu.Unlock()
				}
			}(user)
		}
	}
	close(ready)
	wg.Wait()

	// Wait for rebalancer to converge on capacity constraints.
	// Without rebalancing, initial capacity would be 8 per user (80 total for 10 users),
	// so we must wait for the rebalancer to reduce total to <= 12.
	regularCapacity := int64(float64(15) * 0.8)
	var totalCapacity int64
	require.Eventually(t, func() bool {
		stats := manager.Stats()
		if len(stats.UserPools) != numUsers {
			return false
		}
		totalCapacity = 0
		for _, poolStats := range stats.UserPools {
			totalCapacity += poolStats.Regular.Capacity
		}
		return totalCapacity <= regularCapacity
	}, 5*time.Second, 50*time.Millisecond, "total allocation should converge to <= regular capacity")

	// Verify fair distribution - each should have at least 1 since capacity >= users
	stats := manager.Stats()
	for user, poolStats := range stats.UserPools {
		assert.GreaterOrEqual(t, poolStats.Regular.Capacity, int64(1),
			"user %s should have minimum 1 connection", user)
	}

	// System should handle all users without deadlock
	t.Logf("Total demand: 30, Capacity: %d, Allocated: %d to %d users", regularCapacity, totalCapacity, numUsers)

	// Cleanup
	mu.Lock()
	for _, userConns := range conns {
		for _, conn := range userConns {
			conn.Recycle()
		}
	}
	mu.Unlock()
}
