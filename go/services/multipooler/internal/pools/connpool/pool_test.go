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

package connpool

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/multigres/multigres/go/services/multipooler/internal/connstate"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockConnection is a mock implementation of Connection for testing.
type mockConnection struct {
	settings *connstate.Settings
	closed   atomic.Bool
	// id distinguishes distinct backends produced by a counting connector, so a
	// test can tell a reused connection from a freshly (re)established one.
	id int
}

func newMockConnection() *mockConnection {
	return &mockConnection{}
}

func (m *mockConnection) Settings() *connstate.Settings {
	return m.settings
}

func (m *mockConnection) IsClosed() bool {
	return m.closed.Load()
}

func (m *mockConnection) Close() error {
	m.closed.Store(true)
	return nil
}

func (m *mockConnection) ApplySettings(ctx context.Context, settings *connstate.Settings) error {
	m.settings = settings
	return nil
}

func (m *mockConnection) ResetAllSettings(ctx context.Context) error {
	m.settings = nil
	return nil
}

func newTestPool(capacity int64) *Pool[*mockConnection] {
	pool := NewPool[*mockConnection](context.Background(), &Config{
		Name:         "test",
		Capacity:     capacity,
		MaxIdleCount: capacity,
	})
	pool.Open(func(ctx context.Context, poolCtx context.Context) (*mockConnection, error) {
		return newMockConnection(), nil
	}, nil)
	return pool
}

func TestPoolBasicGetPut(t *testing.T) {
	pool := newTestPool(10)
	defer pool.Close()

	// Get a connection
	ctx := context.Background()
	conn1, err := pool.Get(ctx)
	require.NoError(t, err)
	require.NotNil(t, conn1)

	stats := pool.Stats()
	assert.Equal(t, int64(1), stats.Active)
	assert.Equal(t, int64(1), stats.Borrowed)
	assert.Equal(t, int64(0), stats.Idle)

	// Put it back using Recycle
	conn1.Recycle()

	stats = pool.Stats()
	assert.Equal(t, int64(1), stats.Active)
	assert.Equal(t, int64(0), stats.Borrowed)
	assert.Equal(t, int64(1), stats.Idle)

	// Get again - should reuse the same connection
	conn2, err := pool.Get(ctx)
	require.NoError(t, err)
	assert.Same(t, conn1, conn2)
}

func TestPoolInvalidateDefaults_LazyReconnectOnBorrow(t *testing.T) {
	var created atomic.Int64
	ctx := context.Background()
	pool := NewPool[*mockConnection](ctx, &Config{Name: "test", Capacity: 4, MaxIdleCount: 4})
	pool.Open(func(_ context.Context, _ context.Context) (*mockConnection, error) {
		c := newMockConnection()
		c.id = int(created.Add(1)) // 1, 2, 3, ...
		return c, nil
	}, nil)
	defer pool.Close()

	// Borrow + recycle: establishes backend id=1 in the clean stack.
	c1, err := pool.Get(ctx)
	require.NoError(t, err)
	first := c1.Conn
	require.Equal(t, 1, first.id)
	c1.Recycle()

	// Without an invalidation, the same backend is reused.
	c2, err := pool.Get(ctx)
	require.NoError(t, err)
	require.Same(t, first, c2.Conn, "should reuse the same backend without invalidation")
	c2.Recycle()

	// Invalidate defaults: the next borrow must reconnect a fresh backend so it
	// re-reads per-database/role GUC defaults.
	pool.InvalidateDefaults()

	c3, err := pool.Get(ctx)
	require.NoError(t, err)
	require.NotSame(t, first, c3.Conn, "stale connection must be reconnected after InvalidateDefaults")
	require.True(t, first.IsClosed(), "stale backend must be closed on reconnect")
	require.Equal(t, 2, c3.Conn.id, "borrow must return the freshly reconnected backend")
	require.False(t, c3.Conn.IsClosed())
	c3.Recycle()

	// Capacity is preserved across the reconnect (slot reused, not leaked).
	assert.Equal(t, int64(1), pool.Stats().Active)

	// Generation is now current again: a borrow without a fresh bump reuses the
	// refreshed backend (no spurious reconnect).
	c4, err := pool.Get(ctx)
	require.NoError(t, err)
	require.Same(t, c3.Conn, c4.Conn, "no reconnect when the generation is current")
	c4.Recycle()
}

func TestPoolInvalidateDefaults_ReconnectPreservesTargetSettings(t *testing.T) {
	var created atomic.Int64
	ctx := context.Background()
	pool := NewPool[*mockConnection](ctx, &Config{Name: "test", Capacity: 4, MaxIdleCount: 4})
	pool.Open(func(_ context.Context, _ context.Context) (*mockConnection, error) {
		c := newMockConnection()
		c.id = int(created.Add(1))
		return c, nil
	}, nil)
	defer pool.Close()

	settings := connstate.NewSettings(map[string]string{"timezone": "UTC"}, 1)

	c1, err := pool.GetWithSettings(ctx, settings)
	require.NoError(t, err)
	first := c1.Conn
	require.Same(t, settings, c1.Conn.Settings())
	c1.Recycle()

	// After an invalidation, the stale backend is reconnected AND the target
	// settings are re-applied on top of the fresh backend.
	pool.InvalidateDefaults()

	c2, err := pool.GetWithSettings(ctx, settings)
	require.NoError(t, err)
	require.NotSame(t, first, c2.Conn, "stale settings connection must be reconnected")
	require.True(t, first.IsClosed())
	require.Same(t, settings, c2.Conn.Settings(), "target settings must be applied to the fresh backend")
	c2.Recycle()
}

func TestPoolGetWithSettings(t *testing.T) {
	pool := newTestPool(10)
	defer pool.Close()

	ctx := context.Background()

	// Create settings
	settings1 := connstate.NewSettings(map[string]string{
		"timezone": "UTC",
	}, 1)

	// Get connection with settings
	conn1, err := pool.GetWithSettings(ctx, settings1)
	require.NoError(t, err)

	// Apply the settings to the connection
	err = conn1.Conn.ApplySettings(ctx, settings1)
	require.NoError(t, err)

	// Put it back
	conn1.Recycle()

	// Get with same settings - should get from the same bucket (may or may not be exact same conn)
	conn2, err := pool.GetWithSettings(ctx, settings1)
	require.NoError(t, err)
	// Due to the bucket-based distribution, this might be the same connection
	// but we can't guarantee it anymore like before
	conn2.Recycle()

	// Get with different settings - should work
	settings2 := connstate.NewSettings(map[string]string{
		"timezone": "America/New_York",
	}, 2)
	conn3, err := pool.GetWithSettings(ctx, settings2)
	require.NoError(t, err)
	conn3.Recycle()
}

func TestPoolCapacityWithWait(t *testing.T) {
	pool := newTestPool(2)
	defer pool.Close()

	ctx := context.Background()

	// Get two connections (at capacity)
	conn1, err := pool.Get(ctx)
	require.NoError(t, err)

	conn2, err := pool.Get(ctx)
	require.NoError(t, err)

	// Try to get third with timeout - should timeout since pool is exhausted
	ctxTimeout, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()

	_, err = pool.Get(ctxTimeout)
	assert.ErrorIs(t, err, ErrTimeout)

	// Put one back
	conn1.Recycle()

	// Now should succeed
	conn3, err := pool.Get(ctx)
	require.NoError(t, err)
	assert.Same(t, conn1, conn3)

	conn2.Recycle()
	conn3.Recycle()
}

func TestPoolClose(t *testing.T) {
	pool := newTestPool(10)

	ctx := context.Background()

	// Get some connections
	conn1, _ := pool.Get(ctx)
	conn2, _ := pool.Get(ctx)

	conn1.Recycle()

	// Close pool
	pool.Close()

	// Connections should be closed - but conn2 wasn't recycled so it might still be open
	// The pool only closes idle connections when closed

	// Further operations should fail
	_, err := pool.Get(ctx)
	assert.ErrorIs(t, err, ErrPoolClosed)

	// Recycling conn2 should close it since pool is closed
	conn2.Recycle()
}

func TestPoolCallerContextCancelDoesNotCloseConnection(t *testing.T) {
	// Verify that cancelling the caller's context (used for Get) does not
	// close the underlying connection. The connection's lifetime should be
	// tied to the pool's context, not the caller's.
	var connPoolCtx context.Context
	pool := NewPool[*mockConnection](context.Background(), &Config{
		Name:         "test",
		Capacity:     1,
		MaxIdleCount: 1,
	})
	pool.Open(func(ctx context.Context, poolCtx context.Context) (*mockConnection, error) {
		connPoolCtx = poolCtx
		return newMockConnection(), nil
	}, nil)
	defer pool.Close()

	// Get a connection using a cancellable context.
	callerCtx, callerCancel := context.WithCancel(context.Background())
	conn, err := pool.Get(callerCtx)
	require.NoError(t, err)
	require.NotNil(t, conn)
	assert.False(t, conn.Conn.IsClosed(), "connection should be open after Get")

	// Cancel the caller's context.
	callerCancel()

	// The connection should still be open — its lifetime is tied to the pool context.
	assert.False(t, conn.Conn.IsClosed(), "connection should remain open after caller context is cancelled")

	// The pool context should still be active.
	assert.NoError(t, connPoolCtx.Err(), "pool context should not be cancelled")

	// Recycle and re-get: the connection should be reusable.
	conn.Recycle()
	conn2, err := pool.Get(context.Background())
	require.NoError(t, err)
	assert.Same(t, conn, conn2, "should reuse the same connection from the pool")
	assert.False(t, conn2.Conn.IsClosed(), "reused connection should still be open")
	conn2.Recycle()
}

func TestPoolConcurrentGetPut(t *testing.T) {
	pool := newTestPool(50)
	defer pool.Close()

	ctx := context.Background()
	iterations := 1000
	concurrency := 10

	done := make(chan bool)

	for range concurrency {
		go func() {
			for range iterations {
				conn, err := pool.Get(ctx)
				if err != nil {
					continue
				}
				time.Sleep(time.Microsecond)
				conn.Recycle()
			}
			done <- true
		}()
	}

	for range concurrency {
		<-done
	}

	stats := pool.Stats()
	assert.Equal(t, int64(0), stats.Borrowed)
	assert.Greater(t, stats.Active, int64(0))
}

func TestPoolStateSegregation(t *testing.T) {
	pool := newTestPool(10)
	defer pool.Close()

	ctx := context.Background()

	// Create multiple connections with different settings
	settings1 := connstate.NewSettings(map[string]string{"timezone": "UTC"}, 1)
	settings2 := connstate.NewSettings(map[string]string{"timezone": "PST"}, 2)

	conn1, _ := pool.GetWithSettings(ctx, settings1)
	_ = conn1.Conn.ApplySettings(ctx, settings1)
	conn1.Recycle()

	conn2, _ := pool.GetWithSettings(ctx, settings2)
	_ = conn2.Conn.ApplySettings(ctx, settings2)
	conn2.Recycle()

	// Getting with settings1 should try to get from the matching bucket
	conn3, _ := pool.GetWithSettings(ctx, settings1)
	conn3.Recycle()

	// Getting with settings2 should try to get from the matching bucket
	conn4, _ := pool.GetWithSettings(ctx, settings2)
	conn4.Recycle()
}

func TestPoolWaitForConnection(t *testing.T) {
	pool := newTestPool(1)
	defer pool.Close()

	ctx := context.Background()

	// Exhaust the pool
	conn1, err := pool.Get(ctx)
	require.NoError(t, err)

	// Start a goroutine that will wait for a connection
	done := make(chan *Pooled[*mockConnection])
	go func() {
		conn, err := pool.Get(ctx)
		if err != nil {
			done <- nil
			return
		}
		done <- conn
	}()

	// Give the goroutine time to start waiting
	time.Sleep(50 * time.Millisecond)

	// Return the connection
	conn1.Recycle()

	// The waiting goroutine should get the connection
	select {
	case conn2 := <-done:
		require.NotNil(t, conn2)
		conn2.Recycle()
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for connection")
	}
}

func TestPoolMetrics(t *testing.T) {
	pool := newTestPool(10)
	defer pool.Close()

	ctx := context.Background()

	// Make some gets
	conn1, _ := pool.Get(ctx)
	conn2, _ := pool.Get(ctx)

	assert.Equal(t, int64(2), pool.Metrics.GetCount())

	conn1.Recycle()
	conn2.Recycle()

	// Get with settings
	settings := connstate.NewSettings(map[string]string{"foo": "bar"}, 1)
	conn3, _ := pool.GetWithSettings(ctx, settings)
	conn3.Recycle()

	assert.Equal(t, int64(1), pool.Metrics.GetStateCount())
}

func TestPoolTaint(t *testing.T) {
	pool := newTestPool(10)
	defer pool.Close()

	ctx := context.Background()

	initialActive := pool.Active()

	conn, err := pool.Get(ctx)
	require.NoError(t, err)

	// Taint the connection
	conn.Taint()

	// Active count should eventually decrease as a new connection replaces it
	// The tainted connection is marked for replacement
	time.Sleep(50 * time.Millisecond)

	// Get a new connection to ensure the pool is still working
	conn2, err := pool.Get(ctx)
	require.NoError(t, err)
	conn2.Recycle()

	// Should have returned to initial + 1 state (the tainted one was replaced)
	assert.GreaterOrEqual(t, pool.Active(), initialActive)
}

func TestPoolSetCapacity_NonBlocking(t *testing.T) {
	pool := newTestPool(10)
	defer pool.Close()

	ctx := context.Background()

	// Get 5 connections (all borrowed)
	var conns []*Pooled[*mockConnection]
	for range 5 {
		conn, err := pool.Get(ctx)
		require.NoError(t, err)
		conns = append(conns, conn)
	}

	assert.Equal(t, int64(5), pool.InUse())
	assert.Equal(t, int64(5), pool.Active())

	// Reduce capacity to 2 - should NOT block even though 5 are borrowed
	err := pool.SetCapacity(ctx, 2)
	require.NoError(t, err)

	// Capacity should be updated immediately
	assert.Equal(t, int64(2), pool.Capacity())
	// Active connections are still 5 (borrowed, not yet recycled)
	assert.Equal(t, int64(5), pool.Active())

	// Return connections - they should be closed on recycle since we're over capacity
	for _, conn := range conns {
		conn.Recycle()
	}

	// After recycling, active should be at or below capacity
	// The first 2 might go to waiters or idle, the remaining 3 should be closed
	assert.LessOrEqual(t, pool.Active(), int64(2))
	assert.Equal(t, int64(0), pool.InUse())
}

func TestPoolSetCapacity_ClosesIdleImmediately(t *testing.T) {
	pool := newTestPool(10)
	defer pool.Close()

	ctx := context.Background()

	// Get 5 connections at once to force 5 active connections
	var conns []*Pooled[*mockConnection]
	for range 5 {
		conn, err := pool.Get(ctx)
		require.NoError(t, err)
		conns = append(conns, conn)
	}

	assert.Equal(t, int64(5), pool.Active())
	assert.Equal(t, int64(5), pool.InUse())

	// Return them all to idle
	for _, conn := range conns {
		conn.Recycle()
	}

	assert.Equal(t, int64(5), pool.Active())
	assert.Equal(t, int64(0), pool.InUse())

	// Reduce capacity to 2 - should immediately close idle connections
	err := pool.SetCapacity(ctx, 2)
	require.NoError(t, err)

	// Active should be reduced immediately since connections were idle
	assert.LessOrEqual(t, pool.Active(), int64(2))
}

func TestPoolSetCapacity_IncreaseUnblocksWaiters(t *testing.T) {
	// Start with capacity 1
	pool := newTestPool(1)
	defer pool.Close()

	ctx := context.Background()

	// Exhaust the pool by borrowing the only connection
	conn1, err := pool.Get(ctx)
	require.NoError(t, err)

	assert.Equal(t, int64(1), pool.Active())
	assert.Equal(t, int64(1), pool.InUse())

	// Start goroutines that will wait for connections
	const numWaiters = 3
	results := make(chan *Pooled[*mockConnection], numWaiters)
	errors := make(chan error, numWaiters)

	for range numWaiters {
		go func() {
			// Use a long timeout - we expect to be unblocked by capacity increase
			waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			conn, err := pool.Get(waitCtx)
			if err != nil {
				errors <- err
				return
			}
			results <- conn
		}()
	}

	// Give goroutines time to start waiting
	time.Sleep(50 * time.Millisecond)

	// Verify we have waiters (pool at capacity, requests pending)
	assert.Equal(t, int64(1), pool.Capacity())
	assert.Equal(t, int64(1), pool.InUse())

	// Increase capacity - this should proactively create connections for waiters
	err = pool.SetCapacity(ctx, 4)
	require.NoError(t, err)

	assert.Equal(t, int64(4), pool.Capacity())

	// All waiters should get connections quickly (not waiting for recycle)
	for range numWaiters {
		select {
		case conn := <-results:
			require.NotNil(t, conn)
			conn.Recycle()
		case err := <-errors:
			t.Fatalf("waiter got error: %v", err)
		case <-time.After(500 * time.Millisecond):
			t.Fatal("timed out waiting for waiter to get connection - capacity increase did not unblock waiters")
		}
	}

	// Return the original connection
	conn1.Recycle()
}

func TestPoolConnectTimeout_BoundsBackgroundConnNew(t *testing.T) {
	// Verify that when ConnectTimeout is set, a slow connector is cancelled
	// during background connection creation (the put(nil) / taint path).
	connectTimeout := 100 * time.Millisecond

	pool := NewPool[*mockConnection](context.Background(), &Config{
		Name:           "test-timeout",
		Capacity:       2,
		MaxIdleCount:   2,
		ConnectTimeout: connectTimeout,
	})

	var connectCalls atomic.Int64
	pool.Open(func(ctx context.Context, poolCtx context.Context) (*mockConnection, error) {
		call := connectCalls.Add(1)
		if call > 1 {
			// Second call (background replacement): block until ctx expires
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return newMockConnection(), nil
	}, nil)
	defer pool.Close()

	ctx := context.Background()

	// Get a connection, then taint it so put() calls connNew(pool.ctx) in background
	conn, err := pool.Get(ctx)
	require.NoError(t, err)
	conn.Taint()

	// Wait for the background connNew to be attempted and cancelled by ConnectTimeout
	time.Sleep(3 * connectTimeout)

	// The pool should not be permanently starved — the active slot should be freed
	assert.Equal(t, int64(0), pool.Active(), "active slot should be released after connect timeout")
}

func TestPoolConnectTimeout_BoundsConnReopen(t *testing.T) {
	// Verify that ConnectTimeout bounds connReopen during max-lifetime recycling.
	connectTimeout := 100 * time.Millisecond

	pool := NewPool[*mockConnection](context.Background(), &Config{
		Name:           "test-reopen-timeout",
		Capacity:       1,
		MaxIdleCount:   1,
		MaxLifetime:    1 * time.Nanosecond, // Force immediate max-lifetime expiry
		ConnectTimeout: connectTimeout,
	})

	var connectCalls atomic.Int64
	pool.Open(func(ctx context.Context, poolCtx context.Context) (*mockConnection, error) {
		call := connectCalls.Add(1)
		if call > 1 {
			// Reopen call: block until ctx expires
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return newMockConnection(), nil
	}, nil)
	defer pool.Close()

	conn, err := pool.Get(context.Background())
	require.NoError(t, err)

	// Recycle triggers max-lifetime check → connReopen with a blocking connector
	conn.Recycle()

	// Wait for reopen to timeout
	time.Sleep(3 * connectTimeout)

	// The active slot should be released, not permanently consumed
	assert.Equal(t, int64(0), pool.Active(), "active slot should be released after reopen timeout")
}
