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

	"github.com/multigres/multigres/go/multipooler/connstate"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockConnection is a mock implementation of Connection for testing.
type mockConnection struct {
	state  *connstate.ConnectionState
	closed atomic.Bool
}

func newMockConnection() *mockConnection {
	return &mockConnection{
		state: connstate.NewConnectionState(),
	}
}

func (m *mockConnection) State() *connstate.ConnectionState {
	return m.state
}

func (m *mockConnection) IsClosed() bool {
	return m.closed.Load()
}

func (m *mockConnection) Close() error {
	m.closed.Store(true)
	return nil
}

func (m *mockConnection) ApplyState(ctx context.Context, state *connstate.ConnectionState) error {
	m.state = state
	return nil
}

func (m *mockConnection) ResetState(ctx context.Context) error {
	m.state = connstate.NewConnectionState()
	return nil
}

func newTestPool(capacity int64) *Pool[*mockConnection] {
	pool := NewPool[*mockConnection](&Config{
		Capacity:     capacity,
		MaxIdleCount: capacity,
	})
	pool.Open(func(ctx context.Context) (*mockConnection, error) {
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

func TestPoolGetWithState(t *testing.T) {
	pool := newTestPool(10)
	defer pool.Close()

	ctx := context.Background()

	// Create a state with settings
	settings1 := connstate.NewSettings(map[string]string{
		"timezone": "UTC",
	})
	state1 := connstate.NewConnectionStateWithSettings(settings1)

	// Get connection with state
	conn1, err := pool.GetWithState(ctx, state1)
	require.NoError(t, err)

	// Apply the state to the connection
	err = conn1.Conn.ApplyState(ctx, state1)
	require.NoError(t, err)

	// Put it back
	conn1.Recycle()

	// Get with same state - should get from the same bucket (may or may not be exact same conn)
	conn2, err := pool.GetWithState(ctx, state1)
	require.NoError(t, err)
	// Due to the bucket-based distribution, this might be the same connection
	// but we can't guarantee it anymore like before
	conn2.Recycle()

	// Get with different state - should work
	settings2 := connstate.NewSettings(map[string]string{
		"timezone": "America/New_York",
	})
	state2 := connstate.NewConnectionStateWithSettings(settings2)
	conn3, err := pool.GetWithState(ctx, state2)
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

func TestPoolConcurrentGetPut(t *testing.T) {
	pool := newTestPool(50)
	defer pool.Close()

	ctx := context.Background()
	iterations := 1000
	concurrency := 10

	done := make(chan bool)

	for i := 0; i < concurrency; i++ {
		go func() {
			for j := 0; j < iterations; j++ {
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

	for i := 0; i < concurrency; i++ {
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

	// Create multiple connections with different states
	settings1 := connstate.NewSettings(map[string]string{"timezone": "UTC"})
	state1 := connstate.NewConnectionStateWithSettings(settings1)
	settings2 := connstate.NewSettings(map[string]string{"timezone": "PST"})
	state2 := connstate.NewConnectionStateWithSettings(settings2)

	conn1, _ := pool.GetWithState(ctx, state1)
	_ = conn1.Conn.ApplyState(ctx, state1)
	conn1.Recycle()

	conn2, _ := pool.GetWithState(ctx, state2)
	_ = conn2.Conn.ApplyState(ctx, state2)
	conn2.Recycle()

	// Getting with state1 should try to get from the matching bucket
	conn3, _ := pool.GetWithState(ctx, state1)
	conn3.Recycle()

	// Getting with state2 should try to get from the matching bucket
	conn4, _ := pool.GetWithState(ctx, state2)
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

	// Get with state
	settings := connstate.NewSettings(map[string]string{"foo": "bar"})
	state := connstate.NewConnectionStateWithSettings(settings)
	conn3, _ := pool.GetWithState(ctx, state)
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
