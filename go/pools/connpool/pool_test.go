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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockConnection is a mock implementation of Connection for testing.
type mockConnection struct {
	state  *ConnectionState
	closed atomic.Bool
}

func newMockConnection() *mockConnection {
	return &mockConnection{
		state: NewEmptyConnectionState(),
	}
}

func (m *mockConnection) State() *ConnectionState {
	return m.state
}

func (m *mockConnection) IsClosed() bool {
	return m.closed.Load()
}

func (m *mockConnection) Close() error {
	m.closed.Store(true)
	return nil
}

func (m *mockConnection) ApplyState(ctx context.Context, state *ConnectionState) error {
	m.state = state
	return nil
}

func (m *mockConnection) ResetState(ctx context.Context) error {
	m.state = NewEmptyConnectionState()
	return nil
}

func TestPoolBasicGetPut(t *testing.T) {
	factory := func(ctx context.Context) (*mockConnection, error) {
		return newMockConnection(), nil
	}

	pool := NewPool(factory, Config{Capacity: 10})
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

	// Put it back
	err = pool.Put(conn1)
	require.NoError(t, err)

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
	factory := func(ctx context.Context) (*mockConnection, error) {
		return newMockConnection(), nil
	}

	pool := NewPool(factory, Config{Capacity: 10})
	defer pool.Close()

	ctx := context.Background()

	// Create a state with settings
	state1 := NewConnectionState(map[string]string{
		"timezone": "UTC",
	})

	// Get connection with state
	conn1, err := pool.GetWithState(ctx, state1)
	require.NoError(t, err)

	// Apply the state to the connection
	err = conn1.Conn().ApplyState(ctx, state1)
	require.NoError(t, err)

	// Put it back
	err = pool.Put(conn1)
	require.NoError(t, err)

	// Get with same state - should reuse
	conn2, err := pool.GetWithState(ctx, state1)
	require.NoError(t, err)
	assert.Same(t, conn1, conn2)

	// Get with different state - should create new connection
	state2 := NewConnectionState(map[string]string{
		"timezone": "America/New_York",
	})
	conn3, err := pool.GetWithState(ctx, state2)
	require.NoError(t, err)
	assert.NotSame(t, conn1, conn3)
}

func TestPoolCapacity(t *testing.T) {
	factory := func(ctx context.Context) (*mockConnection, error) {
		return newMockConnection(), nil
	}

	pool := NewPool(factory, Config{Capacity: 2})
	defer pool.Close()

	ctx := context.Background()

	// Get two connections (at capacity)
	conn1, err := pool.Get(ctx)
	require.NoError(t, err)

	conn2, err := pool.Get(ctx)
	require.NoError(t, err)

	// Try to get third - should fail
	_, err = pool.Get(ctx)
	assert.ErrorIs(t, err, ErrPoolExhausted)

	// Put one back
	err = pool.Put(conn1)
	require.NoError(t, err)

	// Now should succeed
	conn3, err := pool.Get(ctx)
	require.NoError(t, err)
	assert.Same(t, conn1, conn3)

	_ = pool.Put(conn2)
	_ = pool.Put(conn3)
}

func TestPoolReserve(t *testing.T) {
	factory := func(ctx context.Context) (*mockConnection, error) {
		return newMockConnection(), nil
	}

	pool := NewPool(factory, Config{Capacity: 10})
	defer pool.Close()

	ctx := context.Background()

	// Get a connection
	conn, err := pool.Get(ctx)
	require.NoError(t, err)

	stats := pool.Stats()
	assert.Equal(t, int64(1), stats.Borrowed)
	assert.Equal(t, int64(0), stats.Reserved)

	// Reserve it for client 123
	err = pool.Reserve(conn, 123)
	require.NoError(t, err)

	stats = pool.Stats()
	assert.Equal(t, int64(0), stats.Borrowed)
	assert.Equal(t, int64(1), stats.Reserved)

	// Check that we can get the reserved connection
	reserved := pool.GetReserved(123)
	assert.Same(t, conn, reserved)

	// Try to reserve again with same connection - should succeed (idempotent)
	err = pool.Reserve(conn, 123)
	assert.NoError(t, err)

	// Try to reserve for a different client - should fail
	err = pool.Reserve(conn, 456)
	assert.Error(t, err)

	// Unreserve - should put back to pool
	err = pool.Unreserve(123)
	require.NoError(t, err)

	stats = pool.Stats()
	assert.Equal(t, int64(0), stats.Borrowed)
	assert.Equal(t, int64(0), stats.Reserved)
	assert.Equal(t, int64(1), stats.Idle)

	// Check that reservation is gone
	reserved = pool.GetReserved(123)
	assert.Nil(t, reserved)
}

func TestPoolClose(t *testing.T) {
	factory := func(ctx context.Context) (*mockConnection, error) {
		return newMockConnection(), nil
	}

	pool := NewPool(factory, Config{Capacity: 10})

	ctx := context.Background()

	// Get some connections
	conn1, _ := pool.Get(ctx)
	conn2, _ := pool.Get(ctx)

	_ = pool.Put(conn1)

	// Close pool
	err := pool.Close()
	require.NoError(t, err)

	// Connections should be closed
	assert.True(t, conn1.Conn().IsClosed())

	// Further operations should fail
	_, err = pool.Get(ctx)
	assert.ErrorIs(t, err, ErrPoolClosed)

	err = pool.Put(conn2)
	assert.ErrorIs(t, err, ErrPoolClosed)
}

func TestPoolConcurrentGetPut(t *testing.T) {
	factory := func(ctx context.Context) (*mockConnection, error) {
		return newMockConnection(), nil
	}

	pool := NewPool(factory, Config{Capacity: 50})
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
				_ = pool.Put(conn)
			}
			done <- true
		}()
	}

	for i := 0; i < concurrency; i++ {
		<-done
	}

	stats := pool.Stats()
	assert.Equal(t, int64(0), stats.Borrowed)
	assert.Equal(t, int64(0), stats.Reserved)
	assert.Greater(t, stats.Active, int64(0))
}

func TestPoolStateSegregation(t *testing.T) {
	factory := func(ctx context.Context) (*mockConnection, error) {
		return newMockConnection(), nil
	}

	pool := NewPool(factory, Config{Capacity: 10})
	defer pool.Close()

	ctx := context.Background()

	// Create multiple connections with different states
	state1 := NewConnectionState(map[string]string{"timezone": "UTC"})
	state2 := NewConnectionState(map[string]string{"timezone": "PST"})

	conn1, _ := pool.GetWithState(ctx, state1)
	_ = conn1.Conn().ApplyState(ctx, state1)
	_ = pool.Put(conn1)

	conn2, _ := pool.GetWithState(ctx, state2)
	_ = conn2.Conn().ApplyState(ctx, state2)
	_ = pool.Put(conn2)

	// Getting with state1 should return conn1
	conn3, _ := pool.GetWithState(ctx, state1)
	assert.Same(t, conn1, conn3)

	// Getting with state2 should return conn2
	conn4, _ := pool.GetWithState(ctx, state2)
	assert.Same(t, conn2, conn4)

	_ = pool.Put(conn3)
	_ = pool.Put(conn4)
}
