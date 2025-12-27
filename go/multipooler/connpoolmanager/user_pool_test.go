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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/fakepgserver"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/multipooler/connstate"
	"github.com/multigres/multigres/go/multipooler/pools/connpool"
	"github.com/multigres/multigres/go/multipooler/pools/reserved"
)

// newTestUserPool creates a UserPool configured for testing with the given fake server.
func newTestUserPool(t *testing.T, server *fakepgserver.Server) *UserPool {
	t.Helper()

	ctx := context.Background()
	config := &UserPoolConfig{
		ClientConfig: server.ClientConfig(),
		AdminPool:    nil, // Not needed for basic tests
		RegularPoolConfig: &connpool.Config{
			Capacity:     4,
			MaxIdleCount: 4,
		},
		ReservedPoolConfig: &connpool.Config{
			Capacity:     4,
			MaxIdleCount: 4,
		},
		ReservedInactivityTimeout: 5 * time.Second,
	}

	return NewUserPool(ctx, config)
}

func TestUserPool_Username(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestUserPool(t, server)
	defer pool.Close()

	// Verify username matches the client config.
	assert.Equal(t, server.ClientConfig().User, pool.Username())
}

func TestUserPool_GetRegularConn(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestUserPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	// Get a regular connection.
	conn, err := pool.GetRegularConn(ctx)
	require.NoError(t, err)
	require.NotNil(t, conn)

	// Verify connection is working.
	assert.False(t, conn.Conn.IsClosed())

	conn.Recycle()

	// Verify stats.
	stats := pool.Stats()
	assert.Equal(t, int64(1), stats.Regular.Active)
	assert.Equal(t, int64(1), stats.Regular.Idle)
}

func TestUserPool_GetRegularConnWithSettings(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	// Accept SET and RESET commands.
	server.AddQueryPattern(`SET SESSION .+ = .+`, &sqltypes.Result{})
	server.AddQueryPattern(`RESET .+`, &sqltypes.Result{})

	pool := newTestUserPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	settings := connstate.NewSettings(map[string]string{
		"search_path": "public",
	}, 1)

	// Get a connection with settings.
	conn, err := pool.GetRegularConnWithSettings(ctx, settings)
	require.NoError(t, err)
	require.NotNil(t, conn)

	// Verify settings were applied.
	assert.Equal(t, settings, conn.Conn.Settings())

	conn.Recycle()

	// Verify SET was called.
	assert.Greater(t, server.GetPatternCalledNum(`SET SESSION .+ = .+`), 0)
}

func TestUserPool_NewReservedConn(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestUserPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	// Create a reserved connection.
	conn, err := pool.NewReservedConn(ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, conn)

	// Verify connection has a unique ID.
	assert.Greater(t, conn.ConnID, int64(0))

	// Verify stats.
	stats := pool.Stats()
	assert.Equal(t, 1, stats.Reserved.Active)

	conn.Release(reserved.ReleaseCommit)
}

func TestUserPool_NewReservedConn_WithSettings(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	// Accept SET commands.
	server.AddQueryPattern(`SET SESSION .+ = .+`, &sqltypes.Result{})

	pool := newTestUserPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	settings := connstate.NewSettings(map[string]string{
		"timezone": "UTC",
	}, 1)

	// Create a reserved connection with settings.
	conn, err := pool.NewReservedConn(ctx, settings)
	require.NoError(t, err)
	require.NotNil(t, conn)

	conn.Release(reserved.ReleaseCommit)

	// Verify SET was called.
	assert.Greater(t, server.GetPatternCalledNum(`SET SESSION .+ = .+`), 0)
}

func TestUserPool_GetReservedConn(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestUserPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	// Create a reserved connection.
	conn, err := pool.NewReservedConn(ctx, nil)
	require.NoError(t, err)
	connID := conn.ConnID

	// Retrieve by ID.
	retrieved, ok := pool.GetReservedConn(connID)
	require.True(t, ok)
	assert.Equal(t, connID, retrieved.ConnID)

	conn.Release(reserved.ReleaseCommit)
}

func TestUserPool_GetReservedConn_NotFound(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestUserPool(t, server)
	defer pool.Close()

	// Non-existent connection ID.
	_, ok := pool.GetReservedConn(999999)
	assert.False(t, ok)
}

func TestUserPool_Close(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestUserPool(t, server)

	ctx := context.Background()

	// Create some connections.
	regularConn, err := pool.GetRegularConn(ctx)
	require.NoError(t, err)
	regularConn.Recycle()

	reservedConn, err := pool.NewReservedConn(ctx, nil)
	require.NoError(t, err)

	// Close the pool.
	pool.Close()

	// Verify closed state.
	assert.True(t, pool.closed)

	// Reserved connection should be closed.
	assert.True(t, reservedConn.IsClosed())
}

func TestUserPool_Close_Idempotent(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestUserPool(t, server)

	// Double close should not panic.
	pool.Close()
	pool.Close()

	assert.True(t, pool.closed)
}

func TestUserPool_Stats(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestUserPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	// Initial stats.
	stats := pool.Stats()
	assert.Equal(t, pool.Username(), stats.Username)
	assert.Equal(t, int64(0), stats.Regular.Active)
	assert.Equal(t, 0, stats.Reserved.Active)

	// Get a regular connection.
	regularConn, err := pool.GetRegularConn(ctx)
	require.NoError(t, err)

	stats = pool.Stats()
	assert.Equal(t, int64(1), stats.Regular.Active)
	assert.Equal(t, int64(1), stats.Regular.Borrowed)

	regularConn.Recycle()

	stats = pool.Stats()
	assert.Equal(t, int64(1), stats.Regular.Idle)
	assert.Equal(t, int64(0), stats.Regular.Borrowed)

	// Create a reserved connection.
	reservedConn, err := pool.NewReservedConn(ctx, nil)
	require.NoError(t, err)

	stats = pool.Stats()
	assert.Equal(t, 1, stats.Reserved.Active)

	reservedConn.Release(reserved.ReleaseCommit)

	stats = pool.Stats()
	assert.Equal(t, 0, stats.Reserved.Active)
}

func TestUserPool_MultipleRegularConnections(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestUserPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	// Get multiple connections.
	conn1, err := pool.GetRegularConn(ctx)
	require.NoError(t, err)

	conn2, err := pool.GetRegularConn(ctx)
	require.NoError(t, err)

	conn3, err := pool.GetRegularConn(ctx)
	require.NoError(t, err)

	// Verify stats.
	stats := pool.Stats()
	assert.Equal(t, int64(3), stats.Regular.Borrowed)

	// Return all connections.
	conn1.Recycle()
	conn2.Recycle()
	conn3.Recycle()

	stats = pool.Stats()
	assert.Equal(t, int64(0), stats.Regular.Borrowed)
	assert.Equal(t, int64(3), stats.Regular.Idle)
}

func TestUserPool_MultipleReservedConnections(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestUserPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	// Create multiple reserved connections.
	conn1, err := pool.NewReservedConn(ctx, nil)
	require.NoError(t, err)

	conn2, err := pool.NewReservedConn(ctx, nil)
	require.NoError(t, err)

	// Verify unique IDs.
	assert.NotEqual(t, conn1.ConnID, conn2.ConnID)

	// Verify stats.
	stats := pool.Stats()
	assert.Equal(t, 2, stats.Reserved.Active)

	// Release all.
	conn1.Release(0) // ReleaseCommit
	conn2.Release(0)

	stats = pool.Stats()
	assert.Equal(t, 0, stats.Reserved.Active)
}
