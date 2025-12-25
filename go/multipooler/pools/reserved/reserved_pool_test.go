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

package reserved

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/fakepgserver"
	"github.com/multigres/multigres/go/multipooler/pools/connpool"
	"github.com/multigres/multigres/go/multipooler/pools/regular"
	"github.com/multigres/multigres/go/pb/query"
)

func newTestPool(t *testing.T, server *fakepgserver.Server) *Pool {
	pool := NewPool(context.Background(), &PoolConfig{
		InactivityTimeout: 5 * time.Second, // Short timeout for testing
		RegularPoolConfig: &regular.PoolConfig{
			ClientConfig: server.ClientConfig(),
			ConnPoolConfig: &connpool.Config{
				Capacity:     4,
				MaxIdleCount: 4,
			},
		},
	})
	return pool
}

func TestPool_NewConn(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	// Create a new reserved connection.
	conn, err := pool.NewConn(ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, conn)

	// Verify connection has a unique ID.
	assert.Greater(t, conn.ConnID, int64(0))

	// Verify pool stats.
	stats := pool.Stats()
	assert.Equal(t, 1, stats.Active)
	assert.Equal(t, int64(1), stats.ReserveCount)

	conn.Release(ReleaseCommit)
}

func TestPool_Get(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	// Create a connection.
	conn, err := pool.NewConn(ctx, nil)
	require.NoError(t, err)
	connID := conn.ConnID

	// Get by ID should work.
	retrieved, ok := pool.Get(connID)
	require.True(t, ok)
	assert.Equal(t, connID, retrieved.ConnID)

	// Get with invalid ID should fail.
	_, ok = pool.Get(999999)
	assert.False(t, ok)

	conn.Release(ReleaseCommit)
}

func TestPool_Close(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestPool(t, server)

	ctx := context.Background()

	// Create connections.
	conn1, err := pool.NewConn(ctx, nil)
	require.NoError(t, err)

	conn2, err := pool.NewConn(ctx, nil)
	require.NoError(t, err)

	// Close the pool.
	pool.Close()

	// Stats should show no active connections.
	stats := pool.Stats()
	assert.Equal(t, 0, stats.Active)

	// Connections should be closed.
	assert.True(t, conn1.IsClosed())
	assert.True(t, conn2.IsClosed())
}

func TestPool_Stats(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	// Initial stats.
	stats := pool.Stats()
	assert.Equal(t, 0, stats.Active)
	assert.Equal(t, int64(0), stats.ReserveCount)

	// Create a connection.
	conn, err := pool.NewConn(ctx, nil)
	require.NoError(t, err)

	stats = pool.Stats()
	assert.Equal(t, 1, stats.Active)
	assert.Equal(t, int64(1), stats.ReserveCount)

	// Release connection.
	conn.Release(ReleaseCommit)

	stats = pool.Stats()
	assert.Equal(t, 0, stats.Active)
	assert.Equal(t, int64(1), stats.ReleaseCount)
	assert.Equal(t, int64(1), stats.TxCommitCount)
}

func TestConn_Transaction(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	// Setup expected queries.
	server.AddQuery("BEGIN", &query.QueryResult{})
	server.AddQuery("COMMIT", &query.QueryResult{})
	server.AddQuery("ROLLBACK", &query.QueryResult{})

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	t.Run("begin and commit", func(t *testing.T) {
		conn, err := pool.NewConn(ctx, nil)
		require.NoError(t, err)
		defer conn.Release(ReleaseCommit)

		// Initially not in transaction.
		assert.False(t, conn.IsInTransaction())

		// Begin transaction.
		err = conn.Begin(ctx)
		require.NoError(t, err)
		assert.True(t, conn.IsInTransaction())

		// Commit transaction.
		err = conn.Commit(ctx)
		require.NoError(t, err)
		assert.False(t, conn.IsInTransaction())
	})

	t.Run("begin and rollback", func(t *testing.T) {
		conn, err := pool.NewConn(ctx, nil)
		require.NoError(t, err)
		defer conn.Release(ReleaseRollback)

		err = conn.Begin(ctx)
		require.NoError(t, err)
		assert.True(t, conn.IsInTransaction())

		err = conn.Rollback(ctx)
		require.NoError(t, err)
		assert.False(t, conn.IsInTransaction())
	})

	t.Run("double begin should fail", func(t *testing.T) {
		conn, err := pool.NewConn(ctx, nil)
		require.NoError(t, err)
		defer conn.Release(ReleaseRollback)

		err = conn.Begin(ctx)
		require.NoError(t, err)

		err = conn.Begin(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "transaction already in progress")
	})

	t.Run("commit without begin should fail", func(t *testing.T) {
		conn, err := pool.NewConn(ctx, nil)
		require.NoError(t, err)
		defer conn.Release(ReleaseCommit)

		err = conn.Commit(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no active transaction")
	})
}

func TestConn_PortalReservation(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	conn, err := pool.NewConn(ctx, nil)
	require.NoError(t, err)
	defer conn.Release(ReleasePortalComplete)

	t.Run("reserve portal", func(t *testing.T) {
		assert.False(t, conn.IsReservedForPortal())
		assert.False(t, conn.HasPortal("p1"))

		conn.ReserveForPortal("p1")

		assert.True(t, conn.IsReservedForPortal())
		assert.True(t, conn.HasPortal("p1"))
	})

	t.Run("multiple portals", func(t *testing.T) {
		conn.ReserveForPortal("p2")
		conn.ReserveForPortal("p3")

		assert.True(t, conn.HasPortal("p1"))
		assert.True(t, conn.HasPortal("p2"))
		assert.True(t, conn.HasPortal("p3"))
	})

	t.Run("release single portal", func(t *testing.T) {
		shouldRelease := conn.ReleasePortal("p2")
		assert.False(t, shouldRelease) // Still has other portals

		assert.True(t, conn.HasPortal("p1"))
		assert.False(t, conn.HasPortal("p2"))
		assert.True(t, conn.HasPortal("p3"))
	})

	t.Run("release all portals", func(t *testing.T) {
		conn.ReleaseAllPortals()

		assert.False(t, conn.IsReservedForPortal())
		assert.False(t, conn.HasPortal("p1"))
	})
}

func TestConn_Timeout(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := NewPool(context.Background(), &PoolConfig{
		InactivityTimeout: 10 * time.Millisecond, // Very short for testing
		RegularPoolConfig: &regular.PoolConfig{
			ClientConfig: server.ClientConfig(),
			ConnPoolConfig: &connpool.Config{
				Capacity:     4,
				MaxIdleCount: 4,
			},
		},
	})
	defer pool.Close()

	ctx := context.Background()

	conn, err := pool.NewConn(ctx, nil)
	require.NoError(t, err)

	// Initially not timed out.
	assert.False(t, conn.IsTimedOut())

	// Wait for timeout.
	time.Sleep(20 * time.Millisecond)

	// Now should be timed out.
	assert.True(t, conn.IsTimedOut())

	conn.Release(ReleaseTimeout)
}

func TestConn_ResetExpiryTime(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := NewPool(context.Background(), &PoolConfig{
		InactivityTimeout: 30 * time.Millisecond,
		RegularPoolConfig: &regular.PoolConfig{
			ClientConfig: server.ClientConfig(),
			ConnPoolConfig: &connpool.Config{
				Capacity:     2,
				MaxIdleCount: 2,
			},
		},
	})
	defer pool.Close()

	ctx := context.Background()

	conn, err := pool.NewConn(ctx, nil)
	require.NoError(t, err)
	defer conn.Release(ReleaseCommit)

	// Wait halfway to timeout.
	time.Sleep(18 * time.Millisecond)
	assert.False(t, conn.IsTimedOut())

	// Reset expiry.
	conn.ResetExpiryTime()

	// Wait another halfway (18ms from reset, not original).
	time.Sleep(18 * time.Millisecond)

	// Should NOT be timed out yet (reset extended lifetime).
	assert.False(t, conn.IsTimedOut())
}

func TestConn_Release(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	conn, err := pool.NewConn(ctx, nil)
	require.NoError(t, err)
	connID := conn.ConnID

	assert.False(t, conn.IsReleased())

	// Release the connection.
	conn.Release(ReleaseCommit)

	assert.True(t, conn.IsReleased())

	// Should no longer be in active map.
	_, ok := pool.Get(connID)
	assert.False(t, ok)

	// Double release should be no-op.
	conn.Release(ReleaseCommit) // Should not panic
}

func TestPool_ForEachActive(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	// Create multiple connections.
	conn1, err := pool.NewConn(ctx, nil)
	require.NoError(t, err)
	defer conn1.Release(ReleaseCommit)

	conn2, err := pool.NewConn(ctx, nil)
	require.NoError(t, err)
	defer conn2.Release(ReleaseCommit)

	// Count active connections.
	var count int
	pool.ForEachActive(func(connID int64, rc *Conn) bool {
		count++
		return true
	})

	assert.Equal(t, 2, count)
}

func TestPool_KillConnection_NotFound(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	// Try to kill non-existent connection.
	err := pool.KillConnection(ctx, 999999)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}
