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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/fakepgserver"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/connpool"
	"github.com/multigres/multigres/go/services/multipooler/internal/pools/regular"
)

// connErrFATAL returns a PgDiagnostic that mterrors.IsConnectionError
// recognises (FATAL 57P01 admin_shutdown). Used to drive the validate-retry
// path in tests, mirroring the real silent-disconnect failure mode the
// pool-layer retry was added to handle.
func connErrFATAL() error {
	return &mterrors.PgDiagnostic{
		MessageType: 'E',
		Severity:    "FATAL",
		Code:        "57P01",
		Message:     "terminating connection due to administrator command",
	}
}

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
	assert.Greater(t, conn.ConnID(), int64(0))

	// Verify pool stats.
	stats := pool.Stats()
	assert.Equal(t, 1, stats.Active)
	assert.Equal(t, int64(1), stats.ReserveCount)

	conn.Release(ReleaseCommit, nil)
}

func TestPool_ReleaseCleanupRunsOnCleanRelease(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()
	called := false
	conn, err := pool.NewConn(ctx, nil, WithReleaseCleanup(func(conn *regular.Conn) bool {
		called = true
		return true
	}))
	require.NoError(t, err)

	conn.Release(ReleaseCommit, nil)
	assert.True(t, called)

	called = false
	conn, err = pool.NewConn(ctx, nil, WithReleaseCleanup(func(conn *regular.Conn) bool {
		called = true
		return true
	}))
	require.NoError(t, err)
	conn.Release(ReleaseError, nil)
	assert.False(t, called, "dirty releases already taint and must skip clean-release hooks")
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
	connID := conn.ConnID()

	// Get by ID should work.
	retrieved, ok := pool.Get(connID)
	require.True(t, ok)
	assert.Equal(t, connID, retrieved.ConnID())

	// Get with invalid ID should fail.
	_, ok = pool.Get(999999)
	assert.False(t, ok)

	conn.Release(ReleaseCommit, nil)
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
	conn.Release(ReleaseCommit, nil)

	stats = pool.Stats()
	assert.Equal(t, 0, stats.Active)
	assert.Equal(t, int64(1), stats.ReleaseCount)
	assert.Equal(t, int64(1), stats.TxCommitCount)
}

func TestConn_Transaction(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	// Setup expected queries.
	server.AddQuery("BEGIN", &sqltypes.Result{})
	server.AddQuery("COMMIT", &sqltypes.Result{})
	server.AddQuery("ROLLBACK", &sqltypes.Result{})

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	t.Run("begin and commit", func(t *testing.T) {
		conn, err := pool.NewConn(ctx, nil)
		require.NoError(t, err)
		defer conn.Release(ReleaseCommit, nil)

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
		defer conn.Release(ReleaseRollback, nil)

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
		defer conn.Release(ReleaseRollback, nil)

		err = conn.Begin(ctx)
		require.NoError(t, err)

		err = conn.Begin(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "transaction already in progress")
	})

	t.Run("commit without begin should fail", func(t *testing.T) {
		conn, err := pool.NewConn(ctx, nil)
		require.NoError(t, err)
		defer conn.Release(ReleaseCommit, nil)

		err = conn.Commit(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no active transaction")
	})
}

func TestConn_CommitResultIncludesNotices(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	server.AddQuery("BEGIN", &sqltypes.Result{CommandTag: "BEGIN"})
	server.AddQuery("COMMIT", &sqltypes.Result{
		CommandTag: "COMMIT",
		Notices: []*mterrors.PgDiagnostic{{
			MessageType: 'N',
			Severity:    "NOTICE",
			Message:     "deferred trigger fired",
		}},
	})

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()
	conn, err := pool.NewConn(ctx, nil)
	require.NoError(t, err)
	defer conn.Release(ReleaseCommit, nil)

	require.NoError(t, conn.Begin(ctx))
	result, err := conn.CommitResult(ctx)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "COMMIT", result.CommandTag)
	require.Len(t, result.Notices, 1)
	require.Equal(t, "deferred trigger fired", result.Notices[0].Message)
}

// TestIsRecoverableSQLError is a direct table test of the positive-classification
// helper: only a non-fatal *mterrors.PgDiagnostic (proof PostgreSQL actually
// sent an ErrorResponse) counts as "PG replied cleanly" — a plain context
// error (proof of nothing; the caller gave up waiting) and a fatal/connection
// diagnostic must both be indeterminate/dead, never clean.
func TestIsRecoverableSQLError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"non-fatal PgDiagnostic", mterrors.NewPgError("ERROR", "23503", "fk violation", ""), true},
		{"context canceled", context.Canceled, false},
		{"context deadline exceeded", context.DeadlineExceeded, false},
		{"fatal PgDiagnostic (admin shutdown)", connErrFATAL(), false},
		{"plain non-PgDiagnostic error", errors.New("boom"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsRecoverableSQLError(tt.err))
		})
	}
}

// TestConn_CommitResult_ContextCancelDoesNotAssumeCleanRollback is a
// regression test: a COMMIT cut off by context cancellation must NOT be
// treated as PostgreSQL having cleanly rolled back the transaction, because
// the returned error (context.Cause) carries no proof PG ever replied — it
// could just as easily mean the backend was force-closed mid-COMMIT. Before
// the fix, `!mterrors.IsConnectionDead(err)` was true for this error too,
// so the transaction reason was wrongly cleared as if PG had answered.
func TestConn_CommitResult_ContextCancelDoesNotAssumeCleanRollback(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	release := make(chan struct{})
	started := make(chan struct{})
	var startOnce sync.Once
	server.AddQueryPatternWithCallback(`^COMMIT$`, &sqltypes.Result{CommandTag: "COMMIT"},
		func(string) {
			startOnce.Do(func() { close(started) })
			<-release
		})

	pool := newTestPool(t, server)
	defer pool.Close()
	// Registered last so it runs first on unwind (LIFO): the blocked server
	// callback must be released before pool.Close()/server.Close() try to
	// tear down the connection, or those would deadlock waiting on it.
	defer close(release)

	ctx := context.Background()
	conn, err := pool.NewConn(ctx, nil)
	require.NoError(t, err)

	require.NoError(t, conn.Begin(ctx))

	commitCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := conn.CommitResult(commitCtx)
		done <- err
	}()

	<-started

	select {
	case err := <-done:
		require.Error(t, err)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(5 * time.Second):
		t.Fatal("CommitResult did not return after the context deadline")
	}

	assert.True(t, conn.IsInTransaction(),
		"a context-cancelled COMMIT carries no proof PG replied — the transaction reason must not be cleared as if it had")
	assert.True(t, protoutil.HasReason(conn.RemainingReasons(), protoutil.ReasonTransaction))
}

// TestConn_RollbackResult_CleanFailureClearsTransactionReason is a regression
// test for the ROLLBACK/COMMIT asymmetry: previously, RollbackResult's error
// path did no bookkeeping at all, so a ROLLBACK that failed with a genuine
// (non-fatal) PostgreSQL error left the transaction reason set forever,
// meaning concludeTransactionError's "release if nothing else holds it"
// logic could never fire for ROLLBACK. Mirroring CommitResult's bookkeeping
// on a clean failure fixes that.
func TestConn_RollbackResult_CleanFailureClearsTransactionReason(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)
	server.AddRejectedQuery("ROLLBACK", mterrors.NewPgError("ERROR", "XX000",
		"some in-rollback trigger error", ""))

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()
	conn, err := pool.NewConn(ctx, nil)
	require.NoError(t, err)

	require.NoError(t, conn.Begin(ctx))

	_, err = conn.RollbackResult(ctx)
	require.Error(t, err)
	assert.False(t, mterrors.IsConnectionDead(err), "a clean SQL-level error is not a dead connection")

	assert.False(t, conn.IsInTransaction(),
		"a clean ROLLBACK failure must still clear the transaction reason, matching CommitResult's bookkeeping")
	assert.Equal(t, uint32(0), conn.RemainingReasons(),
		"with nothing else holding the connection, RemainingReasons() must reach 0 so the caller can release it")
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
	defer conn.Release(ReleasePortalComplete, nil)

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

func TestConn_MultipleReasons(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	server.AddQuery("BEGIN", &sqltypes.Result{})
	server.AddQuery("COMMIT", &sqltypes.Result{})

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	t.Run("transaction and portal concurrent reservation", func(t *testing.T) {
		conn, err := pool.NewConn(ctx, nil)
		require.NoError(t, err)
		defer conn.Release(ReleaseError, nil) // Safety net

		// Begin a transaction.
		err = conn.Begin(ctx)
		require.NoError(t, err)
		assert.True(t, conn.IsInTransaction())
		assert.Equal(t, protoutil.ReasonTransaction, conn.RemainingReasons())

		// Reserve for a portal while in a transaction.
		conn.ReserveForPortal("p1")
		assert.True(t, conn.IsInTransaction())
		assert.True(t, conn.IsReservedForPortal())
		assert.Equal(t, protoutil.ReasonTransaction|protoutil.ReasonPortal, conn.RemainingReasons())

		// Release the portal -- should NOT release the connection (still in transaction).
		shouldRelease := conn.ReleasePortal("p1")
		assert.False(t, shouldRelease)
		assert.True(t, conn.IsInTransaction())
		assert.False(t, conn.IsReservedForPortal())
		assert.Equal(t, protoutil.ReasonTransaction, conn.RemainingReasons())

		// Commit the transaction -- should allow release (no more reasons).
		err = conn.Commit(ctx)
		require.NoError(t, err)
		assert.False(t, conn.IsInTransaction())
		assert.Equal(t, uint32(0), conn.RemainingReasons())
	})

	t.Run("remove reservation reason directly", func(t *testing.T) {
		conn, err := pool.NewConn(ctx, nil)
		require.NoError(t, err)
		defer conn.Release(ReleaseError, nil) // Safety net

		// Add multiple reasons.
		conn.AddReservationReason(protoutil.ReasonTransaction)
		conn.AddReservationReason(protoutil.ReasonTempTable)
		assert.Equal(t, protoutil.ReasonTransaction|protoutil.ReasonTempTable, conn.RemainingReasons())

		// Remove one reason -- should not release.
		shouldRelease := conn.RemoveReservationReason(protoutil.ReasonTransaction)
		assert.False(t, shouldRelease)
		assert.Equal(t, protoutil.ReasonTempTable, conn.RemainingReasons())

		// Remove the last reason -- should release.
		shouldRelease = conn.RemoveReservationReason(protoutil.ReasonTempTable)
		assert.True(t, shouldRelease)
		assert.Equal(t, uint32(0), conn.RemainingReasons())
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

	conn.Release(ReleaseTimeout, nil)
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
	defer conn.Release(ReleaseCommit, nil)

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
	connID := conn.ConnID()

	assert.False(t, conn.IsReleased())

	// Release the connection.
	conn.Release(ReleaseCommit, nil)

	assert.True(t, conn.IsReleased())

	// Should no longer be in active map.
	_, ok := pool.Get(connID)
	assert.False(t, ok)

	// Double release should be no-op.
	conn.Release(ReleaseCommit, nil) // Should not panic
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
	defer conn1.Release(ReleaseCommit, nil)

	conn2, err := pool.NewConn(ctx, nil)
	require.NoError(t, err)
	defer conn2.Release(ReleaseCommit, nil)

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

func TestPool_TimestampBasedConnectionIDs(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	// Use a pool with enough capacity to hold all test connections concurrently.
	pool := NewPool(context.Background(), &PoolConfig{
		InactivityTimeout: 5 * time.Second,
		RegularPoolConfig: &regular.PoolConfig{
			ClientConfig: server.ClientConfig(),
			ConnPoolConfig: &connpool.Config{
				Capacity:     10, // Enough for all test connections
				MaxIdleCount: 10,
			},
		},
	})
	defer pool.Close()

	ctx := context.Background()

	// Create multiple connections and verify IDs are unique and timestamp-based.
	const numConns = 10
	conns := make([]*Conn, numConns)
	ids := make(map[int64]bool)
	var prevID int64

	for i := range numConns {
		conn, err := pool.NewConn(ctx, nil)
		require.NoError(t, err)
		conns[i] = conn

		// Verify ID is positive and large (timestamp-based).
		assert.Greater(t, conn.ConnID(), int64(0), "connection ID should be positive")
		// Timestamps in nanoseconds since 1970 should be > 1e18 (2001+).
		assert.Greater(t, conn.ConnID(), int64(1e18), "connection ID should be timestamp-based")

		// Verify uniqueness.
		assert.False(t, ids[conn.ConnID()], "connection ID should be unique")
		ids[conn.ConnID()] = true

		// Verify IDs are sequential (each ID is larger than the previous).
		if i > 0 {
			assert.Greater(t, conn.ConnID(), prevID, "connection IDs should be sequential")
		}
		prevID = conn.ConnID()
	}

	// Clean up.
	for _, conn := range conns {
		conn.Release(ReleaseCommit, nil)
	}
}

func TestConn_ReleaseError(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	conn, err := pool.NewConn(ctx, nil)
	require.NoError(t, err)
	connID := conn.ConnID()

	// Verify connection is active.
	stats := pool.Stats()
	assert.Equal(t, 1, stats.Active)

	// Release with error.
	conn.Release(ReleaseError, nil)

	// Connection should be marked as released.
	assert.True(t, conn.IsReleased())

	// Should no longer be in active map.
	_, ok := pool.Get(connID)
	assert.False(t, ok)

	// Release count should be incremented.
	stats = pool.Stats()
	assert.Equal(t, 0, stats.Active)
	assert.Equal(t, int64(1), stats.ReleaseCount)

	// Connection should be closed (tainted connections get closed on recycle).
	assert.True(t, conn.IsClosed())
}

func TestConn_ReleaseError_DoubleRelease(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	conn, err := pool.NewConn(ctx, nil)
	require.NoError(t, err)

	// Release with error.
	conn.Release(ReleaseError, nil)
	assert.True(t, conn.IsReleased())

	// Double release should be a no-op (should not panic).
	conn.Release(ReleaseError, nil)

	// Release count should still be 1 (not 2).
	stats := pool.Stats()
	assert.Equal(t, int64(1), stats.ReleaseCount)
}

func TestConn_ReleaseError_TaintsConnection(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	// Pool capacity is 4.
	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	// Create and release a connection with error (taints it).
	conn1, err := pool.NewConn(ctx, nil)
	require.NoError(t, err)
	conn1.Release(ReleaseError, nil)

	// The tainted connection should be closed.
	assert.True(t, conn1.IsClosed())

	// The pool capacity should be preserved — we should still be able to
	// acquire all 4 connections. With the old Close() approach, this slot
	// would have been leaked and only 3 connections would be available.
	conns := make([]*Conn, 4)
	for i := range conns {
		c, err := pool.NewConn(ctx, nil)
		require.NoError(t, err, "should be able to acquire connection %d of 4", i+1)
		conns[i] = c
	}

	stats := pool.Stats()
	assert.Equal(t, 4, stats.Active)

	for _, c := range conns {
		c.Release(ReleaseCommit, nil)
	}
}

func TestPool_NewConn_ValidateRetriesOnConnectionError(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	var calls int
	var firstConn, lastConn *regular.Conn
	validate := func(_ context.Context, conn *regular.Conn) error {
		calls++
		if calls == 1 {
			firstConn = conn
			return connErrFATAL() // triggers Taint + retry
		}
		lastConn = conn
		return nil
	}

	conn, err := pool.NewConn(ctx, nil, WithValidate(validate))
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer conn.Release(ReleaseCommit, nil)

	// Validate should have been invoked once per attempt, and the registered
	// reserved connection should wrap the second (replacement) socket.
	assert.Equal(t, 2, calls)
	require.NotNil(t, firstConn)
	require.NotNil(t, lastConn)
	assert.NotSame(t, firstConn, lastConn, "second attempt should receive a different *regular.Conn after Taint")
	assert.Same(t, lastConn, conn.Conn())
	assert.True(t, firstConn.IsClosed(), "tainted stale conn must be closed promptly, not leaked to the idle killer")

	// Reservation bookkeeping only records the final success.
	stats := pool.Stats()
	assert.Equal(t, 1, stats.Active)
	assert.Equal(t, int64(1), stats.ReserveCount)
}

func TestPool_NewConn_ValidateNonConnectionErrorPropagates(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	sentinel := errors.New("validate refused for business reasons")

	var (
		calls    int
		seenConn *regular.Conn
	)
	validate := func(_ context.Context, c *regular.Conn) error {
		calls++
		seenConn = c
		return sentinel
	}

	conn, err := pool.NewConn(ctx, nil, WithValidate(validate))
	require.Error(t, err)
	assert.ErrorIs(t, err, sentinel, "non-connection errors must propagate unchanged")
	assert.Nil(t, conn)
	assert.Equal(t, 1, calls, "non-connection errors must not trigger a retry")

	// No reservation was registered. The pooled conn is discarded
	// (tainted + recycled) because validate ran state-modifying work on
	// it that may have left the conn in an inconsistent state — see the
	// acquireValidated doc for the full rationale. The happy-path
	// non-connection-error case is indistinguishable from the
	// state-modification case, so we discard unconditionally.
	stats := pool.Stats()
	assert.Equal(t, 0, stats.Active)
	assert.Equal(t, int64(0), stats.ReserveCount, "failed acquisition should not increment ReserveCount")
	require.NotNil(t, seenConn)
	assert.True(t, seenConn.IsClosed(), "validate failure must taint the conn, not recycle it dirty")
}

func TestPool_NewConn_ValidateExhaustsRetries(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	var calls int
	validate := func(_ context.Context, _ *regular.Conn) error {
		calls++
		return connErrFATAL()
	}

	conn, err := pool.NewConn(ctx, nil, WithValidate(validate))
	require.Error(t, err)
	assert.Nil(t, conn)
	assert.Equal(t, constants.MaxConnPoolRetryAttempts, calls, "validate should be invoked exactly constants.MaxConnPoolRetryAttempts times")
	assert.Contains(t, err.Error(), "reserved connection validate failed after")
	// The wrapped error must still classify as a connection error so
	// upstream callers can react accordingly.
	assert.True(t, mterrors.IsConnectionError(err), "wrapped error must still unwrap to a connection error")

	stats := pool.Stats()
	assert.Equal(t, 0, stats.Active)
	assert.Equal(t, int64(0), stats.ReserveCount)
}

func TestPool_NewConn_NilValidateUnchanged(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	// Passing no options is the canonical "no validate" call.
	conn, err := pool.NewConn(ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer conn.Release(ReleaseCommit, nil)

	// Passing WithValidate(nil) should also behave like no validate at all.
	conn2, err := pool.NewConn(ctx, nil, WithValidate(nil))
	require.NoError(t, err)
	require.NotNil(t, conn2)
	defer conn2.Release(ReleaseCommit, nil)

	stats := pool.Stats()
	assert.Equal(t, 2, stats.Active)
	assert.Equal(t, int64(2), stats.ReserveCount)
}

func TestPool_NewConnAfterPoolRecreation(t *testing.T) {
	// This test simulates the scenario where multipooler restarts:
	// - Old pool had connections with certain IDs
	// - New pool should have completely different IDs (no collision)
	// because the new pool's lastID is initialized with a later timestamp.
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	// Create first pool and get connection IDs.
	pool1 := newTestPool(t, server)
	ctx := context.Background()

	const numConns = 5
	var maxPool1ID int64
	for range numConns {
		conn, err := pool1.NewConn(ctx, nil)
		require.NoError(t, err)
		if conn.ConnID() > maxPool1ID {
			maxPool1ID = conn.ConnID()
		}
		conn.Release(ReleaseCommit, nil)
	}
	pool1.Close()

	// Small delay to ensure timestamp advances (though not strictly necessary
	// since the new pool's initial timestamp will be later anyway).
	time.Sleep(time.Millisecond)

	// Create second pool (simulates restart) and verify IDs are greater.
	pool2 := newTestPool(t, server)
	defer pool2.Close()

	for range numConns {
		conn, err := pool2.NewConn(ctx, nil)
		require.NoError(t, err)
		// New pool's IDs should all be greater than the max from the old pool
		// because they're based on a later timestamp.
		assert.Greater(t, conn.ConnID(), maxPool1ID, "new pool IDs should be greater than old pool IDs")
		conn.Release(ReleaseCommit, nil)
	}
}
