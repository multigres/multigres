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

package admin

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/fakepgserver"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/services/multipooler/pools/connpool"
)

func newTestPool(_ *testing.T, server *fakepgserver.Server) *Pool {
	pool := NewPool(context.Background(), &PoolConfig{
		ClientConfig: server.ClientConfig(),
		ConnPoolConfig: &connpool.Config{
			Capacity:     2,
			MaxIdleCount: 2,
		},
	})
	pool.Open()
	return pool
}

func TestPool_GetConnection(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true) // Allow any query

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	// Get a connection.
	pooled, err := pool.Get(ctx)
	require.NoError(t, err)
	require.NotNil(t, pooled)

	// Verify connection is working.
	assert.False(t, pooled.Conn.IsClosed())

	// Return connection.
	pooled.Recycle()

	// Verify stats.
	stats := pool.Stats()
	assert.Equal(t, int64(1), stats.Active)
	assert.Equal(t, int64(1), stats.Idle)
}

func TestPool_TerminateBackend(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	// Set up expected pg_terminate_backend response.
	server.AddQueryPattern(`SELECT pg_terminate_backend\(\d+\)`, fakepgserver.MakeResult(
		[]string{"pg_terminate_backend"},
		[][]any{{"t"}}, // "t" = true in PostgreSQL
	))

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	// Terminate a backend.
	success, err := pool.TerminateBackend(ctx, 12345)
	require.NoError(t, err)
	assert.True(t, success)

	// Verify the query was actually executed.
	server.VerifyAllPatternsUsedOrFail()
}

func TestPool_TerminateBackend_NotFound(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	// Set up expected pg_terminate_backend response for not found.
	server.AddQueryPattern(`SELECT pg_terminate_backend\(\d+\)`, fakepgserver.MakeResult(
		[]string{"pg_terminate_backend"},
		[][]any{{"f"}}, // "f" = false - backend not found
	))

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	// Terminate a non-existent backend.
	success, err := pool.TerminateBackend(ctx, 99999)
	require.NoError(t, err)
	assert.False(t, success)

	// Verify the query was actually executed.
	server.VerifyAllPatternsUsedOrFail()
}

func TestPool_CancelBackend(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	// Set up expected pg_cancel_backend response.
	server.AddQueryPattern(`SELECT pg_cancel_backend\(\d+\)`, fakepgserver.MakeResult(
		[]string{"pg_cancel_backend"},
		[][]any{{"t"}}, // "t" = true
	))

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	// Cancel a backend.
	success, err := pool.CancelBackend(ctx, 12345)
	require.NoError(t, err)
	assert.True(t, success)

	// Verify the query was actually executed.
	server.VerifyAllPatternsUsedOrFail()
}

func TestPool_CancelBackend_NotFound(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	// Set up expected pg_cancel_backend response for not found.
	server.AddQueryPattern(`SELECT pg_cancel_backend\(\d+\)`, fakepgserver.MakeResult(
		[]string{"pg_cancel_backend"},
		[][]any{{"f"}}, // "f" = false - backend not found
	))

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	// Cancel a non-existent backend.
	success, err := pool.CancelBackend(ctx, 99999)
	require.NoError(t, err)
	assert.False(t, success)

	// Verify the query was actually executed.
	server.VerifyAllPatternsUsedOrFail()
}

func TestPool_Close(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestPool(t, server)

	ctx := context.Background()

	// Get a connection.
	pooled, err := pool.Get(ctx)
	require.NoError(t, err)
	pooled.Recycle()

	// Close the pool.
	pool.Close()

	// Verify stats show closed state.
	stats := pool.Stats()
	assert.Equal(t, int64(0), stats.Active)
}

func TestConn_Settings(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	pooled, err := pool.Get(ctx)
	require.NoError(t, err)
	defer pooled.Recycle()

	// Admin connections should have nil settings.
	assert.Nil(t, pooled.Conn.Settings())
}

func TestConn_ApplySettings_Panics(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	pooled, err := pool.Get(ctx)
	require.NoError(t, err)
	defer pooled.Recycle()

	// ApplySettings should panic for admin connections.
	assert.Panics(t, func() {
		_ = pooled.Conn.ApplySettings(ctx, nil)
	})
}

func TestConn_ResetAllSettings_Noop(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	pooled, err := pool.Get(ctx)
	require.NoError(t, err)
	defer pooled.Recycle()

	// ResetAllSettings should be a no-op for admin connections.
	err = pooled.Conn.ResetAllSettings(ctx)
	assert.NoError(t, err)
}

// queryPattern is the regex every GetRolPassword test registers against the
// fake PG. Kept once so updates to the SQL only need to land in one place.
const getRolPasswordQueryPattern = `SELECT rolpassword, rolcanlogin, ` +
	`\(rolvaliduntil IS NULL OR rolvaliduntil > now\(\)\) AS password_valid ` +
	`FROM pg_catalog\.pg_authid WHERE rolname = '%s' LIMIT 1`

// rolpassword / rolcanlogin / password_valid row shape returned by pg_authid
// in the updated query. Helpers below wrap MakeResult to keep tests readable.
func authRow(pw any, canLogin, pwValid string) [][]any {
	return [][]any{{pw, canLogin, pwValid}}
}

func TestConn_GetRolPassword(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	server.AddQueryPattern(
		fmt.Sprintf(getRolPasswordQueryPattern, "testuser"),
		fakepgserver.MakeResult(
			[]string{"rolpassword", "rolcanlogin", "password_valid"},
			authRow("SCRAM-SHA-256$4096:salt$hash", "t", "t"),
		))

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	pooled, err := pool.Get(ctx)
	require.NoError(t, err)
	defer pooled.Recycle()

	hash, err := pooled.Conn.GetRolPassword(ctx, "testuser")
	require.NoError(t, err)
	assert.Equal(t, "SCRAM-SHA-256$4096:salt$hash", hash)

	server.VerifyAllPatternsUsedOrFail()
}

func TestConn_GetRolPassword_UserNotFound(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	server.AddQueryPattern(
		fmt.Sprintf(getRolPasswordQueryPattern, "nonexistent"),
		fakepgserver.MakeResult(
			[]string{"rolpassword", "rolcanlogin", "password_valid"},
			[][]any{}, // No rows
		))

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	pooled, err := pool.Get(ctx)
	require.NoError(t, err)
	defer pooled.Recycle()

	_, err = pooled.Conn.GetRolPassword(ctx, "nonexistent")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUserNotFound)

	server.VerifyAllPatternsUsedOrFail()
}

func TestConn_GetRolPassword_NullPassword(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	server.AddQueryPattern(
		fmt.Sprintf(getRolPasswordQueryPattern, "nopasswd"),
		fakepgserver.MakeResult(
			[]string{"rolpassword", "rolcanlogin", "password_valid"},
			authRow(nil, "t", "t"),
		))

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	pooled, err := pool.Get(ctx)
	require.NoError(t, err)
	defer pooled.Recycle()

	hash, err := pooled.Conn.GetRolPassword(ctx, "nopasswd")
	require.NoError(t, err)
	assert.Equal(t, "", hash)

	server.VerifyAllPatternsUsedOrFail()
}

func TestConn_GetRolPassword_LoginDisabled(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	// rolcanlogin=false — PG rejects independent of password, even with a
	// usable hash.
	server.AddQueryPattern(
		fmt.Sprintf(getRolPasswordQueryPattern, "nologin_user"),
		fakepgserver.MakeResult(
			[]string{"rolpassword", "rolcanlogin", "password_valid"},
			authRow("SCRAM-SHA-256$4096:salt$hash", "f", "t"),
		))

	pool := newTestPool(t, server)
	defer pool.Close()

	pooled, err := pool.Get(context.Background())
	require.NoError(t, err)
	defer pooled.Recycle()

	_, err = pooled.Conn.GetRolPassword(context.Background(), "nologin_user")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrLoginDisabled)

	server.VerifyAllPatternsUsedOrFail()
}

func TestConn_GetRolPassword_PasswordExpired(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	// rolvaliduntil in the past (password_valid=false) — password-based auth
	// must be rejected. PG returns the same 28P01 as wrong-password.
	server.AddQueryPattern(
		fmt.Sprintf(getRolPasswordQueryPattern, "expired_user"),
		fakepgserver.MakeResult(
			[]string{"rolpassword", "rolcanlogin", "password_valid"},
			authRow("SCRAM-SHA-256$4096:salt$hash", "t", "f"),
		))

	pool := newTestPool(t, server)
	defer pool.Close()

	pooled, err := pool.Get(context.Background())
	require.NoError(t, err)
	defer pooled.Recycle()

	_, err = pooled.Conn.GetRolPassword(context.Background(), "expired_user")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrPasswordExpired)

	server.VerifyAllPatternsUsedOrFail()
}

func TestConn_GetRolPassword_LoginDisabledBeatsPasswordExpired(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	// A role that is both NOLOGIN and has an expired password: PG reports the
	// login-disabled error (28000) — it's checked first. Multigateway must
	// match that precedence or operators see inconsistent error codes when
	// disabling a role via either mechanism.
	server.AddQueryPattern(
		fmt.Sprintf(getRolPasswordQueryPattern, "double_disabled"),
		fakepgserver.MakeResult(
			[]string{"rolpassword", "rolcanlogin", "password_valid"},
			authRow("SCRAM-SHA-256$4096:salt$hash", "f", "f"),
		))

	pool := newTestPool(t, server)
	defer pool.Close()

	pooled, err := pool.Get(context.Background())
	require.NoError(t, err)
	defer pooled.Recycle()

	_, err = pooled.Conn.GetRolPassword(context.Background(), "double_disabled")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrLoginDisabled)
	assert.NotErrorIs(t, err, ErrPasswordExpired)

	server.VerifyAllPatternsUsedOrFail()
}

func TestConn_GetRolPassword_SQLInjection(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	// The single quote must be properly escaped; the rest of the SQL shape
	// must stay intact.
	server.AddQueryPattern(
		fmt.Sprintf(getRolPasswordQueryPattern, "user''s"),
		fakepgserver.MakeResult(
			[]string{"rolpassword", "rolcanlogin", "password_valid"},
			authRow("SCRAM-SHA-256$4096:salt$hash", "t", "t"),
		))

	pool := newTestPool(t, server)
	defer pool.Close()

	ctx := context.Background()

	pooled, err := pool.Get(ctx)
	require.NoError(t, err)
	defer pooled.Recycle()

	hash, err := pooled.Conn.GetRolPassword(ctx, "user's")
	require.NoError(t, err)
	assert.Equal(t, "SCRAM-SHA-256$4096:salt$hash", hash)

	server.VerifyAllPatternsUsedOrFail()
}

// --- queryWithRetry tests ---

// newTestDirectConn creates an admin.Conn directly (bypassing the pool)
// for testing Conn-level methods like queryWithRetry.
func newTestDirectConn(t *testing.T, server *fakepgserver.Server) *Conn {
	t.Helper()
	ctx := context.Background()
	clientConn, err := client.Connect(ctx, ctx, server.ClientConfig())
	require.NoError(t, err)
	return NewConn(clientConn)
}

func TestQueryWithRetry_Success(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	server.AddQuery("SELECT 1", fakepgserver.MakeResult([]string{"col"}, [][]any{{"1"}}))

	conn := newTestDirectConn(t, server)
	defer conn.Close()

	results, err := conn.queryWithRetry(context.Background(), "SELECT 1")
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Len(t, results[0].Rows, 1)
	assert.Equal(t, "1", string(results[0].Rows[0].Values[0]))
}

func TestQueryWithRetry_NonConnectionError(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	// The server converts generic errors to PgDiagnostic{Code: "XX000"},
	// which is NOT a connection error. queryWithRetry should not retry.
	server.AddRejectedQuery("SELECT bad_query", errors.New("relation does not exist"))

	conn := newTestDirectConn(t, server)
	defer conn.Close()

	_, err := conn.queryWithRetry(context.Background(), "SELECT bad_query")
	require.Error(t, err)

	// Verify it's not a connection error (no retry happened).
	assert.False(t, mterrors.IsConnectionError(err))

	// Connection should still be open (non-connection errors don't close it).
	assert.False(t, conn.IsClosed())
}

func TestQueryWithRetry_ReconnectsOnConnectionError(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	server.OrderMatters()

	// First attempt: connection error (admin_shutdown).
	server.AddExpectedExecuteFetch(fakepgserver.ExpectedExecuteFetch{
		Query: "SELECT 1",
		Error: &mterrors.PgDiagnostic{
			MessageType: 'E',
			Severity:    "FATAL",
			Code:        "57P01",
			Message:     "terminating connection due to administrator command",
		},
	})

	// Second attempt (after reconnect): success.
	server.AddExpectedExecuteFetch(fakepgserver.ExpectedExecuteFetch{
		Query:       "SELECT 1",
		QueryResult: fakepgserver.MakeResult([]string{"col"}, [][]any{{"1"}}),
	})

	conn := newTestDirectConn(t, server)
	defer conn.Close()

	results, err := conn.queryWithRetry(context.Background(), "SELECT 1")
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "1", string(results[0].Rows[0].Values[0]))

	server.VerifyAllExecutedOrFail()
}

func TestQueryWithRetry_ClosesConnAfterMaxAttempts(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	server.OrderMatters()

	connErr := &mterrors.PgDiagnostic{
		MessageType: 'E',
		Severity:    "FATAL",
		Code:        "57P01",
		Message:     "terminating connection due to administrator command",
	}

	// All 3 attempts fail with connection error.
	for range maxQueryAttempts {
		server.AddExpectedExecuteFetch(fakepgserver.ExpectedExecuteFetch{
			Query: "SELECT 1",
			Error: connErr,
		})
	}

	conn := newTestDirectConn(t, server)

	_, err := conn.queryWithRetry(context.Background(), "SELECT 1")
	require.Error(t, err)
	assert.True(t, mterrors.IsConnectionError(err))

	// Connection should be closed after exhausting all attempts.
	assert.True(t, conn.IsClosed())

	server.VerifyAllExecutedOrFail()
}

func TestQueryWithRetry_ReconnectFails(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	server.OrderMatters()

	// First attempt: connection error.
	server.AddExpectedExecuteFetch(fakepgserver.ExpectedExecuteFetch{
		Query: "SELECT 1",
		Error: &mterrors.PgDiagnostic{
			MessageType: 'E',
			Severity:    "FATAL",
			Code:        "57P01",
			Message:     "terminating connection due to administrator command",
		},
	})

	conn := newTestDirectConn(t, server)

	// Close the listener so reconnect's dial will fail.
	// Existing connections are not affected.
	server.CloseListener()

	_, err := conn.queryWithRetry(context.Background(), "SELECT 1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reconnect dial failed")

	// Connection should be closed after reconnect failure.
	assert.True(t, conn.IsClosed())

	server.VerifyAllExecutedOrFail()
}

// --- execBackendFunc retry tests (via TerminateBackend/CancelBackend) ---

func TestTerminateBackend_ReconnectsOnConnectionError(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	server.OrderMatters()

	// First attempt: connection error.
	server.AddExpectedExecuteFetch(fakepgserver.ExpectedExecuteFetch{
		Query: "SELECT pg_terminate_backend(12345)",
		Error: &mterrors.PgDiagnostic{
			MessageType: 'E',
			Severity:    "FATAL",
			Code:        "57P01",
			Message:     "terminating connection due to administrator command",
		},
	})

	// Second attempt (after reconnect): success.
	server.AddExpectedExecuteFetch(fakepgserver.ExpectedExecuteFetch{
		Query:       "SELECT pg_terminate_backend(12345)",
		QueryResult: fakepgserver.MakeResult([]string{"pg_terminate_backend"}, [][]any{{"t"}}),
	})

	conn := newTestDirectConn(t, server)
	defer conn.Close()

	success, err := conn.TerminateBackend(context.Background(), 12345)
	require.NoError(t, err)
	assert.True(t, success)

	server.VerifyAllExecutedOrFail()
}

// TestQueryWithRetry_ContextCancelledWhileBlocked is a regression test for the
// action lock bug: previously, when a PostgreSQL backend stalled (e.g. an INSERT
// waiting for synchronous standby acknowledgement), the admin conn goroutine
// remained stuck in readMessage() after the caller's context expired, holding
// the action lock and causing all subsequent RPCs that need the lock to time out.
//
// With execQueryWithContextCancel, context cancellation force-closes the connection
// to unblock readMessage, and the call returns promptly with context.Canceled.
func TestQueryWithRetry_ContextCancelledWhileBlocked(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	// blockCh simulates a stalled PostgreSQL backend. The server withholds its
	// response until blockCh is closed, mimicking an INSERT waiting for a
	// synchronous standby that never acknowledges.
	blockCh := make(chan struct{})
	serverReceived := make(chan struct{})

	server.AddQueryPatternWithCallback(
		`SELECT pg_sleep\(100\)`,
		fakepgserver.MakeResult([]string{"pg_sleep"}, [][]any{{""}}),
		func(_ string) {
			close(serverReceived) // signal: server accepted the query and is now stalled
			<-blockCh
		},
	)

	conn := newTestDirectConn(t, server)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, err := conn.QueryWithRetry(ctx, "SELECT pg_sleep(100)")
		errCh <- err
	}()

	// Wait until the server has stalled, then cancel — this is the trigger that
	// should force-close the connection and unblock the goroutine in readMessage.
	<-serverReceived
	cancel()

	// QueryWithRetry must return promptly, not hang indefinitely holding the lock.
	select {
	case err := <-errCh:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("QueryWithRetry did not return after context cancellation")
	}

	// After force-close, IsClosed() must be true so that Recycle() replaces the
	// connection rather than returning a broken one to the idle pool.
	assert.True(t, conn.IsClosed())

	// Release the blocked server goroutine so it can exit cleanly.
	close(blockCh)
}

func TestCancelBackend_ReconnectsOnConnectionError(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	server.OrderMatters()

	// First attempt: connection error.
	server.AddExpectedExecuteFetch(fakepgserver.ExpectedExecuteFetch{
		Query: "SELECT pg_cancel_backend(12345)",
		Error: &mterrors.PgDiagnostic{
			MessageType: 'E',
			Severity:    "FATAL",
			Code:        "57P01",
			Message:     "terminating connection due to administrator command",
		},
	})

	// Second attempt (after reconnect): success.
	server.AddExpectedExecuteFetch(fakepgserver.ExpectedExecuteFetch{
		Query:       "SELECT pg_cancel_backend(12345)",
		QueryResult: fakepgserver.MakeResult([]string{"pg_cancel_backend"}, [][]any{{"t"}}),
	})

	conn := newTestDirectConn(t, server)
	defer conn.Close()

	success, err := conn.CancelBackend(context.Background(), 12345)
	require.NoError(t, err)
	assert.True(t, success)

	server.VerifyAllExecutedOrFail()
}
