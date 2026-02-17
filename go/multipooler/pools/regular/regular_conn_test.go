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

package regular

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/fakepgserver"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/multipooler/connstate"
	"github.com/multigres/multigres/go/pb/query"
)

// --- Test helpers ---

// newTestDirectConn creates a regular.Conn directly (bypassing the pool)
// for testing Conn-level methods like QueryWithRetry, Reconnect, etc.
func newTestDirectConn(t *testing.T, server *fakepgserver.Server) *Conn {
	t.Helper()
	ctx := context.Background()
	clientConn, err := client.Connect(ctx, ctx, server.ClientConfig())
	require.NoError(t, err)
	return NewConn(clientConn, nil /* adminPool */)
}

// connErrFATAL returns a PgDiagnostic for FATAL 57P01 (admin_shutdown),
// which is a connection error that triggers retry/reconnect.
func connErrFATAL() *mterrors.PgDiagnostic {
	return &mterrors.PgDiagnostic{
		MessageType: 'E',
		Severity:    "FATAL",
		Code:        "57P01",
		Message:     "terminating connection due to administrator command",
	}
}

// --- QueryWithRetry tests ---

func TestQueryWithRetry_Success(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	server.AddQuery("SELECT 1", fakepgserver.MakeResult([]string{"col"}, [][]any{{"1"}}))

	conn := newTestDirectConn(t, server)
	defer conn.Close()

	results, err := conn.QueryWithRetry(context.Background(), "SELECT 1")
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Len(t, results[0].Rows, 1)
	assert.Equal(t, "1", string(results[0].Rows[0].Values[0]))
}

func TestQueryWithRetry_NonConnectionError(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	// fakepgserver converts generic errors to PgDiagnostic{Code: "XX000"},
	// which is NOT a connection error. QueryWithRetry should not retry.
	server.AddRejectedQuery("SELECT bad", errors.New("relation does not exist"))

	conn := newTestDirectConn(t, server)
	defer conn.Close()

	_, err := conn.QueryWithRetry(context.Background(), "SELECT bad")
	require.Error(t, err)
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
		Error: connErrFATAL(),
	})

	// Second attempt (after reconnect): success.
	server.AddExpectedExecuteFetch(fakepgserver.ExpectedExecuteFetch{
		Query:       "SELECT 1",
		QueryResult: fakepgserver.MakeResult([]string{"col"}, [][]any{{"1"}}),
	})

	conn := newTestDirectConn(t, server)
	defer conn.Close()

	results, err := conn.QueryWithRetry(context.Background(), "SELECT 1")
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "1", string(results[0].Rows[0].Values[0]))

	server.VerifyAllExecutedOrFail()
}

func TestQueryWithRetry_ClosesConnAfterMaxAttempts(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	server.OrderMatters()

	// All 3 attempts fail with connection error.
	for range maxQueryAttempts {
		server.AddExpectedExecuteFetch(fakepgserver.ExpectedExecuteFetch{
			Query: "SELECT 1",
			Error: connErrFATAL(),
		})
	}

	conn := newTestDirectConn(t, server)

	_, err := conn.QueryWithRetry(context.Background(), "SELECT 1")
	require.Error(t, err)
	assert.True(t, mterrors.IsConnectionError(err))

	// Connection should be closed after exhausting all attempts.
	assert.True(t, conn.IsClosed())

	server.VerifyAllExecutedOrFail()
}

func TestQueryWithRetry_StopsOnContextCancellation(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	server.OrderMatters()

	// First attempt: connection error.
	server.AddExpectedExecuteFetch(fakepgserver.ExpectedExecuteFetch{
		Query: "SELECT 1",
		Error: connErrFATAL(),
	})

	conn := newTestDirectConn(t, server)
	defer conn.Close()

	// Cancel context before the retry can proceed.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := conn.QueryWithRetry(ctx, "SELECT 1")
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// --- QueryStreamingWithRetry tests ---

func TestQueryStreamingWithRetry_Success(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	server.AddQuery("SELECT 1", fakepgserver.MakeResult([]string{"col"}, [][]any{{"1"}}))

	conn := newTestDirectConn(t, server)
	defer conn.Close()

	var collected []*sqltypes.Result
	err := conn.QueryStreamingWithRetry(context.Background(), "SELECT 1", func(_ context.Context, r *sqltypes.Result) error {
		collected = append(collected, r)
		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, collected)
}

func TestQueryStreamingWithRetry_NonConnectionError(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	server.AddRejectedQuery("SELECT bad", errors.New("relation does not exist"))

	conn := newTestDirectConn(t, server)
	defer conn.Close()

	err := conn.QueryStreamingWithRetry(context.Background(), "SELECT bad", func(_ context.Context, _ *sqltypes.Result) error {
		return nil
	})
	require.Error(t, err)
	assert.False(t, mterrors.IsConnectionError(err))
	assert.False(t, conn.IsClosed())
}

func TestQueryStreamingWithRetry_ReconnectsOnConnectionError(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	server.OrderMatters()

	// First attempt: connection error.
	server.AddExpectedExecuteFetch(fakepgserver.ExpectedExecuteFetch{
		Query: "SELECT 1",
		Error: connErrFATAL(),
	})

	// Second attempt (after reconnect): success.
	server.AddExpectedExecuteFetch(fakepgserver.ExpectedExecuteFetch{
		Query:       "SELECT 1",
		QueryResult: fakepgserver.MakeResult([]string{"col"}, [][]any{{"1"}}),
	})

	conn := newTestDirectConn(t, server)
	defer conn.Close()

	var collected []*sqltypes.Result
	err := conn.QueryStreamingWithRetry(context.Background(), "SELECT 1", func(_ context.Context, r *sqltypes.Result) error {
		collected = append(collected, r)
		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, collected)

	server.VerifyAllExecutedOrFail()
}

func TestQueryStreamingWithRetry_ClosesConnAfterMaxAttempts(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	server.OrderMatters()

	for range maxQueryAttempts {
		server.AddExpectedExecuteFetch(fakepgserver.ExpectedExecuteFetch{
			Query: "SELECT 1",
			Error: connErrFATAL(),
		})
	}

	conn := newTestDirectConn(t, server)

	err := conn.QueryStreamingWithRetry(context.Background(), "SELECT 1", func(_ context.Context, _ *sqltypes.Result) error {
		return nil
	})
	require.Error(t, err)
	assert.True(t, mterrors.IsConnectionError(err))
	assert.True(t, conn.IsClosed())

	server.VerifyAllExecutedOrFail()
}

func TestQueryStreamingWithRetry_SentinelIsNotConnectionError(t *testing.T) {
	// The errStreamingAlreadyStarted sentinel is used internally to stop the
	// retry loop. It must NOT be classified as a connection error, otherwise
	// retryOnConnectionError would keep retrying instead of stopping.
	assert.False(t, mterrors.IsConnectionError(errStreamingAlreadyStarted),
		"errStreamingAlreadyStarted must not be classified as a connection error")
}

func TestQueryStreamingWithRetry_PostSwapPreservesOriginalError(t *testing.T) {
	// After the retry loop stops via the sentinel, QueryStreamingWithRetry
	// replaces it with mterrors.Wrapf(streamErr, ...) so that callers can
	// inspect the original PostgreSQL error via errors.As.
	originalErr := connErrFATAL()
	swapped := mterrors.Wrapf(originalErr, "streaming already started, cannot retry")

	// The swapped error should expose the original PgDiagnostic.
	var diag *mterrors.PgDiagnostic
	assert.True(t, errors.As(swapped, &diag), "callers should be able to extract PgDiagnostic")
	assert.Equal(t, "57P01", diag.Code)

	// The sentinel should NOT be in the chain (it was replaced).
	assert.False(t, errors.Is(swapped, errStreamingAlreadyStarted))
}

func TestQueryStreamingWithRetry_MidStreamFailureNoRetry(t *testing.T) {
	// Simulate a mid-stream failure: the server delivers rows (invoking the
	// streaming callback), then the connection dies. QueryStreamingWithRetry
	// must NOT retry (which would send duplicate rows) and must return the
	// original PostgreSQL error.
	server := fakepgserver.New(t)
	defer server.Close()

	server.OrderMatters()

	// First attempt: server delivers rows then fails with FATAL.
	server.AddExpectedExecuteFetch(fakepgserver.ExpectedExecuteFetch{
		Query:              "SELECT 1",
		QueryResult:        fakepgserver.MakeResult([]string{"col"}, [][]any{{"1"}}),
		AfterCallbackError: connErrFATAL(),
	})

	conn := newTestDirectConn(t, server)
	defer conn.Close()

	var callbackCount int
	err := conn.QueryStreamingWithRetry(context.Background(), "SELECT 1", func(_ context.Context, r *sqltypes.Result) error {
		callbackCount++
		return nil
	})

	// Should return an error wrapping the original FATAL.
	require.Error(t, err)
	var diag *mterrors.PgDiagnostic
	assert.True(t, errors.As(err, &diag), "error should contain the original PgDiagnostic")
	assert.Equal(t, "57P01", diag.Code)

	// Callback should have been invoked exactly once (no duplicate rows).
	assert.Equal(t, 1, callbackCount, "callback should be invoked exactly once, no retry")

	server.VerifyAllExecutedOrFail()
}

// --- Reconnect tests ---

func TestReconnect_Success(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	conn := newTestDirectConn(t, server)
	defer conn.Close()

	// Reconnect should succeed.
	err := conn.Reconnect(context.Background())
	require.NoError(t, err)

	// Connection should be usable after reconnect.
	assert.False(t, conn.IsClosed())
}

func TestReconnect_ClearsPreparedStatements(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	conn := newTestDirectConn(t, server)
	defer conn.Close()

	// Simulate having a prepared statement.
	conn.State().StorePreparedStatement(&query.PreparedStatement{
		Name:  "stmt1",
		Query: "SELECT $1",
	})
	require.NotNil(t, conn.State().GetPreparedStatement("stmt1"))

	// Reconnect should reset state.
	err := conn.Reconnect(context.Background())
	require.NoError(t, err)

	// Prepared statement should be gone after reconnect.
	assert.Nil(t, conn.State().GetPreparedStatement("stmt1"))
}

func TestReconnect_ReappliesSettings(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	// Accept SET commands for re-applying settings.
	server.AddQueryPattern(`SET SESSION .+ = .+`, &sqltypes.Result{})

	conn := newTestDirectConn(t, server)
	defer conn.Close()

	// Apply settings to the connection.
	settings := connstate.NewSettings(map[string]string{
		"search_path": "public",
	}, 0)
	conn.State().SetSettings(settings)

	// Reconnect should re-apply the settings.
	err := conn.Reconnect(context.Background())
	require.NoError(t, err)

	// Settings should be preserved after reconnect.
	assert.Equal(t, settings, conn.Settings())

	// Verify SET was actually called during reconnect.
	assert.Greater(t, server.GetPatternCalledNum(`SET SESSION .+ = .+`), 0)
}

func TestReconnect_NoSettingsToReapply(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()
	server.SetNeverFail(true)

	conn := newTestDirectConn(t, server)
	defer conn.Close()

	// No settings applied - reconnect should not try SET commands.
	err := conn.Reconnect(context.Background())
	require.NoError(t, err)

	assert.Nil(t, conn.Settings())
}

func TestQueryWithRetry_ReappliesSettingsAfterReconnect(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	server.OrderMatters()

	// First attempt: connection error.
	server.AddExpectedExecuteFetch(fakepgserver.ExpectedExecuteFetch{
		Query: "SELECT 1",
		Error: connErrFATAL(),
	})

	// Reconnect re-applies settings via SET.
	server.AddExpectedExecuteFetch(fakepgserver.ExpectedExecuteFetch{
		Query:       "SET SESSION search_path = 'public'",
		QueryResult: &sqltypes.Result{},
	})

	// Second attempt (after reconnect): success.
	server.AddExpectedExecuteFetch(fakepgserver.ExpectedExecuteFetch{
		Query:       "SELECT 1",
		QueryResult: fakepgserver.MakeResult([]string{"col"}, [][]any{{"1"}}),
	})

	conn := newTestDirectConn(t, server)
	defer conn.Close()

	// Apply settings before the query.
	settings := connstate.NewSettings(map[string]string{
		"search_path": "public",
	}, 0)
	conn.State().SetSettings(settings)

	results, err := conn.QueryWithRetry(context.Background(), "SELECT 1")
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "1", string(results[0].Rows[0].Values[0]))

	// Settings should still be present after reconnect+retry.
	assert.Equal(t, settings, conn.Settings())

	server.VerifyAllExecutedOrFail()
}

// --- QueryArgsWithRetry tests ---

func TestQueryArgsWithRetry_Success(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	server.AddQuery("SELECT $1", fakepgserver.MakeResult([]string{"col"}, [][]any{{"hello"}}))

	conn := newTestDirectConn(t, server)
	defer conn.Close()

	results, err := conn.QueryArgsWithRetry(context.Background(), "SELECT $1", "hello")
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Len(t, results[0].Rows, 1)
	assert.Equal(t, "hello", string(results[0].Rows[0].Values[0]))
}

func TestQueryArgsWithRetry_NonConnectionError(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	server.AddRejectedQuery("SELECT $1", errors.New("relation does not exist"))

	conn := newTestDirectConn(t, server)
	defer conn.Close()

	_, err := conn.QueryArgsWithRetry(context.Background(), "SELECT $1", "hello")
	require.Error(t, err)
	assert.False(t, mterrors.IsConnectionError(err))
	assert.False(t, conn.IsClosed())
}

func TestQueryArgsWithRetry_ReconnectsOnConnectionError(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	server.OrderMatters()

	// First attempt: connection error.
	server.AddExpectedExecuteFetch(fakepgserver.ExpectedExecuteFetch{
		Query: "SELECT $1",
		Error: connErrFATAL(),
	})

	// Second attempt (after reconnect): success.
	server.AddExpectedExecuteFetch(fakepgserver.ExpectedExecuteFetch{
		Query:       "SELECT $1",
		QueryResult: fakepgserver.MakeResult([]string{"col"}, [][]any{{"hello"}}),
	})

	conn := newTestDirectConn(t, server)
	defer conn.Close()

	results, err := conn.QueryArgsWithRetry(context.Background(), "SELECT $1", "hello")
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "hello", string(results[0].Rows[0].Values[0]))

	server.VerifyAllExecutedOrFail()
}

func TestQueryArgsWithRetry_ClosesConnAfterMaxAttempts(t *testing.T) {
	server := fakepgserver.New(t)
	defer server.Close()

	server.OrderMatters()

	for range maxQueryAttempts {
		server.AddExpectedExecuteFetch(fakepgserver.ExpectedExecuteFetch{
			Query: "SELECT $1",
			Error: connErrFATAL(),
		})
	}

	conn := newTestDirectConn(t, server)

	_, err := conn.QueryArgsWithRetry(context.Background(), "SELECT $1", "hello")
	require.Error(t, err)
	assert.True(t, mterrors.IsConnectionError(err))
	assert.True(t, conn.IsClosed())

	server.VerifyAllExecutedOrFail()
}
