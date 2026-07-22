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

package handler

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerservice "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
)

// mockExecutor is a mock implementation of the Executor interface for testing.
type mockExecutor struct {
	streamExecuteErr         error
	eagerParseErr            error
	eagerParseCalls          int
	releaseAllCalled         bool
	portalStreamExecuteCalls int
	describeCalls            int
	describedStatement       *preparedstatement.PreparedStatementInfo
}

func (m *mockExecutor) StreamExecute(ctx context.Context, conn *server.Conn, state *MultigatewayConnectionState, queryStr string, astStmt ast.Stmt, callback func(ctx context.Context, result *sqltypes.Result) error) (*ExecuteResult, error) {
	if m.streamExecuteErr != nil {
		// Return partial result with PlanTime, matching real executor behavior.
		return &ExecuteResult{PlanTime: time.Microsecond}, m.streamExecuteErr
	}
	// Return a simple test result
	err := callback(ctx, &sqltypes.Result{
		Fields: []*query.Field{
			{Name: "column1", Type: "int4"},
		},
		Rows: []*sqltypes.Row{
			{Values: []sqltypes.Value{[]byte("1")}},
		},
		CommandTag:   "SELECT 1",
		RowsAffected: 1,
	})
	return &ExecuteResult{}, err
}

func (m *mockExecutor) PortalStreamExecute(ctx context.Context, conn *server.Conn, state *MultigatewayConnectionState, portalInfo *preparedstatement.PortalInfo, maxRows int32, _ bool, callback func(ctx context.Context, result *sqltypes.Result) error) (*ExecuteResult, error) {
	m.portalStreamExecuteCalls++
	// Return a simple test result
	err := callback(ctx, &sqltypes.Result{
		Fields: []*query.Field{
			{Name: "column1", Type: "int4"},
		},
		Rows: []*sqltypes.Row{
			{Values: []sqltypes.Value{[]byte("1")}},
		},
		CommandTag:   "SELECT 1",
		RowsAffected: 1,
	})
	return &ExecuteResult{}, err
}

func (m *mockExecutor) Describe(ctx context.Context, conn *server.Conn, state *MultigatewayConnectionState, portalInfo *preparedstatement.PortalInfo, preparedStatementInfo *preparedstatement.PreparedStatementInfo) (*query.StatementDescription, error) {
	m.describeCalls++
	m.describedStatement = preparedStatementInfo
	var parameters []*query.ParameterDescription
	if preparedStatementInfo != nil {
		parameters = parameterDescriptions(preparedStatementInfo.ParamTypes)
	}
	return &query.StatementDescription{
		Parameters: parameters,
		Fields:     []*query.Field{{Name: "test_field", Type: "int4"}},
	}, nil
}

func parameterOIDs(parameters []*query.ParameterDescription) []uint32 {
	if len(parameters) == 0 {
		return nil
	}
	oids := make([]uint32, len(parameters))
	for i, parameter := range parameters {
		oids[i] = parameter.DataTypeOid
	}
	return oids
}

func (m *mockExecutor) EagerParseInTransaction(ctx context.Context, conn *server.Conn, state *MultigatewayConnectionState, queryStr string, paramTypes []uint32) error {
	m.eagerParseCalls++
	return m.eagerParseErr
}

func (m *mockExecutor) ReleaseAll(ctx context.Context, conn *server.Conn, state *MultigatewayConnectionState) error {
	m.releaseAllCalled = true
	return nil
}

func (m *mockExecutor) StreamReplication(ctx context.Context, conn *server.Conn, state *MultigatewayConnectionState, init *multipoolerservice.StreamReplicationInit) (multipoolerservice.MultipoolerService_StreamReplicationClient, error) {
	return nil, nil
}

// TestHandleQueryEmptyQuery tests that empty queries are handled correctly.
func TestMultigatewayHandler_IdleSessionTimeoutProvider(t *testing.T) {
	h := NewMultigatewayHandler(&mockExecutor{}, slog.Default(), 0)
	testConn := server.NewTestConn(&bytes.Buffer{})
	state := NewMultigatewayConnectionState()
	state.SetIdleSessionTimeout(5 * time.Second)
	testConn.SetConnectionState(state)

	require.Equal(t, 5*time.Second, h.IdleSessionTimeout(testConn.Conn))
}

func TestHandleQueryEmptyQuery(t *testing.T) {
	logger := slog.Default()
	executor := &mockExecutor{}
	handler := NewMultigatewayHandler(executor, logger, 0)

	// Create a mock connection
	conn := &server.Conn{}

	tests := []struct {
		name      string
		query     string
		wantNil   bool
		wantError bool
	}{
		{
			name:      "empty query - just semicolon",
			query:     ";",
			wantNil:   true,
			wantError: false,
		},
		{
			name:      "empty query - whitespace and semicolon",
			query:     "  ;  ",
			wantNil:   true,
			wantError: false,
		},
		{
			name:      "empty query - multiple semicolons",
			query:     ";;",
			wantNil:   true,
			wantError: false,
		},
		{
			name:      "empty query - whitespace only",
			query:     "   ",
			wantNil:   true,
			wantError: false,
		},
		{
			name:      "empty query - tabs and newlines",
			query:     "\t\n  \n\t",
			wantNil:   true,
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			callbackCalled := false
			var receivedResult *sqltypes.Result

			err := handler.HandleQuery(context.Background(), conn, tt.query, func(ctx context.Context, result *sqltypes.Result) error {
				callbackCalled = true
				receivedResult = result
				return nil
			})

			if tt.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.True(t, callbackCalled, "HandleQuery() callback was not called")

			if tt.wantNil {
				require.Nil(t, receivedResult, "HandleQuery() callback should receive nil result for empty query")
			}
		})
	}
}

// TestHandleQueryNonEmpty tests that non-empty queries are handled correctly.
func TestHandleQueryNonEmpty(t *testing.T) {
	logger := slog.Default()
	executor := &mockExecutor{}
	handler := NewMultigatewayHandler(executor, logger, 0)

	// Create a mock connection
	conn := &server.Conn{}

	callbackCalled := false
	var receivedResult *sqltypes.Result

	err := handler.HandleQuery(context.Background(), conn, "SELECT 1", func(ctx context.Context, result *sqltypes.Result) error {
		callbackCalled = true
		receivedResult = result
		return nil
	})

	require.NoError(t, err)
	require.True(t, callbackCalled, "HandleQuery() callback was not called")
	require.NotNil(t, receivedResult, "HandleQuery() callback should receive non-nil result for valid query")
}

func TestHandleQueryForwardsParserDiagnostics(t *testing.T) {
	h := NewMultigatewayHandler(&mockExecutor{}, slog.Default(), 0)

	t.Run("warning", func(t *testing.T) {
		conn := server.NewTestConn(&bytes.Buffer{})
		state := NewMultigatewayConnectionState()
		state.SetSessionVariable("standard_conforming_strings", "off")
		conn.SetConnectionState(state)

		var results []*sqltypes.Result
		err := h.HandleQuery(context.Background(), conn.Conn, `SELECT 'a\\b'`, func(_ context.Context, result *sqltypes.Result) error {
			results = append(results, result)
			return nil
		})
		require.NoError(t, err)
		require.Len(t, results, 2)
		require.Len(t, results[0].Notices, 1)
		warning := results[0].Notices[0]
		require.Equal(t, mterrors.PgSSNonstandardUseOfEscapeCharacter, warning.Code)
		require.Equal(t, `nonstandard use of \\ in a string literal`, warning.Message)
		require.NotEmpty(t, warning.Hint)
		require.Positive(t, warning.Position)
	})

	t.Run("error detail", func(t *testing.T) {
		conn := server.NewTestConn(&bytes.Buffer{})
		state := NewMultigatewayConnectionState()
		state.SetSessionVariable("standard_conforming_strings", "off")
		conn.SetConnectionState(state)

		err := h.HandleQuery(context.Background(), conn.Conn, `SELECT U&'\0061'`, func(_ context.Context, _ *sqltypes.Result) error { return nil })
		require.Error(t, err)
		var diag *mterrors.PgDiagnostic
		require.ErrorAs(t, err, &diag)
		require.Equal(t, mterrors.PgSSFeatureNotSupported, diag.Code)
		require.Equal(t, `String constants with Unicode escapes cannot be used when "standard_conforming_strings" is off.`, diag.Detail)
	})
}

// TestPreparedStatementHandling tests the full lifecycle of prepared statements:
// parse, describe, close, and error cases.
func TestPreparedStatementHandling(t *testing.T) {
	logger := slog.Default()
	executor := &mockExecutor{}
	handler := NewMultigatewayHandler(executor, logger, 0)
	conn := &server.Conn{}
	ctx := context.Background()

	// 1. Parse stores the statement in the consolidator
	err := handler.HandleParse(ctx, conn, "stmt1", "SELECT $1::int", []uint32{23})
	require.NoError(t, err)

	// 2. Describe finds it in the consolidator
	desc, err := handler.HandleDescribe(ctx, conn, 'S', "stmt1")
	require.NoError(t, err)
	require.NotNil(t, desc)

	// 3. Re-parsing with the same name replaces (matches PostgreSQL behavior).
	// This is needed when Parse succeeds but Describe fails — the client retries.
	err = handler.HandleParse(ctx, conn, "stmt1", "SELECT 2", nil)
	require.NoError(t, err)

	// 4. Unnamed statements can be overwritten
	err = handler.HandleParse(ctx, conn, "", "SELECT 1", nil)
	require.NoError(t, err)
	err = handler.HandleParse(ctx, conn, "", "SELECT 2", nil)
	require.NoError(t, err)

	// 5. Close removes from consolidator
	err = handler.HandleClose(ctx, conn, 'S', "stmt1")
	require.NoError(t, err)

	// 6. Describe fails after close
	_, err = handler.HandleDescribe(ctx, conn, 'S', "stmt1")
	require.Error(t, err)
	require.True(t, mterrors.IsErrorCode(err, mterrors.PgSSInvalidSQLStatementName))

	// 7. Empty query is accepted as an empty prepared statement (matches PostgreSQL).
	err = handler.HandleParse(ctx, conn, "empty", "", nil)
	require.NoError(t, err)
	emptyPSI := handler.psc.GetPreparedStatementInfo(conn.ConnectionID(), "empty")
	require.NotNil(t, emptyPSI)
	require.True(t, emptyPSI.IsEmpty())

	// 8. Invalid describe type fails
	_, err = handler.HandleDescribe(ctx, conn, 'X', "name")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid describe type")

	// 9. Invalid close type fails
	err = handler.HandleClose(ctx, conn, 'X', "name")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid close type")
}

func TestHandleDescribeSQLExecuteUsesTargetStatement(t *testing.T) {
	t.Run("statement", func(t *testing.T) {
		exec := &mockExecutor{}
		h := NewMultigatewayHandler(exec, slog.Default(), 0)
		conn := server.NewTestConn(&bytes.Buffer{}).Conn

		require.NoError(t, h.HandleParse(t.Context(), conn, "test", "SELECT 1 AS first, 2 AS second", nil))
		require.NoError(t, h.HandleParse(t.Context(), conn, "describe_execute", "EXECUTE test", nil))
		_, err := h.HandleDescribe(t.Context(), conn, 'S', "describe_execute")
		require.NoError(t, err)
		require.NotNil(t, exec.describedStatement)
		require.Equal(t, "SELECT 1 AS first, 2 AS second", exec.describedStatement.Query)
	})

	t.Run("wrapper parameters", func(t *testing.T) {
		for _, tc := range []struct {
			name         string
			wrapper      string
			wrapperTypes []uint32
			wantTypes    []uint32
		}{
			{name: "literal argument", wrapper: "EXECUTE test(1)"},
			{name: "protocol parameter", wrapper: "EXECUTE test($1)", wrapperTypes: []uint32{uint32(ast.INT4OID)}, wantTypes: []uint32{uint32(ast.INT4OID)}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				exec := &mockExecutor{}
				h := NewMultigatewayHandler(exec, slog.Default(), 0)
				conn := server.NewTestConn(&bytes.Buffer{}).Conn

				require.NoError(t, h.HandleParse(t.Context(), conn, "test", "SELECT $1::text", []uint32{uint32(ast.TEXTOID)}))
				require.NoError(t, h.HandleParse(t.Context(), conn, "describe_execute", tc.wrapper, tc.wrapperTypes))
				description, err := h.HandleDescribe(t.Context(), conn, 'S', "describe_execute")
				require.NoError(t, err)
				require.Equal(t, tc.wantTypes, parameterOIDs(description.Parameters))
			})
		}
	})

	t.Run("non execute", func(t *testing.T) {
		exec := &mockExecutor{}
		h := NewMultigatewayHandler(exec, slog.Default(), 0)
		conn := server.NewTestConn(&bytes.Buffer{}).Conn

		require.NoError(t, h.HandleParse(t.Context(), conn, "plain", "SELECT 1", nil))
		_, err := h.HandleDescribe(t.Context(), conn, 'S', "plain")
		require.NoError(t, err)
		require.NotNil(t, exec.describedStatement)
		require.Equal(t, "SELECT 1", exec.describedStatement.Query)
	})

	t.Run("missing target", func(t *testing.T) {
		h := NewMultigatewayHandler(&mockExecutor{}, slog.Default(), 0)
		conn := server.NewTestConn(&bytes.Buffer{}).Conn

		require.NoError(t, h.HandleParse(t.Context(), conn, "describe_execute", "EXECUTE missing", nil))
		_, err := h.HandleDescribe(t.Context(), conn, 'S', "describe_execute")
		require.Error(t, err)
		require.True(t, mterrors.IsErrorCode(err, mterrors.PgSSInvalidSQLStatementName))
	})

	t.Run("portal", func(t *testing.T) {
		exec := &mockExecutor{}
		h := NewMultigatewayHandler(exec, slog.Default(), 0)
		conn := server.NewTestConn(&bytes.Buffer{}).Conn

		require.NoError(t, h.HandleParse(t.Context(), conn, "test", "SELECT 1 AS first, 2 AS second", nil))
		require.NoError(t, h.HandleParse(t.Context(), conn, "describe_execute", "EXECUTE test", nil))
		require.NoError(t, h.HandleBind(t.Context(), conn, "portal", "describe_execute", nil, nil, nil))
		_, err := h.HandleDescribe(t.Context(), conn, 'P', "portal")
		require.NoError(t, err)
		require.NotNil(t, exec.describedStatement)
		require.Equal(t, "SELECT 1 AS first, 2 AS second", exec.describedStatement.Query)
	})
}

func TestHandleParseEagerParsesOnlyInsideTransaction(t *testing.T) {
	exec := &mockExecutor{}
	h := NewMultigatewayHandler(exec, slog.Default(), 0)
	conn := server.NewTestConn(&bytes.Buffer{}).Conn
	ctx := context.Background()

	require.NoError(t, h.HandleParse(ctx, conn, "outside", "SELECT 1", nil))
	require.Equal(t, 0, exec.eagerParseCalls)

	conn.SetTxnStatus(protocol.TxnStatusInBlock)
	require.NoError(t, h.HandleParse(ctx, conn, "inside", "SELECT 1", nil))
	require.Equal(t, 1, exec.eagerParseCalls)
}

func TestHandleParseEagerParseErrorAbortsTransactionAndDoesNotRegister(t *testing.T) {
	exec := &mockExecutor{eagerParseErr: errors.New("missing relation")}
	h := NewMultigatewayHandler(exec, slog.Default(), 0)
	conn := server.NewTestConn(&bytes.Buffer{}).Conn
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	err := h.HandleParse(context.Background(), conn, "stmt", "SELECT * FROM missing", nil)
	require.Error(t, err)
	require.Equal(t, protocol.TxnStatusFailed, conn.TxnStatus())
	require.Nil(t, h.psc.GetPreparedStatementInfo(conn.ConnectionID(), "stmt"))
}

// TestPortalHandling tests the full lifecycle of portals:
// bind, describe, execute, close, and error cases.
func TestPortalHandling(t *testing.T) {
	logger := slog.Default()
	executor := &mockExecutor{}
	handler := NewMultigatewayHandler(executor, logger, 0)
	conn := &server.Conn{}
	ctx := context.Background()

	// Setup: parse a statement first
	err := handler.HandleParse(ctx, conn, "stmt1", "SELECT $1::int", []uint32{23})
	require.NoError(t, err)

	// 1. Bind fails for non-existent statement
	err = handler.HandleBind(ctx, conn, "portal1", "nonexistent", nil, nil, nil)
	require.Error(t, err)
	require.True(t, mterrors.IsErrorCode(err, mterrors.PgSSInvalidSQLStatementName))

	// 2. Bind stores portal in connection state
	params := [][]byte{[]byte("42")}
	err = handler.HandleBind(ctx, conn, "portal1", "stmt1", params, []int16{0}, []int16{0})
	require.NoError(t, err)

	// 3. Describe finds the portal
	desc, err := handler.HandleDescribe(ctx, conn, 'P', "portal1")
	require.NoError(t, err)
	require.NotNil(t, desc)

	// 4. Execute works with valid portal
	var result *sqltypes.Result
	err = handler.HandleExecute(ctx, conn, "portal1", 0, false, func(ctx context.Context, r *sqltypes.Result) error {
		result = r
		return nil
	})
	require.NoError(t, err)
	require.NotNil(t, result)

	// 5. Execute fails for non-existent portal
	err = handler.HandleExecute(ctx, conn, "nonexistent", 0, false, func(ctx context.Context, r *sqltypes.Result) error {
		return nil
	})
	require.Error(t, err)
	require.True(t, mterrors.IsErrorCode(err, mterrors.PgSSInvalidCursorName))

	// 6. Close removes portal from connection state
	err = handler.HandleClose(ctx, conn, 'P', "portal1")
	require.NoError(t, err)

	// 7. Describe fails after close
	_, err = handler.HandleDescribe(ctx, conn, 'P', "portal1")
	require.Error(t, err)
	require.True(t, mterrors.IsErrorCode(err, mterrors.PgSSInvalidCursorName))
}

// TestPreparedStatementConsolidation tests that same queries share the same
// underlying statement, and closing one doesn't affect the other.
func TestPreparedStatementConsolidation(t *testing.T) {
	logger := slog.Default()
	executor := &mockExecutor{}
	handler := NewMultigatewayHandler(executor, logger, 0)
	conn := &server.Conn{}
	ctx := context.Background()

	// Parse two statements with same query but different names
	err := handler.HandleParse(ctx, conn, "stmt1", "SELECT 1", nil)
	require.NoError(t, err)
	err = handler.HandleParse(ctx, conn, "stmt2", "SELECT 1", nil)
	require.NoError(t, err)

	// Both should be describable
	_, err = handler.HandleDescribe(ctx, conn, 'S', "stmt1")
	require.NoError(t, err)
	_, err = handler.HandleDescribe(ctx, conn, 'S', "stmt2")
	require.NoError(t, err)

	// Close stmt1 - stmt2 should still work (shared underlying statement)
	err = handler.HandleClose(ctx, conn, 'S', "stmt1")
	require.NoError(t, err)

	// stmt1 gone, but stmt2 still works
	_, err = handler.HandleDescribe(ctx, conn, 'S', "stmt1")
	require.Error(t, err)
	_, err = handler.HandleDescribe(ctx, conn, 'S', "stmt2")
	require.NoError(t, err)

	// Multiple portals from same statement
	err = handler.HandleBind(ctx, conn, "portal1", "stmt2", nil, nil, nil)
	require.NoError(t, err)
	err = handler.HandleBind(ctx, conn, "portal2", "stmt2", nil, nil, nil)
	require.NoError(t, err)

	// Close one portal, other still exists
	err = handler.HandleClose(ctx, conn, 'P', "portal1")
	require.NoError(t, err)
	_, err = handler.HandleDescribe(ctx, conn, 'P', "portal1")
	require.Error(t, err)
	_, err = handler.HandleDescribe(ctx, conn, 'P', "portal2")
	require.NoError(t, err)
}

// TestConnectionStateIsolation tests that portals are isolated per connection.
func TestConnectionStateIsolation(t *testing.T) {
	logger := slog.Default()
	executor := &mockExecutor{}
	handler := NewMultigatewayHandler(executor, logger, 0)
	conn1 := &server.Conn{}
	conn2 := &server.Conn{}
	ctx := context.Background()

	// Parse on conn1
	err := handler.HandleParse(ctx, conn1, "stmt1", "SELECT 1", nil)
	require.NoError(t, err)

	// Bind portal on conn1
	err = handler.HandleBind(ctx, conn1, "portal1", "stmt1", nil, nil, nil)
	require.NoError(t, err)

	// Portal exists on conn1
	_, err = handler.HandleDescribe(ctx, conn1, 'P', "portal1")
	require.NoError(t, err)

	// Portal does NOT exist on conn2 (isolated connection state)
	_, err = handler.HandleDescribe(ctx, conn2, 'P', "portal1")
	require.Error(t, err)
}

// TestHandleQuery_AbortedTransactionRejectsQueries tests that queries are rejected
// when the transaction is in an aborted state (TxnStatusFailed).
func TestHandleQuery_AbortedTransactionRejectsQueries(t *testing.T) {
	logger := slog.Default()
	executor := &mockExecutor{}
	h := NewMultigatewayHandler(executor, logger, 0)
	conn := server.NewTestConn(&bytes.Buffer{}).Conn

	// Put connection in aborted transaction state
	conn.SetTxnStatus(protocol.TxnStatusFailed)

	err := h.HandleQuery(context.Background(), conn, "SELECT 1", func(_ context.Context, _ *sqltypes.Result) error {
		t.Fatal("callback should not be called for aborted transaction")
		return nil
	})

	require.Error(t, err)
	require.True(t, mterrors.IsErrorCode(err, mterrors.PgSSInFailedTransaction))
	// Status should remain Failed
	require.Equal(t, protocol.TxnStatusFailed, conn.TxnStatus())
}

// TestHandleQuery_AbortedTransactionAllowsRollback tests that ROLLBACK is allowed
// when the transaction is in an aborted state.
func TestHandleQuery_AbortedTransactionAllowsRollback(t *testing.T) {
	logger := slog.Default()
	executor := &mockExecutor{}
	h := NewMultigatewayHandler(executor, logger, 0)
	conn := server.NewTestConn(&bytes.Buffer{}).Conn

	// Put connection in aborted transaction state
	conn.SetTxnStatus(protocol.TxnStatusFailed)

	err := h.HandleQuery(context.Background(), conn, "ROLLBACK", func(_ context.Context, _ *sqltypes.Result) error {
		return nil
	})

	// ROLLBACK should be allowed through (executed by the mock)
	require.NoError(t, err)
}

// TestHandleQuery_AbortedTransactionAllowsRollbackInBatch tests that a multi-statement
// batch starting with ROLLBACK is allowed when in aborted state, matching PostgreSQL
// behavior where "ROLLBACK; SELECT 1;" works even in aborted state.
func TestHandleQuery_AbortedTransactionAllowsRollbackInBatch(t *testing.T) {
	logger := slog.Default()
	executor := &mockExecutor{}
	h := NewMultigatewayHandler(executor, logger, 0)
	conn := server.NewTestConn(&bytes.Buffer{}).Conn

	// Put connection in aborted transaction state
	conn.SetTxnStatus(protocol.TxnStatusFailed)

	err := h.HandleQuery(context.Background(), conn, "ROLLBACK; SELECT 1", func(_ context.Context, _ *sqltypes.Result) error {
		return nil
	})

	// Batch starting with ROLLBACK should be allowed through (not rejected at the gate).
	// Transaction state transitions (ROLLBACK clearing aborted state) are verified
	// by integration tests since the mock executor doesn't process transaction statements.
	require.NoError(t, err)
}

// TestHandleQuery_AbortedTransactionRejectsBatchNotStartingWithRollback tests that
// a multi-statement batch NOT starting with ROLLBACK is rejected in aborted state.
func TestHandleQuery_AbortedTransactionRejectsBatchNotStartingWithRollback(t *testing.T) {
	logger := slog.Default()
	executor := &mockExecutor{}
	h := NewMultigatewayHandler(executor, logger, 0)
	conn := server.NewTestConn(&bytes.Buffer{}).Conn

	// Put connection in aborted transaction state
	conn.SetTxnStatus(protocol.TxnStatusFailed)

	err := h.HandleQuery(context.Background(), conn, "SELECT 1; ROLLBACK", func(_ context.Context, _ *sqltypes.Result) error {
		t.Fatal("callback should not be called for aborted transaction")
		return nil
	})

	require.Error(t, err)
	require.True(t, mterrors.IsErrorCode(err, mterrors.PgSSInFailedTransaction))
}

// TestHandleQuery_ErrorInTransactionSetsAbortedState tests that a query error
// while in a transaction transitions the state to aborted.
func TestHandleQuery_ErrorInTransactionSetsAbortedState(t *testing.T) {
	logger := slog.Default()
	executor := &mockExecutor{streamExecuteErr: errors.New("query failed")}
	h := NewMultigatewayHandler(executor, logger, 0)
	conn := server.NewTestConn(&bytes.Buffer{}).Conn

	// Put connection in active transaction
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	err := h.HandleQuery(context.Background(), conn, "SELECT 1", func(_ context.Context, _ *sqltypes.Result) error {
		return nil
	})

	require.Error(t, err)
	// Should transition to aborted state
	require.Equal(t, protocol.TxnStatusFailed, conn.TxnStatus())
}

// TestHandleQuery_ErrorOutsideTransactionStaysIdle tests that a query error
// outside a transaction does not change the transaction state.
func TestHandleQuery_ErrorOutsideTransactionStaysIdle(t *testing.T) {
	logger := slog.Default()
	executor := &mockExecutor{streamExecuteErr: errors.New("query failed")}
	h := NewMultigatewayHandler(executor, logger, 0)
	conn := server.NewTestConn(&bytes.Buffer{}).Conn

	err := h.HandleQuery(context.Background(), conn, "SELECT 1", func(_ context.Context, _ *sqltypes.Result) error {
		return nil
	})

	require.Error(t, err)
	// Should stay idle (not in a transaction)
	require.Equal(t, protocol.TxnStatusIdle, conn.TxnStatus())
}

// TestHandleParse_EmptyAndCommentOnlyAccepted verifies that an empty or
// comment-only query string parses to an empty prepared statement instead of
// being rejected, matching PostgreSQL.
func TestHandleParse_EmptyAndCommentOnlyAccepted(t *testing.T) {
	logger := slog.Default()
	executor := &mockExecutor{}
	h := NewMultigatewayHandler(executor, logger, 0)
	conn := server.NewTestConn(&bytes.Buffer{}).Conn

	for _, q := range []string{"", "-- only a comment\n"} {
		err := h.HandleParse(t.Context(), conn, "", q, nil)
		require.NoError(t, err, "empty/comment-only Parse should be accepted: %q", q)

		psi := h.psc.GetPreparedStatementInfo(conn.ConnectionID(), "")
		require.NotNil(t, psi)
		require.True(t, psi.IsEmpty())
	}
}

// TestHandleBind_EmptyStatementParamCountMismatch verifies that binding the
// wrong number of parameters to an empty (comment-only) prepared statement is
// rejected as a protocol violation (08P01), matching PostgreSQL. For an empty
// statement the gateway authoritatively knows the parameter count (there is no
// query body in which postgres could infer additional parameters), so the
// check belongs at the gateway rather than being deferred to the backend.
func TestHandleBind_EmptyStatementParamCountMismatch(t *testing.T) {
	logger := slog.Default()
	executor := &mockExecutor{}
	h := NewMultigatewayHandler(executor, logger, 0)
	conn := server.NewTestConn(&bytes.Buffer{}).Conn

	// Empty statement declaring zero parameters.
	require.NoError(t, h.HandleParse(t.Context(), conn, "s0", "-- comment\n", nil))

	// Binding any parameter is a protocol violation.
	err := h.HandleBind(t.Context(), conn, "p0", "s0", [][]byte{[]byte("1")}, []int16{0}, nil)
	require.Error(t, err)
	require.True(t, mterrors.IsErrorCode(err, mterrors.PgSSProtocolViolation))

	// Binding the correct (zero) count still succeeds.
	require.NoError(t, h.HandleBind(t.Context(), conn, "p0", "s0", nil, nil, nil))

	// Empty statement that declares parameter types still validates against the
	// declared count: too few is rejected, the exact count succeeds.
	require.NoError(t, h.HandleParse(t.Context(), conn, "s1", "-- comment\n", []uint32{23}))
	err = h.HandleBind(t.Context(), conn, "p1", "s1", nil, nil, nil)
	require.Error(t, err)
	require.True(t, mterrors.IsErrorCode(err, mterrors.PgSSProtocolViolation))
	require.NoError(t, h.HandleBind(t.Context(), conn, "p1", "s1", [][]byte{[]byte("1")}, []int16{0}, nil))
}

// TestHandleDescribe_EmptyStatementAndPortal verifies that describing an empty
// (comment-only) prepared statement or portal returns an empty description
// (rendered as ParameterDescription(0)/NoData) without calling the executor.
func TestHandleDescribe_EmptyStatementAndPortal(t *testing.T) {
	logger := slog.Default()
	executor := &mockExecutor{}
	h := NewMultigatewayHandler(executor, logger, 0)
	conn := server.NewTestConn(&bytes.Buffer{}).Conn

	require.NoError(t, h.HandleParse(t.Context(), conn, "s", "-- comment\n", nil))
	require.NoError(t, h.HandleBind(t.Context(), conn, "p", "s", nil, nil, nil))

	// Describe('S') of an empty statement: zero params, no row fields, no executor call.
	descS, err := h.HandleDescribe(t.Context(), conn, 'S', "s")
	require.NoError(t, err)
	require.NotNil(t, descS)
	require.Empty(t, descS.Parameters)
	require.Nil(t, descS.Fields)

	// Describe('P') of an empty portal: no row fields.
	descP, err := h.HandleDescribe(t.Context(), conn, 'P', "p")
	require.NoError(t, err)
	require.NotNil(t, descP)
	require.Nil(t, descP.Fields)

	require.Zero(t, executor.describeCalls, "executor must not be called for empty describe")
}

// TestHandleExecute_EmptyPortalShortCircuits verifies that executing an empty
// (comment-only) prepared statement answers via a nil result without reaching
// the executor (whose plan path would dereference the nil AST).
func TestHandleExecute_EmptyPortalShortCircuits(t *testing.T) {
	logger := slog.Default()
	executor := &mockExecutor{}
	h := NewMultigatewayHandler(executor, logger, 0)
	conn := server.NewTestConn(&bytes.Buffer{}).Conn

	// Parse + Bind a comment-only statement so the portal carries an empty PSI.
	require.NoError(t, h.HandleParse(t.Context(), conn, "s", "-- nothing\n", nil))
	require.NoError(t, h.HandleBind(t.Context(), conn, "p", "s", nil, nil, nil))

	var gotResult *sqltypes.Result
	gotCalled := false
	err := h.HandleExecute(t.Context(), conn, "p", 0, false, func(ctx context.Context, r *sqltypes.Result) error {
		gotCalled = true
		gotResult = r
		return nil
	})
	require.NoError(t, err)
	require.True(t, gotCalled, "callback must run for the empty query")
	require.Nil(t, gotResult, "empty query signals via nil result")
	require.Zero(t, executor.portalStreamExecuteCalls, "executor must not be called for an empty portal")
}

// TestHandleExecute_AbortedTransactionRejects tests that Execute messages
// are rejected when the transaction is aborted.
func TestHandleExecute_AbortedTransactionRejects(t *testing.T) {
	logger := slog.Default()
	executor := &mockExecutor{}
	h := NewMultigatewayHandler(executor, logger, 0)
	conn := server.NewTestConn(&bytes.Buffer{}).Conn
	ctx := context.Background()

	// Setup a prepared statement and portal
	err := h.HandleParse(ctx, conn, "stmt1", "SELECT 1", nil)
	require.NoError(t, err)
	err = h.HandleBind(ctx, conn, "portal1", "stmt1", nil, nil, nil)
	require.NoError(t, err)

	// Put connection in aborted transaction state
	conn.SetTxnStatus(protocol.TxnStatusFailed)

	err = h.HandleExecute(ctx, conn, "portal1", 0, false, func(_ context.Context, _ *sqltypes.Result) error {
		t.Fatal("callback should not be called for aborted transaction")
		return nil
	})

	require.Error(t, err)
	require.True(t, mterrors.IsErrorCode(err, mterrors.PgSSInFailedTransaction))
}

// TestConnectionClosed_ReleasesReservedConnections tests that ConnectionClosed
// releases all reserved connections when the client disconnects.
func TestConnectionClosed_ReleasesReservedConnections(t *testing.T) {
	logger := slog.Default()
	executor := &mockExecutor{}
	h := NewMultigatewayHandler(executor, logger, 0)
	conn := server.NewTestConn(&bytes.Buffer{}).Conn

	// Initialize connection state with a reserved connection
	state := NewMultigatewayConnectionState()
	conn.SetConnectionState(state)
	conn.SetTxnStatus(protocol.TxnStatusInBlock)
	target := protoutil.NewTarget(constants.DefaultPostgresDatabase, "tg1", "", query.Mode_MODE_WRITABLE)
	state.SetReservedConnection(target, &query.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonTransaction,
	})

	h.ConnectionClosed(conn)

	require.True(t, executor.releaseAllCalled, "ReleaseAll should be called")
}

// TestConnectionClosed_ReleasesNonTransactionReservedConnections tests that
// ConnectionClosed releases reserved connections even when not in a transaction.
func TestConnectionClosed_ReleasesNonTransactionReservedConnections(t *testing.T) {
	logger := slog.Default()
	executor := &mockExecutor{}
	h := NewMultigatewayHandler(executor, logger, 0)
	conn := server.NewTestConn(&bytes.Buffer{}).Conn

	// Initialize connection state with a reserved connection for COPY (not transaction)
	state := NewMultigatewayConnectionState()
	conn.SetConnectionState(state)
	// NOT in a transaction - TxnStatusIdle
	target := protoutil.NewTarget(constants.DefaultPostgresDatabase, "tg1", "", query.Mode_MODE_WRITABLE)
	state.SetReservedConnection(target, &query.ReservedState{
		ReservedConnectionId: 42,
		PoolerId:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		ReservationReasons:   protoutil.ReasonCopy,
	})

	h.ConnectionClosed(conn)

	require.True(t, executor.releaseAllCalled, "ReleaseAll should be called even without active transaction")
}

// TestConnectionClosed_NoReservedConnections tests that ConnectionClosed
// does not call ReleaseAll when there are no reserved connections.
func TestConnectionClosed_NoReservedConnections(t *testing.T) {
	logger := slog.Default()
	executor := &mockExecutor{}
	h := NewMultigatewayHandler(executor, logger, 0)
	conn := server.NewTestConn(&bytes.Buffer{}).Conn

	// Initialize connection state with no reserved connections
	state := NewMultigatewayConnectionState()
	conn.SetConnectionState(state)

	h.ConnectionClosed(conn)

	require.False(t, executor.releaseAllCalled, "ReleaseAll should not be called when no shard states")
}

// TestConnectionClosed_CleansPreparedStatements tests that ConnectionClosed
// removes prepared statements for the connection.
func TestConnectionClosed_CleansPreparedStatements(t *testing.T) {
	logger := slog.Default()
	executor := &mockExecutor{}
	h := NewMultigatewayHandler(executor, logger, 0)
	conn := server.NewTestConn(&bytes.Buffer{}).Conn
	ctx := context.Background()

	// Add some prepared statements
	err := h.HandleParse(ctx, conn, "stmt1", "SELECT 1", nil)
	require.NoError(t, err)
	err = h.HandleParse(ctx, conn, "stmt2", "SELECT 2", nil)
	require.NoError(t, err)

	// Verify they exist
	psi := h.Consolidator().GetPreparedStatementInfo(conn.ConnectionID(), "stmt1")
	require.NotNil(t, psi)

	h.ConnectionClosed(conn)

	// Prepared statements should be gone
	psi = h.Consolidator().GetPreparedStatementInfo(conn.ConnectionID(), "stmt1")
	require.Nil(t, psi)
	psi = h.Consolidator().GetPreparedStatementInfo(conn.ConnectionID(), "stmt2")
	require.Nil(t, psi)
}
