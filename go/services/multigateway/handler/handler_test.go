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

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/query"
)

// mockExecutor is a mock implementation of the Executor interface for testing.
type mockExecutor struct {
	streamExecuteErr error
	releaseAllCalled bool
}

func (m *mockExecutor) StreamExecute(ctx context.Context, conn *server.Conn, state *MultiGatewayConnectionState, queryStr string, astStmt ast.Stmt, callback func(ctx context.Context, result *sqltypes.Result) error) error {
	if m.streamExecuteErr != nil {
		return m.streamExecuteErr
	}
	// Return a simple test result
	return callback(ctx, &sqltypes.Result{
		Fields: []*query.Field{
			{Name: "column1", Type: "int4"},
		},
		Rows: []*sqltypes.Row{
			{Values: []sqltypes.Value{[]byte("1")}},
		},
		CommandTag:   "SELECT 1",
		RowsAffected: 1,
	})
}

func (m *mockExecutor) PortalStreamExecute(ctx context.Context, conn *server.Conn, state *MultiGatewayConnectionState, portalInfo *preparedstatement.PortalInfo, maxRows int32, callback func(ctx context.Context, result *sqltypes.Result) error) error {
	// Return a simple test result
	return callback(ctx, &sqltypes.Result{
		Fields: []*query.Field{
			{Name: "column1", Type: "int4"},
		},
		Rows: []*sqltypes.Row{
			{Values: []sqltypes.Value{[]byte("1")}},
		},
		CommandTag:   "SELECT 1",
		RowsAffected: 1,
	})
}

func (m *mockExecutor) Describe(ctx context.Context, conn *server.Conn, state *MultiGatewayConnectionState, portalInfo *preparedstatement.PortalInfo, preparedStatementInfo *preparedstatement.PreparedStatementInfo) (*query.StatementDescription, error) {
	// Return a simple test description
	return &query.StatementDescription{
		Fields: []*query.Field{
			{Name: "test_field", Type: "int4"},
		},
	}, nil
}

func (m *mockExecutor) ReleaseAll(ctx context.Context, conn *server.Conn, state *MultiGatewayConnectionState) error {
	m.releaseAllCalled = true
	return nil
}

// TestHandleQueryEmptyQuery tests that empty queries are handled correctly.
func TestHandleQueryEmptyQuery(t *testing.T) {
	logger := slog.Default()
	executor := &mockExecutor{}
	handler := NewMultiGatewayHandler(executor, logger)

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
	handler := NewMultiGatewayHandler(executor, logger)

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

// TestPreparedStatementHandling tests the full lifecycle of prepared statements:
// parse, describe, close, and error cases.
func TestPreparedStatementHandling(t *testing.T) {
	logger := slog.Default()
	executor := &mockExecutor{}
	handler := NewMultiGatewayHandler(executor, logger)
	conn := &server.Conn{}
	ctx := context.Background()

	// 1. Parse stores the statement in the consolidator
	err := handler.HandleParse(ctx, conn, "stmt1", "SELECT $1::int", []uint32{23})
	require.NoError(t, err)

	// 2. Describe finds it in the consolidator
	desc, err := handler.HandleDescribe(ctx, conn, 'S', "stmt1")
	require.NoError(t, err)
	require.NotNil(t, desc)

	// 3. Duplicate named statement fails
	err = handler.HandleParse(ctx, conn, "stmt1", "SELECT 2", nil)
	require.Error(t, err)

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
	require.Contains(t, err.Error(), "does not exist")

	// 7. Empty query fails
	err = handler.HandleParse(ctx, conn, "empty", "", nil)
	require.Error(t, err)

	// 8. Invalid describe type fails
	_, err = handler.HandleDescribe(ctx, conn, 'X', "name")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid describe type")

	// 9. Invalid close type fails
	err = handler.HandleClose(ctx, conn, 'X', "name")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid close type")
}

// TestPortalHandling tests the full lifecycle of portals:
// bind, describe, execute, close, and error cases.
func TestPortalHandling(t *testing.T) {
	logger := slog.Default()
	executor := &mockExecutor{}
	handler := NewMultiGatewayHandler(executor, logger)
	conn := &server.Conn{}
	ctx := context.Background()

	// Setup: parse a statement first
	err := handler.HandleParse(ctx, conn, "stmt1", "SELECT $1::int", []uint32{23})
	require.NoError(t, err)

	// 1. Bind fails for non-existent statement
	err = handler.HandleBind(ctx, conn, "portal1", "nonexistent", nil, nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not exist")

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
	err = handler.HandleExecute(ctx, conn, "portal1", 0, func(ctx context.Context, r *sqltypes.Result) error {
		result = r
		return nil
	})
	require.NoError(t, err)
	require.NotNil(t, result)

	// 5. Execute fails for non-existent portal
	err = handler.HandleExecute(ctx, conn, "nonexistent", 0, func(ctx context.Context, r *sqltypes.Result) error {
		return nil
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not exist")

	// 6. Close removes portal from connection state
	err = handler.HandleClose(ctx, conn, 'P', "portal1")
	require.NoError(t, err)

	// 7. Describe fails after close
	_, err = handler.HandleDescribe(ctx, conn, 'P', "portal1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not exist")
}

// TestPreparedStatementConsolidation tests that same queries share the same
// underlying statement, and closing one doesn't affect the other.
func TestPreparedStatementConsolidation(t *testing.T) {
	logger := slog.Default()
	executor := &mockExecutor{}
	handler := NewMultiGatewayHandler(executor, logger)
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
	handler := NewMultiGatewayHandler(executor, logger)
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
	h := NewMultiGatewayHandler(executor, logger)
	conn := server.NewTestConn(&bytes.Buffer{}).Conn

	// Put connection in aborted transaction state
	conn.SetTxnStatus(protocol.TxnStatusFailed)

	err := h.HandleQuery(context.Background(), conn, "SELECT 1", func(_ context.Context, _ *sqltypes.Result) error {
		t.Fatal("callback should not be called for aborted transaction")
		return nil
	})

	require.Error(t, err)
	require.Equal(t, errAbortedTransaction, err)
	// Status should remain Failed
	require.Equal(t, protocol.TxnStatusFailed, conn.TxnStatus())
}

// TestHandleQuery_AbortedTransactionAllowsRollback tests that ROLLBACK is allowed
// when the transaction is in an aborted state.
func TestHandleQuery_AbortedTransactionAllowsRollback(t *testing.T) {
	logger := slog.Default()
	executor := &mockExecutor{}
	h := NewMultiGatewayHandler(executor, logger)
	conn := server.NewTestConn(&bytes.Buffer{}).Conn

	// Put connection in aborted transaction state
	conn.SetTxnStatus(protocol.TxnStatusFailed)

	err := h.HandleQuery(context.Background(), conn, "ROLLBACK", func(_ context.Context, _ *sqltypes.Result) error {
		return nil
	})

	// ROLLBACK should be allowed through (executed by the mock)
	require.NoError(t, err)
}

// TestHandleQuery_ErrorInTransactionSetsAbortedState tests that a query error
// while in a transaction transitions the state to aborted.
func TestHandleQuery_ErrorInTransactionSetsAbortedState(t *testing.T) {
	logger := slog.Default()
	executor := &mockExecutor{streamExecuteErr: errors.New("query failed")}
	h := NewMultiGatewayHandler(executor, logger)
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
	h := NewMultiGatewayHandler(executor, logger)
	conn := server.NewTestConn(&bytes.Buffer{}).Conn

	err := h.HandleQuery(context.Background(), conn, "SELECT 1", func(_ context.Context, _ *sqltypes.Result) error {
		return nil
	})

	require.Error(t, err)
	// Should stay idle (not in a transaction)
	require.Equal(t, protocol.TxnStatusIdle, conn.TxnStatus())
}

// TestHandleExecute_AbortedTransactionRejects tests that Execute messages
// are rejected when the transaction is aborted.
func TestHandleExecute_AbortedTransactionRejects(t *testing.T) {
	logger := slog.Default()
	executor := &mockExecutor{}
	h := NewMultiGatewayHandler(executor, logger)
	conn := server.NewTestConn(&bytes.Buffer{}).Conn
	ctx := context.Background()

	// Setup a prepared statement and portal
	err := h.HandleParse(ctx, conn, "stmt1", "SELECT 1", nil)
	require.NoError(t, err)
	err = h.HandleBind(ctx, conn, "portal1", "stmt1", nil, nil, nil)
	require.NoError(t, err)

	// Put connection in aborted transaction state
	conn.SetTxnStatus(protocol.TxnStatusFailed)

	err = h.HandleExecute(ctx, conn, "portal1", 0, func(_ context.Context, _ *sqltypes.Result) error {
		t.Fatal("callback should not be called for aborted transaction")
		return nil
	})

	require.Error(t, err)
	require.Equal(t, errAbortedTransaction, err)
}

// TestConnectionClosed_ReleasesReservedConnections tests that ConnectionClosed
// releases all reserved connections when the client disconnects.
func TestConnectionClosed_ReleasesReservedConnections(t *testing.T) {
	logger := slog.Default()
	executor := &mockExecutor{}
	h := NewMultiGatewayHandler(executor, logger)
	conn := server.NewTestConn(&bytes.Buffer{}).Conn

	// Initialize connection state with a reserved connection
	state := NewMultiGatewayConnectionState()
	conn.SetConnectionState(state)
	conn.SetTxnStatus(protocol.TxnStatusInBlock)
	target := &query.Target{
		TableGroup: "tg1",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	state.StoreReservedConnection(target, queryservice.ReservedState{
		ReservedConnectionId: 42,
		PoolerID:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
	}, protoutil.ReasonTransaction)

	h.ConnectionClosed(conn)

	require.True(t, executor.releaseAllCalled, "ReleaseAll should be called")
}

// TestConnectionClosed_ReleasesNonTransactionReservedConnections tests that
// ConnectionClosed releases reserved connections even when not in a transaction.
func TestConnectionClosed_ReleasesNonTransactionReservedConnections(t *testing.T) {
	logger := slog.Default()
	executor := &mockExecutor{}
	h := NewMultiGatewayHandler(executor, logger)
	conn := server.NewTestConn(&bytes.Buffer{}).Conn

	// Initialize connection state with a reserved connection for COPY (not transaction)
	state := NewMultiGatewayConnectionState()
	conn.SetConnectionState(state)
	// NOT in a transaction - TxnStatusIdle
	target := &query.Target{
		TableGroup: "tg1",
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}
	state.StoreReservedConnection(target, queryservice.ReservedState{
		ReservedConnectionId: 42,
		PoolerID:             &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
	}, protoutil.ReasonCopy)

	h.ConnectionClosed(conn)

	require.True(t, executor.releaseAllCalled, "ReleaseAll should be called even without active transaction")
}

// TestConnectionClosed_NoReservedConnections tests that ConnectionClosed
// does not call ReleaseAll when there are no reserved connections.
func TestConnectionClosed_NoReservedConnections(t *testing.T) {
	logger := slog.Default()
	executor := &mockExecutor{}
	h := NewMultiGatewayHandler(executor, logger)
	conn := server.NewTestConn(&bytes.Buffer{}).Conn

	// Initialize connection state with no reserved connections
	state := NewMultiGatewayConnectionState()
	conn.SetConnectionState(state)

	h.ConnectionClosed(conn)

	require.False(t, executor.releaseAllCalled, "ReleaseAll should not be called when no shard states")
}

// TestConnectionClosed_CleansPreparedStatements tests that ConnectionClosed
// removes prepared statements for the connection.
func TestConnectionClosed_CleansPreparedStatements(t *testing.T) {
	logger := slog.Default()
	executor := &mockExecutor{}
	h := NewMultiGatewayHandler(executor, logger)
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
