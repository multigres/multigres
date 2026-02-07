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
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"
)

// mockExecutor is a mock implementation of the Executor interface for testing.
type mockExecutor struct{}

func (m *mockExecutor) StreamExecute(ctx context.Context, conn *server.Conn, state *MultiGatewayConnectionState, queryStr string, astStmt ast.Stmt, callback func(ctx context.Context, result *sqltypes.Result) error) error {
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

func (m *mockExecutor) ValidateStartupParams(ctx context.Context, conn *server.Conn, state *MultiGatewayConnectionState) (map[string]string, error) {
	return nil, nil
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
