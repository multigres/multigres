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

	"github.com/multigres/multigres/go/multipooler/queryservice"
	"github.com/multigres/multigres/go/parser/ast"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/pgprotocol/server"
)

// mockExecutor is a mock implementation of the Executor interface for testing.
type mockExecutor struct{}

func (m *mockExecutor) StreamExecute(ctx context.Context, conn *server.Conn, queryStr string, astStmt ast.Stmt, options *ExecuteOptions, callback func(ctx context.Context, result *query.QueryResult) error) error {
	// Return a simple test result
	return callback(ctx, &query.QueryResult{
		Fields: []*query.Field{
			{Name: "column1", Type: "int4"},
		},
		Rows: []*query.Row{
			{Values: [][]byte{[]byte("1")}},
		},
		CommandTag:   "SELECT 1",
		RowsAffected: 1,
	})
}

func (m *mockExecutor) ReserveStreamExecute(ctx context.Context, conn *server.Conn, queryStr string, astStmt ast.Stmt, options *ExecuteOptions, callback func(ctx context.Context, result *query.QueryResult) error) (queryservice.ReservedState, error) {
	// Return a simple test result and a mock ReservedState
	err := m.StreamExecute(ctx, conn, queryStr, astStmt, options, callback)
	return queryservice.ReservedState{
		ReservedConnectionId: 123,
		PoolerID:             nil, // Mock doesn't have a real pooler ID
	}, err
}

func (m *mockExecutor) Describe(ctx context.Context, conn *server.Conn, options *ExecuteOptions) (*query.StatementDescription, error) {
	// Return a simple test description
	return &query.StatementDescription{
		Fields: []*query.Field{
			{Name: "test_field", Type: "int4"},
		},
	}, nil
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
			var receivedResult *query.QueryResult

			err := handler.HandleQuery(context.Background(), conn, tt.query, func(ctx context.Context, result *query.QueryResult) error {
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
	var receivedResult *query.QueryResult

	err := handler.HandleQuery(context.Background(), conn, "SELECT 1", func(ctx context.Context, result *query.QueryResult) error {
		callbackCalled = true
		receivedResult = result
		return nil
	})

	require.NoError(t, err)
	require.True(t, callbackCalled, "HandleQuery() callback was not called")
	require.NotNil(t, receivedResult, "HandleQuery() callback should receive non-nil result for valid query")
}
