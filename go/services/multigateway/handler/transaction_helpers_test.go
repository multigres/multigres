// Copyright 2026 Supabase, Inc.
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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"
)

// trackingMockExecutor tracks all statements passed to StreamExecute.
type trackingMockExecutor struct {
	executedStmts  []ast.Stmt
	errOnCallIndex int // which call index triggers error (-1 = never)
	errToReturn    error
	callCount      int
}

func (m *trackingMockExecutor) StreamExecute(
	_ context.Context,
	_ *server.Conn,
	_ *MultiGatewayConnectionState,
	_ string,
	astStmt ast.Stmt,
	_ func(context.Context, *sqltypes.Result) error,
) error {
	m.executedStmts = append(m.executedStmts, astStmt)
	idx := m.callCount
	m.callCount++
	if m.errOnCallIndex >= 0 && idx == m.errOnCallIndex {
		return m.errToReturn
	}
	return nil
}

func (m *trackingMockExecutor) PortalStreamExecute(context.Context, *server.Conn, *MultiGatewayConnectionState, *preparedstatement.PortalInfo, int32, func(context.Context, *sqltypes.Result) error) error {
	return nil
}

func (m *trackingMockExecutor) Describe(context.Context, *server.Conn, *MultiGatewayConnectionState, *preparedstatement.PortalInfo, *preparedstatement.PreparedStatementInfo) (*query.StatementDescription, error) {
	return nil, nil
}

func (m *trackingMockExecutor) ReleaseAll(context.Context, *server.Conn, *MultiGatewayConnectionState) error {
	return nil
}

// stmtDescription returns a short label for an AST statement for test comparison.
func stmtDescription(stmt ast.Stmt) string {
	if ast.IsBeginStatement(stmt) {
		return "BEGIN"
	}
	if ast.IsCommitStatement(stmt) {
		return "COMMIT"
	}
	if ast.IsRollbackStatement(stmt) {
		return "ROLLBACK"
	}
	return "OTHER"
}

// stmtDescriptions converts a slice of stmts to string labels.
func stmtDescriptions(stmts []ast.Stmt) []string {
	result := make([]string, len(stmts))
	for i, s := range stmts {
		result[i] = stmtDescription(s)
	}
	return result
}

// parseStmts is a test helper that parses SQL and returns statements.
func parseStmts(t *testing.T, sql string) []ast.Stmt {
	t.Helper()
	stmts, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	return stmts
}

// newTestHandler creates a MultiGatewayHandler with the given mock executor.
func newTestHandler(executor Executor) *MultiGatewayHandler {
	return &MultiGatewayHandler{
		executor: executor,
	}
}

func newImplicitTxTestConn() *server.Conn {
	return server.NewTestConn(&bytes.Buffer{}).Conn
}

func TestExecuteWithImplicitTransaction_PureImplicit(t *testing.T) {
	mock := &trackingMockExecutor{errOnCallIndex: -1}
	h := newTestHandler(mock)
	state := NewMultiGatewayConnectionState()
	stmts := parseStmts(t, "SELECT 1; SELECT 2")

	err := h.executeWithImplicitTransaction(
		context.Background(), newImplicitTxTestConn(), state, "SELECT 1; SELECT 2", stmts,
		func(_ context.Context, _ *sqltypes.Result) error { return nil },
	)

	require.NoError(t, err)
	require.Equal(t, []string{"BEGIN", "OTHER", "OTHER", "COMMIT"}, stmtDescriptions(mock.executedStmts))
}

func TestExecuteWithImplicitTransaction_UserBeginMidBatch(t *testing.T) {
	mock := &trackingMockExecutor{errOnCallIndex: -1}
	h := newTestHandler(mock)
	state := NewMultiGatewayConnectionState()
	stmts := parseStmts(t, "SELECT 1; BEGIN; SELECT 2")

	err := h.executeWithImplicitTransaction(
		context.Background(), newImplicitTxTestConn(), state, "SELECT 1; BEGIN; SELECT 2", stmts,
		func(_ context.Context, _ *sqltypes.Result) error { return nil },
	)

	require.NoError(t, err)
	// Synthetic BEGIN is injected, user's BEGIN is skipped (adoption), no auto-COMMIT
	require.Equal(t, []string{"BEGIN", "OTHER", "OTHER"}, stmtDescriptions(mock.executedStmts))
}

func TestExecuteWithImplicitTransaction_UserCommitMidBatch(t *testing.T) {
	mock := &trackingMockExecutor{errOnCallIndex: -1}
	h := newTestHandler(mock)
	state := NewMultiGatewayConnectionState()
	stmts := parseStmts(t, "SELECT 1; COMMIT; SELECT 2")

	err := h.executeWithImplicitTransaction(
		context.Background(), newImplicitTxTestConn(), state, "SELECT 1; COMMIT; SELECT 2", stmts,
		func(_ context.Context, _ *sqltypes.Result) error { return nil },
	)

	require.NoError(t, err)
	// BEGIN, SELECT 1, COMMIT, BEGIN, SELECT 2, COMMIT
	require.Equal(t, []string{"BEGIN", "OTHER", "COMMIT", "BEGIN", "OTHER", "COMMIT"}, stmtDescriptions(mock.executedStmts))
}

func TestExecuteWithImplicitTransaction_UserRollbackMidBatch(t *testing.T) {
	mock := &trackingMockExecutor{errOnCallIndex: -1}
	h := newTestHandler(mock)
	state := NewMultiGatewayConnectionState()
	stmts := parseStmts(t, "SELECT 1; ROLLBACK; SELECT 2")

	err := h.executeWithImplicitTransaction(
		context.Background(), newImplicitTxTestConn(), state, "SELECT 1; ROLLBACK; SELECT 2", stmts,
		func(_ context.Context, _ *sqltypes.Result) error { return nil },
	)

	require.NoError(t, err)
	require.Equal(t, []string{"BEGIN", "OTHER", "ROLLBACK", "BEGIN", "OTHER", "COMMIT"}, stmtDescriptions(mock.executedStmts))
}

func TestExecuteWithImplicitTransaction_AlreadyInTransaction(t *testing.T) {
	mock := &trackingMockExecutor{errOnCallIndex: -1}
	h := newTestHandler(mock)
	state := NewMultiGatewayConnectionState()
	tc := server.NewTestConn(&bytes.Buffer{})
	tc.Conn.SetTxnStatus(protocol.TxnStatusInBlock)
	stmts := parseStmts(t, "SELECT 1; SELECT 2")

	err := h.executeWithImplicitTransaction(
		context.Background(), tc.Conn, state, "SELECT 1; SELECT 2", stmts,
		func(_ context.Context, _ *sqltypes.Result) error { return nil },
	)

	require.NoError(t, err)
	// No injected BEGIN or COMMIT since already in transaction
	require.Equal(t, []string{"OTHER", "OTHER"}, stmtDescriptions(mock.executedStmts))
}

func TestExecuteWithImplicitTransaction_ErrorInImplicitTx(t *testing.T) {
	mock := &trackingMockExecutor{
		errOnCallIndex: 2, // error on the second SELECT (index 2: BEGIN=0, SELECT1=1, SELECT2=2)
		errToReturn:    errors.New("query failed"),
	}
	h := newTestHandler(mock)
	state := NewMultiGatewayConnectionState()
	stmts := parseStmts(t, "SELECT 1; SELECT 2")

	err := h.executeWithImplicitTransaction(
		context.Background(), newImplicitTxTestConn(), state, "SELECT 1; SELECT 2", stmts,
		func(_ context.Context, _ *sqltypes.Result) error { return nil },
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "query failed")
	// BEGIN, SELECT 1, SELECT 2 (fails), auto-ROLLBACK
	require.Equal(t, []string{"BEGIN", "OTHER", "OTHER", "ROLLBACK"}, stmtDescriptions(mock.executedStmts))
}

func TestExecuteWithImplicitTransaction_ErrorInExplicitTx(t *testing.T) {
	mock := &trackingMockExecutor{
		errOnCallIndex: 2, // error on SELECT 2 (index 2: BEGIN=0, SELECT1=1, SELECT2=2)
		errToReturn:    errors.New("query failed"),
	}
	h := newTestHandler(mock)
	state := NewMultiGatewayConnectionState()
	conn := newImplicitTxTestConn()
	// Batch: SELECT 1; BEGIN; SELECT 2
	// After synthetic BEGIN + SELECT 1, user's BEGIN is skipped (adoption → explicit)
	// Then SELECT 2 fails → no auto-rollback because we're in explicit tx
	stmts := parseStmts(t, "SELECT 1; BEGIN; SELECT 2")

	err := h.executeWithImplicitTransaction(
		context.Background(), conn, state, "SELECT 1; BEGIN; SELECT 2", stmts,
		func(_ context.Context, _ *sqltypes.Result) error { return nil },
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "query failed")
	// BEGIN(synthetic), SELECT 1, SELECT 2 (fails) - no auto-ROLLBACK
	require.Equal(t, []string{"BEGIN", "OTHER", "OTHER"}, stmtDescriptions(mock.executedStmts))
	// Explicit transaction should transition to aborted state
	require.Equal(t, protocol.TxnStatusFailed, conn.TxnStatus())
}

func TestExecuteWithImplicitTransaction_AlreadyInTransaction_CommitMidBatch(t *testing.T) {
	mock := &trackingMockExecutor{errOnCallIndex: -1}
	h := newTestHandler(mock)
	state := NewMultiGatewayConnectionState()
	tc := server.NewTestConn(&bytes.Buffer{})
	tc.Conn.SetTxnStatus(protocol.TxnStatusInBlock)
	stmts := parseStmts(t, "SELECT 1; COMMIT; SELECT 2")

	err := h.executeWithImplicitTransaction(
		context.Background(), tc.Conn, state, "SELECT 1; COMMIT; SELECT 2", stmts,
		func(_ context.Context, _ *sqltypes.Result) error { return nil },
	)

	require.NoError(t, err)
	// Already in transaction: no initial BEGIN, but after COMMIT, new implicit segment starts
	require.Equal(t, []string{"OTHER", "COMMIT", "BEGIN", "OTHER", "COMMIT"}, stmtDescriptions(mock.executedStmts))
}
