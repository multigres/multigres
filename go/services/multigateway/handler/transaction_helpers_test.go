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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	multipoolerservice "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
)

// trackingMockExecutor tracks all statements passed to StreamExecute.
type trackingMockExecutor struct {
	executedStmts      []ast.Stmt
	pendingBeginAtCall []string
	errOnCallIndex     int // which call index triggers error (-1 = never)
	errToReturn        error
	callCount          int
}

func runsWithoutBackend(stmt ast.Stmt) bool {
	switch s := stmt.(type) {
	case *ast.VariableSetStmt:
		return IsGatewayManagedVariable(s.Name)
	case *ast.VariableShowStmt:
		return IsGatewayManagedVariable(s.Name)
	default:
		return false
	}
}

func (m *trackingMockExecutor) StreamExecute(
	ctx context.Context,
	_ *server.Conn,
	state *MultigatewayConnectionState,
	_ string,
	astStmt ast.Stmt,
	callback func(context.Context, *sqltypes.Result) error,
) (*ExecuteResult, error) {
	m.executedStmts = append(m.executedStmts, astStmt)
	if state != nil {
		m.pendingBeginAtCall = append(m.pendingBeginAtCall, state.PendingBeginQuery)
		if state.PendingBeginQuery != "" && !runsWithoutBackend(astStmt) && !ast.IsBeginStatement(astStmt) && !ast.IsCommitStatement(astStmt) && !ast.IsRollbackStatement(astStmt) {
			state.PendingBeginQuery = ""
		}
	}
	idx := m.callCount
	m.callCount++
	if m.errOnCallIndex >= 0 && idx == m.errOnCallIndex {
		// Return partial result with PlanTime, matching real executor behavior.
		return &ExecuteResult{PlanTime: time.Microsecond}, m.errToReturn
	}
	// Call the callback with a result that includes a CommandTag,
	// mimicking real executor behavior.
	err := callback(ctx, &sqltypes.Result{CommandTag: astStmt.SqlString()})
	return &ExecuteResult{}, err
}

func (m *trackingMockExecutor) PortalStreamExecute(context.Context, *server.Conn, *MultigatewayConnectionState, *preparedstatement.PortalInfo, int32, bool, func(context.Context, *sqltypes.Result) error) (*ExecuteResult, error) {
	return &ExecuteResult{}, nil
}

func (m *trackingMockExecutor) Describe(context.Context, *server.Conn, *MultigatewayConnectionState, *preparedstatement.PortalInfo, *preparedstatement.PreparedStatementInfo) (*query.StatementDescription, error) {
	return nil, nil
}

func (m *trackingMockExecutor) EagerParseInTransaction(context.Context, *server.Conn, *MultigatewayConnectionState, string, []uint32) error {
	return nil
}

func (m *trackingMockExecutor) ReleaseAll(context.Context, *server.Conn, *MultigatewayConnectionState) error {
	return nil
}

func (m *trackingMockExecutor) StreamReplication(context.Context, *server.Conn, *MultigatewayConnectionState, *multipoolerservice.StreamReplicationInit) (multipoolerservice.MultipoolerService_StreamReplicationClient, error) {
	return nil, nil
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

// newTestHandler creates a MultigatewayHandler with the given mock executor.
func newTestHandler(executor Executor) *MultigatewayHandler {
	metrics, _ := NewHandlerMetrics()
	return &MultigatewayHandler{
		executor: executor,
		metrics:  metrics,
	}
}

func newImplicitTxTestConn() *server.Conn {
	return server.NewTestConn(&bytes.Buffer{}).Conn
}

func TestExecuteWithImplicitTransaction_PureImplicit(t *testing.T) {
	mock := &trackingMockExecutor{errOnCallIndex: -1}
	h := newTestHandler(mock)
	state := NewMultigatewayConnectionState()
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
	state := NewMultigatewayConnectionState()
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
	state := NewMultigatewayConnectionState()
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
	state := NewMultigatewayConnectionState()
	stmts := parseStmts(t, "SELECT 1; ROLLBACK; SELECT 2")

	err := h.executeWithImplicitTransaction(
		context.Background(), newImplicitTxTestConn(), state, "SELECT 1; ROLLBACK; SELECT 2", stmts,
		func(_ context.Context, _ *sqltypes.Result) error { return nil },
	)

	require.NoError(t, err)
	require.Equal(t, []string{"BEGIN", "OTHER", "ROLLBACK", "BEGIN", "OTHER", "COMMIT"}, stmtDescriptions(mock.executedStmts))
}

func TestExecuteWithImplicitTransaction_ImplicitConclusionWarns(t *testing.T) {
	for _, command := range []string{"COMMIT", "ROLLBACK"} {
		t.Run(command, func(t *testing.T) {
			mock := &trackingMockExecutor{errOnCallIndex: -1}
			h := newTestHandler(mock)
			state := NewMultigatewayConnectionState()
			stmts := parseStmts(t, "SELECT 1; "+command+"; SELECT 2")
			var notices []*mterrors.PgDiagnostic

			err := h.executeWithImplicitTransaction(
				context.Background(), newImplicitTxTestConn(), state, "", stmts,
				func(_ context.Context, result *sqltypes.Result) error {
					notices = append(notices, result.Notices...)
					return nil
				},
			)

			require.NoError(t, err)
			require.Len(t, notices, 1)
			require.Equal(t, mterrors.PgSSNoActiveTransaction, notices[0].Code)
			require.Equal(t, "there is no transaction in progress", notices[0].Message)
		})
	}
}

func TestExecuteWithImplicitTransaction_RejectsSavepointsInImplicitBlock(t *testing.T) {
	for _, sql := range []string{
		"SELECT 1; SAVEPOINT sp",
		"ROLLBACK TO SAVEPOINT sp; SELECT 2",
		"SELECT 2; RELEASE SAVEPOINT sp; SELECT 3",
	} {
		t.Run(sql, func(t *testing.T) {
			mock := &trackingMockExecutor{errOnCallIndex: -1}
			h := newTestHandler(mock)
			state := NewMultigatewayConnectionState()

			err := h.executeWithImplicitTransaction(
				context.Background(), newImplicitTxTestConn(), state, sql, parseStmts(t, sql),
				func(context.Context, *sqltypes.Result) error { return nil },
			)

			require.Error(t, err)
			require.Equal(t, mterrors.PgSSNoActiveTransaction, mterrors.ExtractSQLSTATE(err))
			require.Equal(t, "ROLLBACK", stmtDescription(mock.executedStmts[len(mock.executedStmts)-1]))
		})
	}
}

func TestExecuteWithImplicitTransaction_AlreadyInTransaction(t *testing.T) {
	mock := &trackingMockExecutor{errOnCallIndex: -1}
	h := newTestHandler(mock)
	state := NewMultigatewayConnectionState()
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
	state := NewMultigatewayConnectionState()
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
	state := NewMultigatewayConnectionState()
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

func TestExecuteWithImplicitTransaction_CommandTagDeferredUntilCommit(t *testing.T) {
	mock := &trackingMockExecutor{errOnCallIndex: -1}
	h := newTestHandler(mock)
	state := NewMultigatewayConnectionState()
	stmts := parseStmts(t, "SELECT 1; SELECT 2")

	// Track the order of CommandTags received by the callback.
	var commandTags []string
	err := h.executeWithImplicitTransaction(
		context.Background(), newImplicitTxTestConn(), state, "SELECT 1; SELECT 2", stmts,
		func(_ context.Context, result *sqltypes.Result) error {
			if result.CommandTag != "" {
				commandTags = append(commandTags, result.CommandTag)
			}
			return nil
		},
	)

	require.NoError(t, err)
	require.Equal(t, []string{"BEGIN", "OTHER", "OTHER", "COMMIT"}, stmtDescriptions(mock.executedStmts))
	// SELECT 1's CommandTag is sent immediately, SELECT 2's is deferred until after COMMIT.
	// Both should be received by the callback.
	require.Len(t, commandTags, 2, "both statements should produce CommandTags")
	require.Contains(t, commandTags[0], "SELECT 1", "first statement CommandTag sent immediately")
	require.Contains(t, commandTags[1], "SELECT 2", "last statement CommandTag sent after commit")
}

func TestExecuteWithImplicitTransaction_CommandTagNotSentOnCommitFailure(t *testing.T) {
	mock := &trackingMockExecutor{
		errOnCallIndex: 3, // error on COMMIT (index 3: BEGIN=0, SELECT1=1, SELECT2=2, COMMIT=3)
		errToReturn:    errors.New("commit failed"),
	}
	h := newTestHandler(mock)
	state := NewMultigatewayConnectionState()
	stmts := parseStmts(t, "SELECT 1; SELECT 2")

	// Track CommandTags received by the callback.
	var commandTags []string
	err := h.executeWithImplicitTransaction(
		context.Background(), newImplicitTxTestConn(), state, "SELECT 1; SELECT 2", stmts,
		func(_ context.Context, result *sqltypes.Result) error {
			if result.CommandTag != "" {
				commandTags = append(commandTags, result.CommandTag)
			}
			return nil
		},
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "commit failed")
	// BEGIN, SELECT 1, SELECT 2, COMMIT (fails), ROLLBACK
	require.Equal(t, []string{"BEGIN", "OTHER", "OTHER", "COMMIT", "ROLLBACK"}, stmtDescriptions(mock.executedStmts))
	// SELECT 1's CommandTag was sent immediately, but SELECT 2's was held and never sent
	// because the commit failed — matching PostgreSQL behavior.
	require.Len(t, commandTags, 1, "only first statement's CommandTag should be sent")
	require.Contains(t, commandTags[0], "SELECT 1")
}

func TestExecuteWithImplicitTransaction_CopyLastStatement_CommitFailure(t *testing.T) {
	// When COPY FROM STDIN is the last statement in an implicit transaction batch
	// (e.g., "INSERT INTO t VALUES(1); COPY t FROM STDIN"), the COPY CommandTag
	// ("COPY 3") should be deferred until commit succeeds — just like any other
	// statement. If the commit fails, the COPY CommandTag must NOT be sent.
	mock := &trackingMockExecutor{
		errOnCallIndex: 3, // error on COMMIT (index 3: BEGIN=0, INSERT=1, COPY=2, COMMIT=3)
		errToReturn:    errors.New("commit failed"),
	}
	h := newTestHandler(mock)
	state := NewMultigatewayConnectionState()
	stmts := parseStmts(t, "INSERT INTO t VALUES(1); COPY t FROM STDIN")

	var commandTags []string
	err := h.executeWithImplicitTransaction(
		context.Background(), newImplicitTxTestConn(), state,
		"INSERT INTO t VALUES(1); COPY t FROM STDIN", stmts,
		func(_ context.Context, result *sqltypes.Result) error {
			if result.CommandTag != "" {
				commandTags = append(commandTags, result.CommandTag)
			}
			return nil
		},
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "commit failed")
	// BEGIN, INSERT, COPY, COMMIT (fails), ROLLBACK
	require.Equal(t, []string{"BEGIN", "OTHER", "OTHER", "COMMIT", "ROLLBACK"}, stmtDescriptions(mock.executedStmts))
	// INSERT's CommandTag was sent immediately, but COPY's was held and never sent.
	require.Len(t, commandTags, 1, "only INSERT CommandTag should be sent; COPY's was discarded")
	require.Contains(t, commandTags[0], "INSERT")
}

func TestExecuteWithImplicitTransaction_CopyLastStatement_CommitSuccess(t *testing.T) {
	// When COPY FROM STDIN is the last statement and the implicit commit succeeds,
	// the deferred COPY CommandTag should be flushed to the client.
	mock := &trackingMockExecutor{errOnCallIndex: -1}
	h := newTestHandler(mock)
	state := NewMultigatewayConnectionState()
	stmts := parseStmts(t, "INSERT INTO t VALUES(1); COPY t FROM STDIN")

	var commandTags []string
	err := h.executeWithImplicitTransaction(
		context.Background(), newImplicitTxTestConn(), state,
		"INSERT INTO t VALUES(1); COPY t FROM STDIN", stmts,
		func(_ context.Context, result *sqltypes.Result) error {
			if result.CommandTag != "" {
				commandTags = append(commandTags, result.CommandTag)
			}
			return nil
		},
	)

	require.NoError(t, err)
	// BEGIN, INSERT, COPY, COMMIT
	require.Equal(t, []string{"BEGIN", "OTHER", "OTHER", "COMMIT"}, stmtDescriptions(mock.executedStmts))
	// Both CommandTags should be sent: INSERT immediately, COPY after successful commit.
	require.Len(t, commandTags, 2, "both INSERT and COPY CommandTags should be sent")
	require.Contains(t, commandTags[0], "INSERT")
	require.Contains(t, commandTags[1], "COPY")
}

func TestExecuteWithImplicitTransaction_AndChainInImplicitBlockRollsBack(t *testing.T) {
	mock := &trackingMockExecutor{errOnCallIndex: -1}
	h := newTestHandler(mock)
	state := NewMultigatewayConnectionState()
	conn := newImplicitTxTestConn()
	stmts := parseStmts(t, "INSERT INTO t VALUES(1); COMMIT AND CHAIN")

	err := h.executeWithImplicitTransaction(
		context.Background(), conn, state,
		"INSERT INTO t VALUES(1); COMMIT AND CHAIN", stmts,
		func(_ context.Context, _ *sqltypes.Result) error { return nil },
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "COMMIT AND CHAIN can only be used in transaction blocks")
	require.Equal(t, []string{"BEGIN", "OTHER", "ROLLBACK"}, stmtDescriptions(mock.executedStmts))
	require.Equal(t, protocol.TxnStatusIdle, conn.TxnStatus())
}

func TestExecuteWithImplicitTransaction_AndChainAfterExplicitBeginKeepsTransactionActive(t *testing.T) {
	mock := &trackingMockExecutor{errOnCallIndex: -1}
	h := newTestHandler(mock)
	state := NewMultigatewayConnectionState()
	conn := newImplicitTxTestConn()
	stmts := parseStmts(t, "START TRANSACTION ISOLATION LEVEL REPEATABLE READ; SELECT 1; COMMIT AND CHAIN; SELECT 2")

	err := h.executeWithImplicitTransaction(
		context.Background(), conn, state,
		"START TRANSACTION ISOLATION LEVEL REPEATABLE READ; SELECT 1; COMMIT AND CHAIN; SELECT 2", stmts,
		func(_ context.Context, _ *sqltypes.Result) error { return nil },
	)

	require.NoError(t, err)
	// The user START adopts the synthetic BEGIN. COMMIT AND CHAIN keeps the
	// transaction active, so no new synthetic BEGIN is injected before SELECT 2.
	require.Equal(t, []string{"BEGIN", "OTHER", "COMMIT", "OTHER"}, stmtDescriptions(mock.executedStmts))
	require.Equal(t, protocol.TxnStatusInBlock, conn.TxnStatus())
}

func TestExecuteWithImplicitTransaction_GatewayManagedSetPreservesPendingBegin(t *testing.T) {
	mock := &trackingMockExecutor{errOnCallIndex: -1}
	h := newTestHandler(mock)
	state := NewMultigatewayConnectionState()
	stmts := parseStmts(t, "BEGIN ISOLATION LEVEL SERIALIZABLE; SET statement_timeout = '1s'; SELECT 1")

	err := h.executeWithImplicitTransaction(
		context.Background(), newImplicitTxTestConn(), state,
		"BEGIN ISOLATION LEVEL SERIALIZABLE; SET statement_timeout = '1s'; SELECT 1", stmts,
		func(_ context.Context, _ *sqltypes.Result) error { return nil },
	)

	require.NoError(t, err)
	require.Equal(t, []string{"BEGIN", "OTHER", "OTHER"}, stmtDescriptions(mock.executedStmts))
	require.Len(t, mock.pendingBeginAtCall, 3)
	require.Contains(t, mock.pendingBeginAtCall[1], "SERIALIZABLE", "gateway-local SET must not consume the deferred BEGIN")
	require.Contains(t, mock.pendingBeginAtCall[2], "SERIALIZABLE", "first backend statement must still receive BEGIN options")
}

func TestExecuteWithImplicitTransaction_AlreadyInTransaction_CommitMidBatch(t *testing.T) {
	mock := &trackingMockExecutor{errOnCallIndex: -1}
	h := newTestHandler(mock)
	state := NewMultigatewayConnectionState()
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

func TestExecuteWithImplicitTransaction_BeginWithIsolationAfterQuery_RollsBackImplicitTx(t *testing.T) {
	// Regression test: when a batch runs a query inside an implicit transaction and
	// then issues BEGIN ISOLATION LEVEL SERIALIZABLE, Multigres must rollback the
	// implicit transaction it opened before returning the error — otherwise the
	// backend PostgreSQL connection is left with an open transaction and goes back
	// into the pool in a dirty state.
	mock := &trackingMockExecutor{errOnCallIndex: -1}
	h := newTestHandler(mock)
	state := NewMultigatewayConnectionState()
	stmts := parseStmts(t, "SELECT 1; BEGIN ISOLATION LEVEL SERIALIZABLE")

	err := h.executeWithImplicitTransaction(
		context.Background(), newImplicitTxTestConn(), state,
		"SELECT 1; BEGIN ISOLATION LEVEL SERIALIZABLE", stmts,
		func(_ context.Context, _ *sqltypes.Result) error { return nil },
	)

	require.Error(t, err)
	// Implicit transaction must be rolled back before returning the error.
	// Without the fix, ROLLBACK is missing: ["BEGIN", "OTHER"].
	require.Equal(t, []string{"BEGIN", "OTHER", "ROLLBACK"}, stmtDescriptions(mock.executedStmts))
}

func TestExecuteWithImplicitTransaction_BeginWithIsolationInExplicitTx_DoesNotRollback(t *testing.T) {
	// When already inside an explicit transaction (user issued BEGIN earlier),
	// Multigres must return the same error but must NOT issue a ROLLBACK —
	// the client owns that transaction and must decide to roll it back themselves.
	mock := &trackingMockExecutor{errOnCallIndex: -1}
	h := newTestHandler(mock)
	state := NewMultigatewayConnectionState()
	tc := server.NewTestConn(&bytes.Buffer{})
	tc.Conn.SetTxnStatus(protocol.TxnStatusInBlock) // already in an explicit transaction
	stmts := parseStmts(t, "SELECT 1; BEGIN ISOLATION LEVEL SERIALIZABLE")

	err := h.executeWithImplicitTransaction(
		context.Background(), tc.Conn, state,
		"SELECT 1; BEGIN ISOLATION LEVEL SERIALIZABLE", stmts,
		func(_ context.Context, _ *sqltypes.Result) error { return nil },
	)

	require.Error(t, err)
	// No ROLLBACK — the explicit transaction belongs to the client.
	require.Equal(t, []string{"OTHER"}, stmtDescriptions(mock.executedStmts))
}
