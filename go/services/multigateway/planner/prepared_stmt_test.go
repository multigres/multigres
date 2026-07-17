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

package planner

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"
	pgClient "github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/engine"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// mockIExecute implements engine.IExecute for testing primitives.
type mockIExecute struct {
	portalStreamExecuteCalled bool
	// streamExecuteCalls records the observable arguments of every StreamExecute
	// call so tests can assert on the SQL EXECUTE template and the attached
	// prepared statement metadata for wrapped EXECUTE cases.
	streamExecuteCalls []streamExecuteCall
}

// streamExecuteCall records the observable arguments of a StreamExecute call.
type streamExecuteCall struct {
	sql                         string
	executeSQLPreparedStatement *query.ExecuteSqlPreparedStatement
}

func (m *mockIExecute) StreamExecute(ctx context.Context, _ *server.Conn, _, _ string, sql string, ps *query.ExecuteSqlPreparedStatement, _ *handler.MultigatewayConnectionState, _ engine.PlanExecInfo, _ bool, callback func(context.Context, *sqltypes.Result) error) error {
	m.streamExecuteCalls = append(m.streamExecuteCalls, streamExecuteCall{sql: sql, executeSQLPreparedStatement: ps})
	return callback(ctx, &sqltypes.Result{CommandTag: "SELECT 1"})
}

func (m *mockIExecute) PortalStreamExecute(ctx context.Context, _, _ string, _ *server.Conn, _ *handler.MultigatewayConnectionState, _ *preparedstatement.PortalInfo, _ int32, _ bool, _ engine.PlanExecInfo, _ bool, callback func(context.Context, *sqltypes.Result) error) error {
	m.portalStreamExecuteCalled = true
	return callback(ctx, &sqltypes.Result{CommandTag: "SELECT 1", Rows: []*sqltypes.Row{{Values: []sqltypes.Value{[]byte("1")}}}})
}

func (m *mockIExecute) Describe(context.Context, string, string, *server.Conn, *handler.MultigatewayConnectionState, *preparedstatement.PortalInfo, *preparedstatement.PreparedStatementInfo) (*query.StatementDescription, error) {
	return nil, nil
}

func (m *mockIExecute) ConcludeTransaction(context.Context, *server.Conn, *handler.MultigatewayConnectionState, multipoolerpb.TransactionConclusion, []string, bool, bool, func(context.Context, *sqltypes.Result) error) error {
	return nil
}

func (m *mockIExecute) ReleaseAllReservedConnections(context.Context, *server.Conn, *handler.MultigatewayConnectionState) error {
	return nil
}

func (m *mockIExecute) CopyInitiate(context.Context, *server.Conn, string, string, string, *handler.MultigatewayConnectionState, func(context.Context, *sqltypes.Result) error) (int16, []int16, error) {
	return 0, nil, nil
}

func (m *mockIExecute) CopySendData(context.Context, *server.Conn, string, string, *handler.MultigatewayConnectionState, []byte) error {
	return nil
}

func (m *mockIExecute) CopyFinalize(context.Context, *server.Conn, string, string, *handler.MultigatewayConnectionState, []byte, func(context.Context, *sqltypes.Result) error) error {
	return nil
}

func (m *mockIExecute) CopyAbort(context.Context, *server.Conn, string, string, *handler.MultigatewayConnectionState) error {
	return nil
}

func (m *mockIExecute) CopyOutInitiate(context.Context, *server.Conn, string, string, string, *handler.MultigatewayConnectionState) (int16, []int16, []*mterrors.PgDiagnostic, error) {
	return 0, nil, nil, nil
}

func (m *mockIExecute) CopyOutStream(context.Context, *server.Conn, string, string, *handler.MultigatewayConnectionState, func(pgClient.CopyOutMessage) error) (*sqltypes.Result, error) {
	return nil, nil
}

func (m *mockIExecute) DiscardTempTables(context.Context, *server.Conn, *handler.MultigatewayConnectionState, func(context.Context, *sqltypes.Result) error) error {
	return nil
}

var _ engine.IExecute = (*mockIExecute)(nil)

// mockHandlerExecutor implements handler.Executor for the MultigatewayHandler.
type mockHandlerExecutor struct {
	portalStreamExecuteCalled bool
}

func (m *mockHandlerExecutor) StreamExecute(ctx context.Context, _ *server.Conn, _ *handler.MultigatewayConnectionState, _ string, _ ast.Stmt, callback func(context.Context, *sqltypes.Result) error) (*handler.ExecuteResult, error) {
	err := callback(ctx, &sqltypes.Result{CommandTag: "SELECT 1"})
	return &handler.ExecuteResult{}, err
}

func (m *mockHandlerExecutor) PortalStreamExecute(ctx context.Context, _ *server.Conn, _ *handler.MultigatewayConnectionState, _ *preparedstatement.PortalInfo, _ int32, _ bool, callback func(context.Context, *sqltypes.Result) error) (*handler.ExecuteResult, error) {
	m.portalStreamExecuteCalled = true
	err := callback(ctx, &sqltypes.Result{CommandTag: "SELECT 1"})
	return &handler.ExecuteResult{}, err
}

func (m *mockHandlerExecutor) Describe(context.Context, *server.Conn, *handler.MultigatewayConnectionState, *preparedstatement.PortalInfo, *preparedstatement.PreparedStatementInfo) (*query.StatementDescription, error) {
	return nil, nil
}

func (m *mockHandlerExecutor) ReleaseAll(context.Context, *server.Conn, *handler.MultigatewayConnectionState) error {
	return nil
}

// testSetup bundles the objects needed for prepared statement planner tests.
type testSetup struct {
	psc  *preparedstatement.Consolidator
	p    *Planner
	conn *server.TestConn
	exec *mockIExecute
}

func newTestSetup(t *testing.T) *testSetup {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	exec := &mockIExecute{}

	// The primitive calls conn.Handler().HandleParse/HandleBind/HandleClose,
	// so we wire up a real MultigatewayHandler. The handler owns the consolidator;
	// the test accesses it via h.Consolidator().
	h := handler.NewMultigatewayHandler(&mockHandlerExecutor{}, logger, 0)
	tc := server.NewTestConn(&bytes.Buffer{}, server.WithTestHandler(h))

	return &testSetup{psc: h.Consolidator(), p: p, conn: tc, exec: exec}
}

// planAndExecute is a test helper that parses SQL, plans it, and executes the plan.
func planAndExecute(t *testing.T, s *testSetup, sql string) (*sqltypes.Result, error) {
	t.Helper()
	asts, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	require.Len(t, asts, 1)

	plan, err := s.p.Plan(sql, asts[0], s.conn.Conn, PlanOptions{})
	if err != nil {
		return nil, err
	}

	state := s.conn.Conn.GetConnectionState()
	if state == nil {
		st := handler.NewMultigatewayConnectionState()
		s.conn.Conn.SetConnectionState(st)
		state = st
	}
	var result *sqltypes.Result
	err = plan.StreamExecute(context.Background(), s.exec, s.conn.Conn, state.(*handler.MultigatewayConnectionState), nil, func(_ context.Context, r *sqltypes.Result) error {
		result = r
		return nil
	})
	return result, err
}

func TestPlanPrepareStmt(t *testing.T) {
	s := newTestSetup(t)

	result, err := planAndExecute(t, s, "PREPARE myplan AS SELECT 1")
	require.NoError(t, err)
	assert.Equal(t, "PREPARE", result.CommandTag)

	psi := s.psc.GetPreparedStatementInfo(s.conn.Conn.ConnectionID(), "myplan")
	require.NotNil(t, psi)
	assert.Equal(t, "SELECT 1", psi.Query)
}

func TestPlanPrepareStmtDuplicateName(t *testing.T) {
	s := newTestSetup(t)

	_, err := planAndExecute(t, s, "PREPARE myplan AS SELECT 1")
	require.NoError(t, err)

	_, err = planAndExecute(t, s, "PREPARE myplan AS SELECT 2")
	require.Error(t, err)
	assert.True(t, mterrors.IsErrorCode(err, mterrors.PgSSDuplicatePreparedStmt),
		"expected duplicate_prepared_statement (42P05), got %v", err)

	// The first prepared statement must remain intact.
	psi := s.psc.GetPreparedStatementInfo(s.conn.Conn.ConnectionID(), "myplan")
	require.NotNil(t, psi)
	assert.Equal(t, "SELECT 1", psi.Query)
}

func TestPlanPrepareStmtWithParams(t *testing.T) {
	s := newTestSetup(t)

	result, err := planAndExecute(t, s, "PREPARE myplan (int, text) AS SELECT $1, $2")
	require.NoError(t, err)
	assert.Equal(t, "PREPARE", result.CommandTag)

	psi := s.psc.GetPreparedStatementInfo(s.conn.Conn.ConnectionID(), "myplan")
	require.NotNil(t, psi)
	assert.Equal(t, "SELECT $1, $2", psi.Query)
}

func TestPlanExecuteStmt(t *testing.T) {
	s := newTestSetup(t)

	_, err := planAndExecute(t, s, "PREPARE myplan AS SELECT 1")
	require.NoError(t, err)
	psi := s.psc.GetPreparedStatementInfo(s.conn.Conn.ConnectionID(), "myplan")
	require.NotNil(t, psi)

	result, err := planAndExecute(t, s, "EXECUTE myplan")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, s.exec.portalStreamExecuteCalled, "top-level SQL EXECUTE should route as SQL, not bind a portal")
	require.Len(t, s.exec.streamExecuteCalls, 1)
	call := s.exec.streamExecuteCalls[0]
	assert.Equal(t, "EXECUTE myplan", call.sql)
	require.NotNil(t, call.executeSQLPreparedStatement)
	assert.Equal(t, psi.PreparedStatement, call.executeSQLPreparedStatement.PreparedStatement)
	assert.Equal(t, "EXECUTE ", call.executeSQLPreparedStatement.SqlPrefix)
	assert.Equal(t, "", call.executeSQLPreparedStatement.SqlSuffix)
}

func TestPlanExecuteStmtWithParams(t *testing.T) {
	s := newTestSetup(t)

	_, err := planAndExecute(t, s, "PREPARE myplan (int) AS SELECT $1")
	require.NoError(t, err)
	psi := s.psc.GetPreparedStatementInfo(s.conn.Conn.ConnectionID(), "myplan")
	require.NotNil(t, psi)

	result, err := planAndExecute(t, s, "EXECUTE myplan(42)")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, s.exec.streamExecuteCalls, 1)
	call := s.exec.streamExecuteCalls[0]
	assert.Equal(t, "EXECUTE myplan ( 42 )", call.sql)
	require.NotNil(t, call.executeSQLPreparedStatement)
	assert.Equal(t, psi.PreparedStatement, call.executeSQLPreparedStatement.PreparedStatement)
	assert.Equal(t, "EXECUTE ", call.executeSQLPreparedStatement.SqlPrefix)
	assert.Equal(t, " ( 42 )", call.executeSQLPreparedStatement.SqlSuffix)
}

func TestPlanExecuteStmtPreservesArgumentExpressions(t *testing.T) {
	s := newTestSetup(t)

	_, err := planAndExecute(t, s, "PREPARE myplan (int, int[]) AS SELECT $1, $2")
	require.NoError(t, err)
	psi := s.psc.GetPreparedStatementInfo(s.conn.Conn.ConnectionID(), "myplan")
	require.NotNil(t, psi)

	result, err := planAndExecute(t, s, "EXECUTE myplan(5::smallint, ARRAY[1,2,3])")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, s.exec.streamExecuteCalls, 1)
	call := s.exec.streamExecuteCalls[0]
	assert.Contains(t, call.sql, "EXECUTE myplan")
	require.NotNil(t, call.executeSQLPreparedStatement)
	assert.Equal(t, psi.PreparedStatement, call.executeSQLPreparedStatement.PreparedStatement)
	assert.Equal(t, "EXECUTE ", call.executeSQLPreparedStatement.SqlPrefix)
	assert.Contains(t, call.executeSQLPreparedStatement.SqlSuffix, "SMALLINT")
	assert.Contains(t, call.executeSQLPreparedStatement.SqlSuffix, "ARRAY")
}

func TestPlanExecuteStmtNonExistent(t *testing.T) {
	s := newTestSetup(t)

	_, err := planAndExecute(t, s, "EXECUTE nonexistent")
	require.Error(t, err)
	assert.True(t, mterrors.IsErrorCode(err, mterrors.PgSSInvalidSQLStatementName))
}

func TestPlanDeallocateStmt(t *testing.T) {
	s := newTestSetup(t)

	_, err := planAndExecute(t, s, "PREPARE myplan AS SELECT 1")
	require.NoError(t, err)

	result, err := planAndExecute(t, s, "DEALLOCATE myplan")
	require.NoError(t, err)
	assert.Equal(t, "DEALLOCATE", result.CommandTag)

	assert.Nil(t, s.psc.GetPreparedStatementInfo(s.conn.Conn.ConnectionID(), "myplan"))
}

func TestPlanDeallocateStmtNonExistent(t *testing.T) {
	s := newTestSetup(t)

	_, err := planAndExecute(t, s, "DEALLOCATE nonexistent")
	require.Error(t, err)
	assert.True(t, mterrors.IsErrorCode(err, mterrors.PgSSInvalidSQLStatementName))
}

func TestPlanDeallocateAll(t *testing.T) {
	s := newTestSetup(t)

	_, err := planAndExecute(t, s, "PREPARE plan1 AS SELECT 1")
	require.NoError(t, err)
	_, err = planAndExecute(t, s, "PREPARE plan2 AS SELECT 2")
	require.NoError(t, err)

	result, err := planAndExecute(t, s, "DEALLOCATE ALL")
	require.NoError(t, err)
	assert.Equal(t, "DEALLOCATE ALL", result.CommandTag)

	assert.Nil(t, s.psc.GetPreparedStatementInfo(s.conn.Conn.ConnectionID(), "plan1"))
	assert.Nil(t, s.psc.GetPreparedStatementInfo(s.conn.Conn.ConnectionID(), "plan2"))
}

func TestPlanPrepareExecuteDeallocateLifecycle(t *testing.T) {
	s := newTestSetup(t)

	result, err := planAndExecute(t, s, "PREPARE myplan AS SELECT 1")
	require.NoError(t, err)
	assert.Equal(t, "PREPARE", result.CommandTag)

	_, err = planAndExecute(t, s, "EXECUTE myplan")
	require.NoError(t, err)

	_, err = planAndExecute(t, s, "EXECUTE myplan")
	require.NoError(t, err)

	result, err = planAndExecute(t, s, "DEALLOCATE myplan")
	require.NoError(t, err)
	assert.Equal(t, "DEALLOCATE", result.CommandTag)

	_, err = planAndExecute(t, s, "EXECUTE myplan")
	require.Error(t, err)
	assert.True(t, mterrors.IsErrorCode(err, mterrors.PgSSInvalidSQLStatementName))
}
