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
	"strings"
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
	info                        engine.PlanExecInfo
}

func (m *mockIExecute) StreamExecute(ctx context.Context, _ *server.Conn, _, _ string, sql string, ps *query.ExecuteSqlPreparedStatement, _ *handler.MultigatewayConnectionState, info engine.PlanExecInfo, _ bool, callback func(context.Context, *sqltypes.Result) error) error {
	m.streamExecuteCalls = append(m.streamExecuteCalls, streamExecuteCall{sql: sql, executeSQLPreparedStatement: ps, info: info})
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

func (m *mockIExecute) ReleaseAllReservedConnections(context.Context, *server.Conn, *handler.MultigatewayConnectionState, bool) error {
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

func (m *mockIExecute) StreamReplication(context.Context, *server.Conn, string, string, *handler.MultigatewayConnectionState, *multipoolerpb.StreamReplicationInit) (multipoolerpb.MultipoolerService_StreamReplicationClient, error) {
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

func (m *mockHandlerExecutor) EagerParseInTransaction(context.Context, *server.Conn, *handler.MultigatewayConnectionState, string, []uint32) error {
	return nil
}

func (m *mockHandlerExecutor) ReleaseAll(context.Context, *server.Conn, *handler.MultigatewayConnectionState) error {
	return nil
}

func (m *mockHandlerExecutor) StreamReplication(context.Context, *server.Conn, *handler.MultigatewayConnectionState, *multipoolerpb.StreamReplicationInit) (multipoolerpb.MultipoolerService_StreamReplicationClient, error) {
	return nil, nil
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

func TestPlanExecuteStmtCarriesPreparedBodyAdvisoryLock(t *testing.T) {
	s := newTestSetup(t)

	_, err := planAndExecute(t, s, "PREPARE myplan AS SELECT pg_advisory_lock(0)")
	require.NoError(t, err)

	_, err = planAndExecute(t, s, "EXECUTE myplan")
	require.NoError(t, err)
	require.Len(t, s.exec.streamExecuteCalls, 1)
	assert.True(t, s.exec.streamExecuteCalls[0].info.AdvisoryLock)
	assert.True(t, s.exec.streamExecuteCalls[0].info.RecheckAdvisoryLocks)
}

func TestPlanExecuteStmtCarriesPreparedBodyTempTable(t *testing.T) {
	s := newTestSetup(t)

	_, err := planAndExecute(t, s, "PREPARE myplan AS SELECT 1 AS x INTO TEMP ps_temp")
	require.NoError(t, err)

	_, err = planAndExecute(t, s, "EXECUTE myplan")
	require.NoError(t, err)
	require.Len(t, s.exec.streamExecuteCalls, 1)
	assert.True(t, s.exec.streamExecuteCalls[0].info.TempTable)
}

// TestPlanExecuteStmtCarriesPreparedBodyLogicalReplicationSlot proves EXECUTE
// reserves a connection for a prepared body that creates a temporary logical
// replication slot, mirroring the direct (non-prepared) statement path (see
// TestPlan_LogicalReplicationSlotCreation_SetsExecInfo). The slot only exists
// on the backend that runs this EXECUTE; without the reservation the
// connection returns to the pool and a later operation on the slot lands on
// a different backend where it does not exist.
func TestPlanExecuteStmtCarriesPreparedBodyLogicalReplicationSlot(t *testing.T) {
	s := newTestSetup(t)

	_, err := planAndExecute(t, s, "PREPARE myplan AS SELECT pg_create_logical_replication_slot('s1', 'pgoutput', true)")
	require.NoError(t, err)

	_, err = planAndExecute(t, s, "EXECUTE myplan")
	require.NoError(t, err)
	require.Len(t, s.exec.streamExecuteCalls, 1)
	assert.True(t, s.exec.streamExecuteCalls[0].info.LogicalReplicationSlot)
}

// TestPlanExecuteStmtCarriesPreparedBodySetSeed proves EXECUTE reserves a
// connection for a prepared body that calls setseed(...), mirroring the
// direct (non-prepared) statement path (see TestPlan_SetSeed_SetsExecInfo).
// The seed is backend-local state with no reset command; without the
// reservation a later random() call could land on a different, unseeded
// backend.
func TestPlanExecuteStmtCarriesPreparedBodySetSeed(t *testing.T) {
	s := newTestSetup(t)

	_, err := planAndExecute(t, s, "PREPARE myplan AS SELECT setseed(0.5)")
	require.NoError(t, err)

	_, err = planAndExecute(t, s, "EXECUTE myplan")
	require.NoError(t, err)
	require.Len(t, s.exec.streamExecuteCalls, 1)
	assert.True(t, s.exec.streamExecuteCalls[0].info.SetSeed)
}

// TestPlanExecuteStmtRewritesUnpinnedPersistingSetConfig pins the unpinned
// EXECUTE handling that replaces the old capture reservation: a prepared body
// carrying a session-persisting set_config(..., false) runs on the pooled
// backend with its is_local rewritten to true, so nothing persists there (the
// gateway tracks the value and pool-rotation replay carries it). A
// transaction-scoped (is_local true) body already persists nothing and runs
// verbatim.
func TestPlanExecuteStmtRewritesUnpinnedPersistingSetConfig(t *testing.T) {
	s := newTestSetup(t)

	routedBody := func(sql string) string {
		_, err := planAndExecute(t, s, sql)
		require.NoError(t, err)
		require.NotEmpty(t, s.exec.streamExecuteCalls)
		last := s.exec.streamExecuteCalls[len(s.exec.streamExecuteCalls)-1]
		require.NotNil(t, last.executeSQLPreparedStatement)
		require.NotNil(t, last.executeSQLPreparedStatement.GetPreparedStatement())
		return strings.ToLower(last.executeSQLPreparedStatement.GetPreparedStatement().GetQuery())
	}

	// Unpinned session (no transaction, no reserved conn): the persisting
	// is_local=false body is rewritten so the pooled backend reverts it.
	_, err := planAndExecute(t, s, "PREPARE myplan(text) AS SELECT set_config('application_name', $1, false)")
	require.NoError(t, err)
	body := routedBody("EXECUTE myplan('prepared_app')")
	assert.Contains(t, body, "true", "the routed body must apply set_config with is_local := true")
	assert.NotContains(t, body, "false", "the persisting is_local := false must be rewritten out")

	// A transaction-scoped body persists nothing, so it runs verbatim.
	_, err = planAndExecute(t, s, "PREPARE localplan(text) AS SELECT set_config('application_name', $1, true)")
	require.NoError(t, err)
	body = routedBody("EXECUTE localplan('x')")
	assert.Contains(t, body, "true", "the transaction-scoped body runs verbatim (is_local := true)")
}

// TestPlanExecuteStmtRevalidatesFailoverSlotAdmission proves EXECUTE
// re-derives admission from the live flag rather than inheriting whatever
// held when PREPARE registered the body: the same prepared statement is
// admitted while the feature is on and rejected once it is off, without the
// client re-preparing anything.
func TestPlanExecuteStmtRevalidatesFailoverSlotAdmission(t *testing.T) {
	s := newTestSetup(t)
	enabled := true
	s.p.SetSlotBasedReplicationEnabled(func() bool { return enabled })

	_, err := planAndExecute(t, s, "PREPARE myplan AS SELECT pg_create_logical_replication_slot('slot1', 'pgoutput', failover => true)")
	require.NoError(t, err)

	_, err = planAndExecute(t, s, "EXECUTE myplan")
	require.NoError(t, err)
	require.Len(t, s.exec.streamExecuteCalls, 1)

	enabled = false
	_, err = planAndExecute(t, s, "EXECUTE myplan")
	require.ErrorContains(t, err, "requires temporary=true")
}

// TestPlanExecuteStmtRevertsPersistingSetConfigAlongsideFailoverSlot proves an
// admitted failover-slot creation doesn't suppress the unpinned set_config
// revert: a persistent slot needs no backend reservation (it is visible from
// any backend), so the session stays unpinned and the body's persisting
// set_config must still be flipped to transaction-scoped before running on a
// pooled backend.
func TestPlanExecuteStmtRevertsPersistingSetConfigAlongsideFailoverSlot(t *testing.T) {
	s := newTestSetup(t)
	s.p.SetSlotBasedReplicationEnabled(func() bool { return true })

	_, err := planAndExecute(t, s,
		"PREPARE myplan AS SELECT pg_create_logical_replication_slot('slot1', 'pgoutput', failover => true), set_config('application_name', 'x', false)")
	require.NoError(t, err)

	_, err = planAndExecute(t, s, "EXECUTE myplan")
	require.NoError(t, err)
	require.Len(t, s.exec.streamExecuteCalls, 1)
	body := strings.ToLower(s.exec.streamExecuteCalls[0].executeSQLPreparedStatement.GetPreparedStatement().GetQuery())
	assert.Contains(t, body, "failover => true", "the admitted call is routed exactly as written")
	assert.Contains(t, body, "application_name", "the set_config revert must still apply")
	assert.NotContains(t, body, "'x', false", "the persisting is_local := false must be rewritten out")
}

func TestPlanPrepareStmtRejectsUnsupportedPreparedSetConfigShapes(t *testing.T) {
	s := newTestSetup(t)

	_, err := planAndExecute(t, s, "PREPARE myplan(text) AS SELECT set_config($1, 'x', false)")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "set_config name argument inside SQL PREPARE must be a literal constant")
}

func TestPlanExecuteStmtRechecksPreparedBody(t *testing.T) {
	s := newTestSetup(t)
	require.NoError(t, s.conn.Conn.Handler().HandleParse(context.Background(), s.conn.Conn, "bad", "SELECT pg_read_file('/tmp/x')", nil))

	stmt := parseOne(t, "EXECUTE bad").(*ast.ExecuteStmt)
	_, err := s.p.planExecuteStmt("EXECUTE bad", stmt, s.conn.Conn, nil)
	require.ErrorContains(t, err, "pg_read_file is not supported")
}

func TestPlanExecuteStmtPreservesArgumentExpressions(t *testing.T) {
	s := newTestSetup(t)

	_, err := planAndExecute(t, s, "PREPARE myplan (int, int[]) AS SELECT $1, $2")
	require.NoError(t, err)
	psi := s.psc.GetPreparedStatementInfo(s.conn.Conn.ConnectionID(), "myplan")
	require.NotNil(t, psi)
	assert.Equal(t, []uint32{uint32(ast.INT4OID), uint32(ast.INT4ARRAYOID)}, psi.ParamTypes)

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

// TestPlanPrepareStmtRejectsGatewayManagedSetConfig pins the fail-closed
// gate: a prepared body executes verbatim on the backend, so the direct
// path's gateway-managed rewrite cannot apply — a literal gateway-managed
// set_config in the body would put a real statement_timeout on a pooled
// backend that the release label (built from SessionSettings) can never
// describe. Rejected at PREPARE time, for both is_local variants, so the
// prepared form cannot silently diverge from the identical direct statement.
func TestPlanPrepareStmtRejectsGatewayManagedSetConfig(t *testing.T) {
	s := newTestSetup(t)

	_, err := planAndExecute(t, s,
		"PREPARE leak AS SELECT set_config('statement_timeout', '5s', false)")
	require.ErrorContains(t, err, `gateway-managed variable "statement_timeout"`)
	require.Nil(t, s.psc.GetPreparedStatementInfo(s.conn.Conn.ConnectionID(), "leak"),
		"a rejected PREPARE must register nothing")

	_, err = planAndExecute(t, s,
		"PREPARE leak2 AS SELECT set_config('idle_session_timeout', '5s', true)")
	require.ErrorContains(t, err, `gateway-managed variable "idle_session_timeout"`,
		"statement-local form is rejected too so prepared and direct semantics cannot diverge")

	// An ordinary GUC keeps the capture-intent path.
	_, err = planAndExecute(t, s,
		"PREPARE ok AS SELECT set_config('work_mem', '64MB', false)")
	require.NoError(t, err)
	require.NotNil(t, s.psc.GetPreparedStatementInfo(s.conn.Conn.ConnectionID(), "ok"))
}
