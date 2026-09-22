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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/services/multigateway/engine"
)

// TestUnwrapExplainExecute_NoParams verifies that EXPLAIN EXECUTE of a
// parameterless prepared statement substitutes the prepared body in place of the
// EXECUTE and runs the resulting EXPLAIN as an ordinary query.
func TestUnwrapExplainExecute_NoParams(t *testing.T) {
	s := newTestSetup(t)

	_, err := planAndExecute(t, s, "PREPARE p AS SELECT 1")
	require.NoError(t, err)

	// Now plan EXPLAIN EXECUTE p and observe the mock StreamExecute call.
	_, err = planAndExecute(t, s, "EXPLAIN (COSTS OFF) EXECUTE p")
	require.NoError(t, err)

	// Find the call corresponding to the wrapped EXECUTE (skip the PREPARE call,
	// which is handled via HandleParse and does not hit StreamExecute).
	require.NotEmpty(t, s.exec.streamExecuteCalls)
	call := s.exec.streamExecuteCalls[len(s.exec.streamExecuteCalls)-1]

	// The EXECUTE is replaced by the substituted body and run as a plain EXPLAIN;
	// no prepared-statement carrier is attached.
	assert.Equal(t, "EXPLAIN (COSTS false) SELECT 1", call.sql)
	assert.Nil(t, call.eagerParsePreparedStatement)
}

// TestUnwrapExplainExecute_PreservesOptions verifies that EXPLAIN options
// (ANALYZE, VERBOSE, (COSTS OFF), etc.) survive the AST rewrite.
func TestUnwrapExplainExecute_PreservesOptions(t *testing.T) {
	s := newTestSetup(t)

	_, err := planAndExecute(t, s, "PREPARE p AS SELECT 1")
	require.NoError(t, err)

	_, err = planAndExecute(t, s, "EXPLAIN (COSTS OFF, VERBOSE) EXECUTE p")
	require.NoError(t, err)

	call := s.exec.streamExecuteCalls[len(s.exec.streamExecuteCalls)-1]
	assert.Equal(t, "EXPLAIN (COSTS false, VERBOSE true) SELECT 1", call.sql)
}

// TestUnwrapExplainExecute_WithParams verifies that a parameterized EXECUTE
// inside EXPLAIN has its arguments substituted into the body, each cast to the
// resolved parameter type.
func TestUnwrapExplainExecute_WithParams(t *testing.T) {
	s := newTestSetup(t)

	_, err := planAndExecute(t, s, "PREPARE p(int, text) AS SELECT $1, $2")
	require.NoError(t, err)

	_, err = planAndExecute(t, s, "EXPLAIN (COSTS OFF) EXECUTE p(42, 'hello')")
	require.NoError(t, err)

	call := s.exec.streamExecuteCalls[len(s.exec.streamExecuteCalls)-1]
	assert.Equal(t, "EXPLAIN (COSTS false) SELECT CAST(42 AS INT), CAST('hello' AS TEXT)", call.sql)
	assert.Nil(t, call.eagerParsePreparedStatement)
}

// TestUnwrapCreateTableAsExecute verifies that CREATE TABLE t AS EXECUTE p
// is unwrapped via the Route path (non-temp) with the prepared body substituted.
func TestUnwrapCreateTableAsExecute(t *testing.T) {
	s := newTestSetup(t)

	_, err := planAndExecute(t, s, "PREPARE p AS SELECT 1 AS a")
	require.NoError(t, err)

	_, err = planAndExecute(t, s, "CREATE TABLE t AS EXECUTE p")
	require.NoError(t, err)

	call := s.exec.streamExecuteCalls[len(s.exec.streamExecuteCalls)-1]
	assert.Equal(t, "CREATE TABLE t AS SELECT 1 AS a", call.sql)
	assert.Nil(t, call.eagerParsePreparedStatement)
}

// TestUnwrapCreateTableAsExecute_RevalidatesFailoverSlotAdmission proves that
// CREATE TABLE ... AS EXECUTE, which runs the prepared body as a side
// effect, re-derives admission from the live flag rather than trusting
// whatever held at PREPARE time: a body whose slot creation omits failover
// is rejected here even though PREPARE registered it while the feature was
// on, because the check is re-run against the flag as of this statement.
func TestUnwrapCreateTableAsExecute_RevalidatesFailoverSlotAdmission(t *testing.T) {
	s := newTestSetup(t)
	enabled := true
	s.p.SetSlotBasedReplicationEnabled(func() bool { return enabled })

	_, err := planAndExecute(t, s, "PREPARE p AS SELECT pg_create_logical_replication_slot('slot1', 'pgoutput', failover => true)")
	require.NoError(t, err)

	// Still enabled: the explicitly-marked body is admitted and its SQL
	// reaches the multipooler exactly as the client wrote it.
	_, err = planAndExecute(t, s, "CREATE TABLE t AS EXECUTE p")
	require.NoError(t, err)
	call := s.exec.streamExecuteCalls[len(s.exec.streamExecuteCalls)-1]
	body := strings.ToLower(call.sql)
	assert.Contains(t, body, "failover => true")

	// Disabled after PREPARE: the same wrapped EXECUTE is now rejected,
	// because this path re-runs the admission check instead of inheriting
	// the decision PREPARE made.
	enabled = false
	_, err = planAndExecute(t, s, "CREATE TABLE t2 AS EXECUTE p")
	require.ErrorContains(t, err, "requires temporary=true")
}

// TestUnwrapCreateTableAsExecute_CarriesPreparedBodyReservations proves that
// CREATE TABLE ... AS EXECUTE, which materializes and runs the prepared
// body's own side effects, reserves a connection for whatever the body
// itself needs — a temporary logical replication slot, a session advisory
// lock, or setseed(...) — the same way plain EXECUTE does (see
// TestPlanExecuteStmtCarriesPreparedBodyLogicalReplicationSlot,
// TestPlanExecuteStmtCarriesPreparedBodySetSeed,
// TestPlanExecuteStmtCarriesPreparedBodyAdvisoryLock). Before
// preparedBodyExecInfo unified the two call sites, tryUnwrapWrappedExecute
// only ever set ExecInfo.TempTable from the wrapper's own CREATE TEMP TABLE
// keyword, silently dropping every signal the prepared body's own analysis
// produced — this wrapper would then release the connection the body's
// side effect actually needed pinned.
func TestUnwrapCreateTableAsExecute_CarriesPreparedBodyReservations(t *testing.T) {
	tests := []struct {
		name       string
		prepareSQL string
		check      func(t *testing.T, info engine.PlanExecInfo)
	}{
		{
			name:       "temporary logical replication slot",
			prepareSQL: "PREPARE p AS SELECT pg_create_logical_replication_slot('slot1', 'pgoutput', true)",
			check: func(t *testing.T, info engine.PlanExecInfo) {
				assert.True(t, info.LogicalReplicationSlot)
			},
		},
		{
			name:       "setseed",
			prepareSQL: "PREPARE p AS SELECT setseed(0.5)",
			check: func(t *testing.T, info engine.PlanExecInfo) {
				assert.True(t, info.SetSeed)
			},
		},
		{
			name:       "session advisory lock",
			prepareSQL: "PREPARE p AS SELECT pg_advisory_lock(0)",
			check: func(t *testing.T, info engine.PlanExecInfo) {
				assert.True(t, info.AdvisoryLock)
				assert.True(t, info.RecheckAdvisoryLocks)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestSetup(t)

			_, err := planAndExecute(t, s, tt.prepareSQL)
			require.NoError(t, err)

			_, err = planAndExecute(t, s, "CREATE TABLE t AS EXECUTE p")
			require.NoError(t, err)

			call := s.exec.streamExecuteCalls[len(s.exec.streamExecuteCalls)-1]
			tt.check(t, call.info)
		})
	}
}

// TestUnwrapCreateTempTableAsExecute verifies that CREATE TEMP TABLE ... AS
// EXECUTE p is unwrapped and routed through the temp-table reservation path
// while substituting the prepared body.
func TestUnwrapCreateTempTableAsExecute(t *testing.T) {
	s := newTestSetup(t)

	_, err := planAndExecute(t, s, "PREPARE p AS SELECT 1 AS a")
	require.NoError(t, err)

	// Plan but don't execute — we only want to verify the primitive shape.
	const sql = "CREATE TEMP TABLE tt AS EXECUTE p"
	asts, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	require.Len(t, asts, 1)
	plan, err := s.p.Plan(sql, asts[0], s.conn.Conn, PlanOptions{})
	require.NoError(t, err)

	route, ok := plan.Primitive.(*engine.Route)
	require.True(t, ok, "expected Route primitive, got %T", plan.Primitive)
	assert.True(t, plan.ExecInfo.TempTable, "CREATE TEMP TABLE AS EXECUTE must set ExecInfo.TempTable")
	assert.Equal(t, "CREATE TEMP TABLE tt AS SELECT 1 AS a", route.Query)
}

// TestUnwrapCreateUnloggedTableAsExecute verifies that the wrapped-execute
// early-return path still attaches the unlogged failover warning: a
// CREATE UNLOGGED TABLE ... AS EXECUTE is unwrapped before the main dispatch,
// so the warning must be applied on that path too.
func TestUnwrapCreateUnloggedTableAsExecute(t *testing.T) {
	s := newTestSetup(t)

	_, err := planAndExecute(t, s, "PREPARE pu AS SELECT 1 AS a")
	require.NoError(t, err)

	const sql = "CREATE UNLOGGED TABLE ut AS EXECUTE pu"
	asts, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	require.Len(t, asts, 1)
	plan, err := s.p.Plan(sql, asts[0], s.conn.Conn, PlanOptions{})
	require.NoError(t, err)

	// Sequence[UnloggedTableWarning, Route(substituted body)].
	seq, ok := plan.Primitive.(*engine.Sequence)
	require.True(t, ok, "expected Sequence primitive, got %T", plan.Primitive)
	require.Len(t, seq.Primitives, 2)
	_, ok = seq.Primitives[0].(*engine.StatementWarning)
	require.True(t, ok, "expected leading StatementWarning, got %T", seq.Primitives[0])
	route, ok := seq.Primitives[1].(*engine.Route)
	require.True(t, ok, "expected trailing Route, got %T", seq.Primitives[1])
	// NOTE: the deparser currently drops UNLOGGED (emits CREATE TABLE, not
	// CREATE UNLOGGED TABLE); asserted here as-is. See the failover-warning check
	// above, which is this test's real subject.
	assert.Equal(t, "CREATE TABLE ut AS SELECT 1 AS a", route.Query)
}

// TestUnwrapExplainCreateTableAsExecute verifies that doubly-nested
// EXPLAIN ... CREATE TABLE ... AS EXECUTE p (as seen in pgregress
// select_into.sql and write_parallel.sql) is unwrapped correctly: the
// innermost ExecuteStmt is substituted with the prepared body.
func TestUnwrapExplainCreateTableAsExecute(t *testing.T) {
	s := newTestSetup(t)

	_, err := planAndExecute(t, s, "PREPARE p_nested AS SELECT 1")
	require.NoError(t, err)

	// Plan without executing (the mock would try to create the table).
	const sql = "EXPLAIN (COSTS OFF) CREATE TABLE tnested AS EXECUTE p_nested"
	asts, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	require.Len(t, asts, 1)
	plan, err := s.p.Plan(sql, asts[0], s.conn.Conn, PlanOptions{})
	require.NoError(t, err)

	route, ok := plan.Primitive.(*engine.Route)
	require.True(t, ok, "expected Route primitive, got %T", plan.Primitive)
	assert.Equal(t, "EXPLAIN (COSTS false) CREATE TABLE tnested AS SELECT 1", route.Query)
}

// TestUnwrapExplainAnalyzeCreateTempTableAsExecute verifies that EXPLAIN
// ANALYZE wrapping CREATE TEMP TABLE AS EXECUTE uses the temp-table
// reservation path: ANALYZE is what makes EXPLAIN actually execute and
// materialize the temp table (see explainAnalyzes) — without it, nothing runs
// and nothing needs reserving (see TestUnwrapExplainCreateTempTableAsExecute,
// the bare-EXPLAIN sibling of this test).
func TestUnwrapExplainAnalyzeCreateTempTableAsExecute(t *testing.T) {
	s := newTestSetup(t)

	_, err := planAndExecute(t, s, "PREPARE p_nested_temp AS SELECT 1")
	require.NoError(t, err)

	const sql = "EXPLAIN ANALYZE CREATE TEMP TABLE tmp_nested AS EXECUTE p_nested_temp"
	asts, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	plan, err := s.p.Plan(sql, asts[0], s.conn.Conn, PlanOptions{})
	require.NoError(t, err)

	route, ok := plan.Primitive.(*engine.Route)
	require.True(t, ok, "expected Route primitive for EXPLAIN ANALYZE CREATE TEMP TABLE AS EXECUTE, got %T", plan.Primitive)
	assert.True(t, plan.ExecInfo.TempTable, "EXPLAIN ANALYZE CREATE TEMP TABLE AS EXECUTE must set ExecInfo.TempTable")
	assert.Contains(t, route.Query, "SELECT 1")
	assert.Contains(t, route.Query, "tmp_nested")
}

// TestUnwrapExplainCreateTempTableAsExecute verifies that a bare EXPLAIN
// (no ANALYZE) wrapping CREATE TEMP TABLE AS EXECUTE does NOT reserve a
// connection: without ANALYZE, EXPLAIN only plans the statement and never
// actually runs it, so no temp table is ever materialized (see
// explainAnalyzes). Reserving anyway would strand a pooled connection for a
// side effect that never happens.
func TestUnwrapExplainCreateTempTableAsExecute(t *testing.T) {
	s := newTestSetup(t)

	_, err := planAndExecute(t, s, "PREPARE p_nested_temp AS SELECT 1")
	require.NoError(t, err)

	const sql = "EXPLAIN CREATE TEMP TABLE tmp_nested AS EXECUTE p_nested_temp"
	asts, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	plan, err := s.p.Plan(sql, asts[0], s.conn.Conn, PlanOptions{})
	require.NoError(t, err)

	route, ok := plan.Primitive.(*engine.Route)
	require.True(t, ok, "expected Route primitive for EXPLAIN CREATE TEMP TABLE AS EXECUTE, got %T", plan.Primitive)
	assert.False(t, plan.ExecInfo.TempTable, "a bare EXPLAIN never executes, so it must not reserve for a temp table that is never created")
	assert.Equal(t, "EXPLAIN CREATE TEMP TABLE tmp_nested AS SELECT 1", route.Query)
}

// TestExplainAnalyzes covers the option forms that decide whether a wrapped
// EXECUTE actually runs its body. Presence alone is not the answer: the
// option takes an explicit boolean, and `EXPLAIN (ANALYZE FALSE)` plans
// without executing, so treating the option's mere presence as ANALYZE would
// reserve a backend (and re-check admission) for side effects that never
// happen.
func TestExplainAnalyzes(t *testing.T) {
	tests := []struct {
		sql  string
		want bool
	}{
		{"EXPLAIN SELECT 1", false},
		{"EXPLAIN ANALYZE SELECT 1", true},
		{"EXPLAIN (ANALYZE) SELECT 1", true},
		{"EXPLAIN (ANALYZE TRUE) SELECT 1", true},
		{"EXPLAIN (ANALYZE FALSE) SELECT 1", false},
		{"EXPLAIN (ANALYZE off) SELECT 1", false},
		{"EXPLAIN (ANALYZE on) SELECT 1", true},
		{"EXPLAIN (VERBOSE) SELECT 1", false},
		{"EXPLAIN (VERBOSE, ANALYZE FALSE) SELECT 1", false},
	}
	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			es, ok := parseOne(t, tt.sql).(*ast.ExplainStmt)
			require.True(t, ok)
			assert.Equal(t, tt.want, explainAnalyzes(es))
		})
	}
}

// TestUnwrapBareExplainExecuteReservesNothing proves a bare EXPLAIN EXECUTE
// reserves no connection even when the prepared body would need one had it
// run: without ANALYZE the body is only planned, so pinning the backend for
// a slot or seed that is never created would strand a pooled connection for
// the rest of the session.
func TestUnwrapBareExplainExecuteReservesNothing(t *testing.T) {
	s := newTestSetup(t)

	_, err := planAndExecute(t, s, "PREPARE p AS SELECT setseed(0.5)")
	require.NoError(t, err)

	_, err = planAndExecute(t, s, "EXPLAIN EXECUTE p")
	require.NoError(t, err)

	call := s.exec.streamExecuteCalls[len(s.exec.streamExecuteCalls)-1]
	assert.False(t, call.info.SetSeed, "a bare EXPLAIN never runs the body, so it must reserve nothing")
	assert.False(t, engine.StatementReservesBackend(call.info))
}

// TestUnwrapWrappedExecuteRejectsPreparedSetConfig proves a wrapped EXECUTE
// refuses a prepared body carrying a tracked set_config rather than
// half-handling it. This route runs the substituted body as an ordinary
// query and has no session-state channel: running it verbatim would leak the
// setting to the next client on that pooled backend, and rewriting it to
// revert would make the client's set_config a silent no-op. The same body
// under a plain EXECUTE still works, which is what the error tells the caller
// to use.
func TestUnwrapWrappedExecuteRejectsPreparedSetConfig(t *testing.T) {
	for _, sql := range []string{
		"CREATE TABLE t AS EXECUTE p",
		"EXPLAIN ANALYZE EXECUTE p",
	} {
		t.Run(sql, func(t *testing.T) {
			s := newTestSetup(t)
			_, err := planAndExecute(t, s, "PREPARE p AS SELECT set_config('application_name', 'x', false)")
			require.NoError(t, err)

			_, err = planAndExecute(t, s, sql)
			require.ErrorContains(t, err, "set_config inside a prepared statement is not supported")
		})
	}

	// A bare EXPLAIN never runs the body, so it has nothing to refuse.
	t.Run("bare EXPLAIN EXECUTE is unaffected", func(t *testing.T) {
		s := newTestSetup(t)
		_, err := planAndExecute(t, s, "PREPARE p AS SELECT set_config('application_name', 'x', false)")
		require.NoError(t, err)

		_, err = planAndExecute(t, s, "EXPLAIN EXECUTE p")
		require.NoError(t, err)
	})
}

// TestUnwrapMissingPreparedStatement verifies that EXPLAIN EXECUTE of an
// unknown prepared statement returns the standard PostgreSQL error (SQLSTATE
// 26000 invalid_sql_statement_name).
func TestUnwrapMissingPreparedStatement(t *testing.T) {
	s := newTestSetup(t)

	_, err := planAndExecute(t, s, "EXPLAIN EXECUTE nonexistent")
	require.Error(t, err)
	assert.True(t, mterrors.IsErrorCode(err, mterrors.PgSSInvalidSQLStatementName),
		"expected PgSSInvalidSQLStatementName, got %v", err)
}

// TestUnwrapNoOpForRegularStatements verifies that ordinary queries (no
// EXECUTE wrapper) are not affected by the unwrap pass: no PreparedStatement
// is attached and the SQL is passed through unchanged.
func TestUnwrapNoOpForRegularStatements(t *testing.T) {
	s := newTestSetup(t)

	_, err := planAndExecute(t, s, "SELECT 1")
	require.NoError(t, err)

	require.Len(t, s.exec.streamExecuteCalls, 1)
	call := s.exec.streamExecuteCalls[0]
	assert.Equal(t, "SELECT 1", call.sql)
	assert.Nil(t, call.eagerParsePreparedStatement)
}

// TestUnwrapExplainRegularQuery verifies that EXPLAIN of an ordinary SELECT
// (not wrapping EXECUTE) is not affected by the unwrap pass.
func TestUnwrapExplainRegularQuery(t *testing.T) {
	s := newTestSetup(t)

	_, err := planAndExecute(t, s, "EXPLAIN SELECT 1")
	require.NoError(t, err)

	require.Len(t, s.exec.streamExecuteCalls, 1)
	call := s.exec.streamExecuteCalls[0]
	assert.Equal(t, "EXPLAIN SELECT 1", call.sql)
	assert.Nil(t, call.eagerParsePreparedStatement)
}
