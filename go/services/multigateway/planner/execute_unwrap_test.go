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
	"github.com/multigres/multigres/go/services/multigateway/engine"
)

// TestUnwrapExplainExecute_NoParams verifies that EXPLAIN EXECUTE of a
// parameterless prepared statement is rewritten to reference the canonical
// name and that the Route carries the PreparedStatement metadata so the
// multipooler can ensurePrepared() before running the query.
func TestUnwrapExplainExecute_NoParams(t *testing.T) {
	s := newTestSetup(t)

	_, err := planAndExecute(t, s, "PREPARE p AS SELECT 1")
	require.NoError(t, err)

	// Look up the canonical name assigned by the consolidator.
	psi := s.psc.GetPreparedStatementInfo(s.conn.Conn.ConnectionID(), "p")
	require.NotNil(t, psi)
	canonical := psi.Name
	require.NotEmpty(t, canonical)
	require.NotEqual(t, "p", canonical, "consolidator should assign a canonical name distinct from the user name")

	// Now plan EXPLAIN EXECUTE p and observe the mock StreamExecute call.
	_, err = planAndExecute(t, s, "EXPLAIN (COSTS OFF) EXECUTE p")
	require.NoError(t, err)

	// Find the call corresponding to the wrapped EXECUTE (skip the PREPARE call,
	// which is handled via HandleParse and does not hit StreamExecute).
	require.NotEmpty(t, s.exec.streamExecuteCalls)
	call := s.exec.streamExecuteCalls[len(s.exec.streamExecuteCalls)-1]

	// The SQL should reference the canonical name, not the user name.
	assert.Contains(t, call.sql, canonical,
		"rewritten SQL should reference canonical name")
	assert.NotContains(t, call.sql, "EXECUTE p",
		"rewritten SQL should not contain the user-facing name")
	assert.True(t, strings.HasPrefix(strings.ToUpper(call.sql), "EXPLAIN"),
		"rewritten SQL should still be an EXPLAIN statement")

	// The PreparedStatement metadata must be attached so the multipooler can
	// ensurePrepared() on the backend connection before running the query.
	require.NotNil(t, call.preparedStatement)
	assert.Equal(t, canonical, call.preparedStatement.Name)
	assert.Equal(t, "SELECT 1", call.preparedStatement.Query)
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
	upper := strings.ToUpper(call.sql)
	assert.Contains(t, upper, "COSTS")
	assert.Contains(t, upper, "VERBOSE")
}

// TestUnwrapExplainExecute_WithParams verifies that parameterized EXECUTE
// keeps its literal param values in the rewritten SQL. Params are NOT inlined
// into the inner query body — they remain on the EXECUTE call so the backend's
// prepared-statement machinery handles them normally.
func TestUnwrapExplainExecute_WithParams(t *testing.T) {
	s := newTestSetup(t)

	_, err := planAndExecute(t, s, "PREPARE p(int, text) AS SELECT $1, $2")
	require.NoError(t, err)

	psi := s.psc.GetPreparedStatementInfo(s.conn.Conn.ConnectionID(), "p")
	require.NotNil(t, psi)
	canonical := psi.Name

	_, err = planAndExecute(t, s, "EXPLAIN (COSTS OFF) EXECUTE p(42, 'hello')")
	require.NoError(t, err)

	call := s.exec.streamExecuteCalls[len(s.exec.streamExecuteCalls)-1]
	assert.Contains(t, call.sql, canonical)
	assert.Contains(t, call.sql, "42")
	assert.Contains(t, call.sql, "'hello'")

	// PreparedStatement metadata must reflect the original body and param types.
	require.NotNil(t, call.preparedStatement)
	assert.Equal(t, canonical, call.preparedStatement.Name)
	assert.Equal(t, "SELECT $1, $2", call.preparedStatement.Query)
	assert.Len(t, call.preparedStatement.ParamTypes, 2)
}

// TestUnwrapCreateTableAsExecute verifies that CREATE TABLE t AS EXECUTE p
// is unwrapped via the Route path (non-temp) and carries the PreparedStatement
// metadata for ensurePrepared on the backend.
func TestUnwrapCreateTableAsExecute(t *testing.T) {
	s := newTestSetup(t)

	_, err := planAndExecute(t, s, "PREPARE p AS SELECT 1 AS a")
	require.NoError(t, err)

	psi := s.psc.GetPreparedStatementInfo(s.conn.Conn.ConnectionID(), "p")
	require.NotNil(t, psi)
	canonical := psi.Name

	_, err = planAndExecute(t, s, "CREATE TABLE t AS EXECUTE p")
	require.NoError(t, err)

	call := s.exec.streamExecuteCalls[len(s.exec.streamExecuteCalls)-1]
	upper := strings.ToUpper(call.sql)
	assert.True(t, strings.HasPrefix(upper, "CREATE"))
	assert.Contains(t, upper, "TABLE")
	assert.Contains(t, call.sql, canonical)
	require.NotNil(t, call.preparedStatement)
	assert.Equal(t, canonical, call.preparedStatement.Name)
}

// TestUnwrapCreateTempTableAsExecute verifies that CREATE TEMP TABLE ... AS
// EXECUTE p is unwrapped and routed through TempTableRoute (which sets the
// temp-table reservation flag) while still carrying the PS metadata.
func TestUnwrapCreateTempTableAsExecute(t *testing.T) {
	s := newTestSetup(t)

	_, err := planAndExecute(t, s, "PREPARE p AS SELECT 1 AS a")
	require.NoError(t, err)

	psi := s.psc.GetPreparedStatementInfo(s.conn.Conn.ConnectionID(), "p")
	require.NotNil(t, psi)
	canonical := psi.Name

	// Plan but don't execute — we only want to verify the primitive shape.
	const sql = "CREATE TEMP TABLE tt AS EXECUTE p"
	asts, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	require.Len(t, asts, 1)
	plan, err := s.p.Plan(sql, asts[0], s.conn.Conn)
	require.NoError(t, err)

	// The primitive should be a TempTableRoute with PreparedStatement attached.
	ttr, ok := plan.Primitive.(*engine.TempTableRoute)
	require.True(t, ok, "expected TempTableRoute primitive, got %T", plan.Primitive)
	assert.Contains(t, ttr.Query, canonical)
	require.NotNil(t, ttr.PreparedStatement)
	assert.Equal(t, canonical, ttr.PreparedStatement.Name)
}

// TestUnwrapExplainCreateTableAsExecute verifies that doubly-nested
// EXPLAIN ... CREATE TABLE ... AS EXECUTE p (as seen in pgregress
// select_into.sql and write_parallel.sql) is unwrapped correctly: the
// innermost ExecuteStmt name is rewritten and the PreparedStatement
// metadata is attached.
func TestUnwrapExplainCreateTableAsExecute(t *testing.T) {
	s := newTestSetup(t)

	_, err := planAndExecute(t, s, "PREPARE p_nested AS SELECT 1")
	require.NoError(t, err)

	psi := s.psc.GetPreparedStatementInfo(s.conn.Conn.ConnectionID(), "p_nested")
	require.NotNil(t, psi)
	canonical := psi.Name

	// Plan without executing (the mock would try to create the table).
	const sql = "EXPLAIN (COSTS OFF) CREATE TABLE tnested AS EXECUTE p_nested"
	asts, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	require.Len(t, asts, 1)
	plan, err := s.p.Plan(sql, asts[0], s.conn.Conn)
	require.NoError(t, err)

	route, ok := plan.Primitive.(*engine.Route)
	require.True(t, ok, "expected Route primitive, got %T", plan.Primitive)
	assert.Contains(t, route.Query, canonical)
	assert.Contains(t, strings.ToUpper(route.Query), "EXPLAIN")
	assert.Contains(t, strings.ToUpper(route.Query), "CREATE")
	require.NotNil(t, route.PreparedStatement)
	assert.Equal(t, canonical, route.PreparedStatement.Name)
}

// TestUnwrapExplainCreateTempTableAsExecute verifies that EXPLAIN wrapping
// CREATE TEMP TABLE AS EXECUTE uses the TempTableRoute primitive (since
// EXPLAIN ANALYZE can actually materialize the temp table).
func TestUnwrapExplainCreateTempTableAsExecute(t *testing.T) {
	s := newTestSetup(t)

	_, err := planAndExecute(t, s, "PREPARE p_nested_temp AS SELECT 1")
	require.NoError(t, err)

	psi := s.psc.GetPreparedStatementInfo(s.conn.Conn.ConnectionID(), "p_nested_temp")
	require.NotNil(t, psi)
	canonical := psi.Name

	const sql = "EXPLAIN CREATE TEMP TABLE tmp_nested AS EXECUTE p_nested_temp"
	asts, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	plan, err := s.p.Plan(sql, asts[0], s.conn.Conn)
	require.NoError(t, err)

	ttr, ok := plan.Primitive.(*engine.TempTableRoute)
	require.True(t, ok, "expected TempTableRoute primitive for EXPLAIN CREATE TEMP TABLE AS EXECUTE, got %T", plan.Primitive)
	assert.Contains(t, ttr.Query, canonical)
	require.NotNil(t, ttr.PreparedStatement)
	assert.Equal(t, canonical, ttr.PreparedStatement.Name)
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
	assert.Nil(t, call.preparedStatement)
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
	assert.Nil(t, call.preparedStatement)
}
