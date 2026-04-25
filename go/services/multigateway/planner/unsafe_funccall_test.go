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
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/services/multigateway/engine"
)

func parseOne(t *testing.T, sql string) ast.Stmt {
	t.Helper()
	stmts, err := parser.ParseSQL(sql)
	require.NoError(t, err, "parse failed: %s", sql)
	require.Len(t, stmts, 1, "expected exactly one statement: %s", sql)
	return stmts[0]
}

// TestInspectExpressionFuncCalls_Blocklist covers the hard-reject list —
// built-in functions that must be refused wherever they appear in an
// expression tree.
func TestInspectExpressionFuncCalls_Blocklist(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantMsg string
	}{
		{"dblink bare", "SELECT dblink('host=example.com', 'SELECT 1')", "dblink is not supported"},
		{"dblink schema-qualified", "SELECT pg_catalog.dblink('host=example.com', 'SELECT 1')", "dblink is not supported"},
		{"dblink in WHERE", "SELECT 1 FROM t WHERE (dblink_exec('x','y')) = 0", "dblink_exec is not supported"},
		{"dblink_connect", "SELECT dblink_connect('host=example.com')", "dblink_connect is not supported"},
		{"dblink_connect_u", "SELECT dblink_connect_u('host=example.com')", "dblink_connect_u is not supported"},

		{"pg_read_file", "SELECT pg_read_file('/etc/passwd')", "pg_read_file is not supported"},
		{"pg_read_binary_file", "SELECT pg_read_binary_file('/etc/passwd')", "pg_read_binary_file is not supported"},
		{"pg_ls_dir", "SELECT pg_ls_dir('/')", "pg_ls_dir is not supported"},
		{"pg_stat_file", "SELECT pg_stat_file('/etc/passwd')", "pg_stat_file is not supported"},

		{"lo_import", "SELECT lo_import('/tmp/x')", "lo_import is not supported"},
		{"lo_export", "SELECT lo_export(1, '/tmp/x')", "lo_export is not supported"},

		{"query_to_xml", "SELECT query_to_xml('SELECT 1', true, false, '')", "query_to_xml is not supported"},
		{"query_to_xmlschema", "SELECT query_to_xmlschema('SELECT 1', true, false, '')", "query_to_xmlschema is not supported"},
		{"table_to_xml", "SELECT table_to_xml('t'::regclass, true, false, '')", "table_to_xml is not supported"},
		{"table_to_xmlschema", "SELECT table_to_xmlschema('t'::regclass, true, false, '')", "table_to_xmlschema is not supported"},
		{"cursor_to_xml", "SELECT cursor_to_xml('c', 1, true, false, '')", "cursor_to_xml is not supported"},

		{
			"blocklist in subquery",
			"SELECT x FROM (SELECT dblink('h','q') AS dblink) s",
			"dblink is not supported",
		},
		{
			"blocklist in CTE",
			"WITH bad AS (SELECT pg_read_file('/etc/passwd')) SELECT * FROM bad",
			"pg_read_file is not supported",
		},
		{
			"blocklist in INSERT VALUES",
			"INSERT INTO t VALUES (pg_ls_dir('/'))",
			"pg_ls_dir is not supported",
		},
		{
			"blocklist in DEFAULT expression",
			"CREATE TABLE t (x text DEFAULT pg_read_file('/etc/passwd'))",
			"pg_read_file is not supported",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := parseOne(t, tt.sql)
			result, err := inspectExpressionFuncCalls(stmt)
			require.Nil(t, result)
			require.Error(t, err)

			var diag *mterrors.PgDiagnostic
			require.True(t, errors.As(err, &diag))
			assert.Equal(t, mterrors.PgSSFeatureNotSupported, diag.Code)
			assert.Contains(t, diag.Message, tt.wantMsg)
		})
	}
}

// TestInspectExpressionFuncCalls_SetConfigAccepted covers the allowed shapes
// of set_config — directly as a top-level SELECT target-list entry. These
// must be accepted and returned in result.SetConfigs for the planner to
// turn into SessionSettings updates.
func TestInspectExpressionFuncCalls_SetConfigAccepted(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantCalls []setConfigCall
	}{
		{
			name:      "bare set_config false",
			sql:       "SELECT set_config('work_mem', '256MB', false)",
			wantCalls: []setConfigCall{{Name: "work_mem", Value: "256MB"}},
		},
		{
			name:      "bare pg_catalog.set_config false",
			sql:       "SELECT pg_catalog.set_config('search_path', 'myschema', false)",
			wantCalls: []setConfigCall{{Name: "search_path", Value: "myschema"}},
		},
		{
			name:      "set_config in target list with another target",
			sql:       "SELECT set_config('work_mem', '256MB', false), 1 AS other",
			wantCalls: []setConfigCall{{Name: "work_mem", Value: "256MB"}},
		},
		{
			name:      "set_config alongside SELECT * FROM t",
			sql:       "SELECT set_config('work_mem', '256MB', false), * FROM t",
			wantCalls: []setConfigCall{{Name: "work_mem", Value: "256MB"}},
		},
		{
			name: "multiple set_configs in target list",
			sql:  "SELECT set_config('work_mem', '256MB', false), set_config('search_path', 'myschema', false)",
			wantCalls: []setConfigCall{
				{Name: "work_mem", Value: "256MB"},
				{Name: "search_path", Value: "myschema"},
			},
		},
		{
			name:      "set_config is_local=true is accepted but not tracked",
			sql:       "SELECT set_config('work_mem', '256MB', true)",
			wantCalls: nil,
		},
		{
			name:      "TypeCast on value is unwrapped",
			sql:       "SELECT set_config('work_mem', '256MB'::text, false)",
			wantCalls: []setConfigCall{{Name: "work_mem", Value: "256MB"}},
		},
		{
			name:      "TypeCast on name is unwrapped",
			sql:       "SELECT set_config('work_mem'::text, '256MB', false)",
			wantCalls: []setConfigCall{{Name: "work_mem", Value: "256MB"}},
		},
		{
			name:      "TypeCast on is_local is unwrapped",
			sql:       "SELECT set_config('work_mem', '256MB', false::bool)",
			wantCalls: []setConfigCall{{Name: "work_mem", Value: "256MB"}},
		},
		{
			name:      "string-cast bool 't' is treated as true",
			sql:       "SELECT set_config('work_mem', '256MB', 't'::bool)",
			wantCalls: nil,
		},
		{
			name:      "string-cast bool 'true' is treated as true",
			sql:       "SELECT set_config('work_mem', '256MB', 'true'::bool)",
			wantCalls: nil,
		},
		{
			name:      "string-cast bool 'f' is treated as false and tracked",
			sql:       "SELECT set_config('work_mem', '256MB', 'f'::bool)",
			wantCalls: []setConfigCall{{Name: "work_mem", Value: "256MB"}},
		},
		{
			name:      "integer literal in value position is rendered to text",
			sql:       "SELECT set_config('statement_timeout', 100, false)",
			wantCalls: []setConfigCall{{Name: "statement_timeout", Value: "100"}},
		},
		{
			name:      "is_local=true accepts parameterized name and value",
			sql:       "SELECT set_config($1, $2, true)",
			wantCalls: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := parseOne(t, tt.sql)
			result, err := inspectExpressionFuncCalls(stmt)
			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Equal(t, tt.wantCalls, result.SetConfigs)
		})
	}
}

// TestInspectExpressionFuncCalls_SetConfigRejected covers set_config calls
// in positions where we cannot faithfully represent the side effect: a SET
// wouldn't match the conditional / repeated / nested semantics that
// set_config has in those positions.
func TestInspectExpressionFuncCalls_SetConfigRejected(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantMsg string
	}{
		{
			name:    "set_config in WHERE",
			sql:     "SELECT 1 FROM t WHERE set_config('work_mem','256MB',false) IS NOT NULL",
			wantMsg: "set_config is only supported as a top-level SELECT target list entry",
		},
		{
			name:    "set_config in subquery",
			sql:     "SELECT * FROM (SELECT set_config('work_mem','256MB',false) AS v, 1 AS other) s",
			wantMsg: "set_config is only supported as a top-level SELECT target list entry",
		},
		{
			name:    "set_config in CTE",
			sql:     "WITH cfg AS (SELECT set_config('work_mem','256MB',false)) SELECT * FROM cfg, t",
			wantMsg: "set_config is only supported as a top-level SELECT target list entry",
		},
		{
			name:    "set_config in INSERT VALUES",
			sql:     "INSERT INTO t(x) VALUES (set_config('work_mem','256MB',false))",
			wantMsg: "set_config is only supported as a top-level SELECT target list entry",
		},
		{
			name:    "set_config in DEFAULT expression",
			sql:     "CREATE TABLE t (x text DEFAULT set_config('work_mem','256MB',false))",
			wantMsg: "set_config is only supported as a top-level SELECT target list entry",
		},
		{
			name:    "set_config nested inside another function",
			sql:     "SELECT length(set_config('work_mem','256MB',false))",
			wantMsg: "set_config is only supported as a top-level SELECT target list entry",
		},
		{
			name:    "set_config in SELECT INTO TEMP target list",
			sql:     "SELECT set_config('work_mem','256MB',false), * INTO TEMP foo FROM t",
			wantMsg: "set_config is only supported as a top-level SELECT target list entry",
		},
		{
			name:    "non-literal name arg",
			sql:     "SELECT set_config(name, '256MB', false) FROM gucs",
			wantMsg: "set_config name argument must be a literal constant",
		},
		{
			name:    "non-literal value arg",
			sql:     "SELECT set_config('work_mem', v, false) FROM gucs",
			wantMsg: "set_config value argument must be a literal constant",
		},
		{
			name:    "non-literal is_local",
			sql:     "SELECT set_config('work_mem', '256MB', islocal) FROM gucs",
			wantMsg: "set_config is_local argument must be a literal constant",
		},
		{
			name:    "bound-parameter name arg gets parameter-specific message",
			sql:     "SELECT set_config($1, '256MB', false)",
			wantMsg: "must be a literal, not a bound parameter",
		},
		{
			name:    "bound-parameter value arg gets parameter-specific message",
			sql:     "SELECT set_config('work_mem', $1, false)",
			wantMsg: "must be a literal, not a bound parameter",
		},
		{
			name:    "bound-parameter is_local arg gets parameter-specific message",
			sql:     "SELECT set_config('work_mem', '256MB', $1)",
			wantMsg: "must be a literal, not a bound parameter",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := parseOne(t, tt.sql)
			result, err := inspectExpressionFuncCalls(stmt)
			require.Error(t, err)
			assert.Nil(t, result)
			var diag *mterrors.PgDiagnostic
			require.True(t, errors.As(err, &diag))
			assert.Equal(t, mterrors.PgSSFeatureNotSupported, diag.Code)
			assert.Contains(t, diag.Message, tt.wantMsg)
		})
	}
}

// TestInspectExpressionFuncCalls_Allowed confirms plain queries that don't
// involve the blocklist or set_config pass cleanly.
func TestInspectExpressionFuncCalls_Allowed(t *testing.T) {
	allowed := []string{
		"SELECT 1",
		"SELECT abs(-42)",
		"SELECT now()",
		"SELECT length('hello')",
		"SELECT current_setting('work_mem')",
		"SELECT * FROM t WHERE coalesce(x, 0) > 5",
		"INSERT INTO t(x) VALUES (gen_random_uuid())",
		"WITH c AS (SELECT now()) SELECT * FROM c",
		"SELECT pg_sleep(0)",
	}
	for _, sql := range allowed {
		t.Run(sql, func(t *testing.T) {
			stmt := parseOne(t, sql)
			result, err := inspectExpressionFuncCalls(stmt)
			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Empty(t, result.SetConfigs)
		})
	}
}

// TestResolveFuncName checks the pg_catalog-qualification normalization.
// A user writing `pg_catalog.dblink(...)` must hit the same blocklist entry
// as bare `dblink(...)`.
func TestResolveFuncName(t *testing.T) {
	tests := []struct {
		name  string
		parts []string
		want  string
	}{
		{"unqualified lowercase", []string{"dblink"}, "dblink"},
		{"unqualified mixed case", []string{"DBLink"}, "dblink"},
		{"pg_catalog qualified", []string{"pg_catalog", "dblink"}, "dblink"},
		{"PG_CATALOG uppercase qualified", []string{"PG_CATALOG", "DBLINK"}, "dblink"},
		{"user-schema qualified - not a built-in", []string{"public", "dblink"}, ""},
		{"three-part name (not a built-in)", []string{"db", "public", "dblink"}, ""},
		{"empty", []string{}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			list := ast.NewNodeList()
			for _, p := range tt.parts {
				list.Items = append(list.Items, ast.NewString(p))
			}
			assert.Equal(t, tt.want, resolveFuncName(list))
		})
	}
}

// TestPlan_SetConfig_ProducesSequence verifies that every accepted
// `SELECT set_config(...)` shape — bare or mixed with a FROM/targets —
// plans as the same Sequence[silent ApplySessionState..., Route]. No
// fast-path for the bare case: uniform construction is worth the extra
// round-trip.
func TestPlan_SetConfig_ProducesSequence(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		wantTrackers []string // variable names, in target-list order
	}{
		{
			name:         "bare",
			sql:          "SELECT set_config('work_mem', '256MB', false)",
			wantTrackers: []string{"work_mem"},
		},
		{
			name:         "mixed with SELECT *",
			sql:          "SELECT set_config('work_mem', '256MB', false), * FROM t",
			wantTrackers: []string{"work_mem"},
		},
		{
			name:         "two set_configs in target list",
			sql:          "SELECT set_config('work_mem', '256MB', false), set_config('search_path', 'myschema', false)",
			wantTrackers: []string{"work_mem", "search_path"},
		},
	}

	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := parseOne(t, tt.sql)
			plan, err := p.Plan(tt.sql, stmt, testConn.Conn)
			require.NoError(t, err)
			require.NotNil(t, plan)

			seq, ok := plan.Primitive.(*engine.Sequence)
			require.True(t, ok, "expected Sequence primitive, got %T", plan.Primitive)
			require.Len(t, seq.Primitives, len(tt.wantTrackers)+1)

			for i, wantName := range tt.wantTrackers {
				applyState, ok := seq.Primitives[i].(*engine.ApplySessionState)
				require.True(t, ok, "primitive %d should be ApplySessionState, got %T", i, seq.Primitives[i])
				assert.True(t, applyState.SilentTracking, "tracker step %d must be silent; Route owns the client response", i)
				assert.Equal(t, wantName, applyState.VariableStmt.Name)
			}

			_, ok = seq.Primitives[len(tt.wantTrackers)].(*engine.Route)
			require.True(t, ok, "last primitive should be Route, got %T", seq.Primitives[len(tt.wantTrackers)])
		})
	}
}

// TestPlan_RejectsUnsafeFuncCalls verifies Plan() itself rejects blocklisted
// function calls (not just the walker in isolation).
func TestPlan_RejectsUnsafeFuncCalls(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	tests := []struct {
		name string
		sql  string
		want string
	}{
		{"dblink rejected", "SELECT dblink('h','q')", "dblink is not supported"},
		{"pg_read_file rejected", "SELECT pg_read_file('/etc/passwd')", "pg_read_file is not supported"},
		{"lo_import rejected", "SELECT lo_import('/tmp/x')", "lo_import is not supported"},
		{"query_to_xml rejected", "SELECT query_to_xml('SELECT 1', true, false, '')", "query_to_xml is not supported"},
		{
			"embedded set_config(..., false) rejected",
			"SELECT 1 FROM t WHERE set_config('x','y',false) IS NOT NULL",
			"set_config is only supported as a top-level SELECT target list entry",
		},
		{
			"non-literal set_config rejected",
			"SELECT set_config('x', v, false) FROM gucs",
			"set_config value argument must be a literal constant",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := parseOne(t, tt.sql)
			plan, err := p.Plan(tt.sql, stmt, testConn.Conn)
			require.Error(t, err)
			assert.Nil(t, plan)
			var diag *mterrors.PgDiagnostic
			require.True(t, errors.As(err, &diag))
			assert.Equal(t, mterrors.PgSSFeatureNotSupported, diag.Code)
			assert.Contains(t, diag.Message, tt.want)
		})
	}
}
