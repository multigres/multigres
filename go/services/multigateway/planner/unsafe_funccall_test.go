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

func TestAnalyzeSQLPreparedBodyBranches(t *testing.T) {
	analysis, err := analyzeSQLPreparedBody(nil)
	require.NoError(t, err)
	require.NotNil(t, analysis)

	_, err = analyzeSQLPreparedBody(&ast.CreatedbStmt{BaseNode: ast.BaseNode{Tag: ast.T_CreatedbStmt}, Dbname: "test"})
	require.ErrorContains(t, err, "CREATE DATABASE is not supported")

	_, err = analyzeSQLPreparedBody(parseOne(t, "SET synchronous_commit = off"))
	require.ErrorContains(t, err, "synchronous_commit")

	_, err = analyzeSQLPreparedBody(parseOne(t, "SELECT pg_read_file('/tmp/x')"))
	require.ErrorContains(t, err, "pg_read_file is not supported")

	_, err = analyzeSQLPreparedBody(parseOne(t, "SELECT set_config(name, '256MB', false) FROM pg_settings WHERE name = 'work_mem'"))
	require.ErrorContains(t, err, "dynamic set_config is not supported inside SQL PREPARE")

	require.NoError(t, validateSQLPreparedSetConfigs(nil))
	err = validateSQLPreparedSetConfigs(&statementAnalysis{SetConfigs: []setConfigCall{{IsLocalBind: ast.NewParamRef(1, 0)}}})
	require.ErrorContains(t, err, "set_config is_local argument inside SQL PREPARE must be a literal boolean")
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
		{"dblink_open", "SELECT dblink_open('cur', 'SELECT 1')", "dblink_open is not supported"},
		{"dblink_fetch", "SELECT * FROM dblink_fetch('cur', 1) AS t(c int)", "dblink_fetch is not supported"},
		{"dblink_close", "SELECT dblink_close('cur')", "dblink_close is not supported"},
		{"dblink_send_query", "SELECT dblink_send_query('conn', 'SELECT 1')", "dblink_send_query is not supported"},
		{"dblink_get_result", "SELECT * FROM dblink_get_result('conn') AS t(c int)", "dblink_get_result is not supported"},

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
			result, err := analyzeFunctionCalls(stmt)
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
			name:      "string-cast bool prefix 'tr' is treated as true",
			sql:       "SELECT set_config('work_mem', '256MB', 'tr'::bool)",
			wantCalls: nil,
		},
		{
			name:      "string-cast bool 'f' is treated as false and tracked",
			sql:       "SELECT set_config('work_mem', '256MB', 'f'::bool)",
			wantCalls: []setConfigCall{{Name: "work_mem", Value: "256MB"}},
		},
		{
			name:      "string-cast bool prefix 'of' is treated as false and tracked",
			sql:       "SELECT set_config('work_mem', '256MB', 'of'::bool)",
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
			result, err := analyzeFunctionCalls(stmt)
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
		// A dynamic argument is only accepted when the whole target list is
		// set_config(...) (the resolve-and-apply path — see
		// TestInspectExpressionFuncCalls_DynamicSetConfigAccepted). Mixed with
		// any other target it still can't be tracked, so it's rejected.
		{
			name:    "non-literal name arg (column ref) in mixed target list",
			sql:     "SELECT set_config(name, '256MB', false), x FROM gucs",
			wantMsg: "set_config name argument must be a literal constant or a bound parameter",
		},
		{
			name:    "non-literal value arg (column ref) in mixed target list",
			sql:     "SELECT set_config('work_mem', v, false), x FROM gucs",
			wantMsg: "set_config value argument must be a literal constant or a bound parameter",
		},
		{
			name:    "non-literal is_local (column ref) in mixed target list",
			sql:     "SELECT set_config('work_mem', '256MB', islocal), x FROM gucs",
			wantMsg: "set_config is_local argument must be a literal constant or a bound parameter",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := parseOne(t, tt.sql)
			result, err := analyzeFunctionCalls(stmt)
			require.Error(t, err)
			assert.Nil(t, result)
			var diag *mterrors.PgDiagnostic
			require.True(t, errors.As(err, &diag))
			assert.Equal(t, mterrors.PgSSFeatureNotSupported, diag.Code)
			assert.Contains(t, diag.Message, tt.wantMsg)
		})
	}
}

// TestInspectExpressionFuncCalls_DynamicSetConfigAccepted pins the
// resolve-and-apply path: a SELECT whose target list is entirely
// set_config(...) and that has at least one argument the literal/bound fast
// path can't resolve is accepted with DynamicSetConfig=true only for the
// pg_dump-safe shape: pg_settings.name as the dynamic GUC name, with static
// value and is_local arguments. Broader dynamic expressions would require two
// backend statements (resolve then apply), which cannot preserve native
// PostgreSQL statement atomicity or argument type checks.
func TestInspectExpressionFuncCalls_DynamicSetConfigAccepted(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "pg_dump restrict_nonsystem_relation_kind probe",
			sql:  "SELECT set_config(name, 'view, foreign-table', false) FROM pg_settings WHERE name = 'restrict_nonsystem_relation_kind'",
		},
		{
			name: "qualified pg_settings name",
			sql:  "SELECT set_config(pg_settings.name, '256MB', false) FROM pg_settings WHERE name = 'work_mem'",
		},
		{
			name: "aliased pg_settings name",
			sql:  "SELECT set_config(s.name, '256MB', false) FROM pg_settings AS s WHERE s.name = 'work_mem'",
		},
		{
			name: "multiple set_config calls with one pg_settings dynamic name",
			sql:  "SELECT set_config('application_name', 'multigres', false), set_config(name, '256MB', false) FROM pg_settings WHERE name = 'work_mem'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := parseOne(t, tt.sql)
			result, err := analyzeFunctionCalls(stmt)
			require.NoError(t, err)
			require.NotNil(t, result)
			assert.True(t, result.DynamicSetConfig, "expected DynamicSetConfig")
			assert.Empty(t, result.SetConfigs, "dynamic path tracks via the primitive, not SetConfigs")
		})
	}
}

func TestInspectExpressionFuncCalls_DynamicSetConfigRejected(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantMsg string
	}{
		{
			name:    "dynamic name from arbitrary table rejected",
			sql:     "SELECT set_config(name, '256MB', false) FROM gucs",
			wantMsg: "dynamic set_config name argument is only supported for pg_settings.name",
		},
		{
			name:    "dynamic value column rejected",
			sql:     "SELECT set_config('work_mem', v, false) FROM pg_settings",
			wantMsg: "set_config value argument must be a literal constant or a bound parameter",
		},
		{
			name:    "dynamic is_local column rejected",
			sql:     "SELECT set_config('work_mem', '256MB', islocal) FROM pg_settings",
			wantMsg: "set_config is_local argument must be a literal constant or a bound parameter",
		},
		{
			name:    "bound is_local rejected on dynamic path",
			sql:     "SELECT set_config(name, '256MB', $1) FROM pg_settings",
			wantMsg: "dynamic set_config is_local argument must be a literal boolean",
		},
		{
			name:    "integer value rejected on dynamic path",
			sql:     "SELECT set_config(name, 100, false) FROM pg_settings",
			wantMsg: "dynamic set_config value argument must be a text literal or bound text parameter",
		},
		{
			name:    "integer name rejected on dynamic path",
			sql:     "SELECT set_config(100, 'v', false), set_config(name, '256MB', false) FROM pg_settings",
			wantMsg: "dynamic set_config name argument must be a text literal, bound text parameter, or pg_settings.name",
		},
		{
			name:    "wrong value cast rejected on dynamic path",
			sql:     "SELECT set_config(name, '256MB'::int, false) FROM pg_settings",
			wantMsg: "dynamic set_config value argument must be a text literal or bound text parameter",
		},
		{
			name:    "wrong is_local cast rejected on dynamic path",
			sql:     "SELECT set_config(name, '256MB', false::text) FROM pg_settings",
			wantMsg: "dynamic set_config is_local argument must be a literal boolean",
		},
		{
			name:    "function value rejected to preserve set_config type checks",
			sql:     "SELECT set_config('work_mem', now(), false)",
			wantMsg: "set_config value argument must be a literal constant or a bound parameter",
		},
		{
			name:    "function in WHERE rejected to keep resolve side-effect-free",
			sql:     "SELECT set_config(name, '256MB', false) FROM pg_settings WHERE nextval('s') > 0",
			wantMsg: "dynamic set_config only supports simple pg_settings lookups; function calls outside set_config are not supported",
		},
		{
			name:    "computed name rejected",
			sql:     "SELECT set_config(lower(name), '256MB', false) FROM pg_settings",
			wantMsg: "dynamic set_config name argument is only supported for pg_settings.name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := parseOne(t, tt.sql)
			_, err := analyzeFunctionCalls(stmt)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantMsg)
		})
	}
}

// TestInspectExpressionFuncCalls_DynamicSetConfigNotTriggered pins the cases
// that must NOT take the resolve-and-apply path: all-literal/bound calls keep
// the fast path, and a literal is_local=true call (even with a dynamic name)
// runs transaction-scoped via Route, untracked, exactly as before.
func TestInspectExpressionFuncCalls_DynamicSetConfigNotTriggered(t *testing.T) {
	tests := []struct {
		name           string
		sql            string
		wantSetConfigs int
	}{
		{
			name:           "all literal stays on fast path",
			sql:            "SELECT set_config('work_mem', '256MB', false)",
			wantSetConfigs: 1,
		},
		{
			name:           "bound value stays on fast path",
			sql:            "SELECT set_config('search_path', $1, false)",
			wantSetConfigs: 1,
		},
		{
			name:           "literal is_local=true with dynamic name is passthrough, untracked",
			sql:            "SELECT set_config(name, 'v', true) FROM gucs",
			wantSetConfigs: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := parseOne(t, tt.sql)
			result, err := analyzeFunctionCalls(stmt)
			require.NoError(t, err)
			require.NotNil(t, result)
			assert.False(t, result.DynamicSetConfig, "should not take the dynamic path")
			assert.Len(t, result.SetConfigs, tt.wantSetConfigs)
		})
	}
}

// TestInspectExpressionFuncCalls_BoundParametersAccepted pins the
// extended-protocol shape: each set_config slot may be a wire-protocol
// bound parameter and the walker accepts it, recording a setConfigCall
// with the corresponding *Bind field populated. Decoding is deferred to
// execute time inside ApplySessionState.executeSetWithBinds.
func TestInspectExpressionFuncCalls_BoundParametersAccepted(t *testing.T) {
	tests := []struct {
		name            string
		sql             string
		wantNameBind    bool
		wantValueBind   bool
		wantIsLocalBind bool
		wantLiteralName string
		wantLiteralVal  string
	}{
		{
			name:            "bound value (Slack repro)",
			sql:             "SELECT set_config('search_path', $1, false)",
			wantLiteralName: "search_path",
			wantValueBind:   true,
		},
		{
			name:           "bound name",
			sql:            "SELECT set_config($1, 'public', false)",
			wantNameBind:   true,
			wantLiteralVal: "public",
		},
		{
			name:            "bound is_local",
			sql:             "SELECT set_config('search_path', 'public', $1)",
			wantLiteralName: "search_path",
			wantLiteralVal:  "public",
			wantIsLocalBind: true,
		},
		{
			name:            "all three bound",
			sql:             "SELECT set_config($1, $2, $3)",
			wantNameBind:    true,
			wantValueBind:   true,
			wantIsLocalBind: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := parseOne(t, tt.sql)
			result, err := analyzeFunctionCalls(stmt)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Len(t, result.SetConfigs, 1)
			sc := result.SetConfigs[0]
			assert.True(t, sc.hasBoundParams(), "expected at least one bound slot for any-slot ParamRef shape")
			assert.Equal(t, tt.wantNameBind, sc.NameBind != nil)
			assert.Equal(t, tt.wantValueBind, sc.ValueBind != nil)
			assert.Equal(t, tt.wantIsLocalBind, sc.IsLocalBind != nil)
			if !tt.wantNameBind {
				assert.Equal(t, tt.wantLiteralName, sc.Name)
			}
			if !tt.wantValueBind {
				assert.Equal(t, tt.wantLiteralVal, sc.Value)
			}
		})
	}
}

// TestInspectExpressionFuncCalls_LiteralIsLocalTrueShortCircuits pins that
// a literal is_local=true call still returns no setConfigCall — the
// transaction-scoped semantics are PG's job, gateway must not track. The
// normalizer parameterizes name/value for these calls (PostgREST hot
// path), so the walker must not require literals there either.
func TestInspectExpressionFuncCalls_LiteralIsLocalTrueShortCircuits(t *testing.T) {
	for _, sql := range []string{
		"SELECT set_config('request.jwt.claims', '{...}', true)",
		"SELECT set_config($1, $2, true)",
	} {
		t.Run(sql, func(t *testing.T) {
			stmt := parseOne(t, sql)
			result, err := analyzeFunctionCalls(stmt)
			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Empty(t, result.SetConfigs, "is_local literal true must not produce a tracker entry")
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
			result, err := analyzeFunctionCalls(stmt)
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
// plans as the same Sequence[Route, silent ApplySessionState...]. No fast-path
// for the bare case: uniform construction is worth the extra round-trip.
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
			plan, err := p.Plan(tt.sql, stmt, testConn.Conn, PlanOptions{})
			require.NoError(t, err)
			require.NotNil(t, plan)

			seq, ok := plan.Primitive.(*engine.Sequence)
			require.True(t, ok, "expected Sequence primitive, got %T", plan.Primitive)
			require.Len(t, seq.Primitives, len(tt.wantTrackers)+1)

			_, ok = seq.Primitives[0].(*engine.Route)
			require.True(t, ok, "first primitive should be Route, got %T", seq.Primitives[0])

			for i, wantName := range tt.wantTrackers {
				primIdx := i + 1
				applyState, ok := seq.Primitives[primIdx].(*engine.ApplySessionState)
				require.True(t, ok, "primitive %d should be ApplySessionState, got %T", primIdx, seq.Primitives[primIdx])
				assert.True(t, applyState.SilentTracking, "tracker step %d must be silent; Route owns the client response", primIdx)
				assert.Equal(t, wantName, applyState.VariableStmt.Name)
			}
		})
	}
}

// TestPlan_DynamicSetConfig_ProducesResolvePrimitive verifies that the pg_dump
// shape (target list all set_config with a dynamic argument) plans as a single
// ResolveTrackSetConfig primitive, whose unroll projection replaces each
// set_config(a, b, c) with its three arguments while preserving FROM/WHERE.
func TestPlan_DynamicSetConfig_ProducesResolvePrimitive(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		wantUnroll  string
		wantAliases []string
	}{
		{
			name:        "pg_dump probe",
			sql:         "SELECT set_config(name, 'view, foreign-table', false) FROM pg_settings WHERE name = 'restrict_nonsystem_relation_kind'",
			wantUnroll:  "SELECT name, 'view, foreign-table', FALSE FROM pg_settings WHERE name = 'restrict_nonsystem_relation_kind'",
			wantAliases: []string{""},
		},
		{
			name:        "multi-column with alias",
			sql:         "SELECT set_config(name, '1', false) AS a, set_config('application_name', 'multigres', false) FROM pg_settings WHERE name = 'work_mem'",
			wantUnroll:  "SELECT name, '1', FALSE, 'application_name', 'multigres', FALSE FROM pg_settings WHERE name = 'work_mem'",
			wantAliases: []string{"a", ""},
		},
	}

	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := parseOne(t, tt.sql)
			plan, err := p.Plan(tt.sql, stmt, testConn.Conn, PlanOptions{})
			require.NoError(t, err)
			require.NotNil(t, plan)

			prim, ok := plan.Primitive.(*engine.ResolveTrackSetConfig)
			require.True(t, ok, "expected ResolveTrackSetConfig, got %T", plan.Primitive)
			assert.Equal(t, tt.wantAliases, prim.Aliases)
			assert.Equal(t, tt.wantUnroll, prim.ResolveRoute.GetQuery())
			assert.Equal(t, engine.PlanTypeResolveTrackSetConfig, plan.Type)
			// No advisory lock: the resolve runs through a plain Route.
			_, isPlainRoute := prim.ResolveRoute.(*engine.Route)
			assert.True(t, isPlainRoute, "expected plain Route, got %T", prim.ResolveRoute)
		})
	}
}

// TestPlan_DynamicSetConfig_RejectsAdvisoryLockArg verifies that dynamic
// set_config no longer evaluates arbitrary value expressions during the resolve
// phase. Such expressions would run in a separate backend statement before the
// synthesized apply, breaking PostgreSQL's single-statement semantics.
func TestPlan_DynamicSetConfig_RejectsAdvisoryLockArg(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	sql := "SELECT set_config('x', pg_try_advisory_lock(1)::text, false)"
	stmt := parseOne(t, sql)
	_, err := p.Plan(sql, stmt, testConn.Conn, PlanOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "set_config value argument must be a literal constant or a bound parameter")
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
			// Dynamic value mixed with another target can't take the
			// resolve-and-apply path, so it's still rejected.
			"non-literal set_config in mixed target list rejected",
			"SELECT set_config('x', v, false), 1 FROM gucs",
			"set_config value argument must be a literal constant",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := parseOne(t, tt.sql)
			plan, err := p.Plan(tt.sql, stmt, testConn.Conn, PlanOptions{})
			require.Error(t, err)
			assert.Nil(t, plan)
			var diag *mterrors.PgDiagnostic
			require.True(t, errors.As(err, &diag))
			assert.Equal(t, mterrors.PgSSFeatureNotSupported, diag.Code)
			assert.Contains(t, diag.Message, tt.want)
		})
	}
}
