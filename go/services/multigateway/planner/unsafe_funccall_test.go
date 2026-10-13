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
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
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
	analysis, err := analyzeSQLPreparedBody(nil, false, false)
	require.NoError(t, err)
	require.NotNil(t, analysis)

	_, err = analyzeSQLPreparedBody(&ast.CreatedbStmt{BaseNode: ast.BaseNode{Tag: ast.T_CreatedbStmt}, Dbname: "test"}, false, false)
	require.ErrorContains(t, err, "CREATE DATABASE is not supported")

	_, err = analyzeSQLPreparedBody(parseOne(t, "SET synchronous_commit = off"), false, false)
	require.ErrorContains(t, err, "synchronous_commit")

	_, err = analyzeSQLPreparedBody(parseOne(t, "SELECT pg_read_file('/tmp/x')"), false, false)
	require.ErrorContains(t, err, "pg_read_file is not supported")

	_, err = analyzeSQLPreparedBody(parseOne(t, "SELECT set_config(name, '256MB', false) FROM pg_settings WHERE name = 'work_mem'"), false, false)
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
			result, err := analyzeFunctionCalls(stmt, true, false)
			require.Nil(t, result)
			require.Error(t, err)

			var diag *mterrors.PgDiagnostic
			require.True(t, errors.As(err, &diag))
			assert.Equal(t, mterrors.PgSSFeatureNotSupported, diag.Code)
			assert.Contains(t, diag.Message, tt.wantMsg)
		})
	}
}

// TestInspectExpressionFuncCalls_ReplicationSlots covers the
// pg_create_physical_replication_slot / pg_create_logical_replication_slot
// enforcement: only a literal temporary=true is accepted, since Multigres
// cannot yet migrate a replication slot's position across a primary
// failover.
func TestInspectExpressionFuncCalls_ReplicationSlots(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
		wantMsg string
	}{
		{
			name: "physical: temporary=true literal accepted",
			sql:  "SELECT pg_create_physical_replication_slot('s1', false, true)",
		},
		{
			name:    "physical: temporary=false literal rejected",
			sql:     "SELECT pg_create_physical_replication_slot('s1', false, false)",
			wantErr: true,
			wantMsg: "pg_create_physical_replication_slot requires temporary=true",
		},
		{
			name:    "physical: temporary omitted (2-arg call) rejected",
			sql:     "SELECT pg_create_physical_replication_slot('s1', false)",
			wantErr: true,
			wantMsg: "pg_create_physical_replication_slot requires temporary=true",
		},
		{
			name:    "physical: temporary omitted (1-arg call) rejected",
			sql:     "SELECT pg_create_physical_replication_slot('s1')",
			wantErr: true,
			wantMsg: "pg_create_physical_replication_slot requires temporary=true",
		},
		{
			name:    "physical: non-literal temporary (bound param) rejected",
			sql:     "SELECT pg_create_physical_replication_slot('s1', false, $1)",
			wantErr: true,
			wantMsg: "pg_create_physical_replication_slot requires temporary=true",
		},
		{
			name:    "physical: non-literal temporary (column ref) rejected",
			sql:     "SELECT pg_create_physical_replication_slot('s1', false, col) FROM t",
			wantErr: true,
			wantMsg: "pg_create_physical_replication_slot requires temporary=true",
		},
		{
			name: "physical: qualified pg_catalog form accepted with temporary=true",
			sql:  "SELECT pg_catalog.pg_create_physical_replication_slot('s1', false, true)",
		},
		{
			name:    "physical: qualified pg_catalog form rejected without temporary=true",
			sql:     "SELECT pg_catalog.pg_create_physical_replication_slot('s1', false, false)",
			wantErr: true,
			wantMsg: "pg_create_physical_replication_slot requires temporary=true",
		},
		{
			name: "logical: temporary=true literal accepted",
			sql:  "SELECT pg_create_logical_replication_slot('s1', 'pgoutput', true)",
		},
		{
			name:    "logical: temporary=false literal rejected",
			sql:     "SELECT pg_create_logical_replication_slot('s1', 'pgoutput', false)",
			wantErr: true,
			wantMsg: "pg_create_logical_replication_slot requires temporary=true",
		},
		{
			name:    "logical: temporary omitted rejected",
			sql:     "SELECT pg_create_logical_replication_slot('s1', 'pgoutput')",
			wantErr: true,
			wantMsg: "pg_create_logical_replication_slot requires temporary=true",
		},
		{
			name:    "logical: non-literal temporary (bound param) rejected",
			sql:     "SELECT pg_create_logical_replication_slot('s1', 'pgoutput', $1)",
			wantErr: true,
			wantMsg: "pg_create_logical_replication_slot requires temporary=true",
		},
		{
			name: "logical: qualified pg_catalog form accepted with temporary=true",
			sql:  "SELECT pg_catalog.pg_create_logical_replication_slot('s1', 'pgoutput', true)",
		},
		{
			name: "pg_drop_replication_slot is unaffected",
			sql:  "SELECT pg_drop_replication_slot('s1')",
		},
		{
			name:    "logical: failover=true rejected without the flag",
			sql:     "SELECT pg_create_logical_replication_slot('s1', 'pgoutput', false, false, true)",
			wantErr: true,
			wantMsg: "pg_create_logical_replication_slot requires temporary=true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := parseOne(t, tt.sql)
			result, err := analyzeFunctionCalls(stmt, true, false)
			if !tt.wantErr {
				require.NoError(t, err)
				require.NotNil(t, result)
				return
			}
			require.Error(t, err)
			assert.Nil(t, result)
			var diag *mterrors.PgDiagnostic
			require.True(t, errors.As(err, &diag))
			assert.Equal(t, mterrors.PgSSFeatureNotSupported, diag.Code)
			assert.Contains(t, diag.Message, tt.wantMsg)
		})
	}
}

// TestInspectExpressionFuncCalls_ReplicationSlots_FailoverAdmission covers
// the admitFailoverSlots=true behavior: a non-temporary logical slot is
// admitted only when the call spells out a literal failover=true itself,
// positionally or by name. An omitted failover argument is rejected like any
// other non-temporary call — the check is a pure predicate about what the
// client actually wrote, never a promise that some later code path will
// inject the argument (see rejectNonTemporaryReplicationSlot).
func TestInspectExpressionFuncCalls_ReplicationSlots_FailoverAdmission(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
		wantMsg string
	}{
		{
			name: "logical: temporary=false, failover=true accepted",
			sql:  "SELECT pg_create_logical_replication_slot('s1', 'pgoutput', false, false, true)",
		},
		{
			name:    "logical: temporary=false, failover=false rejected (explicit opt-out)",
			sql:     "SELECT pg_create_logical_replication_slot('s1', 'pgoutput', false, false, false)",
			wantErr: true,
			wantMsg: "pg_create_logical_replication_slot requires temporary=true",
		},
		{
			name:    "logical: temporary=false, failover omitted rejected (must be explicit)",
			sql:     "SELECT pg_create_logical_replication_slot('s1', 'pgoutput', false)",
			wantErr: true,
			wantMsg: "pg_create_logical_replication_slot requires temporary=true",
		},
		{
			name:    "logical: non-literal failover (bound param) rejected",
			sql:     "SELECT pg_create_logical_replication_slot('s1', 'pgoutput', false, false, $1)",
			wantErr: true,
			wantMsg: "pg_create_logical_replication_slot requires temporary=true",
		},
		{
			name:    "logical: non-literal temporary (bound param), failover omitted, rejected",
			sql:     "SELECT pg_create_logical_replication_slot('s1', 'pgoutput', $1)",
			wantErr: true,
			wantMsg: "pg_create_logical_replication_slot requires temporary=true",
		},
		{
			name:    "logical: non-literal temporary (bound param) rejected even with literal failover=true",
			sql:     "SELECT pg_create_logical_replication_slot('s1', 'pgoutput', $1, false, true)",
			wantErr: true,
			wantMsg: "pg_create_logical_replication_slot requires temporary=true",
		},
		{
			name: "logical: temporary=true still accepted regardless of failover",
			sql:  "SELECT pg_create_logical_replication_slot('s1', 'pgoutput', true, false, false)",
		},
		{
			name:    "physical: failover has no meaning, temporary=false still rejected",
			sql:     "SELECT pg_create_physical_replication_slot('s1', false, false)",
			wantErr: true,
			wantMsg: "pg_create_physical_replication_slot requires temporary=true",
		},
		{
			name: "logical: named failover => true accepted, temporary/twophase omitted",
			sql:  "SELECT pg_create_logical_replication_slot('s1', 'pgoutput', failover => true)",
		},
		{
			name: "logical: named temporary => false, failover => true accepted",
			sql:  "SELECT pg_create_logical_replication_slot('s1', 'pgoutput', temporary => false, failover => true)",
		},
		{
			name:    "logical: named failover => false rejected (explicit opt-out)",
			sql:     "SELECT pg_create_logical_replication_slot('s1', 'pgoutput', failover => false)",
			wantErr: true,
			wantMsg: "pg_create_logical_replication_slot requires temporary=true",
		},
		{
			name: "logical: named temporary => true accepted without failover",
			sql:  "SELECT pg_create_logical_replication_slot('s1', 'pgoutput', temporary => true)",
		},
		{
			name:    "logical: bare two-argument call rejected (failover must be explicit)",
			sql:     "SELECT pg_create_logical_replication_slot('s1', 'pgoutput')",
			wantErr: true,
			wantMsg: "pg_create_logical_replication_slot requires temporary=true",
		},
		{
			name:    "physical: temporary omitted still rejected",
			sql:     "SELECT pg_create_physical_replication_slot('s1')",
			wantErr: true,
			wantMsg: "pg_create_physical_replication_slot requires temporary=true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := parseOne(t, tt.sql)
			result, err := analyzeFunctionCalls(stmt, true, true)
			if !tt.wantErr {
				require.NoError(t, err)
				require.NotNil(t, result)
				return
			}
			require.Error(t, err)
			assert.Nil(t, result)
			var diag *mterrors.PgDiagnostic
			require.True(t, errors.As(err, &diag))
			assert.Equal(t, mterrors.PgSSFeatureNotSupported, diag.Code)
			assert.Contains(t, diag.Message, tt.wantMsg)
		})
	}
}

// TestReplicationSlotAfterNormalization confirms the temporary=true check
// survives literal normalization: the planner runs against the normalized
// AST under the plan cache (see executor.go), and the normalizer must keep
// the `temporary` argument literal precisely so this check can still see it
// (see isPlannerLiteralFunc in normalizer.go). Calling analyzeFunctionCalls
// directly on the un-normalized tree, as the table above does, would not
// have caught a regression here.
func TestReplicationSlotAfterNormalization(t *testing.T) {
	accept := func(t *testing.T, sql string) {
		t.Helper()
		norm := ast.Normalize(parseOne(t, sql))
		_, err := analyzeStatement(norm.NormalizedAST, false, false)
		assert.NoError(t, err, "normalized SQL: %s", norm.NormalizedSQL)
	}
	reject := func(t *testing.T, sql string) {
		t.Helper()
		norm := ast.Normalize(parseOne(t, sql))
		_, err := analyzeStatement(norm.NormalizedAST, false, false)
		require.Error(t, err, "normalized SQL: %s", norm.NormalizedSQL)
		var diag *mterrors.PgDiagnostic
		require.True(t, errors.As(err, &diag))
		assert.Contains(t, diag.Message, "requires temporary=true")
	}

	t.Run("physical: temporary=true literal survives normalization", func(t *testing.T) {
		accept(t, "SELECT pg_create_physical_replication_slot('s1', false, true)")
	})
	t.Run("logical: temporary=true literal survives normalization", func(t *testing.T) {
		accept(t, "SELECT pg_create_logical_replication_slot('s1', 'pgoutput', true)")
	})
	t.Run("physical: temporary=false literal still rejected after normalization", func(t *testing.T) {
		reject(t, "SELECT pg_create_physical_replication_slot('s1', false, false)")
	})
	t.Run("logical: temporary=false literal still rejected after normalization", func(t *testing.T) {
		reject(t, "SELECT pg_create_logical_replication_slot('s1', 'pgoutput', false)")
	})
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := parseOne(t, tt.sql)
			result, err := analyzeFunctionCalls(stmt, true, false)
			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Equal(t, tt.wantCalls, result.SetConfigs)
		})
	}
}

// TestSetConfigLiteralNullValue pins PostgreSQL's set_config NULL semantics at
// plan time. set_config is NOT strict (pg_proc.proisstrict = false):
// set_config(name, NULL, false) clears the parameter and returns the restored
// default — indistinguishable from RESET on PostgreSQL 17. The planner must
// therefore accept it and emit a VAR_RESET synthetic so the tracker removes
// the entry, instead of rejecting the statement as it did when the value was
// merely "not a literal string".
func TestSetConfigLiteralNullValue(t *testing.T) {
	t.Run("ordinary GUC tracks a reset", func(t *testing.T) {
		result, err := analyzeFunctionCalls(parseOne(t, "SELECT set_config('work_mem', NULL, false)"), true, false)
		require.NoError(t, err)
		require.Len(t, result.SetConfigs, 1)
		sc := result.SetConfigs[0]
		assert.True(t, sc.ValueIsNull)
		assert.Equal(t, "work_mem", sc.Name)
		assert.Equal(t, ast.VAR_RESET, syntheticSetStmt(sc).Kind,
			"a NULL value must track as a removal, not a value write")
	})

	t.Run("search_path reset needs no pg_temp vet", func(t *testing.T) {
		result, err := analyzeFunctionCalls(parseOne(t, "SELECT set_config('search_path', NULL, false)"), true, false)
		require.NoError(t, err)
		require.Len(t, result.SetConfigs, 1)
		assert.True(t, result.SetConfigs[0].ValueIsNull)
	})

	t.Run("is_local=true reset is untracked passthrough", func(t *testing.T) {
		// PostgreSQL scopes the reset to the transaction, so the gateway holds
		// no state for it.
		for _, sql := range []string{
			"SELECT set_config('work_mem', NULL, true)",
			"SELECT set_config('search_path', NULL, true)",
		} {
			result, err := analyzeFunctionCalls(parseOne(t, sql), true, false)
			require.NoError(t, err, sql)
			assert.Empty(t, result.SetConfigs, sql)
		}
	})

	t.Run("gateway-managed variable stays fail-closed", func(t *testing.T) {
		// The gateway owns the value and has no per-variable reset primitive.
		_, err := analyzeFunctionCalls(parseOne(t, "SELECT set_config('statement_timeout', NULL, false)"), true, false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "RESET statement_timeout")
	})

	t.Run("bound name stays fail-closed", func(t *testing.T) {
		// The VAR_RESET synthetic cannot resolve a bound name; resetting a
		// placeholder would silently drift from the backend.
		_, err := analyzeFunctionCalls(parseOne(t, "SELECT set_config($1, NULL, false)"), true, false)
		require.Error(t, err)
	})
}

// TestSetConfigDirectAndPreparedParity pins the invariant that a set_config
// body must behave identically whether it runs directly or inside a SQL
// PREPARE — the property validateSQLPreparedSetConfigs' own comment says the
// prepared form must not violate.
//
// It exists because the two paths carry the planner's setConfigCall in
// different shapes: the direct path folds it into a synthetic VariableSetStmt
// (VAR_RESET for a NULL value), while the prepared path copies selected fields
// into engine.SQLPreparedSetConfig. A field added to setConfigCall and wired
// into only one of those — as ValueIsNull originally was — makes the prepared
// form silently diverge, tracking an empty string where the direct form
// correctly tracks a removal. Comparing the reset disposition across both
// conversions catches that class of omission at the boundary where it happens.
func TestSetConfigDirectAndPreparedParity(t *testing.T) {
	bodies := []string{
		"SELECT set_config('work_mem', NULL, false)",
		"SELECT set_config('search_path', NULL, false)",
		"SELECT set_config('work_mem', '64MB', false)",
		"SELECT set_config('search_path', 'public', false)",
		"SELECT set_config('work_mem', $1, false)",
	}
	for _, body := range bodies {
		t.Run(body, func(t *testing.T) {
			result, err := analyzeFunctionCalls(parseOne(t, body), true, false)
			require.NoError(t, err)
			require.Len(t, result.SetConfigs, 1)
			sc := result.SetConfigs[0]

			directIsReset := syntheticSetStmt(sc).Kind == ast.VAR_RESET
			prepared := sqlPreparedSetConfigs(result.SetConfigs)
			require.Len(t, prepared, 1)

			assert.Equal(t, directIsReset, prepared[0].ValueIsNull,
				"direct and prepared paths must agree on whether this call is a reset")
			assert.Equal(t, sc.Name, prepared[0].Name)
			assert.Equal(t, sc.Value, prepared[0].Value)
			assert.Equal(t, sc.ValueBind, prepared[0].ValueParam)
			assert.Equal(t, sc.IsLocalLiteralTrue, prepared[0].IsLocalLiteralTrue)
		})
	}
}

// TestSetConfigIsLocalTrueBoundVetOnly pins the vet-only disposition for the
// PostgREST hot path: set_config with bound slots and literal is_local=true is
// accepted and produces a setConfigCall carrying the bind refs, so the plan
// builds an ApplySessionStateFromBind whose resolveSetConfig vets the resolved
// name/value (gateway-managed, restricted GUC, search_path pg_temp) before the
// Route reaches the backend — and then tracks nothing. A fully-literal
// benign call still short-circuits to no setConfigCall (see the "is_local=true
// is accepted but not tracked" case above).
func TestSetConfigIsLocalTrueBoundVetOnly(t *testing.T) {
	t.Run("bound name and value", func(t *testing.T) {
		stmt := parseOne(t, "SELECT set_config($1, $2, true)")
		result, err := analyzeFunctionCalls(stmt, true, false)
		require.NoError(t, err)
		require.Len(t, result.SetConfigs, 1)
		sc := result.SetConfigs[0]
		assert.True(t, sc.IsLocalLiteralTrue)
		require.NotNil(t, sc.NameBind)
		assert.Equal(t, 1, sc.NameBind.Number)
		require.NotNil(t, sc.ValueBind)
		assert.Equal(t, 2, sc.ValueBind.Number)
		assert.Nil(t, sc.IsLocalBind)
	})

	t.Run("literal search_path name with bound value", func(t *testing.T) {
		stmt := parseOne(t, "SELECT set_config('search_path', $1, true)")
		result, err := analyzeFunctionCalls(stmt, true, false)
		require.NoError(t, err)
		require.Len(t, result.SetConfigs, 1)
		sc := result.SetConfigs[0]
		assert.True(t, sc.IsLocalLiteralTrue)
		assert.Equal(t, "search_path", sc.Name)
		require.NotNil(t, sc.ValueBind)
	})

	t.Run("literal non-search_path name with bound value stays untracked", func(t *testing.T) {
		stmt := parseOne(t, "SELECT set_config('request.jwt.claims', $1, true)")
		result, err := analyzeFunctionCalls(stmt, true, false)
		require.NoError(t, err)
		assert.Empty(t, result.SetConfigs)
	})
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
			// An expression-shaped name with literal is_local=true cannot be
			// resolved at execute time (its value comes from rows), so it
			// cannot be vetted for search_path — the shape fails closed. A
			// bound ($N) name is fine: it produces a vet-only entry (see
			// TestSetConfigIsLocalTrueBoundVetOnly).
			name:    "dynamic name with is_local=true",
			sql:     "SELECT set_config(name, 'v', true) FROM gucs",
			wantMsg: "set_config name argument must be a literal constant or a bound parameter",
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
			result, err := analyzeFunctionCalls(stmt, true, false)
			require.Error(t, err)
			assert.Nil(t, result)
			var diag *mterrors.PgDiagnostic
			require.True(t, errors.As(err, &diag))
			assert.Equal(t, mterrors.PgSSFeatureNotSupported, diag.Code)
			assert.Contains(t, diag.Message, tt.wantMsg)
		})
	}
}

// TestInspectExpressionFuncCalls_TransactionLocalSetConfigPassThrough pins the
// carve-out that unblocks PostgREST's mutation row-count trick: a
// transaction-scoped set_config(name, value, true) on an ordinary GUC is
// allowed outside a top-level SELECT target (WHERE, subquery, CTE-INSERT). It
// reverts at transaction end, so PostgreSQL runs it verbatim and the gateway
// tracks nothing — result.SetConfigs stays empty.
func TestInspectExpressionFuncCalls_TransactionLocalSetConfigPassThrough(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "is_local=true in WHERE",
			sql:  "SELECT 1 WHERE set_config('pgrst.inserted', '1', true) <> '0'",
		},
		{
			name: "is_local=true in INSERT ... WHERE inside a CTE (PostgREST shape)",
			sql: "WITH pgrst_source AS (" +
				"INSERT INTO t (x) SELECT 1 WHERE set_config('pgrst.inserted', '1', true) <> '0' RETURNING x" +
				") SELECT * FROM pgrst_source",
		},
		{
			// A benign search_path value stays allowed — only pg_temp is barred.
			name: "non-pg_temp search_path value passes through",
			sql:  "SELECT 1 WHERE set_config('search_path', 'public', true) <> '0'",
		},
		{
			name: "is_local=true in subquery",
			sql:  "SELECT * FROM (SELECT 1 AS v WHERE set_config('pgrst.inserted', '1', true) <> '0') s",
		},
		{
			name: "ordinary GUC with is_local=true in WHERE",
			sql:  "SELECT 1 WHERE set_config('work_mem', '256MB', true) IS NOT NULL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := parseOne(t, tt.sql)
			result, err := analyzeFunctionCalls(stmt, true, false)
			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Empty(t, result.SetConfigs, "a transaction-local pass-through must not be tracked")
		})
	}
}

// TestInspectExpressionFuncCalls_NonTopLevelSetConfigRejected covers the shapes
// the transaction-local carve-out must still reject: only a literal
// is_local=true on a non-restricted, non-gateway-managed GUC passes through.
func TestInspectExpressionFuncCalls_NonTopLevelSetConfigRejected(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantMsg string
	}{
		{
			name:    "is_local=false in WHERE leaks untracked backend state",
			sql:     "SELECT 1 WHERE set_config('pgrst.inserted', '1', false) <> '0'",
			wantMsg: "set_config is only supported as a top-level SELECT target list entry",
		},
		{
			name:    "bound is_local can't be resolved at plan time",
			sql:     "SELECT 1 WHERE set_config('pgrst.inserted', '1', $1) <> '0'",
			wantMsg: "set_config is only supported as a top-level SELECT target list entry",
		},
		{
			name:    "non-literal name can't be checked for restricted/gateway-managed",
			sql:     "SELECT 1 FROM pg_settings WHERE set_config(name, '1', true) <> '0'",
			wantMsg: "set_config is only supported as a top-level SELECT target list entry",
		},
		{
			name:    "restricted GUC is rejected even transaction-scoped",
			sql:     "SELECT 1 WHERE set_config('synchronous_commit', 'off', true) <> '0'",
			wantMsg: "setting synchronous_commit is not supported",
		},
		{
			name:    "gateway-managed variable must never reach the backend",
			sql:     "SELECT 1 WHERE set_config('statement_timeout', '5s', true) <> '0'",
			wantMsg: "set_config is only supported as a top-level SELECT target list entry",
		},
		// search_path is value-restricted here even though the call is
		// transaction-scoped: is_local=true bounds the GUC, not the objects
		// created while it is in effect. With pg_temp as the creation target an
		// unqualified CREATE in the same transaction lands in the pooled
		// backend's temp namespace and SURVIVES the COMMIT, carrying no TEMP
		// keyword and no pg_temp qualification — so planTempTableCreation and
		// checkTempSchemaQualifiedCreate both miss it and the backend goes back
		// to the pool holding a temp object (verified on PostgreSQL 17).
		{
			name:    "pg_temp search_path in WHERE",
			sql:     "SELECT 1 WHERE set_config('search_path', 'pg_temp', true) <> '0'",
			wantMsg: "pg_temp in search_path is not supported",
		},
		{
			name:    "pg_temp search_path in a VALUES list",
			sql:     "INSERT INTO t(x) VALUES (set_config('search_path', 'pg_temp', true))",
			wantMsg: "pg_temp in search_path is not supported",
		},
		{
			name:    "pg_temp search_path in a subquery",
			sql:     "SELECT * FROM (SELECT set_config('search_path', 'pg_temp', true) AS v) s",
			wantMsg: "pg_temp in search_path is not supported",
		},
		{
			name:    "pg_temp search_path in a CTE",
			sql:     "WITH c AS (SELECT set_config('search_path', 'pg_temp', true) AS v) SELECT * FROM c",
			wantMsg: "pg_temp in search_path is not supported",
		},
		{
			name:    "pg_temp search_path in an UPDATE target",
			sql:     "UPDATE t SET x = set_config('search_path', 'pg_temp', true)",
			wantMsg: "pg_temp in search_path is not supported",
		},
		{
			// The guard is position-insensitive: the creation target is the
			// first EXISTING schema, so a nonexistent prefix does not help.
			name:    "nonexistent-prefix bypass attempt",
			sql:     "SELECT 1 WHERE set_config('search_path', 'nosuch, pg_temp', true) <> '0'",
			wantMsg: "pg_temp in search_path is not supported",
		},
		{
			// This path emits no primitive, so unlike every other set_config
			// surface nothing re-checks the value at execute time — a value
			// that cannot be read at plan time must fail closed.
			name:    "bound search_path value has no execute-time re-check",
			sql:     "SELECT 1 WHERE set_config('search_path', $1, true) <> '0'",
			wantMsg: "set_config is only supported as a top-level SELECT target list entry",
		},
		{
			name:    "dynamic search_path value has no execute-time re-check",
			sql:     "SELECT 1 FROM t WHERE set_config('search_path', col, true) <> '0'",
			wantMsg: "set_config is only supported as a top-level SELECT target list entry",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := parseOne(t, tt.sql)
			result, err := analyzeFunctionCalls(stmt, true, false)
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
			result, err := analyzeFunctionCalls(stmt, true, false)
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
			_, err := analyzeFunctionCalls(stmt, true, false)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantMsg)
		})
	}
}

// TestInspectExpressionFuncCalls_SetConfigUnsafeConnection confirms the operator
// opt-out actually lets every untrackable set_config shape through: the same
// statements the enforced path rejects (dynamic-shape and per-call shape errors)
// must analyze without error and produce no tracking (empty SetConfigs, no
// DynamicSetConfig), so they route to PostgreSQL as written and untracked. This
// is the contract in docs/query_serving/unsafe_statement_rejection.md.
func TestInspectExpressionFuncCalls_SetConfigUnsafeConnection(t *testing.T) {
	sqls := []string{
		// Dynamic-shape rejections (reach validateDynamicSetConfigShape).
		"SELECT set_config(name, '256MB', false) FROM gucs",
		"SELECT set_config('work_mem', v, false) FROM pg_settings",
		"SELECT set_config('work_mem', '256MB', islocal) FROM pg_settings",
		"SELECT set_config(name, '256MB', $1) FROM pg_settings",
		"SELECT set_config(name, 100, false) FROM pg_settings",
		"SELECT set_config('work_mem', now(), false)",
		"SELECT set_config(name, '256MB', false) FROM pg_settings WHERE nextval('s') > 0",
		"SELECT set_config(lower(name), '256MB', false) FROM pg_settings",
		// Per-call shape rejection that skips the dynamic path (all args static):
		// a bound is_local on an ordinary variable.
		"SELECT set_config('work_mem', '1GB', $1)",
	}
	for _, sql := range sqls {
		t.Run(sql, func(t *testing.T) {
			stmt := parseOne(t, sql)
			// Enforced: rejected.
			_, err := analyzeFunctionCalls(stmt, true, false)
			require.Error(t, err, "expected rejection when enforced")
			// unsafe-connection: accepted and untracked.
			result, err := analyzeFunctionCalls(parseOne(t, sql), false, false)
			require.NoError(t, err, "unsafe-connection must not reject")
			require.NotNil(t, result)
			assert.False(t, result.DynamicSetConfig, "must not synthesize a dynamic set_config plan")
			assert.Empty(t, result.SetConfigs, "untrackable set_config must not be tracked")
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := parseOne(t, tt.sql)
			result, err := analyzeFunctionCalls(stmt, true, false)
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := parseOne(t, tt.sql)
			result, err := analyzeFunctionCalls(stmt, true, false)
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

// TestInspectExpressionFuncCalls_BoundIsLocalRejected pins that a bound
// is_local on a non-gateway-managed set_config is rejected fail-closed: it can
// resolve to false at execute time, which would persist real session state on
// a pooled backend outside the gateway's authoritative map.
func TestInspectExpressionFuncCalls_BoundIsLocalRejected(t *testing.T) {
	for _, sql := range []string{
		"SELECT set_config('search_path', 'public', $1)",
		"SELECT set_config($1, $2, $3)",
	} {
		t.Run(sql, func(t *testing.T) {
			stmt := parseOne(t, sql)
			_, err := analyzeFunctionCalls(stmt, true, false)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "is_local argument must be a boolean literal")
		})
	}
}

// TestInspectExpressionFuncCalls_LiteralIsLocalTrueShortCircuits pins that
// a fully-vetted literal is_local=true call returns no setConfigCall — the
// transaction-scoped semantics are PG's job, gateway must not track. Calls
// with slots still needing vetting (bound name, or bound value on
// search_path) instead produce a vet-only entry (see
// TestSetConfigIsLocalTrueBoundVetOnly).
func TestInspectExpressionFuncCalls_LiteralIsLocalTrueShortCircuits(t *testing.T) {
	for _, sql := range []string{
		"SELECT set_config('request.jwt.claims', '{...}', true)",
		"SELECT set_config('request.jwt.claims', $1, true)",
	} {
		t.Run(sql, func(t *testing.T) {
			stmt := parseOne(t, sql)
			result, err := analyzeFunctionCalls(stmt, true, false)
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
			result, err := analyzeFunctionCalls(stmt, true, false)
			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Empty(t, result.SetConfigs)
		})
	}
}

// TestInspectExpressionFuncCalls_LogicalReplicationSlotCreation covers
// detection of a TEMPORARY pg_create_logical_replication_slot(...), which
// must fire regardless of how deeply the call is nested — Supabase Realtime's
// actual call site (Extensions.PostgresCdcRls.Replications.prepare_replication/2)
// buries it inside a CASE inside a scalar subquery, not a bare top-level
// SELECT. Every non-temporary shape here is now also rejected outright by
// the same replicationSlotFuncs check TestInspectExpressionFuncCalls_ReplicationSlots
// covers (Multigres cannot yet migrate a slot's position across a primary
// failover) — those cases exist here to confirm rejection composes correctly
// with pinning detection, i.e. that the reject check running first doesn't
// leave CreatesLogicalReplicationSlot's detection unreachable for the
// temporary calls that follow it.
func TestInspectExpressionFuncCalls_LogicalReplicationSlotCreation(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
		want    bool
	}{
		{
			name:    "bare two-argument call: temporary omitted, defaults to false, rejected",
			sql:     "SELECT pg_create_logical_replication_slot('s1', 'pgoutput')",
			wantErr: true,
		},
		{
			name:    "explicit temporary=false rejected",
			sql:     "SELECT pg_create_logical_replication_slot('s1', 'pgoutput', false)",
			wantErr: true,
		},
		{
			name: "schema-qualified, explicit temporary=true",
			sql:  "SELECT pg_catalog.pg_create_logical_replication_slot('s1', 'pgoutput', true)",
			want: true,
		},
		{
			name:    "bound temporary argument can't be resolved at plan time, rejected",
			sql:     "SELECT pg_create_logical_replication_slot('s1', 'pgoutput', $1)",
			wantErr: true,
		},
		{
			name: "Realtime's actual nested shape: CASE + scalar subquery, temporary passed as literal string 'true'",
			sql: `select
			   case when not exists (
			     select 1 from pg_replication_slots where slot_name = 's1'
			   )
			   then (
			     select 1 from pg_create_logical_replication_slot('s1', 'wal2json', 'true')
			   )
			   else 1
			   end`,
			want: true,
		},
		{
			name: "unrelated function call does not set the flag",
			sql:  "SELECT pg_advisory_lock(1)",
		},
		{
			name: "no function call at all",
			sql:  "SELECT 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := parseOne(t, tt.sql)
			result, err := analyzeFunctionCalls(stmt, true, false)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "requires temporary=true")
				return
			}
			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Equal(t, tt.want, result.CreatesLogicalReplicationSlot)
		})
	}
}

// TestInspectExpressionFuncCalls_FailoverSlotCreation_NoPinning confirms a
// non-temporary logical slot admitted via admitFailoverSlots does not set
// CreatesLogicalReplicationSlot: it's a persistent slot visible from any
// backend, so — unlike a temporary slot — the session must not be pinned to
// the backend that created it. Covers both the positional and named-argument
// failover=true forms.
func TestInspectExpressionFuncCalls_FailoverSlotCreation_NoPinning(t *testing.T) {
	for _, sql := range []string{
		"SELECT pg_create_logical_replication_slot('s1', 'pgoutput', false, false, true)",
		"SELECT pg_create_logical_replication_slot('s1', 'pgoutput', failover => true)",
		"SELECT pg_create_logical_replication_slot('s1', 'pgoutput', temporary => false, failover => true)",
	} {
		t.Run(sql, func(t *testing.T) {
			stmt := parseOne(t, sql)
			result, err := analyzeFunctionCalls(stmt, true, true)
			require.NoError(t, err)
			require.NotNil(t, result)
			assert.False(t, result.CreatesLogicalReplicationSlot)
		})
	}
}

// TestInspectExpressionFuncCalls_SetSeed covers detection of setseed(...),
// which must fire regardless of how deeply the call is nested, mirroring
// TestInspectExpressionFuncCalls_LogicalReplicationSlotCreation's nested-call
// coverage.
func TestInspectExpressionFuncCalls_SetSeed(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want bool
	}{
		{
			name: "bare call",
			sql:  "SELECT setseed(0.5)",
			want: true,
		},
		{
			name: "schema-qualified call",
			sql:  "SELECT pg_catalog.setseed(0.5)",
			want: true,
		},
		{
			name: "nested inside a CASE",
			sql: `select case when true
			   then (select 1 from (select setseed(0.5)) s)
			   else 1
			   end`,
			want: true,
		},
		{
			name: "unrelated function call does not set the flag",
			sql:  "SELECT pg_advisory_lock(1)",
		},
		{
			name: "no function call at all",
			sql:  "SELECT 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := parseOne(t, tt.sql)
			result, err := analyzeFunctionCalls(stmt, true, false)
			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Equal(t, tt.want, result.CallsSetSeed)
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
// `SELECT set_config(...)` shape — bare or mixed with a FROM/targets — plans as
// Sequence[SessionStateBranch, silent ApplySessionState...]. The branch carries
// both routes: the pinned one routes the original (is_local false, persists on a
// reserved backend) and the unpinned one reverts (is_local true) so a pooled
// backend is left clean. No fast-path for the bare case: uniform construction is
// worth the extra round-trip.
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

			// The leading primitive is a SessionStateBranch: the pinned branch
			// routes the original (is_local false, persists on a reserved backend)
			// while the unpinned branch reverts (is_local true) so a pooled backend
			// keeps nothing. No capture reservation is involved.
			branch, ok := seq.Primitives[0].(*engine.SessionStateBranch)
			require.True(t, ok, "first primitive should be a SessionStateBranch, got %T", seq.Primitives[0])
			pinnedRoute, ok := branch.Pinned.(*engine.Route)
			require.True(t, ok, "pinned branch should be a plain Route, got %T", branch.Pinned)
			assert.Equal(t, stmt.SqlString(), pinnedRoute.Query, "pinned branch routes the base AST verbatim (is_local=false)")
			unpinnedRoute, ok := branch.Unpinned.(*engine.Route)
			require.True(t, ok, "unpinned branch should be a plain Route, got %T", branch.Unpinned)
			assert.NotEqual(t, pinnedRoute.Query, unpinnedRoute.Query, "unpinned branch must rewrite is_local to revert")

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

// TestPlan_SetConfig_PinnedRoutesOriginal pins the pinned-branch shape: the
// plan is always a SessionStateBranch (the plan is cacheable, so the
// pinned/unpinned choice is deferred to execute time), and its pinned branch
// routes the ORIGINAL query (is_local false intact, plain Route — no value-route
// wrapper). At execute time a pinned session selects that branch so its backend
// genuinely carries the value in lockstep with the gateway map, and no SELECT is
// injected later to re-propagate it (which would latch a
// REPEATABLE READ/SERIALIZABLE snapshot early).
func TestPlan_SetConfig_PinnedRoutesOriginal(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})
	testConn.Conn.SetTxnStatus(protocol.TxnStatusInBlock)

	sql := "SELECT set_config('work_mem', '256MB', false)"
	stmt := parseOne(t, sql)
	plan, err := p.Plan(sql, stmt, testConn.Conn, PlanOptions{})
	require.NoError(t, err)
	require.NotNil(t, plan)

	seq, ok := plan.Primitive.(*engine.Sequence)
	require.True(t, ok, "expected Sequence, got %T", plan.Primitive)
	branch, ok := seq.Primitives[0].(*engine.SessionStateBranch)
	require.True(t, ok, "expected SessionStateBranch, got %T", seq.Primitives[0])
	route, ok := branch.Pinned.(*engine.Route)
	require.True(t, ok, "the pinned branch must route as a plain Route, got %T", branch.Pinned)
	assert.Equal(t, stmt.SqlString(), route.Query, "the is_local=false call must reach the pinned backend unmodified")
}

// TestRewriteSetConfigToRevert pins the revert rewrite that replaces the old
// capture reservation: exactly the tracked calls that would leave real session
// state on the backend get their is_local flipped false→true (so the rewrite
// returns a non-nil clone), while shapes that persist nothing — the hot
// PostgREST is_local-literal-true form, and gateway-managed calls (rewritten out
// of the routed query entirely) — are left unchanged.
func TestRewriteSetConfigToRevert(t *testing.T) {
	tests := []struct {
		sql      string
		wantFlip bool
	}{
		{"SELECT set_config('work_mem', '256MB', false)", true},
		{"SELECT set_config('work_mem', $1, false)", true},
		{"SELECT set_config('work_mem', '256MB', false), 1 AS x", true},
		{"SELECT set_config('request.jwt.claims', '{}', true)", false},
		// Gateway-managed with literal false: not flipped here — it is removed
		// from the routed query entirely by rewriteGatewayManagedSetConfig.
		{"SELECT set_config('statement_timeout', '5s', false)", false},
		{"SELECT 1", false},
	}
	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			stmt := parseOne(t, tt.sql)
			reverted := rewriteSetConfigToRevert(stmt)
			if tt.wantFlip {
				require.NotNil(t, reverted, "expected a reverting rewrite")
				assert.NotEqual(t, stmt.SqlString(), reverted.SqlString(),
					"the rewrite must flip is_local so the routed query differs")
			} else {
				assert.Nil(t, reverted, "expected no rewrite")
			}
		})
	}
}

func TestPlan_LogicalReplicationSlotCreation_SetsExecInfo(t *testing.T) {
	sql := `select
	  case when not exists (
	    select 1 from pg_replication_slots where slot_name = 's1'
	  )
	  then (
	    select 1 from pg_create_logical_replication_slot('s1', 'wal2json', 'true')
	  )
	  else 1
	  end`
	stmt := parseOne(t, sql)

	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	plan, err := p.Plan(sql, stmt, testConn.Conn, PlanOptions{})
	require.NoError(t, err)
	assert.True(t, plan.ExecInfo.LogicalReplicationSlot)
	assert.Equal(t, engine.PlanTypeLogicalReplicationSlotRoute, plan.Type)
}

// TestPlan_SetSeed_SetsExecInfo verifies that a statement calling setseed(...)
// produces a plan whose ExecInfo.SetSeed is true, so the reservation
// machinery in scatterconn picks it up.
func TestPlan_SetSeed_SetsExecInfo(t *testing.T) {
	sql := "SELECT setseed(0.5)"
	stmt := parseOne(t, sql)

	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	plan, err := p.Plan(sql, stmt, testConn.Conn, PlanOptions{})
	require.NoError(t, err)
	assert.True(t, plan.ExecInfo.SetSeed)
	assert.Equal(t, engine.PlanTypeSetSeedRoute, plan.Type)
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
