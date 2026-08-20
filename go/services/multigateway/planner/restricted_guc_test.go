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
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
)

// TestSearchPathPgTempRejected verifies the value-level search_path guard: any
// pg_temp mention is rejected on every gateway-reachable assignment path (SET,
// SET LOCAL, ALTER ROLE/DATABASE ... SET, set_config in its literal and bound
// shapes), while ordinary search_path values and reverts pass. All cases go
// through analyzeStatement, the pre-dispatch pass both protocols share.
func TestSearchPathPgTempRejected(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		// -- Blocked: pg_temp (or a concrete pg_temp_N) in the value --
		{"SET bare", "SET search_path = pg_temp", true},
		{"SET first of list", "SET search_path TO pg_temp, public", true},
		{"SET inside single string", "SET search_path = 'pg_temp, public'", true},
		{"SET not first", "SET search_path = public, pg_temp", true},
		{"SET quoted", `SET search_path = "pg_temp"`, true},
		{"SET concrete backend namespace", "SET search_path = pg_temp_3", true},
		{"SET LOCAL", "SET LOCAL search_path = pg_temp", true},
		{"ALTER ROLE SET", "ALTER ROLE myrole SET search_path = pg_temp", true},
		// SET ... FROM CURRENT carries no value in the statement, so the guard
		// would pass vacuously on an empty arg list while PostgreSQL persists
		// whatever the session holds — which may have been inherited from a
		// more lenient surface or applied natively from a role/database
		// default. Refused on every surface (see TestSetFromCurrentRejected).
		{"SET FROM CURRENT", "SET search_path FROM CURRENT", true},
		{"ALTER ROLE FROM CURRENT", "ALTER ROLE current_user SET search_path FROM CURRENT", true},
		{"ALTER DATABASE FROM CURRENT", "ALTER DATABASE mydb SET search_path FROM CURRENT", true},
		{"CREATE FUNCTION FROM CURRENT", "CREATE FUNCTION f() RETURNS void LANGUAGE sql SET search_path FROM CURRENT AS 'SELECT 1'", true},
		{"ALTER FUNCTION FROM CURRENT", "ALTER FUNCTION f() SET search_path FROM CURRENT", true},
		{"ALTER DATABASE SET", "ALTER DATABASE mydb SET search_path = pg_temp, public", true},
		// Every surface is strict and position-insensitive: a trailing pg_temp
		// is only conditionally safe (the creation target is the first EXISTING
		// schema, so "nosuch, pg_temp" resolves to the temp namespace), and the
		// gateway cannot determine schema existence.
		{"ALTER ROLE trailing pg_temp", "ALTER ROLE app SET search_path = app, pg_temp", true},
		{"ALTER ROLE self nonexistent-prefix bypass", "ALTER ROLE current_user SET search_path = nosuch, pg_temp", true},
		{"ALTER ROLE ALL IN DATABASE", "ALTER ROLE ALL IN DATABASE mydb SET search_path = nosuch, pg_temp", true},
		{"ALTER DATABASE trailing pg_temp", `ALTER DATABASE mydb SET search_path = "$user", public, pg_temp`, true},
		{"ALTER DATABASE nonexistent-prefix", "ALTER DATABASE mydb SET search_path = nosuch, pg_temp", true},
		{"CREATE FUNCTION trailing pg_temp", "CREATE FUNCTION f() RETURNS void LANGUAGE sql SET search_path = admin, pg_temp AS 'SELECT 1'", true},
		{"ALTER FUNCTION trailing pg_temp", "ALTER FUNCTION f() SET search_path = admin, pg_temp", true},
		// Function proconfig: the stored SET applies on every later call of
		// the function, on whatever pooled backend serves it.
		{"CREATE FUNCTION SET proconfig", "CREATE FUNCTION f() RETURNS void LANGUAGE sql SET search_path = pg_temp AS 'SELECT 1'", true},
		{"CREATE PROCEDURE SET proconfig", "CREATE PROCEDURE p() LANGUAGE sql SET search_path = pg_temp, public AS 'SELECT 1'", true},
		{"ALTER FUNCTION SET proconfig", "ALTER FUNCTION f() SET search_path = pg_temp", true},
		{"ALTER PROCEDURE SET proconfig", "ALTER PROCEDURE p() SET search_path = 'pg_temp, public'", true},
		{"set_config literal", "SELECT set_config('search_path', 'pg_temp, public', false)", true},
		{"set_config literal is_local", "SELECT set_config('search_path', 'pg_temp', true)", true},
		// A bound value or name with is_local=true is accepted at plan time:
		// it produces a vet-only ApplySessionStateFromBind whose
		// resolveSetConfig checks the resolved slots at execute time, before
		// the Route reaches the backend (see the deferred cases below).

		// -- Allowed: ordinary values, reverts, deferred-check shapes --
		{"SET public", "SET search_path = public", false},
		{"SET user default", `SET search_path = "$user", public`, false},
		{"SET prefix-similar schema", "SET search_path = mypg_temp", false},
		{"RESET", "RESET search_path", false},
		{"SET TO DEFAULT", "SET search_path TO DEFAULT", false},
		{"set_config benign literal", "SELECT set_config('search_path', 'public', false)", false},
		// Bound value with is_local=false is checked at execute time by
		// resolveSetConfig instead (see apply_session_state.go).
		{"set_config bound value deferred", "SELECT set_config('search_path', $1, false)", false},
		{"set_config bound value is_local deferred", "SELECT set_config('search_path', $1, true)", false},
		{"set_config bound name is_local deferred", "SELECT set_config($1, 'pg_temp', true)", false},
		{"current_schema read", "SELECT current_schema()", false},
		{"CREATE FUNCTION benign proconfig", "CREATE FUNCTION f() RETURNS void LANGUAGE sql SET search_path = public AS 'SELECT 1'", false},
		{"ALTER FUNCTION RESET proconfig", "ALTER FUNCTION f() RESET search_path", false},
		// Benign persisted values are unaffected — only pg_temp is barred.
		{"ALTER DATABASE benign", "ALTER DATABASE mydb SET search_path = public, extensions", false},
		{"ALTER ROLE benign", "ALTER ROLE app SET search_path = app", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := analyzeStatement(parseOne(t, tt.sql))
			if !tt.wantErr {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			var diag *mterrors.PgDiagnostic
			require.True(t, errors.As(err, &diag), "error should be a PgDiagnostic")
			assert.Equal(t, mterrors.PgSSFeatureNotSupported, diag.Code)
			assert.Contains(t, diag.Message, "search_path")
		})
	}
}

// TestSetFromCurrentRejected pins the blanket refusal of SET ... FROM CURRENT.
//
// The value is resolved on the backend and never appears in the statement, so
// a value-restricted GUC cannot be vetted: extractVariableValue returns "" for
// the empty arg list and both pgsettings guards pass unconditionally. It is
// not sufficient that the session's current value once passed a guard — it may
// have been inherited from a more lenient surface (ALTER DATABASE, where a
// trailing pg_temp is allowed) or applied natively by PostgreSQL from a
// role/database default at pooled-backend startup, outside gateway tracking.
// PostgreSQL resolves FROM CURRENT into a concrete stored value, so accepting
// it would pin an unvetted search_path into pg_db_role_setting or proconfig
// that no later guard can see or undo.
//
// Refused for every GUC on every surface, matching planVariableSetStmt which
// already rejects the session-level form. pg_dump/pg_dumpall never emit FROM
// CURRENT (they emit the resolved literal), so dump/restore is unaffected.
func TestSetFromCurrentRejected(t *testing.T) {
	for _, sql := range []string{
		"SET search_path FROM CURRENT",
		"SET LOCAL search_path FROM CURRENT",
		"SET work_mem FROM CURRENT",
		"ALTER ROLE current_user SET search_path FROM CURRENT",
		"ALTER USER app SET search_path FROM CURRENT",
		"ALTER ROLE app IN DATABASE mydb SET search_path FROM CURRENT",
		"ALTER DATABASE mydb SET search_path FROM CURRENT",
		"CREATE FUNCTION f() RETURNS void LANGUAGE sql SET search_path FROM CURRENT AS 'SELECT 1'",
		"ALTER FUNCTION f() SET search_path FROM CURRENT",
		"ALTER ROLE app SET work_mem FROM CURRENT",
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := analyzeStatement(parseOne(t, sql))
			// The session-level form is rejected later, by planVariableSetStmt;
			// every persisted form is rejected here in the shared guard.
			if _, isSessionSet := parseOne(t, sql).(*ast.VariableSetStmt); isSessionSet {
				return
			}
			require.Error(t, err)
			var diag *mterrors.PgDiagnostic
			require.True(t, errors.As(err, &diag))
			assert.Equal(t, mterrors.PgSSFeatureNotSupported, diag.Code)
			assert.Contains(t, diag.Message, "FROM CURRENT is not supported")
		})
	}
}

// TestCheckRestrictedGUCChange verifies the value-level guard that blocks users
// from overriding a cluster-managed GUC (synchronous_commit, the sole current
// entry in restrictedGUCs) across every gateway-reachable statement path, while
// still allowing reverts.
func TestCheckRestrictedGUCChange(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		// -- Blocked: assigning an explicit value --
		{"SET off", "SET synchronous_commit = 'off'", true},
		{"SET on", "SET synchronous_commit = 'on'", true},
		{"SET local", "SET synchronous_commit = 'local'", true},
		{"SET remote_write", "SET synchronous_commit = 'remote_write'", true},
		{"SET remote_apply", "SET synchronous_commit = 'remote_apply'", true},
		{"SET unquoted", "SET synchronous_commit = off", true},
		{"SET case-insensitive name", "SET SYNCHRONOUS_COMMIT = 'off'", true},
		{"SET LOCAL", "SET LOCAL synchronous_commit = 'off'", true},
		{"SET FROM CURRENT", "SET synchronous_commit FROM CURRENT", true},
		{"ALTER DATABASE SET", "ALTER DATABASE mydb SET synchronous_commit = 'off'", true},
		{"ALTER ROLE SET", "ALTER ROLE myrole SET synchronous_commit = 'off'", true},
		{"ALTER ROLE ALL IN DATABASE SET", "ALTER ROLE ALL IN DATABASE mydb SET synchronous_commit = 'local'", true},
		{"CREATE FUNCTION SET proconfig", "CREATE FUNCTION f() RETURNS void LANGUAGE sql SET synchronous_commit = 'off' AS 'SELECT 1'", true},
		{"ALTER FUNCTION SET proconfig", "ALTER FUNCTION f() SET synchronous_commit = 'off'", true},

		// -- Allowed: reverting to the managed value --
		{"RESET", "RESET synchronous_commit", false},
		{"SET TO DEFAULT", "SET synchronous_commit TO DEFAULT", false},
		{"RESET ALL", "RESET ALL", false},
		{"ALTER DATABASE RESET", "ALTER DATABASE mydb RESET synchronous_commit", false},
		{"ALTER ROLE RESET", "ALTER ROLE myrole RESET synchronous_commit", false},
		{"ALTER FUNCTION RESET proconfig", "ALTER FUNCTION f() RESET synchronous_commit", false},

		// -- Allowed: unrelated GUCs are untouched --
		{"SET other GUC", "SET work_mem = '256MB'", false},
		{"ALTER DATABASE SET other GUC", "ALTER DATABASE mydb SET work_mem = '256MB'", false},
		{"SELECT", "SELECT 1", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkRestrictedGUCChange(parseOne(t, tt.sql))
			if !tt.wantErr {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			var diag *mterrors.PgDiagnostic
			require.True(t, errors.As(err, &diag), "error should be a PgDiagnostic")
			assert.Equal(t, mterrors.PgSSFeatureNotSupported, diag.Code)
			assert.Contains(t, diag.Message, "synchronous_commit")
		})
	}
}

// TestSetConfigSynchronousCommit verifies the synchronous_commit guard on the
// set_config() expression path, for both is_local variants. set_config is an
// alternate route to the same session-state override SET takes, so it must be
// blocked the same way.
func TestSetConfigSynchronousCommit(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"set_config is_local=false", "SELECT set_config('synchronous_commit', 'off', false)", true},
		{"set_config is_local=true", "SELECT set_config('synchronous_commit', 'off', true)", true},
		{"set_config case-insensitive", "SELECT set_config('SYNCHRONOUS_COMMIT', 'off', false)", true},
		{"set_config pg_catalog-qualified", "SELECT pg_catalog.set_config('synchronous_commit', 'off', false)", true},
		// Unrelated GUC via set_config is still accepted/tracked.
		{"set_config other GUC", "SELECT set_config('work_mem', '64MB', false)", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := analyzeFunctionCalls(parseOne(t, tt.sql))
			if !tt.wantErr {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			var diag *mterrors.PgDiagnostic
			require.True(t, errors.As(err, &diag))
			assert.Equal(t, mterrors.PgSSFeatureNotSupported, diag.Code)
			assert.Contains(t, diag.Message, "synchronous_commit")
		})
	}
}

// TestSetConfigSynchronousCommitAfterNormalization confirms the guard survives
// literal normalization: the planner runs against the normalized AST under the
// plan cache, and the normalizer keeps the set_config name literal precisely so
// the is_local=true path can still be inspected (see normalizer.go).
func TestSetConfigSynchronousCommitAfterNormalization(t *testing.T) {
	norm := ast.Normalize(parseOne(t, "SELECT set_config('synchronous_commit', 'off', true)"))
	_, err := analyzeStatement(norm.NormalizedAST)
	require.Error(t, err)
	var diag *mterrors.PgDiagnostic
	require.True(t, errors.As(err, &diag))
	assert.Contains(t, diag.Message, "synchronous_commit")

	// A normalized is_local=true call for an unrelated GUC must still pass.
	normOK := ast.Normalize(parseOne(t, "SELECT set_config('work_mem', '64MB', true)"))
	_, err = analyzeStatement(normOK.NormalizedAST)
	assert.NoError(t, err)
}

// TestPlanRejectsSynchronousCommitChange verifies that Plan() itself rejects
// the override before it reaches the SET/default routing path.
func TestPlanRejectsSynchronousCommitChange(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	for _, sql := range []string{
		"SET synchronous_commit = 'off'",
		"SET LOCAL synchronous_commit = 'off'",
		"ALTER ROLE myrole SET synchronous_commit = 'off'",
	} {
		t.Run(sql, func(t *testing.T) {
			plan, err := p.Plan(sql, parseOne(t, sql), testConn.Conn, PlanOptions{})
			require.Error(t, err)
			assert.Nil(t, plan)
			var diag *mterrors.PgDiagnostic
			require.True(t, errors.As(err, &diag))
			assert.Equal(t, mterrors.PgSSFeatureNotSupported, diag.Code)
		})
	}

	// RESET still plans successfully.
	t.Run("RESET allowed", func(t *testing.T) {
		sql := "RESET synchronous_commit"
		plan, err := p.Plan(sql, parseOne(t, sql), testConn.Conn, PlanOptions{})
		require.NoError(t, err)
		assert.NotNil(t, plan)
	})
}
