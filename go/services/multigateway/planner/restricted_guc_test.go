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

		// -- Allowed: reverting to the managed value --
		{"RESET", "RESET synchronous_commit", false},
		{"SET TO DEFAULT", "SET synchronous_commit TO DEFAULT", false},
		{"RESET ALL", "RESET ALL", false},
		{"ALTER DATABASE RESET", "ALTER DATABASE mydb RESET synchronous_commit", false},
		{"ALTER ROLE RESET", "ALTER ROLE myrole RESET synchronous_commit", false},

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
