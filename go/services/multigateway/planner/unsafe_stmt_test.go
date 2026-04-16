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

func TestPlanUnsupportedStmt(t *testing.T) {
	tests := []struct {
		name        string
		stmt        ast.Stmt
		wantErr     bool
		wantMessage string
	}{
		// -- Tier 1: statements containing unparsed code --
		{
			name:        "DO block",
			stmt:        &ast.DoStmt{BaseNode: ast.BaseNode{Tag: ast.T_DoStmt}},
			wantErr:     true,
			wantMessage: "DO blocks are not supported",
		},
		{
			name:        "CREATE FUNCTION",
			stmt:        &ast.CreateFunctionStmt{IsProcedure: false},
			wantErr:     true,
			wantMessage: "CREATE FUNCTION is not supported",
		},
		{
			name:        "CREATE PROCEDURE",
			stmt:        &ast.CreateFunctionStmt{IsProcedure: true},
			wantErr:     true,
			wantMessage: "CREATE PROCEDURE is not supported",
		},
		{
			name:        "CREATE TRIGGER",
			stmt:        &ast.CreateTriggerStmt{},
			wantErr:     true,
			wantMessage: "CREATE TRIGGER is not supported",
		},
		{
			name:        "CREATE RULE",
			stmt:        &ast.RuleStmt{},
			wantErr:     true,
			wantMessage: "CREATE RULE is not supported",
		},
		{
			name:        "CREATE EVENT TRIGGER",
			stmt:        &ast.CreateEventTrigStmt{BaseNode: ast.BaseNode{Tag: ast.T_CreateEventTrigStmt}},
			wantErr:     true,
			wantMessage: "CREATE EVENT TRIGGER is not supported",
		},

		// -- Tier 2: unsafe for hosted infrastructure --
		{
			name:        "LOAD",
			stmt:        &ast.LoadStmt{BaseNode: ast.BaseNode{Tag: ast.T_LoadStmt}, Filename: "auto_explain"},
			wantErr:     true,
			wantMessage: "LOAD is not supported",
		},
		{
			name:        "ALTER SYSTEM",
			stmt:        &ast.AlterSystemStmt{BaseNode: ast.BaseNode{Tag: ast.T_AlterSystemStmt}},
			wantErr:     true,
			wantMessage: "ALTER SYSTEM is not supported",
		},
		{
			name:        "CREATE DATABASE",
			stmt:        &ast.CreatedbStmt{BaseNode: ast.BaseNode{Tag: ast.T_CreatedbStmt}, Dbname: "test"},
			wantErr:     true,
			wantMessage: "CREATE DATABASE is not supported",
		},
		{
			name:        "DROP DATABASE",
			stmt:        &ast.DropdbStmt{BaseNode: ast.BaseNode{Tag: ast.T_DropdbStmt}, Dbname: "test"},
			wantErr:     true,
			wantMessage: "DROP DATABASE is not supported",
		},
		{
			name:        "CREATE LANGUAGE",
			stmt:        &ast.CreatePLangStmt{BaseNode: ast.BaseNode{Tag: ast.T_CreatePLangStmt}, PLName: "plpython3u"},
			wantErr:     true,
			wantMessage: "CREATE LANGUAGE is not supported",
		},
		{
			name:        "CREATE SUBSCRIPTION",
			stmt:        &ast.CreateSubscriptionStmt{BaseNode: ast.BaseNode{Tag: ast.T_CreateSubscriptionStmt}, SubName: "mysub"},
			wantErr:     true,
			wantMessage: "CREATE SUBSCRIPTION is not supported",
		},
		{
			name:        "CREATE FOREIGN DATA WRAPPER",
			stmt:        &ast.CreateFdwStmt{BaseNode: ast.BaseNode{Tag: ast.T_CreateFdwStmt}, FdwName: "myfdw"},
			wantErr:     true,
			wantMessage: "CREATE FOREIGN DATA WRAPPER is not supported",
		},
		{
			name:        "CREATE FOREIGN SERVER",
			stmt:        &ast.CreateForeignServerStmt{BaseNode: ast.BaseNode{Tag: ast.T_CreateForeignServerStmt}, Servername: "myserver"},
			wantErr:     true,
			wantMessage: "CREATE SERVER is not supported",
		},

		// -- Allowed statements should return nil --
		{
			name:    "SELECT is allowed",
			stmt:    &ast.SelectStmt{BaseNode: ast.BaseNode{Tag: ast.T_SelectStmt}},
			wantErr: false,
		},
		{
			name:    "INSERT is allowed",
			stmt:    &ast.InsertStmt{BaseNode: ast.BaseNode{Tag: ast.T_InsertStmt}},
			wantErr: false,
		},
		{
			name:    "CALL is allowed",
			stmt:    &ast.CallStmt{BaseNode: ast.BaseNode{Tag: ast.T_CallStmt}},
			wantErr: false,
		},
		{
			name:    "CREATE EXTENSION is allowed",
			stmt:    &ast.CreateExtensionStmt{BaseNode: ast.BaseNode{Tag: ast.T_CreateExtensionStmt}, Extname: "uuid-ossp"},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := planUnsupportedStmt(tt.stmt)

			if !tt.wantErr {
				assert.NoError(t, err)
				return
			}

			require.Error(t, err)
			var diag *mterrors.PgDiagnostic
			require.True(t, errors.As(err, &diag), "error should be a PgDiagnostic")
			assert.Equal(t, mterrors.PgSSFeatureNotSupported, diag.Code)
			assert.Contains(t, diag.Message, tt.wantMessage)
		})
	}
}

// TestPlanRejectsUnsafeStatements verifies that Plan() itself rejects unsafe
// statements before they reach the default routing path.
func TestPlanRejectsUnsafeStatements(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), nil))
	p := NewPlanner("default", logger, nil)
	testConn := server.NewTestConn(&bytes.Buffer{})

	tests := []struct {
		name string
		sql  string
		stmt ast.Stmt
	}{
		{
			name: "DO block rejected through Plan",
			sql:  "DO $$ BEGIN RAISE NOTICE 'hello'; END $$",
			stmt: &ast.DoStmt{BaseNode: ast.BaseNode{Tag: ast.T_DoStmt}},
		},
		{
			name: "CREATE FUNCTION rejected through Plan",
			sql:  "CREATE FUNCTION f() RETURNS void AS $$ BEGIN END $$ LANGUAGE plpgsql",
			stmt: &ast.CreateFunctionStmt{},
		},
		{
			name: "LOAD rejected through Plan",
			sql:  "LOAD 'auto_explain'",
			stmt: &ast.LoadStmt{BaseNode: ast.BaseNode{Tag: ast.T_LoadStmt}, Filename: "auto_explain"},
		},
		{
			name: "ALTER SYSTEM rejected through Plan",
			sql:  "ALTER SYSTEM SET max_connections = 200",
			stmt: &ast.AlterSystemStmt{BaseNode: ast.BaseNode{Tag: ast.T_AlterSystemStmt}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan, err := p.Plan(tt.sql, tt.stmt, testConn.Conn)
			require.Error(t, err)
			assert.Nil(t, plan)

			var diag *mterrors.PgDiagnostic
			require.True(t, errors.As(err, &diag))
			assert.Equal(t, mterrors.PgSSFeatureNotSupported, diag.Code)
		})
	}
}
