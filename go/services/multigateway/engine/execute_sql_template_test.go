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

package engine

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/pb/query"
)

func parseExecuteStmt(t *testing.T, sql string) (ast.Stmt, *ast.ExecuteStmt) {
	t.Helper()
	stmts, err := parser.ParseSQL(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	execStmt, ok := stmts[0].(*ast.ExecuteStmt)
	require.True(t, ok, "expected ExecuteStmt, got %T", stmts[0])
	return stmts[0], execStmt
}

func TestBuildExecuteSQLPreparedStatementTopLevel(t *testing.T) {
	stmt, execStmt := parseExecuteStmt(t, "EXECUTE myplan (42, 'hello')")
	ps := &query.PreparedStatement{Name: "stmt0", Query: "SELECT $1, $2", ParamTypes: []uint32{23, 25}}

	template, err := BuildExecuteSQLPreparedStatement(stmt, execStmt, ps)
	require.NoError(t, err)

	assert.Same(t, ps, template.PreparedStatement)
	assert.Equal(t, "EXECUTE ", template.SqlPrefix)
	assert.Equal(t, " ( 42, 'hello' )", template.SqlSuffix)
	assert.Equal(t, "myplan", template.LogicalName)
	assert.Equal(t, "myplan", execStmt.Name, "helper must restore the user-visible name")
}

func TestTranslateSQLPreparedStatementError(t *testing.T) {
	sql := "EXECUTE q3(5::smallint, 10.5::float, false, 4::bigint, 'bytea')"
	_, execStmt := parseExecuteStmt(t, sql)
	err := &mterrors.PgDiagnostic{
		MessageType: 'E',
		Severity:    "ERROR",
		Message:     `parameter $3 of type boolean cannot be coerced to the expected type double precision for prepared statement "ppstmt42"`,
		Detail:      `prepared statement "ppstmt42"`,
		Position:    999,
	}

	var translated *mterrors.PgDiagnostic
	require.True(t, errors.As(TranslateSQLPreparedStatementError(err, "q3", sql, execStmt), &translated))
	assert.NotContains(t, translated.Message, "ppstmt")
	assert.Contains(t, translated.Message, `prepared statement "q3"`)
	assert.Equal(t, `prepared statement "q3"`, translated.Detail)
	assert.Equal(t, int32(strings.Index(sql, "false")+1), translated.Position)
	assert.Equal(t, int32(999), err.Position, "translation must not mutate the shared diagnostic")

	bodyError := *err
	bodyError.Message = `relation "ppstmt42" does not exist`
	var translatedBody *mterrors.PgDiagnostic
	require.True(t, errors.As(TranslateSQLPreparedStatementError(&bodyError, "q3", sql, execStmt), &translatedBody))
	assert.Equal(t, bodyError.Message, translatedBody.Message)
	assert.Zero(t, translatedBody.Position)
}

func TestBuildExecuteSQLPreparedStatementValidation(t *testing.T) {
	stmt, execStmt := parseExecuteStmt(t, "EXECUTE myplan")
	ps := &query.PreparedStatement{Name: "stmt0", Query: "SELECT 1"}

	_, err := BuildExecuteSQLPreparedStatement(nil, execStmt, ps)
	require.ErrorContains(t, err, "statement is nil")

	_, err = BuildExecuteSQLPreparedStatement(stmt, nil, ps)
	require.ErrorContains(t, err, "execute statement is nil")

	_, err = BuildExecuteSQLPreparedStatement(stmt, execStmt, nil)
	require.ErrorContains(t, err, "prepared statement is nil")
}

func TestBuildExecuteSQLPreparedStatementSkipsCollidingPlaceholders(t *testing.T) {
	args := make([]string, 100)
	for i := range args {
		placeholderName := fmt.Sprintf("__multigres execute placeholder %d", i)
		args[i] = "'" + ast.QuoteIdentifier(placeholderName) + "'"
	}
	stmt, execStmt := parseExecuteStmt(t, "EXECUTE myplan ("+strings.Join(args, ", ")+")")

	_, err := BuildExecuteSQLPreparedStatement(stmt, execStmt, &query.PreparedStatement{Name: "stmt0", Query: "SELECT 1"})
	require.ErrorContains(t, err, "could not choose a unique placeholder")
	assert.Equal(t, "myplan", execStmt.Name, "helper must restore the user-visible name after errors")
}
