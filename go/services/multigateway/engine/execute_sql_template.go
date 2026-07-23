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

	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/pb/query"
)

// BuildExecuteSQLPreparedStatement builds the structured SQL-level EXECUTE
// wrapper sent to the multipooler. The gateway owns SQL parsing/deparsing; the
// multipooler only resolves the prepared statement through its pooler-level
// consolidator and substitutes the resulting ppstmt* name between prefix/suffix.
//
// stmt is the full statement to execute. execStmt must be the ExecuteStmt inside
// stmt. Legal PostgreSQL grammar shapes contain at most one ExecuteStmt in a
// statement (top-level EXECUTE, EXPLAIN EXECUTE, CREATE TABLE ... AS EXECUTE,
// or EXPLAIN CREATE TABLE ... AS EXECUTE), so replacing exactly one rendered
// placeholder is sufficient.
func BuildExecuteSQLPreparedStatement(
	stmt ast.Node,
	execStmt *ast.ExecuteStmt,
	preparedStatement *query.PreparedStatement,
) (*query.ExecuteSqlPreparedStatement, error) {
	if stmt == nil {
		return nil, errors.New("execute SQL template: statement is nil")
	}
	if execStmt == nil {
		return nil, errors.New("execute SQL template: execute statement is nil")
	}
	if preparedStatement == nil {
		return nil, errors.New("execute SQL template: prepared statement is nil")
	}

	originalName := execStmt.Name
	defer func() { execStmt.Name = originalName }()

	// Pick a placeholder that renders as a quoted identifier and appears exactly
	// once in the deparsed SQL. If a user expression happens to contain the same
	// quoted identifier, advance and try another placeholder.
	for i := range 100 {
		placeholderName := fmt.Sprintf("__multigres execute placeholder %d", i)
		placeholderSQL := ast.QuoteIdentifier(placeholderName)

		execStmt.Name = placeholderName
		rendered := stmt.SqlString()
		prefix, suffix, found := strings.Cut(rendered, placeholderSQL)
		if !found || strings.Contains(suffix, placeholderSQL) {
			continue
		}

		return &query.ExecuteSqlPreparedStatement{
			PreparedStatement: preparedStatement,
			SqlPrefix:         prefix,
			SqlSuffix:         suffix,
		}, nil
	}

	return nil, errors.New("execute SQL template: could not choose a unique placeholder")
}
