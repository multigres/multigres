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
	"regexp"
	"strconv"
	"strings"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser"
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
			LogicalName:       originalName,
		}, nil
	}

	return nil, errors.New("execute SQL template: could not choose a unique placeholder")
}

var (
	poolerPreparedStatementMessageRE = regexp.MustCompile(`prepared statement "ppstmt[0-9]+"`)
	poolerExecuteNameRE              = regexp.MustCompile(`(?i)\bEXECUTE[ \t]+ppstmt[0-9]+`)
	parameterCoercionRE              = regexp.MustCompile(`^parameter \$([0-9]+) .* cannot be coerced to the expected type `)
)

// TranslateSQLPreparedStatementError hides pooler-local names from diagnostics.
// For a top-level EXECUTE, backend cursor positions refer to the rewritten
// ppstmt* command; map argument coercion errors back to the original AST and
// suppress positions that refer to the prepared body rather than EXECUTE.
func TranslateSQLPreparedStatementError(err error, logicalName, sourceSQL string, stmt *ast.ExecuteStmt) error {
	var diagnostic *mterrors.PgDiagnostic
	if !errors.As(err, &diagnostic) {
		return err
	}

	translated := *diagnostic
	replaceMessageName := func(value string) string {
		return poolerPreparedStatementMessageRE.ReplaceAllStringFunc(value, func(string) string {
			return `prepared statement "` + logicalName + `"`
		})
	}
	replaceExecuteName := func(value string) string {
		return poolerExecuteNameRE.ReplaceAllStringFunc(value, func(match string) string {
			return match[:strings.LastIndexAny(match, " \t")+1] + ast.QuoteIdentifier(logicalName)
		})
	}
	translated.Message = replaceMessageName(translated.Message)
	translated.Detail = replaceMessageName(translated.Detail)
	translated.Hint = replaceMessageName(translated.Hint)
	translated.InternalQuery = replaceExecuteName(translated.InternalQuery)
	translated.Where = replaceExecuteName(translated.Where)

	if stmt != nil {
		translated.Position = 0
		match := parameterCoercionRE.FindStringSubmatch(translated.Message)
		if len(match) == 2 && stmt.Params != nil {
			parameter, _ := strconv.Atoi(match[1])
			translated.Position = executeParameterPosition(sourceSQL, stmt, parameter)
		}
	}
	return &translated
}

func executeParameterPosition(sourceSQL string, stmt *ast.ExecuteStmt, parameter int) int32 {
	if parameter <= 0 || stmt.Params == nil || parameter > stmt.Params.Len() {
		return 0
	}
	if sourceSQL != "" {
		lexer := parser.NewLexer(sourceSQL)
		seenExecute, seenName, inArgs, expectArgument := false, false, false, false
		depth, brackets, argument := 0, 0, 0
		for token := lexer.NextToken(); token.Type != parser.EOF && token.Type != parser.INVALID; token = lexer.NextToken() {
			switch {
			case !seenExecute:
				seenExecute = token.Type == parser.EXECUTE
			case !seenName:
				seenName = true
			case !inArgs:
				if token.Text == "(" {
					inArgs, expectArgument, depth = true, true, 1
				}
			case token.Text == ")":
				depth--
				if depth == 0 {
					return 0
				}
			case token.Text == "[":
				brackets++
			case token.Text == "]":
				brackets--
			case token.Text == "," && depth == 1 && brackets == 0:
				expectArgument = true
			case expectArgument:
				argument++
				if argument == parameter {
					return int32(token.Position + 1)
				}
				expectArgument = false
				if token.Text == "(" {
					depth++
				}
			case token.Text == "(":
				depth++
			}
		}
	}

	sql := stmt.SqlString()
	from := strings.Index(sql, "(") + 1
	for i := range parameter {
		value := stmt.Params.Items[i].SqlString()
		offset := strings.Index(sql[from:], value)
		if offset < 0 {
			return 0
		}
		from += offset
		if i == parameter-1 {
			return int32(from + 1)
		}
		from += len(value)
	}
	return 0
}
