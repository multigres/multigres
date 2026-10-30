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

package handler

import (
	"strings"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/servenv"
)

// Gateway-provided SQL functions have no backend implementation, so the gateway
// folds them into constants before the query reaches PostgreSQL. Currently only
// `multigres.version()`, folded to a text constant of the version — which means it
// works in ANY expression position (target list, WHERE, function argument, ...)
// and as a FROM item, in both the simple and extended protocols, unlike a
// standalone-only interception.
//
// Folding happens once the query is parsed, so every downstream consumer — the
// planner, the plan cache, and the extended protocol's Describe/Execute, which
// reuse the stored statement text — sees the same already-substituted query.
// Callers that already hold the parse (the simple-query path) fold the
// statements directly; the extended-protocol Parse path, which only has a
// string, uses foldGatewayFunctions.

// containsGatewayFunction is a cheap pre-check: the only gateway function lives
// in the `multigres` schema, so a query that never names it can't contain one.
// It lets callers skip the AST walk (and, for foldGatewayFunctions, the parse)
// for the overwhelming majority of queries.
func containsGatewayFunction(sql string) bool {
	return strings.Contains(strings.ToLower(sql), constants.MultigresSchema)
}

// foldGatewayFunctionsInStatements folds gateway functions in the already-parsed
// statements, returning whether anything changed. Used by the simple-query path
// to fold without re-parsing. Callers should gate on containsGatewayFunction to
// skip the walk when no gateway function can be present.
func foldGatewayFunctionsInStatements(stmts []ast.Stmt) bool {
	version := servenv.AppVersion()
	changed := false
	for _, stmt := range stmts {
		if foldMultigresVersion(stmt, version) {
			changed = true
		}
	}
	return changed
}

// foldGatewayFunctions folds gateway functions in a raw SQL string, returning the
// rewritten SQL (or the input unchanged). Used where only the string is
// available — the extended-protocol Parse path. It returns sql unchanged when it
// contains no gateway function (detected before any parse) or does not parse (the
// normal path then reports the syntax error with the correct position).
func foldGatewayFunctions(sql string) string {
	if !containsGatewayFunction(sql) {
		return sql
	}
	stmts, err := parser.ParseSQL(sql)
	if err != nil || len(stmts) == 0 {
		return sql
	}
	if !foldGatewayFunctionsInStatements(stmts) {
		return sql
	}
	return renderStatements(stmts)
}

// renderStatements re-renders parsed statements back to a single SQL string,
// joining a multi-statement batch with "; " as PostgreSQL's simple protocol does.
func renderStatements(stmts []ast.Stmt) string {
	parts := make([]string, len(stmts))
	for i, stmt := range stmts {
		parts[i] = stmt.SqlString()
	}
	return strings.Join(parts, "; ")
}

// foldMultigresVersion replaces every `multigres.version()` call in stmt with a
// text constant of version, returning whether it changed anything. When the call
// is a bare top-level target with no alias it also sets the column label to
// `version`, so the result matches what a real `version()` would be labelled
// (a bare literal would otherwise be labelled `?column?`).
//
// A FROM item is not an expression position: the grammar only accepts a
// function-like expression there, so a literal would be a syntax error. There
// the call becomes versionFromItem instead. `CALL multigres.version()` names a
// procedure rather than evaluating an expression, so it is left for the backend
// to reject.
func foldMultigresVersion(stmt ast.Stmt, version string) bool {
	changed := false
	ast.Rewrite(stmt, func(cursor *ast.Cursor) bool {
		switch n := cursor.Node().(type) {
		case *ast.ResTarget:
			// Label a bare call `version`, as PostgreSQL labels version(). The
			// FuncCall case below replaces it.
			if fc, ok := n.Val.(*ast.FuncCall); ok && n.Name == "" && isMultigresVersionFunc(fc) {
				n.Name = constants.MultigresVersionFunction
			}
		case *ast.RangeFunction:
			// A FROM item must stay function-like. Each item is a (function,
			// column definitions) pair; the other functions' arguments are
			// expressions, folded as the walk continues.
			for _, item := range n.Functions.Items {
				pair := item.(*ast.NodeList)
				if fc, _ := pair.Items[0].(*ast.FuncCall); isMultigresVersionFunc(fc) {
					pair.Items[0] = versionFromItem(version)
					changed = true
				}
			}
		case *ast.FuncCall:
			// Anywhere else it is an expression, except the procedure name of CALL.
			if _, isCall := cursor.Parent().(*ast.CallStmt); !isCall && isMultigresVersionFunc(n) {
				cursor.Replace(versionText(version))
				changed = true
			}
		}
		return true
	}, nil)
	return changed
}

// versionText is the expression-position fold: `CAST('<version>' AS text)`.
// The cast types the constant as text, as version() is; a bare literal would be
// of type unknown (visible through pg_typeof, and resolved differently in
// operator and function overload resolution). The type name is unqualified
// because the deparser drops `pg_catalog.` from built-in types anyway; it
// resolves through the session's search_path like any cast the user writes,
// and like version() itself.
func versionText(version string) ast.Node {
	return ast.NewTypeCast(ast.NewA_Const(ast.NewString(version), 0), ast.NewTypeName([]string{"text"}), 0)
}

// versionFromItem is the FROM-item fold:
//
//	CAST((SELECT CAST('<version>' AS text) AS version) AS text)
//
// CAST is one of the function-like expressions the grammar accepts as a FROM
// item. PostgreSQL names a scalar FROM item's column (and, when unaliased, the
// item itself) after FigureColname of the expression, which looks through a
// CAST into a scalar subquery and takes the subquery's column name: `version`,
// as for version(). An alias renames them as it would for version(), and WITH
// ORDINALITY, LATERAL, ROWS FROM (with other functions, in any position) and a
// rejected column definition list behave the same, with no alias rewriting.
//
// The alternative, `pg_catalog.unnest(ARRAY[...])` with an injected `version`
// alias, labels the column `unnest` inside a multi-function ROWS FROM, where
// PostgreSQL takes column names from the functions, not the alias.
func versionFromItem(version string) ast.Node {
	sel := ast.NewSelectStmt()
	sel.TargetList.Append(ast.NewResTarget(constants.MultigresVersionFunction, versionText(version)))
	return ast.NewTypeCast(ast.NewSubLink(ast.EXPR_SUBLINK, sel), ast.NewTypeName([]string{"text"}), 0)
}

// isMultigresVersionFunc reports whether fc is a zero-argument, schema-qualified
// `multigres.version()` call (case-insensitive). The `multigres` schema
// qualification is required so a bare `version()` still routes to PostgreSQL and
// reports the backend version.
func isMultigresVersionFunc(fc *ast.FuncCall) bool {
	if fc == nil || (fc.Args != nil && fc.Args.Len() > 0) {
		return false
	}
	fn := fc.Funcname
	if fn == nil || fn.Len() != 2 {
		return false
	}
	return funcNamePart(fn.Items[0]) == constants.MultigresSchema &&
		funcNamePart(fn.Items[1]) == constants.MultigresVersionFunction
}

// funcNamePart returns the lowercased value if n is a *ast.String, else "".
func funcNamePart(n ast.Node) string {
	s, ok := n.(*ast.String)
	if !ok {
		return ""
	}
	return strings.ToLower(s.SVal)
}
