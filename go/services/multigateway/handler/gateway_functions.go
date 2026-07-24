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
// `multigres.version()`, folded to a text literal of the version — which means it
// works in ANY expression position (target list, WHERE, function argument, ...)
// in both the simple and extended protocols, unlike a standalone-only
// interception.
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
// text literal of version, returning whether it changed anything. When the call
// is a bare top-level target with no alias it also sets the column label to
// `version`, so the result matches what a real `version()` would be labelled
// (a bare literal would otherwise be labelled `?column?`).
func foldMultigresVersion(stmt ast.Stmt, version string) bool {
	changed := false
	ast.Rewrite(stmt, func(cursor *ast.Cursor) bool {
		switch n := cursor.Node().(type) {
		case *ast.ResTarget:
			if fc, ok := n.Val.(*ast.FuncCall); ok && isMultigresVersionFunc(fc) {
				if n.Name == "" {
					n.Name = constants.MultigresVersionFunction
				}
				n.Val = ast.NewA_Const(ast.NewString(version), 0)
				changed = true
				return false
			}
		case *ast.FuncCall:
			if isMultigresVersionFunc(n) {
				cursor.Replace(ast.NewA_Const(ast.NewString(version), 0))
				changed = true
				return false
			}
		}
		return true
	}, nil)
	return changed
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
