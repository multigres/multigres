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
	"strings"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/parser/ast/plpgsqlast"
	"github.com/multigres/multigres/go/common/parser/plpgsql"
)

// analyzeProceduralBody closes the Tier 1 vector: a procedural-language body can
// change backend session state or reach a blocklisted function without the SQL
// parser ever seeing the offending statement, because the body is an opaque
// dollar-quoted string. It extracts the body, parses it, and runs the same
// expression-level analysis the gateway already applies to top-level SQL over
// every embedded fragment.
//
// Only DO and CREATE FUNCTION/PROCEDURE carry such an opaque body. The other
// Tier 1 statements embed their code as ordinary SQL AST that the top-level
// FuncCall walk (analyzeFunctionCalls) already reaches, so they need nothing
// here:
//
//   - CREATE RULE — its actions (RuleStmt.Actions) and qualification are SQL
//     nodes the walk descends into; a set_config there is not a top-level
//     SELECT target and is rejected, a blocklisted call is rejected.
//   - CREATE TRIGGER / CREATE EVENT TRIGGER — carry only a WHEN expression
//     (also walked) and a function reference by name; that function's own body
//     was analyzed when it was created through the pooler.
//   - CREATE FUNCTION with a SQL-standard body (BEGIN ATOMIC …, in SQLBody) —
//     already a parsed SQL AST the walk reaches; handled at the top level.
//
// Returns a *mterrors.PgDiagnostic (feature_not_supported, 0A000) to reject the
// whole statement, or nil to allow it. Fail-closed: a body that cannot be
// parsed, or that uses a procedural language we cannot inspect, is rejected.
func analyzeProceduralBody(stmt ast.Stmt) error {
	switch s := stmt.(type) {
	case *ast.DoStmt:
		body, lang, ok := doStmtBody(s)
		if !ok {
			return nil
		}
		return analyzeBodyForLanguage(body, lang)
	case *ast.CreateFunctionStmt:
		// A SQL-standard body (BEGIN ATOMIC … END / RETURN expr) is a parsed SQL
		// tree in SQLBody, already walked by analyzeFunctionCalls at the top
		// level; only the opaque `AS '…'` string body needs body analysis.
		if s.SQLBody != nil {
			return nil
		}
		body, lang, ok := createFunctionBody(s)
		if !ok {
			return nil
		}
		return analyzeBodyForLanguage(body, lang)
	}
	return nil
}

// analyzeBodyForLanguage dispatches on the body's language:
//
//   - plpgsql — parse with the PL/pgSQL parser and walk every embedded fragment.
//   - sql — a SQL function body is a list of SQL statements; parse and analyze
//     each with the same body policy (no PL/pgSQL parser needed).
//   - anything else (c, internal, plperl, plpython, …) — opaque; we cannot see
//     inside it, so fail closed and reject.
func analyzeBodyForLanguage(body, lang string) error {
	switch strings.ToLower(strings.TrimSpace(lang)) {
	case "plpgsql":
		fn, err := plpgsql.ParsePLpgSQL(body)
		if err != nil {
			return mterrors.NewFeatureNotSupported(
				"the PL/pgSQL body could not be parsed for safety analysis, so it cannot be run through the connection pooler")
		}
		return analyzePLpgSQLFunction(fn)
	case "sql":
		stmts, err := parser.ParseSQL(body)
		if err != nil {
			return mterrors.NewFeatureNotSupported(
				"the SQL function body could not be parsed for safety analysis, so it cannot be run through the connection pooler")
		}
		for _, st := range stmts {
			if err := analyzeBodyFragment(st); err != nil {
				return err
			}
		}
		return nil
	default:
		return mterrors.NewFeatureNotSupported(
			"LANGUAGE " + lang + " function bodies cannot be inspected by the connection pooler and are not supported")
	}
}

// analyzePLpgSQLFunction walks a parsed PL/pgSQL body and analyzes every
// embedded SQL fragment it reaches. See bodyWalker for how the traversal works.
func analyzePLpgSQLFunction(fn *plpgsqlast.PLpgSQL_function) error {
	if fn == nil || fn.Action == nil {
		return mterrors.NewFeatureNotSupported(
			"the PL/pgSQL body is empty or malformed and cannot be run through the connection pooler")
	}
	w := &bodyWalker{}
	plpgsqlast.Rewrite(fn, w.visit, nil)
	return w.err
}

// bodyWalker analyzes every embedded SQL fragment in a PL/pgSQL body. Rewrite
// supplies the traversal for the procedural scaffolding (blocks, IF, loops,
// exception handlers), and most fragments are handled generically at the
// PLpgSQL_expr leaf, keyed off ParseMode.
//
// The handful of statements that carry a child needing non-default treatment
// are intercepted at the statement node instead: a PERFORM expression and a
// bound-cursor argument list are bare expressions (despite their DEFAULT parse
// mode) that must be SELECT-wrapped, and an EXECUTE payload string must take the
// dynamic-string policy rather than being read as an ordinary expression. For
// those, the walker analyzes the children itself and returns false, so the
// generic descent does not reach them a second time — which is why no identity
// map is needed to distinguish an EXECUTE payload from an ordinary expression.
//
// The cost of returning false is that the walker then owns re-descending into
// that statement's other children (USING params, a dynamic FOR loop body); a
// forgotten child would go unanalyzed, so those are covered by tests.
type bodyWalker struct {
	err error
}

func (w *bodyWalker) visit(cursor *plpgsqlast.Cursor) bool {
	if w.err != nil {
		return false
	}
	switch n := cursor.Node().(type) {
	case *plpgsqlast.PLpgSQL_stmt_perform:
		// PERFORM's text is an expression PG runs as `SELECT <expr>` (our port
		// drops the substituted SELECT); this is where we know to add it back
		// rather than guessing from ParseMode.
		w.expression(n.Expr)
		return false
	case *plpgsqlast.PLpgSQL_stmt_dynexecute:
		w.dynamic(n.Query)
		w.expressions(n.Params)
		return false
	case *plpgsqlast.PLpgSQL_stmt_dynfors:
		w.dynamic(n.Query)
		w.expressions(n.Params)
		w.statements(n.Body)
		return false
	case *plpgsqlast.PLpgSQL_stmt_return_query:
		// Exactly one of Query (static) / DynQuery (EXECUTE) is set.
		if n.DynQuery != nil {
			w.dynamic(n.DynQuery)
		} else {
			w.statement(n.Query)
		}
		w.expressions(n.Params)
		return false
	case *plpgsqlast.PLpgSQL_stmt_open:
		w.expression(n.Argquery) // bound-cursor `(args)` — an expression list
		w.statement(n.Query)     // OPEN … FOR <query>
		w.dynamic(n.DynQuery)    // OPEN … FOR EXECUTE <string>
		w.expressions(n.Params)
		return false
	case *plpgsqlast.PLpgSQL_expr:
		// Every other embedded fragment. ParseMode is authoritative here: the
		// only DEFAULT-mode fragments that are not standalone statements are the
		// PERFORM expression and the bound-cursor argument list, both handled
		// above, so a DEFAULT fragment reaching this point is a real statement.
		w.fragment(n)
		return true
	}
	return true
}

// fragment analyzes a leaf reached by the generic descent, choosing the parse
// from its ParseMode.
func (w *bodyWalker) fragment(e *plpgsqlast.PLpgSQL_expr) {
	if e.ParseMode == plpgsqlast.RAW_PARSE_DEFAULT {
		w.statement(e)
	} else {
		w.expression(e)
	}
}

// statement analyzes a fragment whose text is a complete SQL statement.
func (w *bodyWalker) statement(e *plpgsqlast.PLpgSQL_expr) {
	if w.err != nil || e == nil || strings.TrimSpace(e.Query) == "" {
		return
	}
	w.analyzeSQL(e.Query)
}

// expression analyzes a fragment whose text is a bare SQL expression, wrapping
// it as `SELECT <expr>` so the FuncCalls inside are reachable.
func (w *bodyWalker) expression(e *plpgsqlast.PLpgSQL_expr) {
	if w.err != nil || e == nil || strings.TrimSpace(e.Query) == "" {
		return
	}
	w.analyzeSQL("SELECT " + e.Query)
}

// expressions analyzes each fragment in a USING clause.
func (w *bodyWalker) expressions(list []*plpgsqlast.PLpgSQL_expr) {
	for _, e := range list {
		w.expression(e)
	}
}

// dynamic enforces the EXECUTE-payload policy on a fragment (see
// analyzeDynamicExecute): a string literal is analyzed as if written inline, a
// runtime-built string is rejected.
func (w *bodyWalker) dynamic(e *plpgsqlast.PLpgSQL_expr) {
	if w.err != nil || e == nil || strings.TrimSpace(e.Query) == "" {
		return
	}
	w.err = analyzeDynamicExecute(e)
}

// statements re-descends into a nested statement list (a dynamic FOR loop body)
// that the parent's `return false` excluded from the generic traversal.
func (w *bodyWalker) statements(list []plpgsqlast.Stmt) {
	for _, st := range list {
		if w.err != nil {
			return
		}
		plpgsqlast.Rewrite(st, w.visit, nil)
	}
}

// analyzeSQL parses one fragment's SQL text and runs the body policy over each
// resulting statement.
func (w *bodyWalker) analyzeSQL(sql string) {
	stmts, err := parser.ParseSQL(sql)
	if err != nil {
		w.err = mterrors.NewFeatureNotSupported(
			"a PL/pgSQL body fragment could not be parsed for safety analysis, so it cannot be run through the connection pooler")
		return
	}
	for _, st := range stmts {
		if err := analyzeBodyFragment(st); err != nil {
			w.err = err
			return
		}
	}
}

// analyzeDynamicExecute enforces the dynamic-EXECUTE policy: if the executed
// string is a plain string literal we parse and analyze it exactly as if it had
// been written inline; if it is built at runtime (concatenation, format(), a
// variable, …) it cannot be proven safe, so we reject. This matches how a
// non-literal set_config argument is rejected at the top level.
func analyzeDynamicExecute(e *plpgsqlast.PLpgSQL_expr) error {
	literal, ok := dynamicExecuteLiteral(e.Query)
	if !ok {
		return mterrors.NewFeatureNotSupported(
			"EXECUTE of a runtime-built statement inside a PL/pgSQL body is not supported: " +
				"the statement text is not a constant, so it cannot be checked for unsafe session-state changes")
	}
	stmts, err := parser.ParseSQL(literal)
	if err != nil {
		return mterrors.NewFeatureNotSupported(
			"a dynamic EXECUTE statement inside a PL/pgSQL body could not be parsed for safety analysis")
	}
	for _, st := range stmts {
		if err := analyzeBodyFragment(st); err != nil {
			return err
		}
	}
	return nil
}

// dynamicExecuteLiteral returns the constant string an EXECUTE argument reduces
// to, or ("", false) if it is not a plain string literal. The argument text is
// wrapped as `SELECT <text>` so the SQL parser resolves any quoting; only a
// single unadorned string A_Const target counts as a literal.
func dynamicExecuteLiteral(exprText string) (string, bool) {
	stmts, err := parser.ParseSQL("SELECT " + exprText)
	if err != nil || len(stmts) != 1 {
		return "", false
	}
	sel, ok := stmts[0].(*ast.SelectStmt)
	if !ok || sel.TargetList == nil || sel.TargetList.Len() != 1 {
		return "", false
	}
	rt, ok := sel.TargetList.Items[0].(*ast.ResTarget)
	if !ok {
		return "", false
	}
	c, ok := rt.Val.(*ast.A_Const)
	if !ok || c.Isnull {
		return "", false
	}
	s, ok := c.Val.(*ast.String)
	if !ok {
		return "", false
	}
	return s.SVal, true
}

// analyzeBodyFragment runs the unsafe-statement checks over one SQL statement
// extracted from a procedural body, plus the stricter body-only policy: any
// session-state change is rejected rather than tracked. At the top level an
// accepted set_config / SET is mirrored into the gateway's session tracker, but
// a change inside a body may run conditionally (in an IF/LOOP/exception
// handler) or not at all, so the gateway cannot faithfully reproduce it — the
// conservative choice is to reject it.
//
// A fragment that itself carries a procedural body (a nested DO or CREATE
// FUNCTION) is recursed into, so the analysis is not defeated by one level of
// nesting.
func analyzeBodyFragment(stmt ast.Stmt) error {
	if stmt == nil {
		return nil
	}
	if err := rejectUnsupportedStatement(stmt); err != nil {
		return err
	}
	if err := checkRestrictedGUCChange(stmt); err != nil {
		return err
	}
	if err := rejectBodySessionStateStmt(stmt); err != nil {
		return err
	}
	analysis, err := analyzeFunctionCalls(stmt, true /* reject */)
	if err != nil {
		return err
	}
	if len(analysis.SetConfigs) > 0 || analysis.DynamicSetConfig {
		return mterrors.NewFeatureNotSupported(
			"set_config inside a PL/pgSQL body is not supported: it changes backend session state that the " +
				"connection pooler cannot track, because a body may apply it conditionally or not at all")
	}
	return analyzeProceduralBody(stmt)
}

// rejectBodySessionStateStmt rejects statement forms that mutate backend
// session state in a way the pooler cannot track when they run inside a body.
// At the top level a SET / RESET is handled by planVariableSetStmt and a
// LISTEN / DISCARD by a dedicated primitive; inside a body there is no such
// hook, so the change would leak to the next client on the backend.
func rejectBodySessionStateStmt(stmt ast.Stmt) error {
	switch stmt.(type) {
	case *ast.VariableSetStmt:
		return mterrors.NewFeatureNotSupported(
			"SET/RESET inside a PL/pgSQL body is not supported: it changes backend session state that the " +
				"connection pooler cannot track")
	case *ast.DiscardStmt:
		return mterrors.NewFeatureNotSupported(
			"DISCARD inside a PL/pgSQL body is not supported through the connection pooler")
	case *ast.ListenStmt, *ast.UnlistenStmt:
		return mterrors.NewFeatureNotSupported(
			"LISTEN/UNLISTEN inside a PL/pgSQL body is not supported through the connection pooler")
	}
	return nil
}

// doStmtBody pulls the body text and language out of a DO statement's option
// list. The body is the `as` DefElem; the language is the `language` DefElem
// and defaults to plpgsql, matching PostgreSQL. Returns ok=false only when
// there is no body option (a malformed DO we leave for PostgreSQL to reject).
func doStmtBody(s *ast.DoStmt) (body, lang string, ok bool) {
	lang = "plpgsql"
	if s.Args == nil {
		return "", "", false
	}
	haveBody := false
	for _, item := range s.Args.Items {
		de, isDefElem := item.(*ast.DefElem)
		if !isDefElem {
			continue
		}
		switch de.Defname {
		case "as":
			if str, isStr := de.Arg.(*ast.String); isStr {
				body = str.SVal
				haveBody = true
			}
		case "language":
			if str, isStr := de.Arg.(*ast.String); isStr {
				lang = str.SVal
			}
		}
	}
	return body, lang, haveBody
}

// createFunctionBody pulls the body text and language out of a CREATE FUNCTION /
// PROCEDURE option list. The `as` DefElem holds the body: a single string
// literal (`AS $$ … $$`), or a two-element list for a C function
// (`AS 'objfile', 'symbol'`) which carries no analyzable SQL body — for that
// form only the language matters, and it routes to the opaque-language
// rejection. Returns ok=false when there is no `as` option (e.g. CREATE
// FUNCTION … LANGUAGE internal without one), leaving it for PostgreSQL.
func createFunctionBody(s *ast.CreateFunctionStmt) (body, lang string, ok bool) {
	if s.Options == nil {
		return "", "", false
	}
	haveAs := false
	for _, item := range s.Options.Items {
		de, isDefElem := item.(*ast.DefElem)
		if !isDefElem {
			continue
		}
		switch de.Defname {
		case "language":
			if str, isStr := de.Arg.(*ast.String); isStr {
				lang = str.SVal
			}
		case "as":
			haveAs = true
			switch arg := de.Arg.(type) {
			case *ast.String:
				body = arg.SVal
			case *ast.NodeList:
				// A single-element list is the function body; a two-element list is
				// a C function's (objfile, symbol) and has no SQL body to analyze.
				if arg.Len() == 1 {
					if str, isStr := arg.Items[0].(*ast.String); isStr {
						body = str.SVal
					}
				}
			}
		}
	}
	return body, lang, haveAs
}
