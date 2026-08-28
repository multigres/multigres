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
	"fmt"
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
		// A DO block takes no arguments, so there is nothing to seed.
		body, lang, ok := doStmtBody(s)
		if !ok {
			return nil
		}
		return analyzeBodyForLanguage(body, lang, nil)
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
		return analyzeBodyForLanguage(body, lang, createFunctionSeed(s))
	}
	return nil
}

// createFunctionSeed builds the set of function-scope names to pre-declare when
// parsing the body (see plpgsql.ParseSeed). It mirrors what PG's do_compile
// (pl_comp.c) derives from the pg_proc row: every parameter registered as "$N"
// and, if named, by name; and for a trigger / event-trigger function the
// implicit NEW/OLD and TG_* variables. Without these, a body-only parse would
// misread an assignment to a parameter (`param := …`) or a trigger variable
// (`new.field := …`) as an embedded SQL statement and reject the whole body.
func createFunctionSeed(s *ast.CreateFunctionStmt) *plpgsql.ParseSeed {
	seed := &plpgsql.ParseSeed{}
	pos := 0
	if s.Parameters != nil {
		for _, item := range s.Parameters.Items {
			fp, ok := item.(*ast.FunctionParameter)
			if !ok {
				continue
			}
			if fp.Mode == ast.FUNC_PARAM_TABLE {
				// A RETURNS TABLE (col type) column is a body variable by name, not
				// a positional argument.
				if fp.Name != "" {
					seed.Scalars = append(seed.Scalars, fp.Name)
				}
				continue
			}
			// IN / OUT / INOUT / VARIADIC / DEFAULT: positional, reachable as $N and
			// (when named) by name.
			pos++
			seed.Scalars = append(seed.Scalars, fmt.Sprintf("$%d", pos))
			if fp.Name != "" {
				seed.Scalars = append(seed.Scalars, fp.Name)
			}
		}
	}
	switch returnTypeName(s) {
	case "trigger":
		seed.Records = append(seed.Records, "new", "old")
		seed.Scalars = append(seed.Scalars,
			"tg_name", "tg_when", "tg_level", "tg_op", "tg_relid",
			"tg_relname", "tg_table_name", "tg_table_schema", "tg_nargs", "tg_argv")
	case "event_trigger":
		seed.Scalars = append(seed.Scalars, "tg_event", "tg_tag")
	}
	return seed
}

// returnTypeName returns the lowercased unqualified return type name of a
// CREATE FUNCTION, or "" — used to spot trigger / event-trigger functions.
func returnTypeName(s *ast.CreateFunctionStmt) string {
	if s.ReturnType == nil || s.ReturnType.Names == nil || s.ReturnType.Names.Len() == 0 {
		return ""
	}
	last := s.ReturnType.Names.Items[s.ReturnType.Names.Len()-1]
	if str, ok := last.(*ast.String); ok {
		return strings.ToLower(str.SVal)
	}
	return ""
}

// analyzeBodyForLanguage dispatches on the body's language:
//
//   - plpgsql — parse with the PL/pgSQL parser and walk every embedded fragment.
//   - sql — a SQL function body is a list of SQL statements; parse and analyze
//     each with the same body policy (no PL/pgSQL parser needed).
//   - c / internal — the "body" is a symbol reference (`AS 'objfile','symbol'`
//     for C, `AS 'symbol'` for internal) into a shared library or the server
//     binary, not SQL. There is nothing session-state-shaped to hide, and
//     creating one already requires the library to be present on the server (a
//     filesystem/superuser concern gated elsewhere — no LOAD, no pg_read_file,
//     no filesystem writes through the pooler). So the pooler has nothing to
//     analyze and no Tier 1 leak vector to close: allow it.
//   - anything else (plperl, plpython, pltcl, …) — an opaque procedural body
//     that is arbitrary code able to change backend session state we cannot
//     observe. Fail closed and reject.
func analyzeBodyForLanguage(body, lang string, seed *plpgsql.ParseSeed) error {
	switch strings.ToLower(strings.TrimSpace(lang)) {
	case "plpgsql":
		fn, err := plpgsql.ParsePLpgSQLSeeded(body, seed)
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
	case "c", "internal":
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
		for _, arg := range n.Args { // bound-cursor `(args)` — one value per arg
			w.expression(arg.Value)
		}
		w.statement(n.Query)  // OPEN … FOR <query>
		w.dynamic(n.DynQuery) // OPEN … FOR EXECUTE <string>
		w.expressions(n.Params)
		return false
	case *plpgsqlast.PLpgSQL_stmt_fors:
		// A query FOR loop (`FOR r IN <query>`) and a bound-cursor FOR loop
		// (`FOR r IN c[(args)]`) share this node — without variable resolution we
		// cannot tell the query from a cursor reference — so the loop query needs
		// the statement-or-expression treatment (see statementOrExpression).
		w.statementOrExpression(n.Query)
		w.statements(n.Body)
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

// statementOrExpression analyzes a FOR-loop query that may be either a complete
// SQL query (a query FOR loop, `FOR r IN SELECT …`) or a bare cursor reference
// (a bound-cursor FOR loop, `FOR r IN c(5,7)` / `FOR r IN c2`). Without variable
// resolution the parser cannot tell them apart, so we try the text as a statement
// first and fall back to an expression; only text that parses as neither is
// rejected. A cursor reference parses only as the expression, and the cursor's
// bound query was already analyzed at its DECLARE — here we just need to reach any
// calls in the arguments.
func (w *bodyWalker) statementOrExpression(e *plpgsqlast.PLpgSQL_expr) {
	if w.err != nil || e == nil || strings.TrimSpace(e.Query) == "" {
		return
	}
	if _, err := parser.ParseSQL(e.Query); err != nil {
		w.expression(e)
		return
	}
	w.statement(e)
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

// dynSkeleton* are the tokens substituted for an EXECUTE statement's
// injection-safe interpolation points when reconstructing its fixed structure:
// %I / quote_ident collapse their value to a single identifier, %L /
// quote_literal to a single literal.
const (
	dynSkeletonIdent   = "mg_dyn_ident"
	dynSkeletonLiteral = "'mg_dyn_lit'"
)

// analyzeDynamicExecute enforces the dynamic-EXECUTE policy over an EXECUTE
// payload expression:
//
//   - A plain string literal is analyzed exactly as if the statement had been
//     written inline.
//   - A statement assembled with PostgreSQL's injection-safe primitives —
//     format() with only %I/%L conversions, or a `||` chain of string literals
//     and quote_ident()/quote_literal() calls — has a structure fully fixed by
//     its constant text, because %I/quote_ident collapse a value to one
//     identifier token and %L/quote_literal to one literal token. We rebuild that
//     structure (safeExecuteSkeleton), analyze it as a static statement, and
//     analyze each interpolated value for blocklisted calls.
//   - Anything else (raw `||`, %s, a bare variable/param, EXECUTE of a whole
//     query value) can inject arbitrary statement text, so it cannot be proven
//     safe and is rejected — matching how a non-literal set_config argument is
//     rejected at the top level.
func analyzeDynamicExecute(e *plpgsqlast.PLpgSQL_expr) error {
	if literal, ok := dynamicExecuteLiteral(e.Query); ok {
		return analyzeDynamicStatementText(literal)
	}
	if skeleton, values, ok := safeExecuteSkeleton(e.Query); ok {
		if err := analyzeDynamicStatementText(skeleton); err != nil {
			return err
		}
		// The interpolated values must themselves be call-safe: a value that
		// runs when the argument is evaluated (e.g. quote_literal(dblink(…)))
		// must not reach a blocklisted function or change session state.
		for _, v := range values {
			if err := analyzeDynamicStatementText("SELECT " + v.SqlString()); err != nil {
				return err
			}
		}
		return nil
	}
	return mterrors.NewFeatureNotSupported(
		"EXECUTE of a runtime-built statement inside a PL/pgSQL body is not supported: " +
			"the statement text is not a constant, so it cannot be checked for unsafe session-state changes")
}

// analyzeDynamicStatementText parses one dynamic-EXECUTE statement (or a
// reconstructed skeleton) and runs the body policy over each parsed statement.
func analyzeDynamicStatementText(sqlText string) error {
	stmts, err := parser.ParseSQL(sqlText)
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

// executeArgExpr parses an EXECUTE payload expression (wrapped as `SELECT <text>`
// so the SQL parser resolves its quoting) and returns the single target
// expression node, or nil if the text is not a single scalar expression.
func executeArgExpr(exprText string) ast.Node {
	stmts, err := parser.ParseSQL("SELECT " + exprText)
	if err != nil || len(stmts) != 1 {
		return nil
	}
	sel, ok := stmts[0].(*ast.SelectStmt)
	if !ok || sel.TargetList == nil || sel.TargetList.Len() != 1 {
		return nil
	}
	rt, ok := sel.TargetList.Items[0].(*ast.ResTarget)
	if !ok {
		return nil
	}
	return rt.Val
}

// safeExecuteSkeleton reduces an EXECUTE payload expression built from
// PostgreSQL's injection-safe primitives to a fixed statement skeleton plus the
// value expressions it interpolates. The final bool is false if the expression
// could inject arbitrary statement structure. See analyzeDynamicExecute for the
// safety argument; reduceSafeExpr does the work.
func safeExecuteSkeleton(exprText string) (string, []ast.Node, bool) {
	arg := executeArgExpr(exprText)
	if arg == nil {
		return "", nil, false
	}
	var sb strings.Builder
	var values []ast.Node
	if !reduceSafeExpr(arg, &sb, &values) {
		return "", nil, false
	}
	return sb.String(), values, true
}

// reduceSafeExpr appends the fixed-structure contribution of one EXECUTE-payload
// sub-expression to sb, collecting interpolated value expressions into values. It
// returns false the moment it meets anything that could inject arbitrary
// structure — a raw `||` operand, a %s, a bare variable/param, any function other
// than the trusted quoting builtins — so a true result proves the whole payload
// is structurally fixed.
//
// KNOWN LIMITATION: the quoting builtins are matched by unqualified name only, so
// this trust is not proof the call resolves to the pg_catalog implementation. A
// tenant can shadow it — reorder search_path ahead of pg_catalog for quote_ident
// / quote_literal, or define an exact-signature format(text, text) which beats
// pg_catalog.format(text, VARIADIC "any") even under the default path — and have
// it return arbitrary statement text that the skeleton then accepts as safe.
// Closing this needs catalog/search_path resolution the plan-time analyzer does
// not have; matching pg_catalog.<name> only would instead reject the ubiquitous
// unqualified idiom. Accepted as-is for now.
func reduceSafeExpr(node ast.Node, sb *strings.Builder, values *[]ast.Node) bool {
	switch v := node.(type) {
	case *ast.A_Const:
		s, ok := v.Val.(*ast.String)
		if v.Isnull || !ok {
			return false
		}
		sb.WriteString(s.SVal)
		return true
	case *ast.A_Expr:
		if v.Kind != ast.AEXPR_OP || operatorName(v.Name) != "||" {
			return false
		}
		return reduceSafeExpr(v.Lexpr, sb, values) && reduceSafeExpr(v.Rexpr, sb, values)
	case *ast.FuncCall:
		switch bareFuncName(v.Funcname) {
		case "quote_ident":
			if v.Args == nil || v.Args.Len() != 1 {
				return false
			}
			sb.WriteString(dynSkeletonIdent)
			*values = append(*values, v.Args.Items[0])
			return true
		case "quote_literal":
			if v.Args == nil || v.Args.Len() != 1 {
				return false
			}
			sb.WriteString(dynSkeletonLiteral)
			*values = append(*values, v.Args.Items[0])
			return true
		case "format":
			return reduceSafeFormat(v, sb, values)
		}
	}
	return false
}

// reduceSafeFormat handles a format() call: its first argument must be a constant
// format string using only %%/%I/%L (expandFormatSkeleton), and its remaining
// arguments are interpolated values collected for separate analysis.
func reduceSafeFormat(fc *ast.FuncCall, sb *strings.Builder, values *[]ast.Node) bool {
	if fc.Args == nil || fc.Args.Len() < 1 {
		return false
	}
	fmtConst, ok := fc.Args.Items[0].(*ast.A_Const)
	if !ok || fmtConst.Isnull {
		return false
	}
	fmtStr, ok := fmtConst.Val.(*ast.String)
	if !ok {
		return false
	}
	if !expandFormatSkeleton(fmtStr.SVal, sb) {
		return false
	}
	for i := 1; i < fc.Args.Len(); i++ {
		*values = append(*values, fc.Args.Items[i])
	}
	return true
}

// expandFormatSkeleton writes format()'s constant skeleton to sb, replacing each
// conversion: %% -> %, %I -> a placeholder identifier, %L -> a placeholder
// literal. Any other conversion — notably %s (raw text substitution) and the
// width/positional specs we do not model — makes the structure non-constant, so
// it returns false.
func expandFormatSkeleton(f string, sb *strings.Builder) bool {
	for i := 0; i < len(f); i++ {
		if f[i] != '%' {
			sb.WriteByte(f[i])
			continue
		}
		i++
		if i >= len(f) {
			return false
		}
		switch f[i] {
		case '%':
			sb.WriteByte('%')
		case 'I':
			sb.WriteString(dynSkeletonIdent)
		case 'L':
			sb.WriteString(dynSkeletonLiteral)
		default:
			return false
		}
	}
	return true
}

// operatorName returns an A_Expr's unqualified operator name, or "".
func operatorName(nl *ast.NodeList) string {
	if nl == nil || nl.Len() != 1 {
		return ""
	}
	s, ok := nl.Items[0].(*ast.String)
	if !ok {
		return ""
	}
	return s.SVal
}

// bareFuncName returns a FuncCall's unqualified, lower-cased name, or "" if the
// name is schema-qualified (not a trusted pg_catalog builtin for our purposes).
func bareFuncName(nl *ast.NodeList) string {
	if nl == nil || nl.Len() != 1 {
		return ""
	}
	s, ok := nl.Items[0].(*ast.String)
	if !ok {
		return ""
	}
	return strings.ToLower(s.SVal)
}

// dynamicExecuteLiteral returns the constant string an EXECUTE argument reduces
// to, or ("", false) if it is not a plain string literal. The argument text is
// wrapped as `SELECT <text>` so the SQL parser resolves any quoting; only a
// single unadorned string A_Const target counts as a literal.
func dynamicExecuteLiteral(exprText string) (string, bool) {
	c, ok := executeArgExpr(exprText).(*ast.A_Const)
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
//
// The exception is a transaction-scoped SET (isTransactionScopedSet): SET LOCAL
// and SET TRANSACTION revert at transaction end, so they never outlive the
// transaction on a pooled backend. A restricted-GUC change is still caught first
// by checkRestrictedGUCChange, so even a SET LOCAL of a cluster-managed GUC is
// rejected there before reaching this allowance.
func rejectBodySessionStateStmt(stmt ast.Stmt) error {
	switch s := stmt.(type) {
	case *ast.VariableSetStmt:
		if isTransactionScopedSet(s) {
			return nil
		}
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

// isTransactionScopedSet reports whether a SET statement's effect is confined to
// the current transaction, so it cannot leak to the next client on a pooled
// backend. Two forms qualify and both revert at transaction end: SET LOCAL (any
// parameter — IsLocal), and SET TRANSACTION (the current transaction's
// characteristics). It excludes the session-scoped forms that share the node — a
// plain SET, RESET, SET … FROM CURRENT, and SET SESSION CHARACTERISTICS AS
// TRANSACTION, which sets the session default (its Name is not just "TRANSACTION").
func isTransactionScopedSet(s *ast.VariableSetStmt) bool {
	if s.IsLocal {
		return true
	}
	return s.Kind == ast.VAR_SET_MULTI && strings.EqualFold(s.Name, "TRANSACTION")
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
