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
	w := &bodyWalker{resolver: collectVarAssignments(fn)}
	plpgsqlast.Rewrite(fn, w.visit, w.leave)
	return w.err
}

// varResolver backs the intra-body dataflow that lets a bare-variable EXECUTE
// payload (`EXECUTE v` / `RETURN QUERY EXECUTE v`) be checked when v was built
// by safe assignments earlier in the body — e.g. `v := format('… %I …', c)` or
// `v := 'SELECT … ' || '… LIMIT $1'`. PostgreSQL's own guidance (and the newer
// supabase storage migrations) assemble a query into a text variable and then
// EXECUTE it; without following the assignment we would reject every such body.
//
// Collection is deliberately NOT flow-sensitive, which keeps acceptance sound:
// we only accept `EXECUTE v` when EVERY assignment to v anywhere in the body
// reduces to a safe skeleton, so whatever v holds at execution time is one of
// those proven-safe values. A variable written by any form we cannot reduce to
// a skeleton — a SELECT/EXECUTE … INTO target, a loop variable, FETCH INTO, GET
// DIAGNOSTICS, or a non-simple (subscripted / field) assignment target — is
// "tainted" and its bare-variable EXECUTE is rejected, fail-closed.
//
// The soundness argument only holds for a variable whose ENTIRE value history is
// visible in the body: a DECLARE-section local that is in lexical scope at the
// EXECUTE. Two ways a name can hold a value we do not see, both handled here:
//
//   - A function parameter (or OUT parameter, or an ALIAS for one) enters the body
//     already holding the caller's value, so a body that assigns it only
//     conditionally — `IF cond THEN q := 'safe' END IF; EXECUTE q` — would execute
//     the caller's arbitrary q on the false path. Parameters are never DECLARE'd,
//     so a name that is not in scope at the EXECUTE is treated as unresolvable.
//   - A name declared only in a SIBLING or NESTED sub-block is a different variable
//     from an outer name of the same spelling; trusting it would let that block's
//     safe assignment vouch for an outer-scope parameter of the same name. Scope is
//     tracked with a stack the walker pushes on block entry and pops on exit, so a
//     name resolves only when an ENCLOSING block declares it.
//
// The assignment set itself stays flat (collected by name across the whole body).
// That is sound because a same-named variable in another scope can only ADD
// assignments to the set, and resolveExecuteVar requires EVERY assignment to be
// safe — extra ones can only cause a (sound) rejection, never vouch for an unsafe
// value. A DECLARE initializer (`DECLARE v text := <expr>`) is itself one of the
// values the variable can hold, so it is folded into the assignment set and
// analyzed like any `:=` — an initializer built from a parameter is rejected just
// as a body assignment from a parameter would be.
type varResolver struct {
	assigns   map[string][]*plpgsqlast.PLpgSQL_expr // simple `name := expr` right-hand sides
	tainted   map[string]bool                       // names written by a form we cannot reduce
	scopes    []map[string]bool                     // lexical scope stack of in-scope DECLARE'd locals
	resolving map[string]bool                       // names on the current resolution stack (cycle guard)
}

// collectVarAssignments walks the whole function body once, recording every
// simple `:=` assignment (and every DECLARE initializer) and tainting every
// variable written by a form whose value we cannot prove (INTO targets, loop
// variables, FETCH, GET DIAGNOSTICS, non-simple assignment targets, INOUT/OUT
// arguments of a CALL). Declaration NAMES are not recorded here — lexical scope is
// tracked by the walker instead (see bodyWalker.visit/leave). See varResolver for
// the soundness argument.
func collectVarAssignments(fn *plpgsqlast.PLpgSQL_function) *varResolver {
	r := &varResolver{
		assigns:   map[string][]*plpgsqlast.PLpgSQL_expr{},
		tainted:   map[string]bool{},
		resolving: map[string]bool{},
	}
	plpgsqlast.Rewrite(fn, func(cursor *plpgsqlast.Cursor) bool {
		switch n := cursor.Node().(type) {
		case *plpgsqlast.PLpgSQL_stmt_block:
			// A DECLARE initializer is one of the values the variable can hold, so
			// fold it into the assignment set like a body `:=`. Names are recorded
			// for every block regardless of nesting; the walker's scope stack is what
			// decides whether a name is in scope at a given EXECUTE.
			for _, d := range n.Decls {
				switch decl := d.(type) {
				case *plpgsqlast.PLpgSQL_var:
					r.recordDefault(decl.Refname, decl.DefaultVal)
				case *plpgsqlast.PLpgSQL_rec:
					r.recordDefault(decl.Refname, decl.DefaultVal)
				}
			}
		case *plpgsqlast.PLpgSQL_stmt_call:
			// CALL proc(v) can write an INOUT/OUT argument, so a variable passed as
			// a bare identifier argument may hold arbitrary text afterwards. We
			// cannot tell IN from INOUT/OUT without a catalog lookup, so we taint
			// every simple-identifier argument, fail-closed. Because taint is
			// flow-insensitive, a body that overwrites the variable on every path
			// AFTER the CALL and before the EXECUTE is conservatively rejected too;
			// proving that safe needs order-aware analysis we do not have yet.
			r.taintCallArgs(n.Expr)
		case *plpgsqlast.PLpgSQL_stmt_assign:
			if name, ok := simpleIdent(n.Target); ok {
				r.assigns[name] = append(r.assigns[name], n.Expr)
			} else {
				// A subscripted or field target (`arr[i] := …`, `rec.f := …`) is
				// not a plain scalar holding a query string; taint its base.
				r.taint(n.Target)
			}
		case *plpgsqlast.PLpgSQL_stmt_execsql:
			if n.Into {
				r.taintList(n.Target)
			}
		case *plpgsqlast.PLpgSQL_stmt_dynexecute:
			if n.Into {
				r.taintList(n.Target)
			}
		case *plpgsqlast.PLpgSQL_stmt_fetch:
			r.taintList(n.Target)
		case *plpgsqlast.PLpgSQL_stmt_fori:
			r.taint(n.Var)
		case *plpgsqlast.PLpgSQL_stmt_fors:
			r.taint(n.Var)
		case *plpgsqlast.PLpgSQL_stmt_dynfors:
			r.taint(n.Var)
		case *plpgsqlast.PLpgSQL_stmt_foreach_a:
			r.taint(n.Var)
		case *plpgsqlast.PLpgSQL_stmt_getdiag:
			for _, d := range n.DiagItems {
				r.taint(d.Target)
			}
		}
		return true
	}, nil)
	return r
}

// resolveExecuteVar checks a bare-variable EXECUTE payload by reducing every
// assignment to the named variable. Returns nil only when the variable is
// untainted, has at least one assignment, and all of them reduce to a safe
// skeleton (recursing through `w := v` chains, with a cycle guard). Any other
// case returns the same rejection an irreducible inline payload would.
func (r *varResolver) resolveExecuteVar(name string) error {
	reject := dynamicExecuteRejection()
	if !r.resolvable(name) {
		return reject
	}
	exprs, ok := r.assigns[name]
	if !ok || len(exprs) == 0 {
		return reject
	}
	if r.resolving[name] {
		return reject // self-referential build (e.g. v := v || …): cannot fix the structure
	}
	r.resolving[name] = true
	defer delete(r.resolving, name)
	for _, e := range exprs {
		if e == nil || strings.TrimSpace(e.Query) == "" {
			return reject
		}
		if err := analyzeDynamicExecute(e, r); err != nil {
			return err
		}
	}
	return nil
}

// taint marks name (reduced to its base identifier) as written by a form whose
// value we cannot prove safe. A blank or non-identifier name is ignored.
func (r *varResolver) taint(name string) {
	if base, ok := baseIdent(name); ok {
		r.tainted[base] = true
	}
}

// taintList taints each comma-separated name in an INTO/FETCH target list.
func (r *varResolver) taintList(list string) {
	for part := range strings.SplitSeq(list, ",") {
		r.taint(part)
	}
}

// pushScope enters a block, recording the plain scalar/record/row locals it
// declares as a new lexical scope. An ALIAS (a body-local name for a `$n`
// parameter) is intentionally not recorded: it resolves to a caller-supplied
// argument and must stay unresolvable like any bare parameter.
func (r *varResolver) pushScope(decls []plpgsqlast.Datum) {
	scope := map[string]bool{}
	for _, d := range decls {
		var name string
		switch decl := d.(type) {
		case *plpgsqlast.PLpgSQL_var:
			name = decl.Refname
		case *plpgsqlast.PLpgSQL_rec:
			name = decl.Refname
		case *plpgsqlast.PLpgSQL_row:
			name = decl.Refname
		}
		if name = strings.TrimSpace(name); name != "" {
			scope[name] = true
		}
	}
	r.scopes = append(r.scopes, scope)
}

// popScope leaves the innermost block.
func (r *varResolver) popScope() {
	if len(r.scopes) > 0 {
		r.scopes = r.scopes[:len(r.scopes)-1]
	}
}

// inScope reports whether name is DECLARE'd in the current block or any enclosing
// one — i.e. it is a local whose whole value history is visible at this point,
// not a function parameter or a variable from a sibling/nested scope. The stack
// only ever holds enclosing scopes, so any match means in scope (order does not
// matter for this membership check).
func (r *varResolver) inScope(name string) bool {
	for _, scope := range r.scopes {
		if scope[name] {
			return true
		}
	}
	return false
}

// recordDefault treats a DECLARE initializer (`DECLARE v text := <expr>`) as one
// of the values the variable can hold, exactly like a body `:=` assignment, so a
// variable initialized from an unprovable expression (e.g. a parameter) is not
// silently trusted when only a later conditional assignment looks safe.
func (r *varResolver) recordDefault(name string, def *plpgsqlast.PLpgSQL_expr) {
	if def == nil {
		return
	}
	if name = strings.TrimSpace(name); name != "" {
		r.assigns[name] = append(r.assigns[name], def)
	}
}

// resolvable reports whether a bare-variable EXECUTE payload named `name` may be
// resolved from its in-body assignments. It must be a local in scope at this point
// (so its whole value history is visible) and not tainted by an unprovable write.
func (r *varResolver) resolvable(name string) bool {
	return r.inScope(name) && !r.tainted[name]
}

// taintCallArgs parses a CALL/DO statement's text and taints every argument that
// is a bare identifier, since such an argument can be an INOUT/OUT target the
// callee rewrites. A DO block (no CallStmt) or an unparseable payload taints
// nothing here — the statement is still analyzed for policy by the body walker.
func (r *varResolver) taintCallArgs(e *plpgsqlast.PLpgSQL_expr) {
	if e == nil {
		return
	}
	stmts, err := parser.ParseSQL(e.Query)
	if err != nil {
		return
	}
	for _, st := range stmts {
		call, ok := st.(*ast.CallStmt)
		if !ok || call.Funccall == nil || call.Funccall.Args == nil {
			continue
		}
		for _, arg := range call.Funccall.Args.Items {
			if named, ok := arg.(*ast.NamedArgExpr); ok {
				arg = named.Arg
			}
			if ref, ok := arg.(*ast.ColumnRef); ok {
				if name, ok := columnRefName(ref); ok {
					r.tainted[name] = true
				}
			}
		}
	}
}

// columnRefName returns the lower-cased name of a single-identifier ColumnRef
// (a bare `v`), or ("", false) for anything qualified or non-trivial.
func columnRefName(ref *ast.ColumnRef) (string, bool) {
	if ref.Fields == nil || ref.Fields.Len() != 1 {
		return "", false
	}
	s, ok := ref.Fields.Items[0].(*ast.String)
	if !ok {
		return "", false
	}
	return s.SVal, true
}

// simpleIdent returns the name if s is a single unqualified, unsubscripted
// identifier (the only assignment-target shape we track as a clean scalar), else
// ("", false). The name is returned verbatim: the scanner has already applied
// PostgreSQL's identifier folding (unquoted → lower-cased, quoted → case
// preserved), so re-folding here would conflate a quoted `"Q"` with a `q`.
func simpleIdent(s string) (string, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		isFirst := i == 0
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c == '_':
		case !isFirst && (c >= '0' && c <= '9'):
		default:
			return "", false
		}
	}
	return s, true
}

// baseIdent returns the leading identifier of s (the base variable of a possibly
// qualified/subscripted target like `rec.f` or `arr[i]`), or ("", false) if s
// does not begin with an identifier character. As in simpleIdent, the name is
// already folded by the scanner and must not be re-folded.
func baseIdent(s string) (string, bool) {
	s = strings.TrimSpace(s)
	end := 0
	for end < len(s) {
		c := s[end]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_' ||
			(end > 0 && c >= '0' && c <= '9') {
			end++
			continue
		}
		break
	}
	if end == 0 {
		return "", false
	}
	return s[:end], true
}

// bareVarName returns the name of a bare-variable EXECUTE payload (a payload
// that is exactly one unqualified identifier, e.g. `v_query`), or ("", false).
func bareVarName(exprText string) (string, bool) {
	ref, ok := executeArgExpr(exprText).(*ast.ColumnRef)
	if !ok {
		return "", false
	}
	return columnRefName(ref)
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
	// resolver traces variable assignments so a bare-variable EXECUTE payload
	// can be checked against the expressions the variable can hold.
	resolver *varResolver
}

func (w *bodyWalker) visit(cursor *plpgsqlast.Cursor) bool {
	if w.err != nil {
		return false
	}
	switch n := cursor.Node().(type) {
	case *plpgsqlast.PLpgSQL_stmt_block:
		// Entering a block opens a new lexical scope; leave() pops it. Descend
		// generically so the block body is analyzed as usual.
		w.resolver.pushScope(n.Decls)
		return true
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

// leave is the post-order callback: it pops the lexical scope opened by a block in
// visit. Blocks always descend generically (visit returns true for them), so every
// pushed scope is popped here, keeping the stack balanced.
func (w *bodyWalker) leave(cursor *plpgsqlast.Cursor) bool {
	if _, ok := cursor.Node().(*plpgsqlast.PLpgSQL_stmt_block); ok {
		w.resolver.popScope()
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
	w.err = analyzeDynamicExecute(e, w.resolver)
}

// statements re-descends into a nested statement list (a dynamic FOR loop body)
// that the parent's `return false` excluded from the generic traversal. It passes
// w.leave so any blocks in that list push and pop scopes in balance, just as the
// top-level traversal does.
func (w *bodyWalker) statements(list []plpgsqlast.Stmt) {
	for _, st := range list {
		if w.err != nil {
			return
		}
		plpgsqlast.Rewrite(st, w.visit, w.leave)
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
func analyzeDynamicExecute(e *plpgsqlast.PLpgSQL_expr, res *varResolver) error {
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
	// A top-level format() whose %s holes are fed by constrained constants: the
	// strict path above rejects %s, but here we enumerate the finite set of
	// statements it can produce and analyze each. See analyzeConstrainedFormatExecute.
	if err, handled := analyzeConstrainedFormatExecute(e.Query, res); handled {
		return err
	}
	// The payload is a bare variable (`EXECUTE v`): if we can prove every value
	// v can hold is itself a safe skeleton, the EXECUTE is safe. See varResolver.
	if res != nil {
		if name, ok := bareVarName(e.Query); ok {
			return res.resolveExecuteVar(name)
		}
	}
	return dynamicExecuteRejection()
}

// dynamicExecuteRejection is the rejection for an EXECUTE payload whose
// statement structure cannot be proven constant.
func dynamicExecuteRejection() error {
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

// formatArgPosition parses an optional `n$` positional prefix of a format
// conversion starting at f[i] (i points just past the '%'). It returns the index
// past the prefix, the 1-based argument number n, and whether a prefix was
// present. In the strict skeleton n is unused (%I/%L quote whichever argument
// they name); the constrained-%s path uses it to pick the referenced argument.
func formatArgPosition(f string, i int) (next, n int, ok bool) {
	j := i
	for j < len(f) && f[j] >= '0' && f[j] <= '9' {
		n = n*10 + int(f[j]-'0')
		j++
	}
	if j > i && j < len(f) && f[j] == '$' {
		return j + 1, n, true
	}
	return i, 0, false
}

// expandFormatSkeleton writes format()'s constant skeleton to sb, replacing each
// conversion: %% -> %, %I -> a placeholder identifier, %L -> a placeholder
// literal, including the positional forms %n$I / %n$L. Any other conversion —
// notably %s (raw text substitution) and width specs — makes the structure
// non-constant, so it returns false.
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
		if ni, _, ok := formatArgPosition(f, i); ok {
			i = ni
			if i >= len(f) {
				return false
			}
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

// maxFormatVariants bounds the constrained-%s enumeration so a body with many
// %s holes cannot blow up the analysis. Beyond it we fall through to rejection.
const maxFormatVariants = 64

// analyzeConstrainedFormatExecute handles an EXECUTE payload that is a single
// top-level format() call whose %s conversions are fed by constrained constants
// (a string literal, a CASE whose results are all string literals, or a variable
// proven to hold only such values). Unlike the strict reduceSafeExpr path — which
// rejects %s outright because %s substitutes raw text — this expands the finite
// set of concrete statements the format can produce and analyzes each as a static
// statement, so an injected constant is still caught by re-analysis. It is scoped
// to a top-level format() (not one nested in a `||` chain) to keep the fixed
// skeleton single-valued outside the enumerated holes.
//
// Returns (err, true) when it owns the decision (the payload is such a format()
// with all %s constrained); (nil, false) to let the caller fall through to the
// normal rejection. res may be nil, in which case only literal/CASE-of-literal
// %s args qualify (no variable resolution).
//
// Like reduceSafeExpr, the format() call is matched by unqualified name, so the
// same KNOWN LIMITATION applies: a tenant that shadows pg_catalog.format (via
// search_path or an exact-signature overload) makes the plan-time expansion of
// the concrete statements meaningless. Closing it needs the catalog/search_path
// resolution the analyzer does not have. Accepted as-is; see reduceSafeExpr.
func analyzeConstrainedFormatExecute(exprText string, res *varResolver) (error, bool) {
	fc, ok := executeArgExpr(exprText).(*ast.FuncCall)
	if !ok || bareFuncName(fc.Funcname) != "format" {
		return nil, false
	}
	variants, values, ok := reduceConstrainedFormat(fc, res)
	if !ok {
		return nil, false
	}
	// Every concrete statement the format can produce must pass the body policy.
	for _, skel := range variants {
		if err := analyzeDynamicStatementText(skel); err != nil {
			return err, true
		}
	}
	// The interpolated %I/%L values must themselves be call-safe (same as the
	// strict skeleton path): a value expression must not reach a blocklisted
	// function or change session state when evaluated.
	for _, v := range values {
		if err := analyzeDynamicStatementText("SELECT " + v.SqlString()); err != nil {
			return err, true
		}
	}
	return nil, true
}

// reduceConstrainedFormat expands a format() call into the finite set of concrete
// statement skeletons it can produce. %I/%L become the usual placeholders; %s
// becomes each constant value its argument can hold (constStringValues). Returns
// the enumerated skeletons, the interpolated value expressions (for call-safety
// analysis), and ok=false if the format string is non-constant, uses an
// unsupported conversion/positional spec, consumes more arguments than supplied,
// or has a %s whose value set is not a bounded constant set.
func reduceConstrainedFormat(fc *ast.FuncCall, res *varResolver) (variants []string, values []ast.Node, ok bool) {
	if fc.Args == nil || fc.Args.Len() < 1 {
		return nil, nil, false
	}
	fmtConst, isConst := fc.Args.Items[0].(*ast.A_Const)
	if !isConst || fmtConst.Isnull {
		return nil, nil, false
	}
	fmtStr, isStr := fmtConst.Val.(*ast.String)
	if !isStr {
		return nil, nil, false
	}

	f := fmtStr.SVal
	// prefix accumulates fixed text and %I/%L placeholders; each %s closes the
	// current prefix, records the hole's value set, and starts a new prefix.
	var prefix strings.Builder
	var literalChunks []string // text before each %s hole, then a trailing chunk
	var svalSets [][]string    // constant value sets, one per %s hole
	argIdx := 1
	combos := 1
	for i := 0; i < len(f); i++ {
		if f[i] != '%' {
			prefix.WriteByte(f[i])
			continue
		}
		i++
		if i >= len(f) {
			return nil, nil, false
		}
		// A conversion may name its argument positionally (%n$I). When it does, the
		// referenced argument is Items[n] rather than the next sequential one, and
		// the sequential counter is not advanced (PostgreSQL forbids mixing the two
		// styles). Otherwise the argument is the next sequential one.
		arg := argIdx
		positional := false
		if ni, n, ok := formatArgPosition(f, i); ok {
			i = ni
			if i >= len(f) {
				return nil, nil, false
			}
			arg = n
			positional = true
		}
		if f[i] != '%' && (arg < 1 || arg >= fc.Args.Len()) {
			return nil, nil, false
		}
		switch f[i] {
		case '%':
			prefix.WriteByte('%')
		case 'I':
			prefix.WriteString(dynSkeletonIdent)
			if !positional {
				argIdx++
			}
		case 'L':
			prefix.WriteString(dynSkeletonLiteral)
			if !positional {
				argIdx++
			}
		case 's':
			vals, constrained := constStringValues(fc.Args.Items[arg], res, map[string]bool{})
			if !positional {
				argIdx++
			}
			if !constrained || len(vals) == 0 {
				return nil, nil, false
			}
			combos *= len(vals)
			if combos > maxFormatVariants {
				return nil, nil, false
			}
			literalChunks = append(literalChunks, prefix.String())
			prefix.Reset()
			svalSets = append(svalSets, vals)
		default:
			// Width or any other conversion we do not model.
			return nil, nil, false
		}
	}
	literalChunks = append(literalChunks, prefix.String())

	variants = enumerateFormatVariants(literalChunks, svalSets)
	// Collect every argument as a value for call-safety, matching reduceSafeFormat.
	for i := 1; i < fc.Args.Len(); i++ {
		values = append(values, fc.Args.Items[i])
	}
	return variants, values, true
}

// enumerateFormatVariants builds every concrete skeleton from the fixed literal
// chunks (len == len(svalSets)+1) interleaved with one value drawn from each
// %s hole's constant set — the Cartesian product across holes.
func enumerateFormatVariants(literalChunks []string, svalSets [][]string) []string {
	variants := []string{literalChunks[0]}
	for hole, set := range svalSets {
		next := make([]string, 0, len(variants)*len(set))
		for _, base := range variants {
			for _, val := range set {
				next = append(next, base+val+literalChunks[hole+1])
			}
		}
		variants = next
	}
	return variants
}

// constStringValues returns the finite set of constant string values a format
// %s argument can take, or ok=false if that set cannot be bounded. It accepts a
// string literal, a CASE whose every branch (including a required ELSE) is such
// a value, and — when res is non-null — a variable proven untainted and assigned
// only such values. visited guards against assignment cycles. Because callers
// substitute each returned value and re-analyze the result, a hostile constant
// is still caught; the only requirement here is that the set be complete.
func constStringValues(node ast.Node, res *varResolver, visited map[string]bool) ([]string, bool) {
	switch n := unwrapTypeCast(node).(type) {
	case *ast.A_Const:
		if n.Isnull {
			return nil, false
		}
		if s, ok := n.Val.(*ast.String); ok {
			return []string{s.SVal}, true
		}
		return nil, false
	case *ast.CaseExpr:
		// Require an ELSE so the value set is total (no implicit NULL branch).
		if n.Defresult == nil || n.Args == nil {
			return nil, false
		}
		var out []string
		for _, item := range n.Args.Items {
			when, ok := item.(*ast.CaseWhen)
			if !ok {
				return nil, false
			}
			vals, ok := constStringValues(when.Result, res, visited)
			if !ok {
				return nil, false
			}
			out = append(out, vals...)
		}
		vals, ok := constStringValues(n.Defresult, res, visited)
		if !ok {
			return nil, false
		}
		return append(out, vals...), true
	case *ast.ColumnRef:
		if res == nil || n.Fields == nil || n.Fields.Len() != 1 {
			return nil, false
		}
		s, ok := n.Fields.Items[0].(*ast.String)
		if !ok {
			return nil, false
		}
		name := s.SVal
		if !res.resolvable(name) || visited[name] {
			return nil, false
		}
		exprs, ok := res.assigns[name]
		if !ok || len(exprs) == 0 {
			return nil, false
		}
		visited[name] = true
		defer delete(visited, name)
		var out []string
		for _, e := range exprs {
			if e == nil {
				return nil, false
			}
			target := executeArgExpr(e.Query)
			if target == nil {
				return nil, false
			}
			vals, ok := constStringValues(target, res, visited)
			if !ok {
				return nil, false
			}
			out = append(out, vals...)
		}
		return out, true
	}
	return nil, false
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

// bareFuncName returns a FuncCall's unqualified name, or "" if the name is
// schema-qualified (not a trusted pg_catalog builtin for our purposes). The name
// is returned as the parser folded it — unquoted `FORMAT` is already "format",
// while quoted `"FORMAT"` stays "FORMAT" and so will NOT match the trusted
// lower-case builtin names, which is correct: `"FORMAT"` is a different function.
func bareFuncName(nl *ast.NodeList) string {
	if nl == nil || nl.Len() != 1 {
		return ""
	}
	s, ok := nl.Items[0].(*ast.String)
	if !ok {
		return ""
	}
	return s.SVal
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
