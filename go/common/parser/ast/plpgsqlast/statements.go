// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plpgsqlast

import (
	"strconv"
	"strings"

	"github.com/multigres/multigres/go/common/parser/ast"
)

// Stmt is implemented by every PL/pgSQL statement node. It is the Go analogue
// of PG's PLpgSQL_stmt supertype (plpgsql.h): the common type for everything
// that can appear in a statement list (a block body, an IF branch, a loop
// body, …). The unexported marker keeps the set of statements closed to this
// package.
type Stmt interface {
	Node
	isStmt()
}

// PLpgSQL_stmt_block is a BEGIN … END block. Ported from
// postgres/src/pl/plpgsql/src/plpgsql.h (PLpgSQL_stmt_block). PG's execution
// bookkeeping (stmtid, n_initvars, initvarnos) is dropped; cmd_type and lineno
// are carried by BaseNode.
type PLpgSQL_stmt_block struct {
	BaseNode
	Label      string                   `json:"label,omitempty"`      // optional block label
	Decls      []Datum                  `json:"decls,omitempty"`      // DECLARE-section variables
	Body       []Stmt                   `json:"body,omitempty"`       // statements between BEGIN and END
	Exceptions *PLpgSQL_exception_block `json:"exceptions,omitempty"` // EXCEPTION section, or nil
}

func (b *PLpgSQL_stmt_block) isStmt() {}

func (b *PLpgSQL_stmt_block) String() string {
	return "PLpgSQL_stmt_block"
}

// deparseBody writes a statement list the way a block body is rendered: each
// statement's deparse followed by ";\n". Shared by every node that holds a
// statement list (blocks, IF branches, loop bodies) so they round-trip
// identically.
func deparseBody(sb *strings.Builder, body []Stmt) {
	for _, s := range body {
		sb.WriteString(s.SqlString())
		sb.WriteString(";\n")
	}
}

// SqlString deparses the block. Declaration and EXCEPTION rendering are added
// as those chunks land; for now it emits the label, BEGIN, the body, and END.
func (b *PLpgSQL_stmt_block) SqlString() string {
	var sb strings.Builder
	if b.Label != "" {
		sb.WriteString("<<")
		sb.WriteString(b.Label)
		sb.WriteString(">> ")
	}
	if len(b.Decls) > 0 {
		sb.WriteString("DECLARE\n")
		for _, d := range b.Decls {
			sb.WriteString(d.SqlString())
			sb.WriteString("\n")
		}
	}
	sb.WriteString("BEGIN\n")
	deparseBody(&sb, b.Body)
	sb.WriteString("END")
	if b.Label != "" {
		sb.WriteString(" ")
		sb.WriteString(b.Label)
	}
	return sb.String()
}

func NewPLpgSQL_stmt_block() *PLpgSQL_stmt_block {
	return &PLpgSQL_stmt_block{
		BaseNode: BaseNode{Tag: T_PLpgSQL_stmt_block, Loc: -1},
	}
}

// PLpgSQL_stmt_assign is an assignment statement (`target := expr`). Ported from
// plpgsql.h (PLpgSQL_stmt_assign). PG stores the resolved variable (varno) and
// the whole `target := value` as one ASSIGN-mode expr; for the parse-level port
// (no variable resolution) we keep the target as written and the right-hand
// side as its own expression.
type PLpgSQL_stmt_assign struct {
	BaseNode
	Target string        `json:"target,omitempty"` // assignment target (variable name, as written)
	Expr   *PLpgSQL_expr `json:"expr,omitempty"`   // right-hand side expression
}

func (a *PLpgSQL_stmt_assign) isStmt() {}

func (a *PLpgSQL_stmt_assign) String() string { return "PLpgSQL_stmt_assign" }

func (a *PLpgSQL_stmt_assign) SqlString() string {
	rhs := ""
	if a.Expr != nil {
		rhs = a.Expr.SqlString()
	}
	return a.Target + " := " + rhs
}

func NewPLpgSQL_stmt_assign(target string) *PLpgSQL_stmt_assign {
	return &PLpgSQL_stmt_assign{
		BaseNode: BaseNode{Tag: T_PLpgSQL_stmt_assign, Loc: -1},
		Target:   target,
	}
}

// PLpgSQL_stmt_if is an IF statement (PG's PLpgSQL_stmt_if, plpgsql.h):
// `IF cond THEN … [ELSIF cond THEN …] [ELSE …] END IF`. The ELSIF arms are
// PLpgSQL_if_elsif helper nodes; ElseBody is nil when there is no ELSE.
type PLpgSQL_stmt_if struct {
	BaseNode
	Cond      *PLpgSQL_expr       `json:"cond,omitempty"`       // boolean expression for THEN
	ThenBody  []Stmt              `json:"then_body,omitempty"`  // statements in the THEN branch
	ElsifList []*PLpgSQL_if_elsif `json:"elsif_list,omitempty"` // ELSIF arms, in order
	ElseBody  []Stmt              `json:"else_body,omitempty"`  // statements in the ELSE branch, or nil
}

func (s *PLpgSQL_stmt_if) isStmt() {}

func (s *PLpgSQL_stmt_if) String() string { return "PLpgSQL_stmt_if" }

func (s *PLpgSQL_stmt_if) SqlString() string {
	var sb strings.Builder
	sb.WriteString("IF ")
	sb.WriteString(s.Cond.SqlString())
	sb.WriteString(" THEN\n")
	deparseBody(&sb, s.ThenBody)
	for _, ei := range s.ElsifList {
		sb.WriteString(ei.SqlString())
	}
	if s.ElseBody != nil {
		sb.WriteString("ELSE\n")
		deparseBody(&sb, s.ElseBody)
	}
	sb.WriteString("END IF")
	return sb.String()
}

func NewPLpgSQL_stmt_if() *PLpgSQL_stmt_if {
	return &PLpgSQL_stmt_if{
		BaseNode: BaseNode{Tag: T_PLpgSQL_stmt_if, Loc: -1},
	}
}

// PLpgSQL_if_elsif is one ELSIF arm of an IF statement (PG's PLpgSQL_if_elsif).
// PG keeps it as a plain helper struct, not a PLpgSQL_stmt, so this implements
// Node but not Stmt. Its deparse includes the leading `ELSIF … THEN` so the
// enclosing IF can render the arms by concatenation.
type PLpgSQL_if_elsif struct {
	BaseNode
	Cond  *PLpgSQL_expr `json:"cond,omitempty"`  // boolean expression for this arm
	Stmts []Stmt        `json:"stmts,omitempty"` // statements run when Cond holds
}

func (e *PLpgSQL_if_elsif) String() string { return "PLpgSQL_if_elsif" }

func (e *PLpgSQL_if_elsif) SqlString() string {
	var sb strings.Builder
	sb.WriteString("ELSIF ")
	sb.WriteString(e.Cond.SqlString())
	sb.WriteString(" THEN\n")
	deparseBody(&sb, e.Stmts)
	return sb.String()
}

func NewPLpgSQL_if_elsif() *PLpgSQL_if_elsif {
	return &PLpgSQL_if_elsif{
		BaseNode: BaseNode{Tag: T_PLpgSQL_if_elsif, Loc: -1},
	}
}

// PLpgSQL_stmt_loop is an unconditional `LOOP … END LOOP` (PG's
// PLpgSQL_stmt_loop). Label is the optional block label; it is echoed after
// END LOOP on deparse so the result re-parses (checkLabels requires a match).
type PLpgSQL_stmt_loop struct {
	BaseNode
	Label string `json:"label,omitempty"` // optional loop label
	Body  []Stmt `json:"body,omitempty"`  // statements between LOOP and END LOOP
}

func (s *PLpgSQL_stmt_loop) isStmt() {}

func (s *PLpgSQL_stmt_loop) String() string { return "PLpgSQL_stmt_loop" }

func (s *PLpgSQL_stmt_loop) SqlString() string {
	var sb strings.Builder
	writeLoopLabel(&sb, s.Label)
	sb.WriteString("LOOP\n")
	deparseBody(&sb, s.Body)
	writeLoopEnd(&sb, s.Label)
	return sb.String()
}

func NewPLpgSQL_stmt_loop() *PLpgSQL_stmt_loop {
	return &PLpgSQL_stmt_loop{
		BaseNode: BaseNode{Tag: T_PLpgSQL_stmt_loop, Loc: -1},
	}
}

// PLpgSQL_stmt_while is a `WHILE cond LOOP … END LOOP` (PG's PLpgSQL_stmt_while).
type PLpgSQL_stmt_while struct {
	BaseNode
	Label string        `json:"label,omitempty"` // optional loop label
	Cond  *PLpgSQL_expr `json:"cond,omitempty"`  // loop-continuation condition
	Body  []Stmt        `json:"body,omitempty"`  // statements between LOOP and END LOOP
}

func (s *PLpgSQL_stmt_while) isStmt() {}

func (s *PLpgSQL_stmt_while) String() string { return "PLpgSQL_stmt_while" }

func (s *PLpgSQL_stmt_while) SqlString() string {
	var sb strings.Builder
	writeLoopLabel(&sb, s.Label)
	sb.WriteString("WHILE ")
	sb.WriteString(s.Cond.SqlString())
	sb.WriteString(" LOOP\n")
	deparseBody(&sb, s.Body)
	writeLoopEnd(&sb, s.Label)
	return sb.String()
}

func NewPLpgSQL_stmt_while() *PLpgSQL_stmt_while {
	return &PLpgSQL_stmt_while{
		BaseNode: BaseNode{Tag: T_PLpgSQL_stmt_while, Loc: -1},
	}
}

// writeLoopLabel writes a `<<label>> ` prefix when label is non-empty.
func writeLoopLabel(sb *strings.Builder, label string) {
	if label != "" {
		sb.WriteString("<<")
		sb.WriteString(label)
		sb.WriteString(">> ")
	}
}

// writeLoopEnd writes `END LOOP` plus the echoed label when present.
func writeLoopEnd(sb *strings.Builder, label string) {
	sb.WriteString("END LOOP")
	if label != "" {
		sb.WriteString(" ")
		sb.WriteString(label)
	}
}

// PLpgSQL_stmt_exit is an EXIT or CONTINUE (PG's PLpgSQL_stmt_exit). IsExit
// distinguishes the two. Label and Cond (the optional WHEN expression) are both
// optional. PG's namespace-based validation (label exists, CONTINUE forbids
// block labels, EXIT/CONTINUE must be inside a loop) is dropped: we have no
// namespace, so we capture the statement without checking it.
type PLpgSQL_stmt_exit struct {
	BaseNode
	IsExit bool          `json:"is_exit,omitempty"` // true for EXIT, false for CONTINUE
	Label  string        `json:"label,omitempty"`   // optional target label
	Cond   *PLpgSQL_expr `json:"cond,omitempty"`    // optional WHEN condition
}

func (s *PLpgSQL_stmt_exit) isStmt() {}

func (s *PLpgSQL_stmt_exit) String() string { return "PLpgSQL_stmt_exit" }

func (s *PLpgSQL_stmt_exit) SqlString() string {
	var sb strings.Builder
	if s.IsExit {
		sb.WriteString("EXIT")
	} else {
		sb.WriteString("CONTINUE")
	}
	if s.Label != "" {
		sb.WriteString(" ")
		sb.WriteString(s.Label)
	}
	if s.Cond != nil {
		sb.WriteString(" WHEN ")
		sb.WriteString(s.Cond.SqlString())
	}
	return sb.String()
}

func NewPLpgSQL_stmt_exit(isExit bool) *PLpgSQL_stmt_exit {
	return &PLpgSQL_stmt_exit{
		BaseNode: BaseNode{Tag: T_PLpgSQL_stmt_exit, Loc: -1},
		IsExit:   isExit,
	}
}

// PLpgSQL_stmt_fori is an integer FOR loop (PG's PLpgSQL_stmt_fori):
// `FOR var IN [REVERSE] lower .. upper [BY step] LOOP … END LOOP`. The bounds
// are captured as expressions. Drops PG's resolved loop `var` datum — we keep
// the target name as written.
type PLpgSQL_stmt_fori struct {
	BaseNode
	Label   string        `json:"label,omitempty"`   // optional loop label
	Var     string        `json:"var,omitempty"`     // loop variable name, as written
	Lower   *PLpgSQL_expr `json:"lower,omitempty"`   // lower bound
	Upper   *PLpgSQL_expr `json:"upper,omitempty"`   // upper bound
	Step    *PLpgSQL_expr `json:"step,omitempty"`    // BY step, or nil for the default (1)
	Reverse bool          `json:"reverse,omitempty"` // counts down when true
	Body    []Stmt        `json:"body,omitempty"`    // loop body
}

func (s *PLpgSQL_stmt_fori) isStmt() {}

func (s *PLpgSQL_stmt_fori) String() string { return "PLpgSQL_stmt_fori" }

func (s *PLpgSQL_stmt_fori) SqlString() string {
	var sb strings.Builder
	writeLoopLabel(&sb, s.Label)
	sb.WriteString("FOR ")
	sb.WriteString(s.Var)
	sb.WriteString(" IN ")
	if s.Reverse {
		sb.WriteString("REVERSE ")
	}
	sb.WriteString(s.Lower.SqlString())
	sb.WriteString(" .. ")
	sb.WriteString(s.Upper.SqlString())
	if s.Step != nil {
		sb.WriteString(" BY ")
		sb.WriteString(s.Step.SqlString())
	}
	sb.WriteString(" LOOP\n")
	deparseBody(&sb, s.Body)
	writeLoopEnd(&sb, s.Label)
	return sb.String()
}

func NewPLpgSQL_stmt_fori() *PLpgSQL_stmt_fori {
	return &PLpgSQL_stmt_fori{
		BaseNode: BaseNode{Tag: T_PLpgSQL_stmt_fori, Loc: -1},
	}
}

// PLpgSQL_stmt_fors is a query FOR loop (PG's PLpgSQL_stmt_fors):
// `FOR var IN query LOOP … END LOOP`. Without variable resolution we cannot
// distinguish a bound-cursor FOR loop from a query FOR loop, so cursor FOR loops
// are also represented here, with the cursor name captured as the query text.
type PLpgSQL_stmt_fors struct {
	BaseNode
	Label string        `json:"label,omitempty"` // optional loop label
	Var   string        `json:"var,omitempty"`   // loop target name, as written
	Query *PLpgSQL_expr `json:"query,omitempty"` // the query iterated over
	Body  []Stmt        `json:"body,omitempty"`  // loop body
}

func (s *PLpgSQL_stmt_fors) isStmt() {}

func (s *PLpgSQL_stmt_fors) String() string { return "PLpgSQL_stmt_fors" }

func (s *PLpgSQL_stmt_fors) SqlString() string {
	var sb strings.Builder
	writeLoopLabel(&sb, s.Label)
	sb.WriteString("FOR ")
	sb.WriteString(s.Var)
	sb.WriteString(" IN ")
	sb.WriteString(s.Query.SqlString())
	sb.WriteString(" LOOP\n")
	deparseBody(&sb, s.Body)
	writeLoopEnd(&sb, s.Label)
	return sb.String()
}

func NewPLpgSQL_stmt_fors() *PLpgSQL_stmt_fors {
	return &PLpgSQL_stmt_fors{
		BaseNode: BaseNode{Tag: T_PLpgSQL_stmt_fors, Loc: -1},
	}
}

// PLpgSQL_stmt_foreach_a is a FOREACH-over-array loop (PG's
// PLpgSQL_stmt_foreach_a): `FOREACH var [SLICE n] IN ARRAY expr LOOP … END LOOP`.
// Drops PG's resolved `varno` — we keep the target name as written.
type PLpgSQL_stmt_foreach_a struct {
	BaseNode
	Label string        `json:"label,omitempty"` // optional loop label
	Var   string        `json:"var,omitempty"`   // loop variable name, as written
	Slice int           `json:"slice,omitempty"` // SLICE dimension, or 0 for none
	Expr  *PLpgSQL_expr `json:"expr,omitempty"`  // the array expression
	Body  []Stmt        `json:"body,omitempty"`  // loop body
}

func (s *PLpgSQL_stmt_foreach_a) isStmt() {}

func (s *PLpgSQL_stmt_foreach_a) String() string { return "PLpgSQL_stmt_foreach_a" }

func (s *PLpgSQL_stmt_foreach_a) SqlString() string {
	var sb strings.Builder
	writeLoopLabel(&sb, s.Label)
	sb.WriteString("FOREACH ")
	sb.WriteString(s.Var)
	if s.Slice > 0 {
		sb.WriteString(" SLICE ")
		sb.WriteString(strconv.Itoa(s.Slice))
	}
	sb.WriteString(" IN ARRAY ")
	sb.WriteString(s.Expr.SqlString())
	sb.WriteString(" LOOP\n")
	deparseBody(&sb, s.Body)
	writeLoopEnd(&sb, s.Label)
	return sb.String()
}

func NewPLpgSQL_stmt_foreach_a() *PLpgSQL_stmt_foreach_a {
	return &PLpgSQL_stmt_foreach_a{
		BaseNode: BaseNode{Tag: T_PLpgSQL_stmt_foreach_a, Loc: -1},
	}
}

// PLpgSQL_stmt_case is a CASE statement (PG's PLpgSQL_stmt_case): either simple
// (`CASE expr WHEN val THEN …`) or searched (`CASE WHEN cond THEN …`), with an
// optional ELSE. TestExpr is nil for the searched form. Drops PG's resolved
// `t_varno`; an empty ELSE collapses to no ELSE (ElseStmts nil), as for IF.
type PLpgSQL_stmt_case struct {
	BaseNode
	TestExpr  *PLpgSQL_expr        `json:"test_expr,omitempty"`  // simple-CASE test expression, or nil
	WhenList  []*PLpgSQL_case_when `json:"when_list,omitempty"`  // WHEN arms, in order
	ElseStmts []Stmt               `json:"else_stmts,omitempty"` // ELSE branch, or nil
}

func (s *PLpgSQL_stmt_case) isStmt() {}

func (s *PLpgSQL_stmt_case) String() string { return "PLpgSQL_stmt_case" }

func (s *PLpgSQL_stmt_case) SqlString() string {
	var sb strings.Builder
	sb.WriteString("CASE")
	if s.TestExpr != nil {
		sb.WriteString(" ")
		sb.WriteString(s.TestExpr.SqlString())
	}
	sb.WriteString("\n")
	for _, cw := range s.WhenList {
		sb.WriteString(cw.SqlString())
	}
	if s.ElseStmts != nil {
		sb.WriteString("ELSE\n")
		deparseBody(&sb, s.ElseStmts)
	}
	sb.WriteString("END CASE")
	return sb.String()
}

func NewPLpgSQL_stmt_case() *PLpgSQL_stmt_case {
	return &PLpgSQL_stmt_case{
		BaseNode: BaseNode{Tag: T_PLpgSQL_stmt_case, Loc: -1},
	}
}

// PLpgSQL_case_when is one WHEN arm of a CASE statement (PG's PLpgSQL_case_when).
// Helper node (Node, not Stmt), like PLpgSQL_if_elsif; its deparse includes the
// leading `WHEN … THEN` so the enclosing CASE renders the arms by concatenation.
type PLpgSQL_case_when struct {
	BaseNode
	Expr  *PLpgSQL_expr `json:"expr,omitempty"`  // WHEN expression
	Stmts []Stmt        `json:"stmts,omitempty"` // statements run when Expr matches
}

func (w *PLpgSQL_case_when) String() string { return "PLpgSQL_case_when" }

func (w *PLpgSQL_case_when) SqlString() string {
	var sb strings.Builder
	sb.WriteString("WHEN ")
	sb.WriteString(w.Expr.SqlString())
	sb.WriteString(" THEN\n")
	deparseBody(&sb, w.Stmts)
	return sb.String()
}

func NewPLpgSQL_case_when() *PLpgSQL_case_when {
	return &PLpgSQL_case_when{
		BaseNode: BaseNode{Tag: T_PLpgSQL_case_when, Loc: -1},
	}
}

// PLpgSQL_stmt_execsql is an embedded SQL statement — any statement not handled
// by a dedicated PL/pgSQL production (SELECT, INSERT, UPDATE, DELETE, a bare
// function call, …), captured verbatim. Ported from PG's PLpgSQL_stmt_execsql;
// PG's INTO-target extraction and mod_stmt flag are dropped (both need
// resolution / aren't needed for a parse-only port).
type PLpgSQL_stmt_execsql struct {
	BaseNode
	Sqlstmt *PLpgSQL_expr `json:"sqlstmt,omitempty"` // the statement text
}

func (s *PLpgSQL_stmt_execsql) isStmt() {}

func (s *PLpgSQL_stmt_execsql) String() string { return "PLpgSQL_stmt_execsql" }

func (s *PLpgSQL_stmt_execsql) SqlString() string { return s.Sqlstmt.SqlString() }

func NewPLpgSQL_stmt_execsql() *PLpgSQL_stmt_execsql {
	return &PLpgSQL_stmt_execsql{
		BaseNode: BaseNode{Tag: T_PLpgSQL_stmt_execsql, Loc: -1},
	}
}

// PLpgSQL_stmt_perform is `PERFORM expr` (PG's PLpgSQL_stmt_perform): run a query
// and discard its result. Expr is the expression after PERFORM; PG rewrites the
// leading keyword to SELECT for execution, which is deferred (we keep the
// expression and re-emit PERFORM on deparse).
type PLpgSQL_stmt_perform struct {
	BaseNode
	Expr *PLpgSQL_expr `json:"expr,omitempty"` // the expression to evaluate
}

func (s *PLpgSQL_stmt_perform) isStmt() {}

func (s *PLpgSQL_stmt_perform) String() string { return "PLpgSQL_stmt_perform" }

func (s *PLpgSQL_stmt_perform) SqlString() string { return "PERFORM " + s.Expr.SqlString() }

func NewPLpgSQL_stmt_perform() *PLpgSQL_stmt_perform {
	return &PLpgSQL_stmt_perform{
		BaseNode: BaseNode{Tag: T_PLpgSQL_stmt_perform, Loc: -1},
	}
}

// PLpgSQL_stmt_call is `CALL proc(...)` or `DO $$…$$` (PG's PLpgSQL_stmt_call;
// PG uses one struct for both). Expr is the whole statement text, including the
// CALL/DO keyword. IsCall distinguishes the two.
type PLpgSQL_stmt_call struct {
	BaseNode
	Expr   *PLpgSQL_expr `json:"expr,omitempty"`    // the CALL/DO statement text
	IsCall bool          `json:"is_call,omitempty"` // true for CALL, false for DO
}

func (s *PLpgSQL_stmt_call) isStmt() {}

func (s *PLpgSQL_stmt_call) String() string { return "PLpgSQL_stmt_call" }

func (s *PLpgSQL_stmt_call) SqlString() string { return s.Expr.SqlString() }

func NewPLpgSQL_stmt_call(isCall bool) *PLpgSQL_stmt_call {
	return &PLpgSQL_stmt_call{
		BaseNode: BaseNode{Tag: T_PLpgSQL_stmt_call, Loc: -1},
		IsCall:   isCall,
	}
}

// PLpgSQL_stmt_return is `RETURN [expr]` (PG's PLpgSQL_stmt_return). Expr is nil
// for a bare RETURN. Drops PG's resolved retvarno.
type PLpgSQL_stmt_return struct {
	BaseNode
	Expr *PLpgSQL_expr `json:"expr,omitempty"` // the returned expression, or nil
}

func (s *PLpgSQL_stmt_return) isStmt() {}

func (s *PLpgSQL_stmt_return) String() string { return "PLpgSQL_stmt_return" }

func (s *PLpgSQL_stmt_return) SqlString() string {
	if s.Expr == nil {
		return "RETURN"
	}
	return "RETURN " + s.Expr.SqlString()
}

func NewPLpgSQL_stmt_return() *PLpgSQL_stmt_return {
	return &PLpgSQL_stmt_return{
		BaseNode: BaseNode{Tag: T_PLpgSQL_stmt_return, Loc: -1},
	}
}

// PLpgSQL_stmt_return_next is `RETURN NEXT expr` (PG's PLpgSQL_stmt_return_next):
// append a row to the result set of a set-returning function.
type PLpgSQL_stmt_return_next struct {
	BaseNode
	Expr *PLpgSQL_expr `json:"expr,omitempty"` // the row expression
}

func (s *PLpgSQL_stmt_return_next) isStmt() {}

func (s *PLpgSQL_stmt_return_next) String() string { return "PLpgSQL_stmt_return_next" }

func (s *PLpgSQL_stmt_return_next) SqlString() string {
	return "RETURN NEXT " + s.Expr.SqlString()
}

func NewPLpgSQL_stmt_return_next() *PLpgSQL_stmt_return_next {
	return &PLpgSQL_stmt_return_next{
		BaseNode: BaseNode{Tag: T_PLpgSQL_stmt_return_next, Loc: -1},
	}
}

// PLpgSQL_stmt_return_query is `RETURN QUERY query` (PG's
// PLpgSQL_stmt_return_query): append a whole query's rows to the result set.
// Either Query (static) or DynQuery (the `RETURN QUERY EXECUTE expr` form, with
// optional USING Params) is set.
type PLpgSQL_stmt_return_query struct {
	BaseNode
	Query    *PLpgSQL_expr   `json:"query,omitempty"`    // static query, or nil
	DynQuery *PLpgSQL_expr   `json:"dynquery,omitempty"` // EXECUTE query string, or nil
	Params   []*PLpgSQL_expr `json:"params,omitempty"`   // USING expressions (dynamic form)
}

func (s *PLpgSQL_stmt_return_query) isStmt() {}

func (s *PLpgSQL_stmt_return_query) String() string { return "PLpgSQL_stmt_return_query" }

func (s *PLpgSQL_stmt_return_query) SqlString() string {
	if s.DynQuery != nil {
		var sb strings.Builder
		sb.WriteString("RETURN QUERY EXECUTE ")
		sb.WriteString(s.DynQuery.SqlString())
		writeUsing(&sb, s.Params)
		return sb.String()
	}
	return "RETURN QUERY " + s.Query.SqlString()
}

func NewPLpgSQL_stmt_return_query() *PLpgSQL_stmt_return_query {
	return &PLpgSQL_stmt_return_query{
		BaseNode: BaseNode{Tag: T_PLpgSQL_stmt_return_query, Loc: -1},
	}
}

// writeUsing appends a ` USING p1, p2, …` clause when params is non-empty.
// Shared by the dynamic-EXECUTE statements.
func writeUsing(sb *strings.Builder, params []*PLpgSQL_expr) {
	if len(params) == 0 {
		return
	}
	sb.WriteString(" USING ")
	for i, p := range params {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(p.SqlString())
	}
}

// PLpgSQL_stmt_dynexecute is `EXECUTE query [INTO [STRICT] target] [USING …]`
// (PG's PLpgSQL_stmt_dynexecute): run a dynamically-built SQL string. This is the
// primary node the Tier-1 dynamic-EXECUTE policy reasons about. PG resolves the
// INTO target to variables; we keep it as text. UsingFirst records the source
// order of the INTO and USING clauses (PG accepts them in either order) so the
// deparse round-trips.
type PLpgSQL_stmt_dynexecute struct {
	BaseNode
	Query      *PLpgSQL_expr   `json:"query,omitempty"`       // the SQL-string expression
	Into       bool            `json:"into,omitempty"`        // an INTO clause is present
	Strict     bool            `json:"strict,omitempty"`      // INTO STRICT
	Target     string          `json:"target,omitempty"`      // INTO target text (names)
	Params     []*PLpgSQL_expr `json:"params,omitempty"`      // USING expressions
	UsingFirst bool            `json:"using_first,omitempty"` // USING appeared before INTO
}

func (s *PLpgSQL_stmt_dynexecute) isStmt() {}

func (s *PLpgSQL_stmt_dynexecute) String() string { return "PLpgSQL_stmt_dynexecute" }

func (s *PLpgSQL_stmt_dynexecute) SqlString() string {
	var sb strings.Builder
	sb.WriteString("EXECUTE ")
	sb.WriteString(s.Query.SqlString())
	if s.UsingFirst {
		writeUsing(&sb, s.Params)
		s.writeInto(&sb)
	} else {
		s.writeInto(&sb)
		writeUsing(&sb, s.Params)
	}
	return sb.String()
}

func (s *PLpgSQL_stmt_dynexecute) writeInto(sb *strings.Builder) {
	if !s.Into {
		return
	}
	sb.WriteString(" INTO ")
	if s.Strict {
		sb.WriteString("STRICT ")
	}
	sb.WriteString(s.Target)
}

func NewPLpgSQL_stmt_dynexecute() *PLpgSQL_stmt_dynexecute {
	return &PLpgSQL_stmt_dynexecute{
		BaseNode: BaseNode{Tag: T_PLpgSQL_stmt_dynexecute, Loc: -1},
	}
}

// PLpgSQL_stmt_dynfors is `FOR var IN EXECUTE query [USING …] LOOP … END LOOP`
// (PG's PLpgSQL_stmt_dynfors): iterate over the rows of a dynamically-built query.
type PLpgSQL_stmt_dynfors struct {
	BaseNode
	Label  string          `json:"label,omitempty"`  // optional loop label
	Var    string          `json:"var,omitempty"`    // loop target name, as written
	Query  *PLpgSQL_expr   `json:"query,omitempty"`  // the SQL-string expression
	Params []*PLpgSQL_expr `json:"params,omitempty"` // USING expressions
	Body   []Stmt          `json:"body,omitempty"`   // loop body
}

func (s *PLpgSQL_stmt_dynfors) isStmt() {}

func (s *PLpgSQL_stmt_dynfors) String() string { return "PLpgSQL_stmt_dynfors" }

func (s *PLpgSQL_stmt_dynfors) SqlString() string {
	var sb strings.Builder
	writeLoopLabel(&sb, s.Label)
	sb.WriteString("FOR ")
	sb.WriteString(s.Var)
	sb.WriteString(" IN EXECUTE ")
	sb.WriteString(s.Query.SqlString())
	writeUsing(&sb, s.Params)
	sb.WriteString(" LOOP\n")
	deparseBody(&sb, s.Body)
	writeLoopEnd(&sb, s.Label)
	return sb.String()
}

func NewPLpgSQL_stmt_dynfors() *PLpgSQL_stmt_dynfors {
	return &PLpgSQL_stmt_dynfors{
		BaseNode: BaseNode{Tag: T_PLpgSQL_stmt_dynfors, Loc: -1},
	}
}

// FetchDirection mirrors PG's FetchDirection (parsenodes.h): the direction of a
// FETCH/MOVE. For ABSOLUTE/RELATIVE the count is an expression (Expr); for the
// others it is HowMany.
type FetchDirection int

const (
	FETCH_FORWARD FetchDirection = iota
	FETCH_BACKWARD
	FETCH_ABSOLUTE
	FETCH_RELATIVE
)

// FETCH_ALL is the HowMany sentinel for `FETCH ALL` / `MOVE ALL` (PG's
// FETCH_ALL = LONG_MAX).
const FETCH_ALL int64 = 1<<63 - 1

// Cursor option bits, matching PG (parsenodes.h). Only the flags PG's plpgsql
// parser sets are carried: the syntactic SCROLL / NO SCROLL, and the fixed
// FAST_PLAN default it applies to every OPEN / cursor declaration.
const (
	CURSOR_OPT_SCROLL    = 0x0002
	CURSOR_OPT_NO_SCROLL = 0x0004
	CURSOR_OPT_FAST_PLAN = 0x0100
)

// PLpgSQL_stmt_open is `OPEN cursor …` (PG's PLpgSQL_stmt_open): open a bound
// cursor (optionally with args) or an unbound one with a `FOR query` /
// `FOR EXECUTE expr [USING …]`. curvar is the cursor name as text (no
// resolution). Exactly one of Argquery / Query / DynQuery is set.
type PLpgSQL_stmt_open struct {
	BaseNode
	Curvar        string          `json:"curvar,omitempty"`         // cursor name, as written
	CursorOptions int             `json:"cursor_options,omitempty"` // SCROLL / NO SCROLL flags
	Argquery      *PLpgSQL_expr   `json:"argquery,omitempty"`       // bound-cursor args, `(…)`
	Query         *PLpgSQL_expr   `json:"query,omitempty"`          // FOR query
	DynQuery      *PLpgSQL_expr   `json:"dynquery,omitempty"`       // FOR EXECUTE expr
	Params        []*PLpgSQL_expr `json:"params,omitempty"`         // USING expressions (dynamic)
}

func (s *PLpgSQL_stmt_open) isStmt() {}

func (s *PLpgSQL_stmt_open) String() string { return "PLpgSQL_stmt_open" }

func (s *PLpgSQL_stmt_open) SqlString() string {
	var sb strings.Builder
	sb.WriteString("OPEN ")
	sb.WriteString(s.Curvar)
	switch {
	case s.Argquery != nil:
		sb.WriteString(s.Argquery.SqlString())
	case s.DynQuery != nil:
		if s.CursorOptions&CURSOR_OPT_NO_SCROLL != 0 {
			sb.WriteString(" NO SCROLL")
		} else if s.CursorOptions&CURSOR_OPT_SCROLL != 0 {
			sb.WriteString(" SCROLL")
		}
		sb.WriteString(" FOR EXECUTE ")
		sb.WriteString(s.DynQuery.SqlString())
		writeUsing(&sb, s.Params)
	case s.Query != nil:
		if s.CursorOptions&CURSOR_OPT_NO_SCROLL != 0 {
			sb.WriteString(" NO SCROLL")
		} else if s.CursorOptions&CURSOR_OPT_SCROLL != 0 {
			sb.WriteString(" SCROLL")
		}
		sb.WriteString(" FOR ")
		sb.WriteString(s.Query.SqlString())
	}
	return sb.String()
}

func NewPLpgSQL_stmt_open() *PLpgSQL_stmt_open {
	return &PLpgSQL_stmt_open{
		BaseNode: BaseNode{Tag: T_PLpgSQL_stmt_open, Loc: -1},
	}
}

// PLpgSQL_stmt_fetch is `FETCH … cursor INTO target` or `MOVE … cursor` (PG's
// PLpgSQL_stmt_fetch; MOVE reuses the struct with IsMove). The direction is
// parsed to PG's (Direction, HowMany, Expr) model; the deparse renders a
// canonical direction clause, so equivalent surface forms (e.g. NEXT vs the
// default) round-trip to the same text. curvar/target are text (no resolution).
type PLpgSQL_stmt_fetch struct {
	BaseNode
	Curvar              string         `json:"curvar,omitempty"`    // cursor name, as written
	Direction           FetchDirection `json:"direction,omitempty"` // fetch direction
	HowMany             int64          `json:"how_many,omitempty"`  // count when Expr is nil
	Expr                *PLpgSQL_expr  `json:"expr,omitempty"`      // count expression, or nil
	IsMove              bool           `json:"is_move,omitempty"`   // MOVE rather than FETCH
	Target              string         `json:"target,omitempty"`    // INTO target text (FETCH only)
	ReturnsMultipleRows bool           `json:"returns_multiple_rows,omitempty"`
}

func (s *PLpgSQL_stmt_fetch) isStmt() {}

func (s *PLpgSQL_stmt_fetch) String() string { return "PLpgSQL_stmt_fetch" }

func (s *PLpgSQL_stmt_fetch) SqlString() string {
	var sb strings.Builder
	if s.IsMove {
		sb.WriteString("MOVE")
	} else {
		sb.WriteString("FETCH")
	}
	dir := s.directionClause()
	if dir != "" {
		sb.WriteString(" ")
		sb.WriteString(dir)
		sb.WriteString(" FROM")
	}
	sb.WriteString(" ")
	sb.WriteString(s.Curvar)
	if !s.IsMove && s.Target != "" {
		sb.WriteString(" INTO ")
		sb.WriteString(s.Target)
	}
	return sb.String()
}

// directionClause renders the canonical direction keyword(s) for the fetch's
// (Direction, HowMany, Expr) state, or "" for the default (FORWARD, one row).
func (s *PLpgSQL_stmt_fetch) directionClause() string {
	count := ""
	if s.Expr != nil {
		count = " " + s.Expr.SqlString()
	} else if s.HowMany == FETCH_ALL {
		count = " ALL"
	}
	switch s.Direction {
	case FETCH_FORWARD:
		if s.Expr != nil {
			return "FORWARD" + count
		}
		if s.HowMany == FETCH_ALL {
			return "ALL"
		}
		return "" // FORWARD one row = default
	case FETCH_BACKWARD:
		return "BACKWARD" + count
	case FETCH_ABSOLUTE:
		if s.Expr != nil {
			return "ABSOLUTE" + count
		}
		if s.HowMany == -1 {
			return "LAST"
		}
		return "FIRST"
	case FETCH_RELATIVE:
		return "RELATIVE" + count
	}
	return ""
}

func NewPLpgSQL_stmt_fetch(isMove bool) *PLpgSQL_stmt_fetch {
	return &PLpgSQL_stmt_fetch{
		BaseNode:  BaseNode{Tag: T_PLpgSQL_stmt_fetch, Loc: -1},
		Direction: FETCH_FORWARD,
		HowMany:   1,
		IsMove:    isMove,
	}
}

// PLpgSQL_stmt_close is `CLOSE cursor` (PG's PLpgSQL_stmt_close).
type PLpgSQL_stmt_close struct {
	BaseNode
	Curvar string `json:"curvar,omitempty"` // cursor name, as written
}

func (s *PLpgSQL_stmt_close) isStmt() {}

func (s *PLpgSQL_stmt_close) String() string { return "PLpgSQL_stmt_close" }

func (s *PLpgSQL_stmt_close) SqlString() string { return "CLOSE " + s.Curvar }

func NewPLpgSQL_stmt_close() *PLpgSQL_stmt_close {
	return &PLpgSQL_stmt_close{
		BaseNode: BaseNode{Tag: T_PLpgSQL_stmt_close, Loc: -1},
	}
}

// RaiseLevel is the severity of a RAISE statement (PG's PLpgSQL_stmt_raise.
// elog_level, an int set to elog.h constants). The values match PG exactly so
// the field maps 1:1 to PG's struct; the keyword form is recovered on deparse.
type RaiseLevel int

const (
	RAISE_LEVEL_DEBUG     RaiseLevel = 14 // elog.h DEBUG1
	RAISE_LEVEL_LOG       RaiseLevel = 15 // elog.h LOG
	RAISE_LEVEL_INFO      RaiseLevel = 17 // elog.h INFO
	RAISE_LEVEL_NOTICE    RaiseLevel = 18 // elog.h NOTICE
	RAISE_LEVEL_WARNING   RaiseLevel = 19 // elog.h WARNING
	RAISE_LEVEL_EXCEPTION RaiseLevel = 21 // elog.h ERROR (the default)
)

// keyword returns the PL/pgSQL spelling of the level. ERROR is spelled
// EXCEPTION, the keyword the grammar accepts.
func (l RaiseLevel) keyword() string {
	switch l {
	case RAISE_LEVEL_DEBUG:
		return "DEBUG"
	case RAISE_LEVEL_LOG:
		return "LOG"
	case RAISE_LEVEL_INFO:
		return "INFO"
	case RAISE_LEVEL_NOTICE:
		return "NOTICE"
	case RAISE_LEVEL_WARNING:
		return "WARNING"
	default:
		return "EXCEPTION"
	}
}

// RaiseOptionType selects which USING option a PLpgSQL_raise_option carries
// (PG's PLpgSQL_raise_option_type). Order and set match PG exactly.
type RaiseOptionType int

const (
	PLPGSQL_RAISEOPTION_ERRCODE RaiseOptionType = iota
	PLPGSQL_RAISEOPTION_MESSAGE
	PLPGSQL_RAISEOPTION_DETAIL
	PLPGSQL_RAISEOPTION_HINT
	PLPGSQL_RAISEOPTION_COLUMN
	PLPGSQL_RAISEOPTION_CONSTRAINT
	PLPGSQL_RAISEOPTION_DATATYPE
	PLPGSQL_RAISEOPTION_TABLE
	PLPGSQL_RAISEOPTION_SCHEMA
)

// keyword returns the option's spelling in a USING clause.
func (t RaiseOptionType) keyword() string {
	switch t {
	case PLPGSQL_RAISEOPTION_ERRCODE:
		return "ERRCODE"
	case PLPGSQL_RAISEOPTION_MESSAGE:
		return "MESSAGE"
	case PLPGSQL_RAISEOPTION_DETAIL:
		return "DETAIL"
	case PLPGSQL_RAISEOPTION_HINT:
		return "HINT"
	case PLPGSQL_RAISEOPTION_COLUMN:
		return "COLUMN"
	case PLPGSQL_RAISEOPTION_CONSTRAINT:
		return "CONSTRAINT"
	case PLPGSQL_RAISEOPTION_DATATYPE:
		return "DATATYPE"
	case PLPGSQL_RAISEOPTION_TABLE:
		return "TABLE"
	case PLPGSQL_RAISEOPTION_SCHEMA:
		return "SCHEMA"
	default:
		return ""
	}
}

// PLpgSQL_raise_option is one `option = expr` entry of a RAISE ... USING clause
// (PG's PLpgSQL_raise_option). Helper node (Node, not Stmt), like
// PLpgSQL_case_when.
type PLpgSQL_raise_option struct {
	BaseNode
	OptType RaiseOptionType `json:"opt_type,omitempty"` // which USING option
	Expr    *PLpgSQL_expr   `json:"expr,omitempty"`     // the option value
}

func (o *PLpgSQL_raise_option) String() string { return "PLpgSQL_raise_option" }

func (o *PLpgSQL_raise_option) SqlString() string {
	return o.OptType.keyword() + " = " + o.Expr.SqlString()
}

func NewPLpgSQL_raise_option(optType RaiseOptionType) *PLpgSQL_raise_option {
	return &PLpgSQL_raise_option{
		BaseNode: BaseNode{Tag: T_PLpgSQL_raise_option, Loc: -1},
		OptType:  optType,
	}
}

// PLpgSQL_stmt_raise is a RAISE statement (PG's PLpgSQL_stmt_raise): report a
// message at a severity level, optionally with a condition name / SQLSTATE, an
// old-style format string with parameters, and a USING option list. A bare
// RAISE (all fields empty/default) re-throws the current error.
//
// Condname holds either a condition name or, when IsSqlState is set, a 5-char
// SQLSTATE code — PG stores both in one char* and never deparses; IsSqlState
// records which so the deparse re-emits the right form. Message is the dequoted
// literal value (as PG keeps it); the deparse re-quotes it.
type PLpgSQL_stmt_raise struct {
	BaseNode
	ElogLevel  RaiseLevel              `json:"elog_level,omitempty"`  // severity
	Condname   string                  `json:"condname,omitempty"`    // condition name / SQLSTATE, or ""
	IsSqlState bool                    `json:"is_sqlstate,omitempty"` // Condname is a SQLSTATE code
	Message    string                  `json:"message,omitempty"`     // old-style format literal, or ""
	Params     []*PLpgSQL_expr         `json:"params,omitempty"`      // expressions for the message
	Options    []*PLpgSQL_raise_option `json:"options,omitempty"`     // USING options
}

func (s *PLpgSQL_stmt_raise) isStmt() {}

func (s *PLpgSQL_stmt_raise) String() string { return "PLpgSQL_stmt_raise" }

func (s *PLpgSQL_stmt_raise) SqlString() string {
	// A bare re-raise: nothing but the default level.
	if s.Condname == "" && s.Message == "" && len(s.Params) == 0 && len(s.Options) == 0 {
		return "RAISE"
	}
	var sb strings.Builder
	sb.WriteString("RAISE ")
	sb.WriteString(s.ElogLevel.keyword())
	switch {
	case s.Condname != "":
		if s.IsSqlState {
			sb.WriteString(" SQLSTATE ")
			sb.WriteString(ast.QuoteStringLiteral(s.Condname))
		} else {
			sb.WriteString(" ")
			sb.WriteString(s.Condname)
		}
	case s.Message != "":
		sb.WriteString(" ")
		sb.WriteString(ast.QuoteStringLiteral(s.Message))
		for _, p := range s.Params {
			sb.WriteString(", ")
			sb.WriteString(p.SqlString())
		}
	}
	if len(s.Options) > 0 {
		sb.WriteString(" USING ")
		for i, o := range s.Options {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(o.SqlString())
		}
	}
	return sb.String()
}

func NewPLpgSQL_stmt_raise() *PLpgSQL_stmt_raise {
	return &PLpgSQL_stmt_raise{
		BaseNode:  BaseNode{Tag: T_PLpgSQL_stmt_raise, Loc: -1},
		ElogLevel: RAISE_LEVEL_EXCEPTION,
	}
}

// PLpgSQL_stmt_assert is an ASSERT statement (PG's PLpgSQL_stmt_assert):
// `ASSERT cond [, message]` — raise assert_failure if cond is not true.
type PLpgSQL_stmt_assert struct {
	BaseNode
	Cond    *PLpgSQL_expr `json:"cond,omitempty"`    // the asserted condition
	Message *PLpgSQL_expr `json:"message,omitempty"` // optional message expression, or nil
}

func (s *PLpgSQL_stmt_assert) isStmt() {}

func (s *PLpgSQL_stmt_assert) String() string { return "PLpgSQL_stmt_assert" }

func (s *PLpgSQL_stmt_assert) SqlString() string {
	out := "ASSERT " + s.Cond.SqlString()
	if s.Message != nil {
		out += ", " + s.Message.SqlString()
	}
	return out
}

func NewPLpgSQL_stmt_assert() *PLpgSQL_stmt_assert {
	return &PLpgSQL_stmt_assert{
		BaseNode: BaseNode{Tag: T_PLpgSQL_stmt_assert, Loc: -1},
	}
}

// PLpgSQL_exception_block is the EXCEPTION section of a block (PG's
// PLpgSQL_exception_block). Its WHEN-clause list and handler nodes
// (PLpgSQL_exception / PLpgSQL_condition) are added by the exception-block
// chunk; for now this is an empty placeholder so PLpgSQL_stmt_block.Exceptions
// has a stable type.
type PLpgSQL_exception_block struct {
	BaseNode
}

func (e *PLpgSQL_exception_block) String() string {
	return "PLpgSQL_exception_block"
}

func (e *PLpgSQL_exception_block) SqlString() string {
	return ""
}

func NewPLpgSQL_exception_block() *PLpgSQL_exception_block {
	return &PLpgSQL_exception_block{
		BaseNode: BaseNode{Tag: T_PLpgSQL_exception_block, Loc: -1},
	}
}
