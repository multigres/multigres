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

import "strings"

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
