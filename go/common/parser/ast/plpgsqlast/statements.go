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
	for _, s := range b.Body {
		sb.WriteString(s.SqlString())
		sb.WriteString(";\n")
	}
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
