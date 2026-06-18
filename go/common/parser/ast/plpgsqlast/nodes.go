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

// Package plpgsqlast holds the PL/pgSQL AST: the node hierarchy produced by the
// PL/pgSQL parser in go/common/parser/plpgsql. It is deliberately a SEPARATE
// hierarchy from the SQL AST in go/common/parser/ast — mirroring PostgreSQL,
// where PLpgSQL_stmt/PLpgSQL_expr are distinct from the SQL parse tree.
//
// Keeping these nodes out of package ast keeps the ~49-type PL/pgSQL vocabulary
// (and its generated clone/rewrite helpers) from bloating the SQL AST's central
// generated files and NodeTag enum. The two hierarchies meet at the embedded
// SQL boundary: a PL/pgSQL expression node holds an ast.Stmt for its parsed SQL
// fragment, which the gateway hands to the SQL-side analysis. Dependencies flow
// one way (plpgsqlast -> ast), so there is no import cycle.
package plpgsqlast

import "fmt"

// NodeTag identifies the concrete type of a PL/pgSQL AST node. It is independent
// of ast.NodeTag — the two hierarchies have separate tag spaces.
type NodeTag int

const (
	// T_Invalid is the zero value; it marks an uninitialized or placeholder node.
	T_Invalid                 NodeTag = iota
	T_PLpgSQL_function                // Root of a parsed PL/pgSQL function body
	T_PLpgSQL_stmt_block              // BEGIN … END block
	T_PLpgSQL_expr                    // SQL fragment (verbatim text + parsed AST)
	T_PLpgSQL_exception_block         // EXCEPTION section of a block
	T_PLpgSQL_var                     // declared scalar variable
	T_PLpgSQL_type                    // declared type (captured as text)
	T_PLpgSQL_stmt_assign             // assignment statement (target := expr)
	T_PLpgSQL_stmt_if                 // IF … THEN … [ELSIF …] [ELSE …] END IF
	T_PLpgSQL_if_elsif                // one ELSIF arm of an IF statement
	T_PLpgSQL_stmt_loop               // unconditional LOOP … END LOOP
	T_PLpgSQL_stmt_while              // WHILE cond LOOP … END LOOP
	T_PLpgSQL_stmt_exit               // EXIT / CONTINUE [label] [WHEN cond]
)

// String returns the string representation of a NodeTag.
func (nt NodeTag) String() string {
	switch nt {
	case T_PLpgSQL_function:
		return "T_PLpgSQL_function"
	case T_PLpgSQL_stmt_block:
		return "T_PLpgSQL_stmt_block"
	case T_PLpgSQL_expr:
		return "T_PLpgSQL_expr"
	case T_PLpgSQL_exception_block:
		return "T_PLpgSQL_exception_block"
	case T_PLpgSQL_var:
		return "T_PLpgSQL_var"
	case T_PLpgSQL_type:
		return "T_PLpgSQL_type"
	case T_PLpgSQL_stmt_assign:
		return "T_PLpgSQL_stmt_assign"
	case T_PLpgSQL_stmt_if:
		return "T_PLpgSQL_stmt_if"
	case T_PLpgSQL_if_elsif:
		return "T_PLpgSQL_if_elsif"
	case T_PLpgSQL_stmt_loop:
		return "T_PLpgSQL_stmt_loop"
	case T_PLpgSQL_stmt_while:
		return "T_PLpgSQL_stmt_while"
	case T_PLpgSQL_stmt_exit:
		return "T_PLpgSQL_stmt_exit"
	default:
		return fmt.Sprintf("NodeTag(%d)", int(nt))
	}
}

// Node is implemented by every PL/pgSQL AST node. It mirrors ast.Node so the
// two hierarchies feel the same to callers, but it is a separate interface so
// PL/pgSQL nodes are never accepted by the SQL AST's clone/rewrite machinery
// (and vice versa) — they get their own generated helpers instead.
type Node interface {
	// NodeTag returns the type tag for this node.
	NodeTag() NodeTag

	// Location returns the byte offset in the source where this node begins,
	// or -1 if unavailable.
	Location() int

	// SetLocation records the byte offset in the source where this node begins.
	SetLocation(location int)

	// String returns a string representation of the node (for debugging).
	String() string

	// SqlString returns the PL/pgSQL source representation of this node, to
	// support round-tripping a parsed body back to text.
	SqlString() string
}

// BaseNode is embedded by every PL/pgSQL node to supply the common tag and
// location plumbing, mirroring ast.BaseNode.
type BaseNode struct {
	Tag NodeTag // Node type tag
	Loc int     // Source location in bytes (-1 if unavailable)
}

// NodeTag returns the node's type tag.
func (n *BaseNode) NodeTag() NodeTag {
	return n.Tag
}

// Location returns the node's source location.
func (n *BaseNode) Location() int {
	return n.Loc
}

// SetLocation sets the node's source location.
func (n *BaseNode) SetLocation(location int) {
	n.Loc = location
}

// String returns a basic string representation.
func (n *BaseNode) String() string {
	return fmt.Sprintf("%s@%d", n.Tag, n.Loc)
}

// SqlString provides a default that panics; concrete node types override it to
// deparse themselves.
func (n *BaseNode) SqlString() string {
	if n.Tag == T_Invalid {
		return "<INVALID>"
	}
	panic(fmt.Sprintf("SqlString() not implemented for PL/pgSQL node type %s (tag: %d). "+
		"Implement SqlString() on this node type to enable deparsing.",
		n.Tag, int(n.Tag)))
}
