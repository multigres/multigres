// PostgreSQL Database Management System
// (also known as Postgres, formerly known as Postgres95)
//
//  Portions Copyright (c) 2025, Supabase, Inc
//
//  Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//
//  Portions Copyright (c) 1994, The Regents of the University of California
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written agreement
// is hereby granted, provided that the above copyright notice and this
// paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
// DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
// LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
// DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.
//
// THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
// AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
// ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
// PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
//

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
	T_Invalid                   NodeTag = iota
	T_PLpgSQL_function                  // Root of a parsed PL/pgSQL function body
	T_PLpgSQL_stmt_block                // BEGIN … END block
	T_PLpgSQL_expr                      // SQL fragment (verbatim text + parsed AST)
	T_PLpgSQL_exception_block           // EXCEPTION section of a block
	T_PLpgSQL_var                       // declared scalar variable
	T_PLpgSQL_type                      // declared type (captured as text)
	T_PLpgSQL_stmt_assign               // assignment statement (target := expr)
	T_PLpgSQL_stmt_if                   // IF … THEN … [ELSIF …] [ELSE …] END IF
	T_PLpgSQL_if_elsif                  // one ELSIF arm of an IF statement
	T_PLpgSQL_stmt_loop                 // unconditional LOOP … END LOOP
	T_PLpgSQL_stmt_while                // WHILE cond LOOP … END LOOP
	T_PLpgSQL_stmt_exit                 // EXIT / CONTINUE [label] [WHEN cond]
	T_PLpgSQL_stmt_fori                 // integer FOR (FOR i IN a..b LOOP)
	T_PLpgSQL_stmt_fors                 // query FOR (FOR r IN query LOOP)
	T_PLpgSQL_stmt_foreach_a            // FOREACH x IN ARRAY expr LOOP
	T_PLpgSQL_stmt_case                 // CASE … WHEN … END CASE
	T_PLpgSQL_case_when                 // one WHEN arm of a CASE statement
	T_PLpgSQL_stmt_execsql              // an embedded SQL statement (SELECT/INSERT/…)
	T_PLpgSQL_stmt_perform              // PERFORM expr
	T_PLpgSQL_stmt_call                 // CALL / DO
	T_PLpgSQL_stmt_return               // RETURN [expr]
	T_PLpgSQL_stmt_return_next          // RETURN NEXT expr
	T_PLpgSQL_stmt_return_query         // RETURN QUERY query
	T_PLpgSQL_stmt_dynexecute           // EXECUTE expr [INTO …] [USING …]
	T_PLpgSQL_stmt_dynfors              // FOR var IN EXECUTE expr LOOP
	T_PLpgSQL_stmt_open                 // OPEN cursor …
	T_PLpgSQL_stmt_fetch                // FETCH / MOVE … cursor
	T_PLpgSQL_stmt_close                // CLOSE cursor
	T_PLpgSQL_alias                     // name ALIAS FOR target
	T_PLpgSQL_stmt_raise                // RAISE [level] […] [USING …]
	T_PLpgSQL_raise_option              // one USING option of a RAISE
	T_PLpgSQL_stmt_assert               // ASSERT cond [, message]
	T_PLpgSQL_exception                 // one EXCEPTION … WHEN clause
	T_PLpgSQL_condition                 // one condition of a WHEN clause
	T_PLpgSQL_stmt_getdiag              // GET [CURRENT|STACKED] DIAGNOSTICS …
	T_PLpgSQL_diag_item                 // one item of a GET DIAGNOSTICS list
	T_PLpgSQL_stmt_commit               // COMMIT [AND [NO] CHAIN]
	T_PLpgSQL_stmt_rollback             // ROLLBACK [AND [NO] CHAIN]
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
	case T_PLpgSQL_stmt_fori:
		return "T_PLpgSQL_stmt_fori"
	case T_PLpgSQL_stmt_fors:
		return "T_PLpgSQL_stmt_fors"
	case T_PLpgSQL_stmt_foreach_a:
		return "T_PLpgSQL_stmt_foreach_a"
	case T_PLpgSQL_stmt_case:
		return "T_PLpgSQL_stmt_case"
	case T_PLpgSQL_case_when:
		return "T_PLpgSQL_case_when"
	case T_PLpgSQL_stmt_execsql:
		return "T_PLpgSQL_stmt_execsql"
	case T_PLpgSQL_stmt_perform:
		return "T_PLpgSQL_stmt_perform"
	case T_PLpgSQL_stmt_call:
		return "T_PLpgSQL_stmt_call"
	case T_PLpgSQL_stmt_return:
		return "T_PLpgSQL_stmt_return"
	case T_PLpgSQL_stmt_return_next:
		return "T_PLpgSQL_stmt_return_next"
	case T_PLpgSQL_stmt_return_query:
		return "T_PLpgSQL_stmt_return_query"
	case T_PLpgSQL_stmt_dynexecute:
		return "T_PLpgSQL_stmt_dynexecute"
	case T_PLpgSQL_stmt_dynfors:
		return "T_PLpgSQL_stmt_dynfors"
	case T_PLpgSQL_stmt_open:
		return "T_PLpgSQL_stmt_open"
	case T_PLpgSQL_stmt_fetch:
		return "T_PLpgSQL_stmt_fetch"
	case T_PLpgSQL_stmt_close:
		return "T_PLpgSQL_stmt_close"
	case T_PLpgSQL_alias:
		return "T_PLpgSQL_alias"
	case T_PLpgSQL_stmt_raise:
		return "T_PLpgSQL_stmt_raise"
	case T_PLpgSQL_raise_option:
		return "T_PLpgSQL_raise_option"
	case T_PLpgSQL_stmt_assert:
		return "T_PLpgSQL_stmt_assert"
	case T_PLpgSQL_exception:
		return "T_PLpgSQL_exception"
	case T_PLpgSQL_condition:
		return "T_PLpgSQL_condition"
	case T_PLpgSQL_stmt_getdiag:
		return "T_PLpgSQL_stmt_getdiag"
	case T_PLpgSQL_diag_item:
		return "T_PLpgSQL_diag_item"
	case T_PLpgSQL_stmt_commit:
		return "T_PLpgSQL_stmt_commit"
	case T_PLpgSQL_stmt_rollback:
		return "T_PLpgSQL_stmt_rollback"
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
