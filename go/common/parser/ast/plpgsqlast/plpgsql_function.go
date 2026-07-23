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

package plpgsqlast

// PLpgSQL_function is the root of a parsed PL/pgSQL function body.
// Ported from postgres/src/pl/plpgsql/src/plpgsql.h (PLpgSQL_function struct).
//
// As in PG, the body is a single top-level block (PG's function->action). The
// many execution-engine fields of PG's struct (datum array, resolution flags,
// etc.) are intentionally omitted — this is a parse tree for static analysis.
type PLpgSQL_function struct {
	BaseNode
	Action *PLpgSQL_stmt_block `json:"action,omitempty"` // the function body block
}

func (n *PLpgSQL_function) String() string {
	return "PLpgSQL_function"
}

func (n *PLpgSQL_function) SqlString() string {
	if n.Action == nil {
		return ""
	}
	return n.Action.SqlString()
}

func NewPLpgSQL_function() *PLpgSQL_function {
	return &PLpgSQL_function{
		BaseNode: BaseNode{Tag: T_PLpgSQL_function, Loc: -1},
	}
}
