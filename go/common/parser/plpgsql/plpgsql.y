%{
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

// Ported from postgres/src/pl/plpgsql/src/pl_gram.y. Currently only the
// scaffolding: the grammar accepts an empty body and produces an empty
// PLpgSQL_function root node. Statement productions (block, DECLARE, IF,
// LOOP, FOR, EXECSQL, DYNEXECUTE, …) are added incrementally as the grammar
// is ported.

package plpgsql

import (
	"github.com/multigres/multigres/go/common/parser/ast/plpgsqlast"
)

// plpgsqlResultSetter is accessed via type assertion on the lexer so actions
// can publish the parsed function without depending on a concrete lexer type.
type plpgsqlResultSetter interface {
	SetResult(*plpgsqlast.PLpgSQL_function)
}

%}

%union {
	function *plpgsqlast.PLpgSQL_function
}

%type <function> pl_function

%start pl_function

%%

/*
 * Root production. Currently accepts only an empty body; statement-list
 * productions are added as the grammar is ported.
 */
pl_function:
		/* empty */
			{
				$$ = plpgsqlast.NewPLpgSQL_function()
				if l, ok := plpgsqllex.(plpgsqlResultSetter); ok {
					l.SetResult($$)
				}
			}
	;

%%
