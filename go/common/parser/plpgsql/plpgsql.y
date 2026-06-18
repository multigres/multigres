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

// Scalar semantic values the lexer fills in directly (matching the SQL
// grammar's %struct). goyacc promotes these to plain fields on
// plpgsqlSymType, so the lexer can set lval.str / lval.ival / lval.location.
%struct {
	str      string
	ival     int
	location int
}

// Token vocabulary, ported from pl_gram.y. Productions that consume these
// land in later chunks; for now only the lexer (lexer.go) emits them and the
// keyword tables (keywords.go) map names to them. T_WORD/T_CWORD/T_DATUM and
// the keyword value types are simplified to <str> for now (the structured
// word/cword/datum carriers arrive with the datum family).
%token <str>	IDENT UIDENT FCONST SCONST USCONST BCONST XCONST Op
%token <ival>	ICONST PARAM
%token		TYPECAST DOT_DOT COLON_EQUALS EQUALS_GREATER
%token		LESS_EQUALS GREATER_EQUALS NOT_EQUALS

%token <str>	T_WORD		/* unrecognized simple identifier */
%token <str>	T_CWORD		/* unrecognized composite identifier */
%token <str>	T_DATUM		/* a VAR, ROW, REC, or RECFIELD variable */
%token		LESS_LESS
%token		GREATER_GREATER

/*
 * Keyword tokens. Reserved and unreserved are split exactly as in PG's
 * pl_reserved_kwlist.h / pl_unreserved_kwlist.h; the distinction lives in
 * keywords.go, not here.
 */
%token <str>	K_ABSOLUTE K_ALIAS K_ALL K_AND K_ARRAY K_ASSERT K_BACKWARD
%token <str>	K_BEGIN K_BY K_CALL K_CASE K_CHAIN K_CLOSE K_COLLATE K_COLUMN
%token <str>	K_COLUMN_NAME K_COMMIT K_CONSTANT K_CONSTRAINT K_CONSTRAINT_NAME
%token <str>	K_CONTINUE K_CURRENT K_CURSOR K_DATATYPE K_DEBUG K_DECLARE
%token <str>	K_DEFAULT K_DETAIL K_DIAGNOSTICS K_DO K_DUMP K_ELSE K_ELSIF
%token <str>	K_END K_ERRCODE K_ERROR K_EXCEPTION K_EXECUTE K_EXIT K_FETCH
%token <str>	K_FIRST K_FOR K_FOREACH K_FORWARD K_FROM K_GET K_HINT K_IF
%token <str>	K_IMPORT K_IN K_INFO K_INSERT K_INTO K_IS K_LAST K_LOG K_LOOP
%token <str>	K_MERGE K_MESSAGE K_MESSAGE_TEXT K_MOVE K_NEXT K_NO K_NOT
%token <str>	K_NOTICE K_NULL K_OPEN K_OPTION K_OR K_PERFORM K_PG_CONTEXT
%token <str>	K_PG_DATATYPE_NAME K_PG_EXCEPTION_CONTEXT K_PG_EXCEPTION_DETAIL
%token <str>	K_PG_EXCEPTION_HINT K_PG_ROUTINE_OID K_PRINT_STRICT_PARAMS
%token <str>	K_PRIOR K_QUERY K_RAISE K_RELATIVE K_RETURN K_RETURNED_SQLSTATE
%token <str>	K_REVERSE K_ROLLBACK K_ROW_COUNT K_ROWTYPE K_SCHEMA K_SCHEMA_NAME
%token <str>	K_SCROLL K_SLICE K_SQLSTATE K_STACKED K_STRICT K_TABLE
%token <str>	K_TABLE_NAME K_THEN K_TO K_TYPE K_USE_COLUMN K_USE_VARIABLE
%token <str>	K_USING K_VARIABLE_CONFLICT K_WARNING K_WHEN K_WHILE

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
