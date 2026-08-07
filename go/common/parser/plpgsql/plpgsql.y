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

// Ported from postgres/src/pl/plpgsql/src/pl_gram.y. The productions in the
// grammar section below are kept in the same order and named identically to
// their PG counterparts (see the note after %%). PG's execution machinery is
// dropped, but its compile-time namespace and statement-level identifier
// resolution ARE ported: a word declared in a DECLARE section resolves to a
// datum (T_DATUM) via the scanner (lexer.go, namespace.go), exactly as in
// plpgsql_yylex. Type/catalog resolution stays out of scope, so a declared type
// is captured as text. The hand-scan actions delegate to helpers in
// read_construct.go, whose comments name the exact PG function each ports.

package plpgsql

import (
	"fmt"

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
	block    *plpgsqlast.PLpgSQL_stmt_block
	stmt     plpgsqlast.Stmt
	stmts    []plpgsqlast.Stmt
	declsect declSect
	datum    plpgsqlast.Datum
	datums   []plpgsqlast.Datum
	typ      *plpgsqlast.PLpgSQL_type
	expr     *plpgsqlast.PLpgSQL_expr
	elsifs    []*plpgsqlast.PLpgSQL_if_elsif
	loopbody  loopBody
	casewhens []*plpgsqlast.PLpgSQL_case_when
	casewhen  *plpgsqlast.PLpgSQL_case_when
	fetch     *plpgsqlast.PLpgSQL_stmt_fetch
	cursorargs []*plpgsqlast.PLpgSQL_var
	cvar      *plpgsqlast.PLpgSQL_var
	bval      bool
	excblock  *plpgsqlast.PLpgSQL_exception_block
	excs      []*plpgsqlast.PLpgSQL_exception
	exc       *plpgsqlast.PLpgSQL_exception
	conds     []*plpgsqlast.PLpgSQL_condition
	cond      *plpgsqlast.PLpgSQL_condition
	diagitems []*plpgsqlast.PLpgSQL_diag_item
	diagitem  *plpgsqlast.PLpgSQL_diag_item
	forvar    forVariable
	wdatum    plwdatum
}

// Scalar semantic values the lexer fills in directly (matching the SQL
// grammar's %struct). goyacc promotes these to plain fields on
// plpgsqlSymType, so the lexer can set lval.str / lval.ival / lval.location.
%struct {
	str      string
	ival     int
	location int
}

// Token vocabulary, ported from pl_gram.y's %token declarations. The lexer
// (lexer.go) emits these and the keyword tables (keywords.go) map names to them.
// T_WORD/T_CWORD carry <str>; PG's structured word/cword and the T_DATUM carrier
// are simplified to <str> since we never resolve to T_DATUM.
%token <str>	IDENT UIDENT FCONST SCONST USCONST BCONST XCONST Op
%token <ival>	ICONST PARAM
%token		TYPECAST DOT_DOT COLON_EQUALS EQUALS_GREATER
%token		LESS_EQUALS GREATER_EQUALS NOT_EQUALS

%token <str>	T_WORD		/* unrecognized simple identifier */
%token <str>	T_CWORD		/* unrecognized composite identifier */
%token <wdatum>	T_DATUM		/* a VAR, ROW, REC, or RECFIELD variable */
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
%type <block>    pl_block
%type <declsect> decl_sect
%type <excblock> exception_sect
%type <excs>     proc_exceptions
%type <exc>      proc_exception
%type <conds>    proc_conditions
%type <cond>     proc_condition
%type <datums>   decl_stmts
%type <datum>    decl_stmt decl_statement
%type <typ>      decl_datatype
%type <expr>     decl_defval decl_cursor_query
%type <bval>     decl_const decl_notnull
%type <ival>     opt_scrollable
%type <cursorargs> decl_cursor_args decl_cursor_arglist
%type <cvar>     decl_cursor_arg
%type <str>      decl_aliasitem decl_collate
%type <stmts>    proc_sect stmt_else opt_case_else
%type <stmt>     proc_stmt stmt_null stmt_if stmt_loop stmt_while stmt_exit stmt_assign
%type <stmt>     stmt_for stmt_foreach_a stmt_case for_control
%type <stmt>     stmt_execsql stmt_perform stmt_call stmt_return stmt_dynexecute
%type <stmt>     stmt_open stmt_fetch stmt_move stmt_close stmt_raise stmt_assert
%type <stmt>     stmt_getdiag stmt_commit stmt_rollback
%type <bval>     getdiag_area_opt opt_transaction_chain
%type <diagitems> getdiag_list
%type <diagitem> getdiag_list_item
%type <str>      getdiag_target
%type <ival>     getdiag_item
%type <fetch>    opt_fetch_direction
%type <str>      cursor_variable
%type <elsifs>   stmt_elsifs
%type <casewhens> case_when_list
%type <casewhen> case_when
%type <loopbody> loop_body
%type <expr>     expr_until_semi expr_until_then expr_until_loop opt_exitcond opt_expr_until_when
%type <bval>     exit_type
%type <ival>     foreach_slice
%type <str>      opt_block_label opt_loop_label opt_label any_identifier unreserved_keyword
%type <str>      option_value decl_varname
%type <forvar>   for_variable

%start pl_function

%%

/*
 * The productions below are kept in the same order as, and named identically to,
 * their counterparts in postgres/src/pl/plpgsql/src/pl_gram.y, so each rule maps
 * to the same-named PG rule — including stmt_assign, dispatched from a resolved
 * T_DATUM target. The hand-scan actions delegate to helpers in read_construct.go,
 * whose comments name the exact PG function each ports.
 */

/*
 * PG: pl_gram.y pl_function. A PL/pgSQL body is an optional comp_options
 * preamble (#variable_conflict / #print_strict_params / #option dump) followed
 * by a single top-level block and an optional trailing semicolon. The block
 * becomes the function's Action; the comp_options are captured on the lexer by
 * the comp_option actions (PG mutates plpgsql_curr_compile) and copied here.
 * They are semantically inert for static analysis but round-trip on deparse.
 */
pl_function:
		comp_options pl_block opt_semi
			{
				lx := plpgsqllex.(*lexer)
				fn := plpgsqlast.NewPLpgSQL_function()
				fn.Action = $2
				fn.ResolveOption = lx.compResolveOption
				fn.PrintStrictParams = lx.compPrintStrictParams
				fn.DumpExecTree = lx.compDumpExecTree
				if l, ok := plpgsqllex.(plpgsqlResultSetter); ok {
					l.SetResult(fn)
				}
			}
	;

/*
 * PG: pl_gram.y comp_options / comp_option / option_value. The compiler-option
 * preamble. PG's actions mutate the compile state (resolve_option,
 * print_strict_params, plpgsql_DumpExecTree); ours record the same on the lexer
 * for pl_function to read. option_value's on/off validity is kept (PG errors on
 * anything else); the directives do not affect parsing otherwise.
 */
comp_options:
		/* empty */
	|	comp_options comp_option
	;

comp_option:
		'#' K_OPTION K_DUMP
			{
				plpgsqllex.(*lexer).compDumpExecTree = true
			}
	|	'#' K_PRINT_STRICT_PARAMS option_value
			{
				lx := plpgsqllex.(*lexer)
				if $3 != "on" && $3 != "off" {
					lx.Error(fmt.Sprintf("unrecognized print_strict_params option %s", $3))
				}
				lx.compPrintStrictParams = $3
			}
	|	'#' K_VARIABLE_CONFLICT K_ERROR
			{
				plpgsqllex.(*lexer).compResolveOption = plpgsqlast.PLPGSQL_RESOLVE_ERROR
			}
	|	'#' K_VARIABLE_CONFLICT K_USE_VARIABLE
			{
				plpgsqllex.(*lexer).compResolveOption = plpgsqlast.PLPGSQL_RESOLVE_VARIABLE
			}
	|	'#' K_VARIABLE_CONFLICT K_USE_COLUMN
			{
				plpgsqllex.(*lexer).compResolveOption = plpgsqlast.PLPGSQL_RESOLVE_COLUMN
			}
	;

option_value:
		T_WORD
			{
				$$ = $1
			}
	|	unreserved_keyword
			{
				$$ = $1
			}
	;

opt_semi:
		/* empty */
	|	';'
	;

/*
 * PG: pl_gram.y pl_block. An optional DECLARE section, BEGIN, a statement list,
 * an optional EXCEPTION section, END, and an optional matching end label. PG's
 * namespace push/pop and label registration are dropped (no namespace).
 */
pl_block:
		decl_sect K_BEGIN proc_sect exception_sect K_END opt_label
			{
				block := plpgsqlast.NewPLpgSQL_stmt_block()
				block.Label = $1.label
				block.Decls = $1.decls
				block.Body = $3
				block.Exceptions = $4
				if err := checkLabels($1.label, $6); err != nil {
					plpgsqllex.Error(err.Error())
				}
				// Close the block's namespace level (opt_block_label opened it).
				plpgsqllex.(*lexer).ns.pop()
				$$ = block
			}
	;

/*
 * PG: pl_gram.y decl_sect + decl_start. The block label lives here (before
 * DECLARE). DECLARE is optional, and a DECLARE with no declarations is allowed —
 * the three arms mirror PG's decl_sect (opt_block_label [decl_start
 * [decl_stmts]]). decl_start is PG's bare-DECLARE non-terminal; PG's
 * IdentifierLookup / plpgsql_add_initdatums side-effects there are dropped.
 */
decl_sect:
		opt_block_label
			{
				// Done with (absent) decls, so resume identifier resolution.
				plpgsqllex.(*lexer).mode = lookupNormal
				$$ = declSect{label: $1}
			}
	|	opt_block_label decl_start
			{
				plpgsqllex.(*lexer).mode = lookupNormal
				$$ = declSect{label: $1}
			}
	|	opt_block_label decl_start decl_stmts
			{
				// Resume identifier resolution now that the DECLARE section is done.
				// The duplicate-declaration check is enforced per-declaration at
				// registration time (decl_statement, via the block namespace),
				// matching PG's decl_varname, so no structural pass is needed here.
				plpgsqllex.(*lexer).mode = lookupNormal
				$$ = declSect{label: $1, decls: $3}
			}
	;

decl_start:
		K_DECLARE
			{
				// Disable scanner resolution of identifiers while we process the
				// declarations, so a declared name is not resolved against an outer
				// variable (PG sets IDENTIFIER_LOOKUP_DECLARE here). This is its own
				// rule so the mode flips before the first declaration token is read.
				plpgsqllex.(*lexer).mode = lookupDeclare
			}
	;

decl_stmts:
		decl_stmts decl_stmt
			{
				$$ = appendDatum($1, $2)
			}
	|	decl_stmt
			{
				$$ = appendDatum(nil, $1)
			}
	;

decl_stmt:
		decl_statement
			{
				$$ = $1
			}
	|	K_DECLARE
			{
				// extra DECLAREs are allowed and ignored, matching PG
				$$ = nil
			}
	;

/*
 * PG: pl_gram.y decl_statement. A single declaration, in three forms sharing the
 * leading identifier: a variable (`name [CONSTANT] type [COLLATE c] [NOT NULL]
 * [:= expr]`), an ALIAS (`name ALIAS FOR target`), or a CURSOR (`name [scroll]
 * CURSOR [(args)] {IS|FOR} query`). The leading name is decl_varname; PG's
 * namespace registration there is dropped, but its duplicate-declaration check is
 * kept (see decl_sect). The variable-vs-cursor split is disjoint by lookahead
 * (opt_scrollable reduces empty only on K_CURSOR; decl_const is the default) — PG
 * declares %expect 0.
 */
decl_statement:
		decl_varname decl_const decl_datatype decl_collate decl_notnull decl_defval
			{
				// A RECORD / %ROWTYPE declaration builds a record datum, everything
				// else a scalar; both carry the same parse-level fields.
				d := makeDeclDatum($1, $2, $3, $4, $5, $6)
				if $5 && $6 == nil {
					plpgsqllex.Error(fmt.Sprintf(
						"variable %q must have a default value, since it's declared NOT NULL",
						$1))
				}
				// Register the variable so later statement-level references resolve
				// to it (PG's plpgsql_build_variable); also enforces the
				// duplicate-declaration check via the block namespace.
				plpgsqllex.(*lexer).declareVar($1, d)
				$$ = d
			}
	|	decl_varname K_ALIAS K_FOR decl_aliasitem ';'
			{
				a := plpgsqlast.NewPLpgSQL_alias($1)
				a.Target = $4
				plpgsqllex.(*lexer).declareVar(a.Refname, a)
				$$ = a
			}
	|	decl_varname opt_scrollable K_CURSOR decl_cursor_args decl_is_for decl_cursor_query
			{
				v := plpgsqlast.NewPLpgSQL_var($1)
				v.CursorOptions = plpgsqlast.CURSOR_OPT_FAST_PLAN | $2
				v.CursorArgs = $4
				v.CursorExplicitExpr = $6
				plpgsqllex.(*lexer).declareVar(v.Refname, v)
				$$ = v
			}
	;

/*
 * PG: pl_gram.y opt_scrollable / decl_cursor_query / decl_cursor_args /
 * decl_cursor_arglist / decl_cursor_arg / decl_is_for. opt_scrollable yields the
 * SCROLL option bits; decl_cursor_query scans the bound query as raw text (PG's
 * action reads it via read_sql_construct); decl_cursor_args a list of arg
 * variables (name + type). PG builds a cursor_explicit_argrow datum; we keep the
 * args as a slice.
 */
opt_scrollable:
		/* empty */
			{
				$$ = 0
			}
	|	K_NO K_SCROLL
			{
				$$ = plpgsqlast.CURSOR_OPT_NO_SCROLL
			}
	|	K_SCROLL
			{
				$$ = plpgsqlast.CURSOR_OPT_SCROLL
			}
	;

decl_cursor_query:
		/* empty */
			{
				lx := plpgsqllex.(*lexer)
				lx.beginScan(plpgsqlrcvr.char)
				plpgsqlrcvr.char = -1
				plpgsqltoken = -1
				q, _ := lx.readSQLConstruct(plpgsqlast.RAW_PARSE_DEFAULT, ';')
				$$ = q
			}
	;

decl_cursor_args:
		/* empty */
			{
				$$ = nil
			}
	|	'(' decl_cursor_arglist ')'
			{
				$$ = $2
			}
	;

decl_cursor_arglist:
		decl_cursor_arg
			{
				$$ = appendCursorArg(nil, $1)
			}
	|	decl_cursor_arglist ',' decl_cursor_arg
			{
				$$ = appendCursorArg($1, $3)
			}
	;

decl_cursor_arg:
		decl_varname decl_datatype
			{
				v := plpgsqlast.NewPLpgSQL_var($1)
				v.DataType = $2
				$$ = v
			}
	;

decl_is_for:
		K_IS
	|	K_FOR
	;

/*
 * PG: pl_gram.y decl_aliasitem. The ALIAS target — a plain word, an unreserved
 * keyword, or a compound name (T_CWORD, e.g. ALIAS FOR a.b), matching PG's three
 * arms. PG resolves it to an existing variable's namespace entry; we capture the
 * name as text.
 */
decl_aliasitem:
		T_WORD
			{
				$$ = $1
			}
	|	unreserved_keyword
			{
				$$ = $1
			}
	|	T_CWORD
			{
				$$ = $1
			}
	;

/*
 * PG: pl_gram.y decl_varname. The declared name — a plain word or an unreserved
 * keyword (so a variable may be named like an unreserved keyword). PG also
 * registers the name in the block namespace and, from there, rejects a duplicate
 * declaration and optionally warns on a shadowed outer variable. We build no
 * namespace, so the name is captured as text; the duplicate-declaration check is
 * done structurally over the finished DECLARE section (see decl_sect). The
 * shadow-variable check is not ported — it is off by default in PG (gated behind
 * plpgsql.extra_warnings/extra_errors) and needs the cross-block namespace.
 */
decl_varname:
		T_WORD
			{
				$$ = $1
			}
	|	unreserved_keyword
			{
				$$ = $1
			}
	;

decl_const:
		/* empty */
			{
				$$ = false
			}
	|	K_CONSTANT
			{
				$$ = true
			}
	;

/* PG: pl_gram.y decl_datatype — an empty rule whose action reads the type via
 * read_datatype (our readDatatype). */
decl_datatype:
		/* empty */
			{
				lx := plpgsqllex.(*lexer)
				lx.beginScan(plpgsqlrcvr.char)
				plpgsqlrcvr.char = -1
				plpgsqltoken = -1
				$$ = lx.readDatatype()
			}
	;

/*
 * PG: pl_gram.y decl_collate. Optional COLLATE clause on a variable declaration.
 * PG resolves the name to a collation OID via get_collation_oid; we capture it as
 * text (a plain word, an unreserved keyword, or a qualified compound name).
 * readDatatype stops at K_COLLATE so the grammar reaches this. Cursor-arg types
 * have no decl_collate, so a COLLATE there is a syntax error — matching PG.
 */
decl_collate:
		/* empty */
			{
				$$ = ""
			}
	|	K_COLLATE T_WORD
			{
				$$ = $2
			}
	|	K_COLLATE unreserved_keyword
			{
				$$ = $2
			}
	|	K_COLLATE T_CWORD
			{
				$$ = $2
			}
	;

decl_notnull:
		/* empty */
			{
				$$ = false
			}
	|	K_NOT K_NULL
			{
				$$ = true
			}
	;

/* PG: pl_gram.y decl_defval / decl_defkey — the initializer, read via
 * read_sql_expression (our readSQLExpr) after a DEFAULT or := / =. */
decl_defval:
		';'
			{
				$$ = nil
			}
	|	decl_defkey
			{
				lx := plpgsqllex.(*lexer)
				lx.beginScan(plpgsqlrcvr.char)
				plpgsqlrcvr.char = -1
				plpgsqltoken = -1
				$$ = lx.readSQLExpr()
			}
	;

decl_defkey:
		assign_operator
	|	K_DEFAULT
	;

assign_operator:
		'='
	|	COLON_EQUALS
	;

proc_sect:
		/* empty */
			{
				$$ = nil
			}
	|	proc_sect proc_stmt
			{
				// Mirror PG: don't link NULL statements into the body list.
				if $2 == nil {
					$$ = $1
				} else {
					$$ = append($1, $2)
				}
			}
	;

/*
 * PG: pl_gram.y proc_stmt. Alternatives are kept in PG's order for side-by-side
 * reading. stmt_assign sits between `pl_block ';'` and `stmt_if`, dispatched from
 * a resolved T_DATUM target — a word that resolved to a declared variable and is
 * followed by an assignment operator or subscript. A word that does NOT resolve
 * (T_WORD/T_CWORD) is only ever a stmt_execsql.
 */
proc_stmt:
		pl_block ';'
			{
				$$ = $1
			}
	|	stmt_assign
			{
				$$ = $1
			}
	|	stmt_if
			{
				$$ = $1
			}
	|	stmt_case
			{
				$$ = $1
			}
	|	stmt_loop
			{
				$$ = $1
			}
	|	stmt_while
			{
				$$ = $1
			}
	|	stmt_for
			{
				$$ = $1
			}
	|	stmt_foreach_a
			{
				$$ = $1
			}
	|	stmt_exit
			{
				$$ = $1
			}
	|	stmt_return
			{
				$$ = $1
			}
	|	stmt_raise
			{
				$$ = $1
			}
	|	stmt_assert
			{
				$$ = $1
			}
	|	stmt_execsql
			{
				$$ = $1
			}
	|	stmt_dynexecute
			{
				$$ = $1
			}
	|	stmt_perform
			{
				$$ = $1
			}
	|	stmt_call
			{
				$$ = $1
			}
	|	stmt_getdiag
			{
				$$ = $1
			}
	|	stmt_open
			{
				$$ = $1
			}
	|	stmt_fetch
			{
				$$ = $1
			}
	|	stmt_move
			{
				$$ = $1
			}
	|	stmt_close
			{
				$$ = $1
			}
	|	stmt_null
			{
				$$ = $1
			}
	|	stmt_commit
			{
				$$ = $1
			}
	|	stmt_rollback
			{
				$$ = $1
			}
	;

/*
 * PG: pl_gram.y stmt_perform. PERFORM expr — run a query for its side effects.
 * We capture the expression after PERFORM (PG substitutes SELECT for execution;
 * we keep the text and re-emit PERFORM on deparse). PG reads it in
 * RAW_PARSE_DEFAULT mode (it parses the substituted `SELECT …` as a statement),
 * so we record the same mode.
 */
stmt_perform:
		K_PERFORM
			{
				lx := plpgsqllex.(*lexer)
				lx.beginScan(plpgsqlrcvr.char)
				plpgsqlrcvr.char = -1
				plpgsqltoken = -1
				stmt := plpgsqlast.NewPLpgSQL_stmt_perform()
				stmt.Expr, _ = lx.readSQLConstruct(plpgsqlast.RAW_PARSE_DEFAULT, ';')
				$$ = stmt
			}
	;

/*
 * PG: pl_gram.y stmt_call (which handles both CALL and DO via read_sql_stmt).
 * CALL proc(...) and DO $$...$$ — both capture the whole statement text (keyword
 * included) from the keyword's byte offset; IsCall distinguishes them.
 */
stmt_call:
		K_CALL
			{
				lx := plpgsqllex.(*lexer)
				startPos := plpgsqlDollar[1].location
				lx.beginScan(plpgsqlrcvr.char)
				plpgsqlrcvr.char = -1
				plpgsqltoken = -1
				stmt := plpgsqlast.NewPLpgSQL_stmt_call(true)
				stmt.Expr = makeExpr(lx.scanStmtText(false, startPos), plpgsqlast.RAW_PARSE_DEFAULT)
				$$ = stmt
			}
	|	K_DO
			{
				lx := plpgsqllex.(*lexer)
				startPos := plpgsqlDollar[1].location
				lx.beginScan(plpgsqlrcvr.char)
				plpgsqlrcvr.char = -1
				plpgsqltoken = -1
				stmt := plpgsqlast.NewPLpgSQL_stmt_call(false)
				stmt.Expr = makeExpr(lx.scanStmtText(false, startPos), plpgsqlast.RAW_PARSE_DEFAULT)
				$$ = stmt
			}
	;

/*
 * PG: pl_gram.y stmt_getdiag / getdiag_area_opt / getdiag_list /
 * getdiag_list_item / getdiag_item. GET [CURRENT|STACKED] DIAGNOSTICS
 * target := item [, …]. getdiag_item is an empty production that reads the kind
 * keyword itself (PG does the same, to run tok_is_keyword against a possible
 * variable of that name — our readGetDiagItem). Per-item validity by area is
 * checked after the list is built (checkGetDiagItems, PG's per-item switch).
 */
stmt_getdiag:
		K_GET getdiag_area_opt K_DIAGNOSTICS getdiag_list ';'
			{
				stmt := plpgsqlast.NewPLpgSQL_stmt_getdiag()
				stmt.IsStacked = $2
				stmt.DiagItems = $4
				plpgsqllex.(*lexer).checkGetDiagItems(stmt)
				$$ = stmt
			}
	;

getdiag_area_opt:
		/* empty */
			{
				$$ = false
			}
	|	K_CURRENT
			{
				$$ = false
			}
	|	K_STACKED
			{
				$$ = true
			}
	;

/*
 * PG: pl_gram.y stmt_assign. An assignment whose target is a resolved variable
 * (T_DATUM). PG reads the whole `target := rhs` as one ASSIGN-mode SQL construct;
 * we keep the parse-level split — the target name from the resolved datum and the
 * RHS as a separate expression — so the assignment deparses as `target := rhs;`
 * (makeAssignStmt). $1's byte offset (plpgsqlDollar[1].location) is the target's
 * start, needed to capture a subscripted target (`arr[i]`).
 */
stmt_assign:
		T_DATUM
			{
				lx := plpgsqllex.(*lexer)
				startPos := plpgsqlDollar[1].location
				lx.beginScan(plpgsqlrcvr.char)
				plpgsqlrcvr.char = -1
				plpgsqltoken = -1
				$$ = lx.makeAssignStmt($1, startPos)
			}
	;

getdiag_list:
		getdiag_list ',' getdiag_list_item
			{
				$$ = appendDiagItem($1, $3)
			}
	|	getdiag_list_item
			{
				$$ = appendDiagItem(nil, $1)
			}
	;

getdiag_list_item:
		getdiag_target assign_operator getdiag_item
			{
				$$ = plpgsqlast.NewPLpgSQL_diag_item(plpgsqlast.PLpgSQL_getdiag_kind($3), $1)
			}
	;

getdiag_item:
		/* empty */
			{
				lx := plpgsqllex.(*lexer)
				lx.beginScan(plpgsqlrcvr.char)
				plpgsqlrcvr.char = -1
				plpgsqltoken = -1
				$$ = lx.readGetDiagItem()
			}
	;

/*
 * PG: pl_gram.y getdiag_target. The assignment target of a diagnostic item. PG
 * resolves it to a scalar T_DATUM (its T_WORD arm only exists to give a nicer
 * error, and it rejects ROW/REC datums and array-element targets); with no
 * resolution we capture the name as text, as for an assignment target.
 */
getdiag_target:
		T_DATUM
			{
				// A resolved variable is the normal case. We keep the name as text
				// (deparse target) but reject assignment to a CONSTANT, as PG does.
				// PG's ROW/REC/array-target rejections need type info we lack.
				plpgsqllex.(*lexer).checkAssignable($1.datum)
				$$ = $1.name
			}
	|	T_WORD
			{
				$$ = $1
			}
	|	T_CWORD
			{
				$$ = $1
			}
	;

/*
 * PG: pl_gram.y stmt_if / stmt_elsifs / stmt_else. IF … THEN … [ELSIF … THEN …]
 * [ELSE …] END IF. Each condition is captured as a PLpgSQL_expr by the
 * expr_until_then scanner (PG's read_sql_expression up to THEN).
 */
stmt_if:
		K_IF expr_until_then proc_sect stmt_elsifs stmt_else K_END K_IF ';'
			{
				stmt := plpgsqlast.NewPLpgSQL_stmt_if()
				stmt.Cond = $2
				stmt.ThenBody = $3
				stmt.ElsifList = $4
				stmt.ElseBody = $5
				$$ = stmt
			}
	;

stmt_elsifs:
		/* empty */
			{
				$$ = nil
			}
	|	stmt_elsifs K_ELSIF expr_until_then proc_sect
			{
				ei := plpgsqlast.NewPLpgSQL_if_elsif()
				ei.Cond = $3
				ei.Stmts = $4
				$$ = appendElsif($1, ei)
			}
	;

stmt_else:
		/* empty */
			{
				$$ = nil
			}
	|	K_ELSE proc_sect
			{
				$$ = $2
			}
	;

/*
 * PG: pl_gram.y stmt_case / opt_expr_until_when / case_when_list / case_when /
 * opt_case_else, and make_case. CASE is searched (`CASE WHEN … THEN …`) or simple
 * (`CASE expr WHEN … THEN …`); opt_expr_until_when peeks for WHEN to decide which
 * and leaves a WHEN token for case_when. A present-but-empty ELSE is kept
 * distinct from no ELSE via HaveElse (PG's have_else / list-with-NULL hack), since
 * a present ELSE suppresses the runtime CASE_NOT_FOUND error. PG's simple-CASE
 * rewrite to `var IN (…)` is an execution step we omit.
 */
stmt_case:
		K_CASE opt_expr_until_when case_when_list opt_case_else K_END K_CASE ';'
			{
				stmt := plpgsqlast.NewPLpgSQL_stmt_case()
				stmt.TestExpr = $2
				stmt.WhenList = $3
				// A present-but-empty ELSE is a non-nil slice (PG's list-with-NULL
				// hack); no ELSE is nil. HaveElse preserves the distinction, which
				// PG uses to suppress CASE_NOT_FOUND.
				stmt.HaveElse = $4 != nil
				stmt.ElseStmts = $4
				$$ = stmt
			}
	;

opt_expr_until_when:
		/* empty */
			{
				lx := plpgsqllex.(*lexer)
				lx.beginScan(plpgsqlrcvr.char)
				plpgsqlrcvr.char = -1
				plpgsqltoken = -1
				$$ = lx.readCaseTestExpr()
			}
	;

case_when_list:
		case_when_list case_when
			{
				$$ = appendCaseWhen($1, $2)
			}
	|	case_when
			{
				$$ = appendCaseWhen(nil, $1)
			}
	;

case_when:
		K_WHEN expr_until_then proc_sect
			{
				cw := plpgsqlast.NewPLpgSQL_case_when()
				cw.Expr = $2
				cw.Stmts = $3
				$$ = cw
			}
	;

opt_case_else:
		/* empty */
			{
				$$ = nil
			}
	|	K_ELSE proc_sect
			{
				// Return a non-nil slice even for an empty ELSE body, so stmt_case
				// can tell "ELSE present" from "no ELSE" (PG's list-with-NULL hack).
				if $2 == nil {
					$$ = []plpgsqlast.Stmt{}
				} else {
					$$ = $2
				}
			}
	;

/*
 * PG: pl_gram.y stmt_loop / stmt_while (and loop_body). Unconditional LOOP and
 * WHILE, both sharing loop_body for the `… END LOOP <label>;` tail. opt_loop_label
 * mirrors PG's (identical to opt_block_label, kept separate to track the grammar).
 * The end label is validated against the start label (check_labels), like blocks.
 */
stmt_loop:
		opt_loop_label K_LOOP loop_body
			{
				stmt := plpgsqlast.NewPLpgSQL_stmt_loop()
				stmt.Label = $1
				stmt.Body = $3.stmts
				if err := checkLabels($1, $3.endLabel); err != nil {
					plpgsqllex.Error(err.Error())
				}
				plpgsqllex.(*lexer).ns.pop() // close the loop label scope
				$$ = stmt
			}
	;

stmt_while:
		opt_loop_label K_WHILE expr_until_loop loop_body
			{
				stmt := plpgsqlast.NewPLpgSQL_stmt_while()
				stmt.Label = $1
				stmt.Cond = $3
				stmt.Body = $4.stmts
				if err := checkLabels($1, $4.endLabel); err != nil {
					plpgsqllex.Error(err.Error())
				}
				plpgsqllex.(*lexer).ns.pop() // close the loop label scope
				$$ = stmt
			}
	;

/*
 * PG: pl_gram.y stmt_for / for_control. for_control does the heavy lifting in a
 * manual scan (readForControl, porting PG's for_control action): integer FOR
 * (`lower .. upper [BY step]`) vs query FOR vs dynamic FOR (`IN EXECUTE`). It
 * returns the node sans label/body, which stmt_for fills in (PG checks cmd_type;
 * we type-switch). A bound-cursor FOR loop is not distinguished from a query FOR
 * (that needs a resolved refcursor T_DATUM), so it reads as a query FOR.
 */
stmt_for:
		opt_loop_label K_FOR for_control loop_body
			{
				switch s := $3.(type) {
				case *plpgsqlast.PLpgSQL_stmt_fori:
					s.Label = $1
					s.Body = $4.stmts
				case *plpgsqlast.PLpgSQL_stmt_fors:
					s.Label = $1
					s.Body = $4.stmts
				case *plpgsqlast.PLpgSQL_stmt_dynfors:
					s.Label = $1
					s.Body = $4.stmts
				}
				if err := checkLabels($1, $4.endLabel); err != nil {
					plpgsqllex.Error(err.Error())
				}
				plpgsqllex.(*lexer).ns.pop() // close the loop label scope
				$$ = $3
			}
	;

for_control:
		for_variable K_IN
			{
				lx := plpgsqllex.(*lexer)
				lx.beginScan(plpgsqlrcvr.char)
				plpgsqlrcvr.char = -1
				plpgsqltoken = -1
				$$ = lx.readForControl($1)
			}
	;

/*
 * The FOR loop target(s). PG uses T_DATUM/T_WORD/T_CWORD and resolves them; we
 * have no resolution, so a target is a single word or compound name captured as
 * text. readForVariable peeks past the first name for a comma-separated list
 * (valid only for a loop over rows, not an integer FOR — checked in
 * readForControl). A compound target (T_CWORD) is kept as text: PG rejects an
 * *unresolved* compound (cword_is_not_variable) but accepts one that resolves —
 * e.g. a label-qualified variable `lbl.a` or a record field — and without
 * resolution we cannot tell them apart, so we accept it (no-resolution divergence).
 */
for_variable:
		T_DATUM
			{
				lx := plpgsqllex.(*lexer)
				lx.beginScan(plpgsqlrcvr.char)
				plpgsqlrcvr.char = -1
				plpgsqltoken = -1
				// A resolved target: a scalar (which may head a comma list) or a
				// record; the resolved datum drives the loop-target checks.
				$$ = lx.readForVariableDatum($1.name, $1.datum)
			}
	|	T_WORD
			{
				lx := plpgsqllex.(*lexer)
				lx.beginScan(plpgsqlrcvr.char)
				plpgsqlrcvr.char = -1
				plpgsqltoken = -1
				// An unresolved plain word: valid only as an integer-loop variable.
				$$ = lx.readForVariableWord($1, true)
			}
	|	T_CWORD
			{
				lx := plpgsqllex.(*lexer)
				lx.beginScan(plpgsqlrcvr.char)
				plpgsqlrcvr.char = -1
				plpgsqltoken = -1
				// An unresolvable compound; accepted as a list member, never as a
				// known single variable.
				$$ = lx.readForVariableWord($1, false)
			}
	;

/*
 * PG: pl_gram.y stmt_foreach_a / foreach_slice. FOREACH var [SLICE n] IN ARRAY
 * expr LOOP … END LOOP. The target must be a known variable or list of variables
 * (checkForeachTarget); we keep its name as text for deparse.
 */
stmt_foreach_a:
		opt_loop_label K_FOREACH for_variable foreach_slice K_IN K_ARRAY expr_until_loop loop_body
			{
				plpgsqllex.(*lexer).checkForeachTarget($3)
				stmt := plpgsqlast.NewPLpgSQL_stmt_foreach_a()
				stmt.Label = $1
				stmt.Var = $3.name
				stmt.Slice = $4
				stmt.Expr = $7
				stmt.Body = $8.stmts
				if err := checkLabels($1, $8.endLabel); err != nil {
					plpgsqllex.Error(err.Error())
				}
				plpgsqllex.(*lexer).ns.pop() // close the loop label scope
				$$ = stmt
			}
	;

foreach_slice:
		/* empty */
			{
				$$ = 0
			}
	|	K_SLICE ICONST
			{
				$$ = $2
			}
	;

/*
 * PG: pl_gram.y stmt_exit / exit_type. EXIT / CONTINUE [label] [WHEN cond]. The
 * label and loop-nesting are validated against the namespace (label exists,
 * CONTINUE forbids a block label, and an unlabelled EXIT/CONTINUE must be inside a
 * loop) — checkExit. The WHEN condition is scanned up to ';'.
 */
stmt_exit:
		exit_type opt_label opt_exitcond
			{
				stmt := plpgsqlast.NewPLpgSQL_stmt_exit($1)
				stmt.Label = $2
				stmt.Cond = $3
				plpgsqllex.(*lexer).checkExit($1, $2)
				$$ = stmt
			}
	;

exit_type:
		K_EXIT
			{
				$$ = true
			}
	|	K_CONTINUE
			{
				$$ = false
			}
	;

/*
 * PG: pl_gram.y stmt_return (and make_return_stmt / make_return_next_stmt /
 * make_return_query_stmt). RETURN [expr], RETURN NEXT expr, RETURN QUERY [EXECUTE]
 * query. makeReturnStmt peeks the token after RETURN to pick the form. PG's
 * set-returning / void / out-param context checks need compile context we lack.
 */
stmt_return:
		K_RETURN
			{
				lx := plpgsqllex.(*lexer)
				lx.beginScan(plpgsqlrcvr.char)
				plpgsqlrcvr.char = -1
				plpgsqltoken = -1
				$$ = lx.makeReturnStmt()
			}
	;

/*
 * PG: pl_gram.y stmt_raise (and read_raise_options / check_raise_parameters).
 * RAISE [level] [condname | SQLSTATE 'code' | 'message' [, arg …]] [USING opt =
 * expr, …]. makeRaiseStmt hand-scans the whole statement as PG's stmt_raise
 * action does, since the shape after RAISE is decided token by token. A bare
 * RAISE re-throws.
 */
stmt_raise:
		K_RAISE
			{
				lx := plpgsqllex.(*lexer)
				lx.beginScan(plpgsqlrcvr.char)
				plpgsqlrcvr.char = -1
				plpgsqltoken = -1
				$$ = lx.makeRaiseStmt()
			}
	;

/*
 * PG: pl_gram.y stmt_assert. ASSERT cond [, message]. makeAssertStmt scans the
 * condition up to ',' or ';' (read_sql_expression2) and, if a comma followed, the
 * message up to ';' (read_sql_expression).
 */
stmt_assert:
		K_ASSERT
			{
				lx := plpgsqllex.(*lexer)
				lx.beginScan(plpgsqlrcvr.char)
				plpgsqlrcvr.char = -1
				plpgsqltoken = -1
				$$ = lx.makeAssertStmt()
			}
	;

loop_body:
		proc_sect K_END K_LOOP opt_label ';'
			{
				$$ = loopBody{stmts: $1, endLabel: $4}
			}
	;

/*
 * PG: pl_gram.y stmt_execsql (and make_execsql_stmt). Any SQL statement not
 * handled by a PL/pgSQL production, keyed on the same first tokens as PG
 * (K_IMPORT / K_INSERT / K_MERGE / T_WORD / T_CWORD). PG's T_WORD/T_CWORD arms
 * route a leading resolved variable to stmt_assign; with no resolution a
 * word-initiated statement is dispatched by makeWordStmt, which peeks for an
 * assignment operator (PG errors there since a real variable would be T_DATUM; we
 * build the assignment instead). $1's byte offset (plpgsqlDollar[1].location) is
 * the start of the captured text, since the first token is already consumed.
 */
stmt_execsql:
		K_IMPORT
			{
				lx := plpgsqllex.(*lexer)
				startPos := plpgsqlDollar[1].location
				lx.beginScan(plpgsqlrcvr.char)
				plpgsqlrcvr.char = -1
				plpgsqltoken = -1
				stmt := plpgsqlast.NewPLpgSQL_stmt_execsql()
				stmt.Sqlstmt = makeExpr(lx.scanStmtText(false, startPos), plpgsqlast.RAW_PARSE_DEFAULT)
				$$ = stmt
			}
	|	K_INSERT
			{
				lx := plpgsqllex.(*lexer)
				startPos := plpgsqlDollar[1].location
				lx.beginScan(plpgsqlrcvr.char)
				plpgsqlrcvr.char = -1
				plpgsqltoken = -1
				stmt := plpgsqlast.NewPLpgSQL_stmt_execsql()
				stmt.Sqlstmt = makeExpr(lx.scanStmtText(false, startPos), plpgsqlast.RAW_PARSE_DEFAULT)
				$$ = stmt
			}
	|	K_MERGE
			{
				lx := plpgsqllex.(*lexer)
				startPos := plpgsqlDollar[1].location
				lx.beginScan(plpgsqlrcvr.char)
				plpgsqlrcvr.char = -1
				plpgsqltoken = -1
				stmt := plpgsqlast.NewPLpgSQL_stmt_execsql()
				stmt.Sqlstmt = makeExpr(lx.scanStmtText(false, startPos), plpgsqlast.RAW_PARSE_DEFAULT)
				$$ = stmt
			}
	|	T_WORD
			{
				lx := plpgsqllex.(*lexer)
				startPos := plpgsqlDollar[1].location
				lx.beginScan(plpgsqlrcvr.char)
				plpgsqlrcvr.char = -1
				plpgsqltoken = -1
				$$ = lx.makeWordStmt($1, startPos)
			}
	|	T_CWORD
			{
				lx := plpgsqllex.(*lexer)
				startPos := plpgsqlDollar[1].location
				lx.beginScan(plpgsqlrcvr.char)
				plpgsqlrcvr.char = -1
				plpgsqltoken = -1
				$$ = lx.makeWordStmt($1, startPos)
			}
	;

/*
 * PG: pl_gram.y stmt_dynexecute. EXECUTE query [INTO [STRICT] target]
 * [USING args] — dynamic SQL. makeDynExecute scans the query and the INTO/USING
 * clauses (either order), mirroring PG's stmt_dynexecute action. This is the
 * primary statement the Tier-1 dynamic-EXECUTE policy inspects.
 */
stmt_dynexecute:
		K_EXECUTE
			{
				lx := plpgsqllex.(*lexer)
				lx.beginScan(plpgsqlrcvr.char)
				plpgsqlrcvr.char = -1
				plpgsqltoken = -1
				$$ = lx.makeDynExecute()
			}
	;

/*
 * PG: pl_gram.y stmt_open / stmt_fetch / stmt_move / opt_fetch_direction /
 * stmt_close (and read_fetch_direction, read_cursor_args). The cursor is a plain
 * name (T_WORD) since we have no resolution — PG uses a resolved refcursor
 * T_DATUM. PG branches OPEN on cursor_explicit_expr; makeOpen instead disambiguates
 * syntactically (bound-args vs FOR-query vs bare). FETCH/MOVE share a node via
 * IsMove and use readFetchDirection for the direction clause.
 */
stmt_open:
		K_OPEN cursor_variable
			{
				lx := plpgsqllex.(*lexer)
				lx.beginScan(plpgsqlrcvr.char)
				plpgsqlrcvr.char = -1
				plpgsqltoken = -1
				$$ = lx.makeOpen($2)
			}
	;

stmt_fetch:
		K_FETCH opt_fetch_direction cursor_variable K_INTO
			{
				lx := plpgsqllex.(*lexer)
				lx.beginScan(plpgsqlrcvr.char)
				plpgsqlrcvr.char = -1
				plpgsqltoken = -1
				fetch := $2
				fetch.Curvar = $3
				fetch.Target = lx.readFetchTarget()
				// A FETCH pulls one row into one target; multi-row directions
				// (ALL, a count) are MOVE-only. Mirrors PG's stmt_fetch check.
				if fetch.ReturnsMultipleRows {
					plpgsqllex.Error("FETCH statement cannot return multiple rows")
				}
				$$ = fetch
			}
	;

stmt_move:
		K_MOVE opt_fetch_direction cursor_variable ';'
			{
				fetch := $2
				fetch.Curvar = $3
				fetch.IsMove = true
				$$ = fetch
			}
	;

opt_fetch_direction:
		/* empty */
			{
				lx := plpgsqllex.(*lexer)
				lx.beginScan(plpgsqlrcvr.char)
				plpgsqlrcvr.char = -1
				plpgsqltoken = -1
				$$ = lx.readFetchDirection()
			}
	;

stmt_close:
		K_CLOSE cursor_variable ';'
			{
				stmt := plpgsqlast.NewPLpgSQL_stmt_close()
				stmt.Curvar = $2
				$$ = stmt
			}
	;

stmt_null:
		K_NULL ';'
			{
				// Like PG, we build no node for NULL; it carries no meaning.
				$$ = nil
			}
	;

/*
 * PG: pl_gram.y stmt_commit / stmt_rollback / opt_transaction_chain. COMMIT /
 * ROLLBACK [AND [NO] CHAIN] — transaction control inside a procedure.
 */
stmt_commit:
		K_COMMIT opt_transaction_chain ';'
			{
				stmt := plpgsqlast.NewPLpgSQL_stmt_commit()
				stmt.Chain = $2
				$$ = stmt
			}
	;

stmt_rollback:
		K_ROLLBACK opt_transaction_chain ';'
			{
				stmt := plpgsqlast.NewPLpgSQL_stmt_rollback()
				stmt.Chain = $2
				$$ = stmt
			}
	;

opt_transaction_chain:
		K_AND K_CHAIN
			{
				$$ = true
			}
	|	K_AND K_NO K_CHAIN
			{
				$$ = false
			}
	|	/* empty */
			{
				$$ = false
			}
	;

/*
 * PG: pl_gram.y cursor_variable. A cursor reference. PG's only accepting arm is a
 * T_DATUM that must be a simple scalar refcursor variable — it rejects record
 * fields and array elements. PG's T_WORD/T_CWORD arms are pure rejection paths
 * (word_is_not_variable / cword_is_not_variable) that only give a nicer message.
 * We accept the resolved cursor (T_DATUM), keeping a T_WORD arm as the
 * unresolved-name fallback; there is no T_CWORD arm, so a compound name is a
 * syntax error, matching PG's accept/reject. The must-be-refcursor and boundness
 * checks are applied where the cursor is used, not here.
 */
cursor_variable:
		T_DATUM
			{
				// A declared cursor resolves to a scalar refcursor T_DATUM. We keep
				// its name as text; the must-be-a-simple-refcursor and boundness
				// checks are parity checks handled where the cursor is used.
				$$ = $1.name
			}
	|	T_WORD
			{
				$$ = $1
			}
	;

/*
 * PG: pl_gram.y exception_sect / proc_exceptions / proc_exception /
 * proc_conditions. The EXCEPTION section of a block: EXCEPTION followed by one or
 * more WHEN handler clauses. PG injects the implicit sqlstate/sqlerrm namespace
 * variables here in a mid-rule action; we have no namespace, so this is a single
 * action.
 */
exception_sect:
		/* empty */
			{
				$$ = nil
			}
	|	K_EXCEPTION proc_exceptions
			{
				block := plpgsqlast.NewPLpgSQL_exception_block()
				block.ExcList = $2
				$$ = block
			}
	;

proc_exceptions:
		proc_exceptions proc_exception
			{
				$$ = appendException($1, $2)
			}
	|	proc_exception
			{
				$$ = appendException(nil, $1)
			}
	;

proc_exception:
		K_WHEN proc_conditions K_THEN proc_sect
			{
				exc := plpgsqlast.NewPLpgSQL_exception()
				exc.Conditions = $2
				exc.Action = $4
				$$ = exc
			}
	;

proc_conditions:
		proc_conditions K_OR proc_condition
			{
				$$ = appendCondition($1, $3)
			}
	|	proc_condition
			{
				$$ = appendCondition(nil, $1)
			}
	;

/*
 * PG: pl_gram.y proc_condition. A single WHEN condition — normally a named error
 * condition, captured as text (PG's plpgsql_parse_err_condition resolution, which
 * also rejects unknown names, is dropped). The identifier `sqlstate` is special:
 * it is followed by a string-literal SQLSTATE code, read here in the action (PG
 * does the same via yylex) and validated like RAISE's (readSQLStateCondition).
 * The beginScan dance is robust to whether goyacc read the SCONST as a lookahead
 * or reduced by default.
 */
proc_condition:
		any_identifier
			{
				if $1 == "sqlstate" {
					lx := plpgsqllex.(*lexer)
					lx.beginScan(plpgsqlrcvr.char)
					plpgsqlrcvr.char = -1
					plpgsqltoken = -1
					$$ = lx.readSQLStateCondition()
				} else {
					$$ = plpgsqlast.NewPLpgSQL_condition($1)
				}
			}
	;

/*
 * PG: pl_gram.y expr_until_semi / expr_until_then / expr_until_loop. Each is an
 * empty rule whose action manually scans an embedded SQL expression up to a
 * terminator via read_sql_expression (our readSQLExprUntil). The beginScan /
 * clear-lookahead dance matches decl_datatype.
 */
expr_until_semi:
		/* empty */
			{
				lx := plpgsqllex.(*lexer)
				lx.beginScan(plpgsqlrcvr.char)
				plpgsqlrcvr.char = -1
				plpgsqltoken = -1
				$$ = lx.readSQLExprUntil(';')
			}
	;

expr_until_then:
		/* empty */
			{
				lx := plpgsqllex.(*lexer)
				lx.beginScan(plpgsqlrcvr.char)
				plpgsqlrcvr.char = -1
				plpgsqltoken = -1
				$$ = lx.readSQLExprUntil(K_THEN)
			}
	;

expr_until_loop:
		/* empty */
			{
				lx := plpgsqllex.(*lexer)
				lx.beginScan(plpgsqlrcvr.char)
				plpgsqlrcvr.char = -1
				plpgsqltoken = -1
				$$ = lx.readSQLExprUntil(K_LOOP)
			}
	;

/*
 * PG: pl_gram.y opt_block_label / opt_loop_label / opt_label / opt_exitcond /
 * any_identifier. The label options (PG registers labels in the namespace; we
 * keep them as text and validate end labels structurally via check_labels).
 */
opt_block_label:
		/* empty */
			{
				// Open a new namespace level for this block. Variables declared in
				// its DECLARE section register here and are popped at block end
				// (pl_block). PG's plpgsql_ns_push(NULL, LABEL_BLOCK).
				plpgsqllex.(*lexer).ns.push("", labelBlock)
				$$ = ""
			}
	|	LESS_LESS any_identifier GREATER_GREATER
			{
				plpgsqllex.(*lexer).ns.push($2, labelBlock)
				$$ = $2
			}
	;

opt_loop_label:
		/* empty */
			{
				// Open a LOOP-labelled namespace level so EXIT/CONTINUE can find the
				// enclosing loop and validate labels; popped at loop end. PG's
				// plpgsql_ns_push(NULL, LABEL_LOOP).
				plpgsqllex.(*lexer).ns.push("", labelLoop)
				$$ = ""
			}
	|	LESS_LESS any_identifier GREATER_GREATER
			{
				plpgsqllex.(*lexer).ns.push($2, labelLoop)
				$$ = $2
			}
	;

opt_label:
		/* empty */
			{
				$$ = ""
			}
	|	any_identifier
			{
				$$ = $1
			}
	;

opt_exitcond:
		';'
			{
				$$ = nil
			}
	|	K_WHEN expr_until_semi
			{
				$$ = $2
			}
	;

any_identifier:
		T_WORD
			{
				$$ = $1
			}
	|	unreserved_keyword
			{
				$$ = $1
			}
	|	T_DATUM
			{
				// A name that resolved to a variable can still be used as a label
				// (PG allows T_DATUM here because the scanner tried to resolve it).
				// A composite name (nnames > 1) is not a valid identifier.
				if $1.nnames != 1 {
					plpgsqllex.Error("syntax error")
				}
				$$ = $1.name
			}
	;

/*
 * PG: pl_gram.y unreserved_keyword. Unreserved keywords usable as identifiers
 * (labels, variable names, etc.), listed exactly as in PG's rule; the default
 * action carries each keyword's text (the K_* tokens are <str> and the lexer
 * fills it in).
 */
unreserved_keyword:
		K_ABSOLUTE
	|	K_ALIAS
	|	K_AND
	|	K_ARRAY
	|	K_ASSERT
	|	K_BACKWARD
	|	K_CALL
	|	K_CHAIN
	|	K_CLOSE
	|	K_COLLATE
	|	K_COLUMN
	|	K_COLUMN_NAME
	|	K_COMMIT
	|	K_CONSTANT
	|	K_CONSTRAINT
	|	K_CONSTRAINT_NAME
	|	K_CONTINUE
	|	K_CURRENT
	|	K_CURSOR
	|	K_DATATYPE
	|	K_DEBUG
	|	K_DEFAULT
	|	K_DETAIL
	|	K_DIAGNOSTICS
	|	K_DO
	|	K_DUMP
	|	K_ELSIF
	|	K_ERRCODE
	|	K_ERROR
	|	K_EXCEPTION
	|	K_EXIT
	|	K_FETCH
	|	K_FIRST
	|	K_FORWARD
	|	K_GET
	|	K_HINT
	|	K_IMPORT
	|	K_INFO
	|	K_INSERT
	|	K_IS
	|	K_LAST
	|	K_LOG
	|	K_MERGE
	|	K_MESSAGE
	|	K_MESSAGE_TEXT
	|	K_MOVE
	|	K_NEXT
	|	K_NO
	|	K_NOTICE
	|	K_OPEN
	|	K_OPTION
	|	K_PERFORM
	|	K_PG_CONTEXT
	|	K_PG_DATATYPE_NAME
	|	K_PG_EXCEPTION_CONTEXT
	|	K_PG_EXCEPTION_DETAIL
	|	K_PG_EXCEPTION_HINT
	|	K_PG_ROUTINE_OID
	|	K_PRINT_STRICT_PARAMS
	|	K_PRIOR
	|	K_QUERY
	|	K_RAISE
	|	K_RELATIVE
	|	K_RETURN
	|	K_RETURNED_SQLSTATE
	|	K_REVERSE
	|	K_ROLLBACK
	|	K_ROW_COUNT
	|	K_ROWTYPE
	|	K_SCHEMA
	|	K_SCHEMA_NAME
	|	K_SCROLL
	|	K_SLICE
	|	K_SQLSTATE
	|	K_STACKED
	|	K_TABLE
	|	K_TABLE_NAME
	|	K_TYPE
	|	K_USE_COLUMN
	|	K_USE_VARIABLE
	|	K_VARIABLE_CONFLICT
	|	K_WARNING
	;

%%
