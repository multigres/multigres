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
	bval      bool
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
%type <block>    pl_block
%type <declsect> decl_sect
%type <datums>   decl_stmts
%type <datum>    decl_stmt decl_statement
%type <typ>      decl_datatype
%type <expr>     decl_defval
%type <bval>     decl_const decl_notnull
%type <stmts>    proc_sect stmt_else opt_case_else
%type <stmt>     proc_stmt stmt_null stmt_if stmt_loop stmt_while stmt_exit
%type <stmt>     stmt_for stmt_foreach_a stmt_case for_control
%type <stmt>     stmt_execsql stmt_perform stmt_call stmt_return stmt_dynexecute
%type <stmt>     stmt_open stmt_fetch stmt_move stmt_close
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
%type <str>      for_variable

%start pl_function

%%

/*
 * A PL/pgSQL body is a single top-level block, optionally followed by a
 * trailing semicolon. Ported from pl_gram.y (PG's comp_options preamble is
 * deferred). The block becomes the function's Action.
 */
pl_function:
		pl_block opt_semi
			{
				fn := plpgsqlast.NewPLpgSQL_function()
				fn.Action = $1
				if l, ok := plpgsqllex.(plpgsqlResultSetter); ok {
					l.SetResult(fn)
				}
			}
	;

opt_semi:
		/* empty */
	|	';'
	;

/*
 * The block: an optional DECLARE section, BEGIN, a statement list, END, and an
 * optional matching end label. EXCEPTION (exception_sect) lands in a later
 * chunk.
 */
pl_block:
		decl_sect K_BEGIN proc_sect K_END opt_label
			{
				block := plpgsqlast.NewPLpgSQL_stmt_block()
				block.Label = $1.label
				block.Decls = $1.decls
				block.Body = $3
				if err := checkLabels($1.label, $5); err != nil {
					plpgsqllex.Error(err.Error())
				}
				$$ = block
			}
	;

/*
 * Declaration section. The block label lives here (before DECLARE), matching
 * pl_gram.y. DECLARE itself is optional.
 */
decl_sect:
		opt_block_label
			{
				$$ = declSect{label: $1}
			}
	|	opt_block_label K_DECLARE decl_stmts
			{
				$$ = declSect{label: $1, decls: $3}
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
 * A single variable declaration: name [CONSTANT] type [NOT NULL] [:= expr] ;
 * The type and default-value text are captured by the read_sql_construct
 * machinery (see read_construct.go), invoked from the actions below. ALIAS,
 * CURSOR, and COLLATE forms are deferred.
 */
decl_statement:
		any_identifier decl_const decl_datatype decl_notnull decl_defval
			{
				v := plpgsqlast.NewPLpgSQL_var($1)
				v.IsConst = $2
				v.DataType = $3
				v.NotNull = $4
				v.DefaultVal = $5
				if v.NotNull && v.DefaultVal == nil {
					plpgsqllex.Error(fmt.Sprintf(
						"variable %q must have a default value, since it's declared NOT NULL",
						v.Refname))
				}
				$$ = v
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

proc_stmt:
		pl_block ';'
			{
				$$ = $1
			}
	|	stmt_execsql
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
	|	stmt_return
			{
				$$ = $1
			}
	|	stmt_dynexecute
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
	|	stmt_if
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
	|	stmt_case
			{
				$$ = $1
			}
	|	stmt_exit
			{
				$$ = $1
			}
	|	stmt_null
			{
				$$ = $1
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
 * Embedded SQL statement — any statement not handled by a PL/pgSQL production.
 * Mirrors PG's stmt_execsql token set. PG keys assignment on T_DATUM (a resolved
 * variable) and everything else on T_WORD/T_CWORD; we have no resolution, so a
 * word-initiated statement is dispatched by makeWordStmt, which peeks for an
 * assignment operator (PG errors there since a real variable would be T_DATUM;
 * we build the assignment instead). $1's byte offset (plpgsqlDollar[1].location)
 * is the start of the captured statement text, since the first token is already
 * consumed by the grammar.
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
				stmt.Sqlstmt = makeExpr(lx.scanStmtText(startPos), plpgsqlast.RAW_PARSE_DEFAULT)
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
				stmt.Sqlstmt = makeExpr(lx.scanStmtText(startPos), plpgsqlast.RAW_PARSE_DEFAULT)
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
				stmt.Sqlstmt = makeExpr(lx.scanStmtText(startPos), plpgsqlast.RAW_PARSE_DEFAULT)
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
 * PERFORM expr — run a query for its side effects. We capture the expression
 * after PERFORM (PG substitutes SELECT for execution; we keep the text and
 * re-emit PERFORM on deparse).
 */
stmt_perform:
		K_PERFORM
			{
				lx := plpgsqllex.(*lexer)
				lx.beginScan(plpgsqlrcvr.char)
				plpgsqlrcvr.char = -1
				plpgsqltoken = -1
				stmt := plpgsqlast.NewPLpgSQL_stmt_perform()
				stmt.Expr = lx.readSQLExpr()
				$$ = stmt
			}
	;

/*
 * CALL proc(...) and DO $$...$$. Both capture the whole statement text (keyword
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
				stmt.Expr = makeExpr(lx.scanStmtText(startPos), plpgsqlast.RAW_PARSE_DEFAULT)
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
				stmt.Expr = makeExpr(lx.scanStmtText(startPos), plpgsqlast.RAW_PARSE_DEFAULT)
				$$ = stmt
			}
	;

/*
 * RETURN [expr], RETURN NEXT expr, RETURN QUERY query. makeReturnStmt peeks the
 * token after RETURN to pick the form. RETURN QUERY EXECUTE (dynamic) is deferred.
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
 * EXECUTE query [INTO [STRICT] target] [USING args] — dynamic SQL. makeDynExecute
 * scans the query and the INTO/USING clauses (either order). This is the primary
 * statement the Tier-1 dynamic-EXECUTE policy inspects.
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
 * Cursor statements. The cursor is a plain name (T_WORD) since we have no
 * resolution — PG uses a resolved refcursor T_DATUM. OPEN is disambiguated
 * syntactically by makeOpen (bound-args vs FOR-query vs bare); FETCH/MOVE share a
 * node via IsMove and use readFetchDirection for the direction clause.
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

stmt_close:
		K_CLOSE cursor_variable ';'
			{
				stmt := plpgsqlast.NewPLpgSQL_stmt_close()
				stmt.Curvar = $2
				$$ = stmt
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

/*
 * A cursor reference. PG's cursor_variable is a resolved refcursor T_DATUM; we
 * have no resolution, so it is a plain word captured as text.
 */
cursor_variable:
		T_WORD
			{
				$$ = $1
			}
	;

/*
 * IF … THEN … [ELSIF … THEN …] [ELSE …] END IF. Each condition is captured as a
 * PLpgSQL_expr by the expr_until_then scanner (read_sql_expression up to THEN).
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
 * Unconditional LOOP and WHILE. Both share loop_body for the `… END LOOP
 * <label>;` tail. opt_loop_label mirrors PG (identical to opt_block_label, kept
 * separate to track the grammar). The end label is validated against the start
 * label, like blocks.
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
				$$ = stmt
			}
	;

loop_body:
		proc_sect K_END K_LOOP opt_label ';'
			{
				$$ = loopBody{stmts: $1, endLabel: $4}
			}
	;

/*
 * Integer and query FOR loops. for_control does the heavy lifting in a manual
 * scan (readForControl): integer FOR (`lower .. upper [BY step]`) vs query FOR.
 * It returns the node sans label/body, which stmt_for fills in (PG checks
 * cmd_type; we type-switch). Dynamic (EXECUTE) and bound-cursor FOR loops are
 * not distinguished — see the chunk note — so for_control yields only fori/fors.
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
 * The FOR loop target. PG uses T_DATUM/T_WORD/T_CWORD and resolves it; we have no
 * resolution, so it is a single word or compound name captured as text.
 * Comma-separated target lists are deferred (see the chunk note).
 */
for_variable:
		T_WORD
			{
				$$ = $1
			}
	|	T_CWORD
			{
				$$ = $1
			}
	;

stmt_foreach_a:
		opt_loop_label K_FOREACH for_variable foreach_slice K_IN K_ARRAY expr_until_loop loop_body
			{
				stmt := plpgsqlast.NewPLpgSQL_stmt_foreach_a()
				stmt.Label = $1
				stmt.Var = $3
				stmt.Slice = $4
				stmt.Expr = $7
				stmt.Body = $8.stmts
				if err := checkLabels($1, $8.endLabel); err != nil {
					plpgsqllex.Error(err.Error())
				}
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
 * CASE: searched (`CASE WHEN … THEN …`) or simple (`CASE expr WHEN … THEN …`).
 * opt_expr_until_when peeks for WHEN to decide which, and leaves a WHEN token for
 * case_when. An empty ELSE collapses to no ELSE (ElseStmts nil), as for IF.
 */
stmt_case:
		K_CASE opt_expr_until_when case_when_list opt_case_else K_END K_CASE ';'
			{
				stmt := plpgsqlast.NewPLpgSQL_stmt_case()
				stmt.TestExpr = $2
				stmt.WhenList = $3
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
				$$ = $2
			}
	;

/*
 * EXIT / CONTINUE [label] [WHEN cond]. PG validates the label and loop-nesting
 * here using the namespace; we have none, so we only capture the statement (see
 * the chunk note). The WHEN condition is scanned up to ';'.
 */
stmt_exit:
		exit_type opt_label opt_exitcond
			{
				stmt := plpgsqlast.NewPLpgSQL_stmt_exit($1)
				stmt.Label = $2
				stmt.Cond = $3
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

/*
 * Expression-scanning productions. Each is an empty rule whose action manually
 * scans an embedded SQL expression up to a terminator (PG's read_sql_expression
 * family: expr_until_semi / _then / _loop). The beginScan / clear-lookahead
 * dance matches decl_datatype and stmt_assign.
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

opt_loop_label:
		/* empty */
			{
				$$ = ""
			}
	|	LESS_LESS any_identifier GREATER_GREATER
			{
				$$ = $2
			}
	;

opt_block_label:
		/* empty */
			{
				$$ = ""
			}
	|	LESS_LESS any_identifier GREATER_GREATER
			{
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

any_identifier:
		T_WORD
			{
				$$ = $1
			}
	|	unreserved_keyword
			{
				$$ = $1
			}
	;

/*
 * Unreserved keywords may be used as identifiers (labels, variable names,
 * etc.). Listed exactly as in pl_gram.y; the default action carries each
 * keyword's text (the K_* tokens are <str> and the lexer fills it in).
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
