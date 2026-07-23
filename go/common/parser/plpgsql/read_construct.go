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

package plpgsql

import (
	"errors"
	"slices"
	"strings"

	"github.com/multigres/multigres/go/common/parser/ast/plpgsqlast"
)

// This is the Go port of PG's read_sql_construct / read_datatype: a grammar
// action manually scans tokens until a terminator and captures the verbatim
// source text of the fragment in between. We capture by byte offset (first
// token's start .. terminator's start), which is robust to how the lexer groups
// tokens (e.g. compound names).

// declSect carries a block's optional label plus its DECLARE-section datums from
// the decl_sect production up to pl_block. (PG uses a {label, n_initvars,
// initvarnos} struct; we keep the parse-level pieces.)
type declSect struct {
	label string
	decls []plpgsqlast.Datum
}

// loopBody carries the pieces of a `… END LOOP <label>;` tail (PG's loop_body
// struct in pl_gram.y) from the loop_body production up to stmt_loop/stmt_while,
// which build the node and validate the end label.
type loopBody struct {
	stmts    []plpgsqlast.Stmt
	endLabel string
}

// scanNext returns the next fully-classified PL/pgSQL token — the Go analogue of
// PG's plpgsql_yylex: a raw token from internalLex with keyword lookup,
// compound-name assembly (T_CWORD), and the T_WORD fallback applied. It is the
// single token source for both the grammar (via Lex) and the hand-scan actions
// in this file, exactly as PG routes both through plpgsql_yylex — there is no
// separate partially-classified path. (We never emit T_DATUM, the sanctioned
// no-resolution divergence, so a name PG would resolve to a variable is
// T_WORD/T_CWORD here.)
func (l *lexer) scanNext() auxToken {
	a := l.internalLex()
	switch a.tok {
	case IDENT:
		a.tok, a.str = l.reclassifyWord(a)
	case PARAM:
		a.tok, a.str = l.reclassifyParam(a)
	}
	return a
}

// scanFragment scans source tokens until one of the terminators appears at
// paren/bracket depth 0, returning the raw source text from the first token up
// to (not including) the terminator, plus the terminator token. This is the
// read_sql_construct core.
func (l *lexer) scanFragment(terminators ...int) (string, auxToken, error) {
	parenLevel := 0
	start := -1
	for {
		tok := l.scanNext()
		if parenLevel == 0 && slices.Contains(terminators, tok.tok) {
			if start < 0 {
				return "", tok, errors.New("missing expression")
			}
			text := strings.TrimRight(l.input[start:tok.pos], " \t\r\n")
			if text == "" {
				return "", tok, errors.New("missing expression")
			}
			return text, tok, nil
		}
		if start < 0 {
			start = tok.pos
		}
		switch tok.tok {
		case '(', '[':
			parenLevel++
		case ')', ']':
			parenLevel--
			if parenLevel < 0 {
				return "", tok, errors.New("mismatched parentheses")
			}
		case 0: // EOF before a terminator
			return "", tok, errors.New("unterminated SQL fragment")
		}
	}
}

// readSQLConstruct is the Go port of PG's read_sql_construct: it scans an embedded
// SQL fragment up to the first of the given terminators (at paren depth 0) and
// returns it as a PLpgSQL_expr in the given parse mode, plus the terminator token
// that ended it. Like PG's read_sql_construct, a scan failure is reported
// internally (l.Error, the yyerror analogue), so callers do not thread an error —
// they get an empty expr and continue. read_sql_expression / read_sql_expression2
// are just this fixed to RAW_PARSE_PLPGSQL_EXPR (see readSQLExprUntil).
//
// Callers that need the raw fragment text rather than an expr — execsql/CALL
// (scanStmtText) and the INTO target (readIntoTarget) — scan with scanFragment
// directly, exactly as PG's make_execsql_stmt and read_into_target do their own
// token loops rather than calling read_sql_construct.
func (l *lexer) readSQLConstruct(mode plpgsqlast.RawParseMode, terminators ...int) (*plpgsqlast.PLpgSQL_expr, int) {
	text, term, err := l.scanFragment(terminators...)
	if err != nil {
		l.Error(err.Error())
		return plpgsqlast.NewPLpgSQL_expr(""), term.tok
	}
	return makeExpr(text, mode), term.tok
}

// beginScan prepares for a fragment scan invoked from a grammar action. An
// empty production (e.g. decl_datatype) is reduced only after the parser reads
// a lookahead token, which is the fragment's first token — now held by the
// parser, not the lexer. If present (char >= 0) we push it back so scanFragment
// re-reads it from the start; this is the Go analogue of PG passing it into
// read_datatype(yychar). When char < 0 (a default reduction, no lookahead) the
// scan starts fresh, matching PG's `if (tok == YYEMPTY) tok = yylex()`. The
// action must then clear the parser's lookahead (the yyclearin equivalent).
func (l *lexer) beginScan(char int) {
	if char >= 0 {
		l.pushBack(l.lastToken)
	}
}

// readDatatype scans a declared type as raw text (no resolution), and pushes the
// terminator back since NOT NULL / := / DEFAULT / ';' — or ',' / ')' for a cursor
// argument list — belong to the grammar. The ',' and ')' terminators only match
// at paren depth 0, which never follows a variable-decl type (inner type parens
// like numeric(10,2) are at depth ≥1), so one terminator set serves both the
// variable and cursor-arg contexts. Mirrors PG's context-independent read_datatype.
func (l *lexer) readDatatype() *plpgsqlast.PLpgSQL_type {
	text, term, err := l.scanFragment(';', COLON_EQUALS, '=', K_DEFAULT, K_NOT, ',', ')')
	if err != nil {
		l.Error(err.Error())
		return plpgsqlast.NewPLpgSQL_type("")
	}
	l.pushBack(term)
	return plpgsqlast.NewPLpgSQL_type(text)
}

// readSQLExpr scans an expression up to ';' (which it consumes) and returns it as
// a PLpgSQL_expr. Parsed is left nil — turning the text into an ast.Stmt is a
// separate step.
func (l *lexer) readSQLExpr() *plpgsqlast.PLpgSQL_expr {
	return l.readSQLExprUntil(';')
}

// makeExpr wraps captured fragment text in a PLpgSQL_expr with the given parse
// mode. Parsed is left nil, as elsewhere.
func makeExpr(text string, mode plpgsqlast.RawParseMode) *plpgsqlast.PLpgSQL_expr {
	e := plpgsqlast.NewPLpgSQL_expr(text)
	e.ParseMode = mode
	return e
}

// readForControl is the manual scan behind the for_control production (PG's
// for_control action). It runs after `for_variable K_IN` and decides between an
// integer FOR (`lower .. upper [BY step]`) and a query FOR by scanning the first
// construct up to ".." or LOOP and seeing which terminator hit. The dynamic
// (EXECUTE) and bound-cursor forms are not distinguished here: without variable
// resolution a cursor FOR loop reads as a query FOR (see chunk note). varName is
// the already-parsed loop target.
func (l *lexer) readForControl(varName string) plpgsqlast.Stmt {
	tok := l.scanNext()
	if tok.tok == K_EXECUTE {
		// Dynamic FOR: FOR var IN EXECUTE query [USING …] LOOP.
		dynfors := plpgsqlast.NewPLpgSQL_stmt_dynfors()
		dynfors.Var = varName
		query, endtoken := l.readSQLConstruct(plpgsqlast.RAW_PARSE_PLPGSQL_EXPR, K_LOOP, K_USING)
		dynfors.Query = query
		if endtoken == K_USING {
			dynfors.Params, _ = l.readUsingList(',', K_LOOP)
		}
		return dynfors
	}

	reverse := false
	if tok.tok == K_REVERSE {
		reverse = true
	} else {
		l.pushBack(tok)
	}

	// The first construct may be either an integer-loop bound or a whole query, so
	// scan it as RAW_PARSE_DEFAULT and relabel to an expression if we see "..",
	// matching PG's for_control.
	expr1, endtoken := l.readSQLConstruct(plpgsqlast.RAW_PARSE_DEFAULT, DOT_DOT, K_LOOP)

	if endtoken == DOT_DOT {
		// Integer FOR: lower .. upper [BY step]. Bounds are expressions.
		fori := plpgsqlast.NewPLpgSQL_stmt_fori()
		fori.Var = varName
		fori.Reverse = reverse
		expr1.ParseMode = plpgsqlast.RAW_PARSE_PLPGSQL_EXPR
		fori.Lower = expr1

		upper, endtoken2 := l.readSQLConstruct(plpgsqlast.RAW_PARSE_PLPGSQL_EXPR, K_LOOP, K_BY)
		fori.Upper = upper
		if endtoken2 == K_BY {
			step, _ := l.readSQLConstruct(plpgsqlast.RAW_PARSE_PLPGSQL_EXPR, K_LOOP)
			fori.Step = step
		}
		return fori
	}

	// Query FOR (stopped on LOOP). REVERSE is only valid for integer loops.
	if reverse {
		l.Error("cannot specify REVERSE in query FOR loop")
	}
	fors := plpgsqlast.NewPLpgSQL_stmt_fors()
	fors.Var = varName
	fors.Query = expr1
	return fors
}

// scanStmtText scans to the terminating ';' at paren depth 0 and returns the
// verbatim statement text from startPos (the first token's byte offset, which the
// grammar already consumed) up to the ';'. It underlies execsql and CALL/DO,
// which need the leading keyword included in the captured text.
func (l *lexer) scanStmtText(startPos int) string {
	_, term, err := l.scanFragment(';')
	if err != nil {
		l.Error(err.Error())
		return ""
	}
	return strings.TrimRight(l.input[startPos:term.pos], " \t\r\n")
}

// makeWordStmt implements the assign-vs-execsql dispatch for a word-initiated
// statement (PG decides this in the stmt_execsql T_WORD action). word is the
// already-consumed first token; startPos its byte offset. If the next token is an
// assignment operator we build an assignment (PG errors here, since a real
// variable would have been T_DATUM; we have no resolution, so we treat it as the
// assignment it looks like); otherwise the whole statement is captured as execsql.
func (l *lexer) makeWordStmt(word string, startPos int) plpgsqlast.Stmt {
	tok := l.scanNext()
	if tok.tok == COLON_EQUALS || tok.tok == '=' {
		stmt := plpgsqlast.NewPLpgSQL_stmt_assign(word)
		stmt.Expr = l.readSQLExpr()
		return stmt
	}
	l.pushBack(tok)
	stmt := plpgsqlast.NewPLpgSQL_stmt_execsql()
	stmt.Sqlstmt = makeExpr(l.scanStmtText(startPos), plpgsqlast.RAW_PARSE_DEFAULT)
	return stmt
}

// makeReturnStmt implements the RETURN dispatch (PG's stmt_return action): RETURN
// NEXT expr, RETURN QUERY query, or RETURN [expr]. The RETURN QUERY EXECUTE
// (dynamic) form is deferred to the dynamic-EXECUTE chunk; until then a
// `RETURN QUERY EXECUTE …` is captured verbatim as the query text.
func (l *lexer) makeReturnStmt() plpgsqlast.Stmt {
	tok := l.scanNext()
	switch tok.tok {
	case K_NEXT:
		s := plpgsqlast.NewPLpgSQL_stmt_return_next()
		s.Expr = l.readSQLExpr()
		return s
	case K_QUERY:
		s := plpgsqlast.NewPLpgSQL_stmt_return_query()
		if tok := l.scanNext(); tok.tok == K_EXECUTE {
			// RETURN QUERY EXECUTE query [USING …].
			dynquery, endtoken := l.readSQLConstruct(plpgsqlast.RAW_PARSE_PLPGSQL_EXPR, K_USING, ';')
			s.DynQuery = dynquery
			if endtoken == K_USING {
				s.Params, _ = l.readUsingList(',', ';')
			}
			return s
		} else {
			l.pushBack(tok)
		}
		s.Query, _ = l.readSQLConstruct(plpgsqlast.RAW_PARSE_DEFAULT, ';')
		return s
	case ';':
		// Bare RETURN; the ';' is consumed.
		return plpgsqlast.NewPLpgSQL_stmt_return()
	default:
		l.pushBack(tok)
		s := plpgsqlast.NewPLpgSQL_stmt_return()
		s.Expr = l.readSQLExpr()
		return s
	}
}

// makeDynExecute implements the stmt_dynexecute action (PG's stmt_dynexecute):
// EXECUTE query [INTO [STRICT] target] [USING arg, …], where INTO and USING may
// appear in either order. The query and USING expressions are captured as text;
// the INTO target is captured as text (PG resolves it to variables). UsingFirst
// records the source order so the deparse round-trips.
func (l *lexer) makeDynExecute() *plpgsqlast.PLpgSQL_stmt_dynexecute {
	stmt := plpgsqlast.NewPLpgSQL_stmt_dynexecute()
	query, endtoken := l.readSQLConstruct(plpgsqlast.RAW_PARSE_PLPGSQL_EXPR, K_INTO, K_USING, ';')
	stmt.Query = query

	for {
		switch endtoken {
		case K_INTO:
			if stmt.Into {
				l.Error("EXECUTE ... INTO specified more than once")
				return stmt
			}
			stmt.Into = true
			stmt.Strict, stmt.Target, endtoken = l.readIntoTarget(K_USING, ';')
		case K_USING:
			if len(stmt.Params) > 0 {
				l.Error("EXECUTE ... USING specified more than once")
				return stmt
			}
			if !stmt.Into {
				stmt.UsingFirst = true
			}
			stmt.Params, endtoken = l.readUsingList(',', ';', K_INTO)
		case ';':
			return stmt
		default:
			l.Error("syntax error in EXECUTE statement")
			return stmt
		}
	}
}

// makeRaiseStmt implements the stmt_raise action (PG's stmt_raise): RAISE
// [level] [condname | SQLSTATE 'code' | 'message' [, arg …]] [USING opt = expr,
// …]. It hand-scans token by token because the shape after RAISE is only known
// as it is read. PG's condition-name recognition (plpgsql_recognize_err_condition)
// is a resolution step and dropped; the SQLSTATE length/charset check is kept
// (purely lexical). A bare `RAISE;` re-throws the current error.
func (l *lexer) makeRaiseStmt() plpgsqlast.Stmt {
	stmt := plpgsqlast.NewPLpgSQL_stmt_raise()

	tok := l.scanNext()
	if tok.tok == 0 {
		l.Error("unexpected end of function definition")
		return stmt
	}
	if tok.tok == ';' {
		// Bare RAISE: re-throw the current error.
		return stmt
	}

	// Optional elog severity level.
	switch tok.tok {
	case K_EXCEPTION:
		stmt.ElogLevel = plpgsqlast.RAISE_LEVEL_EXCEPTION
		tok = l.scanNext()
	case K_WARNING:
		stmt.ElogLevel = plpgsqlast.RAISE_LEVEL_WARNING
		tok = l.scanNext()
	case K_NOTICE:
		stmt.ElogLevel = plpgsqlast.RAISE_LEVEL_NOTICE
		tok = l.scanNext()
	case K_INFO:
		stmt.ElogLevel = plpgsqlast.RAISE_LEVEL_INFO
		tok = l.scanNext()
	case K_LOG:
		stmt.ElogLevel = plpgsqlast.RAISE_LEVEL_LOG
		tok = l.scanNext()
	case K_DEBUG:
		stmt.ElogLevel = plpgsqlast.RAISE_LEVEL_DEBUG
		tok = l.scanNext()
	}
	if tok.tok == 0 {
		l.Error("unexpected end of function definition")
		return stmt
	}

	// Next is a condition name / SQLSTATE, an old-style message literal, or USING
	// to start the option list immediately.
	if tok.tok == SCONST {
		// Old-style message and parameters.
		stmt.Message = tok.str
		tok = l.scanNext()
		if tok.tok != ',' && tok.tok != ';' && tok.tok != K_USING {
			l.Error("syntax error")
			return stmt
		}
		for tok.tok == ',' {
			expr, endtoken := l.readSQLConstruct(plpgsqlast.RAW_PARSE_PLPGSQL_EXPR, ',', ';', K_USING)
			stmt.Params = append(stmt.Params, expr)
			tok = auxToken{tok: endtoken}
		}
	} else if tok.tok != K_USING {
		// Must be a condition name or SQLSTATE.
		if tok.tok == K_SQLSTATE {
			t := l.scanNext()
			if t.tok != SCONST {
				l.Error("syntax error")
				return stmt
			}
			if !plpgsqlast.IsSQLStateCode(t.str) {
				l.Error("invalid SQLSTATE code")
				return stmt
			}
			stmt.Condname = t.str
			stmt.IsSqlState = true
		} else if tok.tok == T_WORD || isUnreservedKeywordToken(tok.tok) {
			// A plain word (PG's `tok == T_WORD`) or an unreserved keyword.
			stmt.Condname = tok.str
		} else {
			l.Error("syntax error")
			return stmt
		}
		tok = l.scanNext()
		if tok.tok != ';' && tok.tok != K_USING {
			l.Error("syntax error")
			return stmt
		}
	}

	if tok.tok == K_USING {
		stmt.Options = l.readRaiseOptions()
	}

	l.checkRaiseParameters(stmt)
	return stmt
}

// readRaiseOptions is the port of PG's read_raise_options: the `USING` option
// list of a RAISE, each entry `option = expr` (or `:= expr`), comma-separated,
// terminated by ';'. The append is plain Go (not a grammar action), so the
// goyacc fast-append hazard does not apply.
func (l *lexer) readRaiseOptions() []*plpgsqlast.PLpgSQL_raise_option {
	var result []*plpgsqlast.PLpgSQL_raise_option
	for {
		tok := l.scanNext()
		if tok.tok == 0 {
			l.Error("unexpected end of function definition")
			return result
		}

		var optType plpgsqlast.RaiseOptionType
		switch tok.tok {
		case K_ERRCODE:
			optType = plpgsqlast.PLPGSQL_RAISEOPTION_ERRCODE
		case K_MESSAGE:
			optType = plpgsqlast.PLPGSQL_RAISEOPTION_MESSAGE
		case K_DETAIL:
			optType = plpgsqlast.PLPGSQL_RAISEOPTION_DETAIL
		case K_HINT:
			optType = plpgsqlast.PLPGSQL_RAISEOPTION_HINT
		case K_COLUMN:
			optType = plpgsqlast.PLPGSQL_RAISEOPTION_COLUMN
		case K_CONSTRAINT:
			optType = plpgsqlast.PLPGSQL_RAISEOPTION_CONSTRAINT
		case K_DATATYPE:
			optType = plpgsqlast.PLPGSQL_RAISEOPTION_DATATYPE
		case K_TABLE:
			optType = plpgsqlast.PLPGSQL_RAISEOPTION_TABLE
		case K_SCHEMA:
			optType = plpgsqlast.PLPGSQL_RAISEOPTION_SCHEMA
		default:
			l.Error("unrecognized RAISE statement option")
			return result
		}

		if t := l.scanNext(); t.tok != '=' && t.tok != COLON_EQUALS {
			l.Error("syntax error, expected \"=\"")
			return result
		}

		opt := plpgsqlast.NewPLpgSQL_raise_option(optType)
		expr, endtoken := l.readSQLConstruct(plpgsqlast.RAW_PARSE_PLPGSQL_EXPR, ',', ';')
		opt.Expr = expr
		result = append(result, opt)

		if endtoken == ';' {
			break
		}
	}
	return result
}

// checkRaiseParameters ports PG's check_raise_parameters: the number of `%`
// placeholders in the old-style message must equal the number of parameter
// expressions. A literal `%%` is not a placeholder.
func (l *lexer) checkRaiseParameters(stmt *plpgsqlast.PLpgSQL_stmt_raise) {
	if stmt.Message == "" {
		return
	}
	expected := 0
	msg := stmt.Message
	for i := 0; i < len(msg); i++ {
		if msg[i] == '%' {
			if i+1 < len(msg) && msg[i+1] == '%' {
				i++ // skip the escaped %%
			} else {
				expected++
			}
		}
	}
	if expected < len(stmt.Params) {
		l.Error("too many parameters specified for RAISE")
	} else if expected > len(stmt.Params) {
		l.Error("too few parameters specified for RAISE")
	}
}

// readSQLStateCondition reads the `SQLSTATE 'xxxxx'` form of a WHEN condition
// (the `sqlstate` arm of PG's proc_condition action): the next token must be a
// string literal holding a valid 5-char SQLSTATE code. PG resolves it to an
// integer sqlerrstate; we keep the code as text with IsSqlState set.
func (l *lexer) readSQLStateCondition() *plpgsqlast.PLpgSQL_condition {
	tok := l.scanNext()
	if tok.tok != SCONST {
		l.Error("syntax error")
		return plpgsqlast.NewPLpgSQL_condition("")
	}
	if !plpgsqlast.IsSQLStateCode(tok.str) {
		l.Error("invalid SQLSTATE code")
		return plpgsqlast.NewPLpgSQL_condition("")
	}
	// The deparse recovers the SQLSTATE form from the code's shape (five
	// [0-9A-Z] chars), so no form flag is stored.
	return plpgsqlast.NewPLpgSQL_condition(tok.str)
}

// appendException appends a WHEN clause to the proc_exceptions list. Helper for
// the goyacc fast-append reason (see appendElsif).
func appendException(es []*plpgsqlast.PLpgSQL_exception, e *plpgsqlast.PLpgSQL_exception) []*plpgsqlast.PLpgSQL_exception {
	return append(es, e)
}

// appendCondition appends a condition to a WHEN clause's OR-list. Helper for the
// goyacc fast-append reason (see appendElsif).
func appendCondition(cs []*plpgsqlast.PLpgSQL_condition, c *plpgsqlast.PLpgSQL_condition) []*plpgsqlast.PLpgSQL_condition {
	return append(cs, c)
}

// makeAssertStmt implements the stmt_assert action (PG's stmt_assert): scan the
// condition up to ',' or ';', and if a comma followed, the message up to ';'.
func (l *lexer) makeAssertStmt() plpgsqlast.Stmt {
	stmt := plpgsqlast.NewPLpgSQL_stmt_assert()
	cond, endtoken := l.readSQLConstruct(plpgsqlast.RAW_PARSE_PLPGSQL_EXPR, ',', ';')
	stmt.Cond = cond
	if endtoken == ',' {
		stmt.Message = l.readSQLExpr()
	}
	return stmt
}

// isUnreservedKeywordToken reports whether a token code is one of the PL/pgSQL
// unreserved keywords (PG's plpgsql_token_is_unreserved_keyword). Used by RAISE
// to accept an unreserved keyword as a condition name.
func isUnreservedKeywordToken(tok int) bool {
	return unreservedKeywordTokens[tok]
}

// unreservedKeywordTokens is the set of unreserved-keyword token codes, derived
// once from the unreservedKeywords name table.
var unreservedKeywordTokens = func() map[int]bool {
	m := make(map[int]bool, len(unreservedKeywords))
	for _, tok := range unreservedKeywords {
		m[tok] = true
	}
	return m
}()

// readIntoTarget reads an INTO clause: an optional STRICT keyword, then the target
// text up to a terminator, returning the terminator that ended it. (PG's
// read_into_target resolves the target variables; we keep the text.)
func (l *lexer) readIntoTarget(terminators ...int) (strict bool, target string, endtoken int) {
	if tok := l.scanNext(); tok.tok == K_STRICT {
		strict = true
	} else {
		l.pushBack(tok)
	}
	text, term, err := l.scanFragment(terminators...)
	if err != nil {
		l.Error(err.Error())
		return strict, "", term.tok
	}
	return strict, text, term.tok
}

// readUsingList reads a comma-separated USING expression list, stopping at the
// first terminator that is not ','. Returns the expressions and the terminator
// token. The append is plain Go (not a grammar action), so the goyacc fast-append
// hazard does not apply.
func (l *lexer) readUsingList(terminators ...int) ([]*plpgsqlast.PLpgSQL_expr, int) {
	var params []*plpgsqlast.PLpgSQL_expr
	for {
		expr, endtoken := l.readSQLConstruct(plpgsqlast.RAW_PARSE_PLPGSQL_EXPR, terminators...)
		params = append(params, expr)
		if endtoken != ',' {
			return params, endtoken
		}
	}
}

// readFetchDirection is the port of PG's read_fetch_direction (opt_fetch_direction
// action): it builds a fetch node whose direction fields come from the optional
// FETCH/MOVE direction clause, leaving curvar/target/is_move for the grammar. The
// count for ABSOLUTE/RELATIVE (and bare-count/FORWARD/BACKWARD count) is an
// expression scanned up to FROM/IN; the other keywords set Direction/HowMany.
func (l *lexer) readFetchDirection() *plpgsqlast.PLpgSQL_stmt_fetch {
	fetch := plpgsqlast.NewPLpgSQL_stmt_fetch(false)
	checkFrom := true

	tok := l.scanNext()
	switch tok.tok {
	case K_NEXT:
		// defaults (FORWARD, one row)
	case K_PRIOR:
		fetch.Direction = plpgsqlast.FETCH_BACKWARD
	case K_FIRST:
		fetch.Direction = plpgsqlast.FETCH_ABSOLUTE
	case K_LAST:
		fetch.Direction = plpgsqlast.FETCH_ABSOLUTE
		fetch.HowMany = -1
	case K_ABSOLUTE:
		fetch.Direction = plpgsqlast.FETCH_ABSOLUTE
		fetch.Expr, _ = l.readSQLConstruct(plpgsqlast.RAW_PARSE_PLPGSQL_EXPR, K_FROM, K_IN)
		checkFrom = false
	case K_RELATIVE:
		fetch.Direction = plpgsqlast.FETCH_RELATIVE
		fetch.Expr, _ = l.readSQLConstruct(plpgsqlast.RAW_PARSE_PLPGSQL_EXPR, K_FROM, K_IN)
		checkFrom = false
	case K_ALL:
		fetch.HowMany = plpgsqlast.FETCH_ALL
		fetch.ReturnsMultipleRows = true
	case K_FORWARD:
		checkFrom = l.completeDirection(fetch)
	case K_BACKWARD:
		fetch.Direction = plpgsqlast.FETCH_BACKWARD
		checkFrom = l.completeDirection(fetch)
	case K_FROM, K_IN:
		// empty direction; FROM/IN already consumed
		checkFrom = false
	case T_WORD:
		// No direction clause: this is the cursor name. PG checks T_DATUM here
		// (a resolved refcursor variable); with no resolution the cursor name is
		// a plain word (T_WORD), which we push back for the grammar's
		// cursor_variable: T_WORD to consume.
		l.pushBack(tok)
		checkFrom = false
	default:
		// A bare count expression with no preceding keyword.
		l.pushBack(tok)
		fetch.Expr, _ = l.readSQLConstruct(plpgsqlast.RAW_PARSE_PLPGSQL_EXPR, K_FROM, K_IN)
		fetch.ReturnsMultipleRows = true
		checkFrom = false
	}

	if checkFrom {
		if t := l.scanNext(); t.tok != K_FROM && t.tok != K_IN {
			l.Error("expected FROM or IN")
		}
	}
	return fetch
}

// completeDirection handles the tail of FORWARD/BACKWARD (PG's complete_direction):
// FROM/IN (no count), ALL, or a count expression. It fills the fetch's count
// fields and returns whether the caller must still consume a FROM/IN.
func (l *lexer) completeDirection(fetch *plpgsqlast.PLpgSQL_stmt_fetch) bool {
	tok := l.scanNext()
	switch tok.tok {
	case K_FROM, K_IN:
		return false
	case K_ALL:
		fetch.HowMany = plpgsqlast.FETCH_ALL
		fetch.ReturnsMultipleRows = true
		return true
	default:
		l.pushBack(tok)
		fetch.Expr, _ = l.readSQLConstruct(plpgsqlast.RAW_PARSE_PLPGSQL_EXPR, K_FROM, K_IN)
		fetch.ReturnsMultipleRows = true
		return false
	}
}

// readFetchTarget scans a FETCH INTO target as raw text up to ';' (PG resolves it
// via read_into_target; we keep the names).
func (l *lexer) readFetchTarget() string {
	text, _, err := l.scanFragment(';')
	if err != nil {
		l.Error(err.Error())
		return ""
	}
	return text
}

// makeOpen is the OPEN dispatch (PG's stmt_open action). PG branches on whether
// the cursor was declared bound (cursor_explicit_expr); without resolution we peek
// the token after the cursor name: '(' → bound-cursor args, ';' → bare open, and
// [NO] SCROLL / FOR → the FOR query / FOR EXECUTE form.
func (l *lexer) makeOpen(curvar string) *plpgsqlast.PLpgSQL_stmt_open {
	stmt := plpgsqlast.NewPLpgSQL_stmt_open()
	stmt.Curvar = curvar
	stmt.CursorOptions = plpgsqlast.CURSOR_OPT_FAST_PLAN

	tok := l.scanNext()
	switch tok.tok {
	case ';':
		return stmt // bare OPEN c (bound cursor, no args)
	case '(':
		l.pushBack(tok)
		stmt.Argquery, _ = l.readSQLConstruct(plpgsqlast.RAW_PARSE_DEFAULT, ';')
		return stmt
	}

	// Unbound: [NO] SCROLL then FOR query | FOR EXECUTE expr [USING …]. Like PG,
	// a bare NO (not followed by SCROLL) is tolerated — the token after NO simply
	// carries on to the FOR check.
	switch tok.tok {
	case K_NO:
		if t := l.scanNext(); t.tok == K_SCROLL {
			stmt.CursorOptions |= plpgsqlast.CURSOR_OPT_NO_SCROLL
			tok = l.scanNext()
		} else {
			tok = t
		}
	case K_SCROLL:
		stmt.CursorOptions |= plpgsqlast.CURSOR_OPT_SCROLL
		tok = l.scanNext()
	}

	if tok.tok != K_FOR {
		l.Error("syntax error, expected \"FOR\"")
		return stmt
	}

	if t := l.scanNext(); t.tok == K_EXECUTE {
		dynquery, endtoken := l.readSQLConstruct(plpgsqlast.RAW_PARSE_PLPGSQL_EXPR, K_USING, ';')
		stmt.DynQuery = dynquery
		if endtoken == K_USING {
			stmt.Params, _ = l.readUsingList(',', ';')
		}
	} else {
		l.pushBack(t)
		stmt.Query, _ = l.readSQLConstruct(plpgsqlast.RAW_PARSE_DEFAULT, ';')
	}
	return stmt
}

// readCaseTestExpr is the manual scan behind opt_expr_until_when (PG's action).
// It distinguishes a searched CASE (the next token is WHEN — no test expression)
// from a simple CASE (a test expression up to WHEN). Either way it leaves a
// K_WHEN token for the grammar's case_when to consume.
func (l *lexer) readCaseTestExpr() *plpgsqlast.PLpgSQL_expr {
	tok := l.scanNext()
	if tok.tok == K_WHEN {
		l.pushBack(tok)
		return nil
	}
	l.pushBack(tok)

	expr, _ := l.readSQLConstruct(plpgsqlast.RAW_PARSE_PLPGSQL_EXPR, K_WHEN)
	l.pushBackToken(K_WHEN) // hand the WHEN back to the grammar
	return expr
}

// readSQLExprUntil scans an expression up to (and consuming) the first of the
// given terminators, returning it as a PLpgSQL_expr. It is the Go analogue of PG's
// read_sql_expression / read_sql_expression2 — read_sql_construct fixed to
// RAW_PARSE_PLPGSQL_EXPR, discarding the terminator. The `;`, K_THEN, and K_LOOP
// forms (expr_until_semi / _then / _loop in pl_gram.y) differ only in the
// terminator they pass.
func (l *lexer) readSQLExprUntil(terminators ...int) *plpgsqlast.PLpgSQL_expr {
	e, _ := l.readSQLConstruct(plpgsqlast.RAW_PARSE_PLPGSQL_EXPR, terminators...)
	return e
}

// appendDatum appends d to ds, skipping nil (an extra DECLARE keyword yields a
// nil datum). It is a helper rather than an inline `append($1, $2)` so goyacc
// does not apply its in-place-append optimization, which clashes with the
// conditional and silently drops the result.
func appendDatum(ds []plpgsqlast.Datum, d plpgsqlast.Datum) []plpgsqlast.Datum {
	if d == nil {
		return ds
	}
	return append(ds, d)
}

// appendElsif appends an ELSIF arm to the stmt_elsifs list. It exists to keep
// the append out of the grammar action, dodging goyacc's -f "fast-append"
// optimization, which is unsafe for this rule.
//
// fast-append rewrites a literal `$$ = append($1, …)` — when it is the FIRST $$
// write in the action — into an in-place mutation of the value stack's boxed
// slice: `*(*[]T)(Iaddr(VAL.union)) = append(...)`, where Iaddr returns the
// interface's data word (a pointer to the boxed slice header). That reuses $1's
// backing array, but is sound only when $1 owns a non-nil backing array. Our
// base case is `stmt_elsifs: /*empty*/ { $$ = nil }`, so on the first arm $1 is
// a nil slice — and boxing a nil slice into an interface does NOT allocate:
// runtime.convTslice returns &runtime.zeroVal[0], the global shared zero buffer.
// The in-place append then writes a real {ptr,1,1} header into runtime.zeroVal,
// corrupting every nil-slice/zero-value box in the program (symptom: a phantom
// element in some unrelated []Stmt body, crashing the deparse). Verified: inline
// + nil base crashes; inline + a non-nil base (make([]T,0,1)) does not.
//
// Routing through a function call hides the bare idiom, so goyacc emits the
// ordinary `LOCAL = append(...)` form. (This is also why proc_sect's inline
// append is safe: its first $$ write is `$$ = $1`, which switches the action off
// the fast-append path. The trigger is the bare idiom, not a conditional —
// appendDatum guards against the same thing.)
func appendElsif(es []*plpgsqlast.PLpgSQL_if_elsif, e *plpgsqlast.PLpgSQL_if_elsif) []*plpgsqlast.PLpgSQL_if_elsif {
	return append(es, e)
}

// appendCaseWhen appends a WHEN arm to the case_when_list. Helper for the same
// goyacc fast-append reason as appendElsif (see the comment there): never write
// a bare `$$ = append($1, $2)` in an action.
func appendCaseWhen(ws []*plpgsqlast.PLpgSQL_case_when, w *plpgsqlast.PLpgSQL_case_when) []*plpgsqlast.PLpgSQL_case_when {
	return append(ws, w)
}

// appendCursorArg appends a cursor argument to the decl_cursor_arglist. Helper for
// the same goyacc fast-append reason as appendDatum.
func appendCursorArg(as []*plpgsqlast.PLpgSQL_var, a *plpgsqlast.PLpgSQL_var) []*plpgsqlast.PLpgSQL_var {
	return append(as, a)
}
