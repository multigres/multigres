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

// scanNext reads the next token for fragment scanning: a raw token from
// internalLex with simple keyword reclassification (so keyword terminators like
// K_NOT / K_DEFAULT are recognized), but no compound-word merging — byte
// offsets, not token grouping, drive text capture.
func (l *lexer) scanNext() auxToken {
	a := l.internalLex()
	if a.tok == IDENT && !a.quoted {
		if t, ok := reservedKeywords[a.str]; ok {
			a.tok = t
		} else if t, ok := unreservedKeywords[a.str]; ok {
			a.tok = t
		}
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
// terminator back since NOT NULL / := / DEFAULT / ';' belong to the grammar.
// Mirrors PG's read_datatype, reduced to text capture.
func (l *lexer) readDatatype() *plpgsqlast.PLpgSQL_type {
	text, term, err := l.scanFragment(';', COLON_EQUALS, '=', K_DEFAULT, K_NOT)
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
	reverse := false
	if tok := l.scanNext(); tok.tok == K_REVERSE {
		reverse = true
	} else {
		l.pushBack(tok)
	}

	text1, term, err := l.scanFragment(DOT_DOT, K_LOOP)
	if err != nil {
		l.Error(err.Error())
		return plpgsqlast.NewPLpgSQL_stmt_fors()
	}

	if term.tok == DOT_DOT {
		// Integer FOR: lower .. upper [BY step]. Bounds are expressions.
		fori := plpgsqlast.NewPLpgSQL_stmt_fori()
		fori.Var = varName
		fori.Reverse = reverse
		fori.Lower = makeExpr(text1, plpgsqlast.RAW_PARSE_PLPGSQL_EXPR)

		text2, term2, err := l.scanFragment(K_LOOP, K_BY)
		if err != nil {
			l.Error(err.Error())
			return fori
		}
		fori.Upper = makeExpr(text2, plpgsqlast.RAW_PARSE_PLPGSQL_EXPR)

		if term2.tok == K_BY {
			text3, _, err := l.scanFragment(K_LOOP)
			if err != nil {
				l.Error(err.Error())
				return fori
			}
			fori.Step = makeExpr(text3, plpgsqlast.RAW_PARSE_PLPGSQL_EXPR)
		}
		return fori
	}

	// Query FOR (stopped on LOOP). REVERSE is only valid for integer loops.
	if reverse {
		l.Error("cannot specify REVERSE in query FOR loop")
	}
	fors := plpgsqlast.NewPLpgSQL_stmt_fors()
	fors.Var = varName
	fors.Query = makeExpr(text1, plpgsqlast.RAW_PARSE_DEFAULT)
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
		text, _, err := l.scanFragment(';')
		if err != nil {
			l.Error(err.Error())
			s.Query = plpgsqlast.NewPLpgSQL_expr("")
			return s
		}
		s.Query = makeExpr(text, plpgsqlast.RAW_PARSE_DEFAULT)
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

	text, term, err := l.scanFragment(K_WHEN)
	if err != nil {
		l.Error(err.Error())
		return plpgsqlast.NewPLpgSQL_expr("")
	}
	l.pushBack(term) // hand the WHEN back to the grammar
	return makeExpr(text, plpgsqlast.RAW_PARSE_PLPGSQL_EXPR)
}

// readSQLExprUntil scans an expression up to (and consuming) the first of the
// given terminators at paren depth 0, returning it as a PLpgSQL_expr. It is the
// Go analogue of PG's read_sql_expression, which takes the terminator token; the
// `;`, K_THEN, and K_LOOP forms (expr_until_semi / _then / _loop in pl_gram.y)
// differ only in which terminator they pass. Parsed is left nil, as in
// readSQLExpr.
func (l *lexer) readSQLExprUntil(terminators ...int) *plpgsqlast.PLpgSQL_expr {
	text, _, err := l.scanFragment(terminators...)
	if err != nil {
		l.Error(err.Error())
		return plpgsqlast.NewPLpgSQL_expr("")
	}
	e := plpgsqlast.NewPLpgSQL_expr(text)
	e.ParseMode = plpgsqlast.RAW_PARSE_PLPGSQL_EXPR
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
