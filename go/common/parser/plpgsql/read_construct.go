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

package plpgsql

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
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

// forVariable carries the loop target(s) of a FOR/FOREACH from the for_variable
// production (PG's for_variable struct). name is the target text (a single name
// or a comma-separated list, kept for deparse). scalar and row mirror PG's fields:
// scalar is a resolved single scalar variable; row is a record datum or a ROW
// built from a scalar list. All nil means the single target did not resolve to a
// variable in the body — but that is NOT necessarily an error: it may be an
// integer-loop variable name (for_control decides), a field of a named-composite
// record we cannot type without a catalog, or the routine's own argument (we parse
// the body only, so arguments are invisible). A comma list sets both scalar and
// row (scalar = first member, row = the built ROW), which is how the "integer FOR
// must have only one target" check is detected.
type forVariable struct {
	name   string
	scalar plpgsqlast.Datum
	row    plpgsqlast.Datum
}

// isCompositeDatum reports whether a resolved datum is a composite (record or
// row) target — PG's for_variable ROW/REC dtype test. A RECFIELD counts as a
// scalar, matching PG.
func isCompositeDatum(d plpgsqlast.Datum) bool {
	switch d.(type) {
	case *plpgsqlast.PLpgSQL_rec, *plpgsqlast.PLpgSQL_row:
		return true
	}
	return false
}

// scanNext returns the next fully-classified PL/pgSQL token — the Go analogue of
// PG's plpgsql_yylex: a raw token from internalLex with keyword lookup,
// compound-name assembly (T_CWORD), and the T_WORD fallback applied. It is the
// single token source for both the grammar (via Lex) and the hand-scan actions
// in this file, exactly as PG routes both through plpgsql_yylex — there is no
// separate partially-classified path. A name that resolves against the namespace
// is emitted as T_DATUM (carrying the datum), so hand-scan actions that expect a
// keyword in that position recover it via tokKeyword, as PG does with
// tok_is_keyword.
func (l *lexer) scanNext() auxToken {
	a := l.internalLex()
	switch a.tok {
	case IDENT:
		a = l.reclassifyWord(a)
	case PARAM:
		// PG treats PARAM ($1) exactly like IDENT in plpgsql_yylex: it flows
		// through the same word/compound classification. The core scanner gives
		// us the number but no text, so reconstruct the name as "$N" (PG uses
		// yytext). A param is never in our namespace, so it never resolves to a
		// T_DATUM — it becomes T_WORD, or T_CWORD for "$1.field".
		a.str = "$" + strconv.Itoa(a.ival)
		a.quoted = false
		a = l.reclassifyWord(a)
	}
	l.prevToken = a.tok
	return a
}

// scanFragment scans source tokens until one of the terminators appears at
// paren/bracket depth 0, returning the raw source text from the first token up
// to the END of the last content token (excluding trailing whitespace and
// comments, like PG's read_sql_construct, which records endlocation at the last
// accepted token), plus the terminator token. This is the read_sql_construct core.
func (l *lexer) scanFragment(terminators ...int) (string, auxToken, error) {
	// read_sql_construct (and make_execsql_stmt) scan the fragment in EXPR mode:
	// identifiers inside embedded SQL/expression text are left for the SQL parser
	// and are not resolved to a scalar T_DATUM here — only RECFIELD side effects
	// remain, exactly as PG. Save/restore so the enclosing statement-level mode
	// (NORMAL or DECLARE) is untouched. This is what keeps fragment capture
	// byte-identical to the pre-resolution behavior.
	saveMode := l.mode
	l.mode = lookupExpr
	defer func() { l.mode = saveMode }()

	parenLevel := 0
	start := -1
	lastEnd := -1
	for {
		tok := l.scanNext()
		if parenLevel == 0 && slices.Contains(terminators, tok.tok) {
			if start < 0 || lastEnd < 0 {
				return "", tok, errors.New("missing expression")
			}
			text := strings.TrimRight(l.input[start:lastEnd], " \t\r\n")
			if text == "" {
				return "", tok, errors.New("missing expression")
			}
			return text, tok, nil
		}
		if start < 0 {
			start = tok.pos
		}
		lastEnd = tok.end // end of the most recent content token
		switch tok.tok {
		case '(', '[':
			parenLevel++
		case ')', ']':
			parenLevel--
			if parenLevel < 0 {
				return "", tok, errors.New("mismatched parentheses")
			}
		case ';', 0:
			// A ';' or EOF that is not the awaited terminator. Mirroring PG's
			// read_sql_construct (`if (tok == 0 || tok == ';') { if (parenlevel
			// != 0) yyerror("mismatched parentheses"); … }`): a ';' or EOF reached
			// inside unbalanced parens is a mismatched-parentheses error — so a
			// stray ';' at depth > 0 (e.g. `x := (1; 2)`) is rejected, not absorbed
			// into the fragment. At depth 0 the scan ran off the end of the
			// fragment before its terminator.
			if parenLevel != 0 {
				return "", tok, errors.New("mismatched parentheses")
			}
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
// terminator back since COLLATE / NOT NULL / := / DEFAULT / ';' — or ',' / ')'
// for a cursor argument list — belong to the grammar. The ',' and ')'
// terminators only match at paren depth 0, which never follows a variable-decl
// type (inner type parens like numeric(10,2) are at depth ≥1), so one terminator
// set serves both the variable and cursor-arg contexts. Mirrors PG's
// context-independent read_datatype (which likewise stops on K_COLLATE).
func (l *lexer) readDatatype() *plpgsqlast.PLpgSQL_type {
	text, term, err := l.scanFragment(';', COLON_EQUALS, '=', K_DEFAULT, K_NOT, K_COLLATE, ',', ')')
	if err != nil {
		l.Error(err.Error())
		return plpgsqlast.NewPLpgSQL_type("")
	}
	l.pushBack(term)
	return plpgsqlast.NewPLpgSQL_type(text)
}

// readSQLExpr scans an expression up to ';' (which it consumes) and returns it as
// a PLpgSQL_expr — PG's read_sql_expression(';', ";") (pl_gram.y). Parsed is left
// nil — turning the text into an ast.Stmt is a separate step.
func (l *lexer) readSQLExpr() *plpgsqlast.PLpgSQL_expr {
	return l.readSQLExprUntil(';')
}

// isRecordType reports whether a declared type names a composite variable we can
// recognize without a catalog: the RECORD pseudo-type, or a `%ROWTYPE` (the row
// type of a table). A `%TYPE` is the type of one column and stays scalar. A named
// composite type is indistinguishable from a scalar without the catalog, so it is
// not recognized here — it stays a PLpgSQL_var.
//
// No direct PG equivalent: PG decides composite-ness from the catalog when it
// resolves the type (plpgsql_build_datatype / plpgsql_parse_wordtype, pl_comp.c);
// this is the syntactic no-catalog approximation of that.
func isRecordType(typeText string) bool {
	t := strings.TrimSpace(typeText)
	if strings.EqualFold(t, "record") {
		return true
	}
	const rowtype = "%rowtype"
	return len(t) >= len(rowtype) && strings.EqualFold(t[len(t)-len(rowtype):], rowtype)
}

// makeDeclDatum builds the datum for a variable declaration — the parse-level
// half of PG's plpgsql_build_variable (pl_comp.c), which builds a PLpgSQL_var or
// (via plpgsql_build_record) a PLpgSQL_rec. Here a recognizable composite
// (isRecordType) becomes a PLpgSQL_rec, otherwise a scalar PLpgSQL_var. The
// parse-level fields are the same either way, so the two deparse identically (a
// record has no COLLATE, so that field is dropped for a rec).
func makeDeclDatum(name string, isConst bool, dt *plpgsqlast.PLpgSQL_type, collate string, notNull bool, def *plpgsqlast.PLpgSQL_expr) plpgsqlast.Datum {
	if dt != nil && isRecordType(dt.TypeName) {
		r := plpgsqlast.NewPLpgSQL_rec(name)
		r.IsConst = isConst
		r.DataType = dt
		r.NotNull = notNull
		r.DefaultVal = def
		return r
	}
	v := plpgsqlast.NewPLpgSQL_var(name)
	v.IsConst = isConst
	v.DataType = dt
	v.Collate = collate
	v.NotNull = notNull
	v.DefaultVal = def
	return v
}

// makeExpr wraps captured fragment text in a PLpgSQL_expr with the given parse
// mode — the analogue of the PLpgSQL_expr PG allocates in read_sql_construct
// (pl_gram.y). Parsed is left nil, as elsewhere.
func makeExpr(text string, mode plpgsqlast.RawParseMode) *plpgsqlast.PLpgSQL_expr {
	e := plpgsqlast.NewPLpgSQL_expr(text)
	e.ParseMode = mode
	return e
}

// readForVariableDatum handles a FOR/FOREACH target that resolved to a variable
// (PG's for_variable T_DATUM arm). A record/row is a composite target with no
// comma list. A scalar may head a comma-separated list, which becomes a ROW of
// scalars via readScalarList; the joined member names are kept for deparse.
func (l *lexer) readForVariableDatum(name string, d plpgsqlast.Datum) forVariable {
	if isCompositeDatum(d) {
		return forVariable{name: name, row: d}
	}
	fv := forVariable{name: name, scalar: d}
	tok := l.scanNext()
	l.pushBack(tok)
	if tok.tok == ',' {
		row := l.readScalarList(name, d)
		fv.row = row
		fv.name = strings.Join(row.Fieldnames, ", ")
	}
	return fv
}

// readForVariableWord handles a FOR/FOREACH target that did NOT resolve to a
// variable in the body (PG's for_variable T_WORD / T_CWORD arms). With no
// following comma it may still be an integer-loop variable name (for_control
// decides) or — for a loop over rows — an argument or record field we cannot see,
// which for_control accepts. With a comma it heads a scalar list (readScalarList).
func (l *lexer) readForVariableWord(name string) forVariable {
	tok := l.scanNext()
	l.pushBack(tok)
	if tok.tok != ',' {
		return forVariable{name: name}
	}
	row := l.readScalarList(name, nil)
	return forVariable{name: strings.Join(row.Fieldnames, ", "), row: row}
}

// readScalarList reads a comma-separated scalar target list (the first target plus
// the rest) and builds a ROW datum — the Go port of read_into_scalar_list. A
// resolved scalar member is check_assignable'd (rejecting a CONSTANT); a resolved
// record/row member is "not a scalar variable". An unresolved member — a plain
// name (T_WORD) or a compound (T_CWORD) — is accepted, not rejected: parsing the
// body only, we cannot tell a genuinely-unknown name from a routine argument or a
// named-composite field we have no catalog for, so we take the superset rather
// than fail-close on a body PG accepts. firstDatum is nil when the first target
// did not resolve.
func (l *lexer) readScalarList(firstName string, firstDatum plpgsqlast.Datum) *plpgsqlast.PLpgSQL_row {
	// Varnos stays 1:1 with Fieldnames; a member that did not resolve to a datum
	// (the first when firstDatum is nil, or an accepted unresolved member) records
	// noDno so the two slices can always be indexed together.
	fieldnames := []string{firstName}
	varnos := []int{noDno}
	if firstDatum != nil {
		l.checkAssignable(firstDatum)
		varnos[0] = firstDatum.DatumNo()
	}
	for {
		sep := l.scanNext()
		if sep.tok != ',' {
			l.pushBack(sep)
			break
		}
		// PG caps the list at 1024 members (read_into_scalar_list, pl_gram.y).
		if len(fieldnames) >= maxScalarListTargets {
			l.Error("too many INTO variables specified")
			break
		}
		n := l.scanNext()
		dno := noDno
		switch n.tok {
		case T_DATUM:
			// check_assignable runs before the not-a-scalar test, matching PG's
			// read_into_scalar_list (pl_gram.y:3609-3610): a CONSTANT record member
			// reports "declared CONSTANT" (PG's first error), not "is not a scalar
			// variable".
			l.checkAssignable(n.datum)
			if isCompositeDatum(n.datum) {
				l.Error(fmt.Sprintf("%q is not a scalar variable", n.str))
			} else {
				dno = n.datum.DatumNo()
			}
		case T_WORD, T_CWORD:
			// Unresolved member — could be an argument or a named-composite field
			// we cannot see; accept rather than reject a body PG accepts.
		default:
			l.Error("syntax error")
			l.pushBack(n)
			return newRow(fieldnames, varnos, l)
		}
		fieldnames = append(fieldnames, n.str)
		varnos = append(varnos, dno)
	}
	return newRow(fieldnames, varnos, l)
}

// noDno marks a ROW member (Varnos entry) that did not resolve to a datum — an
// accepted-but-unresolvable T_CWORD member, or an unresolved first target. It
// keeps Varnos aligned 1:1 with Fieldnames without inventing a fake datum.
const noDno = -1

// maxScalarListTargets bounds a comma-separated scalar targetlist, matching PG's
// fixed 1024-entry buffer in read_into_scalar_list.
const maxScalarListTargets = 1024

// newRow builds a ROW datum from the collected member names/dnos and registers it.
// Varnos is 1:1 with Fieldnames; an entry of noDno marks a member we could not
// resolve to a datum (see readScalarList). PG's ROW has a dno for every member; we
// diverge only for the members a catalog would be needed to resolve, and safely so
// while PLpgSQL_expr.Parsed is nil (we do not execute).
func newRow(fieldnames []string, varnos []int, l *lexer) *plpgsqlast.PLpgSQL_row {
	row := plpgsqlast.NewPLpgSQL_row("(unnamed row)")
	row.Fieldnames = fieldnames
	row.Varnos = varnos
	l.addDatum(row)
	return row
}

// checkQueryForTarget check-assignables the loop variable of a loop over rows
// (query or dynamic FOR), porting the target handling in PG's for_control. A
// resolved record or scalar target is assignability-checked. An unresolved single
// target is accepted, not rejected: parsing the body only, we cannot see routine
// arguments or resolve named-composite record fields, so PG's "loop variable of
// loop over rows must be a record variable or list of scalar variables" would
// fail-close on bodies PG accepts. Not applied to a cursor FOR, which creates its
// own record loop variable.
func (l *lexer) checkQueryForTarget(v forVariable) {
	if v.row != nil {
		l.checkAssignable(v.row)
	} else if v.scalar != nil {
		l.checkAssignable(v.scalar)
	}
}

// checkForeachTarget check-assignables the loop variable of a FOREACH, porting the
// check in PG's stmt_foreach_a action. As for a query FOR, an unresolved single
// target is accepted rather than rejected (see checkQueryForTarget).
func (l *lexer) checkForeachTarget(v forVariable) {
	if v.row != nil {
		l.checkAssignable(v.row)
	} else if v.scalar != nil {
		l.checkAssignable(v.scalar)
	}
}

// readForControl is the manual scan behind the for_control production, porting
// PG's for_control action (pl_gram.y). It runs after `for_variable K_IN` and
// decides between an integer FOR (`lower .. upper [BY step]`), a query FOR, and a
// dynamic FOR (`IN EXECUTE`) by peeking K_EXECUTE and then scanning the first
// construct up to ".." or LOOP. A bound-cursor FOR (PG's stmt_forc) is not
// distinguished: that needs a resolved refcursor T_DATUM, so it reads as a query
// FOR. v is the already-parsed loop target(s).
func (l *lexer) readForControl(v forVariable) plpgsqlast.Stmt {
	tok := l.scanNext()
	if tok.tok == K_EXECUTE {
		// Dynamic FOR: FOR var IN EXECUTE query [USING …] LOOP.
		l.checkQueryForTarget(v)
		dynfors := plpgsqlast.NewPLpgSQL_stmt_dynfors()
		dynfors.Var = v.name
		query, endtoken := l.readSQLConstruct(plpgsqlast.RAW_PARSE_PLPGSQL_EXPR, K_LOOP, K_USING)
		dynfors.Query = query
		if endtoken == K_USING {
			dynfors.Params, _ = l.readUsingList(',', K_LOOP)
		}
		return dynfors
	}

	// Cursor FOR loop: FOR r IN <cursor>. PG treats a refcursor-typed target as a
	// cursor FOR and requires it to be bound (declared CURSOR FOR <query>). We do
	// not otherwise distinguish a bound cursor FOR from a query FOR — it reads as a
	// query over the cursor name below — but we reject an unbound one, matching PG's
	// "cursor FOR loop must use a bound cursor variable". A cursor FOR builds its
	// own record loop variable, so (unlike a query FOR) its loop variable is NOT
	// required to be a known variable — hence the target check is skipped for it.
	isCursorFor := tok.tok == T_DATUM && isCursorVar(tok.datum)
	if isCursorFor && isUnboundCursorVar(tok.datum) {
		l.Error("cursor FOR loop must use a bound cursor variable")
	}

	reverse := false
	// tokKeyword recovers REVERSE shadowed by a like-named variable (PG's
	// tok_is_keyword).
	if l.tokKeyword(tok) == K_REVERSE {
		reverse = true
	} else {
		l.pushBack(tok)
	}

	// The first construct may be either an integer-loop bound or a whole query, so
	// scan it as RAW_PARSE_DEFAULT and relabel to an expression if we see "..",
	// matching PG's for_control.
	expr1, endtoken := l.readSQLConstruct(plpgsqlast.RAW_PARSE_DEFAULT, DOT_DOT, K_LOOP)

	if endtoken == DOT_DOT {
		// Integer FOR: lower .. upper [BY step]. Bounds are expressions. A
		// comma-separated target list (both scalar and row set) is only valid for a
		// loop over rows. The loop variable is otherwise a fresh integer variable,
		// so no assignability check applies to it.
		if v.scalar != nil && v.row != nil {
			l.Error("integer FOR loop must have only one target variable")
		}
		// PG builds a private INT4 loop variable and adds it to the loop namespace
		// (plpgsql_build_variable, add2namespace=true, pl_gram.y:1514), so a
		// reference to the loop variable in the body resolves to it — `i := i + 1`
		// is an assignment, not an opaque SQL statement. It lives in the loop scope
		// opt_loop_label opened (popped after END LOOP) and may shadow an outer
		// variable, matching PG. Skip the (already-errored) multi-target case.
		if v.row == nil {
			l.declareVar(v.name, plpgsqlast.NewPLpgSQL_var(v.name))
		}
		fori := plpgsqlast.NewPLpgSQL_stmt_fori()
		fori.Var = v.name
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
	// A genuine loop over rows requires a record or scalar-list target; a cursor
	// FOR supplies its own record variable and is exempt.
	if !isCursorFor {
		l.checkQueryForTarget(v)
	} else {
		// PG builds a private RECORD loop variable for a cursor FOR
		// (plpgsql_build_record, add2namespace=true, pl_gram.y:1428), so rec.field
		// references in the body resolve. Like the integer case, it lives in the
		// loop scope and may shadow an outer variable.
		l.declareVar(v.name, plpgsqlast.NewPLpgSQL_rec(v.name))
	}
	fors := plpgsqlast.NewPLpgSQL_stmt_fors()
	fors.Var = v.name
	fors.Query = expr1
	return fors
}

// scanStmtText scans to the terminating ';' that ends an embedded SQL statement
// and returns its verbatim text from startPos (the first token's byte offset,
// already consumed by the grammar). It is the Go port of PG's make_execsql_stmt
// scan loop (minus INTO extraction): a ';' terminates only at paren depth 0 and,
// inside a CREATE [OR REPLACE] {FUNCTION|PROCEDURE} definition, outside any
// BEGIN/CASE … END block — so the inner semicolons of a `BEGIN ATOMIC … END`
// routine body do not cut the statement short. firstIsCreate says whether the
// already-consumed first token was the word "create".
func (l *lexer) scanStmtText(firstIsCreate bool, startPos int) string {
	// make_execsql_stmt scans the statement in EXPR mode: identifiers inside the
	// SQL text are left for the SQL parser, not resolved to a T_DATUM here (PG
	// flips to NORMAL only to re-parse an INTO clause, which we fold into the text
	// instead). Save/restore so the enclosing statement-level mode is untouched.
	saveMode := l.mode
	l.mode = lookupExpr
	defer func() { l.mode = saveMode }()

	parenDepth := 0
	beginDepth := 0
	inRoutineDef := false
	// PG records the first few tokens to spot CREATE [OR REPLACE] FUNCTION; we
	// track the equivalent with a small state seeded by the first token.
	var tokens []byte
	if firstIsCreate {
		tokens = append(tokens, 'c')
	}
	for {
		tok := l.scanNext()

		if len(tokens) > 0 && tokens[0] == 'c' && len(tokens) < 4 {
			switch {
			case tok.tok == K_OR:
				tokens = append(tokens, 'o')
			case tok.tok == T_WORD && strings.EqualFold(tok.str, "replace"):
				tokens = append(tokens, 'r')
			case tok.tok == T_WORD && (strings.EqualFold(tok.str, "function") || strings.EqualFold(tok.str, "procedure")):
				tokens = append(tokens, 'f')
			default:
				tokens = append(tokens, 0)
			}
			if (len(tokens) > 1 && tokens[1] == 'f') ||
				(len(tokens) > 3 && tokens[1] == 'o' && tokens[2] == 'r' && tokens[3] == 'f') {
				inRoutineDef = true
			}
		}

		// Track paren nesting (PG counts only parens here, not brackets).
		switch tok.tok {
		case '(':
			parenDepth++
		case ')':
			if parenDepth > 0 {
				parenDepth--
			}
		}
		// BEGIN/CASE … END nesting matters only within a routine definition.
		if inRoutineDef && parenDepth == 0 {
			if tok.tok == K_BEGIN || tok.tok == K_CASE {
				beginDepth++
			} else if tok.tok == K_END && beginDepth > 0 {
				beginDepth--
			}
		}
		if tok.tok == ';' && parenDepth == 0 && beginDepth == 0 {
			return strings.TrimRight(l.input[startPos:tok.pos], " \t\r\n")
		}
		if tok.tok == 0 {
			l.Error("unexpected end of function definition")
			return ""
		}
	}
}

// makeWordStmt implements the assign-vs-execsql dispatch for a word-initiated
// statement (PG decides this in the stmt_execsql T_WORD action). word is the
// already-consumed first token; startPos its byte offset. If the next token is an
// assignment operator we build an assignment; otherwise the whole statement is
// captured as execsql.
//
// A resolved assignment target is a T_DATUM and reaches stmt_assign directly, so
// PG's T_WORD/T_CWORD arms are only for words that did NOT resolve. Most such
// words begin an embedded SQL statement — but a compound like `rec.field` on a
// composite variable we cannot recognize as composite without a catalog (a named
// row type is indistinguishable from a scalar, so field access on it does not
// resolve) arrives here as an unresolved T_CWORD, and it is still an assignment
// target, not a SQL statement. When an assignment operator follows we therefore
// build the assignment (PG errors in that spot, since a real target would be a
// T_DATUM; with no such resolution we treat it as the assignment it plainly is —
// no valid SQL statement begins `identifier[.identifier] :=`). The target is kept
// as text, as PG keeps a resolved datum's name.
//
// The lvalue can also extend past the leading word: the scanner assembles a
// compound name only up to three parts (A.B.C), and it never folds a subscript
// into the word, so a deeper field path (`b.c.c2.x`) or a subscripted target
// (`a[i]`, `a.c1[1].i`) arrives as the word followed by more `.`/`[` tokens. When
// the token after the word is `.` or `[` we scan the whole target up to the
// assignment operator with readAssignTarget — the same path the resolved T_DATUM
// subscript case (makeAssignStmt) uses.
func (l *lexer) makeWordStmt(word string, startPos int) plpgsqlast.Stmt {
	tok := l.scanNext()
	switch tok.tok {
	case COLON_EQUALS, '=':
		stmt := plpgsqlast.NewPLpgSQL_stmt_assign(word)
		stmt.Expr = l.readSQLExpr()
		return stmt
	case '.', '[':
		l.pushBack(tok)
		stmt := plpgsqlast.NewPLpgSQL_stmt_assign(l.readAssignTarget(startPos))
		stmt.Expr = l.readSQLExpr()
		return stmt
	}
	l.pushBack(tok)
	return l.makeExecSQLStmt(T_WORD, strings.EqualFold(word, "create"), startPos)
}

// makeExecSQLStmt is the Go port of PG's make_execsql_stmt: it scans an embedded
// SQL statement to its terminating ';' and, like PG, lifts out any PL/pgSQL INTO
// clause so the stored query text is valid stand-alone SQL — a plain
// SELECT … INTO, RETURNING … INTO, or INTO STRICT / multi-target clause is not
// accepted by the SQL grammar. firsttoken is the already-consumed first token,
// used to recognise the SQL uses of INTO that are *not* a PL/pgSQL target
// (IMPORT … INTO) and to seed the CREATE … routine-body detection; firstIsCreate
// says that first token was the word "create"; startPos is its byte offset.
func (l *lexer) makeExecSQLStmt(firsttoken int, firstIsCreate bool, startPos int) *plpgsqlast.PLpgSQL_stmt_execsql {
	// Scan the statement in EXPR mode so identifiers in the SQL text are left for
	// the SQL parser, not resolved to a T_DATUM here — matching scanStmtText and
	// PG's make_execsql_stmt. Save/restore keeps the statement-level mode intact.
	saveMode := l.mode
	l.mode = lookupExpr
	defer func() { l.mode = saveMode }()

	parenDepth, beginDepth := 0, 0
	inRoutineDef := false
	var tokens []byte
	if firstIsCreate {
		tokens = append(tokens, 'c')
	}
	prevTok := firsttoken
	intoStart, intoEnd, end := -1, -1, -1
	var haveInto, strict bool
	var target string

	for {
		tok := l.scanNext()

		// CREATE [OR REPLACE] {FUNCTION|PROCEDURE} detection, matching scanStmtText.
		if len(tokens) > 0 && tokens[0] == 'c' && len(tokens) < 4 {
			switch {
			case tok.tok == K_OR:
				tokens = append(tokens, 'o')
			case tok.tok == T_WORD && strings.EqualFold(tok.str, "replace"):
				tokens = append(tokens, 'r')
			case tok.tok == T_WORD && (strings.EqualFold(tok.str, "function") || strings.EqualFold(tok.str, "procedure")):
				tokens = append(tokens, 'f')
			default:
				tokens = append(tokens, 0)
			}
			if (len(tokens) > 1 && tokens[1] == 'f') ||
				(len(tokens) > 3 && tokens[1] == 'o' && tokens[2] == 'r' && tokens[3] == 'f') {
				inRoutineDef = true
			}
		}

		switch tok.tok {
		case '(':
			parenDepth++
		case ')':
			if parenDepth > 0 {
				parenDepth--
			}
		}
		if inRoutineDef && parenDepth == 0 {
			if tok.tok == K_BEGIN || tok.tok == K_CASE {
				beginDepth++
			} else if tok.tok == K_END && beginDepth > 0 {
				beginDepth--
			}
		}

		if tok.tok == ';' && parenDepth == 0 && beginDepth == 0 {
			end = tok.pos
			break
		}
		if tok.tok == 0 {
			// Unterminated statement. Match scanStmtText: flag the error and
			// return an empty node rather than slicing with the EOF token's
			// zero position (which would run off the source).
			l.Error("unexpected end of function definition")
			return plpgsqlast.NewPLpgSQL_stmt_execsql()
		}

		// A PL/pgSQL INTO clause, but only at paren depth 0 and not one of the SQL
		// uses of INTO: INSERT INTO / MERGE INTO (INTO adjacent to the command
		// word) or IMPORT … INTO (INTO anywhere in an IMPORT statement).
		if tok.tok == K_INTO && parenDepth == 0 &&
			prevTok != K_INSERT && prevTok != K_MERGE && firsttoken != K_IMPORT {
			if haveInto {
				l.Error("INTO specified more than once")
			}
			haveInto = true
			intoStart = tok.pos
			strict, target, intoEnd = l.readExecSQLIntoTarget()
		}
		prevTok = tok.tok
	}

	var query string
	if haveInto && intoStart >= 0 && intoEnd >= 0 {
		query = strings.TrimRight(l.input[startPos:intoStart]+l.input[intoEnd:end], " \t\r\n")
	} else {
		query = strings.TrimRight(l.input[startPos:end], " \t\r\n")
	}

	stmt := plpgsqlast.NewPLpgSQL_stmt_execsql()
	stmt.Sqlstmt = makeExpr(query, plpgsqlast.RAW_PARSE_DEFAULT)
	stmt.Into = haveInto
	stmt.Strict = strict
	stmt.Target = target
	return stmt
}

// readExecSQLIntoTarget reads a PL/pgSQL INTO target following the INTO keyword:
// an optional STRICT, then a comma-separated list of target names. It stops at
// the first token that cannot continue the list (a SQL keyword, ';', etc.),
// pushes that terminator back for the caller's scan loop to re-read, and returns
// STRICT, the verbatim target text, and the terminator's byte offset (the end of
// the INTO clause). PG resolves the targets to variables via read_into_target;
// we keep the text, mirroring dynexecute's INTO handling.
func (l *lexer) readExecSQLIntoTarget() (strict bool, target string, endPos int) {
	tok := l.scanNext()
	if tok.tok == K_STRICT {
		strict = true
		tok = l.scanNext()
	}
	if tok.tok == 0 {
		// End of input (or a lexical error) right after INTO [STRICT], with no
		// target name. Push the terminator back so the caller's scan loop reports
		// the unterminated statement, rather than slicing with the EOF token's
		// zero position below. -1 endPos keeps the caller off the INTO slice path.
		l.pushBack(tok)
		return strict, "", -1
	}
	targetStart := tok.pos
	for {
		next := l.scanNext()
		if next.tok == ',' {
			// Consume the next target name and continue the list; a comma with no
			// name after it (end of input, lexical error) is an unterminated list.
			if name := l.scanNext(); name.tok == 0 {
				l.pushBack(name)
				return strict, "", -1
			}
			continue
		}
		if next.tok == 0 {
			// Terminator is end-of-input (or a lexical error): the statement never
			// closed. Push it back for the caller's scan loop, which flags the
			// unterminated statement instead of slicing with next's zero position.
			l.pushBack(next)
			return strict, "", -1
		}
		l.pushBack(next)
		target = strings.TrimRight(l.input[targetStart:next.pos], " \t\r\n")
		return strict, target, next.pos
	}
}

// makeAssignStmt builds an assignment from a resolved target datum, porting the
// parse-level half of PG's stmt_assign. On entry the T_DATUM target has been
// consumed and its trailing lookahead pushed back, so the next token is the
// assignment operator (':=' or '=') or a '[' introducing a subscripted target.
// The target deparses as text (its name, plus any subscripts); the RHS is read as
// a separate expression, so the whole statement round-trips as `target := rhs;`.
func (l *lexer) makeAssignStmt(wd plwdatum, startPos int) plpgsqlast.Stmt {
	// A CONSTANT (or a field of a constant record) may not be assigned to.
	l.checkAssignable(wd.datum)
	tok := l.scanNext()
	switch tok.tok {
	case COLON_EQUALS, '=':
		stmt := plpgsqlast.NewPLpgSQL_stmt_assign(wd.name)
		stmt.Expr = l.readSQLExpr()
		return stmt
	case '[':
		// Subscripted target (`arr[i] := …`): capture the whole target text from
		// the datum's start up to the assignment operator, then read the RHS.
		l.pushBack(tok)
		stmt := plpgsqlast.NewPLpgSQL_stmt_assign(l.readAssignTarget(startPos))
		stmt.Expr = l.readSQLExpr()
		return stmt
	}
	// The scanner only produces a statement-leading T_DATUM when it is followed by
	// an assignment operator or '[' (the AT_STMT_START rule), so anything else is a
	// syntax error.
	l.pushBack(tok)
	l.Error("syntax error")
	return plpgsqlast.NewPLpgSQL_stmt_assign(wd.name)
}

// readAssignTarget scans a subscripted assignment target from startPos up to the
// assignment operator (':=' or '=') at bracket/paren depth 0, returning the
// verbatim target text (e.g. `arr[i]`, `m['k'][2]`). PG parses `arr[i] := v` as
// one ASSIGN expression; we capture just the target and read the RHS separately.
func (l *lexer) readAssignTarget(startPos int) string {
	depth := 0
	for {
		tok := l.scanNext()
		switch tok.tok {
		case '[', '(':
			depth++
		case ']', ')':
			if depth > 0 {
				depth--
			}
		case COLON_EQUALS, '=':
			if depth == 0 {
				return strings.TrimRight(l.input[startPos:tok.pos], " \t\r\n")
			}
		case ';', 0:
			// No assignment operator — a `word[…]` that isn't an assignment is
			// not valid at statement start (PG errors here too).
			l.Error("syntax error")
			return strings.TrimRight(l.input[startPos:tok.pos], " \t\r\n")
		}
	}
}

// makeReturnStmt implements the RETURN dispatch, porting PG's stmt_return action
// (pl_gram.y) and its make_return_stmt / make_return_next_stmt /
// make_return_query_stmt helpers: RETURN [expr], RETURN NEXT expr, RETURN QUERY
// query, and RETURN QUERY EXECUTE dynquery [USING …]. PG's retset / void /
// out-param context checks need compile context we lack.
func (l *lexer) makeReturnStmt() plpgsqlast.Stmt {
	tok := l.scanNext()
	// tokKeyword recovers NEXT/QUERY shadowed by a like-named variable (PG's
	// tok_is_keyword); K_EXECUTE below is not a tok_is_keyword site in PG.
	switch l.tokKeyword(tok) {
	case K_NEXT:
		s := plpgsqlast.NewPLpgSQL_stmt_return_next()
		// PG's make_return_next_stmt peeks the next token: a bare `RETURN NEXT;`
		// (Expr nil) is valid — it is required for a function with OUT parameters,
		// where the current OUT values are returned. Otherwise an expression
		// follows. We can't tell the OUT-param case apart (no compile context), so
		// we accept the bare form syntactically, matching both PG shapes.
		if tok := l.scanNext(); tok.tok == ';' {
			return s
		} else {
			l.pushBack(tok)
		}
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
				l.Error("syntax error")
				return stmt
			}
			stmt.Into = true
			// K_INTO is a terminator so a second INTO surfaces as endtoken and
			// re-enters the loop, tripping the multiple-INTO guard above — PG
			// gets this from read_into_target stopping and the following yylex()
			// (pl_gram.y stmt_dynexecute).
			stmt.Strict, stmt.Target, endtoken = l.readIntoTarget(K_USING, ';', K_INTO)
		case K_USING:
			if len(stmt.Params) > 0 {
				l.Error("syntax error")
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

	// Optional elog severity level. tokKeyword recovers a level keyword shadowed
	// by a like-named variable, matching PG's tok_is_keyword.
	switch l.tokKeyword(tok) {
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
		stmt.HasMessage = true
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
		// Must be a condition name or SQLSTATE. tokKeyword recovers the SQLSTATE
		// keyword when an implicit `sqlstate` handler variable shadows it (A5/A6).
		if l.tokKeyword(tok) == K_SQLSTATE {
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

// tokKeyword returns the keyword token a scanned token should be treated as when
// the grammar expects a keyword in this position — the Go analogue of PG's
// tok_is_keyword (pl_gram.y:2572). A word the scanner resolved to a variable
// (T_DATUM) shadows a like-named keyword; here we recheck by name so the keyword
// still wins where one is required. An unquoted, single-component datum whose name
// matches an unreserved keyword is treated as that keyword; a quoted or compound
// datum is never a keyword, and a reserved keyword can never be shadowed (it wins
// before resolution) — both pass through unchanged, as does the normal case where
// no variable shadowed the keyword.
//
// It is applied at every position PG guards with tok_is_keyword: GET DIAGNOSTICS
// items, RAISE options and level, RAISE SQLSTATE, the FETCH/MOVE direction, RETURN
// NEXT/QUERY, the OPEN [NO] SCROLL option, and the integer-FOR REVERSE. Datatype
// keywords (%TYPE/%ROWTYPE/ARRAY) are captured as raw text by readDatatype, never
// as keyword tokens, so they cannot be shadowed and need no recovery.
func (l *lexer) tokKeyword(a auxToken) int {
	if a.tok == T_DATUM && !a.quoted && a.datumNames == 1 {
		if kw, ok := unreservedKeywords[a.str]; ok {
			return kw
		}
	}
	return a.tok
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
		switch l.tokKeyword(tok) {
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
	if !stmt.HasMessage {
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

// appendCondition appends a condition to a WHEN clause's OR-list — PG's
// proc_conditions production (pl_gram.y:2355). Helper for the goyacc fast-append
// reason (see appendElsif).
func appendCondition(cs []*plpgsqlast.PLpgSQL_condition, c *plpgsqlast.PLpgSQL_condition) []*plpgsqlast.PLpgSQL_condition {
	return append(cs, c)
}

// readGetDiagItem reads the diagnostic-kind keyword of a GET DIAGNOSTICS item
// (PG's getdiag_item action, which yylex()es the keyword itself). It returns the
// PLpgSQL_getdiag_kind as an int (the grammar's <ival>), erroring "unrecognized
// GET DIAGNOSTICS item" for anything else.
func (l *lexer) readGetDiagItem() int {
	tok := l.scanNext()
	var kind plpgsqlast.PLpgSQL_getdiag_kind
	switch l.tokKeyword(tok) {
	case K_ROW_COUNT:
		kind = plpgsqlast.PLPGSQL_GETDIAG_ROW_COUNT
	case K_PG_ROUTINE_OID:
		kind = plpgsqlast.PLPGSQL_GETDIAG_ROUTINE_OID
	case K_PG_CONTEXT:
		kind = plpgsqlast.PLPGSQL_GETDIAG_CONTEXT
	case K_PG_EXCEPTION_DETAIL:
		kind = plpgsqlast.PLPGSQL_GETDIAG_ERROR_DETAIL
	case K_PG_EXCEPTION_HINT:
		kind = plpgsqlast.PLPGSQL_GETDIAG_ERROR_HINT
	case K_PG_EXCEPTION_CONTEXT:
		kind = plpgsqlast.PLPGSQL_GETDIAG_ERROR_CONTEXT
	case K_COLUMN_NAME:
		kind = plpgsqlast.PLPGSQL_GETDIAG_COLUMN_NAME
	case K_CONSTRAINT_NAME:
		kind = plpgsqlast.PLPGSQL_GETDIAG_CONSTRAINT_NAME
	case K_PG_DATATYPE_NAME:
		kind = plpgsqlast.PLPGSQL_GETDIAG_DATATYPE_NAME
	case K_MESSAGE_TEXT:
		kind = plpgsqlast.PLPGSQL_GETDIAG_MESSAGE_TEXT
	case K_TABLE_NAME:
		kind = plpgsqlast.PLPGSQL_GETDIAG_TABLE_NAME
	case K_SCHEMA_NAME:
		kind = plpgsqlast.PLPGSQL_GETDIAG_SCHEMA_NAME
	case K_RETURNED_SQLSTATE:
		kind = plpgsqlast.PLPGSQL_GETDIAG_RETURNED_SQLSTATE
	default:
		l.Error("unrecognized GET DIAGNOSTICS item")
	}
	return int(kind)
}

// checkGetDiagItems validates each item against the diagnostics area (PG's
// per-item switch in stmt_getdiag): ROW_COUNT / PG_ROUTINE_OID are only valid in
// CURRENT, the error/context/name items only in STACKED, and PG_CONTEXT in both.
func (l *lexer) checkGetDiagItems(stmt *plpgsqlast.PLpgSQL_stmt_getdiag) {
	for _, item := range stmt.DiagItems {
		switch item.Kind {
		case plpgsqlast.PLPGSQL_GETDIAG_ROW_COUNT,
			plpgsqlast.PLPGSQL_GETDIAG_ROUTINE_OID:
			if stmt.IsStacked {
				l.Error("diagnostics item " + item.Kind.KindName() +
					" is not allowed in GET STACKED DIAGNOSTICS")
			}
		case plpgsqlast.PLPGSQL_GETDIAG_ERROR_CONTEXT,
			plpgsqlast.PLPGSQL_GETDIAG_ERROR_DETAIL,
			plpgsqlast.PLPGSQL_GETDIAG_ERROR_HINT,
			plpgsqlast.PLPGSQL_GETDIAG_RETURNED_SQLSTATE,
			plpgsqlast.PLPGSQL_GETDIAG_COLUMN_NAME,
			plpgsqlast.PLPGSQL_GETDIAG_CONSTRAINT_NAME,
			plpgsqlast.PLPGSQL_GETDIAG_DATATYPE_NAME,
			plpgsqlast.PLPGSQL_GETDIAG_MESSAGE_TEXT,
			plpgsqlast.PLPGSQL_GETDIAG_TABLE_NAME,
			plpgsqlast.PLPGSQL_GETDIAG_SCHEMA_NAME:
			if !stmt.IsStacked {
				l.Error("diagnostics item " + item.Kind.KindName() +
					" is not allowed in GET CURRENT DIAGNOSTICS")
			}
		case plpgsqlast.PLPGSQL_GETDIAG_CONTEXT:
			// allowed in either area
		}
	}
}

// appendDiagItem appends an item to a GET DIAGNOSTICS list — PG's getdiag_list
// production (pl_gram.y:1067). Helper for the goyacc fast-append reason (see
// appendElsif).
func appendDiagItem(items []*plpgsqlast.PLpgSQL_diag_item, item *plpgsqlast.PLpgSQL_diag_item) []*plpgsqlast.PLpgSQL_diag_item {
	return append(items, item)
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
// token. It ports PG's USING-parameter loops, which read each param with
// read_sql_expression2(',', …) (pl_gram.y, e.g. the RAISE / EXECUTE / OPEN USING
// clauses). The append is plain Go (not a grammar action), so the goyacc
// fast-append hazard does not apply.
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
	// tokKeyword recovers a direction keyword shadowed by a like-named variable
	// (PG's tok_is_keyword); a non-keyword T_DATUM stays a datum and falls to the
	// cursor-name case below, matching PG's precedence.
	switch l.tokKeyword(tok) {
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
	case T_DATUM, T_WORD:
		// No direction clause: this is the cursor name — a resolved refcursor
		// variable (T_DATUM), or, unresolved, a plain word (T_WORD). Either way
		// push it back for the grammar's cursor_variable to consume. PG's
		// read_fetch_direction likewise treats a leading T_DATUM as the cursor.
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
	// tokKeyword recovers ALL shadowed by a like-named variable (PG's
	// tok_is_keyword); FROM/IN are reserved and cannot be shadowed.
	switch l.tokKeyword(tok) {
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
	// FETCH ... INTO does not accept STRICT (PG calls read_into_target with
	// strict==NULL, so a leading STRICT is a syntax error, unlike EXECUTE INTO).
	if tok := l.scanNext(); tok.tok == K_STRICT {
		l.Error("syntax error")
		return ""
	} else {
		l.pushBack(tok)
	}
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
		stmt.Args = l.readCursorArgs()
		return stmt
	}

	// Unbound: [NO] SCROLL then FOR query | FOR EXECUTE expr [USING …]. Like PG,
	// a bare NO (not followed by SCROLL) is tolerated — the token after NO simply
	// carries on to the FOR check.
	// tokKeyword recovers NO/SCROLL shadowed by a like-named variable (PG's
	// tok_is_keyword in stmt_open).
	switch l.tokKeyword(tok) {
	case K_NO:
		if t := l.scanNext(); l.tokKeyword(t) == K_SCROLL {
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

// readCursorArgs ports PG's read_cursor_args for the bound-cursor OPEN argument
// list `( … )`. PG resolves each value against the cursor's declared parameter
// row — a `name :=` label matched by name, a bare value by position — and emits
// a clean positional `SELECT val1, val2, …`; the labels and the PL/pgSQL-only
// `:=` never survive into the query it compiles.
//
// We keep no declared-parameter resolution (no T_DATUM row for the cursor), so
// we cannot reorder the values or validate arity/names — an OPEN that PG rejects
// (duplicate, unknown, or wrong-count arguments) still parses here and is left
// for PG to reject at execution. Instead of PG's positional fold we retain each
// argument's surface form — its optional `name :=` label (PG's raw IDENT +
// COLON_EQUALS peek) and its value expression — as a PLpgSQL_cursor_arg. Keeping
// the value expressions separate from the labels lets the body walker analyze
// each value as its own `SELECT <value>`, so PL/pgSQL's `:=` (not valid SQL)
// never reaches the SQL parser, while the labels survive for a faithful deparse.
func (l *lexer) readCursorArgs() []*plpgsqlast.PLpgSQL_cursor_arg {
	if tok := l.scanNext(); tok.tok != '(' {
		l.pushBack(tok)
		l.Error("syntax error, expected \"(\"")
		return nil
	}

	var args []*plpgsqlast.PLpgSQL_cursor_arg
	for {
		// Optional "name :=" label. PG peeks two RAW tokens (plpgsql_peek2 →
		// internal_yylex, with no variable/keyword reclassification) and treats
		// IDENT + COLON_EQUALS as a named argument; a name that happens to match an
		// in-scope variable is still a plain IDENT at the raw level, so it is
		// detected the same way. On a match we keep the label's verbatim source
		// text (so a quoted name round-trips exactly) and consume both tokens; a
		// non-match is pushed back intact.
		var name string
		tok1 := l.internalLex()
		tok2 := l.internalLex()
		if tok1.tok == IDENT && tok2.tok == COLON_EQUALS {
			name = l.input[tok1.pos:tok1.end]
		} else {
			l.pushBack(tok2)
			l.pushBack(tok1)
		}

		expr, endtoken := l.readSQLConstruct(plpgsqlast.RAW_PARSE_PLPGSQL_EXPR, ',', ')')
		args = append(args, plpgsqlast.NewPLpgSQL_cursor_arg(name, expr))
		if endtoken == ')' {
			break
		}
	}

	if tok := l.scanNext(); tok.tok != ';' {
		l.pushBack(tok)
		l.Error("syntax error")
	}

	return args
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
// nil datum) — PG's decl_stmts production (pl_gram.y:468). It is a helper rather
// than an inline `append($1, $2)` so goyacc does not apply its in-place-append
// optimization, which clashes with the conditional and silently drops the result.
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
