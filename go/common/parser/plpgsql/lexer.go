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
	"fmt"
	"strings"

	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast/plpgsqlast"
)

// lexer is the PL/pgSQL scanner: a wrapper around the SQL lexer
// (go/common/parser) that reclassifies identifier words against the PL/pgSQL
// keyword tables and recognizes compound names, mirroring
// postgres/src/pl/plpgsql/src/pl_scanner.c (plpgsql_yylex / internal_yylex).
//
// The SQL lexer already classifies SQL keywords and emits named operator
// tokens; this wrapper re-derives PL/pgSQL tokens from the token text and
// translates the SQL lexer's token codes into the PL/pgSQL grammar's codes.
//
// PG's variable resolution (T_DATUM) and its IdentifierLookup state machine ARE
// ported (see reclassifyWord, namespace.go): a word that resolves against the
// namespace built from the DECLARE section becomes T_DATUM, exactly as in
// plpgsql_yylex. When nothing resolves, an identifier word becomes
// T_WORD/T_CWORD or a keyword.
type lexer struct {
	input     string        // original source, for slicing raw fragment text
	core      *parser.Lexer // the underlying SQL scanner we wrap
	result    *plpgsqlast.PLpgSQL_function
	err       error
	pushback  []auxToken
	lastToken auxToken // last token returned by Lex (the parser's current lookahead)

	// Compile-time resolution state, the analogue of PG's plpgsql_ns_* stack and
	// plpgsql_Datums list. ns resolves identifiers to declared variables; datums
	// is the flat datum list an nsItem.itemNo indexes into. A fresh lexer per
	// parse means the zero values (empty namespace, empty list) are correct.
	ns     namespace
	datums []plpgsqlast.Datum

	// mode is PG's plpgsql_IdentifierLookup — it gates whether a word is resolved
	// to a scalar T_DATUM. It defaults to NORMAL (the zero value); the grammar
	// switches it to DECLARE inside a DECLARE section and to EXPR around fragment
	// reads, so resolution to a scalar happens only at statement-level tokens.
	mode identifierLookup

	// prevToken is the token most recently returned by scanNext — the analogue of
	// PG's plpgsql_yytoken. It feeds the AT_STMT_START test that decides whether a
	// statement-leading word is looked up as a variable.
	prevToken int

	// comp_options preamble state, set by the comp_option grammar actions and
	// read by pl_function when it builds the result — the analogue of PG mutating
	// plpgsql_curr_compile in its comp_option actions. A fresh lexer per parse
	// means the zero values (unspecified) are the correct defaults.
	compResolveOption     plpgsqlast.PLpgSQL_resolve_option
	compPrintStrictParams string
	compDumpExecTree      bool
}

// auxToken is one scanned token plus the data Lex needs to publish it.
type auxToken struct {
	tok    int    // PL/pgSQL token code, or IDENT for a word awaiting reclassification
	str    string // semantic string (lowercased for unquoted words/keywords)
	ival   int    // semantic int (ICONST, PARAM)
	pos    int    // byte offset of the token's start in the source
	end    int    // byte offset just past the token's end (start of a compound's last part's end)
	quoted bool   // a delimited ("...") identifier — never a keyword

	// Resolution results, set only when tok == T_DATUM (the analogue of PG's
	// PLwdatum). datum is the resolved variable; datumNames is how many leading
	// name components identify it (1, 2, or 3), which selects the assignment
	// parse mode. str carries the datum's name text (PG's NameOfDatum).
	datum      plpgsqlast.Datum
	datumNames int
}

// plwdatum is the semantic value the grammar receives for a T_DATUM token, the
// Go analogue of PG's PLwdatum. datum is the resolved variable; name is its
// source text (PG's NameOfDatum); nnames is how many name components identify it
// (1 for a simple name, 2 or 3 for a qualified/compound one). nnames selects the
// assignment parse mode and distinguishes a simple name (usable as a label or a
// bare identifier) from a composite one.
type plwdatum struct {
	datum  plpgsqlast.Datum
	name   string
	nnames int
}

// identifierLookup is the scanner's resolution mode, ported from PG's
// IdentifierLookup enum (plpgsql.h). The zero value is NORMAL, so a freshly
// constructed lexer resolves words by default; the grammar narrows it to DECLARE
// or EXPR where resolution must be suppressed.
type identifierLookup int

const (
	lookupNormal  identifierLookup = iota // resolve words to scalar T_DATUM
	lookupDeclare                         // inside DECLARE — do no variable lookup
	lookupExpr                            // inside a SQL expression — build RECFIELDs only, don't resolve scalars
)

// atStmtStart reports whether prevToken is one that can immediately precede the
// start of a statement, ported from PG's AT_STMT_START macro (pl_scanner.c). At
// statement start a bare word is not resolved to a variable (so a variable named
// like a statement-introducing keyword does not shadow the statement), unless it
// is followed by an assignment operator or a subscript.
func atStmtStart(prevToken int) bool {
	return prevToken == ';' ||
		prevToken == K_BEGIN ||
		prevToken == K_THEN ||
		prevToken == K_ELSE ||
		prevToken == K_LOOP
}

func newLexer(input string) *lexer {
	return &lexer{input: input, core: parser.NewLexer(input)}
}

// Lex implements the goyacc lexer interface. It reads one fully-classified
// PL/pgSQL token from scanNext (the plpgsql_yylex analogue) and publishes its
// semantic value to the parser.
func (l *lexer) Lex(lval *plpgsqlSymType) int {
	a := l.scanNext()
	// Cache the final token so a fragment-scanning action can push the parser's
	// pending lookahead back to us (see beginScan). pos stays the first token's
	// offset, which is what raw-text capture needs even for compound names.
	l.lastToken = a
	lval.str = a.str
	lval.ival = a.ival
	lval.location = a.pos
	if a.tok == T_DATUM {
		// T_DATUM carries a plwdatum in the union slot (goyacc's typed-union
		// accessor reads it back via wdatumUnion()).
		lval.union = plwdatum{datum: a.datum, name: a.str, nnames: a.datumNames}
	}
	return a.tok
}

// internalLex returns the next raw token, popping the pushback stack first.
// It translates the SQL lexer's token codes into PL/pgSQL codes but does not
// reclassify identifier words — that is Lex's job, exactly as PG splits
// internal_yylex from plpgsql_yylex.
func (l *lexer) internalLex() auxToken {
	if n := len(l.pushback); n > 0 {
		t := l.pushback[n-1]
		l.pushback = l.pushback[:n-1]
		return t
	}

	tk := l.core.NextToken()
	if tk == nil || tk.Type == parser.EOF {
		return auxToken{tok: 0} // 0 == EOF for goyacc
	}
	if tk.Type == parser.INVALID {
		// A lexical error (e.g. an unterminated string or quoted identifier).
		// Surface the core scanner's diagnostic rather than reporting a bare
		// end-of-input, and stop scanning.
		if l.core.HasErrors() {
			if e := l.core.FirstError(); e != nil {
				l.Error(e.Error())
			}
		} else {
			l.Error("invalid token")
		}
		return auxToken{tok: 0}
	}

	a := auxToken{
		str:    tk.Value.Str,
		ival:   tk.Value.Ival,
		pos:    tk.Position,
		end:    tk.Position + len(tk.Text),
		quoted: isQuotedIdentifier(tk.Text),
	}
	a.tok = translateToken(tk)
	return a
}

// isQuotedIdentifier reports whether an identifier token came from a
// double-quoted or U&"..." (delimited) identifier. The SQL lexer returns plain
// IDENT for both quoted and unquoted identifiers — it does not distinguish them
// by token type — and signals quotedness only by preserving the quote
// characters in Token.Text (see delimited.go). An identifier's text contains a
// double quote iff it was delimited, so that is our signal. Delimited
// identifiers are never reclassified as keywords, matching PG's !word.quoted.
func isQuotedIdentifier(text string) bool {
	return strings.ContainsRune(text, '"')
}

// pushBack returns a token to the stream so the next internalLex re-reads it.
// The stack is LIFO; callers push in reverse of the desired re-read order.
func (l *lexer) pushBack(a auxToken) {
	l.pushback = append(l.pushback, a)
}

// pushBackToken re-injects a bare token code so the next internalLex returns it,
// mirroring PG's plpgsql_push_back_token(int). Used when a scan consumed a
// terminator the grammar still needs (e.g. handing K_WHEN back before case_when).
func (l *lexer) pushBackToken(tok int) {
	l.pushBack(auxToken{tok: tok})
}

// reclassifyWord turns an identifier-like token (an IDENT, or a PARAM whose str
// has been pre-set to "$N") into the PL/pgSQL token it should be: a resolved
// variable (T_DATUM), a reserved or unreserved keyword, a compound name
// (T_CWORD), or a plain word (T_WORD). It is the Go port of the IDENT/PARAM
// branch of PG's plpgsql_yylex (pl_scanner.c), including the same nested
// two-token lookahead for A.B / A.B.C and the same variable-lookup decisions.
// It returns the fully classified token; str carries the dotted name for a
// compound (PG's NameOfDatum), and datum/datumNames are set when tok == T_DATUM.
func (l *lexer) reclassifyWord(a auxToken) auxToken {
	// Reserved keywords win unconditionally and never start a compound name,
	// mirroring PG's core scanner returning them before any variable lookup.
	if !a.quoted {
		if tok, ok := reservedKeywords[a.str]; ok {
			a.tok = tok
			return a
		}
	}

	tok2 := l.internalLex()
	if tok2.tok == '.' {
		tok3 := l.internalLex()
		if tok3.tok == IDENT && !endsCompound(tok3) {
			tok4 := l.internalLex()
			if tok4.tok == '.' {
				tok5 := l.internalLex()
				if tok5.tok == IDENT && !endsCompound(tok5) {
					// A.B.C
					a.end = tok5.end
					return l.classifyTripword(a, tok3.str, tok5.str)
				}
				// not A.B.C, so just process A.B
				l.pushBack(tok5)
				l.pushBack(tok4)
				a.end = tok3.end
				return l.classifyDblword(a, tok3.str)
			}
			// not A.B.C, so just process A.B
			l.pushBack(tok4)
			a.end = tok3.end
			return l.classifyDblword(a, tok3.str)
		}
		// not A.B, so just process A. A word whose dotted continuation is not a
		// valid name is still looked up unconditionally, matching PG.
		l.pushBack(tok3)
		l.pushBack(tok2)
		return l.classifyWord(a, true)
	}

	// not A.B, so just process A. Resolve it to a variable except at statement
	// start when it isn't followed by an assignment operator or a subscript — the
	// AT_STMT_START special case that lets a variable be named like a
	// statement-introducing keyword.
	l.pushBack(tok2)
	lookup := !atStmtStart(l.prevToken) ||
		tok2.tok == '=' || tok2.tok == COLON_EQUALS || tok2.tok == '['
	return l.classifyWord(a, lookup)
}

// classifyWord finishes a single-word token: resolve to a scalar/record T_DATUM
// (when lookup is allowed and we are in NORMAL mode), else an unreserved keyword,
// else a plain T_WORD. Ports the tail of plpgsql_yylex's single-word path and
// plpgsql_parse_word.
func (l *lexer) classifyWord(a auxToken, lookup bool) auxToken {
	if lookup && l.mode == lookupNormal {
		if item, _ := l.ns.lookup(l.ns.topItem(), false, a.str, "", ""); item != nil {
			// plpgsql_ns_lookup only ever returns VAR or REC items.
			a.tok = T_DATUM
			a.datum = l.datums[item.itemNo]
			a.datumNames = 1
			return a
		}
	}
	if !a.quoted {
		if tok, ok := unreservedKeywords[a.str]; ok {
			a.tok = tok
			return a
		}
	}
	a.tok = T_WORD
	return a
}

// classifyDblword finishes an A.B compound: a namespace hit yields a T_DATUM for
// a block-qualified scalar variable (label.var); anything else is a T_CWORD
// carrying the dotted name. Ports the scalar path of plpgsql_parse_dblword; PG
// also builds a RECFIELD here for rec.field, which we do not — records are not
// yet typed, so a rec.field reference falls through to T_CWORD. The lookup is
// suppressed in DECLARE mode.
func (l *lexer) classifyDblword(a auxToken, word2 string) auxToken {
	word1 := a.str
	a.str = word1 + "." + word2
	if l.mode != lookupDeclare {
		if item, _ := l.ns.lookup(l.ns.topItem(), false, word1, word2, ""); item != nil {
			// Only a scalar variable can match a two-part name here (a block label
			// qualifying a variable name).
			a.tok = T_DATUM
			a.datum = l.datums[item.itemNo]
			a.datumNames = 2
			return a
		}
	}
	a.tok = T_CWORD
	return a
}

// classifyTripword finishes an A.B.C compound. In PG the only three-part name that
// resolves is a record reference (rec.field.subfield or label.rec.field); since we
// do not yet type record variables, a three-part name never resolves and is always
// a T_CWORD carrying the dotted name. Ports plpgsql_parse_tripword's non-record
// (fall-through) result.
func (l *lexer) classifyTripword(a auxToken, word2, word3 string) auxToken {
	a.str = a.str + "." + word2 + "." + word3
	a.tok = T_CWORD
	return a
}

// endsCompound reports whether an identifier token after a dot ends the compound
// name rather than continuing it. This mirrors PG's pl_scanner.c: its core scanner
// is configured with only the reserved PL/pgSQL keywords, so a reserved keyword
// comes back as a keyword token (not IDENT) and stops the name, while any other
// word — a plain identifier, a SQL keyword, or an unreserved PL/pgSQL keyword —
// comes back as IDENT and continues it (e.g. `rec.table`, `a.value`). A quoted
// identifier is never a keyword and always continues.
func endsCompound(tok auxToken) bool {
	if tok.quoted {
		return false
	}
	_, isReserved := reservedKeywords[tok.str]
	return isReserved
}

// translateToken maps an SQL-lexer token to its PL/pgSQL equivalent. Identifier
// words and SQL keywords return the IDENT sentinel so Lex reclassifies them;
// the operators PL/pgSQL names specially (<<, >>, #) are remapped; everything
// else (named operators, literals, single-char ASCII tokens) passes through.
func translateToken(tk *parser.Token) int {
	switch tk.Type {
	case parser.IDENT, parser.UIDENT:
		return IDENT
	case parser.Op:
		switch tk.Value.Str {
		case "<<":
			return LESS_LESS
		case ">>":
			return GREATER_GREATER
		case "#":
			return '#'
		}
		return Op
	case parser.SCONST, parser.USCONST:
		return SCONST
	case parser.BCONST:
		return BCONST
	case parser.XCONST:
		return XCONST
	case parser.FCONST:
		return FCONST
	case parser.ICONST:
		return ICONST
	case parser.PARAM:
		return PARAM
	case parser.TYPECAST:
		return TYPECAST
	case parser.DOT_DOT:
		return DOT_DOT
	case parser.COLON_EQUALS:
		return COLON_EQUALS
	case parser.EQUALS_GREATER:
		return EQUALS_GREATER
	case parser.LESS_EQUALS:
		return LESS_EQUALS
	case parser.GREATER_EQUALS:
		return GREATER_EQUALS
	case parser.NOT_EQUALS:
		return NOT_EQUALS
	}

	// SQL keyword: reclassify against the PL/pgSQL tables by text. Like PG's
	// plpgsql scanner, these come back as IDENT and are matched against the
	// PL/pgSQL keyword tables in reclassifyWord.
	if tk.Value.Keyword != "" {
		return IDENT
	}
	// Single-character ASCII tokens share code points with the grammar.
	if tk.Type > 0 && tk.Type < 128 {
		return tk.Type
	}
	// Anything else (rare at the PL/pgSQL level) passes through unchanged.
	return tk.Type
}

// Error implements the goyacc lexer interface. Records only the first error.
func (l *lexer) Error(s string) {
	if l.err == nil {
		l.err = fmt.Errorf("plpgsql parse error: %s", s)
	}
}

// SetResult satisfies the plpgsqlResultSetter interface used by the grammar's
// start-rule action to publish the parsed function.
func (l *lexer) SetResult(fn *plpgsqlast.PLpgSQL_function) {
	l.result = fn
}
