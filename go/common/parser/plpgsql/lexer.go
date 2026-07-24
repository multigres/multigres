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
	"strconv"
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
// PG's variable resolution (T_DATUM) and its IdentifierLookup state machine are
// intentionally not ported — we analyze the body statically and never resolve —
// so identifier words become T_WORD/T_CWORD or a keyword, never T_DATUM.
type lexer struct {
	input     string        // original source, for slicing raw fragment text
	core      *parser.Lexer // the underlying SQL scanner we wrap
	result    *plpgsqlast.PLpgSQL_function
	err       error
	pushback  []auxToken
	lastToken auxToken // last token returned by Lex (the parser's current lookahead)
}

// auxToken is one scanned token plus the data Lex needs to publish it.
type auxToken struct {
	tok    int    // PL/pgSQL token code, or IDENT for a word awaiting reclassification
	str    string // semantic string (lowercased for unquoted words/keywords)
	ival   int    // semantic int (ICONST, PARAM)
	pos    int    // byte offset of the token's start in the source
	end    int    // byte offset just past the token's end (start of a compound's last part's end)
	quoted bool   // a delimited ("...") identifier — never a keyword
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

// reclassifyWord turns an identifier-like token (a.tok == IDENT) into the
// PL/pgSQL token it should be: a reserved or unreserved keyword, a compound
// name (T_CWORD), or a plain word (T_WORD). It returns the token code and the
// semantic string to publish — the dotted name for a compound, otherwise the
// word unchanged.
func (l *lexer) reclassifyWord(a auxToken) (int, string, int) {
	// Reserved keywords win unconditionally and never start a compound name,
	// mirroring PG's core scanner returning them before any variable lookup.
	if !a.quoted {
		if tok, ok := reservedKeywords[a.str]; ok {
			return tok, a.str, a.end
		}
	}

	// Compound-name lookahead: A.B and A.B.C.
	if combined, parts, end := l.scanCompound(a.str, a.end); parts >= 2 {
		return T_CWORD, combined, end
	}

	// Single word: an unreserved keyword if it matches, else a plain word.
	if !a.quoted {
		if tok, ok := unreservedKeywords[a.str]; ok {
			return tok, a.str, a.end
		}
	}
	return T_WORD, a.str, a.end
}

// reclassifyParam handles a PARAM token ($1). Like PG's plpgsql_yylex, which
// treats PARAM like IDENT: a param followed by ".field" becomes a single
// T_CWORD (e.g. `$1.field`), and a bare param is run through the word path — PG
// resolves it to the parameter's T_DATUM, or, unresolved, returns T_WORD. We
// never resolve, so a bare param becomes T_WORD (the grammar has no PARAM
// production, matching PG, where every param reaches the grammar as
// T_DATUM/T_WORD/T_CWORD). The core scanner gives us the number (ival) but no
// text, so the name is reconstructed as "$" + ival (PG uses yytext directly).
func (l *lexer) reclassifyParam(a auxToken) (int, string, int) {
	name := "$" + strconv.Itoa(a.ival)
	if combined, parts, end := l.scanCompound(name, a.end); parts >= 2 {
		return T_CWORD, combined, end
	}
	return T_WORD, name, a.end
}

// scanCompound looks past the first word for ".ident" sequences, returning the
// dotted name, how many parts it spans (1, 2, or 3), and the byte offset just
// past the last part (so fragment capture can end at the real token, not any
// trailing comment). firstEnd is the end of the first word. A continuation must
// be an identifier that is not a reserved PL/pgSQL keyword; anything else after
// the dot ends the name. Tokens that turn out not to be part of the name are
// pushed back.
func (l *lexer) scanCompound(firstStr string, firstEnd int) (string, int, int) {
	combined := firstStr
	end := firstEnd

	dot1 := l.internalLex()
	if dot1.tok != '.' {
		l.pushBack(dot1)
		return combined, 1, end
	}
	w1 := l.internalLex()
	if w1.tok != IDENT || endsCompound(w1) {
		l.pushBack(w1)
		l.pushBack(dot1)
		return combined, 1, end
	}
	combined += "." + w1.str
	end = w1.end

	dot2 := l.internalLex()
	if dot2.tok != '.' {
		l.pushBack(dot2)
		return combined, 2, end
	}
	w2 := l.internalLex()
	if w2.tok != IDENT || endsCompound(w2) {
		l.pushBack(w2)
		l.pushBack(dot2)
		return combined, 2, end
	}
	combined += "." + w2.str
	end = w2.end
	return combined, 3, end
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
