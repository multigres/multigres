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
// Variable resolution (T_DATUM) and the IdentifierLookup state machine are not
// implemented yet (parser-setup-hooks chunk), so identifier words become
// T_WORD/T_CWORD or a keyword, never T_DATUM.
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
	tok       int    // PL/pgSQL token code, or IDENT for a word awaiting reclassification
	str       string // semantic string (lowercased for unquoted words/keywords)
	ival      int    // semantic int (ICONST, PARAM)
	pos       int    // byte offset of the token's start in the source
	quoted    bool   // a delimited ("...") identifier — never a keyword
	isKeyword bool   // the SQL lexer classified this word as a SQL keyword
}

func newLexer(input string) *lexer {
	return &lexer{input: input, core: parser.NewLexer(input)}
}

// Lex implements the goyacc lexer interface. It scans one PL/pgSQL token,
// reclassifying identifier words, and publishes its semantic value.
func (l *lexer) Lex(lval *plpgsqlSymType) int {
	a := l.internalLex()
	tok, str := a.tok, a.str
	if tok == IDENT {
		tok, str = l.reclassifyWord(a)
	}
	// Cache the final token so a fragment-scanning action can push the parser's
	// pending lookahead back to us (see beginScan). pos stays the first token's
	// offset, which is what raw-text capture needs even for compound names.
	l.lastToken = auxToken{tok: tok, str: str, ival: a.ival, pos: a.pos, quoted: a.quoted}
	lval.str = str
	lval.ival = a.ival
	lval.location = a.pos
	return tok
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
	if tk == nil || tk.Type == parser.EOF || tk.Type == parser.INVALID {
		return auxToken{tok: 0} // 0 == EOF for goyacc
	}

	a := auxToken{
		str:    tk.Value.Str,
		ival:   tk.Value.Ival,
		pos:    tk.Position,
		quoted: isQuotedIdentifier(tk.Text),
	}
	a.tok, a.isKeyword = translateToken(tk)
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

// reclassifyWord turns an identifier-like token (a.tok == IDENT) into the
// PL/pgSQL token it should be: a reserved or unreserved keyword, a compound
// name (T_CWORD), or a plain word (T_WORD). It returns the token code and the
// semantic string to publish — the dotted name for a compound, otherwise the
// word unchanged.
func (l *lexer) reclassifyWord(a auxToken) (int, string) {
	// Reserved keywords win unconditionally and never start a compound name,
	// mirroring PG's core scanner returning them before any variable lookup.
	if !a.quoted {
		if tok, ok := reservedKeywords[a.str]; ok {
			return tok, a.str
		}
	}

	// Compound-name lookahead: A.B and A.B.C.
	if combined, parts := l.scanCompound(a.str); parts >= 2 {
		return T_CWORD, combined
	}

	// Single word: an unreserved keyword if it matches, else a plain word.
	if !a.quoted {
		if tok, ok := unreservedKeywords[a.str]; ok {
			return tok, a.str
		}
	}
	return T_WORD, a.str
}

// scanCompound looks past the first word for ".ident" sequences, returning the
// dotted name and how many parts it spans (1, 2, or 3). Continuation parts must
// be plain identifiers — a keyword after the dot ends the name, matching PG.
// Tokens that turn out not to be part of the name are pushed back.
func (l *lexer) scanCompound(firstStr string) (string, int) {
	combined := firstStr

	dot1 := l.internalLex()
	if dot1.tok != '.' {
		l.pushBack(dot1)
		return combined, 1
	}
	w1 := l.internalLex()
	if w1.tok != IDENT || w1.isKeyword {
		l.pushBack(w1)
		l.pushBack(dot1)
		return combined, 1
	}
	combined += "." + w1.str

	dot2 := l.internalLex()
	if dot2.tok != '.' {
		l.pushBack(dot2)
		return combined, 2
	}
	w2 := l.internalLex()
	if w2.tok != IDENT || w2.isKeyword {
		l.pushBack(w2)
		l.pushBack(dot2)
		return combined, 2
	}
	combined += "." + w2.str
	return combined, 3
}

// translateToken maps an SQL-lexer token to its PL/pgSQL equivalent. Identifier
// words and SQL keywords return the IDENT sentinel so Lex reclassifies them;
// the operators PL/pgSQL names specially (<<, >>, #) are remapped; everything
// else (named operators, literals, single-char ASCII tokens) passes through.
func translateToken(tk *parser.Token) (tok int, isKeyword bool) {
	switch tk.Type {
	case parser.IDENT, parser.UIDENT:
		return IDENT, false
	case parser.Op:
		switch tk.Value.Str {
		case "<<":
			return LESS_LESS, false
		case ">>":
			return GREATER_GREATER, false
		case "#":
			return '#', false
		}
		return Op, false
	case parser.SCONST, parser.USCONST:
		return SCONST, false
	case parser.BCONST:
		return BCONST, false
	case parser.XCONST:
		return XCONST, false
	case parser.FCONST:
		return FCONST, false
	case parser.ICONST:
		return ICONST, false
	case parser.PARAM:
		return PARAM, false
	case parser.TYPECAST:
		return TYPECAST, false
	case parser.DOT_DOT:
		return DOT_DOT, false
	case parser.COLON_EQUALS:
		return COLON_EQUALS, false
	case parser.EQUALS_GREATER:
		return EQUALS_GREATER, false
	case parser.LESS_EQUALS:
		return LESS_EQUALS, false
	case parser.GREATER_EQUALS:
		return GREATER_EQUALS, false
	case parser.NOT_EQUALS:
		return NOT_EQUALS, false
	}

	// SQL keyword: reclassify against the PL/pgSQL tables by text.
	if tk.Value.Keyword != "" {
		return IDENT, true
	}
	// Single-character ASCII tokens share code points with the grammar.
	if tk.Type > 0 && tk.Type < 128 {
		return tk.Type, false
	}
	// Anything else (rare at the PL/pgSQL level) passes through unchanged.
	return tk.Type, false
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
