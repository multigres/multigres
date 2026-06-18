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
	text, _, err := l.scanFragment(';')
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
