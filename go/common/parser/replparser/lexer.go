// PostgreSQL Database Management System
// (also known as Postgres, formerly known as Postgres95)
//
//	Portions Copyright (c) 2026, Supabase, Inc
//
//	Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//
//	Portions Copyright (c) 1994, The Regents of the University of California
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written agreement
// is hereby granted, provided that the above copyright notice and this
// paragraph and the following two paragraphs appear in all copies.
//
// Hand-written lexer for the replication-protocol command grammar.
// Ported from postgres/src/backend/replication/repl_scanner.l (PG 17.6).

package replparser

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/multigres/multigres/go/common/parser/ast"
)

var (
	errUnterminatedString = errors.New("unterminated quoted string")
	errUnterminatedIdent  = errors.New("unterminated quoted identifier")
	errInvalidRecptr      = errors.New("invalid streaming start location")
)

// replKeywords - ported from repl_scanner.l:120-140 (physical-only keywords
// intentionally omitted).
var replKeywords = map[string]int{
	"IDENTIFY_SYSTEM":         K_IDENTIFY_SYSTEM,
	"READ_REPLICATION_SLOT":   K_READ_REPLICATION_SLOT,
	"SHOW":                    K_SHOW,
	"TIMELINE":                K_TIMELINE,
	"START_REPLICATION":       K_START_REPLICATION,
	"CREATE_REPLICATION_SLOT": K_CREATE_REPLICATION_SLOT,
	"DROP_REPLICATION_SLOT":   K_DROP_REPLICATION_SLOT,
	"ALTER_REPLICATION_SLOT":  K_ALTER_REPLICATION_SLOT,
	"LOGICAL":                 K_LOGICAL,
	"SLOT":                    K_SLOT,
	"RESERVE_WAL":             K_RESERVE_WAL,
	"TEMPORARY":               K_TEMPORARY,
	"TWO_PHASE":               K_TWO_PHASE,
	"EXPORT_SNAPSHOT":         K_EXPORT_SNAPSHOT,
	"NOEXPORT_SNAPSHOT":       K_NOEXPORT_SNAPSHOT,
	"USE_SNAPSHOT":            K_USE_SNAPSHOT,
	"WAIT":                    K_WAIT,
}

type replLexer struct {
	input  []byte
	pos    int
	err    error
	result ast.Stmt
}

func newReplLexer(input string) *replLexer {
	return &replLexer{input: []byte(input)}
}

// Err returns the first lexing/parsing error encountered, or nil.
func (l *replLexer) Err() error { return l.err }

// setResult is invoked from the grammar's top-level action.
func (l *replLexer) setResult(s ast.Stmt) { l.result = s }

// Lex implements the goyacc lexer interface.
// Mirrors repl_scanner.l rules at :120-208.
func (l *replLexer) Lex(lval *replYySymType) int {
	// Skip whitespace (repl_scanner.l:76,142).
	for l.pos < len(l.input) {
		c := l.input[l.pos]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v' {
			l.pos++
			continue
		}
		break
	}
	if l.pos >= len(l.input) {
		return 0
	}

	c := l.input[l.pos]
	switch {
	case c == '\'':
		return l.scanSingleQuoted(lval)
	case c == '"':
		return l.scanDoubleQuoted(lval)
	case c >= '0' && c <= '9':
		// Decimal UCONST or %X/%X RECPTR starting with a digit.
		return l.scanNumberOrRecptr(lval)
	case isIdentStart(c):
		// Identifier, keyword, or %X/%X RECPTR starting with a hex letter.
		return l.scanIdentOrRecptr(lval)
	default:
		l.pos++
		return int(c)
	}
}

// Error implements the goyacc lexer interface.
func (l *replLexer) Error(s string) {
	if l.err == nil {
		l.err = fmt.Errorf("syntax error in replication command: %s", s)
	}
}

// scanSingleQuoted scans a single-quoted string literal.
// Ported from repl_scanner.l:158-176 ('...' with ” as escape).
func (l *replLexer) scanSingleQuoted(lval *replYySymType) int {
	l.pos++ // consume opening '
	var b strings.Builder
	for l.pos < len(l.input) {
		c := l.input[l.pos]
		if c == '\'' {
			if l.pos+1 < len(l.input) && l.input[l.pos+1] == '\'' {
				b.WriteByte('\'')
				l.pos += 2
				continue
			}
			l.pos++
			lval.union = b.String()
			return SCONST
		}
		b.WriteByte(c)
		l.pos++
	}
	l.err = errUnterminatedString
	return 0
}

// scanDoubleQuoted scans a double-quoted identifier.
// Ported from repl_scanner.l:178-196 ("..." with "" as escape).
func (l *replLexer) scanDoubleQuoted(lval *replYySymType) int {
	l.pos++ // consume opening "
	var b strings.Builder
	for l.pos < len(l.input) {
		c := l.input[l.pos]
		if c == '"' {
			if l.pos+1 < len(l.input) && l.input[l.pos+1] == '"' {
				b.WriteByte('"')
				l.pos += 2
				continue
			}
			l.pos++
			lval.union = b.String()
			return IDENT
		}
		b.WriteByte(c)
		l.pos++
	}
	l.err = errUnterminatedIdent
	return 0
}

// scanNumberOrRecptr handles input starting with a decimal digit.
// Emits UCONST for a decimal run, or RECPTR if the run is followed by
// '/<hex>'. Ported from repl_scanner.l:144-156.
func (l *replLexer) scanNumberOrRecptr(lval *replYySymType) int {
	start := l.pos
	for l.pos < len(l.input) && isHexDigit(l.input[l.pos]) {
		l.pos++
	}
	first := string(l.input[start:l.pos])

	if l.pos < len(l.input) && l.input[l.pos] == '/' {
		return l.emitRecptr(lval, first)
	}

	// Not a RECPTR. PG's flex rule for UCONST matches only decimal
	// digits, so back up to the longest leading run of [0-9] and emit
	// that as UCONST. Anything after will be re-scanned as the next
	// token.
	decEnd := start
	for decEnd < l.pos && l.input[decEnd] >= '0' && l.input[decEnd] <= '9' {
		decEnd++
	}
	if decEnd == start {
		l.err = fmt.Errorf("invalid number: %q", first)
		return 0
	}
	l.pos = decEnd
	v, err := strconv.ParseUint(string(l.input[start:decEnd]), 10, 32)
	if err != nil {
		l.err = fmt.Errorf("invalid number: %w", err)
		return 0
	}
	lval.union = uint32(v)
	return UCONST
}

// scanIdentOrRecptr handles input starting with a letter or underscore.
// Most inputs become IDENT/keyword, but a run of hex-only characters
// followed by '/<hex>' is a RECPTR (repl_scanner.l flex's longest-match
// behavior picks the RECPTR rule over the identifier rule in that case).
func (l *replLexer) scanIdentOrRecptr(lval *replYySymType) int {
	start := l.pos
	for l.pos < len(l.input) && isIdentCont(l.input[l.pos]) {
		l.pos++
	}
	word := string(l.input[start:l.pos])

	if l.pos < len(l.input) && l.input[l.pos] == '/' && isAllHexDigits(word) {
		return l.emitRecptr(lval, word)
	}

	// Keyword lookup is case-insensitive against uppercase keys
	// (repl_scanner.l matches IDENTIFY_SYSTEM case-insensitively via flex).
	if tok, ok := replKeywords[strings.ToUpper(word)]; ok {
		return tok
	}

	// Unquoted identifiers are downcased
	// (repl_scanner.l:201 calls downcase_truncate_identifier).
	lval.union = strings.ToLower(word)
	return IDENT
}

// emitRecptr consumes the '/<hex>' tail after the caller has already
// scanned the leading hex run and verified the '/' follows.
func (l *replLexer) emitRecptr(lval *replYySymType, first string) int {
	l.pos++ // consume '/'
	secondStart := l.pos
	for l.pos < len(l.input) && isHexDigit(l.input[l.pos]) {
		l.pos++
	}
	second := string(l.input[secondStart:l.pos])
	if second == "" {
		l.err = errInvalidRecptr
		return 0
	}
	hi, err := strconv.ParseUint(first, 16, 32)
	if err != nil {
		l.err = fmt.Errorf("invalid streaming start location: %w", err)
		return 0
	}
	lo, err := strconv.ParseUint(second, 16, 32)
	if err != nil {
		l.err = fmt.Errorf("invalid streaming start location: %w", err)
		return 0
	}
	lval.union = ast.XLogRecPtr(hi<<32 | lo)
	return RECPTR
}

func isAllHexDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range []byte(s) {
		if !isHexDigit(c) {
			return false
		}
	}
	return true
}

// isHexDigit reports whether c is a hex digit [0-9A-Fa-f].
// Ported from repl_scanner.l:98.
func isHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') || (c >= 'a' && c <= 'f')
}

// isIdentStart reports whether c can start an identifier.
// Ported from repl_scanner.l:100.
func isIdentStart(c byte) bool {
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_' || c >= 0x80
}

// isIdentCont reports whether c can continue an identifier.
// Ported from repl_scanner.l:101.
func isIdentCont(c byte) bool {
	return isIdentStart(c) || (c >= '0' && c <= '9') || c == '$'
}
