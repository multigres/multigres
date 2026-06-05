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

/*
 * Replication-protocol Lexer - Tokenization Tests
 *
 * Coverage:
 *   - Keyword recognition (case-insensitive, all 17 in-scope keywords).
 *   - Unquoted identifier downcasing.
 *   - Double-quoted identifiers, including embedded "" escapes.
 *   - Single-quoted string literals, including embedded '' escapes.
 *   - Decimal UCONST (uint32) and %X/%X RECPTR (uint64).
 *   - Single-character punctuation tokens.
 *   - Whitespace skipping and EOF.
 *   - Error reporting for unterminated literals.
 *
 * Style mirrors go/common/parser/lexer_basic_test.go: table-driven cases
 * with named t.Run subtests and testify assertions.
 */

package replparser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// lex1 lexes a single token from input and returns the type plus the
// resulting symbol value (or nil for keyword/punct tokens that carry no
// payload). Helper to keep test bodies short.
func lex1(t *testing.T, input string) (int, any) {
	t.Helper()
	l := newReplLexer(input)
	var lval replYySymType
	tok := l.Lex(&lval)
	require.NoError(t, l.Err(), "unexpected lexer error on %q", input)
	return tok, lval.union
}

// TestReplLexer_Keywords covers each in-scope keyword and verifies
// case-insensitive matching (repl_scanner.l matches via flex's case-insensitive
// keyword rules; in our port `strings.ToUpper` over the matched word).
func TestReplLexer_Keywords(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want int
	}{
		{"IDENTIFY_SYSTEM upper", "IDENTIFY_SYSTEM", K_IDENTIFY_SYSTEM},
		{"IDENTIFY_SYSTEM lower", "identify_system", K_IDENTIFY_SYSTEM},
		{"IDENTIFY_SYSTEM mixed", "Identify_System", K_IDENTIFY_SYSTEM},
		{"CREATE_REPLICATION_SLOT", "CREATE_REPLICATION_SLOT", K_CREATE_REPLICATION_SLOT},
		{"DROP_REPLICATION_SLOT", "DROP_REPLICATION_SLOT", K_DROP_REPLICATION_SLOT},
		{"ALTER_REPLICATION_SLOT", "ALTER_REPLICATION_SLOT", K_ALTER_REPLICATION_SLOT},
		{"START_REPLICATION", "START_REPLICATION", K_START_REPLICATION},
		{"READ_REPLICATION_SLOT", "READ_REPLICATION_SLOT", K_READ_REPLICATION_SLOT},
		{"SHOW", "SHOW", K_SHOW},
		{"SLOT", "SLOT", K_SLOT},
		{"LOGICAL", "LOGICAL", K_LOGICAL},
		{"TEMPORARY", "TEMPORARY", K_TEMPORARY},
		{"RESERVE_WAL", "RESERVE_WAL", K_RESERVE_WAL},
		{"TWO_PHASE", "TWO_PHASE", K_TWO_PHASE},
		{"EXPORT_SNAPSHOT", "EXPORT_SNAPSHOT", K_EXPORT_SNAPSHOT},
		{"NOEXPORT_SNAPSHOT", "NOEXPORT_SNAPSHOT", K_NOEXPORT_SNAPSHOT},
		{"USE_SNAPSHOT", "USE_SNAPSHOT", K_USE_SNAPSHOT},
		{"WAIT", "WAIT", K_WAIT},
		{"TIMELINE", "TIMELINE", K_TIMELINE},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := lex1(t, tt.in)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestReplLexer_Idents covers unquoted, double-quoted, and escape forms.
// repl_scanner.l:100-103,198-203.
func TestReplLexer_Idents(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "unquoted downcased",
			in:   "MySlot",
			want: "myslot", // downcase_truncate_identifier
		},
		{
			name: "unquoted lowercase preserved",
			in:   "my_slot",
			want: "my_slot",
		},
		{
			name: "double-quoted preserves case and spaces",
			in:   `"Quoted Ident"`,
			want: "Quoted Ident",
		},
		{
			name: `double-quoted with "" escape`,
			in:   `"with""quote"`,
			want: `with"quote`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, val := lex1(t, tt.in)
			assert.Equal(t, IDENT, got)
			assert.Equal(t, tt.want, val)
		})
	}
}

// TestReplLexer_StringLiteral exercises single-quoted strings and the
// ” escape. Ported from repl_scanner.l:158-176.
func TestReplLexer_StringLiteral(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"simple", `'hello'`, "hello"},
		{"empty", `''`, ""},
		{"escaped quote", `'with''quote'`, "with'quote"},
		{"multi-line", "'line1\nline2'", "line1\nline2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, val := lex1(t, tt.in)
			assert.Equal(t, SCONST, got)
			assert.Equal(t, tt.want, val)
		})
	}
}

// TestReplLexer_UCONST covers decimal numeric tokens.
// Ported from repl_scanner.l:144-147.
func TestReplLexer_UCONST(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want uint32
	}{
		{"zero", "0", 0},
		{"small", "42", 42},
		{"max uint32", "4294967295", 4294967295},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, val := lex1(t, tt.in)
			assert.Equal(t, UCONST, got)
			assert.Equal(t, tt.want, val)
		})
	}
}

// TestReplLexer_RECPTR covers the %X/%X LSN form, including the tricky
// case where the high half is a pure hex-letter run that would otherwise
// look like an identifier. Ported from repl_scanner.l:149-156.
func TestReplLexer_RECPTR(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want uint64
	}{
		{"zero/zero", "0/0", 0},
		{"low-only", "0/16B3748", 0x16B3748},
		{"hex-letter high half", "FF/1", 0xFF00000001},
		{"both halves nonzero", "DEADBEEF/CAFEBABE", 0xDEADBEEFCAFEBABE},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, val := lex1(t, tt.in)
			require.Equal(t, RECPTR, got)
			// recptr is stored typed; convert for comparison.
			lval := replYySymType{union: val}
			assert.Equal(t, tt.want, uint64(lval.recptrUnion()))
		})
	}
}

// TestReplLexer_Punct verifies the single-char return path (default branch
// in Lex: returns int(c) for unrecognized characters).
func TestReplLexer_Punct(t *testing.T) {
	l := newReplLexer(`( , ) ;`)
	var lval replYySymType
	for _, want := range []int{'(', ',', ')', ';'} {
		got := l.Lex(&lval)
		require.NoError(t, l.Err())
		assert.Equal(t, want, got)
	}
}

// TestReplLexer_Whitespace checks that all PG whitespace chars are skipped
// (repl_scanner.l:76 space class: [ \t\n\r\f\v]).
func TestReplLexer_Whitespace(t *testing.T) {
	tests := []struct {
		name string
		in   string
	}{
		{"leading spaces", "   IDENTIFY_SYSTEM"},
		{"leading tabs", "\t\tIDENTIFY_SYSTEM"},
		{"mixed leading and trailing", "  \t\nIDENTIFY_SYSTEM\r\n"},
		{"vertical tab and form feed", "\v\fIDENTIFY_SYSTEM"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := lex1(t, tt.in)
			assert.Equal(t, K_IDENTIFY_SYSTEM, got)
		})
	}
}

// TestReplLexer_EOF verifies the empty-input and post-input cases both
// return 0 (the goyacc EOF sentinel).
func TestReplLexer_EOF(t *testing.T) {
	t.Run("empty input", func(t *testing.T) {
		got, _ := lex1(t, "")
		assert.Equal(t, 0, got)
	})

	t.Run("whitespace-only input", func(t *testing.T) {
		got, _ := lex1(t, "   \t\n")
		assert.Equal(t, 0, got)
	})

	t.Run("after-last-token", func(t *testing.T) {
		l := newReplLexer("SHOW")
		var lval replYySymType
		require.Equal(t, K_SHOW, l.Lex(&lval))
		assert.Equal(t, 0, l.Lex(&lval))
		assert.NoError(t, l.Err())
	})
}

// TestReplLexer_Errors verifies that malformed inputs surface via Err()
// and the lexer halts (returns 0 once the error is set).
func TestReplLexer_Errors(t *testing.T) {
	tests := []struct {
		name      string
		in        string
		wantMatch string // substring expected in the error message
	}{
		{"unterminated single quote", `'unterminated`, "unterminated quoted string"},
		{"unterminated double quote", `"unterminated`, "unterminated quoted identifier"},
		{"recptr missing low half", "0/", "invalid streaming start location"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := newReplLexer(tt.in)
			var lval replYySymType
			// Drain until EOF or an error fires.
			for {
				tok := l.Lex(&lval)
				if tok == 0 || l.Err() != nil {
					break
				}
			}
			require.Error(t, l.Err(), "expected an error for %q", tt.in)
			assert.Contains(t, l.Err().Error(), tt.wantMatch)
		})
	}
}

// TestReplLexer_TokenStream walks a realistic command and confirms each
// token in sequence. This is the integration-style coverage equivalent to
// lexer_basic_test.go:TestBasicLexing.
func TestReplLexer_TokenStream(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []int
	}{
		{
			name:  "IDENTIFY_SYSTEM",
			input: "IDENTIFY_SYSTEM",
			want:  []int{K_IDENTIFY_SYSTEM, 0},
		},
		{
			name:  "DROP_REPLICATION_SLOT s1 WAIT",
			input: "DROP_REPLICATION_SLOT s1 WAIT",
			want:  []int{K_DROP_REPLICATION_SLOT, IDENT, K_WAIT, 0},
		},
		{
			name:  "ALTER_REPLICATION_SLOT s1 (failover 'true')",
			input: `ALTER_REPLICATION_SLOT s1 (failover 'true')`,
			want:  []int{K_ALTER_REPLICATION_SLOT, IDENT, '(', IDENT, SCONST, ')', 0},
		},
		{
			name:  "START_REPLICATION SLOT s1 LOGICAL 0/16B3748",
			input: `START_REPLICATION SLOT s1 LOGICAL 0/16B3748`,
			want:  []int{K_START_REPLICATION, K_SLOT, IDENT, K_LOGICAL, RECPTR, 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := newReplLexer(tt.input)
			var lval replYySymType
			got := make([]int, 0, len(tt.want))
			for {
				tok := l.Lex(&lval)
				got = append(got, tok)
				if tok == 0 {
					break
				}
			}
			require.NoError(t, l.Err())
			assert.Equal(t, tt.want, got)
		})
	}
}
