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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// scannedToken is one token produced by the PL/pgSQL lexer.
type scannedToken struct {
	tok int
	str string
}

// scanAll drives the lexer to EOF and returns the token stream.
func scanAll(input string) []scannedToken {
	l := newLexer(input)
	var out []scannedToken
	for {
		var lval plpgsqlSymType
		t := l.Lex(&lval)
		if t == 0 { // EOF
			break
		}
		out = append(out, scannedToken{tok: t, str: lval.str})
	}
	return out
}

// codes extracts just the token codes for terse comparison.
func codes(toks []scannedToken) []int {
	out := make([]int, len(toks))
	for i, tk := range toks {
		out[i] = tk.tok
	}
	return out
}

func TestLexBeginRaiseEnd(t *testing.T) {
	got := scanAll("BEGIN RAISE NOTICE 'hi'; END")
	assert.Equal(t,
		[]int{K_BEGIN, K_RAISE, K_NOTICE, SCONST, ';', K_END},
		codes(got),
	)
	// The string literal's value is carried through.
	require.Len(t, got, 6)
	assert.Equal(t, "hi", got[3].str)
}

func TestLexReservedVsUnreserved(t *testing.T) {
	// Reserved keywords.
	assert.Equal(t, []int{K_IF, K_LOOP, K_WHILE}, codes(scanAll("IF LOOP WHILE")))
	// Unreserved keywords.
	assert.Equal(t, []int{K_RAISE, K_PERFORM, K_EXCEPTION}, codes(scanAll("RAISE PERFORM EXCEPTION")))
}

func TestLexAssignment(t *testing.T) {
	got := scanAll("x := 1")
	assert.Equal(t, []int{T_WORD, COLON_EQUALS, ICONST}, codes(got))
	assert.Equal(t, "x", got[0].str)
}

func TestLexCompoundWord(t *testing.T) {
	// Three-part name collapses to a single T_CWORD spanning all parts.
	got := scanAll("a.b.c")
	require.Equal(t, []int{T_CWORD}, codes(got))
	assert.Equal(t, "a.b.c", got[0].str)

	// Two-part name.
	got = scanAll("a.b")
	require.Equal(t, []int{T_CWORD}, codes(got))
	assert.Equal(t, "a.b", got[0].str)

	// Bare word stays T_WORD.
	assert.Equal(t, []int{T_WORD}, codes(scanAll("a")))
}

func TestLexQuotedIdentifierNotKeyword(t *testing.T) {
	// A delimited identifier is never a keyword and preserves case.
	got := scanAll(`"End"`)
	require.Equal(t, []int{T_WORD}, codes(got))
	assert.Equal(t, "End", got[0].str)
}

// A U&"..." unicode-delimited identifier is also quoted (its text starts with
// "U", not '"'), so it must not be reclassified as a keyword either.
func TestLexUnicodeQuotedIdentifierNotKeyword(t *testing.T) {
	got := scanAll(`U&"end"`)
	require.Equal(t, []int{T_WORD}, codes(got))
	assert.Equal(t, "end", got[0].str)
}

func TestLexBlockLabelOperators(t *testing.T) {
	assert.Equal(t, []int{LESS_LESS, GREATER_GREATER}, codes(scanAll("<< >>")))
}

// A keyword after the dot is not a valid compound continuation: the dot and
// the keyword must be pushed back and re-emitted (the dot as itself, the
// keyword reclassified), not swallowed into a T_CWORD.
func TestLexDotPushback(t *testing.T) {
	got := scanAll("a.from")
	assert.Equal(t, []int{T_WORD, '.', K_FROM}, codes(got))
}
