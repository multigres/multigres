/*
 * PostgreSQL Parser Lexer - Compatibility Tests
 *
 * This file validates that our token constants match PostgreSQL's
 * expected values for compatibility.
 */

package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestTokenNumbering validates that our token constants are generated correctly by goyacc
// NOTE: After refactoring, we no longer use hardcoded PostgreSQL values (258, etc.)
// but use goyacc-generated constants which will be different values
func TestTokenNumbering(t *testing.T) {
	// Test that all tokens have unique, non-zero values
	tokens := []struct {
		token int
		name  string
	}{
		{IDENT, "IDENT"},
		{UIDENT, "UIDENT"},
		{FCONST, "FCONST"},
		{SCONST, "SCONST"},
		{USCONST, "USCONST"},
		{BCONST, "BCONST"},
		{XCONST, "XCONST"},
		{Op, "Op"},
		{ICONST, "ICONST"},
		{PARAM, "PARAM"},
		{TYPECAST, "TYPECAST"},
		{DOT_DOT, "DOT_DOT"},
		{COLON_EQUALS, "COLON_EQUALS"},
		{EQUALS_GREATER, "EQUALS_GREATER"},
		{LESS_EQUALS, "LESS_EQUALS"},
		{GREATER_EQUALS, "GREATER_EQUALS"},
		{NOT_EQUALS, "NOT_EQUALS"},
	}

	// Validate all tokens are non-zero and unique
	seen := make(map[int]bool)
	for _, tok := range tokens {
		assert.NotEqual(t, 0, tok.token, "Token %s should be non-zero", tok.name)
		assert.False(t, seen[tok.token], "Token %s should have unique value", tok.name)
		seen[tok.token] = true
	}

	// Test that keyword tokens also have unique values
	keywordTokens := []struct {
		token int
		name  string
	}{
		{ALL, "ALL"},
		{ALTER, "ALTER"},
		{AS, "AS"},
		{CREATE, "CREATE"},
		{DROP, "DROP"},
		{EXISTS, "EXISTS"},
		{NOT, "NOT"},
		{OR, "OR"},
		{WITH, "WITH"},
	}

	for _, tok := range keywordTokens {
		assert.NotEqual(t, 0, tok.token, "Keyword token %s should be non-zero", tok.name)
		assert.False(t, seen[tok.token], "Keyword token %s should have unique value", tok.name)
		seen[tok.token] = true
	}
}

// TestTokenValueUnion validates that our TokenValue struct matches PostgreSQL's core_YYSTYPE
// Reference: PostgreSQL src/include/parser/scanner.h lines 29-34
func TestTokenValueUnion(t *testing.T) {
	// Test integer value
	token := NewIntToken(42, 0, "42")
	assert.Equal(t, 42, token.Value.Ival, "Integer value mismatch")

	// Test string value
	token = NewStringToken(SCONST, "test", 0, "'test'")
	assert.Equal(t, "test", token.Value.Str, "String value mismatch")

	// Test keyword value
	token = NewKeywordToken(SELECT, "SELECT", 0, "SELECT")
	assert.Equal(t, "SELECT", token.Value.Keyword, "Keyword value mismatch")
}

// TestPostgreSQLStateCompatibility validates our lexer states match PostgreSQL's exclusive states
// Reference: PostgreSQL src/backend/parser/scan.l lines 192-202
func TestPostgreSQLStateCompatibility(t *testing.T) {
	expectedStates := []struct {
		state LexerState
		name  string
	}{
		{StateInitial, "INITIAL (default)"},
		{StateXB, "%x xb (bit strings)"},
		{StateXC, "%x xc (comments)"},
		{StateXD, "%x xd (delimited identifiers)"},
		{StateXH, "%x xh (hex strings)"},
		{StateXQ, "%x xq (quoted strings)"},
		{StateXQS, "%x xqs (quote stop)"},
		{StateXE, "%x xe (extended strings)"},
		{StateXDolQ, "%x xdolq (dollar quoted)"},
		{StateXUI, "%x xui (unicode identifier)"},
		{StateXUS, "%x xus (unicode string)"},
		{StateXEU, "%x xeu (extended unicode)"},
	}

	// Validate we have all the states PostgreSQL defines
	assert.Equal(t, 12, len(expectedStates), "PostgreSQL defines 12 exclusive states")

	// Test state values are sequential
	for i, expected := range expectedStates {
		assert.Equal(t, i, int(expected.state), "State %s value mismatch", expected.name)
	}
}

// TestBackslashQuoteModeCompatibility validates our backslash quote modes match PostgreSQL
// Reference: PostgreSQL src/backend/parser/scan.l line 68
func TestBackslashQuoteModeCompatibility(t *testing.T) {
	// PostgreSQL defines these values in utils/guc.h
	assert.Equal(t, BackslashQuoteMode(0), BackslashQuoteOff, "BackslashQuoteOff should be 0")
	assert.Equal(t, BackslashQuoteMode(1), BackslashQuoteOn, "BackslashQuoteOn should be 1")
	assert.Equal(t, BackslashQuoteMode(2), BackslashQuoteSafeEncoding, "BackslashQuoteSafeEncoding should be 2")
}

// TestThreadSafetyDesign validates that our design eliminates global state
func TestThreadSafetyDesign(t *testing.T) {
	// Create multiple contexts to ensure no shared state
	ctx1 := NewParseContext("input1", nil)
	ctx2 := NewParseContext("input2", nil)

	// Modify state in one context
	ctx1.SetState(StateXQ)
	ctx1.SetXCDepth(5)
	ctx1.SetDolQStart("tag")

	// Verify other context is unaffected
	assert.Equal(t, StateInitial, ctx2.GetState(), "ctx2 state should be StateInitial")
	assert.Equal(t, 0, ctx2.XCDepth(), "ctx2 XCDepth should be 0")
	assert.Equal(t, "", ctx2.DolQStart(), "ctx2 DolQStart should be empty")
}
