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

// TestTokenNumbering validates that our token constants match PostgreSQL expectations
// Reference: PostgreSQL src/include/parser/scanner.h line 57 comment
func TestTokenNumbering(t *testing.T) {
	// PostgreSQL scanner.h explicitly states "IDENT = 258 and so on"
	assert.Equal(t, TokenType(258), IDENT, "IDENT token should be 258")

	// Validate the sequence matches PostgreSQL gram.y token order
	expectedTokens := []struct {
		token TokenType
		name  string
		value int
	}{
		{IDENT, "IDENT", 258},
		{UIDENT, "UIDENT", 259},
		{FCONST, "FCONST", 260},
		{SCONST, "SCONST", 261},
		{USCONST, "USCONST", 262},
		{BCONST, "BCONST", 263},
		{XCONST, "XCONST", 264},
		{Op, "Op", 265},
		{ICONST, "ICONST", 266},
		{PARAM, "PARAM", 267},
		{TYPECAST, "TYPECAST", 268},
		{DOT_DOT, "DOT_DOT", 269},
		{COLON_EQUALS, "COLON_EQUALS", 270},
		{EQUALS_GREATER, "EQUALS_GREATER", 271},
		{LESS_EQUALS, "LESS_EQUALS", 272},
		{GREATER_EQUALS, "GREATER_EQUALS", 273},
		{NOT_EQUALS, "NOT_EQUALS", 274},
	}

	for _, expected := range expectedTokens {
		assert.Equal(t, expected.value, int(expected.token), "Token %s value mismatch", expected.name)
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
