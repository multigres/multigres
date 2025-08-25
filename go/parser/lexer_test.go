/*
 * PostgreSQL Parser Lexer - Test Suite
 *
 * This file implements comprehensive tests for the PostgreSQL lexer,
 * validating token compatibility and thread safety.
 *
 * Test Categories:
 * - Token type validation
 * - Basic lexer functionality
 * - Thread safety validation
 * - PostgreSQL compatibility
 */

package parser

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test token type creation and basic properties
func TestTokenTypes(t *testing.T) {
	tests := []struct {
		name     string
		token    *Token
		expected TokenType
	}{
		{
			name:     "Integer token",
			token:    NewIntToken(42, 0, "42"),
			expected: ICONST,
		},
		{
			name:     "String token",
			token:    NewStringToken(SCONST, "hello", 0, "'hello'"),
			expected: SCONST,
		},
		{
			name:     "Keyword token",
			token:    NewKeywordToken(SELECT, "SELECT", 0, "SELECT"),
			expected: SELECT,
		},
		{
			name:     "Parameter token",
			token:    NewParamToken(1, 0, "$1"),
			expected: PARAM,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.token.Type, "Token type mismatch")
		})
	}
}

// Test token type classification methods
func TestTokenClassification(t *testing.T) {
	tests := []struct {
		name     string
		token    *Token
		isString bool
		isNum    bool
		isBit    bool
		isOp     bool
		isIdent  bool
	}{
		{
			name:     "String literal",
			token:    NewStringToken(SCONST, "test", 0, "'test'"),
			isString: true,
		},
		{
			name:  "Integer literal",
			token: NewIntToken(42, 0, "42"),
			isNum: true,
		},
		{
			name:  "Bit string",
			token: NewStringToken(BCONST, "101", 0, "B'101'"),
			isBit: true,
		},
		{
			name:  "Operator",
			token: NewToken(TYPECAST, 0, "::"),
			isOp:  true,
		},
		{
			name:    "Identifier",
			token:   NewStringToken(IDENT, "name", 0, "name"),
			isIdent: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.isString, tt.token.IsStringLiteral(), "IsStringLiteral() mismatch")
			assert.Equal(t, tt.isNum, tt.token.IsNumericLiteral(), "IsNumericLiteral() mismatch")
			assert.Equal(t, tt.isBit, tt.token.IsBitStringLiteral(), "IsBitStringLiteral() mismatch")
			assert.Equal(t, tt.isOp, tt.token.IsOperator(), "IsOperator() mismatch")
			assert.Equal(t, tt.isIdent, tt.token.IsIdentifier(), "IsIdentifier() mismatch")
		})
	}
}

// Test lexer context creation and basic functionality
func TestLexerContext(t *testing.T) {
	input := "SELECT * FROM table"
	ctx := NewLexerContext(input)

	// Test initial state
	assert.Equal(t, len(input), ctx.GetScanBufLen(), "Buffer length mismatch")
	assert.Equal(t, 1, ctx.LineNumber(), "Initial line number should be 1")
	assert.Equal(t, 1, ctx.ColumnNumber(), "Initial column number should be 1")
	assert.Equal(t, StateInitial, ctx.GetState(), "Initial state mismatch")

	// Test byte reading
	b, ok := ctx.CurrentByte()
	require.True(t, ok, "CurrentByte should succeed")
	assert.Equal(t, byte('S'), b, "Expected to peek 'S'")

	b, ok = ctx.NextByte()
	require.True(t, ok, "NextByte should succeed")
	assert.Equal(t, byte('S'), b, "Expected to read 'S'")

	// Test position tracking
	assert.Equal(t, 1, ctx.CurrentPosition(), "Position should be 1 after reading one byte")
}

// Test literal buffer functionality
func TestLiteralBuffer(t *testing.T) {
	ctx := NewLexerContext("test")

	// Test literal accumulation
	ctx.StartLiteral()
	ctx.AddLiteral("hello")
	ctx.AddLiteral(" ")
	ctx.AddLiteral("world")

	result := ctx.GetLiteral()
	expected := "hello world"
	assert.Equal(t, expected, result, "Literal value mismatch")

	// Test that buffer is reset
	assert.False(t, ctx.LiteralActive(), "Literal buffer should be inactive after GetLiteral()")
}

// Test basic lexer functionality
func TestBasicLexing(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []TokenType
	}{
		{
			name:     "Simple identifier",
			input:    "test",
			expected: []TokenType{IDENT, EOF},
		},
		{
			name:     "Simple number",
			input:    "42",
			expected: []TokenType{ICONST, EOF},
		},
		{
			name:     "String literal",
			input:    "'hello'",
			expected: []TokenType{SCONST, EOF},
		},
		{
			name:     "Keyword",
			input:    "SELECT",
			expected: []TokenType{SELECT, EOF}, // Keywords return their specific token type
		},
		{
			name:     "Parameter",
			input:    "$1",
			expected: []TokenType{PARAM, EOF},
		},
		{
			name:     "Multi-character operator",
			input:    "::",
			expected: []TokenType{TYPECAST, EOF},
		},
		{
			name:     "Single character operator",
			input:    "+",
			expected: []TokenType{TokenType('+'), EOF},
		},
		{
			name:     "Whitespace handling",
			input:    " \t\n test \t\n ",
			expected: []TokenType{IDENT, EOF},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)

			for i, expectedType := range tt.expected {
				token := lexer.NextToken()
				require.NotNil(t, token, "Unexpected error at position %d", i)
				assert.Equal(t, expectedType, token.Type, "Token %d type mismatch", i)
			}
		})
	}
}

// Test error handling
func TestErrorHandling(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		hasError bool
	}{
		{
			name:     "Unterminated string",
			input:    "'unterminated",
			hasError: true,
		},
		{
			name:     "Unterminated delimited identifier",
			input:    "\"unterminated",
			hasError: true,
		},
		{
			name:     "Valid input",
			input:    "'complete'",
			hasError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)

			// Consume all tokens - handle errors gracefully for error tests
			for {
				token := lexer.NextToken()
				if token.Type == EOF {
					break
				}
			}

			hasError := lexer.GetContext().HasErrors()
			assert.Equal(t, tt.hasError, hasError, "Error state mismatch")
			if hasError {
				for _, err := range lexer.GetContext().GetErrors() {
					t.Logf("Error: %s", err.Error())
				}
			}
		})
	}
}

// Test thread safety
func TestThreadSafety(t *testing.T) {
	input := "SELECT * FROM table WHERE id = $1 AND name = 'test'"

	const numGoroutines = 10
	const tokensPerGoroutine = 100

	var wg sync.WaitGroup
	results := make([][]TokenType, numGoroutines)

	// Launch multiple goroutines that each create their own lexer
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			var tokens []TokenType

			// Create many lexer instances to test thread safety
			for j := 0; j < tokensPerGoroutine; j++ {
				lexer := NewLexer(input)

				// Lex the first few tokens
				for k := 0; k < 3; k++ {
					token := lexer.NextToken()
					tokens = append(tokens, token.Type)
				}
			}

			results[goroutineID] = tokens
		}(i)
	}

	wg.Wait()

	// Verify all goroutines produced the same token sequence
	expected := results[0]
	for i := 1; i < numGoroutines; i++ {
		require.Equal(t, len(expected), len(results[i]), "Goroutine %d produced different number of tokens", i)

		for j := 0; j < len(expected); j++ {
			assert.Equal(t, expected[j], results[i][j], "Goroutine %d, token %d mismatch", i, j)
		}
	}
}

// Test position tracking
func TestPositionTracking(t *testing.T) {
	input := "line1\nline2\n  token"
	lexer := NewLexer(input)

	// First token should be "line1" at position 0
	token := lexer.NextToken()
	require.NotNil(t, token, "Unexpected error")
	assert.Equal(t, 0, token.Position, "First token should be at position 0")

	// Second token should be "line2"
	token = lexer.NextToken()
	require.NotNil(t, token, "Unexpected error")

	// Third token should be "token" with proper position
	token = lexer.NextToken()
	require.NotNil(t, token, "Unexpected error")

	// Verify the token text
	assert.Equal(t, "token", token.Value.Str, "Token text mismatch")
}

// Benchmark basic lexing performance
func BenchmarkBasicLexing(b *testing.B) {
	input := "SELECT * FROM users WHERE id = $1 AND name = 'test' AND active = true"

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		lexer := NewLexer(input)

		for {
			token := lexer.NextToken()
			if token.Type == EOF {
				break
			}
		}
	}
}

// Benchmark lexer creation overhead
func BenchmarkLexerCreation(b *testing.B) {
	input := "SELECT * FROM table"

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		lexer := NewLexer(input)
		_ = lexer // Prevent optimization
	}
}

// =============================================================================
// Enhanced Lexer Engine Tests
// =============================================================================

// Test enhanced identifier recognition with PostgreSQL rules
func TestEnhancedIdentifierRecognition(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		expectedTokens []struct {
			tokenType TokenType
			text      string
			isKeyword bool
			keyword   string
		}
	}{
		{
			name:  "Basic identifiers with underscores",
			input: "user_id table_name column_1",
			expectedTokens: []struct {
				tokenType TokenType
				text      string
				isKeyword bool
				keyword   string
			}{
				{IDENT, "user_id", false, ""},
				{IDENT, "table_name", false, ""},
				{IDENT, "column_1", false, ""},
			},
		},
		{
			name:  "Mixed case keywords",
			input: "SELECT Select select FROM From from",
			expectedTokens: []struct {
				tokenType TokenType
				text      string
				isKeyword bool
				keyword   string
			}{
				{SELECT, "SELECT", true, "select"},
				{SELECT, "Select", true, "select"},
				{SELECT, "select", true, "select"},
				{FROM, "FROM", true, "from"},
				{FROM, "From", true, "from"},
				{FROM, "from", true, "from"},
			},
		},
		{
			name:  "Identifiers with dollar signs",
			input: "col$1 table$name func$arg",
			expectedTokens: []struct {
				tokenType TokenType
				text      string
				isKeyword bool
				keyword   string
			}{
				{IDENT, "col$1", false, ""},
				{IDENT, "table$name", false, ""},
				{IDENT, "func$arg", false, ""},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)

			for i, expected := range tt.expectedTokens {
				token := lexer.NextToken()
				require.NotNil(t, token, "Token %d should scan without error", i)

				assert.Equal(t, expected.tokenType, token.Type, "Token %d type mismatch", i)
				assert.Equal(t, expected.text, token.Text, "Token %d text mismatch", i)

				if expected.isKeyword {
					assert.Equal(t, expected.keyword, token.Value.Keyword, "Token %d keyword mismatch", i)
				} else {
					assert.Equal(t, expected.text, token.Value.Str, "Token %d string value mismatch", i)
				}
			}

			// Verify EOF
			token := lexer.NextToken()
			require.NotNil(t, token)
			assert.Equal(t, EOF, token.Type, "Should reach EOF")
		})
	}
}

// Test comprehensive operator recognition
func TestComprehensiveOperatorRecognition(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		expectedTokens []struct {
			tokenType TokenType
			text      string
		}
	}{
		{
			name:  "Multi-character operators",
			input: ":: <= >= <> != => := ..",
			expectedTokens: []struct {
				tokenType TokenType
				text      string
			}{
				{TYPECAST, "::"},
				{LESS_EQUALS, "<="},
				{GREATER_EQUALS, ">="},
				{NOT_EQUALS, "<>"},
				{NOT_EQUALS, "!="},
				{EQUALS_GREATER, "=>"},
				{COLON_EQUALS, ":="},
				{DOT_DOT, ".."},
			},
		},
		{
			name:  "Single character operators",
			input: "( ) [ ] , ; + - * / % ^ < > =",
			expectedTokens: []struct {
				tokenType TokenType
				text      string
			}{
				{TokenType('('), "("},
				{TokenType(')'), ")"},
				{TokenType('['), "["},
				{TokenType(']'), "]"},
				{TokenType(','), ","},
				{TokenType(';'), ";"},
				{TokenType('+'), "+"},
				{TokenType('-'), "-"},
				{TokenType('*'), "*"},
				{TokenType('/'), "/"},
				{TokenType('%'), "%"},
				{TokenType('^'), "^"},
				{TokenType('<'), "<"},
				{TokenType('>'), ">"},
				{TokenType('='), "="},
			},
		},
		{
			name:  "Complex operator expressions",
			input: "price >= 100 AND name <> 'test' OR id := func()::int",
			expectedTokens: []struct {
				tokenType TokenType
				text      string
			}{
				{IDENT, "price"},
				{GREATER_EQUALS, ">="},
				{ICONST, "100"},
				{AND, "AND"}, // keyword returns parser constant
				{IDENT, "name"},
				{NOT_EQUALS, "<>"},
				{SCONST, "'test'"},
				{OR, "OR"}, // keyword returns parser constant
				{IDENT, "id"},
				{COLON_EQUALS, ":="},
				{IDENT, "func"},
				{TokenType('('), "("},
				{TokenType(')'), ")"},
				{TYPECAST, "::"},
				{IDENT, "int"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)

			for i, expected := range tt.expectedTokens {
				token := lexer.NextToken()
				require.NotNil(t, token, "Token %d should scan without error", i)

				assert.Equal(t, expected.tokenType, token.Type, "Token %d type mismatch: got %d, want %d", i, int(token.Type), int(expected.tokenType))
				assert.Equal(t, expected.text, token.Text, "Token %d text mismatch", i)
			}

			// Verify EOF
			token := lexer.NextToken()
			require.NotNil(t, token)
			assert.Equal(t, EOF, token.Type, "Should reach EOF")
		})
	}
}

// Test enhanced whitespace and comment handling
func TestEnhancedWhitespaceAndComments(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		expectedTokens []struct {
			tokenType TokenType
			text      string
			position  int
		}
	}{
		{
			name:  "Line comments",
			input: "SELECT -- this is a comment\nFROM table",
			expectedTokens: []struct {
				tokenType TokenType
				text      string
				position  int
			}{
				{SELECT, "SELECT", 0},
				{FROM, "FROM", 28},
				{TABLE, "table", 33},
			},
		},
		{
			name:  "Multiple whitespace types",
			input: "SELECT\t\n\r\f\v  id",
			expectedTokens: []struct {
				tokenType TokenType
				text      string
				position  int
			}{
				{SELECT, "SELECT", 0},
				{IDENT, "id", 13},
			},
		},
		{
			name:  "Comments at end of line",
			input: "id := 123 -- parameter assignment\nname := 'test'",
			expectedTokens: []struct {
				tokenType TokenType
				text      string
				position  int
			}{
				{IDENT, "id", 0},
				{COLON_EQUALS, ":=", 3},
				{ICONST, "123", 6},
				{IDENT, "name", 34},
				{COLON_EQUALS, ":=", 39},
				{SCONST, "'test'", 42},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)

			for i, expected := range tt.expectedTokens {
				token := lexer.NextToken()
				require.NotNil(t, token, "Token %d should scan without error", i)

				assert.Equal(t, expected.tokenType, token.Type, "Token %d type mismatch", i)
				assert.Equal(t, expected.text, token.Text, "Token %d text mismatch", i)
				assert.Equal(t, expected.position, token.Position, "Token %d position mismatch", i)
			}

			// Verify EOF
			token := lexer.NextToken()
			require.NotNil(t, token)
			assert.Equal(t, EOF, token.Type, "Should reach EOF")
		})
	}
}

// Test state machine foundation
func TestStateMachineFoundation(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		expectedState LexerState
	}{
		{
			name:          "Initial state",
			input:         "SELECT id FROM table",
			expectedState: StateInitial,
		},
		{
			name:          "Basic tokens maintain initial state",
			input:         "id := 123 + 456",
			expectedState: StateInitial,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)

			// Process all tokens
			for {
				token := lexer.NextToken()
				require.NotNil(t, token)
				if token.Type == EOF {
					break
				}
			}

			// Verify final state
			ctx := lexer.GetContext()
			assert.Equal(t, tt.expectedState, ctx.GetState(), "Final state should match expected")
		})
	}
}

// Test parameter recognition
func TestParameterRecognition(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []struct {
			tokenType TokenType
			text      string
			intValue  int
		}
	}{
		{
			name:  "Single parameters",
			input: "$1 $2 $10 $999",
			expected: []struct {
				tokenType TokenType
				text      string
				intValue  int
			}{
				{PARAM, "$1", 1},
				{PARAM, "$2", 2},
				{PARAM, "$10", 10},
				{PARAM, "$999", 999},
			},
		},
		{
			name:  "Parameters in SQL context",
			input: "WHERE id = $1 AND name = $2",
			expected: []struct {
				tokenType TokenType
				text      string
				intValue  int
			}{
				{WHERE, "WHERE", 0}, // WHERE is now a keyword in Phase 3C
				{IDENT, "id", 0},
				{TokenType('='), "=", 0},
				{PARAM, "$1", 1},
				{AND, "AND", 0}, // keyword returns parser constant
				{IDENT, "name", 0},
				{TokenType('='), "=", 0},
				{PARAM, "$2", 2},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)

			for i, expected := range tt.expected {
				token := lexer.NextToken()
				require.NotNil(t, token, "Token %d should scan without error", i)

				assert.Equal(t, expected.tokenType, token.Type, "Token %d type mismatch", i)
				assert.Equal(t, expected.text, token.Text, "Token %d text mismatch", i)

				if expected.tokenType == PARAM {
					assert.Equal(t, expected.intValue, token.Value.Ival, "Token %d parameter value mismatch", i)
				}
			}

			// Verify EOF
			token := lexer.NextToken()
			require.NotNil(t, token)
			assert.Equal(t, EOF, token.Type, "Should reach EOF")
		})
	}
}

// Test comprehensive SQL lexing
func TestComprehensiveSQLLexing(t *testing.T) {
	input := `
		-- Query with comments
		SELECT u.name, p.price
		FROM users u, products p
		WHERE u.id >= $1 AND p.price <> 'free'
		  AND u.created := NOW()::timestamp;
	`

	lexer := NewLexer(input)
	tokens := []struct {
		expectedType TokenType
		expectedText string
	}{
		{SELECT, "SELECT"}, // SELECT is now a keyword in Phase 3C
		{IDENT, "u"},
		{TokenType('.'), "."},
		{IDENT, "name"},
		{TokenType(','), ","},
		{IDENT, "p"},
		{TokenType('.'), "."},
		{IDENT, "price"},
		{FROM, "FROM"}, // FROM is now a keyword in Phase 3C
		{IDENT, "users"},
		{IDENT, "u"},
		{TokenType(','), ","},
		{IDENT, "products"},
		{IDENT, "p"},
		{WHERE, "WHERE"}, // WHERE is now a keyword in Phase 3C
		{IDENT, "u"},
		{TokenType('.'), "."},
		{IDENT, "id"},
		{GREATER_EQUALS, ">="},
		{PARAM, "$1"},
		{AND, "AND"}, // keyword returns parser constant
		{IDENT, "p"},
		{TokenType('.'), "."},
		{IDENT, "price"},
		{NOT_EQUALS, "<>"},
		{SCONST, "'free'"},
		{AND, "AND"}, // keyword returns parser constant
		{IDENT, "u"},
		{TokenType('.'), "."},
		{IDENT, "created"},
		{COLON_EQUALS, ":="},
		{IDENT, "NOW"},
		{TokenType('('), "("},
		{TokenType(')'), ")"},
		{TYPECAST, "::"},
		{IDENT, "timestamp"}, // keyword
		{TokenType(';'), ";"},
	}

	for i, expected := range tokens {
		token := lexer.NextToken()
		require.NotNil(t, token, "Token %d should scan without error", i)

		assert.Equal(t, expected.expectedType, token.Type,
			"Token %d type mismatch: got %d (%q), want %d",
			i, int(token.Type), token.Text, int(expected.expectedType))
		assert.Equal(t, expected.expectedText, token.Text, "Token %d text mismatch", i)
	}

	// Verify EOF
	token := lexer.NextToken()
	require.NotNil(t, token)
	assert.Equal(t, EOF, token.Type, "Should reach EOF")

	// Verify no errors
	ctx := lexer.GetContext()
	assert.Empty(t, ctx.GetErrors(), "Should have no lexer errors")
}

// =============================================================================
// Tests from advanced_test.go - Advanced lexer features
// =============================================================================

// TestParameterPlaceholders tests parameter placeholder recognition
func TestParameterPlaceholders(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []TokenType
		values   []interface{} // Can be string or int for params
	}{
		{
			name:     "simple parameter",
			input:    "$1",
			expected: []TokenType{PARAM, EOF},
			values:   []interface{}{1, ""},
		},
		{
			name:     "multiple parameters",
			input:    "$1 $2 $10 $999",
			expected: []TokenType{PARAM, PARAM, PARAM, PARAM, EOF},
			values:   []interface{}{1, 2, 10, 999, ""},
		},
		{
			name:     "parameter in expression",
			input:    "SELECT * WHERE id = $1",
			expected: []TokenType{SELECT, TokenType('*'), WHERE, IDENT, TokenType('='), PARAM, EOF},
			values:   []interface{}{"select", "*", "where", "id", "=", 1, ""},
		},
		{
			name:     "parameter with operators",
			input:    "$1 + $2",
			expected: []TokenType{PARAM, TokenType('+'), PARAM, EOF},
			values:   []interface{}{1, "+", 2, ""},
		},
		{
			name:     "adjacent parameters",
			input:    "$1$2",
			expected: []TokenType{PARAM, PARAM, EOF},
			values:   []interface{}{1, 2, ""},
		},
		{
			name:     "parameter vs dollar quote",
			input:    "$1 $$text$$",
			expected: []TokenType{PARAM, SCONST, EOF},
			values:   []interface{}{1, "text", ""},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lexer := NewLexer(test.input)
			tokens := scanAllTokens(t, lexer)

			require.Equal(t, len(test.expected), len(tokens))
			for i, token := range tokens {
				assert.Equal(t, test.expected[i], token.Type, "Token %d type mismatch", i)

				// Check value based on token type
				if token.Type == PARAM {
					assert.Equal(t, test.values[i], token.Value.Ival, "Token %d param value mismatch", i)
				} else {
					assert.Equal(t, test.values[i], token.Value.Str, "Token %d string value mismatch", i)
				}
			}
		})
	}
}

// TestParameterJunk tests parameter junk detection ($1abc pattern)
func TestParameterJunk(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    []TokenType
		values      []interface{}
		expectError bool
	}{
		{
			name:        "parameter with trailing identifier",
			input:       "$1abc",
			expected:    []TokenType{PARAM, EOF},
			values:      []interface{}{1, ""},
			expectError: true, // Should add error for trailing junk
		},
		{
			name:        "parameter with underscore",
			input:       "$1_test",
			expected:    []TokenType{PARAM, EOF},
			values:      []interface{}{1, ""},
			expectError: true,
		},
		{
			name:        "parameter with dollar",
			input:       "$1$name",
			expected:    []TokenType{PARAM, EOF},
			values:      []interface{}{1, ""},
			expectError: true,
		},
		{
			name:     "valid parameter followed by space and identifier",
			input:    "$1 abc",
			expected: []TokenType{PARAM, IDENT, EOF},
			values:   []interface{}{1, "abc", ""},
		},
		{
			name:     "parameter followed by operator",
			input:    "$1+",
			expected: []TokenType{PARAM, TokenType('+'), EOF},
			values:   []interface{}{1, "+", ""},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lexer := NewLexer(test.input)
			tokens := scanAllTokens(t, lexer)

			require.Equal(t, len(test.expected), len(tokens))
			for i, token := range tokens {
				assert.Equal(t, test.expected[i], token.Type, "Token %d type mismatch", i)

				if token.Type == PARAM {
					assert.Equal(t, test.values[i], token.Value.Ival, "Token %d param value mismatch", i)
				} else {
					assert.Equal(t, test.values[i], token.Value.Str, "Token %d string value mismatch", i)
				}
			}

			// Check for errors
			if test.expectError {
				assert.True(t, lexer.GetContext().HasErrors(), "Expected lexer errors")
				errors := lexer.GetContext().GetErrors()
				assert.Contains(t, errors[0].Message, "trailing junk after parameter")
			}
		})
	}
}

// TestTypeCastOperator tests the :: type cast operator
func TestTypeCastOperator(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []TokenType
		values   []string
	}{
		{
			name:     "simple type cast",
			input:    "value::int",
			expected: []TokenType{VALUE_P, TYPECAST, IDENT, EOF},
			values:   []string{"value", "::", "int", ""},
		},
		{
			name:     "type cast with spaces",
			input:    "value :: int",
			expected: []TokenType{VALUE_P, TYPECAST, IDENT, EOF},
			values:   []string{"value", "::", "int", ""},
		},
		{
			name:     "chained type casts",
			input:    "value::text::int",
			expected: []TokenType{VALUE_P, TYPECAST, IDENT, TYPECAST, IDENT, EOF},
			values:   []string{"value", "::", "text", "::", "int", ""},
		},
		{
			name:     "type cast with complex type",
			input:    "value::numeric(10,2)",
			expected: []TokenType{VALUE_P, TYPECAST, NUMERIC, TokenType('('), ICONST, TokenType(','), ICONST, TokenType(')'), EOF},
			values:   []string{"value", "::", "numeric", "(", "10", ",", "2", ")", ""},
		},
		{
			name:     "type cast vs colon equals",
			input:    "a::int b:=5",
			expected: []TokenType{IDENT, TYPECAST, IDENT, IDENT, COLON_EQUALS, ICONST, EOF},
			values:   []string{"a", "::", "int", "b", ":=", "5", ""},
		},
		{
			name:     "single colon not type cast",
			input:    "a:b",
			expected: []TokenType{IDENT, TokenType(':'), IDENT, EOF},
			values:   []string{"a", ":", "b", ""},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lexer := NewLexer(test.input)
			tokens := scanAllTokens(t, lexer)

			require.Equal(t, len(test.expected), len(tokens))
			for i, token := range tokens {
				assert.Equal(t, test.expected[i], token.Type, "Token %d type mismatch", i)
				assert.Equal(t, test.values[i], token.Value.Str, "Token %d value mismatch", i)
			}
		})
	}
}

// TestDollarTokenAmbiguity tests disambiguation between parameters and dollar quotes
func TestDollarTokenAmbiguity(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []TokenType
		values   []interface{}
	}{
		{
			name:     "parameter vs empty dollar quote",
			input:    "$1 $$",
			expected: []TokenType{PARAM, Op, Op, EOF},
			values:   []interface{}{1, "$", "$", ""},
		},
		{
			name:     "parameter vs dollar quote with tag",
			input:    "$1 $tag$",
			expected: []TokenType{PARAM, SCONST, EOF},
			values:   []interface{}{1, "", ""},
		},
		{
			name:     "dollar followed by non-digit",
			input:    "$a",
			expected: []TokenType{Op, IDENT, EOF},
			values:   []interface{}{"$", "a", ""},
		},
		{
			name:     "dollar at end",
			input:    "test$",
			expected: []TokenType{IDENT, EOF},
			values:   []interface{}{"test$", ""},
		},
		{
			name:     "dollar in identifier",
			input:    "te$t",
			expected: []TokenType{IDENT, EOF},
			values:   []interface{}{"te$t", ""},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lexer := NewLexer(test.input)
			tokens := scanAllTokens(t, lexer)

			require.Equal(t, len(test.expected), len(tokens))
			for i, token := range tokens {
				assert.Equal(t, test.expected[i], token.Type, "Token %d type mismatch", i)

				if token.Type == PARAM {
					assert.Equal(t, test.values[i], token.Value.Ival, "Token %d param value mismatch", i)
				} else {
					assert.Equal(t, test.values[i], token.Value.Str, "Token %d string value mismatch", i)
				}
			}
		})
	}
}

// TestComplexExpressions tests combinations of advanced features
func TestComplexExpressions(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []TokenType
		values   []interface{}
	}{
		{
			name:     "parameter with type cast",
			input:    "$1::int",
			expected: []TokenType{PARAM, TYPECAST, IDENT, EOF},
			values:   []interface{}{1, "::", "int", ""},
		},
		{
			name:     "delimited identifier with type cast",
			input:    `"column"::text`,
			expected: []TokenType{IDENT, TYPECAST, IDENT, EOF},
			values:   []interface{}{"column", "::", "text", ""},
		},
		{
			name:  "complex expression",
			input: `SELECT "Col1"::int + $1 FROM "Table" WHERE x = $2::text`,
			expected: []TokenType{
				SELECT, IDENT, TYPECAST, IDENT, TokenType('+'), PARAM,
				FROM, IDENT, WHERE, IDENT, TokenType('='), PARAM, TYPECAST, IDENT, EOF,
			},
			values: []interface{}{
				"select", "Col1", "::", "int", "+", 1,
				"from", "Table", "where", "x", "=", 2, "::", "text", "",
			},
		},
		{
			name:     "comment between type cast",
			input:    "value/*comment*/::int",
			expected: []TokenType{VALUE_P, TYPECAST, IDENT, EOF},
			values:   []interface{}{"value", "::", "int", ""},
		},
		{
			name:     "array subscript vs type cast",
			input:    "arr[1]::int[]",
			expected: []TokenType{IDENT, TokenType('['), ICONST, TokenType(']'), TYPECAST, IDENT, TokenType('['), TokenType(']'), EOF},
			values:   []interface{}{"arr", "[", "1", "]", "::", "int", "[", "]", ""},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lexer := NewLexer(test.input)
			tokens := scanAllTokens(t, lexer)

			require.Equal(t, len(test.expected), len(tokens))
			for i, token := range tokens {
				assert.Equal(t, test.expected[i], token.Type, "Token %d type mismatch", i)

				if token.Type == PARAM {
					assert.Equal(t, test.values[i], token.Value.Ival, "Token %d param value mismatch", i)
				} else {
					assert.Equal(t, test.values[i], token.Value.Str, "Token %d string value mismatch", i)
				}
			}
		})
	}
}

// TestOperatorPrecedence tests various operator combinations
func TestOperatorPrecedence(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []TokenType
		values   []string
	}{
		{
			name:     "colon vs type cast vs colon equals",
			input:    ": :: :=",
			expected: []TokenType{TokenType(':'), TYPECAST, COLON_EQUALS, EOF},
			values:   []string{":", "::", ":=", ""},
		},
		{
			name:     "dot vs dot dot",
			input:    ". .. ...",
			expected: []TokenType{TokenType('.'), DOT_DOT, TokenType('.'), TokenType('.'), TokenType('.'), EOF},
			values:   []string{".", "..", ".", ".", ".", ""},
		},
		{
			name:     "less than vs less equal vs not equal",
			input:    "< <= <> !=",
			expected: []TokenType{TokenType('<'), LESS_EQUALS, NOT_EQUALS, NOT_EQUALS, EOF},
			values:   []string{"<", "<=", "<>", "!=", ""},
		},
		{
			name:     "equals vs equals greater",
			input:    "= =>",
			expected: []TokenType{TokenType('='), EQUALS_GREATER, EOF},
			values:   []string{"=", "=>", ""},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lexer := NewLexer(test.input)
			tokens := scanAllTokens(t, lexer)

			require.Equal(t, len(test.expected), len(tokens))
			for i, token := range tokens {
				assert.Equal(t, test.expected[i], token.Type, "Token %d type mismatch", i)
				assert.Equal(t, test.values[i], token.Value.Str, "Token %d value mismatch", i)
			}
		})
	}
}

// Test character classification functions
func TestCharacterClassification(t *testing.T) {
	tests := []struct {
		name     string
		function func(byte) bool
		valid    []byte
		invalid  []byte
	}{
		{
			name:     "isIdentStart",
			function: IsIdentStart,
			valid:    []byte{'a', 'z', 'A', 'Z', '_', 0x80, 0xFF},
			invalid:  []byte{'0', '9', '$', '-', '+', ' ', '\t'},
		},
		{
			name:     "isIdentCont",
			function: IsIdentCont,
			valid:    []byte{'a', 'z', 'A', 'Z', '_', '0', '9', '$', 0x80, 0xFF},
			invalid:  []byte{'-', '+', ' ', '\t', '(', ')'},
		},
		{
			name:     "isDigit",
			function: IsDigit,
			valid:    []byte{'0', '1', '5', '9'},
			invalid:  []byte{'a', 'A', '_', '$', ' ', '\t'},
		},
		{
			name:     "isWhitespace",
			function: IsWhitespace,
			valid:    []byte{' ', '\t', '\n', '\r', '\f', '\v'},
			invalid:  []byte{'a', '0', '_', '$'},
		},
		{
			name:     "isSelfChar",
			function: IsSelfChar,
			valid:    []byte{',', '(', ')', '[', ']', '.', ';', ':', '+', '-', '*', '/', '%', '^', '<', '>', '='},
			invalid:  []byte{'a', '0', '_', '$', ' ', '\t', '~', '!', '@'},
		},
		{
			name:     "isOpChar",
			function: IsOpChar,
			valid:    []byte{'~', '!', '@', '#', '^', '&', '|', '`', '?', '+', '-', '*', '/', '%', '<', '>', '='},
			invalid:  []byte{'a', '0', '_', ' ', '\t', '(', ')', '[', ']'},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test valid characters
			for _, b := range tt.valid {
				assert.True(t, tt.function(b), "Character %c (0x%02x) should be valid for %s", b, b, tt.name)
			}

			// Test invalid characters
			for _, b := range tt.invalid {
				assert.False(t, tt.function(b), "Character %c (0x%02x) should be invalid for %s", b, b, tt.name)
			}
		})
	}
}

// Test hex, bit, and extended string literal recognition
func TestSpecialLiteralRecognition(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		expectedTokens []struct {
			tokenType TokenType
			text      string
		}
	}{
		{
			name:  "Bit string literals",
			input: "B'1010' b'0011' B'11111111'",
			expectedTokens: []struct {
				tokenType TokenType
				text      string
			}{
				{BCONST, "B'1010'"},
				{BCONST, "b'0011'"},
				{BCONST, "B'11111111'"},
			},
		},
		{
			name:  "Hex string literals",
			input: "X'deadbeef' x'CAFE' X'1A2B3C'",
			expectedTokens: []struct {
				tokenType TokenType
				text      string
			}{
				{XCONST, "X'deadbeef'"},
				{XCONST, "x'CAFE'"},
				{XCONST, "X'1A2B3C'"},
			},
		},
		{
			name:  "Extended string literals",
			input: "E'hello\\nworld' e'tab\\there' E'unicode\\u0041'",
			expectedTokens: []struct {
				tokenType TokenType
				text      string
			}{
				{SCONST, "E'hello\\nworld'"},
				{SCONST, "e'tab\\there'"},
				{SCONST, "E'unicode\\u0041'"},
			},
		},
		{
			name:  "National character literals",
			input: "N'national' n'string'",
			expectedTokens: []struct {
				tokenType TokenType
				text      string
			}{
				{SCONST, "N'national'"},
				{SCONST, "n'string'"},
			},
		},
		{
			name:  "Mixed with identifiers (fallback behavior)",
			input: "B'101' BETWEEN b X'FF' x E'test' e N'nat' n",
			expectedTokens: []struct {
				tokenType TokenType
				text      string
			}{
				{BCONST, "B'101'"},
				{IDENT, "BETWEEN"}, // keyword
				{IDENT, "b"},       // identifier (no quote following)
				{XCONST, "X'FF'"},
				{IDENT, "x"}, // identifier (no quote following)
				{SCONST, "E'test'"},
				{IDENT, "e"}, // identifier (no quote following)
				{SCONST, "N'nat'"},
				{IDENT, "n"}, // identifier (no quote following)
			},
		},
		{
			name:  "Unicode identifiers and strings",
			input: "U&\"unicode_id\" U&'unicode_str'",
			expectedTokens: []struct {
				tokenType TokenType
				text      string
			}{
				{UIDENT, "U&\"unicode_id\""},
				{SCONST, "U&'unicode_str'"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)

			for i, expected := range tt.expectedTokens {
				token := lexer.NextToken()
				require.NotNil(t, token, "Token %d should scan without error", i)

				assert.Equal(t, expected.tokenType, token.Type,
					"Token %d type mismatch: got %d (%q), want %d",
					i, int(token.Type), token.Text, int(expected.tokenType))
				assert.Equal(t, expected.text, token.Text, "Token %d text mismatch", i)
			}

			// Verify EOF
			token := lexer.NextToken()
			require.NotNil(t, token)
			assert.Equal(t, EOF, token.Type, "Should reach EOF")
		})
	}
}

// Test edge cases for character dispatch ordering
func TestCharacterDispatchOrdering(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		expectedTokens []struct {
			tokenType TokenType
			text      string
		}
	}{
		{
			name:  "E as identifier vs E'string'",
			input: "E E'string' ESCAPE",
			expectedTokens: []struct {
				tokenType TokenType
				text      string
			}{
				{IDENT, "E"},          // identifier
				{SCONST, "E'string'"}, // extended string
				{IDENT, "ESCAPE"},     // keyword
			},
		},
		{
			name:  "B as identifier vs B'bits'",
			input: "B B'1010' BOOLEAN",
			expectedTokens: []struct {
				tokenType TokenType
				text      string
			}{
				{IDENT, "B"},        // identifier
				{BCONST, "B'1010'"}, // bit string
				{IDENT, "BOOLEAN"},  // keyword
			},
		},
		{
			name:  "X as identifier vs X'hex'",
			input: "X X'DEAD' XMLPARSE",
			expectedTokens: []struct {
				tokenType TokenType
				text      string
			}{
				{IDENT, "X"},        // identifier
				{XCONST, "X'DEAD'"}, // hex string
				{IDENT, "XMLPARSE"}, // keyword
			},
		},
		{
			name:  "N as identifier vs N'national'",
			input: "N N'test' NULLIF",
			expectedTokens: []struct {
				tokenType TokenType
				text      string
			}{
				{IDENT, "N"},        // identifier
				{SCONST, "N'test'"}, // national string
				{IDENT, "NULLIF"},   // keyword
			},
		},
		{
			name:  "U as identifier vs U&'unicode'",
			input: "U U&'test' UNIQUE",
			expectedTokens: []struct {
				tokenType TokenType
				text      string
			}{
				{IDENT, "U"},         // identifier
				{SCONST, "U&'test'"}, // unicode string
				{UNIQUE, "UNIQUE"},   // keyword
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)

			for i, expected := range tt.expectedTokens {
				token := lexer.NextToken()
				require.NotNil(t, token, "Token %d should scan without error", i)

				assert.Equal(t, expected.tokenType, token.Type,
					"Token %d type mismatch: got %d (%q), want %d",
					i, int(token.Type), token.Text, int(expected.tokenType))
				assert.Equal(t, expected.text, token.Text, "Token %d text mismatch", i)
			}

			// Verify EOF
			token := lexer.NextToken()
			require.NotNil(t, token)
			assert.Equal(t, EOF, token.Type, "Should reach EOF")
		})
	}
}

// Benchmark enhanced lexer performance
func BenchmarkEnhancedLexing(b *testing.B) {
	input := `
		-- Performance test query
		SELECT u.user_id, u.name, p.price, p.description
		FROM users u
		JOIN products p ON u.id = p.user_id
		WHERE u.created >= $1 AND p.price <= $2
		  AND u.status <> 'inactive'
		ORDER BY u.name, p.price DESC
		LIMIT 100;
	`

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		lexer := NewLexer(input)

		for {
			token := lexer.NextToken()
			if token.Type == EOF {
				break
			}
		}
	}
}

// =============================================================================
// String Literal Content Extraction Tests
// =============================================================================

// Test proper string literal content extraction with quote doubling
func TestStringLiteralContentExtraction(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		expectedType  TokenType
		expectedValue string // Content without quotes
		expectedText  string // Original text with quotes
	}{
		{
			name:          "Simple string",
			input:         "'hello'",
			expectedType:  SCONST,
			expectedValue: "hello",
			expectedText:  "'hello'",
		},
		{
			name:          "Empty string",
			input:         "''",
			expectedType:  SCONST,
			expectedValue: "",
			expectedText:  "''",
		},
		{
			name:          "String with doubled quotes",
			input:         "'don''t'",
			expectedType:  SCONST,
			expectedValue: "don't",
			expectedText:  "'don''t'",
		},
		{
			name:          "String with multiple doubled quotes",
			input:         "'I''m ''ready'''",
			expectedType:  SCONST,
			expectedValue: "I'm 'ready'",
			expectedText:  "'I''m ''ready'''",
		},
		{
			name:          "String with newlines",
			input:         "'hello\nworld'",
			expectedType:  SCONST,
			expectedValue: "hello\nworld",
			expectedText:  "'hello\nworld'",
		},
		{
			name:          "String with special characters",
			input:         "'tab\ttab space   end'",
			expectedType:  SCONST,
			expectedValue: "tab\ttab space   end",
			expectedText:  "'tab\ttab space   end'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token := lexer.NextToken()

			require.NotNil(t, token, "Should scan without error")
			assert.Equal(t, tt.expectedType, token.Type, "Token type mismatch")
			assert.Equal(t, tt.expectedValue, token.Value.Str, "Token value (content) mismatch")
			assert.Equal(t, tt.expectedText, token.Text, "Token text (original) mismatch")
		})
	}
}

// Test bit string literal content extraction
func TestBitStringContentExtraction(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		expectedType  TokenType
		expectedValue string // Content without B'' wrapper
		expectedText  string // Original text with B'' wrapper
	}{
		{
			name:          "Simple bit string",
			input:         "B'1010'",
			expectedType:  BCONST,
			expectedValue: "1010",
			expectedText:  "B'1010'",
		},
		{
			name:          "Empty bit string",
			input:         "B''",
			expectedType:  BCONST,
			expectedValue: "",
			expectedText:  "B''",
		},
		{
			name:          "Long bit string",
			input:         "B'11110000101010111100'",
			expectedType:  BCONST,
			expectedValue: "11110000101010111100",
			expectedText:  "B'11110000101010111100'",
		},
		{
			name:          "Lowercase b prefix",
			input:         "b'0101'",
			expectedType:  BCONST,
			expectedValue: "0101",
			expectedText:  "b'0101'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token := lexer.NextToken()

			require.NotNil(t, token, "Should scan without error")
			assert.Equal(t, tt.expectedType, token.Type, "Token type mismatch")
			assert.Equal(t, tt.expectedValue, token.Value.Str, "Token value (content) mismatch")
			assert.Equal(t, tt.expectedText, token.Text, "Token text (original) mismatch")
		})
	}
}

// Test hex string literal content extraction
func TestHexStringContentExtraction(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		expectedType  TokenType
		expectedValue string // Content without X'' wrapper
		expectedText  string // Original text with X'' wrapper
	}{
		{
			name:          "Simple hex string",
			input:         "X'DEADBEEF'",
			expectedType:  XCONST,
			expectedValue: "DEADBEEF",
			expectedText:  "X'DEADBEEF'",
		},
		{
			name:          "Empty hex string",
			input:         "X''",
			expectedType:  XCONST,
			expectedValue: "",
			expectedText:  "X''",
		},
		{
			name:          "Mixed case hex",
			input:         "X'CaFeBaBe'",
			expectedType:  XCONST,
			expectedValue: "CaFeBaBe",
			expectedText:  "X'CaFeBaBe'",
		},
		{
			name:          "Lowercase x prefix",
			input:         "x'1234'",
			expectedType:  XCONST,
			expectedValue: "1234",
			expectedText:  "x'1234'",
		},
		{
			name:          "Long hex string",
			input:         "X'0123456789ABCDEF'",
			expectedType:  XCONST,
			expectedValue: "0123456789ABCDEF",
			expectedText:  "X'0123456789ABCDEF'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token := lexer.NextToken()

			require.NotNil(t, token, "Should scan without error")
			assert.Equal(t, tt.expectedType, token.Type, "Token type mismatch")
			assert.Equal(t, tt.expectedValue, token.Value.Str, "Token value (content) mismatch")
			assert.Equal(t, tt.expectedText, token.Text, "Token text (original) mismatch")
		})
	}
}

// Test extended string literal content extraction
func TestExtendedStringContentExtraction(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		expectedType  TokenType
		expectedValue string // Content without E'' wrapper (escapes NOT processed yet - Phase 2C)
		expectedText  string // Original text with E'' wrapper
	}{
		{
			name:          "Simple extended string",
			input:         "E'hello'",
			expectedType:  SCONST,
			expectedValue: "hello",
			expectedText:  "E'hello'",
		},
		{
			name:          "Extended string with doubled quotes",
			input:         "E'don''t'",
			expectedType:  SCONST,
			expectedValue: "don't",
			expectedText:  "E'don''t'",
		},
		{
			name:          "Extended string with escape sequences",
			input:         "E'hello\\nworld'",
			expectedType:  SCONST,
			expectedValue: "hello\nworld", // Phase 2C: Escapes ARE processed
			expectedText:  "E'hello\\nworld'",
		},
		{
			name:          "Lowercase e prefix",
			input:         "e'test'",
			expectedType:  SCONST,
			expectedValue: "test",
			expectedText:  "e'test'",
		},
		{
			name:          "Extended string with tab escape",
			input:         "E'tab\\there'",
			expectedType:  SCONST,
			expectedValue: "tab\there", // Phase 2C: Escapes ARE processed
			expectedText:  "E'tab\\there'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token := lexer.NextToken()

			require.NotNil(t, token, "Should scan without error")
			assert.Equal(t, tt.expectedType, token.Type, "Token type mismatch")
			assert.Equal(t, tt.expectedValue, token.Value.Str, "Token value (content) mismatch")
			assert.Equal(t, tt.expectedText, token.Text, "Token text (original) mismatch")
		})
	}
}

// Test error cases for string literals
func TestStringLiteralErrorCases(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		expectedType TokenType
		shouldError  bool
	}{
		{
			name:         "Unterminated string",
			input:        "'unterminated",
			expectedType: USCONST,
			shouldError:  true,
		},
		{
			name:         "Unterminated bit string",
			input:        "B'unterminated",
			expectedType: BCONST,
			shouldError:  true,
		},
		{
			name:         "Unterminated hex string",
			input:        "X'unterminated",
			expectedType: XCONST,
			shouldError:  true,
		},
		{
			name:         "Unterminated extended string",
			input:        "E'unterminated",
			expectedType: SCONST,
			shouldError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token := lexer.NextToken()

			require.NotNil(t, token, "Lexer should not return error (errors stored in context)")
			assert.Equal(t, tt.expectedType, token.Type, "Token type should match expected error type")

			ctx := lexer.GetContext()
			if tt.shouldError {
				assert.True(t, ctx.HasErrors(), "Should have lexer errors")
				assert.Greater(t, len(ctx.Errors()), 0, "Should have at least one error")
			} else {
				assert.False(t, ctx.HasErrors(), "Should not have lexer errors")
			}
		})
	}
}

// Test string literals in complex SQL context
func TestStringLiteralsInSQLContext(t *testing.T) {
	input := `SELECT 'simple', "identifier", E'extended\\n', B'1010', X'CAFE' FROM table WHERE name = 'O''Reilly' AND data = X'deadbeef'`

	lexer := NewLexer(input)
	tokens := []struct {
		expectedType   TokenType
		expectedText   string
		expectedValue  string // Only check if non-empty
		skipValueCheck bool   // Skip value check for complex cases
	}{
		{SELECT, "SELECT", "", true}, // SELECT is now a keyword in Phase 3C
		{SCONST, "'simple'", "simple", false},
		{TokenType(','), ",", "", true},
		{IDENT, "\"identifier\"", "identifier", false}, // delimited identifier
		{TokenType(','), ",", "", true},
		{SCONST, "E'extended\\\\n'", "extended\\n", false}, // Phase 2C: \\\\ becomes \\
		{TokenType(','), ",", "", true},
		{BCONST, "B'1010'", "1010", false},
		{TokenType(','), ",", "", true},
		{XCONST, "X'CAFE'", "CAFE", false},
		{FROM, "FROM", "", true},   // FROM is now a keyword in Phase 3C
		{TABLE, "table", "", true}, // TABLE is now a keyword in Phase 3C
		{WHERE, "WHERE", "", true}, // WHERE is now a keyword in Phase 3C
		{IDENT, "name", "", true},  // identifier - skip value check for now
		{TokenType('='), "=", "", true},
		{SCONST, "'O''Reilly'", "O'Reilly", false}, // doubled quote handling
		{AND, "AND", "", true},                     // keyword returns parser constant
		{IDENT, "data", "", true},                  // identifier - skip value check for now
		{TokenType('='), "=", "", true},
		{XCONST, "X'deadbeef'", "deadbeef", false},
	}

	for i, expected := range tokens {
		token := lexer.NextToken()
		require.NotNil(t, token, "Token %d should scan without error", i)

		assert.Equal(t, expected.expectedType, token.Type,
			"Token %d type mismatch: got %d (%q), want %d",
			i, int(token.Type), token.Text, int(expected.expectedType))
		assert.Equal(t, expected.expectedText, token.Text, "Token %d text mismatch", i)

		if !expected.skipValueCheck && expected.expectedValue != "" {
			assert.Equal(t, expected.expectedValue, token.Value.Str, "Token %d value mismatch", i)
		}
	}

	// Verify EOF
	token := lexer.NextToken()
	require.NotNil(t, token)
	assert.Equal(t, EOF, token.Type, "Should reach EOF")

	// Verify no errors
	ctx := lexer.GetContext()
	assert.Empty(t, ctx.GetErrors(), "Should have no lexer errors")
}
