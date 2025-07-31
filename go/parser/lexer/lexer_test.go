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

package lexer

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
			token:    NewKeywordToken("SELECT", 0, "SELECT"),
			expected: IDENT,
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
	assert.Equal(t, len(input), ctx.ScanBufLen, "Buffer length mismatch")
	assert.Equal(t, 1, ctx.LineNumber, "Initial line number should be 1")
	assert.Equal(t, 1, ctx.ColumnNumber, "Initial column number should be 1")
	assert.Equal(t, StateInitial, ctx.State, "Initial state mismatch")

	// Test byte reading
	b, ok := ctx.CurrentByte()
	require.True(t, ok, "CurrentByte should succeed")
	assert.Equal(t, byte('S'), b, "Expected to peek 'S'")

	b, ok = ctx.NextByte()
	require.True(t, ok, "NextByte should succeed")
	assert.Equal(t, byte('S'), b, "Expected to read 'S'")

	// Test position tracking
	assert.Equal(t, 1, ctx.CurrentPosition, "Position should be 1 after reading one byte")
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
	assert.False(t, ctx.LiteralActive, "Literal buffer should be inactive after GetLiteral()")
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
			expected: []TokenType{IDENT, EOF}, // Keywords return as IDENT initially
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
				token, err := lexer.NextToken()
				require.NoError(t, err, "Unexpected error at position %d", i)
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

			// Consume all tokens
			for {
				token, err := lexer.NextToken()
				require.NoError(t, err, "Unexpected lexer error")
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
					token, err := lexer.NextToken()
					if err != nil {
						assert.NoError(t, err, "Goroutine %d, iteration %d: unexpected error", goroutineID, j)
						return
					}
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
	token, err := lexer.NextToken()
	require.NoError(t, err, "Unexpected error")
	assert.Equal(t, 0, token.Position, "First token should be at position 0")

	// Second token should be "line2"
	token, err = lexer.NextToken()
	require.NoError(t, err, "Unexpected error")

	// Third token should be "token" with proper position
	token, err = lexer.NextToken()
	require.NoError(t, err, "Unexpected error")

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
			token, err := lexer.NextToken()
			if err != nil {
				b.Fatalf("Unexpected error: %v", err)
			}
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
// Phase 2B: Enhanced Lexer Engine Tests
// =============================================================================

// Phase 2B: Test enhanced identifier recognition with PostgreSQL rules
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
				{IDENT, "SELECT", true, "select"},
				{IDENT, "Select", true, "select"},
				{IDENT, "select", true, "select"},
				{IDENT, "FROM", true, "from"},
				{IDENT, "From", true, "from"},
				{IDENT, "from", true, "from"},
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
				token, err := lexer.NextToken()
				require.NoError(t, err, "Token %d should scan without error", i)

				assert.Equal(t, expected.tokenType, token.Type, "Token %d type mismatch", i)
				assert.Equal(t, expected.text, token.Text, "Token %d text mismatch", i)

				if expected.isKeyword {
					assert.Equal(t, expected.keyword, token.Value.Keyword, "Token %d keyword mismatch", i)
				} else {
					assert.Equal(t, expected.text, token.Value.Str, "Token %d string value mismatch", i)
				}
			}

			// Verify EOF
			token, err := lexer.NextToken()
			require.NoError(t, err)
			assert.Equal(t, EOF, token.Type, "Should reach EOF")
		})
	}
}

// Phase 2B: Test comprehensive operator recognition
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
				{IDENT, "AND"}, // keyword
				{IDENT, "name"},
				{NOT_EQUALS, "<>"},
				{SCONST, "'test'"},
				{IDENT, "OR"}, // keyword
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
				token, err := lexer.NextToken()
				require.NoError(t, err, "Token %d should scan without error", i)

				assert.Equal(t, expected.tokenType, token.Type, "Token %d type mismatch: got %d, want %d", i, int(token.Type), int(expected.tokenType))
				assert.Equal(t, expected.text, token.Text, "Token %d text mismatch", i)
			}

			// Verify EOF
			token, err := lexer.NextToken()
			require.NoError(t, err)
			assert.Equal(t, EOF, token.Type, "Should reach EOF")
		})
	}
}

// Phase 2B: Test enhanced whitespace and comment handling
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
				{IDENT, "SELECT", 0},
				{IDENT, "FROM", 28},
				{IDENT, "table", 33},
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
				{IDENT, "SELECT", 0},
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
				token, err := lexer.NextToken()
				require.NoError(t, err, "Token %d should scan without error", i)

				assert.Equal(t, expected.tokenType, token.Type, "Token %d type mismatch", i)
				assert.Equal(t, expected.text, token.Text, "Token %d text mismatch", i)
				assert.Equal(t, expected.position, token.Position, "Token %d position mismatch", i)
			}

			// Verify EOF
			token, err := lexer.NextToken()
			require.NoError(t, err)
			assert.Equal(t, EOF, token.Type, "Should reach EOF")
		})
	}
}

// Phase 2B: Test state machine foundation
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
				token, err := lexer.NextToken()
				require.NoError(t, err)
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

// Phase 2B: Test parameter recognition
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
				{IDENT, "WHERE", 0}, // keyword
				{IDENT, "id", 0},
				{TokenType('='), "=", 0},
				{PARAM, "$1", 1},
				{IDENT, "AND", 0}, // keyword
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
				token, err := lexer.NextToken()
				require.NoError(t, err, "Token %d should scan without error", i)

				assert.Equal(t, expected.tokenType, token.Type, "Token %d type mismatch", i)
				assert.Equal(t, expected.text, token.Text, "Token %d text mismatch", i)

				if expected.tokenType == PARAM {
					assert.Equal(t, expected.intValue, token.Value.Ival, "Token %d parameter value mismatch", i)
				}
			}

			// Verify EOF
			token, err := lexer.NextToken()
			require.NoError(t, err)
			assert.Equal(t, EOF, token.Type, "Should reach EOF")
		})
	}
}

// Phase 2B: Test comprehensive SQL lexing
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
		{IDENT, "SELECT"}, // keyword
		{IDENT, "u"},
		{TokenType('.'), "."},
		{IDENT, "name"},
		{TokenType(','), ","},
		{IDENT, "p"},
		{TokenType('.'), "."},
		{IDENT, "price"},
		{IDENT, "FROM"}, // keyword
		{IDENT, "users"},
		{IDENT, "u"},
		{TokenType(','), ","},
		{IDENT, "products"},
		{IDENT, "p"},
		{IDENT, "WHERE"}, // keyword
		{IDENT, "u"},
		{TokenType('.'), "."},
		{IDENT, "id"},
		{GREATER_EQUALS, ">="},
		{PARAM, "$1"},
		{IDENT, "AND"}, // keyword
		{IDENT, "p"},
		{TokenType('.'), "."},
		{IDENT, "price"},
		{NOT_EQUALS, "<>"},
		{SCONST, "'free'"},
		{IDENT, "AND"}, // keyword
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
		token, err := lexer.NextToken()
		require.NoError(t, err, "Token %d should scan without error", i)

		assert.Equal(t, expected.expectedType, token.Type,
			"Token %d type mismatch: got %d (%q), want %d",
			i, int(token.Type), token.Text, int(expected.expectedType))
		assert.Equal(t, expected.expectedText, token.Text, "Token %d text mismatch", i)
	}

	// Verify EOF
	token, err := lexer.NextToken()
	require.NoError(t, err)
	assert.Equal(t, EOF, token.Type, "Should reach EOF")

	// Verify no errors
	ctx := lexer.GetContext()
	assert.Empty(t, ctx.Errors, "Should have no lexer errors")
}

// Phase 2B: Test character classification functions
func TestCharacterClassification(t *testing.T) {
	tests := []struct {
		name     string
		function func(byte) bool
		valid    []byte
		invalid  []byte
	}{
		{
			name:     "isIdentStart",
			function: isIdentStart,
			valid:    []byte{'a', 'z', 'A', 'Z', '_', 0x80, 0xFF},
			invalid:  []byte{'0', '9', '$', '-', '+', ' ', '\t'},
		},
		{
			name:     "isIdentCont",
			function: isIdentCont,
			valid:    []byte{'a', 'z', 'A', 'Z', '_', '0', '9', '$', 0x80, 0xFF},
			invalid:  []byte{'-', '+', ' ', '\t', '(', ')'},
		},
		{
			name:     "isDigit",
			function: isDigit,
			valid:    []byte{'0', '1', '5', '9'},
			invalid:  []byte{'a', 'A', '_', '$', ' ', '\t'},
		},
		{
			name:     "isWhitespace",
			function: isWhitespace,
			valid:    []byte{' ', '\t', '\n', '\r', '\f', '\v'},
			invalid:  []byte{'a', '0', '_', '$'},
		},
		{
			name:     "isSelfChar",
			function: isSelfChar,
			valid:    []byte{',', '(', ')', '[', ']', '.', ';', ':', '+', '-', '*', '/', '%', '^', '<', '>', '='},
			invalid:  []byte{'a', '0', '_', '$', ' ', '\t', '~', '!', '@'},
		},
		{
			name:     "isOpChar",
			function: isOpChar,
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

// Phase 2B: Test hex, bit, and extended string literal recognition
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
				{IDENT, "U&\"unicode_id\""},
				{SCONST, "U&'unicode_str'"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)

			for i, expected := range tt.expectedTokens {
				token, err := lexer.NextToken()
				require.NoError(t, err, "Token %d should scan without error", i)

				assert.Equal(t, expected.tokenType, token.Type,
					"Token %d type mismatch: got %d (%q), want %d",
					i, int(token.Type), token.Text, int(expected.tokenType))
				assert.Equal(t, expected.text, token.Text, "Token %d text mismatch", i)
			}

			// Verify EOF
			token, err := lexer.NextToken()
			require.NoError(t, err)
			assert.Equal(t, EOF, token.Type, "Should reach EOF")
		})
	}
}

// Phase 2B: Test edge cases for character dispatch ordering
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
				{IDENT, "UNIQUE"},    // keyword
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)

			for i, expected := range tt.expectedTokens {
				token, err := lexer.NextToken()
				require.NoError(t, err, "Token %d should scan without error", i)

				assert.Equal(t, expected.tokenType, token.Type,
					"Token %d type mismatch: got %d (%q), want %d",
					i, int(token.Type), token.Text, int(expected.tokenType))
				assert.Equal(t, expected.text, token.Text, "Token %d text mismatch", i)
			}

			// Verify EOF
			token, err := lexer.NextToken()
			require.NoError(t, err)
			assert.Equal(t, EOF, token.Type, "Should reach EOF")
		})
	}
}

// Phase 2B: Benchmark enhanced lexer performance
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
			token, err := lexer.NextToken()
			if err != nil {
				b.Fatalf("Unexpected error: %v", err)
			}
			if token.Type == EOF {
				break
			}
		}
	}
}

// =============================================================================
// Phase 2B: String Literal Content Extraction Tests (Enhancement)
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
			token, err := lexer.NextToken()

			require.NoError(t, err, "Should scan without error")
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
			token, err := lexer.NextToken()

			require.NoError(t, err, "Should scan without error")
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
			token, err := lexer.NextToken()

			require.NoError(t, err, "Should scan without error")
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
			token, err := lexer.NextToken()

			require.NoError(t, err, "Should scan without error")
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
			token, err := lexer.NextToken()

			require.NoError(t, err, "Lexer should not return error (errors stored in context)")
			assert.Equal(t, tt.expectedType, token.Type, "Token type should match expected error type")

			ctx := lexer.GetContext()
			if tt.shouldError {
				assert.True(t, ctx.HasErrors(), "Should have lexer errors")
				assert.Greater(t, len(ctx.Errors), 0, "Should have at least one error")
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
		{IDENT, "SELECT", "", true}, // keyword - skip value check
		{SCONST, "'simple'", "simple", false},
		{TokenType(','), ",", "", true},
		{IDENT, "\"identifier\"", "identifier", false}, // delimited identifier
		{TokenType(','), ",", "", true},
		{SCONST, "E'extended\\\\n'", "extended\\n", false}, // Phase 2C: \\\\ becomes \\
		{TokenType(','), ",", "", true},
		{BCONST, "B'1010'", "1010", false},
		{TokenType(','), ",", "", true},
		{XCONST, "X'CAFE'", "CAFE", false},
		{IDENT, "FROM", "", true},  // keyword - skip value check
		{IDENT, "table", "", true}, // identifier - skip value check for now
		{IDENT, "WHERE", "", true}, // keyword - skip value check
		{IDENT, "name", "", true},  // identifier - skip value check for now
		{TokenType('='), "=", "", true},
		{SCONST, "'O''Reilly'", "O'Reilly", false}, // doubled quote handling
		{IDENT, "AND", "", true},                   // keyword - skip value check
		{IDENT, "data", "", true},                  // identifier - skip value check for now
		{TokenType('='), "=", "", true},
		{XCONST, "X'deadbeef'", "deadbeef", false},
	}

	for i, expected := range tokens {
		token, err := lexer.NextToken()
		require.NoError(t, err, "Token %d should scan without error", i)

		assert.Equal(t, expected.expectedType, token.Type,
			"Token %d type mismatch: got %d (%q), want %d",
			i, int(token.Type), token.Text, int(expected.expectedType))
		assert.Equal(t, expected.expectedText, token.Text, "Token %d text mismatch", i)

		if !expected.skipValueCheck && expected.expectedValue != "" {
			assert.Equal(t, expected.expectedValue, token.Value.Str, "Token %d value mismatch", i)
		}
	}

	// Verify EOF
	token, err := lexer.NextToken()
	require.NoError(t, err)
	assert.Equal(t, EOF, token.Type, "Should reach EOF")

	// Verify no errors
	ctx := lexer.GetContext()
	assert.Empty(t, ctx.Errors, "Should have no lexer errors")
}
