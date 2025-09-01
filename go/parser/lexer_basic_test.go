/*
 * PostgreSQL Parser Lexer - Basic Tokenization Tests
 *
 * This file contains tests for basic lexer functionality including:
 * - Token type recognition
 * - Basic lexing operations
 * - Token classification methods
 * - Position tracking
 * - Thread safety
 * - Performance benchmarks
 *
 * Consolidated from lexer_test.go, tokens_test.go, and expression_test.go
 */

package parser

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTokenTypes tests token type creation and basic properties
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

// TestTokenClassification tests token type classification methods
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

// TestBasicLexing tests basic lexer functionality
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

// TestLexicalTokenizing tests that expression-related tokens are correctly recognized
func TestLexicalTokenizing(t *testing.T) {
	tests := []struct {
		name               string
		input              string
		expectedTokenCount int
	}{
		{"simple identifier", "column1", 1},
		{"qualified name", "schema.table", 3},  // schema, dot, table
		{"arithmetic expr", "a + b", 3},        // a, +, b
		{"function call", "func(x)", 4},        // func, (, x, )
		{"type cast", "123::text", 3},          // 123, ::, text
		{"comparison", "a = b", 3},             // a, =, b
		{"logical expr", "x AND y", 3},         // x, AND, y
		{"complex expr", "func(a, b) + 10", 8}, // func, (, a, ,, b, ), +, 10
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)

			var tokenCount int
			for {
				token := lexer.NextToken()
				if token == nil || token.Type == EOF {
					break
				}
				tokenCount++
			}

			AssertNoLexerErrors(t, lexer)
			assert.Equal(t, tt.expectedTokenCount, tokenCount, "Expected %d tokens for %q, got %d", tt.expectedTokenCount, tt.input, tokenCount)
		})
	}
}

// TestExpressionTokenRecognition tests that expression tokens are properly categorized
func TestExpressionTokenRecognition(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		expectedType TokenType
	}{
		// Constants
		{"integer", "123", ICONST},
		{"float", "3.14", FCONST},
		{"string", "'hello'", SCONST},

		// Identifiers
		{"identifier", "column", COLUMN},

		// Operators
		{"typecast", "::", TYPECAST},
		{"less equal", "<=", LESS_EQUALS},
		{"greater equal", ">=", GREATER_EQUALS},
		{"not equal", "<>", NOT_EQUALS},

		// Keywords that are correctly mapped
		{"or keyword", "OR", OR},
		{"not keyword", "NOT", NOT},
		{"and keyword", "AND", AND},

		// Keywords now correctly mapped to their tokens
		{"true keyword", "TRUE", TRUE_P},
		{"false keyword", "FALSE", FALSE_P},
		{"null keyword", "NULL", NULL_P},
		{"bit keyword", "BIT", BIT},
		{"numeric keyword", "NUMERIC", NUMERIC},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token := lexer.NextToken()

			require.NotNil(t, token, "Expected token for %q, got nil", tt.input)
			assert.Equal(t, tt.expectedType, token.Type, "Expected token type %v for %q, got %v", tt.expectedType, tt.input, token.Type)
			AssertNoLexerErrors(t, lexer)
		})
	}
}

// TestLexerTokenGeneration tests that the lexer generates correct token sequences
func TestLexerTokenGeneration(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		tokens []TokenType
	}{
		{
			name:   "identifier token",
			input:  "test_id",
			tokens: []TokenType{IDENT},
		},
		{
			name:   "integer constant",
			input:  "123",
			tokens: []TokenType{ICONST},
		},
		{
			name:   "string constant",
			input:  "'hello'",
			tokens: []TokenType{SCONST},
		},
		{
			name:   "multiple tokens",
			input:  "id1, id2",
			tokens: []TokenType{IDENT, ',', IDENT},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)

			// Collect tokens
			var tokens []TokenType
			for {
				token := lexer.NextToken()
				if token == nil || token.Type == EOF {
					break
				}
				tokens = append(tokens, token.Type)
			}

			// Verify token sequence
			require.Equal(t, len(tt.tokens), len(tokens), "expected %d tokens, got %d", len(tt.tokens), len(tokens))

			for i, expectedType := range tt.tokens {
				assert.Equal(t, expectedType, tokens[i], "token %d: expected %v, got %v", i, expectedType, tokens[i])
			}
		})
	}
}

// TestEnhancedIdentifierRecognition tests enhanced identifier recognition with PostgreSQL rules
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

// TestComprehensiveOperatorRecognition tests comprehensive operator recognition
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

// TestPositionTracking tests position tracking
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

// TestThreadSafety tests thread safety
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

// TestComprehensiveSQLLexing tests comprehensive SQL lexing
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

// BenchmarkBasicLexing benchmarks basic lexing performance
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

// BenchmarkLexerCreation benchmarks lexer creation overhead
func BenchmarkLexerCreation(b *testing.B) {
	input := "SELECT * FROM table"

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		lexer := NewLexer(input)
		_ = lexer // Prevent optimization
	}
}

// BenchmarkEnhancedLexing benchmarks enhanced lexer performance
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

// TestLexerContext tests lexer context creation and basic functionality
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

// TestLiteralBuffer tests literal buffer functionality
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
