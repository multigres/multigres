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

// Note: MockKeywordLookup removed - keyword functionality is now 
// provided directly by lexer package functions

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
			name: "Operator",
			token: NewToken(TYPECAST, 0, "::"),
			isOp: true,
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
	b, ok := ctx.PeekByte()
	require.True(t, ok, "PeekByte should succeed")
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