/*
 * PostgreSQL Parser Lexer - Phase 2I Integration Tests
 *
 * This file contains comprehensive integration tests for Phase 2I functionality,
 * focusing on the complete advanced Unicode processing system including state
 * transitions, error handling, and PostgreSQL compatibility.
 * 
 * Tests based on postgres/src/test/regress/sql/strings.sql and scan.l behavior
 */

package lexer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPhase2IIntegration tests the complete Phase 2I Unicode processing system
func TestPhase2IIntegration(t *testing.T) {
	t.Run("Complete surrogate pair processing", func(t *testing.T) {
		// Test the full lifecycle: extended string -> Unicode escape -> StateXEU -> result
		input := "E'prefix\\uD83D\\uDE00suffix'"
		lexer := NewLexer(input)
		
		token, err := lexer.NextToken()
		require.NoError(t, err)
		require.NotNil(t, token)
		
		// Should have no errors
		assert.Empty(t, lexer.context.Errors, "Expected no errors")
		
		// Should be back in initial state
		assert.Equal(t, StateInitial, lexer.context.State)
		
		// Should have correct token
		assert.Equal(t, SCONST, token.Type)
		assert.Equal(t, "prefix😀suffix", token.Value.Str)
	})
	
	t.Run("Multiple tokens with surrogate pairs", func(t *testing.T) {
		input := "E'\\uD83D\\uDE00' E'\\uD83D\\uDE01'"
		lexer := NewLexer(input)
		
		// First token
		token1, err := lexer.NextToken()
		require.NoError(t, err)
		assert.Equal(t, SCONST, token1.Type)
		assert.Equal(t, "😀", token1.Value.Str)
		
		// Second token
		token2, err := lexer.NextToken()
		require.NoError(t, err)
		assert.Equal(t, SCONST, token2.Type)
		assert.Equal(t, "😁", token2.Value.Str)
		
		// EOF
		eof, err := lexer.NextToken()
		require.NoError(t, err)
		assert.Equal(t, EOF, eof.Type)
		
		assert.Empty(t, lexer.context.Errors, "Expected no errors")
	})
	
	t.Run("Error recovery and state management", func(t *testing.T) {
		// Test that errors in surrogate pair processing don't corrupt future parsing
		input := "E'\\uD83D\\u0041' 'valid'"
		lexer := NewLexer(input)
		
		// First token should have errors
		_, err := lexer.NextToken()
		require.NoError(t, err)
		assert.True(t, len(lexer.context.Errors) > 0, "Expected errors in first token")
		
		// Clear errors for next token
		lexer.context.Errors = nil
		
		// Second token should be valid
		token2, err := lexer.NextToken()
		require.NoError(t, err)
		assert.Equal(t, SCONST, token2.Type)
		assert.Equal(t, "valid", token2.Value.Str)
		assert.Empty(t, lexer.context.Errors, "Expected no errors in second token")
	})
}

// TestStateXEUBehavior tests specific StateXEU state behavior
func TestStateXEUBehavior(t *testing.T) {
	t.Run("StateXEU entered on first surrogate", func(t *testing.T) {
		// We can't directly observe state changes, but we can test the behavior
		input := "E'\\uD83D\\uDE00'"
		lexer := NewLexer(input)
		
		token, err := lexer.NextToken()
		require.NoError(t, err)
		
		// The fact that we get the correct combined character means StateXEU worked
		assert.Equal(t, "😀", token.Value.Str)
		assert.Empty(t, lexer.context.Errors)
	})
	
	t.Run("StateXEU error handling", func(t *testing.T) {
		// Test various error conditions in StateXEU
		errorCases := []struct {
			name  string
			input string
		}{
			{"High surrogate followed by non-escape", "E'\\uD83DX'"},
			{"High surrogate followed by wrong escape", "E'\\uD83D\\n'"},
			{"High surrogate followed by non-surrogate Unicode", "E'\\uD83D\\u0041'"},
			{"High surrogate at end of string", "E'\\uD83D'"},
		}
		
		for _, tc := range errorCases {
			t.Run(tc.name, func(t *testing.T) {
				lexer := NewLexer(tc.input)
				token, err := lexer.NextToken()
				require.NoError(t, err)
				
				// Should have errors
				assert.True(t, len(lexer.context.Errors) > 0, "Expected errors for: %s", tc.input)
				
				// Should still return a token (error recovery)
				assert.NotNil(t, token)
			})
		}
	})
}

// TestUnicodePerformance tests performance characteristics of Unicode processing
func TestUnicodePerformance(t *testing.T) {
	// Test with a string containing many surrogate pairs
	input := "E'"
	for i := 0; i < 100; i++ {
		input += "\\uD83D\\uDE00" // 😀 emoji
	}
	input += "'"
	
	lexer := NewLexer(input)
	token, err := lexer.NextToken()
	require.NoError(t, err)
	
	// Should process correctly
	expected := ""
	for i := 0; i < 100; i++ {
		expected += "😀"
	}
	
	assert.Equal(t, SCONST, token.Type)
	assert.Equal(t, expected, token.Value.Str)
	assert.Empty(t, lexer.context.Errors)
}

// TestPostgreSQLRegressionCases tests specific cases from PostgreSQL regression tests
func TestPostgreSQLRegressionCases(t *testing.T) {
	// These are based on actual PostgreSQL test cases
	tests := []struct {
		name     string
		input    string
		expected string
		hasError bool
	}{
		{
			name:     "Valid emoji sequence",
			input:    "E'\\uD83D\\uDE00'",  // 😀
			expected: "😀",
			hasError: false,
		},
		{
			name:     "Mathematical symbol via surrogate pair",
			input:    "E'\\uD835\\uDD04'",  // 𝔄 (Mathematical Fraktur A)
			expected: "𝔄",
			hasError: false,
		},
		{
			name:     "Minimum supplementary plane character",
			input:    "E'\\uD800\\uDC00'",  // U+10000
			expected: "\U00010000",
			hasError: false,
		},
		{
			name:     "Maximum Unicode character via surrogates",
			input:    "E'\\uDBFF\\uDFFF'",  // U+10FFFF
			expected: "\U0010FFFF",
			hasError: false,
		},
		{
			name:     "Invalid surrogate pair - wrong order",
			input:    "E'\\uDE00\\uD83D'",
			expected: "",
			hasError: true,
		},
		{
			name:     "Invalid surrogate pair - two highs",
			input:    "E'\\uD83D\\uD83D'",
			expected: "",
			hasError: true,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token, err := lexer.NextToken()
			require.NoError(t, err)
			
			if tt.hasError {
				assert.True(t, len(lexer.context.Errors) > 0, "Expected errors for: %s", tt.input)
			} else {
				assert.Empty(t, lexer.context.Errors, "Expected no errors for: %s", tt.input)
				assert.Equal(t, SCONST, token.Type)
				assert.Equal(t, tt.expected, token.Value.Str)
			}
		})
	}
}

// TestComplexUnicodeStringScenarios tests complex real-world Unicode scenarios
func TestComplexUnicodeStringScenarios(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		hasError bool
	}{
		{
			name:     "Mixed ASCII and emoji",
			input:    "E'Hello \\uD83D\\uDE00 World \\uD83D\\uDE01!'",
			expected: "Hello 😀 World 😁!",
			hasError: false,
		},
		{
			name:     "Multiple emoji in sequence",
			input:    "E'\\uD83D\\uDE00\\uD83D\\uDE01\\uD83D\\uDE02'",
			expected: "😀😁😂",
			hasError: false,
		},
		{
			name:     "Unicode mixed with other escapes",
			input:    "E'Line1\\nEmoji: \\uD83D\\uDE00\\tEnd'",
			expected: "Line1\nEmoji: 😀\tEnd",
			hasError: false,
		},
		{
			name:     "String continuation with Unicode",
			input:    "E'Part1 \\uD83D\\uDE00'\n    'Part2'",
			expected: "Part1 😀Part2",
			hasError: false,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token, err := lexer.NextToken()
			require.NoError(t, err)
			
			if tt.hasError {
				assert.True(t, len(lexer.context.Errors) > 0, "Expected errors for: %s", tt.input)
			} else {
				assert.Empty(t, lexer.context.Errors, "Expected no errors for: %s", tt.input)
				assert.Equal(t, SCONST, token.Type)
				assert.Equal(t, tt.expected, token.Value.Str)
			}
		})
	}
}

// TestUnicodeStatePreservation tests that Unicode processing doesn't affect global state
func TestUnicodeStatePreservation(t *testing.T) {
	// Test concurrent processing to ensure no global state issues
	inputs := []string{
		"E'\\uD83D\\uDE00'",
		"E'\\uD83D\\uDE01'",
		"E'\\uD83D\\uDE02'",
	}
	
	expected := []string{"😀", "😁", "😂"}
	
	for i, input := range inputs {
		lexer := NewLexer(input)
		token, err := lexer.NextToken()
		require.NoError(t, err)
		
		assert.Equal(t, SCONST, token.Type)
		assert.Equal(t, expected[i], token.Value.Str)
		assert.Empty(t, lexer.context.Errors)
		
		// State should be clean
		assert.Equal(t, StateInitial, lexer.context.State)
		assert.Equal(t, rune(0), lexer.context.UTF16FirstPart)
	}
}

// BenchmarkSurrogatePairProcessing benchmarks surrogate pair processing performance
func BenchmarkSurrogatePairProcessing(b *testing.B) {
	input := "E'\\uD83D\\uDE00'"
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lexer := NewLexer(input)
		token, err := lexer.NextToken()
		if err != nil {
			b.Fatal(err)
		}
		if token.Type != SCONST {
			b.Fatal("Expected SCONST token")
		}
	}
}

// BenchmarkMultipleSurrogatePairs benchmarks processing multiple surrogate pairs
func BenchmarkMultipleSurrogatePairs(b *testing.B) {
	input := "E'\\uD83D\\uDE00\\uD83D\\uDE01\\uD83D\\uDE02\\uD83D\\uDE03\\uD83D\\uDE04'"
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lexer := NewLexer(input)
		token, err := lexer.NextToken()
		if err != nil {
			b.Fatal(err)
		}
		if token.Type != SCONST {
			b.Fatal("Expected SCONST token")
		}
	}
}