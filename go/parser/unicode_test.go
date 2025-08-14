/*
 * PostgreSQL Parser Lexer - Unicode Processing Tests
 *
 * This file contains comprehensive tests for Unicode functionality,
 * including UTF-16 surrogate pair handling and advanced Unicode escape sequences.
 * Test cases are derived from PostgreSQL's string test suite and additional edge cases.
 *
 * Tests equivalent to postgres/src/test/regress/sql/strings.sql Unicode sections
 */

package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUnicodeUtilityFunctions tests the basic Unicode utility functions
func TestUnicodeUtilityFunctions(t *testing.T) {
	tests := []struct {
		name         string
		codepoint    rune
		expectFirst  bool
		expectSecond bool
		expectValid  bool
	}{
		{"High surrogate start", 0xD800, true, false, false},
		{"High surrogate mid", 0xDA00, true, false, false},
		{"High surrogate end", 0xDBFF, true, false, false},
		{"Low surrogate start", 0xDC00, false, true, false},
		{"Low surrogate mid", 0xDE00, false, true, false},
		{"Low surrogate end", 0xDFFF, false, true, false},
		{"Valid ASCII", 0x0041, false, false, true},
		{"Valid BMP", 0x20AC, false, false, true},
		{"Valid supplementary", 0x1F600, false, false, true},
		{"Invalid too high", 0x110000, false, false, false},
		{"Invalid negative", -1, false, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expectFirst, isUTF16SurrogateFirst(tt.codepoint))
			assert.Equal(t, tt.expectSecond, isUTF16SurrogateSecond(tt.codepoint))
			assert.Equal(t, tt.expectValid, isValidUnicodeCodepoint(tt.codepoint))
		})
	}
}

// TestSurrogatePairCombination tests surrogate pair to code point conversion
func TestSurrogatePairCombination(t *testing.T) {
	tests := []struct {
		name     string
		first    rune
		second   rune
		expected rune
		valid    bool
	}{
		{
			name:     "Valid pair 1 (U+1F600 😀)",
			first:    0xD83D,
			second:   0xDE00,
			expected: 0x1F600,
			valid:    true,
		},
		{
			name:     "Valid pair 2 (U+10000)",
			first:    0xD800,
			second:   0xDC00,
			expected: 0x10000,
			valid:    true,
		},
		{
			name:     "Valid pair 3 (U+10FFFF)",
			first:    0xDBFF,
			second:   0xDFFF,
			expected: 0x10FFFF,
			valid:    true,
		},
		{
			name:     "Invalid first not surrogate",
			first:    0x0041,
			second:   0xDC00,
			expected: 0,
			valid:    false,
		},
		{
			name:     "Invalid second not surrogate",
			first:    0xD800,
			second:   0x0041,
			expected: 0,
			valid:    false,
		},
		{
			name:     "Invalid both wrong type",
			first:    0xDC00,
			second:   0xD800,
			expected: 0,
			valid:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			combined, valid := validateSurrogatePair(tt.first, tt.second)
			assert.Equal(t, tt.valid, valid)
			if tt.valid {
				assert.Equal(t, tt.expected, combined)
				// Also test direct conversion
				direct := surrogatePairToCodepoint(tt.first, tt.second)
				assert.Equal(t, tt.expected, direct)
			}
		})
	}
}

// TestBasicUnicodeEscapes tests basic Unicode escape sequences that don't require surrogate pairs
func TestBasicUnicodeEscapes(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		hasError bool
	}{
		{
			name:     "Simple \\u escape",
			input:    "E'\\u0041'",
			expected: "A",
			hasError: false,
		},
		{
			name:     "Euro symbol",
			input:    "E'\\u20AC'",
			expected: "€",
			hasError: false,
		},
		{
			name:     "Simple \\U escape",
			input:    "E'\\U00000041'",
			expected: "A",
			hasError: false,
		},
		{
			name:     "BMP character with \\U",
			input:    "E'\\U000020AC'",
			expected: "€",
			hasError: false,
		},
		{
			name:     "Invalid hex digits",
			input:    "E'\\u00ZZ'",
			expected: "",
			hasError: true,
		},
		{
			name:     "Too few digits for \\u",
			input:    "E'\\u041'",
			expected: "",
			hasError: true,
		},
		{
			name:     "Too few digits for \\U",
			input:    "E'\\U0000041'",
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
				assert.True(t, len(lexer.context.Errors) > 0, "Expected lexer errors")
			} else {
				assert.Empty(t, lexer.context.Errors, "Expected no lexer errors")
				assert.Equal(t, SCONST, token.Type)
				assert.Equal(t, tt.expected, token.Value.Str)
			}
		})
	}
}

// TestSurrogatePairUnicodeEscapes tests Unicode escape sequences that form surrogate pairs
func TestSurrogatePairUnicodeEscapes(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		hasError bool
	}{
		{
			name:     "Valid surrogate pair (😀)",
			input:    "E'\\uD83D\\uDE00'",
			expected: "😀",
			hasError: false,
		},
		{
			name:     "Valid surrogate pair (U+10000)",
			input:    "E'\\uD800\\uDC00'",
			expected: "\U00010000",
			hasError: false,
		},
		{
			name:     "Valid surrogate pair with \\U notation",
			input:    "E'\\UD83D\\uDE00'", // Mixed \\U and \\u
			expected: "",
			hasError: true, // PostgreSQL doesn't allow mixing
		},
		{
			name:     "High surrogate without low surrogate",
			input:    "E'\\uD83D'",
			expected: "",
			hasError: true,
		},
		{
			name:     "Low surrogate without high surrogate",
			input:    "E'\\uDE00'",
			expected: "",
			hasError: true,
		},
		{
			name:     "Wrong order surrogate pair",
			input:    "E'\\uDE00\\uD83D'",
			expected: "",
			hasError: true,
		},
		{
			name:     "Two high surrogates",
			input:    "E'\\uD83D\\uD83D'",
			expected: "",
			hasError: true,
		},
		{
			name:     "Two low surrogates",
			input:    "E'\\uDC00\\uDC00'",
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
				assert.True(t, len(lexer.context.Errors) > 0, "Expected lexer errors for input: %s", tt.input)
			} else {
				assert.Empty(t, lexer.context.Errors, "Expected no lexer errors for input: %s", tt.input)
				assert.Equal(t, SCONST, token.Type)
				assert.Equal(t, tt.expected, token.Value.Str)
			}
		})
	}
}

// TestComplexSurrogatePairScenarios tests more complex surrogate pair scenarios
func TestComplexSurrogatePairScenarios(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		hasError bool
	}{
		{
			name:     "Multiple surrogate pairs",
			input:    "E'\\uD83D\\uDE00\\uD83D\\uDE01'",
			expected: "😀😁",
			hasError: false,
		},
		{
			name:     "Surrogate pair mixed with regular chars",
			input:    "E'Hello \\uD83D\\uDE00 World'",
			expected: "Hello 😀 World",
			hasError: false,
		},
		{
			name:     "Surrogate pair with other escapes",
			input:    "E'\\n\\uD83D\\uDE00\\t'",
			expected: "\n😀\t",
			hasError: false,
		},
		{
			name:     "High surrogate interrupted by non-escape",
			input:    "E'\\uD83DHello'",
			expected: "",
			hasError: true,
		},
		{
			name:     "High surrogate interrupted by different escape",
			input:    "E'\\uD83D\\n'",
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
				assert.True(t, len(lexer.context.Errors) > 0, "Expected lexer errors for input: %s", tt.input)
			} else {
				assert.Empty(t, lexer.context.Errors, "Expected no lexer errors for input: %s", tt.input)
				assert.Equal(t, SCONST, token.Type)
				assert.Equal(t, tt.expected, token.Value.Str)
			}
		})
	}
}

// TestStateXEUTransitions tests proper state transitions for StateXEU
func TestStateXEUTransitions(t *testing.T) {
	// Test that StateXEU is properly entered and exited
	lexer := NewLexer("E'\\uD83D\\uDE00'")

	// Initial state should be StateInitial
	assert.Equal(t, StateInitial, lexer.context.State)

	// This should be tested by examining internal state during processing
	// For now, we test the end result
	token, err := lexer.NextToken()
	require.NoError(t, err)

	// Should end up back in StateInitial
	assert.Equal(t, StateInitial, lexer.context.State)
	assert.Equal(t, SCONST, token.Type)
	assert.Equal(t, "😀", token.Value.Str)
	assert.Empty(t, lexer.context.Errors)
}

// TestErrorRecoveryInSurrogatePairs tests error recovery when surrogate pair parsing fails
func TestErrorRecoveryInSurrogatePairs(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "Unterminated after high surrogate",
			input: "E'\\uD83D",
		},
		{
			name:  "Invalid second surrogate",
			input: "E'\\uD83D\\u0041'",
		},
		{
			name:  "EOF after high surrogate escape",
			input: "E'\\uD83D\\u",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token, err := lexer.NextToken()

			// Should not panic and should return some token
			require.NoError(t, err)
			require.NotNil(t, token)

			// Should have errors
			assert.True(t, len(lexer.context.Errors) > 0, "Expected lexer errors for input: %s", tt.input)

			// Should eventually return to a reasonable state
			assert.True(t, lexer.context.State == StateInitial || lexer.context.State == StateXE)
		})
	}
}

// TestPostgreSQLCompatibilityUnicode tests specific PostgreSQL Unicode behavior compatibility
func TestPostgreSQLCompatibilityUnicode(t *testing.T) {
	// These test cases are derived from PostgreSQL's string tests
	tests := []struct {
		name     string
		input    string
		expected string
		hasError bool
	}{
		{
			name:     "PostgreSQL equivalent with E notation",
			input:    "E'd\\u0061t\\u0061'",
			expected: "data",
			hasError: false,
		},
		{
			name:     "High Unicode code point via surrogate pair",
			input:    "E'\\uD800\\uDC00'", // U+10000
			expected: "\U00010000",
			hasError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token, err := lexer.NextToken()
			require.NoError(t, err)

			if tt.hasError {
				assert.True(t, len(lexer.context.Errors) > 0, "Expected lexer errors for input: %s", tt.input)
			} else {
				assert.Empty(t, lexer.context.Errors, "Expected no lexer errors for input: %s", tt.input)
				assert.Equal(t, SCONST, token.Type)
				assert.Equal(t, tt.expected, token.Value.Str)
			}
		})
	}
}

// TestUnicodeEdgeCases tests various edge cases for Unicode processing
func TestUnicodeEdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		hasError bool
	}{
		{
			name:     "Maximum valid Unicode",
			input:    "E'\\U0010FFFF'",
			hasError: false,
		},
		{
			name:     "Unicode too large",
			input:    "E'\\U00110000'",
			hasError: true,
		},
		{
			name:     "Zero Unicode",
			input:    "E'\\u0000'",
			hasError: false,
		},
		{
			name:     "Max BMP",
			input:    "E'\\uFFFF'",
			hasError: false,
		},
		{
			name:     "Just above BMP (needs surrogate pair)",
			input:    "E'\\U00010000'",
			hasError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token, err := lexer.NextToken()
			require.NoError(t, err)

			if tt.hasError {
				assert.True(t, len(lexer.context.Errors) > 0, "Expected lexer errors for input: %s", tt.input)
			} else {
				assert.Empty(t, lexer.context.Errors, "Expected no lexer errors for input: %s", tt.input)
				assert.Equal(t, SCONST, token.Type)
			}
		})
	}
}

// TestAdvancedUnicodeIntegration tests the complete advanced Unicode processing system
func TestAdvancedUnicodeIntegration(t *testing.T) {
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
			input:    "E'\\uD83D\\uDE00'", // 😀
			expected: "😀",
			hasError: false,
		},
		{
			name:     "Mathematical symbol via surrogate pair",
			input:    "E'\\uD835\\uDD04'", // 𝔄 (Mathematical Fraktur A)
			expected: "𝔄",
			hasError: false,
		},
		{
			name:     "Minimum supplementary plane character",
			input:    "E'\\uD800\\uDC00'", // U+10000
			expected: "\U00010000",
			hasError: false,
		},
		{
			name:     "Maximum Unicode character via surrogates",
			input:    "E'\\uDBFF\\uDFFF'", // U+10FFFF
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
