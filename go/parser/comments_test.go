/*
PostgreSQL Parser Lexer - Comment Tests

This file tests the comment handling functionality of the PostgreSQL-compatible lexer.
Tests both single-line (--) and multi-line comments with nesting.
*/

package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSingleLineComments tests single-line comment handling
func TestSingleLineComments(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []TokenType
		values   []string
	}{
		{
			name:     "simple single-line comment",
			input:    "SELECT -- this is a comment\n5",
			expected: []TokenType{SELECT, ICONST, EOF},
			values:   []string{"select", "5", ""},
		},
		{
			name:     "comment at end of line",
			input:    "SELECT 5 -- comment",
			expected: []TokenType{SELECT, ICONST, EOF},
			values:   []string{"select", "5", ""},
		},
		{
			name:     "comment with no newline at EOF",
			input:    "SELECT 5-- comment at EOF",
			expected: []TokenType{SELECT, ICONST, EOF},
			values:   []string{"select", "5", ""},
		},
		{
			name:     "multiple single-line comments",
			input:    "-- first comment\nSELECT\n-- second comment\n5",
			expected: []TokenType{SELECT, ICONST, EOF},
			values:   []string{"select", "5", ""},
		},
		{
			name:     "comment with special characters",
			input:    "SELECT -- /* not a nested comment */ still a comment\n5",
			expected: []TokenType{SELECT, ICONST, EOF},
			values:   []string{"select", "5", ""},
		},
		{
			name:     "double dash in operator",
			input:    "5---3", // 5 -- -3 (comment consumes rest of line)
			expected: []TokenType{ICONST, EOF},
			values:   []string{"5", ""},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lexer := NewLexer(test.input)
			tokens := ScanAllTokens(t, lexer)

			require.Equal(t, len(test.expected), len(tokens))
			for i, token := range tokens {
				assert.Equal(t, test.expected[i], token.Type, "Token %d type mismatch", i)
				assert.Equal(t, test.values[i], token.Value.Str, "Token %d value mismatch", i)
			}
		})
	}
}

// TestMultiLineComments tests multi-line comment handling
func TestMultiLineComments(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    []TokenType
		values      []string
		expectError bool
		errorMsg    string
	}{
		{
			name:     "simple multi-line comment",
			input:    "SELECT /* comment */ 5",
			expected: []TokenType{SELECT, ICONST, EOF},
			values:   []string{"select", "5", ""},
		},
		{
			name:     "multi-line comment spanning lines",
			input:    "SELECT /* line 1\nline 2\nline 3 */ 5",
			expected: []TokenType{SELECT, ICONST, EOF},
			values:   []string{"select", "5", ""},
		},
		{
			name:     "nested multi-line comments",
			input:    "SELECT /* outer /* inner */ still outer */ 5",
			expected: []TokenType{SELECT, ICONST, EOF},
			values:   []string{"select", "5", ""},
		},
		{
			name:     "deeply nested comments",
			input:    "SELECT /* 1 /* 2 /* 3 */ 2 */ 1 */ 5",
			expected: []TokenType{SELECT, ICONST, EOF},
			values:   []string{"select", "5", ""},
		},
		{
			name:     "comment with special content",
			input:    "SELECT /* -- not a line comment */ 5",
			expected: []TokenType{SELECT, ICONST, EOF},
			values:   []string{"select", "5", ""},
		},
		{
			name:        "unterminated comment",
			input:       "SELECT /* unterminated",
			expected:    []TokenType{SELECT},
			values:      []string{"select"},
			expectError: true,
			errorMsg:    "unterminated /* comment",
		},
		{
			name:        "unterminated nested comment",
			input:       "SELECT /* outer /* inner */",
			expected:    []TokenType{SELECT},
			values:      []string{"select"},
			expectError: true,
			errorMsg:    "unterminated /* comment",
		},
		{
			name:     "adjacent comments",
			input:    "SELECT /*a*//*b*/ 5",
			expected: []TokenType{SELECT, ICONST, EOF},
			values:   []string{"select", "5", ""},
		},
		{
			name:     "comment in operator",
			input:    "5+/*comment*/3",
			expected: []TokenType{ICONST, TokenType('+'), ICONST, EOF},
			values:   []string{"5", "+", "3", ""},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lexer := NewLexer(test.input)
			tokens := []Token{}

			for {
				token := lexer.NextToken()
				if test.expectError && lexer.HasErrors() {
					errors := lexer.GetErrors()
					assert.Contains(t, errors[0].Error(), test.errorMsg)
					break
				}
				require.NotNil(t, token)

				tokens = append(tokens, *token)
				if token.Type == EOF {
					break
				}
			}

			require.Equal(t, len(test.expected), len(tokens))
			for i, token := range tokens {
				assert.Equal(t, test.expected[i], token.Type, "Token %d type mismatch", i)
				assert.Equal(t, test.values[i], token.Value.Str, "Token %d value mismatch", i)
			}
		})
	}
}

// TestCommentsWithOperators tests comment detection in operators
func TestCommentsWithOperators(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []TokenType
		values   []string
	}{
		{
			name:     "operator with embedded /*",
			input:    "5+/*comment*/3",
			expected: []TokenType{ICONST, TokenType('+'), ICONST, EOF},
			values:   []string{"5", "+", "3", ""},
		},
		{
			name:     "operator with embedded --",
			input:    "5+--comment\n3",
			expected: []TokenType{ICONST, TokenType('+'), ICONST, EOF},
			values:   []string{"5", "+", "3", ""},
		},
		{
			name:     "complex operator with comment",
			input:    "a::/*type cast*/int",
			expected: []TokenType{IDENT, TYPECAST, IDENT, EOF},
			values:   []string{"a", "::", "int", ""},
		},
		{
			name:     "operator sequence interrupted by comment",
			input:    "a</*cmt*/=b",
			expected: []TokenType{IDENT, TokenType('<'), TokenType('='), IDENT, EOF},
			values:   []string{"a", "<", "=", "b", ""},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lexer := NewLexer(test.input)
			tokens := ScanAllTokens(t, lexer)

			require.Equal(t, len(test.expected), len(tokens))
			for i, token := range tokens {
				assert.Equal(t, test.expected[i], token.Type, "Token %d type mismatch", i)
				assert.Equal(t, test.values[i], token.Value.Str, "Token %d value mismatch", i)
			}
		})
	}
}

// TestCommentPositionTracking tests that position tracking works correctly with comments
func TestCommentPositionTracking(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		positions []int // Expected positions for non-EOF tokens
	}{
		{
			name:      "single-line comment position",
			input:     "A -- comment\nB",
			positions: []int{0, 13}, // A at 0, B at 13 (after newline)
		},
		{
			name:      "multi-line comment position",
			input:     "A /* multi\nline */ B",
			positions: []int{0, 19}, // A at 0, B at 19
		},
		{
			name:      "nested comment position",
			input:     "A /* outer /* inner */ */ B",
			positions: []int{0, 26}, // A at 0, B at 26
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lexer := NewLexer(test.input)
			tokens := ScanAllTokens(t, lexer)

			// Filter out EOF token
			nonEOFTokens := []Token{}
			for _, tok := range tokens {
				if tok.Type != EOF {
					nonEOFTokens = append(nonEOFTokens, tok)
				}
			}

			require.Equal(t, len(test.positions), len(nonEOFTokens))
			for i, token := range nonEOFTokens {
				assert.Equal(t, test.positions[i], token.Position, "Token %d position mismatch", i)
			}
		})
	}
}

// TestCommentEdgeCases tests edge cases and special scenarios
func TestCommentEdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []TokenType
		values   []string
	}{
		{
			name:     "slash not starting comment",
			input:    "5/3",
			expected: []TokenType{ICONST, TokenType('/'), ICONST, EOF},
			values:   []string{"5", "/", "3", ""},
		},
		{
			name:     "single dash",
			input:    "5-3",
			expected: []TokenType{ICONST, TokenType('-'), ICONST, EOF},
			values:   []string{"5", "-", "3", ""},
		},
		{
			name:     "star slash outside comment",
			input:    "a */ b",
			expected: []TokenType{IDENT, TokenType('*'), TokenType('/'), IDENT, EOF},
			values:   []string{"a", "*", "/", "b", ""},
		},
		{
			name:     "comment-like content in string",
			input:    "'/* not a comment */'",
			expected: []TokenType{SCONST, EOF},
			values:   []string{"/* not a comment */", ""},
		},
		{
			name:     "empty comment",
			input:    "A /**/B",
			expected: []TokenType{IDENT, IDENT, EOF},
			values:   []string{"a", "b", ""},
		},
		{
			name:     "whitespace in comments",
			input:    "A /* \t\n\r\f\v */ B",
			expected: []TokenType{IDENT, IDENT, EOF},
			values:   []string{"a", "b", ""},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lexer := NewLexer(test.input)
			tokens := ScanAllTokens(t, lexer)

			require.Equal(t, len(test.expected), len(tokens))
			for i, token := range tokens {
				assert.Equal(t, test.expected[i], token.Type, "Token %d type mismatch", i)
				assert.Equal(t, test.values[i], token.Value.Str, "Token %d value mismatch", i)
			}
		})
	}
}

