// Copyright 2025 Supabase, Inc.
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

/*
PostgreSQL Parser Lexer - Delimited Identifier Tests

This file tests the delimited identifier functionality of the PostgreSQL-compatible lexer.
Tests double-quoted identifiers with proper escaping and case preservation.
*/

package parser

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDelimitedIdentifiers tests basic delimited identifier handling
func TestDelimitedIdentifiers(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []TokenType
		values   []string
	}{
		{
			name:     "simple delimited identifier",
			input:    `"identifier"`,
			expected: []TokenType{IDENT, EOF},
			values:   []string{"identifier", ""},
		},
		{
			name:     "case preservation",
			input:    `"MixedCase"`,
			expected: []TokenType{IDENT, EOF},
			values:   []string{"MixedCase", ""},
		},
		{
			name:     "with spaces",
			input:    `"table name"`,
			expected: []TokenType{IDENT, EOF},
			values:   []string{"table name", ""},
		},
		{
			name:     "with special characters",
			input:    `"table-name.with$special@chars!"`,
			expected: []TokenType{IDENT, EOF},
			values:   []string{"table-name.with$special@chars!", ""},
		},
		{
			name:     "keywords as identifiers",
			input:    `"SELECT" "FROM" "WHERE"`,
			expected: []TokenType{IDENT, IDENT, IDENT, EOF},
			values:   []string{"SELECT", "FROM", "WHERE", ""},
		},
		{
			name:     "numbers in identifier",
			input:    `"123table" "col456"`,
			expected: []TokenType{IDENT, IDENT, EOF},
			values:   []string{"123table", "col456", ""},
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

// TestDelimitedIdentifierEscaping tests quote escaping in delimited identifiers
func TestDelimitedIdentifierEscaping(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []TokenType
		values   []string
	}{
		{
			name:     "escaped double quote",
			input:    `"tab""le"`,
			expected: []TokenType{IDENT, EOF},
			values:   []string{`tab"le`, ""},
		},
		{
			name:     "multiple escaped quotes",
			input:    `"a""b""c"`,
			expected: []TokenType{IDENT, EOF},
			values:   []string{`a"b"c`, ""},
		},
		{
			name:     "escaped quote at start",
			input:    `"""table"`,
			expected: []TokenType{IDENT, EOF},
			values:   []string{`"table`, ""},
		},
		{
			name:     "escaped quote at end",
			input:    `"table"""`,
			expected: []TokenType{IDENT, EOF},
			values:   []string{`table"`, ""},
		},
		{
			name:     "only escaped quotes",
			input:    `""""""`,
			expected: []TokenType{IDENT, EOF},
			values:   []string{`""`, ""},
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

// TestDelimitedIdentifierErrors tests error handling for delimited identifiers
func TestDelimitedIdentifierErrors(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "unterminated identifier",
			input:       `"unterminated`,
			expectError: true,
			errorMsg:    "unterminated quoted identifier",
		},
		{
			name:        "unterminated with escape",
			input:       `"unterminated""`,
			expectError: true,
			errorMsg:    "unterminated quoted identifier",
		},
		{
			name:        "zero-length at EOF",
			input:       `""`,
			expectError: true,
			errorMsg:    "zero-length delimited identifier",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lexer := NewLexer(test.input)

			for {
				token := lexer.NextToken()
				if test.expectError && lexer.HasErrors() {
					errors := lexer.GetErrors()
					if len(errors) > 0 {
						assert.Contains(t, errors[0].Error(), test.errorMsg)
					}
					break
				}

				if token != nil && token.Type == EOF {
					if test.expectError {
						t.Errorf("Expected error but got EOF")
					}
					break
				}
			}
		})
	}
}

// TestDelimitedIdentifierMultiline tests multiline delimited identifiers
func TestDelimitedIdentifierMultiline(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []TokenType
		values   []string
	}{
		{
			name:     "identifier with newline",
			input:    "\"multi\nline\"",
			expected: []TokenType{IDENT, EOF},
			values:   []string{"multi\nline", ""},
		},
		{
			name:     "identifier with carriage return",
			input:    "\"multi\rline\"",
			expected: []TokenType{IDENT, EOF},
			values:   []string{"multi\nline", ""}, // \r is normalized to \n
		},
		{
			name:     "identifier with CRLF",
			input:    "\"multi\r\nline\"",
			expected: []TokenType{IDENT, EOF},
			values:   []string{"multi\nline", ""},
		},
		{
			name:     "multiple newlines",
			input:    "\"line1\nline2\nline3\"",
			expected: []TokenType{IDENT, EOF},
			values:   []string{"line1\nline2\nline3", ""},
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

// TestDelimitedIdentifierTruncation tests identifier truncation at NAMEDATALEN
func TestDelimitedIdentifierTruncation(t *testing.T) {
	// Create a long identifier (longer than NAMEDATALEN-1 = 63)
	longName := strings.Repeat("a", 100)
	truncatedName := strings.Repeat("a", 63) // NAMEDATALEN-1

	tests := []struct {
		name     string
		input    string
		expected []TokenType
		values   []string
	}{
		{
			name:     "long identifier truncated",
			input:    `"` + longName + `"`,
			expected: []TokenType{IDENT, EOF},
			values:   []string{truncatedName, ""},
		},
		{
			name:     "exactly 63 chars not truncated",
			input:    `"` + truncatedName + `"`,
			expected: []TokenType{IDENT, EOF},
			values:   []string{truncatedName, ""},
		},
		{
			name:     "62 chars not truncated",
			input:    `"` + strings.Repeat("b", 62) + `"`,
			expected: []TokenType{IDENT, EOF},
			values:   []string{strings.Repeat("b", 62), ""},
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

// TestUnicodeIdentifiers tests Unicode-escaped identifiers (U&"...")
func TestUnicodeIdentifiers(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []TokenType
		values   []string
	}{
		{
			name:     "basic Unicode identifier",
			input:    `U&"identifier"`,
			expected: []TokenType{IDENT, EOF}, // PostgreSQL converts UIDENT→IDENT
			values:   []string{"identifier", ""},
		},
		{
			name:     "lowercase u prefix",
			input:    `u&"identifier"`,
			expected: []TokenType{IDENT, EOF}, // PostgreSQL converts UIDENT→IDENT
			values:   []string{"identifier", ""},
		},
		{
			name:     "Unicode identifier with escapes",
			input:    `U&"test""quote"`,
			expected: []TokenType{IDENT, EOF}, // PostgreSQL converts UIDENT→IDENT
			values:   []string{`test"quote`, ""},
		},
		{
			name:     "empty Unicode identifier in context",
			input:    `SELECT U&""`,
			expected: []TokenType{SELECT, IDENT, EOF}, // PostgreSQL converts UIDENT→IDENT
			values:   []string{"select", "", ""},
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

// TestDelimitedIdentifierContext tests delimited identifiers in SQL context
func TestDelimitedIdentifierContext(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []TokenType
		values   []string
	}{
		{
			name:     "in SELECT statement",
			input:    `SELECT "Column Name" FROM "Table Name"`,
			expected: []TokenType{SELECT, IDENT, FROM, IDENT, EOF},
			values:   []string{"select", "Column Name", "from", "Table Name", ""},
		},
		{
			name:     "with operators",
			input:    `"col1" + "col2"`,
			expected: []TokenType{IDENT, TokenType('+'), IDENT, EOF},
			values:   []string{"col1", "+", "col2", ""},
		},
		{
			name:     "with type cast",
			input:    `"value"::"type"`,
			expected: []TokenType{IDENT, TYPECAST, IDENT, EOF},
			values:   []string{"value", "::", "type", ""},
		},
		{
			name:     "adjacent identifiers",
			input:    `"a""b" "c"`,
			expected: []TokenType{IDENT, IDENT, EOF},
			values:   []string{`a"b`, "c", ""},
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

// TestIdentifierComparison tests the difference between regular and delimited identifiers
func TestIdentifierComparison(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []TokenType
		values   []string
	}{
		{
			name:     "regular identifier lowercased",
			input:    `MixedCase`,
			expected: []TokenType{IDENT, EOF},
			values:   []string{"mixedcase", ""},
		},
		{
			name:     "delimited identifier case preserved",
			input:    `"MixedCase"`,
			expected: []TokenType{IDENT, EOF},
			values:   []string{"MixedCase", ""},
		},
		{
			name:     "keyword as regular identifier",
			input:    `SELECT`,
			expected: []TokenType{SELECT, EOF},
			values:   []string{"select", ""},
		},
		{
			name:     "keyword as delimited identifier",
			input:    `"SELECT"`,
			expected: []TokenType{IDENT, EOF},
			values:   []string{"SELECT", ""},
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
