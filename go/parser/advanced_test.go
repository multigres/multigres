/*
PostgreSQL Parser Lexer - Advanced Feature Tests

This file tests advanced features of the PostgreSQL-compatible lexer including
parameter placeholders ($1, $2) and type cast operators (::).
*/

package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
			expected: []TokenType{IDENT, TYPECAST, IDENT, EOF},
			values:   []string{"value", "::", "int", ""},
		},
		{
			name:     "type cast with spaces",
			input:    "value :: int",
			expected: []TokenType{IDENT, TYPECAST, IDENT, EOF},
			values:   []string{"value", "::", "int", ""},
		},
		{
			name:     "chained type casts",
			input:    "value::text::int",
			expected: []TokenType{IDENT, TYPECAST, IDENT, TYPECAST, IDENT, EOF},
			values:   []string{"value", "::", "text", "::", "int", ""},
		},
		{
			name:     "type cast with complex type",
			input:    "value::numeric(10,2)",
			expected: []TokenType{IDENT, TYPECAST, IDENT, TokenType('('), ICONST, TokenType(','), ICONST, TokenType(')'), EOF},
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
			expected: []TokenType{IDENT, TYPECAST, IDENT, EOF},
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
