/*
 * PostgreSQL Parser Lexer - String Literal System Tests
 *
 * This file provides comprehensive test coverage for PostgreSQL's string
 * literal handling, including standard strings, extended strings, dollar-quoted
 * strings, escape sequences, and string concatenation.
 * Tests verify 100% compatibility with PostgreSQL string processing.
 */

package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStandardStrings tests standard SQL string literal parsing
func TestStandardStrings(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    string
		tokenType   TokenType
		shouldError bool
	}{
		{
			name:      "Simple string",
			input:     "'hello'",
			expected:  "hello",
			tokenType: SCONST,
		},
		{
			name:      "Empty string",
			input:     "''",
			expected:  "",
			tokenType: SCONST,
		},
		{
			name:      "String with quote doubling",
			input:     "'don''t'",
			expected:  "don't",
			tokenType: SCONST,
		},
		{
			name:      "String with multiple quote doubling",
			input:     "'He said ''Hello'' and ''Goodbye'''",
			expected:  "He said 'Hello' and 'Goodbye'",
			tokenType: SCONST,
		},
		{
			name:      "Multi-line string",
			input:     "'line1\nline2'",
			expected:  "line1\nline2",
			tokenType: SCONST,
		},
		{
			name:        "Unterminated string",
			input:       "'unterminated",
			expected:    "unterminated",
			tokenType:   USCONST,
			shouldError: false, // Returns USCONST token, doesn't error
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token := lexer.NextToken()

			if tt.shouldError {
				assert.True(t, lexer.context.HasErrors())
				return
			}

			require.NotNil(t, token)
			assert.Equal(t, tt.tokenType, token.Type)
			assert.Equal(t, tt.expected, token.Value.Str)
		})
	}
}

// TestExtendedStrings tests extended string literal parsing with escape sequences
func TestExtendedStrings(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expected  string
		tokenType TokenType
	}{
		{
			name:      "Simple extended string",
			input:     "E'hello'",
			expected:  "hello",
			tokenType: SCONST,
		},
		{
			name:      "Extended string with backslash escape",
			input:     "E'hello\\nworld'",
			expected:  "hello\nworld",
			tokenType: SCONST,
		},
		{
			name:      "Extended string with tab escape",
			input:     "E'hello\\tworld'",
			expected:  "hello\tworld",
			tokenType: SCONST,
		},
		{
			name:      "Extended string with all basic escapes",
			input:     "E'\\b\\f\\n\\r\\t\\v\\\\\\'\\\"'",
			expected:  "\b\f\n\r\t\v\\'\"",
			tokenType: SCONST,
		},
		{
			name:      "Extended string with octal escape",
			input:     "E'\\101\\102\\103'", // ABC in octal
			expected:  "ABC",
			tokenType: SCONST,
		},
		{
			name:      "Extended string with hex escape",
			input:     "E'\\x41\\x42\\x43'", // ABC in hex
			expected:  "ABC",
			tokenType: SCONST,
		},
		{
			name:      "Extended string with Unicode escape",
			input:     "E'\\u0048\\u0065\\u006C\\u006C\\u006F'", // Hello in Unicode
			expected:  "Hello",
			tokenType: SCONST,
		},
		{
			name:      "Extended string with long Unicode escape",
			input:     "E'\\U00000048\\U00000065\\U0000006C\\U0000006C\\U0000006F'", // Hello in long Unicode
			expected:  "Hello",
			tokenType: SCONST,
		},
		{
			name:      "Case insensitive E prefix",
			input:     "e'hello\\nworld'",
			expected:  "hello\nworld",
			tokenType: SCONST,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token := lexer.NextToken()

			require.NotNil(t, token)
			assert.Equal(t, tt.tokenType, token.Type)
			assert.Equal(t, tt.expected, token.Value.Str)
		})
	}
}

// TestDollarQuotedStrings tests dollar-quoted string literal parsing
func TestDollarQuotedStrings(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expected  string
		tokenType TokenType
	}{
		{
			name:      "Simple dollar-quoted string",
			input:     "$$hello$$",
			expected:  "hello",
			tokenType: SCONST,
		},
		{
			name:      "Empty dollar-quoted string",
			input:     "$$$$",
			expected:  "",
			tokenType: SCONST,
		},
		{
			name:      "Dollar-quoted string with tag",
			input:     "$tag$hello$tag$",
			expected:  "hello",
			tokenType: SCONST,
		},
		{
			name:      "Dollar-quoted string with complex tag",
			input:     "$func_body$SELECT * FROM table$func_body$",
			expected:  "SELECT * FROM table",
			tokenType: SCONST,
		},
		{
			name:      "Dollar-quoted string with quotes inside",
			input:     "$$don't 'quote' \"me\"$$",
			expected:  "don't 'quote' \"me\"",
			tokenType: SCONST,
		},
		{
			name:      "Dollar-quoted string with backslashes",
			input:     "$$\\n\\t\\r$$",
			expected:  "\\n\\t\\r", // No escape processing in dollar-quoted strings
			tokenType: SCONST,
		},
		{
			name:      "Dollar-quoted string with embedded dollars",
			input:     "$tag$price: $10.50$tag$",
			expected:  "price: $10.50",
			tokenType: SCONST,
		},
		{
			name:      "Dollar-quoted string with newlines",
			input:     "$sql$\nSELECT *\nFROM table\n$sql$",
			expected:  "\nSELECT *\nFROM table\n",
			tokenType: SCONST,
		},
		{
			name:      "Dollar-quoted string with nested similar tags",
			input:     "$a$content $aa$ more content$a$",
			expected:  "content $aa$ more content",
			tokenType: SCONST,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token := lexer.NextToken()

			require.NotNil(t, token)
			assert.Equal(t, tt.tokenType, token.Type)
			assert.Equal(t, tt.expected, token.Value.Str)
		})
	}
}

// TestBitStrings tests bit string literal parsing
func TestBitStrings(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expected  string
		tokenType TokenType
	}{
		{
			name:      "Simple bit string",
			input:     "B'101010'",
			expected:  "101010",
			tokenType: BCONST,
		},
		{
			name:      "Empty bit string",
			input:     "B''",
			expected:  "",
			tokenType: BCONST,
		},
		{
			name:      "Bit string with whitespace",
			input:     "B'1010 1010'",
			expected:  "10101010", // Whitespace should be skipped
			tokenType: BCONST,
		},
		{
			name:      "Case insensitive B prefix",
			input:     "b'101010'",
			expected:  "101010",
			tokenType: BCONST,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token := lexer.NextToken()

			require.NotNil(t, token)
			assert.Equal(t, tt.tokenType, token.Type)
			assert.Equal(t, tt.expected, token.Value.Str)
		})
	}
}

// TestHexStrings tests hexadecimal string literal parsing
func TestHexStrings(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expected  string
		tokenType TokenType
	}{
		{
			name:      "Simple hex string",
			input:     "X'deadbeef'",
			expected:  "deadbeef",
			tokenType: XCONST,
		},
		{
			name:      "Empty hex string",
			input:     "X''",
			expected:  "",
			tokenType: XCONST,
		},
		{
			name:      "Hex string with whitespace",
			input:     "X'dead beef'",
			expected:  "deadbeef", // Whitespace should be skipped
			tokenType: XCONST,
		},
		{
			name:      "Mixed case hex string",
			input:     "X'DeAdBeEf'",
			expected:  "DeAdBeEf",
			tokenType: XCONST,
		},
		{
			name:      "Case insensitive X prefix",
			input:     "x'deadbeef'",
			expected:  "deadbeef",
			tokenType: XCONST,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token := lexer.NextToken()

			require.NotNil(t, token)
			assert.Equal(t, tt.tokenType, token.Type)
			assert.Equal(t, tt.expected, token.Value.Str)
		})
	}
}

// TestStringConcatenation tests string concatenation across whitespace
// NOTE: Updated to match PostgreSQL behavior - concatenation requires newline
func TestStringConcatenation(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expected  string
		tokenType TokenType
	}{
		{
			name:      "Two simple strings without newline - no concatenation",
			input:     "'hello' 'world'",
			expected:  "hello", // PostgreSQL: no newline = no concatenation
			tokenType: SCONST,
		},
		{
			name:      "Three strings with spaces only - no concatenation",
			input:     "'hello'   'beautiful'   'world'",
			expected:  "hello", // PostgreSQL: no newline = no concatenation
			tokenType: SCONST,
		},
		{
			name:      "Strings with newlines",
			input:     "'hello'\n'world'",
			expected:  "helloworld",
			tokenType: SCONST,
		},
		{
			name:      "Extended string concatenation without newline - no concatenation",
			input:     "E'hello\\n' 'world'",
			expected:  "hello\n", // PostgreSQL: no newline = no concatenation
			tokenType: SCONST,
		},
		{
			name:      "Mixed string types without newline - no concatenation",
			input:     "'hello' E'\\nworld'",
			expected:  "hello", // PostgreSQL: no newline = no concatenation
			tokenType: SCONST,
		},
		{
			name:      "Extended string concatenation with newline",
			input:     "E'hello\\n'\n'world'",
			expected:  "hello\nworld",
			tokenType: SCONST,
		},
		{
			name:      "Mixed string types with newline",
			input:     "'hello'\nE'\\nworld'",
			expected:  "hello\nworld",
			tokenType: SCONST,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token := lexer.NextToken()

			require.NotNil(t, token)
			assert.Equal(t, tt.tokenType, token.Type)
			assert.Equal(t, tt.expected, token.Value.Str)
		})
	}
}

// TestStringErrors tests error cases in string processing
func TestStringErrors(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		tokenType TokenType
	}{
		{
			name:      "Unterminated standard string",
			input:     "'hello",
			tokenType: USCONST,
		},
		{
			name:      "Unterminated extended string",
			input:     "E'hello",
			tokenType: SCONST, // Extended strings return SCONST when unterminated
		},
		{
			name:      "Unterminated bit string",
			input:     "B'101",
			tokenType: BCONST, // Bit strings return BCONST when unterminated
		},
		{
			name:      "Unterminated hex string",
			input:     "X'dead",
			tokenType: XCONST, // Hex strings return XCONST when unterminated
		},
		{
			name:      "Unterminated dollar-quoted string",
			input:     "$tag$hello",
			tokenType: USCONST,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token := lexer.NextToken()

			require.NotNil(t, token) // Errors are captured in token, not returned
			assert.Equal(t, tt.tokenType, token.Type)
			assert.True(t, len(lexer.context.Errors()) > 0, "Expected error to be recorded")
		})
	}
}

// TestDollarTokens tests parameter tokens vs dollar-quoted strings
func TestDollarTokens(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		expectedType  TokenType
		expectedValue interface{}
	}{
		{
			name:          "Parameter token $1",
			input:         "$1",
			expectedType:  PARAM,
			expectedValue: 1,
		},
		{
			name:          "Parameter token $42",
			input:         "$42",
			expectedType:  PARAM,
			expectedValue: 42,
		},
		{
			name:          "Dollar-quoted string $$",
			input:         "$$hello$$",
			expectedType:  SCONST,
			expectedValue: "hello",
		},
		{
			name:          "Dollar-quoted string with tag",
			input:         "$tag$hello$tag$",
			expectedType:  SCONST,
			expectedValue: "hello",
		},
		{
			name:          "Single dollar as operator",
			input:         "$",
			expectedType:  Op,
			expectedValue: "$",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token := lexer.NextToken()

			require.NotNil(t, token)
			assert.Equal(t, tt.expectedType, token.Type)

			switch tt.expectedType {
			case PARAM:
				assert.Equal(t, tt.expectedValue, token.Value.Ival)
			case SCONST, Op:
				assert.Equal(t, tt.expectedValue, token.Value.Str)
			}
		})
	}
}

// TestComplexStringScenarios tests real-world complex string scenarios
func TestComplexStringScenarios(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "SQL function body",
			input:    "$body$SELECT 'Hello' || ' ' || 'World'$body$",
			expected: "SELECT 'Hello' || ' ' || 'World'",
		},
		{
			name:     "JSON string with escapes",
			input:    `E'{"name": "John", "age": 30, "city": "New York"}'`,
			expected: `{"name": "John", "age": 30, "city": "New York"}`,
		},
		{
			name:     "Regular expression",
			input:    `E'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'`,
			expected: `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`,
		},
		{
			name:     "Multi-line SQL with quotes",
			input:    "$sql$\nSELECT 'user''s name' AS name,\n       'don''t quote me' AS quote\nFROM users\n$sql$",
			expected: "\nSELECT 'user''s name' AS name,\n       'don''t quote me' AS quote\nFROM users\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token := lexer.NextToken()

			require.NotNil(t, token)
			assert.Equal(t, SCONST, token.Type)
			assert.Equal(t, tt.expected, token.Value.Str)
		})
	}
}

// TestStringProcessingDirectly tests the string processing functions directly
func TestStringProcessingDirectly(t *testing.T) {
	lexer := NewLexer("")

	// Test individual escape sequence processing
	lexer.context = NewLexerContext("E'\\n'")
	token, _ := lexer.scanExtendedString(0, 0)
	require.NotNil(t, token)
	assert.Equal(t, "\n", token.Value.Str)

	// Test dollar delimiter parsing
	lexer.context = NewLexerContext("$tag$hello$tag$")
	token, _ = lexer.scanDollarQuotedString(0, 0)
	require.NotNil(t, token)
	assert.Equal(t, "hello", token.Value.Str)
}
