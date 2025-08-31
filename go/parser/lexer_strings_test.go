/*
 * PostgreSQL Parser Lexer - String Literal System Tests
 *
 * This file provides comprehensive test coverage for PostgreSQL's string
 * literal handling, including standard strings, extended strings, dollar-quoted
 * strings, escape sequences, and string concatenation.
 * Tests verify 100% compatibility with PostgreSQL string processing.
 *
 * Consolidated from strings_test.go and string-related tests from lexer_test.go
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

// TestSpecialLiteralRecognition tests hex, bit, and extended string literal recognition
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
				{BETWEEN, "BETWEEN"}, // keyword
				{IDENT, "b"},         // identifier (no quote following)
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

// TestCharacterDispatchOrdering tests edge cases for character dispatch ordering
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

// TestStringLiteralContentExtraction tests proper string literal content extraction with quote doubling
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

// TestBitStringContentExtraction tests bit string literal content extraction
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

// TestHexStringContentExtraction tests hex string literal content extraction
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

// TestExtendedStringContentExtraction tests extended string literal content extraction
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

// TestStringLiteralsInSQLContext tests string literals in complex SQL context
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
		{DATA_P, "data", "", true},                 // identifier - skip value check for now
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