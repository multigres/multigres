/*
 * PostgreSQL Numeric Literal Lexer Tests
 *
 * This file contains comprehensive tests for numeric literal lexing
 * including integers, floating-point numbers, and various numeric formats.
 * Tests match PostgreSQL behavior from postgres/src/backend/parser/scan.l
 */

package lexer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDecimalIntegers tests decimal integer literal scanning
// postgres/src/backend/parser/scan.l:400, 1018-1020
func TestDecimalIntegers(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		token    TokenType
	}{
		// Basic decimal integers
		{"single digit", "0", "0", ICONST},
		{"simple integer", "123", "123", ICONST},
		{"large integer", "9876543210", "9876543210", ICONST},
		
		// With underscores (PostgreSQL 15+ feature)
		{"underscore separator", "1_000_000", "1_000_000", ICONST},
		{"multiple underscores", "123_456_789", "123_456_789", ICONST},
		{"underscore between digits", "1_2_3", "1_2_3", ICONST},
		
		// Edge cases
		{"max int32", "2147483647", "2147483647", ICONST},
		{"overflow to bigint", "2147483648", "2147483648", ICONST},
		{"very large number", "99999999999999999999", "99999999999999999999", ICONST},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token, err := lexer.NextToken()
			
			require.NoError(t, err)
			assert.Equal(t, tt.token, token.Type)
			assert.Equal(t, tt.expected, token.Value.Str)
		})
	}
}

// TestHexadecimalIntegers tests hexadecimal integer literals
// postgres/src/backend/parser/scan.l:401, 1022-1024
func TestHexadecimalIntegers(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		token    TokenType
	}{
		// Basic hex integers
		{"simple hex lowercase", "0x1a", "0x1a", ICONST},
		{"simple hex uppercase", "0X1A", "0X1A", ICONST},
		{"mixed case prefix", "0xABCD", "0xABCD", ICONST},
		{"mixed case digits", "0xaBcD", "0xaBcD", ICONST},
		
		// With underscores
		{"hex with underscores", "0x1234_5678", "0x1234_5678", ICONST},
		{"hex multiple underscores", "0xFF_FF_FF_FF", "0xFF_FF_FF_FF", ICONST},
		
		// Large values
		{"max 32-bit hex", "0xFFFFFFFF", "0xFFFFFFFF", ICONST},
		{"64-bit hex", "0xFFFFFFFFFFFFFFFF", "0xFFFFFFFFFFFFFFFF", ICONST},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token, err := lexer.NextToken()
			
			require.NoError(t, err)
			assert.Equal(t, tt.token, token.Type)
			assert.Equal(t, tt.expected, token.Value.Str)
		})
	}
}

// TestOctalIntegers tests octal integer literals
// postgres/src/backend/parser/scan.l:402, 1026-1028
func TestOctalIntegers(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		token    TokenType
	}{
		// Basic octal integers
		{"simple octal lowercase", "0o777", "0o777", ICONST},
		{"simple octal uppercase", "0O777", "0O777", ICONST},
		{"octal zero", "0o0", "0o0", ICONST},
		{"octal with valid digits", "0o1234567", "0o1234567", ICONST},
		
		// With underscores
		{"octal with underscores", "0o123_456", "0o123_456", ICONST},
		{"octal multiple underscores", "0o7_7_7", "0o7_7_7", ICONST},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token, err := lexer.NextToken()
			
			require.NoError(t, err)
			assert.Equal(t, tt.token, token.Type)
			assert.Equal(t, tt.expected, token.Value.Str)
		})
	}
}

// TestBinaryIntegers tests binary integer literals
// postgres/src/backend/parser/scan.l:403, 1030-1032
func TestBinaryIntegers(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		token    TokenType
	}{
		// Basic binary integers
		{"simple binary lowercase", "0b101010", "0b101010", ICONST},
		{"simple binary uppercase", "0B101010", "0B101010", ICONST},
		{"binary zero", "0b0", "0b0", ICONST},
		{"binary one", "0b1", "0b1", ICONST},
		{"binary alternating", "0b10101010", "0b10101010", ICONST},
		
		// With underscores
		{"binary with underscores", "0b1010_1010", "0b1010_1010", ICONST},
		{"binary byte boundaries", "0b11111111_00000000", "0b11111111_00000000", ICONST},
		{"binary nibble groups", "0b1111_0000_1111_0000", "0b1111_0000_1111_0000", ICONST},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token, err := lexer.NextToken()
			
			require.NoError(t, err)
			assert.Equal(t, tt.token, token.Type)
			assert.Equal(t, tt.expected, token.Value.Str)
		})
	}
}

// TestFloatingPointNumbers tests floating-point literal scanning
// postgres/src/backend/parser/scan.l:409, 1046-1050
func TestFloatingPointNumbers(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		token    TokenType
	}{
		// Basic floats
		{"simple float", "3.14", "3.14", FCONST},
		{"zero decimal", "0.0", "0.0", FCONST},
		{"no leading zero", ".5", ".5", FCONST},
		{"no trailing digits", "42.", "42.", FCONST},
		{"many decimals", "3.14159265358979", "3.14159265358979", FCONST},
		
		// With underscores
		{"float with underscores", "1_234.567_890", "1_234.567_890", FCONST},
		{"underscore in fraction", "3.141_592_653", "3.141_592_653", FCONST},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token, err := lexer.NextToken()
			
			require.NoError(t, err)
			assert.Equal(t, tt.token, token.Type)
			assert.Equal(t, tt.expected, token.Value.Str)
		})
	}
}

// TestScientificNotation tests scientific notation (E notation)
// postgres/src/backend/parser/scan.l:412, 1057-1060
func TestScientificNotation(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		token    TokenType
	}{
		// Integer base with exponent
		{"integer E notation", "1E10", "1E10", FCONST},
		{"integer e notation", "1e10", "1e10", FCONST},
		{"with positive sign", "1E+10", "1E+10", FCONST},
		{"with negative sign", "1E-10", "1E-10", FCONST},
		
		// Float base with exponent
		{"float E notation", "3.14E10", "3.14E10", FCONST},
		{"float negative exp", "3.14e-10", "3.14e-10", FCONST},
		{"decimal only E", ".5E10", ".5E10", FCONST},
		
		// With underscores
		{"underscore in mantissa", "1_234E10", "1_234E10", FCONST},
		{"underscore in exponent", "1.23E1_0", "1.23E1_0", FCONST},
		{"multiple underscores", "1_234.567_890E1_00", "1_234.567_890E1_00", FCONST},
		
		// Edge cases
		{"zero exponent", "1E0", "1E0", FCONST},
		{"large exponent", "1E308", "1E308", FCONST},
		{"negative large exp", "1E-308", "1E-308", FCONST},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token, err := lexer.NextToken()
			
			require.NoError(t, err)
			assert.Equal(t, tt.token, token.Type)
			assert.Equal(t, tt.expected, token.Value.Str)
		})
	}
}

// TestNumericFailPatterns tests error cases for numeric literals
// postgres/src/backend/parser/scan.l:405-407, 410, 413
func TestNumericFailPatterns(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		errorMsg    string
		tokenType   TokenType
		description string
	}{
		// hexfail: 0[xX]_?
		{"hex no digits", "0x", "invalid hexadecimal integer", ICONST, "hexfail pattern"},
		{"hex underscore only", "0x_", "invalid hexadecimal integer", ICONST, "hexfail with underscore"},
		{"hex uppercase no digits", "0X", "invalid hexadecimal integer", ICONST, "hexfail uppercase"},
		
		// octfail: 0[oO]_?
		{"octal no digits", "0o", "invalid octal integer", ICONST, "octfail pattern"},
		{"octal underscore only", "0o_", "invalid octal integer", ICONST, "octfail with underscore"},
		{"octal uppercase no digits", "0O", "invalid octal integer", ICONST, "octfail uppercase"},
		
		// binfail: 0[bB]_?
		{"binary no digits", "0b", "invalid binary integer", ICONST, "binfail pattern"},
		{"binary underscore only", "0b_", "invalid binary integer", ICONST, "binfail with underscore"},
		{"binary uppercase no digits", "0B", "invalid binary integer", ICONST, "binfail uppercase"},
		
		// realfail: ({decinteger}|{numeric})[Ee][-+]
		{"exponent no digits", "1E", "trailing junk after numeric literal", FCONST, "realfail pattern"},
		{"exponent plus no digits", "1E+", "trailing junk after numeric literal", FCONST, "realfail with plus"},
		{"exponent minus no digits", "1E-", "trailing junk after numeric literal", FCONST, "realfail with minus"},
		{"float exponent no digits", "3.14E+", "trailing junk after numeric literal", FCONST, "realfail float"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token, err := lexer.NextToken()
			
			// PostgreSQL continues lexing and returns a token even with errors
			assert.NoError(t, err)
			assert.Equal(t, tt.tokenType, token.Type, tt.description)
			
			// Check that error was recorded in context
			errors := lexer.GetContext().GetErrors()
			assert.Len(t, errors, 1)
			assert.Equal(t, tt.errorMsg, errors[0].Message)
		})
	}
}

// TestNumericJunkPatterns tests trailing junk after numeric literals
// postgres/src/backend/parser/scan.l:435-438, 1066-1076
func TestNumericJunkPatterns(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		errorMsg    string
		tokenType   TokenType
		tokenValue  string
		description string
	}{
		// integer_junk: {decinteger}{identifier}
		{"integer followed by letter", "123abc", "trailing junk after numeric literal", ICONST, "123abc", "integer_junk"},
		{"integer followed by underscore", "123_abc", "trailing junk after numeric literal", ICONST, "123_abc", "underscore not allowed here"},
		{"decimal followed by letter", "123g", "trailing junk after numeric literal", ICONST, "123g", "decimal integer_junk"},
		
		// numeric_junk: {numeric}{identifier}
		{"float followed by letter", "3.14abc", "trailing junk after numeric literal", FCONST, "3.14abc", "numeric_junk"},
		{"decimal followed by ident", "123.456xyz", "trailing junk after numeric literal", FCONST, "123.456xyz", "numeric_junk"},
		
		// real_junk: {real}{identifier}
		{"scientific followed by letter", "1E10abc", "trailing junk after numeric literal", FCONST, "1E10abc", "real_junk"},
		{"float exp followed by ident", "3.14E10xyz", "trailing junk after numeric literal", FCONST, "3.14E10xyz", "real_junk"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token, err := lexer.NextToken()
			
			// PostgreSQL continues lexing and returns a token even with errors
			assert.NoError(t, err)
			assert.Equal(t, tt.tokenType, token.Type, tt.description)
			assert.Equal(t, tt.tokenValue, token.Value.Str, tt.description)
			
			// Check that error was recorded in context
			errors := lexer.GetContext().GetErrors()
			assert.Len(t, errors, 1)
			assert.Equal(t, tt.errorMsg, errors[0].Message)
		})
	}
}

// TestNumericEdgeCases tests special edge cases and PostgreSQL compatibility
func TestNumericEdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    []TokenType
		values      []string
		description string
	}{
		// numericfail: {decinteger}\.\.
		{
			"double dot operator",
			"1..10",
			[]TokenType{ICONST, DOT_DOT, ICONST},
			[]string{"1", "..", "10"},
			"numericfail pattern - should parse as integer, dot_dot, integer",
		},
		{
			"zero double dot",
			"0..5",
			[]TokenType{ICONST, DOT_DOT, ICONST},
			[]string{"0", "..", "5"},
			"numericfail with zero",
		},
		
		// Leading zeros (not octal without 'o')
		{
			"leading zeros decimal",
			"0123",
			[]TokenType{ICONST},
			[]string{"0123"},
			"leading zeros should be decimal, not octal",
		},
		
		// Underscore restrictions - PostgreSQL parses these as single tokens with trailing junk
		{
			"underscore at end",
			"123_",
			[]TokenType{ICONST},
			[]string{"123_"},
			"trailing underscore parsed as single token with junk error",
		},
		{
			"double underscore",
			"1__2",
			[]TokenType{ICONST},
			[]string{"1__2"},
			"double underscore parsed as single token with junk error",
		},
		
		// Mixed numeric formats
		{
			"hex invalid digit",
			"0x123g456",
			[]TokenType{ICONST, IDENT},
			[]string{"0x123", "g456"},
			"invalid hex digit breaks token",
		},
		{
			"octal invalid digit",
			"0o1238",
			[]TokenType{ICONST, ICONST},
			[]string{"0o123", "8"},
			"digit 8 not valid in octal",
		},
		{
			"binary invalid digit",
			"0b1012",
			[]TokenType{ICONST, ICONST},
			[]string{"0b101", "2"},
			"digit 2 not valid in binary",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			
			for i, expectedType := range tt.expected {
				token, err := lexer.NextToken()
				require.NoError(t, err, tt.description)
				assert.Equal(t, expectedType, token.Type, "token %d: %s", i, tt.description)
				if i < len(tt.values) {
					assert.Equal(t, tt.values[i], token.Value.Str, "value %d: %s", i, tt.description)
				}
			}
			
			// Verify we've consumed all input
			token, err := lexer.NextToken()
			require.NoError(t, err)
			assert.Equal(t, EOF, token.Type, "expected EOF after all tokens")
		})
	}
}

// TestNumericUnderscoreRules tests PostgreSQL underscore rules in numeric literals
func TestNumericUnderscoreRules(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		shouldParse bool
		expected    string
		description string
	}{
		// Valid underscore usage
		{"decimal with underscores", "1_234_567", true, "1_234_567", "valid decimal grouping"},
		{"hex with underscores", "0xFF_FF", true, "0xFF_FF", "valid hex grouping"},
		{"float with underscores", "1_234.567_890", true, "1_234.567_890", "valid float grouping"},
		{"exponent with underscores", "1.23E4_5", true, "1.23E4_5", "valid exponent grouping"},
		
		// Invalid underscore usage - NOTE: _123 is not a numeric literal, it's an identifier
		{"leading underscore", "_123", false, "", "underscore cannot start number - should be parsed as identifier"},
		{"trailing underscore number", "123_", false, "", "underscore cannot end number"},
		{"double underscore", "12__34", false, "", "double underscore not allowed"},
		{"underscore after prefix", "0x_FF", true, "0x_FF", "underscore after prefix is valid per PostgreSQL pattern"},
		{"underscore before exponent", "1.23_E10", false, "", "underscore before E invalid"},
		{"underscore after E", "1.23E_10", false, "", "underscore directly after E invalid"},
		{"underscore after sign", "1.23E+_10", false, "", "underscore after sign invalid"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token, err := lexer.NextToken()
			
			if tt.shouldParse {
				require.NoError(t, err, tt.description)
				assert.Equal(t, tt.expected, token.Value.Str, tt.description)
				
				// Check that no errors were recorded for valid patterns
				errors := lexer.GetContext().GetErrors()
				assert.Empty(t, errors, "valid pattern should not generate errors: %s", tt.description)
			} else {
				require.NoError(t, err, tt.description)
				
				// Special case: "_123" should be parsed as IDENT, not numeric
				if tt.name == "leading underscore" {
					assert.Equal(t, IDENT, token.Type, 
						"leading underscore should parse as identifier: %s", tt.description)
					assert.Equal(t, "_123", token.Value.Str, tt.description)
					
					// Should have no errors since it's a valid identifier
					errors := lexer.GetContext().GetErrors()
					assert.Empty(t, errors, "valid identifier should not generate errors: %s", tt.description)
				} else {
					// For other invalid cases, PostgreSQL behavior is:
					// 1. Parse as numeric token but include the junk in the value
					// 2. Record an error but continue parsing
					// 3. No Go error is returned - only context error is recorded
					
					// Should still be parsed as a numeric token (with junk included)
					assert.Contains(t, []TokenType{ICONST, FCONST}, token.Type, 
						"invalid pattern should still parse as numeric token: %s", tt.description)
					
					// Should have an error recorded in context
					errors := lexer.GetContext().GetErrors()
					assert.NotEmpty(t, errors, "invalid pattern should generate context error: %s", tt.description)
					if len(errors) > 0 {
						assert.Contains(t, errors[0].Message, "junk", 
							"error should mention junk: %s", tt.description)
					}
				}
			}
		})
	}
}

// TestNumericRealWorldExamples tests real-world numeric literal examples
func TestNumericRealWorldExamples(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		token    TokenType
	}{
		// Common database values
		{"port number", "5432", "5432", ICONST},
		{"year", "2024", "2024", ICONST},
		{"price", "19.99", "19.99", FCONST},
		{"percentage", "0.15", "0.15", FCONST},
		{"scientific small", "1.23e-10", "1.23e-10", FCONST},
		{"scientific large", "6.022e23", "6.022e23", FCONST},
		
		// Binary/hex common values
		{"permissions octal", "0o755", "0o755", ICONST},
		{"RGB color hex", "0xFF0000", "0xFF0000", ICONST},
		{"bit flags", "0b11110000", "0b11110000", ICONST},
		
		// With underscores for readability
		{"million", "1_000_000", "1_000_000", ICONST},
		{"credit card", "1234_5678_9012_3456", "1234_5678_9012_3456", ICONST},
		{"binary byte", "0b1111_0000", "0b1111_0000", ICONST},
		{"hex address", "0xDEAD_BEEF", "0xDEAD_BEEF", ICONST},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token, err := lexer.NextToken()
			
			require.NoError(t, err)
			assert.Equal(t, tt.token, token.Type)
			assert.Equal(t, tt.expected, token.Value.Str)
		})
	}
}

// BenchmarkNumericLexing benchmarks numeric literal lexing performance
func BenchmarkNumericLexing(b *testing.B) {
	benchmarks := []struct {
		name  string
		input string
	}{
		{"simple integer", "12345"},
		{"integer with underscores", "1_234_567_890"},
		{"simple float", "3.14159"},
		{"scientific notation", "1.23E-10"},
		{"hex integer", "0xDEADBEEF"},
		{"binary integer", "0b11111111000000001111111100000000"},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				lexer := NewLexer(bm.input)
				_, _ = lexer.NextToken()
			}
		})
	}
}