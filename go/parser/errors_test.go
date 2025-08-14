/*
 * PostgreSQL Parser Lexer - Error Handling Tests
 *
 * This file contains comprehensive tests for the error handling and recovery
 * mechanisms of the PostgreSQL-compatible lexer.
 */

package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLexerError tests the error structure and formatting
func TestLexerError(t *testing.T) {
	tests := []struct {
		name       string
		errorType  LexerErrorType
		message    string
		position   int
		line       int
		column     int
		nearText   string
		atEOF      bool
		expected   string
		expectedPG string
	}{
		{
			name:       "UnterminatedString at EOF",
			errorType:  UnterminatedString,
			message:    "unterminated quoted string",
			position:   20,
			line:       2,
			column:     5,
			nearText:   "",
			atEOF:      true,
			expected:   "syntax error at end of input",
			expectedPG: "unterminated quoted string at end of input",
		},
		{
			name:       "UnterminatedString with context",
			errorType:  UnterminatedString,
			message:    "unterminated quoted string",
			position:   15,
			line:       1,
			column:     16,
			nearText:   "hello world",
			atEOF:      false,
			expected:   "syntax error at or near \"hello world\"",
			expectedPG: "unterminated quoted string at or near \"hello world\"",
		},
		{
			name:       "TrailingJunk",
			errorType:  TrailingJunk,
			message:    "trailing junk after numeric literal",
			position:   10,
			line:       1,
			column:     11,
			nearText:   "123abc",
			atEOF:      false,
			expected:   "syntax error at or near \"123abc\"",
			expectedPG: "trailing junk after numeric literal: \"123abc\"",
		},
		{
			name:       "ZeroLengthIdentifier",
			errorType:  ZeroLengthIdentifier,
			message:    "zero-length delimited identifier",
			position:   5,
			line:       1,
			column:     6,
			nearText:   "\"\"",
			atEOF:      false,
			expected:   "syntax error at or near \"\"\"\"",
			expectedPG: "zero-length delimited identifier at or near \"\"\"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := &LexerError{
				Type:     tt.errorType,
				Message:  tt.message,
				Position: tt.position,
				Line:     tt.line,
				Column:   tt.column,
				NearText: tt.nearText,
				AtEOF:    tt.atEOF,
			}

			// Since Error() now returns PostgreSQL-formatted messages, check against expectedPG
			assert.Equal(t, tt.expectedPG, err.Error())
			assert.Equal(t, tt.expectedPG, err.PostgreSQLErrorMessage())
		})
	}
}

// TestErrorRecoveryStrategies tests recovery strategy suggestions
func TestErrorRecoveryStrategies(t *testing.T) {
	tests := []struct {
		name              string
		errorType         LexerErrorType
		expectCanContinue bool
		expectHint        string
	}{
		{
			name:              "UnterminatedString should not continue",
			errorType:         UnterminatedString,
			expectCanContinue: false,
			expectHint:        "Add closing quote to terminate the string",
		},
		{
			name:              "TrailingJunk should not continue",
			errorType:         TrailingJunk,
			expectCanContinue: false,
			expectHint:        "Remove invalid characters after the numeric literal",
		},
		{
			name:              "InvalidEscape should not continue",
			errorType:         InvalidEscape,
			expectCanContinue: false,
			expectHint:        "Use a valid escape sequence (\\n, \\t, \\\\, etc.)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := &LexerError{Type: tt.errorType}
			recovery := err.GetRecoveryStrategy()

			assert.Equal(t, tt.expectCanContinue, recovery.CanContinue)
			assert.Equal(t, tt.expectHint, recovery.RecoveryHint)
		})
	}
}

// TestUnicodePositionCalculation tests Unicode-aware position tracking
func TestUnicodePositionCalculation(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		bytePos     int
		expectedPos int
	}{
		{
			name:        "ASCII only",
			input:       "hello world",
			bytePos:     5,
			expectedPos: 5,
		},
		{
			name:        "Unicode characters",
			input:       "héllo wørld",
			bytePos:     6, // After 'é' (2 bytes)
			expectedPos: 5, // 5 Unicode characters
		},
		{
			name:        "Mixed ASCII and Unicode",
			input:       "test 测试 123",
			bytePos:     8, // After first Unicode character "测"
			expectedPos: 6, // 6 Unicode characters
		},
		{
			name:        "Empty string",
			input:       "",
			bytePos:     0,
			expectedPos: 0,
		},
		{
			name:        "Position beyond end",
			input:       "test",
			bytePos:     100,
			expectedPos: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CalculateUnicodePosition([]byte(tt.input), tt.bytePos)
			assert.Equal(t, tt.expectedPos, result)
		})
	}
}

// TestLineColumnCalculation tests line and column calculation
func TestLineColumnCalculation(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		pos            int
		expectedLine   int
		expectedColumn int
	}{
		{
			name:           "Single line",
			input:          "hello world",
			pos:            5,
			expectedLine:   1,
			expectedColumn: 6,
		},
		{
			name:           "Multiple lines",
			input:          "line1\nline2\nline3",
			pos:            7, // 'n' in "line2"
			expectedLine:   2,
			expectedColumn: 2,
		},
		{
			name:           "Start of second line",
			input:          "line1\nline2",
			pos:            6, // First character of line2
			expectedLine:   2,
			expectedColumn: 1,
		},
		{
			name:           "Unicode characters across lines",
			input:          "测试\n测试",
			pos:            7, // Start of first character on second line
			expectedLine:   2,
			expectedColumn: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			line, col := CalculateLineColumn([]byte(tt.input), tt.pos)
			assert.Equal(t, tt.expectedLine, line)
			assert.Equal(t, tt.expectedColumn, col)
		})
	}
}

// TestSanitizeNearText tests text sanitization for error messages
func TestSanitizeNearText(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		maxLen   int
		expected string
	}{
		{
			name:     "Normal text",
			input:    "hello world",
			maxLen:   20,
			expected: "hello world",
		},
		{
			name:     "Text with control characters",
			input:    "hello\nworld\ttest",
			maxLen:   30,
			expected: "hello.world\ttest",
		},
		{
			name:     "Text too long",
			input:    "this is a very long string that exceeds the maximum length",
			maxLen:   20,
			expected: "this is a very long ...",
		},
		{
			name:     "Empty text",
			input:    "",
			maxLen:   10,
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SanitizeNearText(tt.input, tt.maxLen)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestAddError tests the error addition to context
func TestAddError(t *testing.T) {
	ctx := NewLexerContext("SELECT 'unterminated string")
	ctx.CurrentPosition = 8
	ctx.LineNumber = 1
	ctx.ColumnNumber = 9

	// Add an error (now always returns the error)
	err := ctx.AddError(UnterminatedString, "unterminated quoted string")

	// Verify error properties
	assert.Equal(t, UnterminatedString, err.Type)
	assert.Equal(t, "unterminated quoted string", err.Message)
	assert.Equal(t, 8, err.Position)
	assert.Equal(t, 1, err.Line)
	assert.Equal(t, 9, err.Column)
	assert.False(t, err.AtEOF)
	assert.NotEmpty(t, err.NearText)

	// Verify it was also added to legacy error collection
	assert.True(t, ctx.HasErrors())
	assert.Len(t, ctx.GetErrors(), 1)
}

// TestContextErrorHints tests context-specific error hints
func TestContextErrorHints(t *testing.T) {
	tests := []struct {
		name      string
		state     LexerState
		errorType LexerErrorType
		expected  string
	}{
		{
			name:      "Unterminated string in XQ state",
			state:     StateXQ,
			errorType: UnterminatedString,
			expected:  "Add a closing single quote (') to terminate the string",
		},
		{
			name:      "Unterminated string in XE state",
			state:     StateXE,
			errorType: UnterminatedString,
			expected:  "Add a closing single quote (') to terminate the extended string",
		},
		{
			name:      "Unterminated comment",
			state:     StateXC,
			errorType: UnterminatedComment,
			expected:  "Add */ to close the comment",
		},
		{
			name:      "Invalid escape",
			state:     StateInitial,
			errorType: InvalidEscape,
			expected:  "Use a valid escape sequence like \\n, \\t, \\\\, or \\'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := NewLexerContext("test input")
			ctx.SetState(tt.state)

			hint := ctx.getErrorHint(tt.errorType)
			assert.Equal(t, tt.expected, hint)
		})
	}
}

// TestPositionSaveRestore tests position save and restore functionality
func TestPositionSaveRestore(t *testing.T) {
	input := "hello\nworld\ntest"
	ctx := NewLexerContext(input)

	// Advance to position 7 (first character of "world")
	for i := 0; i < 7; i++ {
		ctx.NextByte()
	}

	// Save position
	savedPos := ctx.SaveCurrentPosition()
	originalLine := ctx.LineNumber
	originalColumn := ctx.ColumnNumber

	// Advance further to reach the next line
	for i := 0; i < 6; i++ { // Need to go from 'w' to past the second '\n'
		ctx.NextByte()
	}

	// Verify we've moved to a different line
	assert.NotEqual(t, originalLine, ctx.LineNumber)

	// Restore position
	ctx.RestoreSavedPosition()

	// Verify position was restored
	assert.Equal(t, savedPos, ctx.CurrentPosition)
	assert.Equal(t, originalLine, ctx.LineNumber)
	assert.Equal(t, originalColumn, ctx.ColumnNumber)
}

// TestUnicodeAdvancement tests Unicode-aware position advancement
func TestUnicodeAdvancement(t *testing.T) {
	input := "test 测试 end"
	ctx := NewLexerContext(input)

	// Advance through ASCII part
	for i := 0; i < 5; i++ { // "test "
		ctx.AdvanceRune()
	}

	// Should be at first Unicode character
	assert.Equal(t, 5, ctx.CurrentPosition)
	assert.Equal(t, 6, ctx.ColumnNumber) // Column 6 (1-based)

	// Advance through first Unicode character (测)
	r := ctx.AdvanceRune()
	assert.NotEqual(t, 0, r)
	assert.Equal(t, 8, ctx.CurrentPosition) // 3 bytes for UTF-8
	assert.Equal(t, 7, ctx.ColumnNumber)    // Column 7

	// Advance through second Unicode character (试)
	r = ctx.AdvanceRune()
	assert.NotEqual(t, 0, r)
	assert.Equal(t, 11, ctx.CurrentPosition) // 3 more bytes
	assert.Equal(t, 8, ctx.ColumnNumber)     // Column 8
}

// TestErrorTypeFormatting tests all error type formatting
func TestErrorTypeFormatting(t *testing.T) {
	errorTypes := []LexerErrorType{
		SyntaxError,
		UnterminatedString,
		UnterminatedComment,
		UnterminatedBitString,
		UnterminatedHexString,
		UnterminatedDollarQuote,
		UnterminatedIdentifier,
		InvalidEscape,
		InvalidUnicode,
		InvalidNumber,
		TrailingJunk,
		OperatorTooLong,
		ZeroLengthIdentifier,
		TrailingJunkAfterParameter,
	}

	for _, errorType := range errorTypes {
		t.Run(errorType.String(), func(t *testing.T) {
			err := &LexerError{
				Type:     errorType,
				Message:  "test message",
				NearText: "test",
			}

			// Just verify it doesn't panic and produces some output
			result := err.PostgreSQLErrorMessage()
			assert.NotEmpty(t, result)
		})
	}
}

// String method for LexerErrorType for test names
func (e LexerErrorType) String() string {
	switch e {
	case SyntaxError:
		return "SyntaxError"
	case UnterminatedString:
		return "UnterminatedString"
	case UnterminatedComment:
		return "UnterminatedComment"
	case UnterminatedBitString:
		return "UnterminatedBitString"
	case UnterminatedHexString:
		return "UnterminatedHexString"
	case UnterminatedDollarQuote:
		return "UnterminatedDollarQuote"
	case UnterminatedIdentifier:
		return "UnterminatedIdentifier"
	case InvalidEscape:
		return "InvalidEscape"
	case InvalidUnicode:
		return "InvalidUnicode"
	case InvalidNumber:
		return "InvalidNumber"
	case TrailingJunk:
		return "TrailingJunk"
	case OperatorTooLong:
		return "OperatorTooLong"
	case ZeroLengthIdentifier:
		return "ZeroLengthIdentifier"
	case TrailingJunkAfterParameter:
		return "TrailingJunkAfterParameter"
	case InvalidUnicodeEscape:
		return "InvalidUnicodeEscape"
	case InvalidUnicodeSurrogatePair:
		return "InvalidUnicodeSurrogatePair"
	case UnsupportedEscapeSequence:
		return "UnsupportedEscapeSequence"
	case InvalidHexInteger:
		return "InvalidHexInteger"
	case InvalidOctalInteger:
		return "InvalidOctalInteger"
	case InvalidBinaryInteger:
		return "InvalidBinaryInteger"
	default:
		return "Unknown"
	}
}

// TestRealWorldErrorScenarios tests error handling with real SQL scenarios
func TestRealWorldErrorScenarios(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		expectedError LexerErrorType
		expectedText  string
	}{
		{
			name:          "Unterminated string literal",
			input:         "SELECT 'hello world",
			expectedError: UnterminatedString,
			expectedText:  "hello world",
		},
		{
			name:          "Unterminated comment",
			input:         "SELECT * FROM table /* this is a comment",
			expectedError: UnterminatedComment,
		},
		{
			name:          "Invalid numeric literal",
			input:         "SELECT 123abc FROM table",
			expectedError: TrailingJunk,
			expectedText:  "123abc",
		},
		{
			name:          "Zero-length identifier",
			input:         "SELECT \"\" FROM table",
			expectedError: ZeroLengthIdentifier,
		},
		{
			name:          "Unterminated delimited identifier",
			input:         "SELECT \"unclosed_identifier FROM table",
			expectedError: UnterminatedIdentifier,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)

			// Keep getting tokens until EOF or error
			var lastError error
			for {
				token, err := lexer.NextToken()
				if err != nil {
					lastError = err
					break
				}
				if token.Type == EOF {
					break
				}
			}

			// Check for errors either as return value or in context
			if lastError != nil {
				// Check if it's a LexerError
				if enhancedErr, ok := lastError.(*LexerError); ok {
					assert.Equal(t, tt.expectedError, enhancedErr.Type)
					if tt.expectedText != "" {
						assert.Contains(t, enhancedErr.NearText, tt.expectedText)
					}
				}
			} else {
				// Check context for errors (PostgreSQL-style error collection)
				require.True(t, lexer.context.HasErrors(), "Expected an error but got none")
				errors := lexer.context.GetErrors()
				require.NotEmpty(t, errors, "Expected errors in context")

				// Check the first error
				firstError := errors[0]
				assert.Equal(t, tt.expectedError, firstError.Type)
				if tt.expectedText != "" {
					assert.Contains(t, firstError.NearText, tt.expectedText)
				}
			}
		})
	}
}
