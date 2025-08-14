/*
 * PostgreSQL Parser Lexer - Error Handling & Recovery
 *
 * This file implements comprehensive error handling and recovery mechanisms
 * for the PostgreSQL-compatible lexer, providing accurate source position
 * tracking and PostgreSQL-compatible error messages.
 *
 * Ported from postgres/src/backend/parser/scan.l (scanner_yyerror functions)
 * and postgres/src/include/parser/scanner.h (error handling structures)
 */

package parser

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// LexerErrorType represents different categories of lexer errors
// Based on PostgreSQL error patterns in scan.l
type LexerErrorType int

const (
	SyntaxError                 LexerErrorType = iota
	UnterminatedString                         // postgres/src/backend/parser/scan.l:756 ("unterminated quoted string")
	UnterminatedComment                        // postgres/src/backend/parser/scan.l:497 ("unterminated /* comment")
	UnterminatedBitString                      // postgres/src/backend/parser/scan.l:517 ("unterminated bit string literal")
	UnterminatedHexString                      // postgres/src/backend/parser/scan.l:531 ("unterminated hexadecimal string literal")
	UnterminatedDollarQuote                    // postgres/src/backend/parser/scan.l (unterminated dollar-quoted string)
	UnterminatedIdentifier                     // postgres/src/backend/parser/scan.l (unterminated quoted identifier)
	InvalidEscape                              // postgres/src/backend/parser/scan.l (invalid escape sequence)
	InvalidUnicode                             // postgres/src/backend/parser/scan.l (invalid Unicode escape)
	InvalidNumber                              // postgres/src/backend/parser/scan.l (various numeric errors)
	InvalidHexInteger                          // postgres/src/backend/parser/scan.l:1036 ("invalid hexadecimal integer")
	InvalidOctalInteger                        // postgres/src/backend/parser/scan.l:1040 ("invalid octal integer")
	InvalidBinaryInteger                       // postgres/src/backend/parser/scan.l:1044 ("invalid binary integer")
	TrailingJunk                               // postgres/src/backend/parser/scan.l (trailing junk after...)
	TrailingJunkAfterParameter                 // Specific for parameter junk (e.g., "$1abc")
	OperatorTooLong                            // postgres/src/backend/parser/scan.l (operator too long)
	ZeroLengthIdentifier                       // postgres/src/backend/parser/scan.l:818 ("zero-length delimited identifier")
	InvalidUnicodeEscape                       // postgres/src/backend/parser/scan.l (Unicode-specific errors)
	InvalidUnicodeSurrogatePair                // postgres/src/backend/parser/scan.l (invalid Unicode surrogate pair)
	UnsupportedEscapeSequence                  // postgres/src/backend/parser/scan.l (unsupported escape patterns)
)

// ErrorRecovery provides context-aware error recovery strategies
type ErrorRecovery struct {
	CanContinue    bool   // Whether lexer can continue after this error
	RecoveryHint   string // Suggestion for fixing the error
	SuggestedFix   string // Possible correction
	RecoveryAction string // What the lexer will do to recover
}

// GetRecoveryStrategy returns appropriate recovery strategy for error type
// Based on PostgreSQL's error handling patterns and modern IDE practices
func (e *LexerError) GetRecoveryStrategy() ErrorRecovery {
	switch e.Type {
	case UnterminatedString:
		return ErrorRecovery{
			CanContinue:    false, // Fatal error in PostgreSQL
			RecoveryHint:   "Add closing quote to terminate the string",
			SuggestedFix:   "Add ' or \" to close the string literal",
			RecoveryAction: "Stop parsing - unterminated strings are fatal",
		}
	case UnterminatedComment:
		return ErrorRecovery{
			CanContinue:    false,
			RecoveryHint:   "Add */ to terminate the comment",
			SuggestedFix:   "Add */ at end of comment",
			RecoveryAction: "Stop parsing - unterminated comments are fatal",
		}
	case TrailingJunk:
		return ErrorRecovery{
			CanContinue:    false,
			RecoveryHint:   "Remove invalid characters after the numeric literal",
			SuggestedFix:   "Separate the number and identifier with whitespace",
			RecoveryAction: "Stop parsing - trailing junk is fatal",
		}
	case InvalidEscape:
		return ErrorRecovery{
			CanContinue:    false,
			RecoveryHint:   "Use a valid escape sequence (\\n, \\t, \\\\, etc.)",
			SuggestedFix:   "Replace with valid escape or remove backslash",
			RecoveryAction: "Stop parsing - invalid escapes are fatal",
		}
	case ZeroLengthIdentifier:
		return ErrorRecovery{
			CanContinue:    false,
			RecoveryHint:   "Provide a non-empty identifier between the quotes",
			SuggestedFix:   "Add identifier name between quotes: \"identifier\"",
			RecoveryAction: "Stop parsing - empty identifiers are fatal",
		}
	case InvalidHexInteger, InvalidOctalInteger, InvalidBinaryInteger:
		return ErrorRecovery{
			CanContinue:    false,
			RecoveryHint:   "Provide valid digits for the chosen number format",
			SuggestedFix:   "Add appropriate digits after the prefix (0x, 0o, 0b)",
			RecoveryAction: "Stop parsing - invalid integer format is fatal",
		}
	case InvalidUnicodeSurrogatePair:
		return ErrorRecovery{
			CanContinue:    false,
			RecoveryHint:   "Use a valid Unicode surrogate pair",
			SuggestedFix:   "Ensure high surrogate (U+D800-U+DBFF) is followed by low surrogate (U+DC00-U+DFFF)",
			RecoveryAction: "Stop parsing - invalid surrogate pairs are fatal",
		}
	case UnsupportedEscapeSequence:
		return ErrorRecovery{
			CanContinue:    false,
			RecoveryHint:   "Use only supported escape sequences",
			SuggestedFix:   "Remove or replace the unsupported escape sequence",
			RecoveryAction: "Stop parsing - unsupported escapes are fatal",
		}
	default:
		return ErrorRecovery{
			CanContinue:    false,
			RecoveryHint:   "Fix the syntax error",
			SuggestedFix:   "Correct the invalid syntax",
			RecoveryAction: "Stop parsing",
		}
	}
}

// Position tracking utilities for enhanced error reporting

// CalculateUnicodePosition converts byte position to Unicode character position
// Equivalent to PostgreSQL's pg_mbstrlen_with_len function
// postgres/src/backend/utils/mb/mbutils.c:1200
func CalculateUnicodePosition(input []byte, bytePos int) int {
	if bytePos <= 0 {
		return 0
	}
	if bytePos >= len(input) {
		bytePos = len(input)
	}

	return utf8.RuneCount(input[:bytePos])
}

// CalculateLineColumn calculates line and column numbers for a byte position
// Uses 1-based indexing like PostgreSQL
func CalculateLineColumn(input []byte, pos int) (line, column int) {
	if pos < 0 {
		return 1, 1
	}
	if pos >= len(input) {
		pos = len(input) - 1
	}

	line = 1
	column = 1

	for i := 0; i < pos; i++ {
		if input[i] == '\n' {
			line++
			column = 1
		} else {
			// Handle multi-byte UTF-8 characters properly
			if utf8.RuneStart(input[i]) {
				column++
			}
		}
	}

	return line, column
}

// LexerError represents a comprehensive lexical analysis error
// This combines all error information for PostgreSQL compatibility
type LexerError struct {
	Type        LexerErrorType // Error category (UnterminatedString, InvalidNumber, etc.)
	Message     string         // Primary error message
	Position    int            // Byte offset where error occurred
	Line        int            // Line number where error occurred (1-based)
	Column      int            // Column number where error occurred (1-based)
	NearText    string         // Text near the error for context
	AtEOF       bool           // True if error occurred at end of file
	Context     string         // Additional context (e.g., "in string literal")
	Hint        string         // Recovery suggestion
	ErrorLength int            // Length of the problematic text
}

// Error implements the error interface with PostgreSQL-compatible formatting
// Based on postgres/src/backend/parser/scan.l:1230 (scanner_yyerror)
func (e *LexerError) Error() string {
	// Return the PostgreSQL-formatted error message for compatibility
	return e.PostgreSQLErrorMessage()
}

// DetailedError returns a detailed error message with context and hints
func (e *LexerError) DetailedError() string {
	var parts []string

	// Basic error message
	if e.AtEOF {
		parts = append(parts, fmt.Sprintf("%s at end of input", e.Message))
	} else if e.NearText != "" {
		parts = append(parts, fmt.Sprintf("%s at or near \"%s\"", e.Message, e.NearText))
	} else {
		parts = append(parts, e.Message)
	}

	// Position information
	parts = append(parts, fmt.Sprintf("at line %d, column %d (position %d)", e.Line, e.Column, e.Position))

	// Context information
	if e.Context != "" {
		parts = append(parts, fmt.Sprintf("Context: %s", e.Context))
	}

	// Hint for recovery
	if e.Hint != "" {
		parts = append(parts, fmt.Sprintf("Hint: %s", e.Hint))
	}

	return strings.Join(parts, "\n")
}

// PostgreSQLErrorMessage formats error message exactly like PostgreSQL
// Based on PostgreSQL's ereport format patterns
func (e *LexerError) PostgreSQLErrorMessage() string {
	switch e.Type {
	case UnterminatedString:
		if e.AtEOF {
			return "unterminated quoted string at end of input"
		}
		return fmt.Sprintf("unterminated quoted string at or near \"%s\"", e.NearText)
	case UnterminatedComment:
		return "unterminated /* comment"
	case UnterminatedBitString:
		return "unterminated bit string literal"
	case UnterminatedHexString:
		return "unterminated hexadecimal string literal"
	case UnterminatedDollarQuote:
		return fmt.Sprintf("unterminated dollar-quoted string at or near \"%s\"", e.NearText)
	case UnterminatedIdentifier:
		return "unterminated quoted identifier"
	case InvalidEscape:
		return fmt.Sprintf("invalid escape sequence at or near \"%s\"", e.NearText)
	case InvalidUnicode:
		return fmt.Sprintf("invalid Unicode escape sequence at or near \"%s\"", e.NearText)
	case InvalidNumber:
		return fmt.Sprintf("invalid input syntax for type numeric: \"%s\"", e.NearText)
	case InvalidHexInteger:
		return "invalid hexadecimal integer"
	case InvalidOctalInteger:
		return "invalid octal integer"
	case InvalidBinaryInteger:
		return "invalid binary integer"
	case TrailingJunk:
		return fmt.Sprintf("trailing junk after numeric literal: \"%s\"", e.NearText)
	case TrailingJunkAfterParameter:
		return fmt.Sprintf("trailing junk after parameter: \"%s\"", e.NearText)
	case OperatorTooLong:
		return "operator too long"
	case ZeroLengthIdentifier:
		return "zero-length delimited identifier at or near \"\"\""
	case InvalidUnicodeEscape:
		return fmt.Sprintf("invalid Unicode escape value at or near \"%s\"", e.NearText)
	case InvalidUnicodeSurrogatePair:
		return "invalid Unicode surrogate pair"
	case UnsupportedEscapeSequence:
		return fmt.Sprintf("unsupported escape sequence at or near \"%s\"", e.NearText)
	default:
		// For SyntaxError and unknown types, return a generic syntax error
		if e.AtEOF {
			return fmt.Sprintf("syntax error at end of input")
		}
		if e.NearText != "" {
			return fmt.Sprintf("syntax error at or near \"%s\"", e.NearText)
		}
		return fmt.Sprintf("syntax error at line %d, column %d", e.Line, e.Column)
	}
}

// SanitizeNearText sanitizes text for display in error messages
// Removes/replaces control characters and limits length
func SanitizeNearText(text string, maxLen int) string {
	if text == "" {
		return ""
	}

	// Replace control characters
	sanitized := strings.Map(func(r rune) rune {
		if unicode.IsControl(r) && r != '\t' {
			return '.'
		}
		return r
	}, text)

	// Limit length
	if len(sanitized) > maxLen {
		sanitized = sanitized[:maxLen] + "..."
	}

	return sanitized
}
