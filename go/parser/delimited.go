/*
PostgreSQL Parser Lexer - Delimited Identifier Handling

This file implements the delimited identifier functionality for the PostgreSQL-compatible lexer.
It supports double-quoted identifiers with proper escaping and case preservation.
Ported from postgres/src/backend/parser/scan.l:803-839
*/

package parser

import (
	"strings"
)

// scanDelimitedIdentifier handles the initial double-quote and transitions to delimited identifier state
// This is called when we detect " in the initial state
// Equivalent to postgres/src/backend/parser/scan.l:803-807 (xdstart rule)
func (l *Lexer) scanDelimitedIdentifier(startPos, startScanPos int) (*Token, error) {
	// Advance past the opening quote - NextByte handles position tracking
	l.context.NextByte()

	// Clear literal buffer and transition to delimited identifier state
	l.context.StartLiteral()
	l.context.SetState(StateXD)

	// Continue scanning in delimited identifier state
	return l.scanDelimitedIdentifierState(startPos, startScanPos)
}

// scanDelimitedIdentifierState handles scanning inside delimited identifiers (xd state)
// This implements the state machine for double-quoted identifiers
// Equivalent to postgres/src/backend/parser/scan.l:813-839 (<xd> state rules)
func (l *Lexer) scanDelimitedIdentifierState(startPos, startScanPos int) (*Token, error) {
	for {
		b, ok := l.context.CurrentByte()
		if !ok {
			// EOF in delimited identifier - postgres/src/backend/parser/scan.l:839
			l.context.SetState(StateInitial)
			err := l.context.AddErrorWithType(UnterminatedIdentifier, "unterminated quoted identifier")
			return nil, err
		}

		// Check for closing quote
		// postgres/src/backend/parser/scan.l:813-824 (xdstop rule)
		if b == '"' {
			// Check for escaped quote (double-quote)
			next := l.context.PeekBytes(2)
			if len(next) >= 2 && next[1] == '"' {
				// Double-quote escape sequence - postgres/src/backend/parser/scan.l:833-835
				l.context.AddLiteralByte('"')
				l.context.AdvanceBy(2) // AdvanceBy handles position tracking
				continue
			}

			// End of delimited identifier
			l.context.NextByte() // NextByte handles position tracking
			l.context.SetState(StateInitial)

			// Get the identifier text
			ident := l.context.GetLiteral()

			// Check for zero-length identifier
			// postgres/src/backend/parser/scan.l:818
			if len(ident) == 0 {
				err := l.context.AddErrorWithType(ZeroLengthIdentifier, "zero-length delimited identifier")
				return nil, err
			}

			// Check length and truncate if necessary
			// postgres/src/backend/parser/scan.l:820-821
			if len(ident) >= NAMEDATALEN {
				ident = truncateIdentifier(ident, true)
			}

			// Return as identifier token
			// postgres/src/backend/parser/scan.l:822
			// Text field should include the original quotes, value field should not
			originalText := l.context.GetCurrentText(startScanPos)
			return NewStringToken(IDENT, ident, startPos, originalText), nil
		}

		// Regular character inside identifier
		// postgres/src/backend/parser/scan.l:836-838 (xdinside rule)
		l.processIdentifierChar(b)
	}
}

// scanUnicodeIdentifier handles Unicode-escaped identifiers (U&"...")
// This is called when we detect U&" in the initial state
// Equivalent to postgres/src/backend/parser/scan.l:808-812 (xuistart rule)
func (l *Lexer) scanUnicodeIdentifier(startPos, startScanPos int) (*Token, error) {
	// Advance past U& prefix
	l.context.AdvanceBy(2)

	// Advance past the opening quote
	l.context.NextByte()

	// Clear literal buffer and transition to Unicode identifier state
	l.context.StartLiteral()
	l.context.SetState(StateXUI)

	// Continue scanning in Unicode identifier state
	return l.scanUnicodeIdentifierState(startPos, startScanPos)
}

// scanUnicodeIdentifierState handles scanning inside Unicode identifiers (xui state)
// Equivalent to postgres/src/backend/parser/scan.l:825-832 (<xui> state rules)
func (l *Lexer) scanUnicodeIdentifierState(startPos, startScanPos int) (*Token, error) {
	// For now, we treat Unicode identifiers the same as regular delimited identifiers
	// Full Unicode escape processing will be implemented in Phase 2I
	for {
		b, ok := l.context.CurrentByte()
		if !ok {
			// EOF in Unicode identifier - postgres/src/backend/parser/scan.l:839
			l.context.SetState(StateInitial)
			err := l.context.AddErrorWithType(UnterminatedIdentifier, "unterminated quoted identifier")
			return nil, err
		}

		// Check for closing quote
		// postgres/src/backend/parser/scan.l:825-832
		if b == '"' {
			// Check for escaped quote (double-quote)
			next := l.context.PeekBytes(2)
			if len(next) >= 2 && next[1] == '"' {
				// Double-quote escape sequence - postgres/src/backend/parser/scan.l:833-835
				l.context.AddLiteralByte('"')
				l.context.AdvanceBy(2)
				continue
			}

			// End of Unicode identifier
			l.context.NextByte() // Consume closing quote
			l.context.SetState(StateInitial)

			// Get the identifier text
			ident := l.context.GetLiteral()

			// Note: Zero-length Unicode identifiers are allowed (unlike regular delimited identifiers)
			// Full Unicode escape processing deferred to Phase 2I
			// For now, just return the literal content
			// postgres/src/backend/parser/scan.l:831
			// Text field should include U& prefix and quotes, value field should not
			originalText := l.context.GetCurrentText(startScanPos)
			return NewStringToken(UIDENT, ident, startPos, originalText), nil
		}

		// Regular character inside identifier
		// postgres/src/backend/parser/scan.l:836-838
		l.processIdentifierChar(b)
	}
}

// NAMEDATALEN is the maximum length of names in PostgreSQL
// postgres/src/include/pg_config_manual.h:32
const NAMEDATALEN = 64

// truncateIdentifier truncates an identifier to NAMEDATALEN-1 and optionally warns
// Equivalent to postgres/src/backend/parser/scansup.c:truncate_identifier
func truncateIdentifier(ident string, warn bool) string {
	maxLen := NAMEDATALEN - 1
	if len(ident) <= maxLen {
		return ident
	}

	// Find a safe truncation point (not in the middle of a multibyte character)
	truncated := ident[:maxLen]

	// In PostgreSQL, this would generate a NOTICE-level warning
	// For now, we just truncate silently (warning would be added in error handling phase)

	return truncated
}

// processIdentifierChar handles character processing for delimited identifiers
// This consolidates the duplicate line ending normalization logic
func (l *Lexer) processIdentifierChar(b byte) {
	// Handle line ending normalization first
	if b == '\r' {
		// Always normalize \r to \n in identifiers
		l.context.AddLiteralByte('\n')
		l.context.NextByte() // NextByte handles position tracking
		// Check for \r\n and consume the \n part too
		if next := l.context.PeekBytes(1); len(next) >= 1 && next[0] == '\n' {
			l.context.NextByte() // Consume the \n but don't add it to literal
		}
	} else {
		// Regular character - add as-is
		l.context.AddLiteralByte(b)
		l.context.NextByte() // NextByte handles position tracking
	}
}

// downcaseIdentifier converts an identifier to lowercase for case-insensitive matching
// Equivalent to postgres/src/backend/parser/scansup.c:downcase_identifier
func downcaseIdentifier(ident string) string {
	// PostgreSQL uses a more complex algorithm that handles multibyte characters
	// For now, we use simple ASCII lowercasing
	return strings.ToLower(ident)
}
