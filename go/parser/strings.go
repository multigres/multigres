// PostgreSQL Database Management System
// (also known as Postgres, formerly known as Postgres95)
//
//  Portions Copyright (c) 2025, Supabase, Inc
//
//  Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//
//  Portions Copyright (c) 1994, The Regents of the University of California
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written agreement
// is hereby granted, provided that the above copyright notice and this
// paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
// DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
// LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
// DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.
//
// THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
// AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
// ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
// PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
//
/*
 * PostgreSQL Parser Lexer - String Literal System
 *
 * This file implements PostgreSQL's comprehensive string literal support,
 * including standard SQL strings, extended strings with escape sequences,
 * and dollar-quoted strings with arbitrary tags.
 * Ported from postgres/src/backend/parser/scan.l (lines 264-700)
 */

package parser

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// String processing functions - equivalent to PostgreSQL's static functions
// in scan.l (lines 1318-1468)

// scanStandardString processes a standard SQL string literal ('...')
// Equivalent to PostgreSQL xq state handling - postgres/src/backend/parser/scan.l:559-587
// When isUnicodeString is true, this handles U&'...' strings and returns USCONST tokens
func (l *Lexer) scanStandardString(startPos, startScanPos int) (*Token, error) {
	return l.scanStandardStringWithType(startPos, startScanPos, false)
}

// scanUnicodeString processes a Unicode string literal (U&'...')
// Equivalent to PostgreSQL xus state handling
func (l *Lexer) scanUnicodeString(startPos, startScanPos int) (*Token, error) {
	return l.scanStandardStringWithType(startPos, startScanPos, true)
}

// scanStandardStringWithType processes a string literal with the specified type
func (l *Lexer) scanStandardStringWithType(startPos, startScanPos int, isUnicodeString bool) (*Token, error) {
	ctx := l.context

	// Clear literal buffer for accumulating string content
	ctx.StartLiteral()

	// Skip opening quote
	ctx.AdvanceBy(1)

	foundClosingQuote := false
	for !ctx.AtEOF() {
		ch := ctx.CurrentChar()

		if ch == '\'' {
			// Look ahead for quote doubling ('') - postgres/src/backend/parser/scan.l:647-649
			if ctx.PeekChar() == '\'' {
				// Quote doubling: '' becomes single '
				ctx.AddLiteral("'")
				ctx.AdvanceBy(1) // Skip first quote
				ctx.AdvanceBy(1) // Skip second quote
				continue
			} else {
				// End of string - advance past closing quote
				ctx.AdvanceBy(1)
				foundClosingQuote = true
				break
			}
		} else if ch == '\\' && !ctx.StandardConformingStrings() {
			// In non-standard mode, backslashes are processed like extended strings
			// postgres/src/backend/parser/scan.l:562-567
			if err := l.scanEscapeSequence(); err != nil {
				return nil, err
			}
		} else {
			// Regular character - use proper UTF-8 handling
			ctx.AddLiteral(string(ch))
			// Calculate the byte size of this rune to advance correctly
			runeSize := utf8.RuneLen(ch)
			if runeSize > 0 {
				ctx.AdvanceBy(runeSize)
			} else {
				// Invalid rune, advance by 1 byte
				ctx.AdvanceBy(1)
			}
		}
	}

	if !foundClosingQuote {
		_ = ctx.AddErrorWithType(UnterminatedString, "unterminated quoted string")
		text := ctx.GetCurrentText(startScanPos)
		if isUnicodeString {
			return NewStringToken(USCONST, ctx.GetLiteral(), startPos, text), nil
		} else {
			return NewStringToken(SCONST, ctx.GetLiteral(), startPos, text), nil
		}
	}

	// Check for string continuation
	if isUnicodeString {
		return l.checkStringContinuation(USCONST, startPos, startScanPos)
	} else {
		return l.checkStringContinuation(SCONST, startPos, startScanPos)
	}
}

// scanExtendedString processes an extended string literal (E'...')
// Equivalent to PostgreSQL xe state handling - postgres/src/backend/parser/scan.l:275-285
func (l *Lexer) scanExtendedString(startPos, startScanPos int) (*Token, error) {
	ctx := l.context

	// Clear literal buffer for accumulating string content
	ctx.StartLiteral()

	// Skip 'E' or 'e' prefix and opening quote
	ctx.AdvanceBy(1) // Skip E/e
	ctx.AdvanceBy(1) // Skip '

	foundClosingQuote := false
	for !ctx.AtEOF() {
		ch := ctx.CurrentChar()

		if ch == '\'' {
			// Look ahead for quote doubling
			if ctx.PeekChar() == '\'' {
				// Quote doubling: '' becomes single '
				ctx.AddLiteral("'")
				ctx.AdvanceBy(1) // Skip first quote
				ctx.AdvanceBy(1) // Skip second quote
				continue
			} else {
				// End of string - advance past closing quote
				ctx.AdvanceBy(1)
				foundClosingQuote = true
				break
			}
		} else if ch == '\\' {
			// Process escape sequence - postgres/src/backend/parser/scan.l:667-700
			if err := l.scanEscapeSequence(); err != nil {
				return nil, err
			}
		} else {
			// Regular character - use proper UTF-8 handling
			ctx.AddLiteral(string(ch))
			// Calculate the byte size of this rune to advance correctly
			runeSize := utf8.RuneLen(ch)
			if runeSize > 0 {
				ctx.AdvanceBy(runeSize)
			} else {
				// Invalid rune, advance by 1 byte
				ctx.AdvanceBy(1)
			}
		}
	}

	if !foundClosingQuote {
		_ = ctx.AddErrorWithType(UnterminatedString, "unterminated quoted string")
		text := ctx.GetCurrentText(startScanPos)
		return NewStringToken(SCONST, ctx.GetLiteral(), startPos, text), nil
	}

	// Check for string continuation
	return l.checkStringContinuation(SCONST, startPos, startScanPos)
}

// scanDollarQuotedString processes a dollar-quoted string ($tag$...$tag$)
// Equivalent to PostgreSQL xdolq state handling - postgres/src/backend/parser/scan.l:290-320
func (l *Lexer) scanDollarQuotedString(startPos, startScanPos int) (*Token, error) {
	ctx := l.context

	// Clear literal buffer for accumulating string content
	ctx.StartLiteral()

	// Parse the opening delimiter ($tag$)
	startDelimiter, err := l.parseDollarDelimiter()
	if err != nil {
		return nil, err
	}

	if startDelimiter == "" {
		// Invalid delimiter format
		_ = ctx.AddErrorWithType(SyntaxError, "invalid dollar-quoted string delimiter")
		text := ctx.GetCurrentText(startScanPos)
		return NewStringToken(USCONST, "", startPos, text), nil
	}

	// Store the delimiter for matching - postgres/src/include/parser/scanner.h:107
	ctx.SetDolQStart(startDelimiter)

	// Special case: if we're immediately at EOF after the opening delimiter
	// and the delimiter is not just "$$", treat it as a complete empty dollar-quoted string
	if ctx.AtEOF() && startDelimiter != "$$" {
		text := ctx.GetCurrentText(startScanPos)
		return NewStringToken(SCONST, "", startPos, text), nil
	}

	// Scan for closing delimiter
	foundClosingDelimiter := false
	for !ctx.AtEOF() {
		ch := ctx.CurrentChar()

		if ch == '$' {
			// Potential closing delimiter - check if it matches
			if l.matchesDollarDelimiter(startDelimiter) {
				// Found matching closing delimiter - advance past it
				for i := 0; i < len(startDelimiter); i++ {
					ctx.AdvanceBy(1)
				}
				foundClosingDelimiter = true
				break
			} else {
				// Not a matching delimiter, treat as literal $
				ctx.AddLiteral("$")
				ctx.AdvanceBy(1)
			}
		} else {
			// Literal character - no escape processing in dollar-quoted strings
			ctx.AddLiteral(string(ch))
			ctx.AdvanceBy(1)
		}
	}

	if !foundClosingDelimiter {
		_ = ctx.AddErrorWithType(UnterminatedDollarQuote, fmt.Sprintf("unterminated dollar-quoted string at or near \"%s\"", startDelimiter))
		text := ctx.GetCurrentText(startScanPos)
		return NewStringToken(USCONST, ctx.GetLiteral(), startPos, text), nil
	}

	// Dollar-quoted strings don't support continuation
	text := ctx.GetCurrentText(startScanPos)
	ctx.SetState(StateInitial)
	return NewStringToken(SCONST, ctx.GetLiteral(), startPos, text), nil
}

// parseDollarDelimiter parses a dollar-quote delimiter ($tag$)
// Equivalent to PostgreSQL dolqdelim pattern - postgres/src/backend/parser/scan.l:290-303
func (l *Lexer) parseDollarDelimiter() (string, error) {
	ctx := l.context

	if ctx.CurrentChar() != '$' {
		return "", fmt.Errorf("expected '$' at start of dollar delimiter")
	}

	var delimiter strings.Builder
	delimiter.WriteString("$")
	ctx.AdvanceBy(1) // Skip initial $

	// Parse optional tag - postgres/src/backend/parser/scan.l:290-303
	// dolq_start: [A-Za-z\200-\377_]
	// dolq_cont: [A-Za-z\200-\377_0-9]
	if !ctx.AtEOF() && l.isDollarQuoteStartChar(ctx.CurrentChar()) {
		// Tag starts with valid character
		delimiter.WriteString(string(ctx.CurrentChar()))
		ctx.AdvanceBy(1)

		// Continue with valid tag characters
		for !ctx.AtEOF() && l.isDollarQuoteCont(ctx.CurrentChar()) {
			delimiter.WriteString(string(ctx.CurrentChar()))
			ctx.AdvanceBy(1)
		}
	}

	// Must end with $
	if ctx.AtEOF() || ctx.CurrentChar() != '$' {
		return "", fmt.Errorf("unterminated dollar-quote delimiter")
	}

	delimiter.WriteString("$")
	ctx.AdvanceBy(1) // Skip closing $

	return delimiter.String(), nil
}

// isDollarQuoteStartChar checks if character can start a dollar-quote tag
// Equivalent to dolq_start pattern - postgres/src/backend/parser/scan.l:290
func (l *Lexer) isDollarQuoteStartChar(ch rune) bool {
	return unicode.IsLetter(ch) || ch == '_' || (ch >= 0x80 && ch <= 0x377)
}

// isDollarQuoteCont checks if character can continue a dollar-quote tag
// Equivalent to dolq_cont pattern - postgres/src/backend/parser/scan.l:291
func (l *Lexer) isDollarQuoteCont(ch rune) bool {
	return unicode.IsLetter(ch) || unicode.IsDigit(ch) || ch == '_' || (ch >= 0x80 && ch <= 0x377)
}

// matchesDollarDelimiter checks if current position has matching dollar delimiter
func (l *Lexer) matchesDollarDelimiter(expectedDelimiter string) bool {
	ctx := l.context

	// Check if we have enough characters remaining
	remaining := len(ctx.ScanBuf()) - ctx.ScanPos()
	if remaining < len(expectedDelimiter) {
		return false
	}

	// Check character by character
	for i, expectedChar := range expectedDelimiter {
		if ctx.ScanPos()+i >= len(ctx.ScanBuf()) {
			return false
		}
		if rune(ctx.ScanBuf()[ctx.ScanPos()+i]) != expectedChar {
			return false
		}
	}

	return true
}

// scanEscapeSequence processes backslash escape sequences in extended strings
// Equivalent to PostgreSQL's unescape_single_char and related functions
// postgres/src/backend/utils/adt/encode.c:454-500
func (l *Lexer) scanEscapeSequence() error {
	ctx := l.context

	if ctx.CurrentChar() != '\\' {
		return fmt.Errorf("expected backslash for escape sequence")
	}

	ctx.AdvanceBy(1) // Skip backslash

	if ctx.AtEOF() {
		_ = ctx.AddErrorWithType(InvalidEscape, "unterminated escape sequence")
		return nil
	}

	ch := ctx.CurrentChar()
	ctx.AdvanceBy(1)

	switch ch {
	case 'b':
		ctx.AddLiteral("\b") // backspace
	case 'f':
		ctx.AddLiteral("\f") // form feed
	case 'n':
		ctx.AddLiteral("\n") // newline
	case 'r':
		ctx.AddLiteral("\r") // carriage return
	case 't':
		ctx.AddLiteral("\t") // tab
	case 'v':
		ctx.AddLiteral("\v") // vertical tab
	case '\\':
		ctx.AddLiteral("\\") // literal backslash
	case '\'':
		ctx.AddLiteral("'") // literal single quote
	case '"':
		ctx.AddLiteral("\"") // literal double quote
	case 'x':
		// Hexadecimal escape \xHH - postgres/src/backend/parser/scan.l:276
		return l.scanHexEscape()
	case 'u':
		// Unicode escape \uXXXX - postgres/src/backend/parser/scan.l:281
		return l.scanUnicodeEscape(4)
	case 'U':
		// Unicode escape \UXXXXXXXX - postgres/src/backend/parser/scan.l:281
		return l.scanUnicodeEscape(8)
	default:
		if ch >= '0' && ch <= '7' {
			// Octal escape \nnn - postgres/src/backend/parser/scan.l:278
			ctx.SetScanPos(ctx.ScanPos() - 1) // Back up to reprocess first octal digit
			return l.scanOctalEscape()
		} else {
			// Literal character after backslash
			ctx.AddLiteral(string(ch))
		}
	}

	return nil
}

// scanHexEscape processes hexadecimal escape sequences (\xHH)
// Equivalent to PostgreSQL xehexesc pattern - postgres/src/backend/parser/scan.l:278
func (l *Lexer) scanHexEscape() error {
	ctx := l.context

	hexDigits := ""
	maxDigits := 2

	for i := 0; i < maxDigits && !ctx.AtEOF(); i++ {
		ch := ctx.CurrentChar()
		if (ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'F') || (ch >= 'a' && ch <= 'f') {
			hexDigits += string(ch)
			ctx.AdvanceBy(1)
		} else {
			break
		}
	}

	if len(hexDigits) == 0 {
		_ = ctx.AddErrorWithType(InvalidEscape, "invalid hexadecimal escape sequence")
		return nil
	}

	value, err := strconv.ParseUint(hexDigits, 16, 8)
	if err != nil {
		_ = ctx.AddErrorWithType(InvalidEscape, "invalid hexadecimal escape sequence")
		return nil
	}

	ctx.AddLiteral(string(rune(value)))
	return nil
}

// scanOctalEscape processes octal escape sequences (\nnn)
// Equivalent to PostgreSQL xeoctesc pattern - postgres/src/backend/parser/scan.l:277
func (l *Lexer) scanOctalEscape() error {
	ctx := l.context

	octalDigits := ""
	maxDigits := 3

	for i := 0; i < maxDigits && !ctx.AtEOF(); i++ {
		ch := ctx.CurrentChar()
		if ch >= '0' && ch <= '7' {
			octalDigits += string(ch)
			ctx.AdvanceBy(1)
		} else {
			break
		}
	}

	if len(octalDigits) == 0 {
		_ = ctx.AddErrorWithType(InvalidEscape, "invalid octal escape sequence")
		return nil
	}

	value, err := strconv.ParseUint(octalDigits, 8, 8)
	if err != nil {
		_ = ctx.AddErrorWithType(InvalidEscape, "invalid octal escape sequence")
		return nil
	}

	ctx.AddLiteral(string(rune(value)))
	return nil
}

// scanUnicodeEscape processes Unicode escape sequences (\uXXXX or \UXXXXXXXX)
// Equivalent to PostgreSQL xeunicode pattern - postgres/src/backend/parser/scan.l:281
func (l *Lexer) scanUnicodeEscape(digitCount int) error {
	ctx := l.context

	hexDigits := ""

	for i := 0; i < digitCount && !ctx.AtEOF(); i++ {
		ch := ctx.CurrentChar()
		if (ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'F') || (ch >= 'a' && ch <= 'f') {
			hexDigits += string(ch)
			ctx.AdvanceBy(1)
		} else {
			_ = ctx.AddErrorWithType(InvalidUnicodeEscape, fmt.Sprintf("invalid Unicode escape sequence, expected %d hex digits", digitCount))
			return nil
		}
	}

	if len(hexDigits) != digitCount {
		_ = ctx.AddErrorWithType(InvalidUnicodeEscape, fmt.Sprintf("invalid Unicode escape sequence, expected %d hex digits", digitCount))
		return nil
	}

	value, err := strconv.ParseUint(hexDigits, 16, 32)
	if err != nil {
		_ = ctx.AddErrorWithType(InvalidUnicodeEscape, "invalid Unicode escape sequence")
		return nil
	}

	// Check for valid Unicode code point
	if value > 0x10FFFF {
		_ = ctx.AddErrorWithType(InvalidUnicodeEscape, "Unicode escape sequence out of range")
		return nil
	}

	// Handle UTF-16 surrogate pairs - PostgreSQL xeu state (postgres/src/backend/parser/scan.l:671-678)
	runeValue := rune(value)

	if isUTF16SurrogateFirst(runeValue) {
		// First part of surrogate pair - need to get the second part
		// Equivalent to postgres/src/backend/parser/scan.l:673-674
		ctx.SetUTF16FirstPart(runeValue)
		return l.scanSurrogatePairSecond()
	} else if isUTF16SurrogateSecond(runeValue) {
		// Second surrogate without first - error
		// Equivalent to postgres/src/backend/parser/scan.l:676-677
		_ = ctx.AddErrorWithType(InvalidUnicodeSurrogatePair, "invalid Unicode surrogate pair")
		return nil
	}

	// Convert to UTF-8 and add to literal
	if utf8.ValidRune(runeValue) {
		ctx.AddLiteral(string(runeValue))
	} else {
		_ = ctx.AddErrorWithType(InvalidUnicodeEscape, "invalid Unicode code point")
	}

	return nil
}

// checkStringContinuation checks for string continuation across whitespace
// Equivalent to PostgreSQL xqs state and quotecontinue pattern
// postgres/src/backend/parser/scan.l:588-645
func (l *Lexer) checkStringContinuation(tokenType TokenType, startPos, startScanPos int) (*Token, error) {
	ctx := l.context

	// Accumulate all string parts
	finalLiteral := ctx.GetLiteral()

	// Enter quote stop state (xqs) - postgres/src/backend/parser/scan.l:588
	originalState := ctx.GetState()
	ctx.SetState(StateXQS)

	for {
		// Skip whitespace to look for continuation
		// SQL requires at least one newline in the whitespace for string concatenation
		savedPos := ctx.ScanPos()
		hasNewline := false

		// Skip whitespace and check for newline
		for !ctx.AtEOF() && unicode.IsSpace(ctx.CurrentChar()) {
			if ctx.CurrentChar() == '\n' {
				hasNewline = true
			}
			ctx.AdvanceBy(1)
		}

		// If no newline was found, string concatenation is not allowed
		if !hasNewline {
			ctx.SetScanPos(savedPos)
			ctx.SetState(StateInitial)

			// Set final literal and return
			ctx.StartLiteral()
			ctx.AddLiteral(finalLiteral)

			text := ctx.GetCurrentText(startScanPos)
			return NewStringToken(tokenType, finalLiteral, startPos, text), nil
		}

		// Check for continuation - could be ' or E' or e'
		if !ctx.AtEOF() {
			ch := ctx.CurrentChar()
			isExtended := false

			if ch == '\'' {
				// Standard string continuation
				ctx.AdvanceBy(1) // Skip continuation quote
			} else if (ch == 'E' || ch == 'e') && ctx.PeekChar() == '\'' {
				// Extended string continuation
				isExtended = true
				ctx.AdvanceBy(1) // Skip E/e
				ctx.AdvanceBy(1) // Skip '
			} else {
				// No continuation found
				ctx.SetScanPos(savedPos)
				ctx.SetState(StateInitial)

				// Set final literal and return
				ctx.StartLiteral()
				ctx.AddLiteral(finalLiteral)

				text := ctx.GetCurrentText(startScanPos)
				return NewStringToken(tokenType, finalLiteral, startPos, text), nil
			}

			// Clear literal buffer for next part
			ctx.StartLiteral()

			// Process continuation string content
			foundClosingQuote := false
			for !ctx.AtEOF() {
				ch := ctx.CurrentChar()

				if ch == '\'' {
					// Check for quote doubling or end of string
					if ctx.PeekChar() == '\'' {
						// Quote doubling
						ctx.AddLiteral("'")
						ctx.AdvanceBy(1) // Skip first quote
						ctx.AdvanceBy(1) // Skip second quote
						continue
					} else {
						// End of this string part
						ctx.AdvanceBy(1)
						foundClosingQuote = true
						break
					}
				} else if ch == '\\' && !ctx.StandardConformingStrings() && originalState == StateXQ && !isExtended {
					// Handle backslashes in non-standard mode for standard strings
					if err := l.scanEscapeSequence(); err != nil {
						return nil, err
					}
				} else if ch == '\\' && (originalState == StateXE || isExtended) {
					// Handle escape sequences in extended strings or extended continuation
					if err := l.scanEscapeSequence(); err != nil {
						return nil, err
					}
				} else {
					// Regular character
					ctx.AddLiteral(string(ch))
					ctx.AdvanceBy(1)
				}
			}

			if !foundClosingQuote {
				_ = ctx.AddErrorWithType(UnterminatedString, "unterminated quoted string")
				text := ctx.GetCurrentText(startScanPos)
				return NewStringToken(USCONST, finalLiteral+ctx.GetLiteral(), startPos, text), nil
			}

			// Concatenate this part to final literal
			finalLiteral += ctx.GetLiteral()

			// Continue looking for more continuations
			continue
		}

		// No continuation found - restore position and return final token
		ctx.SetScanPos(savedPos)
		ctx.SetState(StateInitial)

		// Set final literal and return
		ctx.StartLiteral()
		ctx.AddLiteral(finalLiteral)

		text := ctx.GetCurrentText(startScanPos)
		return NewStringToken(tokenType, finalLiteral, startPos, text), nil
	}
}

// scanBitString processes bit string literals (B'...')
// Equivalent to PostgreSQL xb state handling - postgres/src/backend/parser/scan.l:264-267
func (l *Lexer) scanBitString(startPos, startScanPos int) (*Token, error) {
	ctx := l.context

	// Clear literal buffer
	ctx.StartLiteral()

	// Add 'b' prefix to match PostgreSQL's scan.l line 511: addlitchar('b', yyscanner);
	ctx.AddLiteral("b")

	// Skip 'B' prefix and opening quote
	ctx.AdvanceBy(1) // Skip B
	ctx.AdvanceBy(1) // Skip '

	foundClosingQuote := false
	for !ctx.AtEOF() {
		ch := ctx.CurrentChar()

		if ch == '\'' {
			// End of bit string
			ctx.AdvanceBy(1)
			foundClosingQuote = true
			break
		} else if ch == '0' || ch == '1' {
			// Valid bit character
			ctx.AddLiteral(string(ch))
			ctx.AdvanceBy(1)
		} else if unicode.IsSpace(ch) {
			// Skip whitespace within bit strings
			ctx.AdvanceBy(1)
		} else {
			// Invalid character in bit string
			_ = ctx.AddErrorWithType(SyntaxError, fmt.Sprintf("invalid bit string character: %c", ch))
			ctx.AdvanceBy(1)
		}
	}

	if !foundClosingQuote {
		_ = ctx.AddErrorWithType(UnterminatedBitString, "unterminated bit string literal")
		ctx.SetState(StateInitial) // Reset state even for errors
		text := ctx.GetCurrentText(startScanPos)
		return NewStringToken(BCONST, ctx.GetLiteral(), startPos, text), nil
	}

	ctx.SetState(StateInitial) // Reset state after processing bit string
	text := ctx.GetCurrentText(startScanPos)
	return NewStringToken(BCONST, ctx.GetLiteral(), startPos, text), nil
}

// scanHexString processes hexadecimal string literals (X'...')
// Equivalent to PostgreSQL xh state handling - postgres/src/backend/parser/scan.l:268-271
func (l *Lexer) scanHexString(startPos, startScanPos int) (*Token, error) {
	ctx := l.context

	// Clear literal buffer
	ctx.StartLiteral()

	// Add 'x' prefix to match PostgreSQL's scan.l line 529: addlitchar('x', yyscanner);
	ctx.AddLiteral("x")

	// Skip 'X' prefix and opening quote
	ctx.AdvanceBy(1) // Skip X
	ctx.AdvanceBy(1) // Skip '

	foundClosingQuote := false
	for !ctx.AtEOF() {
		ch := ctx.CurrentChar()

		if ch == '\'' {
			// End of hex string
			ctx.AdvanceBy(1)
			foundClosingQuote = true
			break
		} else if (ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'F') || (ch >= 'a' && ch <= 'f') {
			// Valid hex character
			ctx.AddLiteral(string(ch))
			ctx.AdvanceBy(1)
		} else if unicode.IsSpace(ch) {
			// Skip whitespace within hex strings
			ctx.AdvanceBy(1)
		} else {
			// Invalid character in hex string
			_ = ctx.AddErrorWithType(SyntaxError, fmt.Sprintf("invalid hexadecimal string character: %c", ch))
			ctx.AdvanceBy(1)
		}
	}

	if !foundClosingQuote {
		_ = ctx.AddErrorWithType(UnterminatedHexString, "unterminated hexadecimal string literal")
		ctx.SetState(StateInitial) // Reset state even for errors
		text := ctx.GetCurrentText(startScanPos)
		return NewStringToken(XCONST, ctx.GetLiteral(), startPos, text), nil
	}

	ctx.SetState(StateInitial) // Reset state after processing hex string
	text := ctx.GetCurrentText(startScanPos)
	return NewStringToken(XCONST, ctx.GetLiteral(), startPos, text), nil
}

// scanSurrogatePairSecond processes the second part of a UTF-16 surrogate pair
// Equivalent to PostgreSQL xeu state handling - postgres/src/backend/parser/scan.l:684-703
func (l *Lexer) scanSurrogatePairSecond() error {
	ctx := l.context

	// The first surrogate is stored in ctx.UTF16FirstPart()
	// Now we need to expect and parse the second surrogate (\u or \U)

	// Expect to find \u or \U for the second surrogate
	if ctx.CurrentChar() != '\\' {
		_ = ctx.AddErrorWithType(InvalidUnicodeSurrogatePair, "invalid Unicode surrogate pair: expected escape sequence")
		ctx.SetUTF16FirstPart(0) // Clear stored surrogate
		return nil
	}

	ctx.AdvanceBy(1) // Skip backslash

	if ctx.AtEOF() {
		_ = ctx.AddErrorWithType(InvalidUnicodeSurrogatePair, "invalid Unicode surrogate pair: unexpected end of input")
		ctx.SetUTF16FirstPart(0)
		return nil
	}

	escapeChar := ctx.CurrentChar()
	ctx.AdvanceBy(1)

	var digitCount int
	switch escapeChar {
	case 'u':
		digitCount = 4
	case 'U':
		digitCount = 8
	default:
		_ = ctx.AddErrorWithType(InvalidUnicodeSurrogatePair, "invalid Unicode surrogate pair: expected \\u or \\U")
		ctx.SetUTF16FirstPart(0)
		return nil
	}

	// Parse hex digits for second surrogate
	hexDigits := ""
	for i := 0; i < digitCount && !ctx.AtEOF(); i++ {
		ch := ctx.CurrentChar()
		if (ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'F') || (ch >= 'a' && ch <= 'f') {
			hexDigits += string(ch)
			ctx.AdvanceBy(1)
		} else {
			_ = ctx.AddErrorWithType(InvalidUnicodeEscape, fmt.Sprintf("invalid Unicode escape sequence, expected %d hex digits", digitCount))
			ctx.SetUTF16FirstPart(0)
			return nil
		}
	}

	if len(hexDigits) != digitCount {
		_ = ctx.AddErrorWithType(InvalidUnicodeEscape, fmt.Sprintf("invalid Unicode escape sequence, expected %d hex digits", digitCount))
		ctx.SetUTF16FirstPart(0)
		return nil
	}

	secondValue, err := strconv.ParseUint(hexDigits, 16, 32)
	if err != nil {
		_ = ctx.AddErrorWithType(InvalidUnicodeEscape, "invalid Unicode escape sequence")
		ctx.SetUTF16FirstPart(0)
		return nil
	}

	secondSurrogate := rune(secondValue)

	// Validate and combine surrogate pair - postgres/src/backend/parser/scan.l:692-695
	if !isUTF16SurrogateSecond(secondSurrogate) {
		_ = ctx.AddErrorWithType(InvalidUnicodeSurrogatePair, "invalid Unicode surrogate pair")
		ctx.SetUTF16FirstPart(0)
		return nil
	}

	// Combine surrogates into final code point
	combinedCodepoint := surrogatePairToCodepoint(ctx.UTF16FirstPart(), secondSurrogate)

	// Add combined character to literal - equivalent to addunicode() call
	if utf8.ValidRune(combinedCodepoint) {
		ctx.AddLiteral(string(combinedCodepoint))
	} else {
		_ = ctx.AddErrorWithType(InvalidUnicodeEscape, "invalid Unicode code point")
	}

	// Clear first part - postgres/src/backend/parser/scan.l:698
	ctx.SetUTF16FirstPart(0)

	return nil
}
