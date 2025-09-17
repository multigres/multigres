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
 * PostgreSQL Parser Lexer - Core Lexer Implementation
 *
 * This file implements the core lexer structure and interface for the
 * PostgreSQL-compatible lexer. This provides the foundation for the
 * complete lexical analysis system.
 * Ported from postgres/src/backend/parser/scan.l and postgres/src/include/parser/scanner.h
 */

package parser

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/multigres/multigres/go/parser/ast"
)

// Lexer represents the main lexer instance
// Equivalent to PostgreSQL's scanner state management
type Lexer struct {
	context   *ParseContext // Thread-safe unified context
	parseTree []ast.Stmt    // Parse tree result from parsing
}

// Import ast package (will be added at the top)
// "github.com/manangupta/multigres/go/parser/ast"

// NewLexer creates a new PostgreSQL-compatible lexer instance
// Equivalent to postgres/src/backend/parser/scan.l:1404 (scanner_init function)
func NewLexer(input string) *Lexer {
	return &Lexer{
		context: NewParseContext(input, nil),
	}
}

// NextToken returns the next token from the input stream
// This is the main lexer interface, equivalent to PostgreSQL's base_yylex
// Implements the same token filtering logic as PostgreSQL's base_yylex function
// Equivalent to postgres/src/backend/parser/parser.c:110-324 (base_yylex function)
func (l *Lexer) NextToken() *Token {
	token, err := l.nextTokenInternal()
	if err != nil {
		l.RecordError(err)
		return NewToken(INVALID, l.context.CurrentPosition(), "")
	}

	// Apply PostgreSQL's base_yylex token filtering
	// This handles both keyword lookahead (WITH_LA, etc.) and Unicode token conversion (UIDENT→IDENT, USCONST→SCONST)
	return l.applyTokenFilter(token)
}

// applyTokenFilter implements PostgreSQL's base_yylex token filtering logic
// This function handles both keyword lookahead transformations and Unicode token processing
// Equivalent to postgres/src/backend/parser/parser.c:110-324 (base_yylex function)
func (l *Lexer) applyTokenFilter(token *Token) *Token {
	// First level: Check if this token needs special processing
	// Based on PostgreSQL parser.c lines 138-161 and 252-254
	switch token.Type {
	case FORMAT, NOT, NULLS_P, WITH, WITHOUT:
		// Keywords that need lookahead - handle via existing checkLookaheadToken
		if token.Value.Keyword != "" {
			keyword := LookupKeyword(token.Value.Keyword)
			if keyword != nil {
				newType := l.checkLookaheadToken(keyword, token.Text)
				if newType != token.Type {
					return NewKeywordToken(newType, keyword.Name, token.Position, token.Text)
				}
			}
		}
		return token

	case UIDENT, USCONST:
		// Unicode tokens that need conversion - implement PostgreSQL's UIDENT/USCONST processing
		// Based on PostgreSQL parser.c lines 252-321
		return l.processUnicodeToken(token)

	default:
		// No special processing needed
		return token
	}
}

// processUnicodeToken handles UIDENT and USCONST token conversion following PostgreSQL's logic
// Implements the same logic as PostgreSQL's base_yylex function for UIDENT/USCONST cases
// Equivalent to postgres/src/backend/parser/parser.c:252-321
func (l *Lexer) processUnicodeToken(token *Token) *Token {
	// Look ahead for UESCAPE token following PostgreSQL's pattern
	// postgres/src/backend/parser/parser.c:255-256
	nextToken := l.peekNextTokenType()

	var escapeChar byte = '\\' // Default escape character (postgres/src/backend/parser/parser.c:303)
	consumeUescape := false

	if nextToken == UESCAPE {
		// Found UESCAPE - need to get the third token which should be SCONST
		// postgres/src/backend/parser/parser.c:257-279
		escapeStr, err := l.peekUescapeValue()
		if err != nil {
			// Error getting UESCAPE value - use default escape character
			l.RecordError(err)
		} else if len(escapeStr) != 1 || !l.isValidUescapeChar(escapeStr[0]) {
			// Invalid UESCAPE value - record error but continue with default
			l.RecordError(fmt.Errorf("invalid Unicode escape character"))
		} else {
			escapeChar = escapeStr[0]
			consumeUescape = true
		}
	}

	// Apply Unicode decoding using the escape character
	// postgres/src/backend/parser/parser.c:284-306
	decodedValue, err := l.decodeUnicodeString(token.Value.Str, escapeChar)
	if err != nil {
		l.RecordError(err)
		decodedValue = token.Value.Str // Fall back to original value
	}

	// If we found and processed UESCAPE, consume those tokens
	if consumeUescape {
		l.consumeUescapeTokens()
	}

	// Convert token type and apply identifier truncation if needed
	// postgres/src/backend/parser/parser.c:308-320
	if token.Type == UIDENT {
		// Truncate identifier following PostgreSQL rules
		// postgres/src/backend/parser/parser.c:310-314
		truncatedIdent := l.truncateIdentifier(decodedValue)
		return NewStringToken(IDENT, truncatedIdent, token.Position, token.Text)
	} else { // USCONST
		// Convert to regular string constant
		// postgres/src/backend/parser/parser.c:316-319
		return NewStringToken(SCONST, decodedValue, token.Position, token.Text)
	}
}

// peekUescapeValue peeks ahead to get the UESCAPE value (third token)
// Returns the escape string from the SCONST that follows UESCAPE
// Equivalent to postgres/src/backend/parser/parser.c:268-276
func (l *Lexer) peekUescapeValue() (string, error) {
	// Save current position to restore later
	savedPos := l.context.currentPosition
	savedScanPos := l.context.scanPos

	defer func() {
		// Restore position after lookahead
		l.context.currentPosition = savedPos
		l.context.scanPos = savedScanPos
	}()

	// Skip whitespace to find UESCAPE keyword
	l.skipWhitespaceForLookahead()

	// Skip the UESCAPE keyword
	ident := l.peekNextIdentifier()
	if strings.ToLower(ident) != "uescape" {
		return "", fmt.Errorf("expected UESCAPE keyword")
	}
	l.skipIdentifier()

	// Skip whitespace to find the string literal
	l.skipWhitespaceForLookahead()

	// Check for string literal (SCONST)
	b, ok := l.context.CurrentByte()
	if !ok || b != '\'' {
		return "", fmt.Errorf("UESCAPE must be followed by a simple string literal")
	}

	// Parse the string literal to get the escape character
	token, err := l.nextTokenInternal()
	if err != nil {
		return "", err
	}

	if token.Type != SCONST {
		return "", fmt.Errorf("UESCAPE must be followed by a simple string literal")
	}

	return token.Value.Str, nil
}

// skipIdentifier skips over an identifier for lookahead purposes
func (l *Lexer) skipIdentifier() {
	// Skip first character
	b, ok := l.context.CurrentByte()
	if !ok || !IsIdentStart(b) {
		return
	}
	l.context.NextByte()

	// Skip rest of identifier
	for {
		b, ok := l.context.CurrentByte()
		if !ok || !IsIdentCont(b) {
			break
		}
		l.context.NextByte()
	}
}

// consumeUescapeTokens consumes the UESCAPE keyword and string value tokens
// This is called when we've confirmed the UESCAPE sequence is valid
func (l *Lexer) consumeUescapeTokens() {
	// Skip whitespace and consume UESCAPE keyword
	_ = l.skipWhitespace()
	ident := l.peekNextIdentifier()
	if strings.ToLower(ident) == "uescape" {
		l.skipIdentifier()
	}

	// Skip whitespace and consume the string literal
	_ = l.skipWhitespace()
	if b, ok := l.context.CurrentByte(); ok && b == '\'' {
		// Parse and consume the string literal
		token, _ := l.nextTokenInternal()
		_ = token // Consume the SCONST token
	}
}

// isValidUescapeChar checks if a character is valid as a Unicode escape character
// Equivalent to postgres/src/backend/parser/parser.c:351-362 (check_uescapechar)
func (l *Lexer) isValidUescapeChar(escape byte) bool {
	// Invalid characters: hex digits, +, ', ", whitespace
	if (escape >= '0' && escape <= '9') ||
		(escape >= 'a' && escape <= 'f') ||
		(escape >= 'A' && escape <= 'F') ||
		escape == '+' ||
		escape == '\'' ||
		escape == '"' ||
		unicode.IsSpace(rune(escape)) {
		return false
	}
	return true
}

// decodeUnicodeString decodes Unicode escape sequences in a string
// Implements the same logic as PostgreSQL's str_udeescape function
// Equivalent to postgres/src/backend/parser/parser.c:371-527 (str_udeescape)
func (l *Lexer) decodeUnicodeString(input string, escapeChar byte) (string, error) {
	if len(input) == 0 {
		return input, nil
	}

	result := make([]byte, 0, len(input))
	i := 0

	for i < len(input) {
		if input[i] == escapeChar {
			if i+1 < len(input) && input[i+1] == escapeChar {
				// Escaped escape character
				result = append(result, escapeChar)
				i += 2
			} else if i+4 < len(input) && l.isHexSequence(input[i+1:i+5]) {
				// 4-digit hex escape: \XXXX
				codepoint, err := l.parseHexCodepoint(input[i+1 : i+5])
				if err != nil {
					return "", err
				}
				utf8Bytes := l.codepointToUTF8(codepoint)
				result = append(result, utf8Bytes...)
				i += 5
			} else if i+7 < len(input) && input[i+1] == '+' && l.isHexSequence(input[i+2:i+8]) {
				// 6-digit hex escape: \+XXXXXX
				codepoint, err := l.parseHexCodepoint(input[i+2 : i+8])
				if err != nil {
					return "", err
				}
				utf8Bytes := l.codepointToUTF8(codepoint)
				result = append(result, utf8Bytes...)
				i += 8
			} else {
				return "", fmt.Errorf("invalid Unicode escape sequence")
			}
		} else {
			result = append(result, input[i])
			i++
		}
	}

	return string(result), nil
}

// isHexSequence checks if a string contains only hexadecimal digits
func (l *Lexer) isHexSequence(s string) bool {
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return false
		}
	}
	return true
}

// parseHexCodepoint parses a hex string into a Unicode codepoint
func (l *Lexer) parseHexCodepoint(hex string) (rune, error) {
	var codepoint rune
	for _, c := range hex {
		codepoint <<= 4
		switch {
		case c >= '0' && c <= '9':
			codepoint += rune(c - '0')
		case c >= 'a' && c <= 'f':
			codepoint += rune(c - 'a' + 10)
		case c >= 'A' && c <= 'F':
			codepoint += rune(c - 'A' + 10)
		default:
			return 0, fmt.Errorf("invalid hex digit: %c", c)
		}
	}
	return codepoint, nil
}

// codepointToUTF8 converts a Unicode codepoint to UTF-8 bytes
func (l *Lexer) codepointToUTF8(codepoint rune) []byte {
	if codepoint < 0 || codepoint > 0x10FFFF {
		// Invalid codepoint, return replacement character
		return []byte{0xEF, 0xBF, 0xBD} // UTF-8 for U+FFFD
	}

	result := make([]byte, 4)
	n := len(string(codepoint))
	copy(result[:n], []byte(string(codepoint)))
	return result[:n]
}

// truncateIdentifier truncates an identifier to PostgreSQL's maximum length
// Equivalent to postgres/src/backend/parser/scansup.c:truncate_identifier
func (l *Lexer) truncateIdentifier(ident string) string {
	const NAMEDATALEN = 64 // PostgreSQL's maximum identifier length + 1
	maxLen := NAMEDATALEN - 1

	if len(ident) <= maxLen {
		return ident
	}

	// Truncate and warn (in a real implementation, this would log a warning)
	return ident[:maxLen]
}

// nextTokenInternal is the internal token scanning function
func (l *Lexer) nextTokenInternal() (*Token, error) {
	for {
		// Skip whitespace and comments
		if err := l.skipWhitespace(); err != nil {
			return nil, err
		}

		// Check for end of input
		if l.context.AtEOF() {
			return NewToken(EOF, l.context.CurrentPosition(), ""), nil
		}

		// Record start position for this token - equivalent to SET_YYLLOC()
		// postgres/src/backend/parser/scan.l:105
		startPos := l.context.CurrentPosition()
		startScanPos := l.context.ScanPos()

		// Save position for error reporting (near text extraction)
		l.context.SaveCurrentPosition()

		// State-based dispatch - PostgreSQL uses exclusive states
		// postgres/src/backend/parser/scan.l:175-188
		switch l.context.GetState() {
		case StateInitial:
			return l.scanInitialState(startPos, startScanPos)
		case StateXB:
			return l.scanBitString(startPos, startScanPos)
		case StateXC:
			return l.scanCommentState(startPos, startScanPos)
		case StateXD:
			return l.scanDelimitedIdentifierState(startPos, startScanPos)
		case StateXH:
			return l.scanHexString(startPos, startScanPos)
		case StateXQ:
			return l.scanStandardString(startPos, startScanPos)
		case StateXQS:
			return l.scanQuoteStopState(startPos, startScanPos)
		case StateXE:
			return l.scanExtendedString(startPos, startScanPos)
		case StateXDolQ:
			return l.scanDollarQuotedString(startPos, startScanPos)
		case StateXUI:
			return l.scanUnicodeIdentifierState(startPos, startScanPos)
		case StateXUS:
			return l.scanUnicodeString(startPos, startScanPos) // Unicode strings
		case StateXEU:
			// StateXEU is now handled within string processing, not as a separate token state
			// This should not be reached in normal operation
			return nil, fmt.Errorf("unexpected StateXEU in lexer main loop")
		default:
			return nil, fmt.Errorf("invalid lexer state: %d", l.context.GetState())
		}
	}
}

// scanInitialState handles scanning in the initial (default) state
// Equivalent to postgres/src/backend/parser/scan.l INITIAL state rules
func (l *Lexer) scanInitialState(startPos, startScanPos int) (*Token, error) {
	// Peek at the current character to determine token type
	b, ok := l.context.CurrentByte()
	if !ok {
		return NewToken(EOF, startPos, ""), nil
	}

	// Character-based dispatch following PostgreSQL patterns
	// postgres/src/backend/parser/scan.l:346-900+ (rules section)
	// IMPORTANT: Order matters! Specific character cases must come before general isIdentStart()
	switch {
	// Numbers first - postgres/src/backend/parser/scan.l:430-500+
	case IsDigit(b):
		return l.scanNumber(startPos, startScanPos)

	// Decimal point numbers (.5, .123, etc.) - postgres/src/backend/parser/scan.l:409
	case b == '.':
		nextBytes := l.context.PeekBytes(2)
		// Check for ".." operator FIRST before checking for digits
		if len(nextBytes) >= 2 && nextBytes[1] == '.' {
			// Handle as DOT_DOT operator
			return l.scanOperatorOrPunctuation(startPos, startScanPos)
		}
		if len(nextBytes) >= 2 && IsDigit(nextBytes[1]) {
			return l.scanNumber(startPos, startScanPos)
		}
		// Otherwise, handle as operator/punctuation
		return l.scanOperatorOrPunctuation(startPos, startScanPos)

	// String literals - postgres/src/backend/parser/scan.l:286-318
	case b == '\'':
		l.context.SetState(StateXQ)
		return l.scanStandardString(startPos, startScanPos)

	// Extended string literals (E'...') - postgres/src/backend/parser/scan.l:275
	case b == 'E' || b == 'e':
		if next := l.context.PeekBytes(2); len(next) >= 2 && next[1] == '\'' {
			l.context.SetState(StateXE)
			return l.scanExtendedString(startPos, startScanPos)
		}
		// Otherwise, treat as identifier
		return l.scanIdentifier(startPos, startScanPos)

	// Bit string literals (B'...') - postgres/src/backend/parser/scan.l:264
	case b == 'B' || b == 'b':
		if next := l.context.PeekBytes(2); len(next) >= 2 && next[1] == '\'' {
			l.context.SetState(StateXB)
			return l.scanBitString(startPos, startScanPos)
		}
		// Otherwise, treat as identifier
		return l.scanIdentifier(startPos, startScanPos)

	// Hex string literals (X'...') - postgres/src/backend/parser/scan.l:268
	case b == 'X' || b == 'x':
		if next := l.context.PeekBytes(2); len(next) >= 2 && next[1] == '\'' {
			l.context.SetState(StateXH)
			return l.scanHexString(startPos, startScanPos)
		}
		// Otherwise, treat as identifier
		return l.scanIdentifier(startPos, startScanPos)

	// National character literals (N'...') - postgres/src/backend/parser/scan.l:272
	case b == 'N' || b == 'n':
		if next := l.context.PeekBytes(2); len(next) >= 2 && next[1] == '\'' {
			l.context.NextByte() // consume 'N'
			l.context.SetState(StateXQ)
			// National character strings are treated like regular strings
			return l.scanStandardString(startPos, startScanPos)
		}
		// Otherwise, treat as identifier
		return l.scanIdentifier(startPos, startScanPos)

	// Unicode identifiers (U&"...") - postgres/src/backend/parser/scan.l:315
	case b == 'U' || b == 'u':
		if next := l.context.PeekBytes(3); len(next) >= 3 && next[1] == '&' && next[2] == '"' {
			return l.scanUnicodeIdentifier(startPos, startScanPos)
		} else if len(next) >= 3 && next[1] == '&' && next[2] == '\'' {
			l.context.AdvanceBy(2) // consume 'U&'
			l.context.SetState(StateXUS)
			return l.scanUnicodeString(startPos, startScanPos) // Unicode strings
		}
		// Otherwise, treat as identifier
		return l.scanIdentifier(startPos, startScanPos)

	// Delimited identifiers ("...") - postgres/src/backend/parser/scan.l:309
	case b == '"':
		return l.scanDelimitedIdentifier(startPos, startScanPos)

	// Dollar quoting ($...$) - postgres/src/backend/parser/scan.l:301
	case b == '$':
		return l.scanDollarToken(startPos, startScanPos)

	// Comments and operators - postgres/src/backend/parser/scan.l:342-500+
	case b == '/':
		if next := l.context.PeekBytes(2); len(next) >= 2 && next[1] == '*' {
			return l.scanMultiLineComment(startPos, startScanPos)
		}
		// Otherwise, treat as operator
		return l.scanOperatorOrPunctuation(startPos, startScanPos)

	// Identifiers - MUST come after specific character cases
	// postgres/src/backend/parser/scan.l:346-349
	case IsIdentStart(b):
		return l.scanIdentifier(startPos, startScanPos)

	// Single-character tokens and operators
	default:
		return l.scanOperatorOrPunctuation(startPos, startScanPos)
	}
}

// scanIdentifier scans an identifier or keyword following PostgreSQL rules
// Equivalent to postgres/src/backend/parser/scan.l:346-349 (identifier rule)
func (l *Lexer) scanIdentifier(startPos, startScanPos int) (*Token, error) {
	// PostgreSQL identifier rules: postgres/src/backend/parser/scan.l:346-349
	// ident_start: [A-Za-z\200-\377_]
	// ident_cont: [A-Za-z\200-\377_0-9\$]
	// identifier: {ident_start}{ident_cont}*

	// First character already validated by caller
	l.context.NextByte()

	// Continue with identifier continuation characters
	for {
		b, ok := l.context.CurrentByte()
		if !ok || !IsIdentCont(b) {
			break
		}
		l.context.NextByte()
	}

	text := l.context.GetCurrentText(startScanPos)

	// Check if this is a keyword using the keyword lookup
	if keyword := LookupKeyword(text); keyword != nil {
		// Handle lookahead tokens (WITH_LA, FORMAT_LA, etc.)
		tokenType := l.checkLookaheadToken(keyword, text)
		// Return keyword token with the proper keyword token type and keyword value
		return NewKeywordToken(tokenType, keyword.Name, startPos, text), nil
	}

	// Regular identifier - PostgreSQL lowercases unquoted identifiers
	return NewStringToken(IDENT, normalizeIdentifierCase(text), startPos, text), nil
}

// checkLookaheadToken checks if a keyword should be converted to its lookahead variant
// Based on PostgreSQL's base_yylex function in parser.c
// This implements the same extensible nested switch pattern PostgreSQL uses
func (l *Lexer) checkLookaheadToken(keyword *KeywordInfo, text string) TokenType {
	currentToken := keyword.TokenType

	// First level: Check if this token needs lookahead examination
	// Based on PostgreSQL parser.c lines 138-161
	needsLookahead := false
	switch currentToken {
	case FORMAT:
		needsLookahead = true
	case NOT:
		needsLookahead = true
	case NULLS_P:
		needsLookahead = true
	case WITH:
		needsLookahead = true
	case WITHOUT:
		needsLookahead = true
	default:
		// No lookahead needed for this token
		return currentToken
	}

	if !needsLookahead {
		return currentToken
	}

	// Get the next token for lookahead analysis
	nextToken := l.peekNextTokenType()

	// Second level: Based on current + next token combination, determine if we need *_LA variant
	// Based on PostgreSQL parser.c lines 195-321
	switch currentToken {
	case FORMAT:
		switch nextToken {
		case JSON:
			return FORMAT_LA
		}

	case WITH:
		switch nextToken {
		case TIME, ORDINALITY:
			return WITH_LA
		}

	case NOT:
		switch nextToken {
		case BETWEEN, IN_P, LIKE, ILIKE, SIMILAR:
			return NOT_LA
		}

	case NULLS_P:
		switch nextToken {
		case FIRST_P, LAST_P:
			return NULLS_LA
		}

	case WITHOUT:
		switch nextToken {
		case TIME:
			return WITHOUT_LA
		}
	}

	// No lookahead transformation needed
	return currentToken
}

// peekNextTokenType peeks at the next token type without consuming it
// This is used for lookahead token analysis
func (l *Lexer) peekNextTokenType() TokenType {
	// Save current position to restore later
	savedPos := l.context.currentPosition
	savedScanPos := l.context.scanPos

	defer func() {
		// Restore position after lookahead
		l.context.currentPosition = savedPos
		l.context.scanPos = savedScanPos
	}()

	// Skip whitespace to find next token
	l.skipWhitespaceForLookahead()

	// Try to read the next identifier/keyword
	nextWord := l.peekNextIdentifier()
	if nextWord == "" {
		return INVALID
	}

	// Check if it's a keyword
	if keyword := LookupKeyword(nextWord); keyword != nil {
		return keyword.TokenType
	}

	// Not a keyword, return IDENT
	return IDENT
}

// peekNextIdentifier peeks at the next identifier without consuming it
func (l *Lexer) peekNextIdentifier() string {
	startScanPos := l.context.scanPos

	// Read first character
	b, ok := l.context.CurrentByte()
	if !ok || !IsIdentStart(b) {
		return ""
	}
	l.context.NextByte()

	// Read rest of identifier
	for {
		b, ok := l.context.CurrentByte()
		if !ok || !IsIdentCont(b) {
			break
		}
		l.context.NextByte()
	}

	return l.context.GetCurrentText(startScanPos)
}

// skipWhitespaceForLookahead skips whitespace and comments for lookahead
func (l *Lexer) skipWhitespaceForLookahead() {
	for {
		b, ok := l.context.CurrentByte()
		if !ok {
			break
		}

		if unicode.IsSpace(rune(b)) {
			l.context.NextByte()
			continue
		}

		// Skip single-line comments
		if b == '-' {
			nextBytes := l.context.PeekBytes(2)
			if len(nextBytes) >= 2 && nextBytes[1] == '-' {
				// Skip to end of line
				for {
					b, ok := l.context.CurrentByte()
					if !ok || b == '\n' || b == '\r' {
						break
					}
					l.context.NextByte()
				}
				continue
			}
		}

		// Skip multi-line comments
		if b == '/' {
			nextBytes := l.context.PeekBytes(2)
			if len(nextBytes) >= 2 && nextBytes[1] == '*' {
				l.context.NextByte() // consume '/'
				l.context.NextByte() // consume '*'

				// Skip until */
				for {
					b, ok := l.context.CurrentByte()
					if !ok {
						break
					}
					if b == '*' {
						nextBytes := l.context.PeekBytes(2)
						if len(nextBytes) >= 2 && nextBytes[1] == '/' {
							l.context.NextByte() // consume '*'
							l.context.NextByte() // consume '/'
							break
						}
					}
					l.context.NextByte()
				}
				continue
			}
		}

		// Not whitespace or comment
		break
	}
}

// normalizeIdentifierCase normalizes identifier case following PostgreSQL rules.
// PostgreSQL converts unquoted identifiers to lowercase using ASCII-only conversion.
// Based on postgres/src/backend/parser/scansup.c:downcase_truncate_identifier
func normalizeIdentifierCase(s string) string {
	// PostgreSQL does ASCII-only case conversion for identifiers
	// This is the same logic as normalizeKeywordCase but semantically different
	return normalizeKeywordCase(s)
}

// scanNumber scans numeric literals (integers and floating-point)
// Implements all PostgreSQL numeric literal formats
// postgres/src/backend/parser/scan.l:395-414, 1018-1077
func (l *Lexer) scanNumber(startPos, startScanPos int) (*Token, error) {
	b, _ := l.context.CurrentByte()

	// Handle numbers starting with decimal point (.5, .123, etc.)
	// postgres/src/backend/parser/scan.l:409 - \.{decinteger}
	if b == '.' {
		l.context.NextByte() // consume '.'

		// Scan fractional part
		hasFraction := false
		for {
			b, ok := l.context.CurrentByte()
			if !ok {
				break
			}

			if IsDigit(b) {
				hasFraction = true
				l.context.NextByte()
				continue
			}

			if b == '_' && hasFraction {
				nextBytes := l.context.PeekBytes(2)
				if len(nextBytes) >= 2 && IsDigit(nextBytes[1]) {
					l.context.NextByte()
					continue
				}
			}

			break
		}

		if hasFraction {
			// Check for exponent
			return l.scanExponentPart(startPos, startScanPos, true)
		}

		// Invalid - just a lone dot, shouldn't happen
		text := l.context.GetCurrentText(startScanPos)
		return NewStringToken(FCONST, text, startPos, text), nil
	}

	// Check for special integer formats (hex, octal, binary)
	// postgres/src/backend/parser/scan.l:401-403
	if b == '0' {
		peekAhead := l.context.PeekBytes(4) // Look ahead 4 bytes to handle 0x_X patterns
		if len(peekAhead) >= 2 {
			switch peekAhead[1] {
			case 'x', 'X':
				// Check for hexfail pattern first: 0[xX]_?
				// postgres/src/backend/parser/scan.l:405
				if token, err := l.checkIntegerFailPattern(peekAhead, peekAhead[1], IsHexDigit, "invalid hexadecimal integer", InvalidHexInteger, startPos, startScanPos); token != nil {
					return token, err
				}
				// Hexadecimal: 0[xX](_?{hexdigit})+
				return l.scanSpecialInteger(startPos, startScanPos, IsHexDigit, "invalid hexadecimal integer", InvalidHexInteger)
			case 'o', 'O':
				// Check for octfail pattern first: 0[oO]_?
				// postgres/src/backend/parser/scan.l:406
				if token, err := l.checkIntegerFailPattern(peekAhead, peekAhead[1], IsOctalDigit, "invalid octal integer", InvalidOctalInteger, startPos, startScanPos); token != nil {
					return token, err
				}
				// Octal: 0[oO](_?{octdigit})+
				return l.scanSpecialInteger(startPos, startScanPos, IsOctalDigit, "invalid octal integer", InvalidOctalInteger)
			case 'b', 'B':
				// Check for binfail pattern first: 0[bB]_?
				// postgres/src/backend/parser/scan.l:407
				if token, err := l.checkIntegerFailPattern(peekAhead, peekAhead[1], IsBinaryDigit, "invalid binary integer", InvalidBinaryInteger, startPos, startScanPos); token != nil {
					return token, err
				}
				// Binary: 0[bB](_?{bindigit})+
				return l.scanSpecialInteger(startPos, startScanPos, IsBinaryDigit, "invalid binary integer", InvalidBinaryInteger)
			}
		}
	}

	// Scan decimal part
	// Pattern: {decdigit}(_?{decdigit})*
	// postgres/src/backend/parser/scan.l:400
	hasDigits := false
	for {
		b, ok := l.context.CurrentByte()
		if !ok {
			break
		}

		if IsDigit(b) {
			hasDigits = true
			l.context.NextByte()
			continue
		}

		if b == '_' && hasDigits {
			// PostgreSQL allows underscores in numeric literals for readability
			// Peek ahead to ensure underscore is followed by digit
			nextBytes := l.context.PeekBytes(2)
			if len(nextBytes) >= 2 && IsDigit(nextBytes[1]) {
				l.context.NextByte() // consume underscore
				continue
			}
		}

		// Not a digit or valid underscore - stop scanning
		break
	}

	// Check for decimal point (floating point)
	// postgres/src/backend/parser/scan.l:409
	if b, ok := l.context.CurrentByte(); ok && b == '.' {
		// Check for ".." operator (numericfail pattern)
		// postgres/src/backend/parser/scan.l:410
		nextBytes := l.context.PeekBytes(2)
		if len(nextBytes) >= 2 && nextBytes[1] == '.' {
			// This is ".." operator, not a decimal point
			text := l.context.GetCurrentText(startScanPos)
			return l.processIntegerLiteral(text, startPos), nil
		}

		// Consume decimal point
		l.context.NextByte()

		// Scan fractional part
		hasFraction := false
		for {
			b, ok := l.context.CurrentByte()
			if !ok {
				break
			}

			if IsDigit(b) {
				hasFraction = true
				l.context.NextByte()
				continue
			}

			if b == '_' && hasFraction {
				// Underscores allowed in fractional part too
				nextBytes := l.context.PeekBytes(2)
				if len(nextBytes) >= 2 && IsDigit(nextBytes[1]) {
					l.context.NextByte()
					continue
				}
			}

			// Not a digit or valid underscore in fraction
			break
		}

		// If we have digits before or after decimal point, it's a valid numeric
		if hasDigits || hasFraction {
			// Check for exponent
			return l.scanExponentPart(startPos, startScanPos, true)
		}
	}

	// Check for exponent (scientific notation)
	// postgres/src/backend/parser/scan.l:412
	if hasDigits {
		return l.scanExponentPart(startPos, startScanPos, false)
	}

	// Check for integer_junk pattern BEFORE capturing text
	// postgres/src/backend/parser/scan.l:435, 1066-1076
	_ = l.checkTrailingJunk() // Adds error to context if junk detected

	// Get the text including any trailing junk
	text := l.context.GetCurrentText(startScanPos)

	// Return the integer token (PostgreSQL continues parsing even with junk)
	return NewStringToken(ICONST, text, startPos, text), nil
}

// scanDollarToken scans dollar-related tokens ($1, $tag$, etc.)
// Enhanced in Phase 2C to handle dollar-quoted strings - postgres/src/backend/parser/scan.l:290-320
func (l *Lexer) scanDollarToken(startPos, startScanPos int) (*Token, error) {
	// Check for parameter ($1, $2, etc.)
	if next := l.context.PeekBytes(2); len(next) >= 2 && IsDigit(next[1]) {
		l.context.NextByte() // consume '$'

		paramNum := 0
		for {
			b, ok := l.context.CurrentByte()
			if !ok || !IsDigit(b) {
				break
			}
			l.context.NextByte()
			paramNum = paramNum*10 + int(b-'0')
		}

		// Check for param_junk: $1abc pattern
		// postgres/src/backend/parser/scan.l:438 and :1013-1016
		if b, ok := l.context.CurrentByte(); ok && IsIdentCont(b) {
			// Special case: if the next character is '$' followed by a digit,
			// it's another parameter, not junk
			if b == '$' {
				next := l.context.PeekBytes(2)
				if len(next) >= 2 && IsDigit(next[1]) {
					// This is another parameter ($2, $3, etc.), not junk
					text := l.context.GetCurrentText(startScanPos)
					return NewParamToken(paramNum, startPos, text), nil
				}
			}

			// Continue scanning identifier characters for real junk
			for {
				b, ok := l.context.CurrentByte()
				if !ok || !IsIdentCont(b) {
					break
				}
				l.context.NextByte()
			}
			text := l.context.GetCurrentText(startScanPos)
			_ = l.context.AddErrorWithType(TrailingJunkAfterParameter, "trailing junk after parameter")
			// Return PARAM token despite the error
			return NewParamToken(paramNum, startPos, text), nil
		}

		text := l.context.GetCurrentText(startScanPos)
		return NewParamToken(paramNum, startPos, text), nil
	}

	// Check for potential dollar-quoted string delimiter
	// Look for pattern $tag$ where tag is optional and follows PostgreSQL rules
	if l.isDollarQuoteStart() {
		l.context.SetState(StateXDolQ)
		return l.scanDollarQuotedString(startPos, startScanPos)
	}

	// Single $ character - treat as operator
	l.context.NextByte() // consume '$'
	text := l.context.GetCurrentText(startScanPos)
	return NewStringToken(Op, text, startPos, text), nil
}

// isDollarQuoteStart checks if current position could start a dollar-quoted string
// Equivalent to dolqdelim pattern - postgres/src/backend/parser/scan.l:292
func (l *Lexer) isDollarQuoteStart() bool {
	// Must start with $
	if l.context.CurrentChar() != '$' {
		return false
	}

	// Look ahead to see if this could be a valid delimiter
	next := l.context.PeekToEnd() // Get remaining entire query for dollar-quoted string detection
	if len(next) < 2 {
		return false
	}

	pos := 1
	// Skip optional tag characters - dolq_start followed by dolq_cont
	if pos < len(next) && l.isDollarQuoteTagStart(rune(next[pos])) {
		pos++
		for pos < len(next) && l.isDollarQuoteTagCont(rune(next[pos])) {
			pos++
		}
	}

	// Must end with $ (this completes the opening delimiter)
	if pos >= len(next) || next[pos] != '$' {
		return false
	}

	// Now we have the opening delimiter, extract it
	openingDelimiter := string(next[:pos+1])

	// For "$$", we need to look ahead to see if there's a closing "$$" to determine
	// if this should be treated as a dollar-quoted string or separate operators
	if openingDelimiter == "$$" {
		// Look for closing $$ in the remaining input
		remainingInput := string(next[pos+1:])
		if strings.Contains(remainingInput, "$$") {
			// Found closing delimiter, treat as dollar-quoted string
			return true
		} else {
			// No closing delimiter found, treat as separate operators
			return false
		}
	}

	// We have a valid opening delimiter (e.g., $tag$), so this should be treated
	// as a dollar-quoted string regardless of whether there's a closing delimiter.
	// If no closing delimiter is found, scanDollarQuotedString will handle the error.
	// This matches PostgreSQL's behavior where dolqdelim pattern matches first,
	// then the lexer enters xdolq state to scan for content and closing delimiter.
	return true
}

// isDollarQuoteTagStart checks if character can start a dollar-quote tag
func (l *Lexer) isDollarQuoteTagStart(ch rune) bool {
	return (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || ch == '_' || (ch >= 0x80 && ch <= 0x377)
}

// isDollarQuoteTagCont checks if character can continue a dollar-quote tag
func (l *Lexer) isDollarQuoteTagCont(ch rune) bool {
	return (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_' || (ch >= 0x80 && ch <= 0x377)
}

// scanOperatorOrPunctuation scans operators and punctuation following PostgreSQL rules
// Equivalent to postgres/src/backend/parser/scan.l:380-382 (self and operator rules)
func (l *Lexer) scanOperatorOrPunctuation(startPos, startScanPos int) (*Token, error) {
	b, _ := l.context.NextByte()

	// Special handling for dot sequences (not operator chars in PostgreSQL sense)
	if b == '.' {
		// Handle consecutive dots with multi-dot sequence tracking
		if next, ok := l.context.CurrentByte(); ok && next == '.' {
			if l.context.InMultiDotSequence() {
				// We're already in a multi-dot sequence, treat this as individual dot
				// Fall through to return single dot
			} else {
				// Count total consecutive dots to determine if this starts a multi-dot sequence
				totalDots := 1 // Current dot
				lookahead := l.context.PeekBytes(10)
				for i := 0; i < len(lookahead) && lookahead[i] == '.'; i++ {
					totalDots++
				}

				if totalDots == 2 {
					// Exactly two dots - create DOT_DOT
					l.context.NextByte() // consume the second '.'
					return NewStringToken(DOT_DOT, "..", startPos, ".."), nil
				} else if totalDots >= 3 {
					// Start of multi-dot sequence - mark flag and return first dot
					l.context.SetInMultiDotSequence(true)
					// Fall through to return single dot
				}
			}
		} else {
			// No next dot, clear multi-dot sequence flag
			l.context.SetInMultiDotSequence(false)
		}
		// Check if this is a floating point number (.5, .123, etc.)
		// postgres/src/backend/parser/scan.l:409 - numeric: (({decinteger}\.{decinteger}?)|(\.{decinteger}))
		if next, ok := l.context.CurrentByte(); ok && IsDigit(next) {
			return l.scanNumber(startPos, startScanPos)
		}
		// Single dot - treat as self character
		return NewToken(TokenType(b), startPos, "."), nil
	}

	// Handle special case of colon-based operators (: is not an OpChar)
	if b == ':' {
		if next, ok := l.context.CurrentByte(); ok {
			switch next {
			case ':':
				l.context.NextByte()
				return NewToken(TYPECAST, startPos, "::"), nil
			case '=':
				l.context.NextByte()
				return NewToken(COLON_EQUALS, startPos, ":="), nil
			}
		}
		// Single colon - treat as self character
		return NewToken(TokenType(b), startPos, ":"), nil
	}

	// For operator characters, check if we need to look for multi-character operators
	if IsOpChar(b) {
		// Count consecutive operator chars to determine if this could be multi-char
		opLength := 1 + l.countConsecutiveOpChars()

		// For 2+ character operators, check for known specific operators
		if opLength >= 2 {
			opText := l.getOpText(startScanPos, opLength)

			if opLength == 2 {
				// Two character operators - check for known specific operators
				switch opText {
				case "=>":
					l.context.NextByte() // consume second character
					return NewToken(EQUALS_GREATER, startPos, opText), nil
				case "<=":
					l.context.NextByte()
					return NewToken(LESS_EQUALS, startPos, opText), nil
				case ">=":
					l.context.NextByte()
					return NewToken(GREATER_EQUALS, startPos, opText), nil
				case "<>":
					l.context.NextByte()
					return NewToken(NOT_EQUALS, startPos, opText), nil
				case "!=":
					l.context.NextByte()
					return NewToken(NOT_EQUALS, startPos, opText), nil
				}
			}
			// Unknown 2-char operator or 3 or more characters (like !==, ===) - always use general operator scanning
			return l.scanOperator(startPos, startScanPos)
		}

		// Single character operator - prioritize self-character behavior
		if IsSelfChar(b) {
			return NewToken(TokenType(b), startPos, string(b)), nil
		}
		// Generic single-char operator
		return NewStringToken(Op, string(b), startPos, string(b)), nil
	}

	// Check if this is a "self" character (non-operator single-char token)
	// postgres/src/backend/parser/scan.l:380 - self: [,()\[\].;\:\+\-\*\/\%\^\<\>\=]
	if IsSelfChar(b) {
		text := string(b)
		return NewToken(TokenType(b), startPos, text), nil
	}

	// Fallback to generic operator
	return l.scanOperator(startPos, startScanPos)
}

// countConsecutiveOpChars counts consecutive operator characters from current position
func (l *Lexer) countConsecutiveOpChars() int {
	count := 0
	currentPos := l.context.scanPos

	for currentPos < len(l.context.scanBuf) && IsOpChar(l.context.scanBuf[currentPos]) {
		// Check for comment start sequences and stop before them
		if currentPos+1 < len(l.context.scanBuf) {
			// Check for /* comment start
			if l.context.scanBuf[currentPos] == '/' && l.context.scanBuf[currentPos+1] == '*' {
				break
			}
			// Check for -- comment start
			if l.context.scanBuf[currentPos] == '-' && l.context.scanBuf[currentPos+1] == '-' {
				break
			}
		}

		count++
		currentPos++
	}

	return count
}

// getOpText gets the operator text of specified length from startScanPos
func (l *Lexer) getOpText(startScanPos, length int) string {
	if startScanPos < 0 || startScanPos+length > len(l.context.scanBuf) {
		return ""
	}
	return string(l.context.scanBuf[startScanPos : startScanPos+length])
}

// scanOperator scans a multi-character operator
// Equivalent to postgres/src/backend/parser/scan.l:900-960 (operator rule)
func (l *Lexer) scanOperator(startPos, startScanPos int) (*Token, error) {
	// Continue scanning operator characters
	for {
		b, ok := l.context.CurrentByte()
		if !ok || !IsOpChar(b) {
			break
		}
		l.context.NextByte()
	}

	text := l.context.GetCurrentText(startScanPos)

	// Check for embedded comment starts
	// postgres/src/backend/parser/scan.l:907-920
	truncatedText, hadComment := checkOperatorForCommentStart(text)
	if hadComment {
		// Put back the extra characters
		putBackLen := len(text) - len(truncatedText)
		l.context.PutBack(putBackLen)
		text = truncatedText
	}

	// Return as generic operator
	return NewStringToken(Op, text, startPos, text), nil
}

// skipWhitespace skips whitespace and handles comments following PostgreSQL rules
// Equivalent to postgres/src/backend/parser/scan.l:222-230 (whitespace and comment rules)
func (l *Lexer) skipWhitespace() error {
	for {
		b, ok := l.context.CurrentByte()
		if !ok {
			return nil
		}

		// Handle regular whitespace - postgres/src/backend/parser/scan.l:222
		// space: [ \t\n\r\f\v]
		if IsWhitespace(b) {
			l.context.NextByte()
			// Update line/column tracking for position reporting
			if b == '\n' || b == '\r' {
				l.context.SetLineNumber(l.context.LineNumber() + 1)
				l.context.SetColumnNumber(1)
			} else {
				l.context.SetColumnNumber(l.context.ColumnNumber() + 1)
			}
			continue
		}

		// Handle line comments - postgres/src/backend/parser/scan.l:227
		// comment: ("--"{non_newline}*)
		if b == '-' {
			if next := l.context.PeekBytes(2); len(next) >= 2 && next[1] == '-' {
				// Skip the "--"
				l.context.AdvanceBy(2)
				l.context.SetColumnNumber(l.context.ColumnNumber() + 2)

				// Skip to end of line or EOF
				for {
					b, ok := l.context.CurrentByte()
					if !ok {
						break
					}
					if b == '\n' {
						// Consume the newline and update position tracking
						l.context.NextByte()
						l.context.SetLineNumber(l.context.LineNumber() + 1)
						l.context.SetColumnNumber(1)
						break
					}
					if b == '\r' {
						// Handle \r\n as single newline
						l.context.NextByte()
						if next := l.context.PeekBytes(1); len(next) >= 1 && next[0] == '\n' {
							l.context.NextByte()
						}
						l.context.SetLineNumber(l.context.LineNumber() + 1)
						l.context.SetColumnNumber(1)
						break
					}
					l.context.NextByte()
					l.context.SetColumnNumber(l.context.ColumnNumber() + 1)
				}
				continue
			}
		}

		// No more whitespace to skip
		break
	}

	return nil
}

// GetContext returns the lexer context (for testing and debugging)
func (l *Lexer) GetContext() *ParseContext {
	return l.context
}

// State-based scanner functions - placeholders for future phases
// These implement the PostgreSQL exclusive state scanning

// scanQuoteStopState handles scanning in the xqs state (quote stop detection)
// Placeholder for Phase 2C - postgres/src/backend/parser/scan.l:197
func (l *Lexer) scanQuoteStopState(startPos, startScanPos int) (*Token, error) {
	// For now, return to initial state and scan as operator
	l.context.SetState(StateInitial)
	return l.scanOperatorOrPunctuation(startPos, startScanPos)
}

// Character classification functions are now provided by charclass.go
// This provides optimized lookup table-based classification instead of function calls

// scanExponentPart handles the exponent part of floating point numbers
// postgres/src/backend/parser/scan.l:412-413
func (l *Lexer) scanExponentPart(startPos, startScanPos int, isFloat bool) (*Token, error) {
	b, ok := l.context.CurrentByte()
	if ok && (b == 'e' || b == 'E') {
		// Consume 'e' or 'E'
		l.context.NextByte()

		// Check for optional sign
		b, ok = l.context.CurrentByte()
		if ok && (b == '+' || b == '-') {
			l.context.NextByte()
		}

		// Must have at least one digit after E[+-]
		hasExpDigits := false
		for {
			b, ok := l.context.CurrentByte()
			if !ok {
				break
			}

			if IsDigit(b) {
				hasExpDigits = true
				l.context.NextByte()
				continue
			}

			if b == '_' && hasExpDigits {
				nextBytes := l.context.PeekBytes(2)
				if len(nextBytes) >= 2 && IsDigit(nextBytes[1]) {
					l.context.NextByte()
					continue
				}
			}

			// Not a digit or valid underscore in exponent
			break
		}

		// Check for realfail pattern
		// postgres/src/backend/parser/scan.l:413, 1062-1064
		if !hasExpDigits {
			text := l.context.GetCurrentText(startScanPos)
			_ = l.context.AddErrorWithType(TrailingJunk, "trailing junk after numeric literal")
			// Return as FCONST even if incomplete
			return NewStringToken(FCONST, text, startPos, text), nil
		}

		// Check for real_junk pattern BEFORE capturing text
		_ = l.checkTrailingJunk() // Adds error to context if junk detected

		text := l.context.GetCurrentText(startScanPos)
		return NewStringToken(FCONST, text, startPos, text), nil
	}

	// No exponent
	if isFloat {
		// Check for numeric_junk pattern BEFORE capturing text
		_ = l.checkTrailingJunk() // Adds error to context if junk detected

		text := l.context.GetCurrentText(startScanPos)
		return NewStringToken(FCONST, text, startPos, text), nil
	}

	// Integer - check for trailing junk BEFORE capturing text
	hasTrailingJunk := false
	if err := l.checkTrailingJunk(); err != nil {
		hasTrailingJunk = true
	}

	text := l.context.GetCurrentText(startScanPos)

	if hasTrailingJunk {
		// Error already added to context by checkTrailingJunk
		return NewStringToken(ICONST, text, startPos, text), nil
	}

	return l.processIntegerLiteral(text, startPos), nil
}

// processIntegerLiteral processes an integer literal and returns appropriate token
// Handles decimal, hexadecimal (0x), octal (0o), and binary (0b) integer literals
// with underscore separators and overflow detection like PostgreSQL
// postgres/src/backend/utils/adt/numutils.c:pg_strtoint32_safe
func (l *Lexer) processIntegerLiteral(text string, startPos int) *Token {
	// Try to parse as 32-bit integer first
	val, overflow := l.parseInteger32(text)

	if overflow {
		// Integer too large, treat as float (like PostgreSQL)
		return NewStringToken(FCONST, text, startPos, text)
	}

	// Return as integer constant
	return NewIntToken(int(val), startPos, text)
}

// parseInteger32 parses a string as a 32-bit integer, handling different bases
// Returns the value and whether an overflow occurred
// Based on PostgreSQL's pg_strtoint32_safe implementation
func (l *Lexer) parseInteger32(s string) (int32, bool) {
	if len(s) == 0 {
		return 0, false
	}

	ptr := 0
	neg := false

	// Handle sign
	switch s[ptr] {
	case '-':
		neg = true
		ptr++
	case '+':
		ptr++
	}

	if ptr >= len(s) {
		return 0, false
	}

	var val uint32
	var overflow bool

	// Determine base and parse accordingly
	if ptr+1 < len(s) && s[ptr] == '0' {
		switch s[ptr+1] {
		case 'x', 'X':
			// Hexadecimal
			ptr += 2
			val, overflow = l.parseHexInteger(s, ptr)
		case 'o', 'O':
			// Octal
			ptr += 2
			val, overflow = l.parseOctalInteger(s, ptr)
		case 'b', 'B':
			// Binary
			ptr += 2
			val, overflow = l.parseBinaryInteger(s, ptr)
		default:
			// Regular decimal
			val, overflow = l.parseDecimalInteger(s, ptr)
		}
	} else {
		// Regular decimal
		val, overflow = l.parseDecimalInteger(s, ptr)
	}

	if overflow {
		return 0, true
	}

	// Convert to signed and check bounds
	if neg {
		// Check if negation would overflow
		if val > uint32(-int64(int32(-2147483648))) { // -INT32_MIN
			return 0, true
		}
		return -int32(val), false
	} else {
		if val > 2147483647 { // INT32_MAX
			return 0, true
		}
		return int32(val), false
	}
}

// parseDecimalInteger parses decimal digits with optional underscores
func (l *Lexer) parseDecimalInteger(s string, start int) (uint32, bool) {
	var val uint32
	ptr := start
	hasDigits := false

	for ptr < len(s) {
		c := s[ptr]
		if c >= '0' && c <= '9' {
			// Check for overflow before multiplying
			if val > (0xFFFFFFFF-uint32(c-'0'))/10 {
				return 0, true
			}
			val = val*10 + uint32(c-'0')
			hasDigits = true
			ptr++
		} else if c == '_' {
			// Underscore must be followed by more digits
			ptr++
			if ptr >= len(s) || s[ptr] < '0' || s[ptr] > '9' {
				return 0, false // Invalid syntax
			}
		} else {
			break
		}
	}

	return val, !hasDigits
}

// parseHexInteger parses hexadecimal digits with optional underscores
func (l *Lexer) parseHexInteger(s string, start int) (uint32, bool) {
	var val uint32
	ptr := start
	hasDigits := false

	for ptr < len(s) {
		c := s[ptr]
		var digit uint32

		if c >= '0' && c <= '9' {
			digit = uint32(c - '0')
		} else if c >= 'a' && c <= 'f' {
			digit = uint32(c - 'a' + 10)
		} else if c >= 'A' && c <= 'F' {
			digit = uint32(c - 'A' + 10)
		} else if c == '_' {
			// Underscore must be followed by more hex digits
			ptr++
			if ptr >= len(s) || !isHexDigit(s[ptr]) {
				return 0, false // Invalid syntax
			}
			continue
		} else {
			break
		}

		// Check for overflow before shifting
		if val > 0xFFFFFFFF/16 {
			return 0, true
		}
		val = val*16 + digit
		hasDigits = true
		ptr++
	}

	return val, !hasDigits
}

// parseOctalInteger parses octal digits with optional underscores
func (l *Lexer) parseOctalInteger(s string, start int) (uint32, bool) {
	var val uint32
	ptr := start
	hasDigits := false

	for ptr < len(s) {
		c := s[ptr]
		if c >= '0' && c <= '7' {
			// Check for overflow before shifting
			if val > 0xFFFFFFFF/8 {
				return 0, true
			}
			val = val*8 + uint32(c-'0')
			hasDigits = true
			ptr++
		} else if c == '_' {
			// Underscore must be followed by more octal digits
			ptr++
			if ptr >= len(s) || s[ptr] < '0' || s[ptr] > '7' {
				return 0, false // Invalid syntax
			}
		} else {
			break
		}
	}

	return val, !hasDigits
}

// parseBinaryInteger parses binary digits with optional underscores
func (l *Lexer) parseBinaryInteger(s string, start int) (uint32, bool) {
	var val uint32
	ptr := start
	hasDigits := false

	for ptr < len(s) {
		c := s[ptr]
		switch c {
		case '0', '1':
			// Check for overflow before shifting
			if val > 0xFFFFFFFF/2 {
				return 0, true
			}
			val = val*2 + uint32(c-'0')
			hasDigits = true
			ptr++
		case '_':
			// Underscore must be followed by more binary digits
			ptr++
			if ptr >= len(s) || (s[ptr] != '0' && s[ptr] != '1') {
				return 0, false // Invalid syntax
			}
		default:
			break
		}
	}

	return val, !hasDigits
}

// isHexDigit checks if a character is a valid hexadecimal digit
func isHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

// checkTrailingJunk checks for invalid characters after numeric literals
// Handles PostgreSQL's integer_junk, numeric_junk, and real_junk patterns
// postgres/src/backend/parser/scan.l:435-437, 1066-1076
func (l *Lexer) checkTrailingJunk() error {
	b, ok := l.context.CurrentByte()
	if !ok {
		return nil
	}

	// PostgreSQL's junk patterns: {numeric}{identifier}
	// Any identifier character (including underscore) immediately following
	// a numeric literal is considered trailing junk
	if IsIdentStart(b) {
		// Consume all the junk characters to match PostgreSQL behavior
		// PostgreSQL's junk patterns consume the entire invalid token
		for {
			b, ok := l.context.CurrentByte()
			if !ok {
				break
			}
			if IsIdentCont(b) {
				l.context.NextByte()
				continue
			}
			break
		}
		_ = l.context.AddErrorWithType(TrailingJunk, "trailing junk after numeric literal")
		return fmt.Errorf("trailing junk after numeric literal")
	}
	return nil
}

// checkIntegerFailPattern checks for fail patterns (0x_, 0o_, 0b_) and handles them
// This consolidates the common logic for hexfail, octfail, and binfail patterns
// postgres/src/backend/parser/scan.l:405-407
func (l *Lexer) checkIntegerFailPattern(peekAhead []byte, prefixChar byte, digitChecker func(byte) bool, errorMsg string, errorType LexerErrorType, startPos, startScanPos int) (*Token, error) {
	if len(peekAhead) == 2 ||
		(len(peekAhead) == 3 && peekAhead[2] == '_') ||
		(len(peekAhead) >= 3 && peekAhead[2] == '_' && len(peekAhead) >= 4 && !digitChecker(peekAhead[3])) {

		// This is a fail pattern - consume "0" + prefixChar and optional "_"
		l.context.NextByte() // consume '0'
		l.context.NextByte() // consume prefix char (x/o/b)
		if len(peekAhead) >= 3 && peekAhead[2] == '_' {
			l.context.NextByte() // consume '_'
		}
		text := l.context.GetCurrentText(startScanPos)
		_ = l.context.AddErrorWithType(errorType, errorMsg)
		return NewStringToken(ICONST, text, startPos, text), nil
	}
	return nil, nil // Not a fail pattern, continue with normal processing
}

// scanSpecialInteger consolidates hex/octal/binary integer scanning
// This combines scanHexInteger, scanOctalInteger, and scanBinaryInteger with identical logic
// postgres/src/backend/parser/scan.l:401-403
func (l *Lexer) scanSpecialInteger(startPos, startScanPos int, digitChecker func(byte) bool, errorMsg string, errorType LexerErrorType) (*Token, error) {
	// Consume "0" + prefix char (already done by caller)
	l.context.NextByte() // consume '0'
	l.context.NextByte() // consume prefix char (x/o/b)

	hasDigits := false
	for {
		b, ok := l.context.CurrentByte()
		if !ok {
			break
		}

		if digitChecker(b) {
			hasDigits = true
			l.context.NextByte()
			continue
		}

		if b == '_' {
			// Check underscore is followed by valid digit
			// PostgreSQL pattern: 0[xX](_?{hexdigit})+ allows underscore before any digit
			nextBytes := l.context.PeekBytes(2)
			if len(nextBytes) >= 2 && digitChecker(nextBytes[1]) {
				l.context.NextByte()
				continue
			}
		}

		// Not a valid digit or valid underscore
		break
	}

	// Must have at least one digit (PostgreSQL pattern requires at least one group)
	if !hasDigits {
		text := l.context.GetCurrentText(startScanPos)
		_ = l.context.AddErrorWithType(errorType, errorMsg)
		return NewStringToken(ICONST, text, startPos, text), nil
	}

	text := l.context.GetCurrentText(startScanPos)
	return l.processIntegerLiteral(text, startPos), nil
}

// String returns a string representation of the lexer for debugging
func (l *Lexer) String() string {
	return fmt.Sprintf("Lexer{Context: %s}", l.context.String())
}

// SetParseTree sets the parse tree result
func (l *Lexer) SetParseTree(tree []ast.Stmt) {
	l.parseTree = tree
}

// GetParseTree returns the parse tree result
func (l *Lexer) GetParseTree() []ast.Stmt {
	return l.parseTree
}

// GetPosition returns the current position in the input
func (l *Lexer) GetPosition() int {
	return l.context.CurrentPosition()
}

// RecordError records a parsing error
func (l *Lexer) RecordError(err error) {
	_ = l.context.AddError(err.Error(), l.context.CurrentPosition())
}

// HasErrors returns true if there are any errors
func (l *Lexer) HasErrors() bool {
	return l.context.HasErrors()
}

// GetErrors returns all errors encountered
func (l *Lexer) GetErrors() []error {
	errors := l.context.GetErrors()
	result := make([]error, len(errors))
	for i, e := range errors {
		result[i] = fmt.Errorf("%s", e.Message)
	}
	return result
}
