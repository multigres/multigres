/*
 * PostgreSQL Parser Lexer - Core Lexer Implementation
 *
 * This file implements the core lexer structure and interface for the
 * PostgreSQL-compatible lexer. This provides the foundation for the
 * complete lexical analysis system.
 * Ported from postgres/src/backend/parser/scan.l and postgres/src/include/parser/scanner.h
 */

package lexer

import (
	"fmt"
	"unicode"
)

// Lexer represents the main lexer instance
// Equivalent to PostgreSQL's scanner state management
type Lexer struct {
	context *LexerContext // Thread-safe lexer context
}

// NewLexer creates a new PostgreSQL-compatible lexer instance
// Equivalent to postgres/src/backend/parser/scan.l:1404 (scanner_init function)
func NewLexer(input string) *Lexer {
	return &Lexer{
		context: NewLexerContext(input),
	}
}


// NextToken returns the next token from the input stream
// This is the main lexer interface, equivalent to PostgreSQL's core_yylex
// Equivalent to postgres/src/include/parser/scanner.h:141-142 (core_yylex function)
func (l *Lexer) NextToken() (*Token, error) {
	for {
		// Skip whitespace and comments
		if err := l.skipWhitespace(); err != nil {
			return nil, err
		}

		// Check for end of input
		if l.context.AtEOF() {
			return NewToken(EOF, l.context.CurrentPosition, ""), nil
		}

		// Record start position for this token - equivalent to SET_YYLLOC()
		// postgres/src/backend/parser/scan.l:105
		startPos := l.context.CurrentPosition
		startScanPos := l.context.ScanPos

		// State-based dispatch - PostgreSQL uses exclusive states
		// postgres/src/backend/parser/scan.l:175-188
		switch l.context.State {
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
			return l.scanStandardString(startPos, startScanPos) // Unicode strings treated as standard for now
		case StateXEU:
			// StateXEU is now handled within string processing, not as a separate token state
			// This should not be reached in normal operation
			return nil, fmt.Errorf("unexpected StateXEU in lexer main loop")
		default:
			return nil, fmt.Errorf("invalid lexer state: %d", l.context.State)
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
			return l.scanStandardString(startPos, startScanPos) // Unicode strings treated as standard for now
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
		// Return keyword token with the proper keyword token type and keyword value
		return NewKeywordToken(keyword.TokenType, keyword.Name, startPos, text), nil
	}

	// Regular identifier - PostgreSQL lowercases unquoted identifiers
	return NewStringToken(IDENT, normalizeIdentifierCase(text), startPos, text), nil
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
	l.checkTrailingJunk() // Adds error to context if junk detected

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
			_ = l.context.AddError(TrailingJunkAfterParameter, "trailing junk after parameter")
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
	next := l.context.PeekBytes(200) // Enough lookahead for tag + content + closing tag
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

	// Special case for "$$": treat as two separate operators only when followed by whitespace
	// This handles the ambiguity between "$1 $$" (param + two ops) vs "$$hello$$" (dollar-quoted string)
	if openingDelimiter == "$$" {
		// If we're at the end of input or next character is whitespace, treat as separate operators
		if pos+1 >= len(next) || unicode.IsSpace(rune(next[pos+1])) {
			return false
		}
		// Otherwise, treat as dollar-quoted string (e.g., "$$hello$$")
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

	// Handle multi-character operators first - postgres/src/backend/parser/scan.l:352-368
	switch b {
	case ':':
		if next, ok := l.context.CurrentByte(); ok {
			if next == ':' {
				l.context.NextByte()
				return NewToken(TYPECAST, startPos, "::"), nil
			} else if next == '=' {
				l.context.NextByte()
				return NewToken(COLON_EQUALS, startPos, ":="), nil
			}
		}
	case '=':
		if next, ok := l.context.CurrentByte(); ok && next == '>' {
			l.context.NextByte()
			return NewToken(EQUALS_GREATER, startPos, "=>"), nil
		}
	case '<':
		if next, ok := l.context.CurrentByte(); ok {
			if next == '=' {
				l.context.NextByte()
				return NewToken(LESS_EQUALS, startPos, "<="), nil
			} else if next == '>' {
				l.context.NextByte()
				return NewToken(NOT_EQUALS, startPos, "<>"), nil
			}
		}
	case '>':
		if next, ok := l.context.CurrentByte(); ok && next == '=' {
			l.context.NextByte()
			return NewToken(GREATER_EQUALS, startPos, ">="), nil
		}
	case '!':
		if next, ok := l.context.CurrentByte(); ok && next == '=' {
			l.context.NextByte()
			return NewToken(NOT_EQUALS, startPos, "!="), nil
		}
	case '.':
		// Handle consecutive dots with multi-dot sequence tracking
		if next, ok := l.context.CurrentByte(); ok && next == '.' {
			if l.context.InMultiDotSequence {
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
					l.context.InMultiDotSequence = true
					// Fall through to return single dot
				}
			}
		} else {
			// No next dot, clear multi-dot sequence flag
			l.context.InMultiDotSequence = false
		}
		// Check if this is a floating point number (.5, .123, etc.)
		// postgres/src/backend/parser/scan.l:409 - numeric: (({decinteger}\.{decinteger}?)|(\.{decinteger}))
		if next, ok := l.context.CurrentByte(); ok && IsDigit(next) {
			return l.scanNumber(startPos, startScanPos)
		}
	}

	// Check if this is a "self" character (single-char token)
	// postgres/src/backend/parser/scan.l:380 - self: [,()\[\].;\:\+\-\*\/\%\^\<\>\=]
	if IsSelfChar(b) {
		text := string(b)
		return NewToken(TokenType(b), startPos, text), nil
	}

	// Check if this starts an operator sequence
	// postgres/src/backend/parser/scan.l:381-382 - op_chars: [\~\!\@\#\^\&\|\`\?\+\-\*\/\%\<\>\=]
	if IsOpChar(b) {
		return l.scanOperator(startPos, startScanPos)
	}

	// Unknown character - return as single character
	text := string(b)
	return NewToken(TokenType(b), startPos, text), nil
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
				l.context.LineNumber++
				l.context.ColumnNumber = 1
			} else {
				l.context.ColumnNumber++
			}
			continue
		}

		// Handle line comments - postgres/src/backend/parser/scan.l:227
		// comment: ("--"{non_newline}*)
		if b == '-' {
			if next := l.context.PeekBytes(2); len(next) >= 2 && next[1] == '-' {
				// Skip the "--"
				l.context.AdvanceBy(2)
				l.context.ColumnNumber += 2

				// Skip to end of line or EOF
				for {
					b, ok := l.context.CurrentByte()
					if !ok {
						break
					}
					if b == '\n' {
						// Consume the newline and update position tracking
						l.context.NextByte()
						l.context.LineNumber++
						l.context.ColumnNumber = 1
						break
					}
					if b == '\r' {
						// Handle \r\n as single newline
						l.context.NextByte()
						if next := l.context.PeekBytes(1); len(next) >= 1 && next[0] == '\n' {
							l.context.NextByte()
						}
						l.context.LineNumber++
						l.context.ColumnNumber = 1
						break
					}
					l.context.NextByte()
					l.context.ColumnNumber++
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
func (l *Lexer) GetContext() *LexerContext {
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
			_ = l.context.AddError(TrailingJunk, "trailing junk after numeric literal")
			// Return as FCONST even if incomplete
			return NewStringToken(FCONST, text, startPos, text), nil
		}

		// Check for real_junk pattern BEFORE capturing text
		l.checkTrailingJunk() // Adds error to context if junk detected

		text := l.context.GetCurrentText(startScanPos)
		return NewStringToken(FCONST, text, startPos, text), nil
	}

	// No exponent
	if isFloat {
		// Check for numeric_junk pattern BEFORE capturing text
		l.checkTrailingJunk() // Adds error to context if junk detected

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

// processIntegerLiteral processes an integer literal string
// Equivalent to process_integer_literal - postgres/src/backend/parser/scan.l:1370-1384
func (l *Lexer) processIntegerLiteral(text string, startPos int) *Token {
	// For now, we'll return all integers as ICONST with string value
	// Full integer overflow checking will be implemented later
	// postgres/src/backend/parser/scan.l:1375-1383
	return NewStringToken(ICONST, text, startPos, text)
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
		_ = l.context.AddError(TrailingJunk, "trailing junk after numeric literal")
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
		_ = l.context.AddError(errorType, errorMsg)
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
		_ = l.context.AddError(errorType, errorMsg)
		return NewStringToken(ICONST, text, startPos, text), nil
	}

	text := l.context.GetCurrentText(startScanPos)
	return l.processIntegerLiteral(text, startPos), nil
}

// String returns a string representation of the lexer for debugging
func (l *Lexer) String() string {
	return fmt.Sprintf("Lexer{Context: %s}", l.context.String())
}
