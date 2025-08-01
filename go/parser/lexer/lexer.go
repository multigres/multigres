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
			return l.scanExtendedString(startPos, startScanPos) // Extended Unicode strings treated as extended for now
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
	case isDigit(b):
		return l.scanNumber(startPos, startScanPos)

	// Decimal point numbers (.5, .123, etc.) - postgres/src/backend/parser/scan.l:409
	case b == '.':
		nextBytes := l.context.PeekBytes(2)
		// Check for ".." operator FIRST before checking for digits
		if len(nextBytes) >= 2 && nextBytes[1] == '.' {
			// Handle as DOT_DOT operator
			return l.scanOperatorOrPunctuation(startPos, startScanPos)
		}
		if len(nextBytes) >= 2 && isDigit(nextBytes[1]) {
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
			l.context.AdvanceBy(2) // consume 'U&'
			l.context.SetState(StateXUI)
			return l.scanUnicodeIdentifierState(startPos, startScanPos)
		} else if len(next) >= 3 && next[1] == '&' && next[2] == '\'' {
			l.context.AdvanceBy(2) // consume 'U&'
			l.context.SetState(StateXUS)
			return l.scanStandardString(startPos, startScanPos) // Unicode strings treated as standard for now
		}
		// Otherwise, treat as identifier
		return l.scanIdentifier(startPos, startScanPos)

	// Delimited identifiers ("...") - postgres/src/backend/parser/scan.l:309
	case b == '"':
		l.context.SetState(StateXD)
		return l.scanDelimitedIdentifierState(startPos, startScanPos)

	// Dollar quoting ($...$) - postgres/src/backend/parser/scan.l:301
	case b == '$':
		return l.scanDollarToken(startPos, startScanPos)

	// Comments and operators - postgres/src/backend/parser/scan.l:342-500+
	case b == '/':
		if next := l.context.PeekBytes(2); len(next) >= 2 && next[1] == '*' {
			l.context.SetState(StateXC)
			return l.scanCommentState(startPos, startScanPos)
		}
		// Otherwise, treat as operator
		return l.scanOperatorOrPunctuation(startPos, startScanPos)

	// Identifiers - MUST come after specific character cases
	// postgres/src/backend/parser/scan.l:346-349
	case isIdentStart(b):
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
		if !ok || !isIdentCont(b) {
			break
		}
		l.context.NextByte()
	}

	text := l.context.GetCurrentText(startScanPos)

	// Check if this is a keyword using the integrated keyword lookup
	if keyword := LookupKeyword(text); keyword != nil {
		// Return keyword token with proper Keyword field set
		// In Phase 3, this will return the proper keyword token type instead of IDENT
		return NewKeywordToken(keyword.Name, startPos, text), nil
	}

	// Regular identifier
	return NewStringToken(IDENT, text, startPos, text), nil
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

			if isDigit(b) {
				hasFraction = true
				l.context.NextByte()
				continue
			}

			if b == '_' && hasFraction {
				nextBytes := l.context.PeekBytes(2)
				if len(nextBytes) >= 2 && isDigit(nextBytes[1]) {
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
				if len(peekAhead) == 2 ||
					(len(peekAhead) == 3 && peekAhead[2] == '_') ||
					(len(peekAhead) >= 3 && peekAhead[2] == '_' && len(peekAhead) >= 4 && !isHexDigit(peekAhead[3])) {
					// This is hexfail - consume "0x" and optional "_"
					l.context.NextByte() // consume '0'
					l.context.NextByte() // consume 'x'
					if len(peekAhead) >= 3 && peekAhead[2] == '_' {
						l.context.NextByte() // consume '_'
					}
					text := l.context.GetCurrentText(startScanPos)
					l.context.AddError("invalid hexadecimal integer")
					return NewStringToken(ICONST, text, startPos, text), nil
				}
				// Hexadecimal: 0[xX](_?{hexdigit})+
				return l.scanHexInteger(startPos, startScanPos)
			case 'o', 'O':
				// Check for octfail pattern first: 0[oO]_?
				// postgres/src/backend/parser/scan.l:406
				if len(peekAhead) == 2 ||
					(len(peekAhead) == 3 && peekAhead[2] == '_') ||
					(len(peekAhead) >= 3 && peekAhead[2] == '_' && len(peekAhead) >= 4 && !isOctalDigit(peekAhead[3])) {
					// This is octfail - consume "0o" and optional "_"
					l.context.NextByte() // consume '0'
					l.context.NextByte() // consume 'o'
					if len(peekAhead) >= 3 && peekAhead[2] == '_' {
						l.context.NextByte() // consume '_'
					}
					text := l.context.GetCurrentText(startScanPos)
					l.context.AddError("invalid octal integer")
					return NewStringToken(ICONST, text, startPos, text), nil
				}
				// Octal: 0[oO](_?{octdigit})+
				return l.scanOctalInteger(startPos, startScanPos)
			case 'b', 'B':
				// Check for binfail pattern first: 0[bB]_?
				// postgres/src/backend/parser/scan.l:407
				if len(peekAhead) == 2 ||
					(len(peekAhead) == 3 && peekAhead[2] == '_') ||
					(len(peekAhead) >= 3 && peekAhead[2] == '_' && len(peekAhead) >= 4 && !isBinaryDigit(peekAhead[3])) {
					// This is binfail - consume "0b" and optional "_"
					l.context.NextByte() // consume '0'
					l.context.NextByte() // consume 'b'
					if len(peekAhead) >= 3 && peekAhead[2] == '_' {
						l.context.NextByte() // consume '_'
					}
					text := l.context.GetCurrentText(startScanPos)
					l.context.AddError("invalid binary integer")
					return NewStringToken(ICONST, text, startPos, text), nil
				}
				// Binary: 0[bB](_?{bindigit})+
				return l.scanBinaryInteger(startPos, startScanPos)
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

		if isDigit(b) {
			hasDigits = true
			l.context.NextByte()
			continue
		}

		if b == '_' && hasDigits {
			// PostgreSQL allows underscores in numeric literals for readability
			// Peek ahead to ensure underscore is followed by digit
			nextBytes := l.context.PeekBytes(2)
			if len(nextBytes) >= 2 && isDigit(nextBytes[1]) {
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

			if isDigit(b) {
				hasFraction = true
				l.context.NextByte()
				continue
			}

			if b == '_' && hasFraction {
				// Underscores allowed in fractional part too
				nextBytes := l.context.PeekBytes(2)
				if len(nextBytes) >= 2 && isDigit(nextBytes[1]) {
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

// scanDelimitedIdentifier scans a delimited identifier ("identifier")
// This is a placeholder implementation that will be expanded in Phase 2E
func (l *Lexer) scanDelimitedIdentifier(startPos, startScanPos int) (*Token, error) {
	// Consume opening quote
	l.context.NextByte()

	for {
		b, ok := l.context.NextByte()
		if !ok {
			l.context.AddError("unterminated delimited identifier")
			text := l.context.GetCurrentText(startScanPos)
			return NewStringToken(IDENT, text, startPos, text), nil
		}

		if b == '"' {
			// Check for doubled quote
			if next, ok := l.context.PeekByte(); ok && next == '"' {
				l.context.NextByte() // Consume second quote
				continue
			}
			// End of identifier
			break
		}
	}

	text := l.context.GetCurrentText(startScanPos)
	// Extract content between quotes - simplified for now
	if len(text) >= 2 {
		content := text[1 : len(text)-1]
		return NewStringToken(IDENT, content, startPos, text), nil
	}

	return NewStringToken(IDENT, "", startPos, text), nil
}

// scanDollarToken scans dollar-related tokens ($1, $tag$, etc.)
// Enhanced in Phase 2C to handle dollar-quoted strings - postgres/src/backend/parser/scan.l:290-320
func (l *Lexer) scanDollarToken(startPos, startScanPos int) (*Token, error) {
	// Check for parameter ($1, $2, etc.)
	if next := l.context.PeekBytes(2); len(next) >= 2 && isDigit(next[1]) {
		l.context.NextByte() // consume '$'

		paramNum := 0
		for {
			b, ok := l.context.CurrentByte()
			if !ok || !isDigit(b) {
				break
			}
			l.context.NextByte()
			paramNum = paramNum*10 + int(b-'0')
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
	next := l.context.PeekBytes(50) // Enough lookahead for tag + $
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

	// Must end with $
	return pos < len(next) && next[pos] == '$'
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
		if next, ok := l.context.PeekByte(); ok {
			if next == ':' {
				l.context.NextByte()
				return NewToken(TYPECAST, startPos, "::"), nil
			} else if next == '=' {
				l.context.NextByte()
				return NewToken(COLON_EQUALS, startPos, ":="), nil
			}
		}
	case '=':
		if next, ok := l.context.PeekByte(); ok && next == '>' {
			l.context.NextByte()
			return NewToken(EQUALS_GREATER, startPos, "=>"), nil
		}
	case '<':
		if next, ok := l.context.PeekByte(); ok {
			if next == '=' {
				l.context.NextByte()
				return NewToken(LESS_EQUALS, startPos, "<="), nil
			} else if next == '>' {
				l.context.NextByte()
				return NewToken(NOT_EQUALS, startPos, "<>"), nil
			}
		}
	case '>':
		if next, ok := l.context.PeekByte(); ok && next == '=' {
			l.context.NextByte()
			return NewToken(GREATER_EQUALS, startPos, ">="), nil
		}
	case '!':
		if next, ok := l.context.PeekByte(); ok && next == '=' {
			l.context.NextByte()
			return NewToken(NOT_EQUALS, startPos, "!="), nil
		}
	case '.':
		// Check for ".." operator first
		// Note: we already consumed the first '.', so we need to check if the next byte is also '.'
		if next, ok := l.context.PeekByte(); ok && next == '.' {
			l.context.NextByte() // consume the second '.'
			return NewStringToken(DOT_DOT, "..", startPos, ".."), nil
		}
		// Check if this is a floating point number (.5, .123, etc.)
		// postgres/src/backend/parser/scan.l:409 - numeric: (({decinteger}\.{decinteger}?)|(\.{decinteger}))
		if next, ok := l.context.PeekByte(); ok && isDigit(next) {
			return l.scanNumber(startPos, startScanPos)
		}
	}

	// Check if this is a "self" character (single-char token)
	// postgres/src/backend/parser/scan.l:380 - self: [,()\[\].;\:\+\-\*\/\%\^\<\>\=]
	if isSelfChar(b) {
		text := string(b)
		return NewToken(TokenType(b), startPos, text), nil
	}

	// Check if this starts an operator sequence
	// postgres/src/backend/parser/scan.l:381-382 - op_chars: [\~\!\@\#\^\&\|\`\?\+\-\*\/\%\<\>\=]
	if isOpChar(b) {
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
		if !ok || !isOpChar(b) {
			break
		}
		l.context.NextByte()
	}

	text := l.context.GetCurrentText(startScanPos)

	// Check for embedded comment starts (simplified for Phase 2B)
	// Full implementation of postgres/src/backend/parser/scan.l:907-920 will be in Phase 2E

	// For now, return as generic operator
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
		if isWhitespace(b) {
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
					if !ok || b == '\n' || b == '\r' {
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

// scanCommentState handles scanning in the xc state (C-style comments)
// Placeholder for Phase 2E - postgres/src/backend/parser/scan.l:193
func (l *Lexer) scanCommentState(startPos, startScanPos int) (*Token, error) {
	// For now, return to initial state and scan as operator
	l.context.SetState(StateInitial)
	return l.scanOperatorOrPunctuation(startPos, startScanPos)
}

// scanDelimitedIdentifierState handles scanning in the xd state (delimited identifiers)
// Placeholder for Phase 2E - postgres/src/backend/parser/scan.l:194
func (l *Lexer) scanDelimitedIdentifierState(startPos, startScanPos int) (*Token, error) {
	// For now, return to initial state and use existing implementation
	l.context.SetState(StateInitial)
	return l.scanDelimitedIdentifier(startPos, startScanPos)
}

// scanQuoteStopState handles scanning in the xqs state (quote stop detection)
// Placeholder for Phase 2C - postgres/src/backend/parser/scan.l:197
func (l *Lexer) scanQuoteStopState(startPos, startScanPos int) (*Token, error) {
	// For now, return to initial state and scan as operator
	l.context.SetState(StateInitial)
	return l.scanOperatorOrPunctuation(startPos, startScanPos)
}

// scanUnicodeIdentifierState handles scanning in the xui state (Unicode identifiers)
// Basic implementation for Phase 2B - full implementation in Phase 2I
// postgres/src/backend/parser/scan.l:200
func (l *Lexer) scanUnicodeIdentifierState(startPos, startScanPos int) (*Token, error) {
	// Consume opening quote
	l.context.NextByte()

	// Scan Unicode identifier content - simplified for Phase 2B
	for {
		b, ok := l.context.NextByte()
		if !ok {
			l.context.AddError("unterminated Unicode identifier")
			text := l.context.GetCurrentText(startScanPos)
			l.context.SetState(StateInitial)
			return NewStringToken(IDENT, text, startPos, text), nil
		}

		if b == '"' {
			// Check for doubled quote
			if next, ok := l.context.PeekByte(); ok && next == '"' {
				l.context.NextByte() // Consume second quote
				continue
			}
			// End of identifier
			break
		}
	}

	text := l.context.GetCurrentText(startScanPos)
	l.context.SetState(StateInitial)
	return NewStringToken(IDENT, text, startPos, text), nil
}

// Utility functions following PostgreSQL character classification
// postgres/src/backend/parser/scan.l:346-349

// isIdentStart checks if a byte can start an identifier
// Equivalent to PostgreSQL's ident_start: [A-Za-z\200-\377_]
// postgres/src/backend/parser/scan.l:346
func isIdentStart(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || b == '_' || b >= 0x80
}

// isIdentCont checks if a byte can continue an identifier
// Equivalent to PostgreSQL's ident_cont: [A-Za-z\200-\377_0-9\$]
// postgres/src/backend/parser/scan.l:347
func isIdentCont(b byte) bool {
	return isIdentStart(b) || isDigit(b) || b == '$'
}

// isDigit checks if a byte is a decimal digit
func isDigit(b byte) bool {
	return b >= '0' && b <= '9'
}

// isAlpha checks if a byte is alphabetic (for backward compatibility)
func isAlpha(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

// isAlphaNumeric checks if a byte is alphanumeric (for backward compatibility)
func isAlphaNumeric(b byte) bool {
	return isAlpha(b) || isDigit(b)
}

// isWhitespace checks if a byte is whitespace
// Equivalent to PostgreSQL's space: [ \t\n\r\f\v]
// postgres/src/backend/parser/scan.l:222
func isWhitespace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r' || b == '\f' || b == '\v'
}

// isHexDigit checks if a byte is a hexadecimal digit
func isHexDigit(b byte) bool {
	return isDigit(b) || (b >= 'a' && b <= 'f') || (b >= 'A' && b <= 'F')
}

// isOctalDigit checks if a byte is an octal digit
func isOctalDigit(b byte) bool {
	return b >= '0' && b <= '7'
}

// isBinaryDigit checks if a byte is a binary digit
func isBinaryDigit(b byte) bool {
	return b == '0' || b == '1'
}

// isSelfChar checks if a byte is a PostgreSQL "self" character (single-char token)
// Equivalent to PostgreSQL's self: [,()\[\].;\:\+\-\*\/\%\^\<\>\=]
// postgres/src/backend/parser/scan.l:380
func isSelfChar(b byte) bool {
	switch b {
	case ',', '(', ')', '[', ']', '.', ';', ':', '+', '-', '*', '/', '%', '^', '<', '>', '=':
		return true
	default:
		return false
	}
}

// isOpChar checks if a byte can be part of a PostgreSQL operator
// Equivalent to PostgreSQL's op_chars: [\~\!\@\#\^\&\|\`\?\+\-\*\/\%\<\>\=]
// postgres/src/backend/parser/scan.l:381
func isOpChar(b byte) bool {
	switch b {
	case '~', '!', '@', '#', '^', '&', '|', '`', '?', '+', '-', '*', '/', '%', '<', '>', '=':
		return true
	default:
		return false
	}
}

// scanHexInteger scans hexadecimal integer literals
// postgres/src/backend/parser/scan.l:401, 1022-1024
func (l *Lexer) scanHexInteger(startPos, startScanPos int) (*Token, error) {
	// Consume "0x" or "0X"
	l.context.NextByte()
	l.context.NextByte()

	hasDigits := false
	for {
		b, ok := l.context.CurrentByte()
		if !ok {
			break
		}

		if isHexDigit(b) {
			hasDigits = true
			l.context.NextByte()
			continue
		}

		if b == '_' {
			// Check underscore is followed by hex digit
			// PostgreSQL pattern: 0[xX](_?{hexdigit})+ allows underscore before any hex digit
			nextBytes := l.context.PeekBytes(2)
			if len(nextBytes) >= 2 && isHexDigit(nextBytes[1]) {
				l.context.NextByte()
				continue
			}
		}

		// Not a hex digit or valid underscore
		break
	}

	// Must have at least one hex digit (PostgreSQL pattern requires at least one group)
	if !hasDigits {
		text := l.context.GetCurrentText(startScanPos)
		l.context.AddError("invalid hexadecimal integer")
		return NewStringToken(ICONST, text, startPos, text), nil
	}

	text := l.context.GetCurrentText(startScanPos)
	return l.processIntegerLiteral(text, startPos), nil
}

// scanOctalInteger scans octal integer literals
// postgres/src/backend/parser/scan.l:402, 1026-1028
func (l *Lexer) scanOctalInteger(startPos, startScanPos int) (*Token, error) {
	// Consume "0o" or "0O"
	l.context.NextByte()
	l.context.NextByte()

	hasDigits := false
	for {
		b, ok := l.context.CurrentByte()
		if !ok {
			break
		}

		if isOctalDigit(b) {
			hasDigits = true
			l.context.NextByte()
			continue
		}

		if b == '_' {
			// Check underscore is followed by octal digit
			// PostgreSQL pattern: 0[oO](_?{octdigit})+ allows underscore before any octal digit
			nextBytes := l.context.PeekBytes(2)
			if len(nextBytes) >= 2 && isOctalDigit(nextBytes[1]) {
				l.context.NextByte()
				continue
			}
		}

		// Not an octal digit or valid underscore
		break
	}

	// Must have at least one octal digit (PostgreSQL pattern requires at least one group)
	if !hasDigits {
		text := l.context.GetCurrentText(startScanPos)
		l.context.AddError("invalid octal integer")
		return NewStringToken(ICONST, text, startPos, text), nil
	}

	text := l.context.GetCurrentText(startScanPos)
	return l.processIntegerLiteral(text, startPos), nil
}

// scanBinaryInteger scans binary integer literals
// postgres/src/backend/parser/scan.l:403, 1030-1032
func (l *Lexer) scanBinaryInteger(startPos, startScanPos int) (*Token, error) {
	// Consume "0b" or "0B"
	l.context.NextByte()
	l.context.NextByte()

	hasDigits := false
	for {
		b, ok := l.context.CurrentByte()
		if !ok {
			break
		}

		if isBinaryDigit(b) {
			hasDigits = true
			l.context.NextByte()
			continue
		}

		if b == '_' {
			// Check underscore is followed by binary digit
			// PostgreSQL pattern: 0[bB](_?{bindigit})+ allows underscore before any binary digit
			nextBytes := l.context.PeekBytes(2)
			if len(nextBytes) >= 2 && isBinaryDigit(nextBytes[1]) {
				l.context.NextByte()
				continue
			}
		}

		// Not a binary digit or valid underscore
		break
	}

	// Must have at least one binary digit (PostgreSQL pattern requires at least one group)
	if !hasDigits {
		text := l.context.GetCurrentText(startScanPos)
		l.context.AddError("invalid binary integer")
		return NewStringToken(ICONST, text, startPos, text), nil
	}

	text := l.context.GetCurrentText(startScanPos)
	return l.processIntegerLiteral(text, startPos), nil
}

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

			if isDigit(b) {
				hasExpDigits = true
				l.context.NextByte()
				continue
			}

			if b == '_' && hasExpDigits {
				nextBytes := l.context.PeekBytes(2)
				if len(nextBytes) >= 2 && isDigit(nextBytes[1]) {
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
			l.context.AddError("trailing junk after numeric literal")
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
	if isIdentStart(b) {
		// Consume all the junk characters to match PostgreSQL behavior
		// PostgreSQL's junk patterns consume the entire invalid token
		for {
			b, ok := l.context.CurrentByte()
			if !ok {
				break
			}
			if isIdentCont(b) {
				l.context.NextByte()
				continue
			}
			break
		}
		l.context.AddError("trailing junk after numeric literal")
		return fmt.Errorf("trailing junk after numeric literal")
	}
	return nil
}

// String returns a string representation of the lexer for debugging
func (l *Lexer) String() string {
	return fmt.Sprintf("Lexer{Context: %s}", l.context.String())
}
