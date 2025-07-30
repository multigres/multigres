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
	// Skip whitespace and comments
	if err := l.skipWhitespace(); err != nil {
		return nil, err
	}
	
	// Check for end of input
	if l.context.AtEOF() {
		return NewToken(EOF, l.context.CurrentPosition, ""), nil
	}
	
	// Record start position for this token
	startPos := l.context.CurrentPosition
	startScanPos := l.context.ScanPos
	
	// Peek at the current character to determine token type
	b, ok := l.context.PeekByte()
	if !ok {
		return NewToken(EOF, startPos, ""), nil
	}
	
	// Dispatch based on character - this will be expanded in Phase 2B
	switch {
	case isAlpha(b) || b == '_':
		return l.scanIdentifier(startPos, startScanPos)
	case isDigit(b):
		return l.scanNumber(startPos, startScanPos)
	case b == '\'':
		return l.scanStringLiteral(startPos, startScanPos)
	case b == '"':
		return l.scanDelimitedIdentifier(startPos, startScanPos)
	case b == '$':
		return l.scanDollarToken(startPos, startScanPos)
	default:
		return l.scanOperatorOrPunctuation(startPos, startScanPos)
	}
}

// scanIdentifier scans an identifier or keyword
// This is a placeholder implementation that will be expanded in Phase 2B
func (l *Lexer) scanIdentifier(startPos, startScanPos int) (*Token, error) {
	for {
		b, ok := l.context.PeekByte()
		if !ok || (!isAlphaNumeric(b) && b != '_') {
			break
		}
		l.context.NextByte()
	}
	
	text := l.context.GetCurrentText(startScanPos)
	
	// Check if this is a keyword using the integrated keyword lookup
	if keyword := LookupKeyword(text); keyword != nil {
		// Return keyword token - for now all keywords return IDENT
		// In Phase 3, this will return the proper keyword token type
		return NewStringToken(getKeywordTokenType(keyword), keyword.Name, startPos, text), nil
	}
	
	// Regular identifier
	return NewStringToken(IDENT, text, startPos, text), nil
}

// scanNumber scans a numeric literal
// This is a placeholder implementation that will be expanded in Phase 2D
func (l *Lexer) scanNumber(startPos, startScanPos int) (*Token, error) {
	// Simple integer scanning for now
	for {
		b, ok := l.context.PeekByte()
		if !ok || !isDigit(b) {
			break
		}
		l.context.NextByte()
	}
	
	text := l.context.GetCurrentText(startScanPos)
	
	// For now, just return as string - proper numeric parsing in Phase 2D
	return NewStringToken(ICONST, text, startPos, text), nil
}

// scanStringLiteral scans a string literal
// This is a placeholder implementation that will be expanded in Phase 2C
func (l *Lexer) scanStringLiteral(startPos, startScanPos int) (*Token, error) {
	// Consume opening quote
	l.context.NextByte()
	
	// Simple string scanning - full implementation in Phase 2C
	for {
		b, ok := l.context.NextByte()
		if !ok {
			l.context.AddError("unterminated string literal")
			text := l.context.GetCurrentText(startScanPos)
			return NewStringToken(USCONST, text, startPos, text), nil
		}
		
		if b == '\'' {
			// Check for doubled quote
			if next, ok := l.context.PeekByte(); ok && next == '\'' {
				l.context.NextByte() // Consume second quote
				continue
			}
			// End of string
			break
		}
	}
	
	text := l.context.GetCurrentText(startScanPos)
	// Extract content between quotes - simplified for now
	if len(text) >= 2 {
		content := text[1 : len(text)-1]
		return NewStringToken(SCONST, content, startPos, text), nil
	}
	
	return NewStringToken(SCONST, "", startPos, text), nil
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
// This is a placeholder implementation that will be expanded in Phase 2C/2E
func (l *Lexer) scanDollarToken(startPos, startScanPos int) (*Token, error) {
	l.context.NextByte() // consume '$'
	
	// Check for parameter ($1, $2, etc.)
	if b, ok := l.context.PeekByte(); ok && isDigit(b) {
		paramNum := 0
		for {
			b, ok := l.context.PeekByte()
			if !ok || !isDigit(b) {
				break
			}
			l.context.NextByte()
			paramNum = paramNum*10 + int(b-'0')
		}
		
		text := l.context.GetCurrentText(startScanPos)
		return NewParamToken(paramNum, startPos, text), nil
	}
	
	// For now, treat other dollar constructs as operators
	text := l.context.GetCurrentText(startScanPos)
	return NewStringToken(Op, text, startPos, text), nil
}

// scanOperatorOrPunctuation scans operators and punctuation
// This is a placeholder implementation that will be expanded in Phase 2B/2E
func (l *Lexer) scanOperatorOrPunctuation(startPos, startScanPos int) (*Token, error) {
	b, _ := l.context.NextByte()
	
	// Check for multi-character operators
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
		if next, ok := l.context.PeekByte(); ok && next == '.' {
			l.context.NextByte()
			return NewToken(DOT_DOT, startPos, ".."), nil
		}
	}
	
	// Single character token
	text := string(b)
	return NewToken(TokenType(b), startPos, text), nil
}

// skipWhitespace skips whitespace and handles comments
// This is a placeholder implementation that will be expanded in Phase 2E
func (l *Lexer) skipWhitespace() error {
	for {
		b, ok := l.context.PeekByte()
		if !ok {
			return nil
		}
		
		if isWhitespace(b) {
			l.context.NextByte()
			continue
		}
		
		// Handle line comments (-- to end of line)
		if b == '-' {
			if next := l.context.PeekBytes(2); len(next) >= 2 && next[1] == '-' {
				// Skip to end of line
				l.context.AdvanceBy(2)
				for {
					b, ok := l.context.PeekByte()
					if !ok || b == '\n' {
						break
					}
					l.context.NextByte()
				}
				continue
			}
		}
		
		// No more whitespace
		break
	}
	
	return nil
}

// GetContext returns the lexer context (for testing and debugging)
func (l *Lexer) GetContext() *LexerContext {
	return l.context
}

// Utility functions

func isAlpha(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

func isDigit(b byte) bool {
	return b >= '0' && b <= '9'
}

func isAlphaNumeric(b byte) bool {
	return isAlpha(b) || isDigit(b)
}

func isWhitespace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r' || b == '\f'
}

// String returns a string representation of the lexer for debugging
func (l *Lexer) String() string {
	return fmt.Sprintf("Lexer{Context: %s}", l.context.String())
}