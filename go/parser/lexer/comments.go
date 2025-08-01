/*
PostgreSQL Parser Lexer - Comment Handling

This file implements the comment handling functionality for the PostgreSQL-compatible lexer.
It supports single-line (--) and multi-line (/* ... comments with arbitrary nesting.
Ported from postgres/src/backend/parser/scan.l:324-344 and :461-499
*/

package lexer

import "strings"

// scanMultiLineComment handles the initial /* sequence and transitions to comment state
// This is called when we detect /* in the initial state
// Equivalent to postgres/src/backend/parser/scan.l:461-468 (xcstart rule)
func (l *Lexer) scanMultiLineComment(startPos, startScanPos int) (*Token, error) {
	// Set location in case of syntax error in comment
	// Equivalent to SET_YYLLOC() - postgres/src/backend/parser/scan.l:463
	l.context.XCDepth = 0
	l.context.SetState(StateXC)
	
	// Put back any characters past slash-star
	// postgres/src/backend/parser/scan.l:467 - yyless(2)
	// We've already consumed /* so we need to advance by 2
	l.context.AdvanceBy(2) // AdvanceBy handles position tracking
	
	// Continue scanning in comment state
	return l.scanCommentState(startPos, startScanPos)
}

// scanCommentState handles scanning inside multi-line comments (xc state)
// This implements the state machine for nested C-style comments
// Equivalent to postgres/src/backend/parser/scan.l:470-499 (<xc> state rules)
func (l *Lexer) scanCommentState(startPos, startScanPos int) (*Token, error) {
	for {
		b, ok := l.context.CurrentByte()
		if !ok {
			// EOF in comment - postgres/src/backend/parser/scan.l:496-498
			l.context.SetState(StateInitial)
			l.context.AddError("unterminated /* comment")
			return nil, &LexerError{
				Message:  "unterminated /* comment",
				Position: startPos,
				Line:     l.context.LineNumber,
				Column:   l.context.ColumnNumber,
			}
		}
		
		// Check for nested comment start: /* 
		// postgres/src/backend/parser/scan.l:471-475
		if b == '/' {
			next := l.context.PeekBytes(2)
			if len(next) >= 2 && next[1] == '*' {
				// Increment nesting depth
				l.context.XCDepth++
				// Consume only the /* characters - AdvanceBy handles position tracking
				l.context.AdvanceBy(2)
				continue
			}
		}
		
		// Check for comment end: */
		// postgres/src/backend/parser/scan.l:477-482
		if b == '*' {
			next := l.context.PeekBytes(2)
			if len(next) >= 2 && next[1] == '/' {
				if l.context.XCDepth <= 0 {
					// End of outermost comment, return to initial state
					l.context.AdvanceBy(2) // AdvanceBy handles position tracking
					l.context.SetState(StateInitial)
					// Comments are not tokens - continue to next token
					return l.NextToken()
				} else {
					// End of nested comment
					l.context.XCDepth--
					l.context.AdvanceBy(2) // AdvanceBy handles position tracking
					continue
				}
			}
		}
		
		// Regular character inside comment - just skip it
		// postgres/src/backend/parser/scan.l:484-486 (xcinside rule)
		l.context.NextByte() // NextByte() handles position tracking automatically
	}
}

// checkOperatorForCommentStart checks if an operator contains embedded comment starts
// This implements the logic from postgres/src/backend/parser/scan.l:900-937
func checkOperatorForCommentStart(text string) (string, bool) {
	// Define comment start patterns and their minimum search positions
	commentPatterns := []struct {
		pattern  string
		startPos int // Skip checking at position 0 for "--" since that's handled separately
	}{
		{"/*", 0}, // Can appear anywhere in operator
		{"--", 1}, // Don't check at start position (handled separately)
	}
	
	for _, comment := range commentPatterns {
		if pos := strings.Index(text[comment.startPos:], comment.pattern); pos != -1 {
			actualPos := comment.startPos + pos
			return text[:actualPos], true
		}
	}
	
	return text, false
}