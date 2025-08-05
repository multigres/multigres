/*
 * PostgreSQL Parser Lexer - Context Management
 *
 * This file implements the thread-safe lexer context, porting PostgreSQL's
 * core_yy_extra_type structure to Go with elimination of all global state.
 * Ported from postgres/src/include/parser/scanner.h and postgres/src/backend/parser/scan.l
 */

package lexer

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// LexerState represents the current state of the lexer state machine
// Maps to PostgreSQL's exclusive states defined in postgres/src/backend/parser/scan.l:192-202
type LexerState int

const (
	StateInitial LexerState = iota // Default scanning mode (INITIAL) - postgres/src/backend/parser/scan.l:191
	StateXB                        // Bit string literals (%x xb) - postgres/src/backend/parser/scan.l:192
	StateXC                        // C-style comments (%x xc) - postgres/src/backend/parser/scan.l:193
	StateXD                        // Delimited identifiers (%x xd) - postgres/src/backend/parser/scan.l:194
	StateXH                        // Hexadecimal byte strings (%x xh) - postgres/src/backend/parser/scan.l:195
	StateXQ                        // Standard quoted strings (%x xq) - postgres/src/backend/parser/scan.l:196
	StateXQS                       // Quote stop detection (%x xqs) - postgres/src/backend/parser/scan.l:197
	StateXE                        // Extended quoted strings (with escapes) (%x xe) - postgres/src/backend/parser/scan.l:198
	StateXDolQ                     // Dollar-quoted strings (%x xdolq) - postgres/src/backend/parser/scan.l:199
	StateXUI                       // Unicode identifier (%x xui) - postgres/src/backend/parser/scan.l:200
	StateXUS                       // Unicode string (%x xus) - postgres/src/backend/parser/scan.l:201
	StateXEU                       // Extended string with Unicode escapes (%x xeu) - postgres/src/backend/parser/scan.l:202
)

// BackslashQuoteMode represents the backslash_quote GUC setting
// Ported from postgres/src/backend/parser/scan.l:68 and postgres/src/include/utils/guc.h
type BackslashQuoteMode int

const (
	BackslashQuoteOff          BackslashQuoteMode = 0 // postgres/src/include/utils/guc.h (BACKSLASH_QUOTE_OFF)
	BackslashQuoteOn           BackslashQuoteMode = 1 // postgres/src/include/utils/guc.h (BACKSLASH_QUOTE_ON)
	BackslashQuoteSafeEncoding BackslashQuoteMode = 2 // postgres/src/include/utils/guc.h (BACKSLASH_QUOTE_SAFE_ENCODING)
)

// LexerContext represents the complete lexer state, equivalent to PostgreSQL's core_yy_extra_type
// This eliminates all global state and makes the lexer thread-safe
// Ported from postgres/src/include/parser/scanner.h:66-116
type LexerContext struct {
	// Input buffer management
	// Ported from postgres/src/include/parser/scanner.h:69-73
	ScanBuf    []byte // The string being scanned (scanbuf) - postgres/src/include/parser/scanner.h:72
	ScanBufLen int    // Length of scan buffer (scanbuflen) - postgres/src/include/parser/scanner.h:73
	ScanPos    int    // Current position in buffer (internal tracking)

	// Scanner configuration (eliminates global state)
	// These were global variables in postgres/src/backend/parser/scan.l:68-70
	// Ported from postgres/src/include/parser/scanner.h:82-89
	BackslashQuote            BackslashQuoteMode // backslash_quote setting - postgres/src/include/parser/scanner.h:87
	EscapeStringWarning       bool               // escape_string_warning setting - postgres/src/include/parser/scanner.h:88
	StandardConformingStrings bool               // standard_conforming_strings setting - postgres/src/include/parser/scanner.h:89

	// Literal buffer for multi-rule parsing
	// Ported from postgres/src/include/parser/scanner.h:92-100
	LiteralBuf    strings.Builder // Accumulates literal values (literalbuf) - postgres/src/include/parser/scanner.h:98
	LiteralActive bool            // Whether literal buffer is active (internal tracking)

	// Scanner state variables
	// Ported from postgres/src/include/parser/scanner.h:103-109
	State              LexerState // Current lexer state (internal tracking)
	StateBeforeStrStop int        // Start condition before end quote (state_before_str_stop) - postgres/src/include/parser/scanner.h:105
	XCDepth            int        // Nesting depth in slash-star comments (xcdepth) - postgres/src/include/parser/scanner.h:106
	DolQStart          string     // Current $foo$ quote start string (dolqstart) - postgres/src/include/parser/scanner.h:107
	SavePosition       int        // One-element stack for position saving (save_yylloc) - postgres/src/include/parser/scanner.h:108

	// UTF-16 surrogate pair handling
	// Ported from postgres/src/include/parser/scanner.h:111-112
	UTF16FirstPart int32 // First part of UTF16 surrogate pair for Unicode escapes (utf16_first_part) - postgres/src/include/parser/scanner.h:111

	// Warning state for literal lexing
	// Ported from postgres/src/include/parser/scanner.h:114-116
	WarnOnFirstEscape bool // State variable for literal-lexing warnings (warn_on_first_escape) - postgres/src/include/parser/scanner.h:114
	SawNonASCII       bool // Whether non-ASCII characters were seen (saw_non_ascii) - postgres/src/include/parser/scanner.h:115

	// Position tracking (additional for Go implementation)
	CurrentPosition int // Current byte offset for token positions
	LineNumber      int // Current line number (for error reporting)
	ColumnNumber    int // Current column number (for error reporting)

	// Error handling (additional for Go implementation)
	Errors []LexerError // Accumulated lexer errors

	// Dot sequence handling (for handling "..." as individual dots)
	InMultiDotSequence bool // True if we're in a sequence of 3+ dots
}

// Note: KeywordLookup interface removed - keyword functionality is now
// provided directly by lexer package functions (LookupKeyword, IsKeyword, etc.)

// NewLexerContext creates a new thread-safe lexer context
// Reference: PostgreSQL scanner_init function in scan.l
func NewLexerContext(input string) *LexerContext {
	ctx := &LexerContext{
		// Initialize input buffer
		ScanBuf:    []byte(input),
		ScanBufLen: len(input),
		ScanPos:    0,

		// Initialize configuration with PostgreSQL defaults
		// Reference: PostgreSQL scan.l lines 68-70 default values
		BackslashQuote:            BackslashQuoteSafeEncoding,
		EscapeStringWarning:       true,
		StandardConformingStrings: true,

		// Initialize state
		State:           StateInitial,
		CurrentPosition: 0,
		LineNumber:      1,
		ColumnNumber:    1,

		// Initialize literal buffer
		LiteralActive: false,

		// Initialize error collection
		Errors: make([]LexerError, 0),
	}

	return ctx
}

// StartLiteral starts accumulating a literal value
// Reference: PostgreSQL startlit() function pattern
func (ctx *LexerContext) StartLiteral() {
	ctx.LiteralBuf.Reset()
	ctx.LiteralActive = true
}

// AddLiteral adds text to the current literal being accumulated
// Reference: PostgreSQL addlit() function pattern
func (ctx *LexerContext) AddLiteral(text string) {
	if ctx.LiteralActive {
		ctx.LiteralBuf.WriteString(text)
	}
}

// AddLiteralByte adds a single byte to the current literal
func (ctx *LexerContext) AddLiteralByte(b byte) {
	if ctx.LiteralActive {
		ctx.LiteralBuf.WriteByte(b)
	}
}

// GetLiteral returns the accumulated literal value and resets the buffer
func (ctx *LexerContext) GetLiteral() string {
	if !ctx.LiteralActive {
		return ""
	}

	result := ctx.LiteralBuf.String()
	ctx.LiteralBuf.Reset()
	ctx.LiteralActive = false
	return result
}

// CurrentChar returns the current character at ScanPos
func (ctx *LexerContext) CurrentChar() rune {
	if ctx.ScanPos >= len(ctx.ScanBuf) {
		return 0
	}
	return rune(ctx.ScanBuf[ctx.ScanPos])
}

// PeekChar returns the next character without advancing
func (ctx *LexerContext) PeekChar() rune {
	if ctx.ScanPos+1 >= len(ctx.ScanBuf) {
		return 0
	}
	return rune(ctx.ScanBuf[ctx.ScanPos+1])
}

// getByteAt returns the byte at the specified offset from current position
// This consolidates bounds checking logic used by multiple functions
func (ctx *LexerContext) getByteAt(offset int) (byte, bool) {
	pos := ctx.ScanPos + offset
	if pos >= ctx.ScanBufLen {
		return 0, false
	}
	return ctx.ScanBuf[pos], true
}

// CurrentByte returns the byte at the current position without advancing
func (ctx *LexerContext) CurrentByte() (byte, bool) {
	return ctx.getByteAt(0)
}

// NextByte returns the current byte and advances the position
func (ctx *LexerContext) NextByte() (byte, bool) {
	b, ok := ctx.CurrentByte()
	if !ok {
		return 0, false
	}
	ctx.advancePosition(b)
	return b, true
}

// PeekBytes returns n bytes starting at the current position without advancing
func (ctx *LexerContext) PeekBytes(n int) []byte {
	end := ctx.ScanPos + n
	if end > ctx.ScanBufLen {
		end = ctx.ScanBufLen
	}

	result := make([]byte, end-ctx.ScanPos)
	copy(result, ctx.ScanBuf[ctx.ScanPos:end])
	return result
}

// AdvanceBy moves the scan position forward by n bytes
func (ctx *LexerContext) AdvanceBy(n int) {
	for i := 0; i < n && ctx.ScanPos < ctx.ScanBufLen; i++ {
		b := ctx.ScanBuf[ctx.ScanPos]
		ctx.advancePosition(b)
	}
}

// advancePosition updates position tracking when consuming a byte
func (ctx *LexerContext) advancePosition(b byte) {
	ctx.ScanPos++
	ctx.CurrentPosition++

	if b == '\n' {
		ctx.LineNumber++
		ctx.ColumnNumber = 1
	} else {
		// Only advance column for valid UTF-8 sequence starts
		// This ensures Unicode characters are counted correctly
		if utf8.RuneStart(b) {
			ctx.ColumnNumber++
		}
	}
}

// AdvanceRune moves forward by one Unicode character (rune)
// This provides Unicode-aware position tracking like PostgreSQL's character-based functions
// Equivalent to PostgreSQL's pg_mblen function behavior
func (ctx *LexerContext) AdvanceRune() rune {
	if ctx.AtEOF() {
		return 0
	}

	r, size := utf8.DecodeRune(ctx.ScanBuf[ctx.ScanPos:])

	// Update position tracking for each byte of the rune
	for i := 0; i < size; i++ {
		ctx.advancePosition(ctx.ScanBuf[ctx.ScanPos])
	}

	return r
}

// PeekRune returns the next rune without advancing position
func (ctx *LexerContext) PeekRune() rune {
	if ctx.AtEOF() {
		return 0
	}

	r, _ := utf8.DecodeRune(ctx.ScanBuf[ctx.ScanPos:])
	return r
}

// GetUnicodePosition returns the current position as Unicode character count
// Equivalent to PostgreSQL's pg_mbstrlen_with_len function
// postgres/src/backend/utils/mb/mbutils.c:1200
func (ctx *LexerContext) GetUnicodePosition() int {
	return CalculateUnicodePosition(ctx.ScanBuf, ctx.CurrentPosition)
}

// SaveCurrentPosition saves the current position for error recovery
// Equivalent to PostgreSQL's PUSH_YYLLOC() macro
// postgres/src/backend/parser/scan.l:121
func (ctx *LexerContext) SaveCurrentPosition() int {
	ctx.SavePosition = ctx.CurrentPosition
	return ctx.CurrentPosition
}

// RestoreSavedPosition restores a previously saved position
// Equivalent to PostgreSQL's POP_YYLLOC() macro
// postgres/src/backend/parser/scan.l:122
func (ctx *LexerContext) RestoreSavedPosition() {
	if ctx.SavePosition < 0 || ctx.SavePosition > ctx.ScanBufLen {
		return
	}

	// Calculate how many bytes to go back
	diff := ctx.CurrentPosition - ctx.SavePosition
	if diff > 0 {
		ctx.PutBack(diff)

		// Recalculate line and column numbers from the beginning
		// This is expensive but ensures accuracy after position restoration
		ctx.recalculateLineColumn()
	}
}

// recalculateLineColumn recalculates line and column numbers from current position
// Used after position restoration to ensure accuracy
func (ctx *LexerContext) recalculateLineColumn() {
	line, col := CalculateLineColumn(ctx.ScanBuf, ctx.CurrentPosition)
	ctx.LineNumber = line
	ctx.ColumnNumber = col
}

// AtEOF returns true if we're at the end of input
func (ctx *LexerContext) AtEOF() bool {
	return ctx.ScanPos >= ctx.ScanBufLen
}

// GetCurrentText returns the text from start position to current position
func (ctx *LexerContext) GetCurrentText(startPos int) string {
	if startPos < 0 || startPos > ctx.ScanPos {
		return ""
	}
	return string(ctx.ScanBuf[startPos:ctx.ScanPos])
}

// SetState changes the lexer state
func (ctx *LexerContext) SetState(state LexerState) {
	ctx.State = state
}

// GetState returns the current lexer state
func (ctx *LexerContext) GetState() LexerState {
	return ctx.State
}

// AddError adds a lexer error with a specific error type and returns a pointer to it
// This is the primary method for error creation with consistent return of the created error
func (ctx *LexerContext) AddError(errorType LexerErrorType, message string) *LexerError {
	error := &LexerError{
		Type:        errorType,
		Message:     message,
		Position:    ctx.CurrentPosition,
		Line:        ctx.LineNumber,
		Column:      ctx.ColumnNumber,
		NearText:    ctx.extractNearText(),
		AtEOF:       ctx.AtEOF(),
		Context:     ctx.getErrorContext(),
		Hint:        ctx.getErrorHint(errorType),
		ErrorLength: ctx.calculateErrorLength(errorType),
	}

	ctx.Errors = append(ctx.Errors, *error)
	return error
}

// extractNearText extracts text near the current position for error context
// Based on PostgreSQL's error message formatting
func (ctx *LexerContext) extractNearText() string {
	const maxNearTextLen = 20

	// For errors, we want to show text from where the problematic token starts
	// Use SavePosition if available (which tracks the start of the current token)
	start := ctx.SavePosition
	if start < 0 || start >= ctx.ScanBufLen {
		start = ctx.CurrentPosition
	}

	// Handle EOF cases
	if start >= ctx.ScanBufLen {
		// Try to show the last part of the input
		if ctx.ScanBufLen > maxNearTextLen {
			start = ctx.ScanBufLen - maxNearTextLen
		} else {
			start = 0
		}
		if start < ctx.ScanBufLen {
			nearText := string(ctx.ScanBuf[start:ctx.ScanBufLen])
			return SanitizeNearText(nearText, maxNearTextLen)
		}
		return ""
	}

	end := start + maxNearTextLen
	if end > ctx.ScanBufLen {
		end = ctx.ScanBufLen
	}

	nearText := string(ctx.ScanBuf[start:end])
	return SanitizeNearText(nearText, maxNearTextLen)
}

// getErrorContext provides context information based on current lexer state
func (ctx *LexerContext) getErrorContext() string {
	switch ctx.State {
	case StateXQ:
		return "in quoted string"
	case StateXE:
		return "in extended string with escapes"
	case StateXDolQ:
		return "in dollar-quoted string"
	case StateXC:
		return "in comment"
	case StateXD:
		return "in delimited identifier"
	case StateXB:
		return "in bit string literal"
	case StateXH:
		return "in hexadecimal string literal"
	case StateXUI:
		return "in Unicode identifier"
	case StateXUS:
		return "in Unicode string"
	case StateXEU:
		return "in extended Unicode string"
	default:
		return ""
	}
}

// getErrorHint provides recovery hints based on error type and context
func (ctx *LexerContext) getErrorHint(errorType LexerErrorType) string {
	switch errorType {
	case UnterminatedString:
		switch ctx.State {
		case StateXQ:
			return "Add a closing single quote (') to terminate the string"
		case StateXE:
			return "Add a closing single quote (') to terminate the extended string"
		case StateXDolQ:
			return fmt.Sprintf("Add the closing delimiter %s to terminate the dollar-quoted string", ctx.DolQStart)
		}
	case UnterminatedComment:
		return "Add */ to close the comment"
	case UnterminatedIdentifier:
		return "Add a closing double quote (\") to terminate the identifier"
	case TrailingJunk:
		return "Separate the number and following text with whitespace or an operator"
	case InvalidEscape:
		return "Use a valid escape sequence like \\n, \\t, \\\\, or \\'"
	}
	return ""
}

// calculateErrorLength estimates the length of problematic text
func (ctx *LexerContext) calculateErrorLength(errorType LexerErrorType) int {
	switch errorType {
	case TrailingJunk:
		// Find the end of the trailing junk
		pos := ctx.CurrentPosition
		for pos < ctx.ScanBufLen && !IsWhitespace(ctx.ScanBuf[pos]) {
			pos++
		}
		return pos - ctx.CurrentPosition
	case InvalidEscape:
		return 2 // Typically backslash + one character
	default:
		return 1
	}
}

// HasErrors returns true if any errors have been collected
func (ctx *LexerContext) HasErrors() bool {
	return len(ctx.Errors) > 0
}

// GetErrors returns all collected errors
func (ctx *LexerContext) GetErrors() []LexerError {
	return ctx.Errors
}

// PutBack moves the scan position back by n bytes
// This is equivalent to PostgreSQL's yyless() macro
func (ctx *LexerContext) PutBack(n int) {
	if n <= 0 {
		return
	}

	// Ensure we don't go before the start
	if n > ctx.ScanPos {
		n = ctx.ScanPos
	}

	// Move position back
	ctx.ScanPos -= n
	ctx.CurrentPosition -= n

	// Note: We don't adjust line/column numbers here as that would require
	// re-scanning the put-back text. This matches PostgreSQL behavior where
	// yyless() doesn't update location tracking.
}

// String returns a string representation of the lexer context for debugging
func (ctx *LexerContext) String() string {
	return fmt.Sprintf("LexerContext{Position: %d/%d, Line: %d, Column: %d, State: %d, Errors: %d}",
		ctx.ScanPos, ctx.ScanBufLen, ctx.LineNumber, ctx.ColumnNumber, ctx.State, len(ctx.Errors))
}
