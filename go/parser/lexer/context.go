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
	
	// Note: Keyword lookup is now handled directly by lexer package functions
	// No need for interface-based integration - keywords are resolved during scanning
	
	// Scanner configuration (eliminates global state)
	// These were global variables in postgres/src/backend/parser/scan.l:68-70
	// Ported from postgres/src/include/parser/scanner.h:82-89
	BackslashQuote           BackslashQuoteMode // backslash_quote setting - postgres/src/include/parser/scanner.h:87
	EscapeStringWarning      bool               // escape_string_warning setting - postgres/src/include/parser/scanner.h:88
	StandardConformingStrings bool               // standard_conforming_strings setting - postgres/src/include/parser/scanner.h:89
	
	// Literal buffer for multi-rule parsing
	// Ported from postgres/src/include/parser/scanner.h:92-100
	LiteralBuf    strings.Builder // Accumulates literal values (literalbuf) - postgres/src/include/parser/scanner.h:98
	LiteralActive bool             // Whether literal buffer is active (internal tracking)
	
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
}

// LexerError represents a lexical analysis error
type LexerError struct {
	Message  string // Error message
	Position int    // Byte offset where error occurred
	Line     int    // Line number where error occurred
	Column   int    // Column number where error occurred
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
		BackslashQuote:           BackslashQuoteSafeEncoding,
		EscapeStringWarning:      true,
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

// PeekByte returns the byte at the current position without advancing
func (ctx *LexerContext) PeekByte() (byte, bool) {
	if ctx.ScanPos >= ctx.ScanBufLen {
		return 0, false
	}
	return ctx.ScanBuf[ctx.ScanPos], true
}

// NextByte returns the next byte and advances the position
func (ctx *LexerContext) NextByte() (byte, bool) {
	if ctx.ScanPos >= ctx.ScanBufLen {
		return 0, false
	}
	
	b := ctx.ScanBuf[ctx.ScanPos]
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
		ctx.ColumnNumber++
	}
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

// AddError adds a lexer error to the error collection
func (ctx *LexerContext) AddError(message string) {
	ctx.Errors = append(ctx.Errors, LexerError{
		Message:  message,
		Position: ctx.CurrentPosition,
		Line:     ctx.LineNumber,
		Column:   ctx.ColumnNumber,
	})
}

// HasErrors returns true if any errors have been collected
func (ctx *LexerContext) HasErrors() bool {
	return len(ctx.Errors) > 0
}

// GetErrors returns all collected errors
func (ctx *LexerContext) GetErrors() []LexerError {
	return ctx.Errors
}

// FormatError returns a formatted error message
func (e *LexerError) Error() string {
	return fmt.Sprintf("lexer error at line %d, column %d (position %d): %s",
		e.Line, e.Column, e.Position, e.Message)
}

// String returns a string representation of the lexer context for debugging
func (ctx *LexerContext) String() string {
	return fmt.Sprintf("LexerContext{Position: %d/%d, Line: %d, Column: %d, State: %d, Errors: %d}",
		ctx.ScanPos, ctx.ScanBufLen, ctx.LineNumber, ctx.ColumnNumber, ctx.State, len(ctx.Errors))
}