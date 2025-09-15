/*
 * PostgreSQL Parser - Unified Context Management
 *
 * This file implements a unified context that combines both lexer and parser
 * functionality, eliminating duplication and providing a single source of truth
 * for parsing state.
 * 
 * Note: ParseContext instances are designed for single-threaded use. Each
 * parsing thread should have its own ParseContext instance.
 * 
 * Ported from postgres/src/include/parser/scanner.h and postgres/src/backend/parser/
 */

package parser

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/multigres/parser/go/parser/ast"
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

// ErrorSeverity represents different levels of parsing errors
// Ported from postgres error severity concepts
type ErrorSeverity int

const (
	ErrorSeverityNotice ErrorSeverity = iota
	ErrorSeverityWarning
	ErrorSeverityError
	ErrorSeverityFatal
	ErrorSeverityPanic
)

// String returns the string representation of error severity
func (es ErrorSeverity) String() string {
	switch es {
	case ErrorSeverityNotice:
		return "NOTICE"
	case ErrorSeverityWarning:
		return "WARNING"
	case ErrorSeverityError:
		return "ERROR"
	case ErrorSeverityFatal:
		return "FATAL"
	case ErrorSeverityPanic:
		return "PANIC"
	default:
		return "UNKNOWN"
	}
}

// ParseError represents a unified error structure for both lexer and parser errors
// Combines LexerError and ParseError from the original contexts
type ParseError struct {
	// Core error information
	Type      LexerErrorType // For lexer-specific error types
	Message   string         // Error message
	Severity  ErrorSeverity  // Error severity level
	
	// Location information (unified)
	Position    int    // Byte offset in source text
	Line        int    // Line number (1-based)
	Column      int    // Column number (1-based)
	NearText    string // Text near the error for context
	AtEOF       bool   // Whether error occurred at EOF
	
	// Context and hints
	Context     string // Additional context information (e.g., "in quoted string")
	HintText    string // Helpful hint for user
	SourceText  string // Original source text
	ErrorLength int    // Length of problematic text
}

// Error implements the error interface
func (pe *ParseError) Error() string {
	if pe.Line > 0 && pe.Column > 0 {
		return fmt.Sprintf("%s at line %d, column %d: %s", pe.Severity, pe.Line, pe.Column, pe.Message)
	} else if pe.Position >= 0 {
		return fmt.Sprintf("%s at position %d: %s", pe.Severity, pe.Position, pe.Message)
	}
	return fmt.Sprintf("%s: %s", pe.Severity, pe.Message)
}

// ParseOptions contains unified configuration options for both lexer and parser
// Combines configuration from both original contexts
type ParseOptions struct {
	// SQL standard conformance settings (shared by lexer and parser)
	// Ported from postgres GUC variables related to SQL standards
	StandardConformingStrings bool               // standard_conforming_strings GUC
	EscapeStringWarning       bool               // escape_string_warning GUC
	BackslashQuote            BackslashQuoteMode // backslash_quote GUC

	// Parser behavior settings
	MaxIdentifierLength int // NAMEDATALEN equivalent - ported from postgres/src/include/pg_config_manual.h
	MaxExpressionDepth  int // Maximum expression nesting depth
	MaxStatementLength  int // Maximum statement length in bytes

	// Error handling options
	StopOnFirstError bool // Whether to stop parsing on first error
	CollectAllErrors bool // Whether to collect all errors instead of stopping

	// Feature flags
	EnableExtensions   bool // Enable PostgreSQL extensions
	EnableWindowFuncs  bool // Enable window functions
	EnablePartitioning bool // Enable table partitioning syntax
	EnableMergeStmt    bool // Enable MERGE statement
}

// DefaultParseOptions returns default parsing options
// Based on standard PostgreSQL default settings
func DefaultParseOptions() *ParseOptions {
	return &ParseOptions{
		// Standard PostgreSQL defaults
		StandardConformingStrings: true,             // Default in modern PostgreSQL
		EscapeStringWarning:       true,             // Default warning setting
		BackslashQuote:            BackslashQuoteSafeEncoding, // safe_encoding (default)

		// Parser limits - based on PostgreSQL defaults
		MaxIdentifierLength: 63,          // NAMEDATALEN - 1 (default PostgreSQL)
		MaxExpressionDepth:  1000,        // Reasonable expression nesting limit
		MaxStatementLength:  1024 * 1024, // 1MB statement limit

		// Error handling
		StopOnFirstError: false, // Collect multiple errors for better UX
		CollectAllErrors: true,  // Collect all errors by default

		// Feature flags - all enabled by default for full PostgreSQL compatibility
		EnableExtensions:   true,
		EnableWindowFuncs:  true,
		EnablePartitioning: true,
		EnableMergeStmt:    true,
	}
}

// ParseContext provides unified context for PostgreSQL parsing operations
// This eliminates all global state and combines both lexer and parser functionality
// Note: Each ParseContext instance is designed for single-threaded use.
// Multiple parsing threads should each have their own ParseContext instance.
type ParseContext struct {
	// Configuration (read-only after creation)
	options *ParseOptions

	// Source text and scanning state (unified)
	sourceText  string // Original SQL text being parsed
	scanBuf     []byte // The string being scanned (for lexer)
	scanBufLen  int    // Length of scan buffer
	scanPos     int    // Current position in scan buffer

	// Position tracking (unified - single source of truth)
	currentPosition int // Current byte offset
	lineNumber      int // Current line number (1-based)
	columnNumber    int // Current column number (1-based)

	// Lexer state
	lexerState        LexerState // Current lexer state
	stateBeforeStrStop int       // Start condition before end quote
	xcDepth           int        // Nesting depth in slash-star comments
	dolQStart         string     // Current $foo$ quote start string
	savePosition      int        // One-element stack for position saving

	// Literal buffer for multi-rule parsing
	literalBuf    strings.Builder // Accumulates literal values
	literalActive bool            // Whether literal buffer is active

	// UTF-16 surrogate pair handling
	utf16FirstPart int32 // First part of UTF16 surrogate pair for Unicode escapes

	// Warning state for literal lexing
	warnOnFirstEscape bool // State variable for literal-lexing warnings
	sawNonASCII       bool // Whether non-ASCII characters were seen

	// Parser state
	parseTree    ast.Node // Root of current parse tree
	currentDepth int      // Current expression nesting depth

	// Token and lexing state
	lastToken     *Token // Last token read by lexer
	tokenValue    string // String value of current token
	tokenLocation int    // Location of current token

	// Statement boundaries (for multi-statement parsing)
	statementStart int // Start position of current statement
	statementEnd   int // End position of current statement

	// Unified error collection
	errors   []ParseError // All errors (lexer + parser)
	warnings []ParseError // All warnings (lexer + parser)

	// Dot sequence handling (for handling "..." as individual dots)
	inMultiDotSequence bool // True if we're in a sequence of 3+ dots

	// Unique context ID for debugging
	contextID string
}

// NewParseContext creates a new unified thread-safe parsing context
// This is the main entry point for creating parser instances
func NewParseContext(input string, options *ParseOptions) *ParseContext {
	if options == nil {
		options = DefaultParseOptions()
	}

	ctx := &ParseContext{
		// Configuration
		options: options,

		// Initialize source and scan buffer
		sourceText: input,
		scanBuf:    []byte(input),
		scanBufLen: len(input),
		scanPos:    0,

		// Initialize position tracking
		currentPosition: 0,
		lineNumber:      1,
		columnNumber:    1,

		// Initialize lexer state
		lexerState: StateInitial,

		// Initialize literal buffer
		literalActive: false,

		// Initialize parser state
		currentDepth: 0,

		// Initialize error collections
		errors:   make([]ParseError, 0),
		warnings: make([]ParseError, 0),

		// Initialize statement boundaries
		statementStart: 0,
		statementEnd:   0,

		// Generate unique context ID
		contextID: fmt.Sprintf("parse_ctx_%p", &ParseContext{}),
	}

	return ctx
}

// GetOptions returns the parsing options (read-only)
func (ctx *ParseContext) GetOptions() *ParseOptions {
	return ctx.options // Options are immutable after creation
}

// GetSourceText returns the current source text being parsed
func (ctx *ParseContext) GetSourceText() string {
	return ctx.sourceText
}

// SetSourceText initializes the parser with new source text to parse
// This resets the parser state for a new parsing operation
func (ctx *ParseContext) SetSourceText(sourceText string) {

	ctx.sourceText = sourceText
	ctx.scanBuf = []byte(sourceText)
	ctx.scanBufLen = len(sourceText)
	ctx.scanPos = 0
	ctx.currentPosition = 0
	ctx.lineNumber = 1
	ctx.columnNumber = 1
	ctx.parseTree = nil
	ctx.currentDepth = 0
	ctx.statementStart = 0
	ctx.statementEnd = 0

	// Reset lexer state
	ctx.lexerState = StateInitial
	ctx.literalBuf.Reset()
	ctx.literalActive = false

	// Clear previous errors and warnings
	ctx.errors = ctx.errors[:0]
	ctx.warnings = ctx.warnings[:0]
}

// GetCurrentPosition returns the current parsing position
func (ctx *ParseContext) GetCurrentPosition() (pos int, line int, col int) {
	return ctx.currentPosition, ctx.lineNumber, ctx.columnNumber
}

// SetCurrentPosition updates the current parsing position
func (ctx *ParseContext) SetCurrentPosition(pos int, line int, col int) {
	ctx.currentPosition = pos
	ctx.lineNumber = line
	ctx.columnNumber = col
}

// SaveCurrentPosition saves the current position for error recovery
// Equivalent to PostgreSQL's PUSH_YYLLOC() macro
// postgres/src/backend/parser/scan.l:121
func (ctx *ParseContext) SaveCurrentPosition() int {
	ctx.savePosition = ctx.currentPosition
	return ctx.currentPosition
}

// RestoreSavedPosition restores a previously saved position
// Equivalent to PostgreSQL's POP_YYLLOC() macro
// postgres/src/backend/parser/scan.l:122
func (ctx *ParseContext) RestoreSavedPosition() {
	
	if ctx.savePosition < 0 || ctx.savePosition > len(ctx.sourceText) {
		return
	}

	// Calculate how many bytes to go back
	diff := ctx.currentPosition - ctx.savePosition
	if diff > 0 {
		ctx.putBackInternal(diff)

		// Recalculate line and column numbers from the beginning
		// This is expensive but ensures accuracy after position restoration
		ctx.recalculateLineColumn()
	}
}

// putBackInternal moves the scan position back by n bytes (internal, no mutex)
func (ctx *ParseContext) putBackInternal(n int) {
	if n <= 0 {
		return
	}

	// Ensure we don't go before the start
	if n > ctx.scanPos {
		n = ctx.scanPos
	}

	// Move position back
	ctx.scanPos -= n
	ctx.currentPosition -= n
}

// recalculateLineColumn recalculates line and column numbers from current position
// Used after position restoration to ensure accuracy
func (ctx *ParseContext) recalculateLineColumn() {
	line, col := CalculateLineColumn([]byte(ctx.sourceText), ctx.currentPosition)
	ctx.lineNumber = line
	ctx.columnNumber = col
}

// Lexer-specific methods (migrated from LexerContext)

// StartLiteral starts accumulating a literal value
func (ctx *ParseContext) StartLiteral() {
	ctx.literalBuf.Reset()
	ctx.literalActive = true
}

// AddLiteral adds text to the current literal being accumulated
func (ctx *ParseContext) AddLiteral(text string) {
	if ctx.literalActive {
		ctx.literalBuf.WriteString(text)
	}
}

// AddLiteralByte adds a single byte to the current literal
func (ctx *ParseContext) AddLiteralByte(b byte) {
	if ctx.literalActive {
		ctx.literalBuf.WriteByte(b)
	}
}

// GetLiteral returns the accumulated literal value and resets the buffer
func (ctx *ParseContext) GetLiteral() string {
	
	if !ctx.literalActive {
		return ""
	}

	result := ctx.literalBuf.String()
	ctx.literalBuf.Reset()
	ctx.literalActive = false
	return result
}

// CurrentChar returns the current character at ScanPos
func (ctx *ParseContext) CurrentChar() rune {
	
	if ctx.scanPos >= len(ctx.scanBuf) {
		return 0
	}
	r, _ := utf8.DecodeRune(ctx.scanBuf[ctx.scanPos:])
	return r
}

// PeekChar returns the next character without advancing
func (ctx *ParseContext) PeekChar() rune {
	
	if ctx.scanPos >= len(ctx.scanBuf) {
		return 0
	}
	// First decode the current rune to find its size
	_, currentSize := utf8.DecodeRune(ctx.scanBuf[ctx.scanPos:])
	// Then decode the next rune
	if ctx.scanPos+currentSize >= len(ctx.scanBuf) {
		return 0
	}
	r, _ := utf8.DecodeRune(ctx.scanBuf[ctx.scanPos+currentSize:])
	return r
}

// getByteAt returns the byte at the specified offset from current position
func (ctx *ParseContext) getByteAt(offset int) (byte, bool) {
	pos := ctx.scanPos + offset
	if pos >= ctx.scanBufLen {
		return 0, false
	}
	return ctx.scanBuf[pos], true
}

// CurrentByte returns the byte at the current position without advancing
func (ctx *ParseContext) CurrentByte() (byte, bool) {
	return ctx.getByteAt(0)
}

// NextByte returns the current byte and advances the position
func (ctx *ParseContext) NextByte() (byte, bool) {
	
	b, ok := ctx.getByteAt(0)
	if !ok {
		return 0, false
	}
	ctx.advancePosition(b)
	return b, true
}

// PeekBytes returns n bytes starting at the current position without advancing
func (ctx *ParseContext) PeekBytes(n int) []byte {
	
	end := ctx.scanPos + n
	if end > ctx.scanBufLen {
		end = ctx.scanBufLen
	}

	result := make([]byte, end-ctx.scanPos)
	copy(result, ctx.scanBuf[ctx.scanPos:end])
	return result
}

// AdvanceBy moves the scan position forward by n bytes
func (ctx *ParseContext) AdvanceBy(n int) {
	
	for i := 0; i < n && ctx.scanPos < ctx.scanBufLen; i++ {
		b := ctx.scanBuf[ctx.scanPos]
		ctx.advancePosition(b)
	}
}

// advancePosition updates position tracking when consuming a byte
func (ctx *ParseContext) advancePosition(b byte) {
	ctx.scanPos++
	ctx.currentPosition++

	if b == '\n' {
		ctx.lineNumber++
		ctx.columnNumber = 1
	} else {
		// Only advance column for valid UTF-8 sequence starts
		if utf8.RuneStart(b) {
			ctx.columnNumber++
		}
	}
}

// AdvanceRune moves forward by one Unicode character (rune)
func (ctx *ParseContext) AdvanceRune() rune {
	
	if ctx.AtEOF() {
		return 0
	}

	r, size := utf8.DecodeRune(ctx.scanBuf[ctx.scanPos:])

	// Update position tracking for each byte of the rune
	for i := 0; i < size; i++ {
		ctx.advancePosition(ctx.scanBuf[ctx.scanPos])
	}

	return r
}

// PeekRune returns the next rune without advancing position
func (ctx *ParseContext) PeekRune() rune {
	
	if ctx.AtEOF() {
		return 0
	}

	r, _ := utf8.DecodeRune(ctx.scanBuf[ctx.scanPos:])
	return r
}

// AtEOF returns true if we're at the end of input
func (ctx *ParseContext) AtEOF() bool {
	return ctx.scanPos >= ctx.scanBufLen
}

// GetCurrentText returns the text from start position to current position
func (ctx *ParseContext) GetCurrentText(startPos int) string {
	
	if startPos < 0 || startPos > ctx.scanPos {
		return ""
	}
	return string(ctx.scanBuf[startPos:ctx.scanPos])
}

// SetState changes the lexer state
func (ctx *ParseContext) SetState(state LexerState) {
	ctx.lexerState = state
}

// GetState returns the current lexer state
func (ctx *ParseContext) GetState() LexerState {
	return ctx.lexerState
}

// Parser-specific methods (migrated from ParserContext)

// SetParseTree sets the root of the parse tree
func (ctx *ParseContext) SetParseTree(tree ast.Node) {
	ctx.parseTree = tree
}

// GetParseTree returns the root of the parse tree
func (ctx *ParseContext) GetParseTree() ast.Node {
	return ctx.parseTree
}

// IncrementDepth increments the expression nesting depth
func (ctx *ParseContext) IncrementDepth() error {

	ctx.currentDepth++
	if ctx.currentDepth > ctx.options.MaxExpressionDepth {
		return fmt.Errorf("expression too complex (maximum depth %d exceeded)",
			ctx.options.MaxExpressionDepth)
	}
	return nil
}

// DecrementDepth decrements the expression nesting depth
func (ctx *ParseContext) DecrementDepth() {
	if ctx.currentDepth > 0 {
		ctx.currentDepth--
	}
}

// GetDepth returns the current expression nesting depth
func (ctx *ParseContext) GetDepth() int {
	return ctx.currentDepth
}

// SetStatementBoundaries sets the boundaries of the current statement
func (ctx *ParseContext) SetStatementBoundaries(start int, end int) {
	ctx.statementStart = start
	ctx.statementEnd = end
}

// GetStatementBoundaries returns the boundaries of the current statement
func (ctx *ParseContext) GetStatementBoundaries() (int, int) {
	return ctx.statementStart, ctx.statementEnd
}

// Unified error handling methods

// AddError adds a parsing error to the context
func (ctx *ParseContext) AddError(message string, location int) *ParseError {
	return ctx.addErrorWithSeverity(ErrorSeverityError, message, location, "", "")
}

// AddErrorWithType adds a lexer error with specific error type
func (ctx *ParseContext) AddErrorWithType(errorType LexerErrorType, message string) *ParseError {
	location := ctx.currentPosition
	
	return ctx.addLexerError(errorType, message, location)
}

// AddErrorWithHint adds a parsing error with a helpful hint
func (ctx *ParseContext) AddErrorWithHint(message string, location int, hint string) *ParseError {
	return ctx.addErrorWithSeverity(ErrorSeverityError, message, location, "", hint)
}

// AddWarning adds a parsing warning to the context
func (ctx *ParseContext) AddWarning(message string, location int) *ParseError {
	return ctx.addErrorWithSeverity(ErrorSeverityWarning, message, location, "", "")
}

// addErrorWithSeverity is the internal error adding function
func (ctx *ParseContext) addErrorWithSeverity(severity ErrorSeverity, message string, location int, context string, hint string) *ParseError {

	// Calculate line and column from location
	line, col := ctx.calculateLineColumn(location)

	parseError := &ParseError{
		Message:    message,
		Severity:   severity,
		Position:   location,
		Line:       line,
		Column:     col,
		Context:    context,
		HintText:   hint,
		SourceText: ctx.sourceText,
		NearText:   ctx.extractNearText(location),
		AtEOF:      location >= len(ctx.sourceText),
	}

	if severity == ErrorSeverityWarning {
		ctx.warnings = append(ctx.warnings, *parseError)
	} else {
		ctx.errors = append(ctx.errors, *parseError)
	}
	
	return parseError
}

// addLexerError adds a lexer-specific error with enhanced context
func (ctx *ParseContext) addLexerError(errorType LexerErrorType, message string, location int) *ParseError {

	// Calculate line and column from location
	line, col := ctx.calculateLineColumn(location)

	parseError := &ParseError{
		Type:        errorType,
		Message:     message,
		Severity:    ErrorSeverityError,
		Position:    location,
		Line:        line,
		Column:      col,
		NearText:    ctx.extractNearText(location),
		AtEOF:       location >= len(ctx.sourceText),
		Context:     ctx.getErrorContext(),
		HintText:    ctx.getErrorHint(errorType),
		ErrorLength: ctx.calculateErrorLength(errorType),
		SourceText:  ctx.sourceText,
	}

	ctx.errors = append(ctx.errors, *parseError)
	return parseError
}

// calculateLineColumn calculates line and column numbers from byte offset
func (ctx *ParseContext) calculateLineColumn(location int) (int, int) {
	if location < 0 || location > len(ctx.sourceText) {
		return -1, -1
	}

	line := 1
	col := 1

	for i := 0; i < location && i < len(ctx.sourceText); i++ {
		if ctx.sourceText[i] == '\n' {
			line++
			col = 1
		} else {
			col++
		}
	}

	return line, col
}

// extractNearText extracts text near the specified position for error context
// Based on PostgreSQL's error message formatting, uses savePosition when available
func (ctx *ParseContext) extractNearText(location int) string {
	const maxNearTextLen = 20

	// For errors, we want to show text from where the problematic token starts
	// Use savePosition if available (which tracks the start of the current token)
	start := ctx.savePosition
	if start < 0 || start >= len(ctx.sourceText) {
		start = location
	}

	// Handle EOF cases
	if start >= len(ctx.sourceText) {
		// Try to show the last part of the input
		if len(ctx.sourceText) > maxNearTextLen {
			start = len(ctx.sourceText) - maxNearTextLen
		} else {
			start = 0
		}
		if start < len(ctx.sourceText) {
			nearText := ctx.sourceText[start:len(ctx.sourceText)]
			return SanitizeNearText(nearText, maxNearTextLen)
		}
		return ""
	}

	end := start + maxNearTextLen
	if end > len(ctx.sourceText) {
		end = len(ctx.sourceText)
	}

	nearText := ctx.sourceText[start:end]
	return SanitizeNearText(nearText, maxNearTextLen)
}

// getErrorContext provides context information based on current lexer state
func (ctx *ParseContext) getErrorContext() string {
	switch ctx.lexerState {
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
func (ctx *ParseContext) getErrorHint(errorType LexerErrorType) string {
	switch errorType {
	case UnterminatedString:
		switch ctx.lexerState {
		case StateXQ:
			return "Add a closing single quote (') to terminate the string"
		case StateXE:
			return "Add a closing single quote (') to terminate the extended string"
		case StateXDolQ:
			return fmt.Sprintf("Add the closing delimiter %s to terminate the dollar-quoted string", ctx.dolQStart)
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
func (ctx *ParseContext) calculateErrorLength(errorType LexerErrorType) int {
	switch errorType {
	case TrailingJunk:
		pos := ctx.currentPosition
		for pos < len(ctx.sourceText) && !IsWhitespace(byte(ctx.sourceText[pos])) {
			pos++
		}
		return pos - ctx.currentPosition
	case InvalidEscape:
		return 2 // Typically backslash + one character
	default:
		return 1
	}
}

// GetErrors returns all collected parsing errors
func (ctx *ParseContext) GetErrors() []ParseError {

	// Return copy to prevent external modification
	errors := make([]ParseError, len(ctx.errors))
	copy(errors, ctx.errors)
	return errors
}

// GetWarnings returns all collected parsing warnings
func (ctx *ParseContext) GetWarnings() []ParseError {

	// Return copy to prevent external modification
	warnings := make([]ParseError, len(ctx.warnings))
	copy(warnings, ctx.warnings)
	return warnings
}

// HasErrors returns true if there are any parsing errors
func (ctx *ParseContext) HasErrors() bool {
	return len(ctx.errors) > 0
}

// HasWarnings returns true if there are any parsing warnings
func (ctx *ParseContext) HasWarnings() bool {
	return len(ctx.warnings) > 0
}

// ClearErrors clears all collected errors and warnings
func (ctx *ParseContext) ClearErrors() {
	ctx.errors = ctx.errors[:0]
	ctx.warnings = ctx.warnings[:0]
}

// Additional utility methods

// PutBack moves the scan position back by n bytes
func (ctx *ParseContext) PutBack(n int) {
	
	if n <= 0 {
		return
	}

	// Ensure we don't go before the start
	if n > ctx.scanPos {
		n = ctx.scanPos
	}

	// Move position back
	ctx.scanPos -= n
	ctx.currentPosition -= n
}

// GetContextID returns a unique identifier for this parser context
func (ctx *ParseContext) GetContextID() string {
	return ctx.contextID
}

// Clone creates a copy of the parser context for use in another goroutine
func (ctx *ParseContext) Clone() *ParseContext {
	sourceText := ctx.sourceText
	
	// Options are immutable, so we can share them safely
	return NewParseContext(sourceText, ctx.options)
}

// String returns a string representation of the parser context for debugging
func (ctx *ParseContext) String() string {

	return fmt.Sprintf("ParseContext{id: %s, pos: %d/%d, line: %d, col: %d, errors: %d, warnings: %d, state: %d, depth: %d}",
		ctx.contextID,
		ctx.currentPosition,
		len(ctx.sourceText),
		ctx.lineNumber,
		ctx.columnNumber,
		len(ctx.errors),
		len(ctx.warnings),
		ctx.lexerState,
		ctx.currentDepth)
}

// Compatibility methods for smooth migration

// Legacy LexerContext compatibility methods
func (ctx *ParseContext) GetScanPos() int {
	return ctx.scanPos
}

func (ctx *ParseContext) GetScanBuf() []byte {
	// Return copy for safety
	buf := make([]byte, len(ctx.scanBuf))
	copy(buf, ctx.scanBuf)
	return buf
}

func (ctx *ParseContext) GetScanBufLen() int {
	return ctx.scanBufLen
}

// Legacy ParserContext compatibility method
func (ctx *ParseContext) GetCurrentPos() int {
	return ctx.currentPosition
}

// Getters for private fields (for compatibility)
func (ctx *ParseContext) CurrentPosition() int {
	return ctx.currentPosition
}

func (ctx *ParseContext) ColumnNumber() int {
	return ctx.columnNumber
}

func (ctx *ParseContext) XCDepth() int {
	return ctx.xcDepth
}

func (ctx *ParseContext) SetXCDepth(depth int) {
	ctx.xcDepth = depth
}

// Getters and setters for lexer fields that were previously public
func (ctx *ParseContext) StandardConformingStrings() bool {
	return ctx.options.StandardConformingStrings
}

func (ctx *ParseContext) UTF16FirstPart() int32 {
	return ctx.utf16FirstPart
}

func (ctx *ParseContext) SetUTF16FirstPart(value int32) {
	ctx.utf16FirstPart = value
}

func (ctx *ParseContext) DolQStart() string {
	return ctx.dolQStart
}

func (ctx *ParseContext) SetDolQStart(value string) {
	ctx.dolQStart = value
}

// More compatibility getters and setters
func (ctx *ParseContext) ScanPos() int {
	return ctx.scanPos
}

func (ctx *ParseContext) State() LexerState {
	return ctx.lexerState
}

func (ctx *ParseContext) LineNumber() int {
	return ctx.lineNumber
}

func (ctx *ParseContext) SetLineNumber(line int) {
	ctx.lineNumber = line
}

func (ctx *ParseContext) SetColumnNumber(col int) {
	ctx.columnNumber = col
}

func (ctx *ParseContext) InMultiDotSequence() bool {
	return ctx.inMultiDotSequence
}

func (ctx *ParseContext) SetInMultiDotSequence(value bool) {
	ctx.inMultiDotSequence = value
}

func (ctx *ParseContext) ScanBuf() []byte {
	// Return copy for safety
	buf := make([]byte, len(ctx.scanBuf))
	copy(buf, ctx.scanBuf)
	return buf
}

func (ctx *ParseContext) SetScanPos(pos int) {
	ctx.scanPos = pos
}

func (ctx *ParseContext) GetState2() LexerState {
	return ctx.lexerState
}

// Legacy getter for tests
func (ctx *ParseContext) Errors() []ParseError {
	return ctx.GetErrors()
}

// Compatibility function for tests
func NewLexerContext(input string) *ParseContext {
	return NewParseContext(input, nil)
}

// More compatibility methods for tests
func (ctx *ParseContext) ScanBufLen() int {
	return ctx.GetScanBufLen()
}

func (ctx *ParseContext) LiteralActive() bool {
	return ctx.literalActive
}

func (ctx *ParseContext) SetCurrentPosition2(pos int) {
	ctx.currentPosition = pos
}

func (ctx *ParseContext) SetLineNumber2(line int) {
	ctx.lineNumber = line
}

func (ctx *ParseContext) SetColumnNumber2(col int) {
	ctx.columnNumber = col
}