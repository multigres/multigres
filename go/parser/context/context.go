// Package context provides thread-safe parser context for PostgreSQL parsing.
// Ported from postgres/src/backend/parser/ global state elimination
package context

import (
	"fmt"
	"sync"

	"github.com/multigres/parser/go/parser/ast"
	"github.com/multigres/parser/go/parser/keywords"
)

// ErrorSeverity represents different levels of parsing errors.
// Ported from postgres error severity concepts
type ErrorSeverity int

const (
	ErrorSeverityNotice ErrorSeverity = iota
	ErrorSeverityWarning
	ErrorSeverityError
	ErrorSeverityFatal
	ErrorSeverityPanic
)

// String returns the string representation of error severity.
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

// ParseError represents a parsing error with location information.
// Ported from postgres error reporting with source location tracking
type ParseError struct {
	Message    string        // Error message
	Severity   ErrorSeverity // Error severity level
	Location   int           // Byte offset in source string
	Line       int           // Line number (1-based)
	Column     int           // Column number (1-based)  
	Context    string        // Additional context information
	HintText   string        // Helpful hint for user
	SourceText string        // Original source text
}

// Error implements the error interface.
func (pe *ParseError) Error() string {
	if pe.Line > 0 && pe.Column > 0 {
		return fmt.Sprintf("%s at line %d, column %d: %s", pe.Severity, pe.Line, pe.Column, pe.Message)
	} else if pe.Location >= 0 {
		return fmt.Sprintf("%s at position %d: %s", pe.Severity, pe.Location, pe.Message)
	}
	return fmt.Sprintf("%s: %s", pe.Severity, pe.Message)
}

// ParseOptions contains configuration options for parsing.
// These replace PostgreSQL global configuration variables
type ParseOptions struct {
	// SQL standard conformance settings
	// Ported from postgres GUC variables related to SQL standards
	StandardConformingStrings bool // standard_conforming_strings GUC
	EscapeStringWarning       bool // escape_string_warning GUC
	BackslashQuote            int  // backslash_quote GUC (0=off, 1=on, 2=safe_encoding)
	
	// Parser behavior settings
	MaxIdentifierLength int  // NAMEDATALEN equivalent - ported from postgres/src/include/pg_config_manual.h
	MaxExpressionDepth  int  // Maximum expression nesting depth
	MaxStatementLength  int  // Maximum statement length in bytes
	
	// Error handling options
	StopOnFirstError bool // Whether to stop parsing on first error
	CollectAllErrors bool // Whether to collect all errors instead of stopping
	
	// Feature flags
	EnableExtensions   bool // Enable PostgreSQL extensions
	EnableWindowFuncs  bool // Enable window functions
	EnablePartitioning bool // Enable table partitioning syntax
	EnableMergeStmt    bool // Enable MERGE statement
}

// DefaultParseOptions returns default parsing options.
// Based on standard PostgreSQL default settings
func DefaultParseOptions() *ParseOptions {
	return &ParseOptions{
		// Standard PostgreSQL defaults
		StandardConformingStrings: true,  // Default in modern PostgreSQL
		EscapeStringWarning:       true,  // Default warning setting
		BackslashQuote:            2,     // safe_encoding (default)
		
		// Parser limits - based on PostgreSQL defaults
		MaxIdentifierLength: 63,    // NAMEDATALEN - 1 (default PostgreSQL)
		MaxExpressionDepth:  1000,  // Reasonable expression nesting limit
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

// ParserState holds the current state of parsing operation.
// This replaces all PostgreSQL global parser state variables
type ParserState struct {
	// Source text and position tracking
	// Ported from postgres/src/backend/parser/scan.l state
	SourceText   string // Original SQL text being parsed
	CurrentPos   int    // Current position in source text
	CurrentLine  int    // Current line number (1-based)
	CurrentCol   int    // Current column number (1-based)
	
	// Token and lexing state
	// Ported from postgres lexer state variables
	LastToken    keywords.Token // Last token read by lexer
	TokenValue   string         // String value of current token
	TokenLocation int           // Location of current token
	
	// Parse tree construction
	// Ported from postgres parse tree building state
	ParseTree    ast.Node       // Root of current parse tree
	CurrentDepth int            // Current expression nesting depth
	
	// Error collection
	Errors   []ParseError // Collected parsing errors
	Warnings []ParseError // Collected parsing warnings
	
	// Statement boundaries (for multi-statement parsing)
	// Ported from postgres multi-statement handling
	StatementStart int // Start position of current statement
	StatementEnd   int // End position of current statement
}

// ParserContext provides thread-safe context for PostgreSQL parsing operations.
// This eliminates all global state from the original PostgreSQL parser.
// Ported from postgres global state elimination requirements
type ParserContext struct {
	// Configuration (read-only after creation)
	options *ParseOptions
	
	// Mutable parsing state (protected by mutex)
	mu    sync.RWMutex // Protects mutable state for thread safety
	state *ParserState
	
	// Parser components (thread-safe)
	keywords *keywords.KeywordInfo // Keyword lookup (read-only)
	
	// Unique context ID for debugging
	contextID string
}

// NewParserContext creates a new thread-safe parser context.
// This is the main entry point for creating parser instances.
func NewParserContext(options *ParseOptions) *ParserContext {
	if options == nil {
		options = DefaultParseOptions()
	}
	
	return &ParserContext{
		options: options,
		state: &ParserState{
			CurrentLine: 1,
			CurrentCol:  1,
			Errors:      make([]ParseError, 0),
			Warnings:    make([]ParseError, 0),
		},
		contextID: fmt.Sprintf("parser_%p", &ParserContext{}),
	}
}

// GetOptions returns the parsing options (read-only).
func (ctx *ParserContext) GetOptions() *ParseOptions {
	return ctx.options // Options are immutable after creation
}

// SetSourceText initializes the parser with source text to parse.
// This resets the parser state for a new parsing operation.
func (ctx *ParserContext) SetSourceText(sourceText string) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	
	ctx.state.SourceText = sourceText
	ctx.state.CurrentPos = 0
	ctx.state.CurrentLine = 1
	ctx.state.CurrentCol = 1
	ctx.state.ParseTree = nil
	ctx.state.CurrentDepth = 0
	ctx.state.StatementStart = 0
	ctx.state.StatementEnd = 0
	
	// Clear previous errors and warnings
	ctx.state.Errors = ctx.state.Errors[:0]
	ctx.state.Warnings = ctx.state.Warnings[:0]
}

// GetSourceText returns the current source text being parsed.
func (ctx *ParserContext) GetSourceText() string {
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	return ctx.state.SourceText
}

// GetCurrentPosition returns the current parsing position.
func (ctx *ParserContext) GetCurrentPosition() (pos int, line int, col int) {
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	return ctx.state.CurrentPos, ctx.state.CurrentLine, ctx.state.CurrentCol
}

// SetCurrentPosition updates the current parsing position.
// Used by lexer to track position in source text.
func (ctx *ParserContext) SetCurrentPosition(pos int, line int, col int) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	ctx.state.CurrentPos = pos
	ctx.state.CurrentLine = line
	ctx.state.CurrentCol = col
}

// AddError adds a parsing error to the context.
// Errors are collected for reporting to the user.
func (ctx *ParserContext) AddError(message string, location int) {
	ctx.addErrorWithSeverity(ErrorSeverityError, message, location, "", "")
}

// AddErrorWithHint adds a parsing error with a helpful hint.
func (ctx *ParserContext) AddErrorWithHint(message string, location int, hint string) {
	ctx.addErrorWithSeverity(ErrorSeverityError, message, location, "", hint)
}

// AddWarning adds a parsing warning to the context.
func (ctx *ParserContext) AddWarning(message string, location int) {
	ctx.addErrorWithSeverity(ErrorSeverityWarning, message, location, "", "")
}

// addErrorWithSeverity is the internal error adding function.
func (ctx *ParserContext) addErrorWithSeverity(severity ErrorSeverity, message string, location int, context string, hint string) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	
	// Calculate line and column from location
	line, col := ctx.calculateLineColumn(location)
	
	parseError := ParseError{
		Message:    message,
		Severity:   severity,
		Location:   location,
		Line:       line,
		Column:     col,
		Context:    context,
		HintText:   hint,
		SourceText: ctx.state.SourceText,
	}
	
	if severity == ErrorSeverityWarning {
		ctx.state.Warnings = append(ctx.state.Warnings, parseError)
	} else {
		ctx.state.Errors = append(ctx.state.Errors, parseError)
	}
}

// calculateLineColumn calculates line and column numbers from byte offset.
// Ported from postgres source location tracking logic
func (ctx *ParserContext) calculateLineColumn(location int) (int, int) {
	if location < 0 || location > len(ctx.state.SourceText) {
		return -1, -1
	}
	
	line := 1
	col := 1
	
	for i := 0; i < location && i < len(ctx.state.SourceText); i++ {
		if ctx.state.SourceText[i] == '\n' {
			line++
			col = 1
		} else {
			col++
		}
	}
	
	return line, col
}

// GetErrors returns all collected parsing errors.
func (ctx *ParserContext) GetErrors() []ParseError {
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	
	// Return copy to prevent external modification
	errors := make([]ParseError, len(ctx.state.Errors))
	copy(errors, ctx.state.Errors)
	return errors
}

// GetWarnings returns all collected parsing warnings.
func (ctx *ParserContext) GetWarnings() []ParseError {
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	
	// Return copy to prevent external modification
	warnings := make([]ParseError, len(ctx.state.Warnings))
	copy(warnings, ctx.state.Warnings)
	return warnings
}

// HasErrors returns true if there are any parsing errors.
func (ctx *ParserContext) HasErrors() bool {
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	return len(ctx.state.Errors) > 0
}

// HasWarnings returns true if there are any parsing warnings.
func (ctx *ParserContext) HasWarnings() bool {
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	return len(ctx.state.Warnings) > 0
}

// ClearErrors clears all collected errors and warnings.
func (ctx *ParserContext) ClearErrors() {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	ctx.state.Errors = ctx.state.Errors[:0]
	ctx.state.Warnings = ctx.state.Warnings[:0]
}

// SetParseTree sets the root of the parse tree.
func (ctx *ParserContext) SetParseTree(tree ast.Node) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	ctx.state.ParseTree = tree
}

// GetParseTree returns the root of the parse tree.
func (ctx *ParserContext) GetParseTree() ast.Node {
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	return ctx.state.ParseTree
}

// IncrementDepth increments the expression nesting depth.
// Returns error if maximum depth is exceeded.
func (ctx *ParserContext) IncrementDepth() error {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	
	ctx.state.CurrentDepth++
	if ctx.state.CurrentDepth > ctx.options.MaxExpressionDepth {
		return fmt.Errorf("expression too complex (maximum depth %d exceeded)", 
			ctx.options.MaxExpressionDepth)
	}
	return nil
}

// DecrementDepth decrements the expression nesting depth.
func (ctx *ParserContext) DecrementDepth() {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	if ctx.state.CurrentDepth > 0 {
		ctx.state.CurrentDepth--
	}
}

// GetDepth returns the current expression nesting depth.
func (ctx *ParserContext) GetDepth() int {
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	return ctx.state.CurrentDepth
}

// SetStatementBoundaries sets the boundaries of the current statement.
// Used for multi-statement parsing and error reporting.
func (ctx *ParserContext) SetStatementBoundaries(start int, end int) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	ctx.state.StatementStart = start
	ctx.state.StatementEnd = end
}

// GetStatementBoundaries returns the boundaries of the current statement.
func (ctx *ParserContext) GetStatementBoundaries() (int, int) {
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	return ctx.state.StatementStart, ctx.state.StatementEnd
}

// GetContextID returns a unique identifier for this parser context.
// Useful for debugging and logging in multi-threaded environments.
func (ctx *ParserContext) GetContextID() string {
	return ctx.contextID
}

// Clone creates a copy of the parser context for use in another goroutine.
// The new context shares the same options but has independent state.
func (ctx *ParserContext) Clone() *ParserContext {
	// Options are immutable, so we can share them safely
	return NewParserContext(ctx.options)
}

// String returns a string representation of the parser context for debugging.
func (ctx *ParserContext) String() string {
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	
	return fmt.Sprintf("ParserContext{id: %s, pos: %d/%d, line: %d, col: %d, errors: %d, warnings: %d}",
		ctx.contextID,
		ctx.state.CurrentPos,
		len(ctx.state.SourceText),
		ctx.state.CurrentLine,
		ctx.state.CurrentCol,
		len(ctx.state.Errors),
		len(ctx.state.Warnings))
}