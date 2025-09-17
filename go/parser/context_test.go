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

package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/parser/ast"
)

// TestNewParseContext tests unified context creation
func TestNewParseContext(t *testing.T) {
	testSQL := "SELECT * FROM users WHERE id = 1;"

	// Test with default options
	ctx1 := NewParseContext(testSQL, nil)
	require.NotNil(t, ctx1)
	assert.NotNil(t, ctx1.GetOptions())
	assert.NotEmpty(t, ctx1.GetContextID())
	assert.Equal(t, testSQL, ctx1.GetSourceText())

	// Test with custom options
	customOptions := &ParseOptions{
		MaxIdentifierLength: 100,
		StopOnFirstError:    true,
	}
	ctx2 := NewParseContext(testSQL, customOptions)
	require.NotNil(t, ctx2)
	assert.Equal(t, customOptions, ctx2.GetOptions())
	assert.Equal(t, testSQL, ctx2.GetSourceText())
}

// TestParseContextSourceText tests source text management
func TestParseContextSourceText(t *testing.T) {
	ctx := NewParseContext("", nil)
	testSQL := "SELECT * FROM users WHERE id = 1;"

	// Initially empty
	assert.Empty(t, ctx.GetSourceText())

	// Set source text
	ctx.SetSourceText(testSQL)
	assert.Equal(t, testSQL, ctx.GetSourceText())

	// Check initial position
	pos, line, col := ctx.GetCurrentPosition()
	assert.Equal(t, 0, pos)
	assert.Equal(t, 1, line)
	assert.Equal(t, 1, col)

	// Check scan buffer properties
	assert.Equal(t, 0, ctx.GetScanPos())
	assert.Equal(t, len(testSQL), ctx.GetScanBufLen())
}

// TestParseContextPosition tests position tracking
func TestParseContextPosition(t *testing.T) {
	ctx := NewParseContext("SELECT * FROM users;", nil)

	// Set position
	ctx.SetCurrentPosition(7, 1, 8)

	pos, line, col := ctx.GetCurrentPosition()
	assert.Equal(t, 7, pos)
	assert.Equal(t, 1, line)
	assert.Equal(t, 8, col)
}

// TestParseContextLexerFunctionality tests lexer-specific methods
func TestParseContextLexerFunctionality(t *testing.T) {
	ctx := NewParseContext("Hello World", nil)

	// Test character access
	assert.Equal(t, 'H', ctx.CurrentChar())
	assert.Equal(t, 'e', ctx.PeekChar())

	// Test byte access
	b, ok := ctx.CurrentByte()
	assert.True(t, ok)
	assert.Equal(t, byte('H'), b)

	// Test advance
	b, ok = ctx.NextByte()
	assert.True(t, ok)
	assert.Equal(t, byte('H'), b)

	// Position should have advanced
	pos, _, _ := ctx.GetCurrentPosition()
	assert.Equal(t, 1, pos)

	// Test PeekBytes
	bytes := ctx.PeekBytes(4)
	assert.Equal(t, []byte("ello"), bytes)

	// Test AdvanceBy
	ctx.AdvanceBy(4)
	pos, _, _ = ctx.GetCurrentPosition()
	assert.Equal(t, 5, pos)
	assert.Equal(t, ' ', ctx.CurrentChar()) // Position 5 is the space in "Hello World"
}

// TestParseContextLiteralBuffer tests literal accumulation
func TestParseContextLiteralBuffer(t *testing.T) {
	ctx := NewParseContext("test", nil)

	// Initially no active literal
	literal := ctx.GetLiteral()
	assert.Empty(t, literal)

	// Start literal and add content
	ctx.StartLiteral()
	ctx.AddLiteral("Hello")
	ctx.AddLiteral(" ")
	ctx.AddLiteral("World")
	ctx.AddLiteralByte('!')

	// Get literal (should reset buffer)
	literal = ctx.GetLiteral()
	assert.Equal(t, "Hello World!", literal)

	// Buffer should be reset
	literal = ctx.GetLiteral()
	assert.Empty(t, literal)
}

// TestParseContextLexerState tests lexer state management
func TestParseContextLexerState(t *testing.T) {
	ctx := NewParseContext("test", nil)

	// Initially in initial state
	assert.Equal(t, StateInitial, ctx.GetState())

	// Change state
	ctx.SetState(StateXQ)
	assert.Equal(t, StateXQ, ctx.GetState())

	ctx.SetState(StateXC)
	assert.Equal(t, StateXC, ctx.GetState())
}

// TestParseContextParserFunctionality tests parser-specific methods
func TestParseContextParserFunctionality(t *testing.T) {
	ctx := NewParseContext("SELECT * FROM users;", nil)

	// Initially no parse tree
	assert.Nil(t, ctx.GetParseTree())

	// Set parse tree
	tree := ast.NewIdentifier("test")
	ctx.SetParseTree(tree)
	assert.Equal(t, tree, ctx.GetParseTree())

	// Test depth tracking
	assert.Equal(t, 0, ctx.GetDepth())

	_ = ctx.IncrementDepth()
	assert.Equal(t, 1, ctx.GetDepth())

	ctx.DecrementDepth()
	assert.Equal(t, 0, ctx.GetDepth())

	// Test statement boundaries
	start, end := ctx.GetStatementBoundaries()
	assert.Equal(t, 0, start)
	assert.Equal(t, 0, end)

	ctx.SetStatementBoundaries(10, 25)
	start, end = ctx.GetStatementBoundaries()
	assert.Equal(t, 10, start)
	assert.Equal(t, 25, end)
}

// TestParseContextDepthTracking tests expression depth limits
func TestParseContextDepthTracking(t *testing.T) {
	options := &ParseOptions{
		MaxExpressionDepth: 3,
	}
	ctx := NewParseContext("test", options)

	// Should allow up to max depth
	assert.NoError(t, ctx.IncrementDepth()) // 1
	assert.NoError(t, ctx.IncrementDepth()) // 2
	assert.NoError(t, ctx.IncrementDepth()) // 3

	// Should error on exceeding max
	err := ctx.IncrementDepth() // 4
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expression too complex")

	// Decrement should work
	ctx.DecrementDepth()
	ctx.DecrementDepth()
	ctx.DecrementDepth()
	ctx.DecrementDepth() // Should not go negative
	assert.Equal(t, 0, ctx.GetDepth())
}

// TestParseContextUnifiedErrors tests unified error handling
func TestParseContextUnifiedErrors(t *testing.T) {
	ctx := NewParseContext("SELECT * FROM users\nWHERE id = ?;", nil)

	// Initially no errors
	assert.False(t, ctx.HasErrors())
	assert.False(t, ctx.HasWarnings())
	assert.Empty(t, ctx.GetErrors())
	assert.Empty(t, ctx.GetWarnings())

	// Add a regular error
	_ = ctx.AddError("syntax error", 15)
	assert.True(t, ctx.HasErrors())
	assert.False(t, ctx.HasWarnings())

	errors := ctx.GetErrors()
	assert.Len(t, errors, 1)
	assert.Equal(t, "syntax error", errors[0].Message)
	assert.Equal(t, ErrorSeverityError, errors[0].Severity)
	assert.Equal(t, 15, errors[0].Position)

	// Add a lexer error with type
	_ = ctx.AddErrorWithType(UnterminatedString, "unterminated string")
	errors = ctx.GetErrors()
	assert.Len(t, errors, 2)
	assert.Equal(t, UnterminatedString, errors[1].Type)
	assert.Equal(t, "unterminated string", errors[1].Message)

	// Add a warning
	_ = ctx.AddWarning("deprecated syntax", 5)
	assert.True(t, ctx.HasErrors())
	assert.True(t, ctx.HasWarnings())

	warnings := ctx.GetWarnings()
	assert.Len(t, warnings, 1)
	assert.Equal(t, "deprecated syntax", warnings[0].Message)
	assert.Equal(t, ErrorSeverityWarning, warnings[0].Severity)

	// Add error with hint
	_ = ctx.AddErrorWithHint("missing semicolon", 30, "add ';' at end of statement")
	errors = ctx.GetErrors()
	assert.Len(t, errors, 3)
	assert.Equal(t, "add ';' at end of statement", errors[2].HintText)

	// Clear errors
	ctx.ClearErrors()
	assert.False(t, ctx.HasErrors())
	assert.False(t, ctx.HasWarnings())
	assert.Empty(t, ctx.GetErrors())
	assert.Empty(t, ctx.GetWarnings())
}

// TestParseContextErrorContext tests context-aware error reporting
func TestParseContextErrorContext(t *testing.T) {
	ctx := NewParseContext("SELECT 'hello", nil)

	// Set lexer state to quoted string
	ctx.SetState(StateXQ)

	// Add an error - should include context
	err := ctx.AddErrorWithType(UnterminatedString, "unterminated quoted string")
	assert.Equal(t, "in quoted string", err.Context)
	assert.Contains(t, err.HintText, "closing single quote")

	// Test different states
	ctx.SetState(StateXC)
	err = ctx.AddErrorWithType(UnterminatedComment, "unterminated comment")
	assert.Equal(t, "in comment", err.Context)
	assert.Contains(t, err.HintText, "*/")
}

// TestParseContextPositionUtilities tests position save/restore
func TestParseContextPositionUtilities(t *testing.T) {
	ctx := NewParseContext("SELECT * FROM users;", nil)

	// Advance position
	ctx.AdvanceBy(7) // Move to position 7
	pos, _, _ := ctx.GetCurrentPosition()
	assert.Equal(t, 7, pos)

	// Save position
	savedPos := ctx.SaveCurrentPosition()
	assert.Equal(t, 7, savedPos)

	// Advance more
	ctx.AdvanceBy(5) // Move to position 12
	pos, _, _ = ctx.GetCurrentPosition()
	assert.Equal(t, 12, pos)

	// Restore position
	ctx.RestoreSavedPosition()
	pos, _, _ = ctx.GetCurrentPosition()
	assert.Equal(t, 7, pos)

	// Test PutBack
	ctx.PutBack(3)
	pos, _, _ = ctx.GetCurrentPosition()
	assert.Equal(t, 4, pos)
}

// TestParseContextEOF tests EOF handling
func TestParseContextEOF(t *testing.T) {
	ctx := NewParseContext("ab", nil)

	// Not at EOF initially
	assert.False(t, ctx.AtEOF())

	// Advance to near end
	ctx.AdvanceBy(2)
	assert.True(t, ctx.AtEOF())

	// Test character access at EOF
	assert.Equal(t, rune(0), ctx.CurrentChar())
	assert.Equal(t, rune(0), ctx.PeekChar())

	// Test byte access at EOF
	_, ok := ctx.CurrentByte()
	assert.False(t, ok)

	_, ok = ctx.NextByte()
	assert.False(t, ok)

	// Test rune access at EOF
	assert.Equal(t, rune(0), ctx.PeekRune())
	assert.Equal(t, rune(0), ctx.AdvanceRune())
}

// TestParseContextUnicodeHandling tests Unicode character handling
func TestParseContextUnicodeHandling(t *testing.T) {
	ctx := NewParseContext("Hello 世界", nil)

	// Test UTF-8 rune handling
	assert.Equal(t, 'H', ctx.PeekRune())

	// Advance through ASCII characters
	for i := 0; i < 6; i++ { // "Hello "
		ctx.AdvanceRune()
	}

	// Should be at first Unicode character
	assert.Equal(t, '世', ctx.PeekRune())
	r := ctx.AdvanceRune()
	assert.Equal(t, '世', r)

	// Should be at second Unicode character
	assert.Equal(t, '界', ctx.PeekRune())
	r = ctx.AdvanceRune()
	assert.Equal(t, '界', r)

	// Should be at EOF
	assert.True(t, ctx.AtEOF())
}

// TestParseContextGetCurrentText tests text extraction
func TestParseContextGetCurrentText(t *testing.T) {
	ctx := NewParseContext("SELECT * FROM users;", nil)

	// Get text from start to current position
	text := ctx.GetCurrentText(0)
	assert.Empty(t, text) // Position is still 0

	// Advance and get text
	ctx.AdvanceBy(6)
	text = ctx.GetCurrentText(0)
	assert.Equal(t, "SELECT", text)

	// Get text from middle
	text = ctx.GetCurrentText(2)
	assert.Equal(t, "LECT", text)

	// Invalid range
	text = ctx.GetCurrentText(10)
	assert.Empty(t, text)

	text = ctx.GetCurrentText(-1)
	assert.Empty(t, text)
}

// TestParseContextClone tests context cloning
func TestParseContextClone(t *testing.T) {
	options := &ParseOptions{
		MaxIdentifierLength: 100,
	}
	ctx1 := NewParseContext("SELECT * FROM users;", options)
	_ = ctx1.AddError("test error", 5)
	ctx1.AdvanceBy(5)

	// Clone the context
	ctx2 := ctx1.Clone()

	// Should have same options
	assert.Equal(t, ctx1.GetOptions(), ctx2.GetOptions())

	// Should have different context IDs
	assert.NotEqual(t, ctx1.GetContextID(), ctx2.GetContextID())

	// Should have same source text
	assert.Equal(t, ctx1.GetSourceText(), ctx2.GetSourceText())

	// Should have independent state
	assert.False(t, ctx2.HasErrors()) // Errors not copied
	pos, _, _ := ctx2.GetCurrentPosition()
	assert.Equal(t, 0, pos) // Position reset
}

// TestParseContextSingleThreaded tests that ParseContext works correctly
// Note: ParseContext is designed for single-threaded use. For concurrent parsing,
// each thread should have its own ParseContext instance.
func TestParseContextSingleThreaded(t *testing.T) {
	ctx := NewParseContext("SELECT * FROM users WHERE id = 1;", nil)

	// Test various operations in sequence
	for i := 0; i < 100; i++ {
		// Read operations
		_ = ctx.GetSourceText()
		_, _, _ = ctx.GetCurrentPosition()
		_ = ctx.HasErrors()
		_ = ctx.GetErrors()
		_ = ctx.GetWarnings()
		_ = ctx.GetState()
		_ = ctx.GetDepth()

		// Write operations
		location := i
		_ = ctx.AddError("test error", location)
		ctx.SetCurrentPosition(location, 1, location+1)

		// Depth operations
		_ = ctx.IncrementDepth()
		ctx.DecrementDepth()

		// State operations
		ctx.SetState(StateXQ)
		ctx.SetState(StateInitial)

		// Literal operations
		ctx.StartLiteral()
		ctx.AddLiteral("test")
		_ = ctx.GetLiteral()
	}

	// Verify final state
	errors := ctx.GetErrors()
	assert.Equal(t, 100, len(errors))
	assert.True(t, ctx.HasErrors())
}

// TestParseContextString tests string representation
func TestParseContextString(t *testing.T) {
	ctx := NewParseContext("SELECT * FROM users;", nil)
	ctx.SetCurrentPosition(7, 1, 8)
	_ = ctx.AddError("test error", 5)
	_ = ctx.AddWarning("test warning", 10)
	ctx.SetState(StateXQ)
	_ = ctx.IncrementDepth()

	str := ctx.String()
	assert.Contains(t, str, "ParseContext")
	assert.Contains(t, str, "pos: 7/20") // position/length
	assert.Contains(t, str, "line: 1")
	assert.Contains(t, str, "col: 8")
	assert.Contains(t, str, "errors: 1")
	assert.Contains(t, str, "warnings: 1")
	assert.Contains(t, str, "state: 5") // StateXQ
	assert.Contains(t, str, "depth: 1")
}

// TestParseContextCompatibilityMethods tests legacy compatibility methods
func TestParseContextCompatibilityMethods(t *testing.T) {
	ctx := NewParseContext("SELECT * FROM users;", nil)

	// Test legacy methods
	assert.Equal(t, 0, ctx.GetScanPos())
	assert.Equal(t, len("SELECT * FROM users;"), ctx.GetScanBufLen())
	assert.Equal(t, 0, ctx.GetCurrentPos())

	// Advance and test again
	ctx.AdvanceBy(6)
	assert.Equal(t, 6, ctx.GetScanPos())
	assert.Equal(t, 6, ctx.GetCurrentPos())

	// GetScanBuf should return copy
	buf1 := ctx.GetScanBuf()
	buf2 := ctx.GetScanBuf()
	assert.Equal(t, buf1, buf2)
	// Modifying one shouldn't affect the other (they are copies)
	if len(buf1) > 0 {
		buf1[0] = 'X'
		assert.NotEqual(t, buf1[0], buf2[0])
	}
}

// TestParseContextDefaultOptions tests default option values
func TestParseContextDefaultOptions(t *testing.T) {
	ctx := NewParseContext("test", nil)
	options := ctx.GetOptions()

	// Test PostgreSQL standard defaults
	assert.True(t, options.StandardConformingStrings)
	assert.True(t, options.EscapeStringWarning)
	assert.Equal(t, BackslashQuoteSafeEncoding, options.BackslashQuote)

	// Test parser limits
	assert.Equal(t, 63, options.MaxIdentifierLength)
	assert.Equal(t, 1000, options.MaxExpressionDepth)
	assert.Equal(t, 1024*1024, options.MaxStatementLength)

	// Test error handling
	assert.False(t, options.StopOnFirstError)
	assert.True(t, options.CollectAllErrors)

	// Test feature flags
	assert.True(t, options.EnableExtensions)
	assert.True(t, options.EnableWindowFuncs)
	assert.True(t, options.EnablePartitioning)
	assert.True(t, options.EnableMergeStmt)
}

// TestParseContextLineColumnTracking tests line/column calculation
func TestParseContextLineColumnTracking(t *testing.T) {
	sourceText := "SELECT *\nFROM users\nWHERE id = 1;"
	ctx := NewParseContext(sourceText, nil)

	tests := []struct {
		name           string
		advanceBy      int
		expectedLine   int
		expectedColumn int
	}{
		{"start_of_file", 0, 1, 1},
		{"middle_first_line", 4, 1, 5},
		{"end_first_line", 8, 1, 9},
		{"start_second_line", 9, 2, 1},
		{"middle_second_line", 13, 2, 5},
		{"start_third_line", 20, 3, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx.SetSourceText(sourceText) // Reset
			ctx.AdvanceBy(tt.advanceBy)
			_, line, col := ctx.GetCurrentPosition()
			assert.Equal(t, tt.expectedLine, line, "Line mismatch")
			assert.Equal(t, tt.expectedColumn, col, "Column mismatch")
		})
	}
}

// BenchmarkParseContextOperations benchmarks common context operations
func BenchmarkParseContextOperations(b *testing.B) {
	ctx := NewParseContext("SELECT * FROM users WHERE id = 1;", nil)

	b.Run("AddError", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = ctx.AddError("benchmark error", i%100)
		}
		ctx.ClearErrors() // Clean up
	})

	b.Run("GetCurrentPosition", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _, _ = ctx.GetCurrentPosition()
		}
	})

	b.Run("IncrementDecrementDepth", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = ctx.IncrementDepth()
			ctx.DecrementDepth()
		}
	})

	b.Run("SetGetState", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ctx.SetState(StateXQ)
			_ = ctx.GetState()
		}
	})

	b.Run("LiteralOperations", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ctx.StartLiteral()
			ctx.AddLiteral("test")
			_ = ctx.GetLiteral()
		}
	})

	b.Run("HasErrors", func(b *testing.B) {
		_ = ctx.AddError("test", 0) // Add one error
		for i := 0; i < b.N; i++ {
			_ = ctx.HasErrors()
		}
		ctx.ClearErrors() // Clean up
	})
}
