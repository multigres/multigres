package parser

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/parser/go/parser/ast"
)

// TestErrorSeverity tests the error severity enum and string representation.
func TestErrorSeverity(t *testing.T) {
	tests := []struct {
		severity ErrorSeverity
		expected string
	}{
		{ErrorSeverityNotice, "NOTICE"},
		{ErrorSeverityWarning, "WARNING"},
		{ErrorSeverityError, "ERROR"},
		{ErrorSeverityFatal, "FATAL"},
		{ErrorSeverityPanic, "PANIC"},
		{ErrorSeverity(999), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := tt.severity.String()
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestParseError tests the ParseError structure and error formatting.
func TestParseError(t *testing.T) {
	tests := []struct {
		name     string
		error    ParseError
		expected string
	}{
		{
			name: "error_with_line_column",
			error: ParseError{
				Message:  "syntax error",
				Severity: ErrorSeverityError,
				Line:     10,
				Column:   5,
			},
			expected: "ERROR at line 10, column 5: syntax error",
		},
		{
			name: "error_with_location",
			error: ParseError{
				Message:  "unexpected token",
				Severity: ErrorSeverityWarning,
				Location: 25,
			},
			expected: "WARNING at position 25: unexpected token",
		},
		{
			name: "error_minimal",
			error: ParseError{
				Message:  "parse failed",
				Severity: ErrorSeverityFatal,
				Location: -1,
			},
			expected: "FATAL: parse failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.error.Error()
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestDefaultParseOptions tests the default parsing options.
func TestDefaultParseOptions(t *testing.T) {
	options := DefaultParseOptions()
	require.NotNil(t, options)

	// Test PostgreSQL standard defaults
	assert.True(t, options.StandardConformingStrings)
	assert.True(t, options.EscapeStringWarning)
	assert.Equal(t, 2, options.BackslashQuote)

	// Test parser limits
	assert.Equal(t, 63, options.MaxIdentifierLength) // PostgreSQL NAMEDATALEN - 1
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

// TestNewParserContext tests parser context creation.
func TestNewParserContext(t *testing.T) {
	// Test with default options
	ctx1 := NewParserContext(nil)
	require.NotNil(t, ctx1)
	assert.NotNil(t, ctx1.GetOptions())
	assert.NotEmpty(t, ctx1.GetContextID())

	// Test with custom options
	customOptions := &ParseOptions{
		MaxIdentifierLength: 100,
		StopOnFirstError:    true,
	}
	ctx2 := NewParserContext(customOptions)
	require.NotNil(t, ctx2)
	assert.Equal(t, customOptions, ctx2.GetOptions())
}

// TestParserContextSourceText tests source text management.
func TestParserContextSourceText(t *testing.T) {
	ctx := NewParserContext(nil)
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
}

// TestParserContextPosition tests position tracking.
func TestParserContextPosition(t *testing.T) {
	ctx := NewParserContext(nil)
	ctx.SetSourceText("SELECT * FROM users;")

	// Set position
	ctx.SetCurrentPosition(7, 1, 8)

	pos, line, col := ctx.GetCurrentPosition()
	assert.Equal(t, 7, pos)
	assert.Equal(t, 1, line)
	assert.Equal(t, 8, col)
}

// TestParserContextErrors tests error collection and management.
func TestParserContextErrors(t *testing.T) {
	ctx := NewParserContext(nil)
	ctx.SetSourceText("SELECT * FROM users\nWHERE id = ?;")

	// Initially no errors
	assert.False(t, ctx.HasErrors())
	assert.False(t, ctx.HasWarnings())
	assert.Empty(t, ctx.GetErrors())
	assert.Empty(t, ctx.GetWarnings())

	// Add an error
	ctx.AddError("syntax error", 15)
	assert.True(t, ctx.HasErrors())
	assert.False(t, ctx.HasWarnings())

	errors := ctx.GetErrors()
	assert.Len(t, errors, 1)
	assert.Equal(t, "syntax error", errors[0].Message)
	assert.Equal(t, ErrorSeverityError, errors[0].Severity)
	assert.Equal(t, 15, errors[0].Location)

	// Add a warning
	ctx.AddWarning("deprecated syntax", 5)
	assert.True(t, ctx.HasErrors())
	assert.True(t, ctx.HasWarnings())

	warnings := ctx.GetWarnings()
	assert.Len(t, warnings, 1)
	assert.Equal(t, "deprecated syntax", warnings[0].Message)
	assert.Equal(t, ErrorSeverityWarning, warnings[0].Severity)

	// Add error with hint
	ctx.AddErrorWithHint("missing semicolon", 30, "add ';' at end of statement")
	errors = ctx.GetErrors()
	assert.Len(t, errors, 2)
	assert.Equal(t, "add ';' at end of statement", errors[1].HintText)

	// Clear errors
	ctx.ClearErrors()
	assert.False(t, ctx.HasErrors())
	assert.False(t, ctx.HasWarnings())
	assert.Empty(t, ctx.GetErrors())
	assert.Empty(t, ctx.GetWarnings())
}

// TestLineColumnCalculation tests line and column calculation from byte offset.
func TestLineColumnCalculationError(t *testing.T) {
	ctx := NewParserContext(nil)
	sourceText := "SELECT *\nFROM users\nWHERE id = 1;"
	ctx.SetSourceText(sourceText)

	tests := []struct {
		name     string
		location int
		line     int
		column   int
	}{
		{"start_of_file", 0, 1, 1},
		{"middle_first_line", 4, 1, 5},
		{"end_first_line", 8, 1, 9},
		{"start_second_line", 9, 2, 1},
		{"middle_second_line", 13, 2, 5},
		{"start_third_line", 20, 3, 1},
		{"end_of_file", len(sourceText), 3, 14},
		{"invalid_negative", -1, -1, -1},
		{"invalid_too_large", len(sourceText) + 10, -1, -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx.AddError("test error", tt.location)
			errors := ctx.GetErrors()
			if tt.line == -1 && tt.column == -1 {
				// For invalid locations, we still create the error but with invalid line/col
				assert.True(t, len(errors) > 0)
			} else {
				require.NotEmpty(t, errors)
				lastError := errors[len(errors)-1]
				assert.Equal(t, tt.line, lastError.Line, "Line mismatch for location %d", tt.location)
				assert.Equal(t, tt.column, lastError.Column, "Column mismatch for location %d", tt.location)
			}
			ctx.ClearErrors() // Clear for next test
		})
	}
}

// TestParserContextParseTree tests parse tree management.
func TestParserContextParseTree(t *testing.T) {
	ctx := NewParserContext(nil)

	// Initially no parse tree
	assert.Nil(t, ctx.GetParseTree())

	// Set parse tree
	tree := ast.NewIdentifier("test")
	ctx.SetParseTree(tree)
	assert.Equal(t, tree, ctx.GetParseTree())
}

// TestParserContextDepthTracking tests expression depth tracking.
func TestParserContextDepthTracking(t *testing.T) {
	options := &ParseOptions{
		MaxExpressionDepth: 3,
	}
	ctx := NewParserContext(options)

	// Initially depth is 0
	assert.Equal(t, 0, ctx.GetDepth())

	// Increment depth
	err := ctx.IncrementDepth()
	assert.NoError(t, err)
	assert.Equal(t, 1, ctx.GetDepth())

	err = ctx.IncrementDepth()
	assert.NoError(t, err)
	assert.Equal(t, 2, ctx.GetDepth())

	err = ctx.IncrementDepth()
	assert.NoError(t, err)
	assert.Equal(t, 3, ctx.GetDepth())

	// Should fail to exceed maximum
	err = ctx.IncrementDepth()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expression too complex")
	assert.Equal(t, 4, ctx.GetDepth()) // Depth still incremented even though error returned

	// Decrement depth
	ctx.DecrementDepth()
	assert.Equal(t, 3, ctx.GetDepth())

	ctx.DecrementDepth()
	ctx.DecrementDepth()
	ctx.DecrementDepth()
	assert.Equal(t, 0, ctx.GetDepth())

	// Shouldn't go negative
	ctx.DecrementDepth()
	assert.Equal(t, 0, ctx.GetDepth())
}

// TestParserContextStatementBoundaries tests statement boundary tracking.
func TestParserContextStatementBoundaries(t *testing.T) {
	ctx := NewParserContext(nil)

	// Initially no boundaries set
	start, end := ctx.GetStatementBoundaries()
	assert.Equal(t, 0, start)
	assert.Equal(t, 0, end)

	// Set boundaries
	ctx.SetStatementBoundaries(10, 25)
	start, end = ctx.GetStatementBoundaries()
	assert.Equal(t, 10, start)
	assert.Equal(t, 25, end)
}

// TestParserContextClone tests context cloning for thread safety.
func TestParserContextClone(t *testing.T) {
	options := &ParseOptions{
		MaxIdentifierLength: 100,
	}
	ctx1 := NewParserContext(options)
	ctx1.SetSourceText("SELECT * FROM users;")
	ctx1.AddError("test error", 5)

	// Clone the context
	ctx2 := ctx1.Clone()

	// Should have same options
	assert.Equal(t, ctx1.GetOptions(), ctx2.GetOptions())

	// Should have different context IDs
	assert.NotEqual(t, ctx1.GetContextID(), ctx2.GetContextID())

	// Should have independent state
	assert.Empty(t, ctx2.GetSourceText())
	assert.False(t, ctx2.HasErrors())
}

// TestParserContextConcurrency tests thread safety of parser context.
func TestParserContextConcurrency(t *testing.T) {
	ctx := NewParserContext(nil)
	ctx.SetSourceText("SELECT * FROM users WHERE id = 1;")

	// Run concurrent operations
	var wg sync.WaitGroup
	numGoroutines := 10
	operationsPerGoroutine := 100

	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < operationsPerGoroutine; j++ {
				// Concurrent read operations
				_ = ctx.GetSourceText()
				_, _, _ = ctx.GetCurrentPosition()
				_ = ctx.HasErrors()
				_ = ctx.GetErrors()
				_ = ctx.GetWarnings()

				// Concurrent write operations
				location := goroutineID*operationsPerGoroutine + j
				ctx.AddError("concurrent error", location)
				ctx.SetCurrentPosition(location, 1, location+1)

				// Depth operations
				_ = ctx.IncrementDepth()
				ctx.DecrementDepth()
			}
		}(i)
	}

	wg.Wait()

	// Verify final state is consistent
	errors := ctx.GetErrors()
	assert.Equal(t, numGoroutines*operationsPerGoroutine, len(errors))
	assert.True(t, ctx.HasErrors())
}

// TestParserContextString tests the string representation.
func TestParserContextString(t *testing.T) {
	ctx := NewParserContext(nil)
	ctx.SetSourceText("SELECT * FROM users;")
	ctx.SetCurrentPosition(7, 1, 8)
	ctx.AddError("test error", 5)
	ctx.AddWarning("test warning", 10)

	str := ctx.String()
	assert.Contains(t, str, "ParserContext")
	assert.Contains(t, str, "pos: 7/20") // position/length (SELECT * FROM users; is 20 chars)
	assert.Contains(t, str, "line: 1")
	assert.Contains(t, str, "col: 8")
	assert.Contains(t, str, "errors: 1")
	assert.Contains(t, str, "warnings: 1")
}

// BenchmarkParserContextOperations benchmarks common context operations.
func BenchmarkParserContextOperations(b *testing.B) {
	ctx := NewParserContext(nil)
	ctx.SetSourceText("SELECT * FROM users WHERE id = 1;")

	b.Run("AddError", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ctx.AddError("benchmark error", i%100)
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

	b.Run("HasErrors", func(b *testing.B) {
		ctx.AddError("test", 0) // Add one error
		for i := 0; i < b.N; i++ {
			_ = ctx.HasErrors()
		}
		ctx.ClearErrors() // Clean up
	})
}
