package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBasicParsing tests basic parser functionality with empty input and semicolons
func TestBasicParsing(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantError bool
	}{
		{
			name:      "empty input",
			input:     "",
			wantError: false,
		},
		{
			name:      "single semicolon",
			input:     ";",
			wantError: false,
		},
		{
			name:      "multiple semicolons",
			input:     ";;;",
			wantError: false,
		},
		{
			name:      "whitespace and semicolons",
			input:     "  ;  ;  ",
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			result := yyParse(lexer)
			
			if tt.wantError {
				assert.NotEqual(t, 0, result, "expected parse error, but got success")
			} else {
				assert.Equal(t, 0, result, "expected parse success, but got error code %d", result)
				if lexer.HasErrors() {
					errors := lexer.GetErrors()
					assert.False(t, true, "unexpected parse errors: %v", errors)
				}
			}
		})
	}
}


// TestEmptyStatementParsing tests parsing of empty statements and semicolons
func TestEmptyStatementParsing(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		expectTree   bool
		stmtCount    int
	}{
		{
			name:       "empty input produces empty tree",
			input:      "",
			expectTree: true,
			stmtCount:  0,
		},
		{
			name:       "single semicolon produces empty tree",
			input:      ";",
			expectTree: true,
			stmtCount:  0,
		},
		{
			name:       "multiple semicolons produce empty tree",
			input:      ";;;",
			expectTree: true,
			stmtCount:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree, err := ParseSQL(tt.input)
			
			require.NoError(t, err, "unexpected error: %v", err)
			
			if !tt.expectTree {
				assert.Nil(t, tree, "expected nil tree, got %v", tree)
				return
			}
			
			if tt.expectTree {
				require.NotNil(t, tree, "expected non-nil tree, got nil")
				assert.Equal(t, tt.stmtCount, len(tree), "expected %d statements, got %d", tt.stmtCount, len(tree))
			}
		})
	}
}

func TestLimitOffsetParsing(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		// Basic LIMIT
		{
			name:  "simple limit",
			input: "SELECT * FROM users LIMIT 10",
		},
		{
			name:  "simple offset",
			input: "SELECT * FROM users OFFSET 5",
		},
		{
			name:  "limit with offset",
			input: "SELECT * FROM users LIMIT 10 OFFSET 5",
		},
		{
			name:  "offset with limit",
			input: "SELECT * FROM users OFFSET 5 LIMIT 10",
		},
		{
			name:  "limit all",
			input: "SELECT * FROM users LIMIT ALL",
		},
		{
			name:  "limit with expression",
			input: "SELECT * FROM users LIMIT 5 + 5",
		},
		{
			name:  "offset with expression",
			input: "SELECT * FROM users OFFSET 2 * 3",
		},

		// FETCH FIRST syntax (SQL:2008)
		{
			name:  "fetch first row only",
			input: "SELECT * FROM users FETCH FIRST ROW ONLY",
		},
		{
			name:  "fetch first rows only",
			input: "SELECT * FROM users FETCH FIRST ROWS ONLY",
		},
		{
			name:  "fetch next row only",
			input: "SELECT * FROM users FETCH NEXT ROW ONLY",
		},
		{
			name:  "fetch first 5 rows only",
			input: "SELECT * FROM users FETCH FIRST 5 ROWS ONLY",
		},
		{
			name:  "fetch next 10 rows only",
			input: "SELECT * FROM users FETCH NEXT 10 ROWS ONLY",
		},
		{
			name:  "fetch first with ties",
			input: "SELECT * FROM users ORDER BY score FETCH FIRST ROW WITH TIES",
		},
		{
			name:  "fetch first 5 rows with ties",
			input: "SELECT * FROM users ORDER BY score FETCH FIRST 5 ROWS WITH TIES",
		},
		{
			name:  "offset with fetch first",
			input: "SELECT * FROM users OFFSET 10 ROWS FETCH NEXT 5 ROWS ONLY",
		},
		{
			name:  "offset with row keyword",
			input: "SELECT * FROM users OFFSET 10 ROW",
		},
		{
			name:  "offset with rows keyword",
			input: "SELECT * FROM users OFFSET 10 ROWS",
		},

		// With ORDER BY
		{
			name:  "order by with limit",
			input: "SELECT * FROM users ORDER BY id LIMIT 10",
		},
		{
			name:  "order by with limit and offset",
			input: "SELECT * FROM users ORDER BY id LIMIT 10 OFFSET 5",
		},
		{
			name:  "order by with fetch first",
			input: "SELECT * FROM users ORDER BY score DESC FETCH FIRST 3 ROWS ONLY",
		},
		{
			name:  "order by with fetch first with ties",
			input: "SELECT * FROM users ORDER BY score DESC FETCH FIRST 3 ROWS WITH TIES",
		},

		// Complex queries
		{
			name:  "with clause and limit",
			input: "WITH top_users AS (SELECT * FROM users) SELECT * FROM top_users LIMIT 10",
		},
		{
			name:  "subquery with limit",
			input: "SELECT * FROM (SELECT * FROM users LIMIT 10) AS limited_users",
		},
		{
			name:  "limit with complex expression",
			input: "SELECT * FROM users LIMIT (SELECT COUNT(*) / 10 FROM users)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmts, err := ParseSQL(test.input)
			require.NoError(t, err, "Failed to parse: %v", err)
			require.Len(t, stmts, 1, "Expected 1 statement, got %d", len(stmts))

			// Basic validation - just ensure it parses without error
			// The actual AST structure validation can be more detailed if needed
			assert.NotNil(t, stmts[0], "Parsed statement is nil")
		})
	}
}

func TestLimitOffsetErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "deprecated limit comma syntax",
			input: "SELECT * FROM users LIMIT 10, 5", // Should error - deprecated syntax
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseSQL(test.input)
			assert.Error(t, err, "Expected parse error but got none")
		})
	}
}
