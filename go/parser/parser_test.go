package parser

import (
	"testing"
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
				if result == 0 {
					t.Errorf("expected parse error, but got success")
				}
			} else {
				if result != 0 {
					t.Errorf("expected parse success, but got error code %d", result)
				}
				if lexer.HasErrors() {
					errors := lexer.GetErrors()
					t.Errorf("unexpected parse errors: %v", errors)
				}
			}
		})
	}
}

// TestLexerTokenGeneration tests that the lexer generates correct token sequences
func TestLexerTokenGeneration(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		tokens []TokenType
	}{
		{
			name:   "identifier token",
			input:  "test_id",
			tokens: []TokenType{IDENT},
		},
		{
			name:   "integer constant",
			input:  "123",
			tokens: []TokenType{ICONST},
		},
		{
			name:   "string constant",
			input:  "'hello'",
			tokens: []TokenType{SCONST},
		},
		{
			name:   "multiple tokens",
			input:  "id1, id2",
			tokens: []TokenType{IDENT, ',', IDENT},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			
			// Collect tokens
			var tokens []TokenType
			for {
				token := lexer.NextToken()
				if token == nil || token.Type == EOF {
					break
				}
				tokens = append(tokens, token.Type)
			}
			
			// Verify token sequence
			if len(tokens) != len(tt.tokens) {
				t.Errorf("expected %d tokens, got %d", len(tt.tokens), len(tokens))
				return
			}
			
			for i, expectedType := range tt.tokens {
				if tokens[i] != expectedType {
					t.Errorf("token %d: expected %v, got %v", i, expectedType, tokens[i])
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
			
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			
			if !tt.expectTree && tree != nil {
				t.Errorf("expected nil tree, got %v", tree)
				return
			}
			
			if tt.expectTree {
				if tree == nil {
					t.Error("expected non-nil tree, got nil")
					return
				}
				
				if len(tree) != tt.stmtCount {
					t.Errorf("expected %d statements, got %d", tt.stmtCount, len(tree))
				}
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
			if err != nil {
				t.Fatalf("Failed to parse: %v", err)
			}

			if len(stmts) != 1 {
				t.Fatalf("Expected 1 statement, got %d", len(stmts))
			}

			// Basic validation - just ensure it parses without error
			// The actual AST structure validation can be more detailed if needed
			if stmts[0] == nil {
				t.Fatal("Parsed statement is nil")
			}
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
			if err == nil {
				t.Fatal("Expected parse error but got none")
			}
		})
	}
}
