package parser

import (
	"testing"
)

// TestLexicalTokenizing tests that expression-related tokens are correctly recognized
func TestLexicalTokenizing(t *testing.T) {
	tests := []struct {
		name               string
		input              string
		expectedTokenCount int
	}{
		{"simple identifier", "column1", 1},
		{"qualified name", "schema.table", 3},  // schema, dot, table
		{"arithmetic expr", "a + b", 3},        // a, +, b
		{"function call", "func(x)", 4},        // func, (, x, )
		{"type cast", "123::text", 3},          // 123, ::, text
		{"comparison", "a = b", 3},             // a, =, b
		{"logical expr", "x AND y", 3},         // x, AND, y
		{"complex expr", "func(a, b) + 10", 8}, // func, (, a, ,, b, ), +, 10
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)

			var tokenCount int
			for {
				token := lexer.NextToken()
				if token == nil || token.Type == EOF {
					break
				}
				tokenCount++
			}

			if lexer.HasErrors() {
				t.Errorf("Unexpected lexer errors for %q: %v", tt.input, lexer.GetErrors())
			}

			if tokenCount != tt.expectedTokenCount {
				t.Errorf("Expected %d tokens for %q, got %d", tt.expectedTokenCount, tt.input, tokenCount)
			}
		})
	}
}

// TestExpressionTokenRecognition tests that expression tokens are properly categorized
func TestExpressionTokenRecognition(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		expectedType TokenType
	}{
		// Constants
		{"integer", "123", ICONST},
		{"float", "3.14", FCONST},
		{"string", "'hello'", SCONST},

		// Identifiers
		{"identifier", "column", COLUMN},

		// Operators
		{"typecast", "::", TYPECAST},
		{"less equal", "<=", LESS_EQUALS},
		{"greater equal", ">=", GREATER_EQUALS},
		{"not equal", "<>", NOT_EQUALS},

		// Keywords that are correctly mapped
		{"or keyword", "OR", OR},
		{"not keyword", "NOT", NOT},
		{"and keyword", "AND", AND},

		// Keywords now correctly mapped to their tokens
		{"true keyword", "TRUE", TRUE_P},
		{"false keyword", "FALSE", FALSE_P},
		{"null keyword", "NULL", NULL_P},
		{"bit keyword", "BIT", BIT},
		{"numeric keyword", "NUMERIC", NUMERIC},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			token := lexer.NextToken()

			if token == nil {
				t.Errorf("Expected token for %q, got nil", tt.input)
				return
			}

			if token.Type != tt.expectedType {
				t.Errorf("Expected token type %v for %q, got %v", tt.expectedType, tt.input, token.Type)
			}

			if lexer.HasErrors() {
				t.Errorf("Unexpected lexer errors for %q: %v", tt.input, lexer.GetErrors())
			}
		})
	}
}
