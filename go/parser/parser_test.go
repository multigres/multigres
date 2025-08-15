package parser

import (
	"testing"
)

// TestPhase3ABasicParsing tests the foundational parser infrastructure
func TestPhase3ABasicParsing(t *testing.T) {
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

// TestPhase3ALexerParserIntegration tests that the lexer correctly interfaces with the parser
func TestPhase3ALexerParserIntegration(t *testing.T) {
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

// TestPhase3AParseTree tests that the parse tree is correctly constructed
func TestPhase3AParseTree(t *testing.T) {
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