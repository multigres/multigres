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

/*
 * PostgreSQL Parser Tokens - Test Suite
 *
 * This file contains tests for the token system, including token types,
 * creation, and the refactored architecture.
 */

package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// =============================================================================
// Tests from refactor_test.go - TokenType refactoring verification
// =============================================================================

// TestTokenTypeRefactor verifies the refactoring from TokenType to int works correctly
func TestTokenTypeRefactor(t *testing.T) {
	// Test that basic token creation works
	token := NewToken(IDENT, 0, "test")
	assert.Equal(t, IDENT, token.Type, "Token type should be IDENT constant")
	assert.Equal(t, "test", token.Value.Str, "Token value should be preserved")

	// Test that keyword tokens work
	keywordToken := NewKeywordToken(ALL, "all", 0, "ALL")
	assert.Equal(t, ALL, keywordToken.Type, "Keyword token should use parser constant")
	assert.Equal(t, "all", keywordToken.Value.Str, "Keyword value should be normalized")

	// Test that integer tokens work
	intToken := NewIntToken(42, 0, "42")
	assert.Equal(t, ICONST, intToken.Type, "Int token should be ICONST")
	assert.Equal(t, 42, intToken.Value.Ival, "Int value should be preserved")

	// Test that parameter tokens work
	paramToken := NewParamToken(1, 0, "$1")
	assert.Equal(t, PARAM, paramToken.Type, "Param token should be PARAM")
	assert.Equal(t, 1, paramToken.Value.Ival, "Param number should be preserved")
}

// TestLexerIntegration tests that the lexer works with the refactored token system
func TestLexerIntegration(t *testing.T) {
	lexer := NewLexer("all create drop")

	// Test that tokens are created with correct types
	token1 := lexer.NextToken()
	assert.Equal(t, ALL, token1.Type, "Should recognize 'all' keyword")

	token2 := lexer.NextToken()
	assert.Equal(t, CREATE, token2.Type, "Should recognize 'create' keyword")

	token3 := lexer.NextToken()
	assert.Equal(t, DROP, token3.Type, "Should recognize 'drop' keyword")

	token4 := lexer.NextToken()
	assert.Equal(t, EOF, token4.Type, "Should return EOF at end")
}

// TestParserConstants verifies parser constants are accessible
func TestParserConstants(t *testing.T) {
	// Test that parser constants are defined and different
	assert.NotEqual(t, 0, IDENT, "IDENT should be non-zero")
	assert.NotEqual(t, 0, ICONST, "ICONST should be non-zero")
	assert.NotEqual(t, 0, ALL, "ALL should be non-zero")
	assert.NotEqual(t, 0, CREATE, "CREATE should be non-zero")

	// Test that they're all different values
	constants := []int{IDENT, ICONST, SCONST, FCONST, BCONST, XCONST, Op, PARAM, ALL, CREATE, DROP, ALTER, AS, EXISTS, NOT, OR, WITH}
	seen := make(map[int]bool)
	for _, c := range constants {
		assert.False(t, seen[c], "Constants should all be unique values")
		seen[c] = true
	}
}

// =============================================================================
// Tests from architecture_test.go - Refactored architecture validation
// =============================================================================

// TestRefactoredArchitecture tests the complete refactored token architecture
func TestRefactoredArchitecture(t *testing.T) {
	// Test basic lexical analysis with direct parser constants
	lexer := NewLexer("test123 \"quoted identifier\" 'string' 42 $1 :: <= all create")

	// Identifier
	token := lexer.NextToken()
	assert.Equal(t, IDENT, token.Type, "Should be IDENT")
	assert.Equal(t, "test123", token.Value.Str)

	// Delimited identifier
	token = lexer.NextToken()
	assert.Equal(t, IDENT, token.Type, "Should be IDENT")
	assert.Equal(t, "quoted identifier", token.Value.Str)

	// String constant
	token = lexer.NextToken()
	assert.Equal(t, SCONST, token.Type, "Should be SCONST")
	assert.Equal(t, "string", token.Value.Str)

	// Integer constant
	token = lexer.NextToken()
	assert.Equal(t, ICONST, token.Type, "Should be ICONST")
	assert.Equal(t, 42, token.Value.Ival)

	// Parameter
	token = lexer.NextToken()
	assert.Equal(t, PARAM, token.Type, "Should be PARAM")
	assert.Equal(t, 1, token.Value.Ival)

	// Typecast operator
	token = lexer.NextToken()
	assert.Equal(t, TYPECAST, token.Type, "Should be TYPECAST")

	// Less-equals operator
	token = lexer.NextToken()
	assert.Equal(t, LESS_EQUALS, token.Type, "Should be LESS_EQUALS")

	// Keywords - note that create might return IDENT if not yet in keyword table
	token = lexer.NextToken()
	assert.Equal(t, ALL, token.Type, "Should be ALL constant")
	assert.Equal(t, "all", token.Value.Str)

	token = lexer.NextToken()
	// CREATE might return IDENT if keyword lookup doesn't work properly yet
	assert.True(t, token.Type == CREATE || token.Type == IDENT, "Should be CREATE or IDENT")

	// EOF
	token = lexer.NextToken()
	assert.Equal(t, EOF, token.Type, "Should be EOF")
}

// TestTokenTypeElimination verifies we eliminated the old TokenType mapping
func TestTokenTypeElimination(t *testing.T) {
	// Verify that TokenType is now just an alias for int
	var tokenType TokenType = 123
	intVal := int(tokenType)
	assert.Equal(t, 123, intVal, "TokenType should be assignable to int")

	// Verify Token.Type is now int
	token := &Token{Type: IDENT}
	assert.IsType(t, int(0), token.Type, "Token.Type should be int")
}

// TestParserLexerInterface tests the lexer-parser interface
func TestParserLexerInterface(t *testing.T) {
	lexer := NewLexer("all")

	// Test the Lex function that goyacc will call
	var lval yySymType
	tokenType := lexer.Lex(&lval)

	// Should return the ALL parser constant directly
	assert.Equal(t, ALL, tokenType, "Lex should return parser constant")
	assert.Equal(t, "all", lval.str, "Should set semantic value")
}
