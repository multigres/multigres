/*
 * Common Test Helpers for PostgreSQL Parser Tests
 *
 * This file contains shared utility functions used across multiple test files
 * to reduce code duplication and improve maintainability.
 */

package parser

import (
	"testing"

	"github.com/multigres/parser/go/parser/ast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ScanAllTokens scans all tokens from a lexer and returns them as a slice
// This is the consolidated version of the scanAllTokens function that was
// duplicated across multiple test files
func ScanAllTokens(t *testing.T, lexer *Lexer) []Token {
	tokens := []Token{}
	for {
		token := lexer.NextToken()
		require.NotNil(t, token, "Token should not be nil")
		tokens = append(tokens, *token)
		if token.Type == EOF {
			break
		}
	}
	return tokens
}

// AssertNoLexerErrors verifies that the lexer has no errors
func AssertNoLexerErrors(t *testing.T, lexer *Lexer) {
	if lexer.HasErrors() {
		errors := lexer.GetErrors()
		require.False(t, true, "Expected no lexer errors, but got: %v", errors)
	}
}

// AssertLexerHasErrors verifies that the lexer has errors
func AssertLexerHasErrors(t *testing.T, lexer *Lexer) {
	require.True(t, lexer.HasErrors(), "Expected lexer to have errors")
}

// AssertTokenSequence tests that an input produces the expected sequence of token types
func AssertTokenSequence(t *testing.T, input string, expectedTypes []TokenType) {
	lexer := NewLexer(input)
	tokens := ScanAllTokens(t, lexer)
	
	require.Len(t, tokens, len(expectedTypes)+1, "Expected %d tokens plus EOF", len(expectedTypes))
	for i, expectedType := range expectedTypes {
		assert.Equal(t, expectedType, tokens[i].Type, "Token %d type mismatch", i)
	}
	
	// Last token should be EOF
	assert.Equal(t, EOF, tokens[len(tokens)-1].Type, "Last token should be EOF")
}

// AssertTokenTypes verifies that tokens match expected types
func AssertTokenTypes(t *testing.T, tokens []Token, expectedTypes []TokenType) {
	require.Len(t, tokens, len(expectedTypes), "Token count mismatch")
	for i, expectedType := range expectedTypes {
		assert.Equal(t, expectedType, tokens[i].Type, "Token %d type mismatch", i)
	}
}

// AssertTokenValues verifies that tokens match expected string values
func AssertTokenValues(t *testing.T, tokens []Token, expectedValues []string) {
	require.Len(t, tokens, len(expectedValues), "Token count mismatch")
	for i, expectedValue := range expectedValues {
		assert.Equal(t, expectedValue, tokens[i].Value.Str, "Token %d value mismatch", i)
	}
}

// AssertTokenTypesAndValues verifies both types and values
func AssertTokenTypesAndValues(t *testing.T, tokens []Token, expected []struct {
	Type  TokenType
	Value string
}) {
	require.Len(t, tokens, len(expected), "Token count mismatch")
	for i, exp := range expected {
		assert.Equal(t, exp.Type, tokens[i].Type, "Token %d type mismatch", i)
		if exp.Value != "" {
			assert.Equal(t, exp.Value, tokens[i].Value.Str, "Token %d value mismatch", i)
		}
	}
}

// AssertParseSQL tests that SQL parses without error and returns expected number of statements
func AssertParseSQL(t *testing.T, sql string, expectedStmtCount int) []ast.Stmt {
	stmts, err := ParseSQL(sql)
	require.NoError(t, err, "Failed to parse SQL: %s", sql)
	require.Len(t, stmts, expectedStmtCount, "Expected %d statements", expectedStmtCount)
	return stmts
}

// AssertRoundTripSQL tests that SQL can be parsed and deparsed consistently
func AssertRoundTripSQL(t *testing.T, sql string, expectedSQL string) {
	// Parse the input
	stmts, err := ParseSQL(sql)
	require.NoError(t, err, "Failed to parse SQL: %s", sql)
	require.Len(t, stmts, 1, "Expected exactly one statement")

	// Deparse the statement
	deparsed := stmts[0].SqlString()
	require.NotEmpty(t, deparsed, "Deparsed SQL should not be empty")

	// Determine expected output
	expected := expectedSQL
	if expected == "" {
		expected = sql
	}

	// Check if output matches expected
	assert.Equal(t, expected, deparsed, "Deparsed SQL should match expected output")

	// Test round-trip parsing (re-parse the deparsed SQL)
	stmts2, err2 := ParseSQL(deparsed)
	require.NoError(t, err2, "Re-parsing deparsed SQL should succeed: %s", deparsed)
	require.Len(t, stmts2, 1, "Re-parsed should have exactly one statement")

	// Verify statement types match
	assert.Equal(t, stmts[0].StatementType(), stmts2[0].StatementType(),
		"Statement types should match after round-trip")

	// Test stability - second deparse should match first
	deparsed2 := stmts2[0].SqlString()
	assert.Equal(t, deparsed, deparsed2,
		"Deparsing should be stable.\nFirst: %s\nSecond: %s", deparsed, deparsed2)
}

// AssertTokenPosition verifies token position information
func AssertTokenPosition(t *testing.T, token *Token, expectedPosition int) {
	assert.Equal(t, expectedPosition, token.Position, "Token position mismatch")
}

// AssertLexerContextError verifies specific error types in lexer context
func AssertLexerContextError(t *testing.T, lexer *Lexer, expectedErrorType LexerErrorType) {
	require.True(t, lexer.GetContext().HasErrors(), "Expected lexer to have errors")
	errors := lexer.GetContext().GetErrors()
	require.NotEmpty(t, errors, "Expected at least one error")
	
	found := false
	for _, err := range errors {
		if err.Type == expectedErrorType {
			found = true
			break
		}
	}
	assert.True(t, found, "Expected to find error type %v in errors: %v", expectedErrorType, errors)
}