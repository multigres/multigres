/*
 * PostgreSQL AST Deparsing Tests
 *
 * This file contains tests for SQL deparsing functionality - converting
 * AST nodes back to their SQL string representation. This ensures round-trip
 * compatibility: SQL -> AST -> SQL
 */

package ast

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestSQLUtilities tests the SQL utility functions
func TestSQLUtilities(t *testing.T) {
	t.Run("QuoteIdentifier", testQuoteIdentifier)
	t.Run("FormatQualifiedName", testFormatQualifiedName)
	t.Run("FormatAlias", testFormatAlias)
	t.Run("FormatStringLiteral", testFormatStringLiteral)
}

// testQuoteIdentifier tests the identifier quoting utility function
func testQuoteIdentifier(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		// Simple identifiers that don't need quoting
		{"simple lowercase", "users", "users"},
		{"with underscore", "user_id", "user_id"},
		{"with dollar", "user$table", "user$table"},
		{"with numbers", "table123", "table123"},
		
		// Identifiers that need quoting - keywords
		{"reserved keyword", "select", `"select"`},
		{"reserved keyword uppercase", "SELECT", `"SELECT"`},
		{"mixed case keyword", "Select", `"Select"`},
		
		// Identifiers that need quoting - case sensitivity
		{"uppercase letters", "Users", `"Users"`},
		{"mixed case", "UserTable", `"UserTable"`},
		
		// Identifiers that need quoting - special characters
		{"spaces", "user table", `"user table"`},
		{"special chars", "user-table", `"user-table"`},
		{"starts with number", "123users", `"123users"`},
		
		// Identifiers with internal quotes
		{"internal quote", `user"table`, `"user""table"`},
		{"multiple quotes", `user""table`, `"user""""table"`},
		
		// Edge cases
		{"empty string", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := QuoteIdentifier(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestRangeVarSqlString tests SQL deparsing for table references
func TestRangeVarSqlString(t *testing.T) {
	tests := []struct {
		name        string
		rangeVar    *RangeVar
		expected    string
		description string
	}{
		{
			name: "simple table",
			rangeVar: &RangeVar{
				RelName: "users",
			},
			expected:    "users",
			description: "Simple table name without schema",
		},
		{
			name: "table with schema",
			rangeVar: &RangeVar{
				SchemaName: "public",
				RelName:    "users",
			},
			expected:    "public.users",
			description: "Table with explicit schema",
		},
		{
			name: "fully qualified table",
			rangeVar: &RangeVar{
				CatalogName: "mydb",
				SchemaName:  "public",
				RelName:     "users",
			},
			expected:    "mydb.public.users",
			description: "Fully qualified table reference",
		},
		{
			name: "table with simple alias",
			rangeVar: &RangeVar{
				RelName: "users",
				Alias: &Alias{
					AliasName: "u",
				},
			},
			expected:    "users AS u",
			description: "Table with simple alias",
		},
		{
			name: "table with column aliases",
			rangeVar: &RangeVar{
				RelName: "users",
				Alias: &Alias{
					AliasName: "u",
					ColNames:  []Node{}, // We'll leave this empty for now since we don't have string nodes yet
				},
			},
			expected:    "users AS u",
			description: "Table with alias (column aliases empty for now)",
		},
		{
			name: "quoted identifiers",
			rangeVar: &RangeVar{
				SchemaName: "user schema",  // Has space, needs quoting
				RelName:    "Users",        // Has uppercase, needs quoting
				Alias: &Alias{
					AliasName: "UserAlias", // Has uppercase, needs quoting
				},
			},
			expected:    `"user schema"."Users" AS "UserAlias"`,
			description: "Identifiers requiring quotes",
		},
		{
			name: "reserved keyword table",
			rangeVar: &RangeVar{
				RelName: "select", // Reserved keyword
			},
			expected:    `"select"`,
			description: "Table name is reserved keyword",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.rangeVar.SqlString()
			assert.Equal(t, tt.expected, result, tt.description)
		})
	}
}

// TestAliasSqlString tests SQL deparsing for aliases
func TestAliasSqlString(t *testing.T) {
	tests := []struct {
		name        string
		alias       *Alias
		expected    string
		description string
	}{
		{
			name: "simple alias",
			alias: &Alias{
				AliasName: "u",
			},
			expected:    "AS u",
			description: "Simple table alias",
		},
		{
			name: "quoted alias",
			alias: &Alias{
				AliasName: "UserAlias",
			},
			expected:    `AS "UserAlias"`,
			description: "Alias with uppercase letters",
		},
		{
			name: "keyword alias",
			alias: &Alias{
				AliasName: "select",
			},
			expected:    `AS "select"`,
			description: "Alias using reserved keyword",
		},
		{
			name: "empty alias",
			alias: &Alias{
				AliasName: "",
			},
			expected:    "",
			description: "Empty alias returns empty string",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.alias.SqlString()
			assert.Equal(t, tt.expected, result, tt.description)
		})
	}
}

// TestDefaultSqlStringPanic tests that unimplemented nodes panic with helpful message
func TestDefaultSqlStringPanic(t *testing.T) {
	// Create a node type that doesn't have SqlString() implemented (uses default from BaseNode)
	node := &BaseNode{
		Tag: T_Query,
		Loc: 0,
	}

	// Test that calling SqlString() panics with helpful message
	assert.Panics(t, func() {
		node.SqlString()
	}, "Should panic when SqlString() is not implemented")

	// Test the panic message contains useful information
	defer func() {
		if r := recover(); r != nil {
			message := r.(string)
			assert.Contains(t, message, "SqlString() not implemented")
			assert.Contains(t, message, "T_Query")
			assert.Contains(t, message, "Please implement SqlString()")
		}
	}()
	
	node.SqlString()
}

// TestRoundTripCompatibility tests that basic nodes can be converted to SQL and back
// This is a foundational test for ensuring parsing/deparsing round-trip fidelity
func TestRoundTripCompatibility(t *testing.T) {
	tests := []struct {
		name     string
		node     Node
		expected string
	}{
		{
			name: "simple table reference",
			node: &RangeVar{RelName: "users"},
			expected: "users",
		},
		{
			name: "qualified table reference", 
			node: &RangeVar{SchemaName: "public", RelName: "users"},
			expected: "public.users",
		},
		{
			name: "table with alias",
			node: &RangeVar{
				RelName: "users",
				Alias:   &Alias{AliasName: "u"},
			},
			expected: "users AS u",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sqlString := tt.node.SqlString()
			assert.Equal(t, tt.expected, sqlString)
			
			// TODO: In the future, we can add parsing of the sqlString back to AST
			// and verify it produces an equivalent node structure
			// For now, we just verify the SQL string output is correct
		})
	}
}

// testFormatQualifiedName tests qualified name formatting
func testFormatQualifiedName(t *testing.T) {
	tests := []struct {
		name     string
		database string
		schema   string
		table    string
		expected string
	}{
		{"table only", "", "", "users", "users"},
		{"schema and table", "", "public", "users", "public.users"},
		{"full qualification", "mydb", "public", "users", "mydb.public.users"},
		{"quoted parts", "", "user schema", "Select", `"user schema"."Select"`},
		{"empty database", "", "public", "users", "public.users"},
		{"empty schema", "mydb", "", "users", "mydb.users"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatFullyQualifiedName(tt.database, tt.schema, tt.table)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// testFormatAlias tests alias formatting
func testFormatAlias(t *testing.T) {
	tests := []struct {
		name     string
		alias    string
		expected string
	}{
		{"simple alias", "u", "AS u"},
		{"quoted alias", "UserAlias", `AS "UserAlias"`},
		{"keyword alias", "select", `AS "select"`},
		{"empty alias", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatAlias(tt.alias)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// testFormatStringLiteral tests string literal formatting
func testFormatStringLiteral(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"simple string", "hello", "'hello'"},
		{"string with quote", "it's", "'it''s'"},
		{"string with multiple quotes", "she's got 'em", "'she''s got ''em'"},
		{"empty string", "", "''"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := QuoteStringLiteral(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}