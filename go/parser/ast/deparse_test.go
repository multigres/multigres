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
				Inh:     true, // Enable inheritance (no ONLY)
			},
			expected:    "users",
			description: "Simple table name without schema",
		},
		{
			name: "table with schema",
			rangeVar: &RangeVar{
				SchemaName: "public",
				RelName:    "users",
				Inh:        true, // Enable inheritance (no ONLY)
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
				Inh:         true, // Enable inheritance (no ONLY)
			},
			expected:    "mydb.public.users",
			description: "Fully qualified table reference",
		},
		{
			name: "table with simple alias",
			rangeVar: &RangeVar{
				RelName: "users",
				Inh:     true, // Enable inheritance (no ONLY)
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
				Inh:     true, // Enable inheritance (no ONLY)
				Alias: &Alias{
					AliasName: "u",
					ColNames:  NewNodeList(), // We'll leave this empty for now since we don't have string nodes yet
				},
			},
			expected:    "users AS u",
			description: "Table with alias (column aliases empty for now)",
		},
		{
			name: "quoted identifiers",
			rangeVar: &RangeVar{
				SchemaName: "user schema", // Has space, needs quoting
				RelName:    "Users",       // Has uppercase, needs quoting
				Inh:        true,          // Enable inheritance (no ONLY)
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
				Inh:     true,     // Enable inheritance (no ONLY)
			},
			expected:    `"select"`,
			description: "Table name is reserved keyword",
		},
		{
			name: "table with ONLY modifier",
			rangeVar: &RangeVar{
				RelName: "users",
				Inh:     false, // Disable inheritance (ONLY)
			},
			expected:    "ONLY users",
			description: "Table with ONLY modifier",
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
			name:     "simple table reference",
			node:     &RangeVar{RelName: "users", Inh: true},
			expected: "users",
		},
		{
			name:     "qualified table reference",
			node:     &RangeVar{SchemaName: "public", RelName: "users", Inh: true},
			expected: "public.users",
		},
		{
			name: "table with alias",
			node: &RangeVar{
				RelName: "users",
				Inh:     true,
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

// TestExpressionDeparsing tests SQL deparsing functionality for expression nodes
func TestExpressionDeparsing(t *testing.T) {
	tests := []struct {
		name     string
		node     Node
		expected string
	}{
		// Constants
		{
			name:     "integer constant",
			node:     NewA_Const(NewInteger(123), 0),
			expected: "123",
		},
		{
			name:     "negative integer",
			node:     NewA_Const(NewInteger(-456), 0),
			expected: "-456",
		},
		{
			name:     "float constant",
			node:     NewA_Const(NewFloat("3.14"), 0),
			expected: "3.14",
		},
		{
			name:     "string constant",
			node:     NewA_Const(NewString("hello"), 0),
			expected: "'hello'",
		},
		{
			name:     "boolean true",
			node:     NewA_Const(NewBoolean(true), 0),
			expected: "TRUE",
		},
		{
			name:     "boolean false",
			node:     NewA_Const(NewBoolean(false), 0),
			expected: "FALSE",
		},
		{
			name:     "null constant",
			node:     NewA_ConstNull(0),
			expected: "NULL",
		},

		// Column references
		{
			name:     "simple column",
			node:     NewColumnRef(NewString("col1")),
			expected: "col1",
		},
		{
			name:     "qualified column",
			node:     NewColumnRef(NewString("table"), NewString("col1")),
			expected: "table.col1",
		},
		{
			name:     "schema qualified column",
			node:     NewColumnRef(NewString("schema"), NewString("table"), NewString("col1")),
			expected: "schema.table.col1",
		},

		// Binary arithmetic expressions
		{
			name: "addition",
			node: NewA_Expr(AEXPR_OP, &NodeList{Items: []Node{NewString("+")}},
				NewA_Const(NewInteger(1), 0),
				NewA_Const(NewInteger(2), 0), 0),
			expected: "1 + 2",
		},
		{
			name: "subtraction",
			node: NewA_Expr(AEXPR_OP, &NodeList{Items: []Node{NewString("-")}},
				NewA_Const(NewInteger(5), 0),
				NewA_Const(NewInteger(3), 0), 0),
			expected: "5 - 3",
		},
		{
			name: "multiplication",
			node: NewA_Expr(AEXPR_OP, &NodeList{Items: []Node{NewString("*")}},
				NewA_Const(NewInteger(4), 0),
				NewA_Const(NewInteger(6), 0), 0),
			expected: "4 * 6",
		},
		{
			name: "division",
			node: NewA_Expr(AEXPR_OP, &NodeList{Items: []Node{NewString("/")}},
				NewA_Const(NewInteger(10), 0),
				NewA_Const(NewInteger(2), 0), 0),
			expected: "10 / 2",
		},
		{
			name: "modulo",
			node: NewA_Expr(AEXPR_OP, &NodeList{Items: []Node{NewString("%")}},
				NewA_Const(NewInteger(7), 0),
				NewA_Const(NewInteger(3), 0), 0),
			expected: "7 % 3",
		},
		{
			name: "exponentiation",
			node: NewA_Expr(AEXPR_OP, &NodeList{Items: []Node{NewString("^")}},
				NewA_Const(NewInteger(2), 0),
				NewA_Const(NewInteger(3), 0), 0),
			expected: "2 ^ 3",
		},

		// Unary arithmetic expressions
		{
			name: "unary plus",
			node: NewA_Expr(AEXPR_OP, &NodeList{Items: []Node{NewString("+")}},
				nil,
				NewA_Const(NewInteger(5), 0), 0),
			expected: "+5",
		},
		{
			name: "unary minus",
			node: NewA_Expr(AEXPR_OP, &NodeList{Items: []Node{NewString("-")}},
				nil,
				NewA_Const(NewInteger(10), 0), 0),
			expected: "-10",
		},

		// Comparison expressions
		{
			name: "less than",
			node: NewA_Expr(AEXPR_OP, &NodeList{Items: []Node{NewString("<")}},
				NewColumnRef(NewString("a")),
				NewColumnRef(NewString("b")), 0),
			expected: "a < b",
		},
		{
			name: "greater than",
			node: NewA_Expr(AEXPR_OP, &NodeList{Items: []Node{NewString(">")}},
				NewColumnRef(NewString("x")),
				NewColumnRef(NewString("y")), 0),
			expected: "x > y",
		},
		{
			name: "equal",
			node: NewA_Expr(AEXPR_OP, &NodeList{Items: []Node{NewString("=")}},
				NewColumnRef(NewString("p")),
				NewColumnRef(NewString("q")), 0),
			expected: "p = q",
		},
		{
			name: "less equal",
			node: NewA_Expr(AEXPR_OP, &NodeList{Items: []Node{NewString("<=")}},
				NewColumnRef(NewString("m")),
				NewColumnRef(NewString("n")), 0),
			expected: "m <= n",
		},
		{
			name: "greater equal",
			node: NewA_Expr(AEXPR_OP, &NodeList{Items: []Node{NewString(">=")}},
				NewColumnRef(NewString("r")),
				NewColumnRef(NewString("s")), 0),
			expected: "r >= s",
		},
		{
			name: "not equal",
			node: NewA_Expr(AEXPR_OP, &NodeList{Items: []Node{NewString("<>")}},
				NewColumnRef(NewString("u")),
				NewColumnRef(NewString("v")), 0),
			expected: "u <> v",
		},

		// Logical expressions
		{
			name: "and expression",
			node: NewA_Expr(AEXPR_OP, &NodeList{Items: []Node{NewString("AND")}},
				NewColumnRef(NewString("a")),
				NewColumnRef(NewString("b")), 0),
			expected: "a AND b",
		},
		{
			name: "or expression",
			node: NewA_Expr(AEXPR_OP, &NodeList{Items: []Node{NewString("OR")}},
				NewColumnRef(NewString("x")),
				NewColumnRef(NewString("y")), 0),
			expected: "x OR y",
		},
		{
			name: "not expression",
			node: NewA_Expr(AEXPR_OP, &NodeList{Items: []Node{NewString("NOT")}},
				nil,
				NewColumnRef(NewString("p")), 0),
			expected: "NOT p",
		},

		// Function calls
		{
			name:     "function no args",
			node:     NewFuncCall(&NodeList{Items: []Node{NewString("func")}}, nil, 0),
			expected: "func()",
		},
		{
			name:     "function one arg",
			node:     NewFuncCall(&NodeList{Items: []Node{NewString("func")}}, NewNodeList(NewColumnRef(NewString("x"))), 0),
			expected: "func(x)",
		},
		{
			name: "function multi args",
			node: NewFuncCall(&NodeList{Items: []Node{NewString("func")}}, NewNodeList(
				NewColumnRef(NewString("a")),
				NewColumnRef(NewString("b")),
				NewColumnRef(NewString("c")),
			), 0),
			expected: "func(a, b, c)",
		},
		{
			name:     "qualified function",
			node:     NewFuncCall(&NodeList{Items: []Node{NewString("schema"), NewString("func")}}, NewNodeList(NewColumnRef(NewString("x"))), 0),
			expected: "schema.func(x)",
		},

		// Type casting
		{
			name:     "simple type cast",
			node:     NewTypeCast(NewA_Const(NewInteger(123), 0), NewTypeName([]string{"text"}), 0),
			expected: "123::TEXT",
		},
		{
			name:     "column type cast",
			node:     NewTypeCast(NewColumnRef(NewString("col")), NewTypeName([]string{"varchar"}), 0),
			expected: "col::VARCHAR",
		},
		{
			name:     "qualified type cast",
			node:     NewTypeCast(NewA_Const(NewInteger(123), 0), NewTypeName([]string{"pg_catalog", "text"}), 0),
			expected: "123::TEXT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// All nodes implement SqlString()
			result := tt.node.SqlString()
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestComplexExpressionDeparsing tests deparsing of complex nested expressions
func TestComplexExpressionDeparsing(t *testing.T) {
	tests := []struct {
		name     string
		node     Node
		expected string
	}{
		// Complex nested expressions
		{
			name: "nested arithmetic",
			node: NewA_Expr(AEXPR_OP, &NodeList{Items: []Node{NewString("+")}},
				NewColumnRef(NewString("a")),
				NewA_Expr(AEXPR_OP, &NodeList{Items: []Node{NewString("*")}},
					NewColumnRef(NewString("b")),
					NewColumnRef(NewString("c")), 0), 0),
			expected: "a + b * c",
		},
		{
			name: "comparison with arithmetic",
			node: NewA_Expr(AEXPR_OP, &NodeList{Items: []Node{NewString(">")}},
				NewA_Expr(AEXPR_OP, &NodeList{Items: []Node{NewString("+")}},
					NewColumnRef(NewString("x")),
					NewColumnRef(NewString("y")), 0),
				NewA_Const(NewInteger(10), 0), 0),
			expected: "x + y > 10",
		},
		{
			name: "logical with comparison",
			node: NewA_Expr(AEXPR_OP, &NodeList{Items: []Node{NewString("AND")}},
				NewA_Expr(AEXPR_OP, &NodeList{Items: []Node{NewString(">")}},
					NewColumnRef(NewString("a")),
					NewColumnRef(NewString("b")), 0),
				NewA_Expr(AEXPR_OP, &NodeList{Items: []Node{NewString("<")}},
					NewColumnRef(NewString("c")),
					NewColumnRef(NewString("d")), 0), 0),
			expected: "a > b AND c < d",
		},
		{
			name: "function in expression",
			node: NewA_Expr(AEXPR_OP, &NodeList{Items: []Node{NewString("+")}},
				NewFuncCall(&NodeList{Items: []Node{NewString("func")}}, NewNodeList(NewColumnRef(NewString("x"))), 0),
				NewA_Const(NewInteger(10), 0), 0),
			expected: "func(x) + 10",
		},
		{
			name: "cast in arithmetic",
			node: NewA_Expr(AEXPR_OP, &NodeList{Items: []Node{NewString("+")}},
				NewTypeCast(NewColumnRef(NewString("a")), NewTypeName([]string{"float"}), 0),
				NewTypeCast(NewColumnRef(NewString("b")), NewTypeName([]string{"float"}), 0), 0),
			expected: "a::FLOAT + b::FLOAT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// All nodes implement SqlString()
			result := tt.node.SqlString()
			assert.Equal(t, tt.expected, result)
		})
	}
}
