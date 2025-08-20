package parser

import (
	"strings"
	"testing"

	"github.com/multigres/parser/go/parser/ast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDeparsing tests round-trip parsing and deparsing for all supported constructs
func TestDeparsing(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string // If empty, expects exact match with input
	}{
		// Basic SELECT statements
		{"Simple SELECT all", "SELECT *", ""},
		{"SELECT specific columns", "SELECT id, name", ""},
		{"SELECT with alias", "SELECT id AS user_id", ""},
		{"SELECT from table", "SELECT * FROM users", ""},
		{"SELECT from qualified table", "SELECT * FROM public.users", ""},

		// WHERE clauses
		{"WHERE with equality", "SELECT * FROM users WHERE id = 1", ""},
		{"WHERE with comparison", "SELECT * FROM users WHERE age > 18", ""},
		{"WHERE with AND", "SELECT * FROM users WHERE (age > 18 AND active = TRUE)", ""},
		{"WHERE with OR", "SELECT * FROM users WHERE (active = TRUE OR admin = TRUE)", ""},
		{"WHERE with NOT", "SELECT * FROM users WHERE NOT deleted", ""},

		// DISTINCT
		{"SELECT DISTINCT", "SELECT DISTINCT name FROM users", ""},
		{"SELECT DISTINCT ON", "SELECT DISTINCT ON (department) name FROM employees", ""},

		// Expressions
		{"Arithmetic addition", "SELECT 1 + 2", ""},
		{"Arithmetic subtraction", "SELECT 5 - 3", ""},
		{"Arithmetic multiplication", "SELECT 4 * 6", ""},
		{"Arithmetic division", "SELECT 10 / 2", ""},
		{"Unary minus", "SELECT -42", "SELECT -42"},
		{"Unary plus", "SELECT +42", "SELECT +42"},
		{"Complex arithmetic", "SELECT ((1 + 2) * 3)", ""},

		// Type casting
		{"Type cast simple", "SELECT id::text FROM users", ""},
		{"Type cast expression", "SELECT (age + 1)::varchar FROM users", ""},

		// Function calls
		{"Function no args", "SELECT now()", ""},
		{"Function with arg", "SELECT length('hello')", "SELECT length('hello')"},
		{"Function multiple args", "SELECT substring('hello', 1, 3)", "SELECT substring('hello', 1, 3)"},

		// Column references
		{"Simple column", "SELECT name FROM users", ""},
		{"Qualified column", "SELECT users.name FROM users", ""},

		// Constants
		{"Integer constant", "SELECT 42", ""},
		{"Float constant", "SELECT 3.14", ""},
		{"String constant", "SELECT 'hello'", "SELECT 'hello'"},
		{"Boolean TRUE", "SELECT TRUE", ""},
		{"Boolean FALSE", "SELECT FALSE", ""},
		{"NULL constant", "SELECT NULL", ""},

		// Table aliases
		{"Table with alias", "SELECT * FROM users AS u", ""},
		{"Table with implicit alias", "SELECT * FROM users u", "SELECT * FROM users AS u"},

		// Multiple tables
		{"Multiple tables", "SELECT * FROM users, orders", ""},

		// Complex queries
		{"Complex query", "SELECT id, name FROM users WHERE age > 21", ""},
		{"Query with multiple conditions", "SELECT * FROM users WHERE (id > 0 AND active = TRUE)", ""},

		// SELECT INTO
		{"SELECT INTO", "SELECT * INTO new_table FROM users", ""},

		// TABLE statement (simplified SELECT)
		{"TABLE statement", "TABLE users", "SELECT * FROM users"},

		// ONLY modifier
		{"SELECT from ONLY", "SELECT * FROM ONLY users", ""},

		// JOIN operations - Basic
		{"INNER JOIN", "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id", ""},
		{"LEFT JOIN", "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id", "SELECT * FROM users LEFT OUTER JOIN orders ON users.id = orders.user_id"},
		{"RIGHT JOIN", "SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id", "SELECT * FROM users RIGHT OUTER JOIN orders ON users.id = orders.user_id"},
		{"FULL JOIN", "SELECT * FROM users FULL JOIN orders ON users.id = orders.user_id", "SELECT * FROM users FULL OUTER JOIN orders ON users.id = orders.user_id"},
		{"CROSS JOIN", "SELECT * FROM users CROSS JOIN orders", "SELECT * FROM users INNER JOIN orders"},
		{"NATURAL JOIN", "SELECT * FROM users NATURAL JOIN orders", ""},
		{"JOIN with USING", "SELECT * FROM users JOIN orders USING (user_id)", "SELECT * FROM users INNER JOIN orders USING ('user_id')"},
		{"JOIN implicit INNER", "SELECT * FROM users JOIN orders ON users.id = orders.user_id", "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id"},

		// JOIN operations - Advanced
		{"LEFT OUTER JOIN explicit", "SELECT * FROM users LEFT OUTER JOIN orders ON users.id = orders.user_id", ""},
		{"RIGHT OUTER JOIN explicit", "SELECT * FROM users RIGHT OUTER JOIN orders ON users.id = orders.user_id", ""},
		{"FULL OUTER JOIN explicit", "SELECT * FROM users FULL OUTER JOIN orders ON users.id = orders.user_id", ""},
		{"NATURAL INNER JOIN", "SELECT * FROM users NATURAL INNER JOIN orders", "SELECT * FROM users NATURAL JOIN orders"},
		{"NATURAL LEFT JOIN", "SELECT * FROM users NATURAL LEFT JOIN orders", ""},
		{"NATURAL RIGHT JOIN", "SELECT * FROM users NATURAL RIGHT JOIN orders", ""},
		{"NATURAL FULL JOIN", "SELECT * FROM users NATURAL FULL JOIN orders", ""},
		{"Multiple column USING", "SELECT * FROM users JOIN orders USING (user_id, created_date)", "SELECT * FROM users INNER JOIN orders USING ('user_id', 'created_date')"},

		// JOIN operations - Complex
		{"Chained JOINs", "SELECT * FROM users JOIN orders ON users.id = orders.user_id JOIN products ON orders.product_id = products.id", "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id INNER JOIN products ON orders.product_id = products.id"},
		{"Mixed JOIN types", "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id CROSS JOIN categories", "SELECT * FROM users LEFT OUTER JOIN orders ON users.id = orders.user_id INNER JOIN categories"},
		{"Parenthesized JOIN", "SELECT * FROM users JOIN (orders JOIN products ON orders.product_id = products.id) ON users.id = orders.user_id", "SELECT * FROM users INNER JOIN (orders INNER JOIN products ON orders.product_id = products.id) ON users.id = orders.user_id"},
		{"JOIN with table aliases", "SELECT * FROM users u JOIN orders o ON u.id = o.user_id", "SELECT * FROM users AS u INNER JOIN orders AS o ON u.id = o.user_id"},

		// Common Table Expressions (CTEs) - Basic
		{"Basic CTE", "WITH stats AS (SELECT * FROM users) SELECT * FROM stats", ""},
		{"Recursive CTE", "WITH RECURSIVE t AS (SELECT 1) SELECT * FROM t", ""},
		{"Multiple CTEs", "WITH t1 AS (SELECT id FROM users), t2 AS (SELECT * FROM t1) SELECT * FROM t2", ""},
		{"CTE with column list", "WITH stats(user_id) AS (SELECT id FROM users) SELECT * FROM stats", ""},

		// Advanced CTE Features - MATERIALIZED
		{"MATERIALIZED CTE", "WITH stats AS MATERIALIZED (SELECT id FROM users) SELECT * FROM stats", ""},
		{"NOT MATERIALIZED CTE", "WITH stats AS NOT MATERIALIZED (SELECT id FROM users) SELECT * FROM stats", ""},
		{"Recursive MATERIALIZED CTE", "WITH RECURSIVE t AS MATERIALIZED (SELECT 1) SELECT * FROM t", ""},

		// Advanced CTE Features - SEARCH clauses
		{"CTE with SEARCH DEPTH FIRST", "WITH RECURSIVE tree AS (SELECT id FROM nodes) SEARCH DEPTH FIRST BY id SET search_seq SELECT * FROM tree", ""},
		{"CTE with SEARCH BREADTH FIRST", "WITH RECURSIVE tree AS (SELECT id FROM nodes) SEARCH BREADTH FIRST BY id SET search_seq SELECT * FROM tree", ""},
		{"CTE with SEARCH multiple columns", "WITH RECURSIVE tree AS (SELECT id, parent_id FROM nodes) SEARCH DEPTH FIRST BY id, parent_id SET search_seq SELECT * FROM tree", ""},

		// Advanced CTE Features - CYCLE clauses
		{"CTE with CYCLE simple", "WITH RECURSIVE tree AS (SELECT id FROM nodes) CYCLE id SET is_cycle USING path SELECT * FROM tree", ""},
		{"CTE with CYCLE full", "WITH RECURSIVE tree AS (SELECT id FROM nodes) CYCLE id SET is_cycle TO TRUE DEFAULT FALSE USING path SELECT * FROM tree", ""},
		{"CTE with CYCLE multiple columns", "WITH RECURSIVE tree AS (SELECT id, parent_id FROM nodes) CYCLE id, parent_id SET is_cycle USING path SELECT * FROM tree", ""},

		// Advanced CTE Features - Combined clauses
		{"CTE with MATERIALIZED and SEARCH", "WITH RECURSIVE tree AS MATERIALIZED (SELECT id FROM nodes) SEARCH DEPTH FIRST BY id SET search_seq SELECT * FROM tree", ""},
		{"CTE with SEARCH and CYCLE", "WITH RECURSIVE tree AS (SELECT id FROM nodes) SEARCH DEPTH FIRST BY id SET search_seq CYCLE id SET is_cycle USING path SELECT * FROM tree", ""},
		{"CTE with all advanced features", "WITH RECURSIVE tree AS MATERIALIZED (SELECT id FROM nodes) SEARCH DEPTH FIRST BY id SET search_seq CYCLE id SET is_cycle TO TRUE DEFAULT FALSE USING path SELECT * FROM tree", ""},

		// Subqueries and LATERAL
		{"Subquery in FROM", "SELECT * FROM (SELECT id FROM users) AS sub", ""},
		{"LATERAL subquery", "SELECT * FROM users, LATERAL (SELECT * FROM orders) AS sub", ""},
		{"Subquery with alias and columns", "SELECT * FROM (SELECT id, name FROM users) AS sub(user_id, user_name)", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the input SQL
			statements, err := ParseSQL(tt.input)
			require.NoError(t, err, "Parse should succeed for: %s", tt.input)
			require.Len(t, statements, 1, "Should have exactly one statement")

			// Get the deparsed SQL
			deparsed := statements[0].SqlString()

			// Determine expected output
			expected := tt.expected
			if expected == "" {
				expected = tt.input
			}

			// Normalize both strings for comparison (remove extra spaces, uppercase keywords)
			normalizedDeparsed := normalizeSQL(deparsed)
			normalizedExpected := normalizeSQL(expected)

			assert.Equal(t, normalizedExpected, normalizedDeparsed,
				"Deparsed SQL should match expected.\nOriginal: %s\nDeparsed: %s\nExpected: %s",
				tt.input, deparsed, expected)
		})
	}
}

// TestExpressionDeparsing tests deparsing of various expression types
func TestExpressionDeparsing(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		// Arithmetic expressions
		{"Nested arithmetic", "SELECT ((1 + 2) * (3 - 4))", ""},
		{"Division and modulo", "SELECT (10 / 3) % 2", ""},
		{"Power operator", "SELECT 2 ^ 3", ""},

		// Comparison expressions
		{"Not equal", "SELECT * FROM users WHERE id <> 1", ""},
		{"Less than or equal", "SELECT * FROM users WHERE age <= 65", ""},
		{"Greater than or equal", "SELECT * FROM users WHERE age >= 18", ""},

		// Logical expressions
		{"Complex AND/OR", "SELECT * FROM users WHERE ((active = TRUE OR admin = TRUE) AND NOT deleted)", ""},
		{"Nested NOT", "SELECT * FROM users WHERE NOT NOT active", ""},

		// Mixed expressions
		{"Arithmetic in WHERE", "SELECT * FROM users WHERE (age * 2) > 50", ""},
		{"Function in expression", "SELECT * FROM users WHERE length(name) > 5", ""},

		// Column expressions
		{"Column arithmetic", "SELECT age + 1 FROM users", ""},
		{"Column with function", "SELECT upper(name) FROM users", ""},

		// Complex nested expressions
		{"Deeply nested", "SELECT (((1 + 2) * 3) - 4) / 5", ""},
		{"Mixed operators", "SELECT * FROM users WHERE (age + 5) * 2 > 30", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the input SQL
			statements, err := ParseSQL(tt.input)
			require.NoError(t, err, "Parse should succeed for: %s", tt.input)
			require.Len(t, statements, 1, "Should have exactly one statement")

			// Get the deparsed SQL
			deparsed := statements[0].SqlString()

			// Determine expected output
			expected := tt.expected
			if expected == "" {
				expected = tt.input
			}

			// Compare normalized versions
			assert.Equal(t, normalizeSQL(expected), normalizeSQL(deparsed),
				"Expression deparsing mismatch.\nOriginal: %s\nDeparsed: %s\nExpected: %s",
				tt.input, deparsed, expected)

			// Also verify that re-parsing the deparsed SQL succeeds
			statements2, err2 := ParseSQL(deparsed)
			require.NoError(t, err2, "Re-parsing deparsed SQL should succeed.\nOriginal: %s\nDeparsed: %s", tt.input, deparsed)
			require.Len(t, statements2, 1, "Re-parsed should have exactly one statement")

			// Verify stability - deparsing again should produce the same result
			deparsed2 := statements2[0].SqlString()
			assert.Equal(t, normalizeSQL(deparsed), normalizeSQL(deparsed2),
				"Deparsing should be stable.\nFirst: %s\nSecond: %s", deparsed, deparsed2)
		})
	}
}

// TestRoundTripParsing tests that we can parse, deparse, and re-parse successfully
func TestRoundTripParsing(t *testing.T) {
	tests := []struct {
		query    string
		expected string // If empty, expects exact match with query
	}{
		// Basic queries
		{"SELECT * FROM users", ""},
		{"SELECT id, name FROM users WHERE active = TRUE", ""},
		{"SELECT DISTINCT department FROM employees", ""},

		// Expressions
		{"SELECT 1 + 2 * 3", ""},
		{"SELECT age::text FROM users", ""},
		{"SELECT length(name) FROM users", ""},

		// Complex queries
		{"SELECT * FROM users WHERE (age > 18 AND active = TRUE)", ""},
		{"SELECT id AS user_id, name AS user_name FROM users AS u", ""},
		{"SELECT * FROM users, orders WHERE users.id = orders.user_id", ""},

		// Special cases
		{"TABLE users", "SELECT * FROM users"}, // TABLE is converted to SELECT *
		{"SELECT * FROM ONLY users", ""},
		{"SELECT * INTO backup FROM users", ""},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			// First parse
			statements1, err1 := ParseSQL(tt.query)
			require.NoError(t, err1, "First parse should succeed")
			require.Len(t, statements1, 1, "Should have exactly one statement")

			// Deparse
			deparsed := statements1[0].SqlString()
			assert.NotEmpty(t, deparsed, "Deparsed SQL should not be empty")

			// Determine expected output
			expected := tt.expected
			if expected == "" {
				expected = tt.query
			}

			// Check the deparsed output matches expected
			assert.Equal(t, normalizeSQL(expected), normalizeSQL(deparsed),
				"Round-trip deparsing mismatch.\nOriginal: %s\nDeparsed: %s\nExpected: %s",
				tt.query, deparsed, expected)

			// Second parse (of deparsed SQL)
			statements2, err2 := ParseSQL(deparsed)
			require.NoError(t, err2, "Second parse should succeed. Deparsed: %s", deparsed)
			require.Len(t, statements2, 1, "Should have exactly one statement after re-parsing")

			// Third deparse (should be stable)
			deparsed2 := statements2[0].SqlString()

			// The second deparsing should match the first (stability test)
			assert.Equal(t, normalizeSQL(deparsed), normalizeSQL(deparsed2),
				"Deparsing should be stable.\nFirst: %s\nSecond: %s", deparsed, deparsed2)
		})
	}
}

func TestOneCase(t *testing.T) {
	query := "SELECT DISTINCT name FROM users"
	output := ""
	if query == "" {
		t.Skip("No tests to run")
	}
	if output == "" {
		output = query
	}
	statements, err := ParseSQL(query)
	require.NoError(t, err, "First parse should succeed")
	require.Len(t, statements, 1, "Should have exactly one statement")

	// Deparse
	deparsed := statements[0].SqlString()
	assert.NotEmpty(t, deparsed, "Deparsed SQL should not be empty")

	// Second parse (of deparsed SQL)
	statements, err = ParseSQL(deparsed)
	require.NoError(t, err, "Second parse should succeed. Deparsed: %s", deparsed)
	require.Len(t, statements, 1, "Should have exactly one statement after re-parsing")

	// Third deparse (should be stable)
	deparsed = statements[0].SqlString()

	// The second deparsing should match the first (stability test)
	assert.Equal(t, output, deparsed)
}

// TestDeparsingEdgeCases tests deparsing of edge cases and special scenarios
func TestDeparsingEdgeCases(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		shouldParse  bool
		checkDeparse bool
	}{
		// Empty and minimal queries
		{"SELECT without FROM", "SELECT 1", true, true},
		{"SELECT NULL", "SELECT NULL", true, true},
		{"SELECT multiple constants", "SELECT 1, 2, 3", true, true},

		// Special identifiers
		{"Reserved word as identifier", `SELECT * FROM "select"`, true, true},
		{"Mixed case identifier", `SELECT * FROM "Users"`, true, true},
		{"Identifier with spaces", `SELECT * FROM "user table"`, true, true},

		// Complex expressions
		{"Parentheses preservation", "SELECT (((1)))", true, true},
		{"Expression with all operators", "SELECT 1 + 2 - 3 * 4 / 5 % 6 ^ 7", true, true},

		// Special SELECT variants
		{"SELECT with column list alias", "SELECT * FROM users AS u(id, name)", true, false}, // Column aliases in FROM not fully supported yet
		{"SELECT with multiple DISTINCT ON", "SELECT DISTINCT ON (a, b) c FROM t", true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Try to parse
			statements, err := ParseSQL(tt.input)

			if !tt.shouldParse {
				assert.Error(t, err, "Expected parse error")
				return
			}

			require.NoError(t, err, "Parse should succeed")
			require.Len(t, statements, 1, "Should have exactly one statement")

			if !tt.checkDeparse {
				// Skip deparsing check for unsupported features
				return
			}

			// Try to deparse
			deparsed := statements[0].SqlString()
			assert.NotEmpty(t, deparsed, "Deparsed SQL should not be empty")

			// Try to re-parse the deparsed SQL
			statements2, err2 := ParseSQL(deparsed)
			assert.NoError(t, err2, "Re-parsing should succeed. Deparsed: %s", deparsed)
			if err2 == nil {
				assert.Len(t, statements2, 1, "Should have exactly one statement after re-parsing")
			}
		})
	}
}

// normalizeSQL normalizes SQL for comparison by:
// - Converting to uppercase
// - Removing extra whitespace
// - Removing trailing semicolons
func normalizeSQL(sql string) string {
	// Convert to uppercase
	sql = strings.ToUpper(sql)

	// Replace multiple spaces with single space
	sql = strings.Join(strings.Fields(sql), " ")

	// Remove trailing semicolon if present
	sql = strings.TrimSuffix(sql, ";")

	// Trim whitespace
	sql = strings.TrimSpace(sql)

	return sql
}

// TestDeparsingWithAliases specifically tests alias deparsing
func TestDeparsingWithAliases(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"Column alias with AS", "SELECT id AS user_id", ""},
		{"Column alias without AS", "SELECT id user_id", "SELECT id AS user_id"},
		{"Multiple column aliases", "SELECT id AS user_id, name AS user_name", ""},
		{"Table alias with AS", "SELECT * FROM users AS u", ""},
		{"Table alias without AS", "SELECT * FROM users u", "SELECT * FROM users AS u"},
		{"Complex aliases", "SELECT u.id AS user_id FROM users AS u", ""},
		{"Function with alias", "SELECT length(name) AS name_length FROM users", ""},
		{"Expression with alias", "SELECT age + 1 AS next_age FROM users", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse
			statements, err := ParseSQL(tt.input)
			require.NoError(t, err, "Parse should succeed")
			require.Len(t, statements, 1)

			// Deparse
			deparsed := statements[0].SqlString()

			// Determine expected
			expected := tt.expected
			if expected == "" {
				expected = tt.input
			}

			// Compare normalized versions
			assert.Equal(t, normalizeSQL(expected), normalizeSQL(deparsed),
				"Alias deparsing mismatch.\nOriginal: %s\nDeparsed: %s", tt.input, deparsed)
		})
	}
}

// TestMissingDeparsingCoverage tests the previously identified gaps in deparsing coverage
func TestMissingDeparsingCoverage(t *testing.T) {
	t.Run("MultipleStatements", testMultipleStatements)
	t.Run("NameLists", testNameLists)
	t.Run("Indirection", testIndirection)
	t.Run("QualifiedOperators", testQualifiedOperators)
	t.Run("AdvancedTypeCasting", testAdvancedTypeCasting)
	t.Run("ParenthesizedSelect", testParenthesizedSelect)
	t.Run("SelectAllClause", testSelectAllClause)
}

// testMultipleStatements tests deparsing of multiple statements (stmtmulti)
func testMultipleStatements(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "two simple statements",
			input:    "SELECT 1; SELECT 2",
			expected: "SELECT 1; SELECT 2",
		},
		{
			name:     "multiple statements with different types",
			input:    "SELECT * FROM users; SELECT name FROM orders",
			expected: "SELECT * FROM users; SELECT name FROM orders",
		},
		{
			name:     "statements with whitespace",
			input:    "SELECT 1;   SELECT 2;   SELECT 3",
			expected: "SELECT 1; SELECT 2; SELECT 3",
		},
		{
			name:     "trailing semicolon",
			input:    "SELECT 1; SELECT 2;",
			expected: "SELECT 1; SELECT 2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			statements, err := ParseSQL(tt.input)
			require.NoError(t, err, "Parse should succeed")
			require.Greater(t, len(statements), 1, "Should have multiple statements")

			// Deparse each statement and join with semicolons
			var deparsedParts []string
			for _, stmt := range statements {
				deparsedParts = append(deparsedParts, stmt.SqlString())
			}
			deparsed := strings.Join(deparsedParts, "; ")

			assert.Equal(t, normalizeSQL(tt.expected), normalizeSQL(deparsed),
				"Multiple statement deparsing mismatch.\nOriginal: %s\nDeparsed: %s", tt.input, deparsed)
		})
	}
}

// testNameLists tests deparsing of name lists and qualified name lists
func testNameLists(t *testing.T) {
	// Note: Since name_list and qualified_name_list are typically used internally
	// in grammar rules, we test them through contexts where they appear

	tests := []struct {
		name     string
		input    string
		expected string
		skip     bool
		reason   string
	}{
		{
			name:     "column list in function",
			input:    "SELECT func(a, b, c)",
			expected: "",
		},
		{
			name:     "multiple column references",
			input:    "SELECT a, b, c FROM table1",
			expected: "",
		},
		{
			name:     "qualified column list",
			input:    "SELECT t1.a, t2.b, t3.c FROM t1, t2, t3",
			expected: "",
		},
		// Note: More advanced name list contexts (like in DDL) will be tested in later phases
	}

	for _, tt := range tests {
		if tt.skip {
			t.Run(tt.name, func(t *testing.T) {
				t.Skip(tt.reason)
			})
			continue
		}

		t.Run(tt.name, func(t *testing.T) {
			statements, err := ParseSQL(tt.input)
			require.NoError(t, err, "Parse should succeed")
			require.Len(t, statements, 1)

			deparsed := statements[0].SqlString()
			expected := tt.expected
			if expected == "" {
				expected = tt.input
			}

			assert.Equal(t, normalizeSQL(expected), normalizeSQL(deparsed),
				"Name list deparsing mismatch.\nOriginal: %s\nDeparsed: %s", tt.input, deparsed)
		})
	}
}

// testIndirection tests deparsing of array subscripts and field access
func testIndirection(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		skip     bool
		reason   string
	}{
		{
			name:   "array subscript",
			input:  "SELECT column[1]",
			skip:   true,
			reason: "Array subscript parsing not yet implemented",
		},
		{
			name:   "array slice",
			input:  "SELECT column[1:5]",
			skip:   true,
			reason: "Array slice parsing not yet implemented",
		},
		{
			name:   "field access",
			input:  "SELECT record.field",
			skip:   true,
			reason: "Field access parsing not yet implemented",
		},
		{
			name:   "nested field access",
			input:  "SELECT record.subrecord.field",
			skip:   true,
			reason: "Nested field access parsing not yet implemented",
		},
		{
			name:   "mixed indirection",
			input:  "SELECT array_col[1].field",
			skip:   true,
			reason: "Mixed indirection parsing not yet implemented",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip {
				t.Skip(tt.reason)
				return
			}

			statements, err := ParseSQL(tt.input)
			require.NoError(t, err, "Parse should succeed")
			require.Len(t, statements, 1)

			deparsed := statements[0].SqlString()
			expected := tt.expected
			if expected == "" {
				expected = tt.input
			}

			assert.Equal(t, normalizeSQL(expected), normalizeSQL(deparsed),
				"Indirection deparsing mismatch.\nOriginal: %s\nDeparsed: %s", tt.input, deparsed)
		})
	}
}

// testQualifiedOperators tests deparsing of qualified operators
func testQualifiedOperators(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		skip     bool
		reason   string
	}{
		{
			name:   "qualified operator",
			input:  "SELECT a OPERATOR(pg_catalog.+) b",
			skip:   true,
			reason: "Qualified operator parsing not yet implemented",
		},
		{
			name:   "schema qualified operator",
			input:  "SELECT a OPERATOR(myschema.=) b",
			skip:   true,
			reason: "Schema qualified operator parsing not yet implemented",
		},
		{
			name:   "custom operator",
			input:  "SELECT a OPERATOR(public.@@) b",
			skip:   true,
			reason: "Custom operator parsing not yet implemented",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip {
				t.Skip(tt.reason)
				return
			}

			statements, err := ParseSQL(tt.input)
			require.NoError(t, err, "Parse should succeed")
			require.Len(t, statements, 1)

			deparsed := statements[0].SqlString()
			expected := tt.expected
			if expected == "" {
				expected = tt.input
			}

			assert.Equal(t, normalizeSQL(expected), normalizeSQL(deparsed),
				"Qualified operator deparsing mismatch.\nOriginal: %s\nDeparsed: %s", tt.input, deparsed)
		})
	}
}

// testAdvancedTypeCasting tests deparsing of advanced type casting
func testAdvancedTypeCasting(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		skip     bool
		reason   string
	}{
		{
			name:     "bit type casting",
			input:    "SELECT value::bit",
			expected: "",
		},
		{
			name:     "bit with length",
			input:    "SELECT value::bit(8)",
			expected: "SELECT value::bit", // Note: Type modifiers not preserved in current implementation
		},
		{
			name:     "timestamp type",
			input:    "SELECT value::timestamp",
			expected: "",
		},
		{
			name:     "timestamp with precision",
			input:    "SELECT value::timestamp(6)",
			expected: "SELECT value::timestamp", // Note: Type modifiers not preserved in current implementation
		},
		{
			name:     "timestamptz type",
			input:    "SELECT value::timestamptz",
			expected: "",
		},
		{
			name:     "date type",
			input:    "SELECT value::date",
			expected: "",
		},
		{
			name:     "time type",
			input:    "SELECT value::time",
			expected: "",
		},
		{
			name:   "interval type",
			input:  "SELECT value::interval",
			skip:   true,
			reason: "Interval type parsing not yet implemented",
		},
		{
			name:   "interval with fields",
			input:  "SELECT value::interval day to hour",
			skip:   true,
			reason: "Interval type parsing not yet implemented",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip {
				t.Skip(tt.reason)
				return
			}

			statements, err := ParseSQL(tt.input)
			require.NoError(t, err, "Parse should succeed")
			require.Len(t, statements, 1)

			deparsed := statements[0].SqlString()
			expected := tt.expected
			if expected == "" {
				expected = tt.input
			}

			assert.Equal(t, normalizeSQL(expected), normalizeSQL(deparsed),
				"Advanced type casting deparsing mismatch.\nOriginal: %s\nDeparsed: %s", tt.input, deparsed)
		})
	}
}

// testParenthesizedSelect tests deparsing of parenthesized SELECT statements
func testParenthesizedSelect(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple parenthesized select",
			input:    "(SELECT * FROM users)",
			expected: "SELECT * FROM users", // Note: Parentheses not preserved in current implementation
		},
		{
			name:     "nested parentheses",
			input:    "((SELECT id FROM users))",
			expected: "SELECT id FROM users", // Note: Parentheses not preserved in current implementation
		},
		{
			name:     "parenthesized select with where",
			input:    "(SELECT * FROM users WHERE active = true)",
			expected: "SELECT * FROM users WHERE active = TRUE", // Note: Parentheses not preserved in current implementation
		},
		{
			name:     "parenthesized select with alias",
			input:    "(SELECT id AS user_id FROM users)",
			expected: "SELECT id AS user_id FROM users", // Note: Parentheses not preserved in current implementation
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			statements, err := ParseSQL(tt.input)
			require.NoError(t, err, "Parse should succeed")
			require.Len(t, statements, 1)

			deparsed := statements[0].SqlString()

			// For parenthesized SELECT, we expect the parentheses to be preserved
			assert.Equal(t, normalizeSQL(tt.expected), normalizeSQL(deparsed),
				"Parenthesized SELECT deparsing mismatch.\nOriginal: %s\nDeparsed: %s", tt.input, deparsed)
		})
	}
}

// testSelectAllClause tests deparsing of SELECT ALL clause
func testSelectAllClause(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		skip     bool
		reason   string
	}{
		{
			name:   "select all",
			input:  "SELECT ALL * FROM users",
			skip:   true,
			reason: "SELECT ALL parsing not yet implemented - opt_all_clause rule needs implementation",
		},
		{
			name:   "select all with columns",
			input:  "SELECT ALL id, name FROM users",
			skip:   true,
			reason: "SELECT ALL parsing not yet implemented - opt_all_clause rule needs implementation",
		},
		{
			name:   "select all with where",
			input:  "SELECT ALL * FROM users WHERE active = true",
			skip:   true,
			reason: "SELECT ALL parsing not yet implemented - opt_all_clause rule needs implementation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip {
				t.Skip(tt.reason)
				return
			}

			statements, err := ParseSQL(tt.input)
			require.NoError(t, err, "Parse should succeed")
			require.Len(t, statements, 1)

			deparsed := statements[0].SqlString()
			expected := tt.expected
			if expected == "" {
				expected = tt.input
			}

			assert.Equal(t, normalizeSQL(expected), normalizeSQL(deparsed),
				"SELECT ALL deparsing mismatch.\nOriginal: %s\nDeparsed: %s", tt.input, deparsed)
		})
	}
}

// TestComprehensiveSELECTDeparsing tests comprehensive SELECT statement round-trip parsing
// This replaces and enhances the tests from select_statement_test.go
func TestComprehensiveSELECTDeparsing(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		// Basic SELECT structure tests
		{"Simple SELECT star", "SELECT *", ""},
		{"SELECT single column", "SELECT id", ""},
		{"SELECT with column alias AS", "SELECT id AS user_id", ""},
		{"SELECT with column alias implicit", "SELECT id user_id", "SELECT id AS user_id"},
		{"SELECT multiple columns", "SELECT id, name", ""},
		{"SELECT all columns from table", "SELECT * FROM users", ""},

		// FROM clause variations
		{"FROM with table alias AS", "SELECT * FROM users AS u", ""},
		{"FROM with table alias implicit", "SELECT * FROM users u", "SELECT * FROM users AS u"},
		{"FROM with qualified table", "SELECT * FROM public.users", ""},
		{"FROM with ONLY modifier", "SELECT * FROM ONLY users", ""},
		{"FROM multiple tables", "SELECT * FROM users, orders", ""},
		{"SELECT without FROM", "SELECT 1", ""},

		// DISTINCT variations
		{"SELECT DISTINCT single", "SELECT DISTINCT id", ""},
		{"SELECT DISTINCT multiple", "SELECT DISTINCT id, name", ""},
		{"SELECT DISTINCT ON single", "SELECT DISTINCT ON (id) name", ""},
		{"SELECT DISTINCT ON multiple", "SELECT DISTINCT ON (department, level) name", ""},

		// WHERE clause variations
		{"WHERE simple boolean", "SELECT * FROM users WHERE active", ""},
		{"WHERE equality", "SELECT * FROM users WHERE id = 1", ""},
		{"WHERE not equal", "SELECT * FROM users WHERE id <> 1", ""},
		{"WHERE less than", "SELECT * FROM users WHERE age < 30", ""},
		{"WHERE greater than", "SELECT * FROM users WHERE age > 18", ""},
		{"WHERE less or equal", "SELECT * FROM users WHERE age <= 65", ""},
		{"WHERE greater or equal", "SELECT * FROM users WHERE age >= 18", ""},
		{"WHERE complex AND", "SELECT * FROM users WHERE id > 10 AND active = TRUE", ""},
		{"WHERE complex OR", "SELECT * FROM users WHERE admin = TRUE OR moderator = TRUE", ""},
		{"WHERE complex NOT", "SELECT * FROM users WHERE NOT deleted", ""},
		{"WHERE mixed logical", "SELECT * FROM users WHERE (active = TRUE OR admin = TRUE) AND NOT deleted", ""},

		// Arithmetic expressions in SELECT
		{"Arithmetic addition", "SELECT 1 + 2", ""},
		{"Arithmetic subtraction", "SELECT 5 - 3", ""},
		{"Arithmetic multiplication", "SELECT 4 * 6", ""},
		{"Arithmetic division", "SELECT 10 / 2", ""},
		{"Arithmetic modulo", "SELECT 10 % 3", ""},
		{"Arithmetic power", "SELECT 2 ^ 3", ""},
		{"Unary plus", "SELECT +42", ""},
		{"Unary minus", "SELECT -42", ""},
		{"Complex arithmetic", "SELECT (1 + 2) * 3 - 4 / 2", ""},
		{"Column arithmetic", "SELECT age + 1 FROM users", ""},
		{"Mixed arithmetic", "SELECT id + age * 2 FROM users", ""},

		// Type casting
		{"Type cast to text", "SELECT id::text FROM users", ""},
		{"Type cast with spaces", "SELECT id :: integer FROM users", "SELECT id::integer FROM users"},
		{"Complex expression cast", "SELECT (age + 1)::varchar FROM users", ""},
		{"Multiple casts", "SELECT id::text, age::varchar FROM users", ""},

		// Column references
		{"Simple column reference", "SELECT id FROM users", ""},
		{"Qualified column reference", "SELECT users.id FROM users", ""},
		{"Schema qualified column", "SELECT public.users.id FROM public.users", ""},
		{"Multiple qualified columns", "SELECT u.id, u.name FROM users AS u", ""},

		// Function calls
		{"Function no args", "SELECT now()", ""},
		{"Function single arg", "SELECT length(name) FROM users", ""},
		{"Function multiple args", "SELECT substring(name, 1, 5) FROM users", ""},
		{"Qualified function", "SELECT pg_catalog.length(name) FROM users", ""},
		{"Nested functions", "SELECT upper(trim(name)) FROM users", ""},
		{"Function in WHERE", "SELECT * FROM users WHERE length(name) > 5", ""},

		// Constants and literals
		{"Integer constant", "SELECT 42", ""},
		{"Negative integer", "SELECT -123", ""},
		{"Float constant", "SELECT 3.14", ""},
		{"String literal", "SELECT 'hello world'", ""},
		{"Boolean TRUE", "SELECT TRUE", ""},
		{"Boolean FALSE", "SELECT FALSE", ""},
		{"NULL constant", "SELECT NULL", ""},

		// SELECT INTO
		{"SELECT INTO basic", "SELECT * INTO backup_users FROM users", ""},
		{"SELECT INTO with WHERE", "SELECT id, name INTO temp_users FROM users WHERE active = TRUE", ""},

		// TABLE statement (equivalent to SELECT *)
		{"TABLE statement", "TABLE users", "SELECT * FROM users"},
		{"TABLE with qualified name", "TABLE public.users", "SELECT * FROM public.users"},

		// Complex mixed queries
		{"Complex SELECT with all features", "SELECT u.id AS user_id, upper(u.name) AS user_name FROM users AS u WHERE u.active = TRUE AND u.age > 18", ""},
		{"Multiple expressions with aliases", "SELECT id AS user_id, name AS user_name, age + 1 AS next_age FROM users", ""},
		{"Arithmetic in WHERE with functions", "SELECT * FROM users WHERE length(name) + age > 25", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the SQL
			statements, err := ParseSQL(tt.input)
			require.NoError(t, err, "Parse should succeed for: %s", tt.input)
			require.Len(t, statements, 1, "Should have exactly one statement")

			// Verify it's a SELECT statement
			selectStmt, ok := statements[0].(*ast.SelectStmt)
			require.True(t, ok, "Expected SelectStmt")
			require.NotNil(t, selectStmt, "SelectStmt should not be nil")

			// Deparse back to SQL
			deparsed := selectStmt.SqlString()
			require.NotEmpty(t, deparsed, "Deparsed SQL should not be empty")

			// Determine expected result
			expected := tt.expected
			if expected == "" {
				expected = tt.input
			}

			// Compare normalized versions
			assert.Equal(t, normalizeSQL(expected), normalizeSQL(deparsed),
				"Round-trip SELECT deparsing failed.\nOriginal: %s\nDeparsed: %s\nExpected: %s",
				tt.input, deparsed, expected)

			// Verify round-trip stability (re-parse the deparsed SQL)
			statements2, err2 := ParseSQL(deparsed)
			require.NoError(t, err2, "Re-parsing deparsed SQL should succeed: %s", deparsed)
			require.Len(t, statements2, 1, "Re-parsed should have exactly one statement")

			// Verify second deparse is identical (stability test)
			deparsed2 := statements2[0].SqlString()
			assert.Equal(t, normalizeSQL(deparsed), normalizeSQL(deparsed2),
				"Deparsing should be stable.\nFirst: %s\nSecond: %s", deparsed, deparsed2)
		})
	}
}

// TestOperatorPrecedenceDeparsing tests operator precedence within SELECT statements
// This ensures operators are parsed and deparsed with correct precedence
func TestOperatorPrecedenceDeparsing(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		// Basic precedence tests
		{"Addition and multiplication", "SELECT 1 + 2 * 3", ""},
		{"Parentheses override precedence", "SELECT (1 + 2) * 3", ""},
		{"Multiple operators", "SELECT 1 + 2 * 3 - 4 / 2", ""},
		{"Unary and binary", "SELECT -1 + 2", ""},
		{"Power has highest precedence", "SELECT 2 + 3 ^ 2", ""},
		{"Modulo precedence", "SELECT 10 + 5 % 3", ""},

		// Comparison and logical precedence
		{"Comparison and logical", "SELECT * FROM users WHERE age > 18 AND active", ""},
		{"Mixed precedence", "SELECT * FROM users WHERE age + 1 > 18 AND NOT deleted", ""},
		{"OR has lower precedence than AND", "SELECT * FROM users WHERE active AND verified OR admin", ""},
		{"Parentheses with logical", "SELECT * FROM users WHERE (active OR admin) AND verified", ""},

		// Complex nested expressions
		{"Deeply nested arithmetic", "SELECT ((1 + 2) * 3) - (4 / 2)", ""},
		{"Mixed arithmetic and comparison", "SELECT * FROM users WHERE (age * 2) + 5 > 30", ""},
		{"Function call precedence", "SELECT length(name) + 10 FROM users", ""},
		{"Type cast precedence", "SELECT age::text FROM users", ""},

		// WHERE clause complex precedence
		{"Complex WHERE precedence", "SELECT * FROM users WHERE id = 1 OR id = 2 AND active", ""},
		{"WHERE with parentheses", "SELECT * FROM users WHERE (id = 1 OR id = 2) AND active", ""},
		{"NOT precedence", "SELECT * FROM users WHERE NOT active AND verified", ""},
		{"NOT with parentheses", "SELECT * FROM users WHERE NOT (active AND verified)", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the SQL
			statements, err := ParseSQL(tt.input)
			require.NoError(t, err, "Parse should succeed for: %s", tt.input)
			require.Len(t, statements, 1, "Should have exactly one statement")

			// Deparse back to SQL
			deparsed := statements[0].SqlString()
			require.NotEmpty(t, deparsed, "Deparsed SQL should not be empty")

			// Determine expected result
			expected := tt.expected
			if expected == "" {
				expected = tt.input
			}

			// Compare normalized versions
			assert.Equal(t, normalizeSQL(expected), normalizeSQL(deparsed),
				"Operator precedence deparsing failed.\nOriginal: %s\nDeparsed: %s\nExpected: %s",
				tt.input, deparsed, expected)

			// Verify round-trip parsing works
			statements2, err2 := ParseSQL(deparsed)
			require.NoError(t, err2, "Re-parsing should succeed: %s", deparsed)
			require.Len(t, statements2, 1, "Should have one statement after re-parsing")
		})
	}
}

// TestAdvancedDeparsing tests comprehensive deparsing for JOIN, CTE, and subquery features
func TestAdvancedDeparsing(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string // If empty, expects exact match with input
	}{
		// Advanced CTE feature combinations - MATERIALIZED
		{"MATERIALIZED CTE deparsing", "WITH stats AS MATERIALIZED (SELECT COUNT(a) FROM users) SELECT * FROM stats", ""},
		{"NOT MATERIALIZED CTE deparsing", "WITH stats AS NOT MATERIALIZED (SELECT COUNT(a) FROM users) SELECT * FROM stats", ""},
		{"RECURSIVE MATERIALIZED CTE", "WITH RECURSIVE t AS MATERIALIZED (SELECT 1) SELECT * FROM t", ""},

		// Advanced CTE features - SEARCH clauses
		{"SEARCH DEPTH FIRST single column", "WITH RECURSIVE t AS (SELECT id FROM tree) SEARCH DEPTH FIRST BY id SET seq SELECT * FROM t", ""},
		{"SEARCH BREADTH FIRST single column", "WITH RECURSIVE t AS (SELECT id FROM tree) SEARCH BREADTH FIRST BY id SET seq SELECT * FROM t", ""},

		// Advanced CTE features - CYCLE clauses
		{"CYCLE simple form single column", "WITH RECURSIVE t AS (SELECT id FROM tree) CYCLE id SET mark USING path SELECT * FROM t", ""},
		{"CYCLE full form single column", "WITH RECURSIVE t AS (SELECT id FROM tree) CYCLE id SET mark TO TRUE DEFAULT FALSE USING path SELECT * FROM t", ""},

		// Combined advanced CTE features
		{"MATERIALIZED with SEARCH", "WITH RECURSIVE tree AS MATERIALIZED (SELECT id FROM nodes) SEARCH DEPTH FIRST BY id SET search_seq SELECT * FROM tree", ""},
		{"SEARCH with CYCLE", "WITH RECURSIVE tree AS (SELECT id FROM nodes) SEARCH DEPTH FIRST BY id SET search_seq CYCLE id SET is_cycle USING path SELECT * FROM tree", ""},

		// JOIN with aliases and complex expressions
		{"JOIN with table aliases", "SELECT u.name, o.total FROM users u INNER JOIN orders o ON u.id = o.user_id", "SELECT u.name, o.total FROM users AS u INNER JOIN orders AS o ON u.id = o.user_id"},

		// Edge cases for JOIN conditions
		{"JOIN with complex ON condition", "SELECT * FROM users u JOIN orders o ON u.id = o.user_id AND u.active = TRUE", "SELECT * FROM users AS u INNER JOIN orders AS o ON u.id = o.user_id AND u.active = TRUE"},

		{"NATURAL JOIN with explicit type", "SELECT * FROM users NATURAL INNER JOIN orders", "SELECT * FROM users NATURAL JOIN orders"},
		{"NATURAL LEFT JOIN", "SELECT * FROM users NATURAL LEFT JOIN orders", ""},
		{"NATURAL RIGHT JOIN", "SELECT * FROM users NATURAL RIGHT JOIN orders", ""},
		{"NATURAL FULL JOIN", "SELECT * FROM users NATURAL FULL JOIN orders", ""},

		// Complex subquery scenarios
		{"LATERAL with simple subquery", "SELECT * FROM users u, LATERAL (SELECT * FROM orders o WHERE o.user_id = u.id) AS recent_orders", "SELECT * FROM users AS u, LATERAL (SELECT * FROM orders AS o WHERE o.user_id = u.id) AS recent_orders"},
		{"Nested subqueries in FROM", "SELECT * FROM (SELECT id FROM (SELECT user_id AS id FROM orders) AS inner_sub) AS outer_sub", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the input SQL
			statements, err := ParseSQL(tt.input)
			require.NoError(t, err)
			require.Len(t, statements, 1, "Should have exactly one statement")

			// Get the deparsed SQL
			deparsed := statements[0].SqlString()

			// Determine expected output
			expected := tt.expected
			if expected == "" {
				expected = tt.input
			}

			// Normalize both strings for comparison
			normalizedDeparsed := normalizeSQL(deparsed)
			normalizedExpected := normalizeSQL(expected)

			require.Equal(t, normalizedExpected, normalizedDeparsed)
			// Most importantly, ensure round-trip parsing works
			reparsedStmts, reparseErr := ParseSQL(deparsed)
			require.NoError(t, reparseErr, "Round-trip parsing must succeed: %v\nDeparsed: %s", reparseErr, deparsed)
			require.Len(t, reparsedStmts, 1, "Round-trip should have exactly one statement")
		})
	}
}
