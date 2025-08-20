// JOIN & CTE Test File
// Tests for JOIN operations, Common Table Expressions (CTEs), and subqueries
package parser

import (
	"testing"

	"github.com/multigres/parser/go/parser/ast"
	"github.com/stretchr/testify/assert"
)

// TestJoinParsing tests basic JOIN functionality
func TestJoinParsing(t *testing.T) {

	testCases := []struct {
		name     string
		sql      string
		expected string
	}{
		{
			name:     "INNER JOIN",
			sql:      "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id",
			expected: "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id",
		},
		{
			name:     "LEFT JOIN",
			sql:      "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id",
			expected: "SELECT * FROM users LEFT OUTER JOIN orders ON users.id = orders.user_id",
		},
		{
			name:     "JOIN with USING",
			sql:      "SELECT * FROM users JOIN orders USING (user_id)",
			expected: "SELECT * FROM users INNER JOIN orders USING ('user_id')",
		},
		{
			name:     "CROSS JOIN",
			sql:      "SELECT * FROM users CROSS JOIN orders",
			expected: "SELECT * FROM users INNER JOIN orders",
		},
		{
			name:     "NATURAL JOIN",
			sql:      "SELECT * FROM users NATURAL JOIN orders",
			expected: "SELECT * FROM users NATURAL JOIN orders",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Parse the SQL
			statements, err := ParseSQL(tc.sql)
			assert.NoError(t, err, "Parse should succeed for: %s", tc.sql)
			assert.Len(t, statements, 1, "Should have exactly one statement")

			// Verify it's a SELECT statement
			selectStmt, ok := statements[0].(*ast.SelectStmt)
			assert.True(t, ok, "Should be a SelectStmt")
			assert.NotNil(t, selectStmt, "SelectStmt should not be nil")

			// Test deparsing round-trip
			deparsed := selectStmt.SqlString()
			assert.NotEmpty(t, deparsed, "Deparsed SQL should not be empty")
			assert.Equal(t, tc.expected, deparsed, "Deparsed SQL should match expected output")
		})
	}
}

// TestCTEParsing tests Common Table Expression functionality
func TestCTEParsing(t *testing.T) {

	testCases := []struct {
		name     string
		sql      string
		expected string
	}{
		{
			name:     "Basic CTE",
			sql:      "WITH user_stats AS (SELECT user_id FROM orders) SELECT * FROM user_stats",
			expected: "WITH user_stats AS (SELECT user_id FROM orders) SELECT * FROM user_stats",
		},
		{
			name:     "Recursive CTE",
			sql:      "WITH RECURSIVE t AS (SELECT 1) SELECT * FROM t",
			expected: "WITH RECURSIVE t AS (SELECT 1) SELECT * FROM t",
		},
		{
			name:     "Multiple CTEs",
			sql:      "WITH stats AS (SELECT * FROM orders), totals AS (SELECT amount FROM stats) SELECT * FROM totals",
			expected: "WITH stats AS (SELECT * FROM orders), totals AS (SELECT amount FROM stats) SELECT * FROM totals",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Parse the SQL
			statements, err := ParseSQL(tc.sql)
			assert.NoError(t, err, "Parse should succeed for: %s", tc.sql)
			assert.Len(t, statements, 1, "Should have exactly one statement")

			// Verify it's a SELECT statement
			selectStmt, ok := statements[0].(*ast.SelectStmt)
			assert.True(t, ok, "Should be a SelectStmt")
			assert.NotNil(t, selectStmt, "SelectStmt should not be nil")

			// Verify WITH clause exists for CTE tests
			assert.NotNil(t, selectStmt.WithClause, "SELECT should have WITH clause")

			// Test deparsing round-trip
			deparsed := selectStmt.SqlString()
			assert.NotEmpty(t, deparsed, "Deparsed SQL should not be empty")
			assert.Equal(t, tc.expected, deparsed, "Deparsed SQL should match expected output")
		})
	}
}

// TestSubqueryParsing tests subquery functionality
func TestSubqueryParsing(t *testing.T) {

	testCases := []struct {
		name     string
		sql      string
		expected string
	}{
		{
			name:     "Subquery in FROM",
			sql:      "SELECT * FROM (SELECT user_id FROM orders) AS stats",
			expected: "SELECT * FROM (SELECT user_id FROM orders) AS stats",
		},
		{
			name:     "LATERAL subquery",
			sql:      "SELECT * FROM users, LATERAL (SELECT * FROM orders) AS user_orders",
			expected: "SELECT * FROM users, LATERAL (SELECT * FROM orders) AS user_orders",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Parse the SQL
			statements, err := ParseSQL(tc.sql)
			assert.NoError(t, err, "Parse should succeed for: %s", tc.sql)
			assert.Len(t, statements, 1, "Should have exactly one statement")

			// Verify it's a SELECT statement
			selectStmt, ok := statements[0].(*ast.SelectStmt)
			assert.True(t, ok, "Should be a SelectStmt")
			assert.NotNil(t, selectStmt, "SelectStmt should not be nil")

			// Test deparsing round-trip
			deparsed := selectStmt.SqlString()
			assert.NotEmpty(t, deparsed, "Deparsed SQL should not be empty")
			assert.Equal(t, tc.expected, deparsed, "Deparsed SQL should match expected output")
		})
	}
}

// TestJoinCTEASTParsing tests that AST nodes are created correctly
func TestJoinCTEASTParsing(t *testing.T) {
	// Test that our AST nodes exist and work correctly
	t.Run("JoinExpr AST", func(t *testing.T) {
		// Test basic JoinExpr creation
		left := ast.NewRangeVar("users", "", "")
		right := ast.NewRangeVar("orders", "", "")
		condition := ast.NewConst(16, ast.Datum(1), false) // boolean constant

		join := ast.NewJoinExpr(ast.JOIN_INNER, left, right, condition)

		assert.Equal(t, ast.T_JoinExpr, join.Tag)
		assert.Equal(t, ast.JOIN_INNER, join.Jointype)
		assert.False(t, join.IsNatural)
		assert.NotNil(t, join.Larg)
		assert.NotNil(t, join.Rarg)
		assert.NotNil(t, join.Quals)
	})

	t.Run("USING JoinExpr AST", func(t *testing.T) {
		// Test USING join creation
		left := ast.NewRangeVar("users", "", "")
		right := ast.NewRangeVar("orders", "", "")
		usingClause := ast.NewNodeList(ast.NewString("user_id"))

		join := ast.NewUsingJoinExpr(ast.JOIN_LEFT, left, right, usingClause)

		assert.Equal(t, ast.T_JoinExpr, join.Tag)
		assert.Equal(t, ast.JOIN_LEFT, join.Jointype)
		assert.False(t, join.IsNatural)
		assert.NotNil(t, join.UsingClause)
		assert.Equal(t, 1, join.UsingClause.Len())
	})

	t.Run("CommonTableExpr AST", func(t *testing.T) {
		// Test CTE creation
		query := ast.NewSelectStmt()
		cte := ast.NewCommonTableExpr("test_cte", query)

		assert.Equal(t, ast.T_CommonTableExpr, cte.Tag)
		assert.Equal(t, "test_cte", cte.Ctename)
		assert.NotNil(t, cte.Ctequery)
	})

	t.Run("WithClause AST", func(t *testing.T) {
		// Test WITH clause creation
		cte := ast.NewCommonTableExpr("test", ast.NewSelectStmt())
		ctes := ast.NewNodeList(cte)
		withClause := ast.NewWithClause(ctes, false, 0)

		assert.Equal(t, ast.T_WithClause, withClause.Tag)
		assert.False(t, withClause.Recursive)
		assert.NotNil(t, withClause.Ctes)
		assert.Equal(t, 1, withClause.Ctes.Len())
	})

	t.Run("RangeSubselect AST", func(t *testing.T) {
		// Test subquery in FROM clause
		subquery := ast.NewSelectStmt()
		alias := ast.NewAlias("sub", nil)
		rangeSubselect := ast.NewRangeSubselect(false, subquery, alias)

		assert.Equal(t, ast.T_RangeSubselect, rangeSubselect.Tag)
		assert.False(t, rangeSubselect.Lateral)
		assert.NotNil(t, rangeSubselect.Subquery)
		assert.NotNil(t, rangeSubselect.Alias)
	})
}

// TestAdvancedCTE tests the advanced CTE functionality including SEARCH, CYCLE, and MATERIALIZED
func TestAdvancedCTE(t *testing.T) {
	testCases := []struct {
		name  string
		query string
	}{
		{
			"MATERIALIZED CTE",
			"WITH user_stats AS MATERIALIZED (SELECT user_id FROM orders) SELECT * FROM user_stats",
		},
		{
			"NOT MATERIALIZED CTE",
			"WITH user_stats AS NOT MATERIALIZED (SELECT user_id FROM orders) SELECT * FROM user_stats",
		},
		{
			"CTE with SEARCH DEPTH FIRST",
			"WITH RECURSIVE org_chart AS (SELECT id FROM employees) SEARCH DEPTH FIRST BY id SET search_seq SELECT * FROM org_chart",
		},
		{
			"CTE with SEARCH BREADTH FIRST",
			"WITH RECURSIVE org_chart AS (SELECT id FROM employees) SEARCH BREADTH FIRST BY id SET search_seq SELECT * FROM org_chart",
		},
		{
			"CTE with CYCLE simple",
			"WITH RECURSIVE org_chart AS (SELECT id FROM employees) CYCLE id SET is_cycle USING path SELECT * FROM org_chart",
		},
		{
			"CTE with CYCLE full",
			"WITH RECURSIVE org_chart AS (SELECT id FROM employees) CYCLE id SET is_cycle TO TRUE DEFAULT FALSE USING path SELECT * FROM org_chart",
		},
		{
			"CTE with both SEARCH and CYCLE",
			"WITH RECURSIVE org_chart AS (SELECT id FROM employees) SEARCH DEPTH FIRST BY id SET search_seq CYCLE id SET is_cycle USING path SELECT * FROM org_chart",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test that the query can be parsed
			stmts, err := ParseSQL(tc.query)
			assert.NoError(t, err, "Failed to parse query: %s", tc.query)
			assert.Len(t, stmts, 1, "Expected exactly one statement")

			// Test that we can deparse it back to SQL
			deparsed := stmts[0].SqlString()
			assert.NotEmpty(t, deparsed, "Deparsed query should not be empty")

			// Test that the deparsed query is still parseable
			_, err = ParseSQL(deparsed)
			assert.NoError(t, err, "Failed to parse deparsed query: %s", deparsed)
			assert.EqualValues(t, tc.query, deparsed)
		})
	}
}

// TestAdvancedCTEAST tests the AST nodes for advanced CTE features
func TestAdvancedCTEAST(t *testing.T) {
	t.Run("CTESearchClause AST", func(t *testing.T) {
		// Test SEARCH clause creation
		colList := ast.NewNodeList(ast.NewString("id"))
		searchClause := ast.NewCTESearchClause(colList, false, "search_seq")

		assert.Equal(t, ast.T_CTESearchClause, searchClause.Tag)
		assert.False(t, searchClause.SearchBreadthFirst)
		assert.Equal(t, "search_seq", searchClause.SearchSeqColumn)
		assert.NotNil(t, searchClause.SearchColList)

		// Test SqlString method
		sqlStr := searchClause.SqlString()
		assert.Contains(t, sqlStr, "SEARCH DEPTH FIRST")
		assert.Contains(t, sqlStr, "SET search_seq")
	})

	t.Run("CTECycleClause AST", func(t *testing.T) {
		// Test CYCLE clause creation
		colList := ast.NewNodeList(ast.NewString("id"))
		trueConst := ast.NewA_Const(ast.NewBoolean(true), -1)
		falseConst := ast.NewA_Const(ast.NewBoolean(false), -1)
		cycleClause := ast.NewCTECycleClause(colList, "is_cycle", trueConst, falseConst, "path")

		assert.Equal(t, ast.T_CTECycleClause, cycleClause.Tag)
		assert.Equal(t, "is_cycle", cycleClause.CycleMarkColumn)
		assert.Equal(t, "path", cycleClause.CyclePathColumn)
		assert.NotNil(t, cycleClause.CycleColList)
		assert.NotNil(t, cycleClause.CycleMarkValue)
		assert.NotNil(t, cycleClause.CycleMarkDefault)

		// Test SqlString method
		sqlStr := cycleClause.SqlString()
		assert.Contains(t, sqlStr, "CYCLE")
		assert.Contains(t, sqlStr, "SET is_cycle")
		assert.Contains(t, sqlStr, "USING path")
	})

	t.Run("CTE with SEARCH and CYCLE clauses", func(t *testing.T) {
		// Test CommonTableExpr with advanced features
		query := ast.NewSelectStmt()
		cte := ast.NewCommonTableExpr("test_cte", query)

		// Add SEARCH clause
		colList := ast.NewNodeList(ast.NewString("id"))
		searchClause := ast.NewCTESearchClause(colList, true, "search_seq")
		cte.SearchClause = searchClause

		// Add CYCLE clause
		trueConst := ast.NewA_Const(ast.NewBoolean(true), -1)
		falseConst := ast.NewA_Const(ast.NewBoolean(false), -1)
		cycleClause := ast.NewCTECycleClause(colList, "is_cycle", trueConst, falseConst, "path")
		cte.CycleClause = cycleClause

		// Set materialization
		cte.Ctematerialized = ast.CTEMaterializeAlways

		assert.Equal(t, ast.T_CommonTableExpr, cte.Tag)
		assert.NotNil(t, cte.SearchClause)
		assert.NotNil(t, cte.CycleClause)
		assert.Equal(t, ast.CTEMaterializeAlways, cte.Ctematerialized)

		// Test SqlString method includes all clauses
		sqlStr := cte.SqlString()
		assert.Contains(t, sqlStr, "MATERIALIZED")
		assert.Contains(t, sqlStr, "SEARCH BREADTH FIRST")
		assert.Contains(t, sqlStr, "CYCLE")
	})
}
