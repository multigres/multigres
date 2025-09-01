package parser

import (
	"strings"
	"testing"

	"github.com/multigres/parser/go/parser/ast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGroupByBasic tests basic GROUP BY functionality
func TestGroupByBasic(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantErr  bool
		validate func(*testing.T, ast.Node)
	}{
		{
			name: "simple GROUP BY",
			sql:  "SELECT col1, COUNT(*) FROM table1 GROUP BY col1",
			validate: func(t *testing.T, node ast.Node) {
				selectStmt := node.(*ast.SelectStmt)
				assert.NotNil(t, selectStmt.GroupClause)
				assert.Equal(t, 1, selectStmt.GroupClause.Len())
			},
		},
		{
			name: "GROUP BY multiple columns",
			sql:  "SELECT col1, col2, COUNT(*) FROM table1 GROUP BY col1, col2",
			validate: func(t *testing.T, node ast.Node) {
				selectStmt := node.(*ast.SelectStmt)
				assert.NotNil(t, selectStmt.GroupClause)
				assert.Equal(t, 2, selectStmt.GroupClause.Len())
			},
		},
		{
			name: "GROUP BY with expression",
			sql:  "SELECT DATE(created_at), COUNT(*) FROM table1 GROUP BY DATE(created_at)",
			validate: func(t *testing.T, node ast.Node) {
				selectStmt := node.(*ast.SelectStmt)
				assert.NotNil(t, selectStmt.GroupClause)
				assert.Equal(t, 1, selectStmt.GroupClause.Len())
			},
		},
		{
			name: "GROUP BY with ORDER BY",
			sql:  "SELECT col1, COUNT(*) FROM table1 GROUP BY col1 ORDER BY col1",
			validate: func(t *testing.T, node ast.Node) {
				selectStmt := node.(*ast.SelectStmt)
				assert.NotNil(t, selectStmt.GroupClause)
				assert.NotNil(t, selectStmt.SortClause)
				assert.Equal(t, 1, selectStmt.GroupClause.Len())
				assert.Equal(t, 1, selectStmt.SortClause.Len())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := ParseSQL(tt.sql)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, stmts)
			require.Equal(t, 1, len(stmts))

			if tt.validate != nil {
				tt.validate(t, stmts[0])
			}
		})
	}
}

// TestGroupByAdvanced tests advanced GROUP BY features (ROLLUP, CUBE, GROUPING SETS)
func TestGroupByAdvanced(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantErr  bool
		validate func(*testing.T, ast.Node)
	}{
		{
			name: "GROUP BY ROLLUP",
			sql:  "SELECT col1, col2, COUNT(*) FROM table1 GROUP BY ROLLUP(col1, col2)",
			validate: func(t *testing.T, node ast.Node) {
				selectStmt := node.(*ast.SelectStmt)
				assert.NotNil(t, selectStmt.GroupClause)
				assert.Equal(t, 1, selectStmt.GroupClause.Len())

				// Check that the group item is a GroupingSet
				groupItem := selectStmt.GroupClause.Items[0]
				groupingSet, ok := groupItem.(*ast.GroupingSet)
				assert.True(t, ok, "Expected GroupingSet")
				assert.Equal(t, ast.GROUPING_SET_ROLLUP, groupingSet.Kind)
			},
		},
		{
			name: "GROUP BY CUBE",
			sql:  "SELECT col1, col2, COUNT(*) FROM table1 GROUP BY CUBE(col1, col2)",
			validate: func(t *testing.T, node ast.Node) {
				selectStmt := node.(*ast.SelectStmt)
				assert.NotNil(t, selectStmt.GroupClause)
				assert.Equal(t, 1, selectStmt.GroupClause.Len())

				groupItem := selectStmt.GroupClause.Items[0]
				groupingSet, ok := groupItem.(*ast.GroupingSet)
				assert.True(t, ok, "Expected GroupingSet")
				assert.Equal(t, ast.GROUPING_SET_CUBE, groupingSet.Kind)
			},
		},
		{
			name: "GROUP BY GROUPING SETS",
			sql:  "SELECT col1, col2, COUNT(*) FROM table1 GROUP BY GROUPING SETS ((col1), (col2), ())",
			validate: func(t *testing.T, node ast.Node) {
				selectStmt := node.(*ast.SelectStmt)
				assert.NotNil(t, selectStmt.GroupClause)
				assert.Equal(t, 1, selectStmt.GroupClause.Len())

				groupItem := selectStmt.GroupClause.Items[0]
				groupingSet, ok := groupItem.(*ast.GroupingSet)
				assert.True(t, ok, "Expected GroupingSet")
				assert.Equal(t, ast.GROUPING_SET_SETS, groupingSet.Kind)
			},
		},
		{
			name: "GROUP BY empty grouping set",
			sql:  "SELECT COUNT(*) FROM table1 GROUP BY ()",
			validate: func(t *testing.T, node ast.Node) {
				selectStmt := node.(*ast.SelectStmt)
				assert.NotNil(t, selectStmt.GroupClause)
				assert.Equal(t, 1, selectStmt.GroupClause.Len())

				groupItem := selectStmt.GroupClause.Items[0]
				groupingSet, ok := groupItem.(*ast.GroupingSet)
				assert.True(t, ok, "Expected GroupingSet")
				assert.Equal(t, ast.GROUPING_SET_EMPTY, groupingSet.Kind)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := ParseSQL(tt.sql)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, stmts)
			require.Equal(t, 1, len(stmts))

			if tt.validate != nil {
				tt.validate(t, stmts[0])
			}
		})
	}
}

// TestHavingClause tests HAVING clause functionality
func TestHavingClause(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantErr  bool
		validate func(*testing.T, ast.Node)
	}{
		{
			name: "simple HAVING",
			sql:  "SELECT col1, COUNT(*) FROM table1 GROUP BY col1 HAVING COUNT(*) > 1",
			validate: func(t *testing.T, node ast.Node) {
				selectStmt := node.(*ast.SelectStmt)
				assert.NotNil(t, selectStmt.GroupClause)
				assert.NotNil(t, selectStmt.HavingClause)
			},
		},
		{
			name: "HAVING with AND",
			sql:  "SELECT col1, COUNT(*) FROM table1 GROUP BY col1 HAVING COUNT(*) > 1 AND col1 IS NOT NULL",
			validate: func(t *testing.T, node ast.Node) {
				selectStmt := node.(*ast.SelectStmt)
				assert.NotNil(t, selectStmt.GroupClause)
				assert.NotNil(t, selectStmt.HavingClause)
			},
		},
		{
			name: "HAVING with ORDER BY",
			sql:  "SELECT col1, COUNT(*) FROM table1 GROUP BY col1 HAVING COUNT(*) > 1 ORDER BY col1",
			validate: func(t *testing.T, node ast.Node) {
				selectStmt := node.(*ast.SelectStmt)
				assert.NotNil(t, selectStmt.GroupClause)
				assert.NotNil(t, selectStmt.HavingClause)
				assert.NotNil(t, selectStmt.SortClause)
				assert.Equal(t, 1, selectStmt.SortClause.Len())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := ParseSQL(tt.sql)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, stmts)
			require.Equal(t, 1, len(stmts))

			if tt.validate != nil {
				tt.validate(t, stmts[0])
			}
		})
	}
}

// TestOrderByClause tests ORDER BY functionality
func TestOrderByClause(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantErr  bool
		validate func(*testing.T, ast.Node)
	}{
		{
			name: "simple ORDER BY",
			sql:  "SELECT col1, col2 FROM table1 ORDER BY col1",
			validate: func(t *testing.T, node ast.Node) {
				selectStmt := node.(*ast.SelectStmt)
				assert.NotNil(t, selectStmt.SortClause)
				assert.Equal(t, 1, selectStmt.SortClause.Len())

				sortBy := selectStmt.SortClause.Items[0].(*ast.SortBy)
				assert.Equal(t, ast.SORTBY_DEFAULT, sortBy.SortbyDir)
				assert.Equal(t, ast.SORTBY_NULLS_DEFAULT, sortBy.SortbyNulls)
			},
		},
		{
			name: "ORDER BY ASC",
			sql:  "SELECT col1, col2 FROM table1 ORDER BY col1 ASC",
			validate: func(t *testing.T, node ast.Node) {
				selectStmt := node.(*ast.SelectStmt)
				assert.NotNil(t, selectStmt.SortClause)
				assert.Equal(t, 1, selectStmt.SortClause.Len())

				sortBy := selectStmt.SortClause.Items[0].(*ast.SortBy)
				assert.Equal(t, ast.SORTBY_ASC, sortBy.SortbyDir)
				assert.Equal(t, ast.SORTBY_NULLS_DEFAULT, sortBy.SortbyNulls)
			},
		},
		{
			name: "ORDER BY DESC",
			sql:  "SELECT col1, col2 FROM table1 ORDER BY col1 DESC",
			validate: func(t *testing.T, node ast.Node) {
				selectStmt := node.(*ast.SelectStmt)
				assert.NotNil(t, selectStmt.SortClause)
				assert.Equal(t, 1, selectStmt.SortClause.Len())

				sortBy := selectStmt.SortClause.Items[0].(*ast.SortBy)
				assert.Equal(t, ast.SORTBY_DESC, sortBy.SortbyDir)
				assert.Equal(t, ast.SORTBY_NULLS_DEFAULT, sortBy.SortbyNulls)
			},
		},
		{
			name: "ORDER BY NULLS FIRST",
			sql:  "SELECT col1, col2 FROM table1 ORDER BY col1 NULLS FIRST",
			validate: func(t *testing.T, node ast.Node) {
				selectStmt := node.(*ast.SelectStmt)
				assert.NotNil(t, selectStmt.SortClause)
				assert.Equal(t, 1, selectStmt.SortClause.Len())

				sortBy := selectStmt.SortClause.Items[0].(*ast.SortBy)
				assert.Equal(t, ast.SORTBY_DEFAULT, sortBy.SortbyDir)
				assert.Equal(t, ast.SORTBY_NULLS_FIRST, sortBy.SortbyNulls)
			},
		},
		{
			name: "ORDER BY DESC NULLS LAST",
			sql:  "SELECT col1, col2 FROM table1 ORDER BY col1 DESC NULLS LAST",
			validate: func(t *testing.T, node ast.Node) {
				selectStmt := node.(*ast.SelectStmt)
				assert.NotNil(t, selectStmt.SortClause)
				assert.Equal(t, 1, selectStmt.SortClause.Len())

				sortBy := selectStmt.SortClause.Items[0].(*ast.SortBy)
				assert.Equal(t, ast.SORTBY_DESC, sortBy.SortbyDir)
				assert.Equal(t, ast.SORTBY_NULLS_LAST, sortBy.SortbyNulls)
			},
		},
		{
			name: "ORDER BY multiple columns",
			sql:  "SELECT col1, col2 FROM table1 ORDER BY col1 ASC, col2 DESC NULLS FIRST",
			validate: func(t *testing.T, node ast.Node) {
				selectStmt := node.(*ast.SelectStmt)
				assert.NotNil(t, selectStmt.SortClause)
				assert.Equal(t, 2, selectStmt.SortClause.Len())

				// First sort column
				sortBy1 := selectStmt.SortClause.Items[0].(*ast.SortBy)
				assert.Equal(t, ast.SORTBY_ASC, sortBy1.SortbyDir)
				assert.Equal(t, ast.SORTBY_NULLS_DEFAULT, sortBy1.SortbyNulls)

				// Second sort column
				sortBy2 := selectStmt.SortClause.Items[1].(*ast.SortBy)
				assert.Equal(t, ast.SORTBY_DESC, sortBy2.SortbyDir)
				assert.Equal(t, ast.SORTBY_NULLS_FIRST, sortBy2.SortbyNulls)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := ParseSQL(tt.sql)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, stmts)
			require.Equal(t, 1, len(stmts))

			if tt.validate != nil {
				tt.validate(t, stmts[0])
			}
		})
	}
}

// TestComplexSelectStatements tests combinations of GROUP BY, HAVING, and ORDER BY
func TestComplexSelectStatements(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantErr  bool
		validate func(*testing.T, ast.Node)
	}{
		{
			name: "GROUP BY + HAVING + ORDER BY",
			sql: `SELECT dept_id, COUNT(*), AVG(salary) 
					FROM employees 
					GROUP BY dept_id 
					HAVING COUNT(*) > 5 
					ORDER BY dept_id ASC`,
			validate: func(t *testing.T, node ast.Node) {
				selectStmt := node.(*ast.SelectStmt)
				assert.NotNil(t, selectStmt.GroupClause)
				assert.NotNil(t, selectStmt.HavingClause)
				assert.NotNil(t, selectStmt.SortClause)
				assert.Equal(t, 1, selectStmt.GroupClause.Len())
				assert.Equal(t, 1, selectStmt.SortClause.Len())
			},
		},
		{
			name: "Complex GROUP BY with ROLLUP and ORDER BY",
			sql: `SELECT year, quarter, SUM(sales) 
					FROM sales_data 
					GROUP BY ROLLUP(year, quarter) 
					ORDER BY year, quarter NULLS LAST`,
			validate: func(t *testing.T, node ast.Node) {
				selectStmt := node.(*ast.SelectStmt)
				assert.NotNil(t, selectStmt.GroupClause)
				assert.NotNil(t, selectStmt.SortClause)
				assert.Equal(t, 1, selectStmt.GroupClause.Len())
				assert.Equal(t, 2, selectStmt.SortClause.Len())

				// Check ROLLUP grouping set
				groupItem := selectStmt.GroupClause.Items[0]
				groupingSet, ok := groupItem.(*ast.GroupingSet)
				assert.True(t, ok, "Expected GroupingSet")
				assert.Equal(t, ast.GROUPING_SET_ROLLUP, groupingSet.Kind)
			},
		},
		{
			name: "All features combined",
			sql: `SELECT category, subcategory, COUNT(*), SUM(amount)
					FROM transactions
					WHERE amount > 0
					GROUP BY GROUPING SETS ((category), (category, subcategory), ())
					HAVING SUM(amount) > 1000
					ORDER BY category NULLS FIRST, subcategory DESC`,
			validate: func(t *testing.T, node ast.Node) {
				selectStmt := node.(*ast.SelectStmt)
				assert.NotNil(t, selectStmt.WhereClause)
				assert.NotNil(t, selectStmt.GroupClause)
				assert.NotNil(t, selectStmt.HavingClause)
				assert.NotNil(t, selectStmt.SortClause)
				assert.Equal(t, 1, selectStmt.GroupClause.Len())
				assert.Equal(t, 2, selectStmt.SortClause.Len())

				// Check GROUPING SETS
				groupItem := selectStmt.GroupClause.Items[0]
				groupingSet, ok := groupItem.(*ast.GroupingSet)
				assert.True(t, ok, "Expected GroupingSet")
				assert.Equal(t, ast.GROUPING_SET_SETS, groupingSet.Kind)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := ParseSQL(tt.sql)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, stmts)
			require.Equal(t, 1, len(stmts))

			if tt.validate != nil {
				tt.validate(t, stmts[0])
			}
		})
	}
}

// TestWindowFunctions tests window function parsing
func TestWindowFunctions(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		// Basic window functions
		{"ROW_NUMBER window function", "SELECT ROW_NUMBER() OVER (ORDER BY id) FROM users"},
		{"RANK window function with PARTITION BY", "SELECT RANK() OVER (PARTITION BY department ORDER BY salary DESC) FROM employees"},
		{"DENSE_RANK window function", "SELECT DENSE_RANK() OVER (PARTITION BY category ORDER BY price) FROM products"},
		{"LAG window function", "SELECT LAG(price, 1) OVER (ORDER BY date) FROM stock_prices"},
		{"LEAD window function with default", "SELECT LEAD(value, 2, 0) OVER (PARTITION BY group_id ORDER BY seq) FROM data"},
		{"FIRST_VALUE window function", "SELECT FIRST_VALUE(name) OVER (PARTITION BY dept ORDER BY hire_date) FROM employees"},
		{"LAST_VALUE window function with frame", "SELECT LAST_VALUE(score) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM scores"},
		{"NTH_VALUE window function", "SELECT NTH_VALUE(amount, 2) OVER (ORDER BY date) FROM transactions"},
		{"PERCENT_RANK window function", "SELECT PERCENT_RANK() OVER (ORDER BY score) FROM test_results"},
		{"CUME_DIST window function", "SELECT CUME_DIST() OVER (ORDER BY salary) FROM employees"},
		{"NTILE window function", "SELECT NTILE(4) OVER (ORDER BY score DESC) FROM students"},

		// Aggregate functions as window functions
		{"SUM window function", "SELECT SUM(amount) OVER (PARTITION BY account ORDER BY date) FROM transactions"},
		{"COUNT window function", "SELECT COUNT(*) OVER (PARTITION BY region ORDER BY date ROWS UNBOUNDED PRECEDING) FROM sales"},
		{"AVG window function with frame", "SELECT AVG(temperature) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) FROM weather"},
		{"MIN and MAX window functions", "SELECT MIN(price) OVER (PARTITION BY category), MAX(price) OVER (PARTITION BY category) FROM products"},

		// Named windows
		{"Named window definition", "SELECT ROW_NUMBER() OVER w FROM users WINDOW w AS (ORDER BY created_at)"},
		{"Multiple named windows", "SELECT ROW_NUMBER() OVER w1, SUM(amount) OVER w2 FROM data WINDOW w1 AS (ORDER BY id), w2 AS (PARTITION BY type ORDER BY date)"},
		{"Named window with inheritance", "SELECT ROW_NUMBER() OVER (w ORDER BY name) FROM users WINDOW w AS (PARTITION BY department)"},

		// Frame specifications
		{"ROWS frame - UNBOUNDED PRECEDING", "SELECT SUM(amount) OVER (ORDER BY date ROWS UNBOUNDED PRECEDING) FROM sales"},
		{"ROWS frame - n PRECEDING", "SELECT AVG(price) OVER (ORDER BY date ROWS 3 PRECEDING) FROM stocks"},
		{"ROWS frame - CURRENT ROW", "SELECT SUM(quantity) OVER (ORDER BY id ROWS CURRENT ROW) FROM orders"},
		{"ROWS frame - BETWEEN PRECEDING AND FOLLOWING", "SELECT AVG(amount) OVER (ORDER BY date ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM transactions"},
		{"ROWS frame - BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW", "SELECT SUM(sales) OVER (PARTITION BY region ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM monthly_sales"},
		{"ROWS frame - BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING", "SELECT COUNT(*) OVER (ORDER BY date ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM events"},
		{"RANGE frame - UNBOUNDED PRECEDING", "SELECT SUM(amount) OVER (ORDER BY amount RANGE UNBOUNDED PRECEDING) FROM payments"},
		{"RANGE frame - CURRENT ROW", "SELECT COUNT(*) OVER (ORDER BY score RANGE CURRENT ROW) FROM results"},
		{"RANGE frame - BETWEEN PRECEDING AND FOLLOWING", "SELECT AVG(value) OVER (ORDER BY val RANGE BETWEEN (SELECT 3) PRECEDING AND (SELECT 4) FOLLOWING) FROM daily_metrics"},
		{"GROUPS frame - UNBOUNDED PRECEDING", "SELECT FIRST_VALUE(name) OVER (ORDER BY score GROUPS UNBOUNDED PRECEDING) FROM players"},
		{"GROUPS frame - n PRECEDING", "SELECT COUNT(*) OVER (ORDER BY category GROUPS 2 PRECEDING) FROM products"},
		{"GROUPS frame - BETWEEN PRECEDING AND FOLLOWING", "SELECT SUM(amount) OVER (ORDER BY date GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM transactions"},

		// Frame exclusion
		{"Frame with EXCLUDE CURRENT ROW", "SELECT AVG(salary) OVER (PARTITION BY dept ORDER BY salary ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW) FROM employees"},
		{"Frame with EXCLUDE GROUP", "SELECT SUM(amount) OVER (ORDER BY category GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE GROUP) FROM sales"},
		{"Frame with EXCLUDE TIES", "SELECT COUNT(*) OVER (ORDER BY score ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE TIES) FROM test_scores"},
		{"Frame with EXCLUDE NO OTHERS", "SELECT MAX(value) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING EXCLUDE NO OTHERS) FROM measurements"},

		// Complex queries
		{"Window function in WHERE clause (subquery)", "SELECT * FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY created_at) as rn FROM users) t WHERE rn <= 10"},
		{"Multiple window functions", "SELECT id, ROW_NUMBER() OVER (ORDER BY salary), RANK() OVER (ORDER BY salary), PERCENT_RANK() OVER (ORDER BY salary) FROM employees"},
		{"Window function with CASE expression", "SELECT CASE WHEN ROW_NUMBER() OVER (ORDER BY score DESC) <= 3 THEN 'Top 3' ELSE 'Other' END FROM students"},
		{"Window function in ORDER BY", "SELECT name FROM employees ORDER BY ROW_NUMBER() OVER (PARTITION BY department ORDER BY hire_date)"},

		// Edge cases
		{"Window function with DISTINCT", "SELECT COUNT(DISTINCT category) OVER (PARTITION BY region) FROM products"},
		{"Window function with ALL", "SELECT SUM(ALL amount) OVER (ORDER BY date) FROM transactions"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := ParseSQL(tt.sql)
			require.NoError(t, err, "Failed to parse SQL: %s", tt.sql)
			require.NotEmpty(t, stmts, "No statements parsed")

			stmt := stmts[0]
			require.IsType(t, &ast.SelectStmt{}, stmt, "Expected SelectStmt")

			selectStmt := stmt.(*ast.SelectStmt)
			require.NotNil(t, selectStmt.TargetList, "TargetList should not be nil")
			assert.True(t, len(selectStmt.TargetList.Items) > 0, "Should have at least one target")
		})
	}
}

// TestAggregateFunctionsWithFilterAndWithinGroup tests aggregate functions with FILTER and WITHIN GROUP clauses
func TestAggregateFunctionsWithFilterAndWithinGroup(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		// Basic FILTER clause tests
		{"COUNT with FILTER", "SELECT COUNT(*) FILTER (WHERE status = 'active') FROM users"},
		{"SUM with FILTER", "SELECT SUM(amount) FILTER (WHERE date > '2024-01-01') FROM transactions"},
		{"AVG with FILTER and GROUP BY", "SELECT department, AVG(salary) FILTER (WHERE experience > 5) FROM employees GROUP BY department"},
		{"Multiple aggregates with FILTER", "SELECT COUNT(*) FILTER (WHERE active), SUM(value) FILTER (WHERE type = 'sale') FROM records"},
		{"FILTER with complex condition", "SELECT MAX(price) FILTER (WHERE category = 'electronics' AND in_stock = true) FROM products"},

		// WITHIN GROUP clause tests
		{"percentile_cont with WITHIN GROUP", "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY salary) FROM employees"},
		{"mode with WITHIN GROUP", "SELECT mode() WITHIN GROUP (ORDER BY category) FROM products"},
		{"string_agg with WITHIN GROUP", "SELECT string_agg(name, ', ') WITHIN GROUP (ORDER BY name) FROM users"},
		{"WITHIN GROUP with DESC", "SELECT percentile_disc(0.9) WITHIN GROUP (ORDER BY score DESC) FROM tests"},

		// Combined FILTER and WITHIN GROUP
		{"Aggregate with both FILTER and WITHIN GROUP", "SELECT percentile_cont(0.9) WITHIN GROUP (ORDER BY score) FILTER (WHERE status = 'completed') FROM tests"},

		// With window functions
		{"Aggregate with FILTER and OVER", "SELECT SUM(amount) FILTER (WHERE type = 'credit') OVER (PARTITION BY account_id) FROM transactions"},
		{"COUNT FILTER with window", "SELECT COUNT(*) FILTER (WHERE active = true) OVER (ORDER BY created_at) FROM users"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := ParseSQL(tt.sql)
			require.NoError(t, err, "Failed to parse SQL: %s", tt.sql)
			require.NotEmpty(t, stmts, "No statements parsed")

			stmt := stmts[0]
			require.IsType(t, &ast.SelectStmt{}, stmt, "Expected SelectStmt")
		})
	}
}

// TestLockingClauses tests FOR UPDATE/SHARE locking clauses
func TestLockingClauses(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		// Basic FOR UPDATE
		{"Simple FOR UPDATE", "SELECT * FROM users FOR UPDATE"},
		{"FOR UPDATE with specific tables", "SELECT * FROM users u, orders o WHERE u.id = o.user_id FOR UPDATE OF u"},
		{"FOR UPDATE NOWAIT", "SELECT * FROM accounts WHERE balance > 1000 FOR UPDATE NOWAIT"},
		{"FOR UPDATE SKIP LOCKED", "SELECT * FROM queue WHERE processed = false FOR UPDATE SKIP LOCKED"},

		// FOR NO KEY UPDATE
		{"FOR NO KEY UPDATE", "SELECT * FROM settings FOR NO KEY UPDATE"},
		{"FOR NO KEY UPDATE SKIP LOCKED", "SELECT * FROM tasks FOR NO KEY UPDATE SKIP LOCKED"},
		{"FOR NO KEY UPDATE with specific table", "SELECT * FROM users u, posts p WHERE u.id = p.user_id FOR NO KEY UPDATE OF u"},

		// FOR SHARE
		{"Simple FOR SHARE", "SELECT * FROM products FOR SHARE"},
		{"FOR SHARE with multiple tables", "SELECT * FROM orders o, items i WHERE o.id = i.order_id FOR SHARE OF o, i"},
		{"FOR SHARE NOWAIT", "SELECT * FROM inventory FOR SHARE NOWAIT"},

		// FOR KEY SHARE
		{"FOR KEY SHARE", "SELECT * FROM categories FOR KEY SHARE"},
		{"FOR KEY SHARE NOWAIT", "SELECT * FROM configs FOR KEY SHARE NOWAIT"},
		{"FOR KEY SHARE SKIP LOCKED", "SELECT * FROM jobs FOR KEY SHARE SKIP LOCKED"},

		// Multiple locking clauses
		{"Multiple locking clauses", "SELECT * FROM users u, accounts a WHERE u.id = a.user_id FOR UPDATE OF u FOR SHARE OF a"},
		{"Mixed locking strengths", "SELECT * FROM t1, t2, t3 WHERE t1.id = t2.ref_id AND t2.id = t3.ref_id FOR UPDATE OF t1 FOR NO KEY UPDATE OF t2 FOR SHARE OF t3"},

		// FOR READ ONLY
		{"FOR READ ONLY", "SELECT * FROM logs FOR READ ONLY"},

		// With other clauses
		{"Locking with ORDER BY and LIMIT", "SELECT * FROM queue ORDER BY priority DESC LIMIT 10 FOR UPDATE SKIP LOCKED"},
		{"Locking with GROUP BY", "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id FOR UPDATE"},
		{"Locking with subquery", "SELECT * FROM (SELECT * FROM users WHERE active = true) AS u FOR UPDATE"},
		{"Locking with JOIN", "SELECT * FROM users u JOIN accounts a ON u.id = a.user_id FOR UPDATE OF u"},
		{"Locking with CTE", "WITH active_users AS (SELECT * FROM users WHERE active = true) SELECT * FROM active_users FOR UPDATE"},

		// Complex combinations
		{"Aggregate with FILTER and locking", "SELECT user_id, COUNT(*) FILTER (WHERE status = 'pending') FROM orders GROUP BY user_id FOR UPDATE"},
		{"WITHIN GROUP with locking", "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY amount) FROM transactions FOR SHARE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := ParseSQL(tt.sql)
			require.NoError(t, err, "Failed to parse SQL: %s", tt.sql)
			require.NotEmpty(t, stmts, "No statements parsed")

			stmt := stmts[0]
			require.IsType(t, &ast.SelectStmt{}, stmt, "Expected SelectStmt")
		})
	}
}

// TestINTOClause tests the INTO clause parsing for SELECT statements
func TestINTOClause(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected *ast.IntoClause
	}{
		{
			name: "Simple INTO",
			sql:  "SELECT * INTO new_table FROM users",
			expected: &ast.IntoClause{
				Rel: &ast.RangeVar{
					RelName: "new_table",
				},
			},
		},
		{
			name: "INTO TEMPORARY table",
			sql:  "SELECT * INTO TEMPORARY temp_users FROM users",
			expected: &ast.IntoClause{
				Rel: &ast.RangeVar{
					RelName:        "temp_users",
					RelPersistence: ast.RELPERSISTENCE_TEMP,
				},
			},
		},
		{
			name: "INTO TEMP table",
			sql:  "SELECT * INTO TEMP temp_users FROM users",
			expected: &ast.IntoClause{
				Rel: &ast.RangeVar{
					RelName:        "temp_users",
					RelPersistence: ast.RELPERSISTENCE_TEMP,
				},
			},
		},
		{
			name: "INTO LOCAL TEMPORARY table",
			sql:  "SELECT * INTO LOCAL TEMPORARY local_temp_users FROM users",
			expected: &ast.IntoClause{
				Rel: &ast.RangeVar{
					RelName:        "local_temp_users",
					RelPersistence: ast.RELPERSISTENCE_TEMP,
				},
			},
		},
		{
			name: "INTO UNLOGGED table",
			sql:  "SELECT * INTO UNLOGGED unlogged_users FROM users",
			expected: &ast.IntoClause{
				Rel: &ast.RangeVar{
					RelName:        "unlogged_users",
					RelPersistence: ast.RELPERSISTENCE_UNLOGGED,
				},
			},
		},
		{
			name: "INTO TABLE explicit",
			sql:  "SELECT * INTO TABLE explicit_table FROM users",
			expected: &ast.IntoClause{
				Rel: &ast.RangeVar{
					RelName:        "explicit_table",
					RelPersistence: ast.RELPERSISTENCE_PERMANENT,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := ParseSQL(tt.sql)
			require.NoError(t, err)
			require.Len(t, stmts, 1)

			selectStmt, ok := stmts[0].(*ast.SelectStmt)
			require.True(t, ok, "Expected SelectStmt, got %T", stmts[0])
			require.NotNil(t, selectStmt.IntoClause)

			// Check basic INTO clause structure
			assert.NotNil(t, selectStmt.IntoClause.Rel)
			assert.Equal(t, tt.expected.Rel.RelName, selectStmt.IntoClause.Rel.RelName)

			// Check persistence if specified in expected
			if tt.expected.Rel.RelPersistence != 0 {
				assert.Equal(t, tt.expected.Rel.RelPersistence, selectStmt.IntoClause.Rel.RelPersistence)
			}
		})
	}
}

// TestJSONAggregateFunctions tests JSON aggregate function parsing
func TestJSONAggregateFunctions(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		nodeType ast.NodeTag
	}{
		{
			name:     "JSON_OBJECTAGG simple",
			sql:      "SELECT JSON_OBJECTAGG('name' VALUE value) FROM users",
			nodeType: ast.T_JsonObjectAgg,
		},
		{
			name:     "JSON_ARRAYAGG simple",
			sql:      "SELECT JSON_ARRAYAGG(name) FROM users",
			nodeType: ast.T_JsonArrayAgg,
		},
		{
			name:     "JSON_OBJECTAGG with RETURNING",
			sql:      "SELECT JSON_OBJECTAGG('key' VALUE value RETURNING TEXT) FROM users",
			nodeType: ast.T_JsonObjectAgg,
		},
		{
			name:     "JSON_ARRAYAGG with RETURNING",
			sql:      "SELECT JSON_ARRAYAGG(name RETURNING JSONB) FROM users",
			nodeType: ast.T_JsonArrayAgg,
		},
		{
			name:     "JSON_OBJECTAGG with FILTER",
			sql:      "SELECT JSON_OBJECTAGG('key' VALUE value) FILTER (WHERE value IS NOT NULL) FROM users",
			nodeType: ast.T_JsonObjectAgg,
		},
		{
			name:     "JSON_ARRAYAGG with OVER window",
			sql:      "SELECT JSON_ARRAYAGG(name) OVER (PARTITION BY department) FROM users",
			nodeType: ast.T_JsonArrayAgg,
		},
		{
			name:     "JSON_OBJECTAGG with FILTER and OVER",
			sql:      "SELECT JSON_OBJECTAGG('key' VALUE value RETURNING TEXT) FILTER (WHERE value > 0) OVER (ORDER BY id) FROM users",
			nodeType: ast.T_JsonObjectAgg,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := ParseSQL(tt.sql)
			require.NoError(t, err)
			require.Len(t, stmts, 1)

			selectStmt, ok := stmts[0].(*ast.SelectStmt)
			require.True(t, ok, "Expected SelectStmt, got %T", stmts[0])
			require.NotNil(t, selectStmt.TargetList)
			require.Len(t, selectStmt.TargetList.Items, 1)

			target := selectStmt.TargetList.Items[0].(*ast.ResTarget)
			require.NotNil(t, target.Val)

			// For JSON aggregate functions, we expect them to be expression nodes
			expr := target.Val
			require.NotNil(t, expr)

			// Check that the node type matches expectations
			switch tt.nodeType {
			case ast.T_JsonObjectAgg:
				jsonObj, ok := expr.(*ast.JsonObjectAgg)
				assert.True(t, ok, "Expected JsonObjectAgg, got %T", expr)

				// For FILTER and OVER test cases, check Constructor fields
				if strings.Contains(tt.sql, "FILTER") || strings.Contains(tt.sql, "OVER") {
					assert.NotNil(t, jsonObj.Constructor, "Constructor should be set for FILTER/OVER clauses")
					if strings.Contains(tt.sql, "FILTER") {
						assert.NotNil(t, jsonObj.Constructor.AggFilter, "AggFilter should be set")
					}
					if strings.Contains(tt.sql, "OVER") {
						assert.NotNil(t, jsonObj.Constructor.Over, "Over should be set")
					}
				}

			case ast.T_JsonArrayAgg:
				jsonArr, ok := expr.(*ast.JsonArrayAgg)
				assert.True(t, ok, "Expected JsonArrayAgg, got %T", expr)

				// For FILTER and OVER test cases, check Constructor fields
				if strings.Contains(tt.sql, "FILTER") || strings.Contains(tt.sql, "OVER") {
					assert.NotNil(t, jsonArr.Constructor, "Constructor should be set for FILTER/OVER clauses")
					if strings.Contains(tt.sql, "FILTER") {
						assert.NotNil(t, jsonArr.Constructor.AggFilter, "AggFilter should be set")
					}
					if strings.Contains(tt.sql, "OVER") {
						assert.NotNil(t, jsonArr.Constructor.Over, "Over should be set")
					}
				}
			}
		})
	}
}
