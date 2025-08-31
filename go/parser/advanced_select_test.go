package parser

import (
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
			sql:  `SELECT dept_id, COUNT(*), AVG(salary) 
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
			sql:  `SELECT year, quarter, SUM(sales) 
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
			sql:  `SELECT category, subcategory, COUNT(*), SUM(amount)
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
