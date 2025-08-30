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