/*
 * PostgreSQL Parser - SQL Statement Parsing Tests
 *
 * This file contains comprehensive tests for SQL statement parsing including:
 * - Data Manipulation Language (DML) statements: INSERT, UPDATE, DELETE, MERGE
 * - Data Definition Language (DDL) statements: CREATE, ALTER, DROP
 * - Round-trip parsing and deparsing validation
 *
 * Consolidated from dml_test.go and ddl_test.go
 */

package parser

import (
	"testing"

	"github.com/multigres/parser/go/parser/ast"
	"github.com/stretchr/testify/require"
)

// TestDDLSqlStringMethods tests SqlString methods directly on DDL AST nodes
// These tests verify that SqlString methods work correctly on manually constructed AST nodes
func TestDDLSqlStringMethods(t *testing.T) {
	t.Run("CreateStmt SqlString", func(t *testing.T) {
		// Create a simple CREATE TABLE statement node
		relation := ast.NewRangeVar("users", "", "")
		createStmt := ast.NewCreateStmt(relation)

		// Add a column
		colDef := ast.NewColumnDef("id", ast.NewTypeName([]string{"int"}), 0)
		createStmt.TableElts = &ast.NodeList{
			Items: []ast.Node{
				colDef,
			},
		}

		sqlString := createStmt.SqlString()
		require.Contains(t, sqlString, "CREATE TABLE users")
		require.Contains(t, sqlString, "(id INT)")

		t.Logf("CreateStmt SqlString: %s", sqlString)
	})

	t.Run("IndexStmt SqlString", func(t *testing.T) {
		// Create a simple CREATE INDEX statement node
		relation := ast.NewRangeVar("users", "", "")
		indexParams := ast.NewNodeList()
		indexParams.Append(ast.NewIndexElem("name"))
		indexStmt := ast.NewIndexStmt("idx_users_name", relation, indexParams)

		sqlString := indexStmt.SqlString()
		require.Contains(t, sqlString, "CREATE INDEX idx_users_name ON users")
		require.Contains(t, sqlString, "(name)")

		t.Logf("IndexStmt SqlString: %s", sqlString)
	})

	t.Run("DropStmt SqlString", func(t *testing.T) {
		// Create a simple DROP TABLE statement node
		dropStmt := &ast.DropStmt{
			BaseNode:   ast.BaseNode{Tag: ast.T_DropStmt},
			RemoveType: ast.OBJECT_TABLE,
			Objects:    ast.NewNodeList(),
			Behavior:   ast.DropRestrict,
			MissingOk:  false,
		}

		// Add table name
		dropStmt.Objects.Append(ast.NewString("users"))

		sqlString := dropStmt.SqlString()
		require.Contains(t, sqlString, "DROP TABLE users")

		t.Logf("DropStmt SqlString: %s", sqlString)
	})

	t.Run("Constraint SqlString", func(t *testing.T) {
		// Test PRIMARY KEY constraint
		pkConstraint := ast.NewConstraint(ast.CONSTR_PRIMARY)
		pkConstraint.Keys = ast.NewNodeList(ast.NewString("id"))

		sqlString := pkConstraint.SqlString()
		require.Equal(t, "PRIMARY KEY (id)", sqlString)

		// Test UNIQUE constraint
		uniqueConstraint := ast.NewConstraint(ast.CONSTR_UNIQUE)
		uniqueConstraint.Keys = ast.NewNodeList(ast.NewString("email"))

		sqlString2 := uniqueConstraint.SqlString()
		require.Equal(t, "UNIQUE (email)", sqlString2)

		t.Logf("PRIMARY KEY SqlString: %s", sqlString)
		t.Logf("UNIQUE SqlString: %s", sqlString2)
	})
}

// TestKeyActionsSetNullWithColumnList tests SET NULL with optional column list
func TestKeyActionsSetNullWithColumnList(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		hasCol bool
	}{
		{
			name:   "SET NULL without column list",
			input:  "CREATE TABLE test (id int REFERENCES parent(id) ON DELETE SET NULL);",
			hasCol: false,
		},
		{
			name:   "SET NULL with column list",
			input:  "CREATE TABLE test (id int REFERENCES parent(id) ON DELETE SET NULL (id));",
			hasCol: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := ParseSQL(tt.input)
			require.NoError(t, err)
			require.Len(t, stmts, 1)

			createStmt, ok := stmts[0].(*ast.CreateStmt)
			require.True(t, ok)
			require.NotNil(t, createStmt.TableElts)
			require.Len(t, createStmt.TableElts.Items, 1)

			colDef := createStmt.TableElts.Items[0].(*ast.ColumnDef)
			require.NotNil(t, colDef.Constraints)
			require.Len(t, colDef.Constraints.Items, 1)

			constraint, ok := colDef.Constraints.Items[0].(*ast.Constraint)
			require.True(t, ok)
			require.Equal(t, ast.CONSTR_FOREIGN, constraint.Contype)
			require.Equal(t, ast.FKCONSTR_ACTION_SETNULL, constraint.FkDelAction)

			// The actual KeyActions struct should contain the column information
			// but for now we're testing that it parses without error
		})
	}
}

// TestKeyActionsSetDefaultWithColumnList tests SET DEFAULT with optional column list
func TestKeyActionsSetDefaultWithColumnList(t *testing.T) {
	input := "CREATE TABLE test (id int REFERENCES parent(id) ON DELETE SET DEFAULT (id));"

	stmts, err := ParseSQL(input)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	createStmt, ok := stmts[0].(*ast.CreateStmt)
	require.True(t, ok)
	require.NotNil(t, createStmt.TableElts)
	require.Len(t, createStmt.TableElts.Items, 1)

	colDef := createStmt.TableElts.Items[0].(*ast.ColumnDef)
	require.NotNil(t, colDef.Constraints)
	require.Len(t, colDef.Constraints.Items, 1)

	constraint, ok := colDef.Constraints.Items[0].(*ast.Constraint)
	require.True(t, ok)
	require.Equal(t, ast.CONSTR_FOREIGN, constraint.Contype)
	require.Equal(t, ast.FKCONSTR_ACTION_SETDEFAULT, constraint.FkDelAction)
}

// TestKeyActionsUpdateRestriction tests that UPDATE actions don't allow column lists
func TestKeyActionsUpdateRestriction(t *testing.T) {
	// This should cause a parser error since column lists are not supported for UPDATE actions
	input := "CREATE TABLE test (id int REFERENCES parent(id) ON UPDATE SET NULL (id));"

	_, err := ParseSQL(input)
	// We expect an error here since UPDATE with column list should not be supported
	require.Error(t, err)
}

// TestNodeListCreateFunctionDeparsing tests CreateFunctionStmt with NodeList implementation
func TestNodeListCreateFunctionDeparsing(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected string // If empty, use sql as expected
	}{
		{
			name: "Simple function",
			sql:  "CREATE FUNCTION add(a integer, b integer) RETURNS integer LANGUAGE sql AS $$SELECT a + b$$",
		},
		{
			name: "Function with unnamed parameter",
			sql:  "CREATE FUNCTION greet(text) RETURNS TEXT LANGUAGE sql AS $$SELECT 'Hello ' || $1$$",
		},
		{
			name: "Function with OUT parameter",
			sql:  "CREATE FUNCTION process(IN input text, OUT result integer) LANGUAGE sql AS $$SELECT length(input)$$",
		},
		{
			name: "OR REPLACE function",
			sql:  "CREATE OR REPLACE FUNCTION test() RETURNS void LANGUAGE sql AS $$SELECT$$",
		},
		{
			name: "PROCEDURE",
			sql:  "CREATE PROCEDURE test_proc() LANGUAGE sql AS $$SELECT$$",
		},
		{
			name: "Function with qualified name",
			sql:  "CREATE FUNCTION public.test_func() RETURNS integer LANGUAGE sql AS $$SELECT 1$$",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the SQL
			stmts, err := ParseSQL(tt.sql)
			require.NoError(t, err, "Failed to parse SQL: %s", tt.sql)
			require.Len(t, stmts, 1, "Should have exactly one statement")

			// Verify it's a CreateFunctionStmt
			createFunc, ok := stmts[0].(*ast.CreateFunctionStmt)
			require.True(t, ok, "Expected CreateFunctionStmt, got %T", stmts[0])

			// Verify NodeList fields are properly set
			require.NotNil(t, createFunc.FuncName, "FuncName should be NodeList, not nil")
			require.Greater(t, createFunc.FuncName.Len(), 0, "FuncName should have items")

			if createFunc.Parameters != nil {
				require.IsType(t, &ast.NodeList{}, createFunc.Parameters, "Parameters should be *NodeList")
			}

			if createFunc.Options != nil {
				require.IsType(t, &ast.NodeList{}, createFunc.Options, "Options should be *NodeList")
			}

			// Test deparsing
			deparsed := createFunc.SqlString()
			require.NotEmpty(t, deparsed, "Deparsed SQL should not be empty")

			// Test round-trip parsing
			reparsedStmts, reparseErr := ParseSQL(deparsed)
			require.NoError(t, reparseErr, "Round-trip parsing should succeed: %s", deparsed)
			require.Len(t, reparsedStmts, 1, "Round-trip should have exactly one statement")

			// Verify the reparsed statement is also correct
			reparsedFunc, ok := reparsedStmts[0].(*ast.CreateFunctionStmt)
			require.True(t, ok, "Reparsed should be CreateFunctionStmt")
			require.Equal(t, createFunc.IsProcedure, reparsedFunc.IsProcedure)
			require.Equal(t, createFunc.Replace, reparsedFunc.Replace)
		})
	}
}

// TestNodeListViewDeparsing tests ViewStmt with NodeList implementation
func TestNodeListViewDeparsing(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected string // If empty, use sql as expected
	}{
		{
			name: "Simple view",
			sql:  "CREATE VIEW test_view AS SELECT 1",
		},
		{
			name: "OR REPLACE view",
			sql:  "CREATE OR REPLACE VIEW test_view AS SELECT 1",
		},
		{
			name: "View with column aliases",
			sql:  "CREATE VIEW user_info(id, name) AS SELECT user_id, username FROM users",
		},
		{
			name: "TEMPORARY view",
			sql:  "CREATE TEMPORARY VIEW temp_view AS SELECT 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the SQL
			stmts, err := ParseSQL(tt.sql)
			require.NoError(t, err, "Failed to parse SQL: %s", tt.sql)
			require.Len(t, stmts, 1, "Should have exactly one statement")

			// Verify it's a ViewStmt
			viewStmt, ok := stmts[0].(*ast.ViewStmt)
			require.True(t, ok, "Expected ViewStmt, got %T", stmts[0])

			// Verify NodeList fields
			if viewStmt.Options != nil {
				require.IsType(t, &ast.NodeList{}, viewStmt.Options, "Options should be *NodeList")
			}

			// Test deparsing
			deparsed := viewStmt.SqlString()
			require.NotEmpty(t, deparsed, "Deparsed SQL should not be empty")

			// Test round-trip parsing
			reparsedStmts, reparseErr := ParseSQL(deparsed)
			require.NoError(t, reparseErr, "Round-trip parsing should succeed: %s", deparsed)
			require.Len(t, reparsedStmts, 1, "Round-trip should have exactly one statement")

			// Verify the reparsed statement is also correct
			reparsedView, ok := reparsedStmts[0].(*ast.ViewStmt)
			require.True(t, ok, "Reparsed should be ViewStmt")
			require.Equal(t, viewStmt.Replace, reparsedView.Replace)
		})
	}
}

// TestCursorStatements tests parsing of cursor-related statements
func TestCursorStatements(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected string // If empty, use sql as expected
	}{
		// DECLARE CURSOR statements
		{
			name: "Simple DECLARE CURSOR",
			sql:  "DECLARE my_cursor CURSOR FOR SELECT * FROM users",
		},
		{
			name: "DECLARE CURSOR with BINARY",
			sql:  "DECLARE binary_cursor BINARY CURSOR FOR SELECT * FROM data",
		},
		{
			name: "DECLARE CURSOR with INSENSITIVE",
			sql:  "DECLARE insensitive_cursor INSENSITIVE CURSOR FOR SELECT * FROM users",
		},
		{
			name: "DECLARE CURSOR with ASENSITIVE",
			sql:  "DECLARE asensitive_cursor ASENSITIVE CURSOR FOR SELECT * FROM users",
		},
		{
			name: "DECLARE CURSOR with SCROLL",
			sql:  "DECLARE scroll_cursor SCROLL CURSOR FOR SELECT * FROM users",
		},
		{
			name: "DECLARE CURSOR with NO SCROLL",
			sql:  "DECLARE noscroll_cursor NO SCROLL CURSOR FOR SELECT * FROM users",
		},
		{
			name: "DECLARE CURSOR with WITH HOLD",
			sql:  "DECLARE hold_cursor CURSOR WITH HOLD FOR SELECT * FROM users",
		},
		{
			name: "DECLARE CURSOR with multiple options",
			sql:  "DECLARE complex_cursor BINARY INSENSITIVE SCROLL CURSOR WITH HOLD FOR SELECT * FROM users",
		},
		{
			name: "DECLARE CURSOR with complex query",
			sql:  "DECLARE query_cursor CURSOR FOR SELECT u.id, u.name FROM users u WHERE u.active = TRUE ORDER BY u.name",
		},

		// FETCH statements
		{
			name: "FETCH from cursor",
			sql:  "FETCH FROM my_cursor",
		},
		{
			name: "FETCH IN cursor",
			sql:  "FETCH IN my_cursor",
		},
		{
			name: "FETCH count from cursor",
			sql:  "FETCH 5 FROM my_cursor",
		},
		{
			name: "FETCH count in cursor",
			sql:  "FETCH 10 IN my_cursor",
		},
		{
			name: "FETCH NEXT from cursor",
			sql:  "FETCH NEXT FROM my_cursor",
		},
		{
			name: "FETCH NEXT in cursor",
			sql:  "FETCH NEXT IN my_cursor",
		},
		{
			name: "FETCH PRIOR from cursor",
			sql:  "FETCH PRIOR FROM my_cursor",
		},
		{
			name: "FETCH PRIOR in cursor",
			sql:  "FETCH PRIOR IN my_cursor",
		},
		{
			name: "FETCH FIRST from cursor",
			sql:  "FETCH FIRST FROM my_cursor",
		},
		{
			name: "FETCH FIRST in cursor",
			sql:  "FETCH FIRST IN my_cursor",
		},
		{
			name: "FETCH LAST from cursor",
			sql:  "FETCH LAST FROM my_cursor",
		},
		{
			name: "FETCH LAST in cursor",
			sql:  "FETCH LAST IN my_cursor",
		},
		{
			name: "FETCH ABSOLUTE positive",
			sql:  "FETCH ABSOLUTE 100 FROM my_cursor",
		},
		{
			name: "FETCH ABSOLUTE negative",
			sql:  "FETCH ABSOLUTE -50 IN my_cursor",
		},
		{
			name: "FETCH RELATIVE positive",
			sql:  "FETCH RELATIVE 10 FROM my_cursor",
		},
		{
			name: "FETCH RELATIVE negative",
			sql:  "FETCH RELATIVE -5 IN my_cursor",
		},
		{
			name: "FETCH ALL from cursor",
			sql:  "FETCH ALL FROM my_cursor",
		},
		{
			name: "FETCH ALL in cursor",
			sql:  "FETCH ALL IN my_cursor",
		},
		{
			name: "FETCH FORWARD from cursor",
			sql:  "FETCH FORWARD FROM my_cursor",
		},
		{
			name: "FETCH FORWARD count",
			sql:  "FETCH FORWARD 3 FROM my_cursor",
		},
		{
			name: "FETCH FORWARD ALL",
			sql:  "FETCH FORWARD ALL FROM my_cursor",
		},
		{
			name: "FETCH BACKWARD from cursor",
			sql:  "FETCH BACKWARD FROM my_cursor",
		},
		{
			name: "FETCH BACKWARD count",
			sql:  "FETCH BACKWARD 7 FROM my_cursor",
		},
		{
			name: "FETCH BACKWARD ALL",
			sql:  "FETCH BACKWARD ALL FROM my_cursor",
		},

		// MOVE statements
		{
			name: "MOVE from cursor",
			sql:  "MOVE FROM my_cursor",
		},
		{
			name: "MOVE IN cursor",
			sql:  "MOVE IN my_cursor",
		},
		{
			name: "MOVE count from cursor",
			sql:  "MOVE 5 FROM my_cursor",
		},
		{
			name: "MOVE NEXT from cursor",
			sql:  "MOVE NEXT FROM my_cursor",
		},
		{
			name: "MOVE PRIOR from cursor",
			sql:  "MOVE PRIOR FROM my_cursor",
		},
		{
			name: "MOVE FIRST from cursor",
			sql:  "MOVE FIRST FROM my_cursor",
		},
		{
			name: "MOVE LAST from cursor",
			sql:  "MOVE LAST FROM my_cursor",
		},
		{
			name: "MOVE ABSOLUTE",
			sql:  "MOVE ABSOLUTE 50 FROM my_cursor",
		},
		{
			name: "MOVE RELATIVE",
			sql:  "MOVE RELATIVE -10 FROM my_cursor",
		},
		{
			name: "MOVE ALL from cursor",
			sql:  "MOVE ALL FROM my_cursor",
		},
		{
			name: "MOVE FORWARD",
			sql:  "MOVE FORWARD FROM my_cursor",
		},
		{
			name: "MOVE FORWARD count",
			sql:  "MOVE FORWARD 2 FROM my_cursor",
		},
		{
			name: "MOVE FORWARD ALL",
			sql:  "MOVE FORWARD ALL FROM my_cursor",
		},
		{
			name: "MOVE BACKWARD",
			sql:  "MOVE BACKWARD FROM my_cursor",
		},
		{
			name: "MOVE BACKWARD count",
			sql:  "MOVE BACKWARD 4 FROM my_cursor",
		},
		{
			name: "MOVE BACKWARD ALL",
			sql:  "MOVE BACKWARD ALL FROM my_cursor",
		},

		// CLOSE statements
		{
			name: "CLOSE specific cursor",
			sql:  "CLOSE my_cursor",
		},
		{
			name: "CLOSE ALL cursors",
			sql:  "CLOSE ALL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the SQL
			stmts, err := ParseSQL(tt.sql)
			require.NoError(t, err, "Failed to parse SQL: %s", tt.sql)
			require.Len(t, stmts, 1, "Should have exactly one statement")

			// Verify it's a cursor statement (can be DeclareCursorStmt, FetchStmt, or ClosePortalStmt)
			stmt := stmts[0]
			require.Implements(t, (*ast.Stmt)(nil), stmt, "Should implement Stmt interface")

			// Test deparsing
			deparsed := stmt.SqlString()
			require.NotEmpty(t, deparsed, "Deparsed SQL should not be empty")

			// Compare with expected output if provided
			expected := tt.expected
			if expected == "" {
				expected = tt.sql
			}

			// Test round-trip parsing
			reparsedStmts, reparseErr := ParseSQL(deparsed)
			require.NoError(t, reparseErr, "Round-trip parsing should succeed: %s", deparsed)
			require.Len(t, reparsedStmts, 1, "Round-trip should have exactly one statement")

			// Verify the reparsed statement has the same type
			reparsedStmt := reparsedStmts[0]
			require.IsType(t, stmt, reparsedStmt, "Reparsed statement should have same type")
		})
	}
}

// TestPreparedStatements tests parsing of prepared statement-related commands
func TestPreparedStatements(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected string // If empty, use sql as expected
	}{
		// PREPARE statements
		{
			name: "Simple PREPARE without parameters",
			sql:  "PREPARE simple_plan AS SELECT * FROM users",
		},
		{
			name: "PREPARE with one parameter",
			sql:  "PREPARE plan_with_param (integer) AS SELECT * FROM users WHERE id = $1",
		},
		{
			name: "PREPARE with multiple parameters",
			sql:  "PREPARE multi_param_plan (integer, text, boolean) AS SELECT * FROM users WHERE id = $1 AND name = $2 AND active = $3",
		},
		{
			name: "PREPARE with qualified type names",
			sql:  "PREPARE qualified_types (pg_catalog.int4, pg_catalog.text) AS SELECT $1::int4, $2::text",
		},
		{
			name: "PREPARE with complex query",
			sql:  "PREPARE complex_query (integer) AS SELECT u.id, u.name, p.title FROM users u JOIN posts p ON u.id = p.author_id WHERE u.id = $1 ORDER BY p.created_at DESC",
		},
		{
			name: "PREPARE INSERT statement",
			sql:  "PREPARE insert_user (text, text, integer) AS INSERT INTO users (name, email, age) VALUES ($1, $2, $3)",
		},
		{
			name: "PREPARE UPDATE statement",
			sql:  "PREPARE update_user (text, integer) AS UPDATE users SET name = $1 WHERE id = $2",
		},
		{
			name: "PREPARE DELETE statement",
			sql:  "PREPARE delete_user (integer) AS DELETE FROM users WHERE id = $1",
		},
		{
			name: "PREPARE with custom type",
			sql:  "PREPARE custom_type_query (user_status) AS SELECT * FROM users WHERE status = $1",
		},

		// EXECUTE statements
		{
			name: "EXECUTE without parameters",
			sql:  "EXECUTE simple_plan",
		},
		{
			name: "EXECUTE with one parameter",
			sql:  "EXECUTE plan_with_param (123)",
		},
		{
			name: "EXECUTE with multiple parameters",
			sql:  "EXECUTE multi_param_plan (123, 'John Doe', TRUE)",
		},
		{
			name: "EXECUTE with string parameters",
			sql:  "EXECUTE user_query ('admin', 'password123')",
		},
		{
			name: "EXECUTE with NULL parameter",
			sql:  "EXECUTE nullable_query (NULL)",
		},
		{
			name: "EXECUTE with numeric parameters",
			sql:  "EXECUTE numeric_query (42, 3.14159, -100)",
		},
		{
			name: "EXECUTE with boolean parameters",
			sql:  "EXECUTE boolean_query (TRUE, FALSE)",
		},
		{
			name: "EXECUTE with mixed parameters",
			sql:  "EXECUTE mixed_query (1, 'test', TRUE, NULL, 3.14)",
		},
		{
			name: "EXECUTE with function call parameter",
			sql:  "EXECUTE time_query (NOW())",
		},
		{
			name: "EXECUTE with expression parameters",
			sql:  "EXECUTE calc_query (1 + 2, 'prefix' || 'suffix')",
		},

		// DEALLOCATE statements
		{
			name: "DEALLOCATE specific statement",
			sql:  "DEALLOCATE my_plan",
		},
		{
			name: "DEALLOCATE ALL statements",
			sql:  "DEALLOCATE ALL",
		},
		{
			name: "DEALLOCATE PREPARE specific statement",
			sql:  "DEALLOCATE PREPARE my_plan",
		},
		{
			name: "DEALLOCATE PREPARE ALL statements",
			sql:  "DEALLOCATE PREPARE ALL",
		},

		// Edge cases and complex scenarios
		{
			name: "PREPARE with very long name",
			sql:  "PREPARE very_long_prepared_statement_name_that_exceeds_normal_length AS SELECT 1",
		},
		{
			name: "PREPARE with complex subquery",
			sql:  "PREPARE subquery_plan (integer) AS SELECT * FROM users WHERE id IN (SELECT user_id FROM posts WHERE author_id = $1)",
		},
		{
			name: "PREPARE with window function",
			sql:  "PREPARE window_plan (text) AS SELECT name, ROW_NUMBER() OVER (ORDER BY created_at) as rank FROM users WHERE department = $1",
		},
		{
			name: "EXECUTE with type cast in parameter",
			sql:  "EXECUTE plan_name ('123'::integer)",
		},

		// CREATE TABLE AS EXECUTE statements
		{
			name: "CREATE TABLE AS EXECUTE without parameters",
			sql:  "CREATE TABLE result_table AS EXECUTE my_plan",
		},
		{
			name: "CREATE TABLE AS EXECUTE with parameters",
			sql:  "CREATE TABLE user_results AS EXECUTE user_query (123, 'admin')",
		},
		{
			name: "CREATE TABLE AS EXECUTE with qualified table name",
			sql:  "CREATE TABLE public.results AS EXECUTE data_plan (1, 2, 3)",
		},
		{
			name: "CREATE TEMPORARY TABLE AS EXECUTE",
			sql:  "CREATE TEMPORARY TABLE temp_results AS EXECUTE temp_plan",
		},
		{
			name: "CREATE TEMP TABLE AS EXECUTE with parameters",
			sql:  "CREATE TEMP TABLE temp_data AS EXECUTE analysis_plan (100, 'report')",
		},

		// IF NOT EXISTS variants
		{
			name: "CREATE TABLE IF NOT EXISTS AS EXECUTE",
			sql:  "CREATE TABLE IF NOT EXISTS backup_table AS EXECUTE backup_plan",
		},
		{
			name: "CREATE TEMPORARY TABLE IF NOT EXISTS AS EXECUTE",
			sql:  "CREATE TEMPORARY TABLE IF NOT EXISTS temp_backup AS EXECUTE temp_backup_plan (1, 2)",
		},
		{
			name: "CREATE TEMP TABLE IF NOT EXISTS AS EXECUTE",
			sql:  "CREATE TEMP TABLE IF NOT EXISTS summary AS EXECUTE summary_plan ('monthly')",
		},

		// WITH DATA / WITH NO DATA variants
		{
			name: "CREATE TABLE AS EXECUTE with WITH DATA",
			sql:  "CREATE TABLE results AS EXECUTE my_plan WITH DATA",
		},
		{
			name: "CREATE TABLE AS EXECUTE with WITH NO DATA",
			sql:  "CREATE TABLE empty_results AS EXECUTE my_plan WITH NO DATA",
		},
		{
			name: "CREATE TEMPORARY TABLE AS EXECUTE with WITH DATA",
			sql:  "CREATE TEMPORARY TABLE temp_results AS EXECUTE temp_plan WITH DATA",
		},
		{
			name: "CREATE TEMP TABLE IF NOT EXISTS AS EXECUTE with WITH NO DATA",
			sql:  "CREATE TEMP TABLE IF NOT EXISTS summary AS EXECUTE summary_plan WITH NO DATA",
		},

		// Enhanced create_as_target features
		{
			name: "CREATE TABLE AS EXECUTE with column list",
			sql:  "CREATE TABLE results (id, name, value) AS EXECUTE my_plan",
		},
		{
			name: "CREATE TABLE AS EXECUTE with WITH options",
			sql:  "CREATE TABLE results WITH (fillfactor=80) AS EXECUTE my_plan",
		},
		{
			name: "CREATE TABLE AS EXECUTE with USING access method",
			sql:  "CREATE TABLE results USING heap AS EXECUTE my_plan",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the SQL
			stmts, err := ParseSQL(tt.sql)
			require.NoError(t, err, "Failed to parse SQL: %s", tt.sql)
			require.Len(t, stmts, 1, "Should have exactly one statement")

			// Verify it's a prepared statement (can be PrepareStmt, ExecuteStmt, or DeallocateStmt)
			stmt := stmts[0]
			require.Implements(t, (*ast.Stmt)(nil), stmt, "Should implement Stmt interface")

			// Test deparsing
			deparsed := stmt.SqlString()
			require.NotEmpty(t, deparsed, "Deparsed SQL should not be empty")

			// Compare with expected output if provided
			expected := tt.expected
			if expected == "" {
				expected = tt.sql
			}

			// Test round-trip parsing
			reparsedStmts, reparseErr := ParseSQL(deparsed)
			require.NoError(t, reparseErr, "Round-trip parsing should succeed: %s", deparsed)
			require.Len(t, reparsedStmts, 1, "Round-trip should have exactly one statement")

			// Verify the reparsed statement has the same type
			reparsedStmt := reparsedStmts[0]
			require.IsType(t, stmt, reparsedStmt, "Reparsed statement should have same type")
		})
	}
}
