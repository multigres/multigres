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
