// Package ast provides PostgreSQL AST statement node tests.
package ast

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCmdType tests the CmdType enumeration.
func TestCmdType(t *testing.T) {
	tests := []struct {
		cmdType  CmdType
		expected string
	}{
		{CMD_UNKNOWN, "UNKNOWN"},
		{CMD_SELECT, "SELECT"},
		{CMD_UPDATE, "UPDATE"},
		{CMD_INSERT, "INSERT"},
		{CMD_DELETE, "DELETE"},
		{CMD_MERGE, "MERGE"},
		{CMD_UTILITY, "UTILITY"},
		{CMD_NOTHING, "NOTHING"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.cmdType.String())
		})
	}
}

// TestQuery tests the core Query structure.
func TestQuery(t *testing.T) {
	t.Run("NewQuery", func(t *testing.T) {
		query := NewQuery(CMD_SELECT)

		require.NotNil(t, query)
		assert.Equal(t, T_Query, query.NodeTag())
		assert.Equal(t, CMD_SELECT, query.CommandType)
		assert.Equal(t, QSRC_ORIGINAL, query.QuerySource)
		assert.Equal(t, "SELECT", query.StatementType())
		assert.Contains(t, query.String(), "SELECT")
	})

	t.Run("QueryFeatureFlags", func(t *testing.T) {
		query := NewQuery(CMD_SELECT)

		// Test default values
		assert.False(t, query.HasAggs)
		assert.False(t, query.HasWindowFuncs)
		assert.False(t, query.HasSubLinks)
		assert.False(t, query.HasRecursive)

		// Test setting flags
		query.HasAggs = true
		query.HasWindowFuncs = true

		assert.True(t, query.HasAggs)
		assert.True(t, query.HasWindowFuncs)
	})
}

// TestRangeVar tests table/relation references.
func TestRangeVar(t *testing.T) {
	t.Run("SimpleTable", func(t *testing.T) {
		rv := NewRangeVar("users", "", "")

		require.NotNil(t, rv)
		assert.Equal(t, T_RangeVar, rv.NodeTag())
		assert.Equal(t, "users", rv.RelName)
		assert.Equal(t, "", rv.SchemaName)
		assert.Equal(t, "", rv.CatalogName)
		assert.Equal(t, "RangeVar", rv.StatementType())
		assert.Contains(t, rv.String(), "users")
	})

	t.Run("SchemaQualified", func(t *testing.T) {
		schema := "public"
		rv := NewRangeVar("users", schema, "")

		require.NotNil(t, rv)
		assert.Equal(t, "users", rv.RelName)
		assert.Equal(t, schema, rv.SchemaName)
		assert.Contains(t, rv.String(), "public.users")
	})

	t.Run("FullyQualified", func(t *testing.T) {
		schema := "public"
		catalog := "mydb"
		rv := NewRangeVar("users", schema, catalog)

		require.NotNil(t, rv)
		assert.Equal(t, "users", rv.RelName)
		assert.Equal(t, schema, rv.SchemaName)
		assert.Equal(t, catalog, rv.CatalogName)
		assert.Contains(t, rv.String(), "mydb.public.users")
	})
}

// TestResTarget tests result target nodes.
func TestResTarget(t *testing.T) {
	t.Run("WithName", func(t *testing.T) {
		name := "user_id"
		val := NewInteger(42)
		rt := NewResTarget(name, val)

		require.NotNil(t, rt)
		assert.Equal(t, T_ResTarget, rt.NodeTag())
		assert.Equal(t, name, rt.Name)
		assert.Equal(t, val, rt.Val)
		assert.Equal(t, "ResTarget", rt.ExpressionType())
		assert.Contains(t, rt.String(), "user_id")
	})

	t.Run("WithoutName", func(t *testing.T) {
		val := NewString("test")
		rt := NewResTarget("", val)

		require.NotNil(t, rt)
		assert.Equal(t, "", rt.Name)
		assert.Equal(t, val, rt.Val)
		assert.Contains(t, rt.String(), "ResTarget")
	})
}

// TestSelectStmt tests SELECT statement structure.
func TestSelectStmt(t *testing.T) {
	stmt := NewSelectStmt()

	require.NotNil(t, stmt)
	assert.Equal(t, T_SelectStmt, stmt.NodeTag())
	assert.Equal(t, "SELECT", stmt.StatementType())
	assert.Contains(t, stmt.String(), "SelectStmt")

	// Test basic structure
	assert.NotNil(t, stmt.TargetList)
	assert.NotNil(t, stmt.FromClause)
	assert.False(t, stmt.All) // Default is false
}

// TestInsertStmt tests INSERT statement structure.
func TestInsertStmt(t *testing.T) {
	relation := NewRangeVar("users", "", "")
	stmt := NewInsertStmt(relation)

	require.NotNil(t, stmt)
	assert.Equal(t, T_InsertStmt, stmt.NodeTag())
	assert.Equal(t, "INSERT", stmt.StatementType())
	assert.Equal(t, relation, stmt.Relation)
	assert.Contains(t, stmt.String(), "InsertStmt")
	assert.Contains(t, stmt.String(), "users")
}

// TestUpdateStmt tests UPDATE statement structure.
func TestUpdateStmt(t *testing.T) {
	relation := NewRangeVar("users", "", "")
	stmt := NewUpdateStmt(relation)

	require.NotNil(t, stmt)
	assert.Equal(t, T_UpdateStmt, stmt.NodeTag())
	assert.Equal(t, "UPDATE", stmt.StatementType())
	assert.Equal(t, relation, stmt.Relation)
	assert.Contains(t, stmt.String(), "UpdateStmt")
	assert.Contains(t, stmt.String(), "users")
}

// TestDeleteStmt tests DELETE statement structure.
func TestDeleteStmt(t *testing.T) {
	relation := NewRangeVar("users", "", "")
	stmt := NewDeleteStmt(relation)

	require.NotNil(t, stmt)
	assert.Equal(t, T_DeleteStmt, stmt.NodeTag())
	assert.Equal(t, "DELETE", stmt.StatementType())
	assert.Equal(t, relation, stmt.Relation)
	assert.Contains(t, stmt.String(), "DeleteStmt")
	assert.Contains(t, stmt.String(), "users")
}

// TestCreateStmt tests CREATE TABLE statement structure.
func TestCreateStmt(t *testing.T) {
	relation := NewRangeVar("users", "", "")
	stmt := NewCreateStmt(relation)

	require.NotNil(t, stmt)
	assert.Equal(t, T_CreateStmt, stmt.NodeTag())
	assert.Equal(t, "CREATE", stmt.StatementType())
	assert.Equal(t, relation, stmt.Relation)
	assert.False(t, stmt.IfNotExists) // Default is false
	assert.Contains(t, stmt.String(), "CreateStmt")
	assert.Contains(t, stmt.String(), "users")

	// Test IF NOT EXISTS flag
	stmt.IfNotExists = true
	assert.True(t, stmt.IfNotExists)
}

// TestDropStmt tests DROP statement structure.
func TestDropStmt(t *testing.T) {
	objects := NewNodeList(NewString("users"))
	stmt := NewDropStmt(objects, OBJECT_TABLE)

	require.NotNil(t, stmt)
	assert.Equal(t, T_DropStmt, stmt.NodeTag())
	assert.Equal(t, "DROP", stmt.StatementType())
	assert.Equal(t, objects, stmt.Objects)
	assert.Equal(t, OBJECT_TABLE, stmt.RemoveType)
	assert.Equal(t, DropRestrict, stmt.Behavior) // Default
	assert.False(t, stmt.MissingOk)               // Default
	assert.False(t, stmt.Concurrent)              // Default
	assert.Contains(t, stmt.String(), "DropStmt")

	// Test CASCADE behavior
	stmt.Behavior = DropCascade
	assert.Equal(t, DropCascade, stmt.Behavior)

	// Test IF EXISTS flag
	stmt.MissingOk = true
	assert.True(t, stmt.MissingOk)
}

// TestColumnRef tests column reference nodes.
func TestColumnRef(t *testing.T) {
	t.Run("SimpleColumn", func(t *testing.T) {
		field := NewString("name")
		colRef := NewColumnRef(field)

		require.NotNil(t, colRef)
		assert.Equal(t, T_ColumnRef, colRef.NodeTag())
		assert.Equal(t, "ColumnRef", colRef.ExpressionType())
		assert.Equal(t, 1, colRef.Fields.Len())
		assert.Equal(t, field, colRef.Fields.Items[0])
		assert.Contains(t, colRef.String(), "1 fields")
	})

	t.Run("QualifiedColumn", func(t *testing.T) {
		table := NewString("users")
		column := NewString("name")
		colRef := NewColumnRef(table, column)

		require.NotNil(t, colRef)
		assert.Equal(t, 2, colRef.Fields.Len())
		assert.Equal(t, table, colRef.Fields.Items[0])
		assert.Equal(t, column, colRef.Fields.Items[1])
		assert.Contains(t, colRef.String(), "2 fields")
	})
}

// TestStmtInterfaces tests that statements implement required interfaces.
func TestStmtInterfaces(t *testing.T) {
	tests := []struct {
		name         string
		stmt         Stmt
		expectedType string
	}{
		{"Query", NewQuery(CMD_SELECT), "SELECT"},
		{"SelectStmt", NewSelectStmt(), "SELECT"},
		{"InsertStmt", NewInsertStmt(NewRangeVar("users", "", "")), "INSERT"},
		{"UpdateStmt", NewUpdateStmt(NewRangeVar("users", "", "")), "UPDATE"},
		{"DeleteStmt", NewDeleteStmt(NewRangeVar("users", "", "")), "DELETE"},
		{"CreateStmt", NewCreateStmt(NewRangeVar("users", "", "")), "CREATE"},
		{"DropStmt", NewDropStmt(NewNodeList(NewString("users")), OBJECT_TABLE), "DROP"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test Stmt interface
			assert.Equal(t, tt.expectedType, tt.stmt.StatementType())

			// Test Node interface
			var node Node = tt.stmt
			assert.NotNil(t, node)
			assert.True(t, int(node.NodeTag()) >= 0)
			assert.NotEmpty(t, node.String())
		})
	}
}

// TestComplexStmtCreation tests building complex statement structures.
func TestComplexStmtCreation(t *testing.T) {
	t.Run("SELECT with targets and WHERE", func(t *testing.T) {
		stmt := NewSelectStmt()

		// Add target list items
		nameTarget := NewResTarget("", NewColumnRef(NewString("name")))
		ageTarget := NewResTarget("", NewColumnRef(NewString("age")))

		stmt.TargetList = []*ResTarget{nameTarget, ageTarget}

		// Add FROM clause
		fromTable := NewRangeVar("users", "", "")
		stmt.FromClause = NewNodeList(fromTable)

		// Add WHERE clause
		stmt.WhereClause = NewColumnRef(NewString("active"))

		// Verify structure
		require.NotNil(t, stmt)
		assert.Len(t, stmt.TargetList, 2)
		assert.Equal(t, 1, stmt.FromClause.Len())
		assert.NotNil(t, stmt.WhereClause)
	})

	t.Run("INSERT with columns and VALUES", func(t *testing.T) {
		relation := NewRangeVar("users", "", "")
		stmt := NewInsertStmt(relation)

		// Add target columns
		nameCol := NewResTarget("", NewString("name"))
		ageCol := NewResTarget("", NewString("age"))
		stmt.Cols = []*ResTarget{nameCol, ageCol}

		// Add VALUES (simulated as a simple node for now)
		stmt.SelectStmt = NewString("VALUES ('John', 30)")

		// Verify structure
		require.NotNil(t, stmt)
		assert.Equal(t, relation, stmt.Relation)
		assert.Len(t, stmt.Cols, 2)
		assert.NotNil(t, stmt.SelectStmt)
	})

	t.Run("UPDATE with SET and WHERE", func(t *testing.T) {
		relation := NewRangeVar("users", "", "")
		stmt := NewUpdateStmt(relation)

		// Add SET clause targets
		setTarget := NewResTarget("", NewColumnRef(NewString("age")))
		stmt.TargetList = []*ResTarget{setTarget}

		// Add WHERE clause
		stmt.WhereClause = NewColumnRef(NewString("id"))

		// Verify structure
		require.NotNil(t, stmt)
		assert.Equal(t, relation, stmt.Relation)
		assert.Len(t, stmt.TargetList, 1)
		assert.NotNil(t, stmt.WhereClause)
	})
}

// TestStmtNodeTraversal tests that statement nodes work with the node traversal system.
func TestStmtNodeTraversal(t *testing.T) {
	// Create a simple SELECT statement
	stmt := NewSelectStmt()

	// Add a target with a column reference
	target := NewResTarget("", NewColumnRef(NewString("name")))
	stmt.TargetList = []*ResTarget{target}

	// Add a table reference
	table := NewRangeVar("users", "", "")
	stmt.FromClause = NewNodeList(table)

	// Walk the statement tree (basic test - full traversal will be implemented later)
	var visited []Node
	WalkNodes(stmt, func(node Node) bool {
		visited = append(visited, node)
		return true
	})

	// For now, just verify that the root node is visited
	// Full traversal support for statement nodes will be added later
	assert.Len(t, visited, 1)
	assert.Equal(t, stmt, visited[0])
}
