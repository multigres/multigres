// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ast_test

import (
	"testing"

	"github.com/multigres/multigres/go/parser"
	"github.com/multigres/multigres/go/parser/ast"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCloneBasicNode tests cloning a simple node
func TestCloneBasicNode(t *testing.T) {
	original := ast.NewString("test")

	cloned := ast.CloneRefOfString(original)

	assert.NotNil(t, cloned, "Cloned node should not be nil")
	assert.Equal(t, original.SVal, cloned.SVal, "Cloned node should have same value")
	assert.NotSame(t, original, cloned, "Cloned node should be a different instance")
}

// TestCloneNilNode tests that cloning nil returns nil
func TestCloneNilNode(t *testing.T) {
	var original *ast.String = nil

	cloned := ast.CloneRefOfString(original)

	assert.Nil(t, cloned, "Cloning nil should return nil")
}

// TestCloneDeepCopy tests that modifications to cloned nodes don't affect originals
func TestCloneDeepCopy(t *testing.T) {
	original := ast.NewString("original")

	cloned := ast.CloneRefOfString(original)
	cloned.SVal = "modified"

	assert.Equal(t, "original", original.SVal, "Original should not be modified")
	assert.Equal(t, "modified", cloned.SVal, "Clone should be modified")
}

// TestCloneNodeList tests cloning a list of nodes
func TestCloneNodeList(t *testing.T) {
	original := ast.NewNodeList(
		ast.NewString("item1"),
		ast.NewString("item2"),
	)

	cloned := ast.CloneRefOfNodeList(original)

	assert.NotNil(t, cloned, "Cloned list should not be nil")
	assert.Equal(t, len(original.Items), len(cloned.Items), "Cloned list should have same length")

	// Verify the slices are different instances (not the same underlying array)
	if len(original.Items) > 0 && len(cloned.Items) > 0 {
		assert.NotSame(t, &original.Items[0], &cloned.Items[0], "Cloned list slice should be different")
	}

	// Verify deep copy - modifying clone shouldn't affect original
	clonedStr := cloned.Items[0].(*ast.String)
	clonedStr.SVal = "modified"

	originalStr := original.Items[0].(*ast.String)
	assert.Equal(t, "item1", originalStr.SVal, "Original list item should not be modified")
	assert.Equal(t, "modified", clonedStr.SVal, "Cloned list item should be modified")
}

// TestCloneComplexStruct tests cloning a complex struct with multiple fields
func TestCloneComplexStruct(t *testing.T) {
	stmts, err := parser.ParseSQL("SELECT id, name FROM users WHERE id = 1")
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	original := stmts[0]
	cloned := ast.CloneNode(original)

	assert.NotNil(t, cloned, "Cloned statement should not be nil")
	assert.NotSame(t, original, cloned, "Cloned statement should be different instance")

	// Verify SQL string is identical
	assert.Equal(t, original.SqlString(), cloned.SqlString(), "SQL string should be identical")
}

// TestCloneWithModification tests that we can modify a cloned AST without affecting original
func TestCloneWithModification(t *testing.T) {
	stmts, err := parser.ParseSQL("SELECT * FROM users WHERE id = 1")
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	original := stmts[0]
	originalSQL := original.SqlString()

	cloned := ast.CloneNode(original)

	// Modify the cloned AST by replacing integer values
	modified := ast.Rewrite(cloned, func(c *ast.Cursor) bool {
		if intNode, ok := c.Node().(*ast.Integer); ok {
			if intNode.IVal == 1 {
				c.Replace(ast.NewInteger(999))
			}
		}
		return true
	}, nil)

	modifiedSQL := modified.SqlString()
	originalSQL2 := original.SqlString()

	// Original should remain unchanged
	assert.Equal(t, originalSQL, originalSQL2, "Original SQL should not change")
	assert.Equal(t, "SELECT * FROM users WHERE id = 999", modifiedSQL, "Modified SQL should be different")
}

// TestCloneInterface tests cloning through the Node interface
func TestCloneInterface(t *testing.T) {
	var original ast.Node = ast.NewString("test")

	cloned := ast.CloneNode(original)

	assert.NotNil(t, cloned, "Cloned node should not be nil")
	assert.NotSame(t, original, cloned, "Cloned node should be different instance")

	originalStr, ok1 := original.(*ast.String)
	clonedStr, ok2 := cloned.(*ast.String)

	assert.True(t, ok1, "Original should be *ast.String")
	assert.True(t, ok2, "Cloned should be *ast.String")
	assert.Equal(t, originalStr.SVal, clonedStr.SVal, "Values should be equal")
}

// TestCloneInteger tests cloning integer nodes
func TestCloneInteger(t *testing.T) {
	original := ast.NewInteger(42)

	cloned := ast.CloneRefOfInteger(original)

	assert.NotNil(t, cloned, "Cloned node should not be nil")
	assert.Equal(t, original.IVal, cloned.IVal, "Cloned integer should have same value")
	assert.NotSame(t, original, cloned, "Cloned node should be different instance")

	// Modify clone
	cloned.IVal = 100

	assert.Equal(t, 42, original.IVal, "Original should not be modified")
	assert.Equal(t, 100, cloned.IVal, "Clone should be modified")
}
