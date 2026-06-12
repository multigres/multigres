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

	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRewriteBasicTraversal tests basic tree traversal without modification
func TestRewriteBasicTraversal(t *testing.T) {
	// Create a simple AST node
	node := ast.NewString("test")

	visited := 0
	result := ast.Rewrite(node, func(cursor *ast.Cursor) bool {
		visited++
		return true // Continue traversal
	}, nil)

	assert.NotNil(t, result, "Rewrite should not return nil")
	assert.Equal(t, visited, 1, "Pre function should have been called")
	assert.Same(t, node, result, "Same node should be returned when not modified")
}

// TestRewriteNodeReplacement tests replacing a node
func TestRewriteNodeReplacement(t *testing.T) {
	original := ast.NewString("original")

	result := ast.Rewrite(original, func(cursor *ast.Cursor) bool {
		if s, ok := cursor.Node().(*ast.String); ok {
			if s.SVal == "original" {
				cursor.Replace(ast.NewString("replaced"))
			}
		}
		return true
	}, nil)

	assert.NotNil(t, result, "Rewrite should not return nil")

	s, ok := result.(*ast.String)
	assert.True(t, ok, "Result should be a *ast.String")
	assert.Equal(t, "replaced", s.SVal, "String value should have been replaced")
}

// TestRewritePostOrder tests post-order traversal
func TestRewritePostOrder(t *testing.T) {
	node := ast.NewString("test")

	preCount := 0
	postCount := 0

	ast.Rewrite(node,
		func(cursor *ast.Cursor) bool {
			preCount++
			return true
		},
		func(cursor *ast.Cursor) bool {
			postCount++
			return true
		},
	)

	assert.Equal(t, 1, preCount, "Pre function should be called exactly once for a single node")
	assert.Equal(t, 1, postCount, "Post function should be called exactly once for a single node")
}

// TestRewriteSkipChildren tests skipping children by returning false from pre
func TestRewriteSkipChildren(t *testing.T) {
	// Create a more complex node with children
	node := &ast.NodeList{
		Items: []ast.Node{
			ast.NewString("child1"),
			ast.NewString("child2"),
		},
	}

	visited := make(map[string]bool)

	ast.Rewrite(node, func(cursor *ast.Cursor) bool {
		if _, ok := cursor.Node().(*ast.NodeList); ok {
			visited["NodeList"] = true
			return false // Skip children
		}
		if s, ok := cursor.Node().(*ast.String); ok {
			visited[s.SVal] = true
		}
		return true
	}, nil)

	assert.True(t, visited["NodeList"], "NodeList should have been visited")
	assert.False(t, visited["child1"], "child1 should not have been visited (skipped)")
	assert.False(t, visited["child2"], "child2 should not have been visited (skipped)")
}

// TestRewritePreparedStatement tests that rewriting parameters in prepared statements works fine.
func TestRewritePreparedStatement(t *testing.T) {
	stmts, err := parser.ParseSQL("PREPARE stmt1 AS SELECT * FROM t2 where id = $1 AND col = $2")
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	stmt := stmts[0]
	rewrittenAst := ast.Rewrite(stmt, func(c *ast.Cursor) bool {
		if param, ok := c.Node().(*ast.ParamRef); ok {
			switch param.Number {
			case 1:
				c.Replace(ast.NewInteger(32))
			case 2:
				c.Replace(ast.NewString("val"))
			}
			return false
		}
		return true
	}, nil)

	rewrittenSQL := rewrittenAst.SqlString()
	require.Equal(t, "PREPARE stmt1 AS SELECT * FROM t2 WHERE id = 32 AND col = 'val'", rewrittenSQL)
}
