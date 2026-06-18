// Copyright 2026 Supabase, Inc.
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

package plpgsqlast

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser/ast"
)

// CloneNode deep-copies PL/pgSQL nodes but shares the embedded SQL at the
// boundary: PLpgSQL_expr.Parsed (an ast.Stmt) is shallow-copied, not deep
// cloned into this hierarchy.
func TestCloneSharesParsedBoundary(t *testing.T) {
	inner := ast.NewSelectStmt() // some ast.Stmt from the other hierarchy
	e := NewPLpgSQL_expr("SELECT 1")
	e.Parsed = inner

	cloned := CloneRefOfPLpgSQL_expr(e)
	assert.NotSame(t, e, cloned, "the expr node itself is deep-copied")
	assert.Equal(t, e.Query, cloned.Query)
	assert.True(t, e.Parsed == cloned.Parsed, "Parsed is shared (shallow boundary), not deep cloned")
}

// Owned PL/pgSQL nodes are genuinely deep-cloned.
func TestCloneFunctionIsDeep(t *testing.T) {
	fn := NewPLpgSQL_function()
	fn.Action = NewPLpgSQL_stmt_block()
	fn.Action.Label = "outer"

	cloned := CloneRefOfPLpgSQL_function(fn)
	require.NotNil(t, cloned.Action)
	assert.NotSame(t, fn.Action, cloned.Action, "the body block is a distinct copy")
	assert.Equal(t, "outer", cloned.Action.Label)
}

// collectTags walks a tree with Rewrite and records each visited node's tag.
func collectTags(node Node) []NodeTag {
	var tags []NodeTag
	Rewrite(node, func(c *Cursor) bool {
		tags = append(tags, c.Node().NodeTag())
		return true
	}, nil)
	return tags
}

// Rewrite descends through owned PL/pgSQL nodes.
func TestRewriteVisitsOwnedNodes(t *testing.T) {
	fn := NewPLpgSQL_function()
	fn.Action = NewPLpgSQL_stmt_block()

	assert.Equal(t,
		[]NodeTag{T_PLpgSQL_function, T_PLpgSQL_stmt_block},
		collectTags(fn),
	)
}

// Rewrite stops at the boundary: it never descends into PLpgSQL_expr.Parsed,
// because an ast.Stmt is not a plpgsqlast.Node. The embedded SQL is analyzed
// separately by the SQL-side walker.
func TestRewriteDoesNotCrossBoundary(t *testing.T) {
	e := NewPLpgSQL_expr("SELECT 1")
	e.Parsed = ast.NewSelectStmt()

	assert.Equal(t, []NodeTag{T_PLpgSQL_expr}, collectTags(e),
		"only the expr is visited; the embedded ast.Stmt is not")
}
