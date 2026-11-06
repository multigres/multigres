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

package ast_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/parser/datum"
)

func int4(v int32) ast.Datum { return datum.Int32GetDatum(v) }

// TestDistinctAndNullIfExpr covers the two OpExpr-shaped nodes that
// IS DISTINCT FROM and NULLIF analyze into: construction, the generated
// clone, and the generated rewriter.
func TestDistinctAndNullIfExpr(t *testing.T) {
	left, right := ast.NewConst(ast.INT4OID, int4(1), false), ast.NewConst(ast.INT4OID, int4(2), false)
	nodes := map[string]ast.Node{
		"DistinctExpr": ast.NewDistinctExpr(96, 65, ast.BOOLOID, ast.NewNodeList(left, right)),
		"NullIfExpr":   ast.NewNullIfExpr(96, 65, ast.INT4OID, ast.NewNodeList(left, right)),
	}
	assert.Equal(t, ast.T_DistinctExpr, nodes["DistinctExpr"].NodeTag())
	assert.Equal(t, ast.T_NullIfExpr, nodes["NullIfExpr"].NodeTag())
	assert.Equal(t, "T_DistinctExpr", ast.T_DistinctExpr.String())
	assert.Equal(t, "T_NullIfExpr", ast.T_NullIfExpr.String())
	assert.Equal(t, "DistinctExpr(oid:96)@0", nodes["DistinctExpr"].String())
	assert.Equal(t, "NullIfExpr(oid:96)@0", nodes["NullIfExpr"].String())
	assert.Equal(t, ast.Oid(96), nodes["DistinctExpr"].(*ast.DistinctExpr).Opno)
	assert.Equal(t, ast.Oid(65), nodes["NullIfExpr"].(*ast.NullIfExpr).Opfuncid)

	for name, node := range nodes {
		t.Run(name, func(t *testing.T) {
			clone := ast.CloneNode(node)
			require.NotNil(t, clone)
			assert.NotSame(t, node, clone)
			assert.Equal(t, node.NodeTag(), clone.NodeTag())
			var args, clonedArgs *ast.NodeList
			switch n := node.(type) {
			case *ast.DistinctExpr:
				args, clonedArgs = n.Args, clone.(*ast.DistinctExpr).Args
			case *ast.NullIfExpr:
				args, clonedArgs = n.Args, clone.(*ast.NullIfExpr).Args
			}
			require.Len(t, clonedArgs.Items, 2)
			assert.NotSame(t, args.Items[0], clonedArgs.Items[0], "arguments are deep-copied")

			// The rewriter descends into the arguments and applies a
			// replacement there.
			visited := 0
			rewritten := ast.Rewrite(node, func(cursor *ast.Cursor) bool {
				visited++
				if c, ok := cursor.Node().(*ast.Const); ok && datum.DatumGetInt32(c.Constvalue) == 2 {
					cursor.Replace(ast.NewConst(ast.INT4OID, int4(3), false))
				}
				return true
			}, func(*ast.Cursor) bool { visited++; return true })
			assert.Same(t, node, rewritten)
			assert.Equal(t, 8, visited, "node, list and two arguments, pre and post")
			assert.Equal(t, int32(3), datum.DatumGetInt32(args.Items[1].(*ast.Const).Constvalue))
			assert.Equal(t, int32(2), datum.DatumGetInt32(clonedArgs.Items[1].(*ast.Const).Constvalue), "clone unaffected")

			// A nil node is skipped by both.
			stopped := ast.Rewrite(node, func(*ast.Cursor) bool { return false }, nil)
			assert.Same(t, node, stopped)
		})
	}
	assert.Nil(t, ast.CloneRefOfDistinctExpr(nil))
	assert.Nil(t, ast.CloneRefOfNullIfExpr(nil))
}
