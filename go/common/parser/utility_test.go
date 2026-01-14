// PostgreSQL Database Management System
// (also known as Postgres, formerly known as Postgres95)
//
//	Portions Copyright (c) 2025, Supabase, Inc
//
//	Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//
//	Portions Copyright (c) 1994, The Regents of the University of California
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written agreement
// is hereby granted, provided that the above copyright notice and this
// paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
// DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
// LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
// DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.
//
// THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
// AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
// ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
// PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/multigres/multigres/go/common/parser/ast"
)

func TestMakeRangeVarFromAnyName(t *testing.T) {
	t.Run("single name", func(t *testing.T) {
		names := &ast.NodeList{
			Items: []ast.Node{ast.NewString("users")},
		}

		rangeVar, err := makeRangeVarFromAnyName(names, 10)

		assert.NoError(t, err)
		assert.NotNil(t, rangeVar)
		assert.Equal(t, "", rangeVar.CatalogName)
		assert.Equal(t, "", rangeVar.SchemaName)
		assert.Equal(t, "users", rangeVar.RelName)
		assert.Equal(t, ast.RELPERSISTENCE_PERMANENT, rangeVar.RelPersistence)
		assert.Equal(t, 10, rangeVar.Location())
	})

	t.Run("schema qualified name", func(t *testing.T) {
		names := &ast.NodeList{
			Items: []ast.Node{
				ast.NewString("public"),
				ast.NewString("users"),
			},
		}

		rangeVar, err := makeRangeVarFromAnyName(names, 20)

		assert.NoError(t, err)
		assert.NotNil(t, rangeVar)
		assert.Equal(t, "", rangeVar.CatalogName)
		assert.Equal(t, "public", rangeVar.SchemaName)
		assert.Equal(t, "users", rangeVar.RelName)
		assert.Equal(t, ast.RELPERSISTENCE_PERMANENT, rangeVar.RelPersistence)
		assert.Equal(t, 20, rangeVar.Location())
	})

	t.Run("fully qualified name", func(t *testing.T) {
		names := &ast.NodeList{
			Items: []ast.Node{
				ast.NewString("mydb"),
				ast.NewString("public"),
				ast.NewString("users"),
			},
		}

		rangeVar, err := makeRangeVarFromAnyName(names, 30)

		assert.NoError(t, err)
		assert.NotNil(t, rangeVar)
		assert.Equal(t, "mydb", rangeVar.CatalogName)
		assert.Equal(t, "public", rangeVar.SchemaName)
		assert.Equal(t, "users", rangeVar.RelName)
		assert.Equal(t, ast.RELPERSISTENCE_PERMANENT, rangeVar.RelPersistence)
		assert.Equal(t, 30, rangeVar.Location())
	})

	t.Run("nil names", func(t *testing.T) {
		rangeVar, err := makeRangeVarFromAnyName(nil, 0)

		assert.Error(t, err)
		assert.Nil(t, rangeVar)
		assert.Contains(t, err.Error(), "names cannot be nil")
	})

	t.Run("empty names", func(t *testing.T) {
		names := &ast.NodeList{Items: []ast.Node{}}

		rangeVar, err := makeRangeVarFromAnyName(names, 0)

		assert.Error(t, err)
		assert.Nil(t, rangeVar)
		assert.Contains(t, err.Error(), "expected 1-3 names, got 0")
	})

	t.Run("too many names", func(t *testing.T) {
		names := &ast.NodeList{
			Items: []ast.Node{
				ast.NewString("a"),
				ast.NewString("b"),
				ast.NewString("c"),
				ast.NewString("d"),
			},
		}

		rangeVar, err := makeRangeVarFromAnyName(names, 0)

		assert.Error(t, err)
		assert.Nil(t, rangeVar)
		assert.Contains(t, err.Error(), "expected 1-3 names, got 4")
	})

	t.Run("non-string node", func(t *testing.T) {
		names := &ast.NodeList{
			Items: []ast.Node{ast.NewInteger(123)}, // Not a string
		}

		rangeVar, err := makeRangeVarFromAnyName(names, 0)

		assert.Error(t, err)
		assert.Nil(t, rangeVar)
		assert.Contains(t, err.Error(), "expected string node")
	})
}

func TestSplitColQualList(t *testing.T) {
	t.Run("nil input", func(t *testing.T) {
		constraints, collClause := SplitColQualList(nil)

		assert.NotNil(t, constraints)
		assert.Equal(t, 0, len(constraints.Items))
		assert.Nil(t, collClause)
	})

	t.Run("empty list", func(t *testing.T) {
		qualList := ast.NewNodeList()
		constraints, collClause := SplitColQualList(qualList)

		assert.NotNil(t, constraints)
		assert.Equal(t, 0, len(constraints.Items))
		assert.Nil(t, collClause)
	})

	t.Run("single constraint", func(t *testing.T) {
		constraint := ast.NewConstraint(ast.CONSTR_NOTNULL)
		qualList := ast.NewNodeList()
		qualList.Append(constraint)

		constraints, collClause := SplitColQualList(qualList)

		assert.NotNil(t, constraints)
		assert.Equal(t, 1, len(constraints.Items))
		assert.Equal(t, constraint, constraints.Items[0])
		assert.Nil(t, collClause)
	})

	t.Run("single collate clause", func(t *testing.T) {
		collate := &ast.CollateClause{
			Collname: ast.NewNodeList(),
		}
		collate.Collname.Append(ast.NewString("en_US"))

		qualList := ast.NewNodeList()
		qualList.Append(collate)

		constraints, collClause := SplitColQualList(qualList)

		assert.NotNil(t, constraints)
		assert.Equal(t, 0, len(constraints.Items))
		assert.Equal(t, collate, collClause)
	})

	t.Run("mixed constraints and collate", func(t *testing.T) {
		constraint1 := ast.NewConstraint(ast.CONSTR_NOTNULL)
		constraint2 := ast.NewConstraint(ast.CONSTR_CHECK)
		collate := &ast.CollateClause{
			Collname: ast.NewNodeList(),
		}
		collate.Collname.Append(ast.NewString("C"))

		qualList := ast.NewNodeList()
		qualList.Append(constraint1)
		qualList.Append(collate)
		qualList.Append(constraint2)

		constraints, collClause := SplitColQualList(qualList)

		assert.NotNil(t, constraints)
		assert.Equal(t, 2, len(constraints.Items))
		assert.Equal(t, constraint1, constraints.Items[0])
		assert.Equal(t, constraint2, constraints.Items[1])
		assert.Equal(t, collate, collClause)
	})

	t.Run("multiple collate clauses - last wins", func(t *testing.T) {
		collate1 := &ast.CollateClause{
			Collname: ast.NewNodeList(),
		}
		collate1.Collname.Append(ast.NewString("en_US"))

		collate2 := &ast.CollateClause{
			Collname: ast.NewNodeList(),
		}
		collate2.Collname.Append(ast.NewString("C"))

		qualList := ast.NewNodeList()
		qualList.Append(collate1)
		qualList.Append(collate2)

		constraints, collClause := SplitColQualList(qualList)

		assert.NotNil(t, constraints)
		assert.Equal(t, 0, len(constraints.Items))
		// The last collate clause should win
		assert.Equal(t, collate2, collClause)
	})
}
