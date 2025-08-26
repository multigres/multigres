package parser

import (
	"testing"
	"github.com/multigres/parser/go/parser/ast"
	"github.com/stretchr/testify/assert"
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