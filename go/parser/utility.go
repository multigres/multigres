package parser

import (
	"fmt"
	"github.com/multigres/parser/go/parser/ast"
)

// makeTypeNameFromNodeList converts *ast.NodeList to *ast.TypeName
func makeTypeNameFromNodeList(list *ast.NodeList) *ast.TypeName {
	return &ast.TypeName{
		Names: list,
	}
}

func makeTypeNameFromString(str string) *ast.TypeName {
	return makeTypeNameFromNodeList(&ast.NodeList{
		Items: []ast.Node{
			ast.NewString(str),
		},
	})
}

// makeRangeVarFromAnyName converts a list of (dotted) names to a RangeVar.
// The "AnyName" refers to the any_name production in the grammar.
// 
// This function should be used when you have an any_name (NodeList of String nodes)
// but need a RangeVar for cases where the grammar has any_name but the AST expects
// a relation reference. This mirrors PostgreSQL's approach in cases where they
// comment "can't use qualified_name, sigh".
//
// Supports:
//   - Single name: "table" -> RangeVar{RelName: "table"}
//   - Schema qualified: "schema.table" -> RangeVar{SchemaName: "schema", RelName: "table"}  
//   - Fully qualified: "catalog.schema.table" -> RangeVar{CatalogName: "catalog", SchemaName: "schema", RelName: "table"}
//
// Ported from PostgreSQL's makeRangeVarFromAnyName function.
func makeRangeVarFromAnyName(names *ast.NodeList, position int) (*ast.RangeVar, error) {
	if names == nil {
		return nil, fmt.Errorf("names cannot be nil")
	}

	r := &ast.RangeVar{
		BaseNode: ast.BaseNode{Tag: ast.T_RangeVar, Loc: position},
	}

	length := names.Len()
	switch length {
	case 1:
		// Single name: just the relation name
		if str, ok := names.Items[0].(*ast.String); ok {
			r.CatalogName = ""
			r.SchemaName = ""
			r.RelName = str.SVal
		} else {
			return nil, fmt.Errorf("expected string node in names list")
		}
	case 2:
		// Two names: schema.relation
		if str1, ok := names.Items[0].(*ast.String); ok {
			if str2, ok := names.Items[1].(*ast.String); ok {
				r.CatalogName = ""
				r.SchemaName = str1.SVal
				r.RelName = str2.SVal
			} else {
				return nil, fmt.Errorf("expected string node for relation name")
			}
		} else {
			return nil, fmt.Errorf("expected string node for schema name")
		}
	case 3:
		// Three names: catalog.schema.relation
		if str1, ok := names.Items[0].(*ast.String); ok {
			if str2, ok := names.Items[1].(*ast.String); ok {
				if str3, ok := names.Items[2].(*ast.String); ok {
					r.CatalogName = str1.SVal
					r.SchemaName = str2.SVal
					r.RelName = str3.SVal
				} else {
					return nil, fmt.Errorf("expected string node for relation name")
				}
			} else {
				return nil, fmt.Errorf("expected string node for schema name")
			}
		} else {
			return nil, fmt.Errorf("expected string node for catalog name")
		}
	default:
		return nil, fmt.Errorf("improper qualified name (too many dotted names): expected 1-3 names, got %d", length)
	}

	r.RelPersistence = ast.RELPERSISTENCE_PERMANENT
	return r, nil
}
