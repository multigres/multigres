package parser

import "github.com/multigres/parser/go/parser/ast"

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
