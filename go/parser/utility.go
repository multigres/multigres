package parser

import (
	"fmt"

	"github.com/multigres/parser/go/parser/ast"
)

// linitial returns the first element of a NodeList, equivalent to PostgreSQL's linitial()
func linitial(list *ast.NodeList) ast.Node {
	if list == nil || len(list.Items) == 0 {
		return nil
	}
	return list.Items[0]
}

// lsecond returns the second element of a NodeList, equivalent to PostgreSQL's lsecond()
func lsecond(list *ast.NodeList) ast.Node {
	if list == nil || len(list.Items) < 2 {
		return nil
	}
	return list.Items[1]
}

// lthird returns the third element of a NodeList, equivalent to PostgreSQL's lthird()
func lthird(list *ast.NodeList) ast.Node {
	if list == nil || len(list.Items) < 3 {
		return nil
	}
	return list.Items[2]
}

// llast returns the last element of a NodeList, equivalent to PostgreSQL's llast()
func llast(list *ast.NodeList) ast.Node {
	if list == nil || len(list.Items) == 0 {
		return nil
	}
	return list.Items[len(list.Items)-1]
}

// makeTypeNameFromNodeList converts *ast.NodeList to *ast.TypeName
func makeTypeNameFromNodeList(list *ast.NodeList) *ast.TypeName {
	return &ast.TypeName{
		BaseNode: ast.BaseNode{Tag: ast.T_TypeName},
		Names:    list,
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
		Inh:      true, // Default to inheritance enabled (no ONLY)
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

// makeRangeVarFromQualifiedName constructs a RangeVar from a ColId and indirection list.
// This mirrors PostgreSQL's makeRangeVarFromQualifiedName function which is used in
// grammar rules that have "ColId indirection" patterns.
//
// The function handles:
//   - ColId alone: "table" -> RangeVar{RelName: "table"}
//   - ColId + single indirection: "schema" + [".table"] -> RangeVar{SchemaName: "schema", RelName: "table"}
//   - ColId + multiple indirections: "catalog" + [".schema", ".table"] -> RangeVar{CatalogName: "catalog", SchemaName: "schema", RelName: "table"}
//
// Parameters:
//   - name: The initial ColId string
//   - indirection: NodeList containing the indirection elements (can be nil)
//   - position: Source location for error reporting
//
// Returns the constructed RangeVar.
//
// Ported from PostgreSQL's makeRangeVarFromQualifiedName function.
func makeRangeVarFromQualifiedName(name string, indirection *ast.NodeList, position int) *ast.RangeVar {
	r := &ast.RangeVar{
		BaseNode: ast.BaseNode{Tag: ast.T_RangeVar, Loc: position},
		Inh:      true, // Default to inheritance enabled (no ONLY)
	}

	// Start with the base name
	names := []string{name}

	// Add indirection elements
	if indirection != nil {
		for _, item := range indirection.Items {
			if str, ok := item.(*ast.String); ok {
				names = append(names, str.SVal)
			}
			// Note: PostgreSQL also handles A_Star nodes for ".*" but we'll focus on String nodes for now
		}
	}

	// Build RangeVar based on number of names
	switch len(names) {
	case 1:
		// Single name: just the relation name
		r.CatalogName = ""
		r.SchemaName = ""
		r.RelName = names[0]
	case 2:
		// Two names: schema.relation
		r.CatalogName = ""
		r.SchemaName = names[0]
		r.RelName = names[1]
	case 3:
		// Three names: catalog.schema.relation
		r.CatalogName = names[0]
		r.SchemaName = names[1]
		r.RelName = names[2]
	default:
		// For more than 3 names, use the last as relation, second-to-last as schema, third-to-last as catalog
		// This is a fallback - PostgreSQL would likely error on too many names
		if len(names) >= 3 {
			r.CatalogName = names[len(names)-3]
			r.SchemaName = names[len(names)-2]
			r.RelName = names[len(names)-1]
		} else {
			r.RelName = names[len(names)-1]
		}
	}

	r.RelPersistence = ast.RELPERSISTENCE_PERMANENT
	return r
}

// SplitColQualList separates a ColQualList (column qualifier list) into constraints and collate clauses.
// This mirrors PostgreSQL's SplitColQualList function which is used to process column qualifiers
// and domain qualifiers by separating them into appropriate lists.
//
// PostgreSQL reference: src/backend/parser/gram.tab.c:SplitColQualList
//
// Parameters:
//   - qualList: A NodeList containing Constraint and CollateClause nodes
//
// Returns:
//   - constraints: A NodeList of Constraint nodes
//   - collClause: A single CollateClause (PostgreSQL allows only one COLLATE per column/domain)
func SplitColQualList(qualList *ast.NodeList) (*ast.NodeList, *ast.CollateClause) {
	var constraints []*ast.Constraint
	var collClause *ast.CollateClause

	if qualList == nil {
		return ast.NewNodeList(), nil
	}

	for _, item := range qualList.Items {
		switch node := item.(type) {
		case *ast.Constraint:
			constraints = append(constraints, node)
		case *ast.CollateClause:
			// PostgreSQL allows only one COLLATE clause per column/domain
			// If multiple are specified, the last one wins
			collClause = node
		}
	}

	// Convert constraints slice to NodeList
	constraintList := ast.NewNodeList()
	for _, constraint := range constraints {
		constraintList.Append(constraint)
	}

	return constraintList, collClause
}

// makeOrderedSetArgs processes arguments for hypothetical-set aggregates.
// It validates VARIADIC argument consistency and returns a list containing:
// - The concatenated direct and ordered arguments
// - An integer indicating the number of direct arguments
//
// This mirrors PostgreSQL's makeOrderedSetArgs function.
//
// PostgreSQL reference: src/backend/parser/gram.y:makeOrderedSetArgs
func makeOrderedSetArgs(directArgs *ast.NodeList, orderedArgs *ast.NodeList) (*ast.NodeList, error) {
	if directArgs == nil {
		directArgs = ast.NewNodeList()
	}
	if orderedArgs == nil {
		orderedArgs = ast.NewNodeList()
	}

	// Check if the last direct argument is VARIADIC
	if directArgs.Len() > 0 {
		if lastParam, ok := directArgs.Items[directArgs.Len()-1].(*ast.FunctionParameter); ok {
			if lastParam.Mode == ast.FUNC_PARAM_VARIADIC {
				// PostgreSQL requires exactly one VARIADIC ordered argument of the same type
				if orderedArgs.Len() != 1 {
					return nil, fmt.Errorf("an ordered-set aggregate with a VARIADIC direct argument must have one VARIADIC aggregated argument of the same data type")
				}

				if firstOrdered, ok := orderedArgs.Items[0].(*ast.FunctionParameter); ok {
					if firstOrdered.Mode != ast.FUNC_PARAM_VARIADIC {
						return nil, fmt.Errorf("an ordered-set aggregate with a VARIADIC direct argument must have one VARIADIC aggregated argument of the same data type")
					}
					// TODO: Check that types are equal when we have proper type comparison
					// For now, we skip type checking but drop the duplicate VARIADIC argument
					orderedArgs = ast.NewNodeList()
				}
			}
		}
	}

	// Store the number of direct arguments
	numDirectArgs := ast.NewInteger(directArgs.Len())

	// Concatenate direct and ordered arguments
	allArgs := ast.NewNodeList()
	for _, arg := range directArgs.Items {
		allArgs.Append(arg)
	}
	for _, arg := range orderedArgs.Items {
		allArgs.Append(arg)
	}

	// Return [concatenated_args, num_direct_args]
	return ast.NewNodeList(allArgs, numDirectArgs), nil
}

// processConstraintAttributeSpec processes constraint attribute specification bits.
// This is a simplified version of processCASbits from PostgreSQL.
func processConstraintAttributeSpec(casbits int, constraint *ast.Constraint) {
	if casbits&ast.CAS_DEFERRABLE != 0 {
		constraint.Deferrable = true
	} else if casbits&ast.CAS_NOT_DEFERRABLE != 0 {
		constraint.Deferrable = false
	}

	if casbits&ast.CAS_INITIALLY_DEFERRED != 0 {
		constraint.Initdeferred = true
	} else if casbits&ast.CAS_INITIALLY_IMMEDIATE != 0 {
		constraint.Initdeferred = false
	}

	if casbits&ast.CAS_NOT_VALID != 0 {
		constraint.SkipValidation = true
		constraint.InitiallyValid = false
	} else {
		constraint.SkipValidation = false
		constraint.InitiallyValid = true
	}

	if casbits&ast.CAS_NO_INHERIT != 0 {
		constraint.IsNoInherit = true
	}
}

// doNegate handles negation of nodes, equivalent to PostgreSQL's doNegate()
// Ported from postgres/src/backend/parser/gram.y:doNegate
func doNegate(n ast.Node, location int) ast.Node {
	if aConst, ok := n.(*ast.A_Const); ok {
		aConst.SetLocation(location)

		if intVal, ok := aConst.Val.(*ast.Integer); ok {
			intVal.IVal = -intVal.IVal
			return n
		}
		if floatVal, ok := aConst.Val.(*ast.Float); ok {
			doNegateFloat(floatVal)
			return n
		}
	}

	// Default: create unary minus expression
	name := ast.NewNodeList(ast.NewString("-"))
	return ast.NewA_Expr(ast.AEXPR_OP, name, nil, n, location)
}

// doNegateFloat handles negation of float values
// Ported from postgres/src/backend/parser/gram.y:doNegateFloat
func doNegateFloat(v *ast.Float) {
	oldval := v.FVal
	if len(oldval) > 0 && oldval[0] == '+' {
		// Remove leading +
		v.FVal = "-" + oldval[1:]
	} else if len(oldval) > 0 && oldval[0] == '-' {
		// Remove leading -
		v.FVal = oldval[1:]
	} else {
		// Add leading -
		v.FVal = "-" + oldval
	}
}

// makeSetOp creates a set operation (UNION, INTERSECT, EXCEPT) SelectStmt
// Ported from postgres/src/backend/parser/gram.y:makeSetOp
func makeSetOp(op ast.SetOperation, all bool, larg ast.Stmt, rarg ast.Stmt) ast.Stmt {
	n := ast.NewSelectStmt()
	n.Op = op
	n.All = all
	n.Larg = larg.(*ast.SelectStmt)
	n.Rarg = rarg.(*ast.SelectStmt)
	return n
}
