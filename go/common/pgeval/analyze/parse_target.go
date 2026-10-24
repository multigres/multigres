// PostgreSQL Database Management System
// (also known as Postgres, formerly known as Postgres95)
//
//	Portions Copyright (c) 2026, Supabase, Inc
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

package analyze

import (
	"github.com/multigres/multigres/go/common/parser/ast"
)

// FigureColname chooses the output column name for an unaliased target
// list expression - parse_target.c:1704. It inspects the raw (unanalyzed)
// expression, so call it before TransformExpr consumes the tree.
func FigureColname(node ast.Node) string {
	name := ""
	figureColnameInternal(node, &name)
	if name != "" {
		return name
	}
	// default result if we can't guess anything
	return "?column?"
}

// figureColnameInternal returns how strongly it could name the node -
// parse_target.c:1743: 0 means no name, 1 a weak guess (a cast's type
// name, CASE) that a stronger one from an inner node overrides, 2 a
// definite name.
func figureColnameInternal(node ast.Node, name *string) int {
	strength := 0

	switch n := node.(type) {
	case nil:
		return strength
	case *ast.ParenExpr:
		// The Go grammar keeps parentheses as a node; upstream's drops them.
		return figureColnameInternal(n.Expr, name)
	case *ast.ColumnRef:
		// find last field name, if any, ignoring "*"
		if fname := lastStringField(n.Fields); fname != "" {
			*name = fname
			return 2
		}
	case *ast.A_Indirection:
		// find last field name, if any, ignoring "*" and subscripts
		if fname := lastStringField(n.Indirection); fname != "" {
			*name = fname
			return 2
		}
		return figureColnameInternal(n.Arg, name)
	case *ast.FuncCall:
		*name = lastString(n.Funcname)
		return 2
	case *ast.A_Expr:
		if n.Kind == ast.AEXPR_NULLIF {
			// make nullif() act like a regular function
			*name = "nullif"
			return 2
		}
	case *ast.TypeCast:
		strength = figureColnameInternal(n.Arg, name)
		if strength <= 1 {
			if n.TypeName != nil {
				*name = lastString(n.TypeName.Names)
				return 1
			}
		}
	case *ast.CollateClause:
		return figureColnameInternal(n.Arg, name)
	case *ast.GroupingFunc:
		// make GROUPING() act like a regular function
		*name = "grouping"
		return 2
	case *ast.MergeSupportFunc:
		// make MERGE_ACTION() act like a regular function
		*name = "merge_action"
		return 2
	case *ast.SubLink:
		switch n.SubLinkType {
		case ast.EXISTS_SUBLINK:
			*name = "exists"
			return 2
		case ast.ARRAY_SUBLINK:
			*name = "array"
			return 2
		case ast.EXPR_SUBLINK:
			// Get column name of the subquery's single target. Upstream reads
			// the transformed subquery's resname; on a raw tree that is the
			// target's alias, else its own figured name.
			if sel, ok := n.Subselect.(*ast.SelectStmt); ok && sel.TargetList != nil && sel.TargetList.Len() > 0 {
				if rt, ok := sel.TargetList.Items[0].(*ast.ResTarget); ok {
					if rt.Name != "" {
						*name = rt.Name
						return 2
					}
					*name = FigureColname(rt.Val)
					return 2
				}
			}
		}
		// As with other operator-like nodes, these have no names
	case *ast.CaseExpr:
		strength = figureColnameInternal(n.Defresult, name)
		if strength <= 1 {
			*name = "case"
			return 1
		}
	case *ast.A_ArrayExpr, *ast.ArrayExpr:
		// make ARRAY[] act like a function
		*name = "array"
		return 2
	case *ast.RowExpr:
		// make ROW() act like a function
		*name = "row"
		return 2
	case *ast.CoalesceExpr:
		// make coalesce() act like a regular function
		*name = "coalesce"
		return 2
	case *ast.MinMaxExpr:
		// make greatest/least act like a regular function
		switch n.Op {
		case ast.IS_GREATEST:
			*name = "greatest"
			return 2
		case ast.IS_LEAST:
			*name = "least"
			return 2
		}
	case *ast.SQLValueFunction:
		// make these act like a function or variable
		switch n.Op {
		case ast.SVFOP_CURRENT_DATE:
			*name = "current_date"
			return 2
		case ast.SVFOP_CURRENT_TIME, ast.SVFOP_CURRENT_TIME_N:
			*name = "current_time"
			return 2
		case ast.SVFOP_CURRENT_TIMESTAMP, ast.SVFOP_CURRENT_TIMESTAMP_N:
			*name = "current_timestamp"
			return 2
		case ast.SVFOP_LOCALTIME, ast.SVFOP_LOCALTIME_N:
			*name = "localtime"
			return 2
		case ast.SVFOP_LOCALTIMESTAMP, ast.SVFOP_LOCALTIMESTAMP_N:
			*name = "localtimestamp"
			return 2
		case ast.SVFOP_CURRENT_ROLE:
			*name = "current_role"
			return 2
		case ast.SVFOP_CURRENT_USER:
			*name = "current_user"
			return 2
		case ast.SVFOP_USER:
			*name = "user"
			return 2
		case ast.SVFOP_SESSION_USER:
			*name = "session_user"
			return 2
		case ast.SVFOP_CURRENT_CATALOG:
			*name = "current_catalog"
			return 2
		case ast.SVFOP_CURRENT_SCHEMA:
			*name = "current_schema"
			return 2
		}
	case *ast.XmlExpr:
		// make SQL/XML functions act like a regular function
		switch n.Op {
		case ast.IS_XMLCONCAT:
			*name = "xmlconcat"
			return 2
		case ast.IS_XMLELEMENT:
			*name = "xmlelement"
			return 2
		case ast.IS_XMLFOREST:
			*name = "xmlforest"
			return 2
		case ast.IS_XMLPARSE:
			*name = "xmlparse"
			return 2
		case ast.IS_XMLPI:
			*name = "xmlpi"
			return 2
		case ast.IS_XMLROOT:
			*name = "xmlroot"
			return 2
		case ast.IS_XMLSERIALIZE:
			*name = "xmlserialize"
			return 2
		case ast.IS_DOCUMENT:
			// nothing
		}
	case *ast.XmlSerialize:
		// make XMLSERIALIZE act like a regular function
		*name = "xmlserialize"
		return 2
	case *ast.JsonParseExpr:
		// make JSON act like a regular function
		*name = "json"
		return 2
	case *ast.JsonScalarExpr:
		// make JSON_SCALAR act like a regular function
		*name = "json_scalar"
		return 2
	case *ast.JsonSerializeExpr:
		// make JSON_SERIALIZE act like a regular function
		*name = "json_serialize"
		return 2
	case *ast.JsonObjectConstructor:
		// make JSON_OBJECT act like a regular function
		*name = "json_object"
		return 2
	case *ast.JsonArrayConstructor, *ast.JsonArrayQueryConstructor:
		// make JSON_ARRAY act like a regular function
		*name = "json_array"
		return 2
	case *ast.JsonObjectAgg:
		// make JSON_OBJECTAGG act like a regular function
		*name = "json_objectagg"
		return 2
	case *ast.JsonArrayAgg:
		// make JSON_ARRAYAGG act like a regular function
		*name = "json_arrayagg"
		return 2
	case *ast.JsonFuncExpr:
		// make SQL/JSON functions act like a regular function
		switch n.Op {
		case ast.JSON_EXISTS_OP:
			*name = "json_exists"
			return 2
		case ast.JSON_QUERY_OP:
			*name = "json_query"
			return 2
		case ast.JSON_VALUE_OP:
			*name = "json_value"
			return 2
		}
		// JSON_TABLE_OP can't happen here.
	}

	return strength
}

// lastStringField returns the last String item of a name list, or "" when
// there is none (a bare "*" or subscripts only).
func lastStringField(fields *ast.NodeList) string {
	fname := ""
	if fields == nil {
		return fname
	}
	for _, i := range fields.Items {
		if s, ok := i.(*ast.String); ok {
			fname = s.SVal
		}
	}
	return fname
}

// lastString is llast(list) for a qualified name, "" for an empty list.
func lastString(names *ast.NodeList) string {
	if names == nil || names.Len() == 0 {
		return ""
	}
	if s, ok := names.Items[names.Len()-1].(*ast.String); ok {
		return s.SVal
	}
	return ""
}
