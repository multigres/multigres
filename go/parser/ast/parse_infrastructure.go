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
//
// Package ast provides PostgreSQL AST parse infrastructure node definitions.
// This file contains core parsing infrastructure nodes essential for lexer/parser integration.
// Ported from postgres/src/include/nodes/parsenodes.h
package ast

import (
	"fmt"
	"strings"
)

// ==============================================================================
// CORE PARSE INFRASTRUCTURE - Stage 1A Implementation
// Essential parsing foundation nodes for lexer/parser integration
// Ported from postgres/src/include/nodes/parsenodes.h
// ==============================================================================

// RawStmt represents a raw statement wrapper that contains the original query text
// and location information before semantic analysis.
// Ported from postgres/src/include/nodes/parsenodes.h:2017-2024
type RawStmt struct {
	BaseNode
	Stmt         Stmt // The parsed statement tree
	StmtLocation int  // Start location of stmt in original query string
	StmtLen      int  // Length of stmt in original query string
}

// NewRawStmt creates a new RawStmt node.
func NewRawStmt(stmt Stmt, location int, length int) *RawStmt {
	return &RawStmt{
		BaseNode:     BaseNode{Tag: T_RawStmt},
		Stmt:         stmt,
		StmtLocation: location,
		StmtLen:      length,
	}
}

func (r *RawStmt) String() string {
	if r.Stmt != nil {
		return fmt.Sprintf("RawStmt{%s}@%d", r.Stmt.StatementType(), r.Location())
	}
	return fmt.Sprintf("RawStmt{nil}@%d", r.Location())
}

func (r *RawStmt) StatementType() string {
	return "RAW"
}

// A_Expr represents a generic expression node used during parsing before semantic analysis.
// This is the primary expression node used in the parse tree.
// Ported from postgres/src/include/nodes/parsenodes.h:329-339
type A_Expr struct {
	BaseNode
	Kind  A_Expr_Kind // Expression type (operator, comparison, etc.)
	Name  *NodeList   // Possibly-qualified operator name
	Lexpr Node        // Left operand
	Rexpr Node        // Right operand (or NULL for unary operators)
	// Note: Location is handled by BaseNode.Loc
}

// A_Expr_Kind represents the type of A_Expr.
// Ported from postgres/src/include/nodes/parsenodes.h:331-339
type A_Expr_Kind int

const (
	AEXPR_OP              A_Expr_Kind = iota // Normal operator
	AEXPR_OP_ANY                             // Scalar op ANY (array)
	AEXPR_OP_ALL                             // Scalar op ALL (array)
	AEXPR_DISTINCT                           // IS DISTINCT FROM
	AEXPR_NOT_DISTINCT                       // IS NOT DISTINCT FROM
	AEXPR_NULLIF                             // NULLIF(a, b)
	AEXPR_IN                                 // IN (list)
	AEXPR_LIKE                               // LIKE
	AEXPR_ILIKE                              // ILIKE
	AEXPR_SIMILAR                            // SIMILAR TO
	AEXPR_BETWEEN                            // BETWEEN
	AEXPR_NOT_BETWEEN                        // NOT BETWEEN
	AEXPR_BETWEEN_SYM                        // BETWEEN SYMMETRIC
	AEXPR_NOT_BETWEEN_SYM                    // NOT BETWEEN SYMMETRIC
)

// NewA_Expr creates a new A_Expr node.
func NewA_Expr(kind A_Expr_Kind, name *NodeList, lexpr, rexpr Node, location int) *A_Expr {
	return &A_Expr{
		BaseNode: BaseNode{Tag: T_A_Expr, Loc: location},
		Kind:     kind,
		Name:     name,
		Lexpr:    lexpr,
		Rexpr:    rexpr,
	}
}

func (a *A_Expr) String() string {
	return fmt.Sprintf("A_Expr{kind=%d}@%d", a.Kind, a.Location())
}

// SqlString returns the SQL representation of the A_Expr
func (a *A_Expr) SqlString() string {
	switch a.Kind {
	case AEXPR_OP:
		if a.Name == nil || a.Name.Len() == 0 {
			return "UNKNOWN_OP"
		}

		// Check if this is a qualified operator (OPERATOR(schema.op) syntax)
		// Qualified operators have multiple items in the Name list
		if a.Name.Len() > 1 {
			// This is a qualified operator - format as OPERATOR(schema.op)
			var parts []string
			for _, item := range a.Name.Items {
				if str, ok := item.(*String); ok {
					parts = append(parts, str.SVal)
				} else {
					parts = append(parts, item.String())
				}
			}
			// Join the parts with dots (e.g., "pg_catalog" "+" becomes "pg_catalog.+")
			qualifiedOp := strings.Join(parts, ".")

			// Format the expression with OPERATOR syntax
			if a.Lexpr != nil && a.Rexpr != nil {
				leftStr := a.Lexpr.SqlString()
				rightStr := a.Rexpr.SqlString()
				return fmt.Sprintf("%s OPERATOR(%s) %s", leftStr, qualifiedOp, rightStr)
			}
			// Unary qualified operator (rare but possible)
			if a.Lexpr == nil && a.Rexpr != nil {
				return fmt.Sprintf("OPERATOR(%s) %s", qualifiedOp, a.Rexpr.SqlString())
			}
			return "UNKNOWN_EXPR"
		}

		// Simple operator (not qualified)
		firstItem := a.Name.Items[0]
		// For operators, we need the raw string value, not the SQL quoted version
		var op string
		if str, ok := firstItem.(*String); ok {
			op = str.SVal
		} else {
			// Fallback for other node types - use their string representation
			op = firstItem.String()
		}

		// Unary operators (NOT, unary +, unary -)
		if a.Lexpr == nil && a.Rexpr != nil {
			if op == "NOT" {
				return "NOT " + a.Rexpr.SqlString()
			}
			// Unary + or -
			return fmt.Sprintf("%s%s", op, a.Rexpr.SqlString())
		}

		// Binary operators
		if a.Lexpr != nil && a.Rexpr != nil {
			leftStr := a.Lexpr.SqlString()
			rightStr := a.Rexpr.SqlString()
			return fmt.Sprintf("%s %s %s", leftStr, op, rightStr)
		}

		return "UNKNOWN_EXPR"

	case AEXPR_IN:
		if a.Lexpr != nil && a.Rexpr != nil {
			leftStr := a.Lexpr.SqlString()
			// Determine if this is IN or NOT IN based on the operator
			var op string
			if a.Name != nil && len(a.Name.Items) > 0 {
				if str, ok := a.Name.Items[0].(*String); ok {
					op = str.SVal
				}
			}
			// Check if Rexpr is a SubLink (for subqueries)
			if sublink, ok := a.Rexpr.(*SubLink); ok {
				// SubLink will handle its own deparsing
				if op == "<>" {
					return fmt.Sprintf("%s NOT IN %s", leftStr, sublink.SqlString())
				}
				return fmt.Sprintf("%s IN %s", leftStr, sublink.SqlString())
			}
			// Otherwise, it's a list of values
			rightStr := a.Rexpr.SqlString()
			if op == "<>" {
				return fmt.Sprintf("%s NOT IN (%s)", leftStr, rightStr)
			}
			return fmt.Sprintf("%s IN (%s)", leftStr, rightStr)
		}
		return "IN_EXPR"

	case AEXPR_LIKE:
		if a.Lexpr != nil && a.Rexpr != nil {
			leftStr := a.Lexpr.SqlString()
			rightStr := a.Rexpr.SqlString()
			return fmt.Sprintf("%s LIKE %s", leftStr, rightStr)
		}

	case AEXPR_ILIKE:
		if a.Lexpr != nil && a.Rexpr != nil {
			leftStr := a.Lexpr.SqlString()
			rightStr := a.Rexpr.SqlString()
			return fmt.Sprintf("%s ILIKE %s", leftStr, rightStr)
		}

	case AEXPR_OP_ANY:
		// Handle scalar op ANY (array) expressions
		if a.Lexpr != nil && a.Rexpr != nil {
			leftStr := a.Lexpr.SqlString()
			rightStr := a.Rexpr.SqlString()

			// Get the operator
			op := "=" // Default operator
			if a.Name != nil && len(a.Name.Items) > 0 {
				if str, ok := a.Name.Items[0].(*String); ok {
					op = str.SVal
				}
			}
			return fmt.Sprintf("%s %s ANY (%s)", leftStr, op, rightStr)
		}
		return "ANY_EXPR"

	case AEXPR_OP_ALL:
		// Handle scalar op ALL (array) expressions
		if a.Lexpr != nil && a.Rexpr != nil {
			leftStr := a.Lexpr.SqlString()
			rightStr := a.Rexpr.SqlString()

			// Get the operator
			op := "=" // Default operator
			if a.Name != nil && len(a.Name.Items) > 0 {
				if str, ok := a.Name.Items[0].(*String); ok {
					op = str.SVal
				}
			}
			return fmt.Sprintf("%s %s ALL (%s)", leftStr, op, rightStr)
		}
		return "ALL_EXPR"

	case AEXPR_DISTINCT:
		// Handle IS DISTINCT FROM expressions
		if a.Lexpr != nil && a.Rexpr != nil {
			leftStr := a.Lexpr.SqlString()
			rightStr := a.Rexpr.SqlString()
			return fmt.Sprintf("%s IS DISTINCT FROM %s", leftStr, rightStr)
		}
		return "DISTINCT_EXPR"

	case AEXPR_NOT_DISTINCT:
		// Handle IS NOT DISTINCT FROM expressions
		if a.Lexpr != nil && a.Rexpr != nil {
			leftStr := a.Lexpr.SqlString()
			rightStr := a.Rexpr.SqlString()
			return fmt.Sprintf("%s IS NOT DISTINCT FROM %s", leftStr, rightStr)
		}
		return "NOT_DISTINCT_EXPR"

	case AEXPR_NULLIF:
		// Handle NULLIF(a, b) expressions
		if a.Lexpr != nil && a.Rexpr != nil {
			leftStr := a.Lexpr.SqlString()
			rightStr := a.Rexpr.SqlString()
			return fmt.Sprintf("nullif(%s, %s)", leftStr, rightStr)
		}
		return "NULLIF_EXPR"

	case AEXPR_BETWEEN:
		if a.Lexpr != nil && a.Rexpr != nil {
			leftStr := a.Lexpr.SqlString()
			// Rexpr should be a NodeList with two elements
			if nodeList, ok := a.Rexpr.(*NodeList); ok && nodeList.Len() >= 2 {
				lowerStr := nodeList.Items[0].SqlString()
				upperStr := nodeList.Items[1].SqlString()
				return fmt.Sprintf("%s BETWEEN %s AND %s", leftStr, lowerStr, upperStr)
			}
		}
		return "BETWEEN_EXPR"

	case AEXPR_NOT_BETWEEN:
		if a.Lexpr != nil && a.Rexpr != nil {
			leftStr := a.Lexpr.SqlString()
			// Rexpr should be a NodeList with two elements
			if nodeList, ok := a.Rexpr.(*NodeList); ok && nodeList.Len() >= 2 {
				lowerStr := nodeList.Items[0].SqlString()
				upperStr := nodeList.Items[1].SqlString()
				return fmt.Sprintf("%s NOT BETWEEN %s AND %s", leftStr, lowerStr, upperStr)
			}
		}
		return "NOT_BETWEEN_EXPR"

	case AEXPR_BETWEEN_SYM:
		if a.Lexpr != nil && a.Rexpr != nil {
			leftStr := a.Lexpr.SqlString()
			// Rexpr should be a NodeList with two elements
			if nodeList, ok := a.Rexpr.(*NodeList); ok && nodeList.Len() >= 2 {
				lowerStr := nodeList.Items[0].SqlString()
				upperStr := nodeList.Items[1].SqlString()
				return fmt.Sprintf("%s BETWEEN SYMMETRIC %s AND %s", leftStr, lowerStr, upperStr)
			}
		}
		return "BETWEEN_SYM_EXPR"

	case AEXPR_NOT_BETWEEN_SYM:
		if a.Lexpr != nil && a.Rexpr != nil {
			leftStr := a.Lexpr.SqlString()
			// Rexpr should be a NodeList with two elements
			if nodeList, ok := a.Rexpr.(*NodeList); ok && nodeList.Len() >= 2 {
				lowerStr := nodeList.Items[0].SqlString()
				upperStr := nodeList.Items[1].SqlString()
				return fmt.Sprintf("%s NOT BETWEEN SYMMETRIC %s AND %s", leftStr, lowerStr, upperStr)
			}
		}
		return "NOT_BETWEEN_SYM_EXPR"

	case AEXPR_SIMILAR:
		if a.Lexpr != nil && a.Rexpr != nil {
			leftStr := a.Lexpr.SqlString()

			// Check if the right expression is a similar_to_escape function call
			// If it is, extract just the argument instead of the whole function call
			rightStr := ""
			if funcCall, ok := a.Rexpr.(*FuncCall); ok {
				// Check if it's pg_catalog.similar_to_escape
				if funcCall.Funcname != nil && funcCall.Funcname.Len() >= 2 {
					items := funcCall.Funcname.Items
					if str1, ok1 := items[0].(*String); ok1 && str1.SVal == "pg_catalog" {
						if str2, ok2 := items[1].(*String); ok2 && str2.SVal == "similar_to_escape" {
							// It's similar_to_escape, extract the first argument
							if funcCall.Args != nil && funcCall.Args.Len() > 0 {
								rightStr = funcCall.Args.Items[0].SqlString()
							}
						}
					}
				}
			}

			// If we didn't find similar_to_escape, use the full expression
			if rightStr == "" {
				rightStr = a.Rexpr.SqlString()
			}

			// Check if it's NOT SIMILAR TO based on the operator
			if a.Name != nil && a.Name.Len() > 0 {
				if str, ok := a.Name.Items[0].(*String); ok && str.SVal == "!~" {
					return fmt.Sprintf("%s NOT SIMILAR TO %s", leftStr, rightStr)
				}
			}
			return fmt.Sprintf("%s SIMILAR TO %s", leftStr, rightStr)
		}
		return "SIMILAR_EXPR"

	default:
		return fmt.Sprintf("A_EXPR_%d", a.Kind)
	}

	return "UNKNOWN_A_EXPR"
}

func (a *A_Expr) ExpressionType() string {
	return "A_EXPR"
}

func (a *A_Expr) IsExpr() bool {
	return true
}

// A_Const represents a constant value in the parse tree.
// Ported from postgres/src/include/nodes/parsenodes.h:357-365
type A_Const struct {
	BaseNode
	Val    Value // The constant value (Integer, Float, String, BitString, Boolean, or Null)
	Isnull bool  // SQL NULL constant
}

// NewA_Const creates a new A_Const node.
func NewA_Const(val Value, location int) *A_Const {
	aConst := &A_Const{
		BaseNode: BaseNode{Tag: T_A_Const},
		Val:      val,
		Isnull:   false,
	}
	aConst.SetLocation(location)
	return aConst
}

// NewA_ConstNull creates a new A_Const node representing a NULL value.
func NewA_ConstNull(location int) *A_Const {
	aConst := &A_Const{
		BaseNode: BaseNode{Tag: T_A_Const},
		Val:      NewNull(),
		Isnull:   true,
	}
	aConst.SetLocation(location)
	return aConst
}

func (a *A_Const) String() string {
	if a.Isnull {
		return fmt.Sprintf("A_Const{NULL}@%d", a.Location())
	}
	if a.Val != nil {
		return fmt.Sprintf("A_Const{%v}@%d", a.Val, a.Location())
	}
	return fmt.Sprintf("A_Const{nil}@%d", a.Location())
}

// SqlString returns the SQL representation of the A_Const
func (a *A_Const) SqlString() string {
	if a.Isnull {
		return "NULL"
	}

	if a.Val == nil {
		return "NULL"
	}

	// Use the Value's SqlString() method
	return a.Val.SqlString()
}

func (a *A_Const) ExpressionType() string {
	return "A_CONST"
}

func (a *A_Const) IsExpr() bool {
	return true
}

// ParamRef represents a parameter reference ($1, $2, etc.) in the parse tree.
// Ported from postgres/src/include/nodes/parsenodes.h:301-309
type ParamRef struct {
	BaseNode
	Number int // Parameter number (1-based)
}

// NewParamRef creates a new ParamRef node.
func NewParamRef(number int, location int) *ParamRef {
	paramRef := &ParamRef{
		BaseNode: BaseNode{Tag: T_ParamRef},
		Number:   number,
	}
	paramRef.SetLocation(location)
	return paramRef
}

func (p *ParamRef) String() string {
	return fmt.Sprintf("ParamRef{$%d}@%d", p.Number, p.Location())
}

func (p *ParamRef) ExpressionType() string {
	return "PARAM_REF"
}

func (p *ParamRef) IsExpr() bool {
	return true
}

// SqlString returns the SQL representation of the ParamRef
func (p *ParamRef) SqlString() string {
	return fmt.Sprintf("$%d", p.Number)
}

// TypeCast represents a type cast expression (CAST(expr AS type) or expr::type).
// Ported from postgres/src/include/nodes/parsenodes.h:370-380
type TypeCast struct {
	BaseNode
	Arg      Node      // The expression being cast
	TypeName *TypeName // The target type
}

// ParenExpr represents a parenthesized expression to preserve grouping
type ParenExpr struct {
	BaseNode
	Expr Node // The expression inside parentheses
}

// NewTypeCast creates a new TypeCast node.
func NewTypeCast(arg Node, typeName *TypeName, location int) *TypeCast {
	typeCast := &TypeCast{
		BaseNode: BaseNode{Tag: T_TypeCast},
		Arg:      arg,
		TypeName: typeName,
	}
	typeCast.SetLocation(location)
	return typeCast
}

// NewParenExpr creates a new ParenExpr node.
func NewParenExpr(expr Node, location int) *ParenExpr {
	parenExpr := &ParenExpr{
		BaseNode: BaseNode{Tag: T_ParenExpr},
		Expr:     expr,
	}
	parenExpr.SetLocation(location)
	return parenExpr
}

func (t *TypeCast) String() string {
	return fmt.Sprintf("TypeCast@%d", t.Location())
}

func (p *ParenExpr) String() string {
	return fmt.Sprintf("ParenExpr@%d", p.Location())
}

// SqlString returns the SQL representation of the TypeCast (CAST syntax)
func (t *TypeCast) SqlString() string {
	argStr := ""
	if t.Arg != nil {
		argStr = t.Arg.SqlString()
	}

	typeStr := ""
	if t.TypeName != nil {
		typeStr = t.TypeName.SqlString()
	}

	// Special handling for INTERVAL literals - convert back to INTERVAL 'value' UNIT syntax
	// if t.TypeName != nil && t.TypeName.Names != nil && t.TypeName.Names.Len() > 0 {
	// 	if firstItem, ok := t.TypeName.Names.Items[0].(*String); ok && firstItem.SVal == "interval" {
	// 		if t.TypeName.Typmods != nil && t.TypeName.Typmods.Len() > 0 {
	// 			if firstMod, ok := t.TypeName.Typmods.Items[0].(*Integer); ok {
	// 				intervalUnit := intervalMaskToString(firstMod.IVal)
	// 				if intervalUnit == "FULL_RANGE" {
	// 					if t.TypeName.Typmods.Len() == 2 {
	// 						// INTERVAL(precision) 'value' format for full range with precision
	// 						if precision, ok := t.TypeName.Typmods.Items[1].(*Integer); ok {
	// 							return fmt.Sprintf("INTERVAL(%d) %s", precision.IVal, argStr)
	// 						}
	// 					}
	// 					// INTERVAL 'value' format for full range without precision
	// 					return fmt.Sprintf("INTERVAL %s", argStr)
	// 				} else if intervalUnit != "" {
	// 					// INTERVAL 'value' UNIT format for specific units
	// 					return fmt.Sprintf("INTERVAL %s %s", argStr, intervalUnit)
	// 				}
	// 			}
	// 		}
	// 	}
	// }

	// Always use explicit CAST(expr AS type) syntax to avoid precedence issues
	return fmt.Sprintf("CAST(%s AS %s)", argStr, typeStr)
}

// SqlString returns the SQL representation of the ParenExpr (preserves parentheses)
func (p *ParenExpr) SqlString() string {
	if p.Expr == nil {
		return "()"
	}
	return fmt.Sprintf("(%s)", p.Expr.SqlString())
}

func (t *TypeCast) ExpressionType() string {
	return "TYPE_CAST"
}

func (t *TypeCast) IsExpr() bool {
	return true
}

func (p *ParenExpr) ExpressionType() string {
	return "PAREN_EXPR"
}

func (p *ParenExpr) IsExpr() bool {
	return true
}

// FuncCall represents a function call in the parse tree.
// Ported from postgres/src/include/nodes/parsenodes.h:423-444
type FuncCall struct {
	BaseNode
	Funcname       *NodeList    // Qualified function name
	Args           *NodeList    // List of arguments
	AggOrder       *NodeList    // ORDER BY list for aggregates
	AggFilter      Node         // FILTER clause for aggregates
	Over           *WindowDef   // OVER clause for window functions
	AggWithinGroup bool         // ORDER BY appeared in WITHIN GROUP
	AggStar        bool         // Function was written as foo(*)
	AggDistinct    bool         // DISTINCT was specified
	FuncVariadic   bool         // VARIADIC was specified
	Funcformat     CoercionForm // How to display this node
}

// NewFuncCall creates a new FuncCall node.
func NewFuncCall(funcname *NodeList, args *NodeList, location int) *FuncCall {
	funcCall := &FuncCall{
		BaseNode:   BaseNode{Tag: T_FuncCall},
		Funcname:   funcname,
		Args:       args,
		Funcformat: COERCE_EXPLICIT_CALL, // Default to explicit call syntax
	}
	funcCall.SetLocation(location)
	return funcCall
}

func (f *FuncCall) String() string {
	if f.Funcname != nil && len(f.Funcname.Items) > 0 {
		if str, ok := f.Funcname.Items[0].(*String); ok {
			return fmt.Sprintf("FuncCall{%s}@%d", str.SVal, f.Location())
		}
	}
	return fmt.Sprintf("FuncCall@%d", f.Location())
}

// isInternalPgCatalogFunction checks if a function name is one where the parser
// adds pg_catalog qualification during syntax transformation. Returns the
// normalized function name and true if it's internal, or empty string and false otherwise.
func isInternalPgCatalogFunction(name string) bool {
	switch strings.ToUpper(name) {
	case "TIMEZONE": // AT TIME ZONE
		return true
	case "LIKE_ESCAPE": // LIKE ... ESCAPE
		return true
	case "SIMILAR_TO_ESCAPE": // SIMILAR TO
		return true
	case "OVERLAPS": // OVERLAPS
		return true
	case "IS_NORMALIZED": // IS NORMALIZED
		return true
	case "PG_COLLATION_FOR": // COLLATION FOR
		return true
	case "SYSTEM_USER": // SYSTEM_USER
		return true
	case "EXTRACT": // EXTRACT
		return true
	case "NORMALIZE": // NORMALIZE
		return true
	case "OVERLAY": // OVERLAY
		return true
	case "POSITION": // POSITION
		return true
	case "SUBSTRING": // SUBSTRING
		return true
	case "BTRIM": // TRIM(BOTH ...)
		return true
	case "LTRIM": // TRIM(LEADING ...)
		return true
	case "RTRIM": // TRIM(TRAILING ...)
		return true
	case "XMLEXISTS": // XMLEXISTS
		return true
	default:
		return false
	}
}

// SqlString returns the SQL representation of the FuncCall
func (f *FuncCall) SqlString() string {
	// Build function name (could be qualified like schema.func)
	funcName := ""
	if f.Funcname != nil && len(f.Funcname.Items) > 0 {
		var nameParts []string

		for i, item := range f.Funcname.Items {
			if part, ok := item.(*String); ok && part != nil {
				// Check for pg_catalog.func where the parser added pg_catalog
				if i == 0 && len(f.Funcname.Items) == 2 && strings.ToLower(part.SVal) == "pg_catalog" {
					if funcPart, ok := f.Funcname.Items[1].(*String); ok {
						if isInternalPgCatalogFunction(funcPart.SVal) {
							continue // Skip pg_catalog for internal functions
						}
					}
				}

				// Normalize common function names to uppercase
				name := part.SVal
				switch strings.ToLower(name) {
				case "now":
					name = "NOW"
				case "current_timestamp":
					name = "CURRENT_TIMESTAMP"
				case "current_date":
					name = "CURRENT_DATE"
				case "current_time":
					name = "CURRENT_TIME"
				case "localtime":
					name = "LOCALTIME"
				case "localtimestamp":
					name = "LOCALTIMESTAMP"
				// Window functions
				case "row_number":
					name = "ROW_NUMBER"
				case "rank":
					name = "RANK"
				case "dense_rank":
					name = "DENSE_RANK"
				case "percent_rank":
					name = "PERCENT_RANK"
				case "cume_dist":
					name = "CUME_DIST"
				case "ntile":
					name = "NTILE"
				case "lag":
					name = "LAG"
				case "lead":
					name = "LEAD"
				case "first_value":
					name = "FIRST_VALUE"
				case "last_value":
					name = "LAST_VALUE"
				case "nth_value":
					name = "NTH_VALUE"
				// Aggregate functions commonly used as window functions
				case "sum":
					name = "SUM"
				case "count":
					name = "COUNT"
				case "avg":
					name = "AVG"
				case "min":
					name = "MIN"
				case "max":
					name = "MAX"
				// Special SQL functions that should use their original syntax
				case "extract":
					name = "EXTRACT"
				case "overlay":
					name = "OVERLAY"
				case "position":
					name = "POSITION"
				case "substring":
					name = "substring"
				case "trim":
					name = "trim"
				case "btrim":
					name = "trim"
				}
				nameParts = append(nameParts, name)
			}
		}
		funcName = strings.Join(nameParts, ".")
	}

	// Build argument list
	argStrs := []string{}

	if f.Args != nil {
		for _, arg := range f.Args.Items {
			if arg != nil {
				argStrs = append(argStrs, arg.SqlString())
			}
		}
	}

	if f.AggStar {
		argStrs = append(argStrs, "*")
	}

	// Prepend DISTINCT qualifier if needed
	if f.AggDistinct && len(argStrs) > 0 {
		argStrs[0] = "DISTINCT " + argStrs[0]
	}

	// Prepend VARIADIC qualifier to last argument if needed
	if f.FuncVariadic && len(argStrs) > 0 {
		lastIdx := len(argStrs) - 1
		argStrs[lastIdx] = "VARIADIC " + argStrs[lastIdx]
	}

	// Add ORDER BY clause inside function parentheses if present (for aggregates that aren't WITHIN GROUP)
	var funcArgs string
	if f.AggOrder != nil && f.AggOrder.Len() > 0 && !f.AggWithinGroup {
		var orderItems []string
		for _, item := range f.AggOrder.Items {
			if item != nil {
				orderItems = append(orderItems, item.SqlString())
			}
		}
		funcArgs = strings.Join(argStrs, ", ") + " ORDER BY " + strings.Join(orderItems, ", ")
	} else {
		funcArgs = strings.Join(argStrs, ", ")
	}

	// Handle special function syntax
	var result string
	if strings.ToLower(funcName) == "extract" && len(argStrs) >= 2 {
		// EXTRACT function uses special syntax: EXTRACT(field FROM source)
		// The first argument should be the field name without quotes, the second is the source
		field := argStrs[0]
		// Remove quotes from field name if it's a string literal
		if len(field) >= 2 && field[0] == '\'' && field[len(field)-1] == '\'' {
			field = field[1 : len(field)-1]
		}
		// Use lowercase for function name to match PostgreSQL style
		result = fmt.Sprintf("extract(%s FROM %s)", field, strings.Join(argStrs[1:], ", "))
	} else if strings.ToLower(funcName) == "substring" && f.Funcformat == COERCE_SQL_SYNTAX {
		// SUBSTRING function with SQL standard syntax: SUBSTRING(string FROM start [FOR length])
		if len(argStrs) >= 3 {
			// SUBSTRING(string FROM start FOR length)
			result = fmt.Sprintf("SUBSTRING(%s FROM %s FOR %s)", argStrs[0], argStrs[1], argStrs[2])
		} else if len(argStrs) >= 2 {
			// SUBSTRING(string FROM start)
			result = fmt.Sprintf("SUBSTRING(%s FROM %s)", argStrs[0], argStrs[1])
		} else {
			// Fallback to regular function call
			result = fmt.Sprintf("%s(%s)", funcName, funcArgs)
		}
	} else if strings.ToLower(funcName) == "position" && f.Funcformat == COERCE_SQL_SYNTAX {
		// POSITION function with SQL standard syntax: POSITION(substring IN string)
		// Note: Parser reorders arguments to [string, substring], so we need to swap them back
		if len(argStrs) >= 2 {
			// POSITION(substring IN string)
			result = fmt.Sprintf("POSITION(%s IN %s)", argStrs[1], argStrs[0])
		} else {
			// Fallback to regular function call
			result = fmt.Sprintf("%s(%s)", funcName, funcArgs)
		}
	} else if strings.ToLower(funcName) == "overlay" && f.Funcformat == COERCE_SQL_SYNTAX {
		// OVERLAY function with SQL standard syntax: OVERLAY(string PLACING substring FROM start [FOR length])
		if len(argStrs) >= 4 {
			// OVERLAY(string PLACING substring FROM start FOR length)
			result = fmt.Sprintf("overlay(%s placing %s from %s for %s)", argStrs[0], argStrs[1], argStrs[2], argStrs[3])
		} else if len(argStrs) >= 3 {
			// OVERLAY(string PLACING substring FROM start)
			result = fmt.Sprintf("overlay(%s placing %s from %s)", argStrs[0], argStrs[1], argStrs[2])
		} else {
			// Fallback to regular function call
			result = fmt.Sprintf("%s(%s)", funcName, funcArgs)
		}
	} else if strings.ToLower(funcName) == "normalize" && f.Args != nil && len(f.Args.Items) >= 2 {
		// NORMALIZE function: normalize(string [, form])
		// The second argument is an A_Const containing the normalization form as a keyword
		normalForm := argStrs[1] // Default to the string representation
		if constNode, ok := f.Args.Items[1].(*A_Const); ok && constNode.Val != nil {
			if strVal, ok := constNode.Val.(*String); ok {
				// The grammar stores the normalization form as an unquoted string
				normalForm = strVal.SVal
			}
		}

		if len(argStrs) >= 3 {
			// normalize(string, form, ...)
			result = fmt.Sprintf("normalize(%s, %s, %s)", argStrs[0], normalForm, strings.Join(argStrs[2:], ", "))
		} else {
			// normalize(string, form)
			result = fmt.Sprintf("normalize(%s, %s)", argStrs[0], normalForm)
		}
	} else if strings.ToLower(funcName) == "is_normalized" {
		if len(f.Args.Items) == 2 {
			normalForm := argStrs[1] // Default to the string representation
			if constNode, ok := f.Args.Items[1].(*A_Const); ok && constNode.Val != nil {
				if strVal, ok := constNode.Val.(*String); ok {
					// The grammar stores the normalization form from unicode_normal_form rule
					// which returns "NFC", "NFD", "NFKC", or "NFKD" (uppercase)
					normalForm = strVal.SVal
				}
			}
			result = fmt.Sprintf("%s is %s normalized", argStrs[0], normalForm)
		} else {
			result = argStrs[0] + " is normalized"
		}
	} else if strings.ToLower(funcName) == "system_user" && len(argStrs) == 0 {
		// SYSTEM_USER function call with no arguments should be deparsed as SYSTEM_USER (SQL value function)
		result = "SYSTEM_USER"
	} else if strings.ToLower(funcName) == "xmlexists" && f.Funcformat == COERCE_SQL_SYNTAX {
		// xmlexists function with SQL syntax: xmlexists(xpath PASSING [BY REF] document [BY REF])
		// The grammar converts xmlexists(A PASSING [BY REF] B [BY REF]) to xmlexists(A, B, ...)
		// We restore the xmlexists syntax using BY REF as separator between arguments
		if len(argStrs) >= 2 {
			passingArgs := strings.Join(argStrs[1:], " BY REF ")
			result = fmt.Sprintf("xmlexists(%s PASSING %s)", argStrs[0], passingArgs)
		} else {
			// Fallback for edge cases
			result = fmt.Sprintf("%s(%s)", funcName, funcArgs)
		}
	} else {
		result = fmt.Sprintf("%s(%s)", funcName, funcArgs)
	}

	// Add WITHIN GROUP clause for ordered-set aggregates
	if f.AggWithinGroup && f.AggOrder != nil && f.AggOrder.Len() > 0 {
		var orderItems []string
		for _, item := range f.AggOrder.Items {
			if item != nil {
				orderItems = append(orderItems, item.SqlString())
			}
		}
		result += " WITHIN GROUP (ORDER BY " + strings.Join(orderItems, ", ") + ")"
	}

	// Add FILTER clause for filtered aggregates
	if f.AggFilter != nil {
		result += " FILTER (WHERE " + f.AggFilter.SqlString() + ")"
	}

	// Add OVER clause for window functions
	if f.Over != nil {
		windowSpec := f.Over.SqlString()
		// Check if this is ONLY a window reference (no additional clauses)
		hasOnlyReference := f.Over.Refname != "" &&
			(f.Over.PartitionClause == nil || f.Over.PartitionClause.Len() == 0) &&
			(f.Over.OrderClause == nil || f.Over.OrderClause.Len() == 0) &&
			(f.Over.FrameOptions == 0 || f.Over.FrameOptions == FRAMEOPTION_DEFAULTS)

		if hasOnlyReference {
			// Pure window reference - no parentheses
			result += " OVER " + windowSpec
		} else {
			// Window specification or reference with additional clauses - with parentheses
			result += " OVER (" + windowSpec + ")"
		}
	}

	return result
}

func (f *FuncCall) ExpressionType() string {
	return "FUNC_CALL"
}

func (f *FuncCall) IsExpr() bool {
	return true
}

// A_Star represents an asterisk (*) in the parse tree, typically used in SELECT *.
// Ported from postgres/src/include/nodes/parsenodes.h:445-455
type A_Star struct {
	BaseNode
}

// NewA_Star creates a new A_Star node.
func NewA_Star(location int) *A_Star {
	aStar := &A_Star{
		BaseNode: BaseNode{Tag: T_A_Star},
	}
	aStar.SetLocation(location)
	return aStar
}

func (a *A_Star) String() string {
	return fmt.Sprintf("A_Star@%d", a.Location())
}

func (a *A_Star) ExpressionType() string {
	return "A_STAR"
}

func (a *A_Star) IsExpr() bool {
	return true
}

// SqlString returns the SQL representation of A_Star
func (a *A_Star) SqlString() string {
	return "*"
}

// A_Indices represents array indices in the parse tree (e.g., array[1:3]).
// Ported from postgres/src/include/nodes/parsenodes.h:456-462
type A_Indices struct {
	BaseNode
	IsSlice bool // True for slicing (e.g., array[1:3])
	Lidx    Node // Lower index (NULL if not specified)
	Uidx    Node // Upper index (NULL if not specified)
}

// NewA_Indices creates a new A_Indices node for single index access.
func NewA_Indices(idx Node, location int) *A_Indices {
	aIndices := &A_Indices{
		BaseNode: BaseNode{Tag: T_A_Indices},
		IsSlice:  false,
		Uidx:     idx,
	}
	aIndices.SetLocation(location)
	return aIndices
}

// NewA_IndicesSlice creates a new A_Indices node for slice access.
func NewA_IndicesSlice(lidx, uidx Node, location int) *A_Indices {
	aIndices := &A_Indices{
		BaseNode: BaseNode{Tag: T_A_Indices},
		IsSlice:  true,
		Lidx:     lidx,
		Uidx:     uidx,
	}
	aIndices.SetLocation(location)
	return aIndices
}

func (a *A_Indices) String() string {
	if a.IsSlice {
		return fmt.Sprintf("A_Indices{slice}@%d", a.Location())
	}
	return fmt.Sprintf("A_Indices{index}@%d", a.Location())
}

func (a *A_Indices) ExpressionType() string {
	return "A_INDICES"
}

func (a *A_Indices) IsExpr() bool {
	return true
}

// SqlString returns the SQL representation of A_Indices (handles both single index and slice)
func (a *A_Indices) SqlString() string {
	if a.IsSlice {
		// Slice syntax [lower:upper]
		var lower, upper string

		if a.Lidx != nil {
			lower = a.Lidx.SqlString()
		}

		if a.Uidx != nil {
			upper = a.Uidx.SqlString()
		}

		return fmt.Sprintf("[%s:%s]", lower, upper)
	} else {
		// Single index syntax [index]
		if a.Uidx != nil {
			return fmt.Sprintf("[%s]", a.Uidx.SqlString())
		}
		return "[]"
	}
}

// A_Indirection represents indirection (field access) in the parse tree (e.g., obj.field).
// Ported from postgres/src/include/nodes/parsenodes.h:479-488
type A_Indirection struct {
	BaseNode
	Arg         Node      // The base expression
	Indirection *NodeList // List of A_Indices and/or String nodes
}

// NewA_Indirection creates a new A_Indirection node.
func NewA_Indirection(arg Node, indirection *NodeList, location int) *A_Indirection {
	aIndirection := &A_Indirection{
		BaseNode:    BaseNode{Tag: T_A_Indirection},
		Arg:         arg,
		Indirection: indirection,
	}
	aIndirection.SetLocation(location)
	return aIndirection
}

func (a *A_Indirection) String() string {
	return fmt.Sprintf("A_Indirection@%d", a.Location())
}

func (a *A_Indirection) ExpressionType() string {
	return "A_INDIRECTION"
}

func (a *A_Indirection) IsExpr() bool {
	return true
}

// SqlString returns the SQL representation of A_Indirection
func (a *A_Indirection) SqlString() string {
	var result strings.Builder

	// Write the base expression
	if a.Arg != nil {
		if expr, ok := a.Arg.(Expression); ok {
			result.WriteString(expr.SqlString())
		} else if cn, ok := a.Arg.(*ColumnRef); ok {
			result.WriteString(cn.SqlString())
		} else {
			result.WriteString(a.Arg.String())
		}
	}

	// Handle indirections (field access or array subscripts)
	if a.Indirection != nil && len(a.Indirection.Items) > 0 {
		for _, ind := range a.Indirection.Items {
			switch indNode := ind.(type) {
			case *String:
				// Field access: obj.field
				result.WriteString(".")
				result.WriteString(indNode.SVal)
			case *A_Indices:
				// Array subscript: obj[index] or obj[lower:upper]
				result.WriteString(indNode.SqlString())
			case *A_Star:
				// Star expansion: obj.*
				result.WriteString(".*")
			default:
				// Fallback for unknown indirection types
				result.WriteString(".<unknown>")
			}
		}
	}

	return result.String()
}

// A_ArrayExpr represents an array expression in the parse tree (e.g., ARRAY[1,2,3]).
// Ported from postgres/src/include/nodes/parsenodes.h:489-501
type A_ArrayExpr struct {
	BaseNode
	Elements *NodeList // List of expressions
}

// NewA_ArrayExpr creates a new A_ArrayExpr node.
func NewA_ArrayExpr(elements *NodeList, location int) *A_ArrayExpr {
	aArrayExpr := &A_ArrayExpr{
		BaseNode: BaseNode{Tag: T_A_ArrayExpr},
		Elements: elements,
	}
	aArrayExpr.SetLocation(location)
	return aArrayExpr
}

func (a *A_ArrayExpr) String() string {
	elementCount := 0
	if a.Elements != nil {
		elementCount = len(a.Elements.Items)
	}
	return fmt.Sprintf("A_ArrayExpr{%d elements}@%d", elementCount, a.Location())
}

func (a *A_ArrayExpr) ExpressionType() string {
	return "A_ARRAY_EXPR"
}

func (a *A_ArrayExpr) IsExpr() bool {
	return true
}

// Note: CollateClause and TypeName already exist in ddl_statements.go
// Using existing implementations to avoid conflicts

// ColumnDef represents a complete column definition in CREATE TABLE.
// Ported from postgres/src/include/nodes/parsenodes.h:723-750
type ColumnDef struct {
	BaseNode
	Colname       string         // Name of the column
	TypeName      *TypeName      // Type of the column
	Compression   string         // Compression method, or NULL
	Inhcount      int            // Number of times column is inherited
	IsLocal       bool           // Column is defined locally
	IsNotNull     bool           // NOT NULL constraint specified
	IsFromType    bool           // Column definition came from table type
	StorageType   char           // Storage type (TOAST)
	StorageName   string         // Storage setting name or NULL for default
	RawDefault    Node           // Default value (untransformed parse tree)
	CookedDefault Node           // Default value (transformed)
	Identity      char           // IDENTITY property
	IdentitySeq   *RangeVar      // To store identity sequence name for ALTER TABLE
	Generated     char           // GENERATED property
	Collclause    *CollateClause // Collation, if any
	CollOid       Oid            // Collation OID (InvalidOid if not set)
	Constraints   *NodeList      // Column constraints
	Fdwoptions    *NodeList      // Foreign-data-wrapper specific options
}

// SqlString generates SQL representation of a column definition
func (c *ColumnDef) SqlString() string {
	parts := []string{QuoteIdentifier(c.Colname)}

	// Add type name
	if c.TypeName != nil {
		parts = append(parts, c.TypeName.SqlString())
	}

	// Add compression clause if specified
	if c.Compression != "" {
		parts = append(parts, "COMPRESSION", c.Compression)
	}

	// Add storage clause if specified
	if c.StorageName != "" {
		parts = append(parts, "STORAGE", c.StorageName)
	}

	// Add NOT NULL constraint if specified
	if c.IsNotNull {
		parts = append(parts, "NOT NULL")
	}

	// Add DEFAULT clause if specified
	if c.RawDefault != nil {
		parts = append(parts, "DEFAULT", c.RawDefault.SqlString())
	}

	// Add collation if specified
	if c.Collclause != nil {
		parts = append(parts, c.Collclause.SqlString())
	}

	// Add constraints if any
	if c.Constraints != nil && c.Constraints.Len() > 0 {
		for _, item := range c.Constraints.Items {
			if constraint, ok := item.(*Constraint); ok {
				// Handle identity constraints specially for column definitions
				if constraint.Contype == CONSTR_IDENTITY {
					// Build the identity specification with proper formatting for column definitions
					result := "GENERATED "
					switch constraint.GeneratedWhen {
					case ATTRIBUTE_IDENTITY_ALWAYS:
						result += "ALWAYS"
					case ATTRIBUTE_IDENTITY_BY_DEFAULT:
						result += "BY DEFAULT"
					}
					result += " AS IDENTITY"

					// Add sequence options in parentheses (without SET keywords)
					if constraint.Options != nil && len(constraint.Options.Items) > 0 {
						var optParts []string
						for _, optItem := range constraint.Options.Items {
							if defElem, ok := optItem.(*DefElem); ok {
								switch defElem.Defname {
								case "increment":
									if defElem.Arg != nil {
										optParts = append(optParts, "INCREMENT BY "+defElem.Arg.SqlString())
									}
								case "start":
									if defElem.Arg != nil {
										optParts = append(optParts, "START WITH "+defElem.Arg.SqlString())
									}
								case "restart":
									if defElem.Arg != nil {
										optParts = append(optParts, "RESTART WITH "+defElem.Arg.SqlString())
									} else {
										optParts = append(optParts, "RESTART")
									}
								case "maxvalue":
									if defElem.Arg != nil {
										optParts = append(optParts, "MAXVALUE "+defElem.Arg.SqlString())
									}
								case "minvalue":
									if defElem.Arg != nil {
										optParts = append(optParts, "MINVALUE "+defElem.Arg.SqlString())
									}
								case "cache":
									if defElem.Arg != nil {
										optParts = append(optParts, "CACHE "+defElem.Arg.SqlString())
									}
								case "cycle":
									if defElem.Arg != nil {
										if boolNode, ok := defElem.Arg.(*Boolean); ok {
											if boolNode.BoolVal {
												optParts = append(optParts, "CYCLE")
											} else {
												optParts = append(optParts, "NO CYCLE")
											}
										}
									}
								}
							}
						}
						if len(optParts) > 0 {
							result += " (" + strings.Join(optParts, " ") + ")"
						}
					}
					parts = append(parts, result)
				} else {
					// Use regular SqlString for non-identity constraints
					constraintStr := constraint.SqlString()
					if constraintStr != "" {
						parts = append(parts, constraintStr)
					}
				}
			}
		}
	}

	return strings.Join(parts, " ")
}

// NewColumnDef creates a new ColumnDef node.
func NewColumnDef(colname string, typeName *TypeName, location int) *ColumnDef {
	columnDef := &ColumnDef{
		BaseNode:    BaseNode{Tag: T_ColumnDef},
		Colname:     colname,
		TypeName:    typeName,
		Inhcount:    0,
		IsLocal:     true,
		IsNotNull:   false,
		IsFromType:  false,
		StorageType: 0,
		StorageName: "",
		Identity:    0,
		Generated:   0,
		CollOid:     InvalidOid,
		Constraints: NewNodeList(),
		Fdwoptions:  NewNodeList(),
	}
	columnDef.SetLocation(location)
	return columnDef
}

func (c *ColumnDef) String() string {
	return fmt.Sprintf("ColumnDef{%s}@%d", c.Colname, c.Location())
}

func (c *ColumnDef) StatementType() string {
	return "COLUMN_DEF"
}

// WithClause represents a complete WITH clause (Common Table Expression clause).
// Ported from postgres/src/include/nodes/parsenodes.h:1592-1605
type WithClause struct {
	BaseNode
	Ctes      *NodeList // List of CommonTableExpr nodes
	Recursive bool      // TRUE for WITH RECURSIVE
}

// NewWithClause creates a new WithClause node.
func NewWithClause(ctes *NodeList, recursive bool, location int) *WithClause {
	withClause := &WithClause{
		BaseNode:  BaseNode{Tag: T_WithClause},
		Ctes:      ctes,
		Recursive: recursive,
	}
	withClause.SetLocation(location)
	return withClause
}

func (w *WithClause) String() string {
	cteCount := 0
	if w.Ctes != nil {
		cteCount = len(w.Ctes.Items)
	}
	if w.Recursive {
		return fmt.Sprintf("WithClause{RECURSIVE, %d CTEs}@%d", cteCount, w.Location())
	}
	return fmt.Sprintf("WithClause{%d CTEs}@%d", cteCount, w.Location())
}

func (w *WithClause) ExpressionType() string {
	return "WITH_CLAUSE"
}

func (w *WithClause) IsExpr() bool {
	return true
}

// SqlString returns the SQL representation of the WithClause
func (w *WithClause) SqlString() string {
	if w.Ctes == nil || len(w.Ctes.Items) == 0 {
		return ""
	}

	parts := []string{"WITH"}

	if w.Recursive {
		parts = append(parts, "RECURSIVE")
	}

	var ctes []string
	for _, cte := range w.Ctes.Items {
		if cte != nil {
			ctes = append(ctes, cte.SqlString())
		}
	}

	parts = append(parts, strings.Join(ctes, ", "))

	return strings.Join(parts, " ")
}

// MultiAssignRef represents a multi-assignment reference (used in UPDATE (col1, col2) = (val1, val2)).
// Ported from postgres/src/include/nodes/parsenodes.h:532-542
type MultiAssignRef struct {
	BaseNode
	Source   Node // The sub-expression
	Colno    int  // Column number (1-based)
	Ncolumns int  // Number of columns in the multi-assignment
}

// NewMultiAssignRef creates a new MultiAssignRef node.
func NewMultiAssignRef(source Node, colno, ncolumns int, location int) *MultiAssignRef {
	multiAssignRef := &MultiAssignRef{
		BaseNode: BaseNode{Tag: T_MultiAssignRef},
		Source:   source,
		Colno:    colno,
		Ncolumns: ncolumns,
	}
	multiAssignRef.SetLocation(location)
	return multiAssignRef
}

func (m *MultiAssignRef) String() string {
	return fmt.Sprintf("MultiAssignRef{col %d of %d}@%d", m.Colno, m.Ncolumns, m.Location())
}

func (m *MultiAssignRef) ExpressionType() string {
	return "MULTI_ASSIGN_REF"
}

func (m *MultiAssignRef) IsExpr() bool {
	return true
}

// SqlString returns the SQL representation of MultiAssignRef
func (m *MultiAssignRef) SqlString() string {
	// MultiAssignRef represents a reference to a specific column in a multi-column assignment
	// In SQL, this appears as the source expression
	if m.Source != nil {
		return m.Source.SqlString()
	}
	return ""
}

// ==============================================================================
// SUPPORT INFRASTRUCTURE NODES
// ==============================================================================

// WindowDef represents a window definition in a WINDOW clause.
// Ported from postgres/src/include/nodes/parsenodes.h:561-583
type WindowDef struct {
	BaseNode
	Name            string    // Window name (NULL for inline windows)
	Refname         string    // Referenced window name, if any
	PartitionClause *NodeList // PARTITION BY expression list
	OrderClause     *NodeList // ORDER BY (list of SortBy)
	FrameOptions    int       // Frame_option flags
	StartOffset     Node      // Expression for start offset
	EndOffset       Node      // Expression for end offset
}

// Frame option constants for WindowDef FrameOptions field.
// Ported from postgres/src/include/nodes/parsenodes.h:581-605
const (
	FRAMEOPTION_NONDEFAULT                = 0x00001 // any specified?
	FRAMEOPTION_RANGE                     = 0x00002 // RANGE behavior
	FRAMEOPTION_ROWS                      = 0x00004 // ROWS behavior
	FRAMEOPTION_GROUPS                    = 0x00008 // GROUPS behavior
	FRAMEOPTION_BETWEEN                   = 0x00010 // BETWEEN given?
	FRAMEOPTION_START_UNBOUNDED_PRECEDING = 0x00020 // start is UNBOUNDED PRECEDING
	FRAMEOPTION_END_UNBOUNDED_PRECEDING   = 0x00040 // (disallowed)
	FRAMEOPTION_START_UNBOUNDED_FOLLOWING = 0x00080 // (disallowed)
	FRAMEOPTION_END_UNBOUNDED_FOLLOWING   = 0x00100 // end is UNBOUNDED FOLLOWING
	FRAMEOPTION_START_CURRENT_ROW         = 0x00200 // start is CURRENT ROW
	FRAMEOPTION_END_CURRENT_ROW           = 0x00400 // end is CURRENT ROW
	FRAMEOPTION_START_OFFSET_PRECEDING    = 0x00800 // start is OFFSET PRECEDING
	FRAMEOPTION_END_OFFSET_PRECEDING      = 0x01000 // end is OFFSET PRECEDING
	FRAMEOPTION_START_OFFSET_FOLLOWING    = 0x02000 // start is OFFSET FOLLOWING
	FRAMEOPTION_END_OFFSET_FOLLOWING      = 0x04000 // end is OFFSET FOLLOWING
	FRAMEOPTION_EXCLUDE_CURRENT_ROW       = 0x08000 // omit current row
	FRAMEOPTION_EXCLUDE_GROUP             = 0x10000 // omit current row & peers
	FRAMEOPTION_EXCLUDE_TIES              = 0x20000 // omit current row's peers

	// Compound options - postgres/src/include/nodes/parsenodes.h:600
	FRAMEOPTION_START_OFFSET = FRAMEOPTION_START_OFFSET_PRECEDING | FRAMEOPTION_START_OFFSET_FOLLOWING
	FRAMEOPTION_END_OFFSET   = FRAMEOPTION_END_OFFSET_PRECEDING | FRAMEOPTION_END_OFFSET_FOLLOWING
	FRAMEOPTION_DEFAULTS     = FRAMEOPTION_RANGE | FRAMEOPTION_START_UNBOUNDED_PRECEDING | FRAMEOPTION_END_CURRENT_ROW
)

// NewWindowDef creates a new WindowDef node.
func NewWindowDef(name string, location int) *WindowDef {
	windowDef := &WindowDef{
		BaseNode:     BaseNode{Tag: T_WindowDef},
		Name:         name,
		FrameOptions: FRAMEOPTION_DEFAULTS,
	}
	windowDef.SetLocation(location)
	return windowDef
}

func (w *WindowDef) String() string {
	if w.Name != "" {
		return fmt.Sprintf("WindowDef{%s}@%d", w.Name, w.Location())
	}
	return fmt.Sprintf("WindowDef{inline}@%d", w.Location())
}

func (w *WindowDef) StatementType() string {
	return "WINDOW_DEF"
}

func (w *WindowDef) SqlString() string {
	return w.SqlStringForContext(false)
}

// renderFrameOptions converts frame options to SQL string
func (w *WindowDef) renderFrameOptions() string {
	var parts []string

	// Frame mode (ROWS, RANGE, or GROUPS)
	if w.FrameOptions&FRAMEOPTION_ROWS != 0 {
		parts = append(parts, "ROWS")
	} else if w.FrameOptions&FRAMEOPTION_GROUPS != 0 {
		parts = append(parts, "GROUPS")
	} else if w.FrameOptions&FRAMEOPTION_RANGE != 0 {
		parts = append(parts, "RANGE")
	}

	// Handle BETWEEN clause
	if w.FrameOptions&FRAMEOPTION_BETWEEN != 0 {
		parts = append(parts, "BETWEEN")

		// Start boundary
		startBoundary := w.renderFrameBoundary(true)
		if startBoundary != "" {
			parts = append(parts, startBoundary)
		}

		parts = append(parts, "AND")

		// End boundary
		endBoundary := w.renderFrameBoundary(false)
		if endBoundary != "" {
			parts = append(parts, endBoundary)
		}
	} else {
		// Single boundary (no BETWEEN)
		boundary := w.renderFrameBoundary(true)
		if boundary != "" {
			parts = append(parts, boundary)
		}
	}

	// Handle exclusion clause
	if w.FrameOptions&FRAMEOPTION_EXCLUDE_CURRENT_ROW != 0 {
		parts = append(parts, "EXCLUDE CURRENT ROW")
	} else if w.FrameOptions&FRAMEOPTION_EXCLUDE_GROUP != 0 {
		parts = append(parts, "EXCLUDE GROUP")
	} else if w.FrameOptions&FRAMEOPTION_EXCLUDE_TIES != 0 {
		parts = append(parts, "EXCLUDE TIES")
	}

	return strings.Join(parts, " ")
}

// renderFrameBoundary renders a single frame boundary (start or end)
func (w *WindowDef) renderFrameBoundary(isStart bool) string {
	var parts []string

	if isStart {
		// Start boundary
		if w.FrameOptions&FRAMEOPTION_START_UNBOUNDED_PRECEDING != 0 {
			return "UNBOUNDED PRECEDING"
		} else if w.FrameOptions&FRAMEOPTION_START_UNBOUNDED_FOLLOWING != 0 {
			return "UNBOUNDED FOLLOWING"
		} else if w.FrameOptions&FRAMEOPTION_START_CURRENT_ROW != 0 {
			return "CURRENT ROW"
		} else if w.FrameOptions&FRAMEOPTION_START_OFFSET_PRECEDING != 0 {
			if w.StartOffset != nil {
				return w.StartOffset.SqlString() + " PRECEDING"
			}
			return "PRECEDING"
		} else if w.FrameOptions&FRAMEOPTION_START_OFFSET_FOLLOWING != 0 {
			if w.StartOffset != nil {
				return w.StartOffset.SqlString() + " FOLLOWING"
			}
			return "FOLLOWING"
		}
	} else {
		// End boundary
		if w.FrameOptions&FRAMEOPTION_END_UNBOUNDED_PRECEDING != 0 {
			return "UNBOUNDED PRECEDING"
		} else if w.FrameOptions&FRAMEOPTION_END_UNBOUNDED_FOLLOWING != 0 {
			return "UNBOUNDED FOLLOWING"
		} else if w.FrameOptions&FRAMEOPTION_END_CURRENT_ROW != 0 {
			return "CURRENT ROW"
		} else if w.FrameOptions&FRAMEOPTION_END_OFFSET_PRECEDING != 0 {
			if w.EndOffset != nil {
				return w.EndOffset.SqlString() + " PRECEDING"
			}
			return "PRECEDING"
		} else if w.FrameOptions&FRAMEOPTION_END_OFFSET_FOLLOWING != 0 {
			if w.EndOffset != nil {
				return w.EndOffset.SqlString() + " FOLLOWING"
			}
			return "FOLLOWING"
		}
	}

	return strings.Join(parts, " ")
}

func (w *WindowDef) SqlStringForContext(inWindowClause bool) string {
	var parts []string

	// Add window reference if present
	if w.Refname != "" {
		parts = append(parts, w.Refname)
	}

	// Add PARTITION BY clause
	if w.PartitionClause != nil && w.PartitionClause.Len() > 0 {
		var partitions []string
		for _, item := range w.PartitionClause.Items {
			if item != nil {
				partitions = append(partitions, item.SqlString())
			}
		}
		parts = append(parts, "PARTITION BY "+strings.Join(partitions, ", "))
	}

	// Add ORDER BY clause
	if w.OrderClause != nil && w.OrderClause.Len() > 0 {
		var orders []string
		for _, item := range w.OrderClause.Items {
			if item != nil {
				orders = append(orders, item.SqlString())
			}
		}
		parts = append(parts, "ORDER BY "+strings.Join(orders, ", "))
	}

	// Add frame specification
	if w.FrameOptions != 0 && w.FrameOptions != FRAMEOPTION_DEFAULTS {
		frameStr := w.renderFrameOptions()
		if frameStr != "" {
			parts = append(parts, frameStr)
		}
	}

	return strings.Join(parts, " ")
}

// Note: SortBy, SortByDir, and SortByNulls already exist in ddl_statements.go
// However, the existing SortBy is incomplete - let me implement a more complete version

// SortBy represents a sort specification in ORDER BY clauses.
// Ported from postgres/src/include/nodes/parsenodes.h:543-560
type SortBy struct {
	BaseNode
	Node        Node        // Expression to sort on
	SortbyDir   SortByDir   // ASC/DESC/USING/DEFAULT
	SortbyNulls SortByNulls // NULLS FIRST/LAST
	UseOp       *NodeList   // Name of operator to use for comparison
}

// NewSortBy creates a new complete SortBy node.
func NewSortBy(node Node, dir SortByDir, nulls SortByNulls, location int) *SortBy {
	sortBy := &SortBy{
		BaseNode:    BaseNode{Tag: T_SortBy},
		Node:        node,
		SortbyDir:   dir,
		SortbyNulls: nulls,
	}
	sortBy.SetLocation(location)
	return sortBy
}

func (s *SortBy) String() string {
	return fmt.Sprintf("SortBy@%d", s.Location())
}

func (s *SortBy) StatementType() string {
	return "SORT_BY"
}

// SqlString returns the SQL representation of the SortBy
func (s *SortBy) SqlString() string {
	if s.Node == nil {
		return ""
	}

	result := s.Node.SqlString()

	// Add sort direction
	switch s.SortbyDir {
	case SORTBY_ASC:
		result += " ASC"
	case SORTBY_DESC:
		result += " DESC"
	case SORTBY_USING:
		if s.UseOp != nil && s.UseOp.Len() > 0 {
			var parts []string
			for _, op := range s.UseOp.Items {
				if str, ok := op.(*String); ok {
					// Use raw string value (without quotes) for operators
					parts = append(parts, str.SVal)
				} else if op != nil {
					parts = append(parts, op.String())
				}
			}

			if len(parts) > 1 {
				// Multiple parts: use OPERATOR(schema.op) syntax
				opName := strings.Join(parts, ".")
				result += " USING OPERATOR(" + opName + ")"
			} else if len(parts) == 1 {
				// Single part: use direct operator syntax
				result += " USING " + parts[0]
			}
		}
	}

	// Add null ordering
	switch s.SortbyNulls {
	case SORTBY_NULLS_FIRST:
		result += " NULLS FIRST"
	case SORTBY_NULLS_LAST:
		result += " NULLS LAST"
	}

	return result
}

// GroupingSet represents a grouping set in GROUP BY clauses.
// Ported from postgres/src/include/nodes/parsenodes.h:1506-1517
type GroupingSet struct {
	BaseNode
	Kind    GroupingSetKind // Type of grouping set
	Content *NodeList       // List of expressions
}

// GroupingSetKind represents the type of grouping set.
// Ported from postgres/src/include/nodes/parsenodes.h:1490-1505
type GroupingSetKind int

const (
	GROUPING_SET_EMPTY GroupingSetKind = iota
	GROUPING_SET_SIMPLE
	GROUPING_SET_ROLLUP
	GROUPING_SET_CUBE
	GROUPING_SET_SETS
)

// NewGroupingSet creates a new GroupingSet node.
func NewGroupingSet(kind GroupingSetKind, content *NodeList, location int) *GroupingSet {
	groupingSet := &GroupingSet{
		BaseNode: BaseNode{Tag: T_GroupingSet},
		Kind:     kind,
		Content:  content,
	}
	groupingSet.SetLocation(location)
	return groupingSet
}

func (g *GroupingSet) String() string {
	return fmt.Sprintf("GroupingSet{kind=%d}@%d", g.Kind, g.Location())
}

func (g *GroupingSet) StatementType() string {
	return "GROUPING_SET"
}

// SqlString returns the SQL representation of the GroupingSet
func (g *GroupingSet) SqlString() string {
	switch g.Kind {
	case GROUPING_SET_EMPTY:
		return "()"
	case GROUPING_SET_SIMPLE:
		// Simple grouping set - just return the content expressions
		if g.Content != nil && len(g.Content.Items) > 0 {
			var items []string
			for _, item := range g.Content.Items {
				if item != nil {
					items = append(items, item.SqlString())
				}
			}
			return strings.Join(items, ", ")
		}
		return ""
	case GROUPING_SET_ROLLUP:
		// ROLLUP(expr1, expr2, ...)
		if g.Content != nil && len(g.Content.Items) > 0 {
			var items []string
			for _, item := range g.Content.Items {
				if item != nil {
					items = append(items, item.SqlString())
				}
			}
			return fmt.Sprintf("ROLLUP(%s)", strings.Join(items, ", "))
		}
		return "ROLLUP()"
	case GROUPING_SET_CUBE:
		// CUBE(expr1, expr2, ...)
		if g.Content != nil && len(g.Content.Items) > 0 {
			var items []string
			for _, item := range g.Content.Items {
				if item != nil {
					items = append(items, item.SqlString())
				}
			}
			return fmt.Sprintf("CUBE(%s)", strings.Join(items, ", "))
		}
		return "CUBE()"
	case GROUPING_SET_SETS:
		// GROUPING SETS((expr1), (expr2), ...)
		if g.Content != nil && len(g.Content.Items) > 0 {
			var sets []string
			for _, item := range g.Content.Items {
				if item != nil {
					// Each item should be a GroupingSet or expression
					if gs, ok := item.(*GroupingSet); ok {
						// Handle different GroupingSet kinds
						switch gs.Kind {
						case GROUPING_SET_EMPTY:
							// Empty grouping set: ()
							sets = append(sets, "()")
						case GROUPING_SET_SIMPLE:
							// Simple grouping set: (expr1, expr2)
							sets = append(sets, fmt.Sprintf("(%s)", gs.SqlString()))
						default:
							// Other grouping sets (ROLLUP, CUBE, etc)
							sets = append(sets, gs.SqlString())
						}
					} else if _, ok := item.(*ParenExpr); ok {
						// Parenthesized expression - already has parentheses
						sets = append(sets, item.SqlString())
					} else if _, ok := item.(*RowExpr); ok {
						// Row expression - already has parentheses in its SqlString
						sets = append(sets, item.SqlString())
					} else {
						// Simple expression - needs parentheses
						sets = append(sets, fmt.Sprintf("(%s)", item.SqlString()))
					}
				}
			}
			return fmt.Sprintf("GROUPING SETS (%s)", strings.Join(sets, ", "))
		}
		return "GROUPING SETS ()"
	default:
		return "UNKNOWN_GROUPING_SET"
	}
}

// LockingClause represents a complete locking clause (FOR UPDATE, FOR SHARE, etc.).
// Ported from postgres/src/include/nodes/parsenodes.h:831-841
type LockingClause struct {
	BaseNode
	LockedRels *NodeList          // For table locking, list of RangeVar nodes
	Strength   LockClauseStrength // Lock strength
	WaitPolicy LockWaitPolicy     // NOWAIT and SKIP LOCKED
}

// LockClauseStrength represents lock strength.
// Ported from postgres/src/include/nodes/parsenodes.h:61-67
type LockClauseStrength int

const (
	LCS_NONE LockClauseStrength = iota
	LCS_FORKEYSHARE
	LCS_FORSHARE
	LCS_FORNOKEYUPDATE
	LCS_FORUPDATE
)

// Note: LockWaitPolicy already exists in query_execution_nodes.go

// NewLockingClause creates a new LockingClause node.
func NewLockingClause(lockedRels *NodeList, strength LockClauseStrength, waitPolicy LockWaitPolicy, location int) *LockingClause {
	lockingClause := &LockingClause{
		BaseNode:   BaseNode{Tag: T_LockingClause},
		LockedRels: lockedRels,
		Strength:   strength,
		WaitPolicy: waitPolicy,
	}
	lockingClause.SetLocation(location)
	return lockingClause
}

func (l *LockingClause) String() string {
	return fmt.Sprintf("LockingClause{strength=%d}@%d", l.Strength, l.Location())
}

func (l *LockingClause) StatementType() string {
	return "LOCKING_CLAUSE"
}

// SqlString returns the SQL representation of the LockingClause
func (l *LockingClause) SqlString() string {
	parts := []string{}

	// Determine locking strength
	switch l.Strength {
	case LCS_FORKEYSHARE:
		parts = append(parts, "FOR KEY SHARE")
	case LCS_FORSHARE:
		parts = append(parts, "FOR SHARE")
	case LCS_FORNOKEYUPDATE:
		parts = append(parts, "FOR NO KEY UPDATE")
	case LCS_FORUPDATE:
		parts = append(parts, "FOR UPDATE")
	}

	// Add table names if specified
	if l.LockedRels != nil && l.LockedRels.Len() > 0 {
		var tables []string
		for _, item := range l.LockedRels.Items {
			if rel, ok := item.(*RangeVar); ok && rel != nil {
				tables = append(tables, rel.SqlString())
			}
		}
		parts = append(parts, "OF", strings.Join(tables, ", "))
	}

	// Add wait policy
	switch l.WaitPolicy {
	case LockWaitSkip:
		parts = append(parts, "SKIP LOCKED")
	case LockWaitError:
		parts = append(parts, "NOWAIT")
	}

	return strings.Join(parts, " ")
}

// XmlSerialize represents an XML serialization expression.
// Ported from postgres/src/include/nodes/parsenodes.h:842-859
type XmlSerialize struct {
	BaseNode
	XmlOptionType XmlOptionType // DOCUMENT or CONTENT
	Expr          Node          // Expression to serialize
	TypeName      *TypeName     // Target type
	Indent        bool          // INDENT option
}

// XmlOptionType represents XML option types.
// Ported from postgres/src/include/nodes/parsenodes.h:76-80
type XmlOptionType int

const (
	XMLOPTION_DOCUMENT XmlOptionType = iota
	XMLOPTION_CONTENT
)

// XmlStandaloneType represents XML standalone options for XMLROOT
type XmlStandaloneType int

const (
	XML_STANDALONE_YES XmlStandaloneType = iota
	XML_STANDALONE_NO
	XML_STANDALONE_NO_VALUE
	XML_STANDALONE_OMITTED
)

// NewXmlSerialize creates a new XmlSerialize node.
func NewXmlSerialize(xmlOptionType XmlOptionType, expr Node, typeName *TypeName, indent bool, location int) *XmlSerialize {
	xmlSerialize := &XmlSerialize{
		BaseNode:      BaseNode{Tag: T_XmlSerialize},
		XmlOptionType: xmlOptionType,
		Expr:          expr,
		TypeName:      typeName,
		Indent:        indent,
	}
	xmlSerialize.SetLocation(location)
	return xmlSerialize
}

func (x *XmlSerialize) String() string {
	return fmt.Sprintf("XmlSerialize@%d", x.Location())
}

func (x *XmlSerialize) ExpressionType() string {
	return "XML_SERIALIZE"
}

func (x *XmlSerialize) IsExpr() bool {
	return true
}

// SqlString returns the SQL representation of XmlSerialize
func (x *XmlSerialize) SqlString() string {
	var result strings.Builder
	result.WriteString("XMLSERIALIZE(")

	// Add DOCUMENT or CONTENT
	switch x.XmlOptionType {
	case XMLOPTION_DOCUMENT:
		result.WriteString("DOCUMENT ")
	case XMLOPTION_CONTENT:
		result.WriteString("CONTENT ")
	}

	// Add the expression
	if x.Expr != nil {
		result.WriteString(x.Expr.SqlString())
	}

	// Add AS TYPE
	if x.TypeName != nil {
		result.WriteString(" AS ")
		result.WriteString(x.TypeName.SqlString())
	}

	// Add INDENT if specified
	if x.Indent {
		result.WriteString(" INDENT")
	}

	result.WriteString(")")
	return result.String()
}

// PartitionElem represents a partition element in partition specifications.
// Ported from postgres/src/include/nodes/parsenodes.h:860-881
type PartitionElem struct {
	BaseNode
	Name      string    // Name of column to partition on
	Expr      Node      // Expression to partition on, or NULL
	Collation *NodeList // Collation name
	Opclass   *NodeList // Operator class name
}

// NewPartitionElem creates a new PartitionElem node.
func NewPartitionElem(name string, expr Node, location int) *PartitionElem {
	partitionElem := &PartitionElem{
		BaseNode:  BaseNode{Tag: T_PartitionElem},
		Name:      name,
		Expr:      expr,
		Collation: NewNodeList(),
		Opclass:   NewNodeList(),
	}
	partitionElem.SetLocation(location)
	return partitionElem
}

func (p *PartitionElem) String() string {
	if p.Name != "" {
		return fmt.Sprintf("PartitionElem{%s}@%d", p.Name, p.Location())
	}
	return fmt.Sprintf("PartitionElem{expr}@%d", p.Location())
}

// SqlString returns the SQL representation of PartitionElem
func (p *PartitionElem) SqlString() string {
	var result string

	if p.Name != "" {
		result = p.Name
	} else if p.Expr != nil {
		result = p.Expr.SqlString()
	} else {
		return ""
	}

	// Add COLLATE clause if present
	if p.Collation != nil && p.Collation.Len() > 0 {
		// Collation names should be output as identifiers, not string literals
		var collationParts []string
		for _, item := range p.Collation.Items {
			if strNode, ok := item.(*String); ok {
				// Quote as identifier if needed
				collationParts = append(collationParts, QuoteIdentifier(strNode.SVal))
			} else if item != nil {
				collationParts = append(collationParts, item.SqlString())
			}
		}
		if len(collationParts) > 0 {
			result += " COLLATE " + strings.Join(collationParts, ".")
		}
	}

	// Add operator class if present
	if p.Opclass != nil && p.Opclass.Len() > 0 {
		// Operator class names should be output as identifiers, not string literals
		var opclassParts []string
		for _, item := range p.Opclass.Items {
			if strNode, ok := item.(*String); ok {
				// Quote as identifier if needed
				opclassParts = append(opclassParts, QuoteIdentifier(strNode.SVal))
			} else if item != nil {
				opclassParts = append(opclassParts, item.SqlString())
			}
		}
		if len(opclassParts) > 0 {
			result += " " + strings.Join(opclassParts, ".")
		}
	}

	return result
}

func (p *PartitionElem) StatementType() string {
	return "PARTITION_ELEM"
}

// TableSampleClause represents a TABLESAMPLE clause.
// Ported from postgres/src/include/nodes/parsenodes.h:1344-1367
type TableSampleClause struct {
	BaseNode
	Tsmhandler Oid       // OID of the tablesample handler function
	Args       *NodeList // List of tablesample arguments
	Repeatable Expr      // REPEATABLE expression, or NULL
}

// NewTableSampleClause creates a new TableSampleClause node.
func NewTableSampleClause(tsmhandler Oid, args *NodeList, repeatable Expr, location int) *TableSampleClause {
	tableSampleClause := &TableSampleClause{
		BaseNode:   BaseNode{Tag: T_TableSampleClause},
		Tsmhandler: tsmhandler,
		Args:       args,
		Repeatable: repeatable,
	}
	tableSampleClause.SetLocation(location)
	return tableSampleClause
}

func (t *TableSampleClause) String() string {
	return fmt.Sprintf("TableSampleClause@%d", t.Location())
}

func (t *TableSampleClause) StatementType() string {
	return "TABLE_SAMPLE_CLAUSE"
}

// ObjectWithArgs represents an object name with arguments (used for functions, operators, etc.).
// Ported from postgres/src/include/nodes/parsenodes.h:2524-2539
type ObjectWithArgs struct {
	BaseNode
	Objname         *NodeList // Qualified object name
	Objargs         *NodeList // List of argument types (TypeName nodes)
	ObjfuncArgs     *NodeList // List of function arguments for ALTER FUNCTION
	ArgsUnspecified bool      // Arguments were omitted, so name must be unique
}

// NewObjectWithArgs creates a new ObjectWithArgs node.
func NewObjectWithArgs(objname *NodeList, objargs *NodeList, argsUnspecified bool, location int) *ObjectWithArgs {
	objectWithArgs := &ObjectWithArgs{
		BaseNode:        BaseNode{Tag: T_ObjectWithArgs},
		Objname:         objname,
		Objargs:         objargs,
		ObjfuncArgs:     NewNodeList(),
		ArgsUnspecified: argsUnspecified,
	}
	objectWithArgs.SetLocation(location)
	return objectWithArgs
}

func (o *ObjectWithArgs) String() string {
	if o.Objname != nil && len(o.Objname.Items) > 0 {
		if str, ok := o.Objname.Items[len(o.Objname.Items)-1].(*String); ok {
			return fmt.Sprintf("ObjectWithArgs{%s}@%d", str.SVal, o.Location())
		}
	}
	return fmt.Sprintf("ObjectWithArgs@%d", o.Location())
}

func (o *ObjectWithArgs) StatementType() string {
	return "OBJECT_WITH_ARGS"
}

// SqlString returns the SQL representation of ObjectWithArgs
func (o *ObjectWithArgs) SqlString() string {
	var parts []string

	// Add object name
	if o.Objname != nil && o.Objname.Len() > 0 {
		var names []string
		for _, item := range o.Objname.Items {
			if str, ok := item.(*String); ok {
				names = append(names, str.SVal)
			}
		}
		if len(names) > 0 {
			parts = append(parts, strings.Join(names, "."))
		}
	}

	// Add arguments if specified
	if !o.ArgsUnspecified {
		// Check if this is an aggregate with ObjfuncArgs from aggr_args
		// If so, use the DefineStmt logic to properly format ORDER BY and VARIADIC
		if o.ObjfuncArgs != nil {
			// ObjfuncArgs is already a *NodeList, no need for type assertion
			args := formatAggrArgsList(o.ObjfuncArgs)
			if args != "" {
				parts = append(parts, args)
			} else {
				// Fall back to regular Objargs formatting
				if o.Objargs != nil {
					var args []string
					for _, item := range o.Objargs.Items {
						if item == nil {
							args = append(args, "NONE")
							continue
						}
						args = append(args, item.SqlString())
					}
					parts = append(parts, "("+strings.Join(args, ", ")+")")
				}
			}
		} else if o.Objargs != nil {
			// Regular case - use Objargs
			// Special case: if ObjfuncArgs is nil and Objargs is empty, this is a star aggregate
			if o.ObjfuncArgs == nil && o.Objargs.Len() == 0 {
				parts = append(parts, "(*)")
			} else {
				var args []string
				for _, item := range o.Objargs.Items {
					if item == nil {
						args = append(args, "NONE")
						continue
					}
					args = append(args, item.SqlString())
				}
				parts = append(parts, "("+strings.Join(args, ", ")+")")
			}
		}
	}

	return strings.Join(parts, "")
}

// formatAggrArgsList formats aggregate argument list with proper ORDER BY and VARIADIC syntax
// This is similar to the logic in DefineStmt.SqlString() for aggregates
func formatAggrArgsList(argsList *NodeList) string {
	if argsList == nil || argsList.Len() == 0 {
		return ""
	}

	// For aggr_args, we expect the full function parameters list, not the 2-element structure
	// But we need to check if it's actually a 2-element structure from aggr_args
	if argsList.Len() == 2 {
		if argListNode, ok := argsList.Items[0].(*NodeList); ok {
			if numDirectNode, ok := argsList.Items[1].(*Integer); ok {
				// This is the aggr_args [args, numDirectArgs] structure
				return formatAggrArgsStructure(argListNode, int(numDirectNode.IVal))
			}
		}
	}

	// Fall back to regular function parameter formatting
	var argStrs []string
	for _, item := range argsList.Items {
		if item == nil {
			argStrs = append(argStrs, "*")
		} else {
			argStrs = append(argStrs, item.SqlString())
		}
	}

	if len(argStrs) == 1 && argStrs[0] == "*" {
		return "(*)"
	} else if len(argStrs) > 0 {
		return "(" + strings.Join(argStrs, ", ") + ")"
	}

	return ""
}

// formatAggrArgsStructure formats the [args, numDirectArgs] structure from aggr_args
func formatAggrArgsStructure(argList *NodeList, numDirectArgs int) string {
	switch numDirectArgs {
	case -1:
		// Regular aggregate or COUNT(*)
		if argList == nil || argList.Len() == 0 {
			// COUNT(*) case
			return "(*)"
		} else {
			var argStrs []string
			for _, item := range argList.Items {
				switch arg := item.(type) {
				case *TypeName:
					argStrs = append(argStrs, arg.SqlString())
				case *FunctionParameter:
					argStrs = append(argStrs, arg.SqlString())
				}
			}
			if len(argStrs) > 0 {
				return "(" + strings.Join(argStrs, ", ") + ")"
			}
		}
	case 0:
		// Ordered-set aggregate without direct args: (ORDER BY args)
		if argList != nil {
			var argStrs []string
			for _, item := range argList.Items {
				switch arg := item.(type) {
				case *TypeName:
					argStrs = append(argStrs, arg.SqlString())
				case *FunctionParameter:
					argStrs = append(argStrs, arg.SqlString())
				}
			}
			if len(argStrs) > 0 {
				return "(ORDER BY " + strings.Join(argStrs, ", ") + ")"
			}
		}
	default:
		// Hypothetical-set aggregate: (direct_args ORDER BY ordered_args)
		if argList != nil {
			var directArgs []string
			var orderedArgs []string

			for i, item := range argList.Items {
				var argStr string
				switch arg := item.(type) {
				case *TypeName:
					argStr = arg.SqlString()
				case *FunctionParameter:
					argStr = arg.SqlString()
				}

				if i < numDirectArgs {
					directArgs = append(directArgs, argStr)
				} else {
					orderedArgs = append(orderedArgs, argStr)
				}
			}

			if len(orderedArgs) > 0 {
				return "(" + strings.Join(directArgs, ", ") + " ORDER BY " + strings.Join(orderedArgs, ", ") + ")"
			} else if len(directArgs) > 0 {
				return "(" + strings.Join(directArgs, ", ") + ")"
			}
		}
	}

	return ""
}

// ExtractArgTypes extracts argument types from function arguments
// This function is used to convert FunctionParameter nodes to TypeName nodes
func ExtractArgTypes(funcArgs *NodeList) *NodeList {
	if funcArgs == nil {
		return nil
	}

	argTypes := NewNodeList()
	for i := 0; i < funcArgs.Len(); i++ {
		if funcParam, ok := funcArgs.Items[i].(*FunctionParameter); ok {
			if funcParam.ArgType != nil {
				argTypes.Append(funcParam.ArgType)
			}
		}
	}

	return argTypes
}

// NewEmptyObjectWithArgs creates a new ObjectWithArgs node with empty constructor
func NewEmptyObjectWithArgs() *ObjectWithArgs {
	return &ObjectWithArgs{
		BaseNode:        BaseNode{Tag: T_ObjectWithArgs},
		Objname:         nil,
		Objargs:         nil,
		ObjfuncArgs:     nil,
		ArgsUnspecified: false,
	}
}

// SinglePartitionSpec represents a single partition specification.
// Ported from postgres/src/include/nodes/parsenodes.h:945-952
type SinglePartitionSpec struct {
	BaseNode
}

// NewSinglePartitionSpec creates a new SinglePartitionSpec node.
func NewSinglePartitionSpec(location int) *SinglePartitionSpec {
	singlePartitionSpec := &SinglePartitionSpec{
		BaseNode: BaseNode{Tag: T_SinglePartitionSpec},
	}
	singlePartitionSpec.SetLocation(location)
	return singlePartitionSpec
}

func (s *SinglePartitionSpec) String() string {
	return fmt.Sprintf("SinglePartitionSpec@%d", s.Location())
}

func (s *SinglePartitionSpec) StatementType() string {
	return "SINGLE_PARTITION_SPEC"
}

// PartitionCmd represents a partition command in ALTER TABLE.
// Ported from postgres/src/include/nodes/parsenodes.h:953-964
type PartitionCmd struct {
	BaseNode
	Name       *RangeVar           // Name of the partition
	Bound      *PartitionBoundSpec // Partition bound specification
	Concurrent bool                // CONCURRENTLY option
}

// NewPartitionCmd creates a new PartitionCmd node.
func NewPartitionCmd(name *RangeVar, bound *PartitionBoundSpec, concurrent bool, location int) *PartitionCmd {
	partitionCmd := &PartitionCmd{
		BaseNode:   BaseNode{Tag: T_PartitionCmd},
		Name:       name,
		Bound:      bound,
		Concurrent: concurrent,
	}
	partitionCmd.SetLocation(location)
	return partitionCmd
}

func (p *PartitionCmd) String() string {
	return fmt.Sprintf("PartitionCmd@%d", p.Location())
}

func (p *PartitionCmd) SqlString() string {
	parts := []string{}

	// Add the partition name
	if p.Name != nil {
		parts = append(parts, p.Name.SqlString())
	}

	// Add the partition bound specification if present (for ATTACH PARTITION)
	if p.Bound != nil {
		parts = append(parts, p.Bound.SqlString())
	}

	return strings.Join(parts, " ")
}

func (p *PartitionCmd) StatementType() string {
	return "PARTITION_CMD"
}

// ==============================================================================
// SUPPORTING CONSTANTS AND HELPER TYPES
// ==============================================================================

// char represents a single character (PostgreSQL char type).
type char byte
