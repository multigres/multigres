// Package ast provides PostgreSQL AST expression node definitions.
// Ported from postgres/src/include/nodes/primnodes.h
package ast

import (
	"fmt"
	"strings"
)

// ==============================================================================
// EXPRESSION FRAMEWORK - PostgreSQL primnodes.h implementation
// Ported from postgres/src/include/nodes/primnodes.h
// ==============================================================================

// Supporting types for expressions

// Oid represents an object identifier - ported from postgres/src/include/postgres_ext.h
type Oid uint32

// AttrNumber represents an attribute number - ported from postgres/src/include/access/attnum.h:21
type AttrNumber int16

// Index represents an array index - ported from postgres/src/include/c.h:614
type Index uint32

// Datum represents a PostgreSQL datum - ported from postgres/src/include/postgres.h:64
type Datum uintptr

// CoercionForm represents type coercion forms - ported from postgres/src/include/nodes/primnodes.h:732-737
type CoercionForm int

const (
	COERCE_EXPLICIT_CALL CoercionForm = iota // Explicit function call syntax
	COERCE_EXPLICIT_CAST                     // Explicit cast syntax
	COERCE_IMPLICIT_CAST                     // Implicit cast
	COERCE_SQL_SYNTAX                        // SQL standard syntax
)

// ParamKind represents parameter types - ported from postgres/src/include/nodes/primnodes.h:365-371
type ParamKind int

const (
	PARAM_EXTERN    ParamKind = iota // External parameter
	PARAM_EXEC                       // Executor internal parameter
	PARAM_SUBLINK                    // Sublink output column
	PARAM_MULTIEXPR                  // Multiexpr sublink column
)

// BoolExprType represents boolean expression types - ported from postgres/src/include/nodes/primnodes.h:929-932
type BoolExprType int

const (
	AND_EXPR BoolExprType = iota // AND expression
	OR_EXPR                      // OR expression
	NOT_EXPR                     // NOT expression
)

func (b BoolExprType) String() string {
	switch b {
	case AND_EXPR:
		return "AND"
	case OR_EXPR:
		return "OR"
	case NOT_EXPR:
		return "NOT"
	default:
		return fmt.Sprintf("BoolExprType(%d)", int(b))
	}
}

// ==============================================================================
// BASE EXPRESSION INTERFACE
// ==============================================================================

// Expr is the abstract base type for all expression nodes.
// Ported from postgres/src/include/nodes/primnodes.h:187-192
type Expr interface {
	Node
	ExpressionType() string
	IsExpr() bool
}

// BaseExpr provides common expression functionality.
type BaseExpr struct {
	BaseNode
}

func (e *BaseExpr) IsExpr() bool {
	return true
}

// ==============================================================================
// TIER 1 EXPRESSIONS - Foundation Reference and Function Expressions
// ==============================================================================

// Var represents a reference to a table column.
// Ported from postgres/src/include/nodes/primnodes.h:247
type Var struct {
	BaseExpr
	Varno     int        // Relation index in range table - postgres/src/include/nodes/primnodes.h:249
	Varattno  AttrNumber // Attribute number (0 = whole-row) - postgres/src/include/nodes/primnodes.h:250
	Vartype   Oid        // pg_type OID - postgres/src/include/nodes/primnodes.h:251
	Vartypmod int32      // Type modifier - postgres/src/include/nodes/primnodes.h:252
	Varcollid Oid        // Collation OID - postgres/src/include/nodes/primnodes.h:253
	// TODO: varnullingrels *Bitmapset not yet ported - postgres/src/include/nodes/primnodes.h:274
	Varlevelsup Index      // Subquery nesting level - postgres/src/include/nodes/primnodes.h:265
	Varnosyn    Index      // Syntactic relation index - postgres/src/include/nodes/primnodes.h:276
	Varattnosyn AttrNumber // Syntactic attribute number - postgres/src/include/nodes/primnodes.h:277
}

// NewVar creates a new Var node.
func NewVar(varno int, varattno AttrNumber, vartype Oid) *Var {
	return &Var{
		BaseExpr: BaseExpr{BaseNode: BaseNode{Tag: T_Var}},
		Varno:    varno,
		Varattno: varattno,
		Vartype:  vartype,
	}
}

func (v *Var) ExpressionType() string {
	return "Var"
}

func (v *Var) String() string {
	return fmt.Sprintf("Var(%d.%d)@%d", v.Varno, v.Varattno, v.Location())
}

// Const represents a constant value in an expression.
// Ported from postgres/src/include/nodes/primnodes.h:306
type Const struct {
	BaseExpr
	Consttype   Oid   // Datatype OID - postgres/src/include/nodes/primnodes.h:308
	Consttypmod int32 // Type modifier - postgres/src/include/nodes/primnodes.h:309
	Constcollid Oid   // Collation OID - postgres/src/include/nodes/primnodes.h:310
	Constlen    int   // Type length - postgres/src/include/nodes/primnodes.h:311
	Constvalue  Datum // The actual value - postgres/src/include/nodes/primnodes.h:312
	Constisnull bool  // Whether null - postgres/src/include/nodes/primnodes.h:313
	Constbyval  bool  // Pass by value? - postgres/src/include/nodes/primnodes.h:315
}

// NewConst creates a new Const node.
func NewConst(consttype Oid, constvalue Datum, constisnull bool) *Const {
	return &Const{
		BaseExpr:    BaseExpr{BaseNode: BaseNode{Tag: T_Const}},
		Consttype:   consttype,
		Constvalue:  constvalue,
		Constisnull: constisnull,
	}
}

func (c *Const) ExpressionType() string {
	return "Const"
}

func (c *Const) String() string {
	if c.Constisnull {
		return fmt.Sprintf("Const(NULL)@%d", c.Location())
	}
	return fmt.Sprintf("Const(%v)@%d", c.Constvalue, c.Location())
}

// Param represents a parameter reference in a prepared statement.
// Ported from postgres/src/include/nodes/primnodes.h:373
type Param struct {
	BaseExpr
	Paramkind   ParamKind // Parameter type - postgres/src/include/nodes/primnodes.h:390
	Paramid     int       // Parameter ID - postgres/src/include/nodes/primnodes.h:391
	Paramtype   Oid       // Datatype OID - postgres/src/include/nodes/primnodes.h:392
	Paramtypmod int32     // Type modifier - postgres/src/include/nodes/primnodes.h:393
	Paramcollid Oid       // Collation OID - postgres/src/include/nodes/primnodes.h:394
}

// NewParam creates a new Param node.
func NewParam(paramkind ParamKind, paramid int, paramtype Oid) *Param {
	return &Param{
		BaseExpr:  BaseExpr{BaseNode: BaseNode{Tag: T_Param}},
		Paramkind: paramkind,
		Paramid:   paramid,
		Paramtype: paramtype,
	}
}

func (p *Param) ExpressionType() string {
	return "Param"
}

func (p *Param) String() string {
	return fmt.Sprintf("Param($%d)@%d", p.Paramid, p.Location())
}

// FuncExpr represents a function call expression.
// Ported from postgres/src/include/nodes/primnodes.h:746
type FuncExpr struct {
	BaseExpr
	Funcid         Oid          // pg_proc OID - postgres/src/include/nodes/primnodes.h:749
	Funcresulttype Oid          // Result type OID - postgres/src/include/nodes/primnodes.h:750
	Funcretset     bool         // Returns set? - postgres/src/include/nodes/primnodes.h:751
	Funcvariadic   bool         // Variadic arguments? - postgres/src/include/nodes/primnodes.h:752
	Funcformat     CoercionForm // Display format - postgres/src/include/nodes/primnodes.h:753
	Funccollid     Oid          // Result collation - postgres/src/include/nodes/primnodes.h:754
	Inputcollid    Oid          // Input collation - postgres/src/include/nodes/primnodes.h:755
	Args           *NodeList    // Function arguments - postgres/src/include/nodes/primnodes.h:756
}

// NewFuncExpr creates a new FuncExpr node.
func NewFuncExpr(funcid Oid, funcresulttype Oid, args *NodeList) *FuncExpr {
	return &FuncExpr{
		BaseExpr:       BaseExpr{BaseNode: BaseNode{Tag: T_FuncExpr}},
		Funcid:         funcid,
		Funcresulttype: funcresulttype,
		Args:           args,
	}
}

func (f *FuncExpr) ExpressionType() string {
	return "FuncExpr"
}

func (f *FuncExpr) String() string {
	argCount := 0
	if f.Args != nil {
		argCount = len(f.Args.Items)
	}
	return fmt.Sprintf("FuncExpr(oid:%d, %d args)@%d", f.Funcid, argCount, f.Location())
}

// OpExpr represents a binary or unary operator expression.
// Ported from postgres/src/include/nodes/primnodes.h:813
type OpExpr struct {
	BaseExpr
	Opno         Oid    // pg_operator OID - postgres/src/include/nodes/primnodes.h:816
	Opfuncid     Oid    // Underlying function OID - postgres/src/include/nodes/primnodes.h:817
	Opresulttype Oid    // Result type - postgres/src/include/nodes/primnodes.h:818
	Opretset     bool   // Returns set? - postgres/src/include/nodes/primnodes.h:819
	Opcollid     Oid    // Result collation - postgres/src/include/nodes/primnodes.h:820
	Inputcollid  Oid    // Input collation - postgres/src/include/nodes/primnodes.h:821
	Args         *NodeList // Operator arguments (1 or 2) - postgres/src/include/nodes/primnodes.h:822
}

// NewOpExpr creates a new OpExpr node.
func NewOpExpr(opno Oid, opfuncid Oid, opresulttype Oid, args *NodeList) *OpExpr {
	return &OpExpr{
		BaseExpr:     BaseExpr{BaseNode: BaseNode{Tag: T_OpExpr}},
		Opno:         opno,
		Opfuncid:     opfuncid,
		Opresulttype: opresulttype,
		Args:         args,
	}
}

func (o *OpExpr) ExpressionType() string {
	return "OpExpr"
}

func (o *OpExpr) String() string {
	opType := "binary"
	if o.Args != nil && len(o.Args.Items) == 1 {
		opType = "unary"
	}
	return fmt.Sprintf("OpExpr(%s, oid:%d)@%d", opType, o.Opno, o.Location())
}

// BoolExpr represents a boolean expression (AND/OR/NOT).
// Ported from postgres/src/include/nodes/primnodes.h:934
type BoolExpr struct {
	BaseExpr
	Boolop BoolExprType // AND/OR/NOT - postgres/src/include/nodes/primnodes.h:947
	Args   *NodeList    // Operand expressions - postgres/src/include/nodes/primnodes.h:948
}

// NewBoolExpr creates a new BoolExpr node.
func NewBoolExpr(boolop BoolExprType, args *NodeList) *BoolExpr {
	return &BoolExpr{
		BaseExpr: BaseExpr{BaseNode: BaseNode{Tag: T_BoolExpr}},
		Boolop:   boolop,
		Args:     args,
	}
}

func (b *BoolExpr) ExpressionType() string {
	return "BoolExpr"
}

func (b *BoolExpr) String() string {
	argCount := 0
	if b.Args != nil {
		argCount = len(b.Args.Items)
	}
	return fmt.Sprintf("BoolExpr(%s, %d args)@%d", b.Boolop, argCount, b.Location())
}

// SqlString returns the SQL representation of BoolExpr
func (b *BoolExpr) SqlString() string {
	if b.Args == nil || len(b.Args.Items) == 0 {
		return ""
	}
	
	switch b.Boolop {
	case AND_EXPR:
		var parts []string
		for _, arg := range b.Args.Items {
			if arg != nil {
				parts = append(parts, arg.SqlString())
			}
		}
		// Don't add parentheses - let ParenExpr handle explicit parentheses
		// and let precedence rules determine when they're needed
		return strings.Join(parts, " AND ")
		
	case OR_EXPR:
		var parts []string
		for _, arg := range b.Args.Items {
			if arg != nil {
				parts = append(parts, arg.SqlString())
			}
		}
		// Don't add parentheses - let ParenExpr handle explicit parentheses
		// and let precedence rules determine when they're needed
		return strings.Join(parts, " OR ")
		
	case NOT_EXPR:
		if len(b.Args.Items) > 0 && b.Args.Items[0] != nil {
			return fmt.Sprintf("NOT %s", b.Args.Items[0].SqlString())
		}
		
	default:
		return "UNKNOWN_BOOL_EXPR"
	}
	
	return ""
}

// ==============================================================================
// CONVENIENCE CONSTRUCTORS FOR COMMON PATTERNS
// ==============================================================================

// NewAndExpr creates a new AND boolean expression.
func NewAndExpr(left, right Node) *BoolExpr {
	return NewBoolExpr(AND_EXPR, NewNodeList(left, right))
}

// NewOrExpr creates a new OR boolean expression.
func NewOrExpr(left, right Node) *BoolExpr {
	return NewBoolExpr(OR_EXPR, NewNodeList(left, right))
}

// NewNotExpr creates a new NOT boolean expression.
func NewNotExpr(arg Node) *BoolExpr {
	return NewBoolExpr(NOT_EXPR, NewNodeList(arg))
}

// NewBinaryOp creates a binary operator expression.
func NewBinaryOp(opno Oid, left, right Node) *OpExpr {
	return NewOpExpr(opno, 0, 0, NewNodeList(left, right))
}

// NewUnaryOp creates a unary operator expression.
func NewUnaryOp(opno Oid, arg Node) *OpExpr {
	return NewOpExpr(opno, 0, 0, NewNodeList(arg))
}

// ==============================================================================
// TIER 2 EXPRESSIONS - Common SQL Features
// ==============================================================================

// CaseExpr represents a CASE expression.
// Ported from postgres/src/include/nodes/primnodes.h:1306
type CaseExpr struct {
	BaseExpr
	Casetype   Oid    // Result type - postgres/src/include/nodes/primnodes.h:1309
	Casecollid Oid    // Result collation - postgres/src/include/nodes/primnodes.h:1310
	Arg        Node   // Implicit comparison argument - postgres/src/include/nodes/primnodes.h:1311
	Args       *NodeList // List of WHEN clauses - postgres/src/include/nodes/primnodes.h:1312
	Defresult  Node   // Default result (ELSE) - postgres/src/include/nodes/primnodes.h:1313
}

// NewCaseExpr creates a new CaseExpr node.
func NewCaseExpr(casetype Oid, arg Node, whens *NodeList, defresult Node) *CaseExpr {
	return &CaseExpr{
		BaseExpr:  BaseExpr{BaseNode: BaseNode{Tag: T_CaseExpr}},
		Casetype:  casetype,
		Arg:       arg,
		Args:      whens,
		Defresult: defresult,
	}
}

func (c *CaseExpr) ExpressionType() string {
	return "CaseExpr"
}

func (c *CaseExpr) String() string {
	whenCount := 0
	if c.Args != nil {
		whenCount = len(c.Args.Items)
	}
	hasElse := c.Defresult != nil
	return fmt.Sprintf("CaseExpr(%d whens, else:%t)@%d", whenCount, hasElse, c.Location())
}

func (c *CaseExpr) SqlString() string {
	if c == nil {
		return ""
	}
	
	var result strings.Builder
	
	// Start with CASE
	result.WriteString("CASE")
	
	// Add the case argument if present (for simple CASE expressions)
	if c.Arg != nil {
		result.WriteString(" ")
		result.WriteString(c.Arg.SqlString())
	}
	
	// Add WHEN clauses
	if c.Args != nil {
		for _, when := range c.Args.Items {
			if caseWhen, ok := when.(*CaseWhen); ok {
				result.WriteString(" WHEN ")
				result.WriteString(caseWhen.Expr.SqlString())
				result.WriteString(" THEN ")
				result.WriteString(caseWhen.Result.SqlString())
			}
		}
	}
	
	// Add ELSE clause if present
	if c.Defresult != nil {
		result.WriteString(" ELSE ")
		result.WriteString(c.Defresult.SqlString())
	}
	
	result.WriteString(" END")
	
	return result.String()
}

// CaseWhen represents a WHEN clause in a CASE expression.
// Ported from postgres/src/include/nodes/primnodes.h:1322
type CaseWhen struct {
	BaseExpr
	Expr   Node // Condition expression - postgres/src/include/nodes/primnodes.h:1325
	Result Node // Result expression - postgres/src/include/nodes/primnodes.h:1326
}

// NewCaseWhen creates a new CaseWhen node.
func NewCaseWhen(expr Node, result Node) *CaseWhen {
	return &CaseWhen{
		BaseExpr: BaseExpr{BaseNode: BaseNode{Tag: T_CaseExpr}}, // CaseWhen uses same tag family
		Expr:     expr,
		Result:   result,
	}
}

func (cw *CaseWhen) ExpressionType() string {
	return "CaseWhen"
}

func (cw *CaseWhen) String() string {
	return fmt.Sprintf("CaseWhen@%d", cw.Location())
}

// CoalesceExpr represents a COALESCE expression.
// Ported from postgres/src/include/nodes/primnodes.h:1484
type CoalesceExpr struct {
	BaseExpr
	Coalescetype   Oid    // Result type - postgres/src/include/nodes/primnodes.h:1487
	Coalescecollid Oid    // Result collation - postgres/src/include/nodes/primnodes.h:1488
	Args           *NodeList // Argument expressions - postgres/src/include/nodes/primnodes.h:1489
}

// NewCoalesceExpr creates a new CoalesceExpr node.
func NewCoalesceExpr(coalescetype Oid, args *NodeList) *CoalesceExpr {
	return &CoalesceExpr{
		BaseExpr:     BaseExpr{BaseNode: BaseNode{Tag: T_CoalesceExpr}},
		Coalescetype: coalescetype,
		Args:         args,
	}
}

func (c *CoalesceExpr) ExpressionType() string {
	return "CoalesceExpr"
}

func (c *CoalesceExpr) String() string {
	argCount := 0
	if c.Args != nil {
		argCount = len(c.Args.Items)
	}
	return fmt.Sprintf("CoalesceExpr(%d args)@%d", argCount, c.Location())
}

// ArrayExpr represents an ARRAY[] constructor expression.
// Ported from postgres/src/include/nodes/primnodes.h:1370
type ArrayExpr struct {
	BaseExpr
	ArrayTypeid   Oid    // Array type OID - postgres/src/include/nodes/primnodes.h:1373
	ArrayCollid   Oid    // Array collation - postgres/src/include/nodes/primnodes.h:1374
	ElementTypeid Oid    // Element type OID - postgres/src/include/nodes/primnodes.h:1375
	Elements      *NodeList // Array elements - postgres/src/include/nodes/primnodes.h:1376
	Multidims     bool   // Multi-dimensional? - postgres/src/include/nodes/primnodes.h:1377
}

// NewArrayExpr creates a new ArrayExpr node.
func NewArrayExpr(arrayTypeid Oid, elementTypeid Oid, elements *NodeList) *ArrayExpr {
	return &ArrayExpr{
		BaseExpr:      BaseExpr{BaseNode: BaseNode{Tag: T_ArrayExpr}},
		ArrayTypeid:   arrayTypeid,
		ElementTypeid: elementTypeid,
		Elements:      elements,
	}
}

func (a *ArrayExpr) ExpressionType() string {
	return "ArrayExpr"
}

func (a *ArrayExpr) String() string {
	dims := "1D"
	if a.Multidims {
		dims = "Multi-D"
	}
	elementCount := 0
	if a.Elements != nil {
		elementCount = len(a.Elements.Items)
	}
	return fmt.Sprintf("ArrayExpr(%s, %d elements)@%d", dims, elementCount, a.Location())
}

// ScalarArrayOpExpr represents a scalar op ANY/ALL (array) expression.
// Ported from postgres/src/include/nodes/primnodes.h:893
type ScalarArrayOpExpr struct {
	BaseExpr
	Opno        Oid    // pg_operator OID - postgres/src/include/nodes/primnodes.h:896
	Opfuncid    Oid    // Comparison function OID - postgres/src/include/nodes/primnodes.h:897
	Hashfuncid  Oid    // Hash function OID (optimization) - postgres/src/include/nodes/primnodes.h:898
	Negfuncid   Oid    // Negation function OID - postgres/src/include/nodes/primnodes.h:899
	UseOr       bool   // True for ANY, false for ALL - postgres/src/include/nodes/primnodes.h:900
	Inputcollid Oid    // Input collation - postgres/src/include/nodes/primnodes.h:901
	Args        *NodeList // Scalar and array operands - postgres/src/include/nodes/primnodes.h:902
}

// NewScalarArrayOpExpr creates a new ScalarArrayOpExpr node.
func NewScalarArrayOpExpr(opno Oid, useOr bool, scalar Node, array Node) *ScalarArrayOpExpr {
	return &ScalarArrayOpExpr{
		BaseExpr: BaseExpr{BaseNode: BaseNode{Tag: T_ScalarArrayOpExpr}},
		Opno:     opno,
		UseOr:    useOr,
		Args:     NewNodeList(scalar, array),
	}
}

func (s *ScalarArrayOpExpr) ExpressionType() string {
	return "ScalarArrayOpExpr"
}

func (s *ScalarArrayOpExpr) String() string {
	opType := "ALL"
	if s.UseOr {
		opType = "ANY"
	}
	return fmt.Sprintf("ScalarArrayOpExpr(%s, oid:%d)@%d", opType, s.Opno, s.Location())
}

// RowExpr represents a ROW() constructor expression.
// Ported from postgres/src/include/nodes/primnodes.h:1408
type RowExpr struct {
	BaseExpr
	Args      *NodeList    // Row field expressions - postgres/src/include/nodes/primnodes.h:1411
	RowTypeid Oid          // Composite type OID - postgres/src/include/nodes/primnodes.h:1412
	RowFormat CoercionForm // Display format - postgres/src/include/nodes/primnodes.h:1413
	Colnames  []*string    // Field names (RECORD type only) - postgres/src/include/nodes/primnodes.h:1414
}

// NewRowExpr creates a new RowExpr node.
func NewRowExpr(args *NodeList, rowTypeid Oid) *RowExpr {
	return &RowExpr{
		BaseExpr:  BaseExpr{BaseNode: BaseNode{Tag: T_RowExpr}},
		Args:      args,
		RowTypeid: rowTypeid,
	}
}

func (r *RowExpr) ExpressionType() string {
	return "RowExpr"
}

func (r *RowExpr) String() string {
	fieldCount := 0
	if r.Args != nil {
		fieldCount = len(r.Args.Items)
	}
	return fmt.Sprintf("RowExpr(%d fields)@%d", fieldCount, r.Location())
}

// SqlString returns the SQL representation of the row expression
func (r *RowExpr) SqlString() string {
	if r.Args == nil || len(r.Args.Items) == 0 {
		return "ROW()"
	}
	
	var items []string
	for _, arg := range r.Args.Items {
		if arg != nil {
			items = append(items, arg.SqlString())
		}
	}
	
	// ROW expressions are typically written as just (expr1, expr2, ...)
	// unless explicitly using ROW keyword
	if r.RowFormat == COERCE_EXPLICIT_CALL {
		return fmt.Sprintf("ROW(%s)", strings.Join(items, ", "))
	}
	return fmt.Sprintf("(%s)", strings.Join(items, ", "))
}

// ==============================================================================
// TIER 2 CONVENIENCE CONSTRUCTORS
// ==============================================================================

// NewSimpleCase creates a simple CASE expression: CASE expr WHEN val1 THEN result1 ... ELSE def END
func NewSimpleCase(expr Node, whens *NodeList, defresult Node) *CaseExpr {
	return NewCaseExpr(0, expr, whens, defresult)
}

// NewSearchedCase creates a searched CASE expression: CASE WHEN condition1 THEN result1 ... ELSE def END
func NewSearchedCase(whens *NodeList, defresult Node) *CaseExpr {
	return NewCaseExpr(0, nil, whens, defresult)
}

// NewArrayConstructor creates an ARRAY[...] constructor.
func NewArrayConstructor(elements *NodeList) *ArrayExpr {
	return NewArrayExpr(0, 0, elements)
}

// NewRowConstructor creates a ROW(...) constructor.
func NewRowConstructor(fields *NodeList) *RowExpr {
	return NewRowExpr(fields, 0)
}

// NewAnyExpr creates a scalar = ANY(array) expression.
func NewAnyExpr(opno Oid, scalar Node, array Node) *ScalarArrayOpExpr {
	return NewScalarArrayOpExpr(opno, true, scalar, array)
}

// NewAllExpr creates a scalar = ALL(array) expression.
func NewAllExpr(opno Oid, scalar Node, array Node) *ScalarArrayOpExpr {
	return NewScalarArrayOpExpr(opno, false, scalar, array)
}

// NewInExpr creates a scalar IN (array) expression using = ANY.
func NewInExpr(scalar Node, array Node) *ScalarArrayOpExpr {
	return NewAnyExpr(96, scalar, array) // 96 is "=" operator OID
}

// NewNotInExpr creates a scalar NOT IN (array) expression using <> ALL.
func NewNotInExpr(scalar Node, array Node) *ScalarArrayOpExpr {
	return NewAllExpr(518, scalar, array) // 518 is "<>" operator OID
}

// ==============================================================================
// EXPRESSION TYPE MAPPING
// ==============================================================================

// GetExprTag maps expression types to NodeTag constants.
// Expression NodeTag constants are defined in nodes.go
func GetExprTag(exprType string) NodeTag {
	switch exprType {
	case "Var":
		return T_Var
	case "Const":
		return T_Const
	case "Param":
		return T_Param
	case "FuncExpr":
		return T_FuncExpr
	case "OpExpr":
		return T_OpExpr
	case "BoolExpr":
		return T_BoolExpr
	case "CaseExpr":
		return T_CaseExpr
	case "CoalesceExpr":
		return T_CoalesceExpr
	case "ArrayExpr":
		return T_ArrayExpr
	case "ScalarArrayOpExpr":
		return T_ScalarArrayOpExpr
	case "RowExpr":
		return T_RowExpr
	case "Aggref":
		return T_Aggref
	case "WindowFunc":
		return T_WindowFunc
	case "SubLink":
		return T_SubLink
	default:
		return T_Invalid
	}
}

// ==============================================================================
// EXPRESSION UTILITIES
// ==============================================================================

// IsConstant checks if an expression is a constant value.
func IsConstant(expr Node) bool {
	_, ok := expr.(*Const)
	return ok
}

// IsVariable checks if an expression is a variable reference.
func IsVariable(expr Node) bool {
	_, ok := expr.(*Var)
	return ok
}

// IsFunction checks if an expression is a function call.
func IsFunction(expr Node) bool {
	_, ok := expr.(*FuncExpr)
	return ok
}

// GetExpressionArgs returns the arguments of an expression if it has any.
func GetExpressionArgs(expr Node) *NodeList {
	switch e := expr.(type) {
	case *FuncExpr:
		return e.Args
	case *OpExpr:
		return e.Args
	case *BoolExpr:
		return e.Args
	case *CaseExpr:
		return e.Args
	case *CaseWhen:
		return NewNodeList(e.Expr, e.Result)
	case *CoalesceExpr:
		return e.Args
	case *ArrayExpr:
		return e.Elements
	case *ScalarArrayOpExpr:
		return e.Args
	case *RowExpr:
		return e.Args
	case *Aggref:
		return e.Args
	case *WindowFunc:
		return e.Args
	case *SubLink:
		if e.Testexpr != nil {
			return NewNodeList(e.Testexpr, e.Subselect)
		}
		return NewNodeList(e.Subselect)
	default:
		return nil
	}
}

// CountArgs returns the number of arguments in an expression.
func CountArgs(expr Node) int {
	args := GetExpressionArgs(expr)
	if args == nil {
		return 0
	}
	return len(args.Items)
}

// ==============================================================================
// TIER 3 EXPRESSIONS - Advanced Expressions and Aggregations
// ==============================================================================

// SubLinkType represents types of sublinks - ported from postgres/src/include/nodes/primnodes.h:1556-1565
type SubLinkType int

const (
	EXISTS_SUBLINK     SubLinkType = iota // EXISTS(subquery)
	ALL_SUBLINK                           // expr op ALL(subquery)
	ANY_SUBLINK                           // expr op ANY(subquery)
	ROWCOMPARE_SUBLINK                    // (expr list) op (subquery)
	EXPR_SUBLINK                          // expr(subquery)
	MULTIEXPR_SUBLINK                     // multiple expressions
	ARRAY_SUBLINK                         // ARRAY(subquery)
	CTE_SUBLINK                           // for SubPlans only
)

func (s SubLinkType) String() string {
	switch s {
	case EXISTS_SUBLINK:
		return "EXISTS"
	case ALL_SUBLINK:
		return "ALL"
	case ANY_SUBLINK:
		return "ANY"
	case ROWCOMPARE_SUBLINK:
		return "ROWCOMPARE"
	case EXPR_SUBLINK:
		return "EXPR"
	case MULTIEXPR_SUBLINK:
		return "MULTIEXPR"
	case ARRAY_SUBLINK:
		return "ARRAY"
	case CTE_SUBLINK:
		return "CTE"
	default:
		return fmt.Sprintf("SubLinkType(%d)", int(s))
	}
}

// AggSplit represents aggregate splitting modes - ported from postgres/src/include/nodes/nodes.h:479-487
type AggSplit int

const (
	AGGSPLIT_SIMPLE         AggSplit = 0  // Basic, non-split aggregation
	AGGSPLIT_INITIAL_SERIAL AggSplit = 3  // Initial phase with serialization
	AGGSPLIT_FINAL_DESERIAL AggSplit = 12 // Final phase with deserialization
)

func (a AggSplit) String() string {
	switch a {
	case AGGSPLIT_SIMPLE:
		return "SIMPLE"
	case AGGSPLIT_INITIAL_SERIAL:
		return "INITIAL_SERIAL"
	case AGGSPLIT_FINAL_DESERIAL:
		return "FINAL_DESERIAL"
	default:
		return fmt.Sprintf("AggSplit(%d)", int(a))
	}
}

// Aggref represents an aggregate function call expression.
// Ported from postgres/src/include/nodes/primnodes.h:439
type Aggref struct {
	BaseExpr
	Aggfnoid      Oid      // pg_proc OID of the aggregate - postgres/src/include/nodes/primnodes.h:481
	Aggtype       Oid      // Result type OID - postgres/src/include/nodes/primnodes.h:483
	Aggcollid     Oid      // Result collation - postgres/src/include/nodes/primnodes.h:485
	Inputcollid   Oid      // Input collation - postgres/src/include/nodes/primnodes.h:487
	Aggtranstype  Oid      // Transition value type - postgres/src/include/nodes/primnodes.h:492
	Aggargtypes   []Oid    // Argument type OIDs - postgres/src/include/nodes/primnodes.h:494
	Aggdirectargs *NodeList // Direct arguments for ordered-set aggs - postgres/src/include/nodes/primnodes.h:495
	Args          *NodeList // Aggregated arguments - postgres/src/include/nodes/primnodes.h:496
	Aggorder      *NodeList // ORDER BY expressions - postgres/src/include/nodes/primnodes.h:497
	Aggdistinct   *NodeList // DISTINCT expressions - postgres/src/include/nodes/primnodes.h:498
	Aggfilter     Node     // FILTER expression - postgres/src/include/nodes/primnodes.h:499
	Aggstar       bool     // True if argument list was '*' - postgres/src/include/nodes/primnodes.h:501
	Aggvariadic   bool     // True if variadic args combined - postgres/src/include/nodes/primnodes.h:505
	Aggkind       byte     // Aggregate kind - postgres/src/include/nodes/primnodes.h:507
	Aggpresorted  bool     // Input already sorted - postgres/src/include/nodes/primnodes.h:509
	Agglevelsup   Index    // > 0 if agg belongs to outer query - postgres/src/include/nodes/primnodes.h:511
	Aggsplit      AggSplit // Expected splitting mode - postgres/src/include/nodes/primnodes.h:513
	Aggno         int      // Unique ID within Agg node - postgres/src/include/nodes/primnodes.h:515
	Aggtransno    int      // Unique ID of transition state - postgres/src/include/nodes/primnodes.h:517
}

// NewAggref creates a new Aggref node.
func NewAggref(aggfnoid Oid, aggtype Oid, args *NodeList) *Aggref {
	return &Aggref{
		BaseExpr: BaseExpr{BaseNode: BaseNode{Tag: T_Aggref}},
		Aggfnoid: aggfnoid,
		Aggtype:  aggtype,
		Args:     args,
	}
}

func (a *Aggref) ExpressionType() string {
	return "Aggref"
}

func (a *Aggref) String() string {
	distinct := ""
	if a.Aggdistinct != nil && len(a.Aggdistinct.Items) > 0 {
		distinct = " DISTINCT"
	}
	filter := ""
	if a.Aggfilter != nil {
		filter = " FILTER"
	}
	argCount := 0
	if a.Args != nil {
		argCount = len(a.Args.Items)
	}
	return fmt.Sprintf("Aggref(oid:%d%s%s, %d args)@%d", a.Aggfnoid, distinct, filter, argCount, a.Location())
}

// WindowFunc represents a window function call expression.
// Ported from postgres/src/include/nodes/primnodes.h:563
type WindowFunc struct {
	BaseExpr
	Winfnoid     Oid    // pg_proc OID of the function - postgres/src/include/nodes/primnodes.h:593
	Wintype      Oid    // Result type OID - postgres/src/include/nodes/primnodes.h:595
	Wincollid    Oid    // Result collation - postgres/src/include/nodes/primnodes.h:597
	Inputcollid  Oid    // Input collation - postgres/src/include/nodes/primnodes.h:599
	Args         *NodeList // Arguments to window function - postgres/src/include/nodes/primnodes.h:601
	Aggfilter    Node   // FILTER expression - postgres/src/include/nodes/primnodes.h:603
	RunCondition *NodeList // List of run conditions - postgres/src/include/nodes/primnodes.h:605
	Winref       Index  // Index of associated WindowClause - postgres/src/include/nodes/primnodes.h:607
	Winstar      bool   // True if argument list was '*' - postgres/src/include/nodes/primnodes.h:609
	Winagg       bool   // Is function a simple aggregate? - postgres/src/include/nodes/primnodes.h:611
}

// NewWindowFunc creates a new WindowFunc node.
func NewWindowFunc(winfnoid Oid, wintype Oid, args *NodeList, winref Index) *WindowFunc {
	return &WindowFunc{
		BaseExpr: BaseExpr{BaseNode: BaseNode{Tag: T_WindowFunc}},
		Winfnoid: winfnoid,
		Wintype:  wintype,
		Args:     args,
		Winref:   winref,
	}
}

func (w *WindowFunc) ExpressionType() string {
	return "WindowFunc"
}

func (w *WindowFunc) String() string {
	star := ""
	if w.Winstar {
		star = "*"
	} else {
		argCount := 0
		if w.Args != nil {
			argCount = len(w.Args.Items)
		}
		star = fmt.Sprintf("%d args", argCount)
	}
	filter := ""
	if w.Aggfilter != nil {
		filter = " FILTER"
	}
	return fmt.Sprintf("WindowFunc(oid:%d%s, %s)@%d", w.Winfnoid, filter, star, w.Location())
}

// SubLink represents a sublink expression (subquery).
// Ported from postgres/src/include/nodes/primnodes.h:1008
type SubLink struct {
	BaseExpr
	SubLinkType SubLinkType // Type of sublink - postgres/src/include/nodes/primnodes.h:1575
	SubLinkId   int         // ID (1..n); 0 if not MULTIEXPR - postgres/src/include/nodes/primnodes.h:1576
	Testexpr    Node        // Outer-query test for ALL/ANY/ROWCOMPARE - postgres/src/include/nodes/primnodes.h:1577
	OperName    *NodeList   // Originally specified operator name - postgres/src/include/nodes/primnodes.h:1579
	Subselect   Node        // Subselect as Query* or raw parsetree - postgres/src/include/nodes/primnodes.h:1581
}

// NewSubLink creates a new SubLink node.
func NewSubLink(subLinkType SubLinkType, subselect Node) *SubLink {
	return &SubLink{
		BaseExpr:    BaseExpr{BaseNode: BaseNode{Tag: T_SubLink}},
		SubLinkType: subLinkType,
		Subselect:   subselect,
	}
}

func (s *SubLink) ExpressionType() string {
	return "SubLink"
}

// SqlString returns the SQL representation of the SubLink
func (s *SubLink) SqlString() string {
	if s == nil {
		return ""
	}
	
	// Handle different sublink types
	switch s.SubLinkType {
	case EXISTS_SUBLINK:
		// EXISTS(subquery)
		return fmt.Sprintf("EXISTS (%s)", s.Subselect.SqlString())
		
	case ALL_SUBLINK:
		// expr op ALL(subquery)
		if s.Testexpr != nil && s.OperName != nil && len(s.OperName.Items) > 0 {
			op := extractOperatorFromNodeList(s.OperName)
			return fmt.Sprintf("%s %s ALL (%s)", s.Testexpr.SqlString(), op, s.Subselect.SqlString())
		}
		return fmt.Sprintf("ALL (%s)", s.Subselect.SqlString())
		
	case ANY_SUBLINK:
		// expr op ANY(subquery) - includes IN which is = ANY
		if s.Testexpr != nil {
			// Special case: OperName nil means it's IN not = ANY
			if s.OperName == nil {
				return fmt.Sprintf("%s IN (%s)", s.Testexpr.SqlString(), s.Subselect.SqlString())
			}
			// Regular ANY with operator
			if len(s.OperName.Items) > 0 {
				op := extractOperatorFromNodeList(s.OperName)
				return fmt.Sprintf("%s %s ANY (%s)", s.Testexpr.SqlString(), op, s.Subselect.SqlString())
			}
		}
		return fmt.Sprintf("ANY (%s)", s.Subselect.SqlString())
		
	case ROWCOMPARE_SUBLINK:
		// (expr list) op (subquery)
		if s.Testexpr != nil && s.OperName != nil && len(s.OperName.Items) > 0 {
			op := extractOperatorFromNodeList(s.OperName)
			return fmt.Sprintf("%s %s (%s)", s.Testexpr.SqlString(), op, s.Subselect.SqlString())
		}
		return fmt.Sprintf("(%s)", s.Subselect.SqlString())
		
	case EXPR_SUBLINK:
		// Simple scalar subquery: (subquery)
		return fmt.Sprintf("(%s)", s.Subselect.SqlString())
		
	case MULTIEXPR_SUBLINK:
		// Multiple expressions - just wrap in parentheses
		return fmt.Sprintf("(%s)", s.Subselect.SqlString())
		
	case ARRAY_SUBLINK:
		// ARRAY(subquery)
		return fmt.Sprintf("ARRAY(%s)", s.Subselect.SqlString())
		
	case CTE_SUBLINK:
		// For SubPlans only - shouldn't appear in normal SQL
		return fmt.Sprintf("(%s)", s.Subselect.SqlString())
		
	default:
		// Fallback to simple subquery
		return fmt.Sprintf("(%s)", s.Subselect.SqlString())
	}
}

// extractOperatorFromNodeList extracts the operator string from a NodeList
func extractOperatorFromNodeList(list *NodeList) string {
	if list == nil || len(list.Items) == 0 {
		return ""
	}
	
	// The operator is typically stored as a String node in the list
	for _, item := range list.Items {
		if str, ok := item.(*String); ok && str != nil {
			return str.SVal
		}
	}
	
	// Fallback: try to get SqlString of first item
	if list.Items[0] != nil {
		return list.Items[0].SqlString()
	}
	
	return ""
}

func (s *SubLink) String() string {
	test := ""
	if s.Testexpr != nil {
		test = " with test"
	}
	return fmt.Sprintf("SubLink(%s%s)@%d", s.SubLinkType, test, s.Location())
}

// ==============================================================================
// TIER 3 CONVENIENCE CONSTRUCTORS
// ==============================================================================

// NewCountStar creates a COUNT(*) aggregate.
func NewCountStar() *Aggref {
	agg := NewAggref(2147, 20, nil) // COUNT function OID 2147, result type bigint 20
	agg.Aggstar = true
	return agg
}

// NewCount creates a COUNT(expr) aggregate.
func NewCount(expr Node) *Aggref {
	return NewAggref(2147, 20, NewNodeList(expr)) // COUNT function OID 2147, result type bigint 20
}

// NewSum creates a SUM(expr) aggregate.
func NewSum(expr Node) *Aggref {
	return NewAggref(2108, 0, NewNodeList(expr)) // SUM function OID (varies by type)
}

// NewAvg creates an AVG(expr) aggregate.
func NewAvg(expr Node) *Aggref {
	return NewAggref(2100, 0, NewNodeList(expr)) // AVG function OID (varies by type)
}

// NewMax creates a MAX(expr) aggregate.
func NewMax(expr Node) *Aggref {
	return NewAggref(2116, 0, NewNodeList(expr)) // MAX function OID (varies by type)
}

// NewMin creates a MIN(expr) aggregate.
func NewMin(expr Node) *Aggref {
	return NewAggref(2132, 0, NewNodeList(expr)) // MIN function OID (varies by type)
}

// NewExistsSublink creates an EXISTS(subquery) expression.
func NewExistsSublink(subquery Node) *SubLink {
	return NewSubLink(EXISTS_SUBLINK, subquery)
}

// NewInSublink creates an expr IN (subquery) expression.
func NewInSublink(testexpr Node, subquery Node) *SubLink {
	sublink := NewSubLink(ANY_SUBLINK, subquery)
	sublink.Testexpr = testexpr
	return sublink
}

// NewNotInSublink creates an expr NOT IN (subquery) expression.
func NewNotInSublink(testexpr Node, subquery Node) *SubLink {
	sublink := NewSubLink(ALL_SUBLINK, subquery)
	sublink.Testexpr = testexpr
	return sublink
}

// NewExprSublink creates a scalar subquery expression.
func NewExprSublink(subquery Node) *SubLink {
	return NewSubLink(EXPR_SUBLINK, subquery)
}

// NewArraySublink creates an ARRAY(subquery) expression.
func NewArraySublink(subquery Node) *SubLink {
	return NewSubLink(ARRAY_SUBLINK, subquery)
}

// NewRowNumber creates a ROW_NUMBER() window function.
func NewRowNumber() *WindowFunc {
	return NewWindowFunc(3100, 20, nil, 0) // ROW_NUMBER function OID 3100, result type bigint 20
}

// NewRank creates a RANK() window function.
func NewRank() *WindowFunc {
	return NewWindowFunc(3101, 20, nil, 0) // RANK function OID 3101, result type bigint 20
}

// NewDenseRank creates a DENSE_RANK() window function.
func NewDenseRank() *WindowFunc {
	return NewWindowFunc(3102, 20, nil, 0) // DENSE_RANK function OID 3102, result type bigint 20
}

// NewLag creates a LAG(expr) window function.
func NewLag(expr Node) *WindowFunc {
	return NewWindowFunc(3105, 0, NewNodeList(expr), 0) // LAG function OID (varies by type)
}

// NewLead creates a LEAD(expr) window function.
func NewLead(expr Node) *WindowFunc {
	return NewWindowFunc(3106, 0, NewNodeList(expr), 0) // LEAD function OID (varies by type)
}

// ==============================================================================
// TIER 2 EXPRESSION UTILITIES
// ==============================================================================

// IsCaseExpr checks if an expression is a CASE expression.
func IsCaseExpr(expr Node) bool {
	_, ok := expr.(*CaseExpr)
	return ok
}

// IsCoalesceExpr checks if an expression is a COALESCE expression.
func IsCoalesceExpr(expr Node) bool {
	_, ok := expr.(*CoalesceExpr)
	return ok
}

// IsArrayExpr checks if an expression is an ARRAY constructor.
func IsArrayExpr(expr Node) bool {
	_, ok := expr.(*ArrayExpr)
	return ok
}

// IsScalarArrayOpExpr checks if an expression is a scalar array operation.
func IsScalarArrayOpExpr(expr Node) bool {
	_, ok := expr.(*ScalarArrayOpExpr)
	return ok
}

// IsRowExpr checks if an expression is a ROW constructor.
func IsRowExpr(expr Node) bool {
	_, ok := expr.(*RowExpr)
	return ok
}

// IsInExpr checks if an expression is an IN operation (scalar = ANY(array)).
func IsInExpr(expr Node) bool {
	if saoe, ok := expr.(*ScalarArrayOpExpr); ok {
		return saoe.UseOr && saoe.Opno == 96 // "=" operator with ANY
	}
	return false
}

// IsNotInExpr checks if an expression is a NOT IN operation (scalar <> ALL(array)).
func IsNotInExpr(expr Node) bool {
	if saoe, ok := expr.(*ScalarArrayOpExpr); ok {
		return !saoe.UseOr && saoe.Opno == 518 // "<>" operator with ALL
	}
	return false
}

// GetCaseWhenCount returns the number of WHEN clauses in a CASE expression.
func GetCaseWhenCount(expr Node) int {
	if caseExpr, ok := expr.(*CaseExpr); ok {
		if caseExpr.Args == nil {
			return 0
		}
		return len(caseExpr.Args.Items)
	}
	return 0
}

// HasCaseElse checks if a CASE expression has an ELSE clause.
func HasCaseElse(expr Node) bool {
	if caseExpr, ok := expr.(*CaseExpr); ok {
		return caseExpr.Defresult != nil
	}
	return false
}

// GetArrayElements returns the elements of an array expression.
func GetArrayElements(expr Node) *NodeList {
	if arrayExpr, ok := expr.(*ArrayExpr); ok {
		return arrayExpr.Elements
	}
	return nil
}

// IsMultiDimArray checks if an array expression is multi-dimensional.
func IsMultiDimArray(expr Node) bool {
	if arrayExpr, ok := expr.(*ArrayExpr); ok {
		return arrayExpr.Multidims
	}
	return false
}

// ==============================================================================
// TIER 3 EXPRESSION UTILITIES
// ==============================================================================

// IsAggref checks if an expression is an aggregate function call.
func IsAggref(expr Node) bool {
	_, ok := expr.(*Aggref)
	return ok
}

// IsWindowFunc checks if an expression is a window function call.
func IsWindowFunc(expr Node) bool {
	_, ok := expr.(*WindowFunc)
	return ok
}

// IsSubLink checks if an expression is a sublink (subquery).
func IsSubLink(expr Node) bool {
	_, ok := expr.(*SubLink)
	return ok
}

// IsAggregate checks if an expression is any kind of aggregate (Aggref or WindowFunc with winagg=true).
func IsAggregate(expr Node) bool {
	switch e := expr.(type) {
	case *Aggref:
		return true
	case *WindowFunc:
		return e.Winagg
	default:
		return false
	}
}

// GetAggregateArgs returns the arguments of an aggregate expression.
func GetAggregateArgs(expr Node) *NodeList {
	switch e := expr.(type) {
	case *Aggref:
		return e.Args
	case *WindowFunc:
		return e.Args
	default:
		return nil
	}
}

// HasAggregateFilter checks if an aggregate expression has a FILTER clause.
func HasAggregateFilter(expr Node) bool {
	switch e := expr.(type) {
	case *Aggref:
		return e.Aggfilter != nil
	case *WindowFunc:
		return e.Aggfilter != nil
	default:
		return false
	}
}

// GetAggregateFilter returns the FILTER expression of an aggregate.
func GetAggregateFilter(expr Node) Node {
	switch e := expr.(type) {
	case *Aggref:
		return e.Aggfilter
	case *WindowFunc:
		return e.Aggfilter
	default:
		return nil
	}
}

// IsDistinctAggregate checks if an aggregate has DISTINCT.
func IsDistinctAggregate(expr Node) bool {
	if aggref, ok := expr.(*Aggref); ok {
		return aggref.Aggdistinct != nil && len(aggref.Aggdistinct.Items) > 0
	}
	return false
}

// IsStarAggregate checks if an aggregate uses * (like COUNT(*)).
func IsStarAggregate(expr Node) bool {
	switch e := expr.(type) {
	case *Aggref:
		return e.Aggstar
	case *WindowFunc:
		return e.Winstar
	default:
		return false
	}
}

// GetSubLinkType returns the type of a SubLink.
func GetSubLinkType(expr Node) SubLinkType {
	if sublink, ok := expr.(*SubLink); ok {
		return sublink.SubLinkType
	}
	return -1 // Invalid
}

// IsExistsSublink checks if an expression is an EXISTS sublink.
func IsExistsSublink(expr Node) bool {
	if sublink, ok := expr.(*SubLink); ok {
		return sublink.SubLinkType == EXISTS_SUBLINK
	}
	return false
}

// IsScalarSublink checks if an expression is a scalar sublink (EXPR_SUBLINK).
func IsScalarSublink(expr Node) bool {
	if sublink, ok := expr.(*SubLink); ok {
		return sublink.SubLinkType == EXPR_SUBLINK
	}
	return false
}

// IsArraySublink checks if an expression is an ARRAY sublink.
func IsArraySublink(expr Node) bool {
	if sublink, ok := expr.(*SubLink); ok {
		return sublink.SubLinkType == ARRAY_SUBLINK
	}
	return false
}

// GetSubquery returns the subquery node from a SubLink.
func GetSubquery(expr Node) Node {
	if sublink, ok := expr.(*SubLink); ok {
		return sublink.Subselect
	}
	return nil
}

// HasSubLinkTest checks if a SubLink has a test expression.
func HasSubLinkTest(expr Node) bool {
	if sublink, ok := expr.(*SubLink); ok {
		return sublink.Testexpr != nil
	}
	return false
}

// GetSubLinkTest returns the test expression from a SubLink.
func GetSubLinkTest(expr Node) Node {
	if sublink, ok := expr.(*SubLink); ok {
		return sublink.Testexpr
	}
	return nil
}

// IsOrderedSetAggregate checks if an aggregate is an ordered-set aggregate.
func IsOrderedSetAggregate(expr Node) bool {
	if aggref, ok := expr.(*Aggref); ok {
		return aggref.Aggdirectargs != nil && len(aggref.Aggdirectargs.Items) > 0
	}
	return false
}

// GetAggregateDirectArgs returns the direct arguments of an ordered-set aggregate.
func GetAggregateDirectArgs(expr Node) *NodeList {
	if aggref, ok := expr.(*Aggref); ok {
		return aggref.Aggdirectargs
	}
	return nil
}

// GetAggregateOrderBy returns the ORDER BY expressions of an aggregate.
func GetAggregateOrderBy(expr Node) *NodeList {
	if aggref, ok := expr.(*Aggref); ok {
		return aggref.Aggorder
	}
	return nil
}

// GetWindowFuncRef returns the window reference index from a WindowFunc.
func GetWindowFuncRef(expr Node) Index {
	if winFunc, ok := expr.(*WindowFunc); ok {
		return winFunc.Winref
	}
	return 0
}

// IsSimpleWindowAgg checks if a WindowFunc is a simple aggregate.
func IsSimpleWindowAgg(expr Node) bool {
	if winFunc, ok := expr.(*WindowFunc); ok {
		return winFunc.Winagg
	}
	return false
}

// ==============================================================================
// PHASE 1F: PRIMITIVE EXPRESSION COMPLETION PART 1
// Core primitive expressions from PostgreSQL primnodes.h
// ==============================================================================

// RowCompareType represents row comparison types
// Ported from postgres/src/include/nodes/primnodes.h:1445-1452
type RowCompareType int

const (
	ROWCOMPARE_LT RowCompareType = 1 // BTLessStrategyNumber
	ROWCOMPARE_LE RowCompareType = 2 // BTLessEqualStrategyNumber
	ROWCOMPARE_EQ RowCompareType = 3 // BTEqualStrategyNumber
	ROWCOMPARE_GE RowCompareType = 4 // BTGreaterEqualStrategyNumber
	ROWCOMPARE_GT RowCompareType = 5 // BTGreaterStrategyNumber
	ROWCOMPARE_NE RowCompareType = 6 // no such btree strategy
)

// MinMaxOp represents MIN/MAX operation types
// Ported from postgres/src/include/nodes/primnodes.h:1499-1502
type MinMaxOp int

const (
	IS_GREATEST MinMaxOp = iota
	IS_LEAST
)

// SQLValueFunctionOp represents SQL value function operation types
// Ported from postgres/src/include/nodes/primnodes.h:1522-1541
type SQLValueFunctionOp int

const (
	SVFOP_CURRENT_DATE SQLValueFunctionOp = iota
	SVFOP_CURRENT_TIME
	SVFOP_CURRENT_TIME_N
	SVFOP_CURRENT_TIMESTAMP
	SVFOP_CURRENT_TIMESTAMP_N
	SVFOP_LOCALTIME
	SVFOP_LOCALTIME_N
	SVFOP_LOCALTIMESTAMP
	SVFOP_LOCALTIMESTAMP_N
	SVFOP_CURRENT_ROLE
	SVFOP_CURRENT_USER
	SVFOP_USER
	SVFOP_SESSION_USER
	SVFOP_CURRENT_CATALOG
	SVFOP_CURRENT_SCHEMA
)

// XmlExprOp represents XML expression operation types
// Ported from postgres/src/include/nodes/primnodes.h:1577-1586
type XmlExprOp int

const (
	IS_XMLCONCAT    XmlExprOp = iota // XMLCONCAT(args)
	IS_XMLELEMENT                    // XMLELEMENT(name, xml_attributes, args)
	IS_XMLFOREST                     // XMLFOREST(xml_attributes)
	IS_XMLPARSE                      // XMLPARSE(text, is_doc, preserve_ws)
	IS_XMLPI                         // XMLPI(name [, args])
	IS_XMLROOT                       // XMLROOT(xml, version, standalone)
	IS_XMLSERIALIZE                  // XMLSERIALIZE(is_document, xmlval, indent)
	IS_DOCUMENT                      // xmlval IS DOCUMENT
)

// TableFuncType represents table function types
// Ported from postgres/src/include/nodes/primnodes.h:98-101
type TableFuncType int

const (
	TFT_XMLTABLE TableFuncType = iota
	TFT_JSON_TABLE
)

// GroupingFunc represents a GROUPING function call
// Ported from postgres/src/include/nodes/primnodes.h:537-548
type GroupingFunc struct {
	BaseExpr
	Args        *NodeList // Arguments, not evaluated but kept for EXPLAIN
	Refs        *NodeList // Resource sort group refs of arguments
	Cols        *NodeList // Actual column positions set by planner
	AggLevelsUp Index     // Same as Aggref.agglevelsup
}

// NewGroupingFunc creates a new GroupingFunc node.
func NewGroupingFunc(args *NodeList, refs, cols *NodeList, aggLevelsUp Index, location int) *GroupingFunc {
	return &GroupingFunc{
		BaseExpr:    BaseExpr{BaseNode: BaseNode{Tag: T_GroupingFunc, Loc: location}},
		Args:        args,
		Refs:        refs,
		Cols:        cols,
		AggLevelsUp: aggLevelsUp,
	}
}

func (g *GroupingFunc) ExpressionType() string {
	return "GroupingFunc"
}

func (g *GroupingFunc) String() string {
	argsCount := 0
	if g.Args != nil {
		argsCount = len(g.Args.Items)
	}
	return fmt.Sprintf("GroupingFunc{%d args, agglevelsup=%d}@%d", argsCount, g.AggLevelsUp, g.Location())
}

// WindowFuncRunCondition represents a window function run condition
// Ported from postgres/src/include/nodes/primnodes.h:596-609
type WindowFuncRunCondition struct {
	BaseExpr
	Opno        Oid        // PG_OPERATOR OID of the operator
	InputCollid Oid        // OID of collation that operator should use
	WfuncLeft   bool       // True if WindowFunc belongs on the left
	Arg         Expression // The argument expression
}

// NewWindowFuncRunCondition creates a new WindowFuncRunCondition node.
func NewWindowFuncRunCondition(opno, inputCollid Oid, wfuncLeft bool, arg Expression, location int) *WindowFuncRunCondition {
	return &WindowFuncRunCondition{
		BaseExpr:    BaseExpr{BaseNode: BaseNode{Tag: T_WindowFuncRunCondition, Loc: location}},
		Opno:        opno,
		InputCollid: inputCollid,
		WfuncLeft:   wfuncLeft,
		Arg:         arg,
	}
}

func (w *WindowFuncRunCondition) ExpressionType() string {
	return "WindowFuncRunCondition"
}

func (w *WindowFuncRunCondition) String() string {
	return fmt.Sprintf("WindowFuncRunCondition{opno=%d, left=%t}@%d", w.Opno, w.WfuncLeft, w.Location())
}

// MergeSupportFunc represents a merge support function
// Ported from postgres/src/include/nodes/primnodes.h:628-635
type MergeSupportFunc struct {
	BaseExpr
	MsfType   Oid // Type OID of result
	MsfCollid Oid // OID of collation, or InvalidOid if none
}

// NewMergeSupportFunc creates a new MergeSupportFunc node.
func NewMergeSupportFunc(msfType, msfCollid Oid, location int) *MergeSupportFunc {
	return &MergeSupportFunc{
		BaseExpr:  BaseExpr{BaseNode: BaseNode{Tag: T_MergeSupportFunc, Loc: location}},
		MsfType:   msfType,
		MsfCollid: msfCollid,
	}
}

func (m *MergeSupportFunc) ExpressionType() string {
	return "MergeSupportFunc"
}

func (m *MergeSupportFunc) String() string {
	return fmt.Sprintf("MergeSupportFunc{type=%d, collid=%d}@%d", m.MsfType, m.MsfCollid, m.Location())
}

// SqlString returns the SQL representation of MergeSupportFunc
func (m *MergeSupportFunc) SqlString() string {
	return "MERGE_ACTION()"
}

// NamedArgExpr represents a named argument expression
// Ported from postgres/src/include/nodes/primnodes.h:787-795
type NamedArgExpr struct {
	BaseExpr
	Arg       Expression // The argument expression
	Name      string     // The name
	ArgNumber int        // Argument's number in positional notation
}

// NewNamedArgExpr creates a new NamedArgExpr node.
func NewNamedArgExpr(arg Expression, name string, argNumber, location int) *NamedArgExpr {
	return &NamedArgExpr{
		BaseExpr:  BaseExpr{BaseNode: BaseNode{Tag: T_NamedArgExpr, Loc: location}},
		Arg:       arg,
		Name:      name,
		ArgNumber: argNumber,
	}
}

func (n *NamedArgExpr) ExpressionType() string {
	return "NamedArgExpr"
}

func (n *NamedArgExpr) String() string {
	return fmt.Sprintf("NamedArgExpr{name='%s', argnum=%d}@%d", n.Name, n.ArgNumber, n.Location())
}

// CaseTestExpr represents a CASE test expression
// Ported from postgres/src/include/nodes/primnodes.h:1352-1359
type CaseTestExpr struct {
	BaseExpr
	TypeId    Oid // Type for substituted value
	TypeMod   int // Typemod for substituted value
	Collation Oid // Collation for the substituted value
}

// NewCaseTestExpr creates a new CaseTestExpr node.
func NewCaseTestExpr(typeId Oid, typeMod int, collation Oid, location int) *CaseTestExpr {
	return &CaseTestExpr{
		BaseExpr:  BaseExpr{BaseNode: BaseNode{Tag: T_CaseTestExpr, Loc: location}},
		TypeId:    typeId,
		TypeMod:   typeMod,
		Collation: collation,
	}
}

func (c *CaseTestExpr) ExpressionType() string {
	return "CaseTestExpr"
}

func (c *CaseTestExpr) String() string {
	return fmt.Sprintf("CaseTestExpr{type=%d, typmod=%d, collation=%d}@%d", c.TypeId, c.TypeMod, c.Collation, c.Location())
}

// MinMaxExpr represents a MIN/MAX expression
// Ported from postgres/src/include/nodes/primnodes.h:1506-1517
type MinMaxExpr struct {
	BaseExpr
	MinMaxType   Oid       // Common type of arguments and result
	MinMaxCollid Oid       // OID of collation of result
	InputCollid  Oid       // OID of collation that function should use
	Op           MinMaxOp  // Function to execute
	Args         *NodeList // The arguments
}

// NewMinMaxExpr creates a new MinMaxExpr node.
func NewMinMaxExpr(minMaxType, minMaxCollid, inputCollid Oid, op MinMaxOp, args *NodeList, location int) *MinMaxExpr {
	return &MinMaxExpr{
		BaseExpr:     BaseExpr{BaseNode: BaseNode{Tag: T_MinMaxExpr, Loc: location}},
		MinMaxType:   minMaxType,
		MinMaxCollid: minMaxCollid,
		InputCollid:  inputCollid,
		Op:           op,
		Args:         args,
	}
}

func (m *MinMaxExpr) ExpressionType() string {
	return "MinMaxExpr"
}

func (m *MinMaxExpr) String() string {
	opStr := "GREATEST"
	if m.Op == IS_LEAST {
		opStr = "LEAST"
	}
	argCount := 0
	if m.Args != nil {
		argCount = len(m.Args.Items)
	}
	return fmt.Sprintf("MinMaxExpr{%s, %d args}@%d", opStr, argCount, m.Location())
}

// RowCompareExpr represents a row comparison expression
// Ported from postgres/src/include/nodes/primnodes.h:1463-1474
type RowCompareExpr struct {
	BaseExpr
	Rctype       RowCompareType // LT LE GE or GT, never EQ or NE
	Opnos        []Oid          // OID list of pairwise comparison ops
	Opfamilies   []Oid          // OID list of containing operator families
	InputCollids []Oid          // OID list of collations for comparisons
	Largs        []Expression   // The left-hand input arguments
	Rargs        []Expression   // The right-hand input arguments
}

// NewRowCompareExpr creates a new RowCompareExpr node.
func NewRowCompareExpr(rctype RowCompareType, opnos, opfamilies, inputCollids []Oid, largs, rargs []Expression, location int) *RowCompareExpr {
	return &RowCompareExpr{
		BaseExpr:     BaseExpr{BaseNode: BaseNode{Tag: T_RowCompareExpr, Loc: location}},
		Rctype:       rctype,
		Opnos:        opnos,
		Opfamilies:   opfamilies,
		InputCollids: inputCollids,
		Largs:        largs,
		Rargs:        rargs,
	}
}

func (r *RowCompareExpr) ExpressionType() string {
	return "RowCompareExpr"
}

func (r *RowCompareExpr) String() string {
	var opStr string
	switch r.Rctype {
	case ROWCOMPARE_LT:
		opStr = "<"
	case ROWCOMPARE_LE:
		opStr = "<="
	case ROWCOMPARE_EQ:
		opStr = "="
	case ROWCOMPARE_GE:
		opStr = ">="
	case ROWCOMPARE_GT:
		opStr = ">"
	case ROWCOMPARE_NE:
		opStr = "<>"
	default:
		opStr = "?"
	}
	return fmt.Sprintf("RowCompareExpr{(%d) %s (%d)}@%d", len(r.Largs), opStr, len(r.Rargs), r.Location())
}

// SQLValueFunction represents parameterless functions with special grammar
// Ported from postgres/src/include/nodes/primnodes.h:1553-1563
type SQLValueFunction struct {
	BaseExpr
	Op      SQLValueFunctionOp // Which function this is
	Type    Oid                // Result type
	TypeMod int                // Result typmod
}

// NewSQLValueFunction creates a new SQLValueFunction node.
func NewSQLValueFunction(op SQLValueFunctionOp, typ Oid, typeMod, location int) *SQLValueFunction {
	return &SQLValueFunction{
		BaseExpr: BaseExpr{BaseNode: BaseNode{Tag: T_SQLValueFunction, Loc: location}},
		Op:       op,
		Type:     typ,
		TypeMod:  typeMod,
	}
}

func (s *SQLValueFunction) ExpressionType() string {
	return "SQLValueFunction"
}

func (s *SQLValueFunction) String() string {
	var opStr string
	switch s.Op {
	case SVFOP_CURRENT_DATE:
		opStr = "CURRENT_DATE"
	case SVFOP_CURRENT_TIME:
		opStr = "CURRENT_TIME"
	case SVFOP_CURRENT_TIMESTAMP:
		opStr = "CURRENT_TIMESTAMP"
	case SVFOP_LOCALTIME:
		opStr = "LOCALTIME"
	case SVFOP_LOCALTIMESTAMP:
		opStr = "LOCALTIMESTAMP"
	case SVFOP_CURRENT_ROLE:
		opStr = "CURRENT_ROLE"
	case SVFOP_CURRENT_USER:
		opStr = "CURRENT_USER"
	case SVFOP_USER:
		opStr = "USER"
	case SVFOP_SESSION_USER:
		opStr = "SESSION_USER"
	case SVFOP_CURRENT_CATALOG:
		opStr = "CURRENT_CATALOG"
	case SVFOP_CURRENT_SCHEMA:
		opStr = "CURRENT_SCHEMA"
	default:
		opStr = "UNKNOWN"
	}
	return fmt.Sprintf("SQLValueFunction{%s}@%d", opStr, s.Location())
}

// XmlExpr represents various SQL/XML functions requiring special grammar
// Ported from postgres/src/include/nodes/primnodes.h:1596-1618
type XmlExpr struct {
	BaseExpr
	Op        XmlExprOp     // XML function ID
	Name      string        // Name in xml(NAME foo ...) syntaxes
	NamedArgs *NodeList     // Non-XML expressions for xml_attributes
	ArgNames  *NodeList     // Parallel list of String values
	Args      *NodeList     // List of expressions
	Xmloption XmlOptionType // DOCUMENT or CONTENT
	Indent    bool          // INDENT option for XMLSERIALIZE
	Type      Oid           // Target type for XMLSERIALIZE
	TypeMod   int           // Target typmod for XMLSERIALIZE
}

// NewXmlExpr creates a new XmlExpr node.
func NewXmlExpr(op XmlExprOp, name string, namedArgs, argNames, args *NodeList, xmloption XmlOptionType, indent bool, typ Oid, typeMod, location int) *XmlExpr {
	return &XmlExpr{
		BaseExpr:  BaseExpr{BaseNode: BaseNode{Tag: T_XmlExpr, Loc: location}},
		Op:        op,
		Name:      name,
		NamedArgs: namedArgs,
		ArgNames:  argNames,
		Args:      args,
		Xmloption: xmloption,
		Indent:    indent,
		Type:      typ,
		TypeMod:   typeMod,
	}
}

func (x *XmlExpr) ExpressionType() string {
	return "XmlExpr"
}

func (x *XmlExpr) String() string {
	var opStr string
	switch x.Op {
	case IS_XMLCONCAT:
		opStr = "XMLCONCAT"
	case IS_XMLELEMENT:
		opStr = "XMLELEMENT"
	case IS_XMLFOREST:
		opStr = "XMLFOREST"
	case IS_XMLPARSE:
		opStr = "XMLPARSE"
	case IS_XMLPI:
		opStr = "XMLPI"
	case IS_XMLROOT:
		opStr = "XMLROOT"
	case IS_XMLSERIALIZE:
		opStr = "XMLSERIALIZE"
	case IS_DOCUMENT:
		opStr = "IS_DOCUMENT"
	default:
		opStr = "UNKNOWN"
	}
	argCount := 0
	if x.Args != nil {
		argCount = len(x.Args.Items)
	}
	return fmt.Sprintf("XmlExpr{%s, %d args}@%d", opStr, argCount, x.Location())
}

// TableFunc represents a table function such as XMLTABLE and JSON_TABLE
// Ported from postgres/src/include/nodes/primnodes.h:109-146
type TableFunc struct {
	BaseNode
	Functype        TableFuncType // XMLTABLE or JSON_TABLE
	NsUris          []Expression  // List of namespace URI expressions
	NsNames         []string      // List of namespace names or NULL
	Docexpr         Expression    // Input document expression
	Rowexpr         Expression    // Row filter expression
	Colnames        []string      // Column names (list of String)
	Coltypes        []Oid         // OID list of column type OIDs
	Coltypmods      []int         // Integer list of column typmods
	Colcollations   []Oid         // OID list of column collation OIDs
	Colexprs        []Expression  // List of column filter expressions
	Coldefexprs     []Expression  // List of column default expressions
	Colvalexprs     []Expression  // JSON_TABLE: list of column value expressions
	Passingvalexprs []Expression  // JSON_TABLE: list of PASSING argument expressions
	Notnulls        []bool        // Nullability flag for each output column
	Plan            Node          // JSON_TABLE plan
	Ordinalitycol   int           // Counts from 0; -1 if none specified
}

// NewTableFunc creates a new TableFunc node.
func NewTableFunc(functype TableFuncType, nsUris []Expression, nsNames []string, docexpr, rowexpr Expression, colnames []string, coltypes []Oid, coltypmods []int, colcollations []Oid, colexprs, coldefexprs, colvalexprs, passingvalexprs []Expression, notnulls []bool, plan Node, ordinalitycol, location int) *TableFunc {
	return &TableFunc{
		BaseNode:        BaseNode{Tag: T_TableFunc, Loc: location},
		Functype:        functype,
		NsUris:          nsUris,
		NsNames:         nsNames,
		Docexpr:         docexpr,
		Rowexpr:         rowexpr,
		Colnames:        colnames,
		Coltypes:        coltypes,
		Coltypmods:      coltypmods,
		Colcollations:   colcollations,
		Colexprs:        colexprs,
		Coldefexprs:     coldefexprs,
		Colvalexprs:     colvalexprs,
		Passingvalexprs: passingvalexprs,
		Notnulls:        notnulls,
		Plan:            plan,
		Ordinalitycol:   ordinalitycol,
	}
}

func (t *TableFunc) String() string {
	funcStr := "XMLTABLE"
	if t.Functype == TFT_JSON_TABLE {
		funcStr = "JSON_TABLE"
	}
	return fmt.Sprintf("TableFunc{%s, %d cols}@%d", funcStr, len(t.Colnames), t.Location())
}

func (t *TableFunc) StatementType() string {
	return "TABLE_FUNC"
}

// IntoClause represents target information for SELECT INTO, CREATE TABLE AS, and CREATE MATERIALIZED VIEW
// Ported from postgres/src/include/nodes/primnodes.h:158-171
type IntoClause struct {
	BaseNode
	Rel            *RangeVar      // Target relation name
	ColNames       *NodeList      // Column names to assign, or NIL
	AccessMethod   string         // Table access method
	Options        *NodeList      // Options from WITH clause
	OnCommit       OnCommitAction // What do we do at COMMIT?
	TableSpaceName string         // Table space to use, or NULL
	ViewQuery      Node           // Materialized view's SELECT query
	SkipData       bool           // True for WITH NO DATA
}

// NewIntoClause creates a new IntoClause node.
func NewIntoClause(rel *RangeVar, colNames *NodeList, accessMethod string, options *NodeList, onCommit OnCommitAction, tableSpaceName string, viewQuery Node, skipData bool, location int) *IntoClause {
	return &IntoClause{
		BaseNode:       BaseNode{Tag: T_IntoClause, Loc: location},
		Rel:            rel,
		ColNames:       colNames,
		AccessMethod:   accessMethod,
		Options:        options,
		OnCommit:       onCommit,
		TableSpaceName: tableSpaceName,
		ViewQuery:      viewQuery,
		SkipData:       skipData,
	}
}

func (i *IntoClause) String() string {
	var target string
	if i.Rel != nil {
		target = i.Rel.String()
	} else {
		target = "?"
	}
	return fmt.Sprintf("IntoClause{%s, skipData=%t}@%d", target, i.SkipData, i.Location())
}

func (i *IntoClause) StatementType() string {
	return "INTO_CLAUSE"
}

// SqlString returns the SQL representation of the IntoClause
// TargetString returns just the target relation and column list without "INTO"
func (i *IntoClause) TargetString() string {
	if i.Rel == nil {
		return ""
	}
	
	var parts []string
	
	// Add the target relation
	parts = append(parts, i.Rel.SqlString())
	
	// Add column names if specified
	if i.ColNames != nil && len(i.ColNames.Items) > 0 {
		var colNames []string
		for _, item := range i.ColNames.Items {
			if str, ok := item.(*String); ok {
				colNames = append(colNames, str.SVal)
			}
		}
		if len(colNames) > 0 {
			parts = append(parts, fmt.Sprintf("(%s)", strings.Join(colNames, ", ")))
		}
	}
	
	return strings.Join(parts, " ")
}

func (i *IntoClause) SqlString() string {
	if i.Rel == nil {
		return ""
	}
	
	parts := []string{"INTO"}
	
	// Add persistence keywords if present
	if i.Rel.RelPersistence == RELPERSISTENCE_TEMP {
		parts = append(parts, "TEMPORARY")
	} else if i.Rel.RelPersistence == RELPERSISTENCE_UNLOGGED {
		parts = append(parts, "UNLOGGED")
	}
	
	// Add the target relation
	parts = append(parts, i.Rel.SqlString())
	
	// Add column names if specified
	if i.ColNames != nil && len(i.ColNames.Items) > 0 {
		var colNames []string
		for _, item := range i.ColNames.Items {
			if str, ok := item.(*String); ok {
				colNames = append(colNames, str.SVal)
			}
		}
		if len(colNames) > 0 {
			parts = append(parts, fmt.Sprintf("(%s)", strings.Join(colNames, ", ")))
		}
	}
	
	// Add WITH options if present
	if i.Options != nil && len(i.Options.Items) > 0 {
		var opts []string
		for _, opt := range i.Options.Items {
			if opt != nil {
				opts = append(opts, opt.SqlString())
			}
		}
		parts = append(parts, "WITH", fmt.Sprintf("(%s)", strings.Join(opts, ", ")))
	}
	
	// Add ON COMMIT clause
	switch i.OnCommit {
	case ONCOMMIT_DROP:
		parts = append(parts, "ON COMMIT DROP")
	case ONCOMMIT_DELETE_ROWS:
		parts = append(parts, "ON COMMIT DELETE ROWS")
	case ONCOMMIT_PRESERVE_ROWS:
		parts = append(parts, "ON COMMIT PRESERVE ROWS")
	}
	
	// Add TABLESPACE clause
	if i.TableSpaceName != "" {
		parts = append(parts, "TABLESPACE", QuoteIdentifier(i.TableSpaceName))
	}
	
	// Add WITH NO DATA if specified
	if i.SkipData {
		parts = append(parts, "WITH NO DATA")
	}
	
	return strings.Join(parts, " ")
}

// MergeAction represents a MERGE action
// Ported from postgres/src/include/nodes/primnodes.h:2003-2013
type MergeAction struct {
	BaseNode
	MatchKind    MergeMatchKind // MATCHED/NOT MATCHED BY SOURCE/TARGET
	CommandType  CmdType        // INSERT/UPDATE/DELETE/DO NOTHING
	Override     OverridingKind // OVERRIDING clause
	Qual         Node           // Transformed WHEN conditions
	TargetList   []*TargetEntry // The target list (of TargetEntry)
	UpdateColnos []AttrNumber   // Target attribute numbers of an UPDATE
}

// NewMergeAction creates a new MergeAction node.
func NewMergeAction(matchKind MergeMatchKind, commandType CmdType, override OverridingKind, qual Node, targetList []*TargetEntry, updateColnos []AttrNumber, location int) *MergeAction {
	return &MergeAction{
		BaseNode:     BaseNode{Tag: T_MergeAction, Loc: location},
		MatchKind:    matchKind,
		CommandType:  commandType,
		Override:     override,
		Qual:         qual,
		TargetList:   targetList,
		UpdateColnos: updateColnos,
	}
}

func (m *MergeAction) String() string {
	var matchStr, cmdStr string
	switch m.MatchKind {
	case MERGE_WHEN_MATCHED:
		matchStr = "MATCHED"
	case MERGE_WHEN_NOT_MATCHED_BY_SOURCE:
		matchStr = "NOT MATCHED BY SOURCE"
	case MERGE_WHEN_NOT_MATCHED_BY_TARGET:
		matchStr = "NOT MATCHED BY TARGET"
	default:
		matchStr = "UNKNOWN"
	}
	switch m.CommandType {
	case CMD_INSERT:
		cmdStr = "INSERT"
	case CMD_UPDATE:
		cmdStr = "UPDATE"
	case CMD_DELETE:
		cmdStr = "DELETE"
	default:
		cmdStr = "DO NOTHING"
	}
	return fmt.Sprintf("MergeAction{%s %s, %d targets}@%d", matchStr, cmdStr, len(m.TargetList), m.Location())
}

func (m *MergeAction) StatementType() string {
	return "MERGE_ACTION"
}
