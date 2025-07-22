// Package ast provides PostgreSQL AST expression node definitions.
// Ported from postgres/src/include/nodes/primnodes.h
package ast

import (
	"fmt"
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

// ParamKind represents parameter types - ported from postgres/src/include/nodes/primnodes.h:373-385
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
// Ported from postgres/src/include/nodes/primnodes.h:247-294
type Var struct {
	BaseExpr
	Varno           Index      // Relation index in range table - postgres/src/include/nodes/primnodes.h:252
	Varattno        AttrNumber // Attribute number (0 = whole-row) - postgres/src/include/nodes/primnodes.h:253
	Vartype         Oid        // pg_type OID - postgres/src/include/nodes/primnodes.h:254
	Vartypmod       int32      // Type modifier - postgres/src/include/nodes/primnodes.h:255
	Varcollid       Oid        // Collation OID - postgres/src/include/nodes/primnodes.h:256
	Varlevelsup     Index      // Subquery nesting level - postgres/src/include/nodes/primnodes.h:268
	Varnosyn        Index      // Syntactic relation index - postgres/src/include/nodes/primnodes.h:279
	Varattnosyn     AttrNumber // Syntactic attribute number - postgres/src/include/nodes/primnodes.h:280
}

// NewVar creates a new Var node.
func NewVar(varno Index, varattno AttrNumber, vartype Oid) *Var {
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
// Ported from postgres/src/include/nodes/primnodes.h:306-336
type Const struct {
	BaseExpr
	Consttype     Oid    // Datatype OID - postgres/src/include/nodes/primnodes.h:309
	Consttypmod   int32  // Type modifier - postgres/src/include/nodes/primnodes.h:310
	Constcollid   Oid    // Collation OID - postgres/src/include/nodes/primnodes.h:311
	Constlen      int    // Type length - postgres/src/include/nodes/primnodes.h:312
	Constvalue    Datum  // The actual value - postgres/src/include/nodes/primnodes.h:313
	Constisnull   bool   // Whether null - postgres/src/include/nodes/primnodes.h:314
	Constbyval    bool   // Pass by value? - postgres/src/include/nodes/primnodes.h:315
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
// Ported from postgres/src/include/nodes/primnodes.h:387-409
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
// Ported from postgres/src/include/nodes/primnodes.h:746-771
type FuncExpr struct {
	BaseExpr
	Funcid         Oid          // pg_proc OID - postgres/src/include/nodes/primnodes.h:749
	Funcresulttype Oid          // Result type OID - postgres/src/include/nodes/primnodes.h:750
	Funcretset     bool         // Returns set? - postgres/src/include/nodes/primnodes.h:751
	Funcvariadic   bool         // Variadic arguments? - postgres/src/include/nodes/primnodes.h:752
	Funcformat     CoercionForm // Display format - postgres/src/include/nodes/primnodes.h:753
	Funccollid     Oid          // Result collation - postgres/src/include/nodes/primnodes.h:754
	Inputcollid    Oid          // Input collation - postgres/src/include/nodes/primnodes.h:755
	Args           []Node       // Function arguments - postgres/src/include/nodes/primnodes.h:756
}

// NewFuncExpr creates a new FuncExpr node.
func NewFuncExpr(funcid Oid, funcresulttype Oid, args []Node) *FuncExpr {
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
	return fmt.Sprintf("FuncExpr(oid:%d, %d args)@%d", f.Funcid, len(f.Args), f.Location())
}

// OpExpr represents a binary or unary operator expression.
// Ported from postgres/src/include/nodes/primnodes.h:813-840
type OpExpr struct {
	BaseExpr
	Opno         Oid  // pg_operator OID - postgres/src/include/nodes/primnodes.h:816
	Opfuncid     Oid  // Underlying function OID - postgres/src/include/nodes/primnodes.h:817
	Opresulttype Oid  // Result type - postgres/src/include/nodes/primnodes.h:818
	Opretset     bool // Returns set? - postgres/src/include/nodes/primnodes.h:819
	Opcollid     Oid  // Result collation - postgres/src/include/nodes/primnodes.h:820
	Inputcollid  Oid  // Input collation - postgres/src/include/nodes/primnodes.h:821
	Args         []Node // Operator arguments (1 or 2) - postgres/src/include/nodes/primnodes.h:822
}

// NewOpExpr creates a new OpExpr node.
func NewOpExpr(opno Oid, opfuncid Oid, opresulttype Oid, args []Node) *OpExpr {
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
	if len(o.Args) == 1 {
		opType = "unary"
	}
	return fmt.Sprintf("OpExpr(%s, oid:%d)@%d", opType, o.Opno, o.Location())
}

// BoolExpr represents a boolean expression (AND/OR/NOT).
// Ported from postgres/src/include/nodes/primnodes.h:944-952
type BoolExpr struct {
	BaseExpr
	Boolop BoolExprType // AND/OR/NOT - postgres/src/include/nodes/primnodes.h:947
	Args   []Node       // Operand expressions - postgres/src/include/nodes/primnodes.h:948
}

// NewBoolExpr creates a new BoolExpr node.
func NewBoolExpr(boolop BoolExprType, args []Node) *BoolExpr {
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
	return fmt.Sprintf("BoolExpr(%s, %d args)@%d", b.Boolop, len(b.Args), b.Location())
}

// ==============================================================================
// CONVENIENCE CONSTRUCTORS FOR COMMON PATTERNS
// ==============================================================================

// NewAndExpr creates a new AND boolean expression.
func NewAndExpr(left, right Node) *BoolExpr {
	return NewBoolExpr(AND_EXPR, []Node{left, right})
}

// NewOrExpr creates a new OR boolean expression.
func NewOrExpr(left, right Node) *BoolExpr {
	return NewBoolExpr(OR_EXPR, []Node{left, right})
}

// NewNotExpr creates a new NOT boolean expression.
func NewNotExpr(arg Node) *BoolExpr {
	return NewBoolExpr(NOT_EXPR, []Node{arg})
}

// NewBinaryOp creates a binary operator expression.
func NewBinaryOp(opno Oid, left, right Node) *OpExpr {
	return NewOpExpr(opno, 0, 0, []Node{left, right})
}

// NewUnaryOp creates a unary operator expression.
func NewUnaryOp(opno Oid, arg Node) *OpExpr {
	return NewOpExpr(opno, 0, 0, []Node{arg})
}

// ==============================================================================
// TIER 2 EXPRESSIONS - Common SQL Features
// ==============================================================================

// CaseExpr represents a CASE expression.
// Ported from postgres/src/include/nodes/primnodes.h:1306-1317
type CaseExpr struct {
	BaseExpr
	Casetype   Oid    // Result type - postgres/src/include/nodes/primnodes.h:1309
	Casecollid Oid    // Result collation - postgres/src/include/nodes/primnodes.h:1310
	Arg        Node   // Implicit comparison argument - postgres/src/include/nodes/primnodes.h:1311
	Args       []Node // List of WHEN clauses - postgres/src/include/nodes/primnodes.h:1312
	Defresult  Node   // Default result (ELSE) - postgres/src/include/nodes/primnodes.h:1313
}

// NewCaseExpr creates a new CaseExpr node.
func NewCaseExpr(casetype Oid, arg Node, whens []Node, defresult Node) *CaseExpr {
	return &CaseExpr{
		BaseExpr:   BaseExpr{BaseNode: BaseNode{Tag: T_CaseExpr}},
		Casetype:   casetype,
		Arg:        arg,
		Args:       whens,
		Defresult:  defresult,
	}
}

func (c *CaseExpr) ExpressionType() string {
	return "CaseExpr"
}

func (c *CaseExpr) String() string {
	whenCount := len(c.Args)
	hasElse := c.Defresult != nil
	return fmt.Sprintf("CaseExpr(%d whens, else:%t)@%d", whenCount, hasElse, c.Location())
}

// CaseWhen represents a WHEN clause in a CASE expression.
// Ported from postgres/src/include/nodes/primnodes.h:1322-1328
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
// Ported from postgres/src/include/nodes/primnodes.h:1484-1495
type CoalesceExpr struct {
	BaseExpr
	Coalescetype   Oid    // Result type - postgres/src/include/nodes/primnodes.h:1487
	Coalescecollid Oid    // Result collation - postgres/src/include/nodes/primnodes.h:1488
	Args           []Node // Argument expressions - postgres/src/include/nodes/primnodes.h:1489
}

// NewCoalesceExpr creates a new CoalesceExpr node.
func NewCoalesceExpr(coalescetype Oid, args []Node) *CoalesceExpr {
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
	return fmt.Sprintf("CoalesceExpr(%d args)@%d", len(c.Args), c.Location())
}

// ArrayExpr represents an ARRAY[] constructor expression.
// Ported from postgres/src/include/nodes/primnodes.h:1370-1385
type ArrayExpr struct {
	BaseExpr
	ArrayTypeid   Oid    // Array type OID - postgres/src/include/nodes/primnodes.h:1373
	ArrayCollid   Oid    // Array collation - postgres/src/include/nodes/primnodes.h:1374
	ElementTypeid Oid    // Element type OID - postgres/src/include/nodes/primnodes.h:1375
	Elements      []Node // Array elements - postgres/src/include/nodes/primnodes.h:1376
	Multidims     bool   // Multi-dimensional? - postgres/src/include/nodes/primnodes.h:1377
}

// NewArrayExpr creates a new ArrayExpr node.
func NewArrayExpr(arrayTypeid Oid, elementTypeid Oid, elements []Node) *ArrayExpr {
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
	return fmt.Sprintf("ArrayExpr(%s, %d elements)@%d", dims, len(a.Elements), a.Location())
}

// ScalarArrayOpExpr represents a scalar op ANY/ALL (array) expression.
// Ported from postgres/src/include/nodes/primnodes.h:893-920
type ScalarArrayOpExpr struct {
	BaseExpr
	Opno         Oid    // pg_operator OID - postgres/src/include/nodes/primnodes.h:896
	Opfuncid     Oid    // Comparison function OID - postgres/src/include/nodes/primnodes.h:897
	Hashfuncid   Oid    // Hash function OID (optimization) - postgres/src/include/nodes/primnodes.h:898
	Negfuncid    Oid    // Negation function OID - postgres/src/include/nodes/primnodes.h:899
	UseOr        bool   // True for ANY, false for ALL - postgres/src/include/nodes/primnodes.h:900
	Inputcollid  Oid    // Input collation - postgres/src/include/nodes/primnodes.h:901
	Args         []Node // Scalar and array operands - postgres/src/include/nodes/primnodes.h:902
}

// NewScalarArrayOpExpr creates a new ScalarArrayOpExpr node.
func NewScalarArrayOpExpr(opno Oid, useOr bool, scalar Node, array Node) *ScalarArrayOpExpr {
	return &ScalarArrayOpExpr{
		BaseExpr: BaseExpr{BaseNode: BaseNode{Tag: T_ScalarArrayOpExpr}},
		Opno:     opno,
		UseOr:    useOr,
		Args:     []Node{scalar, array},
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
// Ported from postgres/src/include/nodes/primnodes.h:1408+
type RowExpr struct {
	BaseExpr
	Args       []Node        // Row field expressions - postgres/src/include/nodes/primnodes.h:1411
	RowTypeid  Oid           // Composite type OID - postgres/src/include/nodes/primnodes.h:1412
	RowFormat  CoercionForm  // Display format - postgres/src/include/nodes/primnodes.h:1413
	Colnames   []*string     // Field names (RECORD type only) - postgres/src/include/nodes/primnodes.h:1414
}

// NewRowExpr creates a new RowExpr node.
func NewRowExpr(args []Node, rowTypeid Oid) *RowExpr {
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
	return fmt.Sprintf("RowExpr(%d fields)@%d", len(r.Args), r.Location())
}

// ==============================================================================
// TIER 2 CONVENIENCE CONSTRUCTORS
// ==============================================================================

// NewSimpleCase creates a simple CASE expression: CASE expr WHEN val1 THEN result1 ... ELSE def END
func NewSimpleCase(expr Node, whens []Node, defresult Node) *CaseExpr {
	return NewCaseExpr(0, expr, whens, defresult)
}

// NewSearchedCase creates a searched CASE expression: CASE WHEN condition1 THEN result1 ... ELSE def END
func NewSearchedCase(whens []Node, defresult Node) *CaseExpr {
	return NewCaseExpr(0, nil, whens, defresult)
}

// NewArrayConstructor creates an ARRAY[...] constructor.
func NewArrayConstructor(elements []Node) *ArrayExpr {
	return NewArrayExpr(0, 0, elements)
}

// NewRowConstructor creates a ROW(...) constructor.
func NewRowConstructor(fields []Node) *RowExpr {
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
func GetExpressionArgs(expr Node) []Node {
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
		return []Node{e.Expr, e.Result}
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
			return []Node{e.Testexpr, e.Subselect}
		}
		return []Node{e.Subselect}
	default:
		return nil
	}
}

// CountArgs returns the number of arguments in an expression.
func CountArgs(expr Node) int {
	args := GetExpressionArgs(expr)
	return len(args)
}

// ==============================================================================
// TIER 3 EXPRESSIONS - Advanced Expressions and Aggregations
// ==============================================================================

// SubLinkType represents types of sublinks - ported from postgres/src/include/nodes/primnodes.h:1556-1565
type SubLinkType int

const (
	EXISTS_SUBLINK    SubLinkType = iota // EXISTS(subquery)
	ALL_SUBLINK                          // expr op ALL(subquery)
	ANY_SUBLINK                          // expr op ANY(subquery)
	ROWCOMPARE_SUBLINK                   // (expr list) op (subquery)
	EXPR_SUBLINK                         // expr(subquery)
	MULTIEXPR_SUBLINK                    // multiple expressions
	ARRAY_SUBLINK                        // ARRAY(subquery)
	CTE_SUBLINK                          // for SubPlans only
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
	AGGSPLIT_SIMPLE          AggSplit = 0  // Basic, non-split aggregation
	AGGSPLIT_INITIAL_SERIAL  AggSplit = 3  // Initial phase with serialization
	AGGSPLIT_FINAL_DESERIAL  AggSplit = 12 // Final phase with deserialization
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
// Ported from postgres/src/include/nodes/primnodes.h:439-508
type Aggref struct {
	BaseExpr
	Aggfnoid      Oid       // pg_proc OID of the aggregate - postgres/src/include/nodes/primnodes.h:481
	Aggtype       Oid       // Result type OID - postgres/src/include/nodes/primnodes.h:483
	Aggcollid     Oid       // Result collation - postgres/src/include/nodes/primnodes.h:485
	Inputcollid   Oid       // Input collation - postgres/src/include/nodes/primnodes.h:487
	Aggtranstype  Oid       // Transition value type - postgres/src/include/nodes/primnodes.h:492
	Aggargtypes   []Oid     // Argument type OIDs - postgres/src/include/nodes/primnodes.h:494
	Aggdirectargs []Node    // Direct arguments for ordered-set aggs - postgres/src/include/nodes/primnodes.h:495
	Args          []Node    // Aggregated arguments - postgres/src/include/nodes/primnodes.h:496
	Aggorder      []Node    // ORDER BY expressions - postgres/src/include/nodes/primnodes.h:497
	Aggdistinct   []Node    // DISTINCT expressions - postgres/src/include/nodes/primnodes.h:498
	Aggfilter     Node      // FILTER expression - postgres/src/include/nodes/primnodes.h:499
	Aggstar       bool      // True if argument list was '*' - postgres/src/include/nodes/primnodes.h:501
	Aggvariadic   bool      // True if variadic args combined - postgres/src/include/nodes/primnodes.h:505
	Aggkind       byte      // Aggregate kind - postgres/src/include/nodes/primnodes.h:507
	Aggpresorted  bool      // Input already sorted - postgres/src/include/nodes/primnodes.h:509
	Agglevelsup   Index     // > 0 if agg belongs to outer query - postgres/src/include/nodes/primnodes.h:511
	Aggsplit      AggSplit  // Expected splitting mode - postgres/src/include/nodes/primnodes.h:513
	Aggno         int       // Unique ID within Agg node - postgres/src/include/nodes/primnodes.h:515
	Aggtransno    int       // Unique ID of transition state - postgres/src/include/nodes/primnodes.h:517
}

// NewAggref creates a new Aggref node.
func NewAggref(aggfnoid Oid, aggtype Oid, args []Node) *Aggref {
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
	if len(a.Aggdistinct) > 0 {
		distinct = " DISTINCT"
	}
	filter := ""
	if a.Aggfilter != nil {
		filter = " FILTER"
	}
	return fmt.Sprintf("Aggref(oid:%d%s%s, %d args)@%d", a.Aggfnoid, distinct, filter, len(a.Args), a.Location())
}

// WindowFunc represents a window function call expression.
// Ported from postgres/src/include/nodes/primnodes.h:590-622
type WindowFunc struct {
	BaseExpr
	Winfnoid      Oid    // pg_proc OID of the function - postgres/src/include/nodes/primnodes.h:593
	Wintype       Oid    // Result type OID - postgres/src/include/nodes/primnodes.h:595
	Wincollid     Oid    // Result collation - postgres/src/include/nodes/primnodes.h:597
	Inputcollid   Oid    // Input collation - postgres/src/include/nodes/primnodes.h:599
	Args          []Node // Arguments to window function - postgres/src/include/nodes/primnodes.h:601
	Aggfilter     Node   // FILTER expression - postgres/src/include/nodes/primnodes.h:603
	RunCondition  []Node // List of run conditions - postgres/src/include/nodes/primnodes.h:605
	Winref        Index  // Index of associated WindowClause - postgres/src/include/nodes/primnodes.h:607
	Winstar       bool   // True if argument list was '*' - postgres/src/include/nodes/primnodes.h:609
	Winagg        bool   // Is function a simple aggregate? - postgres/src/include/nodes/primnodes.h:611
}

// NewWindowFunc creates a new WindowFunc node.
func NewWindowFunc(winfnoid Oid, wintype Oid, args []Node, winref Index) *WindowFunc {
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
		star = fmt.Sprintf("%d args", len(w.Args))
	}
	filter := ""
	if w.Aggfilter != nil {
		filter = " FILTER"
	}
	return fmt.Sprintf("WindowFunc(oid:%d%s, %s)@%d", w.Winfnoid, filter, star, w.Location())
}

// SubLink represents a sublink expression (subquery).
// Ported from postgres/src/include/nodes/primnodes.h:1572-1583
type SubLink struct {
	BaseExpr
	SubLinkType SubLinkType // Type of sublink - postgres/src/include/nodes/primnodes.h:1575
	SubLinkId   int         // ID (1..n); 0 if not MULTIEXPR - postgres/src/include/nodes/primnodes.h:1576
	Testexpr    Node        // Outer-query test for ALL/ANY/ROWCOMPARE - postgres/src/include/nodes/primnodes.h:1577
	OperName    []Node      // Originally specified operator name - postgres/src/include/nodes/primnodes.h:1579
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
	return NewAggref(2147, 20, []Node{expr}) // COUNT function OID 2147, result type bigint 20
}

// NewSum creates a SUM(expr) aggregate.
func NewSum(expr Node) *Aggref {
	return NewAggref(2108, 0, []Node{expr}) // SUM function OID (varies by type)
}

// NewAvg creates an AVG(expr) aggregate.
func NewAvg(expr Node) *Aggref {
	return NewAggref(2100, 0, []Node{expr}) // AVG function OID (varies by type)
}

// NewMax creates a MAX(expr) aggregate.
func NewMax(expr Node) *Aggref {
	return NewAggref(2116, 0, []Node{expr}) // MAX function OID (varies by type)
}

// NewMin creates a MIN(expr) aggregate.
func NewMin(expr Node) *Aggref {
	return NewAggref(2132, 0, []Node{expr}) // MIN function OID (varies by type)
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
	return NewWindowFunc(3105, 0, []Node{expr}, 0) // LAG function OID (varies by type)
}

// NewLead creates a LEAD(expr) window function.
func NewLead(expr Node) *WindowFunc {
	return NewWindowFunc(3106, 0, []Node{expr}, 0) // LEAD function OID (varies by type)
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
		return len(caseExpr.Args)
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
func GetArrayElements(expr Node) []Node {
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
func GetAggregateArgs(expr Node) []Node {
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
		return len(aggref.Aggdistinct) > 0
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
		return len(aggref.Aggdirectargs) > 0
	}
	return false
}

// GetAggregateDirectArgs returns the direct arguments of an ordered-set aggregate.
func GetAggregateDirectArgs(expr Node) []Node {
	if aggref, ok := expr.(*Aggref); ok {
		return aggref.Aggdirectargs
	}
	return nil
}

// GetAggregateOrderBy returns the ORDER BY expressions of an aggregate.
func GetAggregateOrderBy(expr Node) []Node {
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