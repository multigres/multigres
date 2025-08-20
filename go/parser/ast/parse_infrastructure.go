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
	StmtLocation int       // Start location of stmt in original query string
	StmtLen      int       // Length of stmt in original query string
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
	Kind     A_Expr_Kind // Expression type (operator, comparison, etc.)
	Name     []*String   // Possibly-qualified operator name
	Lexpr    Node        // Left operand
	Rexpr    Node        // Right operand (or NULL for unary operators)
	// Note: Location is handled by BaseNode.Loc
}

// A_Expr_Kind represents the type of A_Expr.
// Ported from postgres/src/include/nodes/parsenodes.h:331-339
type A_Expr_Kind int

const (
	AEXPR_OP         A_Expr_Kind = iota // Normal operator
	AEXPR_OP_ANY                        // Scalar op ANY (array)
	AEXPR_OP_ALL                        // Scalar op ALL (array)
	AEXPR_DISTINCT                      // IS DISTINCT FROM
	AEXPR_NOT_DISTINCT                  // IS NOT DISTINCT FROM
	AEXPR_NULLIF                        // NULLIF(a, b)
	AEXPR_IN                            // IN (list)
	AEXPR_LIKE                          // LIKE
	AEXPR_ILIKE                         // ILIKE
	AEXPR_SIMILAR                       // SIMILAR TO
	AEXPR_BETWEEN                       // BETWEEN
	AEXPR_NOT_BETWEEN                   // NOT BETWEEN
	AEXPR_BETWEEN_SYM                   // BETWEEN SYMMETRIC
	AEXPR_NOT_BETWEEN_SYM               // NOT BETWEEN SYMMETRIC
)

// NewA_Expr creates a new A_Expr node.
func NewA_Expr(kind A_Expr_Kind, name []*String, lexpr, rexpr Node, location int) *A_Expr {
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
		if len(a.Name) == 0 {
			return "UNKNOWN_OP"
		}
		op := a.Name[0].SVal
		
		// Unary operators (NOT, unary +, unary -)
		if a.Lexpr == nil && a.Rexpr != nil {
			if op == "NOT" {
				return fmt.Sprintf("NOT %s", a.Rexpr.SqlString())
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
		
	case AEXPR_LIKE:
		if a.Lexpr != nil && a.Rexpr != nil {
			leftStr := a.Lexpr.SqlString()
			rightStr := a.Rexpr.SqlString()
			return fmt.Sprintf("%s LIKE %s", leftStr, rightStr)
		}
		
	case AEXPR_BETWEEN:
		// For now, simplified - full BETWEEN would need more complex structure
		return "BETWEEN_EXPR"
		
	default:
		return fmt.Sprintf("A_EXPR_%d", a.Kind)
	}
	
	return "UNKNOWN_A_EXPR"
}

func (a *A_Expr) ExpressionType() string {
	return "A_EXPR"
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
	Number   int // Parameter number (1-based)
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

// SqlString returns the SQL representation of the TypeCast (::type syntax)
func (t *TypeCast) SqlString() string {
	argStr := ""
	if t.Arg != nil {
		argStr = t.Arg.SqlString()
	}
	
	typeStr := ""
	if t.TypeName != nil {
		typeStr = t.TypeName.SqlString()
	}
	
	return fmt.Sprintf("%s::%s", argStr, typeStr)
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

func (p *ParenExpr) ExpressionType() string {
	return "PAREN_EXPR"
}

// FuncCall represents a function call in the parse tree.
// Ported from postgres/src/include/nodes/parsenodes.h:423-444
type FuncCall struct {
	BaseNode
	Funcname       []*String    // Qualified function name
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
func NewFuncCall(funcname []*String, args *NodeList, location int) *FuncCall {
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
	if len(f.Funcname) > 0 && f.Funcname[0] != nil {
		return fmt.Sprintf("FuncCall{%s}@%d", f.Funcname[0].SVal, f.Location())
	}
	return fmt.Sprintf("FuncCall@%d", f.Location())
}

// SqlString returns the SQL representation of the FuncCall
func (f *FuncCall) SqlString() string {
	// Build function name (could be qualified like schema.func)
	funcName := ""
	if len(f.Funcname) > 0 {
		var nameParts []string
		for _, part := range f.Funcname {
			if part != nil {
				nameParts = append(nameParts, part.SVal)
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
	
	return fmt.Sprintf("%s(%s)", funcName, strings.Join(argStrs, ", "))
}

func (f *FuncCall) ExpressionType() string {
	return "FUNC_CALL"
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

// SqlString returns the SQL representation of A_Star
func (a *A_Star) SqlString() string {
	return "*"
}

// A_Indices represents array indices in the parse tree (e.g., array[1:3]).
// Ported from postgres/src/include/nodes/parsenodes.h:456-462
type A_Indices struct {
	BaseNode
	IsSlice   bool // True for slicing (e.g., array[1:3])
	Lidx      Node // Lower index (NULL if not specified)
	Uidx      Node // Upper index (NULL if not specified)
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
	Arg       Node   // The base expression
	Indirection *NodeList // List of A_Indices and/or String nodes
}

// NewA_Indirection creates a new A_Indirection node.
func NewA_Indirection(arg Node, indirection *NodeList, location int) *A_Indirection {
	aIndirection := &A_Indirection{
		BaseNode:   BaseNode{Tag: T_A_Indirection},
		Arg:        arg,
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
	Recursive bool   // TRUE for WITH RECURSIVE
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
	OrderClause     []*SortBy // ORDER BY (list of SortBy)
	FrameOptions    int       // Frame_option flags
	StartOffset     Node      // Expression for start offset
	EndOffset       Node      // Expression for end offset
}

// NewWindowDef creates a new WindowDef node.
func NewWindowDef(name string, location int) *WindowDef {
	windowDef := &WindowDef{
		BaseNode:        BaseNode{Tag: T_WindowDef},
		Name:            name,
		PartitionClause: NewNodeList(),
		OrderClause:     []*SortBy{},
		FrameOptions:    0,
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

// Note: SortBy, SortByDir, and SortByNulls already exist in ddl_statements.go
// However, the existing SortBy is incomplete - let me implement a more complete version

// SortBy represents a sort specification in ORDER BY clauses.
// Ported from postgres/src/include/nodes/parsenodes.h:543-560
type SortBy struct {
	BaseNode
	Node        Node        // Expression to sort on
	SortbyDir   SortByDir   // ASC/DESC/USING/DEFAULT
	SortbyNulls SortByNulls // NULLS FIRST/LAST
	UseOp       []*String   // Name of operator to use for comparison
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
		if len(s.UseOp) > 0 {
			var ops []string
			for _, op := range s.UseOp {
				if op != nil {
					ops = append(ops, op.SqlString())
				}
			}
			result += " USING " + strings.Join(ops, ".")
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
	Kind     GroupingSetKind // Type of grouping set
	Content  *NodeList       // List of expressions
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

// LockingClause represents a complete locking clause (FOR UPDATE, FOR SHARE, etc.).
// Ported from postgres/src/include/nodes/parsenodes.h:831-841
type LockingClause struct {
	BaseNode
	LockedRels []*RangeVar      // For table locking, list of RangeVar
	Strength   LockClauseStrength // Lock strength
	WaitPolicy LockWaitPolicy   // NOWAIT and SKIP LOCKED
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
func NewLockingClause(lockedRels []*RangeVar, strength LockClauseStrength, waitPolicy LockWaitPolicy, location int) *LockingClause {
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
	if len(l.LockedRels) > 0 {
		var tables []string
		for _, rel := range l.LockedRels {
			if rel != nil {
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

// PartitionElem represents a partition element in partition specifications.
// Ported from postgres/src/include/nodes/parsenodes.h:860-881
type PartitionElem struct {
	BaseNode
	Name       string        // Name of column to partition on
	Expr       Node          // Expression to partition on, or NULL
	Collation  []*String     // Collation name
	Opclass    []*String     // Operator class name
}

// NewPartitionElem creates a new PartitionElem node.
func NewPartitionElem(name string, expr Node, location int) *PartitionElem {
	partitionElem := &PartitionElem{
		BaseNode:  BaseNode{Tag: T_PartitionElem},
		Name:      name,
		Expr:      expr,
		Collation: []*String{},
		Opclass:   []*String{},
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

func (p *PartitionElem) StatementType() string {
	return "PARTITION_ELEM"
}

// TableSampleClause represents a TABLESAMPLE clause.
// Ported from postgres/src/include/nodes/parsenodes.h:1344-1367
type TableSampleClause struct {
	BaseNode
	Tsmhandler   Oid     // OID of the tablesample handler function
	Args         *NodeList // List of tablesample arguments
	Repeatable   Expr    // REPEATABLE expression, or NULL
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
	Objname      []*String // Qualified object name
	Objargs      *NodeList // List of argument types (TypeName nodes)
	ObjfuncArgs  *NodeList // List of function arguments for ALTER FUNCTION
	ArgsUnspecified bool   // Arguments were omitted, so name must be unique
}

// NewObjectWithArgs creates a new ObjectWithArgs node.
func NewObjectWithArgs(objname []*String, objargs *NodeList, argsUnspecified bool, location int) *ObjectWithArgs {
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
	if len(o.Objname) > 0 && o.Objname[len(o.Objname)-1] != nil {
		return fmt.Sprintf("ObjectWithArgs{%s}@%d", o.Objname[len(o.Objname)-1].SVal, o.Location())
	}
	return fmt.Sprintf("ObjectWithArgs@%d", o.Location())
}

func (o *ObjectWithArgs) StatementType() string {
	return "OBJECT_WITH_ARGS"
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
	Name      *RangeVar // Name of the partition
	Bound     *PartitionBoundSpec // Partition bound specification
	Concurrent bool     // CONCURRENTLY option
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

func (p *PartitionCmd) StatementType() string {
	return "PARTITION_CMD"
}

// ==============================================================================
// SUPPORTING CONSTANTS AND HELPER TYPES
// ==============================================================================

// InvalidOid represents an invalid object identifier.
const InvalidOid = Oid(0)

// char represents a single character (PostgreSQL char type).
type char byte