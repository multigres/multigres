// Package ast provides PostgreSQL AST parse infrastructure node definitions.
// This file contains core parsing infrastructure nodes essential for lexer/parser integration.
// Ported from postgres/src/include/nodes/parsenodes.h
package ast

import (
	"fmt"
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
	Stmt         Statement // The parsed statement tree
	StmtLocation int       // Start location of stmt in original query string
	StmtLen      int       // Length of stmt in original query string
}

// NewRawStmt creates a new RawStmt node.
func NewRawStmt(stmt Statement, location int, length int) *RawStmt {
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

func (t *TypeCast) String() string {
	return fmt.Sprintf("TypeCast@%d", t.Location())
}

func (t *TypeCast) ExpressionType() string {
	return "TYPE_CAST"
}

// FuncCall represents a function call in the parse tree.
// Ported from postgres/src/include/nodes/parsenodes.h:423-444
type FuncCall struct {
	BaseNode
	Funcname       []*String    // Qualified function name
	Args           []Node       // List of arguments
	AggOrder       []Node       // ORDER BY list for aggregates
	AggFilter      Node         // FILTER clause for aggregates
	Over           *WindowDef   // OVER clause for window functions
	AggWithinGroup bool         // ORDER BY appeared in WITHIN GROUP
	AggStar        bool         // Function was written as foo(*)
	AggDistinct    bool         // DISTINCT was specified
	FuncVariadic   bool         // VARIADIC was specified
	Funcformat     CoercionForm // How to display this node
}

// NewFuncCall creates a new FuncCall node.
func NewFuncCall(funcname []*String, args []Node, location int) *FuncCall {
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

// A_Indirection represents indirection (field access) in the parse tree (e.g., obj.field).
// Ported from postgres/src/include/nodes/parsenodes.h:479-488
type A_Indirection struct {
	BaseNode
	Arg       Node   // The base expression
	Indirection []Node // List of A_Indices and/or String nodes
}

// NewA_Indirection creates a new A_Indirection node.
func NewA_Indirection(arg Node, indirection []Node, location int) *A_Indirection {
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
	Elements []Node // List of expressions
}

// NewA_ArrayExpr creates a new A_ArrayExpr node.
func NewA_ArrayExpr(elements []Node, location int) *A_ArrayExpr {
	aArrayExpr := &A_ArrayExpr{
		BaseNode: BaseNode{Tag: T_A_ArrayExpr},
		Elements: elements,
	}
	aArrayExpr.SetLocation(location)
	return aArrayExpr
}

func (a *A_ArrayExpr) String() string {
	return fmt.Sprintf("A_ArrayExpr{%d elements}@%d", len(a.Elements), a.Location())
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
	Constraints   []Node         // Column constraints
	Fdwoptions    []Node         // Foreign-data-wrapper specific options
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
		Constraints: []Node{},
		Fdwoptions:  []Node{},
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
	Ctes      []Node // List of CommonTableExpr nodes
	Recursive bool   // TRUE for WITH RECURSIVE
}

// NewWithClause creates a new WithClause node.
func NewWithClause(ctes []Node, recursive bool, location int) *WithClause {
	withClause := &WithClause{
		BaseNode:  BaseNode{Tag: T_WithClause},
		Ctes:      ctes,
		Recursive: recursive,
	}
	withClause.SetLocation(location)
	return withClause
}

func (w *WithClause) String() string {
	if w.Recursive {
		return fmt.Sprintf("WithClause{RECURSIVE, %d CTEs}@%d", len(w.Ctes), w.Location())
	}
	return fmt.Sprintf("WithClause{%d CTEs}@%d", len(w.Ctes), w.Location())
}

func (w *WithClause) ExpressionType() string {
	return "WITH_CLAUSE"
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
	PartitionClause []Node    // PARTITION BY expression list
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
		PartitionClause: []Node{},
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

// GroupingSet represents a grouping set in GROUP BY clauses.
// Ported from postgres/src/include/nodes/parsenodes.h:1506-1517
type GroupingSet struct {
	BaseNode
	Kind     GroupingSetKind // Type of grouping set
	Content  []Node          // List of expressions
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
func NewGroupingSet(kind GroupingSetKind, content []Node, location int) *GroupingSet {
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
	Args         []Node  // List of tablesample arguments
	Repeatable   Expr    // REPEATABLE expression, or NULL
}

// NewTableSampleClause creates a new TableSampleClause node.
func NewTableSampleClause(tsmhandler Oid, args []Node, repeatable Expr, location int) *TableSampleClause {
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
	Objargs      []Node    // List of argument types (TypeName nodes)
	ObjfuncArgs  []Node    // List of function arguments for ALTER FUNCTION
	ArgsUnspecified bool   // Arguments were omitted, so name must be unique
}

// NewObjectWithArgs creates a new ObjectWithArgs node.
func NewObjectWithArgs(objname []*String, objargs []Node, argsUnspecified bool, location int) *ObjectWithArgs {
	objectWithArgs := &ObjectWithArgs{
		BaseNode:        BaseNode{Tag: T_ObjectWithArgs},
		Objname:         objname,
		Objargs:         objargs,
		ObjfuncArgs:     []Node{},
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