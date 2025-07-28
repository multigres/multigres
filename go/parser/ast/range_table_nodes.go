// Package ast provides PostgreSQL AST range table and FROM clause node definitions.
// This file contains range table infrastructure nodes essential for FROM clause and JOIN support.
// Ported from postgres/src/include/nodes/parsenodes.h and primnodes.h
package ast

import (
	"fmt"
	"strings"
)

// ==============================================================================
// RANGE TABLE AND FROM CLAUSE INFRASTRUCTURE - Phase 1D Implementation
// Essential range table nodes for FROM clause and JOIN support
// Ported from postgres/src/include/nodes/parsenodes.h and primnodes.h
// ==============================================================================

// Type aliases for PostgreSQL types
type AclMode uint32    // Access control mode bitmask
type Cardinality float64 // Row count estimates

// TableFunc placeholder removed - now implemented in expressions.go

// RTEKind represents the type of a Range Table Entry.
// Ported from postgres/src/include/nodes/parsenodes.h:1022-1033
type RTEKind int

const (
	RTE_RELATION         RTEKind = iota // ordinary relation reference
	RTE_SUBQUERY                        // subquery in FROM
	RTE_JOIN                            // join
	RTE_FUNCTION                        // function in FROM
	RTE_TABLEFUNC                       // TableFunc(.., column list)
	RTE_VALUES                          // VALUES (<exprlist>), (<exprlist>), ...
	RTE_CTE                             // common table expr (WITH list element)
	RTE_NAMEDTUPLESTORE                 // tuplestore, e.g. for AFTER triggers
	RTE_RESULT                          // RTE represents an empty FROM clause
)

// String returns the string representation of RTEKind.
func (k RTEKind) String() string {
	switch k {
	case RTE_RELATION:
		return "RELATION"
	case RTE_SUBQUERY:
		return "SUBQUERY"
	case RTE_JOIN:
		return "JOIN"
	case RTE_FUNCTION:
		return "FUNCTION"
	case RTE_TABLEFUNC:
		return "TABLEFUNC"
	case RTE_VALUES:
		return "VALUES"
	case RTE_CTE:
		return "CTE"
	case RTE_NAMEDTUPLESTORE:
		return "NAMEDTUPLESTORE"
	case RTE_RESULT:
		return "RESULT"
	default:
		return "UNKNOWN"
	}
}

// RangeTblEntry represents a range table entry which describes a table or subquery in the FROM clause.
// This is a complex structure that supports multiple types of table sources.
// Ported from postgres/src/include/nodes/parsenodes.h:1038-1251
type RangeTblEntry struct {
	BaseNode

	// Fields valid in all RTEs
	Alias     *Alias  // user-written alias clause, if any
	Eref      *Alias  // expanded reference names
	RteKind   RTEKind // see RTEKind enum above
	Lateral   bool    // was LATERAL specified?
	InFromCl  bool    // present in FROM clause?
	
	// Fields valid for a plain relation RTE (RTE_RELATION)
	Relid         Oid    // OID of the relation
	Inh           bool   // inheritance requested?
	RelKind       byte   // relation kind (see pg_class.relkind)
	RelLockMode   int    // lock level that query requires on the rel
	PermInfoIndex int    // index of RTEPermissionInfo entry, or 0
	TableSample   *TableSampleClause // sampling info, or NULL
	
	// Fields valid for a subquery RTE (RTE_SUBQUERY)
	Subquery        *Query // the sub-query
	SecurityBarrier bool   // is from security_barrier view?
	
	// Fields valid for a join RTE (RTE_JOIN)
	JoinType        JoinType // type of join
	JoinMergedCols  int      // number of merged (JOIN USING) columns
	JoinAliasVars   []Node   // list of alias-var expansions
	JoinLeftCols    []int    // left-side input column numbers
	JoinRightCols   []int    // right-side input column numbers
	JoinUsingAlias  *Alias   // alias clause attached directly to JOIN/USING
	
	// Fields valid for a function RTE (RTE_FUNCTION)
	Functions        []*RangeTblFunction // list of RangeTblFunction nodes
	FuncOrdinality   bool                // is this called WITH ORDINALITY?
	
	// Fields valid for a TableFunc RTE (RTE_TABLEFUNC)
	TableFunc *TableFunc // table function specification
	
	// Fields valid for a values RTE (RTE_VALUES)
	ValuesLists [][]Node // list of expression lists
	
	// Fields valid for a CTE RTE (RTE_CTE)
	CteName       string // name of the WITH list item
	CteLevelsUp   Index  // number of query levels up
	SelfReference bool   // is this a recursive self-reference?
	
	// Fields valid for CTE, VALUES, ENR, and TableFunc RTEs
	ColTypes      []Oid    // OID list of column type OIDs
	ColTypMods    []int    // integer list of column typmods
	ColCollations []Oid    // OID list of column collation OIDs
	
	// Fields valid for ENR RTEs (RTE_NAMEDTUPLESTORE)
	EnrName    string      // name of ephemeral named relation
	EnrTuples  Cardinality // estimated or actual from caller
	
	// Security-related fields
	SecurityQuals []Node // security barrier quals to apply, if any
}

// NewRangeTblEntry creates a new RangeTblEntry node.
func NewRangeTblEntry(rteKind RTEKind, alias *Alias) *RangeTblEntry {
	return &RangeTblEntry{
		BaseNode: BaseNode{Tag: T_RangeTblEntry},
		RteKind:  rteKind,
		Alias:    alias,
	}
}

func (r *RangeTblEntry) String() string {
	return fmt.Sprintf("RangeTblEntry{kind=%s}@%d", r.RteKind, r.Location())
}

func (r *RangeTblEntry) StatementType() string {
	return "RANGE_TBL_ENTRY"
}

// RangeSubselect represents a subquery in FROM clause.
// Ported from postgres/src/include/nodes/parsenodes.h:615-621
type RangeSubselect struct {
	BaseNode
	Lateral  bool  // does it have LATERAL prefix?
	Subquery Node  // the untransformed sub-select clause
	Alias    *Alias // table alias & optional column aliases
}

// NewRangeSubselect creates a new RangeSubselect node.
func NewRangeSubselect(lateral bool, subquery Node, alias *Alias) *RangeSubselect {
	return &RangeSubselect{
		BaseNode: BaseNode{Tag: T_RangeSubselect},
		Lateral:  lateral,
		Subquery: subquery,
		Alias:    alias,
	}
}

func (r *RangeSubselect) String() string {
	lateral := ""
	if r.Lateral {
		lateral = "LATERAL "
	}
	return fmt.Sprintf("RangeSubselect{%sSubquery}@%d", lateral, r.Location())
}

func (r *RangeSubselect) StatementType() string {
	return "RANGE_SUBSELECT"
}

// RangeFunction represents a function call appearing in a FROM clause.
// Supports ROWS FROM() syntax and WITH ORDINALITY.
// Ported from postgres/src/include/nodes/parsenodes.h:637-647
type RangeFunction struct {
	BaseNode
	Lateral     bool        // does it have LATERAL prefix?
	Ordinality  bool        // does it have WITH ORDINALITY suffix?
	IsRowsFrom  bool        // is result of ROWS FROM() syntax?
	Functions   [][]Node    // per-function information (function + column definitions)
	Alias       *Alias      // table alias & optional column aliases
	ColDefList  []*ColumnDef // list of ColumnDef nodes to describe result of function returning RECORD
}

// NewRangeFunction creates a new RangeFunction node.
func NewRangeFunction(lateral, ordinality, isRowsFrom bool, functions [][]Node, alias *Alias, colDefList []*ColumnDef) *RangeFunction {
	return &RangeFunction{
		BaseNode:   BaseNode{Tag: T_RangeFunction},
		Lateral:    lateral,
		Ordinality: ordinality,
		IsRowsFrom: isRowsFrom,
		Functions:  functions,
		Alias:      alias,
		ColDefList: colDefList,
	}
}

func (r *RangeFunction) String() string {
	var parts []string
	if r.Lateral {
		parts = append(parts, "LATERAL")
	}
	parts = append(parts, "RangeFunction")
	if r.IsRowsFrom {
		parts = append(parts, "ROWS_FROM")
	}
	if r.Ordinality {
		parts = append(parts, "WITH_ORDINALITY")
	}
	return fmt.Sprintf("%s@%d", strings.Join(parts, " "), r.Location())
}

func (r *RangeFunction) StatementType() string {
	return "RANGE_FUNCTION"
}

// RangeTableFunc represents raw form of "table functions" such as XMLTABLE.
// Note: JSON_TABLE uses JsonTable node, not RangeTableFunc.
// Ported from postgres/src/include/nodes/parsenodes.h:655-665
type RangeTableFunc struct {
	BaseNode
	Lateral    bool                   // does it have LATERAL prefix?
	DocExpr    Node                   // document expression
	RowExpr    Node                   // row generator expression
	Namespaces []*ResTarget           // list of namespaces as ResTarget
	Columns    []*RangeTableFuncCol   // list of RangeTableFuncCol
	Alias      *Alias                 // table alias & optional column aliases
}

// NewRangeTableFunc creates a new RangeTableFunc node.
func NewRangeTableFunc(lateral bool, docExpr, rowExpr Node, namespaces []*ResTarget, columns []*RangeTableFuncCol, alias *Alias, location int) *RangeTableFunc {
	return &RangeTableFunc{
		BaseNode:   BaseNode{Tag: T_RangeTableFunc, Loc: location},
		Lateral:    lateral,
		DocExpr:    docExpr,
		RowExpr:    rowExpr,
		Namespaces: namespaces,
		Columns:    columns,
		Alias:      alias,
	}
}

func (r *RangeTableFunc) String() string {
	lateral := ""
	if r.Lateral {
		lateral = "LATERAL "
	}
	return fmt.Sprintf("RangeTableFunc{%s%d columns}@%d", lateral, len(r.Columns), r.Location())
}

func (r *RangeTableFunc) StatementType() string {
	return "RANGE_TABLE_FUNC"
}

// RangeTableFuncCol represents one column in a RangeTableFunc->columns.
// If ForOrdinality is true (FOR ORDINALITY), then the column is an int4 column
// and the rest of the fields are ignored.
// Ported from postgres/src/include/nodes/parsenodes.h:673-683
type RangeTableFuncCol struct {
	BaseNode
	ColName       string    // name of generated column
	TypeName      *TypeName // type of generated column
	ForOrdinality bool      // does it have FOR ORDINALITY?
	IsNotNull     bool      // does it have NOT NULL?
	ColExpr       Node      // column filter expression
	ColDefExpr    Node      // column default value expression
}

// NewRangeTableFuncCol creates a new RangeTableFuncCol node.
func NewRangeTableFuncCol(colName string, typeName *TypeName, forOrdinality, isNotNull bool, colExpr, colDefExpr Node, location int) *RangeTableFuncCol {
	return &RangeTableFuncCol{
		BaseNode:      BaseNode{Tag: T_RangeTableFuncCol, Loc: location},
		ColName:       colName,
		TypeName:      typeName,
		ForOrdinality: forOrdinality,
		IsNotNull:     isNotNull,
		ColExpr:       colExpr,
		ColDefExpr:    colDefExpr,
	}
}

func (r *RangeTableFuncCol) String() string {
	var parts []string
	parts = append(parts, fmt.Sprintf("RangeTableFuncCol{%s", r.ColName))
	if r.ForOrdinality {
		parts = append(parts, "FOR_ORDINALITY")
	}
	if r.IsNotNull {
		parts = append(parts, "NOT_NULL")
	}
	return fmt.Sprintf("%s}@%d", strings.Join(parts, " "), r.Location())
}

func (r *RangeTableFuncCol) StatementType() string {
	return "RANGE_TABLE_FUNC_COL"
}

// RangeTableSample represents TABLESAMPLE appearing in a raw FROM clause.
// This node represents: <relation> TABLESAMPLE <method> (<params>) REPEATABLE (<num>)
// Ported from postgres/src/include/nodes/parsenodes.h:695-703
type RangeTableSample struct {
	BaseNode
	Relation   Node     // relation to be sampled
	Method     []Node   // sampling method name (possibly qualified)
	Args       []Node   // argument(s) for sampling method
	Repeatable Node     // REPEATABLE expression, or NULL if none
}

// NewRangeTableSample creates a new RangeTableSample node.
func NewRangeTableSample(relation Node, method, args []Node, repeatable Node, location int) *RangeTableSample {
	return &RangeTableSample{
		BaseNode:   BaseNode{Tag: T_RangeTableSample, Loc: location},
		Relation:   relation,
		Method:     method,
		Args:       args,
		Repeatable: repeatable,
	}
}

func (r *RangeTableSample) String() string {
	repeatable := ""
	if r.Repeatable != nil {
		repeatable = " REPEATABLE"
	}
	return fmt.Sprintf("RangeTableSample{%d args%s}@%d", len(r.Args), repeatable, r.Location())
}

func (r *RangeTableSample) StatementType() string {
	return "RANGE_TABLE_SAMPLE"
}

// RangeTblFunction represents RangeTblEntry subsidiary data for one function in a FUNCTION RTE.
// Used when a function had a column definition list for an otherwise-unspecified RECORD result.
// Ported from postgres/src/include/nodes/parsenodes.h:1317-1337
type RangeTblFunction struct {
	BaseNode
	FuncExpr        Node     // expression tree for func call
	FuncColCount    int      // number of columns it contributes to RTE
	FuncColNames    []string // column names (list of String)
	FuncColTypes    []Oid    // OID list of column type OIDs
	FuncColTypMods  []int    // integer list of column typmods
	FuncColCollations []Oid    // OID list of column collation OIDs
	FuncParams      []int    // PARAM_EXEC Param IDs affecting this func (set during planning)
}

// NewRangeTblFunction creates a new RangeTblFunction node.
func NewRangeTblFunction(funcExpr Node, funcColCount int) *RangeTblFunction {
	return &RangeTblFunction{
		BaseNode:     BaseNode{Tag: T_RangeTblFunction},
		FuncExpr:     funcExpr,
		FuncColCount: funcColCount,
	}
}

func (r *RangeTblFunction) String() string {
	return fmt.Sprintf("RangeTblFunction{%d cols}@%d", r.FuncColCount, r.Location())
}

func (r *RangeTblFunction) StatementType() string {
	return "RANGE_TBL_FUNCTION"
}

// RTEPermissionInfo represents per-relation information for permission checking.
// Added to the Query node by the parser when adding the corresponding RTE to the query range table.
// Ported from postgres/src/include/nodes/parsenodes.h:1286-1297
type RTEPermissionInfo struct {
	BaseNode
	Relid         Oid       // relation OID
	Inh           bool      // separately check inheritance children?
	RequiredPerms AclMode   // bitmask of required access permissions
	CheckAsUser   Oid       // if valid, check access as this role
	SelectedCols  []int     // columns needing SELECT permission (simplified from Bitmapset)
	InsertedCols  []int     // columns needing INSERT permission (simplified from Bitmapset)
	UpdatedCols   []int     // columns needing UPDATE permission (simplified from Bitmapset)
}

// NewRTEPermissionInfo creates a new RTEPermissionInfo node.
func NewRTEPermissionInfo(relid Oid, inh bool, requiredPerms AclMode, checkAsUser Oid) *RTEPermissionInfo {
	return &RTEPermissionInfo{
		BaseNode:      BaseNode{Tag: T_RTEPermissionInfo},
		Relid:         relid,
		Inh:           inh,
		RequiredPerms: requiredPerms,
		CheckAsUser:   checkAsUser,
	}
}

func (r *RTEPermissionInfo) String() string {
	return fmt.Sprintf("RTEPermissionInfo{relid=%d, perms=0x%x}@%d", r.Relid, r.RequiredPerms, r.Location())
}

func (r *RTEPermissionInfo) StatementType() string {
	return "RTE_PERMISSION_INFO"
}

// RangeTblRef represents a range table reference from primnodes.h.
// This is a simple reference to an entry in the range table by index.
// Ported from postgres/src/include/nodes/primnodes.h:2243-2247
type RangeTblRef struct {
	BaseNode
	RtIndex int // index into the range table
}

// NewRangeTblRef creates a new RangeTblRef node.
func NewRangeTblRef(rtIndex int) *RangeTblRef {
	return &RangeTblRef{
		BaseNode: BaseNode{Tag: T_RangeTblRef},
		RtIndex:  rtIndex,
	}
}

func (r *RangeTblRef) String() string {
	return fmt.Sprintf("RangeTblRef{index=%d}@%d", r.RtIndex, r.Location())
}

func (r *RangeTblRef) ExpressionType() string {
	return "RANGE_TBL_REF"
}