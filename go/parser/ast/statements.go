// Package ast provides PostgreSQL AST statement node definitions.
// Ported from postgres/src/include/nodes/parsenodes.h
package ast

import (
	"fmt"
	"strings"
)

// ==============================================================================
// CORE STATEMENT FRAMEWORK - PostgreSQL parsenodes.h implementation
// Ported from postgres/src/include/nodes/parsenodes.h
// ==============================================================================

// CmdType represents the type of SQL command - ported from postgres/src/include/nodes/nodes.h:262
type CmdType int

const (
	CMD_UNKNOWN CmdType = iota // Unknown command type
	CMD_SELECT                 // SELECT statement
	CMD_UPDATE                 // UPDATE statement  
	CMD_INSERT                 // INSERT statement
	CMD_DELETE                 // DELETE statement
	CMD_MERGE                  // MERGE statement
	CMD_UTILITY                // Utility commands (CREATE, DROP, etc.)
	CMD_NOTHING                // Dummy command for INSTEAD NOTHING rules
)

func (c CmdType) String() string {
	switch c {
	case CMD_UNKNOWN:
		return "UNKNOWN"
	case CMD_SELECT:
		return "SELECT"
	case CMD_UPDATE:
		return "UPDATE"
	case CMD_INSERT:
		return "INSERT"
	case CMD_DELETE:
		return "DELETE"
	case CMD_MERGE:
		return "MERGE"
	case CMD_UTILITY:
		return "UTILITY"
	case CMD_NOTHING:
		return "NOTHING"
	default:
		return fmt.Sprintf("CmdType(%d)", int(c))
	}
}

// QuerySource represents possible sources of a Query - ported from postgres/src/include/nodes/parsenodes.h:34
type QuerySource int

const (
	QSRC_ORIGINAL           QuerySource = iota // Original parse tree (explicit query)
	QSRC_PARSER                                // Added by parse analysis (now unused)
	QSRC_INSTEAD_RULE                          // Added by unconditional INSTEAD rule
	QSRC_QUAL_INSTEAD_RULE                     // Added by conditional INSTEAD rule
	QSRC_NON_INSTEAD_RULE                      // Added by non-INSTEAD rule
)

// Note: DropBehavior and ObjectType are now defined in ddl_statements.go

// ==============================================================================
// SUPPORTING STRUCTURES
// ==============================================================================

// RangeVar represents a table/relation reference.
// Ported from postgres/src/include/nodes/primnodes.h:71
type RangeVar struct {
	BaseNode
	CatalogName     *string // Database name, or nil - postgres/src/include/nodes/primnodes.h:76
	SchemaName      *string // Schema name, or nil - postgres/src/include/nodes/primnodes.h:79
	RelName         string  // Relation/sequence name - postgres/src/include/nodes/primnodes.h:82
	Inh             bool    // Expand relation by inheritance? - postgres/src/include/nodes/primnodes.h:85
	RelPersistence  rune    // Persistence type - postgres/src/include/nodes/primnodes.h:87
	Alias           *Alias  // Table alias & optional column aliases - postgres/src/include/nodes/primnodes.h:90
}

// NewRangeVar creates a new RangeVar node.
func NewRangeVar(relName string, schemaName, catalogName *string) *RangeVar {
	return &RangeVar{
		BaseNode:    BaseNode{Tag: T_RangeVar},
		RelName:     relName,
		SchemaName:  schemaName,
		CatalogName: catalogName,
	}
}

func (rv *RangeVar) String() string {
	parts := []string{}
	if rv.CatalogName != nil {
		parts = append(parts, *rv.CatalogName)
	}
	if rv.SchemaName != nil {
		parts = append(parts, *rv.SchemaName)
	}
	parts = append(parts, rv.RelName)
	return fmt.Sprintf("RangeVar(%s)@%d", strings.Join(parts, "."), rv.Location())
}

func (rv *RangeVar) StatementType() string {
	return "RangeVar"
}

// Alias represents table and column aliases.
// Ported from postgres/src/include/nodes/primnodes.h:47
type Alias struct {
	BaseNode
	AliasName *string      // Alias name - postgres/src/include/nodes/primnodes.h:50
	ColNames  []*string    // Column aliases - postgres/src/include/nodes/primnodes.h:51
}

// NewAlias creates a new Alias node.
func NewAlias(aliasName *string, colNames []*string) *Alias {
	return &Alias{
		BaseNode:  BaseNode{Tag: T_String}, // Use T_String for alias
		AliasName: aliasName,
		ColNames:  colNames,
	}
}

func (a *Alias) String() string {
	name := ""
	if a.AliasName != nil {
		name = *a.AliasName
	}
	return fmt.Sprintf("Alias(%s)@%d", name, a.Location())
}

// ResTarget represents a target item in a SELECT list or UPDATE SET clause.
// Ported from postgres/src/include/nodes/parsenodes.h:514
type ResTarget struct {
	BaseNode
	Name        *string // Column name or nil - postgres/src/include/nodes/parsenodes.h:518
	Indirection []Node  // Subscripts, field names, and '*', or nil - postgres/src/include/nodes/parsenodes.h:519
	Val         Node    // Value expression to compute or assign - postgres/src/include/nodes/parsenodes.h:520
}

// NewResTarget creates a new ResTarget node.
func NewResTarget(name *string, val Node) *ResTarget {
	return &ResTarget{
		BaseNode: BaseNode{Tag: T_ResTarget},
		Name:     name,
		Val:      val,
	}
}

func (rt *ResTarget) String() string {
	name := ""
	if rt.Name != nil {
		name = *rt.Name
	}
	return fmt.Sprintf("ResTarget(%s)@%d", name, rt.Location())
}

func (rt *ResTarget) ExpressionType() string {
	return "ResTarget"
}

// ==============================================================================
// CORE QUERY STRUCTURE
// ==============================================================================

// Query is the fundamental query structure that all parsed queries transform into.
// Ported from postgres/src/include/nodes/parsenodes.h:117
type Query struct {
	BaseNode
	CommandType     CmdType     // select|insert|update|delete|merge|utility - postgres/src/include/nodes/parsenodes.h:120
	QuerySource     QuerySource // Where did this query come from? - postgres/src/include/nodes/parsenodes.h:121
	QueryId         uint64      // Query identifier - postgres/src/include/nodes/parsenodes.h:122
	CanSetTag       bool        // Do I set the command result tag? - postgres/src/include/nodes/parsenodes.h:123
	UtilityStmt     Node        // Non-null if commandType == CMD_UTILITY - postgres/src/include/nodes/parsenodes.h:124
	ResultRelation  int         // rtable index of target relation - postgres/src/include/nodes/parsenodes.h:125

	// Boolean flags for query characteristics - postgres/src/include/nodes/parsenodes.h:127-140
	HasAggs         bool // Has aggregates in tlist or havingQual
	HasWindowFuncs  bool // Has window functions in tlist
	HasTargetSRFs   bool // Has set-returning functions in tlist
	HasSubLinks     bool // Has subquery SubLink
	HasDistinctOn   bool // distinctClause is from DISTINCT ON
	HasRecursive    bool // WITH RECURSIVE was specified
	HasModifyingCTE bool // Has INSERT/UPDATE/DELETE/MERGE in WITH
	HasForUpdate    bool // FOR [KEY] UPDATE/SHARE was specified
	HasRowSecurity  bool // Rewriter has applied some RLS policy
	IsReturn        bool // Is a RETURN statement

	// Query components - postgres/src/include/nodes/parsenodes.h:142-192
	CteList         []*CommonTableExpr // WITH list
	Rtable          []*RangeTblEntry   // Range table entries
	Jointree        *FromExpr          // Table join tree (FROM and WHERE clauses)
	TargetList      []*TargetEntry     // Target list
	ReturningList   []*TargetEntry     // Return-values list
	GroupClause     []*SortGroupClause // GROUP BY clauses
	GroupDistinct   bool               // Is the GROUP BY clause distinct?
	GroupingSets    []Node             // GROUPING SETS if present
	HavingQual      Node               // Qualifications applied to groups
	WindowClause    []*WindowClause    // WINDOW clauses
	DistinctClause  []*SortGroupClause // DISTINCT clauses
	SortClause      []*SortGroupClause // ORDER BY clauses
	LimitOffset     Node               // Number of result tuples to skip
	LimitCount      Node               // Number of result tuples to return
	RowMarks        []*RowMarkClause   // Row mark clauses
	StmtLocation    int                // Start location - postgres/src/include/nodes/parsenodes.h:239
	StmtLen         int                // Length in bytes - postgres/src/include/nodes/parsenodes.h:240
}

// NewQuery creates a new Query node.
func NewQuery(cmdType CmdType) *Query {
	return &Query{
		BaseNode:    BaseNode{Tag: T_Query},
		CommandType: cmdType,
		QuerySource: QSRC_ORIGINAL,
	}
}

func (q *Query) String() string {
	return fmt.Sprintf("Query(%s)@%d", q.CommandType, q.Location())
}

func (q *Query) StatementType() string {
	return q.CommandType.String()
}

// ==============================================================================
// DML STATEMENTS
// ==============================================================================

// SelectStmt represents a raw SELECT statement before analysis.
// Ported from postgres/src/include/nodes/parsenodes.h:2116
type SelectStmt struct {
	BaseNode
	// Fields used in "leaf" SelectStmts - postgres/src/include/nodes/parsenodes.h:2120-2130
	DistinctClause []*String    // NULL, list of DISTINCT ON exprs, or special marker for ALL
	IntoClause     *IntoClause  // Target for SELECT INTO
	TargetList     []*ResTarget // Target list
	FromClause     []Node       // FROM clause
	WhereClause    Node         // WHERE qualification
	GroupClause    []Node       // GROUP BY clauses
	GroupDistinct  bool         // Is this GROUP BY DISTINCT?
	HavingClause   Node         // HAVING conditional-expression
	WindowClause   []Node       // WINDOW window_name AS (...), ...
	ValuesLists    [][]Node     // Untransformed list of expression lists

	// Fields used in both "leaf" and upper-level SelectStmts - postgres/src/include/nodes/parsenodes.h:2132-2137
	SortClause     []*SortBy      // Sort clause
	LimitOffset    Node           // Number of result tuples to skip
	LimitCount     Node           // Number of result tuples to return
	LockingClause  []*LockingClause // FOR UPDATE clauses
	WithClause     *WithClause    // WITH clause

	// Fields used only in upper-level SelectStmts - postgres/src/include/nodes/parsenodes.h:2139-2143
	Op   SetOperation // Type of set operation
	All  bool         // ALL specified?
	Larg *SelectStmt  // Left child
	Rarg *SelectStmt  // Right child
}

// NewSelectStmt creates a new SelectStmt node.
func NewSelectStmt() *SelectStmt {
	return &SelectStmt{
		BaseNode:   BaseNode{Tag: T_SelectStmt},
		TargetList: []*ResTarget{},
		FromClause: []Node{},
	}
}

func (s *SelectStmt) String() string {
	return fmt.Sprintf("SelectStmt@%d", s.Location())
}

func (s *SelectStmt) StatementType() string {
	return "SELECT"
}

// InsertStmt represents an INSERT statement.
// Ported from postgres/src/include/nodes/parsenodes.h:2039
type InsertStmt struct {
	BaseNode
	Relation           *RangeVar           // Relation to insert into - postgres/src/include/nodes/parsenodes.h:2042
	Cols               []*ResTarget        // Optional: names of the target columns - postgres/src/include/nodes/parsenodes.h:2043
	SelectStmt         Node                // Source SELECT/VALUES, or NULL - postgres/src/include/nodes/parsenodes.h:2044
	OnConflictClause   *OnConflictClause   // ON CONFLICT clause - postgres/src/include/nodes/parsenodes.h:2045
	ReturningList      []*ResTarget        // List of expressions to return - postgres/src/include/nodes/parsenodes.h:2046
	WithClause         *WithClause         // WITH clause - postgres/src/include/nodes/parsenodes.h:2047
	Override           OverridingKind      // OVERRIDING clause - postgres/src/include/nodes/parsenodes.h:2048
}

// NewInsertStmt creates a new InsertStmt node.
func NewInsertStmt(relation *RangeVar) *InsertStmt {
	return &InsertStmt{
		BaseNode: BaseNode{Tag: T_InsertStmt},
		Relation: relation,
	}
}

func (i *InsertStmt) String() string {
	relName := ""
	if i.Relation != nil {
		relName = i.Relation.RelName
	}
	return fmt.Sprintf("InsertStmt(%s)@%d", relName, i.Location())
}

func (i *InsertStmt) StatementType() string {
	return "INSERT"
}

// UpdateStmt represents an UPDATE statement.
// Ported from postgres/src/include/nodes/parsenodes.h:2069
type UpdateStmt struct {
	BaseNode
	Relation      *RangeVar    // Relation to update - postgres/src/include/nodes/parsenodes.h:2072
	TargetList    []*ResTarget // Target list (of ResTarget) - postgres/src/include/nodes/parsenodes.h:2073
	WhereClause   Node         // Qualifications - postgres/src/include/nodes/parsenodes.h:2074
	FromClause    []Node       // Optional from clause for more tables - postgres/src/include/nodes/parsenodes.h:2075
	ReturningList []*ResTarget // List of expressions to return - postgres/src/include/nodes/parsenodes.h:2076
	WithClause    *WithClause  // WITH clause - postgres/src/include/nodes/parsenodes.h:2077
}

// NewUpdateStmt creates a new UpdateStmt node.
func NewUpdateStmt(relation *RangeVar) *UpdateStmt {
	return &UpdateStmt{
		BaseNode: BaseNode{Tag: T_UpdateStmt},
		Relation: relation,
	}
}

func (u *UpdateStmt) String() string {
	relName := ""
	if u.Relation != nil {
		relName = u.Relation.RelName
	}
	return fmt.Sprintf("UpdateStmt(%s)@%d", relName, u.Location())
}

func (u *UpdateStmt) StatementType() string {
	return "UPDATE"
}

// DeleteStmt represents a DELETE statement.
// Ported from postgres/src/include/nodes/parsenodes.h:2055
type DeleteStmt struct {
	BaseNode
	Relation      *RangeVar    // Relation to delete from - postgres/src/include/nodes/parsenodes.h:2058
	UsingClause   []Node       // Optional using clause for more tables - postgres/src/include/nodes/parsenodes.h:2059
	WhereClause   Node         // Qualifications - postgres/src/include/nodes/parsenodes.h:2060
	ReturningList []*ResTarget // List of expressions to return - postgres/src/include/nodes/parsenodes.h:2061
	WithClause    *WithClause  // WITH clause - postgres/src/include/nodes/parsenodes.h:2062
}

// NewDeleteStmt creates a new DeleteStmt node.
func NewDeleteStmt(relation *RangeVar) *DeleteStmt {
	return &DeleteStmt{
		BaseNode: BaseNode{Tag: T_DeleteStmt},
		Relation: relation,
	}
}

func (d *DeleteStmt) String() string {
	relName := ""
	if d.Relation != nil {
		relName = d.Relation.RelName
	}
	return fmt.Sprintf("DeleteStmt(%s)@%d", relName, d.Location())
}

func (d *DeleteStmt) StatementType() string {
	return "DELETE"
}

// ==============================================================================
// DDL STATEMENTS
// ==============================================================================

// CreateStmt represents a CREATE TABLE statement.
// Ported from postgres/src/include/nodes/parsenodes.h:2648
type CreateStmt struct {
	BaseNode
	Relation        *RangeVar      // Relation to create - postgres/src/include/nodes/parsenodes.h:2651
	TableElts       []*ColumnDef   // Column definitions - postgres/src/include/nodes/parsenodes.h:2652
	InhRelations    []*RangeVar    // Relations to inherit from - postgres/src/include/nodes/parsenodes.h:2653
	Constraints     []*Constraint  // Constraints - postgres/src/include/nodes/parsenodes.h:2659
	Options         []Node         // Options from WITH clause - postgres/src/include/nodes/parsenodes.h:2660
	TableSpaceName  *string        // Table space to use, or nil - postgres/src/include/nodes/parsenodes.h:2662
	AccessMethod    *string        // Table access method - postgres/src/include/nodes/parsenodes.h:2663
	IfNotExists     bool           // Just do nothing if it already exists? - postgres/src/include/nodes/parsenodes.h:2664
}

// NewCreateStmt creates a new CreateStmt node.
func NewCreateStmt(relation *RangeVar) *CreateStmt {
	return &CreateStmt{
		BaseNode: BaseNode{Tag: T_CreateStmt},
		Relation: relation,
	}
}

func (c *CreateStmt) String() string {
	relName := ""
	if c.Relation != nil {
		relName = c.Relation.RelName
	}
	return fmt.Sprintf("CreateStmt(%s)@%d", relName, c.Location())
}

func (c *CreateStmt) StatementType() string {
	return "CREATE"
}

// DropStmt represents a DROP statement.
// Ported from postgres/src/include/nodes/parsenodes.h:3226
type DropStmt struct {
	BaseNode
	Objects     []Node       // List of names - postgres/src/include/nodes/parsenodes.h:3229
	RemoveType  ObjectType   // Object type - postgres/src/include/nodes/parsenodes.h:3230
	Behavior    DropBehavior // RESTRICT or CASCADE behavior - postgres/src/include/nodes/parsenodes.h:3231
	MissingOk   bool         // Skip error if object is missing? - postgres/src/include/nodes/parsenodes.h:3232
	Concurrent  bool         // Drop index concurrently? - postgres/src/include/nodes/parsenodes.h:3233
}

// NewDropStmt creates a new DropStmt node.
func NewDropStmt(objects []Node, removeType ObjectType) *DropStmt {
	return &DropStmt{
		BaseNode:   BaseNode{Tag: T_DropStmt},
		Objects:    objects,
		RemoveType: removeType,
		Behavior:   DROP_RESTRICT,
	}
}

func (d *DropStmt) String() string {
	return fmt.Sprintf("DropStmt(%d objects)@%d", len(d.Objects), d.Location())
}

func (d *DropStmt) StatementType() string {
	return "DROP"
}

// ==============================================================================
// COLUMN REFERENCES
// ==============================================================================

// ColumnRef represents a column reference in expressions.
// Ported from postgres/src/include/nodes/parsenodes.h:291
type ColumnRef struct {
	BaseNode
	Fields []Node // List of field names - postgres/src/include/nodes/parsenodes.h:292
}

// NewColumnRef creates a new ColumnRef node.
func NewColumnRef(fields ...Node) *ColumnRef {
	return &ColumnRef{
		BaseNode: BaseNode{Tag: T_ColumnRef},
		Fields:   fields,
	}
}

func (c *ColumnRef) String() string {
	return fmt.Sprintf("ColumnRef[%d fields]@%d", len(c.Fields), c.Location())
}

func (c *ColumnRef) ExpressionType() string {
	return "ColumnRef"
}

// ==============================================================================
// PLACEHOLDER TYPES - To be fully implemented later
// ==============================================================================

// These types are referenced by the main statements above and are now fully implemented.
// Implementation moved to query_execution_nodes.go for better organization.

// CommonTableExpr represents a WITH clause (Common Table Expression / CTE).
// CTEs are increasingly common in modern SQL and enable recursive queries
// and improved query organization.
// Ported from postgres/src/include/nodes/parsenodes.h:1668
type CommonTableExpr struct {
	BaseNode
	Ctename         string     // Query name (never qualified) - parsenodes.h:1676
	Aliascolnames   []Node     // Optional list of column names - parsenodes.h:1678
	Ctematerialized CTEMaterialized // Is this an optimization fence? - parsenodes.h:1679
	Ctequery        Node       // The CTE's subquery - parsenodes.h:1681
	Search_clause   *CTESearchClause // SEARCH clause, if any - parsenodes.h:1682
	Cycle_clause    *CTECycleClause  // CYCLE clause, if any - parsenodes.h:1683
	Location        int        // Token location, or -1 if unknown - parsenodes.h:1684
	Cterecursive    bool       // Is this a recursive CTE? - parsenodes.h:1687
	Cterefcount     int        // Number of RTEs referencing this CTE - parsenodes.h:1693
	Ctecolnames     []Node     // List of output column names - parsenodes.h:1696
	Ctecoltypes     []Oid      // OID list of output column type OIDs - parsenodes.h:1697
	Ctecoltypmods   []int32    // Integer list of output column typmods - parsenodes.h:1698
	Ctecolcollations []Oid     // OID list of column collation OIDs - parsenodes.h:1699
}

// CTEMaterialized represents CTE materialization settings.
// Ported from postgres/src/include/nodes/parsenodes.h:1636
type CTEMaterialized int

const (
	CTEMaterializeDefault  CTEMaterialized = iota // No materialization clause - parsenodes.h:1638
	CTEMaterializeAlways                          // MATERIALIZED - parsenodes.h:1639
	CTEMaterializeNever                           // NOT MATERIALIZED - parsenodes.h:1640
)

// CTESearchClause represents a SEARCH clause for recursive CTEs.
// Ported from postgres/src/include/nodes/parsenodes.h:1643
type CTESearchClause struct {
	BaseNode
	SearchColList   []Node // List of column names - parsenodes.h:1646
	SearchBreadthFirst bool // True for BREADTH FIRST, false for DEPTH FIRST - parsenodes.h:1647
	SearchSeqColumn string // Name of sequence column - parsenodes.h:1648
	Location        int    // Token location, or -1 if unknown - parsenodes.h:1649
}

// CTECycleClause represents a CYCLE clause for recursive CTEs.
// Ported from postgres/src/include/nodes/parsenodes.h:1652
type CTECycleClause struct {
	BaseNode
	CycleColList       []Node // List of column names - parsenodes.h:1655
	CycleMarkColumn    string // Name of cycle mark column - parsenodes.h:1656
	CycleMarkValue     Node   // Cycle mark value - parsenodes.h:1657
	CycleMarkDefault   Node   // Cycle mark default value - parsenodes.h:1658
	CyclePathColumn    string // Name of path column - parsenodes.h:1659
	Location           int    // Token location, or -1 if unknown - parsenodes.h:1660
	CycleMarkType      Oid    // Type of cycle mark column - parsenodes.h:1662
	CycleMarkTypmod    int32  // Typmod of cycle mark column - parsenodes.h:1663
	CycleMarkCollation Oid    // Collation of cycle mark column - parsenodes.h:1664
	CycleMarkNeop      Oid    // Inequality operator for cycle mark - parsenodes.h:1665
}

// NewCommonTableExpr creates a new CommonTableExpr node.
func NewCommonTableExpr(ctename string, ctequery Node) *CommonTableExpr {
	return &CommonTableExpr{
		BaseNode: BaseNode{Tag: T_CommonTableExpr},
		Ctename:  ctename,
		Ctequery: ctequery,
		Location: -1,
	}
}

// NewRecursiveCommonTableExpr creates a new recursive CommonTableExpr node.
func NewRecursiveCommonTableExpr(ctename string, ctequery Node) *CommonTableExpr {
	return &CommonTableExpr{
		BaseNode:     BaseNode{Tag: T_CommonTableExpr},
		Ctename:      ctename,
		Ctequery:     ctequery,
		Cterecursive: true,
		Location:     -1,
	}
}

func (cte *CommonTableExpr) String() string {
	recursive := ""
	if cte.Cterecursive {
		recursive = " RECURSIVE"
	}
	return fmt.Sprintf("CommonTableExpr(%s%s)", cte.Ctename, recursive)
}

// Placeholder structs for other query execution nodes implemented in query_execution_nodes.go
type RangeTblEntry struct{ BaseNode }
type IntoClause struct{ BaseNode }
type SortBy struct{ BaseNode }
type LockingClause struct{ BaseNode }
type WithClause struct{ BaseNode }
type SetOperation int
type OnConflictClause struct{ BaseNode }
type OverridingKind int
type ColumnDef struct{ BaseNode }
// Note: Constraint is now defined in ddl_statements.go