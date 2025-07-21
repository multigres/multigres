// Package ast - SQL statement node definitions
// Ported from postgres/src/include/nodes/parsenodes.h statement structures
package ast

import (
	"fmt"
)

// CmdType represents different SQL command types.
// Ported from postgres/src/include/nodes/parsenodes.h CmdType enum
type CmdType int

const (
	CMD_UNKNOWN CmdType = iota
	CMD_SELECT  // Ported from postgres/src/include/nodes/parsenodes.h
	CMD_UPDATE  // Ported from postgres/src/include/nodes/parsenodes.h
	CMD_INSERT  // Ported from postgres/src/include/nodes/parsenodes.h
	CMD_DELETE  // Ported from postgres/src/include/nodes/parsenodes.h
	CMD_MERGE   // Ported from postgres/src/include/nodes/parsenodes.h
	CMD_UTILITY // Ported from postgres/src/include/nodes/parsenodes.h
	CMD_NOTHING // Ported from postgres/src/include/nodes/parsenodes.h
)

// String returns the string representation of a CmdType.
func (ct CmdType) String() string {
	switch ct {
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
		return "INVALID_CMD"
	}
}

// QuerySource represents the source of a query.
// Ported from postgres/src/include/nodes/parsenodes.h:34-41 QuerySource enum
type QuerySource int

const (
	QSRC_ORIGINAL QuerySource = iota // Ported from postgres/src/include/nodes/parsenodes.h:36
	QSRC_PARSER                      // Ported from postgres/src/include/nodes/parsenodes.h:37
	QSRC_INSTEAD_RULE                // Ported from postgres/src/include/nodes/parsenodes.h:38
	QSRC_QUAL_INSTEAD_RULE           // Ported from postgres/src/include/nodes/parsenodes.h:39
	QSRC_NON_INSTEAD_RULE            // Ported from postgres/src/include/nodes/parsenodes.h:40
)

// Query represents the main query node structure.
// All statements are converted to a Query tree for further processing.
// Ported from postgres/src/include/nodes/parsenodes.h:117-180 Query struct
type Query struct {
	BaseNode
	
	CommandType CmdType     // Ported from postgres/src/include/nodes/parsenodes.h:121
	QuerySource QuerySource // Ported from postgres/src/include/nodes/parsenodes.h:124
	QueryId     uint64      // Ported from postgres/src/include/nodes/parsenodes.h:131
	CanSetTag   bool        // Ported from postgres/src/include/nodes/parsenodes.h:134
	
	UtilityStmt Node // Ported from postgres/src/include/nodes/parsenodes.h:136
	
	// Target relation index for INSERT/UPDATE/DELETE/MERGE; 0 for SELECT
	ResultRelation int // Ported from postgres/src/include/nodes/parsenodes.h:143
	
	// Query features flags - ported from postgres/src/include/nodes/parsenodes.h:145-150
	HasAggs        bool // Has aggregates in tlist or havingQual
	HasWindowFuncs bool // Has window functions in tlist
	HasTargetSRFs  bool // Has set-returning functions in tlist
	HasSubLinks    bool // Has subqueries in expressions
	HasDistinctOn  bool // Has DISTINCT ON clause
	HasRecursive   bool // Has recursive CTE
	HasModifyingCTE bool // Has modifying CTE
	HasForUpdate   bool // Has FOR UPDATE/SHARE
	HasRowSecurity bool // Has row security
	IsReturn       bool // Is a RETURN statement
	
	// Core query components (to be expanded in later phases)
	TargetList      *NodeList // SELECT list - ported concept from postgres
	FromClause      *NodeList // FROM clause - ported concept from postgres
	WhereClause     Node      // WHERE clause - ported concept from postgres  
	GroupClause     *NodeList // GROUP BY clause - ported concept from postgres
	HavingClause    Node      // HAVING clause - ported concept from postgres
	OrderByClause   *NodeList // ORDER BY clause - ported concept from postgres
	LimitClause     Node      // LIMIT clause - ported concept from postgres
	OffsetClause    Node      // OFFSET clause - ported concept from postgres
	
	// Statement length in source (for multi-statement strings)
	// Ported from postgres/src/include/nodes/parsenodes.h:10-12 concept
	StmtLength int
}

// NewQuery creates a new Query node.
func NewQuery(cmdType CmdType) *Query {
	return &Query{
		BaseNode:    BaseNode{Tag: T_Query},
		CommandType: cmdType,
		QuerySource: QSRC_ORIGINAL,
		TargetList:  NewNodeList(),
		FromClause:  NewNodeList(),
		GroupClause: NewNodeList(),
		OrderByClause: NewNodeList(),
	}
}

// StatementType returns the statement type for Statement interface.
func (q *Query) StatementType() string {
	return q.CommandType.String()
}

// String returns a string representation of the Query.
func (q *Query) String() string {
	return fmt.Sprintf("Query(%s)@%d", q.CommandType, q.Location())
}

// SelectStmt represents a SELECT statement before analysis.
// This is the raw parse tree form before conversion to Query.
// Ported from postgres/src/include/nodes/parsenodes.h SelectStmt concept
type SelectStmt struct {
	BaseNode
	
	DistinctClause *NodeList // DISTINCT clause - ported concept from postgres
	IntoClause     Node      // INTO clause - ported concept from postgres  
	TargetList     *NodeList // SELECT list - ported concept from postgres
	FromClause     *NodeList // FROM clause - ported concept from postgres
	WhereClause    Node      // WHERE clause - ported concept from postgres
	GroupClause    *NodeList // GROUP BY clause - ported concept from postgres
	HavingClause   Node      // HAVING clause - ported concept from postgres
	WindowClause   *NodeList // WINDOW clause - ported concept from postgres
	
	ValuesLists   *NodeList // VALUES lists (for VALUES clause) - ported concept from postgres
	SortClause    *NodeList // ORDER BY clause - ported concept from postgres
	LimitOffset   Node      // OFFSET clause - ported concept from postgres
	LimitCount    Node      // LIMIT clause - ported concept from postgres  
	LimitOption   int       // LIMIT option flags - ported concept from postgres
	LockingClause *NodeList // FOR UPDATE/SHARE - ported concept from postgres
	WithClause    Node      // WITH clause (CTEs) - ported concept from postgres
	
	Op     int  // Set operation type (UNION, INTERSECT, EXCEPT) - ported concept from postgres
	All    bool // ALL flag for set operations - ported concept from postgres
	Larg   Node // Left argument for set operations - ported concept from postgres  
	Rarg   Node // Right argument for set operations - ported concept from postgres
}

// NewSelectStmt creates a new SelectStmt node.
func NewSelectStmt() *SelectStmt {
	return &SelectStmt{
		BaseNode:      BaseNode{Tag: T_SelectStmt},
		TargetList:    NewNodeList(),
		FromClause:    NewNodeList(),
		GroupClause:   NewNodeList(),
		SortClause:    NewNodeList(),
		LockingClause: NewNodeList(),
		ValuesLists:   NewNodeList(),
		WindowClause:  NewNodeList(),
	}
}

// StatementType returns the statement type for Statement interface.
func (s *SelectStmt) StatementType() string {
	return "SELECT"
}

// String returns a string representation of the SelectStmt.
func (s *SelectStmt) String() string {
	return fmt.Sprintf("SelectStmt@%d", s.Location())
}

// InsertStmt represents an INSERT statement.
// Ported from postgres/src/include/nodes/parsenodes.h InsertStmt concept
type InsertStmt struct {
	BaseNode
	
	Relation      Node      // Target table - ported concept from postgres
	Cols          *NodeList // Target columns - ported concept from postgres
	SelectStmt    Node      // SELECT statement or VALUES - ported concept from postgres
	OnConflict    Node      // ON CONFLICT clause - ported concept from postgres
	ReturningList *NodeList // RETURNING clause - ported concept from postgres
	WithClause    Node      // WITH clause - ported concept from postgres
	Override      int       // OVERRIDING clause - ported concept from postgres
}

// NewInsertStmt creates a new InsertStmt node.
func NewInsertStmt() *InsertStmt {
	return &InsertStmt{
		BaseNode:      BaseNode{Tag: T_InsertStmt},
		Cols:          NewNodeList(),
		ReturningList: NewNodeList(),
	}
}

// StatementType returns the statement type for Statement interface.
func (i *InsertStmt) StatementType() string {
	return "INSERT"
}

// String returns a string representation of the InsertStmt.
func (i *InsertStmt) String() string {
	return fmt.Sprintf("InsertStmt@%d", i.Location())
}

// UpdateStmt represents an UPDATE statement.
// Ported from postgres/src/include/nodes/parsenodes.h UpdateStmt concept  
type UpdateStmt struct {
	BaseNode
	
	Relation      Node      // Target table - ported concept from postgres
	TargetList    *NodeList // SET clause - ported concept from postgres
	WhereClause   Node      // WHERE clause - ported concept from postgres
	FromClause    *NodeList // FROM clause - ported concept from postgres
	ReturningList *NodeList // RETURNING clause - ported concept from postgres
	WithClause    Node      // WITH clause - ported concept from postgres
}

// NewUpdateStmt creates a new UpdateStmt node.
func NewUpdateStmt() *UpdateStmt {
	return &UpdateStmt{
		BaseNode:      BaseNode{Tag: T_UpdateStmt},
		TargetList:    NewNodeList(),
		FromClause:    NewNodeList(),
		ReturningList: NewNodeList(),
	}
}

// StatementType returns the statement type for Statement interface.
func (u *UpdateStmt) StatementType() string {
	return "UPDATE"
}

// String returns a string representation of the UpdateStmt.
func (u *UpdateStmt) String() string {
	return fmt.Sprintf("UpdateStmt@%d", u.Location())
}

// DeleteStmt represents a DELETE statement.
// Ported from postgres/src/include/nodes/parsenodes.h DeleteStmt concept
type DeleteStmt struct {
	BaseNode
	
	Relation      Node      // Target table - ported concept from postgres
	UsingClause   *NodeList // USING clause - ported concept from postgres
	WhereClause   Node      // WHERE clause - ported concept from postgres
	ReturningList *NodeList // RETURNING clause - ported concept from postgres
	WithClause    Node      // WITH clause - ported concept from postgres
}

// NewDeleteStmt creates a new DeleteStmt node.
func NewDeleteStmt() *DeleteStmt {
	return &DeleteStmt{
		BaseNode:      BaseNode{Tag: T_DeleteStmt},
		UsingClause:   NewNodeList(),
		ReturningList: NewNodeList(),
	}
}

// StatementType returns the statement type for Statement interface.
func (d *DeleteStmt) StatementType() string {
	return "DELETE"
}

// String returns a string representation of the DeleteStmt.
func (d *DeleteStmt) String() string {
	return fmt.Sprintf("DeleteStmt@%d", d.Location())
}

// CreateStmt represents a CREATE TABLE statement.
// Ported from postgres/src/include/nodes/parsenodes.h CreateStmt concept
type CreateStmt struct {
	BaseNode
	
	Relation     Node      // Table name - ported concept from postgres
	TableElts    *NodeList // Column definitions - ported concept from postgres
	InhRelations *NodeList // Inheritance parents - ported concept from postgres
	Constraints  *NodeList // Table constraints - ported concept from postgres
	Options      *NodeList // Table options - ported concept from postgres
	OnCommit     int       // ON COMMIT option - ported concept from postgres
	TableSpace   string    // Tablespace name - ported concept from postgres
	IfNotExists  bool      // IF NOT EXISTS flag - ported concept from postgres
}

// NewCreateStmt creates a new CreateStmt node.
func NewCreateStmt() *CreateStmt {
	return &CreateStmt{
		BaseNode:     BaseNode{Tag: T_CreateStmt},
		TableElts:    NewNodeList(),
		InhRelations: NewNodeList(),
		Constraints:  NewNodeList(),
		Options:      NewNodeList(),
	}
}

// StatementType returns the statement type for Statement interface.
func (c *CreateStmt) StatementType() string {
	return "CREATE"
}

// String returns a string representation of the CreateStmt.
func (c *CreateStmt) String() string {
	return fmt.Sprintf("CreateStmt@%d", c.Location())
}

// ResTarget represents a result target (in SELECT lists, etc.).
// Ported from postgres/src/include/nodes/parsenodes.h ResTarget concept
type ResTarget struct {
	BaseNode
	
	Name        string // Column alias - ported concept from postgres
	Indirection *NodeList // Array subscripts, field selections - ported concept from postgres
	Val         Node   // Expression - ported concept from postgres
}

// NewResTarget creates a new ResTarget node.
func NewResTarget(name string, val Node) *ResTarget {
	return &ResTarget{
		BaseNode:    BaseNode{Tag: T_ResTarget},
		Name:        name,
		Val:         val,
		Indirection: NewNodeList(),
	}
}

// String returns a string representation of the ResTarget.
func (r *ResTarget) String() string {
	if r.Name != "" {
		return fmt.Sprintf("ResTarget(%s)@%d", r.Name, r.Location())
	}
	return fmt.Sprintf("ResTarget@%d", r.Location())
}

// RangeVar represents a table reference.
// Ported from postgres/src/include/nodes/parsenodes.h RangeVar concept
type RangeVar struct {
	BaseNode
	
	CatalogName string // Catalog (database) name - ported concept from postgres
	SchemaName  string // Schema name - ported concept from postgres  
	RelName     string // Relation name - ported concept from postgres
	Alias       Node   // Table alias - ported concept from postgres
	InhOpt      int    // Inheritance option - ported concept from postgres
	Relpersistence byte // Persistence (permanent, temp, etc.) - ported concept from postgres
	IsStar      bool   // "foo.*" syntax - ported concept from postgres
}

// NewRangeVar creates a new RangeVar node.
func NewRangeVar(schemaName, relName string) *RangeVar {
	return &RangeVar{
		BaseNode:   BaseNode{Tag: T_RangeVar},
		SchemaName: schemaName,
		RelName:    relName,
	}
}

// String returns a string representation of the RangeVar.
func (r *RangeVar) String() string {
	name := r.RelName
	if r.SchemaName != "" {
		name = r.SchemaName + "." + name
	}
	return fmt.Sprintf("RangeVar(%s)@%d", name, r.Location())
}

// ColumnRef represents a column reference.
// Ported from postgres/src/include/nodes/parsenodes.h ColumnRef concept
type ColumnRef struct {
	BaseNode
	
	Fields *NodeList // List of field names - ported concept from postgres
}

// NewColumnRef creates a new ColumnRef node.
func NewColumnRef(fields ...string) *ColumnRef {
	nodeFields := NewNodeList()
	for _, field := range fields {
		nodeFields.Append(NewIdentifier(field))
	}
	
	return &ColumnRef{
		BaseNode: BaseNode{Tag: T_ColumnRef},
		Fields:   nodeFields,
	}
}

// String returns a string representation of the ColumnRef.
func (c *ColumnRef) String() string {
	return fmt.Sprintf("ColumnRef[%d fields]@%d", c.Fields.Len(), c.Location())
}

// ExpressionType returns the expression type for Expression interface.
func (c *ColumnRef) ExpressionType() string {
	return "ColumnRef"
}