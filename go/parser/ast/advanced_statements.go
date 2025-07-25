// Package ast provides PostgreSQL AST advanced statement node definitions.
// Ported from postgres/src/include/nodes/parsenodes.h
package ast

import (
	"fmt"
	"strings"
)

// ==============================================================================
// ADVANCED STATEMENT NODES - PostgreSQL Phase 1B Implementation
// Ported from postgres/src/include/nodes/parsenodes.h
// ==============================================================================

// ==============================================================================
// MERGE Statement Support
// ==============================================================================

// MergeStmt represents a MERGE statement for conditional INSERT/UPDATE/DELETE operations
// Ported from postgres/src/include/nodes/parsenodes.h:2084-2093
type MergeStmt struct {
	BaseNode
	Relation         *RangeVar          `json:"relation"`          // Target relation to merge into
	SourceRelation   Node               `json:"sourceRelation"`    // Source relation
	JoinCondition    Node               `json:"joinCondition"`     // Join condition between source and target
	MergeWhenClauses []*MergeWhenClause `json:"mergeWhenClauses"`  // List of WHEN clauses
	ReturningList    []Node             `json:"returningList"`     // List of expressions to return
	WithClause       *WithClause        `json:"withClause"`        // WITH clause
}

func (n *MergeStmt) node() {}
func (n *MergeStmt) stmt() {}

func (n *MergeStmt) String() string {
	var parts []string
	
	if n.WithClause != nil {
		parts = append(parts, n.WithClause.String())
	}
	
	parts = append(parts, "MERGE INTO", n.Relation.String(), "USING", n.SourceRelation.String(), "ON", n.JoinCondition.String())
	
	for _, clause := range n.MergeWhenClauses {
		parts = append(parts, clause.String())
	}
	
	if len(n.ReturningList) > 0 {
		returning := make([]string, len(n.ReturningList))
		for i, expr := range n.ReturningList {
			returning[i] = expr.String()
		}
		parts = append(parts, "RETURNING", strings.Join(returning, ", "))
	}
	
	return strings.Join(parts, " ")
}

// NewMergeStmt creates a new MergeStmt node
func NewMergeStmt(relation *RangeVar, sourceRelation Node, joinCondition Node) *MergeStmt {
	return &MergeStmt{
		BaseNode:       BaseNode{Tag: T_MergeStmt},
		Relation:       relation,
		SourceRelation: sourceRelation,
		JoinCondition:  joinCondition,
	}
}

// MergeMatchKind represents the type of WHEN clause in MERGE statements
// Ported from postgres/src/include/nodes/primnodes.h:1994-1999
type MergeMatchKind int

const (
	MERGE_WHEN_MATCHED MergeMatchKind = iota // WHEN MATCHED
	MERGE_WHEN_NOT_MATCHED_BY_SOURCE         // WHEN NOT MATCHED BY SOURCE
	MERGE_WHEN_NOT_MATCHED_BY_TARGET         // WHEN NOT MATCHED BY TARGET
)

func (m MergeMatchKind) String() string {
	switch m {
	case MERGE_WHEN_MATCHED:
		return "WHEN MATCHED"
	case MERGE_WHEN_NOT_MATCHED_BY_SOURCE:
		return "WHEN NOT MATCHED BY SOURCE"
	case MERGE_WHEN_NOT_MATCHED_BY_TARGET:
		return "WHEN NOT MATCHED BY TARGET"
	default:
		return fmt.Sprintf("MergeMatchKind(%d)", int(m))
	}
}

// Note: OverridingKind is already defined in statements.go

// MergeWhenClause represents individual WHEN clauses in MERGE statements
// Ported from postgres/src/include/nodes/parsenodes.h:1717-1727
type MergeWhenClause struct {
	BaseNode
	MatchKind   MergeMatchKind `json:"matchKind"`   // MATCHED/NOT MATCHED BY SOURCE/TARGET
	CommandType CmdType        `json:"commandType"` // INSERT/UPDATE/DELETE/DO NOTHING
	Override    OverridingKind `json:"override"`    // OVERRIDING clause
	Condition   Node           `json:"condition"`   // WHEN conditions
	TargetList  []*ResTarget   `json:"targetList"`  // INSERT/UPDATE targetlist
	Values      []Node         `json:"values"`      // VALUES to INSERT, or NULL
}

func (n *MergeWhenClause) node() {}

func (n *MergeWhenClause) String() string {
	var parts []string
	parts = append(parts, n.MatchKind.String())
	
	if n.Condition != nil {
		parts = append(parts, "AND", n.Condition.String())
	}
	
	parts = append(parts, "THEN")
	
	switch n.CommandType {
	case CMD_INSERT:
		parts = append(parts, "INSERT")
		if n.Override != OVERRIDING_NOT_SET {
			parts = append(parts, n.Override.String())
		}
		if len(n.Values) > 0 {
			values := make([]string, len(n.Values))
			for i, val := range n.Values {
				values[i] = val.String()
			}
			parts = append(parts, "VALUES", "("+strings.Join(values, ", ")+")")
		}
	case CMD_UPDATE:
		parts = append(parts, "UPDATE SET")
		if len(n.TargetList) > 0 {
			targets := make([]string, len(n.TargetList))
			for i, target := range n.TargetList {
				targets[i] = target.String()
			}
			parts = append(parts, strings.Join(targets, ", "))
		}
	case CMD_DELETE:
		parts = append(parts, "DELETE")
	case CMD_NOTHING:
		parts = append(parts, "DO NOTHING")
	}
	
	return strings.Join(parts, " ")
}

// NewMergeWhenClause creates a new MergeWhenClause node
func NewMergeWhenClause(matchKind MergeMatchKind, commandType CmdType) *MergeWhenClause {
	return &MergeWhenClause{
		BaseNode:     BaseNode{Tag: T_MergeWhenClause},
		MatchKind:   matchKind,
		CommandType: commandType,
	}
}

// ==============================================================================
// SET Operations (UNION, INTERSECT, EXCEPT)
// ==============================================================================

// Note: SetOperation is already defined in statements.go

// SetOperationStmt represents set operations like UNION, INTERSECT, EXCEPT
// Ported from postgres/src/include/nodes/parsenodes.h:2185-2204
type SetOperationStmt struct {
	BaseNode
	Op            SetOperation        `json:"op"`            // Type of set operation
	All           bool                `json:"all"`           // ALL specified?
	Larg          Node                `json:"larg"`          // Left child
	Rarg          Node                `json:"rarg"`          // Right child
	ColTypes      []Oid               `json:"colTypes"`      // OID list of output column type OIDs
	ColTypmods    []int32             `json:"colTypmods"`    // Integer list of output column typmods
	ColCollations []Oid               `json:"colCollations"` // OID list of output column collation OIDs
	GroupClauses  []*SortGroupClause  `json:"groupClauses"`  // List of SortGroupClauses
}

func (n *SetOperationStmt) node() {}
func (n *SetOperationStmt) stmt() {}

func (n *SetOperationStmt) String() string {
	var parts []string
	
	parts = append(parts, n.Larg.String())
	parts = append(parts, n.Op.String())
	
	if n.All {
		parts = append(parts, "ALL")
	}
	
	parts = append(parts, n.Rarg.String())
	
	return strings.Join(parts, " ")
}

// NewSetOperationStmt creates a new SetOperationStmt node
func NewSetOperationStmt(op SetOperation, all bool, larg Node, rarg Node) *SetOperationStmt {
	return &SetOperationStmt{
		BaseNode: BaseNode{Tag: T_SetOperationStmt},
		Op:      op,
		All:     all,
		Larg:    larg,
		Rarg:    rarg,
	}
}

// ==============================================================================
// PL/pgSQL Statement Support
// ==============================================================================

// ReturnStmt represents a RETURN statement in stored procedures/functions
// Ported from postgres/src/include/nodes/parsenodes.h:2210-2214
type ReturnStmt struct {
	BaseNode
	ReturnVal Node `json:"returnval"` // Expression to return
}

func (n *ReturnStmt) node() {}
func (n *ReturnStmt) stmt() {}

func (n *ReturnStmt) String() string {
	if n.ReturnVal != nil {
		return "RETURN " + n.ReturnVal.String()
	}
	return "RETURN"
}

// NewReturnStmt creates a new ReturnStmt node
func NewReturnStmt(returnVal Node) *ReturnStmt {
	return &ReturnStmt{
		BaseNode:   BaseNode{Tag: T_ReturnStmt},
		ReturnVal: returnVal,
	}
}

// PLAssignStmt represents PL/pgSQL assignment statements
// Ported from postgres/src/include/nodes/parsenodes.h:2224-2233
type PLAssignStmt struct {
	BaseNode
	Name        string       `json:"name"`        // Initial column name
	Indirection []Node       `json:"indirection"` // Subscripts and field names, if any
	Nnames      int          `json:"nnames"`      // Number of names to use in ColumnRef
	Val         *SelectStmt  `json:"val"`         // The PL/pgSQL expression to assign
	Location    int          `json:"location"`    // Name's token location, or -1 if unknown
}

func (n *PLAssignStmt) node() {}
func (n *PLAssignStmt) stmt() {}

func (n *PLAssignStmt) String() string {
	var parts []string
	parts = append(parts, n.Name)
	
	for _, ind := range n.Indirection {
		parts = append(parts, "["+ind.String()+"]")
	}
	
	parts = append(parts, ":=")
	
	if n.Val != nil {
		parts = append(parts, n.Val.String())
	}
	
	return strings.Join(parts, "")
}

// NewPLAssignStmt creates a new PLAssignStmt node
func NewPLAssignStmt(name string, val *SelectStmt) *PLAssignStmt {
	return &PLAssignStmt{
		BaseNode:  BaseNode{Tag: T_PLAssignStmt},
		Name:     name,
		Val:      val,
		Location: -1,
	}
}

// ==============================================================================
// INSERT ON CONFLICT Support
// ==============================================================================

// Note: OnConflictAction is already defined in query_execution_nodes.go

// Note: OnConflictClause will be added to statements.go to replace the placeholder

// InferClause represents index inference clause for ON CONFLICT
// Ported from postgres/src/include/nodes/parsenodes.h:1606-1613
type InferClause struct {
	BaseNode
	IndexElems   []*IndexElem `json:"indexElems"`   // IndexElems to infer unique index
	WhereClause  Node         `json:"whereClause"`  // Qualification (partial-index predicate)
	Conname      string       `json:"conname"`      // Constraint name, or NULL if unnamed
	Location     int          `json:"location"`     // Token location, or -1 if unknown
}

func (n *InferClause) node() {}

func (n *InferClause) String() string {
	var parts []string
	
	if len(n.IndexElems) > 0 {
		elems := make([]string, len(n.IndexElems))
		for i, elem := range n.IndexElems {
			elems[i] = elem.String()
		}
		parts = append(parts, "("+strings.Join(elems, ", ")+")")
	}
	
	if n.WhereClause != nil {
		parts = append(parts, "WHERE", n.WhereClause.String())
	}
	
	if n.Conname != "" {
		parts = append(parts, "ON CONSTRAINT", n.Conname)
	}
	
	return strings.Join(parts, " ")
}

// NewInferClause creates a new InferClause node
func NewInferClause() *InferClause {
	return &InferClause{
		BaseNode:  BaseNode{Tag: T_InferClause},
		Location: -1,
	}
}

// ==============================================================================
// WITH CHECK OPTION Support
// ==============================================================================

// WCOKind represents the type of WITH CHECK OPTION
// Ported from postgres/src/include/nodes/parsenodes.h:1358-1366
type WCOKind int

const (
	WCO_VIEW_CHECK WCOKind = iota // WCO on an auto-updatable view
	WCO_RLS_INSERT_CHECK          // RLS INSERT WITH CHECK policy
	WCO_RLS_UPDATE_CHECK          // RLS UPDATE WITH CHECK policy
	WCO_RLS_CONFLICT_CHECK        // RLS ON CONFLICT DO UPDATE USING policy
	WCO_RLS_MERGE_UPDATE_CHECK    // RLS MERGE UPDATE USING policy
	WCO_RLS_MERGE_DELETE_CHECK    // RLS MERGE DELETE USING policy
)

func (w WCOKind) String() string {
	switch w {
	case WCO_VIEW_CHECK:
		return "VIEW CHECK"
	case WCO_RLS_INSERT_CHECK:
		return "RLS INSERT CHECK"
	case WCO_RLS_UPDATE_CHECK:
		return "RLS UPDATE CHECK"
	case WCO_RLS_CONFLICT_CHECK:
		return "RLS CONFLICT CHECK"
	case WCO_RLS_MERGE_UPDATE_CHECK:
		return "RLS MERGE UPDATE CHECK"
	case WCO_RLS_MERGE_DELETE_CHECK:
		return "RLS MERGE DELETE CHECK"
	default:
		return fmt.Sprintf("WCOKind(%d)", int(w))
	}
}

// WithCheckOption represents WITH CHECK OPTION for views and RLS policies
// Ported from postgres/src/include/nodes/parsenodes.h:1368-1376
type WithCheckOption struct {
	BaseNode
	Kind     WCOKind `json:"kind"`     // Kind of WCO
	Relname  string  `json:"relname"`  // Name of relation that specified the WCO
	Polname  string  `json:"polname"`  // Name of RLS policy being checked
	Qual     Node    `json:"qual"`     // Constraint qual to check
	Cascaded bool    `json:"cascaded"` // True for a cascaded WCO on a view
}

func (n *WithCheckOption) node() {}

func (n *WithCheckOption) String() string {
	var parts []string
	parts = append(parts, "WITH")
	
	if n.Cascaded {
		parts = append(parts, "CASCADED")
	} else {
		parts = append(parts, "LOCAL")
	}
	
	parts = append(parts, "CHECK OPTION")
	
	return strings.Join(parts, " ")
}

// NewWithCheckOption creates a new WithCheckOption node
func NewWithCheckOption(kind WCOKind, cascaded bool) *WithCheckOption {
	return &WithCheckOption{
		BaseNode:  BaseNode{Tag: T_WithCheckOption},
		Kind:     kind,
		Cascaded: cascaded,
	}
}

// ==============================================================================
// Additional Statement Types
// ==============================================================================

// TruncateStmt represents TRUNCATE TABLE statements
// Ported from postgres/src/include/nodes/parsenodes.h:3240-3246
type TruncateStmt struct {
	BaseNode
	Relations     []*RangeVar   `json:"relations"`     // Relations (RangeVars) to be truncated
	RestartSeqs   bool          `json:"restartSeqs"`   // Restart owned sequences?
	Behavior      DropBehavior  `json:"behavior"`      // RESTRICT or CASCADE behavior
}

func (n *TruncateStmt) node() {}
func (n *TruncateStmt) stmt() {}

func (n *TruncateStmt) String() string {
	var parts []string
	parts = append(parts, "TRUNCATE TABLE")
	
	if len(n.Relations) > 0 {
		relations := make([]string, len(n.Relations))
		for i, rel := range n.Relations {
			relations[i] = rel.String()
		}
		parts = append(parts, strings.Join(relations, ", "))
	}
	
	if n.RestartSeqs {
		parts = append(parts, "RESTART IDENTITY")
	} else {
		parts = append(parts, "CONTINUE IDENTITY")
	}
	
	parts = append(parts, n.Behavior.String())
	
	return strings.Join(parts, " ")
}

// NewTruncateStmt creates a new TruncateStmt node
func NewTruncateStmt(relations []*RangeVar) *TruncateStmt {
	return &TruncateStmt{
		BaseNode:   BaseNode{Tag: T_TruncateStmt},
		Relations: relations,
		Behavior:  DROP_RESTRICT,
	}
}

// CommentStmt represents COMMENT ON statements
// Ported from postgres/src/include/nodes/parsenodes.h:3252-3258
type CommentStmt struct {
	BaseNode
	Objtype ObjectType `json:"objtype"` // Object's type
	Object  Node       `json:"object"`  // Qualified name of the object
	Comment string     `json:"comment"` // Comment to insert, or NULL to remove
}

func (n *CommentStmt) node() {}
func (n *CommentStmt) stmt() {}

func (n *CommentStmt) String() string {
	var parts []string
	parts = append(parts, "COMMENT ON", n.Objtype.String(), n.Object.String(), "IS")
	
	if n.Comment != "" {
		parts = append(parts, "'"+n.Comment+"'")
	} else {
		parts = append(parts, "NULL")
	}
	
	return strings.Join(parts, " ")
}

// NewCommentStmt creates a new CommentStmt node
func NewCommentStmt(objtype ObjectType, object Node, comment string) *CommentStmt {
	return &CommentStmt{
		BaseNode: BaseNode{Tag: T_CommentStmt},
		Objtype: objtype,
		Object:  object,
		Comment: comment,
	}
}

// RenameStmt represents ALTER ... RENAME TO statements
// Ported from postgres/src/include/nodes/parsenodes.h:3525-3537
type RenameStmt struct {
	BaseNode
	RenameType   ObjectType   `json:"renameType"`   // OBJECT_TABLE, OBJECT_COLUMN, etc
	RelationType ObjectType   `json:"relationType"` // If column name, associated relation type
	Relation     *RangeVar    `json:"relation"`     // In case it's a table
	Object       Node         `json:"object"`       // In case it's some other object
	Subname      string       `json:"subname"`      // Name of contained object (column, rule, trigger, etc)
	Newname      string       `json:"newname"`      // The new name
	Behavior     DropBehavior `json:"behavior"`     // RESTRICT or CASCADE behavior
	MissingOk    bool         `json:"missingOk"`    // Skip error if missing?
}

func (n *RenameStmt) node() {}
func (n *RenameStmt) stmt() {}

func (n *RenameStmt) String() string {
	var parts []string
	parts = append(parts, "ALTER", n.RenameType.String())
	
	if n.Relation != nil {
		parts = append(parts, n.Relation.String())
	} else if n.Object != nil {
		parts = append(parts, n.Object.String())
	}
	
	if n.Subname != "" {
		parts = append(parts, "RENAME", n.Subname, "TO", n.Newname)
	} else {
		parts = append(parts, "RENAME TO", n.Newname)
	}
	
	return strings.Join(parts, " ")
}

// NewRenameStmt creates a new RenameStmt node
func NewRenameStmt(renameType ObjectType, newname string) *RenameStmt {
	return &RenameStmt{
		BaseNode:    BaseNode{Tag: T_RenameStmt},
		RenameType: renameType,
		Newname:    newname,
		Behavior:   DROP_RESTRICT,
	}
}

// AlterOwnerStmt represents ALTER ... OWNER TO statements
// Ported from postgres/src/include/nodes/parsenodes.h:3571-3578
type AlterOwnerStmt struct {
	BaseNode
	ObjectType ObjectType `json:"objectType"` // OBJECT_TABLE, OBJECT_TYPE, etc
	Relation   *RangeVar  `json:"relation"`   // In case it's a table
	Object     Node       `json:"object"`     // In case it's some other object
	Newowner   *RoleSpec  `json:"newowner"`   // The new owner
}

func (n *AlterOwnerStmt) node() {}
func (n *AlterOwnerStmt) stmt() {}

func (n *AlterOwnerStmt) String() string {
	var parts []string
	parts = append(parts, "ALTER", n.ObjectType.String())
	
	if n.Relation != nil {
		parts = append(parts, n.Relation.String())
	} else if n.Object != nil {
		parts = append(parts, n.Object.String())
	}
	
	parts = append(parts, "OWNER TO")
	
	if n.Newowner != nil {
		parts = append(parts, n.Newowner.String())
	}
	
	return strings.Join(parts, " ")
}

// NewAlterOwnerStmt creates a new AlterOwnerStmt node
func NewAlterOwnerStmt(objectType ObjectType, newowner *RoleSpec) *AlterOwnerStmt {
	return &AlterOwnerStmt{
		BaseNode:    BaseNode{Tag: T_AlterOwnerStmt},
		ObjectType: objectType,
		Newowner:   newowner,
	}
}

// RuleStmt represents CREATE RULE statements
// Ported from postgres/src/include/nodes/parsenodes.h:3606-3616
type RuleStmt struct {
	BaseNode
	Relation    *RangeVar `json:"relation"`    // Relation the rule is for
	Rulename    string    `json:"rulename"`    // Name of the rule
	WhereClause Node      `json:"whereClause"` // Qualifications
	Event       CmdType   `json:"event"`       // SELECT, INSERT, etc
	Instead     bool      `json:"instead"`     // Is a 'do instead'?
	Actions     []Node    `json:"actions"`     // The action statements
	Replace     bool      `json:"replace"`     // OR REPLACE
}

func (n *RuleStmt) node() {}
func (n *RuleStmt) stmt() {}

func (n *RuleStmt) String() string {
	var parts []string
	parts = append(parts, "CREATE")
	
	if n.Replace {
		parts = append(parts, "OR REPLACE")
	}
	
	parts = append(parts, "RULE", n.Rulename, "AS ON", n.Event.String(), "TO", n.Relation.String())
	
	if n.WhereClause != nil {
		parts = append(parts, "WHERE", n.WhereClause.String())
	}
	
	parts = append(parts, "DO")
	
	if n.Instead {
		parts = append(parts, "INSTEAD")
	}
	
	if len(n.Actions) == 0 {
		parts = append(parts, "NOTHING")
	} else if len(n.Actions) == 1 {
		parts = append(parts, n.Actions[0].String())
	} else {
		actions := make([]string, len(n.Actions))
		for i, action := range n.Actions {
			actions[i] = action.String()
		}
		parts = append(parts, "("+strings.Join(actions, "; ")+")")
	}
	
	return strings.Join(parts, " ")
}

// NewRuleStmt creates a new RuleStmt node
func NewRuleStmt(relation *RangeVar, rulename string, event CmdType) *RuleStmt {
	return &RuleStmt{
		BaseNode:  BaseNode{Tag: T_RuleStmt},
		Relation: relation,
		Rulename: rulename,
		Event:    event,
	}
}

// LockStmt represents LOCK TABLE statements
// Ported from postgres/src/include/nodes/parsenodes.h:3942-3948
type LockStmt struct {
	BaseNode
	Relations []*RangeVar `json:"relations"` // Relations to lock
	Mode      int         `json:"mode"`      // Lock mode
	Nowait    bool        `json:"nowait"`    // No wait mode
}

func (n *LockStmt) node() {}
func (n *LockStmt) stmt() {}

func (n *LockStmt) String() string {
	var parts []string
	parts = append(parts, "LOCK TABLE")
	
	if len(n.Relations) > 0 {
		relations := make([]string, len(n.Relations))
		for i, rel := range n.Relations {
			relations[i] = rel.String()
		}
		parts = append(parts, strings.Join(relations, ", "))
	}
	
	// Lock modes would need additional enum definition for proper string representation
	parts = append(parts, fmt.Sprintf("IN MODE %d", n.Mode))
	
	if n.Nowait {
		parts = append(parts, "NOWAIT")
	}
	
	return strings.Join(parts, " ")
}

// NewLockStmt creates a new LockStmt node
func NewLockStmt(relations []*RangeVar, mode int) *LockStmt {
	return &LockStmt{
		BaseNode:   BaseNode{Tag: T_LockStmt},
		Relations: relations,
		Mode:      mode,
	}
}