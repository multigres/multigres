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
	Relation         *RangeVar   `json:"relation"`         // Target relation to merge into
	SourceRelation   Node        `json:"sourceRelation"`   // Source relation
	JoinCondition    Node        `json:"joinCondition"`    // Join condition between source and target
	MergeWhenClauses *NodeList   `json:"mergeWhenClauses"` // List of WHEN clauses
	ReturningList    *NodeList   `json:"returningList"`    // List of expressions to return
	WithClause       *WithClause `json:"withClause"`       // WITH clause
}

func (n *MergeStmt) node() {}
func (n *MergeStmt) stmt() {}

func (n *MergeStmt) StatementType() string {
	return "MERGE"
}

func (n *MergeStmt) String() string {
	var parts []string

	if n.WithClause != nil {
		parts = append(parts, n.WithClause.String())
	}

	// Get proper table names from RangeVar
	targetTable := n.Relation.RelName
	if n.Relation.SchemaName != "" {
		targetTable = n.Relation.SchemaName + "." + targetTable
	}

	sourceTable := "source"
	if sourceRv, ok := n.SourceRelation.(*RangeVar); ok {
		sourceTable = sourceRv.RelName
		if sourceRv.SchemaName != "" {
			sourceTable = sourceRv.SchemaName + "." + sourceTable
		}
	}

	parts = append(parts, "MERGE INTO", targetTable, "USING", sourceTable, "ON", n.JoinCondition.String())

	if n.MergeWhenClauses != nil {
		for _, item := range n.MergeWhenClauses.Items {
			if clause, ok := item.(*MergeWhenClause); ok {
				parts = append(parts, clause.String())
			}
		}
	}

	if n.ReturningList != nil && n.ReturningList.Len() > 0 {
		returning := make([]string, n.ReturningList.Len())
		for i, expr := range n.ReturningList.Items {
			returning[i] = expr.String()
		}
		parts = append(parts, "RETURNING", strings.Join(returning, ", "))
	}

	return strings.Join(parts, " ")
}

// SqlString returns the SQL representation of the MergeStmt
func (m *MergeStmt) SqlString() string {
	var parts []string

	// WITH clause
	if m.WithClause != nil {
		parts = append(parts, m.WithClause.SqlString())
	}

	// MERGE INTO target
	parts = append(parts, "MERGE INTO")
	if m.Relation != nil {
		parts = append(parts, m.Relation.SqlString())
	}

	// USING source
	parts = append(parts, "USING")
	if m.SourceRelation != nil {
		parts = append(parts, m.SourceRelation.SqlString())
	}

	// ON condition
	if m.JoinCondition != nil {
		parts = append(parts, "ON", m.JoinCondition.SqlString())
	}

	// WHEN clauses
	if m.MergeWhenClauses != nil {
		for _, item := range m.MergeWhenClauses.Items {
			if clause, ok := item.(*MergeWhenClause); ok && clause != nil {
				parts = append(parts, clause.SqlString())
			}
		}
	}

	// RETURNING clause
	if m.ReturningList != nil && m.ReturningList.Len() > 0 {
		var returning []string
		for _, ret := range m.ReturningList.Items {
			returning = append(returning, ret.SqlString())
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
	MERGE_WHEN_MATCHED               MergeMatchKind = iota // WHEN MATCHED
	MERGE_WHEN_NOT_MATCHED_BY_SOURCE                       // WHEN NOT MATCHED BY SOURCE
	MERGE_WHEN_NOT_MATCHED_BY_TARGET                       // WHEN NOT MATCHED BY TARGET
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

func (m MergeMatchKind) SqlString() string {
	switch m {
	case MERGE_WHEN_MATCHED:
		return "WHEN MATCHED"
	case MERGE_WHEN_NOT_MATCHED_BY_SOURCE:
		return "WHEN NOT MATCHED BY SOURCE"
	case MERGE_WHEN_NOT_MATCHED_BY_TARGET:
		return "WHEN NOT MATCHED" // BY TARGET is the default, so we omit it
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
	Values      *NodeList      `json:"values"`      // VALUES to INSERT, or NULL
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
		if n.Values != nil && n.Values.Len() > 0 {
			values := make([]string, n.Values.Len())
			for i, val := range n.Values.Items {
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

func (n *MergeWhenClause) SqlString() string {
	var parts []string
	parts = append(parts, n.MatchKind.SqlString())

	if n.Condition != nil {
		parts = append(parts, "AND", n.Condition.SqlString())
	}

	parts = append(parts, "THEN")

	switch n.CommandType {
	case CMD_INSERT:
		parts = append(parts, "INSERT")
		if n.Override != OVERRIDING_NOT_SET {
			parts = append(parts, n.Override.SqlString())
		}
		if len(n.TargetList) > 0 {
			// If we have columns specified
			columns := make([]string, len(n.TargetList))
			for i, target := range n.TargetList {
				columns[i] = target.Name
			}
			parts = append(parts, "("+strings.Join(columns, ", ")+")")
		}
		if n.Values != nil && n.Values.Len() > 0 {
			values := make([]string, n.Values.Len())
			for i, val := range n.Values.Items {
				values[i] = val.SqlString()
			}
			parts = append(parts, "VALUES", "("+strings.Join(values, ", ")+")")
		}
	case CMD_UPDATE:
		parts = append(parts, "UPDATE SET")
		if len(n.TargetList) > 0 {
			targets := make([]string, len(n.TargetList))
			for i, target := range n.TargetList {
				// For UPDATE SET, format as "column = value" not "value AS column"
				if target.Val != nil {
					targets[i] = target.Name + " = " + target.Val.SqlString()
				} else {
					targets[i] = target.Name
				}
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
		BaseNode:    BaseNode{Tag: T_MergeWhenClause},
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
	Op            SetOperation       `json:"op"`            // Type of set operation
	All           bool               `json:"all"`           // ALL specified?
	Larg          Node               `json:"larg"`          // Left child
	Rarg          Node               `json:"rarg"`          // Right child
	ColTypes      []Oid              `json:"colTypes"`      // OID list of output column type OIDs
	ColTypmods    []int32            `json:"colTypmods"`    // Integer list of output column typmods
	ColCollations []Oid              `json:"colCollations"` // OID list of output column collation OIDs
	GroupClauses  []*SortGroupClause `json:"groupClauses"`  // List of SortGroupClauses
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
		Op:       op,
		All:      all,
		Larg:     larg,
		Rarg:     rarg,
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

// StatementType returns the statement type for this node
func (n *ReturnStmt) StatementType() string {
	return "RETURN"
}

// NewReturnStmt creates a new ReturnStmt node
func NewReturnStmt(returnVal Node) *ReturnStmt {
	return &ReturnStmt{
		BaseNode:  BaseNode{Tag: T_ReturnStmt},
		ReturnVal: returnVal,
	}
}

// PLAssignStmt represents PL/pgSQL assignment statements
// Ported from postgres/src/include/nodes/parsenodes.h:2224-2233
type PLAssignStmt struct {
	BaseNode
	Name        string      `json:"name"`        // Initial column name
	Indirection *NodeList   `json:"indirection"` // Subscripts and field names, if any
	Nnames      int         `json:"nnames"`      // Number of names to use in ColumnRef
	Val         *SelectStmt `json:"val"`         // The PL/pgSQL expression to assign
	Location    int         `json:"location"`    // Name's token location, or -1 if unknown
}

func (n *PLAssignStmt) node() {}
func (n *PLAssignStmt) stmt() {}

func (n *PLAssignStmt) String() string {
	var parts []string
	parts = append(parts, n.Name)

	if n.Indirection != nil {
		for _, ind := range n.Indirection.Items {
			parts = append(parts, "["+ind.String()+"]")
		}
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
		BaseNode: BaseNode{Tag: T_PLAssignStmt},
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
	IndexElems  *NodeList `json:"indexElems"`  // IndexElems to infer unique index
	WhereClause Node      `json:"whereClause"` // Qualification (partial-index predicate)
	Conname     string    `json:"conname"`     // Constraint name, or NULL if unnamed
}

func (n *InferClause) node() {}

func (n *InferClause) String() string {
	var parts []string

	if n.IndexElems != nil && n.IndexElems.Len() > 0 {
		var elems []string
		for _, item := range n.IndexElems.Items {
			if elem, ok := item.(*IndexElem); ok {
				elems = append(elems, elem.String())
			}
		}
		if len(elems) > 0 {
			parts = append(parts, "("+strings.Join(elems, ", ")+")")
		}
	}

	if n.WhereClause != nil {
		parts = append(parts, "WHERE", n.WhereClause.String())
	}

	if n.Conname != "" {
		parts = append(parts, "ON CONSTRAINT", n.Conname)
	}

	return strings.Join(parts, " ")
}

func (n *InferClause) SqlString() string {
	var parts []string

	if n.IndexElems != nil && n.IndexElems.Len() > 0 {
		var elems []string
		for _, item := range n.IndexElems.Items {
			if elem, ok := item.(*IndexElem); ok {
				elems = append(elems, elem.SqlString())
			}
		}
		if len(elems) > 0 {
			parts = append(parts, "("+strings.Join(elems, ", ")+")")
		}
	}

	if n.WhereClause != nil {
		parts = append(parts, "WHERE", n.WhereClause.SqlString())
	}

	if n.Conname != "" {
		parts = append(parts, "ON CONSTRAINT", n.Conname)
	}

	return strings.Join(parts, " ")
}

// NewInferClause creates a new InferClause node
func NewInferClause() *InferClause {
	return &InferClause{
		BaseNode: BaseNode{Tag: T_InferClause},
	}
}

// ==============================================================================
// WITH CHECK OPTION Support
// ==============================================================================

// WCOKind represents the type of WITH CHECK OPTION
// Ported from postgres/src/include/nodes/parsenodes.h:1358-1366
type WCOKind int

const (
	WCO_VIEW_CHECK             WCOKind = iota // WCO on an auto-updatable view
	WCO_RLS_INSERT_CHECK                      // RLS INSERT WITH CHECK policy
	WCO_RLS_UPDATE_CHECK                      // RLS UPDATE WITH CHECK policy
	WCO_RLS_CONFLICT_CHECK                    // RLS ON CONFLICT DO UPDATE USING policy
	WCO_RLS_MERGE_UPDATE_CHECK                // RLS MERGE UPDATE USING policy
	WCO_RLS_MERGE_DELETE_CHECK                // RLS MERGE DELETE USING policy
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
		BaseNode: BaseNode{Tag: T_WithCheckOption},
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
	Relations   *NodeList    `json:"relations"`   // Relations (RangeVars) to be truncated
	RestartSeqs bool         `json:"restartSeqs"` // Restart owned sequences?
	Behavior    DropBehavior `json:"behavior"`    // RESTRICT or CASCADE behavior
}

func (n *TruncateStmt) node() {}
func (n *TruncateStmt) stmt() {}

func (n *TruncateStmt) String() string {
	var parts []string
	parts = append(parts, "TRUNCATE TABLE")

	if n.Relations != nil && n.Relations.Len() > 0 {
		relations := make([]string, n.Relations.Len())
		for i, item := range n.Relations.Items {
			relations[i] = item.String()
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
func NewTruncateStmt(relations *NodeList) *TruncateStmt {
	return &TruncateStmt{
		BaseNode:  BaseNode{Tag: T_TruncateStmt},
		Relations: relations,
		Behavior:  DropRestrict,
	}
}

// StatementType implements the Stmt interface
func (n *TruncateStmt) StatementType() string {
	return "TRUNCATE"
}

// SqlString returns the SQL representation of the TRUNCATE statement
func (n *TruncateStmt) SqlString() string {
	var parts []string
	parts = append(parts, "TRUNCATE TABLE")

	if n.Relations != nil && n.Relations.Len() > 0 {
		relations := make([]string, n.Relations.Len())
		for i, item := range n.Relations.Items {
			if rangeVar, ok := item.(*RangeVar); ok {
				relations[i] = rangeVar.RelName
			}
		}
		parts = append(parts, strings.Join(relations, ", "))
	}

	if n.RestartSeqs {
		parts = append(parts, "RESTART IDENTITY")
	} else {
		parts = append(parts, "CONTINUE IDENTITY")
	}

	switch n.Behavior {
	case DropCascade:
		parts = append(parts, "CASCADE")
	case DropRestrict:
		parts = append(parts, "RESTRICT")
	}

	return strings.Join(parts, " ")
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

func (n *CommentStmt) StatementType() string {
	return "CommentStmt"
}

func (n *CommentStmt) String() string {
	return n.SqlString()
}

func (n *CommentStmt) SqlString() string {
	var parts []string

	// Handle special object types that need custom formatting
	switch n.Objtype {
	case OBJECT_TABCONSTRAINT:
		if nodeList, ok := n.Object.(*NodeList); ok && len(nodeList.Items) > 1 {
			// Last item is constraint name, preceding items are table name
			constraintName := ""
			var tableNameParts []string

			for i, item := range nodeList.Items {
				if str, ok := item.(*String); ok {
					if i == len(nodeList.Items)-1 {
						constraintName = str.SVal
					} else {
						tableNameParts = append(tableNameParts, str.SVal)
					}
				}
			}

			tableName := FormatQualifiedName(tableNameParts...)
			parts = append(parts, "COMMENT ON CONSTRAINT", QuoteIdentifier(constraintName), "ON", tableName, "IS")
		}

	case OBJECT_DOMCONSTRAINT:
		if nodeList, ok := n.Object.(*NodeList); ok && len(nodeList.Items) == 2 {
			// First item is TypeName, second is constraint name (as String)
			domainName := ""
			constraintName := ""

			// Get the domain TypeName and format it
			if typeName, ok := nodeList.Items[0].(*TypeName); ok {
				domainName = typeName.SqlString()
			}

			// Get the constraint name
			if str, ok := nodeList.Items[1].(*String); ok {
				constraintName = str.SVal
			}

			parts = append(parts, "COMMENT ON CONSTRAINT", QuoteIdentifier(constraintName), "ON DOMAIN", domainName, "IS")
		}

	case OBJECT_TRANSFORM:
		if nodeList, ok := n.Object.(*NodeList); ok && len(nodeList.Items) == 2 {
			// First item is Typename, second is language name
			typeName := nodeList.Items[0].SqlString()
			if str, ok := nodeList.Items[1].(*String); ok {
				parts = append(parts, "COMMENT ON TRANSFORM FOR", typeName, "LANGUAGE", QuoteIdentifier(str.SVal), "IS")
			}
		}

	case OBJECT_OPCLASS:
		if nodeList, ok := n.Object.(*NodeList); ok && len(nodeList.Items) > 1 {
			// First item is access method, rest are class name parts
			if str, ok := nodeList.Items[0].(*String); ok {
				accessMethod := str.SVal
				var classNameParts []string
				for i := 1; i < len(nodeList.Items); i++ {
					if str, ok := nodeList.Items[i].(*String); ok {
						classNameParts = append(classNameParts, str.SVal)
					}
				}
				className := FormatQualifiedName(classNameParts...)
				parts = append(parts, "COMMENT ON OPERATOR CLASS", className, "USING", QuoteIdentifier(accessMethod), "IS")
			}
		}

	case OBJECT_OPFAMILY:
		if nodeList, ok := n.Object.(*NodeList); ok && len(nodeList.Items) > 1 {
			// First item is access method, rest are family name parts
			if str, ok := nodeList.Items[0].(*String); ok {
				accessMethod := str.SVal
				var familyNameParts []string
				for i := 1; i < len(nodeList.Items); i++ {
					if str, ok := nodeList.Items[i].(*String); ok {
						familyNameParts = append(familyNameParts, str.SVal)
					}
				}
				familyName := FormatQualifiedName(familyNameParts...)
				parts = append(parts, "COMMENT ON OPERATOR FAMILY", familyName, "USING", QuoteIdentifier(accessMethod), "IS")
			}
		}

	case OBJECT_LARGEOBJECT:
		// NumericOnly should format directly
		parts = append(parts, "COMMENT ON LARGE OBJECT", n.Object.SqlString(), "IS")

	case OBJECT_CAST:
		if nodeList, ok := n.Object.(*NodeList); ok && len(nodeList.Items) == 2 {
			// First is source type, second is target type
			sourceType := nodeList.Items[0].SqlString()
			targetType := nodeList.Items[1].SqlString()
			parts = append(parts, "COMMENT ON CAST (", sourceType, "AS", targetType, ") IS")
		}

	default:
		// Regular format for other object types
		objectName := formatObjectName(n.Object)
		parts = append(parts, "COMMENT ON", n.Objtype.String(), objectName, "IS")
	}

	if n.Comment != "" {
		parts = append(parts, "'"+n.Comment+"'")
	} else {
		parts = append(parts, "NULL")
	}

	return strings.Join(parts, " ")
}

// formatObjectName formats a Node (NodeList of String nodes or single String) as a qualified identifier
func formatObjectName(obj Node) string {
	if nodeList, ok := obj.(*NodeList); ok && len(nodeList.Items) > 0 {
		var parts []string
		for _, item := range nodeList.Items {
			if str, ok := item.(*String); ok {
				parts = append(parts, str.SVal)
			}
		}
		return FormatQualifiedName(parts...)
	}
	// Handle single String nodes (from object_type_name rules)
	if str, ok := obj.(*String); ok {
		return QuoteIdentifier(str.SVal)
	}
	// Fallback to regular SqlString for other node types (Typename, etc.)
	return obj.SqlString()
}

// NewCommentStmt creates a new CommentStmt node
func NewCommentStmt(objtype ObjectType, object Node, comment string) *CommentStmt {
	return &CommentStmt{
		BaseNode: BaseNode{Tag: T_CommentStmt},
		Objtype:  objtype,
		Object:   object,
		Comment:  comment,
	}
}

// SecLabelStmt represents SECURITY LABEL statements
// Ported from postgres/src/include/nodes/parsenodes.h:3267-3274
type SecLabelStmt struct {
	BaseNode
	Objtype  ObjectType `json:"objtype"`  // Object's type
	Object   Node       `json:"object"`   // Qualified name of the object
	Provider string     `json:"provider"` // Label provider (or NULL)
	Label    string     `json:"label"`    // New security label to be assigned
}

func (n *SecLabelStmt) node() {}
func (n *SecLabelStmt) stmt() {}

func (n *SecLabelStmt) StatementType() string {
	return "SecLabelStmt"
}

func (n *SecLabelStmt) String() string {
	return n.SqlString()
}

func (n *SecLabelStmt) SqlString() string {
	var parts []string
	parts = append(parts, "SECURITY LABEL")

	if n.Provider != "" {
		parts = append(parts, "FOR", n.Provider)
	}

	// Format object name as qualified identifier, not string literal
	objectName := formatObjectName(n.Object)
	parts = append(parts, "ON", n.Objtype.String(), objectName, "IS")

	if n.Label != "" {
		parts = append(parts, "'"+n.Label+"'")
	} else {
		parts = append(parts, "NULL")
	}

	return strings.Join(parts, " ")
}

// NewSecLabelStmt creates a new SecLabelStmt node
func NewSecLabelStmt(objtype ObjectType, object Node, provider string, label string) *SecLabelStmt {
	return &SecLabelStmt{
		BaseNode: BaseNode{Tag: T_SecLabelStmt},
		Objtype:  objtype,
		Object:   object,
		Provider: provider,
		Label:    label,
	}
}

// DoStmt represents DO statements
// Ported from postgres/src/include/nodes/parsenodes.h:3437-3441
type DoStmt struct {
	BaseNode
	Args *NodeList `json:"args"` // List of DefElem nodes
}

func (n *DoStmt) node() {}
func (n *DoStmt) stmt() {}

func (n *DoStmt) StatementType() string {
	return "DoStmt"
}

func (n *DoStmt) String() string {
	return n.SqlString()
}

func (n *DoStmt) SqlString() string {
	var parts []string
	parts = append(parts, "DO")

	if n.Args != nil {
		var language string
		var code string

		// Process DefElem arguments to extract language and code
		for _, arg := range n.Args.Items {
			if defElem, ok := arg.(*DefElem); ok {
				switch defElem.Defname {
				case "language":
					if str, ok := defElem.Arg.(*String); ok {
						language = str.SVal
					}
				case "as":
					if str, ok := defElem.Arg.(*String); ok {
						code = str.SVal
					}
				}
			}
		}

		// Format according to PostgreSQL DO statement syntax
		if language != "" {
			parts = append(parts, "LANGUAGE", QuoteIdentifier(language))
		}

		if code != "" {
			// Use single quotes for the code block
			parts = append(parts, QuoteStringLiteral(code))
		}
	}

	return strings.Join(parts, " ")
}

// NewDoStmt creates a new DoStmt node
func NewDoStmt(args *NodeList) *DoStmt {
	return &DoStmt{
		BaseNode: BaseNode{Tag: T_DoStmt},
		Args:     args,
	}
}

// CallStmt represents CALL statements
// Ported from postgres/src/include/nodes/parsenodes.h:3451-3458
type CallStmt struct {
	BaseNode
	Funccall *FuncCall `json:"funccall"` // from the parser
	// Note: funcexpr and outargs are used at execution time, not parsing
}

func (n *CallStmt) node() {}
func (n *CallStmt) stmt() {}

func (n *CallStmt) StatementType() string {
	return "CallStmt"
}

func (n *CallStmt) String() string {
	return n.SqlString()
}

func (n *CallStmt) SqlString() string {
	return "CALL " + n.Funccall.SqlString()
}

// NewCallStmt creates a new CallStmt node
func NewCallStmt(funccall *FuncCall) *CallStmt {
	return &CallStmt{
		BaseNode: BaseNode{Tag: T_CallStmt},
		Funccall: funccall,
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

func (n *RenameStmt) StatementType() string {
	return "RenameStmt"
}

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

func (n *RenameStmt) SqlString() string {
	var parts []string

	// Start with ALTER
	parts = append(parts, "ALTER")

	// Add object type
	switch n.RenameType {
	case OBJECT_COLUMN:
		// Column rename requires the relation type
		switch n.RelationType {
		case OBJECT_TABLE:
			parts = append(parts, "TABLE")
		case OBJECT_VIEW:
			parts = append(parts, "VIEW")
		case OBJECT_MATVIEW:
			parts = append(parts, "MATERIALIZED VIEW")
		case OBJECT_FOREIGN_TABLE:
			parts = append(parts, "FOREIGN TABLE")
		default:
			parts = append(parts, "TABLE")
		}
	case OBJECT_TABLE:
		parts = append(parts, "TABLE")
	case OBJECT_INDEX:
		parts = append(parts, "INDEX")
	case OBJECT_SEQUENCE:
		parts = append(parts, "SEQUENCE")
	case OBJECT_VIEW:
		parts = append(parts, "VIEW")
	case OBJECT_MATVIEW:
		parts = append(parts, "MATERIALIZED VIEW")
	case OBJECT_FOREIGN_TABLE:
		parts = append(parts, "FOREIGN TABLE")
	case OBJECT_TABCONSTRAINT:
		parts = append(parts, "TABLE")
	default:
		parts = append(parts, "TABLE")
	}

	// Add IF EXISTS if specified
	if n.MissingOk {
		parts = append(parts, "IF EXISTS")
	}

	// Add relation/object name
	if n.Relation != nil {
		parts = append(parts, n.Relation.SqlString())
	} else if n.Object != nil {
		parts = append(parts, n.Object.SqlString())
	}

	// Add RENAME clause
	parts = append(parts, "RENAME")

	if n.RenameType == OBJECT_COLUMN {
		// Column rename: RENAME [COLUMN] old_name TO new_name
		parts = append(parts, "COLUMN", n.Subname, "TO", n.Newname)
	} else if n.RenameType == OBJECT_TABCONSTRAINT {
		// Constraint rename: RENAME CONSTRAINT old_name TO new_name
		parts = append(parts, "CONSTRAINT", n.Subname, "TO", n.Newname)
	} else {
		// Table/Index/View rename: RENAME TO new_name
		parts = append(parts, "TO", n.Newname)
	}

	return strings.Join(parts, " ")
}

// NewRenameStmt creates a new RenameStmt node
func NewRenameStmt(renameType ObjectType, newname string) *RenameStmt {
	return &RenameStmt{
		BaseNode:   BaseNode{Tag: T_RenameStmt},
		RenameType: renameType,
		Newname:    newname,
		Behavior:   DropRestrict,
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

func (n *AlterOwnerStmt) String() string {
	return n.SqlString()
}

// SqlString returns the SQL representation of the AlterOwnerStmt
func (n *AlterOwnerStmt) SqlString() string {
	var parts []string
	parts = append(parts, "ALTER")

	// Add object type
	parts = append(parts, n.ObjectType.String())

	// Add object name
	if n.Relation != nil {
		parts = append(parts, n.Relation.SqlString())
	} else if n.Object != nil {
		// Special handling for NodeList objects
		if nodeList, ok := n.Object.(*NodeList); ok {
			if n.ObjectType == OBJECT_OPCLASS || n.ObjectType == OBJECT_OPFAMILY {
				// Format as "name USING method" where first item is method, rest is name
				if nodeList.Len() >= 2 {
					methodStr := ""
					if str, ok := nodeList.Items[0].(*String); ok {
						methodStr = str.SVal
					} else {
						methodStr = nodeList.Items[0].SqlString()
					}

					var nameStr string
					for i := 1; i < nodeList.Len(); i++ {
						if i > 1 {
							nameStr += "."
						}
						if str, ok := nodeList.Items[i].(*String); ok {
							nameStr += QuoteIdentifier(str.SVal)
						} else {
							nameStr += nodeList.Items[i].SqlString()
						}
					}
					parts = append(parts, nameStr, "USING", methodStr)
				} else {
					parts = append(parts, n.Object.SqlString())
				}
			} else {
				// For other object types, handle as qualified identifier names
				var nameParts []string
				for _, item := range nodeList.Items {
					if str, ok := item.(*String); ok {
						nameParts = append(nameParts, QuoteIdentifier(str.SVal))
					} else {
						nameParts = append(nameParts, item.SqlString())
					}
				}
				parts = append(parts, strings.Join(nameParts, "."))
			}
		} else if str, ok := n.Object.(*String); ok {
			// Handle simple string identifiers
			parts = append(parts, QuoteIdentifier(str.SVal))
		} else {
			parts = append(parts, n.Object.SqlString())
		}
	}

	// Add OWNER TO clause
	parts = append(parts, "OWNER TO")
	if n.Newowner != nil {
		parts = append(parts, n.Newowner.SqlString())
	}

	return strings.Join(parts, " ")
}

// StatementType returns the statement type
func (n *AlterOwnerStmt) StatementType() string {
	return "ALTER"
}

// NewAlterOwnerStmt creates a new AlterOwnerStmt node
func NewAlterOwnerStmt(objectType ObjectType, newowner *RoleSpec) *AlterOwnerStmt {
	return &AlterOwnerStmt{
		BaseNode:   BaseNode{Tag: T_AlterOwnerStmt},
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
	Actions     *NodeList `json:"actions"`     // The action statements
	Replace     bool      `json:"replace"`     // OR REPLACE
}

func (n *RuleStmt) String() string {
	var parts []string
	parts = append(parts, "CREATE")

	if n.Replace {
		parts = append(parts, "OR REPLACE")
	}

	// Get the proper table name from RangeVar
	tableName := n.Relation.RelName
	if n.Relation.SchemaName != "" {
		tableName = n.Relation.SchemaName + "." + tableName
	}
	if n.Relation.CatalogName != "" {
		tableName = n.Relation.CatalogName + "." + tableName
	}

	parts = append(parts, "RULE", n.Rulename, "AS ON", n.Event.String(), "TO", tableName)

	if n.WhereClause != nil {
		// Use SqlString() for proper expression deparsing
		if sqlStringer, ok := n.WhereClause.(interface{ SqlString() string }); ok {
			parts = append(parts, "WHERE", sqlStringer.SqlString())
		} else {
			parts = append(parts, "WHERE", n.WhereClause.String())
		}
	}

	parts = append(parts, "DO")

	if n.Instead {
		parts = append(parts, "INSTEAD")
	}

	if n.Actions == nil || n.Actions.Len() == 0 {
		parts = append(parts, "NOTHING")
	} else if n.Actions.Len() == 1 {
		if stmt, ok := n.Actions.Items[0].(Stmt); ok {
			parts = append(parts, stmt.SqlString())
		} else {
			parts = append(parts, n.Actions.Items[0].String())
		}
	} else {
		actions := make([]string, n.Actions.Len())
		for i, action := range n.Actions.Items {
			if stmt, ok := action.(Stmt); ok {
				actions[i] = stmt.SqlString()
			} else {
				actions[i] = action.String()
			}
		}
		parts = append(parts, "("+strings.Join(actions, "; ")+")")
	}

	return strings.Join(parts, " ")
}

func (n *RuleStmt) StatementType() string {
	return "CREATE RULE"
}

func (n *RuleStmt) Location() int {
	return n.BaseNode.Loc
}

func (n *RuleStmt) NodeTag() NodeTag {
	return T_RuleStmt
}

func (n *RuleStmt) SqlString() string {
	return n.String()
}

// NewRuleStmt creates a new RuleStmt node
func NewRuleStmt(relation *RangeVar, rulename string, event CmdType) *RuleStmt {
	return &RuleStmt{
		BaseNode: BaseNode{Tag: T_RuleStmt},
		Relation: relation,
		Rulename: rulename,
		Event:    event,
	}
}

// Lock mode constants based on PostgreSQL's LOCKMODE
// From postgres/src/include/storage/lockdefs.h
type LockMode int

const (
	NoLock LockMode = iota
	AccessShareLock
	RowShareLock
	RowExclusiveLock
	ShareUpdateExclusiveLock
	ShareLock
	ShareRowExclusiveLock
	ExclusiveLock
	AccessExclusiveLock
)

// LockStmt represents LOCK TABLE statements
// Ported from postgres/src/include/nodes/parsenodes.h:3942-3948
type LockStmt struct {
	BaseNode
	Relations *NodeList `json:"relations"` // Relations to lock
	Mode      LockMode  `json:"mode"`      // Lock mode
	Nowait    bool      `json:"nowait"`    // No wait mode
}

func (n *LockStmt) String() string {
	var parts []string
	parts = append(parts, "LOCK TABLE")

	if n.Relations != nil && n.Relations.Len() > 0 {
		relations := make([]string, n.Relations.Len())
		for i, item := range n.Relations.Items {
			relations[i] = item.String()
		}
		parts = append(parts, strings.Join(relations, ", "))
	}

	// Add lock mode
	switch n.Mode {
	case AccessShareLock:
		parts = append(parts, "IN ACCESS SHARE MODE")
	case RowShareLock:
		parts = append(parts, "IN ROW SHARE MODE")
	case RowExclusiveLock:
		parts = append(parts, "IN ROW EXCLUSIVE MODE")
	case ShareUpdateExclusiveLock:
		parts = append(parts, "IN SHARE UPDATE EXCLUSIVE MODE")
	case ShareLock:
		parts = append(parts, "IN SHARE MODE")
	case ShareRowExclusiveLock:
		parts = append(parts, "IN SHARE ROW EXCLUSIVE MODE")
	case ExclusiveLock:
		parts = append(parts, "IN EXCLUSIVE MODE")
	case AccessExclusiveLock:
		parts = append(parts, "IN ACCESS EXCLUSIVE MODE")
	default:
		parts = append(parts, "IN ACCESS EXCLUSIVE MODE")
	}

	if n.Nowait {
		parts = append(parts, "NOWAIT")
	}

	return strings.Join(parts, " ")
}

// NewLockStmt creates a new LockStmt node
func NewLockStmt(relations *NodeList, mode LockMode) *LockStmt {
	return &LockStmt{
		BaseNode:  BaseNode{Tag: T_LockStmt},
		Relations: relations,
		Mode:      mode,
	}
}

// StatementType implements the Stmt interface
func (n *LockStmt) StatementType() string {
	return "LOCK"
}

// SqlString returns the SQL representation of the LOCK statement
func (n *LockStmt) SqlString() string {
	var parts []string
	parts = append(parts, "LOCK TABLE")

	if n.Relations != nil && n.Relations.Len() > 0 {
		relations := make([]string, n.Relations.Len())
		for i, item := range n.Relations.Items {
			if rangeVar, ok := item.(*RangeVar); ok {
				relations[i] = rangeVar.RelName
			}
		}
		parts = append(parts, strings.Join(relations, ", "))
	}

	// Add lock mode
	switch n.Mode {
	case AccessShareLock:
		parts = append(parts, "IN ACCESS SHARE MODE")
	case RowShareLock:
		parts = append(parts, "IN ROW SHARE MODE")
	case RowExclusiveLock:
		parts = append(parts, "IN ROW EXCLUSIVE MODE")
	case ShareUpdateExclusiveLock:
		parts = append(parts, "IN SHARE UPDATE EXCLUSIVE MODE")
	case ShareLock:
		parts = append(parts, "IN SHARE MODE")
	case ShareRowExclusiveLock:
		parts = append(parts, "IN SHARE ROW EXCLUSIVE MODE")
	case ExclusiveLock:
		parts = append(parts, "IN EXCLUSIVE MODE")
	case AccessExclusiveLock:
		parts = append(parts, "IN ACCESS EXCLUSIVE MODE")
	default:
		parts = append(parts, "IN ACCESS EXCLUSIVE MODE")
	}

	if n.Nowait {
		parts = append(parts, "NOWAIT")
	}

	return strings.Join(parts, " ")
}
