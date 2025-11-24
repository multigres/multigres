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
	QSRC_ORIGINAL          QuerySource = iota // Original parse tree (explicit query)
	QSRC_PARSER                               // Added by parse analysis (now unused)
	QSRC_INSTEAD_RULE                         // Added by unconditional INSTEAD rule
	QSRC_QUAL_INSTEAD_RULE                    // Added by conditional INSTEAD rule
	QSRC_NON_INSTEAD_RULE                     // Added by non-INSTEAD rule
)

// Note: DropBehavior and ObjectType are now defined in ddl_statements.go

// SetQuantifier represents set quantifier options for GROUP BY and SELECT DISTINCT.
// Ported from postgres/src/include/nodes/parsenodes.h:61
type SetQuantifier int

const (
	SET_QUANTIFIER_DEFAULT  SetQuantifier = iota // No quantifier specified
	SET_QUANTIFIER_ALL                           // ALL quantifier
	SET_QUANTIFIER_DISTINCT                      // DISTINCT quantifier
)

// LimitOption represents LIMIT clause options.
// Ported from postgres/src/include/nodes/nodes.h:428
type LimitOption int

const (
	LIMIT_OPTION_COUNT     LimitOption = iota // FETCH FIRST... ONLY - nodes.h:430
	LIMIT_OPTION_WITH_TIES                    // FETCH FIRST... WITH TIES - nodes.h:431
)

// OnCommitAction represents actions for temporary tables on transaction commit.
// Ported from postgres/src/include/nodes/primnodes.h:55
type OnCommitAction int

const (
	ONCOMMIT_NOOP          OnCommitAction = iota // No ON COMMIT clause (do nothing) - primnodes.h:57
	ONCOMMIT_PRESERVE_ROWS                       // ON COMMIT PRESERVE ROWS (do nothing) - primnodes.h:58
	ONCOMMIT_DELETE_ROWS                         // ON COMMIT DELETE ROWS - primnodes.h:59
	ONCOMMIT_DROP                                // ON COMMIT DROP - primnodes.h:60
)

// RelPersistence represents table persistence types.
// Ported from postgres/src/include/nodes/primnodes.h:87
const (
	RELPERSISTENCE_PERMANENT rune = 'p' // Regular table - primnodes.h:89
	RELPERSISTENCE_UNLOGGED  rune = 'u' // Unlogged table - primnodes.h:90
	RELPERSISTENCE_TEMP      rune = 't' // Temporary table - primnodes.h:91
)

// ==============================================================================
// SUPPORTING STRUCTURES
// ==============================================================================

// RangeVar represents a table/relation reference.
// Ported from postgres/src/include/nodes/primnodes.h:71
type RangeVar struct {
	BaseNode
	CatalogName    string // Database name, or empty - postgres/src/include/nodes/primnodes.h:76
	SchemaName     string // Schema name, or empty - postgres/src/include/nodes/primnodes.h:79
	RelName        string // Relation/sequence name - postgres/src/include/nodes/primnodes.h:82
	Inh            bool   // Expand relation by inheritance? - postgres/src/include/nodes/primnodes.h:85
	RelPersistence rune   // Persistence type - postgres/src/include/nodes/primnodes.h:87
	Alias          *Alias // Table alias & optional column aliases - postgres/src/include/nodes/primnodes.h:90
}

// SqlString returns the SQL representation of this table reference
func (r *RangeVar) SqlString() string {
	// Use utility function for qualified name formatting
	result := FormatFullyQualifiedName(r.CatalogName, r.SchemaName, r.RelName)

	// Add ONLY prefix if inheritance is disabled
	if !r.Inh {
		result = "ONLY " + result
	}

	// Add alias if present
	if r.Alias != nil {
		aliasStr := r.Alias.SqlString()
		if aliasStr != "" {
			result += " " + aliasStr
		}
	}

	return result
}

// NewRangeVar creates a new RangeVar node.
func NewRangeVar(relName string, schemaName, catalogName string) *RangeVar {
	return &RangeVar{
		BaseNode:    BaseNode{Tag: T_RangeVar},
		RelName:     relName,
		SchemaName:  schemaName,
		CatalogName: catalogName,
		Inh:         true, // Default to inheritance enabled (no ONLY)
	}
}

func (rv *RangeVar) String() string {
	parts := []string{}
	if rv.CatalogName != "" {
		parts = append(parts, rv.CatalogName)
	}
	if rv.SchemaName != "" {
		parts = append(parts, rv.SchemaName)
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
	AliasName string    // Alias name - postgres/src/include/nodes/primnodes.h:50
	ColNames  *NodeList // Column aliases - postgres/src/include/nodes/primnodes.h:51
}

// SqlString returns the SQL representation of this alias
func (a *Alias) SqlString() string {
	if a.AliasName == "" {
		return ""
	}

	result := FormatAlias(a.AliasName)

	// Add column aliases if present
	if a.ColNames != nil && len(a.ColNames.Items) > 0 {
		var colAliases []string
		for _, col := range a.ColNames.Items {
			if str, ok := col.(*String); ok {
				colAliases = append(colAliases, str.SVal)
			}
		}
		result += FormatParentheses(FormatCommaList(colAliases))
	}

	return result
}

// NewAlias creates a new Alias node.
func NewAlias(aliasName string, colNames *NodeList) *Alias {
	return &Alias{
		BaseNode:  BaseNode{Tag: T_String}, // Use T_String for alias
		AliasName: aliasName,
		ColNames:  colNames,
	}
}

func (a *Alias) String() string {
	return fmt.Sprintf("Alias(%s)@%d", a.AliasName, a.Location())
}

// ResTarget represents a target item in a SELECT list or UPDATE SET clause.
// Ported from postgres/src/include/nodes/parsenodes.h:514
type ResTarget struct {
	BaseNode
	Name        string    // Column name or empty - postgres/src/include/nodes/parsenodes.h:518
	Indirection *NodeList // Subscripts, field names, and '*', or nil - postgres/src/include/nodes/parsenodes.h:519
	Val         Node      // Value expression to compute or assign - postgres/src/include/nodes/parsenodes.h:520
}

// NewResTarget creates a new ResTarget node.
func NewResTarget(name string, val Node) *ResTarget {
	return &ResTarget{
		BaseNode: BaseNode{Tag: T_ResTarget},
		Name:     name,
		Val:      val,
	}
}

// NewResTargetWithIndirection creates a new ResTarget node with indirection (for column references with array subscripts).
func NewResTargetWithIndirection(name string, indirection *NodeList) *ResTarget {
	return &ResTarget{
		BaseNode:    BaseNode{Tag: T_ResTarget},
		Name:        name,
		Indirection: indirection,
		Val:         nil,
	}
}

// ColumnNameWithIndirection returns the column name with indirection (array subscripts, field access, etc.)
// This is used for INSERT column lists where we need "column[index]" format
func (r *ResTarget) ColumnNameWithIndirection() string {
	if r.Name == "" {
		return ""
	}

	var result strings.Builder
	result.WriteString(QuoteIdentifier(r.Name))
	if r.Indirection != nil {
		for _, ind := range r.Indirection.Items {
			if ind != nil {
				switch i := ind.(type) {
				case *String:
					// Field selection
					result.WriteString("." + i.SVal)
				case *A_Indices:
					// Array index or slice
					result.WriteString(i.SqlString())
				default:
					// Generic indirection
					result.WriteString(ind.SqlString())
				}
			}
		}
	}
	return result.String()
}

// SetClauseString returns the SQL representation for SET clauses (UPDATE, ON CONFLICT DO UPDATE)
// Format: "column[index] = value" instead of "value AS column"
func (r *ResTarget) SetClauseString() string {
	if r.Name == "" {
		return ""
	}

	// Build the column name with indirection (e.g., "col[1]", "col.field")
	columnPart := r.ColumnNameWithIndirection()

	if r.Val != nil {
		return columnPart + " = " + r.Val.SqlString()
	}

	// If no value, just return the column name
	return columnPart
}

func (rt *ResTarget) String() string {
	return fmt.Sprintf("ResTarget(%s)@%d", rt.Name, rt.Location())
}

func (rt *ResTarget) ExpressionType() string {
	return "ResTarget"
}

// SqlString returns the SQL representation of the ResTarget
func (r *ResTarget) SqlString() string {
	if r.Val == nil {
		return ""
	}

	var result strings.Builder
	result.WriteString(r.Val.SqlString())

	// Add indirection if present (e.g., array subscripts, field selection)
	if r.Indirection != nil && len(r.Indirection.Items) > 0 {
		for _, ind := range r.Indirection.Items {
			if ind != nil {
				// Handle different types of indirection
				switch i := ind.(type) {
				case *String:
					// Field selection
					result.WriteString("." + i.SVal)
				case *A_Indices:
					// Array index or slice
					result.WriteString(i.SqlString())
				default:
					// Generic indirection
					result.WriteString(ind.SqlString())
				}
			}
		}
	}

	// Add alias if present
	if r.Name != "" {
		result.WriteString(" AS " + QuoteIdentifier(r.Name))
	}

	return result.String()
}

// ==============================================================================
// CORE QUERY STRUCTURE
// ==============================================================================

// Query is the fundamental query structure that all parsed queries transform into.
// Ported from postgres/src/include/nodes/parsenodes.h:117
type Query struct {
	BaseNode
	CommandType    CmdType     // select|insert|update|delete|merge|utility - postgres/src/include/nodes/parsenodes.h:120
	QuerySource    QuerySource // Where did this query come from? - postgres/src/include/nodes/parsenodes.h:121
	QueryId        uint64      // Query identifier - postgres/src/include/nodes/parsenodes.h:122
	CanSetTag      bool        // Do I set the command result tag? - postgres/src/include/nodes/parsenodes.h:123
	UtilityStmt    Node        // Non-null if commandType == CMD_UTILITY - postgres/src/include/nodes/parsenodes.h:124
	ResultRelation int         // rtable index of target relation - postgres/src/include/nodes/parsenodes.h:125

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
	CteList             []*CommonTableExpr // WITH list
	Rtable              []*RangeTblEntry   // Range table entries
	RtePermInfos        *NodeList          // Permission info for rtable entries - parsenodes.h:174
	Jointree            *FromExpr          // Table join tree (FROM and WHERE clauses)
	MergeActionList     *NodeList          // MERGE statement actions - parsenodes.h:178
	MergeTargetRelation int                // MERGE target relation index - parsenodes.h:186
	MergeJoinCondition  Node               // JOIN condition for MERGE - parsenodes.h:189
	TargetList          []*TargetEntry     // Target list
	Override            OverridingKind     // OVERRIDING clause - parsenodes.h:194
	OnConflict          *OnConflictExpr    // ON CONFLICT expression - parsenodes.h:196
	ReturningList       []*TargetEntry     // Return-values list
	GroupClause         []*SortGroupClause // GROUP BY clauses
	GroupDistinct       bool               // Is the GROUP BY clause distinct?
	GroupingSets        *NodeList          // GROUPING SETS if present
	HavingQual          Node               // Qualifications applied to groups
	WindowClause        []*WindowClause    // WINDOW clauses
	DistinctClause      []*SortGroupClause // DISTINCT clauses
	SortClause          []*SortGroupClause // ORDER BY clauses
	LimitOffset         Node               // Number of result tuples to skip
	LimitCount          Node               // Number of result tuples to return
	LimitOption         LimitOption        // Limit type option - parsenodes.h:215
	RowMarks            []*RowMarkClause   // Row mark clauses
	SetOperations       Node               // Set operation tree - parsenodes.h:219
	ConstraintDeps      []Oid              // Constraint dependencies - parsenodes.h:226
	WithCheckOptions    *NodeList          // WITH CHECK OPTIONS - parsenodes.h:228
	StmtLocation        int                // Start location - postgres/src/include/nodes/parsenodes.h:239
	StmtLen             int                // Length in bytes - postgres/src/include/nodes/parsenodes.h:240
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

// GroupClause represents a GROUP BY clause with quantifier.
// Private struct for the result of group_clause production
// Ported from postgres/src/backend/parser/gram.y:135
type GroupClause struct {
	Distinct bool      // true if GROUP BY DISTINCT
	List     *NodeList // list of GROUP BY expressions
}

// SelectStmt represents a raw SELECT statement before analysis.
// Ported from postgres/src/include/nodes/parsenodes.h:2116
type SelectStmt struct {
	BaseNode
	// Fields used in "leaf" SelectStmts - postgres/src/include/nodes/parsenodes.h:2120-2130
	DistinctClause *NodeList   // NULL, list of DISTINCT ON exprs, or special marker for ALL
	IntoClause     *IntoClause // Target for SELECT INTO
	TargetList     *NodeList   // Target list
	FromClause     *NodeList   // FROM clause
	WhereClause    Node        // WHERE qualification
	GroupClause    *NodeList   // GROUP BY clauses
	GroupDistinct  bool        // Is this GROUP BY DISTINCT?
	HavingClause   Node        // HAVING conditional-expression
	WindowClause   *NodeList   // WINDOW window_name AS (...), ...
	ValuesLists    *NodeList   // Untransformed list of expression lists

	// Fields used in both "leaf" and upper-level SelectStmts - postgres/src/include/nodes/parsenodes.h:2132-2137
	SortClause    *NodeList   // Sort clause
	LimitOffset   Node        // Number of result tuples to skip
	LimitCount    Node        // Number of result tuples to return
	LimitOption   LimitOption // Limit type option
	LockingClause *NodeList   // FOR UPDATE clauses
	WithClause    *WithClause // WITH clause

	// Fields used only in upper-level SelectStmts - postgres/src/include/nodes/parsenodes.h:2139-2143
	Op   SetOperation // Type of set operation
	All  bool         // ALL specified?
	Larg *SelectStmt  // Left child
	Rarg *SelectStmt  // Right child
}

// NewSelectStmt creates a new SelectStmt node.
func NewSelectStmt() *SelectStmt {
	return &SelectStmt{
		BaseNode:       BaseNode{Tag: T_SelectStmt},
		DistinctClause: nil,
		TargetList:     NewNodeList(),
		FromClause:     NewNodeList(),
	}
}

func (s *SelectStmt) String() string {
	return fmt.Sprintf("SelectStmt@%d", s.Location())
}

func (s *SelectStmt) StatementType() string {
	return "SELECT"
}

// SqlString returns the SQL representation of the SelectStmt
func (s *SelectStmt) SqlString() string {
	var parts []string

	// Handle set operations (UNION, INTERSECT, EXCEPT)
	if s.Op != SETOP_NONE {
		// Add left operand, with parentheses only if needed for complex queries
		if s.Larg != nil {
			lstr := s.Larg.SqlString()
			if s.Larg.needsParenthesesInSetOperation() {
				parts = append(parts, "("+lstr+")")
			} else {
				parts = append(parts, lstr)
			}
		}

		switch s.Op {
		case SETOP_UNION:
			if s.All {
				parts = append(parts, "UNION ALL")
			} else {
				parts = append(parts, "UNION")
			}
		case SETOP_INTERSECT:
			if s.All {
				parts = append(parts, "INTERSECT ALL")
			} else {
				parts = append(parts, "INTERSECT")
			}
		case SETOP_EXCEPT:
			if s.All {
				parts = append(parts, "EXCEPT ALL")
			} else {
				parts = append(parts, "EXCEPT")
			}
		}

		if s.Rarg != nil {
			rstr := s.Rarg.SqlString()
			if s.Rarg.needsParenthesesInSetOperation() {
				parts = append(parts, "("+rstr+")")
			} else {
				parts = append(parts, rstr)
			}
		}

		// Handle ORDER BY clause for set operations
		if s.SortClause != nil && s.SortClause.Len() > 0 {
			var sortItems []string
			for _, sort := range s.SortClause.Items {
				if sort != nil {
					sortItems = append(sortItems, sort.SqlString())
				}
			}
			parts = append(parts, "ORDER BY", strings.Join(sortItems, ", "))
		}

		// Handle LIMIT clause for set operations
		if s.LimitCount != nil {
			if s.LimitOption == LIMIT_OPTION_WITH_TIES {
				// Use FETCH FIRST syntax for WITH TIES (required by SQL standard)
				limitStr := "FETCH FIRST"

				// Check if the limit count is a constant 1 (implicit case)
				if isConstantOne(s.LimitCount) {
					limitStr += " ROW"
				} else {
					limitStr += " " + s.LimitCount.SqlString() + " ROWS"
				}

				limitStr += " WITH TIES"
				parts = append(parts, limitStr)
			} else {
				// Traditional LIMIT syntax (default for LIMIT_OPTION_COUNT)
				// Handle LIMIT ALL case specially
				limitValue := s.LimitCount.SqlString()
				if limitValue == "NULL" {
					limitValue = "ALL"
				}
				parts = append(parts, "LIMIT", limitValue)
			}
		}

		// Handle OFFSET clause for set operations
		if s.LimitOffset != nil {
			parts = append(parts, "OFFSET", s.LimitOffset.SqlString())
		}

		return strings.Join(parts, " ")
	}

	// Handle VALUES clause
	if s.ValuesLists != nil && s.ValuesLists.Len() > 0 {
		var valueRows []string
		for _, row := range s.ValuesLists.Items {
			if rowList, ok := row.(*NodeList); ok && rowList.Items != nil {
				var values []string
				for _, val := range rowList.Items {
					values = append(values, val.SqlString())
				}
				valueRows = append(valueRows, fmt.Sprintf("(%s)", strings.Join(values, ", ")))
			}
		}
		return fmt.Sprintf("VALUES %s", strings.Join(valueRows, ", "))
	}

	// Regular SELECT statement
	parts = append(parts, "SELECT")

	// DISTINCT clause
	// Note: DistinctClause == nil means no DISTINCT
	//       DistinctClause == &NodeList{Items: []} means plain DISTINCT
	//       DistinctClause == &NodeList{Items: [expr1, expr2]} means DISTINCT ON (...)
	if s.DistinctClause != nil {
		distinctParts := []string{"DISTINCT"}

		// Check if there are any expressions (means DISTINCT ON)
		if len(s.DistinctClause.Items) > 0 {
			var distinctOn []string
			for _, d := range s.DistinctClause.Items {
				if d != nil {
					distinctOn = append(distinctOn, d.SqlString())
				}
			}
			if len(distinctOn) > 0 {
				distinctParts = append(distinctParts, fmt.Sprintf("ON (%s)", strings.Join(distinctOn, ", ")))
			}
		}
		// Always add DISTINCT part if DistinctClause is not nil
		parts = append(parts, strings.Join(distinctParts, " "))
	}

	// Target list (what to select)
	if s.TargetList != nil && s.TargetList.Len() > 0 {
		var targets []string
		for _, item := range s.TargetList.Items {
			if target, ok := item.(*ResTarget); ok && target != nil {
				targets = append(targets, target.SqlString())
			}
		}
		parts = append(parts, strings.Join(targets, ", "))
	} else {
		parts = append(parts, "*")
	}

	// INTO clause
	if s.IntoClause != nil {
		parts = append(parts, s.IntoClause.SqlString())
	}

	// FROM clause
	if s.FromClause != nil && len(s.FromClause.Items) > 0 {
		var fromItems []string
		for _, from := range s.FromClause.Items {
			if from != nil {
				fromItems = append(fromItems, from.SqlString())
			}
		}
		parts = append(parts, "FROM", strings.Join(fromItems, ", "))
	}

	// WHERE clause
	if s.WhereClause != nil {
		parts = append(parts, "WHERE", s.WhereClause.SqlString())
	}

	// GROUP BY clause
	if s.GroupClause != nil && s.GroupClause.Len() > 0 {
		var groupItems []string
		for _, group := range s.GroupClause.Items {
			if group != nil {
				groupItems = append(groupItems, group.SqlString())
			}
		}
		groupByStr := "GROUP BY"
		if s.GroupDistinct {
			groupByStr = "GROUP BY DISTINCT"
		}
		parts = append(parts, groupByStr, strings.Join(groupItems, ", "))
	}

	// HAVING clause
	if s.HavingClause != nil {
		parts = append(parts, "HAVING", s.HavingClause.SqlString())
	}

	// WINDOW clause
	if s.WindowClause != nil && len(s.WindowClause.Items) > 0 {
		var windowItems []string
		for _, window := range s.WindowClause.Items {
			if windowDef, ok := window.(*WindowDef); ok && windowDef != nil {
				// For WINDOW clause, format as "name AS (specification)"
				if windowDef.Name != "" {
					spec := windowDef.SqlStringForContext(true)
					if spec != "" {
						windowItems = append(windowItems, windowDef.Name+" AS ("+spec+")")
					} else {
						windowItems = append(windowItems, windowDef.Name+" AS ()")
					}
				}
			}
		}
		if len(windowItems) > 0 {
			parts = append(parts, "WINDOW "+strings.Join(windowItems, ", "))
		}
	}

	// ORDER BY clause (from SortClause)
	if s.SortClause != nil && s.SortClause.Len() > 0 {
		var sortItems []string
		for _, sort := range s.SortClause.Items {
			if sort != nil {
				sortItems = append(sortItems, sort.SqlString())
			}
		}
		parts = append(parts, "ORDER BY", strings.Join(sortItems, ", "))
	}

	// LIMIT clause
	if s.LimitCount != nil {
		if s.LimitOption == LIMIT_OPTION_WITH_TIES {
			// Use FETCH FIRST syntax for WITH TIES (required by SQL standard)
			limitStr := "FETCH FIRST"

			// Check if the limit count is a constant 1 (implicit case)
			if isConstantOne(s.LimitCount) {
				limitStr += " ROW"
			} else {
				limitStr += " " + s.LimitCount.SqlString() + " ROWS"
			}

			limitStr += " WITH TIES"
			parts = append(parts, limitStr)
		} else {
			// Traditional LIMIT syntax (default for LIMIT_OPTION_COUNT)
			// Handle LIMIT ALL case specially
			limitValue := s.LimitCount.SqlString()
			if limitValue == "NULL" {
				limitValue = "ALL"
			}
			parts = append(parts, "LIMIT", limitValue)
		}
	}

	// OFFSET clause
	if s.LimitOffset != nil {
		parts = append(parts, "OFFSET", s.LimitOffset.SqlString())
	}

	// FOR UPDATE/SHARE clauses
	if s.LockingClause != nil && s.LockingClause.Len() > 0 {
		for _, item := range s.LockingClause.Items {
			if locking, ok := item.(*LockingClause); ok && locking != nil {
				parts = append(parts, locking.SqlString())
			}
		}
	}

	// WITH clause (CTEs)
	if s.WithClause != nil {
		// WITH clause typically comes first, so we need to prepend it
		withStr := s.WithClause.SqlString()
		return withStr + " " + strings.Join(parts, " ")
	}

	return strings.Join(parts, " ")
}

// needsParenthesesInSetOperation determines if a SelectStmt needs parentheses when used in a set operation
func (s *SelectStmt) needsParenthesesInSetOperation() bool {
	if s == nil {
		return false
	}

	// A SELECT needs parentheses if it has any of these complex clauses
	return s.WhereClause != nil ||
		s.GroupClause != nil && s.GroupClause.Len() > 0 ||
		s.HavingClause != nil ||
		s.SortClause != nil && s.SortClause.Len() > 0 ||
		s.LimitOffset != nil ||
		s.LimitCount != nil ||
		s.WithClause != nil || // WITH clauses need parentheses in set operations
		s.Op != SETOP_NONE // Nested set operations always need parentheses
}

// isConstantOne checks if a Node represents the constant integer 1
func isConstantOne(node Node) bool {
	if node == nil {
		return false
	}

	// Check if it's an A_Const with integer value 1
	if aConst, ok := node.(*A_Const); ok {
		if intVal, ok := aConst.Val.(*Integer); ok {
			return intVal.IVal == 1
		}
	}

	return false
}

// InsertStmt represents an INSERT statement.
// Ported from postgres/src/include/nodes/parsenodes.h:2039
type InsertStmt struct {
	BaseNode
	Relation         *RangeVar         // Relation to insert into - postgres/src/include/nodes/parsenodes.h:2042
	Cols             *NodeList         // Optional: names of the target columns - postgres/src/include/nodes/parsenodes.h:2043
	SelectStmt       Node              // Source SELECT/VALUES, or NULL - postgres/src/include/nodes/parsenodes.h:2044
	OnConflictClause *OnConflictClause // ON CONFLICT clause - postgres/src/include/nodes/parsenodes.h:2045
	ReturningList    *NodeList         // List of expressions to return - postgres/src/include/nodes/parsenodes.h:2046
	WithClause       *WithClause       // WITH clause - postgres/src/include/nodes/parsenodes.h:2047
	Override         OverridingKind    // OVERRIDING clause - postgres/src/include/nodes/parsenodes.h:2048
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

// SqlString returns the SQL representation of the InsertStmt
func (i *InsertStmt) SqlString() string {
	var parts []string

	// WITH clause
	if i.WithClause != nil {
		parts = append(parts, i.WithClause.SqlString())
	}

	// INSERT INTO table
	parts = append(parts, "INSERT INTO")
	if i.Relation != nil {
		parts = append(parts, i.Relation.SqlString())
	}

	// Column list (if specified)
	if i.Cols != nil && i.Cols.Len() > 0 {
		var cols []string
		for _, item := range i.Cols.Items {
			if col, ok := item.(*ResTarget); ok && col.Name != "" {
				cols = append(cols, col.ColumnNameWithIndirection())
			}
		}
		parts = append(parts, fmt.Sprintf("(%s)", strings.Join(cols, ", ")))
	}
	// SelectStmt/VALUES clause
	if i.SelectStmt != nil {
		selectStr := i.SelectStmt.SqlString()
		// If the INSERT has parentheses around the SELECT, we need to preserve them
		// This is determined by checking if the original query had parentheses
		// For now, we'll check if this is a simple SELECT vs a subquery by looking at the SelectStmt
		// If it has a WHERE clause or other complexity, it's likely a subquery that should be parenthesized
		if selectStmt, ok := i.SelectStmt.(*SelectStmt); ok {
			if selectStmt.WhereClause != nil || selectStmt.GroupClause != nil || selectStmt.HavingClause != nil ||
				selectStmt.SortClause != nil || selectStmt.LimitOffset != nil || selectStmt.LimitCount != nil {
				// This appears to be a complex SELECT that was likely parenthesized in the original
				parts = append(parts, fmt.Sprintf("(%s)", selectStr))
			} else {
				parts = append(parts, selectStr)
			}
		} else {
			// Not a SelectStmt, could be VALUES clause, append as is
			parts = append(parts, selectStr)
		}
	} else {
		// DEFAULT VALUES case (SelectStmt is nil)
		parts = append(parts, "DEFAULT VALUES")
	}

	// ON CONFLICT clause
	if i.OnConflictClause != nil {
		parts = append(parts, i.OnConflictClause.SqlString())
	}

	// RETURNING clause
	if i.ReturningList != nil && i.ReturningList.Len() > 0 {
		var returning []string
		for _, item := range i.ReturningList.Items {
			if ret, ok := item.(*ResTarget); ok && ret != nil {
				returning = append(returning, ret.SqlString())
			}
		}
		parts = append(parts, "RETURNING", strings.Join(returning, ", "))
	}

	return strings.Join(parts, " ")
}

// UpdateStmt represents an UPDATE statement.
// Ported from postgres/src/include/nodes/parsenodes.h:2069
type UpdateStmt struct {
	BaseNode
	Relation      *RangeVar   // Relation to update - postgres/src/include/nodes/parsenodes.h:2072
	TargetList    *NodeList   // Target list (of ResTarget) - postgres/src/include/nodes/parsenodes.h:2073
	WhereClause   Node        // Qualifications - postgres/src/include/nodes/parsenodes.h:2074
	FromClause    *NodeList   // Optional from clause for more tables - postgres/src/include/nodes/parsenodes.h:2075
	ReturningList *NodeList   // List of expressions to return - postgres/src/include/nodes/parsenodes.h:2076
	WithClause    *WithClause // WITH clause - postgres/src/include/nodes/parsenodes.h:2077
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

// SqlString returns the SQL representation of the UpdateStmt
func (u *UpdateStmt) SqlString() string {
	var parts []string

	// WITH clause
	if u.WithClause != nil {
		parts = append(parts, u.WithClause.SqlString())
	}

	// UPDATE table
	parts = append(parts, "UPDATE")
	if u.Relation != nil {
		parts = append(parts, u.Relation.SqlString())
	}

	// SET clause
	if u.TargetList != nil && u.TargetList.Len() > 0 {
		var setClauses []string
		for _, item := range u.TargetList.Items {
			if target, ok := item.(*ResTarget); ok && target.Name != "" && target.Val != nil {
				setClauses = append(setClauses, target.SetClauseString())
			}
		}
		parts = append(parts, "SET", strings.Join(setClauses, ", "))
	}

	// FROM clause
	if u.FromClause != nil && u.FromClause.Len() > 0 {
		var fromParts []string
		for _, item := range u.FromClause.Items {
			fromParts = append(fromParts, item.SqlString())
		}
		parts = append(parts, "FROM", strings.Join(fromParts, ", "))
	}

	// WHERE clause
	if u.WhereClause != nil {
		parts = append(parts, "WHERE", u.WhereClause.SqlString())
	}

	// RETURNING clause
	if u.ReturningList != nil && u.ReturningList.Len() > 0 {
		var returning []string
		for _, item := range u.ReturningList.Items {
			if ret, ok := item.(*ResTarget); ok && ret != nil {
				returning = append(returning, ret.SqlString())
			}
		}
		parts = append(parts, "RETURNING", strings.Join(returning, ", "))
	}

	return strings.Join(parts, " ")
}

// DeleteStmt represents a DELETE statement.
// Ported from postgres/src/include/nodes/parsenodes.h:2055
type DeleteStmt struct {
	BaseNode
	Relation      *RangeVar   // Relation to delete from - postgres/src/include/nodes/parsenodes.h:2058
	UsingClause   *NodeList   // Optional using clause for more tables - postgres/src/include/nodes/parsenodes.h:2059
	WhereClause   Node        // Qualifications - postgres/src/include/nodes/parsenodes.h:2060
	ReturningList *NodeList   // List of expressions to return - postgres/src/include/nodes/parsenodes.h:2061
	WithClause    *WithClause // WITH clause - postgres/src/include/nodes/parsenodes.h:2062
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

// SqlString returns the SQL representation of the DeleteStmt
func (d *DeleteStmt) SqlString() string {
	var parts []string

	// WITH clause
	if d.WithClause != nil {
		parts = append(parts, d.WithClause.SqlString())
	}

	// DELETE FROM table
	parts = append(parts, "DELETE FROM")
	if d.Relation != nil {
		parts = append(parts, d.Relation.SqlString())
	}

	// USING clause
	if d.UsingClause != nil && d.UsingClause.Len() > 0 {
		var usingParts []string
		for _, item := range d.UsingClause.Items {
			usingParts = append(usingParts, item.SqlString())
		}
		parts = append(parts, "USING", strings.Join(usingParts, ", "))
	}

	// WHERE clause
	if d.WhereClause != nil {
		parts = append(parts, "WHERE", d.WhereClause.SqlString())
	}

	// RETURNING clause
	if d.ReturningList != nil && d.ReturningList.Len() > 0 {
		var returning []string
		for _, item := range d.ReturningList.Items {
			if ret, ok := item.(*ResTarget); ok && ret != nil {
				returning = append(returning, ret.SqlString())
			}
		}
		parts = append(parts, "RETURNING", strings.Join(returning, ", "))
	}

	return strings.Join(parts, " ")
}

// ==============================================================================
// DDL STATEMENTS
// ==============================================================================

// CreateStmt represents a CREATE TABLE statement.
// Ported from postgres/src/include/nodes/parsenodes.h:2648
type CreateStmt struct {
	BaseNode
	Relation       *RangeVar           // Relation to create - postgres/src/include/nodes/parsenodes.h:2651
	TableElts      *NodeList           // Column definitions - postgres/src/include/nodes/parsenodes.h:2652
	InhRelations   *NodeList           // Relations to inherit from - postgres/src/include/nodes/parsenodes.h:2653
	PartBound      *PartitionBoundSpec // FOR VALUES clause - parsenodes.h
	PartSpec       *PartitionSpec      // PARTITION BY clause - parsenodes.h
	OfTypename     *TypeName           // OF typename clause - parsenodes.h
	Constraints    []*Constraint       // Constraints - postgres/src/include/nodes/parsenodes.h:2659
	Options        *NodeList           // Options from WITH clause - postgres/src/include/nodes/parsenodes.h:2660
	OnCommit       OnCommitAction      // OnCommitAction for temp tables - parsenodes.h
	TableSpaceName string              // Table space to use, or empty - postgres/src/include/nodes/parsenodes.h:2662
	AccessMethod   string              // Table access method - postgres/src/include/nodes/parsenodes.h:2663
	IfNotExists    bool                // Just do nothing if it already exists? - postgres/src/include/nodes/parsenodes.h:2664
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
	Objects    *NodeList    // List of names - postgres/src/include/nodes/parsenodes.h:3229
	RemoveType ObjectType   // Object type - postgres/src/include/nodes/parsenodes.h:3230
	Behavior   DropBehavior // RESTRICT or CASCADE behavior - postgres/src/include/nodes/parsenodes.h:3231
	MissingOk  bool         // Skip error if object is missing? - postgres/src/include/nodes/parsenodes.h:3232
	Concurrent bool         // Drop index concurrently? - postgres/src/include/nodes/parsenodes.h:3233
}

// NewDropStmt creates a new DropStmt node.
func NewDropStmt(objects *NodeList, removeType ObjectType) *DropStmt {
	return &DropStmt{
		BaseNode:   BaseNode{Tag: T_DropStmt},
		Objects:    objects,
		RemoveType: removeType,
		Behavior:   DropRestrict,
	}
}

func (d *DropStmt) String() string {
	count := 0
	if d.Objects != nil {
		count = len(d.Objects.Items)
	}
	return fmt.Sprintf("DropStmt(%d objects)@%d", count, d.Location())
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
	Fields *NodeList // List of field names - postgres/src/include/nodes/parsenodes.h:292
}

// NewColumnRef creates a new ColumnRef node.
func NewColumnRef(fields ...Node) *ColumnRef {
	return &ColumnRef{
		BaseNode: BaseNode{Tag: T_ColumnRef},
		Fields:   NewNodeList(fields...),
	}
}

func (c *ColumnRef) String() string {
	count := 0
	if c.Fields != nil {
		count = len(c.Fields.Items)
	}
	return fmt.Sprintf("ColumnRef[%d fields]@%d", count, c.Location())
}

// SqlString returns the SQL representation of the ColumnRef
func (c *ColumnRef) SqlString() string {
	if c.Fields == nil || len(c.Fields.Items) == 0 {
		return ""
	}

	var parts []string
	for _, field := range c.Fields.Items {
		if field == nil {
			continue
		}

		// Handle different field types (String for column names, A_Star for *, A_Indices for array access)
		switch f := field.(type) {
		case *String:
			parts = append(parts, QuoteIdentifier(f.SVal))
		case *A_Star:
			parts = append(parts, "*")
		case *A_Indices:
			// Array access - format as [index] or [start:end]
			if f.IsSlice {
				startStr := ""
				endStr := ""
				if f.Lidx != nil {
					startStr = f.Lidx.SqlString()
				}
				if f.Uidx != nil {
					endStr = f.Uidx.SqlString()
				}
				parts = append(parts, fmt.Sprintf("[%s:%s]", startStr, endStr))
			} else {
				if f.Uidx != nil {
					parts = append(parts, fmt.Sprintf("[%s]", f.Uidx.SqlString()))
				}
			}
		default:
			// For other field types, all nodes implement SqlString()
			parts = append(parts, field.SqlString())
		}
	}

	// For simple column references, join with dots
	// For complex ones with array access, concatenate appropriately
	result := ""
	for i, part := range parts {
		if i == 0 {
			result = part
		} else if strings.HasPrefix(part, "[") {
			// Array access - no dot separator
			result += part
		} else {
			// Regular field access - use dot separator
			result += "." + part
		}
	}

	return result
}

func (c *ColumnRef) ExpressionType() string {
	return "ColumnRef"
}

func (c *ColumnRef) IsExpr() bool {
	return true
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
	Ctename          string           // Query name (never qualified) - parsenodes.h:1676
	Aliascolnames    *NodeList        // Optional list of column names - parsenodes.h:1678
	Ctematerialized  CTEMaterialized  // Is this an optimization fence? - parsenodes.h:1679
	Ctequery         Node             // The CTE's subquery - parsenodes.h:1681
	SearchClause     *CTESearchClause // SEARCH clause, if any - parsenodes.h:1682
	CycleClause      *CTECycleClause  // CYCLE clause, if any - parsenodes.h:1683
	Cterecursive     bool             // Is this a recursive CTE? - parsenodes.h:1687
	Cterefcount      int              // Number of RTEs referencing this CTE - parsenodes.h:1693
	Ctecolnames      *NodeList        // List of output column names - parsenodes.h:1696
	Ctecoltypes      []Oid            // OID list of output column type OIDs - parsenodes.h:1697
	Ctecoltypmods    []int32          // Integer list of output column typmods - parsenodes.h:1698
	Ctecolcollations []Oid            // OID list of column collation OIDs - parsenodes.h:1699
}

// CTEMaterialized represents CTE materialization settings.
// Ported from postgres/src/include/nodes/parsenodes.h:1636
type CTEMaterialized int

const (
	CTEMaterializeDefault CTEMaterialized = iota // No materialization clause - parsenodes.h:1638
	CTEMaterializeAlways                         // MATERIALIZED - parsenodes.h:1639
	CTEMaterializeNever                          // NOT MATERIALIZED - parsenodes.h:1640
)

// CTESearchClause represents a SEARCH clause in recursive CTEs.
// Ported from postgres/src/include/nodes/parsenodes.h:1643
type CTESearchClause struct {
	BaseNode
	SearchColList      *NodeList // List of columns to search - parsenodes.h:1646
	SearchBreadthFirst bool      // True for BREADTH FIRST, false for DEPTH FIRST - parsenodes.h:1647
	SearchSeqColumn    string    // Name of column to set search sequence - parsenodes.h:1648
}

// CTECycleClause represents a CYCLE clause in recursive CTEs.
// Ported from postgres/src/include/nodes/parsenodes.h:1652
type CTECycleClause struct {
	BaseNode
	CycleColList       *NodeList  // List of columns to check for cycles - parsenodes.h:1655
	CycleMarkColumn    string     // Name of column to mark cycles - parsenodes.h:1656
	CycleMarkValue     Expression // Value to set when cycle detected - parsenodes.h:1657
	CycleMarkDefault   Expression // Value to set when no cycle - parsenodes.h:1658
	CyclePathColumn    string     // Name of column to track path - parsenodes.h:1659
	CycleMarkType      Oid        // Common type of mark_value and mark_default - parsenodes.h:1662
	CycleMarkTypmod    int32      // Type modifier - parsenodes.h:1663
	CycleMarkCollation Oid        // Collation - parsenodes.h:1664
	CycleMarkNeop      Oid        // <> operator for type - parsenodes.h:1665
}

// NewCTESearchClause creates a new CTESearchClause node.
func NewCTESearchClause(searchColList *NodeList, breadthFirst bool, seqColumn string) *CTESearchClause {
	return &CTESearchClause{
		BaseNode:           BaseNode{Tag: T_CTESearchClause},
		SearchColList:      searchColList,
		SearchBreadthFirst: breadthFirst,
		SearchSeqColumn:    seqColumn,
	}
}

// SqlString returns the SQL representation of the CTESearchClause.
func (sc *CTESearchClause) SqlString() string {
	var direction string
	if sc.SearchBreadthFirst {
		direction = "BREADTH FIRST"
	} else {
		direction = "DEPTH FIRST"
	}

	colNames := make([]string, 0, sc.SearchColList.Len())
	for _, item := range sc.SearchColList.Items {
		if str, ok := item.(*String); ok {
			colNames = append(colNames, str.SVal)
		}
	}

	return fmt.Sprintf("SEARCH %s BY %s SET %s",
		direction,
		strings.Join(colNames, ", "),
		sc.SearchSeqColumn)
}

// NewCTECycleClause creates a new CTECycleClause node.
func NewCTECycleClause(cycleColList *NodeList, markColumn string, markValue, markDefault Expression, pathColumn string) *CTECycleClause {
	return &CTECycleClause{
		BaseNode:         BaseNode{Tag: T_CTECycleClause},
		CycleColList:     cycleColList,
		CycleMarkColumn:  markColumn,
		CycleMarkValue:   markValue,
		CycleMarkDefault: markDefault,
		CyclePathColumn:  pathColumn,
	}
}

// SqlString returns the SQL representation of the CTECycleClause.
func (cc *CTECycleClause) SqlString() string {
	colNames := make([]string, 0, cc.CycleColList.Len())
	for _, item := range cc.CycleColList.Items {
		if str, ok := item.(*String); ok {
			colNames = append(colNames, str.SVal)
		}
	}

	result := fmt.Sprintf("CYCLE %s SET %s",
		strings.Join(colNames, ", "),
		cc.CycleMarkColumn)

	if cc.CycleMarkValue != nil && cc.CycleMarkDefault != nil {
		result += fmt.Sprintf(" TO %s DEFAULT %s",
			PrintAExprConst(cc.CycleMarkValue),
			PrintAExprConst(cc.CycleMarkDefault))
	}

	result += fmt.Sprintf(" USING %s", cc.CyclePathColumn)

	return result
}

// NewCommonTableExpr creates a new CommonTableExpr node.
func NewCommonTableExpr(ctename string, ctequery Node) *CommonTableExpr {
	cte := &CommonTableExpr{
		BaseNode: BaseNode{Tag: T_CommonTableExpr},
		Ctename:  ctename,
		Ctequery: ctequery,
	}
	cte.SetLocation(-1)
	return cte
}

// NewRecursiveCommonTableExpr creates a new recursive CommonTableExpr node.
func NewRecursiveCommonTableExpr(ctename string, ctequery Node) *CommonTableExpr {
	cte := &CommonTableExpr{
		BaseNode:     BaseNode{Tag: T_CommonTableExpr},
		Ctename:      ctename,
		Ctequery:     ctequery,
		Cterecursive: true,
	}
	cte.SetLocation(-1)
	return cte
}

func (cte *CommonTableExpr) String() string {
	recursive := ""
	if cte.Cterecursive {
		recursive = " RECURSIVE"
	}
	return fmt.Sprintf("CommonTableExpr(%s%s)", cte.Ctename, recursive)
}

// SqlString returns the SQL representation of the CommonTableExpr
func (c *CommonTableExpr) SqlString() string {
	parts := []string{QuoteIdentifier(c.Ctename)}

	// Add column names if specified
	if c.Aliascolnames != nil && len(c.Aliascolnames.Items) > 0 {
		var cols []string
		for _, col := range c.Aliascolnames.Items {
			if str, ok := col.(*String); ok {
				cols = append(cols, str.SVal)
			}
		}
		parts[0] += fmt.Sprintf("(%s)", strings.Join(cols, ", "))
	}

	// Add the CTE query
	parts = append(parts, "AS")

	// Determine if materialized/not materialized
	switch c.Ctematerialized {
	case CTEMaterializeAlways:
		parts = append(parts, "MATERIALIZED")
	case CTEMaterializeNever:
		parts = append(parts, "NOT MATERIALIZED")
	}

	// Add the actual query (usually in parentheses)
	if c.Ctequery != nil {
		parts = append(parts, fmt.Sprintf("(%s)", c.Ctequery.SqlString()))
	}

	// Add SEARCH clause if present
	if c.SearchClause != nil {
		parts = append(parts, c.SearchClause.SqlString())
	}

	// Add CYCLE clause if present
	if c.CycleClause != nil {
		parts = append(parts, c.CycleClause.SqlString())
	}

	return strings.Join(parts, " ")
}

// Placeholder structs for other query execution nodes implemented in query_execution_nodes.go
// IntoClause placeholder removed - now implemented in expressions.go
// SetOperation represents the type of set operation
// Ported from postgres/src/include/nodes/parsenodes.h:2108-2114
type SetOperation int

const (
	SETOP_NONE      SetOperation = iota // No set operation
	SETOP_UNION                         // UNION
	SETOP_INTERSECT                     // INTERSECT
	SETOP_EXCEPT                        // EXCEPT
)

func (s SetOperation) String() string {
	switch s {
	case SETOP_NONE:
		return ""
	case SETOP_UNION:
		return "UNION"
	case SETOP_INTERSECT:
		return "INTERSECT"
	case SETOP_EXCEPT:
		return "EXCEPT"
	default:
		return fmt.Sprintf("SetOperation(%d)", int(s))
	}
}

// OnConflictClause represents ON CONFLICT clause for INSERT statements
// Ported from postgres/src/include/nodes/parsenodes.h:1621-1629
type OnConflictClause struct {
	BaseNode
	Action      OnConflictAction `json:"action"`      // DO NOTHING or UPDATE?
	Infer       *InferClause     `json:"infer"`       // Optional index inference clause
	TargetList  *NodeList        `json:"targetList"`  // The target list (of ResTarget)
	WhereClause Node             `json:"whereClause"` // Qualifications
}

func (n *OnConflictClause) node() {}

func (n *OnConflictClause) String() string {
	var parts []string
	parts = append(parts, "ON CONFLICT")

	if n.Infer != nil {
		parts = append(parts, n.Infer.String())
	}

	parts = append(parts, n.Action.String())

	if n.Action == ONCONFLICT_UPDATE && n.TargetList != nil && n.TargetList.Len() > 0 {
		var targets []string
		for _, item := range n.TargetList.Items {
			if target, ok := item.(*ResTarget); ok {
				targets = append(targets, target.String())
			}
		}
		if len(targets) > 0 {
			parts = append(parts, "SET", strings.Join(targets, ", "))
		}
	}

	if n.WhereClause != nil {
		parts = append(parts, "WHERE", n.WhereClause.String())
	}

	return strings.Join(parts, " ")
}

func (n *OnConflictClause) SqlString() string {
	var parts []string
	parts = append(parts, "ON CONFLICT")

	if n.Infer != nil {
		parts = append(parts, n.Infer.SqlString())
	}

	parts = append(parts, n.Action.SqlString())

	if n.Action == ONCONFLICT_UPDATE && n.TargetList != nil && n.TargetList.Len() > 0 {
		var targets []string
		for _, item := range n.TargetList.Items {
			if target, ok := item.(*ResTarget); ok {
				// Use ResTarget's SetClauseString method for proper formatting
				targets = append(targets, target.SetClauseString())
			}
		}
		if len(targets) > 0 {
			parts = append(parts, "SET", strings.Join(targets, ", "))
		}
	}

	if n.WhereClause != nil {
		parts = append(parts, "WHERE", n.WhereClause.SqlString())
	}

	return strings.Join(parts, " ")
}

// NewOnConflictClause creates a new OnConflictClause node
func NewOnConflictClause(action OnConflictAction) *OnConflictClause {
	return &OnConflictClause{
		BaseNode: BaseNode{Tag: T_OnConflictClause},
		Action:   action,
	}
}

// OverridingKind represents OVERRIDING clause options
// Ported from postgres/src/include/nodes/primnodes.h:25-30
type OverridingKind int

const (
	OVERRIDING_NOT_SET      OverridingKind = iota // No OVERRIDING clause
	OVERRIDING_USER_VALUE                         // OVERRIDING USER VALUE
	OVERRIDING_SYSTEM_VALUE                       // OVERRIDING SYSTEM VALUE
)

func (o OverridingKind) String() string {
	switch o {
	case OVERRIDING_NOT_SET:
		return ""
	case OVERRIDING_USER_VALUE:
		return "OVERRIDING USER VALUE"
	case OVERRIDING_SYSTEM_VALUE:
		return "OVERRIDING SYSTEM VALUE"
	default:
		return fmt.Sprintf("OverridingKind(%d)", int(o))
	}
}

func (o OverridingKind) SqlString() string {
	switch o {
	case OVERRIDING_NOT_SET:
		return ""
	case OVERRIDING_USER_VALUE:
		return "OVERRIDING USER VALUE"
	case OVERRIDING_SYSTEM_VALUE:
		return "OVERRIDING SYSTEM VALUE"
	default:
		return fmt.Sprintf("OverridingKind(%d)", int(o))
	}
}

// Note: Constraint is now defined in ddl_statements.go

// SqlString returns the SQL representation of CREATE TABLE statement
func (c *CreateStmt) SqlString() string {
	var parts []string

	parts = append(parts, "CREATE")

	// Add TEMPORARY if specified
	if c.Relation != nil && c.Relation.RelPersistence == 't' {
		parts = append(parts, "TEMPORARY")
	} else if c.Relation != nil && c.Relation.RelPersistence == 'u' {
		parts = append(parts, "UNLOGGED")
	}

	parts = append(parts, "TABLE")

	// Add IF NOT EXISTS if specified
	if c.IfNotExists {
		parts = append(parts, "IF NOT EXISTS")
	}

	// Add table name
	if c.Relation != nil {
		parts = append(parts, c.Relation.SqlString())
	}

	// Handle different table types with correct ordering
	isPartitionTable := c.PartBound != nil && c.InhRelations != nil && c.InhRelations.Len() > 0
	isDefaultPartitionTable := isPartitionTable && c.PartSpec != nil
	isTypedTable := c.OfTypename != nil

	if isPartitionTable {
		// For partition tables: PARTITION OF parent [constraints] FOR VALUES [PARTITION BY]
		var inhParts []string
		for _, item := range c.InhRelations.Items {
			if inh, ok := item.(*RangeVar); ok && inh != nil {
				inhParts = append(inhParts, inh.SqlString())
			}
		}
		parts = append(parts, "PARTITION OF", strings.Join(inhParts, ", "))

		// Add constraints for partition tables (but only if there are any)
		var columnParts []string
		if c.TableElts != nil {
			for _, col := range c.TableElts.Items {
				if col != nil {
					columnParts = append(columnParts, col.SqlString())
				}
			}
		}
		for _, constraint := range c.Constraints {
			if constraint != nil {
				columnParts = append(columnParts, constraint.SqlString())
			}
		}
		if len(columnParts) > 0 {
			parts = append(parts, "(", strings.Join(columnParts, ", "), ")")
		}

		// Add FOR VALUES clause
		parts = append(parts, c.PartBound.SqlString())

		// Add PARTITION BY for default partition tables
		if isDefaultPartitionTable {
			parts = append(parts, c.PartSpec.SqlString())
		}
	} else if isTypedTable {
		// For typed tables: OF typename
		parts = append(parts, "OF", c.OfTypename.SqlString())
	} else {
		// Regular table with columns and constraints
		var columnParts []string
		if c.TableElts != nil {
			for _, col := range c.TableElts.Items {
				if col != nil {
					columnParts = append(columnParts, col.SqlString())
				}
			}
		}

		// Add table-level constraints
		for _, constraint := range c.Constraints {
			if constraint != nil {
				columnParts = append(columnParts, constraint.SqlString())
			}
		}
		if len(columnParts) > 0 {
			parts = append(parts, "("+strings.Join(columnParts, ", ")+")")
		} else {
			parts = append(parts, "()")
		}

		// Add INHERITS clause for regular inheritance
		if c.InhRelations != nil && c.InhRelations.Len() > 0 {
			var inhParts []string
			for _, item := range c.InhRelations.Items {
				if inh, ok := item.(*RangeVar); ok && inh != nil {
					inhParts = append(inhParts, inh.SqlString())
				}
			}
			parts = append(parts, "INHERITS", "("+strings.Join(inhParts, ", ")+")")
		}

		// Add PARTITION BY clause for regular partitioned tables
		if c.PartSpec != nil {
			parts = append(parts, c.PartSpec.SqlString())
		}
	}

	// Add USING clause if specified (for table access method)
	if c.AccessMethod != "" {
		parts = append(parts, "USING", QuoteIdentifier(c.AccessMethod))
	}

	// Add WITH options if specified
	if c.Options != nil && len(c.Options.Items) > 0 {
		var optParts []string
		for _, opt := range c.Options.Items {
			if defElem, ok := opt.(*DefElem); ok {
				optParts = append(optParts, defElem.SqlString())
			}
		}
		if len(optParts) > 0 {
			parts = append(parts, "WITH", "("+strings.Join(optParts, ", ")+")")
		}
	}

	// Add ON COMMIT clause if specified
	if c.OnCommit != 0 {
		switch c.OnCommit {
		case 1: // ONCOMMIT_PRESERVE_ROWS
			parts = append(parts, "ON COMMIT PRESERVE ROWS")
		case 2: // ONCOMMIT_DELETE_ROWS
			parts = append(parts, "ON COMMIT DELETE ROWS")
		case 3: // ONCOMMIT_DROP
			parts = append(parts, "ON COMMIT DROP")
		}
	}

	// Add tablespace if specified
	if c.TableSpaceName != "" {
		parts = append(parts, "TABLESPACE", c.TableSpaceName)
	}

	return strings.Join(parts, " ")
}

// SqlString returns the SQL representation of DROP statement
func (d *DropStmt) SqlString() string {
	var parts []string

	parts = append(parts, "DROP")

	// Handle special cases first
	switch d.RemoveType {
	case OBJECT_CAST:
		return d.sqlStringForDropCast()
	case OBJECT_OPCLASS:
		return d.sqlStringForDropOpClass()
	case OBJECT_OPFAMILY:
		return d.sqlStringForDropOpFamily()
	case OBJECT_TRANSFORM:
		return d.sqlStringForDropTransform()
	case OBJECT_SUBSCRIPTION:
		return d.sqlStringForDropSubscription()
	case OBJECT_RULE, OBJECT_TRIGGER, OBJECT_POLICY:
		return d.sqlStringForDropOnTable()
	}

	// Add object type - always include it as all valid ObjectTypes should have string representations
	parts = append(parts, d.RemoveType.String())

	// Add CONCURRENTLY if specified for indexes
	if d.Concurrent && d.RemoveType == OBJECT_INDEX {
		parts = append(parts, "CONCURRENTLY")
	}

	// Add IF EXISTS if specified
	if d.MissingOk {
		parts = append(parts, "IF EXISTS")
	}

	// Add object names
	if d.Objects != nil && len(d.Objects.Items) > 0 {
		var nameParts []string
		for _, obj := range d.Objects.Items {
			if objWithArgs, ok := obj.(*ObjectWithArgs); ok {
				// Handle functions, aggregates, operators with arguments
				nameParts = append(nameParts, objWithArgs.SqlString())
			} else if nodeList, ok := obj.(*NodeList); ok {
				// Handle qualified names (schema.table)
				var qualParts []string
				for _, nameItem := range nodeList.Items {
					if strVal, ok := nameItem.(*String); ok {
						qualParts = append(qualParts, QuoteIdentifier(strVal.SVal))
					}
				}
				if len(qualParts) > 0 {
					nameParts = append(nameParts, strings.Join(qualParts, "."))
				}
			} else if strVal, ok := obj.(*String); ok {
				nameParts = append(nameParts, QuoteIdentifier(strVal.SVal))
			} else if typeName, ok := obj.(*TypeName); ok {
				// Handle type names (for DROP TYPE, etc.)
				nameParts = append(nameParts, typeName.SqlString())
			}
		}
		if len(nameParts) > 0 {
			parts = append(parts, strings.Join(nameParts, ", "))
		}
	}

	// Add CASCADE/RESTRICT behavior
	if d.Behavior == DropCascade {
		parts = append(parts, "CASCADE")
	}
	// Note: We don't output RESTRICT as it's the default behavior in PostgreSQL
	// Only CASCADE needs to be explicitly specified

	return strings.Join(parts, " ")
}

// sqlStringForDropCast handles DROP CAST (source_type AS target_type)
func (d *DropStmt) sqlStringForDropCast() string {
	var parts []string
	parts = append(parts, "DROP CAST")

	if d.MissingOk {
		parts = append(parts, "IF EXISTS")
	}

	// Objects should contain a single NodeList with [source_type, target_type]
	if d.Objects != nil && len(d.Objects.Items) > 0 {
		if typeList, ok := d.Objects.Items[0].(*NodeList); ok && len(typeList.Items) >= 2 {
			sourceType := typeList.Items[0]
			targetType := typeList.Items[1]

			parts = append(parts, "(")
			if srcTypeName, ok := sourceType.(*TypeName); ok {
				parts = append(parts, srcTypeName.SqlString())
			}
			parts = append(parts, "AS")
			if tgtTypeName, ok := targetType.(*TypeName); ok {
				parts = append(parts, tgtTypeName.SqlString())
			}
			parts = append(parts, ")")
		}
	}

	switch d.Behavior {
	case DropCascade:
		parts = append(parts, "CASCADE")
	case DropRestrict:
		parts = append(parts, "RESTRICT")
	}

	return strings.Join(parts, " ")
}

// sqlStringForDropOpClass handles DROP OPERATOR CLASS name USING access_method
func (d *DropStmt) sqlStringForDropOpClass() string {
	var parts []string
	parts = append(parts, "DROP OPERATOR CLASS")

	if d.MissingOk {
		parts = append(parts, "IF EXISTS")
	}

	// Objects should contain a single NodeList with [access_method, ...names]
	if d.Objects != nil && len(d.Objects.Items) > 0 {
		if objList, ok := d.Objects.Items[0].(*NodeList); ok {
			// First item is access method, rest are qualified name parts
			if len(objList.Items) > 0 {
				accessMethod := ""
				var nameParts []string

				// First item is access method
				if strVal, ok := objList.Items[0].(*String); ok {
					accessMethod = strVal.SVal
				}

				// Rest are name parts
				for i := 1; i < len(objList.Items); i++ {
					if strVal, ok := objList.Items[i].(*String); ok {
						nameParts = append(nameParts, strVal.SVal)
					}
				}

				// Add qualified name
				if len(nameParts) > 0 {
					parts = append(parts, strings.Join(nameParts, "."))
				}

				// Add USING access_method
				if accessMethod != "" {
					parts = append(parts, "USING", accessMethod)
				}
			}
		}
	}

	switch d.Behavior {
	case DropCascade:
		parts = append(parts, "CASCADE")
	case DropRestrict:
		parts = append(parts, "RESTRICT")
	}

	return strings.Join(parts, " ")
}

// sqlStringForDropOpFamily handles DROP OPERATOR FAMILY name USING access_method
func (d *DropStmt) sqlStringForDropOpFamily() string {
	var parts []string
	parts = append(parts, "DROP OPERATOR FAMILY")

	if d.MissingOk {
		parts = append(parts, "IF EXISTS")
	}

	// Same logic as DROP OPERATOR CLASS
	if d.Objects != nil && len(d.Objects.Items) > 0 {
		if objList, ok := d.Objects.Items[0].(*NodeList); ok {
			if len(objList.Items) > 0 {
				accessMethod := ""
				var nameParts []string

				// First item is access method
				if strVal, ok := objList.Items[0].(*String); ok {
					accessMethod = strVal.SVal
				}

				// Rest are name parts
				for i := 1; i < len(objList.Items); i++ {
					if strVal, ok := objList.Items[i].(*String); ok {
						nameParts = append(nameParts, strVal.SVal)
					}
				}

				// Add qualified name
				if len(nameParts) > 0 {
					parts = append(parts, strings.Join(nameParts, "."))
				}

				// Add USING access_method
				if accessMethod != "" {
					parts = append(parts, "USING", accessMethod)
				}
			}
		}
	}

	switch d.Behavior {
	case DropCascade:
		parts = append(parts, "CASCADE")
	case DropRestrict:
		parts = append(parts, "RESTRICT")
	}

	return strings.Join(parts, " ")
}

// sqlStringForDropTransform handles DROP TRANSFORM FOR type LANGUAGE lang
func (d *DropStmt) sqlStringForDropTransform() string {
	var parts []string
	parts = append(parts, "DROP TRANSFORM")

	if d.MissingOk {
		parts = append(parts, "IF EXISTS")
	}

	// Objects should contain a single NodeList with [type, language_name]
	if d.Objects != nil && len(d.Objects.Items) > 0 {
		if typeList, ok := d.Objects.Items[0].(*NodeList); ok && len(typeList.Items) >= 2 {
			typeName := typeList.Items[0]
			langName := typeList.Items[1]

			parts = append(parts, "FOR")
			if typNode, ok := typeName.(*TypeName); ok {
				parts = append(parts, typNode.SqlString())
			}
			parts = append(parts, "LANGUAGE")
			if strVal, ok := langName.(*String); ok {
				parts = append(parts, strVal.SVal)
			}
		}
	}

	switch d.Behavior {
	case DropCascade:
		parts = append(parts, "CASCADE")
	case DropRestrict:
		parts = append(parts, "RESTRICT")
	}

	return strings.Join(parts, " ")
}

// sqlStringForDropSubscription handles DROP SUBSCRIPTION name
func (d *DropStmt) sqlStringForDropSubscription() string {
	var parts []string
	parts = append(parts, "DROP SUBSCRIPTION")

	if d.MissingOk {
		parts = append(parts, "IF EXISTS")
	}

	// Objects should contain a single NodeList with subscription name
	if d.Objects != nil && len(d.Objects.Items) > 0 {
		if nameList, ok := d.Objects.Items[0].(*NodeList); ok && len(nameList.Items) > 0 {
			if strVal, ok := nameList.Items[0].(*String); ok {
				parts = append(parts, strVal.SVal)
			}
		}
	}

	switch d.Behavior {
	case DropCascade:
		parts = append(parts, "CASCADE")
	case DropRestrict:
		parts = append(parts, "RESTRICT")
	}

	return strings.Join(parts, " ")
}

// sqlStringForDropOnTable handles DROP RULE/TRIGGER/POLICY name ON table
func (d *DropStmt) sqlStringForDropOnTable() string {
	var parts []string
	parts = append(parts, "DROP")

	// Add object type
	if d.RemoveType != 0 {
		parts = append(parts, d.RemoveType.String())
	}

	// Add IF EXISTS if specified
	if d.MissingOk {
		parts = append(parts, "IF EXISTS")
	}

	// For RULE/TRIGGER/POLICY, Objects contains: [table_name, object_name]
	// We need to format as: DROP TYPE object_name ON table_name
	if d.Objects != nil && len(d.Objects.Items) >= 2 {
		// The first item is the table, the second is the rule/trigger/policy name
		tableName := ""
		objectName := ""

		// First item should be the table (from any_name)
		if nodeList, ok := d.Objects.Items[0].(*NodeList); ok {
			var qualParts []string
			for _, nameItem := range nodeList.Items {
				if strVal, ok := nameItem.(*String); ok {
					qualParts = append(qualParts, strVal.SVal)
				}
			}
			if len(qualParts) > 0 {
				tableName = strings.Join(qualParts, ".")
			}
		} else if strVal, ok := d.Objects.Items[0].(*String); ok {
			tableName = strVal.SVal
		}

		// Second item should be the object name
		if strVal, ok := d.Objects.Items[1].(*String); ok {
			objectName = strVal.SVal
		}

		if objectName != "" {
			parts = append(parts, QuoteIdentifier(objectName))
		}
		if tableName != "" {
			parts = append(parts, "ON", tableName)
		}
	}

	// Add CASCADE/RESTRICT behavior
	if d.Behavior == DropCascade {
		parts = append(parts, "CASCADE")
	}
	// Note: We don't output RESTRICT as it's the default behavior in PostgreSQL

	return strings.Join(parts, " ")
}
