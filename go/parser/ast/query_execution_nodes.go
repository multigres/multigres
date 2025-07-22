// Package ast provides PostgreSQL AST query execution node definitions.
// These nodes are essential for basic SQL query functionality including SELECT operations,
// JOIN processing, subquery support, and modern SQL features like CTEs and window functions.
// Ported from postgres/src/include/nodes/parsenodes.h and primnodes.h
package ast

import (
	"fmt"
)

// ==============================================================================
// QUERY EXECUTION NODES - Essential PostgreSQL Query Processing
// ==============================================================================

// TargetEntry represents a target list entry in a query (SELECT clause item).
// This is critical for query execution as it represents individual columns or expressions
// in the target list of a query.
// Ported from postgres/src/include/nodes/primnodes.h:1726-1745
type TargetEntry struct {
	BaseExpr
	Expr           Expression  // The expression to compute - primnodes.h:1727
	Resno          AttrNumber  // Attribute number (>= 1) - primnodes.h:1728
	Resname        string      // Name of the column (could be NULL) - primnodes.h:1729
	Ressortgroupref Index      // Nonzero if referenced by ORDER BY/GROUP BY/HAVING - primnodes.h:1730
	Resorigtbl      Oid        // OID of column's source table - primnodes.h:1731
	Resorigcol      AttrNumber // Column's number in source table - primnodes.h:1732
	Resjunk         bool       // Set to true to eliminate the attribute from final target list - primnodes.h:1733
}

// NewTargetEntry creates a new TargetEntry node.
func NewTargetEntry(expr Expression, resno AttrNumber, resname string) *TargetEntry {
	return &TargetEntry{
		BaseExpr: BaseExpr{BaseNode: BaseNode{Tag: T_TargetEntry}},
		Expr:     expr,
		Resno:    resno,
		Resname:  resname,
	}
}

// NewJunkTargetEntry creates a new junk TargetEntry (for internal use).
func NewJunkTargetEntry(expr Expression, resno AttrNumber) *TargetEntry {
	return &TargetEntry{
		BaseExpr: BaseExpr{BaseNode: BaseNode{Tag: T_TargetEntry}},
		Expr:     expr,
		Resno:    resno,
		Resjunk:  true,
	}
}

func (te *TargetEntry) ExpressionType() string {
	return "TargetEntry"
}

func (te *TargetEntry) String() string {
	junkStr := ""
	if te.Resjunk {
		junkStr = " (junk)"
	}
	return fmt.Sprintf("TargetEntry(%s as %s)%s", te.Expr, te.Resname, junkStr)
}

// FromExpr represents a FROM clause.
// This node represents the FROM clause of a query, including all table references
// and their qualifying conditions.
// Ported from postgres/src/include/nodes/primnodes.h:1877-1884
type FromExpr struct {
	BaseExpr
	Fromlist []Node      // List of join subtrees - primnodes.h:1878
	Quals    Expression  // Qualifiers on join, if any - primnodes.h:1879
}

// NewFromExpr creates a new FromExpr node.
func NewFromExpr(fromlist []Node, quals Expression) *FromExpr {
	return &FromExpr{
		BaseExpr: BaseExpr{BaseNode: BaseNode{Tag: T_FromExpr}},
		Fromlist: fromlist,
		Quals:    quals,
	}
}

func (fe *FromExpr) ExpressionType() string {
	return "FromExpr"
}

func (fe *FromExpr) String() string {
	return fmt.Sprintf("FromExpr(tables:%d, quals:%v)", len(fe.Fromlist), fe.Quals != nil)
}

// JoinType represents the type of join operation.
// Ported from postgres/src/include/nodes/primnodes.h:1790-1801
type JoinType int

const (
	JOIN_INNER     JoinType = iota // Inner join - primnodes.h:1791
	JOIN_LEFT                      // Left outer join - primnodes.h:1792
	JOIN_FULL                      // Full outer join - primnodes.h:1793
	JOIN_RIGHT                     // Right outer join - primnodes.h:1794
	JOIN_SEMI                      // Semi join - primnodes.h:1795
	JOIN_ANTI                      // Anti join - primnodes.h:1796
	JOIN_RIGHT_ANTI                // Right anti join - primnodes.h:1797
	JOIN_UNIQUE_OUTER              // LHS path must be made unique - primnodes.h:1798
	JOIN_UNIQUE_INNER              // RHS path must be made unique - primnodes.h:1799
)

// JoinExpr represents a JOIN operation.
// This represents all types of JOIN operations (INNER, LEFT, RIGHT, FULL, etc.)
// and is core to SQL functionality.
// Ported from postgres/src/include/nodes/primnodes.h:1803-1827
type JoinExpr struct {
	BaseExpr
	Jointype    JoinType   // Type of join - primnodes.h:1804
	IsNatural   bool       // Natural join? Will need to shape the join - primnodes.h:1805
	Larg        Node       // Left subtree - primnodes.h:1806
	Rarg        Node       // Right subtree - primnodes.h:1807
	UsingClause []Node     // USING clause, if any (list of String nodes) - primnodes.h:1808
	JoinUsing   []Node     // Join condition if using natural/using join - primnodes.h:1809
	Quals       Expression // Qualifiers on join, if any - primnodes.h:1810
	Alias       *Alias     // User-written alias clause, if any - primnodes.h:1811
	Rtindex     Index      // RT index assigned for join, or 0 - primnodes.h:1812
}

// NewJoinExpr creates a new JoinExpr node.
func NewJoinExpr(jointype JoinType, larg, rarg Node, quals Expression) *JoinExpr {
	return &JoinExpr{
		BaseExpr: BaseExpr{BaseNode: BaseNode{Tag: T_JoinExpr}},
		Jointype: jointype,
		Larg:     larg,
		Rarg:     rarg,
		Quals:    quals,
	}
}

// NewNaturalJoinExpr creates a new natural JOIN expression.
func NewNaturalJoinExpr(jointype JoinType, larg, rarg Node) *JoinExpr {
	return &JoinExpr{
		BaseExpr:  BaseExpr{BaseNode: BaseNode{Tag: T_JoinExpr}},
		Jointype:  jointype,
		IsNatural: true,
		Larg:      larg,
		Rarg:      rarg,
	}
}

// NewUsingJoinExpr creates a new USING JOIN expression.
func NewUsingJoinExpr(jointype JoinType, larg, rarg Node, usingClause []Node) *JoinExpr {
	return &JoinExpr{
		BaseExpr:    BaseExpr{BaseNode: BaseNode{Tag: T_JoinExpr}},
		Jointype:    jointype,
		Larg:        larg,
		Rarg:        rarg,
		UsingClause: usingClause,
	}
}

func (je *JoinExpr) ExpressionType() string {
	return "JoinExpr"
}

func (je *JoinExpr) String() string {
	joinTypes := map[JoinType]string{
		JOIN_INNER: "INNER", JOIN_LEFT: "LEFT", JOIN_RIGHT: "RIGHT", JOIN_FULL: "FULL",
		JOIN_SEMI: "SEMI", JOIN_ANTI: "ANTI",
	}
	joinTypeStr := joinTypes[je.Jointype]
	if joinTypeStr == "" {
		joinTypeStr = fmt.Sprintf("JOIN_%d", int(je.Jointype))
	}
	
	natural := ""
	if je.IsNatural {
		natural = " NATURAL"
	}
	
	return fmt.Sprintf("JoinExpr(%s%s JOIN)", joinTypeStr, natural)
}

// Using existing SubLinkType from expressions.go

// SubPlan represents the execution-level representation of a subquery.
// This is the internal representation used during query execution, after
// the SubLink has been processed by the planner.
// Ported from postgres/src/include/nodes/primnodes.h:1067-1105
type SubPlan struct {
	BaseExpr
	SubLinkType  SubLinkType // Type of sublink - primnodes.h:1068
	Testexpr     Expression  // OpExpr or RowCompareExpr expression tree - primnodes.h:1069
	ParamIds     []int       // IDs of Params embedded in the above - primnodes.h:1070
	PlanId       int         // Index (from 1) in PlannedStmt.subplans - primnodes.h:1071
	PlanName     string      // A name assigned during planning - primnodes.h:1072
	FirstColType Oid         // Type of first column of subplan result - primnodes.h:1073
	FirstColTypmod int32     // Typmod of first column of subplan result - primnodes.h:1074
	FirstColCollation Oid    // Collation of first column of subplan result - primnodes.h:1075
	UseHashTable bool        // Use hashtable if TRUE, otherwise nested-loop - primnodes.h:1076
	UnknownEqFalse bool       // True if the comparison can never return NULL - primnodes.h:1077
	ParallelSafe bool        // Is subplan parallel-safe? - primnodes.h:1078
	SetParam     []int       // initplan subqueries have to set these Params for parent plan - primnodes.h:1079
	ParParam     []int       // indices of input Params from parent plan - primnodes.h:1080
	Args         []Expression// exprs to pass as parParam values - primnodes.h:1081
	Startup_cost float64     // one-time setup cost - primnodes.h:1082
	Per_call_cost float64    // cost for each subplan evaluation - primnodes.h:1083
}

// NewSubPlan creates a new SubPlan node.
func NewSubPlan(subLinkType SubLinkType, planId int, planName string) *SubPlan {
	return &SubPlan{
		BaseExpr:    BaseExpr{BaseNode: BaseNode{Tag: T_SubPlan}},
		SubLinkType: subLinkType,
		PlanId:      planId,
		PlanName:    planName,
	}
}

func (sp *SubPlan) ExpressionType() string {
	return "SubPlan"
}

func (sp *SubPlan) String() string {
	subTypes := map[SubLinkType]string{
		EXISTS_SUBLINK: "EXISTS", ALL_SUBLINK: "ALL", ANY_SUBLINK: "ANY",
		EXPR_SUBLINK: "EXPR", ARRAY_SUBLINK: "ARRAY", CTE_SUBLINK: "CTE",
	}
	subTypeStr := subTypes[sp.SubLinkType]
	if subTypeStr == "" {
		subTypeStr = fmt.Sprintf("SUBLINK_%d", int(sp.SubLinkType))
	}
	
	return fmt.Sprintf("SubPlan(%s, plan=%d, name=%s)", subTypeStr, sp.PlanId, sp.PlanName)
}

// AlternativeSubPlan represents alternative execution strategies for a subplan.
// This allows the executor to choose the most efficient strategy at runtime.
// Ported from postgres/src/include/nodes/primnodes.h:1113-1123
type AlternativeSubPlan struct {
	BaseExpr
	Subplans []Expression // SubPlan(s) with equivalent results - primnodes.h:1114
}

// NewAlternativeSubPlan creates a new AlternativeSubPlan node.
func NewAlternativeSubPlan(subplans []Expression) *AlternativeSubPlan {
	return &AlternativeSubPlan{
		BaseExpr: BaseExpr{BaseNode: BaseNode{Tag: T_AlternativeSubPlan}},
		Subplans: subplans,
	}
}

func (asp *AlternativeSubPlan) ExpressionType() string {
	return "AlternativeSubPlan"
}

func (asp *AlternativeSubPlan) String() string {
	return fmt.Sprintf("AlternativeSubPlan(%d alternatives)", len(asp.Subplans))
}

// CommonTableExpr and related structures are now implemented in statements.go

// WindowClause represents a window function clause.
// Window functions are essential for analytical queries and are becoming
// increasingly common in modern SQL usage.
// Ported from postgres/src/include/nodes/parsenodes.h:571-583
type WindowClause struct {
	BaseNode
	Name              string     // Window name (NULL in an OVER clause) - parsenodes.h:572
	Refname           string     // Referenced window name, if any - parsenodes.h:573
	PartitionClause   []Node     // PARTITION BY expression list - parsenodes.h:574
	OrderClause       []Node     // ORDER BY (list of SortBy) - parsenodes.h:575
	FrameOptions      int        // Frame_clause options, see WindowDef - parsenodes.h:576
	StartOffset       Node       // Expression for starting bound, if any - parsenodes.h:577
	EndOffset         Node       // Expression for ending bound, if any - parsenodes.h:578
	StartInRangeFunc  Oid        // In_range function for startOffset - parsenodes.h:579
	EndInRangeFunc    Oid        // In_range function for endOffset - parsenodes.h:580
	InRangeColl       Oid        // Collation for in_range tests - parsenodes.h:581
	InRangeAsc        bool       // True if ASC in the ORDER BY clause - parsenodes.h:582
	InRangeNullsFirst bool       // True if NULLS FIRST in ORDER BY clause - parsenodes.h:583
	WinRef            Index      // winref assigned by windowClausesProcessor - parsenodes.h:584
	CopiedOrder       bool       // Did we copy orderClause from refname? - parsenodes.h:585
}

// NewWindowClause creates a new WindowClause node.
func NewWindowClause(name string) *WindowClause {
	return &WindowClause{
		BaseNode: BaseNode{Tag: T_WindowClause},
		Name:     name,
	}
}

// NewPartitionedWindowClause creates a new WindowClause with PARTITION BY.
func NewPartitionedWindowClause(name string, partitionClause []Node) *WindowClause {
	return &WindowClause{
		BaseNode:        BaseNode{Tag: T_WindowClause},
		Name:            name,
		PartitionClause: partitionClause,
	}
}

// NewOrderedWindowClause creates a new WindowClause with ORDER BY.
func NewOrderedWindowClause(name string, orderClause []Node) *WindowClause {
	return &WindowClause{
		BaseNode:    BaseNode{Tag: T_WindowClause},
		Name:        name,
		OrderClause: orderClause,
	}
}

func (wc *WindowClause) String() string {
	parts := []string{}
	if wc.Name != "" {
		parts = append(parts, fmt.Sprintf("name=%s", wc.Name))
	}
	if wc.Refname != "" {
		parts = append(parts, fmt.Sprintf("ref=%s", wc.Refname))
	}
	if len(wc.PartitionClause) > 0 {
		parts = append(parts, fmt.Sprintf("partition=%d", len(wc.PartitionClause)))
	}
	if len(wc.OrderClause) > 0 {
		parts = append(parts, fmt.Sprintf("order=%d", len(wc.OrderClause)))
	}
	
	detail := ""
	if len(parts) > 0 {
		detail = fmt.Sprintf(" (%s)", fmt.Sprintf("%v", parts))
	}
	
	return fmt.Sprintf("WindowClause%s", detail)
}

// SortGroupClause represents ORDER BY and GROUP BY clauses.
// This structure is used for both ORDER BY and GROUP BY operations
// and is fundamental to SQL query processing.
// Ported from postgres/src/include/nodes/parsenodes.h:1563-1576
type SortGroupClause struct {
	BaseNode
	TleSortGroupRef Index // Reference into targetlist - parsenodes.h:1564
	Eqop            Oid   // The equality operator ('=' op) - parsenodes.h:1565
	Sortop          Oid   // The ordering operator ('<' op), or InvalidOid - parsenodes.h:1566
	NullsFirst      bool  // Do NULLs come before normal values? - parsenodes.h:1567
	Hashable        bool  // Can eqop be implemented by hashing? - parsenodes.h:1568
}

// NewSortGroupClause creates a new SortGroupClause node.
func NewSortGroupClause(tleSortGroupRef Index, eqop, sortop Oid) *SortGroupClause {
	return &SortGroupClause{
		BaseNode:        BaseNode{Tag: T_SortGroupClause},
		TleSortGroupRef: tleSortGroupRef,
		Eqop:            eqop,
		Sortop:          sortop,
	}
}

// NewSortGroupClauseNullsFirst creates a new SortGroupClause with NULLS FIRST.
func NewSortGroupClauseNullsFirst(tleSortGroupRef Index, eqop, sortop Oid) *SortGroupClause {
	return &SortGroupClause{
		BaseNode:        BaseNode{Tag: T_SortGroupClause},
		TleSortGroupRef: tleSortGroupRef,
		Eqop:            eqop,
		Sortop:          sortop,
		NullsFirst:      true,
	}
}

func (sgc *SortGroupClause) String() string {
	nulls := ""
	if sgc.NullsFirst {
		nulls = " NULLS FIRST"
	}
	return fmt.Sprintf("SortGroupClause(ref=%d%s)", sgc.TleSortGroupRef, nulls)
}

// RowMarkType represents the type of row marking for SELECT FOR UPDATE/SHARE.
// Ported from postgres/src/include/nodes/parsenodes.h:1578-1583  
type RowMarkType int

const (
	ROW_MARK_EXCLUSIVE    RowMarkType = iota // FOR UPDATE - parsenodes.h:1579
	ROW_MARK_NOKEYEXCLUSIVE                  // FOR NO KEY UPDATE - parsenodes.h:1580
	ROW_MARK_SHARE                           // FOR SHARE - parsenodes.h:1581
	ROW_MARK_KEYSHARE                        // FOR KEY SHARE - parsenodes.h:1582
	ROW_MARK_REFERENCE                       // FOR REFERENCE (MySQL compat) - parsenodes.h:1583
	ROW_MARK_COPY                            // FOR COPY (internal) - parsenodes.h:1584
)

// LockWaitPolicy represents lock wait policies.
// Ported from postgres/src/include/nodes/parsenodes.h:1585-1589
type LockWaitPolicy int

const (
	LockWaitBlock  LockWaitPolicy = iota // Block on locks - parsenodes.h:1586
	LockWaitSkip                         // SKIP LOCKED - parsenodes.h:1587  
	LockWaitError                        // NOWAIT - parsenodes.h:1588
)

// RowMarkClause represents FOR UPDATE/SHARE clauses.
// This supports row-level locking which is essential for concurrency control.
// Ported from postgres/src/include/nodes/parsenodes.h:1585-1595
type RowMarkClause struct {
	BaseNode
	Rti        Index          // Range table index of target relation - parsenodes.h:1586
	Strength   RowMarkType    // Row mark type - parsenodes.h:1587
	WaitPolicy LockWaitPolicy // NOWAIT and SKIP LOCKED - parsenodes.h:1588
	PushedDown bool           // Pushed down from higher query level? - parsenodes.h:1589
}

// NewRowMarkClause creates a new RowMarkClause node.
func NewRowMarkClause(rti Index, strength RowMarkType) *RowMarkClause {
	return &RowMarkClause{
		BaseNode: BaseNode{Tag: T_RowMarkClause},
		Rti:      rti,
		Strength: strength,
	}
}

// NewRowMarkClauseWithPolicy creates a new RowMarkClause with wait policy.
func NewRowMarkClauseWithPolicy(rti Index, strength RowMarkType, waitPolicy LockWaitPolicy) *RowMarkClause {
	return &RowMarkClause{
		BaseNode:   BaseNode{Tag: T_RowMarkClause},
		Rti:        rti,
		Strength:   strength,
		WaitPolicy: waitPolicy,
	}
}

func (rmc *RowMarkClause) String() string {
	strengths := map[RowMarkType]string{
		ROW_MARK_EXCLUSIVE: "UPDATE", ROW_MARK_NOKEYEXCLUSIVE: "NO KEY UPDATE",
		ROW_MARK_SHARE: "SHARE", ROW_MARK_KEYSHARE: "KEY SHARE",
	}
	strengthStr := strengths[rmc.Strength]
	if strengthStr == "" {
		strengthStr = fmt.Sprintf("MARK_%d", int(rmc.Strength))
	}
	
	policies := map[LockWaitPolicy]string{
		LockWaitSkip: " SKIP LOCKED", LockWaitError: " NOWAIT",
	}
	policyStr := policies[rmc.WaitPolicy]
	
	return fmt.Sprintf("RowMarkClause(FOR %s%s)", strengthStr, policyStr)
}

// OnConflictAction represents the action to take on conflicts.
// Ported from postgres/src/include/nodes/parsenodes.h:2042-2046
type OnConflictAction int

const (
	ONCONFLICT_NONE   OnConflictAction = iota // No ON CONFLICT clause - parsenodes.h:2043
	ONCONFLICT_NOTHING                        // ON CONFLICT DO NOTHING - parsenodes.h:2044
	ONCONFLICT_UPDATE                         // ON CONFLICT DO UPDATE - parsenodes.h:2045
)

// OnConflictExpr represents INSERT ... ON CONFLICT expressions.
// This is an important PostgreSQL-specific feature for handling conflicts
// during INSERT operations (UPSERT functionality).
// Ported from postgres/src/include/nodes/primnodes.h:2046-2061
type OnConflictExpr struct {
	BaseExpr
	Action       OnConflictAction // The action to take - primnodes.h:2047
	ArbiterElems []Node           // Unique index arbiter list (of InferenceElem's) - primnodes.h:2048
	ArbiterWhere Expression       // Unique index arbiter WHERE clause - primnodes.h:2049
	Constraint   Oid              // Constraint to infer from - primnodes.h:2050
	OnConflictSet []Node          // List of ON CONFLICT SET targets - primnodes.h:2051
	OnConflictWhere Expression    // WHERE clause for ON CONFLICT UPDATE - primnodes.h:2052
	ExclRelIndex int              // RT index of the EXCLUDED pseudo-relation - primnodes.h:2053
	ExclRelTlist []Node           // Tlist of the EXCLUDED pseudo-relation - primnodes.h:2054
}

// NewOnConflictExpr creates a new OnConflictExpr node.
func NewOnConflictExpr(action OnConflictAction) *OnConflictExpr {
	return &OnConflictExpr{
		BaseExpr: BaseExpr{BaseNode: BaseNode{Tag: T_OnConflictExpr}},
		Action:   action,
	}
}

// NewOnConflictDoNothing creates a new ON CONFLICT DO NOTHING expression.
func NewOnConflictDoNothing() *OnConflictExpr {
	return &OnConflictExpr{
		BaseExpr: BaseExpr{BaseNode: BaseNode{Tag: T_OnConflictExpr}},
		Action:   ONCONFLICT_NOTHING,
	}
}

// NewOnConflictDoUpdate creates a new ON CONFLICT DO UPDATE expression.
func NewOnConflictDoUpdate(onConflictSet []Node) *OnConflictExpr {
	return &OnConflictExpr{
		BaseExpr:      BaseExpr{BaseNode: BaseNode{Tag: T_OnConflictExpr}},
		Action:        ONCONFLICT_UPDATE,
		OnConflictSet: onConflictSet,
	}
}

func (oce *OnConflictExpr) ExpressionType() string {
	return "OnConflictExpr"
}

func (oce *OnConflictExpr) String() string {
	actions := map[OnConflictAction]string{
		ONCONFLICT_NOTHING: "DO NOTHING", ONCONFLICT_UPDATE: "DO UPDATE",
	}
	actionStr := actions[oce.Action]
	if actionStr == "" {
		actionStr = fmt.Sprintf("ACTION_%d", int(oce.Action))
	}
	
	return fmt.Sprintf("OnConflictExpr(%s)", actionStr)
}