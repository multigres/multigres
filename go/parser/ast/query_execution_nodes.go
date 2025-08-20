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
// Ported from postgres/src/include/nodes/primnodes.h:2186
type TargetEntry struct {
	BaseExpr
	Expr            Expression // The expression to compute - primnodes.h:2190
	Resno           AttrNumber // Attribute number (>= 1) - primnodes.h:2192
	Resname         string     // Name of the column (could be NULL) - primnodes.h:2194
	Ressortgroupref Index      // Nonzero if referenced by ORDER BY/GROUP BY/HAVING - primnodes.h:2196
	Resorigtbl      Oid        // OID of column's source table - primnodes.h:2198
	Resorigcol      AttrNumber // Column's number in source table - primnodes.h:2200
	Resjunk         bool       // Set to true to eliminate the attribute from final target list - primnodes.h:2202
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
// Ported from postgres/src/include/nodes/primnodes.h:2305
type FromExpr struct {
	BaseExpr
	Fromlist *NodeList  // List of join subtrees - primnodes.h:2308
	Quals    Expression // Qualifiers on join, if any - primnodes.h:2309
}

// NewFromExpr creates a new FromExpr node.
func NewFromExpr(fromlist *NodeList, quals Expression) *FromExpr {
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
	tableCount := 0
	if fe.Fromlist != nil {
		tableCount = len(fe.Fromlist.Items)
	}
	return fmt.Sprintf("FromExpr(tables:%d, quals:%v)", tableCount, fe.Quals != nil)
}

// JoinType represents the type of join operation.
// Ported from postgres/src/include/nodes/nodes.h:287
type JoinType int

const (
	JOIN_INNER        JoinType = iota // Inner join - nodes.h:293
	JOIN_LEFT                         // Left outer join - nodes.h:294
	JOIN_FULL                         // Full outer join - nodes.h:295
	JOIN_RIGHT                        // Right outer join - nodes.h:296
	JOIN_SEMI                         // Semi join - nodes.h:302
	JOIN_ANTI                         // Anti join - nodes.h:303
	JOIN_RIGHT_ANTI                   // Right anti join - nodes.h:304
	JOIN_UNIQUE_OUTER                 // LHS path must be made unique - nodes.h:308
	JOIN_UNIQUE_INNER                 // RHS path must be made unique - nodes.h:309
)

// JoinExpr represents a JOIN operation.
// This represents all types of JOIN operations (INNER, LEFT, RIGHT, FULL, etc.)
// and is core to SQL functionality.
// Ported from postgres/src/include/nodes/primnodes.h:2277
type JoinExpr struct {
	BaseExpr
	Jointype       JoinType   // Type of join - primnodes.h:2280
	IsNatural      bool       // Natural join? Will need to shape the join - primnodes.h:2281
	Larg           Node       // Left subtree - primnodes.h:2282
	Rarg           Node       // Right subtree - primnodes.h:2283
	UsingClause    *NodeList  // USING clause, if any (list of String nodes) - primnodes.h:2285
	JoinUsingAlias *Alias     // JOIN USING alias clause - primnodes.h:2287
	Quals          Expression // Qualifiers on join, if any - primnodes.h:2289
	Alias          *Alias     // User-written alias clause, if any - primnodes.h:2291
	Rtindex        Index      // RT index assigned for join, or 0 - primnodes.h:2293
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
func NewUsingJoinExpr(jointype JoinType, larg, rarg Node, usingClause *NodeList) *JoinExpr {
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
// Ported from postgres/src/include/nodes/primnodes.h:1059
type SubPlan struct {
	BaseExpr
	SubLinkType       SubLinkType  // Type of sublink - primnodes.h:1065
	Testexpr          Expression   // OpExpr or RowCompareExpr expression tree - primnodes.h:1067
	ParamIds          []int        // IDs of Params embedded in the above - primnodes.h:1068
	PlanId            int          // Index (from 1) in PlannedStmt.subplans - primnodes.h:1070
	PlanName          string       // A name assigned during planning - primnodes.h:1072
	FirstColType      Oid          // Type of first column of subplan result - primnodes.h:1074
	FirstColTypmod    int32        // Typmod of first column of subplan result - primnodes.h:1075
	FirstColCollation Oid          // Collation of first column of subplan result - primnodes.h:1076
	UseHashTable      bool         // Use hashtable if TRUE, otherwise nested-loop - primnodes.h:1079
	UnknownEqFalse    bool         // True if the comparison can never return NULL - primnodes.h:1081
	ParallelSafe      bool         // Is subplan parallel-safe? - primnodes.h:1084
	SetParam          []int        // initplan subqueries have to set these Params for parent plan - primnodes.h:1087
	ParParam          []int        // indices of input Params from parent plan - primnodes.h:1088
	Args              []Expression // exprs to pass as parParam values - primnodes.h:1089
	StartupCost      float64      // one-time setup cost - primnodes.h:1090
	PerCallCost     float64      // cost for each subplan evaluation - primnodes.h:1091
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
// Ported from postgres/src/include/nodes/primnodes.h:1108
type AlternativeSubPlan struct {
	BaseExpr
	Subplans []Expression // SubPlan(s) with equivalent results - primnodes.h:1113
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
// Ported from postgres/src/include/nodes/parsenodes.h:1536
type WindowClause struct {
	BaseNode
	Name              string // Window name (NULL in an OVER clause) - parsenodes.h:1540
	Refname           string // Referenced window name, if any - parsenodes.h:1542
	PartitionClause   *NodeList // PARTITION BY expression list - parsenodes.h:1543
	OrderClause       *NodeList // ORDER BY (list of SortBy) - parsenodes.h:1545
	FrameOptions      int    // Frame_clause options, see WindowDef - parsenodes.h:1546
	StartOffset       Node   // Expression for starting bound, if any - parsenodes.h:1547
	EndOffset         Node   // Expression for ending bound, if any - parsenodes.h:1548
	StartInRangeFunc  Oid    // In_range function for startOffset - parsenodes.h:1550
	EndInRangeFunc    Oid    // In_range function for endOffset - parsenodes.h:1552
	InRangeColl       Oid    // Collation for in_range tests - parsenodes.h:1554
	InRangeAsc        bool   // True if ASC in the ORDER BY clause - parsenodes.h:1556
	InRangeNullsFirst bool   // True if NULLS FIRST in ORDER BY clause - parsenodes.h:1557
	WinRef            Index  // winref assigned by windowClausesProcessor - parsenodes.h:1559
	CopiedOrder       bool   // Did we copy orderClause from refname? - parsenodes.h:1560
}

// NewWindowClause creates a new WindowClause node.
func NewWindowClause(name string) *WindowClause {
	return &WindowClause{
		BaseNode: BaseNode{Tag: T_WindowClause},
		Name:     name,
	}
}

// NewPartitionedWindowClause creates a new WindowClause with PARTITION BY.
func NewPartitionedWindowClause(name string, partitionClause *NodeList) *WindowClause {
	return &WindowClause{
		BaseNode:        BaseNode{Tag: T_WindowClause},
		Name:            name,
		PartitionClause: partitionClause,
	}
}

// NewOrderedWindowClause creates a new WindowClause with ORDER BY.
func NewOrderedWindowClause(name string, orderClause *NodeList) *WindowClause {
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
	partitionCount := 0
	if wc.PartitionClause != nil {
		partitionCount = len(wc.PartitionClause.Items)
	}
	if partitionCount > 0 {
		parts = append(parts, fmt.Sprintf("partition=%d", partitionCount))
	}
	orderCount := 0
	if wc.OrderClause != nil {
		orderCount = len(wc.OrderClause.Items)
	}
	if orderCount > 0 {
		parts = append(parts, fmt.Sprintf("order=%d", orderCount))
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
// Ported from postgres/src/include/nodes/parsenodes.h:1436
type SortGroupClause struct {
	BaseNode
	TleSortGroupRef Index // Reference into targetlist - parsenodes.h:1439
	Eqop            Oid   // The equality operator ('=' op) - parsenodes.h:1440
	Sortop          Oid   // The ordering operator ('<' op), or InvalidOid - parsenodes.h:1441
	NullsFirst      bool  // Do NULLs come before normal values? - parsenodes.h:1442
	Hashable        bool  // Can eqop be implemented by hashing? - parsenodes.h:1444
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
// Ported from postgres/src/include/nodes/plannodes.h:1327
type RowMarkType int

const (
	ROW_MARK_EXCLUSIVE      RowMarkType = iota // FOR UPDATE - plannodes.h:1329
	ROW_MARK_NOKEYEXCLUSIVE                    // FOR NO KEY UPDATE - plannodes.h:1330
	ROW_MARK_SHARE                             // FOR SHARE - plannodes.h:1331
	ROW_MARK_KEYSHARE                          // FOR KEY SHARE - plannodes.h:1332
	ROW_MARK_REFERENCE                         // FOR REFERENCE (MySQL compat) - plannodes.h:1333
	ROW_MARK_COPY                              // FOR COPY (internal) - plannodes.h:1334
)

// LockWaitPolicy represents lock wait policies.
// Ported from postgres/src/include/nodes/lockoptions.h:36
type LockWaitPolicy int

const (
	LockWaitBlock LockWaitPolicy = iota // Block on locks - lockoptions.h:39
	LockWaitSkip                        // SKIP LOCKED - lockoptions.h:41
	LockWaitError                       // NOWAIT - lockoptions.h:43
)

// RowMarkClause represents FOR UPDATE/SHARE clauses.
// This supports row-level locking which is essential for concurrency control.
// Ported from postgres/src/include/nodes/parsenodes.h:1576
type RowMarkClause struct {
	BaseNode
	Rti        Index              // Range table index of target relation - parsenodes.h:1579
	Strength   LockClauseStrength // Lock clause strength - parsenodes.h:1580
	WaitPolicy LockWaitPolicy     // NOWAIT and SKIP LOCKED - parsenodes.h:1581
	PushedDown bool               // Pushed down from higher query level? - parsenodes.h:1582
}

// NewRowMarkClause creates a new RowMarkClause node.
func NewRowMarkClause(rti Index, strength LockClauseStrength) *RowMarkClause {
	return &RowMarkClause{
		BaseNode: BaseNode{Tag: T_RowMarkClause},
		Rti:      rti,
		Strength: strength,
	}
}

// NewRowMarkClauseWithPolicy creates a new RowMarkClause with wait policy.
func NewRowMarkClauseWithPolicy(rti Index, strength LockClauseStrength, waitPolicy LockWaitPolicy) *RowMarkClause {
	return &RowMarkClause{
		BaseNode:   BaseNode{Tag: T_RowMarkClause},
		Rti:        rti,
		Strength:   strength,
		WaitPolicy: waitPolicy,
	}
}

func (rmc *RowMarkClause) String() string {
	strengths := map[LockClauseStrength]string{
		LCS_FORKEYSHARE: "KEY SHARE", LCS_FORSHARE: "SHARE",
		LCS_FORNOKEYUPDATE: "NO KEY UPDATE", LCS_FORUPDATE: "UPDATE",
	}
	strengthStr := strengths[rmc.Strength]
	if strengthStr == "" {
		strengthStr = fmt.Sprintf("LCS_%d", int(rmc.Strength))
	}

	policies := map[LockWaitPolicy]string{
		LockWaitSkip: " SKIP LOCKED", LockWaitError: " NOWAIT",
	}
	policyStr := policies[rmc.WaitPolicy]

	return fmt.Sprintf("RowMarkClause(FOR %s%s)", strengthStr, policyStr)
}

// OnConflictAction represents the action to take on conflicts.
// Ported from postgres/src/include/nodes/nodes.h:415
type OnConflictAction int

const (
	ONCONFLICT_NONE    OnConflictAction = iota // No ON CONFLICT clause - nodes.h:417
	ONCONFLICT_NOTHING                         // ON CONFLICT DO NOTHING - nodes.h:418
	ONCONFLICT_UPDATE                          // ON CONFLICT DO UPDATE - nodes.h:419
)

func (o OnConflictAction) String() string {
	switch o {
	case ONCONFLICT_NONE:
		return ""
	case ONCONFLICT_NOTHING:
		return "DO NOTHING"
	case ONCONFLICT_UPDATE:
		return "DO UPDATE"
	default:
		return fmt.Sprintf("OnConflictAction(%d)", int(o))
	}
}

// OnConflictExpr represents INSERT ... ON CONFLICT expressions.
// This is an important PostgreSQL-specific feature for handling conflicts
// during INSERT operations (UPSERT functionality).
// Ported from postgres/src/include/nodes/primnodes.h:2321
type OnConflictExpr struct {
	BaseExpr
	Action          OnConflictAction // The action to take - primnodes.h:2324
	ArbiterElems    *NodeList        // Unique index arbiter list (of InferenceElem's) - primnodes.h:2327
	ArbiterWhere    Expression       // Unique index arbiter WHERE clause - primnodes.h:2329
	Constraint      Oid              // Constraint to infer from - primnodes.h:2330
	OnConflictSet   *NodeList        // List of ON CONFLICT SET targets - primnodes.h:2333
	OnConflictWhere Expression       // WHERE clause for ON CONFLICT UPDATE - primnodes.h:2334
	ExclRelIndex    int              // RT index of the EXCLUDED pseudo-relation - primnodes.h:2335
	ExclRelTlist    *NodeList        // Tlist of the EXCLUDED pseudo-relation - primnodes.h:2336
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
		BaseExpr:      BaseExpr{BaseNode: BaseNode{Tag: T_OnConflictExpr}},
		Action:        ONCONFLICT_NOTHING,
		ArbiterElems:  NewNodeList(),
		OnConflictSet: NewNodeList(),
	}
}

// NewOnConflictDoUpdate creates a new ON CONFLICT DO UPDATE expression.
func NewOnConflictDoUpdate(onConflictSet *NodeList) *OnConflictExpr {
	return &OnConflictExpr{
		BaseExpr:      BaseExpr{BaseNode: BaseNode{Tag: T_OnConflictExpr}},
		Action:        ONCONFLICT_UPDATE,
		ArbiterElems:  NewNodeList(),
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
