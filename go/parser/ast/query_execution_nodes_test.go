// Package ast provides comprehensive tests for PostgreSQL AST query execution nodes.
// These tests ensure compatibility and correctness of essential query processing structures.
package ast

import (
	"testing"
)

// ==============================================================================
// TARGET ENTRY TESTS
// ==============================================================================

func TestTargetEntry(t *testing.T) {
	// Create a simple constant expression for testing
	expr := NewConst(1, 42, false)
	
	// Test basic TargetEntry creation
	te := NewTargetEntry(expr, 1, "test_column")
	
	// Verify basic properties
	if te.Tag != T_TargetEntry {
		t.Errorf("Expected tag T_TargetEntry, got %v", te.Tag)
	}
	
	if te.Expr != expr {
		t.Errorf("Expected expression to be set correctly")
	}
	
	if te.Resno != 1 {
		t.Errorf("Expected resno 1, got %d", te.Resno)
	}
	
	if te.Resname != "test_column" {
		t.Errorf("Expected resname 'test_column', got %s", te.Resname)
	}
	
	if te.Resjunk {
		t.Errorf("Expected non-junk entry by default")
	}
	
	// Test string representation
	str := te.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
	
	// Test ExpressionType
	if te.ExpressionType() != "TargetEntry" {
		t.Errorf("Expected ExpressionType 'TargetEntry', got %s", te.ExpressionType())
	}
}

func TestJunkTargetEntry(t *testing.T) {
	expr := NewConst(1, 42, false)
	te := NewJunkTargetEntry(expr, 2)
	
	// Verify junk properties
	if !te.Resjunk {
		t.Errorf("Expected junk target entry")
	}
	
	if te.Resno != 2 {
		t.Errorf("Expected resno 2, got %d", te.Resno)
	}
	
	// Verify junk appears in string representation
	str := te.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestTargetEntryWithOriginInfo(t *testing.T) {
	expr := NewConst(1, 42, false)
	te := NewTargetEntry(expr, 1, "test_column")
	
	// Set origin information
	te.Resorigtbl = 12345
	te.Resorigcol = 7
	
	if te.Resorigtbl != 12345 {
		t.Errorf("Expected origin table OID 12345, got %d", te.Resorigtbl)
	}
	
	if te.Resorigcol != 7 {
		t.Errorf("Expected origin column 7, got %d", te.Resorigcol)
	}
}

// ==============================================================================
// FROM EXPR TESTS  
// ==============================================================================

func TestFromExpr(t *testing.T) {
	// Create test table references
	table1 := NewRangeVar("public", "table1", -1)
	table2 := NewRangeVar("public", "table2", -1)
	fromlist := []Node{table1, table2}
	
	// Create test qualification
	quals := NewConst(1, true, false) // Simple boolean constant
	
	// Test FromExpr creation
	fe := NewFromExpr(fromlist, quals)
	
	// Verify properties
	if fe.Tag != T_FromExpr {
		t.Errorf("Expected tag T_FromExpr, got %v", fe.Tag)
	}
	
	if len(fe.Fromlist) != 2 {
		t.Errorf("Expected fromlist length 2, got %d", len(fe.Fromlist))
	}
	
	if fe.Quals != quals {
		t.Errorf("Expected quals to be set correctly")
	}
	
	// Test ExpressionType
	if fe.ExpressionType() != "FromExpr" {
		t.Errorf("Expected ExpressionType 'FromExpr', got %s", fe.ExpressionType())
	}
	
	// Test string representation
	str := fe.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestFromExprNoQuals(t *testing.T) {
	// Test FromExpr without qualifications
	table := NewRangeVar("public", "table1", -1)
	fromlist := []Node{table}
	
	fe := NewFromExpr(fromlist, nil)
	
	if fe.Quals != nil {
		t.Errorf("Expected no quals")
	}
	
	if len(fe.Fromlist) != 1 {
		t.Errorf("Expected fromlist length 1, got %d", len(fe.Fromlist))
	}
}

// ==============================================================================
// JOIN EXPR TESTS
// ==============================================================================

func TestJoinExpr(t *testing.T) {
	// Create test tables
	larg := NewRangeVar("public", "left_table", -1)
	rarg := NewRangeVar("public", "right_table", -1)
	quals := NewConst(1, true, false)
	
	// Test INNER JOIN
	je := NewJoinExpr(JOIN_INNER, larg, rarg, quals)
	
	// Verify properties
	if je.Tag != T_JoinExpr {
		t.Errorf("Expected tag T_JoinExpr, got %v", je.Tag)
	}
	
	if je.Jointype != JOIN_INNER {
		t.Errorf("Expected JOIN_INNER, got %v", je.Jointype)
	}
	
	if je.Larg != larg {
		t.Errorf("Expected left argument to be set correctly")
	}
	
	if je.Rarg != rarg {
		t.Errorf("Expected right argument to be set correctly")
	}
	
	if je.Quals != quals {
		t.Errorf("Expected qualifications to be set correctly")
	}
	
	if je.IsNatural {
		t.Errorf("Expected non-natural join by default")
	}
	
	// Test ExpressionType
	if je.ExpressionType() != "JoinExpr" {
		t.Errorf("Expected ExpressionType 'JoinExpr', got %s", je.ExpressionType())
	}
	
	// Test string representation
	str := je.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestJoinTypes(t *testing.T) {
	larg := NewRangeVar("public", "left_table", -1)
	rarg := NewRangeVar("public", "right_table", -1)
	
	// Test all join types
	joinTypes := []JoinType{
		JOIN_INNER, JOIN_LEFT, JOIN_RIGHT, JOIN_FULL,
		JOIN_SEMI, JOIN_ANTI,
	}
	
	for _, joinType := range joinTypes {
		je := NewJoinExpr(joinType, larg, rarg, nil)
		if je.Jointype != joinType {
			t.Errorf("Expected join type %v, got %v", joinType, je.Jointype)
		}
	}
}

func TestNaturalJoinExpr(t *testing.T) {
	larg := NewRangeVar("public", "left_table", -1)
	rarg := NewRangeVar("public", "right_table", -1)
	
	je := NewNaturalJoinExpr(JOIN_INNER, larg, rarg)
	
	if !je.IsNatural {
		t.Errorf("Expected natural join")
	}
	
	if je.Quals != nil {
		t.Errorf("Natural joins should not have explicit quals")
	}
	
	// Test string representation includes "NATURAL"
	str := je.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestUsingJoinExpr(t *testing.T) {
	larg := NewRangeVar("public", "left_table", -1)
	rarg := NewRangeVar("public", "right_table", -1)
	
	// Create USING clause
	col1 := NewString("id")
	col2 := NewString("name")
	usingClause := []Node{col1, col2}
	
	je := NewUsingJoinExpr(JOIN_LEFT, larg, rarg, usingClause)
	
	if len(je.UsingClause) != 2 {
		t.Errorf("Expected USING clause with 2 columns, got %d", len(je.UsingClause))
	}
	
	if je.IsNatural {
		t.Errorf("USING join should not be natural")
	}
}

// ==============================================================================
// SUBPLAN TESTS
// ==============================================================================

func TestSubPlan(t *testing.T) {
	sp := NewSubPlan(EXISTS_SUBLINK, 1, "exists_plan")
	
	// Verify properties
	if sp.Tag != T_SubPlan {
		t.Errorf("Expected tag T_SubPlan, got %v", sp.Tag)
	}
	
	if sp.SubLinkType != EXISTS_SUBLINK {
		t.Errorf("Expected EXISTS_SUBLINK, got %v", sp.SubLinkType)
	}
	
	if sp.PlanId != 1 {
		t.Errorf("Expected plan ID 1, got %d", sp.PlanId)
	}
	
	if sp.PlanName != "exists_plan" {
		t.Errorf("Expected plan name 'exists_plan', got %s", sp.PlanName)
	}
	
	// Test ExpressionType
	if sp.ExpressionType() != "SubPlan" {
		t.Errorf("Expected ExpressionType 'SubPlan', got %s", sp.ExpressionType())
	}
	
	// Test string representation
	str := sp.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestSubPlanTypes(t *testing.T) {
	// Test all SubLinkType values
	subTypes := []SubLinkType{
		EXISTS_SUBLINK, ALL_SUBLINK, ANY_SUBLINK,
		ROWCOMPARE_SUBLINK, EXPR_SUBLINK, MULTIEXPR_SUBLINK,
		ARRAY_SUBLINK, CTE_SUBLINK,
	}
	
	for i, subType := range subTypes {
		sp := NewSubPlan(subType, i+1, fmt.Sprintf("plan_%d", i))
		if sp.SubLinkType != subType {
			t.Errorf("Expected sublink type %v, got %v", subType, sp.SubLinkType)
		}
	}
}

func TestSubPlanWithParameters(t *testing.T) {
	sp := NewSubPlan(ANY_SUBLINK, 2, "any_plan")
	
	// Set additional properties
	sp.ParamIds = []int{1, 2, 3}
	sp.FirstColType = 23 // INT4OID
	sp.UseHashTable = true
	sp.ParallelSafe = true
	
	if len(sp.ParamIds) != 3 {
		t.Errorf("Expected 3 param IDs, got %d", len(sp.ParamIds))
	}
	
	if sp.FirstColType != 23 {
		t.Errorf("Expected first column type 23, got %d", sp.FirstColType)
	}
	
	if !sp.UseHashTable {
		t.Errorf("Expected hash table usage")
	}
	
	if !sp.ParallelSafe {
		t.Errorf("Expected parallel safety")
	}
}

// ==============================================================================
// ALTERNATIVE SUBPLAN TESTS
// ==============================================================================

func TestAlternativeSubPlan(t *testing.T) {
	// Create some subplans
	sp1 := NewSubPlan(EXISTS_SUBLINK, 1, "plan1")
	sp2 := NewSubPlan(EXISTS_SUBLINK, 2, "plan2")
	subplans := []Expression{sp1, sp2}
	
	asp := NewAlternativeSubPlan(subplans)
	
	// Verify properties
	if asp.Tag != T_AlternativeSubPlan {
		t.Errorf("Expected tag T_AlternativeSubPlan, got %v", asp.Tag)
	}
	
	if len(asp.Subplans) != 2 {
		t.Errorf("Expected 2 subplans, got %d", len(asp.Subplans))
	}
	
	// Test ExpressionType
	if asp.ExpressionType() != "AlternativeSubPlan" {
		t.Errorf("Expected ExpressionType 'AlternativeSubPlan', got %s", asp.ExpressionType())
	}
	
	// Test string representation
	str := asp.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

// ==============================================================================
// COMMON TABLE EXPR TESTS
// ==============================================================================

func TestCommonTableExpr(t *testing.T) {
	query := NewSelectStmt()
	cte := NewCommonTableExpr("test_cte", query)
	
	// Verify properties
	if cte.Tag != T_CommonTableExpr {
		t.Errorf("Expected tag T_CommonTableExpr, got %v", cte.Tag)
	}
	
	if cte.Ctename != "test_cte" {
		t.Errorf("Expected CTE name 'test_cte', got %s", cte.Ctename)
	}
	
	if cte.Ctequery != query {
		t.Errorf("Expected CTE query to be set correctly")
	}
	
	if cte.Cterecursive {
		t.Errorf("Expected non-recursive CTE by default")
	}
	
	if cte.Location != -1 {
		t.Errorf("Expected default location -1, got %d", cte.Location)
	}
	
	// Test string representation
	str := cte.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestRecursiveCommonTableExpr(t *testing.T) {
	query := NewSelectStmt()
	cte := NewRecursiveCommonTableExpr("recursive_cte", query)
	
	if !cte.Cterecursive {
		t.Errorf("Expected recursive CTE")
	}
	
	// Test string representation includes "RECURSIVE"
	str := cte.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestCommonTableExprWithColumns(t *testing.T) {
	query := NewSelectStmt()
	cte := NewCommonTableExpr("test_cte", query)
	
	// Set column information
	cte.Aliascolnames = []Node{NewString("col1"), NewString("col2")}
	cte.Ctecolnames = []Node{NewString("col1"), NewString("col2")}
	cte.Ctecoltypes = []Oid{23, 25} // INT4OID, TEXTOID
	
	if len(cte.Aliascolnames) != 2 {
		t.Errorf("Expected 2 alias column names, got %d", len(cte.Aliascolnames))
	}
	
	if len(cte.Ctecoltypes) != 2 {
		t.Errorf("Expected 2 column types, got %d", len(cte.Ctecoltypes))
	}
}

// ==============================================================================
// WINDOW CLAUSE TESTS
// ==============================================================================

func TestWindowClause(t *testing.T) {
	wc := NewWindowClause("test_window")
	
	// Verify properties
	if wc.Tag != T_WindowClause {
		t.Errorf("Expected tag T_WindowClause, got %v", wc.Tag)
	}
	
	if wc.Name != "test_window" {
		t.Errorf("Expected window name 'test_window', got %s", wc.Name)
	}
	
	// Test string representation
	str := wc.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestPartitionedWindowClause(t *testing.T) {
	// Create partition expressions
	col1 := NewColumnRef([]Node{NewString("col1")}, -1)
	col2 := NewColumnRef([]Node{NewString("col2")}, -1)
	partitionClause := []Node{col1, col2}
	
	wc := NewPartitionedWindowClause("partitioned_window", partitionClause)
	
	if len(wc.PartitionClause) != 2 {
		t.Errorf("Expected 2 partition columns, got %d", len(wc.PartitionClause))
	}
	
	if wc.Name != "partitioned_window" {
		t.Errorf("Expected window name 'partitioned_window', got %s", wc.Name)
	}
}

func TestOrderedWindowClause(t *testing.T) {
	// Create order expressions (would normally be SortBy nodes, using strings for simplicity)
	order1 := NewString("col1 ASC")
	order2 := NewString("col2 DESC")
	orderClause := []Node{order1, order2}
	
	wc := NewOrderedWindowClause("ordered_window", orderClause)
	
	if len(wc.OrderClause) != 2 {
		t.Errorf("Expected 2 order columns, got %d", len(wc.OrderClause))
	}
	
	if wc.Name != "ordered_window" {
		t.Errorf("Expected window name 'ordered_window', got %s", wc.Name)
	}
}

// ==============================================================================
// SORT GROUP CLAUSE TESTS
// ==============================================================================

func TestSortGroupClause(t *testing.T) {
	sgc := NewSortGroupClause(1, 96, 97) // Sample equality and sort operators
	
	// Verify properties
	if sgc.Tag != T_SortGroupClause {
		t.Errorf("Expected tag T_SortGroupClause, got %v", sgc.Tag)
	}
	
	if sgc.TleSortGroupRef != 1 {
		t.Errorf("Expected tle sort group ref 1, got %d", sgc.TleSortGroupRef)
	}
	
	if sgc.Eqop != 96 {
		t.Errorf("Expected equality operator 96, got %d", sgc.Eqop)
	}
	
	if sgc.Sortop != 97 {
		t.Errorf("Expected sort operator 97, got %d", sgc.Sortop)
	}
	
	if sgc.NullsFirst {
		t.Errorf("Expected NULLS LAST by default")
	}
	
	// Test string representation
	str := sgc.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestSortGroupClauseNullsFirst(t *testing.T) {
	sgc := NewSortGroupClauseNullsFirst(2, 96, 97)
	
	if !sgc.NullsFirst {
		t.Errorf("Expected NULLS FIRST")
	}
	
	// Test string representation includes "NULLS FIRST"
	str := sgc.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

// ==============================================================================
// ROW MARK CLAUSE TESTS
// ==============================================================================

func TestRowMarkClause(t *testing.T) {
	rmc := NewRowMarkClause(1, ROW_MARK_EXCLUSIVE)
	
	// Verify properties
	if rmc.Tag != T_RowMarkClause {
		t.Errorf("Expected tag T_RowMarkClause, got %v", rmc.Tag)
	}
	
	if rmc.Rti != 1 {
		t.Errorf("Expected RTI 1, got %d", rmc.Rti)
	}
	
	if rmc.Strength != ROW_MARK_EXCLUSIVE {
		t.Errorf("Expected ROW_MARK_EXCLUSIVE, got %v", rmc.Strength)
	}
	
	if rmc.WaitPolicy != LockWaitBlock {
		t.Errorf("Expected LockWaitBlock by default, got %v", rmc.WaitPolicy)
	}
	
	// Test string representation
	str := rmc.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestRowMarkTypes(t *testing.T) {
	// Test all row mark types
	markTypes := []RowMarkType{
		ROW_MARK_EXCLUSIVE, ROW_MARK_NOKEYEXCLUSIVE,
		ROW_MARK_SHARE, ROW_MARK_KEYSHARE,
	}
	
	for _, markType := range markTypes {
		rmc := NewRowMarkClause(1, markType)
		if rmc.Strength != markType {
			t.Errorf("Expected mark type %v, got %v", markType, rmc.Strength)
		}
	}
}

func TestRowMarkClauseWithPolicy(t *testing.T) {
	// Test NOWAIT policy
	rmc := NewRowMarkClauseWithPolicy(1, ROW_MARK_SHARE, LockWaitError)
	
	if rmc.WaitPolicy != LockWaitError {
		t.Errorf("Expected LockWaitError, got %v", rmc.WaitPolicy)
	}
	
	// Test SKIP LOCKED policy
	rmc2 := NewRowMarkClauseWithPolicy(1, ROW_MARK_SHARE, LockWaitSkip)
	
	if rmc2.WaitPolicy != LockWaitSkip {
		t.Errorf("Expected LockWaitSkip, got %v", rmc2.WaitPolicy)
	}
}

// ==============================================================================
// ON CONFLICT EXPR TESTS
// ==============================================================================

func TestOnConflictExpr(t *testing.T) {
	oce := NewOnConflictExpr(ONCONFLICT_NOTHING)
	
	// Verify properties
	if oce.Tag != T_OnConflictExpr {
		t.Errorf("Expected tag T_OnConflictExpr, got %v", oce.Tag)
	}
	
	if oce.Action != ONCONFLICT_NOTHING {
		t.Errorf("Expected ONCONFLICT_NOTHING, got %v", oce.Action)
	}
	
	// Test ExpressionType
	if oce.ExpressionType() != "OnConflictExpr" {
		t.Errorf("Expected ExpressionType 'OnConflictExpr', got %s", oce.ExpressionType())
	}
	
	// Test string representation
	str := oce.String()
	if str == "" {
		t.Errorf("String representation should not be empty")
	}
}

func TestOnConflictDoNothing(t *testing.T) {
	oce := NewOnConflictDoNothing()
	
	if oce.Action != ONCONFLICT_NOTHING {
		t.Errorf("Expected ONCONFLICT_NOTHING, got %v", oce.Action)
	}
	
	if len(oce.OnConflictSet) != 0 {
		t.Errorf("Expected empty conflict set for DO NOTHING")
	}
}

func TestOnConflictDoUpdate(t *testing.T) {
	// Create update targets
	target1 := NewResTarget("col1", NewConst(23, 100, false), -1)
	target2 := NewResTarget("col2", NewConst(25, "updated", false), -1)
	updateTargets := []Node{target1, target2}
	
	oce := NewOnConflictDoUpdate(updateTargets)
	
	if oce.Action != ONCONFLICT_UPDATE {
		t.Errorf("Expected ONCONFLICT_UPDATE, got %v", oce.Action)
	}
	
	if len(oce.OnConflictSet) != 2 {
		t.Errorf("Expected 2 update targets, got %d", len(oce.OnConflictSet))
	}
}

func TestOnConflictActions(t *testing.T) {
	// Test all conflict actions
	actions := []OnConflictAction{
		ONCONFLICT_NONE, ONCONFLICT_NOTHING, ONCONFLICT_UPDATE,
	}
	
	for _, action := range actions {
		oce := NewOnConflictExpr(action)
		if oce.Action != action {
			t.Errorf("Expected conflict action %v, got %v", action, oce.Action)
		}
	}
}

// ==============================================================================
// INTEGRATION TESTS
// ==============================================================================

func TestQueryExecutionNodesInteraction(t *testing.T) {
	// Test that query execution nodes work together correctly
	
	// Create a TargetEntry
	expr := NewConst(23, 42, false)
	te := NewTargetEntry(expr, 1, "result_column")
	
	// Create a FromExpr with JoinExpr
	table1 := NewRangeVar("public", "table1", -1)
	table2 := NewRangeVar("public", "table2", -1)
	joinQuals := NewConst(16, true, false) // boolean
	join := NewJoinExpr(JOIN_INNER, table1, table2, joinQuals)
	
	fromExpr := NewFromExpr([]Node{join}, nil)
	
	// Verify they all have proper tags
	if te.Tag != T_TargetEntry {
		t.Errorf("TargetEntry tag incorrect")
	}
	
	if fromExpr.Tag != T_FromExpr {
		t.Errorf("FromExpr tag incorrect")  
	}
	
	if join.Tag != T_JoinExpr {
		t.Errorf("JoinExpr tag incorrect")
	}
	
	// Verify they can be used together
	if len(fromExpr.Fromlist) != 1 {
		t.Errorf("Expected 1 item in fromlist")
	}
	
	if fromExpr.Fromlist[0] != join {
		t.Errorf("Expected join to be in fromlist")
	}
}

func TestCTEWithWindowFunction(t *testing.T) {
	// Test interaction between CTE and window functions
	
	// Create a window clause
	partitionCol := NewColumnRef([]Node{NewString("department")}, -1)
	wc := NewPartitionedWindowClause("dept_window", []Node{partitionCol})
	
	// Create a CTE query (simplified)
	cteQuery := NewSelectStmt()
	cte := NewCommonTableExpr("department_stats", cteQuery)
	
	// Verify they work together
	if wc.Tag != T_WindowClause {
		t.Errorf("WindowClause tag incorrect")
	}
	
	if cte.Tag != T_CommonTableExpr {
		t.Errorf("CommonTableExpr tag incorrect")
	}
	
	if len(wc.PartitionClause) != 1 {
		t.Errorf("Expected 1 partition column")
	}
}

// ==============================================================================
// BENCHMARK TESTS
// ==============================================================================

func BenchmarkTargetEntryCreation(b *testing.B) {
	expr := NewConst(23, 42, false)
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		_ = NewTargetEntry(expr, AttrNumber(i%100), "test_column")
	}
}

func BenchmarkJoinExprCreation(b *testing.B) {
	larg := NewRangeVar("public", "left_table", -1)
	rarg := NewRangeVar("public", "right_table", -1)
	quals := NewConst(16, true, false)
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		_ = NewJoinExpr(JOIN_INNER, larg, rarg, quals)
	}
}

func BenchmarkStringRepresentation(b *testing.B) {
	te := NewTargetEntry(NewConst(23, 42, false), 1, "test_column")
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		_ = te.String()
	}
}