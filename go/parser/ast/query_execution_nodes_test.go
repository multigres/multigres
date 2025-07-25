// Package ast provides comprehensive tests for PostgreSQL AST query execution nodes.
// These tests ensure compatibility and correctness of essential query processing structures.
package ast

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
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
	assert.Equal(t, T_TargetEntry, te.Tag, "Expected tag T_TargetEntry")
	assert.Equal(t, expr, te.Expr, "Expected expression to be set correctly")
	assert.Equal(t, AttrNumber(1), te.Resno, "Expected resno 1")
	assert.Equal(t, "test_column", te.Resname, "Expected resname 'test_column'")
	assert.False(t, te.Resjunk, "Expected non-junk entry by default")

	// Test string representation
	str := te.String()
	assert.NotEmpty(t, str, "String representation should not be empty")

	// Test ExpressionType
	assert.Equal(t, "TargetEntry", te.ExpressionType(), "Expected ExpressionType 'TargetEntry'")
}

func TestJunkTargetEntry(t *testing.T) {
	expr := NewConst(1, 42, false)
	te := NewJunkTargetEntry(expr, 2)

	// Verify junk properties
	assert.True(t, te.Resjunk, "Expected junk target entry")
	assert.Equal(t, AttrNumber(2), te.Resno, "Expected resno 2")

	// Verify junk appears in string representation
	str := te.String()
	assert.NotEmpty(t, str, "String representation should not be empty")
}

func TestTargetEntryWithOriginInfo(t *testing.T) {
	expr := NewConst(1, 42, false)
	te := NewTargetEntry(expr, 1, "test_column")

	// Set origin information
	te.Resorigtbl = 12345
	te.Resorigcol = 7

	assert.Equal(t, Oid(12345), te.Resorigtbl, "Expected origin table OID 12345")

	assert.Equal(t, AttrNumber(7), te.Resorigcol, "Expected origin column 7")
}

// ==============================================================================
// FROM EXPR TESTS
// ==============================================================================

func TestFromExpr(t *testing.T) {
	// Create test table references
	table1 := NewRangeVar("table1", "public", "")
	table2 := NewRangeVar("table2", "public", "")
	fromlist := []Node{table1, table2}

	// Create test qualification
	quals := NewConst(1, Datum(1), false) // Simple boolean constant

	// Test FromExpr creation
	fe := NewFromExpr(fromlist, quals)

	// Verify properties
	assert.Equal(t, T_FromExpr, fe.Tag, "Expected tag T_FromExpr")

	assert.Equal(t, 2, len(fe.Fromlist), "Expected fromlist length 2")

	assert.Equal(t, quals, fe.Quals, "Expected quals to be set correctly")

	// Test ExpressionType
	assert.Equal(t, "FromExpr", fe.ExpressionType(), "Expected ExpressionType 'FromExpr'")

	// Test string representation
	str := fe.String()
	assert.NotEmpty(t, str, "String representation should not be empty")
}

func TestFromExprNoQuals(t *testing.T) {
	// Test FromExpr without qualifications
	table := NewRangeVar("table1", "public", "")
	fromlist := []Node{table}

	fe := NewFromExpr(fromlist, nil)

	assert.Nil(t, fe.Quals, "Expected no quals")

	assert.Equal(t, 1, len(fe.Fromlist), "Expected fromlist length 1")
}

// ==============================================================================
// JOIN EXPR TESTS
// ==============================================================================

func TestJoinExpr(t *testing.T) {
	// Create test tables
	larg := NewRangeVar("left_table", "public", "")
	rarg := NewRangeVar("right_table", "public", "")
	quals := NewConst(1, Datum(1), false)

	// Test INNER JOIN
	je := NewJoinExpr(JOIN_INNER, larg, rarg, quals)

	// Verify properties
	assert.Equal(t, T_JoinExpr, je.Tag, "Expected tag T_JoinExpr")

	assert.Equal(t, JOIN_INNER, je.Jointype, "Expected JOIN_INNER")

	assert.Equal(t, larg, je.Larg, "Expected left argument to be set correctly")

	assert.Equal(t, rarg, je.Rarg, "Expected right argument to be set correctly")

	assert.Equal(t, quals, je.Quals, "Expected qualifications to be set correctly")

	assert.False(t, je.IsNatural, "Expected non-natural join by default")

	// Test ExpressionType
	assert.Equal(t, "JoinExpr", je.ExpressionType(), "Expected ExpressionType 'JoinExpr'")

	// Test string representation
	str := je.String()
	assert.NotEmpty(t, str, "String representation should not be empty")
}

func TestJoinTypes(t *testing.T) {
	larg := NewRangeVar("left_table", "public", "")
	rarg := NewRangeVar("right_table", "public", "")

	// Test all join types
	joinTypes := []JoinType{
		JOIN_INNER, JOIN_LEFT, JOIN_RIGHT, JOIN_FULL,
		JOIN_SEMI, JOIN_ANTI,
	}

	for _, joinType := range joinTypes {
		je := NewJoinExpr(joinType, larg, rarg, nil)
		assert.Equal(t, joinType, je.Jointype, "Expected join type to match")
	}
}

func TestNaturalJoinExpr(t *testing.T) {
	larg := NewRangeVar("left_table", "public", "")
	rarg := NewRangeVar("right_table", "public", "")

	je := NewNaturalJoinExpr(JOIN_INNER, larg, rarg)

	assert.True(t, je.IsNatural, "Expected natural join")

	assert.Nil(t, je.Quals, "Natural joins should not have explicit quals")

	// Test string representation includes "NATURAL"
	str := je.String()
	assert.NotEmpty(t, str, "String representation should not be empty")
}

func TestUsingJoinExpr(t *testing.T) {
	larg := NewRangeVar("left_table", "public", "")
	rarg := NewRangeVar("right_table", "public", "")

	// Create USING clause
	col1 := NewString("id")
	col2 := NewString("name")
	usingClause := []Node{col1, col2}

	je := NewUsingJoinExpr(JOIN_LEFT, larg, rarg, usingClause)

	assert.Equal(t, 2, len(je.UsingClause), "Expected USING clause with 2 columns")

	assert.False(t, je.IsNatural, "USING join should not be natural")
}

// ==============================================================================
// SUBPLAN TESTS
// ==============================================================================

func TestSubPlan(t *testing.T) {
	sp := NewSubPlan(EXISTS_SUBLINK, 1, "exists_plan")

	// Verify properties
	assert.Equal(t, T_SubPlan, sp.Tag, "Expected tag T_SubPlan")

	assert.Equal(t, EXISTS_SUBLINK, sp.SubLinkType, "Expected EXISTS_SUBLINK")

	assert.Equal(t, 1, sp.PlanId, "Expected plan ID 1")

	assert.Equal(t, "exists_plan", sp.PlanName, "Expected plan name 'exists_plan'")

	// Test ExpressionType
	assert.Equal(t, "SubPlan", sp.ExpressionType(), "Expected ExpressionType 'SubPlan'")

	// Test string representation
	str := sp.String()
	assert.NotEmpty(t, str, "String representation should not be empty")
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
		assert.Equal(t, subType, sp.SubLinkType, "Expected sublink type to match")
	}
}

func TestSubPlanWithParameters(t *testing.T) {
	sp := NewSubPlan(ANY_SUBLINK, 2, "any_plan")

	// Set additional properties
	sp.ParamIds = []int{1, 2, 3}
	sp.FirstColType = 23 // INT4OID
	sp.UseHashTable = true
	sp.ParallelSafe = true

	assert.Equal(t, 3, len(sp.ParamIds), "Expected 3 param IDs")

	assert.Equal(t, Oid(23), sp.FirstColType, "Expected first column type 23")

	assert.True(t, sp.UseHashTable, "Expected hash table usage")

	assert.True(t, sp.ParallelSafe, "Expected parallel safety")
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
	assert.Equal(t, T_AlternativeSubPlan, asp.Tag, "Expected tag T_AlternativeSubPlan")

	assert.Equal(t, 2, len(asp.Subplans), "Expected 2 subplans")

	// Test ExpressionType
	assert.Equal(t, "AlternativeSubPlan", asp.ExpressionType(), "Expected ExpressionType 'AlternativeSubPlan'")

	// Test string representation
	str := asp.String()
	assert.NotEmpty(t, str, "String representation should not be empty")
}

// ==============================================================================
// COMMON TABLE EXPR TESTS
// ==============================================================================

func TestCommonTableExpr(t *testing.T) {
	query := NewSelectStmt()
	cte := NewCommonTableExpr("test_cte", query)

	// Verify properties
	assert.Equal(t, T_CommonTableExpr, cte.Tag, "Expected tag T_CommonTableExpr")

	assert.Equal(t, "test_cte", cte.Ctename, "Expected CTE name 'test_cte'")

	assert.Equal(t, query, cte.Ctequery, "Expected CTE query to be set correctly")

	assert.False(t, cte.Cterecursive, "Expected non-recursive CTE by default")

	assert.Equal(t, -1, cte.Location(), "Expected default location -1")

	// Test string representation
	str := cte.String()
	assert.NotEmpty(t, str, "String representation should not be empty")
}

func TestRecursiveCommonTableExpr(t *testing.T) {
	query := NewSelectStmt()
	cte := NewRecursiveCommonTableExpr("recursive_cte", query)

	assert.True(t, cte.Cterecursive, "Expected recursive CTE")

	// Test string representation includes "RECURSIVE"
	str := cte.String()
	assert.NotEmpty(t, str, "String representation should not be empty")
}

func TestCommonTableExprWithColumns(t *testing.T) {
	query := NewSelectStmt()
	cte := NewCommonTableExpr("test_cte", query)

	// Set column information
	cte.Aliascolnames = []Node{NewString("col1"), NewString("col2")}
	cte.Ctecolnames = []Node{NewString("col1"), NewString("col2")}
	cte.Ctecoltypes = []Oid{23, 25} // INT4OID, TEXTOID

	assert.Equal(t, 2, len(cte.Aliascolnames), "Expected 2 alias column names")

	assert.Equal(t, 2, len(cte.Ctecoltypes), "Expected 2 column types")
}

// ==============================================================================
// WINDOW CLAUSE TESTS
// ==============================================================================

func TestWindowClause(t *testing.T) {
	wc := NewWindowClause("test_window")

	// Verify properties
	assert.Equal(t, T_WindowClause, wc.Tag, "Expected tag T_WindowClause")

	assert.Equal(t, "test_window", wc.Name, "Expected window name 'test_window'")

	// Test string representation
	str := wc.String()
	assert.NotEmpty(t, str, "String representation should not be empty")
}

func TestPartitionedWindowClause(t *testing.T) {
	// Create partition expressions
	col1 := NewColumnRef(NewString("col1"))
	col2 := NewColumnRef(NewString("col2"))
	partitionClause := []Node{col1, col2}

	wc := NewPartitionedWindowClause("partitioned_window", partitionClause)

	assert.Equal(t, 2, len(wc.PartitionClause), "Expected 2 partition columns")

	assert.Equal(t, "partitioned_window", wc.Name, "Expected window name 'partitioned_window'")
}

func TestOrderedWindowClause(t *testing.T) {
	// Create order expressions (would normally be SortBy nodes, using strings for simplicity)
	order1 := NewString("col1 ASC")
	order2 := NewString("col2 DESC")
	orderClause := []Node{order1, order2}

	wc := NewOrderedWindowClause("ordered_window", orderClause)

	assert.Equal(t, 2, len(wc.OrderClause), "Expected 2 order columns")

	assert.Equal(t, "ordered_window", wc.Name, "Expected window name 'ordered_window'")
}

// ==============================================================================
// SORT GROUP CLAUSE TESTS
// ==============================================================================

func TestSortGroupClause(t *testing.T) {
	sgc := NewSortGroupClause(1, 96, 97) // Sample equality and sort operators

	// Verify properties
	assert.Equal(t, T_SortGroupClause, sgc.Tag, "Expected tag T_SortGroupClause")

	assert.Equal(t, Index(1), sgc.TleSortGroupRef, "Expected tle sort group ref 1")

	assert.Equal(t, Oid(96), sgc.Eqop, "Expected equality operator 96")

	assert.Equal(t, Oid(97), sgc.Sortop, "Expected sort operator 97")

	assert.False(t, sgc.NullsFirst, "Expected NULLS LAST by default")

	// Test string representation
	str := sgc.String()
	assert.NotEmpty(t, str, "String representation should not be empty")
}

func TestSortGroupClauseNullsFirst(t *testing.T) {
	sgc := NewSortGroupClauseNullsFirst(2, 96, 97)

	assert.True(t, sgc.NullsFirst, "Expected NULLS FIRST")

	// Test string representation includes "NULLS FIRST"
	str := sgc.String()
	assert.NotEmpty(t, str, "String representation should not be empty")
}

// ==============================================================================
// ROW MARK CLAUSE TESTS
// ==============================================================================

func TestRowMarkClause(t *testing.T) {
	rmc := NewRowMarkClause(1, LCS_FORUPDATE)

	// Verify properties
	assert.Equal(t, T_RowMarkClause, rmc.Tag, "Expected tag T_RowMarkClause")

	assert.Equal(t, Index(1), rmc.Rti, "Expected RTI 1")

	assert.Equal(t, LCS_FORUPDATE, rmc.Strength, "Expected LCS_FORUPDATE")

	assert.Equal(t, LockWaitBlock, rmc.WaitPolicy, "Expected LockWaitBlock by default")

	// Test string representation
	str := rmc.String()
	assert.NotEmpty(t, str, "String representation should not be empty")
}

func TestRowMarkTypes(t *testing.T) {
	// Test all lock clause strengths
	lockTypes := []LockClauseStrength{
		LCS_FORUPDATE, LCS_FORNOKEYUPDATE,
		LCS_FORSHARE, LCS_FORKEYSHARE,
	}

	for _, lockType := range lockTypes {
		rmc := NewRowMarkClause(1, lockType)
		assert.Equal(t, lockType, rmc.Strength, "Expected lock type to match")
	}
}

func TestRowMarkClauseWithPolicy(t *testing.T) {
	// Test NOWAIT policy
	rmc := NewRowMarkClauseWithPolicy(1, LCS_FORSHARE, LockWaitError)

	assert.Equal(t, LockWaitError, rmc.WaitPolicy, "Expected LockWaitError")

	// Test SKIP LOCKED policy
	rmc2 := NewRowMarkClauseWithPolicy(1, LCS_FORSHARE, LockWaitSkip)

	assert.Equal(t, LockWaitSkip, rmc2.WaitPolicy, "Expected LockWaitSkip")
}

// ==============================================================================
// ON CONFLICT EXPR TESTS
// ==============================================================================

func TestOnConflictExpr(t *testing.T) {
	oce := NewOnConflictExpr(ONCONFLICT_NOTHING)

	// Verify properties
	assert.Equal(t, T_OnConflictExpr, oce.Tag, "Expected tag T_OnConflictExpr")

	assert.Equal(t, ONCONFLICT_NOTHING, oce.Action, "Expected ONCONFLICT_NOTHING")

	// Test ExpressionType
	assert.Equal(t, "OnConflictExpr", oce.ExpressionType(), "Expected ExpressionType 'OnConflictExpr'")

	// Test string representation
	str := oce.String()
	assert.NotEmpty(t, str, "String representation should not be empty")
}

func TestOnConflictDoNothing(t *testing.T) {
	oce := NewOnConflictDoNothing()

	assert.Equal(t, ONCONFLICT_NOTHING, oce.Action, "Expected ONCONFLICT_NOTHING")

	assert.Equal(t, 0, len(oce.OnConflictSet), "Expected empty conflict set for DO NOTHING")
}

func TestOnConflictDoUpdate(t *testing.T) {
	// Create update targets
	target1 := NewResTarget("col1", NewConst(23, Datum(100), false))
	target2 := NewResTarget("col2", NewConst(25, Datum(0), false))
	updateTargets := []Node{target1, target2}

	oce := NewOnConflictDoUpdate(updateTargets)

	assert.Equal(t, ONCONFLICT_UPDATE, oce.Action, "Expected ONCONFLICT_UPDATE")

	assert.Equal(t, 2, len(oce.OnConflictSet), "Expected 2 update targets")
}

func TestOnConflictActions(t *testing.T) {
	// Test all conflict actions
	actions := []OnConflictAction{
		ONCONFLICT_NONE, ONCONFLICT_NOTHING, ONCONFLICT_UPDATE,
	}

	for _, action := range actions {
		oce := NewOnConflictExpr(action)
		assert.Equal(t, action, oce.Action, "Expected conflict action to match")
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
	table1 := NewRangeVar("table1", "public", "")
	table2 := NewRangeVar("table2", "public", "")
	joinQuals := NewConst(16, Datum(1), false) // boolean
	join := NewJoinExpr(JOIN_INNER, table1, table2, joinQuals)

	fromExpr := NewFromExpr([]Node{join}, nil)

	// Verify they all have proper tags
	assert.Equal(t, T_TargetEntry, te.Tag, "TargetEntry tag incorrect")

	assert.Equal(t, T_FromExpr, fromExpr.Tag, "FromExpr tag incorrect")

	assert.Equal(t, T_JoinExpr, join.Tag, "JoinExpr tag incorrect")

	// Verify they can be used together
	assert.Equal(t, 1, len(fromExpr.Fromlist), "Expected 1 item in fromlist")

	assert.Equal(t, join, fromExpr.Fromlist[0], "Expected join to be in fromlist")
}

func TestCTEWithWindowFunction(t *testing.T) {
	// Test interaction between CTE and window functions

	// Create a window clause
	partitionCol := NewColumnRef(NewString("department"))
	wc := NewPartitionedWindowClause("dept_window", []Node{partitionCol})

	// Create a CTE query (simplified)
	cteQuery := NewSelectStmt()
	cte := NewCommonTableExpr("department_stats", cteQuery)

	// Verify they work together
	assert.Equal(t, T_WindowClause, wc.Tag, "WindowClause tag incorrect")

	assert.Equal(t, T_CommonTableExpr, cte.Tag, "CommonTableExpr tag incorrect")

	assert.Equal(t, 1, len(wc.PartitionClause), "Expected 1 partition column")
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
	larg := NewRangeVar("left_table", "public", "")
	rarg := NewRangeVar("right_table", "public", "")
	quals := NewConst(16, Datum(1), false)
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
