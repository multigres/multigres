package ast

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// ==============================================================================
// CORE PARSE INFRASTRUCTURE TESTS - Stage 1A Implementation
// Comprehensive test suite for all 25 parse infrastructure nodes
// Using stretchr/testify framework as per technical decisions
// ==============================================================================

func TestRawStmt(t *testing.T) {
	t.Run("basic construction", func(t *testing.T) {
		stmt := NewSelectStmt()
		rawStmt := NewRawStmt(stmt, 0, 10)

		assert.Equal(t, T_RawStmt, rawStmt.NodeTag())
		assert.Equal(t, stmt, rawStmt.Stmt)
		assert.Equal(t, 0, rawStmt.StmtLocation)
		assert.Equal(t, 10, rawStmt.StmtLen)
		assert.Equal(t, "RAW", rawStmt.StatementType())
		assert.Equal(t, "RawStmt{SELECT}@0", rawStmt.String())
	})

	t.Run("with nil statement", func(t *testing.T) {
		nilRawStmt := NewRawStmt(nil, 5, 15)
		nilRawStmt.SetLocation(5)
		assert.Equal(t, "RawStmt{nil}@5", nilRawStmt.String())
	})
}

func TestA_Expr(t *testing.T) {
	t.Run("basic construction", func(t *testing.T) {
		name := []*String{NewString("+")}
		lexpr := NewA_Const(NewString("1"), 0)
		rexpr := NewA_Const(NewString("2"), 2)
		aExpr := NewA_Expr(AEXPR_OP, name, lexpr, rexpr, 1)

		assert.Equal(t, T_A_Expr, aExpr.NodeTag())
		assert.Equal(t, AEXPR_OP, aExpr.Kind)
		assert.Equal(t, 1, len(aExpr.Name))
		assert.Equal(t, "+", aExpr.Name[0].SVal)
		assert.Equal(t, lexpr, aExpr.Lexpr)
		assert.Equal(t, rexpr, aExpr.Rexpr)
		assert.Equal(t, "A_EXPR", aExpr.ExpressionType())
	})

	t.Run("all expression kinds", func(t *testing.T) {
		kinds := []A_Expr_Kind{
			AEXPR_OP, AEXPR_OP_ANY, AEXPR_OP_ALL, AEXPR_DISTINCT,
			AEXPR_NOT_DISTINCT, AEXPR_NULLIF, AEXPR_IN, AEXPR_LIKE,
			AEXPR_ILIKE, AEXPR_SIMILAR, AEXPR_BETWEEN, AEXPR_NOT_BETWEEN,
			AEXPR_BETWEEN_SYM, AEXPR_NOT_BETWEEN_SYM,
		}

		name := []*String{NewString("+")}
		for _, kind := range kinds {
			expr := NewA_Expr(kind, name, nil, nil, 0)
			assert.Equal(t, kind, expr.Kind)
		}
	})
}

func TestA_Const(t *testing.T) {
	t.Run("normal constant", func(t *testing.T) {
		val := NewString("test")
		aConst := NewA_Const(val, 5)
		aConst.SetLocation(5)

		assert.Equal(t, T_A_Const, aConst.NodeTag())
		assert.False(t, aConst.Isnull)
		assert.Equal(t, val, aConst.Val)
		assert.Equal(t, "A_CONST", aConst.ExpressionType())
		assert.Equal(t, "A_Const{String(\"test\")@0}@5", aConst.String())
	})

	t.Run("null constant (NULL)", func(t *testing.T) {
		nullConst := NewA_ConstNull(10)
		nullConst.SetLocation(10)

		assert.True(t, nullConst.Isnull)
		assert.NotNil(t, nullConst.Val) // Contains a Null value node
		assert.Equal(t, "A_Const{NULL}@10", nullConst.String())
	})

	t.Run("with nil value", func(t *testing.T) {
		nilConst := NewA_Const(nil, 15)
		nilConst.SetLocation(15)
		assert.Equal(t, "A_Const{nil}@15", nilConst.String())
	})
}

func TestParamRef(t *testing.T) {
	t.Run("basic construction", func(t *testing.T) {
		paramRef := NewParamRef(3, 8)
		paramRef.SetLocation(8)

		assert.Equal(t, T_ParamRef, paramRef.NodeTag())
		assert.Equal(t, 3, paramRef.Number)
		assert.Equal(t, "PARAM_REF", paramRef.ExpressionType())
		assert.Equal(t, "ParamRef{$3}@8", paramRef.String())
	})

	t.Run("different parameter numbers", func(t *testing.T) {
		for i := 1; i <= 10; i++ {
			param := NewParamRef(i, 0)
			assert.Equal(t, i, param.Number)
		}
	})
}

func TestTypeCast(t *testing.T) {
	t.Run("basic construction", func(t *testing.T) {
		arg := NewA_Const(NewString("123"), 0)
		typeName := NewTypeName([]string{"int"})
		typeCast := NewTypeCast(arg, typeName, 10)
		typeCast.SetLocation(10)

		assert.Equal(t, T_TypeCast, typeCast.NodeTag())
		assert.Equal(t, arg, typeCast.Arg)
		assert.Equal(t, typeName, typeCast.TypeName)
		assert.Equal(t, "TYPE_CAST", typeCast.ExpressionType())
		assert.Equal(t, "TypeCast@10", typeCast.String())
	})
}

func TestFuncCall(t *testing.T) {
	t.Run("basic construction", func(t *testing.T) {
		funcname := []*String{NewString("upper")}
		args := []Node{NewA_Const(NewString("test"), 0)}
		funcCall := NewFuncCall(funcname, args, 15)
		funcCall.SetLocation(15)

		assert.Equal(t, T_FuncCall, funcCall.NodeTag())
		assert.Equal(t, 1, len(funcCall.Funcname))
		assert.Equal(t, "upper", funcCall.Funcname[0].SVal)
		assert.Equal(t, 1, len(funcCall.Args))
		assert.Equal(t, "FUNC_CALL", funcCall.ExpressionType())
		assert.Equal(t, "FuncCall{upper}@15", funcCall.String())
	})

	t.Run("empty funcname", func(t *testing.T) {
		emptyCall := NewFuncCall([]*String{}, []Node{}, 20)
		emptyCall.SetLocation(20)
		assert.Equal(t, "FuncCall@20", emptyCall.String())
	})

	t.Run("aggregate fields", func(t *testing.T) {
		funcCall := NewFuncCall([]*String{NewString("count")}, []Node{}, 0)
		funcCall.AggStar = true
		funcCall.AggDistinct = true
		funcCall.FuncVariadic = true
		funcCall.AggWithinGroup = true

		assert.True(t, funcCall.AggStar)
		assert.True(t, funcCall.AggDistinct)
		assert.True(t, funcCall.FuncVariadic)
		assert.True(t, funcCall.AggWithinGroup)
	})
}

func TestA_Star(t *testing.T) {
	t.Run("basic construction", func(t *testing.T) {
		aStar := NewA_Star(7)
		aStar.SetLocation(7)

		assert.Equal(t, T_A_Star, aStar.NodeTag())
		assert.Equal(t, "A_STAR", aStar.ExpressionType())
		assert.Equal(t, "A_Star@7", aStar.String())
	})
}

func TestA_Indices(t *testing.T) {
	t.Run("single index", func(t *testing.T) {
		idx := NewA_Const(NewString("1"), 0)
		indices := NewA_Indices(idx, 5)
		indices.SetLocation(5)

		assert.Equal(t, T_A_Indices, indices.NodeTag())
		assert.False(t, indices.IsSlice)
		assert.Equal(t, idx, indices.Uidx)
		assert.Nil(t, indices.Lidx)
		assert.Equal(t, "A_INDICES", indices.ExpressionType())
		assert.Equal(t, "A_Indices{index}@5", indices.String())
	})

	t.Run("slice", func(t *testing.T) {
		lidx := NewA_Const(NewString("1"), 0)
		uidx := NewA_Const(NewString("3"), 2)
		slice := NewA_IndicesSlice(lidx, uidx, 10)
		slice.SetLocation(10)

		assert.True(t, slice.IsSlice)
		assert.Equal(t, lidx, slice.Lidx)
		assert.Equal(t, uidx, slice.Uidx)
		assert.Equal(t, "A_Indices{slice}@10", slice.String())
	})
}

func TestA_Indirection(t *testing.T) {
	t.Run("basic construction", func(t *testing.T) {
		arg := NewColumnRef(NewString("table"))
		indirection := []Node{NewString("field")}
		aIndirection := NewA_Indirection(arg, indirection, 12)
		aIndirection.SetLocation(12)

		assert.Equal(t, T_A_Indirection, aIndirection.NodeTag())
		assert.Equal(t, arg, aIndirection.Arg)
		assert.Equal(t, 1, len(aIndirection.Indirection))
		assert.Equal(t, "A_INDIRECTION", aIndirection.ExpressionType())
		assert.Equal(t, "A_Indirection@12", aIndirection.String())
	})
}

func TestA_ArrayExpr(t *testing.T) {
	t.Run("with elements", func(t *testing.T) {
		elements := []Node{
			NewA_Const(NewString("1"), 0),
			NewA_Const(NewString("2"), 2),
			NewA_Const(NewString("3"), 4),
		}
		arrayExpr := NewA_ArrayExpr(elements, 8)
		arrayExpr.SetLocation(8)

		assert.Equal(t, T_A_ArrayExpr, arrayExpr.NodeTag())
		assert.Equal(t, 3, len(arrayExpr.Elements))
		assert.Equal(t, "A_ARRAY_EXPR", arrayExpr.ExpressionType())
		assert.Equal(t, "A_ArrayExpr{3 elements}@8", arrayExpr.String())
	})

	t.Run("empty array", func(t *testing.T) {
		emptyArray := NewA_ArrayExpr([]Node{}, 15)
		emptyArray.SetLocation(15)
		assert.Equal(t, "A_ArrayExpr{0 elements}@15", emptyArray.String())
	})
}

func TestTypeNameUsage(t *testing.T) {
	t.Run("basic construction", func(t *testing.T) {
		names := []string{"int4"}
		typeName := NewTypeName(names)

		assert.Equal(t, T_TypeName, typeName.NodeTag())
		assert.Equal(t, 1, len(typeName.Names))
		assert.Equal(t, "int4", typeName.Names[0])
	})

	t.Run("additional fields", func(t *testing.T) {
		typeName := NewTypeName([]string{"int4"})
		typeName.Setof = true
		typeName.PctType = true

		assert.True(t, typeName.Setof)
		assert.True(t, typeName.PctType)
	})

	t.Run("qualified type name", func(t *testing.T) {
		qualifiedNames := []string{"pg_catalog", "int4"}
		qualifiedType := NewTypeName(qualifiedNames)
		assert.Equal(t, 2, len(qualifiedType.Names))
	})
}

func TestColumnDef(t *testing.T) {
	t.Run("basic construction", func(t *testing.T) {
		typeName := NewTypeName([]string{"varchar"})
		columnDef := NewColumnDef("username", typeName, 30)
		columnDef.SetLocation(30)

		assert.Equal(t, T_ColumnDef, columnDef.NodeTag())
		assert.Equal(t, "username", columnDef.Colname)
		assert.Equal(t, typeName, columnDef.TypeName)
		assert.Equal(t, "COLUMN_DEF", columnDef.StatementType())
		assert.Equal(t, "ColumnDef{username}@30", columnDef.String())
	})

	t.Run("default values", func(t *testing.T) {
		columnDef := NewColumnDef("test", nil, 0)

		assert.Equal(t, 0, columnDef.Inhcount)
		assert.True(t, columnDef.IsLocal)
		assert.False(t, columnDef.IsNotNull)
		assert.False(t, columnDef.IsFromType)
		assert.Equal(t, InvalidOid, columnDef.CollOid)
		assert.Equal(t, 0, len(columnDef.Constraints))
		assert.Equal(t, 0, len(columnDef.Fdwoptions))
	})

	t.Run("setting additional fields", func(t *testing.T) {
		columnDef := NewColumnDef("test", nil, 0)
		columnDef.IsNotNull = true
		columnDef.IsFromType = true

		assert.True(t, columnDef.IsNotNull)
		assert.True(t, columnDef.IsFromType)
	})
}

func TestWithClause(t *testing.T) {
	t.Run("basic construction", func(t *testing.T) {
		ctes := []Node{NewCommonTableExpr("test_cte", NewSelectStmt())}
		withClause := NewWithClause(ctes, false, 35)
		withClause.SetLocation(35)

		assert.Equal(t, T_WithClause, withClause.NodeTag())
		assert.Equal(t, 1, len(withClause.Ctes))
		assert.False(t, withClause.Recursive)
		assert.Equal(t, "WITH_CLAUSE", withClause.ExpressionType())
		assert.Equal(t, "WithClause{1 CTEs}@35", withClause.String())
	})

	t.Run("recursive WITH clause", func(t *testing.T) {
		ctes := []Node{NewCommonTableExpr("test_cte", NewSelectStmt())}
		recursiveWith := NewWithClause(ctes, true, 40)
		recursiveWith.SetLocation(40)

		assert.True(t, recursiveWith.Recursive)
		assert.Equal(t, "WithClause{RECURSIVE, 1 CTEs}@40", recursiveWith.String())
	})
}

func TestMultiAssignRef(t *testing.T) {
	t.Run("basic construction", func(t *testing.T) {
		source := NewA_Const(NewString("test"), 0)
		multiAssign := NewMultiAssignRef(source, 2, 3, 45)
		multiAssign.SetLocation(45)

		assert.Equal(t, T_MultiAssignRef, multiAssign.NodeTag())
		assert.Equal(t, source, multiAssign.Source)
		assert.Equal(t, 2, multiAssign.Colno)
		assert.Equal(t, 3, multiAssign.Ncolumns)
		assert.Equal(t, "MULTI_ASSIGN_REF", multiAssign.ExpressionType())
		assert.Equal(t, "MultiAssignRef{col 2 of 3}@45", multiAssign.String())
	})
}

func TestWindowDef(t *testing.T) {
	t.Run("named window", func(t *testing.T) {
		windowDef := NewWindowDef("mywindow", 50)
		windowDef.SetLocation(50)

		assert.Equal(t, T_WindowDef, windowDef.NodeTag())
		assert.Equal(t, "mywindow", windowDef.Name)
		assert.Equal(t, "WINDOW_DEF", windowDef.StatementType())
		assert.Equal(t, "WindowDef{mywindow}@50", windowDef.String())
	})

	t.Run("default values", func(t *testing.T) {
		windowDef := NewWindowDef("test", 0)

		assert.Equal(t, 0, len(windowDef.PartitionClause))
		assert.Equal(t, 0, len(windowDef.OrderClause))
		assert.Equal(t, 0, windowDef.FrameOptions)
	})

	t.Run("inline window", func(t *testing.T) {
		inlineWindow := NewWindowDef("", 55)
		inlineWindow.SetLocation(55)
		assert.Equal(t, "WindowDef{inline}@55", inlineWindow.String())
	})
}

func TestSortBy(t *testing.T) {
	t.Run("basic construction", func(t *testing.T) {
		node := NewA_Const(NewString("column"), 0)
		sortBy := NewSortBy(node, SORTBY_ASC, SORTBY_NULLS_FIRST, 60)
		sortBy.SetLocation(60)

		assert.Equal(t, T_SortBy, sortBy.NodeTag())
		assert.Equal(t, node, sortBy.Node)
		assert.Equal(t, SORTBY_ASC, sortBy.SortbyDir)
		assert.Equal(t, SORTBY_NULLS_FIRST, sortBy.SortbyNulls)
		assert.Equal(t, "SORT_BY", sortBy.StatementType())
		assert.Equal(t, "SortBy@60", sortBy.String())
	})

	t.Run("all direction values", func(t *testing.T) {
		node := NewA_Const(NewString("column"), 0)
		directions := []SortByDir{SORTBY_DEFAULT, SORTBY_ASC, SORTBY_DESC, SORTBY_USING}

		for _, dir := range directions {
			sort := NewSortBy(node, dir, SORTBY_NULLS_DEFAULT, 0)
			assert.Equal(t, dir, sort.SortbyDir)
		}
	})

	t.Run("all nulls values", func(t *testing.T) {
		node := NewA_Const(NewString("column"), 0)
		nullsOptions := []SortByNulls{SORTBY_NULLS_DEFAULT, SORTBY_NULLS_FIRST, SORTBY_NULLS_LAST}

		for _, nulls := range nullsOptions {
			sort := NewSortBy(node, SORTBY_DEFAULT, nulls, 0)
			assert.Equal(t, nulls, sort.SortbyNulls)
		}
	})
}

func TestGroupingSet(t *testing.T) {
	t.Run("basic construction", func(t *testing.T) {
		content := []Node{NewA_Const(NewString("col1"), 0), NewA_Const(NewString("col2"), 0)}
		groupingSet := NewGroupingSet(GROUPING_SET_SIMPLE, content, 65)
		groupingSet.SetLocation(65)

		assert.Equal(t, T_GroupingSet, groupingSet.NodeTag())
		assert.Equal(t, GROUPING_SET_SIMPLE, groupingSet.Kind)
		assert.Equal(t, 2, len(groupingSet.Content))
		assert.Equal(t, "GROUPING_SET", groupingSet.StatementType())
		assert.Equal(t, "GroupingSet{kind=1}@65", groupingSet.String())
	})

	t.Run("all grouping set kinds", func(t *testing.T) {
		content := []Node{NewA_Const(NewString("col1"), 0)}
		kinds := []GroupingSetKind{
			GROUPING_SET_EMPTY, GROUPING_SET_SIMPLE, GROUPING_SET_ROLLUP,
			GROUPING_SET_CUBE, GROUPING_SET_SETS,
		}

		for i, kind := range kinds {
			gs := NewGroupingSet(kind, content, 0)
			gs.SetLocation(0)
			assert.Equal(t, kind, gs.Kind)

			expectedStr := fmt.Sprintf("GroupingSet{kind=%d}@0", i)
			assert.Equal(t, expectedStr, gs.String())
		}
	})
}

func TestLockingClause(t *testing.T) {
	t.Run("basic construction", func(t *testing.T) {
		lockedRels := []*RangeVar{NewRangeVar("test_table", "", "")}
		lockingClause := NewLockingClause(lockedRels, LCS_FORUPDATE, LockWaitBlock, 70)
		lockingClause.SetLocation(70)

		assert.Equal(t, T_LockingClause, lockingClause.NodeTag())
		assert.Equal(t, 1, len(lockingClause.LockedRels))
		assert.Equal(t, LCS_FORUPDATE, lockingClause.Strength)
		assert.Equal(t, LockWaitBlock, lockingClause.WaitPolicy)
		assert.Equal(t, "LOCKING_CLAUSE", lockingClause.StatementType())
		assert.Equal(t, "LockingClause{strength=4}@70", lockingClause.String())
	})

	t.Run("all strength values", func(t *testing.T) {
		lockedRels := []*RangeVar{NewRangeVar("test_table", "", "")}
		strengths := []LockClauseStrength{
			LCS_NONE, LCS_FORKEYSHARE, LCS_FORSHARE, LCS_FORNOKEYUPDATE, LCS_FORUPDATE,
		}

		for i, strength := range strengths {
			lc := NewLockingClause(lockedRels, strength, LockWaitBlock, 0)
			lc.SetLocation(0)
			assert.Equal(t, strength, lc.Strength)

			expectedStr := fmt.Sprintf("LockingClause{strength=%d}@0", i)
			assert.Equal(t, expectedStr, lc.String())
		}
	})

	t.Run("all wait policies", func(t *testing.T) {
		lockedRels := []*RangeVar{NewRangeVar("test_table", "", "")}
		policies := []LockWaitPolicy{LockWaitBlock, LockWaitSkip, LockWaitError}

		for _, policy := range policies {
			lc := NewLockingClause(lockedRels, LCS_FORUPDATE, policy, 0)
			assert.Equal(t, policy, lc.WaitPolicy)
		}
	})
}

func TestXmlSerialize(t *testing.T) {
	t.Run("basic construction", func(t *testing.T) {
		expr := NewA_Const(NewString("xml_data"), 0)
		typeName := NewTypeName([]string{"text"})
		xmlSerialize := NewXmlSerialize(XMLOPTION_DOCUMENT, expr, typeName, true, 75)
		xmlSerialize.SetLocation(75)

		assert.Equal(t, T_XmlSerialize, xmlSerialize.NodeTag())
		assert.Equal(t, XMLOPTION_DOCUMENT, xmlSerialize.XmlOptionType)
		assert.Equal(t, expr, xmlSerialize.Expr)
		assert.Equal(t, typeName, xmlSerialize.TypeName)
		assert.True(t, xmlSerialize.Indent)
		assert.Equal(t, "XML_SERIALIZE", xmlSerialize.ExpressionType())
		assert.Equal(t, "XmlSerialize@75", xmlSerialize.String())
	})

	t.Run("both XML option types", func(t *testing.T) {
		expr := NewA_Const(NewString("xml_data"), 0)
		typeName := NewTypeName([]string{"text"})
		options := []XmlOptionType{XMLOPTION_DOCUMENT, XMLOPTION_CONTENT}

		for _, option := range options {
			xs := NewXmlSerialize(option, expr, typeName, false, 0)
			assert.Equal(t, option, xs.XmlOptionType)
		}
	})
}

func TestPartitionElem(t *testing.T) {
	t.Run("with column name", func(t *testing.T) {
		partitionElem := NewPartitionElem("column1", nil, 80)
		partitionElem.SetLocation(80)

		assert.Equal(t, T_PartitionElem, partitionElem.NodeTag())
		assert.Equal(t, "column1", partitionElem.Name)
		assert.Nil(t, partitionElem.Expr)
		assert.Equal(t, "PARTITION_ELEM", partitionElem.StatementType())
		assert.Equal(t, "PartitionElem{column1}@80", partitionElem.String())
	})

	t.Run("with expression", func(t *testing.T) {
		expr := NewA_Const(NewString("expr"), 0)
		exprPartition := NewPartitionElem("", expr, 85)
		exprPartition.SetLocation(85)

		assert.Equal(t, "", exprPartition.Name)
		assert.Equal(t, expr, exprPartition.Expr)
		assert.Equal(t, "PartitionElem{expr}@85", exprPartition.String())
	})

	t.Run("default values", func(t *testing.T) {
		partitionElem := NewPartitionElem("col", nil, 0)

		assert.Equal(t, 0, len(partitionElem.Collation))
		assert.Equal(t, 0, len(partitionElem.Opclass))
	})
}

func TestTableSampleClause(t *testing.T) {
	t.Run("basic construction", func(t *testing.T) {
		args := []Node{NewA_Const(NewString("10"), 0)}
		repeatable := NewA_Const(NewString("42"), 0)
		tableSample := NewTableSampleClause(123, args, repeatable, 90)
		tableSample.SetLocation(90)

		assert.Equal(t, T_TableSampleClause, tableSample.NodeTag())
		assert.Equal(t, Oid(123), tableSample.Tsmhandler)
		assert.Equal(t, 1, len(tableSample.Args))
		assert.Equal(t, repeatable, tableSample.Repeatable)
		assert.Equal(t, "TABLE_SAMPLE_CLAUSE", tableSample.StatementType())
		assert.Equal(t, "TableSampleClause@90", tableSample.String())
	})
}

func TestObjectWithArgs(t *testing.T) {
	t.Run("basic construction", func(t *testing.T) {
		objname := []*String{NewString("public"), NewString("my_function")}
		objargs := []Node{NewTypeName([]string{"int"})}
		objectWithArgs := NewObjectWithArgs(objname, objargs, false, 95)
		objectWithArgs.SetLocation(95)

		assert.Equal(t, T_ObjectWithArgs, objectWithArgs.NodeTag())
		assert.Equal(t, 2, len(objectWithArgs.Objname))
		assert.Equal(t, "my_function", objectWithArgs.Objname[1].SVal)
		assert.Equal(t, 1, len(objectWithArgs.Objargs))
		assert.False(t, objectWithArgs.ArgsUnspecified)
		assert.Equal(t, "OBJECT_WITH_ARGS", objectWithArgs.StatementType())
		assert.Equal(t, "ObjectWithArgs{my_function}@95", objectWithArgs.String())
	})

	t.Run("with unspecified args", func(t *testing.T) {
		objname := []*String{NewString("func")}
		unspecifiedObj := NewObjectWithArgs(objname, []Node{}, true, 100)
		assert.True(t, unspecifiedObj.ArgsUnspecified)
	})

	t.Run("empty name", func(t *testing.T) {
		emptyObj := NewObjectWithArgs([]*String{}, []Node{}, false, 105)
		emptyObj.SetLocation(105)
		assert.Equal(t, "ObjectWithArgs@105", emptyObj.String())
	})

	t.Run("default values", func(t *testing.T) {
		objectWithArgs := NewObjectWithArgs([]*String{}, []Node{}, false, 0)
		assert.Equal(t, 0, len(objectWithArgs.ObjfuncArgs))
	})
}

func TestSinglePartitionSpec(t *testing.T) {
	t.Run("basic construction", func(t *testing.T) {
		singlePartition := NewSinglePartitionSpec(115)
		singlePartition.SetLocation(115)

		assert.Equal(t, T_SinglePartitionSpec, singlePartition.NodeTag())
		assert.Equal(t, "SINGLE_PARTITION_SPEC", singlePartition.StatementType())
		assert.Equal(t, "SinglePartitionSpec@115", singlePartition.String())
	})
}

func TestPartitionCmd(t *testing.T) {
	t.Run("basic construction", func(t *testing.T) {
		name := NewRangeVar("partition1", "", "")
		bound := &PartitionBoundSpec{}
		partitionCmd := NewPartitionCmd(name, bound, true, 120)
		partitionCmd.SetLocation(120)

		assert.Equal(t, T_PartitionCmd, partitionCmd.NodeTag())
		assert.Equal(t, name, partitionCmd.Name)
		assert.Equal(t, bound, partitionCmd.Bound)
		assert.True(t, partitionCmd.Concurrent)
		assert.Equal(t, "PARTITION_CMD", partitionCmd.StatementType())
		assert.Equal(t, "PartitionCmd@120", partitionCmd.String())
	})

	t.Run("with concurrent false", func(t *testing.T) {
		name := NewRangeVar("partition1", "", "")
		bound := &PartitionBoundSpec{}
		nonConcurrent := NewPartitionCmd(name, bound, false, 125)
		assert.False(t, nonConcurrent.Concurrent)
	})
}

func TestConstants(t *testing.T) {
	t.Run("InvalidOid", func(t *testing.T) {
		assert.Equal(t, Oid(0), InvalidOid)
	})

	t.Run("char type", func(t *testing.T) {
		var c char = 65
		assert.Equal(t, byte(65), byte(c))
	})
}

// Integration tests to ensure nodes work with existing interfaces
func TestParseInfrastructureNodeInterfaces(t *testing.T) {
	t.Run("Node interface implementation", func(t *testing.T) {
		// Test that all nodes implement Node interface
		nodes := []Node{
			NewRawStmt(NewSelectStmt(), 0, 10),
			NewA_Expr(AEXPR_OP, []*String{NewString("+")}, nil, nil, 0),
			NewA_Const(NewString("test"), 0),
			NewParamRef(1, 0),
			NewTypeCast(nil, nil, 0),
			NewFuncCall([]*String{NewString("func")}, []Node{}, 0),
			NewA_Star(0),
			NewA_Indices(nil, 0),
			NewA_Indirection(nil, []Node{}, 0),
			NewA_ArrayExpr([]Node{}, 0),
			NewTypeName([]string{"int"}),
			NewColumnDef("col", nil, 0),
			NewWithClause([]Node{}, false, 0),
			NewMultiAssignRef(nil, 1, 2, 0),
			NewWindowDef("win", 0),
			NewSortBy(nil, SORTBY_ASC, SORTBY_NULLS_DEFAULT, 0),
			NewGroupingSet(GROUPING_SET_SIMPLE, []Node{}, 0),
			NewLockingClause([]*RangeVar{}, LCS_FORUPDATE, LockWaitBlock, 0),
			NewXmlSerialize(XMLOPTION_DOCUMENT, nil, nil, false, 0),
			NewPartitionElem("col", nil, 0),
			NewTableSampleClause(0, []Node{}, nil, 0),
			NewObjectWithArgs([]*String{}, []Node{}, false, 0),
			NewSinglePartitionSpec(0),
			NewPartitionCmd(nil, nil, false, 0),
		}

		for i, node := range nodes {
			assert.NotNil(t, node, "Node %d should not be nil", i)

			// Test Node interface methods
			tag := node.NodeTag()
			assert.NotEqual(t, T_Invalid, tag, "Node %d should not have invalid tag", i)

			location := node.Location()
			assert.GreaterOrEqual(t, location, -1, "Node %d should have valid location", i)

			str := node.String()
			assert.NotEmpty(t, str, "Node %d should have non-empty string representation", i)
		}
	})

	t.Run("Expression interface implementation", func(t *testing.T) {
		// Test that expression nodes implement Expression interface
		expressions := []Expression{
			NewA_Expr(AEXPR_OP, []*String{NewString("+")}, nil, nil, 0),
			NewA_Const(NewString("test"), 0),
			NewParamRef(1, 0),
			NewTypeCast(nil, nil, 0),
			NewFuncCall([]*String{NewString("func")}, []Node{}, 0),
			NewA_Star(0),
			NewA_Indices(nil, 0),
			NewA_Indirection(nil, []Node{}, 0),
			NewA_ArrayExpr([]Node{}, 0),
			NewWithClause([]Node{}, false, 0),
			NewMultiAssignRef(nil, 1, 2, 0),
			NewXmlSerialize(XMLOPTION_DOCUMENT, nil, nil, false, 0),
		}

		for i, expr := range expressions {
			exprType := expr.ExpressionType()
			assert.NotEmpty(t, exprType, "Expression %d should have non-empty expression type", i)
		}
	})

	t.Run("Statement interface implementation", func(t *testing.T) {
		// Test that statement nodes implement Stmt interface
		statements := []Stmt{
			NewRawStmt(NewSelectStmt(), 0, 10),
			NewColumnDef("col", nil, 0),
			NewWindowDef("win", 0),
			NewSortBy(nil, SORTBY_ASC, SORTBY_NULLS_DEFAULT, 0),
			NewGroupingSet(GROUPING_SET_SIMPLE, []Node{}, 0),
			NewLockingClause([]*RangeVar{}, LCS_FORUPDATE, LockWaitBlock, 0),
			NewPartitionElem("col", nil, 0),
			NewTableSampleClause(0, []Node{}, nil, 0),
			NewObjectWithArgs([]*String{}, []Node{}, false, 0),
			NewSinglePartitionSpec(0),
			NewPartitionCmd(nil, nil, false, 0),
		}

		for i, stmt := range statements {
			stmtType := stmt.StatementType()
			assert.NotEmpty(t, stmtType, "Statement %d should have non-empty statement type", i)
		}
	})
}
