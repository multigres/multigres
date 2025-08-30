// Package ast provides PostgreSQL AST expression node tests.
package ast

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSupportingTypes tests the supporting types for expressions.
func TestSupportingTypes(t *testing.T) {
	t.Run("BoolExprType", func(t *testing.T) {
		tests := []struct {
			boolType BoolExprType
			expected string
		}{
			{AND_EXPR, "AND"},
			{OR_EXPR, "OR"},
			{NOT_EXPR, "NOT"},
		}

		for _, tt := range tests {
			assert.Equal(t, tt.expected, tt.boolType.String())
		}
	})

	t.Run("ParamKind", func(t *testing.T) {
		// Test that ParamKind constants are defined
		assert.Equal(t, PARAM_EXTERN, ParamKind(0))
		assert.Equal(t, PARAM_EXEC, ParamKind(1))
		assert.Equal(t, PARAM_SUBLINK, ParamKind(2))
		assert.Equal(t, PARAM_MULTIEXPR, ParamKind(3))
	})

	t.Run("CoercionForm", func(t *testing.T) {
		// Test that CoercionForm constants are defined
		assert.Equal(t, COERCE_EXPLICIT_CALL, CoercionForm(0))
		assert.Equal(t, COERCE_EXPLICIT_CAST, CoercionForm(1))
		assert.Equal(t, COERCE_IMPLICIT_CAST, CoercionForm(2))
		assert.Equal(t, COERCE_SQL_SYNTAX, CoercionForm(3))
	})
}

// TestVar tests the Var expression node.
func TestVar(t *testing.T) {
	varno := int(1)
	varattno := AttrNumber(2)
	vartype := Oid(23) // int4 in PostgreSQL

	v := NewVar(varno, varattno, vartype)

	require.NotNil(t, v)
	assert.Equal(t, T_Var, v.NodeTag())
	assert.Equal(t, "Var", v.ExpressionType())
	assert.True(t, v.IsExpr())
	assert.Equal(t, varno, v.Varno)
	assert.Equal(t, varattno, v.Varattno)
	assert.Equal(t, vartype, v.Vartype)
	assert.Contains(t, v.String(), "Var(1.2)")
}

// TestConst tests the Const expression node.
func TestConst(t *testing.T) {
	t.Run("NonNull", func(t *testing.T) {
		consttype := Oid(23) // int4
		constvalue := Datum(42)

		c := NewConst(consttype, constvalue, false)

		require.NotNil(t, c)
		assert.Equal(t, T_Const, c.NodeTag())
		assert.Equal(t, "Const", c.ExpressionType())
		assert.True(t, c.IsExpr())
		assert.Equal(t, consttype, c.Consttype)
		assert.Equal(t, constvalue, c.Constvalue)
		assert.False(t, c.Constisnull)
		assert.Contains(t, c.String(), "42")
	})

	t.Run("Null", func(t *testing.T) {
		consttype := Oid(23)

		c := NewConst(consttype, 0, true)

		require.NotNil(t, c)
		assert.True(t, c.Constisnull)
		assert.Contains(t, c.String(), "NULL")
	})
}

// TestParam tests the Param expression node.
func TestParam(t *testing.T) {
	paramkind := PARAM_EXTERN
	paramid := 1
	paramtype := Oid(23)

	p := NewParam(paramkind, paramid, paramtype)

	require.NotNil(t, p)
	assert.Equal(t, T_Param, p.NodeTag())
	assert.Equal(t, "Param", p.ExpressionType())
	assert.True(t, p.IsExpr())
	assert.Equal(t, paramkind, p.Paramkind)
	assert.Equal(t, paramid, p.Paramid)
	assert.Equal(t, paramtype, p.Paramtype)
	assert.Contains(t, p.String(), "$1")
}

// TestFuncExpr tests the FuncExpr expression node.
func TestFuncExpr(t *testing.T) {
	funcid := Oid(100)
	funcresulttype := Oid(23)
	arg1 := NewVar(1, 1, 23)
	arg2 := NewConst(23, 42, false)
	args := NewNodeList(arg1, arg2)

	f := NewFuncExpr(funcid, funcresulttype, args)

	require.NotNil(t, f)
	assert.Equal(t, T_FuncExpr, f.NodeTag())
	assert.Equal(t, "FuncExpr", f.ExpressionType())
	assert.True(t, f.IsExpr())
	assert.Equal(t, funcid, f.Funcid)
	assert.Equal(t, funcresulttype, f.Funcresulttype)
	assert.Equal(t, 2, f.Args.Len())
	assert.Equal(t, arg1, f.Args.Items[0])
	assert.Equal(t, arg2, f.Args.Items[1])
	assert.Contains(t, f.String(), "FuncExpr")
	assert.Contains(t, f.String(), "2 args")
}

// TestOpExpr tests the OpExpr expression node.
func TestOpExpr(t *testing.T) {
	t.Run("BinaryOperator", func(t *testing.T) {
		opno := Oid(96)         // "=" operator in PostgreSQL
		opfuncid := Oid(65)     // int4eq function
		opresulttype := Oid(16) // bool
		left := NewVar(1, 1, 23)
		right := NewConst(23, 42, false)
		args := NewNodeList(left, right)

		o := NewOpExpr(opno, opfuncid, opresulttype, args)

		require.NotNil(t, o)
		assert.Equal(t, T_OpExpr, o.NodeTag())
		assert.Equal(t, "OpExpr", o.ExpressionType())
		assert.True(t, o.IsExpr())
		assert.Equal(t, opno, o.Opno)
		assert.Equal(t, opfuncid, o.Opfuncid)
		assert.Equal(t, opresulttype, o.Opresulttype)
		assert.Equal(t, 2, o.Args.Len())
		assert.Contains(t, o.String(), "binary")
	})

	t.Run("UnaryOperator", func(t *testing.T) {
		opno := Oid(484) // "-" unary minus
		arg := NewVar(1, 1, 23)

		o := NewUnaryOp(opno, arg)

		require.NotNil(t, o)
		assert.Equal(t, 1, o.Args.Len())
		assert.Contains(t, o.String(), "unary")
	})

	t.Run("ConvenienceConstructor", func(t *testing.T) {
		opno := Oid(96)
		left := NewVar(1, 1, 23)
		right := NewConst(23, 42, false)

		o := NewBinaryOp(opno, left, right)

		require.NotNil(t, o)
		assert.Equal(t, opno, o.Opno)
		assert.Equal(t, 2, o.Args.Len())
		assert.Equal(t, left, o.Args.Items[0])
		assert.Equal(t, right, o.Args.Items[1])
	})
}

// TestBoolExpr tests the BoolExpr expression node.
func TestBoolExpr(t *testing.T) {
	t.Run("AndExpression", func(t *testing.T) {
		left := NewVar(1, 1, 16)  // bool column
		right := NewVar(1, 2, 16) // bool column
		args := NewNodeList(left, right)

		b := NewBoolExpr(AND_EXPR, args)

		require.NotNil(t, b)
		assert.Equal(t, T_BoolExpr, b.NodeTag())
		assert.Equal(t, "BoolExpr", b.ExpressionType())
		assert.True(t, b.IsExpr())
		assert.Equal(t, AND_EXPR, b.Boolop)
		assert.Equal(t, 2, b.Args.Len())
		assert.Contains(t, b.String(), "AND")
	})

	t.Run("OrExpression", func(t *testing.T) {
		left := NewVar(1, 1, 16)
		right := NewVar(1, 2, 16)

		b := NewOrExpr(left, right)

		require.NotNil(t, b)
		assert.Equal(t, OR_EXPR, b.Boolop)
		assert.Contains(t, b.String(), "OR")
	})

	t.Run("NotExpression", func(t *testing.T) {
		arg := NewVar(1, 1, 16)

		b := NewNotExpr(arg)

		require.NotNil(t, b)
		assert.Equal(t, NOT_EXPR, b.Boolop)
		assert.Equal(t, 1, b.Args.Len())
		assert.Contains(t, b.String(), "NOT")
	})

	t.Run("ConvenienceConstructor", func(t *testing.T) {
		left := NewVar(1, 1, 16)
		right := NewVar(1, 2, 16)

		and := NewAndExpr(left, right)
		require.NotNil(t, and)
		assert.Equal(t, AND_EXPR, and.Boolop)
		assert.Equal(t, 2, and.Args.Len())
	})
}

// TestExpressionInterfaces tests that expressions implement required interfaces.
func TestExpressionInterfaces(t *testing.T) {
	expressions := []struct {
		name string
		expr Expr
	}{
		{"Var", NewVar(1, 1, 23)},
		{"Const", NewConst(23, 42, false)},
		{"Param", NewParam(PARAM_EXTERN, 1, 23)},
		{"FuncExpr", NewFuncExpr(100, 23, NewNodeList())},
		{"OpExpr", NewOpExpr(96, 65, 16, NewNodeList())},
		{"BoolExpr", NewBoolExpr(AND_EXPR, NewNodeList())},
	}

	for _, tt := range expressions {
		t.Run(tt.name, func(t *testing.T) {
			// Test Expr interface
			assert.True(t, tt.expr.IsExpr())
			assert.NotEmpty(t, tt.expr.ExpressionType())

			// Test Node interface
			var node Node = tt.expr
			assert.NotNil(t, node)
			assert.True(t, int(node.NodeTag()) > 0)
			assert.NotEmpty(t, node.String())
		})
	}
}

// TestExpressionUtilities tests the utility functions.
func TestExpressionUtilities(t *testing.T) {
	t.Run("IsConstant", func(t *testing.T) {
		constExpr := NewConst(23, 42, false)
		varExpr := NewVar(1, 1, 23)

		assert.True(t, IsConstant(constExpr))
		assert.False(t, IsConstant(varExpr))
	})

	t.Run("IsVariable", func(t *testing.T) {
		varExpr := NewVar(1, 1, 23)
		constExpr := NewConst(23, 42, false)

		assert.True(t, IsVariable(varExpr))
		assert.False(t, IsVariable(constExpr))
	})

	t.Run("IsFunction", func(t *testing.T) {
		funcExpr := NewFuncExpr(100, 23, NewNodeList())
		varExpr := NewVar(1, 1, 23)

		assert.True(t, IsFunction(funcExpr))
		assert.False(t, IsFunction(varExpr))
	})

	t.Run("GetExpressionArgs", func(t *testing.T) {
		arg1 := NewVar(1, 1, 23)
		arg2 := NewConst(23, 42, false)

		funcExpr := NewFuncExpr(100, 23, NewNodeList(arg1, arg2))
		args := GetExpressionArgs(funcExpr)
		assert.Equal(t, 2, args.Len())
		assert.Equal(t, arg1, args.Items[0])
		assert.Equal(t, arg2, args.Items[1])

		opExpr := NewBinaryOp(96, arg1, arg2)
		args = GetExpressionArgs(opExpr)
		assert.Equal(t, 2, args.Len())

		boolExpr := NewAndExpr(arg1, arg2)
		args = GetExpressionArgs(boolExpr)
		assert.Equal(t, 2, args.Len())

		varExpr := NewVar(1, 1, 23)
		args = GetExpressionArgs(varExpr)
		assert.Nil(t, args)
	})

	t.Run("CountArgs", func(t *testing.T) {
		arg1 := NewVar(1, 1, 23)
		arg2 := NewConst(23, 42, false)

		funcExpr := NewFuncExpr(100, 23, NewNodeList(arg1, arg2))
		assert.Equal(t, 2, CountArgs(funcExpr))

		unaryOp := NewUnaryOp(484, arg1)
		assert.Equal(t, 1, CountArgs(unaryOp))

		varExpr := NewVar(1, 1, 23)
		assert.Equal(t, 0, CountArgs(varExpr))
	})
}

// TestGetExprTag tests the expression tag mapping function.
func TestGetExprTag(t *testing.T) {
	tests := []struct {
		exprType string
		expected NodeTag
	}{
		{"Var", T_Var},
		{"Const", T_Const},
		{"Param", T_Param},
		{"FuncExpr", T_FuncExpr},
		{"OpExpr", T_OpExpr},
		{"BoolExpr", T_BoolExpr},
		{"Unknown", T_Invalid},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, GetExprTag(tt.exprType))
	}
}

// TestComplexExpressionTrees tests building complex expression trees.
func TestComplexExpressionTrees(t *testing.T) {
	t.Run("NestedBooleanExpression", func(t *testing.T) {
		// Build: (col1 = 42) AND (col2 > 10) OR (col3 IS NOT NULL)

		// col1 = 42
		col1 := NewVar(1, 1, 23)
		val42 := NewConst(23, 42, false)
		eq := NewBinaryOp(96, col1, val42)

		// col2 > 10
		col2 := NewVar(1, 2, 23)
		val10 := NewConst(23, 10, false)
		gt := NewBinaryOp(521, col2, val10) // ">" operator

		// (col1 = 42) AND (col2 > 10)
		and := NewAndExpr(eq, gt)

		// col3 IS NOT NULL
		col3 := NewVar(1, 3, 23)
		notNull := NewUnaryOp(1295, col3) // IS NOT NULL

		// Final OR expression
		or := NewOrExpr(and, notNull)

		require.NotNil(t, or)
		assert.Equal(t, T_BoolExpr, or.NodeTag())
		assert.Equal(t, OR_EXPR, or.Boolop)
		assert.Equal(t, 2, or.Args.Len())

		// Verify structure
		leftArg := or.Args.Items[0].(*BoolExpr)
		assert.Equal(t, AND_EXPR, leftArg.Boolop)
		assert.Equal(t, 2, leftArg.Args.Len())
	})

	t.Run("FunctionWithMultipleArgs", func(t *testing.T) {
		// Build: SUBSTRING(col1, 1, 5)

		stringCol := NewVar(1, 1, 25) // text column
		startPos := NewConst(23, 1, false)
		length := NewConst(23, 5, false)

		substring := NewFuncExpr(883, 25, NewNodeList(stringCol, startPos, length))

		require.NotNil(t, substring)
		assert.Equal(t, T_FuncExpr, substring.NodeTag())
		assert.Equal(t, 3, substring.Args.Len())
		assert.Equal(t, Oid(883), substring.Funcid)        // SUBSTRING function
		assert.Equal(t, Oid(25), substring.Funcresulttype) // text result
	})
}

// ==============================================================================
// TIER 2 EXPRESSION TESTS
// ==============================================================================

// TestCaseExpr tests the CaseExpr expression node.
func TestCaseExpr(t *testing.T) {
	t.Run("SimpleCase", func(t *testing.T) {
		// CASE status WHEN 1 THEN 'Active' WHEN 2 THEN 'Inactive' ELSE 'Unknown' END
		statusVar := NewVar(1, 1, 23) // status column
		when1 := NewCaseWhen(NewConst(23, 1, false), NewString("Active"))
		when2 := NewCaseWhen(NewConst(23, 2, false), NewString("Inactive"))
		elseResult := NewString("Unknown")

		caseExpr := NewCaseExpr(25, statusVar, NewNodeList(when1, when2), elseResult)

		require.NotNil(t, caseExpr)
		assert.Equal(t, T_CaseExpr, caseExpr.NodeTag())
		assert.Equal(t, "CaseExpr", caseExpr.ExpressionType())
		assert.True(t, caseExpr.IsExpr())
		assert.Equal(t, statusVar, caseExpr.Arg)
		assert.Equal(t, 2, caseExpr.Args.Len())
		assert.Equal(t, elseResult, caseExpr.Defresult)
		assert.Contains(t, caseExpr.String(), "2 whens")
		assert.Contains(t, caseExpr.String(), "else:true")
	})

	t.Run("SearchedCase", func(t *testing.T) {
		// CASE WHEN age < 18 THEN 'Minor' WHEN age >= 65 THEN 'Senior' ELSE 'Adult' END
		ageVar := NewVar(1, 2, 23)
		when1 := NewCaseWhen(NewBinaryOp(97, ageVar, NewConst(23, 18, false)), NewString("Minor"))
		when2 := NewCaseWhen(NewBinaryOp(525, ageVar, NewConst(23, 65, false)), NewString("Senior"))
		elseResult := NewString("Adult")

		caseExpr := NewSearchedCase(NewNodeList(when1, when2), elseResult)

		require.NotNil(t, caseExpr)
		assert.Nil(t, caseExpr.Arg) // No implicit comparison argument
		assert.Equal(t, 2, caseExpr.Args.Len())
		assert.NotNil(t, caseExpr.Defresult)
	})

	t.Run("ConvenienceConstructor", func(t *testing.T) {
		statusVar := NewVar(1, 1, 23)
		whens := NewNodeList(NewCaseWhen(NewConst(23, 1, false), NewString("Active")))
		elseResult := NewString("Unknown")

		simpleCase := NewSimpleCase(statusVar, whens, elseResult)
		require.NotNil(t, simpleCase)
		assert.Equal(t, statusVar, simpleCase.Arg)

		searchedCase := NewSearchedCase(whens, elseResult)
		require.NotNil(t, searchedCase)
		assert.Nil(t, searchedCase.Arg)
	})
}

// TestCaseWhen tests the CaseWhen expression node.
func TestCaseWhen(t *testing.T) {
	condition := NewBinaryOp(96, NewVar(1, 1, 23), NewConst(23, 42, false))
	result := NewString("matched")

	when := NewCaseWhen(condition, result)

	require.NotNil(t, when)
	assert.Equal(t, "CaseWhen", when.ExpressionType())
	assert.True(t, when.IsExpr())
	assert.Equal(t, condition, when.Expr)
	assert.Equal(t, result, when.Result)
	assert.Contains(t, when.String(), "CaseWhen")
}

// TestCoalesceExpr tests the CoalesceExpr expression node.
func TestCoalesceExpr(t *testing.T) {
	// COALESCE(col1, col2, 'default')
	col1 := NewVar(1, 1, 25) // text column
	col2 := NewVar(1, 2, 25) // text column
	defaultVal := NewString("default")
	args := NewNodeList(col1, col2, defaultVal)

	coalesce := NewCoalesceExpr(25, args)

	require.NotNil(t, coalesce)
	assert.Equal(t, T_CoalesceExpr, coalesce.NodeTag())
	assert.Equal(t, "CoalesceExpr", coalesce.ExpressionType())
	assert.True(t, coalesce.IsExpr())
	assert.Equal(t, Oid(25), coalesce.Coalescetype)
	assert.Equal(t, 3, coalesce.Args.Len())
	assert.Equal(t, col1, coalesce.Args.Items[0])
	assert.Equal(t, col2, coalesce.Args.Items[1])
	assert.Equal(t, defaultVal, coalesce.Args.Items[2])
	assert.Contains(t, coalesce.String(), "3 args")
}

// TestArrayExpr tests the ArrayExpr expression node.
func TestArrayExpr(t *testing.T) {
	t.Run("SimpleArray", func(t *testing.T) {
		// ARRAY[1, 2, 3]
		elem1 := NewConst(23, 1, false)
		elem2 := NewConst(23, 2, false)
		elem3 := NewConst(23, 3, false)
		elements := NewNodeList(elem1, elem2, elem3)

		array := NewArrayExpr(1007, 23, elements) // int4[] type

		require.NotNil(t, array)
		assert.Equal(t, T_ArrayExpr, array.NodeTag())
		assert.Equal(t, "ArrayExpr", array.ExpressionType())
		assert.True(t, array.IsExpr())
		assert.Equal(t, Oid(1007), array.ArrayTypeid)
		assert.Equal(t, Oid(23), array.ElementTypeid)
		assert.Equal(t, 3, array.Elements.Len())
		assert.False(t, array.Multidims)
		assert.Contains(t, array.String(), "3 elements")
		assert.Contains(t, array.String(), "1D")
	})

	t.Run("MultiDimArray", func(t *testing.T) {
		elements := NewNodeList(NewConst(23, 1, false))
		array := NewArrayExpr(1007, 23, elements)
		array.Multidims = true

		require.NotNil(t, array)
		assert.True(t, array.Multidims)
		assert.Contains(t, array.String(), "Multi-D")
	})

	t.Run("ConvenienceConstructor", func(t *testing.T) {
		elements := NewNodeList(NewConst(23, 1, false), NewConst(23, 2, false))
		array := NewArrayConstructor(elements)

		require.NotNil(t, array)
		assert.Equal(t, 2, array.Elements.Len())
		assert.Equal(t, Oid(0), array.ArrayTypeid) // Default unspecified
	})
}

// TestScalarArrayOpExpr tests the ScalarArrayOpExpr expression node.
func TestScalarArrayOpExpr(t *testing.T) {
	t.Run("AnyOperation", func(t *testing.T) {
		// column = ANY(ARRAY[1, 2, 3])
		column := NewVar(1, 1, 23)
		array := NewArrayConstructor(NewNodeList(NewConst(23, 1, false), NewConst(23, 2, false), NewConst(23, 3, false)))

		anyExpr := NewScalarArrayOpExpr(96, true, column, array) // "=" operator with ANY

		require.NotNil(t, anyExpr)
		assert.Equal(t, T_ScalarArrayOpExpr, anyExpr.NodeTag())
		assert.Equal(t, "ScalarArrayOpExpr", anyExpr.ExpressionType())
		assert.True(t, anyExpr.IsExpr())
		assert.Equal(t, Oid(96), anyExpr.Opno)
		assert.True(t, anyExpr.UseOr)
		assert.Equal(t, 2, anyExpr.Args.Len())
		assert.Equal(t, column, anyExpr.Args.Items[0])
		assert.Equal(t, array, anyExpr.Args.Items[1])
		assert.Contains(t, anyExpr.String(), "ANY")
	})

	t.Run("AllOperation", func(t *testing.T) {
		// column <> ALL(ARRAY[1, 2, 3])
		column := NewVar(1, 1, 23)
		array := NewArrayConstructor(NewNodeList(NewConst(23, 1, false)))

		allExpr := NewScalarArrayOpExpr(518, false, column, array) // "<>" operator with ALL

		require.NotNil(t, allExpr)
		assert.Equal(t, Oid(518), allExpr.Opno)
		assert.False(t, allExpr.UseOr)
		assert.Contains(t, allExpr.String(), "ALL")
	})

	t.Run("ConvenienceConstructors", func(t *testing.T) {
		column := NewVar(1, 1, 23)
		array := NewArrayConstructor(NewNodeList(NewConst(23, 1, false)))

		// Test IN expression
		inExpr := NewInExpr(column, array)
		require.NotNil(t, inExpr)
		assert.Equal(t, Oid(96), inExpr.Opno) // "=" operator
		assert.True(t, inExpr.UseOr)

		// Test NOT IN expression
		notInExpr := NewNotInExpr(column, array)
		require.NotNil(t, notInExpr)
		assert.Equal(t, Oid(518), notInExpr.Opno) // "<>" operator
		assert.False(t, notInExpr.UseOr)

		// Test generic ANY/ALL
		anyExpr := NewAnyExpr(521, column, array) // ">" operator
		assert.Equal(t, Oid(521), anyExpr.Opno)
		assert.True(t, anyExpr.UseOr)

		allExpr := NewAllExpr(521, column, array)
		assert.Equal(t, Oid(521), allExpr.Opno)
		assert.False(t, allExpr.UseOr)
	})
}

// TestRowExpr tests the RowExpr expression node.
func TestRowExpr(t *testing.T) {
	t.Run("BasicRow", func(t *testing.T) {
		// ROW(col1, col2, 42)
		col1 := NewVar(1, 1, 23)
		col2 := NewVar(1, 2, 25)
		literal := NewConst(23, 42, false)
		fields := NewNodeList(col1, col2, literal)

		row := NewRowExpr(fields, 2249) // record type OID

		require.NotNil(t, row)
		assert.Equal(t, T_RowExpr, row.NodeTag())
		assert.Equal(t, "RowExpr", row.ExpressionType())
		assert.True(t, row.IsExpr())
		assert.Equal(t, Oid(2249), row.RowTypeid)
		assert.Equal(t, 3, row.Args.Len())
		assert.Equal(t, col1, row.Args.Items[0])
		assert.Equal(t, col2, row.Args.Items[1])
		assert.Equal(t, literal, row.Args.Items[2])
		assert.Contains(t, row.String(), "3 fields")
	})

	t.Run("ConvenienceConstructor", func(t *testing.T) {
		fields := NewNodeList(NewConst(23, 1, false), NewConst(23, 2, false))
		row := NewRowConstructor(fields)

		require.NotNil(t, row)
		assert.Equal(t, 2, row.Args.Len())
		assert.Equal(t, Oid(0), row.RowTypeid) // Default unspecified
	})
}

// TestTier2ExpressionInterfaces tests that Tier 2 expressions implement required interfaces.
func TestTier2ExpressionInterfaces(t *testing.T) {
	expressions := []struct {
		name string
		expr Expr
	}{
		{"CaseExpr", NewCaseExpr(25, nil, NewNodeList(), nil)},
		{"CaseWhen", NewCaseWhen(NewConst(16, 1, false), NewString("result"))},
		{"CoalesceExpr", NewCoalesceExpr(25, NewNodeList())},
		{"ArrayExpr", NewArrayExpr(1007, 23, NewNodeList())},
		{"ScalarArrayOpExpr", NewScalarArrayOpExpr(96, true, NewVar(1, 1, 23), NewArrayConstructor(NewNodeList()))},
		{"RowExpr", NewRowExpr(NewNodeList(), 2249)},
	}

	for _, tt := range expressions {
		t.Run(tt.name, func(t *testing.T) {
			// Test Expr interface
			assert.True(t, tt.expr.IsExpr())
			assert.NotEmpty(t, tt.expr.ExpressionType())

			// Test Node interface
			var node Node = tt.expr
			assert.NotNil(t, node)
			assert.True(t, int(node.NodeTag()) > 0)
			assert.NotEmpty(t, node.String())
		})
	}
}

// TestTier2ExpressionUtilities tests the utility functions for Tier 2 expressions.
func TestTier2ExpressionUtilities(t *testing.T) {
	t.Run("TypeChecking", func(t *testing.T) {
		caseExpr := NewCaseExpr(25, nil, NewNodeList(), nil)
		coalesceExpr := NewCoalesceExpr(25, NewNodeList())
		arrayExpr := NewArrayConstructor(NewNodeList())
		scalarArrayExpr := NewInExpr(NewVar(1, 1, 23), arrayExpr)
		rowExpr := NewRowConstructor(NewNodeList())
		varExpr := NewVar(1, 1, 23)

		assert.True(t, IsCaseExpr(caseExpr))
		assert.False(t, IsCaseExpr(varExpr))

		assert.True(t, IsCoalesceExpr(coalesceExpr))
		assert.False(t, IsCoalesceExpr(varExpr))

		assert.True(t, IsArrayExpr(arrayExpr))
		assert.False(t, IsArrayExpr(varExpr))

		assert.True(t, IsScalarArrayOpExpr(scalarArrayExpr))
		assert.False(t, IsScalarArrayOpExpr(varExpr))

		assert.True(t, IsRowExpr(rowExpr))
		assert.False(t, IsRowExpr(varExpr))
	})

	t.Run("InOperations", func(t *testing.T) {
		column := NewVar(1, 1, 23)
		array := NewArrayConstructor(NewNodeList())

		inExpr := NewInExpr(column, array)
		assert.True(t, IsInExpr(inExpr))
		assert.False(t, IsNotInExpr(inExpr))

		notInExpr := NewNotInExpr(column, array)
		assert.False(t, IsInExpr(notInExpr))
		assert.True(t, IsNotInExpr(notInExpr))

		regularExpr := NewVar(1, 1, 23)
		assert.False(t, IsInExpr(regularExpr))
		assert.False(t, IsNotInExpr(regularExpr))
	})

	t.Run("CaseExpressionUtilities", func(t *testing.T) {
		when1 := NewCaseWhen(NewConst(23, 1, false), NewString("One"))
		when2 := NewCaseWhen(NewConst(23, 2, false), NewString("Two"))
		elseResult := NewString("Other")

		caseExpr := NewCaseExpr(25, nil, NewNodeList(when1, when2), elseResult)

		assert.Equal(t, 2, GetCaseWhenCount(caseExpr))
		assert.True(t, HasCaseElse(caseExpr))

		caseWithoutElse := NewCaseExpr(25, nil, NewNodeList(when1), nil)
		assert.False(t, HasCaseElse(caseWithoutElse))

		nonCaseExpr := NewVar(1, 1, 23)
		assert.Equal(t, 0, GetCaseWhenCount(nonCaseExpr))
		assert.False(t, HasCaseElse(nonCaseExpr))
	})

	t.Run("ArrayUtilities", func(t *testing.T) {
		elem1 := NewConst(23, 1, false)
		elem2 := NewConst(23, 2, false)
		elements := NewNodeList(elem1, elem2)

		arrayExpr := NewArrayExpr(1007, 23, elements)

		retrievedElements := GetArrayElements(arrayExpr)
		assert.Equal(t, 2, retrievedElements.Len())
		assert.Equal(t, elem1, retrievedElements.Items[0])
		assert.Equal(t, elem2, retrievedElements.Items[1])

		assert.False(t, IsMultiDimArray(arrayExpr))
		arrayExpr.Multidims = true
		assert.True(t, IsMultiDimArray(arrayExpr))

		nonArrayExpr := NewVar(1, 1, 23)
		assert.Nil(t, GetArrayElements(nonArrayExpr))
		assert.False(t, IsMultiDimArray(nonArrayExpr))
	})

	t.Run("GetExpressionArgsEnhanced", func(t *testing.T) {
		// Test that GetExpressionArgs works with Tier 2 expressions
		caseExpr := NewCaseExpr(25, nil, NewNodeList(NewCaseWhen(nil, nil)), nil)
		args := GetExpressionArgs(caseExpr)
		assert.Equal(t, 1, args.Len())

		coalesceExpr := NewCoalesceExpr(25, NewNodeList(NewVar(1, 1, 23), NewString("default")))
		args = GetExpressionArgs(coalesceExpr)
		assert.Equal(t, 2, args.Len())

		arrayExpr := NewArrayConstructor(NewNodeList(NewConst(23, 1, false), NewConst(23, 2, false)))
		args = GetExpressionArgs(arrayExpr)
		assert.Equal(t, 2, args.Len()) // Should return Elements

		rowExpr := NewRowConstructor(NewNodeList(NewVar(1, 1, 23), NewVar(1, 2, 25)))
		args = GetExpressionArgs(rowExpr)
		assert.Equal(t, 2, args.Len())
	})
}

// TestComplexTier2ExpressionTrees tests building complex expression trees with Tier 2 expressions.
func TestComplexTier2ExpressionTrees(t *testing.T) {
	t.Run("NestedCaseInCoalesce", func(t *testing.T) {
		// COALESCE(CASE WHEN status = 1 THEN 'Active' ELSE NULL END, 'Unknown')

		statusVar := NewVar(1, 1, 23)
		statusCheck := NewBinaryOp(96, statusVar, NewConst(23, 1, false)) // status = 1
		when := NewCaseWhen(statusCheck, NewString("Active"))
		caseExpr := NewCaseExpr(25, nil, NewNodeList(when), NewNull())

		coalesceExpr := NewCoalesceExpr(25, NewNodeList(caseExpr, NewString("Unknown")))

		require.NotNil(t, coalesceExpr)
		assert.Equal(t, 2, coalesceExpr.Args.Len())
		assert.Equal(t, caseExpr, coalesceExpr.Args.Items[0])
		assert.Equal(t, "CoalesceExpr", coalesceExpr.ExpressionType())

		// Verify nested structure
		nestedCase := coalesceExpr.Args.Items[0].(*CaseExpr)
		assert.Equal(t, 1, nestedCase.Args.Len())
	})

	t.Run("ArrayInInExpression", func(t *testing.T) {
		// user_id IN (SELECT ARRAY_AGG(id) FROM allowed_users)
		// Simplified as: user_id IN ARRAY[1, 2, 3]

		userIdVar := NewVar(1, 1, 23)
		allowedIds := NewArrayConstructor(NewNodeList(
			NewConst(23, 1, false),
			NewConst(23, 2, false),
			NewConst(23, 3, false),
		))

		inExpr := NewInExpr(userIdVar, allowedIds)

		require.NotNil(t, inExpr)
		assert.True(t, IsInExpr(inExpr))
		assert.Equal(t, "ScalarArrayOpExpr", inExpr.ExpressionType())
		assert.True(t, inExpr.UseOr)

		// Verify array structure
		arrayArg := inExpr.Args.Items[1].(*ArrayExpr)
		assert.Equal(t, 3, arrayArg.Elements.Len())
	})
}

// TestExpressionNodeTraversal tests that expression nodes work with node traversal.
func TestExpressionNodeTraversal(t *testing.T) {
	// Create a complex expression tree
	left := NewVar(1, 1, 23)
	right := NewConst(23, 42, false)
	comparison := NewBinaryOp(96, left, right)

	// Walk the tree (basic test - will be enhanced when traversal system is complete)
	var visited []Node
	WalkNodes(comparison, func(node Node) bool {
		visited = append(visited, node)
		return true
	})

	// For now, just verify the root is visited
	// Full expression traversal will be added when we enhance the traversal system
	assert.Len(t, visited, 1)
	assert.Equal(t, comparison, visited[0])
}

// TestTier3Expressions tests Tier 3 expressions (advanced expressions and aggregations).
func TestTier3Expressions(t *testing.T) {
	t.Run("SubLinkType", func(t *testing.T) {
		tests := []struct {
			sublinkType SubLinkType
			expected    string
		}{
			{EXISTS_SUBLINK, "EXISTS"},
			{ALL_SUBLINK, "ALL"},
			{ANY_SUBLINK, "ANY"},
			{ROWCOMPARE_SUBLINK, "ROWCOMPARE"},
			{EXPR_SUBLINK, "EXPR"},
			{MULTIEXPR_SUBLINK, "MULTIEXPR"},
			{ARRAY_SUBLINK, "ARRAY"},
			{CTE_SUBLINK, "CTE"},
		}

		for _, tt := range tests {
			assert.Equal(t, tt.expected, tt.sublinkType.String())
		}
	})

	t.Run("AggSplit", func(t *testing.T) {
		tests := []struct {
			aggSplit AggSplit
			expected string
		}{
			{AGGSPLIT_SIMPLE, "SIMPLE"},
			{AGGSPLIT_INITIAL_SERIAL, "INITIAL_SERIAL"},
			{AGGSPLIT_FINAL_DESERIAL, "FINAL_DESERIAL"},
		}

		for _, tt := range tests {
			assert.Equal(t, tt.expected, tt.aggSplit.String())
		}
	})
}

// TestAggref tests the Aggref expression node.
func TestAggref(t *testing.T) {
	t.Run("Basic Aggregate", func(t *testing.T) {
		aggfnoid := Oid(2147) // COUNT function
		aggtype := Oid(20)    // bigint
		arg1 := NewVar(1, 1, 23)
		args := NewNodeList(arg1)

		aggref := NewAggref(aggfnoid, aggtype, args)

		// Test basic properties
		assert.Equal(t, T_Aggref, aggref.NodeTag())
		assert.Equal(t, "Aggref", aggref.ExpressionType())
		assert.True(t, aggref.IsExpr())
		assert.Equal(t, aggfnoid, aggref.Aggfnoid)
		assert.Equal(t, aggtype, aggref.Aggtype)
		assert.Equal(t, args, aggref.Args)
		assert.False(t, aggref.Aggstar)
		assert.False(t, aggref.Aggvariadic)

		// Test interface compliance
		var _ Node = aggref
		var _ Expr = aggref
		var _ Expression = aggref

		// Test utility functions
		assert.True(t, IsAggref(aggref))
		assert.True(t, IsAggregate(aggref))
		assert.False(t, IsDistinctAggregate(aggref))
		assert.False(t, IsStarAggregate(aggref))
		assert.False(t, HasAggregateFilter(aggref))
		assert.Equal(t, args, GetAggregateArgs(aggref))
	})

	t.Run("COUNT(*) Aggregate", func(t *testing.T) {
		countStar := NewCountStar()

		assert.Equal(t, T_Aggref, countStar.NodeTag())
		assert.True(t, countStar.Aggstar)
		assert.True(t, IsStarAggregate(countStar))
		assert.Equal(t, Oid(2147), countStar.Aggfnoid) // COUNT function OID
		assert.Equal(t, Oid(20), countStar.Aggtype)    // bigint result type
	})

	t.Run("Aggregate with DISTINCT", func(t *testing.T) {
		arg := NewVar(1, 1, 23)
		aggref := NewCount(arg)
		aggref.Aggdistinct = NewNodeList(arg) // Add DISTINCT

		assert.True(t, IsDistinctAggregate(aggref))
		assert.Contains(t, aggref.String(), "DISTINCT")
	})

	t.Run("Aggregate with FILTER", func(t *testing.T) {
		arg := NewVar(1, 1, 23)
		filter := NewBinaryOp(96, arg, NewConst(23, 10, false)) // arg = 10
		aggref := NewCount(arg)
		aggref.Aggfilter = filter

		assert.True(t, HasAggregateFilter(aggref))
		assert.Equal(t, filter, GetAggregateFilter(aggref))
		assert.Contains(t, aggref.String(), "FILTER")
	})

	t.Run("Ordered-Set Aggregate", func(t *testing.T) {
		arg := NewVar(1, 1, 23)
		directArg := NewConst(25, 50, false)            // 0.5 for percentile
		aggref := NewAggref(3972, 23, NewNodeList(arg)) // percentile_cont
		aggref.Aggdirectargs = NewNodeList(directArg)
		aggref.Aggorder = NewNodeList(arg)

		assert.True(t, IsOrderedSetAggregate(aggref))
		assert.Equal(t, NewNodeList(directArg), GetAggregateDirectArgs(aggref))
		assert.Equal(t, NewNodeList(arg), GetAggregateOrderBy(aggref))
	})
}

// TestWindowFunc tests the WindowFunc expression node.
func TestWindowFunc(t *testing.T) {
	t.Run("Basic Window Function", func(t *testing.T) {
		winfnoid := Oid(3100) // ROW_NUMBER
		wintype := Oid(20)    // bigint
		winref := Index(1)
		args := NewNodeList(NewVar(1, 1, 23))

		winFunc := NewWindowFunc(winfnoid, wintype, args, winref)

		// Test basic properties
		assert.Equal(t, T_WindowFunc, winFunc.NodeTag())
		assert.Equal(t, "WindowFunc", winFunc.ExpressionType())
		assert.True(t, winFunc.IsExpr())
		assert.Equal(t, winfnoid, winFunc.Winfnoid)
		assert.Equal(t, wintype, winFunc.Wintype)
		assert.Equal(t, args, winFunc.Args)
		assert.Equal(t, winref, winFunc.Winref)
		assert.False(t, winFunc.Winstar)
		assert.False(t, winFunc.Winagg)

		// Test interface compliance
		var _ Node = winFunc
		var _ Expr = winFunc
		var _ Expression = winFunc

		// Test utility functions
		assert.True(t, IsWindowFunc(winFunc))
		assert.False(t, IsAggregate(winFunc)) // Winagg is false
		assert.False(t, IsStarAggregate(winFunc))
		assert.False(t, HasAggregateFilter(winFunc))
		assert.Equal(t, args, GetAggregateArgs(winFunc))
		assert.Equal(t, winref, GetWindowFuncRef(winFunc))
		assert.False(t, IsSimpleWindowAgg(winFunc))
	})

	t.Run("ROW_NUMBER() Window Function", func(t *testing.T) {
		rowNumber := NewRowNumber()

		assert.Equal(t, T_WindowFunc, rowNumber.NodeTag())
		assert.Equal(t, Oid(3100), rowNumber.Winfnoid) // ROW_NUMBER function OID
		assert.Equal(t, Oid(20), rowNumber.Wintype)    // bigint result type
		assert.Nil(t, rowNumber.Args)                  // No arguments
	})

	t.Run("Window Aggregate Function", func(t *testing.T) {
		arg := NewVar(1, 1, 23)
		winFunc := NewWindowFunc(2108, 23, NewNodeList(arg), 1) // SUM over window
		winFunc.Winagg = true

		assert.True(t, IsAggregate(winFunc)) // Now it's an aggregate
		assert.True(t, IsSimpleWindowAgg(winFunc))
	})

	t.Run("Window Function with Filter", func(t *testing.T) {
		arg := NewVar(1, 1, 23)
		filter := NewBinaryOp(96, arg, NewConst(23, 0, false))
		winFunc := NewWindowFunc(2108, 23, NewNodeList(arg), 1) // SUM
		winFunc.Aggfilter = filter

		assert.True(t, HasAggregateFilter(winFunc))
		assert.Equal(t, filter, GetAggregateFilter(winFunc))
		assert.Contains(t, winFunc.String(), "FILTER")
	})
}

// TestSubLink tests the SubLink expression node.
func TestSubLink(t *testing.T) {
	t.Run("EXISTS Sublink", func(t *testing.T) {
		subquery := NewString("SELECT 1") // Simplified subquery representation
		sublink := NewExistsSublink(subquery)

		// Test basic properties
		assert.Equal(t, T_SubLink, sublink.NodeTag())
		assert.Equal(t, "SubLink", sublink.ExpressionType())
		assert.True(t, sublink.IsExpr())
		assert.Equal(t, EXISTS_SUBLINK, sublink.SubLinkType)
		assert.Equal(t, subquery, sublink.Subselect)
		assert.Nil(t, sublink.Testexpr)

		// Test interface compliance
		var _ Node = sublink
		var _ Expr = sublink
		var _ Expression = sublink

		// Test utility functions
		assert.True(t, IsSubLink(sublink))
		assert.True(t, IsExistsSublink(sublink))
		assert.False(t, IsScalarSublink(sublink))
		assert.False(t, IsArraySublink(sublink))
		assert.Equal(t, EXISTS_SUBLINK, GetSubLinkType(sublink))
		assert.Equal(t, subquery, GetSubquery(sublink))
		assert.False(t, HasSubLinkTest(sublink))
		assert.Nil(t, GetSubLinkTest(sublink))
	})

	t.Run("Scalar Sublink", func(t *testing.T) {
		subquery := NewString("SELECT max_salary FROM departments WHERE dept_id = 1")
		sublink := NewExprSublink(subquery)

		assert.Equal(t, EXPR_SUBLINK, sublink.SubLinkType)
		assert.True(t, IsScalarSublink(sublink))
		assert.False(t, IsExistsSublink(sublink))
		assert.Contains(t, sublink.String(), "EXPR")
	})

	t.Run("IN Sublink", func(t *testing.T) {
		testExpr := NewVar(1, 1, 23)
		subquery := NewString("SELECT dept_id FROM departments")
		sublink := NewInSublink(testExpr, subquery)

		assert.Equal(t, ANY_SUBLINK, sublink.SubLinkType)
		assert.Equal(t, testExpr, sublink.Testexpr)
		assert.True(t, HasSubLinkTest(sublink))
		assert.Equal(t, testExpr, GetSubLinkTest(sublink))
		assert.Contains(t, sublink.String(), "with test")
	})

	t.Run("NOT IN Sublink", func(t *testing.T) {
		testExpr := NewVar(1, 1, 23)
		subquery := NewString("SELECT blocked_ids FROM blocked_users")
		sublink := NewNotInSublink(testExpr, subquery)

		assert.Equal(t, ALL_SUBLINK, sublink.SubLinkType)
		assert.Equal(t, testExpr, sublink.Testexpr)
		assert.True(t, HasSubLinkTest(sublink))
	})

	t.Run("ARRAY Sublink", func(t *testing.T) {
		subquery := NewString("SELECT name FROM users ORDER BY name")
		sublink := NewArraySublink(subquery)

		assert.Equal(t, ARRAY_SUBLINK, sublink.SubLinkType)
		assert.True(t, IsArraySublink(sublink))
		assert.Contains(t, sublink.String(), "ARRAY")
	})
}

// TestTier3ComplexExpressions tests complex expressions involving Tier 3 nodes.
func TestTier3ComplexExpressions(t *testing.T) {
	t.Run("Aggregate in CASE Expression", func(t *testing.T) {
		// CASE WHEN COUNT(*) > 10 THEN 'High' ELSE 'Low' END
		countStar := NewCountStar()
		threshold := NewConst(23, 10, false)                // 10
		condition := NewBinaryOp(521, countStar, threshold) // COUNT(*) > 10
		highResult := NewString("High")
		lowResult := NewString("Low")
		whenClause := NewCaseWhen(condition, highResult)
		caseExpr := NewSearchedCase(NewNodeList(whenClause), lowResult)

		// Test that we can extract arguments properly
		caseArgs := GetExpressionArgs(caseExpr)
		assert.Equal(t, 1, caseArgs.Len())

		// The WHEN clause should contain the condition with aggregate
		whenArgs := GetExpressionArgs(caseArgs.Items[0].(*CaseWhen))
		assert.Equal(t, 2, whenArgs.Len()) // condition and result

		// The condition should be an OpExpr containing the aggregate
		conditionArgs := GetExpressionArgs(whenArgs.Items[0])
		assert.Equal(t, 2, conditionArgs.Len()) // COUNT(*) and 10
		assert.True(t, IsAggref(conditionArgs.Items[0]))
		assert.True(t, IsStarAggregate(conditionArgs.Items[0]))
	})

	t.Run("Window Function with Sublink", func(t *testing.T) {
		// ROW_NUMBER() OVER (ORDER BY (SELECT count(*) FROM related_table))
		subquery := NewString("SELECT count(*) FROM related_table")
		scalarSublink := NewExprSublink(subquery)
		rowNumber := NewRowNumber()

		// In a real implementation, the ORDER BY would be part of the window specification
		// Here we just test that we can create and identify these expressions
		assert.True(t, IsWindowFunc(rowNumber))
		assert.True(t, IsScalarSublink(scalarSublink))

		// Test GetExpressionArgs for SubLink
		sublinkArgs := GetExpressionArgs(scalarSublink)
		assert.Equal(t, 1, sublinkArgs.Len()) // Just the subquery
		assert.Equal(t, subquery, sublinkArgs.Items[0])
	})

	t.Run("EXISTS with Correlated Sublink", func(t *testing.T) {
		// WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id)
		// Simplified representation of correlated subquery
		subquery := NewString("SELECT 1 FROM orders o WHERE condition")
		existsSublink := NewExistsSublink(subquery)

		assert.True(t, IsExistsSublink(existsSublink))
		assert.Equal(t, EXISTS_SUBLINK, GetSubLinkType(existsSublink))

		// Test GetExpressionArgs returns just the subquery for EXISTS
		existsArgs := GetExpressionArgs(existsSublink)
		assert.Equal(t, 1, existsArgs.Len())
		assert.Equal(t, subquery, existsArgs.Items[0])
	})
}

// ==============================================================================
// PHASE 1F: PRIMITIVE EXPRESSION COMPLETION PART 1 - TEST COVERAGE
// Unit tests for primitive expression nodes
// ==============================================================================

// TestEnumTypes tests the enumeration types
func TestEnumTypes(t *testing.T) {
	t.Run("RowCompareType", func(t *testing.T) {
		assert.Equal(t, RowCompareType(1), ROWCOMPARE_LT)
		assert.Equal(t, RowCompareType(2), ROWCOMPARE_LE)
		assert.Equal(t, RowCompareType(3), ROWCOMPARE_EQ)
		assert.Equal(t, RowCompareType(4), ROWCOMPARE_GE)
		assert.Equal(t, RowCompareType(5), ROWCOMPARE_GT)
		assert.Equal(t, RowCompareType(6), ROWCOMPARE_NE)
	})

	t.Run("MinMaxOp", func(t *testing.T) {
		assert.Equal(t, MinMaxOp(0), IS_GREATEST)
		assert.Equal(t, MinMaxOp(1), IS_LEAST)
	})

	t.Run("SQLValueFunctionOp", func(t *testing.T) {
		assert.Equal(t, SQLValueFunctionOp(0), SVFOP_CURRENT_DATE)
		assert.Equal(t, SQLValueFunctionOp(1), SVFOP_CURRENT_TIME)
		assert.Equal(t, SQLValueFunctionOp(9), SVFOP_CURRENT_ROLE)
	})

	t.Run("XmlExprOp", func(t *testing.T) {
		assert.Equal(t, XmlExprOp(0), IS_XMLCONCAT)
		assert.Equal(t, XmlExprOp(1), IS_XMLELEMENT)
		assert.Equal(t, XmlExprOp(7), IS_DOCUMENT)
	})

	t.Run("XmlOptionType", func(t *testing.T) {
		assert.Equal(t, XmlOptionType(0), XMLOPTION_DOCUMENT)
		assert.Equal(t, XmlOptionType(1), XMLOPTION_CONTENT)
	})

	t.Run("TableFuncType", func(t *testing.T) {
		assert.Equal(t, TableFuncType(0), TFT_XMLTABLE)
		assert.Equal(t, TableFuncType(1), TFT_JSON_TABLE)
	})

	t.Run("OnCommitAction", func(t *testing.T) {
		assert.Equal(t, OnCommitAction(0), ONCOMMIT_NOOP)
		assert.Equal(t, OnCommitAction(1), ONCOMMIT_PRESERVE_ROWS)
		assert.Equal(t, OnCommitAction(2), ONCOMMIT_DELETE_ROWS)
		assert.Equal(t, OnCommitAction(3), ONCOMMIT_DROP)
	})

	t.Run("OverridingKind", func(t *testing.T) {
		assert.Equal(t, OverridingKind(0), OVERRIDING_NOT_SET)
		assert.Equal(t, OverridingKind(1), OVERRIDING_USER_VALUE)
		assert.Equal(t, OverridingKind(2), OVERRIDING_SYSTEM_VALUE)
	})

	t.Run("MergeMatchKind", func(t *testing.T) {
		assert.Equal(t, MergeMatchKind(0), MERGE_WHEN_MATCHED)
		assert.Equal(t, MergeMatchKind(1), MERGE_WHEN_NOT_MATCHED_BY_SOURCE)
		assert.Equal(t, MergeMatchKind(2), MERGE_WHEN_NOT_MATCHED_BY_TARGET)
	})
}

// TestGroupingFunc tests the GroupingFunc node implementation
func TestGroupingFunc(t *testing.T) {
	t.Run("NewGroupingFunc", func(t *testing.T) {
		args := NewNodeList(NewConst(Oid(23), 1, false), NewConst(Oid(23), 2, false))
		refs := NewNodeList(NewInteger(1), NewInteger(2))
		cols := NewNodeList(NewInteger(10), NewInteger(20))
		aggLevelsUp := Index(0)
		location := 150

		gf := NewGroupingFunc(args, refs, cols, aggLevelsUp, location)

		require.NotNil(t, gf)
		assert.Equal(t, T_GroupingFunc, gf.Tag)
		assert.Equal(t, args, gf.Args)
		assert.Equal(t, refs, gf.Refs)
		assert.Equal(t, cols, gf.Cols)
		assert.Equal(t, aggLevelsUp, gf.AggLevelsUp)
		assert.Equal(t, location, gf.Location())
		assert.Equal(t, location, gf.BaseNode.Loc)
	})

	t.Run("String", func(t *testing.T) {
		args := NewNodeList(NewConst(Oid(23), 1, false))
		refs := NewNodeList(NewInteger(1))
		cols := NewNodeList(NewInteger(10))
		gf := NewGroupingFunc(args, refs, cols, Index(1), 200)
		str := gf.String()

		assert.Contains(t, str, "GroupingFunc")
		assert.Contains(t, str, "1 args")
		assert.Contains(t, str, "agglevelsup=1")
		assert.Contains(t, str, "@200")
	})

	t.Run("Interface Compliance", func(t *testing.T) {
		gf := NewGroupingFunc(nil, nil, nil, Index(0), 0)

		// Test Expression interface
		var expr Expression = gf
		assert.NotNil(t, expr)

		// Test Node interface
		var node Node = gf
		assert.Equal(t, T_GroupingFunc, node.NodeTag())
		assert.Equal(t, 0, node.Location())
	})
}

// TestWindowFuncRunCondition tests the WindowFuncRunCondition node implementation
func TestWindowFuncRunCondition(t *testing.T) {
	t.Run("NewWindowFuncRunCondition", func(t *testing.T) {
		opno := Oid(96) // = operator OID
		inputCollid := Oid(100)
		wfuncLeft := true
		arg := NewConst(Oid(23), Datum(42), false)
		location := 200

		wfrc := NewWindowFuncRunCondition(opno, inputCollid, wfuncLeft, arg, location)

		require.NotNil(t, wfrc)
		assert.Equal(t, T_WindowFuncRunCondition, wfrc.Tag)
		assert.Equal(t, opno, wfrc.Opno)
		assert.Equal(t, inputCollid, wfrc.InputCollid)
		assert.Equal(t, wfuncLeft, wfrc.WfuncLeft)
		assert.Equal(t, arg, wfrc.Arg)
		assert.Equal(t, location, wfrc.Location())
	})

	t.Run("String", func(t *testing.T) {
		wfrc := NewWindowFuncRunCondition(Oid(96), Oid(100), false, nil, 250)
		str := wfrc.String()

		assert.Contains(t, str, "WindowFuncRunCondition")
		assert.Contains(t, str, "opno=96")
		assert.Contains(t, str, "left=false")
		assert.Contains(t, str, "@250")
	})

	t.Run("Interface Compliance", func(t *testing.T) {
		wfrc := NewWindowFuncRunCondition(Oid(0), Oid(0), false, nil, 0)

		var expr Expression = wfrc
		assert.NotNil(t, expr)

		var node Node = wfrc
		assert.Equal(t, T_WindowFuncRunCondition, node.NodeTag())
	})
}

// TestMergeSupportFunc tests the MergeSupportFunc node implementation
func TestMergeSupportFunc(t *testing.T) {
	t.Run("NewMergeSupportFunc", func(t *testing.T) {
		msfType := Oid(23)  // int4
		msfCollid := Oid(0) // No collation
		location := 300

		msf := NewMergeSupportFunc(msfType, msfCollid, location)

		require.NotNil(t, msf)
		assert.Equal(t, T_MergeSupportFunc, msf.Tag)
		assert.Equal(t, msfType, msf.MsfType)
		assert.Equal(t, msfCollid, msf.MsfCollid)
		assert.Equal(t, location, msf.Location())
	})

	t.Run("String", func(t *testing.T) {
		msf := NewMergeSupportFunc(Oid(25), Oid(100), 350)
		str := msf.String()

		assert.Contains(t, str, "MergeSupportFunc")
		assert.Contains(t, str, "type=25")
		assert.Contains(t, str, "collid=100")
		assert.Contains(t, str, "@350")
	})

	t.Run("Interface Compliance", func(t *testing.T) {
		msf := NewMergeSupportFunc(Oid(0), Oid(0), 0)

		var expr Expression = msf
		assert.NotNil(t, expr)

		var node Node = msf
		assert.Equal(t, T_MergeSupportFunc, node.NodeTag())
	})
}

// TestNamedArgExpr tests the NamedArgExpr node implementation
func TestNamedArgExpr(t *testing.T) {
	t.Run("NewNamedArgExpr", func(t *testing.T) {
		arg := NewConst(Oid(25), Datum(uintptr(0)), false)
		name := "param_name"
		argNumber := 1
		location := 400

		nae := NewNamedArgExpr(arg, name, argNumber, location)

		require.NotNil(t, nae)
		assert.Equal(t, T_NamedArgExpr, nae.Tag)
		assert.Equal(t, arg, nae.Arg)
		assert.Equal(t, name, nae.Name)
		assert.Equal(t, argNumber, nae.ArgNumber)
		assert.Equal(t, location, nae.Location())
	})

	t.Run("String", func(t *testing.T) {
		nae := NewNamedArgExpr(nil, "my_param", 2, 450)
		str := nae.String()

		assert.Contains(t, str, "NamedArgExpr")
		assert.Contains(t, str, "name='my_param'")
		assert.Contains(t, str, "argnum=2")
		assert.Contains(t, str, "@450")
	})

	t.Run("Interface Compliance", func(t *testing.T) {
		nae := NewNamedArgExpr(nil, "", 0, 0)

		var expr Expression = nae
		assert.NotNil(t, expr)

		var node Node = nae
		assert.Equal(t, T_NamedArgExpr, node.NodeTag())
	})
}

// TestCaseTestExpr tests the CaseTestExpr node implementation
func TestCaseTestExpr(t *testing.T) {
	t.Run("NewCaseTestExpr", func(t *testing.T) {
		typeId := Oid(23) // int4
		typeMod := -1
		collation := Oid(100)
		location := 500

		cte := NewCaseTestExpr(typeId, typeMod, collation, location)

		require.NotNil(t, cte)
		assert.Equal(t, T_CaseTestExpr, cte.Tag)
		assert.Equal(t, typeId, cte.TypeId)
		assert.Equal(t, typeMod, cte.TypeMod)
		assert.Equal(t, collation, cte.Collation)
	})

	t.Run("String", func(t *testing.T) {
		cte := NewCaseTestExpr(Oid(25), 100, Oid(200), 550)
		str := cte.String()

		assert.Contains(t, str, "CaseTestExpr")
		assert.Contains(t, str, "type=25")
		assert.Contains(t, str, "typmod=100")
		assert.Contains(t, str, "collation=200")
		assert.Contains(t, str, "@550")
	})

	t.Run("Interface Compliance", func(t *testing.T) {
		cte := NewCaseTestExpr(Oid(0), 0, Oid(0), 0)

		var expr Expression = cte
		assert.NotNil(t, expr)

		var node Node = cte
		assert.Equal(t, T_CaseTestExpr, node.NodeTag())
	})
}

// TestMinMaxExpr tests the MinMaxExpr node implementation
func TestMinMaxExpr(t *testing.T) {
	t.Run("NewMinMaxExpr", func(t *testing.T) {
		minMaxType := Oid(23) // int4
		minMaxCollid := Oid(0)
		inputCollid := Oid(0)
		op := IS_GREATEST
		args := []Expression{
			NewConst(Oid(23), Datum(1), false),
			NewConst(Oid(23), Datum(2), false),
		}
		location := 600

		mme := NewMinMaxExpr(minMaxType, minMaxCollid, inputCollid, op, args, location)

		require.NotNil(t, mme)
		assert.Equal(t, T_MinMaxExpr, mme.Tag)
		assert.Equal(t, minMaxType, mme.MinMaxType)
		assert.Equal(t, minMaxCollid, mme.MinMaxCollid)
		assert.Equal(t, inputCollid, mme.InputCollid)
		assert.Equal(t, op, mme.Op)
		assert.Equal(t, args, mme.Args)
		assert.Equal(t, location, mme.Location())
	})

	t.Run("String GREATEST", func(t *testing.T) {
		args := []Expression{NewConst(Oid(23), Datum(1), false)}
		mme := NewMinMaxExpr(Oid(23), Oid(0), Oid(0), IS_GREATEST, args, 650)
		str := mme.String()

		assert.Contains(t, str, "MinMaxExpr")
		assert.Contains(t, str, "GREATEST")
		assert.Contains(t, str, "1 args")
		assert.Contains(t, str, "@650")
	})

	t.Run("String LEAST", func(t *testing.T) {
		args := []Expression{NewConst(Oid(23), Datum(1), false)}
		mme := NewMinMaxExpr(Oid(23), Oid(0), Oid(0), IS_LEAST, args, 700)
		str := mme.String()

		assert.Contains(t, str, "MinMaxExpr")
		assert.Contains(t, str, "LEAST")
		assert.Contains(t, str, "1 args")
		assert.Contains(t, str, "@700")
	})

	t.Run("Interface Compliance", func(t *testing.T) {
		mme := NewMinMaxExpr(Oid(0), Oid(0), Oid(0), IS_GREATEST, nil, 0)

		var expr Expression = mme
		assert.NotNil(t, expr)

		var node Node = mme
		assert.Equal(t, T_MinMaxExpr, node.NodeTag())
	})
}

// TestRowCompareExpr tests the RowCompareExpr node implementation
func TestRowCompareExpr(t *testing.T) {
	t.Run("NewRowCompareExpr", func(t *testing.T) {
		rctype := ROWCOMPARE_LT
		opnos := []Oid{Oid(96), Oid(97)}          // Operator OIDs
		opfamilies := []Oid{Oid(1), Oid(2)}       // Operator family OIDs
		inputCollids := []Oid{Oid(100), Oid(101)} // Collation OIDs
		largs := []Expression{NewConst(Oid(23), Datum(1), false)}
		rargs := []Expression{NewConst(Oid(23), Datum(2), false)}
		location := 800

		rce := NewRowCompareExpr(rctype, opnos, opfamilies, inputCollids, largs, rargs, location)

		require.NotNil(t, rce)
		assert.Equal(t, T_RowCompareExpr, rce.Tag)
		assert.Equal(t, rctype, rce.Rctype)
		assert.Equal(t, opnos, rce.Opnos)
		assert.Equal(t, opfamilies, rce.Opfamilies)
		assert.Equal(t, inputCollids, rce.InputCollids)
		assert.Equal(t, largs, rce.Largs)
		assert.Equal(t, rargs, rce.Rargs)
	})

	t.Run("String with different operators", func(t *testing.T) {
		tests := []struct {
			rctype   RowCompareType
			expected string
		}{
			{ROWCOMPARE_LT, "<"},
			{ROWCOMPARE_LE, "<="},
			{ROWCOMPARE_EQ, "="},
			{ROWCOMPARE_GE, ">="},
			{ROWCOMPARE_GT, ">"},
			{ROWCOMPARE_NE, "<>"},
		}

		for _, tt := range tests {
			largs := []Expression{NewConst(Oid(23), Datum(1), false)}
			rargs := []Expression{NewConst(Oid(23), Datum(2), false)}
			rce := NewRowCompareExpr(tt.rctype, nil, nil, nil, largs, rargs, 850)
			str := rce.String()

			assert.Contains(t, str, "RowCompareExpr")
			assert.Contains(t, str, tt.expected)
			assert.Contains(t, str, "(1)")
			assert.Contains(t, str, "(1)")
			assert.Contains(t, str, "@850")
		}
	})

	t.Run("Interface Compliance", func(t *testing.T) {
		rce := NewRowCompareExpr(ROWCOMPARE_LT, nil, nil, nil, nil, nil, 0)

		var expr Expression = rce
		assert.NotNil(t, expr)

		var node Node = rce
		assert.Equal(t, T_RowCompareExpr, node.NodeTag())
	})
}

// TestSQLValueFunction tests the SQLValueFunction node implementation
func TestSQLValueFunction(t *testing.T) {
	t.Run("NewSQLValueFunction", func(t *testing.T) {
		op := SVFOP_CURRENT_DATE
		typ := Oid(1082) // date type OID
		typeMod := -1
		location := 900

		svf := NewSQLValueFunction(op, typ, typeMod, location)

		require.NotNil(t, svf)
		assert.Equal(t, T_SQLValueFunction, svf.Tag)
		assert.Equal(t, op, svf.Op)
		assert.Equal(t, typ, svf.Type)
		assert.Equal(t, typeMod, svf.TypeMod)
		assert.Equal(t, location, svf.Location())
	})

	t.Run("String with different functions", func(t *testing.T) {
		tests := []struct {
			op       SQLValueFunctionOp
			expected string
		}{
			{SVFOP_CURRENT_DATE, "CURRENT_DATE"},
			{SVFOP_CURRENT_TIME, "CURRENT_TIME"},
			{SVFOP_CURRENT_TIMESTAMP, "CURRENT_TIMESTAMP"},
			{SVFOP_LOCALTIME, "LOCALTIME"},
			{SVFOP_LOCALTIMESTAMP, "LOCALTIMESTAMP"},
			{SVFOP_CURRENT_ROLE, "CURRENT_ROLE"},
			{SVFOP_CURRENT_USER, "CURRENT_USER"},
			{SVFOP_USER, "USER"},
			{SVFOP_SESSION_USER, "SESSION_USER"},
			{SVFOP_CURRENT_CATALOG, "CURRENT_CATALOG"},
			{SVFOP_CURRENT_SCHEMA, "CURRENT_SCHEMA"},
		}

		for _, tt := range tests {
			svf := NewSQLValueFunction(tt.op, Oid(25), -1, 950)
			str := svf.String()

			assert.Contains(t, str, "SQLValueFunction")
			assert.Contains(t, str, tt.expected)
			assert.Contains(t, str, "@950")
		}
	})

	t.Run("Interface Compliance", func(t *testing.T) {
		svf := NewSQLValueFunction(SVFOP_CURRENT_DATE, Oid(0), 0, 0)

		var expr Expression = svf
		assert.NotNil(t, expr)

		var node Node = svf
		assert.Equal(t, T_SQLValueFunction, node.NodeTag())
	})
}

// TestXmlExpr tests the XmlExpr node implementation
func TestXmlExpr(t *testing.T) {
	t.Run("NewXmlExpr", func(t *testing.T) {
		op := IS_XMLCONCAT
		name := "test_element"
		namedArgs := []Expression{NewConst(Oid(25), Datum(uintptr(0)), false)}
		argNames := []string{"attr_name"}
		args := []Expression{NewConst(Oid(25), Datum(uintptr(0)), false)}
		xmloption := XMLOPTION_DOCUMENT
		indent := true
		typ := Oid(142) // xml type OID
		typeMod := -1
		location := 1000

		xe := NewXmlExpr(op, name, namedArgs, argNames, args, xmloption, indent, typ, typeMod, location)

		require.NotNil(t, xe)
		assert.Equal(t, T_XmlExpr, xe.Tag)
		assert.Equal(t, op, xe.Op)
		assert.Equal(t, name, xe.Name)
		assert.Equal(t, namedArgs, xe.NamedArgs)
		assert.Equal(t, argNames, xe.ArgNames)
		assert.Equal(t, args, xe.Args)
		assert.Equal(t, xmloption, xe.Xmloption)
		assert.Equal(t, indent, xe.Indent)
		assert.Equal(t, typ, xe.Type)
		assert.Equal(t, typeMod, xe.TypeMod)
		assert.Equal(t, location, xe.Location())
	})

	t.Run("String with different XML operations", func(t *testing.T) {
		tests := []struct {
			op       XmlExprOp
			expected string
		}{
			{IS_XMLCONCAT, "XMLCONCAT"},
			{IS_XMLELEMENT, "XMLELEMENT"},
			{IS_XMLFOREST, "XMLFOREST"},
			{IS_XMLPARSE, "XMLPARSE"},
			{IS_XMLPI, "XMLPI"},
			{IS_XMLROOT, "XMLROOT"},
			{IS_XMLSERIALIZE, "XMLSERIALIZE"},
			{IS_DOCUMENT, "IS_DOCUMENT"},
		}

		for _, tt := range tests {
			args := []Expression{NewConst(Oid(25), Datum(uintptr(0)), false)}
			xe := NewXmlExpr(tt.op, "", nil, nil, args, XMLOPTION_DOCUMENT, false, Oid(142), -1, 1050)
			str := xe.String()

			assert.Contains(t, str, "XmlExpr")
			assert.Contains(t, str, tt.expected)
			assert.Contains(t, str, "1 args")
			assert.Contains(t, str, "@1050")
		}
	})

	t.Run("Interface Compliance", func(t *testing.T) {
		xe := NewXmlExpr(IS_XMLCONCAT, "", nil, nil, nil, XMLOPTION_DOCUMENT, false, Oid(0), 0, 0)

		var expr Expression = xe
		assert.NotNil(t, expr)

		var node Node = xe
		assert.Equal(t, T_XmlExpr, node.NodeTag())
	})
}

// TestTableFunc tests the TableFunc node implementation
func TestTableFunc(t *testing.T) {
	t.Run("NewTableFunc", func(t *testing.T) {
		functype := TFT_XMLTABLE
		nsUris := []Expression{NewConst(Oid(25), Datum(uintptr(0)), false)}
		nsNames := []string{"ns1"}
		docexpr := NewConst(Oid(25), Datum(uintptr(0)), false)
		rowexpr := NewConst(Oid(25), Datum(uintptr(0)), false)
		colnames := []string{"col1", "col2"}
		coltypes := []Oid{Oid(25), Oid(23)} // text, int4
		coltypmods := []int{-1, -1}
		colcollations := []Oid{Oid(100), Oid(0)}
		colexprs := []Expression{NewConst(Oid(25), Datum(uintptr(0)), false)}
		coldefexprs := []Expression{NewConst(Oid(25), Datum(uintptr(0)), false)}
		colvalexprs := []Expression{NewConst(Oid(25), Datum(uintptr(0)), false)}
		passingvalexprs := []Expression{NewConst(Oid(23), Datum(42), false)}
		notnulls := []bool{true, false}
		plan := NewConst(Oid(25), Datum(uintptr(0)), false)
		ordinalitycol := 1
		location := 1100

		tf := NewTableFunc(functype, nsUris, nsNames, docexpr, rowexpr, colnames, coltypes, coltypmods, colcollations, colexprs, coldefexprs, colvalexprs, passingvalexprs, notnulls, plan, ordinalitycol, location)

		require.NotNil(t, tf)
		assert.Equal(t, T_TableFunc, tf.Tag)
		assert.Equal(t, functype, tf.Functype)
		assert.Equal(t, nsUris, tf.NsUris)
		assert.Equal(t, nsNames, tf.NsNames)
		assert.Equal(t, docexpr, tf.Docexpr)
		assert.Equal(t, rowexpr, tf.Rowexpr)
		assert.Equal(t, colnames, tf.Colnames)
		assert.Equal(t, coltypes, tf.Coltypes)
		assert.Equal(t, coltypmods, tf.Coltypmods)
		assert.Equal(t, colcollations, tf.Colcollations)
		assert.Equal(t, colexprs, tf.Colexprs)
		assert.Equal(t, coldefexprs, tf.Coldefexprs)
		assert.Equal(t, colvalexprs, tf.Colvalexprs)
		assert.Equal(t, passingvalexprs, tf.Passingvalexprs)
		assert.Equal(t, notnulls, tf.Notnulls)
		assert.Equal(t, plan, tf.Plan)
		assert.Equal(t, ordinalitycol, tf.Ordinalitycol)
		assert.Equal(t, location, tf.Location())
	})

	t.Run("String XMLTABLE", func(t *testing.T) {
		colnames := []string{"col1", "col2"}
		tf := NewTableFunc(TFT_XMLTABLE, nil, nil, nil, nil, colnames, nil, nil, nil, nil, nil, nil, nil, nil, nil, -1, 1150)
		str := tf.String()

		assert.Contains(t, str, "TableFunc")
		assert.Contains(t, str, "XMLTABLE")
		assert.Contains(t, str, "2 cols")
		assert.Contains(t, str, "@1150")
	})

	t.Run("String JSON_TABLE", func(t *testing.T) {
		colnames := []string{"col1"}
		tf := NewTableFunc(TFT_JSON_TABLE, nil, nil, nil, nil, colnames, nil, nil, nil, nil, nil, nil, nil, nil, nil, -1, 1200)
		str := tf.String()

		assert.Contains(t, str, "TableFunc")
		assert.Contains(t, str, "JSON_TABLE")
		assert.Contains(t, str, "1 cols")
		assert.Contains(t, str, "@1200")
	})

	t.Run("StatementType", func(t *testing.T) {
		tf := NewTableFunc(TFT_XMLTABLE, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, -1, 0)
		assert.Equal(t, "TABLE_FUNC", tf.StatementType())
	})

	t.Run("Interface Compliance", func(t *testing.T) {
		tf := NewTableFunc(TFT_XMLTABLE, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, -1, 0)

		var node Node = tf
		assert.Equal(t, T_TableFunc, node.NodeTag())

		var stmt Stmt = tf
		assert.Equal(t, "TABLE_FUNC", stmt.StatementType())
	})
}

// TestIntoClause tests the IntoClause node implementation
func TestIntoClause(t *testing.T) {
	t.Run("NewIntoClause", func(t *testing.T) {
		rel := NewRangeVar("table", "schema", "")
		colNames := NewNodeList(NewString("col1"), NewString("col2"))
		accessMethod := "heap"
		options := NewNodeList(NewDefElem("fillfactor", NewInteger(80)))
		onCommit := ONCOMMIT_DELETE_ROWS
		tableSpaceName := "my_tablespace"
		viewQuery := NewSelectStmt()
		skipData := true
		location := 1250

		ic := NewIntoClause(rel, colNames, accessMethod, options, onCommit, tableSpaceName, viewQuery, skipData, location)

		require.NotNil(t, ic)
		assert.Equal(t, T_IntoClause, ic.Tag)
		assert.Equal(t, rel, ic.Rel)
		assert.Equal(t, colNames, ic.ColNames)
		assert.Equal(t, accessMethod, ic.AccessMethod)
		assert.Equal(t, options, ic.Options)
		assert.Equal(t, onCommit, ic.OnCommit)
		assert.Equal(t, tableSpaceName, ic.TableSpaceName)
		assert.Equal(t, viewQuery, ic.ViewQuery)
		assert.Equal(t, skipData, ic.SkipData)
	})

	t.Run("String", func(t *testing.T) {
		rel := NewRangeVar("test_table", "", "")
		ic := NewIntoClause(rel, nil, "", nil, ONCOMMIT_NOOP, "", nil, false, 1300)
		str := ic.String()

		assert.Contains(t, str, "IntoClause")
		assert.Contains(t, str, "test_table")
		assert.Contains(t, str, "skipData=false")
		assert.Contains(t, str, "@1300")
	})

	t.Run("String with nil Rel", func(t *testing.T) {
		ic := NewIntoClause(nil, nil, "", nil, ONCOMMIT_NOOP, "", nil, true, 1350)
		str := ic.String()

		assert.Contains(t, str, "IntoClause")
		assert.Contains(t, str, "?")
		assert.Contains(t, str, "skipData=true")
		assert.Contains(t, str, "@1350")
	})

	t.Run("StatementType", func(t *testing.T) {
		ic := NewIntoClause(nil, nil, "", nil, ONCOMMIT_NOOP, "", nil, false, 0)
		assert.Equal(t, "INTO_CLAUSE", ic.StatementType())
	})

	t.Run("Interface Compliance", func(t *testing.T) {
		ic := NewIntoClause(nil, nil, "", nil, ONCOMMIT_NOOP, "", nil, false, 0)

		var node Node = ic
		assert.Equal(t, T_IntoClause, node.NodeTag())

		var stmt Stmt = ic
		assert.Equal(t, "INTO_CLAUSE", stmt.StatementType())
	})
}

// TestMergeAction tests the MergeAction node implementation
func TestMergeAction(t *testing.T) {
	t.Run("NewMergeAction", func(t *testing.T) {
		matchKind := MERGE_WHEN_MATCHED
		commandType := CMD_UPDATE
		override := OVERRIDING_USER_VALUE
		qual := NewConst(Oid(16), Datum(0), false)
		targetList := []*TargetEntry{NewTargetEntry(NewConst(Oid(23), Datum(1), false), AttrNumber(1), "col1")}
		updateColnos := []AttrNumber{AttrNumber(1), AttrNumber(2)}
		location := 1400

		ma := NewMergeAction(matchKind, commandType, override, qual, targetList, updateColnos, location)

		require.NotNil(t, ma)
		assert.Equal(t, T_MergeAction, ma.Tag)
		assert.Equal(t, matchKind, ma.MatchKind)
		assert.Equal(t, commandType, ma.CommandType)
		assert.Equal(t, override, ma.Override)
		assert.Equal(t, qual, ma.Qual)
		assert.Equal(t, targetList, ma.TargetList)
		assert.Equal(t, updateColnos, ma.UpdateColnos)
	})

	t.Run("String with different match kinds and commands", func(t *testing.T) {
		tests := []struct {
			matchKind     MergeMatchKind
			commandType   CmdType
			expectedMatch string
			expectedCmd   string
		}{
			{MERGE_WHEN_MATCHED, CMD_UPDATE, "MATCHED", "UPDATE"},
			{MERGE_WHEN_NOT_MATCHED_BY_SOURCE, CMD_DELETE, "NOT MATCHED BY SOURCE", "DELETE"},
			{MERGE_WHEN_NOT_MATCHED_BY_TARGET, CMD_INSERT, "NOT MATCHED BY TARGET", "INSERT"},
			{MERGE_WHEN_MATCHED, CMD_UNKNOWN, "MATCHED", "DO NOTHING"},
		}

		for _, tt := range tests {
			targetList := []*TargetEntry{NewTargetEntry(NewConst(Oid(23), Datum(1), false), AttrNumber(1), "col1")}
			ma := NewMergeAction(tt.matchKind, tt.commandType, OVERRIDING_NOT_SET, nil, targetList, nil, 1450)
			str := ma.String()

			assert.Contains(t, str, "MergeAction")
			assert.Contains(t, str, tt.expectedMatch)
			assert.Contains(t, str, tt.expectedCmd)
			assert.Contains(t, str, "1 targets")
			assert.Contains(t, str, "@1450")
		}
	})

	t.Run("StatementType", func(t *testing.T) {
		ma := NewMergeAction(MERGE_WHEN_MATCHED, CMD_UPDATE, OVERRIDING_NOT_SET, nil, nil, nil, 0)
		assert.Equal(t, "MERGE_ACTION", ma.StatementType())
	})

	t.Run("Interface Compliance", func(t *testing.T) {
		ma := NewMergeAction(MERGE_WHEN_MATCHED, CMD_UPDATE, OVERRIDING_NOT_SET, nil, nil, nil, 0)

		var node Node = ma
		assert.Equal(t, T_MergeAction, node.NodeTag())

		var stmt Stmt = ma
		assert.Equal(t, "MERGE_ACTION", stmt.StatementType())
	})
}
