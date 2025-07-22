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
	varno := Index(1)
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
	args := []Node{arg1, arg2}

	f := NewFuncExpr(funcid, funcresulttype, args)
	
	require.NotNil(t, f)
	assert.Equal(t, T_FuncExpr, f.NodeTag())
	assert.Equal(t, "FuncExpr", f.ExpressionType())
	assert.True(t, f.IsExpr())
	assert.Equal(t, funcid, f.Funcid)
	assert.Equal(t, funcresulttype, f.Funcresulttype)
	assert.Len(t, f.Args, 2)
	assert.Equal(t, arg1, f.Args[0])
	assert.Equal(t, arg2, f.Args[1])
	assert.Contains(t, f.String(), "FuncExpr")
	assert.Contains(t, f.String(), "2 args")
}

// TestOpExpr tests the OpExpr expression node.
func TestOpExpr(t *testing.T) {
	t.Run("BinaryOperator", func(t *testing.T) {
		opno := Oid(96) // "=" operator in PostgreSQL
		opfuncid := Oid(65) // int4eq function
		opresulttype := Oid(16) // bool
		left := NewVar(1, 1, 23)
		right := NewConst(23, 42, false)
		args := []Node{left, right}

		o := NewOpExpr(opno, opfuncid, opresulttype, args)
		
		require.NotNil(t, o)
		assert.Equal(t, T_OpExpr, o.NodeTag())
		assert.Equal(t, "OpExpr", o.ExpressionType())
		assert.True(t, o.IsExpr())
		assert.Equal(t, opno, o.Opno)
		assert.Equal(t, opfuncid, o.Opfuncid)
		assert.Equal(t, opresulttype, o.Opresulttype)
		assert.Len(t, o.Args, 2)
		assert.Contains(t, o.String(), "binary")
	})

	t.Run("UnaryOperator", func(t *testing.T) {
		opno := Oid(484) // "-" unary minus
		arg := NewVar(1, 1, 23)
		
		o := NewUnaryOp(opno, arg)
		
		require.NotNil(t, o)
		assert.Len(t, o.Args, 1)
		assert.Contains(t, o.String(), "unary")
	})

	t.Run("ConvenienceConstructor", func(t *testing.T) {
		opno := Oid(96)
		left := NewVar(1, 1, 23)
		right := NewConst(23, 42, false)
		
		o := NewBinaryOp(opno, left, right)
		
		require.NotNil(t, o)
		assert.Equal(t, opno, o.Opno)
		assert.Len(t, o.Args, 2)
		assert.Equal(t, left, o.Args[0])
		assert.Equal(t, right, o.Args[1])
	})
}

// TestBoolExpr tests the BoolExpr expression node.
func TestBoolExpr(t *testing.T) {
	t.Run("AndExpression", func(t *testing.T) {
		left := NewVar(1, 1, 16) // bool column
		right := NewVar(1, 2, 16) // bool column
		args := []Node{left, right}

		b := NewBoolExpr(AND_EXPR, args)
		
		require.NotNil(t, b)
		assert.Equal(t, T_BoolExpr, b.NodeTag())
		assert.Equal(t, "BoolExpr", b.ExpressionType())
		assert.True(t, b.IsExpr())
		assert.Equal(t, AND_EXPR, b.Boolop)
		assert.Len(t, b.Args, 2)
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
		assert.Len(t, b.Args, 1)
		assert.Contains(t, b.String(), "NOT")
	})

	t.Run("ConvenienceConstructor", func(t *testing.T) {
		left := NewVar(1, 1, 16)
		right := NewVar(1, 2, 16)

		and := NewAndExpr(left, right)
		require.NotNil(t, and)
		assert.Equal(t, AND_EXPR, and.Boolop)
		assert.Len(t, and.Args, 2)
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
		{"FuncExpr", NewFuncExpr(100, 23, []Node{})},
		{"OpExpr", NewOpExpr(96, 65, 16, []Node{})},
		{"BoolExpr", NewBoolExpr(AND_EXPR, []Node{})},
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
		funcExpr := NewFuncExpr(100, 23, []Node{})
		varExpr := NewVar(1, 1, 23)
		
		assert.True(t, IsFunction(funcExpr))
		assert.False(t, IsFunction(varExpr))
	})

	t.Run("GetExpressionArgs", func(t *testing.T) {
		arg1 := NewVar(1, 1, 23)
		arg2 := NewConst(23, 42, false)
		
		funcExpr := NewFuncExpr(100, 23, []Node{arg1, arg2})
		args := GetExpressionArgs(funcExpr)
		assert.Len(t, args, 2)
		assert.Equal(t, arg1, args[0])
		assert.Equal(t, arg2, args[1])

		opExpr := NewBinaryOp(96, arg1, arg2)
		args = GetExpressionArgs(opExpr)
		assert.Len(t, args, 2)

		boolExpr := NewAndExpr(arg1, arg2)
		args = GetExpressionArgs(boolExpr)
		assert.Len(t, args, 2)

		varExpr := NewVar(1, 1, 23)
		args = GetExpressionArgs(varExpr)
		assert.Nil(t, args)
	})

	t.Run("CountArgs", func(t *testing.T) {
		arg1 := NewVar(1, 1, 23)
		arg2 := NewConst(23, 42, false)
		
		funcExpr := NewFuncExpr(100, 23, []Node{arg1, arg2})
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
		assert.Len(t, or.Args, 2)
		
		// Verify structure
		leftArg := or.Args[0].(*BoolExpr)
		assert.Equal(t, AND_EXPR, leftArg.Boolop)
		assert.Len(t, leftArg.Args, 2)
	})

	t.Run("FunctionWithMultipleArgs", func(t *testing.T) {
		// Build: SUBSTRING(col1, 1, 5)
		
		stringCol := NewVar(1, 1, 25) // text column
		startPos := NewConst(23, 1, false)
		length := NewConst(23, 5, false)
		
		substring := NewFuncExpr(883, 25, []Node{stringCol, startPos, length})
		
		require.NotNil(t, substring)
		assert.Equal(t, T_FuncExpr, substring.NodeTag())
		assert.Len(t, substring.Args, 3)
		assert.Equal(t, Oid(883), substring.Funcid) // SUBSTRING function
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
		
		caseExpr := NewCaseExpr(25, statusVar, []Node{when1, when2}, elseResult)
		
		require.NotNil(t, caseExpr)
		assert.Equal(t, T_CaseExpr, caseExpr.NodeTag())
		assert.Equal(t, "CaseExpr", caseExpr.ExpressionType())
		assert.True(t, caseExpr.IsExpr())
		assert.Equal(t, statusVar, caseExpr.Arg)
		assert.Len(t, caseExpr.Args, 2)
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
		
		caseExpr := NewSearchedCase([]Node{when1, when2}, elseResult)
		
		require.NotNil(t, caseExpr)
		assert.Nil(t, caseExpr.Arg) // No implicit comparison argument
		assert.Len(t, caseExpr.Args, 2)
		assert.NotNil(t, caseExpr.Defresult)
	})

	t.Run("ConvenienceConstructor", func(t *testing.T) {
		statusVar := NewVar(1, 1, 23)
		whens := []Node{NewCaseWhen(NewConst(23, 1, false), NewString("Active"))}
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
	args := []Node{col1, col2, defaultVal}
	
	coalesce := NewCoalesceExpr(25, args)
	
	require.NotNil(t, coalesce)
	assert.Equal(t, T_CoalesceExpr, coalesce.NodeTag())
	assert.Equal(t, "CoalesceExpr", coalesce.ExpressionType())
	assert.True(t, coalesce.IsExpr())
	assert.Equal(t, Oid(25), coalesce.Coalescetype)
	assert.Len(t, coalesce.Args, 3)
	assert.Equal(t, col1, coalesce.Args[0])
	assert.Equal(t, col2, coalesce.Args[1])
	assert.Equal(t, defaultVal, coalesce.Args[2])
	assert.Contains(t, coalesce.String(), "3 args")
}

// TestArrayExpr tests the ArrayExpr expression node.
func TestArrayExpr(t *testing.T) {
	t.Run("SimpleArray", func(t *testing.T) {
		// ARRAY[1, 2, 3]
		elem1 := NewConst(23, 1, false)
		elem2 := NewConst(23, 2, false)
		elem3 := NewConst(23, 3, false)
		elements := []Node{elem1, elem2, elem3}
		
		array := NewArrayExpr(1007, 23, elements) // int4[] type
		
		require.NotNil(t, array)
		assert.Equal(t, T_ArrayExpr, array.NodeTag())
		assert.Equal(t, "ArrayExpr", array.ExpressionType())
		assert.True(t, array.IsExpr())
		assert.Equal(t, Oid(1007), array.ArrayTypeid)
		assert.Equal(t, Oid(23), array.ElementTypeid)
		assert.Len(t, array.Elements, 3)
		assert.False(t, array.Multidims)
		assert.Contains(t, array.String(), "3 elements")
		assert.Contains(t, array.String(), "1D")
	})

	t.Run("MultiDimArray", func(t *testing.T) {
		elements := []Node{NewConst(23, 1, false)}
		array := NewArrayExpr(1007, 23, elements)
		array.Multidims = true
		
		require.NotNil(t, array)
		assert.True(t, array.Multidims)
		assert.Contains(t, array.String(), "Multi-D")
	})

	t.Run("ConvenienceConstructor", func(t *testing.T) {
		elements := []Node{NewConst(23, 1, false), NewConst(23, 2, false)}
		array := NewArrayConstructor(elements)
		
		require.NotNil(t, array)
		assert.Len(t, array.Elements, 2)
		assert.Equal(t, Oid(0), array.ArrayTypeid) // Default unspecified
	})
}

// TestScalarArrayOpExpr tests the ScalarArrayOpExpr expression node.
func TestScalarArrayOpExpr(t *testing.T) {
	t.Run("AnyOperation", func(t *testing.T) {
		// column = ANY(ARRAY[1, 2, 3])
		column := NewVar(1, 1, 23)
		array := NewArrayConstructor([]Node{NewConst(23, 1, false), NewConst(23, 2, false), NewConst(23, 3, false)})
		
		anyExpr := NewScalarArrayOpExpr(96, true, column, array) // "=" operator with ANY
		
		require.NotNil(t, anyExpr)
		assert.Equal(t, T_ScalarArrayOpExpr, anyExpr.NodeTag())
		assert.Equal(t, "ScalarArrayOpExpr", anyExpr.ExpressionType())
		assert.True(t, anyExpr.IsExpr())
		assert.Equal(t, Oid(96), anyExpr.Opno)
		assert.True(t, anyExpr.UseOr)
		assert.Len(t, anyExpr.Args, 2)
		assert.Equal(t, column, anyExpr.Args[0])
		assert.Equal(t, array, anyExpr.Args[1])
		assert.Contains(t, anyExpr.String(), "ANY")
	})

	t.Run("AllOperation", func(t *testing.T) {
		// column <> ALL(ARRAY[1, 2, 3])
		column := NewVar(1, 1, 23)
		array := NewArrayConstructor([]Node{NewConst(23, 1, false)})
		
		allExpr := NewScalarArrayOpExpr(518, false, column, array) // "<>" operator with ALL
		
		require.NotNil(t, allExpr)
		assert.Equal(t, Oid(518), allExpr.Opno)
		assert.False(t, allExpr.UseOr)
		assert.Contains(t, allExpr.String(), "ALL")
	})

	t.Run("ConvenienceConstructors", func(t *testing.T) {
		column := NewVar(1, 1, 23)
		array := NewArrayConstructor([]Node{NewConst(23, 1, false)})
		
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
		fields := []Node{col1, col2, literal}
		
		row := NewRowExpr(fields, 2249) // record type OID
		
		require.NotNil(t, row)
		assert.Equal(t, T_RowExpr, row.NodeTag())
		assert.Equal(t, "RowExpr", row.ExpressionType())
		assert.True(t, row.IsExpr())
		assert.Equal(t, Oid(2249), row.RowTypeid)
		assert.Len(t, row.Args, 3)
		assert.Equal(t, col1, row.Args[0])
		assert.Equal(t, col2, row.Args[1])
		assert.Equal(t, literal, row.Args[2])
		assert.Contains(t, row.String(), "3 fields")
	})

	t.Run("ConvenienceConstructor", func(t *testing.T) {
		fields := []Node{NewConst(23, 1, false), NewConst(23, 2, false)}
		row := NewRowConstructor(fields)
		
		require.NotNil(t, row)
		assert.Len(t, row.Args, 2)
		assert.Equal(t, Oid(0), row.RowTypeid) // Default unspecified
	})
}

// TestTier2ExpressionInterfaces tests that Tier 2 expressions implement required interfaces.
func TestTier2ExpressionInterfaces(t *testing.T) {
	expressions := []struct {
		name string
		expr Expr
	}{
		{"CaseExpr", NewCaseExpr(25, nil, []Node{}, nil)},
		{"CaseWhen", NewCaseWhen(NewConst(16, 1, false), NewString("result"))},
		{"CoalesceExpr", NewCoalesceExpr(25, []Node{})},
		{"ArrayExpr", NewArrayExpr(1007, 23, []Node{})},
		{"ScalarArrayOpExpr", NewScalarArrayOpExpr(96, true, NewVar(1, 1, 23), NewArrayConstructor([]Node{}))},
		{"RowExpr", NewRowExpr([]Node{}, 2249)},
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
		caseExpr := NewCaseExpr(25, nil, []Node{}, nil)
		coalesceExpr := NewCoalesceExpr(25, []Node{})
		arrayExpr := NewArrayConstructor([]Node{})
		scalarArrayExpr := NewInExpr(NewVar(1, 1, 23), arrayExpr)
		rowExpr := NewRowConstructor([]Node{})
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
		array := NewArrayConstructor([]Node{})
		
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
		
		caseExpr := NewCaseExpr(25, nil, []Node{when1, when2}, elseResult)
		
		assert.Equal(t, 2, GetCaseWhenCount(caseExpr))
		assert.True(t, HasCaseElse(caseExpr))
		
		caseWithoutElse := NewCaseExpr(25, nil, []Node{when1}, nil)
		assert.False(t, HasCaseElse(caseWithoutElse))
		
		nonCaseExpr := NewVar(1, 1, 23)
		assert.Equal(t, 0, GetCaseWhenCount(nonCaseExpr))
		assert.False(t, HasCaseElse(nonCaseExpr))
	})

	t.Run("ArrayUtilities", func(t *testing.T) {
		elem1 := NewConst(23, 1, false)
		elem2 := NewConst(23, 2, false)
		elements := []Node{elem1, elem2}
		
		arrayExpr := NewArrayExpr(1007, 23, elements)
		
		retrievedElements := GetArrayElements(arrayExpr)
		assert.Len(t, retrievedElements, 2)
		assert.Equal(t, elem1, retrievedElements[0])
		assert.Equal(t, elem2, retrievedElements[1])
		
		assert.False(t, IsMultiDimArray(arrayExpr))
		arrayExpr.Multidims = true
		assert.True(t, IsMultiDimArray(arrayExpr))
		
		nonArrayExpr := NewVar(1, 1, 23)
		assert.Nil(t, GetArrayElements(nonArrayExpr))
		assert.False(t, IsMultiDimArray(nonArrayExpr))
	})

	t.Run("GetExpressionArgsEnhanced", func(t *testing.T) {
		// Test that GetExpressionArgs works with Tier 2 expressions
		caseExpr := NewCaseExpr(25, nil, []Node{NewCaseWhen(nil, nil)}, nil)
		args := GetExpressionArgs(caseExpr)
		assert.Len(t, args, 1)
		
		coalesceExpr := NewCoalesceExpr(25, []Node{NewVar(1, 1, 23), NewString("default")})
		args = GetExpressionArgs(coalesceExpr)
		assert.Len(t, args, 2)
		
		arrayExpr := NewArrayConstructor([]Node{NewConst(23, 1, false), NewConst(23, 2, false)})
		args = GetExpressionArgs(arrayExpr)
		assert.Len(t, args, 2) // Should return Elements
		
		rowExpr := NewRowConstructor([]Node{NewVar(1, 1, 23), NewVar(1, 2, 25)})
		args = GetExpressionArgs(rowExpr)
		assert.Len(t, args, 2)
	})
}

// TestComplexTier2ExpressionTrees tests building complex expression trees with Tier 2 expressions.
func TestComplexTier2ExpressionTrees(t *testing.T) {
	t.Run("NestedCaseInCoalesce", func(t *testing.T) {
		// COALESCE(CASE WHEN status = 1 THEN 'Active' ELSE NULL END, 'Unknown')
		
		statusVar := NewVar(1, 1, 23)
		statusCheck := NewBinaryOp(96, statusVar, NewConst(23, 1, false)) // status = 1
		when := NewCaseWhen(statusCheck, NewString("Active"))
		caseExpr := NewCaseExpr(25, nil, []Node{when}, NewNull())
		
		coalesceExpr := NewCoalesceExpr(25, []Node{caseExpr, NewString("Unknown")})
		
		require.NotNil(t, coalesceExpr)
		assert.Len(t, coalesceExpr.Args, 2)
		assert.Equal(t, caseExpr, coalesceExpr.Args[0])
		assert.Equal(t, "CoalesceExpr", coalesceExpr.ExpressionType())
		
		// Verify nested structure
		nestedCase := coalesceExpr.Args[0].(*CaseExpr)
		assert.Len(t, nestedCase.Args, 1)
	})

	t.Run("ArrayInInExpression", func(t *testing.T) {
		// user_id IN (SELECT ARRAY_AGG(id) FROM allowed_users)
		// Simplified as: user_id IN ARRAY[1, 2, 3]
		
		userIdVar := NewVar(1, 1, 23)
		allowedIds := NewArrayConstructor([]Node{
			NewConst(23, 1, false),
			NewConst(23, 2, false), 
			NewConst(23, 3, false),
		})
		
		inExpr := NewInExpr(userIdVar, allowedIds)
		
		require.NotNil(t, inExpr)
		assert.True(t, IsInExpr(inExpr))
		assert.Equal(t, "ScalarArrayOpExpr", inExpr.ExpressionType())
		assert.True(t, inExpr.UseOr)
		
		// Verify array structure
		arrayArg := inExpr.Args[1].(*ArrayExpr)
		assert.Len(t, arrayArg.Elements, 3)
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
		args := []Node{arg1}

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
		aggref.Aggdistinct = []Node{arg} // Add DISTINCT

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
		directArg := NewConst(25, 50, false) // 0.5 for percentile
		aggref := NewAggref(3972, 23, []Node{arg}) // percentile_cont
		aggref.Aggdirectargs = []Node{directArg}
		aggref.Aggorder = []Node{arg}

		assert.True(t, IsOrderedSetAggregate(aggref))
		assert.Equal(t, []Node{directArg}, GetAggregateDirectArgs(aggref))
		assert.Equal(t, []Node{arg}, GetAggregateOrderBy(aggref))
	})
}

// TestWindowFunc tests the WindowFunc expression node.
func TestWindowFunc(t *testing.T) {
	t.Run("Basic Window Function", func(t *testing.T) {
		winfnoid := Oid(3100) // ROW_NUMBER
		wintype := Oid(20)    // bigint
		winref := Index(1)
		args := []Node{NewVar(1, 1, 23)}

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
		winFunc := NewWindowFunc(2108, 23, []Node{arg}, 1) // SUM over window
		winFunc.Winagg = true

		assert.True(t, IsAggregate(winFunc)) // Now it's an aggregate
		assert.True(t, IsSimpleWindowAgg(winFunc))
	})

	t.Run("Window Function with Filter", func(t *testing.T) {
		arg := NewVar(1, 1, 23)
		filter := NewBinaryOp(96, arg, NewConst(23, 0, false))
		winFunc := NewWindowFunc(2108, 23, []Node{arg}, 1) // SUM
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
		threshold := NewConst(23, 10, false) // 10
		condition := NewBinaryOp(521, countStar, threshold) // COUNT(*) > 10
		highResult := NewString("High")
		lowResult := NewString("Low")
		whenClause := NewCaseWhen(condition, highResult)
		caseExpr := NewSearchedCase([]Node{whenClause}, lowResult)

		// Test that we can extract arguments properly
		caseArgs := GetExpressionArgs(caseExpr)
		assert.Len(t, caseArgs, 1)
		
		// The WHEN clause should contain the condition with aggregate
		whenArgs := GetExpressionArgs(caseArgs[0].(*CaseWhen))
		assert.Len(t, whenArgs, 2) // condition and result
		
		// The condition should be an OpExpr containing the aggregate
		conditionArgs := GetExpressionArgs(whenArgs[0])
		assert.Len(t, conditionArgs, 2) // COUNT(*) and 10
		assert.True(t, IsAggref(conditionArgs[0]))
		assert.True(t, IsStarAggregate(conditionArgs[0]))
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
		assert.Len(t, sublinkArgs, 1) // Just the subquery
		assert.Equal(t, subquery, sublinkArgs[0])
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
		assert.Len(t, existsArgs, 1)
		assert.Equal(t, subquery, existsArgs[0])
	})
}