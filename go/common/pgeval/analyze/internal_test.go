// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package analyze

// The helpers here are exercised directly because the SQL that would reach
// them needs catalog content the gateway does not carry (domains, typmods,
// range types, enums, user functions) or a tree shape the grammar does not
// produce. Each test states which upstream branch it stands in for.

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/parser/datum"
	"github.com/multigres/multigres/go/common/parser/pgoid"
	"github.com/multigres/multigres/go/common/pgcatalog"
	_ "github.com/multigres/multigres/go/common/pgeval/funcs"
	"github.com/multigres/multigres/go/common/pgeval/pgerror"
)

// catch runs fn and returns the diagnostic it raised, failing if it raised
// none.
func catch(t *testing.T, fn func()) *mterrors.PgDiagnostic {
	t.Helper()
	err := pgerror.Recover(fn)
	require.Error(t, err)
	var diag *mterrors.PgDiagnostic
	require.True(t, errors.As(err, &diag), "%v", err)
	return diag
}

func int4Const(v int32) *ast.Const {
	return makeConst(pgoid.INT4OID, -1, pgoid.InvalidOid, 4, datum.Int32GetDatum(v), false, true)
}

func typedConst(typ Oid, typmod int32) *ast.Const {
	return makeConst(typ, typmod, pgoid.InvalidOid, 4, datum.Int32GetDatum(0), false, true)
}

func arrayOf(t *testing.T, elem Oid) Oid {
	t.Helper()
	arr := getArrayType(elem)
	require.NotEqual(t, pgoid.InvalidOid, arr)
	return arr
}

func castFunc(t *testing.T, source, target Oid) Oid {
	t.Helper()
	c := pgcatalog.LookupCast(source, target)
	require.NotNil(t, c, "no cast %d -> %d", source, target)
	require.NotEqual(t, pgoid.InvalidOid, c.Func)
	return c.Func
}

func TestExprTypmod(t *testing.T) {
	t.Parallel()
	i4, i8, varchar := pgoid.INT4OID, pgoid.INT8OID, pgoid.VARCHAROID
	c7, c7b, c9, cNeg := typedConst(varchar, 7), typedConst(varchar, 7), typedConst(varchar, 9), typedConst(varchar, -1)

	assert.Equal(t, int32(-1), exprTypmod(nil))
	assert.Equal(t, int32(7), exprTypmod(c7))
	p := ast.NewParam(ast.PARAM_EXTERN, 1, varchar)
	p.Paramtypmod = 5
	assert.Equal(t, int32(5), exprTypmod(p))
	assert.Equal(t, int32(7), exprTypmod(ast.NewNamedArgExpr(c7, "x", 0, -1)))
	assert.Equal(t, int32(7), exprTypmod(ast.NewNullIfExpr(0, 0, varchar, ast.NewNodeList(c7, c9))))
	r := ast.NewRelabelType(c7, varchar, 11, ast.COERCE_EXPLICIT_CAST)
	assert.Equal(t, int32(11), exprTypmod(r))
	assert.Equal(t, int32(7), exprTypmod(ast.NewCollateExpr(c7, 100)))
	assert.Equal(t, int32(3), exprTypmod(ast.NewCaseTestExpr(varchar, 3, 0, -1)))
	d := ast.NewCoerceToDomain(c7, varchar, 13, ast.COERCE_EXPLICIT_CAST)
	assert.Equal(t, int32(13), exprTypmod(d))
	assert.Equal(t, int32(-1), exprTypmod(ast.NewString("not an expression")))

	t.Run("length coercion", func(t *testing.T) {
		coerce := func(format ast.CoercionForm, args ...ast.Node) *ast.FuncExpr {
			f := ast.NewFuncExpr(castFunc(t, varchar, varchar), varchar, ast.NewNodeList(args...))
			f.Funcformat = format
			return f
		}
		assert.Equal(t, int32(12), exprTypmod(coerce(ast.COERCE_EXPLICIT_CAST, c7, int4Const(12))))
		assert.Equal(t, int32(12), exprTypmod(coerce(ast.COERCE_IMPLICIT_CAST, c7, int4Const(12), c7)))
		assert.Equal(t, int32(-1), exprTypmod(coerce(ast.COERCE_EXPLICIT_CALL, c7, int4Const(12))), "not a coercion")
		assert.Equal(t, int32(-1), exprTypmod(coerce(ast.COERCE_EXPLICIT_CAST, c7)), "too few arguments")
		assert.Equal(t, int32(-1), exprTypmod(coerce(ast.COERCE_EXPLICIT_CAST, c7, c7)), "second argument not an int4 Const")
		ac := ast.NewArrayCoerceExpr(c7, c7, arrayOf(t, varchar), ast.COERCE_EXPLICIT_CAST)
		ac.Resulttypmod = 4
		assert.Equal(t, int32(4), exprTypmod(ac))
		ac.Resulttypmod = -1
		assert.Equal(t, int32(-1), exprTypmod(ac))
		_, ok := exprIsLengthCoercion(ac)
		assert.False(t, ok, "an array coercion without a typmod is not a length coercion")
		ac.Resulttypmod = 4
		typmod, ok := exprIsLengthCoercion(ac)
		assert.True(t, ok)
		assert.Equal(t, int32(4), typmod)
		_, ok = exprIsLengthCoercion(c7)
		assert.False(t, ok)
	})

	t.Run("CASE", func(t *testing.T) {
		when := func(result ast.Node) *ast.NodeList {
			return ast.NewNodeList(ast.NewCaseWhen(makeConst(pgoid.BOOLOID, -1, 0, 1, datum.BoolGetDatum(true), false, true), result))
		}
		assert.Equal(t, int32(7), exprTypmod(ast.NewCaseExpr(varchar, nil, when(c7b), c7)))
		assert.Equal(t, int32(-1), exprTypmod(ast.NewCaseExpr(varchar, nil, when(c7b), nil)), "no ELSE")
		assert.Equal(t, int32(-1), exprTypmod(ast.NewCaseExpr(i4, nil, when(c7b), c7)), "ELSE type differs")
		assert.Equal(t, int32(-1), exprTypmod(ast.NewCaseExpr(varchar, nil, when(c7b), cNeg)), "ELSE typmod unknown")
		assert.Equal(t, int32(-1), exprTypmod(ast.NewCaseExpr(varchar, nil, when(c9), c7)), "WHEN result differs")
	})

	t.Run("ARRAY COALESCE GREATEST", func(t *testing.T) {
		arr := ast.NewArrayExpr(arrayOf(t, varchar), varchar, ast.NewNodeList(c7, c7b))
		assert.Equal(t, int32(7), exprTypmod(arr))
		assert.Equal(t, int32(-1), exprTypmod(ast.NewArrayExpr(arrayOf(t, varchar), varchar, ast.NewNodeList())), "empty")
		assert.Equal(t, int32(-1), exprTypmod(ast.NewArrayExpr(arrayOf(t, varchar), varchar, ast.NewNodeList(cNeg, c7))), "first typmod unknown")
		assert.Equal(t, int32(-1), exprTypmod(ast.NewArrayExpr(arrayOf(t, varchar), varchar, ast.NewNodeList(c7, c9))), "elements differ")
		multi := ast.NewArrayExpr(arrayOf(t, varchar), varchar, ast.NewNodeList(arr))
		multi.Multidims = true
		assert.Equal(t, int32(7), exprTypmod(multi), "multidims compares against the array type")

		assert.Equal(t, int32(7), exprTypmod(ast.NewCoalesceExpr(varchar, ast.NewNodeList(c7, c7b))))
		assert.Equal(t, int32(-1), exprTypmod(ast.NewCoalesceExpr(i8, ast.NewNodeList(c7, c7b))), "type differs")
		assert.Equal(t, int32(-1), exprTypmod(ast.NewCoalesceExpr(varchar, ast.NewNodeList(cNeg, c7))), "first typmod unknown")
		assert.Equal(t, int32(-1), exprTypmod(ast.NewCoalesceExpr(varchar, ast.NewNodeList(c7, c9))), "arguments differ")

		mm := func(typ Oid, args ...ast.Node) *ast.MinMaxExpr {
			return ast.NewMinMaxExpr(typ, 0, 0, ast.IS_GREATEST, ast.NewNodeList(args...), -1)
		}
		assert.Equal(t, int32(7), exprTypmod(mm(varchar, c7, c7b)))
		assert.Equal(t, int32(-1), exprTypmod(mm(i8, c7, c7b)))
		assert.Equal(t, int32(-1), exprTypmod(mm(varchar, cNeg, c7)))
		assert.Equal(t, int32(-1), exprTypmod(mm(varchar, c7, c9)))
	})
}

func TestExprTypeAndCollation(t *testing.T) {
	t.Parallel()
	i4, text := pgoid.INT4OID, pgoid.TEXTOID
	c := int4Const(1)
	c.Constcollid = 100
	bool1 := makeConst(pgoid.BOOLOID, -1, 0, 1, datum.BoolGetDatum(true), false, true)

	assert.Equal(t, pgoid.InvalidOid, exprType(nil))
	assert.Equal(t, i4, exprType(ast.NewNamedArgExpr(c, "x", 0, -1)))
	assert.Equal(t, i4, exprType(ast.NewCollateExpr(c, 100)))
	assert.Equal(t, text, exprType(ast.NewCoerceToDomain(c, text, -1, ast.COERCE_EXPLICIT_CAST)))
	assert.Equal(t, pgoid.BOOLOID, exprType(ast.NewScalarArrayOpExpr(0, true, c, c)))
	diag := catch(t, func() { exprType(ast.NewString("x")) })
	assert.Equal(t, mterrors.PgSSInternalError, diag.Code)
	assert.Equal(t, "unrecognized node type: String", diag.Message)

	// Collations are carried from the node fields; nothing assigns them yet.
	p := ast.NewParam(ast.PARAM_EXTERN, 1, text)
	p.Paramcollid = 100
	f := ast.NewFuncExpr(0, text, ast.NewNodeList(c))
	f.Funccollid = 100
	op := ast.NewOpExpr(0, 0, text, ast.NewNodeList(c))
	op.Opcollid = 100
	dist := ast.NewDistinctExpr(0, 0, pgoid.BOOLOID, ast.NewNodeList(c, c))
	dist.Opcollid = 100
	nullif := ast.NewNullIfExpr(0, 0, i4, ast.NewNodeList(c, c))
	nullif.Opcollid = 100
	r := ast.NewRelabelType(c, text, -1, ast.COERCE_EXPLICIT_CAST)
	r.Resultcollid = 100
	io := ast.NewCoerceViaIO(c, text, ast.COERCE_EXPLICIT_CAST)
	io.Resultcollid = 100
	ac := ast.NewArrayCoerceExpr(c, c, arrayOf(t, text), ast.COERCE_EXPLICIT_CAST)
	ac.Resultcollid = 100
	cs := ast.NewCaseExpr(i4, nil, ast.NewNodeList(ast.NewCaseWhen(bool1, c)), c)
	cs.Casecollid = 100
	arr := ast.NewArrayExpr(arrayOf(t, i4), i4, ast.NewNodeList(c))
	arr.ArrayCollid = 100
	co := ast.NewCoalesceExpr(i4, ast.NewNodeList(c))
	co.Coalescecollid = 100
	dom := ast.NewCoerceToDomain(c, i4, -1, ast.COERCE_EXPLICIT_CAST)
	dom.Resultcollid = 100
	for name, n := range map[string]ast.Node{
		"Const": c, "Param": p, "FuncExpr": f, "NamedArgExpr": ast.NewNamedArgExpr(c, "x", 0, -1), "OpExpr": op,
		"DistinctExpr": dist, "NullIfExpr": nullif, "RelabelType": r, "CoerceViaIO": io, "ArrayCoerceExpr": ac,
		"CollateExpr": ast.NewCollateExpr(c, 100), "CaseExpr": cs, "CaseTestExpr": ast.NewCaseTestExpr(i4, -1, 100, -1),
		"ArrayExpr": arr, "CoalesceExpr": co, "MinMaxExpr": ast.NewMinMaxExpr(i4, 100, 0, ast.IS_LEAST, ast.NewNodeList(c), -1),
		"CoerceToDomain": dom,
	} {
		assert.Equal(t, Oid(100), exprCollation(n), name)
	}
	assert.Equal(t, pgoid.InvalidOid, exprCollation(ast.NewNullTest(c, ast.IS_NULL)))
	assert.Equal(t, -1, exprLocation(nil))
	assert.Equal(t, "nil", nodeTypeName(nil))
}

func TestWalkExpr(t *testing.T) {
	t.Parallel()
	i4, text := pgoid.INT4OID, pgoid.TEXTOID
	c := int4Const(1)
	bool1 := makeConst(pgoid.BOOLOID, -1, 0, 1, datum.BoolGetDatum(true), false, true)
	f := ast.NewFuncExpr(0, text, ast.NewNodeList(c))
	f.Funcretset = true
	op := ast.NewOpExpr(0, 0, text, ast.NewNodeList(c))
	op.Opretset = true

	// Every node kind analysis produces is walkable, and the count of
	// visited nodes is the count of nodes.
	for name, tc := range map[string]struct {
		node ast.Node
		want int
	}{
		"Const":           {c, 1},
		"Param":           {ast.NewParam(ast.PARAM_EXTERN, 1, i4), 1},
		"CaseTestExpr":    {ast.NewCaseTestExpr(i4, -1, 0, -1), 1},
		"FuncExpr":        {ast.NewFuncExpr(0, text, ast.NewNodeList(c, c)), 3},
		"NamedArgExpr":    {ast.NewNamedArgExpr(c, "x", 0, -1), 2},
		"OpExpr":          {ast.NewOpExpr(0, 0, text, ast.NewNodeList(c)), 2},
		"DistinctExpr":    {ast.NewDistinctExpr(0, 0, pgoid.BOOLOID, ast.NewNodeList(c, c)), 3},
		"NullIfExpr":      {ast.NewNullIfExpr(0, 0, i4, ast.NewNodeList(c, c)), 3},
		"ScalarArrayOp":   {ast.NewScalarArrayOpExpr(0, true, c, c), 3},
		"BoolExpr":        {ast.NewBoolExpr(ast.AND_EXPR, ast.NewNodeList(bool1, bool1)), 3},
		"RelabelType":     {ast.NewRelabelType(c, text, -1, ast.COERCE_EXPLICIT_CAST), 2},
		"CoerceViaIO":     {ast.NewCoerceViaIO(c, text, ast.COERCE_EXPLICIT_CAST), 2},
		"ArrayCoerceExpr": {ast.NewArrayCoerceExpr(c, c, arrayOf(t, text), ast.COERCE_EXPLICIT_CAST), 3},
		"CollateExpr":     {ast.NewCollateExpr(c, 100), 2},
		"CaseExpr":        {ast.NewCaseExpr(i4, c, ast.NewNodeList(ast.NewCaseWhen(bool1, c)), c), 5},
		"ArrayExpr":       {ast.NewArrayExpr(arrayOf(t, i4), i4, ast.NewNodeList(c, c)), 3},
		"CoalesceExpr":    {ast.NewCoalesceExpr(i4, ast.NewNodeList(c, c)), 3},
		"MinMaxExpr":      {ast.NewMinMaxExpr(i4, 0, 0, ast.IS_LEAST, ast.NewNodeList(c, c), -1), 3},
		"NullTest":        {ast.NewNullTest(c, ast.IS_NULL), 2},
		"BooleanTest":     {ast.NewBooleanTest(bool1, ast.IS_TRUE), 2},
		"CoerceToDomain":  {ast.NewCoerceToDomain(c, i4, -1, ast.COERCE_EXPLICIT_CAST), 2},
		"nil list":        {ast.NewFuncExpr(0, text, nil), 1},
	} {
		visited := 0
		assert.True(t, walkExpr(tc.node, func(ast.Node) bool { visited++; return true }), name)
		assert.Equal(t, tc.want, visited, name)
	}

	// Returning false stops the walk, at the top or in a child.
	visited := 0
	assert.False(t, walkExpr(ast.NewFuncExpr(0, text, ast.NewNodeList(c, c)), func(ast.Node) bool { visited++; return false }))
	assert.Equal(t, 1, visited)
	visited = 0
	stopInChild := func(n ast.Node) bool { visited++; _, isConst := n.(*ast.Const); return !isConst }
	assert.False(t, walkExpr(ast.NewFuncExpr(0, text, ast.NewNodeList(c, c)), stopInChild))
	assert.Equal(t, 2, visited)
	for name, node := range map[string]ast.Node{
		"CaseExpr arg":    ast.NewCaseExpr(i4, c, ast.NewNodeList(ast.NewCaseWhen(bool1, c)), c),
		"CaseExpr when":   ast.NewCaseExpr(i4, nil, ast.NewNodeList(ast.NewCaseWhen(c, c)), c),
		"CaseExpr result": ast.NewCaseExpr(i4, nil, ast.NewNodeList(ast.NewCaseWhen(bool1, c)), c),
		"ArrayCoerce arg": ast.NewArrayCoerceExpr(c, c, arrayOf(t, text), ast.COERCE_EXPLICIT_CAST),
	} {
		assert.False(t, walkExpr(node, stopInChild), name)
	}
	assert.True(t, walkExpr(nil, func(ast.Node) bool { t.Fatal("visited nil"); return true }))
	diag := catch(t, func() { walkExpr(ast.NewString("x"), func(ast.Node) bool { return true }) })
	assert.Equal(t, "unrecognized node type: String", diag.Message)

	assert.True(t, expressionReturnsSet(f))
	assert.True(t, expressionReturnsSet(ast.NewCoalesceExpr(text, ast.NewNodeList(c, op))))
	assert.False(t, expressionReturnsSet(ast.NewFuncExpr(0, text, ast.NewNodeList(c))))
}

func TestApplyRelabelType(t *testing.T) {
	t.Parallel()
	i4, oid, text := pgoid.INT4OID, pgoid.OIDOID, pgoid.TEXTOID
	c := int4Const(1)
	p := ast.NewParam(ast.PARAM_EXTERN, 1, i4)
	p.Paramtypmod = -1

	// Stacked relabels collapse to one, keeping the innermost argument.
	inner := ast.NewRelabelType(p, oid, -1, ast.COERCE_IMPLICIT_CAST)
	outer := ast.NewRelabelType(inner, text, -1, ast.COERCE_IMPLICIT_CAST)
	r, ok := applyRelabelType(outer, oid, 5, 100, ast.COERCE_EXPLICIT_CAST, 7, false).(*ast.RelabelType)
	require.True(t, ok)
	assert.Same(t, p, r.Arg)
	assert.Equal(t, oid, r.Resulttype)
	assert.Equal(t, int32(5), r.Resulttypmod)
	assert.Equal(t, Oid(100), r.Resultcollid)
	assert.Equal(t, ast.COERCE_EXPLICIT_CAST, r.Relabelformat)
	assert.Equal(t, 7, r.Location())

	// A Const is retyped in place, or in a copy.
	same := applyRelabelType(c, oid, -1, 0, ast.COERCE_IMPLICIT_CAST, -1, true)
	assert.Same(t, c, same)
	assert.Equal(t, oid, c.Consttype)
	copied := applyRelabelType(c, text, 3, 100, ast.COERCE_IMPLICIT_CAST, -1, false).(*ast.Const)
	assert.NotSame(t, c, copied)
	assert.Equal(t, oid, c.Consttype, "original untouched")
	assert.Equal(t, text, copied.Consttype)
	assert.Equal(t, int32(3), copied.Consttypmod)
	assert.Equal(t, Oid(100), copied.Constcollid)

	// A relabel that changes nothing is dropped.
	assert.Same(t, p, applyRelabelType(ast.NewRelabelType(p, oid, -1, ast.COERCE_IMPLICIT_CAST), i4, -1, 0, ast.COERCE_IMPLICIT_CAST, -1, false))
}

func TestHideCoercionNode(t *testing.T) {
	t.Parallel()
	c := int4Const(1)
	f := ast.NewFuncExpr(0, pgoid.INT8OID, ast.NewNodeList(c))
	f.Funcformat = ast.COERCE_EXPLICIT_CAST
	r := ast.NewRelabelType(c, pgoid.OIDOID, -1, ast.COERCE_EXPLICIT_CAST)
	io := ast.NewCoerceViaIO(c, pgoid.TEXTOID, ast.COERCE_EXPLICIT_CAST)
	ac := ast.NewArrayCoerceExpr(c, c, pgoid.TEXTARRAYOID, ast.COERCE_EXPLICIT_CAST)
	d := ast.NewCoerceToDomain(c, pgoid.INT4OID, -1, ast.COERCE_EXPLICIT_CAST)
	for _, n := range []ast.Node{f, r, io, ac, d} {
		hideCoercionNode(n)
	}
	assert.Equal(t, ast.COERCE_IMPLICIT_CAST, f.Funcformat)
	assert.Equal(t, ast.COERCE_IMPLICIT_CAST, r.Relabelformat)
	assert.Equal(t, ast.COERCE_IMPLICIT_CAST, io.Coerceformat)
	assert.Equal(t, ast.COERCE_IMPLICIT_CAST, ac.Coerceformat)
	assert.Equal(t, ast.COERCE_IMPLICIT_CAST, d.Coercionformat)
	diag := catch(t, func() { hideCoercionNode(c) })
	assert.Equal(t, "unsupported node type: Const", diag.Message)
}

// TestTypmodCoercion stands in for the typmod paths that typenameTypeMod
// refuses at the SQL level: varchar(n), numeric(p,s) and their arrays.
func TestTypmodCoercion(t *testing.T) {
	t.Parallel()
	i4, varchar, numeric := pgoid.INT4OID, pgoid.VARCHAROID, pgoid.NUMERICOID
	varcharArr := arrayOf(t, varchar)

	t.Run("findTypmodCoercionFunction", func(t *testing.T) {
		path, fn := findTypmodCoercionFunction(varchar)
		assert.Equal(t, coercionPathFunc, path)
		assert.Equal(t, castFunc(t, varchar, varchar), fn)
		path, fn = findTypmodCoercionFunction(varcharArr)
		assert.Equal(t, coercionPathArrayCoerce, path)
		assert.Equal(t, castFunc(t, varchar, varchar), fn)
		path, _ = findTypmodCoercionFunction(i4)
		assert.Equal(t, coercionPathNone, path)
	})

	t.Run("coerceTypeTypmod", func(t *testing.T) {
		p := ast.NewParam(ast.PARAM_EXTERN, 1, varchar)
		p.Paramtypmod = -1
		assert.Same(t, p, coerceTypeTypmod(p, varchar, -1, ast.COERCION_EXPLICIT, ast.COERCE_EXPLICIT_CAST, -1, false), "already that typmod")

		// varchar(varchar, int4, bool): the typmod and isExplicit flag are
		// passed as constants.
		f, ok := coerceTypeTypmod(p, varchar, 10, ast.COERCION_EXPLICIT, ast.COERCE_EXPLICIT_CAST, 3, false).(*ast.FuncExpr)
		require.True(t, ok)
		assert.Equal(t, castFunc(t, varchar, varchar), f.Funcid)
		require.Len(t, f.Args.Items, 3)
		assert.Same(t, p, f.Args.Items[0])
		assert.Equal(t, int32(10), datum.DatumGetInt32(f.Args.Items[1].(*ast.Const).Constvalue))
		assert.True(t, datum.DatumGetBool(f.Args.Items[2].(*ast.Const).Constvalue))
		assert.Equal(t, int32(10), exprTypmod(f), "the result is a length coercion")
		assert.Equal(t, 3, f.Location())
		f = coerceTypeTypmod(p, varchar, 10, ast.COERCION_IMPLICIT, ast.COERCE_IMPLICIT_CAST, -1, false).(*ast.FuncExpr)
		assert.False(t, datum.DatumGetBool(f.Args.Items[2].(*ast.Const).Constvalue))

		// numeric(numeric, int4) takes no isExplicit flag.
		np := ast.NewParam(ast.PARAM_EXTERN, 1, numeric)
		f = coerceTypeTypmod(np, numeric, 655366, ast.COERCION_EXPLICIT, ast.COERCE_EXPLICIT_CAST, -1, false).(*ast.FuncExpr)
		assert.Len(t, f.Args.Items, 2)

		// A negative target typmod only relabels.
		p.Paramtypmod = 10
		r, ok := coerceTypeTypmod(p, varchar, -1, ast.COERCION_EXPLICIT, ast.COERCE_EXPLICIT_CAST, -1, false).(*ast.RelabelType)
		require.True(t, ok)
		assert.Equal(t, int32(-1), r.Resulttypmod)

		// hideInputCoercion demotes the input's own coercion node.
		inner := ast.NewCoerceViaIO(int4Const(1), varchar, ast.COERCE_EXPLICIT_CAST)
		coerceTypeTypmod(inner, varchar, 10, ast.COERCION_EXPLICIT, ast.COERCE_EXPLICIT_CAST, -1, true)
		assert.Equal(t, ast.COERCE_IMPLICIT_CAST, inner.Coerceformat)

		// An array applies the element function through ArrayCoerceExpr.
		ap := ast.NewParam(ast.PARAM_EXTERN, 1, varcharArr)
		ac, ok := coerceTypeTypmod(ap, varcharArr, 10, ast.COERCION_EXPLICIT, ast.COERCE_EXPLICIT_CAST, -1, false).(*ast.ArrayCoerceExpr)
		require.True(t, ok)
		assert.Same(t, ap, ac.Arg)
		assert.Equal(t, varcharArr, ac.Resulttype)
		assert.Equal(t, int32(10), ac.Resulttypmod)
		elem, ok := ac.Elemexpr.(*ast.FuncExpr)
		require.True(t, ok)
		_, ok = elem.Args.Items[0].(*ast.CaseTestExpr)
		assert.True(t, ok)
	})

	t.Run("coerceToDomain is the identity without domains", func(t *testing.T) {
		p := ast.NewParam(ast.PARAM_EXTERN, 1, varchar)
		assert.Same(t, p, coerceToDomain(p, varchar, -1, varchar, ast.COERCION_EXPLICIT, ast.COERCE_EXPLICIT_CAST, -1, false))
		assert.Same(t, p, coerceToDomain(p, pgoid.InvalidOid, -1, varchar, ast.COERCION_EXPLICIT, ast.COERCE_EXPLICIT_CAST, -1, true))
	})

	t.Run("buildCoercionExpression internal errors", func(t *testing.T) {
		p := ast.NewParam(ast.PARAM_EXTERN, 1, varchar)
		build := func(path coercionPathType, fn, target Oid) func() {
			return func() {
				buildCoercionExpression(p, path, fn, target, -1, ast.COERCION_EXPLICIT, ast.COERCE_EXPLICIT_CAST, -1)
			}
		}
		fourArgs := pgcatalog.ProcsByName("width_bucket")[0].Oid
		for name, tc := range []struct {
			fn      func()
			message string
		}{
			{build(coercionPathFunc, pgoid.InvalidOid, i4), "coercion function expected"},
			{build(coercionPathFunc, 999999, i4), "cache lookup failed for function 999999"},
			{build(coercionPathFunc, fourArgs, i4), "takes 4 arguments"},
			{build(coercionPathCoerceViaIO, castFunc(t, varchar, varchar), i4), "unexpected coercion function"},
			{build(coercionPathNone, pgoid.InvalidOid, i4), "unsupported pathtype"},
			{build(coercionPathArrayCoerce, pgoid.InvalidOid, varcharArr), "source type character varying is not an array"},
			{func() {
				buildCoercionExpression(ast.NewParam(ast.PARAM_EXTERN, 1, varcharArr), coercionPathArrayCoerce,
					pgoid.InvalidOid, i4, -1, ast.COERCION_EXPLICIT, ast.COERCE_EXPLICIT_CAST, -1)
			}, "target type integer is not an array"},
		} {
			diag := catch(t, tc.fn)
			assert.Equal(t, mterrors.PgSSInternalError, diag.Code, name)
			assert.Contains(t, diag.Message, tc.message, name)
		}
	})
}

func TestSelectCommonTypeFromOids(t *testing.T) {
	t.Parallel()
	i2, i4, f8, b, text, unknown := pgoid.INT2OID, pgoid.INT4OID, pgoid.FLOAT8OID, pgoid.BOOLOID, pgoid.TEXTOID, pgoid.UNKNOWNOID
	assert.Equal(t, i4, selectCommonTypeFromOids([]Oid{unknown, i4}, false))
	assert.Equal(t, text, selectCommonTypeFromOids([]Oid{unknown, unknown}, false))
	assert.Equal(t, i4, selectCommonTypeFromOids([]Oid{i2, i4}, false))
	assert.Equal(t, i4, selectCommonTypeFromOids([]Oid{i4, i2}, false))
	assert.Equal(t, f8, selectCommonTypeFromOids([]Oid{i4, f8}, false), "preferred type")
	assert.Equal(t, pgoid.InvalidOid, selectCommonTypeFromOids([]Oid{i4, b}, true))
	diag := catch(t, func() { selectCommonTypeFromOids([]Oid{i4, b}, false) })
	assert.Equal(t, mterrors.PgSSDatatypeMismatch, diag.Code)
	assert.Equal(t, "argument types integer and boolean cannot be matched", diag.Message)

	assert.True(t, verifyCommonTypeFromOids(pgoid.INT8OID, []Oid{i4, i2}))
	assert.False(t, verifyCommonTypeFromOids(i4, []Oid{pgoid.MONEYOID}))
}

// TestEnforceGenericTypeConsistency drives the polymorphic resolution rules
// that no builtin signature reaches through SQL: the anyelement/anyarray
// cross-checks, range and multirange families, allowPoly, and every
// polymorphic result type.
func TestEnforceGenericTypeConsistency(t *testing.T) {
	t.Parallel()
	i4, i8, unknown, void := pgoid.INT4OID, pgoid.INT8OID, pgoid.UNKNOWNOID, pgoid.VOIDOID
	i4arr, i8arr := arrayOf(t, i4), arrayOf(t, i8)
	anyelem, anyarr, anyrange, anymulti := pgoid.ANYELEMENTOID, pgoid.ANYARRAYOID, pgoid.ANYRANGEOID, pgoid.ANYMULTIRANGEOID
	anycompat, anycompatArr := pgoid.ANYCOMPATIBLEOID, pgoid.ANYCOMPATIBLEARRAYOID
	anycompatRange, anycompatMulti := pgoid.ANYCOMPATIBLERANGEOID, pgoid.ANYCOMPATIBLEMULTIRANGEOID
	anynonarr, anyenum, anycompatNonarr := pgoid.ANYNONARRAYOID, pgoid.ANYENUMOID, pgoid.ANYCOMPATIBLENONARRAYOID

	enforce := func(actual, declared []Oid, rettype Oid) Oid {
		return enforceGenericTypeConsistency(actual, declared, rettype, false)
	}
	enforceErr := func(t *testing.T, actual, declared []Oid, rettype Oid) *mterrors.PgDiagnostic {
		t.Helper()
		return catch(t, func() { enforce(actual, declared, rettype) })
	}

	t.Run("family 1", func(t *testing.T) {
		assert.Equal(t, i4, enforce([]Oid{i4}, []Oid{i4}, i4), "nothing polymorphic")
		assert.Equal(t, i4arr, enforce([]Oid{i4}, []Oid{anyelem}, anyarr))
		assert.Equal(t, i4, enforce([]Oid{i4arr}, []Oid{anyarr}, anyelem))
		assert.Equal(t, i4, enforce([]Oid{i4arr, i4}, []Oid{anyarr, anyelem}, anyelem))
		assert.Equal(t, i4, enforce([]Oid{i4}, []Oid{anynonarr}, anyelem))
		assert.Equal(t, i4, enforce([]Oid{anyarr}, []Oid{anyarr}, i4), "anyarray input to a lone anyarray argument")

		declared := []Oid{anyelem, anyarr}
		assert.Equal(t, i4arr, enforce([]Oid{i4, unknown}, declared, anyarr))
		assert.Equal(t, i4arr, declared[1], "unknown anyarray takes the array over the element type")
		declared = []Oid{anyarr, anyelem}
		assert.Equal(t, i4, enforce([]Oid{i4arr, unknown}, declared, anyelem))
		assert.Equal(t, i4, declared[1])

		diag := enforceErr(t, []Oid{i4, i8}, []Oid{anyelem, anyelem}, anyelem)
		assert.Equal(t, mterrors.PgSSDatatypeMismatch, diag.Code)
		assert.Equal(t, `arguments declared "anyelement" are not all alike`, diag.Message)
		assert.Equal(t, "integer versus bigint", diag.Detail)
		diag = enforceErr(t, []Oid{i4arr, i8arr}, []Oid{anyarr, anyarr}, anyarr)
		assert.Equal(t, `arguments declared "anyarray" are not all alike`, diag.Message)
		diag = enforceErr(t, []Oid{i4, i8}, []Oid{anyrange, anyrange}, anyrange)
		assert.Equal(t, `arguments declared "anyrange" are not all alike`, diag.Message)
		diag = enforceErr(t, []Oid{i4, i8}, []Oid{anymulti, anymulti}, anymulti)
		assert.Equal(t, `arguments declared "anymultirange" are not all alike`, diag.Message)
		diag = enforceErr(t, []Oid{anyarr, i4}, []Oid{anyarr, anyelem}, i4)
		assert.Equal(t, `cannot determine element type of "anyarray" argument`, diag.Message)
		diag = enforceErr(t, []Oid{i4}, []Oid{anyarr}, i4)
		assert.Equal(t, "argument declared anyarray is not an array but type integer", diag.Message)
		diag = enforceErr(t, []Oid{i8arr, i4}, []Oid{anyarr, anyelem}, i4)
		assert.Equal(t, "argument declared anyarray is not consistent with argument declared anyelement", diag.Message)
		assert.Equal(t, "bigint[] versus integer", diag.Detail)
		diag = enforceErr(t, []Oid{i4}, []Oid{anyrange}, i4)
		assert.Equal(t, "argument declared anyrange is not a range type but type integer", diag.Message)
		diag = enforceErr(t, []Oid{i4}, []Oid{anymulti}, i4)
		assert.Equal(t, "argument declared anymultirange is not a multirange type but type integer", diag.Message)
		diag = enforceErr(t, []Oid{i4arr}, []Oid{anynonarr}, i4)
		assert.Equal(t, "type matched to anynonarray is an array type: integer[]", diag.Message)
		diag = enforceErr(t, []Oid{i4}, []Oid{anyenum}, i4)
		assert.Equal(t, "type matched to anyenum is not an enum type: integer", diag.Message)
		diag = enforceErr(t, []Oid{unknown}, []Oid{anyelem}, i4)
		assert.Equal(t, "could not determine polymorphic type because input has type unknown", diag.Message)
		diag = enforceErr(t, []Oid{anyelem, unknown}, []Oid{anyelem, anyarr}, i4)
		assert.Equal(t, mterrors.PgSSUndefinedObject, diag.Code)
		assert.Equal(t, "could not find array type for data type anyelement", diag.Message)
		diag = enforceErr(t, []Oid{unknown, i4}, []Oid{anyrange, anyelem}, i4)
		assert.Equal(t, "could not determine polymorphic type anyrange because input has type unknown", diag.Message)
		diag = enforceErr(t, []Oid{unknown, i4}, []Oid{anymulti, anyelem}, i4)
		assert.Equal(t, "could not determine polymorphic type anymultirange because input has type unknown", diag.Message)

		// Polymorphic result types.
		diag = enforceErr(t, []Oid{void}, []Oid{anyelem}, anyarr)
		assert.Equal(t, "could not find array type for data type void", diag.Message)
		diag = enforceErr(t, []Oid{i4}, []Oid{anyelem}, anyrange)
		assert.Equal(t, "could not determine polymorphic type anyrange because input has type unknown", diag.Message)
		diag = enforceErr(t, []Oid{i4}, []Oid{anyelem}, anymulti)
		assert.Equal(t, "could not determine polymorphic type anymultirange because input has type unknown", diag.Message)
	})

	t.Run("family 2", func(t *testing.T) {
		assert.Equal(t, i8, enforce([]Oid{i4, i8}, []Oid{anycompat, anycompat}, anycompat))
		assert.Equal(t, i8arr, enforce([]Oid{i4, i8}, []Oid{anycompat, anycompat}, anycompatArr))
		assert.Equal(t, i4, enforce([]Oid{i4}, []Oid{anycompatNonarr}, anycompat))
		assert.Equal(t, pgoid.TEXTOID, enforce([]Oid{unknown}, []Oid{anycompat}, anycompat), "all unknown resolves to text")
		assert.Equal(t, pgoid.TEXTARRAYOID, enforce([]Oid{unknown}, []Oid{anycompat}, anycompatArr))

		diag := enforceErr(t, []Oid{i4, pgoid.BOOLOID}, []Oid{anycompat, anycompat}, anycompat)
		assert.Equal(t, "argument types integer and boolean cannot be matched", diag.Message)
		diag = enforceErr(t, []Oid{pgoid.MONEYOID, i4}, []Oid{anycompat, anycompat}, anycompat)
		assert.Equal(t, "arguments of anycompatible family cannot be cast to a common type", diag.Message, "selected type fails verification")
		diag = enforceErr(t, []Oid{i4}, []Oid{anycompatArr}, anycompat)
		assert.Equal(t, "argument declared anycompatiblearray is not an array but type integer", diag.Message)
		diag = enforceErr(t, []Oid{i4}, []Oid{anycompatRange}, anycompat)
		assert.Equal(t, "argument declared anycompatiblerange is not a range type but type integer", diag.Message)
		diag = enforceErr(t, []Oid{i4}, []Oid{anycompatMulti}, anycompat)
		assert.Equal(t, "argument declared anycompatiblemultirange is not a multirange type but type integer", diag.Message)
		diag = enforceErr(t, []Oid{i4arr}, []Oid{anycompatNonarr}, anycompat)
		assert.Equal(t, "type matched to anycompatiblenonarray is an array type: integer[]", diag.Message)
		diag = enforceErr(t, []Oid{void}, []Oid{anycompat}, anycompatArr)
		assert.Equal(t, "could not find array type for data type void", diag.Message)
		diag = enforceErr(t, []Oid{i4}, []Oid{anycompat}, anycompatRange)
		assert.Equal(t, "could not determine polymorphic type anycompatiblerange because input has type unknown", diag.Message)
		diag = enforceErr(t, []Oid{i4}, []Oid{anycompat}, anycompatMulti)
		assert.Equal(t, "could not determine polymorphic type anycompatiblemultirange because input has type unknown", diag.Message)
		diag = enforceErr(t, []Oid{unknown}, []Oid{anycompat}, anycompatRange)
		assert.Equal(t, "could not determine polymorphic type anycompatiblerange because input has type unknown", diag.Message)
		diag = enforceErr(t, []Oid{unknown}, []Oid{anycompat}, anycompatMulti)
		assert.Equal(t, "could not determine polymorphic type anycompatiblemultirange because input has type unknown", diag.Message)
	})

	t.Run("allowPoly keeps the pseudotypes", func(t *testing.T) {
		for _, poly := range []Oid{anyelem, anyarr, anyrange, anymulti, anycompat, anycompatArr, anycompatRange, anycompatMulti} {
			assert.Equal(t, poly, enforceGenericTypeConsistency([]Oid{poly}, []Oid{poly}, poly, true), "%d", poly)
		}
		assert.Equal(t, anyarr, enforceGenericTypeConsistency([]Oid{anyelem}, []Oid{anyelem}, anyarr, true))
		assert.Equal(t, anycompatArr, enforceGenericTypeConsistency([]Oid{anycompat}, []Oid{anycompat}, anycompatArr, true))
	})

	t.Run("checkGenericTypeConsistency", func(t *testing.T) {
		check := checkGenericTypeConsistency
		assert.True(t, check([]Oid{i4, i4arr}, []Oid{anyelem, anyarr}))
		assert.True(t, check([]Oid{i4arr, i4}, []Oid{anyarr, anyelem}))
		assert.True(t, check([]Oid{anyarr}, []Oid{anyarr}))
		assert.True(t, check([]Oid{unknown, unknown, unknown, unknown}, []Oid{anyarr, anycompat, anycompatRange, anycompatMulti}))
		assert.False(t, check([]Oid{i4, i8}, []Oid{anyelem, anyelem}))
		assert.False(t, check([]Oid{i4arr, i8arr}, []Oid{anyarr, anyarr}))
		assert.False(t, check([]Oid{i4, i8}, []Oid{anyrange, anyrange}))
		assert.False(t, check([]Oid{i4, i8}, []Oid{anymulti, anymulti}))
		assert.False(t, check([]Oid{i4}, []Oid{anyarr}), "not an array")
		assert.False(t, check([]Oid{i8arr, i4}, []Oid{anyarr, anyelem}), "element mismatch")
		assert.False(t, check([]Oid{i4}, []Oid{anyrange}), "not a range")
		assert.False(t, check([]Oid{i4}, []Oid{anymulti}), "not a multirange")
		assert.False(t, check([]Oid{i4arr}, []Oid{anynonarr}))
		assert.False(t, check([]Oid{i4}, []Oid{anyenum}))
		assert.False(t, check([]Oid{i4}, []Oid{anycompatArr}))
		assert.False(t, check([]Oid{i4}, []Oid{anycompatRange}))
		assert.False(t, check([]Oid{i4}, []Oid{anycompatMulti}))
		assert.False(t, check([]Oid{i4, pgoid.BOOLOID}, []Oid{anycompat, anycompat}))
		assert.False(t, check([]Oid{pgoid.MONEYOID, i4}, []Oid{anycompat, anycompat}), "common type fails verification")
		assert.False(t, check([]Oid{i4arr}, []Oid{anycompatNonarr}))
		assert.True(t, check([]Oid{i4arr, i8}, []Oid{anycompatArr, anycompat}))
	})
}

func TestFuncSelectCandidateDirect(t *testing.T) {
	t.Parallel()
	i4, i8, unknown, text, bytea, b := pgoid.INT4OID, pgoid.INT8OID, pgoid.UNKNOWNOID, pgoid.TEXTOID, pgoid.BYTEAOID, pgoid.BOOLOID
	cand := func(args ...Oid) *funcCandidate { return &funcCandidate{oid: 1, args: args} }

	// The STRING category wins an unknown position even when a candidate of
	// another category is listed first.
	byteaFirst, textSecond := cand(bytea), cand(text)
	assert.Same(t, textSecond, funcSelectCandidate([]Oid{unknown}, []*funcCandidate{byteaFirst, textSecond}))
	// A category conflict without a string candidate cannot be resolved,
	// and with no known inputs the last-gasp rule does not apply.
	assert.Nil(t, funcSelectCandidate([]Oid{unknown}, []*funcCandidate{cand(i4), cand(b)}))
	// Known inputs of different types defeat the last-gasp rule.
	assert.Nil(t, funcSelectCandidate([]Oid{i4, i8, unknown}, []*funcCandidate{cand(i4, i8, i4), cand(i4, i8, i8)}))
	// Preferred types within a category are kept over non-preferred ones.
	preferred := cand(pgoid.FLOAT8OID)
	assert.Same(t, preferred, funcSelectCandidate([]Oid{unknown}, []*funcCandidate{cand(i4), preferred}))

	tooMany := make([]Oid, funcMaxArgs+1)
	diag := catch(t, func() { funcSelectCandidate(tooMany, nil) })
	assert.Equal(t, mterrors.PgSSTooManyArguments, diag.Code)
	assert.Equal(t, "cannot pass more than 100 arguments to a function", diag.Message)
}

func TestCatalogHelpers(t *testing.T) {
	t.Parallel()
	i4, i4arr := pgoid.INT4OID, arrayOf(t, pgoid.INT4OID)

	diag := catch(t, func() { typeidType(999999) })
	assert.Equal(t, "cache lookup failed for type 999999", diag.Message)
	assert.Equal(t, pgoid.InvalidOid, getArrayType(999999))
	assert.Equal(t, pgoid.InvalidOid, getElementType(999999))
	assert.True(t, typeIsArray(i4arr))
	assert.True(t, typeIsArrayDomain(i4arr))
	assert.False(t, typeIsArrayDomain(i4))
	assert.False(t, typeIsEnum(i4))
	assert.False(t, isComplexArray(i4arr))
	assert.False(t, isComplex(pgoid.RECORDOID), "record is a pseudotype, not a composite")
	assert.True(t, typeIsRowtype(pgoid.RECORDOID))
	assert.Equal(t, pgoid.InvalidOid, getRangeSubtype(i4))
	assert.Equal(t, pgoid.InvalidOid, getMultirangeRange(i4))
	assert.Equal(t, pgoid.InvalidOid, getRangeMultirange(i4))
	for _, rangeType := range []Oid{pgoid.INT4RANGEOID, pgoid.INT4MULTIRANGEOID} {
		diag = catch(t, func() { getRangeSubtype(rangeType) })
		assert.Equal(t, mterrors.PgSSFeatureNotSupported, diag.Code)
	}
	assert.False(t, typeInheritsFrom(i4, i4))
	assert.False(t, typeIsOfTypedTable(i4, i4))
	diag = catch(t, func() { getFuncRetset(999999) })
	assert.Equal(t, "cache lookup failed for function 999999", diag.Message)
	assert.Equal(t, i4, getTypeIOParam(pgcatalog.TypeByOid(i4arr)))
	assert.Equal(t, i4, getTypeIOParam(pgcatalog.TypeByOid(i4)))

	diag = catch(t, func() { typeCategory(999999) })
	assert.Equal(t, "cache lookup failed for type 999999", diag.Message)
	assert.True(t, isPreferredType(byte('N'), pgoid.FLOAT8OID))
	assert.False(t, isPreferredType(typcategoryString, pgoid.FLOAT8OID), "wrong category")

	path, _ := findCoercionPathway(i4, i4, ast.COERCION_IMPLICIT)
	assert.Equal(t, coercionPathRelabelType, path, "a type coerces to itself")
	path, _ = findCoercionPathway(pgoid.INT8OID, pgoid.BOOLOID, ast.COERCION_EXPLICIT)
	assert.Equal(t, coercionPathNone, path)
	path, _ = findCoercionPathway(pgoid.INT8OID, pgoid.BOOLOID, ast.COERCION_PLPGSQL)
	assert.Equal(t, coercionPathCoerceViaIO, path, "PL/pgSQL assignment falls back to I/O")
}

func TestNamesAndQuoting(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "a.*", nameListToString(ast.NewNodeList(ast.NewString("a"), ast.NewA_Star(-1))))
	for ident, want := range map[string]string{
		"abc": "abc", "a_1": "a_1", "Abc": `"Abc"`, "1a": `"1a"`, "a-b": `"a-b"`, "": `""`,
		"select": `"select"`, "char": `"char"`, "name": "name", "interval": `"interval"`,
	} {
		assert.Equal(t, want, quoteIdentifier(ident), ident)
	}
	tn := &ast.TypeName{TypeOid: pgoid.INT4OID}
	assert.Equal(t, "integer", typeNameToString(tn))
	tn = ast.NewTypeName([]string{"t"})
	tn.PctType = true
	tn.ArrayBounds = ast.NewNodeList(ast.NewInteger(-1))
	assert.Equal(t, "t%TYPE[]", typeNameToString(tn))
	diag := catch(t, func() { deconstructQualifiedName(ast.NewNodeList()) })
	assert.Equal(t, mterrors.PgSSSyntaxError, diag.Code)
	assert.Equal(t, "improper qualified name (too many dotted names): ", diag.Message)
}

func TestParamHooksDirect(t *testing.T) {
	t.Parallel()
	i4, i8 := pgoid.INT4OID, pgoid.INT8OID

	ps := NewParseState()
	ps.SetVariableParameters([]Oid{i4})
	// A parameter number the hook has not seen is an internal mismatch.
	stray := ast.NewParam(ast.PARAM_EXTERN, 5, pgoid.UNKNOWNOID)
	diag := catch(t, func() { ps.coerceParamHook(stray, i8, -1, -1) })
	assert.Equal(t, mterrors.PgSSUndefinedParameter, diag.Code)
	assert.Equal(t, "there is no parameter $5", diag.Message)
	err := ps.CheckVariableParameters(ast.NewParam(ast.PARAM_EXTERN, 5, i4))
	require.True(t, errors.As(err, &diag))
	assert.Equal(t, "there is no parameter $5", diag.Message)

	// A Param of unknown type takes the coercion's location when that is
	// to the left of its own. (paramrefHook records UNKNOWN for a parameter
	// it has handed out; the hook is only ever called after that.)
	ps.SetVariableParameters([]Oid{pgoid.UNKNOWNOID})
	p := ast.NewParam(ast.PARAM_EXTERN, 1, pgoid.UNKNOWNOID)
	p.SetLocation(10)
	got := ps.coerceParamHook(p, i8, -1, 4)
	assert.Same(t, p, got)
	assert.Equal(t, i8, p.Paramtype)
	assert.Equal(t, 4, p.Location())
	assert.Equal(t, []Oid{i8}, ps.ParamTypes())
	// The same parameter asked for the same type again is fine.
	q := ast.NewParam(ast.PARAM_EXTERN, 1, pgoid.UNKNOWNOID)
	assert.Same(t, q, ps.coerceParamHook(q, i8, -1, -1))

	// Nothing to deduce in fixed mode.
	fixed := NewParseState()
	fixed.SetFixedParameters([]Oid{i4})
	assert.Nil(t, fixed.coerceParamHook(ast.NewParam(ast.PARAM_EXTERN, 1, i4), i8, -1, -1))
}

func TestAnalysisInternalErrors(t *testing.T) {
	t.Parallel()
	i4, i4arr := pgoid.INT4OID, arrayOf(t, pgoid.INT4OID)
	ps := NewParseState()
	c := int4Const(1)

	diag := catch(t, func() { ps.makeOp(ast.NewNodeList(ast.NewString("+")), c, nil, -1) })
	assert.Equal(t, mterrors.PgSSSyntaxError, diag.Code)
	assert.Equal(t, "postfix operators are not supported", diag.Message)

	elements := ast.NewNodeList(ast.NewA_Const(ast.NewInteger(1), -1))
	diag = catch(t, func() { ps.transformArrayExpr(&ast.ArrayExpr{Elements: elements}, i4arr, pgoid.InvalidOid, -1) })
	assert.Equal(t, mterrors.PgSSInternalError, diag.Code)
	assert.Equal(t, "array type integer[] given without its element type", diag.Message)
	// An element type with no array type over it.
	voidParam := NewParseState()
	voidParam.SetFixedParameters([]Oid{pgoid.VOIDOID})
	_, err := voidParam.TransformExpr(&ast.ArrayExpr{Elements: ast.NewNodeList(ast.NewParamRef(1, -1))})
	require.True(t, errors.As(err, &diag))
	assert.Equal(t, mterrors.PgSSUndefinedObject, diag.Code)
	assert.Equal(t, "could not find array type for data type void", diag.Message)
	one := "1"
	diag = catch(t, func() { stringTypeDatum(pgcatalog.TypeByOid(pgoid.NUMERICOID), &one, -1) })
	assert.Equal(t, mterrors.PgSSFeatureNotSupported, diag.Code, "numeric_in is not ported")
	assert.Equal(t, int32(0), datum.DatumGetInt32(stringTypeDatum(pgcatalog.TypeByOid(i4), nil, -1)), "strict input of NULL")
	diag = catch(t, func() { tooManyArguments() })
	assert.Equal(t, mterrors.PgSSTooManyArguments, diag.Code)
	diag = catch(t, func() { notAllAlike("anyelement", i4, pgoid.INT8OID) })
	assert.Equal(t, "integer versus bigint", diag.Detail)
}
