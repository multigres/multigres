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

package analyze_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/parser/datum"
	"github.com/multigres/multigres/go/common/parser/pgoid"
	"github.com/multigres/multigres/go/common/pgcatalog"
	"github.com/multigres/multigres/go/common/pgeval/analyze"
	_ "github.com/multigres/multigres/go/common/pgeval/funcs"
)

// parseExpr returns the raw expression of "SELECT <sql>".
func parseExpr(t *testing.T, sql string) ast.Node {
	t.Helper()
	stmts, err := parser.ParseSQL("SELECT " + sql)
	require.NoError(t, err, sql)
	require.Len(t, stmts, 1)
	sel, ok := stmts[0].(*ast.SelectStmt)
	require.True(t, ok)
	require.Len(t, sel.TargetList.Items, 1)
	return sel.TargetList.Items[0].(*ast.ResTarget).Val
}

func analyzeExpr(t *testing.T, ps *analyze.ParseState, sql string) ast.Node {
	t.Helper()
	result, err := ps.TransformExpr(parseExpr(t, sql))
	require.NoError(t, err, sql)
	require.NotNil(t, result, sql)
	return result
}

func analyzeErr(t *testing.T, ps *analyze.ParseState, sql string) *mterrors.PgDiagnostic {
	t.Helper()
	_, err := ps.TransformExpr(parseExpr(t, sql))
	require.Error(t, err, sql)
	var diag *mterrors.PgDiagnostic
	require.True(t, errors.As(err, &diag), "%s: %v", sql, err)
	return diag
}

// opOID finds the builtin operator by name and operand types; 0 for a
// prefix operator's left type.
func opOID(t *testing.T, name string, left, right pgoid.Oid) pgoid.Oid {
	t.Helper()
	for _, op := range pgcatalog.OperatorsByName(name) {
		if op.Left == left && op.Right == right {
			return op.Oid
		}
	}
	t.Fatalf("no operator %s(%d, %d)", name, left, right)
	return 0
}

// procOID finds the builtin function by name and exact argument types.
func procOID(t *testing.T, name string, args ...pgoid.Oid) pgoid.Oid {
	t.Helper()
	for _, proc := range pgcatalog.ProcsByName(name) {
		if len(proc.ArgTypes) != len(args) {
			continue
		}
		match := true
		for i := range args {
			if proc.ArgTypes[i] != args[i] {
				match = false
			}
		}
		if match {
			return proc.Oid
		}
	}
	t.Fatalf("no function %s%v", name, args)
	return 0
}

func constInt32(t *testing.T, n ast.Node, typ pgoid.Oid, want int32, msg ...any) {
	t.Helper()
	c, ok := n.(*ast.Const)
	require.True(t, ok, "%T is not a Const", n)
	assert.Equal(t, typ, c.Consttype, msg...)
	assert.False(t, c.Constisnull, msg...)
	assert.Equal(t, want, datum.DatumGetInt32(c.Constvalue), msg...)
}

func TestConstants(t *testing.T) {
	t.Parallel()
	ps := analyze.NewParseState()

	constInt32(t, analyzeExpr(t, ps, "1"), pgoid.INT4OID, 1)
	constInt32(t, analyzeExpr(t, ps, "0x10"), pgoid.INT4OID, 16)

	// An integer literal beyond int4 arrives as a Float and becomes int8.
	big := analyzeExpr(t, ps, "2147483648").(*ast.Const)
	assert.Equal(t, pgoid.INT8OID, big.Consttype)
	assert.Equal(t, int64(2147483648), datum.DatumGetInt64(big.Constvalue))
	assert.True(t, big.Constbyval)
	assert.Equal(t, 8, big.Constlen)

	b := analyzeExpr(t, ps, "true").(*ast.Const)
	assert.Equal(t, pgoid.BOOLOID, b.Consttype)
	assert.True(t, datum.DatumGetBool(b.Constvalue))

	// A string literal stays unknown, as a cstring payload, until coerced.
	s := analyzeExpr(t, ps, "'hello'").(*ast.Const)
	assert.Equal(t, pgoid.UNKNOWNOID, s.Consttype)
	assert.Equal(t, -2, s.Constlen)
	assert.Equal(t, "hello", string(datum.DatumGetBytes(s.Constvalue)))

	null := analyzeExpr(t, ps, "NULL").(*ast.Const)
	assert.Equal(t, pgoid.UNKNOWNOID, null.Consttype)
	assert.True(t, null.Constisnull)

	// numeric_in and bit_in are not ported: the literal is recognized and
	// reported as unsupported rather than mistyped.
	for _, sql := range []string{"3.5", "99999999999999999999", "x'1F'", "b'101'"} {
		assert.Equal(t, mterrors.PgSSFeatureNotSupported, analyzeErr(t, ps, sql).Code, sql)
	}
}

func TestOperatorResolution(t *testing.T) {
	t.Parallel()
	ps := analyze.NewParseState()
	i2, i4, i8, f8, b := pgoid.INT2OID, pgoid.INT4OID, pgoid.INT8OID, pgoid.FLOAT8OID, pgoid.BOOLOID

	cases := []struct {
		sql         string
		name        string
		left, right pgoid.Oid
		result      pgoid.Oid
	}{
		{"1 + 2", "+", i4, i4, i4},
		{"1::int2 + 2", "+", i2, i4, i4},
		{"2 + 1::int2", "+", i4, i2, i4},
		{"1 + 2::int8", "+", i4, i8, i8},
		{"1::int8 * 2::int2", "*", i8, i2, i8},
		// An unknown literal takes the other operand's type (binary_oper_exact).
		{"'1' + 2", "+", i4, i4, i4},
		{"1::int8 < '2'", "<", i8, i8, b},
		// Mixed categories: int4 + float8 has no exact match; float8 is the
		// preferred numeric type, so (float8, float8) wins (func_select_candidate).
		{"1 + $1::float8", "+", f8, f8, f8},
		{"1 OPERATOR(pg_catalog.+) 2", "+", i4, i4, i4},
		{"true AND false OR 1 < 2", "<", i4, i4, b}, // the only OpExpr in the tree
		{"1 = 1 AND 2 <> 3", "=", i4, i4, b},        // the first OpExpr visited
		{"(1 + 2) * 3", "*", i4, i4, i4},            // the outermost OpExpr
	}
	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			ps := analyze.NewParseState()
			ps.SetVariableParameters(nil)
			tree := analyzeExpr(t, ps, tc.sql)
			var ops []*ast.OpExpr
			analyze.Walk(tree, func(n ast.Node) bool {
				if op, ok := n.(*ast.OpExpr); ok {
					ops = append(ops, op)
				}
				return true
			})
			require.NotEmpty(t, ops)
			op := ops[0]
			want := pgcatalog.OperatorByOid(opOID(t, tc.name, tc.left, tc.right))
			assert.Equal(t, want.Oid, op.Opno, "operator OID")
			assert.Equal(t, want.Code, op.Opfuncid, "operator function")
			assert.Equal(t, tc.result, op.Opresulttype, "result type")
			assert.Equal(t, tc.left, analyze.ExprType(op.Args.Items[0]), "left argument type after coercion")
			assert.Equal(t, tc.right, analyze.ExprType(op.Args.Items[1]), "right argument type after coercion")
			assert.False(t, op.Opretset)
		})
	}

	t.Run("unknown literal becomes a typed Const", func(t *testing.T) {
		op := analyzeExpr(t, ps, "'7' + 2").(*ast.OpExpr)
		constInt32(t, op.Args.Items[0], i4, 7)
	})

	t.Run("implicit cast is a FuncExpr", func(t *testing.T) {
		op := analyzeExpr(t, ps, "1 + 2::int8").(*ast.OpExpr)
		constInt32(t, op.Args.Items[0], i4, 1)
		cast, ok := op.Args.Items[1].(*ast.FuncExpr)
		require.True(t, ok, "%T", op.Args.Items[1])
		assert.Equal(t, procOID(t, "int8", i4), cast.Funcid)
		assert.Equal(t, ast.COERCE_EXPLICIT_CAST, cast.Funcformat)
		constInt32(t, cast.Args.Items[0], i4, 2)
	})

	t.Run("prefix operator", func(t *testing.T) {
		op := analyzeExpr(t, ps, "-(1)").(*ast.OpExpr)
		assert.Equal(t, opOID(t, "-", 0, i4), op.Opno)
		assert.Len(t, op.Args.Items, 1)
		assert.Equal(t, i4, op.Opresulttype)
	})

	t.Run("errors", func(t *testing.T) {
		for _, tc := range []struct{ sql, code, message, hint string }{
			{
				"true = 1", mterrors.PgSSUndefinedFunction,
				"operator does not exist: boolean = integer",
				"No operator matches the given name and argument types. You might need to add explicit type casts.",
			},
			{
				"-true", mterrors.PgSSUndefinedFunction,
				"operator does not exist: - boolean",
				"No operator matches the given name and argument type. You might need to add an explicit type cast.",
			},
			{
				"'1' + '2'", mterrors.PgSSAmbiguousFunction,
				"operator is not unique: unknown + unknown",
				"Could not choose a best candidate operator. You might need to add explicit type casts.",
			},
			{
				"-'1'", mterrors.PgSSAmbiguousFunction,
				"operator is not unique: - unknown",
				"Could not choose a best candidate operator. You might need to add explicit type casts.",
			},
			{"1 OPERATOR(nosuch.+) 2", mterrors.PgSSFeatureNotSupported, "", ""},
		} {
			diag := analyzeErr(t, ps, tc.sql)
			assert.Equal(t, tc.code, diag.Code, tc.sql)
			if tc.message != "" {
				assert.Equal(t, tc.message, diag.Message, tc.sql)
				assert.Equal(t, tc.hint, diag.Hint, tc.sql)
			}
		}
	})
}

func TestCasts(t *testing.T) {
	t.Parallel()
	ps := analyze.NewParseState()
	i2, i4, i8 := pgoid.INT2OID, pgoid.INT4OID, pgoid.INT8OID

	t.Run("same type is a no-op", func(t *testing.T) {
		raw := parseExpr(t, "1::int4")
		tree, err := ps.TransformExpr(raw)
		require.NoError(t, err)
		constInt32(t, tree, i4, 1)
	})

	t.Run("cast function", func(t *testing.T) {
		for _, tc := range []struct {
			sql    string
			fn     string
			arg    pgoid.Oid
			result pgoid.Oid
		}{
			{"1::int8", "int8", i4, i8},
			{"CAST(1 AS smallint)", "int2", i4, i2},
			{"1::bigint::int2", "int2", i8, i2},
			{"true::int4", "int4", pgoid.BOOLOID, i4},
			{"1::bool", "bool", i4, pgoid.BOOLOID},
		} {
			f, ok := analyzeExpr(t, ps, tc.sql).(*ast.FuncExpr)
			require.True(t, ok, tc.sql)
			assert.Equal(t, procOID(t, tc.fn, tc.arg), f.Funcid, tc.sql)
			assert.Equal(t, tc.result, f.Funcresulttype, tc.sql)
			assert.Equal(t, ast.COERCE_EXPLICIT_CAST, f.Funcformat, tc.sql)
			assert.Equal(t, tc.arg, analyze.ExprType(f.Args.Items[0]), tc.sql)
		}
	})

	t.Run("binary-compatible cast is a RelabelType", func(t *testing.T) {
		r, ok := analyzeExpr(t, ps, "1::oid").(*ast.RelabelType)
		require.True(t, ok)
		assert.Equal(t, pgoid.OIDOID, r.Resulttype)
		assert.Equal(t, ast.COERCE_EXPLICIT_CAST, r.Relabelformat)
		constInt32(t, r.Arg, i4, 1)
	})

	t.Run("I/O cast to a string type", func(t *testing.T) {
		c, ok := analyzeExpr(t, ps, "1::text").(*ast.CoerceViaIO)
		require.True(t, ok)
		assert.Equal(t, pgoid.TEXTOID, c.Resulttype)
		constInt32(t, c.Arg, i4, 1)
	})

	t.Run("unknown literal goes through the input function", func(t *testing.T) {
		constInt32(t, analyzeExpr(t, ps, "'5'::int4"), i4, 5)
		constInt32(t, analyzeExpr(t, ps, "int4 '  0x1f '"), i4, 31)
		c := analyzeExpr(t, ps, "'yes'::bool").(*ast.Const)
		assert.Equal(t, pgoid.BOOLOID, c.Consttype)
		assert.True(t, datum.DatumGetBool(c.Constvalue))
		null := analyzeExpr(t, ps, "NULL::int8").(*ast.Const)
		assert.Equal(t, i8, null.Consttype)
		assert.True(t, null.Constisnull)

		diag := analyzeErr(t, ps, "'abc'::int4")
		assert.Equal(t, mterrors.PgSSInvalidTextRepresentation, diag.Code)
		assert.Equal(t, `invalid input syntax for type integer: "abc"`, diag.Message)
		diag = analyzeErr(t, ps, "'99999999999'::int4")
		assert.Equal(t, mterrors.PgSSNumericValueOutOfRange, diag.Code)
		assert.Equal(t, `value "99999999999" is out of range for type integer`, diag.Message)
	})

	t.Run("errors", func(t *testing.T) {
		for _, tc := range []struct{ sql, code, message string }{
			{"1::nosuchtype", mterrors.PgSSUndefinedObject, `type "nosuchtype" does not exist`},
			{"1::pg_catalog.nosuchtype", mterrors.PgSSUndefinedObject, `type "pg_catalog.nosuchtype" does not exist`},
			{"1::int4[]", mterrors.PgSSCannotCoerce, "cannot cast type integer to integer[]"},
			{"true::int8", mterrors.PgSSCannotCoerce, "cannot cast type boolean to bigint"},
			{"1::numeric(10,2)", mterrors.PgSSFeatureNotSupported, ""},
			{"1::public.mytype", mterrors.PgSSFeatureNotSupported, ""},
		} {
			diag := analyzeErr(t, ps, tc.sql)
			assert.Equal(t, tc.code, diag.Code, tc.sql)
			if tc.message != "" {
				assert.Equal(t, tc.message, diag.Message, tc.sql)
			}
		}
	})
}

func TestFunctionResolution(t *testing.T) {
	t.Parallel()
	ps := analyze.NewParseState()
	i2, i4, i8 := pgoid.INT2OID, pgoid.INT4OID, pgoid.INT8OID

	t.Run("exact and coerced matches", func(t *testing.T) {
		for _, tc := range []struct {
			sql    string
			fn     string
			args   []pgoid.Oid
			result pgoid.Oid
		}{
			{"abs(1)", "abs", []pgoid.Oid{i4}, i4},
			{"pg_catalog.abs(1::int2)", "abs", []pgoid.Oid{i2}, i2},
			{"abs(1::int8)", "abs", []pgoid.Oid{i8}, i8},
			{"int4(1::int8)", "int4", []pgoid.Oid{i8}, i4},
			{"int8(1)", "int8", []pgoid.Oid{i4}, i8},
			{"gcd(1, 2::int8)", "gcd", []pgoid.Oid{i8, i8}, i8},
			{"int4smaller(1, 2::int2)", "int4smaller", []pgoid.Oid{i4, i4}, i4},
		} {
			f, ok := analyzeExpr(t, ps, tc.sql).(*ast.FuncExpr)
			require.True(t, ok, tc.sql)
			assert.Equal(t, procOID(t, tc.fn, tc.args...), f.Funcid, tc.sql)
			assert.Equal(t, tc.result, f.Funcresulttype, tc.sql)
			assert.Equal(t, ast.COERCE_EXPLICIT_CALL, f.Funcformat, tc.sql)
			require.Len(t, f.Args.Items, len(tc.args), tc.sql)
			for i, arg := range f.Args.Items {
				assert.Equal(t, tc.args[i], analyze.ExprType(arg), "%s argument %d", tc.sql, i)
			}
		}
	})

	t.Run("typename(x) is a coercion", func(t *testing.T) {
		constInt32(t, analyzeExpr(t, ps, "int4('5')"), i4, 5)
		c, ok := analyzeExpr(t, ps, "text(1)").(*ast.CoerceViaIO)
		require.True(t, ok)
		assert.Equal(t, pgoid.TEXTOID, c.Resulttype)
		assert.Equal(t, ast.COERCE_EXPLICIT_CALL, c.Coerceformat)
		r, ok := analyzeExpr(t, ps, "oid(1)").(*ast.RelabelType)
		require.True(t, ok)
		assert.Equal(t, ast.COERCE_EXPLICIT_CALL, r.Relabelformat)
	})

	t.Run("variadic any passes arguments as they are", func(t *testing.T) {
		f, ok := analyzeExpr(t, ps, "concat(1, true, 2::int8)").(*ast.FuncExpr)
		require.True(t, ok)
		assert.Equal(t, procOID(t, "concat", pgoid.ANYOID), f.Funcid)
		assert.Equal(t, pgoid.TEXTOID, f.Funcresulttype)
		assert.False(t, f.Funcvariadic)
		require.Len(t, f.Args.Items, 3)
		constInt32(t, f.Args.Items[0], i4, 1)
	})

	t.Run("variadic array parameter is gathered into an ArrayExpr", func(t *testing.T) {
		ps := analyze.NewParseState()
		ps.SetFixedParameters([]pgoid.Oid{pgoid.JSONOID, pgoid.TEXTOID, pgoid.TEXTOID})
		f, ok := analyzeExpr(t, ps, "json_extract_path($1, $2, $3)").(*ast.FuncExpr)
		require.True(t, ok)
		assert.Equal(t, procOID(t, "json_extract_path", pgoid.JSONOID, pgoid.TEXTARRAYOID), f.Funcid)
		assert.True(t, f.Funcvariadic)
		require.Len(t, f.Args.Items, 2)
		arr, ok := f.Args.Items[1].(*ast.ArrayExpr)
		require.True(t, ok)
		assert.Equal(t, pgoid.TEXTARRAYOID, arr.ArrayTypeid)
		assert.Equal(t, pgoid.TEXTOID, arr.ElementTypeid)
		assert.Len(t, arr.Elements.Items, 2)
	})

	t.Run("errors", func(t *testing.T) {
		notFoundHint := "No function matches the given name and argument types. You might need to add explicit type casts."
		for _, tc := range []struct{ sql, code, message, hint string }{
			{"abs()", mterrors.PgSSUndefinedFunction, "function abs() does not exist", notFoundHint},
			{"abs(1, 2)", mterrors.PgSSUndefinedFunction, "function abs(integer, integer) does not exist", notFoundHint},
			{"nosuch(1)", mterrors.PgSSUndefinedFunction, "function nosuch(integer) does not exist", notFoundHint},
			{"pg_catalog.nosuch('a')", mterrors.PgSSUndefinedFunction, "function pg_catalog.nosuch(unknown) does not exist", notFoundHint},
			{"abs(true)", mterrors.PgSSUndefinedFunction, "function abs(boolean) does not exist", notFoundHint},
			{"abs(*)", mterrors.PgSSUndefinedFunction, "function abs() does not exist", notFoundHint},
			{"pi(*)", mterrors.PgSSWrongObjectType, "pi(*) specified, but pi is not an aggregate function", ""},
			{"abs(DISTINCT 1)", mterrors.PgSSWrongObjectType, "DISTINCT specified, but abs is not an aggregate function", ""},
			{"abs(1 ORDER BY 1)", mterrors.PgSSWrongObjectType, "ORDER BY specified, but abs is not an aggregate function", ""},
			{"abs(1) FILTER (WHERE true)", mterrors.PgSSWrongObjectType, "FILTER specified, but abs is not an aggregate function", ""},
			{"abs(1) OVER ()", mterrors.PgSSWrongObjectType, "OVER specified, but abs is not a window function nor an aggregate function", ""},
			{"abs(x => 1)", mterrors.PgSSFeatureNotSupported, "", ""},
			{"abs(x => 1, x => 2)", mterrors.PgSSSyntaxError, `argument name "x" used more than once`, ""},
			{"count(*)", mterrors.PgSSFeatureNotSupported, "", ""},
			{"generate_series(1, 2)", mterrors.PgSSFeatureNotSupported, "", ""},
			{"public.abs(1)", mterrors.PgSSFeatureNotSupported, "", ""},
		} {
			diag := analyzeErr(t, ps, tc.sql)
			assert.Equal(t, tc.code, diag.Code, tc.sql)
			if tc.message != "" {
				assert.Equal(t, tc.message, diag.Message, tc.sql)
				assert.Equal(t, tc.hint, diag.Hint, tc.sql)
			}
		}
	})
}

func TestParameters(t *testing.T) {
	t.Parallel()
	i4, i8 := pgoid.INT4OID, pgoid.INT8OID

	t.Run("no parameters declared", func(t *testing.T) {
		diag := analyzeErr(t, analyze.NewParseState(), "$1")
		assert.Equal(t, mterrors.PgSSUndefinedParameter, diag.Code)
		assert.Equal(t, "there is no parameter $1", diag.Message)
	})

	t.Run("fixed", func(t *testing.T) {
		ps := analyze.NewParseState()
		ps.SetFixedParameters([]pgoid.Oid{i4, i8})
		op := analyzeExpr(t, ps, "$1 + $2").(*ast.OpExpr)
		assert.Equal(t, opOID(t, "+", i4, i8), op.Opno)
		param := op.Args.Items[0].(*ast.Param)
		assert.Equal(t, ast.PARAM_EXTERN, param.Paramkind)
		assert.Equal(t, 1, param.Paramid)
		assert.Equal(t, i4, param.Paramtype)
		assert.Equal(t, int32(-1), param.Paramtypmod)

		diag := analyzeErr(t, ps, "$3")
		assert.Equal(t, mterrors.PgSSUndefinedParameter, diag.Code)
		assert.Equal(t, "there is no parameter $3", diag.Message)
		assert.Equal(t, []pgoid.Oid{i4, i8}, ps.ParamTypes())
	})

	t.Run("variable: deduced from an operator", func(t *testing.T) {
		ps := analyze.NewParseState()
		ps.SetVariableParameters([]pgoid.Oid{0})
		op := analyzeExpr(t, ps, "$1 + 1").(*ast.OpExpr)
		assert.Equal(t, opOID(t, "+", i4, i4), op.Opno)
		// The Param itself is retyped; no cast node is added.
		param, ok := op.Args.Items[0].(*ast.Param)
		require.True(t, ok, "%T", op.Args.Items[0])
		assert.Equal(t, i4, param.Paramtype)
		assert.Equal(t, []pgoid.Oid{i4}, ps.ParamTypes())
		require.NoError(t, ps.CheckVariableParameters(op))
	})

	t.Run("variable: deduced from a cast and reused", func(t *testing.T) {
		ps := analyze.NewParseState()
		ps.SetVariableParameters(nil)
		op := analyzeExpr(t, ps, "$2::int8 + $2").(*ast.OpExpr)
		assert.Equal(t, opOID(t, "+", i8, i8), op.Opno)
		assert.Equal(t, []pgoid.Oid{0, i8}, ps.ParamTypes())
	})

	t.Run("variable: declared type is kept", func(t *testing.T) {
		ps := analyze.NewParseState()
		ps.SetVariableParameters([]pgoid.Oid{i8})
		op := analyzeExpr(t, ps, "$1 + 1").(*ast.OpExpr)
		assert.Equal(t, opOID(t, "+", i8, i4), op.Opno)
	})

	t.Run("variable: unresolved", func(t *testing.T) {
		ps := analyze.NewParseState()
		ps.SetVariableParameters(nil)
		// A bare parameter stays unknown; whether that is an error is the
		// caller's decision (statement-level analysis resolves it to text).
		tree := analyzeExpr(t, ps, "$1")
		assert.Equal(t, pgoid.UNKNOWNOID, analyze.ExprType(tree))
		assert.Equal(t, []pgoid.Oid{pgoid.UNKNOWNOID}, ps.ParamTypes())
		require.NoError(t, ps.CheckVariableParameters(tree))

		diag := analyzeErr(t, ps, "$1 + $2")
		assert.Equal(t, mterrors.PgSSAmbiguousFunction, diag.Code)
		assert.Equal(t, "operator is not unique: unknown + unknown", diag.Message)
	})

	t.Run("variable: a Param left behind by a later deduction", func(t *testing.T) {
		ps := analyze.NewParseState()
		ps.SetVariableParameters(nil)
		tree := analyzeExpr(t, ps, "$1 IS NULL AND $1 = 1")
		assert.Equal(t, []pgoid.Oid{i4}, ps.ParamTypes())
		err := ps.CheckVariableParameters(tree)
		var diag *mterrors.PgDiagnostic
		require.True(t, errors.As(err, &diag))
		assert.Equal(t, mterrors.PgSSAmbiguousParameter, diag.Code)
		assert.Equal(t, "could not determine data type of parameter $1", diag.Message)

		// The other order deduces first, so the second Param is born int4.
		tree = analyzeExpr(t, ps, "$1 = 1 AND $1 IS NULL")
		require.NoError(t, ps.CheckVariableParameters(tree))
	})
}

func TestBooleanConstructs(t *testing.T) {
	t.Parallel()
	ps := analyze.NewParseState()
	i4 := pgoid.INT4OID

	t.Run("AND/OR/NOT", func(t *testing.T) {
		b := analyzeExpr(t, ps, "NOT (1 < 2)").(*ast.BoolExpr)
		assert.Equal(t, ast.NOT_EXPR, b.Boolop)
		require.Len(t, b.Args.Items, 1)
		_, ok := b.Args.Items[0].(*ast.OpExpr)
		assert.True(t, ok)
		assert.Equal(t, pgoid.BOOLOID, analyze.ExprType(b))

		diag := analyzeErr(t, ps, "NOT 1")
		assert.Equal(t, mterrors.PgSSDatatypeMismatch, diag.Code)
		assert.Equal(t, "argument of NOT must be type boolean, not type integer", diag.Message)
		diag = analyzeErr(t, ps, "true AND 1::int8")
		assert.Equal(t, "argument of AND must be type boolean, not type bigint", diag.Message)
		// An unknown literal is coerced to boolean through boolin.
		b = analyzeExpr(t, ps, "'t' OR false").(*ast.BoolExpr)
		assert.Equal(t, pgoid.BOOLOID, b.Args.Items[0].(*ast.Const).Consttype)
	})

	t.Run("IS NULL and IS TRUE", func(t *testing.T) {
		nt := analyzeExpr(t, ps, "1 IS NULL").(*ast.NullTest)
		assert.Equal(t, ast.IS_NULL, nt.Nulltesttype)
		assert.False(t, nt.Argisrow)
		constInt32(t, nt.Arg, i4, 1)

		bt := analyzeExpr(t, ps, "true IS NOT TRUE").(*ast.BooleanTest)
		assert.Equal(t, ast.IS_NOT_TRUE, bt.Booltesttype)
		diag := analyzeErr(t, ps, "1 IS UNKNOWN")
		assert.Equal(t, "argument of IS UNKNOWN must be type boolean, not type integer", diag.Message)
	})

	t.Run("IS DISTINCT FROM", func(t *testing.T) {
		d := analyzeExpr(t, ps, "1 IS DISTINCT FROM 2::int8").(*ast.DistinctExpr)
		assert.Equal(t, opOID(t, "=", i4, pgoid.INT8OID), d.Opno)
		assert.Equal(t, pgoid.BOOLOID, d.Opresulttype)
		assert.Equal(t, pgoid.BOOLOID, analyze.ExprType(d))

		not := analyzeExpr(t, ps, "1 IS NOT DISTINCT FROM 2").(*ast.BoolExpr)
		assert.Equal(t, ast.NOT_EXPR, not.Boolop)
		_, ok := not.Args.Items[0].(*ast.DistinctExpr)
		assert.True(t, ok)

		nt := analyzeExpr(t, ps, "1 IS DISTINCT FROM NULL").(*ast.NullTest)
		assert.Equal(t, ast.IS_NOT_NULL, nt.Nulltesttype)
		nt = analyzeExpr(t, ps, "(NULL) IS NOT DISTINCT FROM 1").(*ast.NullTest)
		assert.Equal(t, ast.IS_NULL, nt.Nulltesttype)
	})

	t.Run("NULLIF", func(t *testing.T) {
		n := analyzeExpr(t, ps, "NULLIF(1, 2::int8)").(*ast.NullIfExpr)
		assert.Equal(t, opOID(t, "=", i4, pgoid.INT8OID), n.Opno)
		assert.Equal(t, i4, n.Opresulttype, "NULLIF yields the first operand's type")
		assert.Equal(t, i4, analyze.ExprType(n))
	})

	t.Run("BETWEEN", func(t *testing.T) {
		b := analyzeExpr(t, ps, "1 BETWEEN 0 AND 2").(*ast.BoolExpr)
		assert.Equal(t, ast.AND_EXPR, b.Boolop)
		require.Len(t, b.Args.Items, 2)
		assert.Equal(t, opOID(t, ">=", i4, i4), b.Args.Items[0].(*ast.OpExpr).Opno)
		assert.Equal(t, opOID(t, "<=", i4, i4), b.Args.Items[1].(*ast.OpExpr).Opno)

		b = analyzeExpr(t, ps, "1 NOT BETWEEN SYMMETRIC 0 AND 2::int8").(*ast.BoolExpr)
		assert.Equal(t, ast.AND_EXPR, b.Boolop)
		require.Len(t, b.Args.Items, 2)
		sub := b.Args.Items[1].(*ast.BoolExpr)
		assert.Equal(t, ast.OR_EXPR, sub.Boolop)
		assert.Equal(t, opOID(t, "<", i4, pgoid.INT8OID), sub.Args.Items[0].(*ast.OpExpr).Opno)
	})

	t.Run("IN", func(t *testing.T) {
		// The common type of the whole list is int8, so the array is int8[];
		// the left side keeps its own type and the operator is int4 = int8.
		sa := analyzeExpr(t, ps, "1 IN (1, 2::int8)").(*ast.ScalarArrayOpExpr)
		assert.True(t, sa.UseOr)
		assert.Equal(t, opOID(t, "=", i4, pgoid.INT8OID), sa.Opno)
		constInt32(t, sa.Args.Items[0], i4, 1)
		arr := sa.Args.Items[1].(*ast.ArrayExpr)
		assert.Equal(t, pgoid.INT8OID, arr.ElementTypeid)
		assert.Equal(t, getArrayType(t, pgoid.INT8OID), arr.ArrayTypeid)
		assert.Equal(t, pgoid.BOOLOID, analyze.ExprType(sa))

		sa = analyzeExpr(t, ps, "1 NOT IN ('1', 2)").(*ast.ScalarArrayOpExpr)
		assert.False(t, sa.UseOr)
		assert.Equal(t, opOID(t, "<>", i4, i4), sa.Opno)

		// A single item is a plain comparison.
		op := analyzeExpr(t, ps, "1 IN (2)").(*ast.OpExpr)
		assert.Equal(t, opOID(t, "=", i4, i4), op.Opno)

		params := analyze.NewParseState()
		params.SetFixedParameters([]pgoid.Oid{pgoid.INT4ARRAYOID})
		sa = analyzeExpr(t, params, "1 = ANY ($1)").(*ast.ScalarArrayOpExpr)
		assert.Equal(t, opOID(t, "=", i4, i4), sa.Opno)
		assert.Equal(t, pgoid.INT4ARRAYOID, analyze.ExprType(sa.Args.Items[1]))
		sa = analyzeExpr(t, params, "1::int8 <> ALL ($1)").(*ast.ScalarArrayOpExpr)
		assert.False(t, sa.UseOr)
		assert.Equal(t, opOID(t, "<>", pgoid.INT8OID, i4), sa.Opno)
	})
}

func getArrayType(t *testing.T, elem pgoid.Oid) pgoid.Oid {
	t.Helper()
	typ := pgcatalog.TypeByOid(elem)
	require.NotNil(t, typ)
	return typ.Array
}

func TestCommonTypeConstructs(t *testing.T) {
	t.Parallel()
	ps := analyze.NewParseState()
	i4, i8 := pgoid.INT4OID, pgoid.INT8OID

	t.Run("CASE", func(t *testing.T) {
		c := analyzeExpr(t, ps, "CASE WHEN true THEN 1 ELSE 2::int8 END").(*ast.CaseExpr)
		assert.Equal(t, i8, c.Casetype)
		assert.Nil(t, c.Arg)
		when := c.Args.Items[0].(*ast.CaseWhen)
		assert.Equal(t, pgoid.BOOLOID, analyze.ExprType(when.Expr))
		assert.Equal(t, i8, analyze.ExprType(when.Result), "WHEN result coerced to the common type")
		constInt32(t, when.Result.(*ast.FuncExpr).Args.Items[0], i4, 1)

		// Simple CASE compares a placeholder for the test value.
		c = analyzeExpr(t, ps, "CASE 1 WHEN '1' THEN true END").(*ast.CaseExpr)
		assert.Equal(t, pgoid.BOOLOID, c.Casetype)
		constInt32(t, c.Arg, i4, 1)
		cmp := c.Args.Items[0].(*ast.CaseWhen).Expr.(*ast.OpExpr)
		assert.Equal(t, opOID(t, "=", i4, i4), cmp.Opno)
		placeholder, ok := cmp.Args.Items[0].(*ast.CaseTestExpr)
		require.True(t, ok)
		assert.Equal(t, i4, placeholder.TypeId)
		constInt32(t, cmp.Args.Items[1], i4, 1)
		def := c.Defresult.(*ast.Const)
		assert.Equal(t, pgoid.BOOLOID, def.Consttype, "implicit ELSE NULL takes the common type")
		assert.True(t, def.Constisnull)

		diag := analyzeErr(t, ps, "CASE WHEN 1 THEN 1 END")
		assert.Equal(t, mterrors.PgSSDatatypeMismatch, diag.Code)
		assert.Equal(t, "argument of CASE/WHEN must be type boolean, not type integer", diag.Message)
		diag = analyzeErr(t, ps, "CASE WHEN true THEN 1 ELSE false END")
		assert.Equal(t, "CASE types boolean and integer cannot be matched", diag.Message)
	})

	t.Run("COALESCE and GREATEST", func(t *testing.T) {
		c := analyzeExpr(t, ps, "COALESCE(1, '2')").(*ast.CoalesceExpr)
		assert.Equal(t, i4, c.Coalescetype)
		constInt32(t, c.Args.Items[1], i4, 2)

		m := analyzeExpr(t, ps, "GREATEST(1, 2::int8, 3::int2)").(*ast.MinMaxExpr)
		assert.Equal(t, ast.IS_GREATEST, m.Op)
		assert.Equal(t, i8, m.MinMaxType)
		for _, arg := range m.Args.Items {
			assert.Equal(t, i8, analyze.ExprType(arg))
		}

		diag := analyzeErr(t, ps, "COALESCE(1, true)")
		assert.Equal(t, mterrors.PgSSDatatypeMismatch, diag.Code)
		assert.Equal(t, "COALESCE types integer and boolean cannot be matched", diag.Message)
		diag = analyzeErr(t, ps, "LEAST('a', 1)")
		assert.Equal(t, mterrors.PgSSInvalidTextRepresentation, diag.Code)
		assert.Equal(t, `invalid input syntax for type integer: "a"`, diag.Message)
	})

	t.Run("ARRAY", func(t *testing.T) {
		a := analyzeExpr(t, ps, "ARRAY[1, 2::int8]").(*ast.ArrayExpr)
		assert.Equal(t, i8, a.ElementTypeid)
		assert.Equal(t, getArrayType(t, i8), a.ArrayTypeid)
		assert.False(t, a.Multidims)
		assert.Equal(t, i8, analyze.ExprType(a.Elements.Items[0]))

		a = analyzeExpr(t, ps, "ARRAY[[1, 2], [3, 4]]").(*ast.ArrayExpr)
		assert.True(t, a.Multidims)
		assert.Equal(t, getArrayType(t, i4), a.ArrayTypeid)
		assert.Equal(t, i4, a.ElementTypeid)

		a = analyzeExpr(t, ps, "ARRAY[]::int4[]").(*ast.ArrayExpr)
		assert.Equal(t, getArrayType(t, i4), a.ArrayTypeid)
		assert.Empty(t, a.Elements.Items)

		a = analyzeExpr(t, ps, "ARRAY[1, '2']::int8[]").(*ast.ArrayExpr)
		assert.Equal(t, i8, a.ElementTypeid)

		diag := analyzeErr(t, ps, "ARRAY[]")
		assert.Equal(t, mterrors.PgSSIndeterminateDatatype, diag.Code)
		assert.Equal(t, "cannot determine type of empty array", diag.Message)
		assert.Equal(t, "Explicitly cast to the desired type, for example ARRAY[]::integer[].", diag.Hint)
		diag = analyzeErr(t, ps, "ARRAY[1, true]")
		assert.Equal(t, "ARRAY types integer and boolean cannot be matched", diag.Message)
	})
}

func TestUnsupportedConstructs(t *testing.T) {
	t.Parallel()
	ps := analyze.NewParseState()
	for _, sql := range []string{
		"a + 1", "(SELECT 1)", "ROW(1, 2)", "'a' COLLATE \"C\"", "current_date",
		"1 OPERATOR(public.+) 2", "(1).f", "ARRAY[1]::public.t[]",
	} {
		diag := analyzeErr(t, ps, sql)
		assert.Equal(t, mterrors.PgSSFeatureNotSupported, diag.Code, sql)
		assert.Contains(t, diag.Message, "not supported by the gateway evaluation engine", sql)
	}
}

// TestAnalyzedTreeIsWellFormed checks the tree-level invariants the
// evaluator will rely on: every node has a type, no coercion is left
// pending, and the tree survives the generated clone.
func TestAnalyzedTreeIsWellFormed(t *testing.T) {
	t.Parallel()
	for _, sql := range []string{
		"1 + 2::int8 * 3::int2 - '4'", "abs(-1) > 0 AND NOT (1 IS NULL)",
		"CASE 1::int8 WHEN 1 THEN COALESCE(2, 3::int2) ELSE GREATEST(4, 5) END",
		"1 IN (1, 2, 3) OR 2 BETWEEN 1 AND 3", "NULLIF(1, 2) IS DISTINCT FROM 3", "ARRAY[[1], [2]]",
		"concat(1, 2)", "1::oid::int4", "int8(1)::text",
	} {
		t.Run(sql, func(t *testing.T) {
			ps := analyze.NewParseState()
			tree := analyzeExpr(t, ps, sql)
			visited := 0
			analyze.Walk(tree, func(n ast.Node) bool {
				visited++
				typ := analyze.ExprType(n)
				assert.NotEqual(t, pgoid.InvalidOid, typ, "%s has no type", n)
				assert.NotEqual(t, pgoid.UNKNOWNOID, typ, "%s left unknown", n)
				assert.NotNil(t, pgcatalog.TypeByOid(typ), "%s has unknown type %d", n, typ)
				return true
			})
			assert.Greater(t, visited, 1)
			clone := ast.CloneNode(tree)
			require.NotNil(t, clone)
			assert.Equal(t, analyze.ExprType(tree), analyze.ExprType(clone))
			assert.Equal(t, tree.String(), clone.String())
		})
	}
}

func TestFormatTypeBe(t *testing.T) {
	t.Parallel()
	for typ, want := range map[pgoid.Oid]string{
		pgoid.INT2OID: "smallint", pgoid.INT4OID: "integer", pgoid.INT8OID: "bigint",
		pgoid.BOOLOID: "boolean", pgoid.FLOAT4OID: "real", pgoid.FLOAT8OID: "double precision",
		pgoid.NUMERICOID: "numeric", pgoid.VARCHAROID: "character varying", pgoid.BPCHAROID: "character",
		pgoid.TIMESTAMPTZOID: "timestamp with time zone", pgoid.TEXTOID: "text", pgoid.UNKNOWNOID: "unknown",
		pgoid.CHAROID: `"char"`, pgoid.ANYOID: `"any"`, pgoid.NAMEOID: "name", pgoid.OIDVECTOROID: "oidvector",
		pgoid.INT4ARRAYOID: "integer[]", pgoid.TEXTARRAYOID: "text[]", pgoid.ANYELEMENTOID: "anyelement",
	} {
		assert.Equal(t, want, analyze.FormatTypeBe(typ), "%d", typ)
	}
}

// TestPolymorphicResolution covers enforce_generic_type_consistency: the
// anyelement/anyarray family, the anycompatible family (whose common type
// becomes the cast target), and the array coercion node that results.
func TestPolymorphicResolution(t *testing.T) {
	t.Parallel()
	ps := analyze.NewParseState()
	i4, i8 := pgoid.INT4OID, pgoid.INT8OID
	i4arr, i8arr := getArrayType(t, i4), getArrayType(t, i8)

	t.Run("anycompatible picks a common type", func(t *testing.T) {
		f := analyzeExpr(t, ps, "array_append(ARRAY[1], 2)").(*ast.FuncExpr)
		assert.Equal(t, procOID(t, "array_append", pgoid.ANYCOMPATIBLEARRAYOID, pgoid.ANYCOMPATIBLEOID), f.Funcid)
		assert.Equal(t, i4arr, f.Funcresulttype)

		f = analyzeExpr(t, ps, "array_append(ARRAY[1], '2')").(*ast.FuncExpr)
		assert.Equal(t, i4arr, f.Funcresulttype)
		constInt32(t, f.Args.Items[1], i4, 2, "unknown literal takes the common type")

		// int4[] is coerced to int8[] element by element.
		f = analyzeExpr(t, ps, "array_cat(ARRAY[1], ARRAY[2::int8])").(*ast.FuncExpr)
		assert.Equal(t, i8arr, f.Funcresulttype)
		ac, ok := f.Args.Items[0].(*ast.ArrayCoerceExpr)
		require.True(t, ok, "%T", f.Args.Items[0])
		assert.Equal(t, i8arr, ac.Resulttype)
		assert.Equal(t, i4arr, analyze.ExprType(ac.Arg))
		elem, ok := ac.Elemexpr.(*ast.FuncExpr)
		require.True(t, ok, "%T", ac.Elemexpr)
		assert.Equal(t, procOID(t, "int8", i4), elem.Funcid)
		_, ok = elem.Args.Items[0].(*ast.CaseTestExpr)
		assert.True(t, ok, "element placeholder")

		f = analyzeExpr(t, ps, "array_position(ARRAY[1, 2], 2::int8)").(*ast.FuncExpr)
		assert.Equal(t, i4, f.Funcresulttype)
		_, ok = f.Args.Items[0].(*ast.ArrayCoerceExpr)
		assert.True(t, ok)

		params := analyze.NewParseState()
		params.SetFixedParameters([]pgoid.Oid{i8arr})
		f = analyzeExpr(t, params, "array_remove($1, 1)").(*ast.FuncExpr)
		assert.Equal(t, i8arr, f.Funcresulttype)
		assert.Equal(t, i8, analyze.ExprType(f.Args.Items[1]))
	})

	t.Run("anyelement and anyarray", func(t *testing.T) {
		f := analyzeExpr(t, ps, "array_ndims(ARRAY[[1], [2]])").(*ast.FuncExpr)
		assert.Equal(t, i4, f.Funcresulttype)
		f = analyzeExpr(t, ps, "array_fill(1::int8, ARRAY[2])").(*ast.FuncExpr)
		assert.Equal(t, i8arr, f.Funcresulttype, "anyarray result is the array over the anyelement input")
		f = analyzeExpr(t, ps, "array_lower(ARRAY[1], 1)").(*ast.FuncExpr)
		assert.Equal(t, i4, f.Funcresulttype)
	})

	t.Run("errors", func(t *testing.T) {
		diag := analyzeErr(t, ps, "array_fill('1', ARRAY[2])")
		assert.Equal(t, mterrors.PgSSDatatypeMismatch, diag.Code)
		assert.Equal(t, "could not determine polymorphic type because input has type unknown", diag.Message)
		diag = analyzeErr(t, ps, "array_append(ARRAY[1], true)")
		assert.Equal(t, mterrors.PgSSUndefinedFunction, diag.Code)
		assert.Equal(t, "function array_append(integer[], boolean) does not exist", diag.Message)
		diag = analyzeErr(t, ps, "array_ndims(1)")
		assert.Equal(t, "function array_ndims(integer) does not exist", diag.Message)
	})
}
