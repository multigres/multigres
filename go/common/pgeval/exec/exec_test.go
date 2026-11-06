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

package exec_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/parser/datum"
	"github.com/multigres/multigres/go/common/parser/pgoid"
	"github.com/multigres/multigres/go/common/pgcatalog"
	"github.com/multigres/multigres/go/common/pgeval/analyze"
	"github.com/multigres/multigres/go/common/pgeval/exec"
	"github.com/multigres/multigres/go/common/pgeval/fmgr"
	_ "github.com/multigres/multigres/go/common/pgeval/funcs"
)

// analyzeSQL parses and analyzes "SELECT <sql>" with the given fixed
// parameter types.
func analyzeSQL(t *testing.T, sql string, paramTypes ...pgoid.Oid) ast.Node {
	t.Helper()
	stmts, err := parser.ParseSQL("SELECT " + sql)
	require.NoError(t, err, sql)
	raw := stmts[0].(*ast.SelectStmt).TargetList.Items[0].(*ast.ResTarget).Val
	ps := analyze.NewParseState()
	ps.SetFixedParameters(paramTypes)
	tree, err := ps.TransformExpr(raw)
	require.NoError(t, err, sql)
	return tree
}

// render returns a value through its type's output function, or "NULL".
func render(t *testing.T, typ pgoid.Oid, v datum.NullableDatum) string {
	t.Helper()
	if v.IsNull {
		return "NULL"
	}
	flinfo, err := fmgr.FmgrInfoFor(pgcatalog.TypeByOid(typ).Output)
	require.NoError(t, err)
	return string(datum.DatumGetBytes(fmgr.FunctionCall1Coll(flinfo, pgoid.InvalidOid, v.Value)))
}

// eval analyzes, prepares and evaluates sql with the given parameters,
// whose types are declared to analysis.
func eval(t *testing.T, sql string, params ...exec.ExternParam) (string, error) {
	t.Helper()
	types := make([]pgoid.Oid, len(params))
	for i, p := range params {
		types[i] = p.Type
	}
	tree := analyzeSQL(t, sql, types...)
	state, err := exec.Prepare(tree)
	require.NoError(t, err, sql)
	v, err := state.Eval(params)
	if err != nil {
		return "", err
	}
	return render(t, analyze.ExprType(tree), v), nil
}

func mustEval(t *testing.T, sql string, params ...exec.ExternParam) string {
	t.Helper()
	got, err := eval(t, sql, params...)
	require.NoError(t, err, sql)
	return got
}

func diagnostic(t *testing.T, err error) *mterrors.PgDiagnostic {
	t.Helper()
	var diag *mterrors.PgDiagnostic
	require.ErrorAs(t, err, &diag)
	return diag
}

func int4Param(v int32) exec.ExternParam {
	return exec.ExternParam{Value: datum.Int32GetDatum(v), Type: pgoid.INT4OID}
}

func TestValues(t *testing.T) {
	for _, tc := range []struct{ sql, want string }{
		{"1 + 2", "3"},
		{"2::int2 * 3::int8", "6"},
		{"-5", "-5"},
		{"-(5)", "-5"},
		{"abs(-7)", "7"},
		{"7 % 3", "1"},
		{"1 << 4", "16"},
		{"gcd(12, 18)", "6"},
		{"int4inc(41)", "42"},
		{"1::int4::int8", "1"},
		{"'12'::int4", "12"},
		{"true", "t"},
		{"1 = 1", "t"},
		{"1 <> 1", "f"},
		{"2 > 1::int8", "t"},
		{"1 + NULL::int4", "NULL"},
		{"NULL::int4 * 2", "NULL"},
		{"abs(NULL::int8)", "NULL"},
	} {
		require.Equal(t, tc.want, mustEval(t, tc.sql), tc.sql)
	}
}

func TestBooleanLogic(t *testing.T) {
	for _, tc := range []struct{ sql, want string }{
		{"true AND true", "t"},
		{"true AND false", "f"},
		{"true AND NULL", "NULL"},
		{"false AND NULL", "f"},
		{"NULL AND NULL", "NULL"},
		{"true AND true AND NULL AND true", "NULL"},
		{"false OR false", "f"},
		{"false OR true", "t"},
		{"true OR NULL", "t"},
		{"false OR NULL", "NULL"},
		{"NULL OR NULL", "NULL"},
		{"NOT true", "f"},
		{"NOT false", "t"},
		{"NOT NULL::bool", "NULL"},
		// Short-circuiting: the division is never evaluated.
		{"false AND 1/0 = 1", "f"},
		{"true OR 1/0 = 1", "t"},
		{"NULL AND false AND 1/0 = 1", "f"},
	} {
		require.Equal(t, tc.want, mustEval(t, tc.sql), tc.sql)
	}

	_, err := eval(t, "true AND 1/0 = 1")
	require.Equal(t, mterrors.PgSSDivisionByZero, diagnostic(t, err).Code)
}

func TestNullAndBooleanTests(t *testing.T) {
	args := []string{"true", "false", "NULL::bool"}
	for _, tc := range []struct {
		test string
		want [3]string // for true, false, NULL
	}{
		{"IS NULL", [3]string{"f", "f", "t"}},
		{"IS NOT NULL", [3]string{"t", "t", "f"}},
		{"IS TRUE", [3]string{"t", "f", "f"}},
		{"IS NOT TRUE", [3]string{"f", "t", "t"}},
		{"IS FALSE", [3]string{"f", "t", "f"}},
		{"IS NOT FALSE", [3]string{"t", "f", "t"}},
		{"IS UNKNOWN", [3]string{"f", "f", "t"}},
		{"IS NOT UNKNOWN", [3]string{"t", "t", "f"}},
	} {
		for i, arg := range args {
			sql := "(" + arg + ") " + tc.test
			require.Equal(t, tc.want[i], mustEval(t, sql), sql)
		}
	}
}

func TestConditionals(t *testing.T) {
	for _, tc := range []struct{ sql, want string }{
		{"CASE WHEN 1 = 1 THEN 10 ELSE 20 END", "10"},
		{"CASE WHEN 1 = 2 THEN 10 ELSE 20 END", "20"},
		{"CASE WHEN NULL THEN 10 ELSE 20 END", "20"},
		{"CASE WHEN 1 = 2 THEN 10 END", "NULL"},
		{"CASE WHEN 1 = 2 THEN 10 WHEN 2 = 2 THEN 11 WHEN 3 = 3 THEN 12 END", "11"},
		{"CASE 2 WHEN 1 THEN 10 WHEN 2 THEN 20 END", "20"},
		{"CASE 3 WHEN 1 THEN 10 WHEN 2 THEN 20 END", "NULL"},
		{"CASE NULL::int4 WHEN 1 THEN 10 ELSE 20 END", "20"},
		// A CASE inside a result sees its own test value.
		{"CASE 1 WHEN 1 THEN CASE 2 WHEN 2 THEN 5 END ELSE 6 END", "5"},
		// A CASE inside a WHEN restores the outer test value afterwards.
		{"CASE 1 WHEN CASE 2 WHEN 2 THEN 1 END THEN 7 ELSE 8 END", "7"},
		{"CASE 1 WHEN CASE 2 WHEN 2 THEN 0 END THEN 7 WHEN 1 THEN 9 END", "9"},
		// Only the selected branch is evaluated.
		{"CASE WHEN true THEN 1 ELSE 1/0 END", "1"},
		{"CASE WHEN false THEN 1/0 ELSE 2 END", "2"},
		{"COALESCE(NULL::int4, 2, 1/0)", "2"},
		{"COALESCE(1, 2)", "1"},
		{"COALESCE(NULL::int4, NULL)", "NULL"},
		{"NULLIF(1, 1)", "NULL"},
		{"NULLIF(1, 2)", "1"},
		{"NULLIF(NULL::int4, 1)", "NULL"},
		{"NULLIF(1, NULL)", "1"},
		{"1 IS DISTINCT FROM 1", "f"},
		{"1 IS DISTINCT FROM 2", "t"},
		{"1 IS DISTINCT FROM NULL", "t"},
		{"NULL::int4 IS DISTINCT FROM NULL", "f"},
		{"1 IS NOT DISTINCT FROM 1", "t"},
		{"NULL::int4 IS NOT DISTINCT FROM 1", "f"},
	} {
		require.Equal(t, tc.want, mustEval(t, tc.sql), tc.sql)
	}

	// The test expression is evaluated before any WHEN.
	_, err := eval(t, "CASE 1/0 WHEN 1 THEN 1 ELSE 2 END")
	require.Equal(t, mterrors.PgSSDivisionByZero, diagnostic(t, err).Code)
}

func TestEvaluationErrors(t *testing.T) {
	for _, tc := range []struct{ sql, code, message string }{
		{"1/0", mterrors.PgSSDivisionByZero, "division by zero"},
		{"2147483647 + 1", mterrors.PgSSNumericValueOutOfRange, "integer out of range"},
		{"abs(-32768::int2)", mterrors.PgSSNumericValueOutOfRange, "smallint out of range"},
	} {
		_, err := eval(t, tc.sql)
		diag := diagnostic(t, err)
		require.Equal(t, tc.code, diag.Code, tc.sql)
		require.Equal(t, tc.message, diag.Message, tc.sql)
	}
}

func TestParameters(t *testing.T) {
	require.Equal(t, "7", mustEval(t, "$1 + $2", int4Param(3), int4Param(4)))
	require.Equal(t, "t", mustEval(t, "$1 IS NULL", exec.ExternParam{Type: pgoid.INT4OID, IsNull: true}))
	require.Equal(t, "NULL", mustEval(t, "$1 + 1", exec.ExternParam{Type: pgoid.INT4OID, IsNull: true}))
	require.Equal(t, "3", mustEval(t, "CASE $1 WHEN 1 THEN 2 WHEN 2 THEN 3 END", int4Param(2)))

	// One prepared state serves many evaluations.
	state, err := exec.Prepare(analyzeSQL(t, "$1 * 2", pgoid.INT4OID))
	require.NoError(t, err)
	for _, v := range []int32{1, 5, -3} {
		got, err := state.Eval([]exec.ExternParam{int4Param(v)})
		require.NoError(t, err)
		require.Equal(t, v*2, datum.DatumGetInt32(got.Value))
	}

	t.Run("missing", func(t *testing.T) {
		_, err := state.Eval(nil)
		diag := diagnostic(t, err)
		require.Equal(t, mterrors.PgSSUndefinedObject, diag.Code)
		require.Equal(t, "no value found for parameter 1", diag.Message)
	})
	t.Run("untyped counts as missing", func(t *testing.T) {
		_, err := state.Eval([]exec.ExternParam{{Value: datum.Int32GetDatum(1)}})
		require.Equal(t, mterrors.PgSSUndefinedObject, diagnostic(t, err).Code)
	})
	t.Run("type mismatch", func(t *testing.T) {
		_, err := state.Eval([]exec.ExternParam{{Value: datum.Int64GetDatum(1), Type: pgoid.INT8OID}})
		diag := diagnostic(t, err)
		require.Equal(t, mterrors.PgSSDatatypeMismatch, diag.Code)
		require.Equal(t, "type of parameter 1 (bigint) does not match that when preparing the plan (integer)", diag.Message)
	})
}

func TestPrepareDeclines(t *testing.T) {
	for _, tc := range []struct{ sql, message string }{
		{"ARRAY[1, 2]", "array values are not supported by the gateway evaluation engine"},
		{"1 = ANY (ARRAY[1, 2])", "array values are not supported by the gateway evaluation engine"},
		{"ARRAY[1, 2]::int8[]", "array values are not supported by the gateway evaluation engine"},
		{"GREATEST(1, 2)", "GREATEST and LEAST are not supported by the gateway evaluation engine"},
		// textin is not ported: declined before anything runs, even in a
		// branch that would never execute.
		{"(1)::text", "function textin (OID 46) is not yet implemented in the gateway evaluation engine"},
		{"CASE WHEN false THEN (1)::text ELSE (2)::text END", "function textin (OID 46) is not yet implemented in the gateway evaluation engine"},
	} {
		_, err := exec.Prepare(analyzeSQL(t, tc.sql))
		diag := diagnostic(t, err)
		require.Equal(t, mterrors.PgSSFeatureNotSupported, diag.Code, tc.sql)
		require.Equal(t, tc.message, diag.Message, tc.sql)
	}
}

func int4Const(v int32) *ast.Const {
	return ast.NewConst(pgoid.INT4OID, datum.Int32GetDatum(v), false)
}

func procOid(t *testing.T, name string, nargs int) pgoid.Oid {
	t.Helper()
	for _, p := range pgcatalog.ProcsByName(name) {
		if len(p.ArgTypes) == nargs {
			return p.Oid
		}
	}
	t.Fatalf("no builtin %s with %d arguments", name, nargs)
	return 0
}

// Trees analysis never builds, to cover the executor's own checks.
func TestPrepareMalformedTrees(t *testing.T) {
	manyArgs := ast.NewNodeList()
	for range 101 {
		manyArgs.Append(int4Const(1))
	}
	nullConst := ast.NewConst(pgoid.INT4OID, datum.Datum{}, true)
	for _, tc := range []struct {
		name    string
		tree    ast.Node
		code    string
		message string
	}{
		{"nil", nil, mterrors.PgSSInternalError, "unrecognized node type: <nil>"},
		{"raw node", ast.NewColumnRef(ast.NewString("x")), mterrors.PgSSInternalError, "unrecognized node type: *ast.ColumnRef"},
		{"exec param", ast.NewParam(ast.PARAM_EXEC, 1, pgoid.INT4OID), mterrors.PgSSInternalError, "unrecognized paramkind: 1"},
		{"unknown function", ast.NewFuncExpr(999999, pgoid.INT4OID, ast.NewNodeList(int4Const(1))), mterrors.PgSSInternalError, "cache lookup failed for function 999999"},
		{
			"set-returning function", ast.NewFuncExpr(procOid(t, "generate_series", 2), pgoid.INT4OID, ast.NewNodeList(int4Const(1), int4Const(2))),
			mterrors.PgSSFeatureNotSupported, "set-valued function called in context that cannot accept a set",
		},
		{"too many arguments", ast.NewFuncExpr(procOid(t, "int4pl", 2), pgoid.INT4OID, manyArgs), mterrors.PgSSTooManyArguments, "cannot pass more than 100 arguments to a function"},
		{"stray CaseTestExpr", ast.NewCaseTestExpr(pgoid.INT4OID, -1, 0, -1), mterrors.PgSSInternalError, "CaseTestExpr outside a CASE expression"},
		{"domain", ast.NewCoerceToDomain(int4Const(1), pgoid.INT4OID, -1, ast.COERCE_EXPLICIT_CAST), mterrors.PgSSFeatureNotSupported, "domain types are not supported by the gateway evaluation engine"},
		{"row null test", ast.NewRowNullTest(int4Const(1), ast.IS_NULL), mterrors.PgSSFeatureNotSupported, "row-valued IS NULL tests are not supported by the gateway evaluation engine"},
		{"unknown type", ast.NewCoerceViaIO(int4Const(1), 999999, ast.COERCE_EXPLICIT_CAST), mterrors.PgSSInternalError, "cache lookup failed for type 999999"},
		{"no default result", &ast.CaseExpr{Casetype: pgoid.INT4OID, Args: ast.NewNodeList()}, mterrors.PgSSInternalError, "CASE expression without a default result"},
		{"bad WHEN", &ast.CaseExpr{Casetype: pgoid.INT4OID, Args: ast.NewNodeList(int4Const(1)), Defresult: nullConst}, mterrors.PgSSInternalError, "unrecognized node type: *ast.Const"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := exec.Prepare(tc.tree)
			diag := diagnostic(t, err)
			require.Equal(t, tc.code, diag.Code)
			require.Equal(t, tc.message, diag.Message)
		})
	}
}

// The analyzer only builds CoerceViaIO toward types whose input function is
// not ported yet, so the step is driven with a hand-built tree.
func TestCoerceViaIO(t *testing.T) {
	evalTree := func(t *testing.T, tree ast.Node) datum.NullableDatum {
		t.Helper()
		state, err := exec.Prepare(tree)
		require.NoError(t, err)
		v, err := state.Eval(nil)
		require.NoError(t, err)
		return v
	}

	v := evalTree(t, ast.NewCoerceViaIO(int4Const(42), pgoid.INT8OID, ast.COERCE_EXPLICIT_CAST))
	require.False(t, v.IsNull)
	require.Equal(t, int64(42), datum.DatumGetInt64(v.Value))

	v = evalTree(t, ast.NewCoerceViaIO(int4Const(-7), pgoid.INT2OID, ast.COERCE_EXPLICIT_CAST))
	require.Equal(t, int16(-7), datum.DatumGetInt16(v.Value))

	// Strict input function: NULL in, NULL out, nothing called.
	v = evalTree(t, ast.NewCoerceViaIO(ast.NewConst(pgoid.INT4OID, datum.Datum{}, true), pgoid.INT8OID, ast.COERCE_EXPLICIT_CAST))
	require.True(t, v.IsNull)

	// The input function's own errors come through.
	state, err := exec.Prepare(ast.NewCoerceViaIO(int4Const(70000), pgoid.INT2OID, ast.COERCE_EXPLICIT_CAST))
	require.NoError(t, err)
	_, err = state.Eval(nil)
	diag := diagnostic(t, err)
	require.Equal(t, mterrors.PgSSNumericValueOutOfRange, diag.Code)
	require.Equal(t, `value "70000" is out of range for type smallint`, diag.Message)

	// A relabel is a no-op at runtime.
	v = evalTree(t, ast.NewRelabelType(int4Const(9), pgoid.OIDOID, -1, ast.COERCE_IMPLICIT_CAST))
	require.Equal(t, int32(9), datum.DatumGetInt32(v.Value))
}
