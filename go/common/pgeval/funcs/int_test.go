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

package funcs_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/pgoid"
	"github.com/multigres/multigres/go/common/pgcatalog"
	"github.com/multigres/multigres/go/common/pgeval/datum"
	"github.com/multigres/multigres/go/common/pgeval/fmgr"
	_ "github.com/multigres/multigres/go/common/pgeval/funcs"
	"github.com/multigres/multigres/go/common/pgeval/pgerror"
)

func callInt4(t *testing.T, oid fmgr.Oid, args ...int32) (datum.Datum, error) {
	t.Helper()
	info, err := fmgr.FmgrInfoFor(oid)
	require.NoError(t, err)
	require.Equal(t, int(info.FnNargs), len(args))
	fcinfo := fmgr.NewFunctionCallInfo(info, len(args), pgoid.InvalidOid)
	for i, arg := range args {
		fcinfo.Args[i].Value = datum.Int32GetDatum(arg)
	}
	var result datum.Datum
	err = pgerror.Recover(func() { result = fmgr.CallFunction(fcinfo) })
	assert.False(t, fcinfo.IsNull)
	return result, err
}

func TestInt4(t *testing.T) {
	t.Parallel()
	i32 := datum.Int32GetDatum
	boolean := datum.BoolGetDatum
	for _, tc := range []struct {
		name string
		oid  fmgr.Oid
		args []int32
		want datum.Datum
	}{
		{"add", 177, []int32{20, 22}, i32(42)},
		{"add negative", 177, []int32{-20, -22}, i32(-42)},
		{"add mixed extremes", 177, []int32{math.MinInt32, math.MaxInt32}, i32(-1)},
		{"add upper boundary", 177, []int32{math.MaxInt32 - 1, 1}, i32(math.MaxInt32)},
		{"add lower boundary", 177, []int32{math.MinInt32 + 1, -1}, i32(math.MinInt32)},
		{"subtract", 181, []int32{20, 22}, i32(-2)},
		{"subtract negative", 181, []int32{20, -22}, i32(42)},
		{"subtract upper boundary", 181, []int32{math.MaxInt32 - 1, -1}, i32(math.MaxInt32)},
		{"subtract lower boundary", 181, []int32{math.MinInt32 + 1, 1}, i32(math.MinInt32)},
		{"subtract equal extremes", 181, []int32{math.MinInt32, math.MinInt32}, i32(0)},
		{"multiply", 141, []int32{6, 7}, i32(42)},
		{"multiply negative", 141, []int32{-6, 7}, i32(-42)},
		{"multiply negatives", 141, []int32{-6, -7}, i32(42)},
		{"multiply zero", 141, []int32{math.MinInt32, 0}, i32(0)},
		{"multiply upper boundary", 141, []int32{math.MaxInt32, 1}, i32(math.MaxInt32)},
		{"multiply lower boundary", 141, []int32{math.MinInt32 / 2, 2}, i32(math.MinInt32)},
		{"multiply square boundary", 141, []int32{46340, 46340}, i32(2147395600)},
		{"divide", 154, []int32{8, 3}, i32(2)},
		{"divide negative dividend", 154, []int32{-8, 3}, i32(-2)},
		{"divide negative divisor", 154, []int32{8, -3}, i32(-2)},
		{"divide negatives", 154, []int32{-8, -3}, i32(2)},
		{"divide by minus one", 154, []int32{math.MaxInt32, -1}, i32(-math.MaxInt32)},
		{"divide min by one", 154, []int32{math.MinInt32, 1}, i32(math.MinInt32)},
		{"divide min by min", 154, []int32{math.MinInt32, math.MinInt32}, i32(1)},
		{"modulo", 156, []int32{8, 3}, i32(2)},
		{"modulo negative dividend", 156, []int32{-8, 3}, i32(-2)},
		{"modulo negative divisor", 156, []int32{8, -3}, i32(2)},
		{"modulo negatives", 156, []int32{-8, -3}, i32(-2)},
		{"modulo min by minus one", 156, []int32{math.MinInt32, -1}, i32(0)},
		{"modulo min by min", 156, []int32{math.MinInt32, math.MinInt32}, i32(0)},
		{"modulo SQL alias", 941, []int32{8, 3}, i32(2)},
		{"negate", 212, []int32{42}, i32(-42)},
		{"negate negative", 212, []int32{-42}, i32(42)},
		{"negate zero", 212, []int32{0}, i32(0)},
		{"unary plus", 1912, []int32{math.MinInt32}, i32(math.MinInt32)},
		{"increment", 766, []int32{41}, i32(42)},
		{"increment min", 766, []int32{math.MinInt32}, i32(math.MinInt32 + 1)},
		{"increment to max", 766, []int32{math.MaxInt32 - 1}, i32(math.MaxInt32)},
		{"abs positive", 1251, []int32{42}, i32(42)},
		{"abs negative", 1251, []int32{-42}, i32(42)},
		{"abs zero", 1251, []int32{0}, i32(0)},
		{"abs upper boundary", 1251, []int32{-math.MaxInt32}, i32(math.MaxInt32)},
		{"abs SQL alias", 1397, []int32{-42}, i32(42)},
		{"equal true", 65, []int32{-42, -42}, boolean(true)},
		{"equal false", 65, []int32{math.MinInt32, math.MaxInt32}, boolean(false)},
		{"not equal true", 144, []int32{math.MinInt32, math.MaxInt32}, boolean(true)},
		{"not equal false", 144, []int32{-42, -42}, boolean(false)},
		{"less true", 66, []int32{math.MinInt32, math.MaxInt32}, boolean(true)},
		{"less false", 66, []int32{0, 0}, boolean(false)},
		{"less or equal true", 149, []int32{0, 0}, boolean(true)},
		{"less or equal false", 149, []int32{math.MaxInt32, math.MinInt32}, boolean(false)},
		{"greater true", 147, []int32{math.MaxInt32, math.MinInt32}, boolean(true)},
		{"greater false", 147, []int32{0, 0}, boolean(false)},
		{"greater or equal true", 150, []int32{0, 0}, boolean(true)},
		{"greater or equal false", 150, []int32{math.MinInt32, math.MaxInt32}, boolean(false)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := callInt4(t, tc.oid, tc.args...)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestInt4Errors(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		oid  fmgr.Oid
		args []int32
		code string
	}{
		{"add overflow", 177, []int32{math.MaxInt32, 1}, "22003"},
		{"add underflow", 177, []int32{math.MinInt32, -1}, "22003"},
		{"subtract overflow", 181, []int32{math.MaxInt32, -1}, "22003"},
		{"subtract underflow", 181, []int32{math.MinInt32, 1}, "22003"},
		{"subtract extremes", 181, []int32{math.MaxInt32, math.MinInt32}, "22003"},
		{"multiply overflow", 141, []int32{math.MaxInt32, 2}, "22003"},
		{"multiply underflow", 141, []int32{math.MinInt32, 2}, "22003"},
		{"multiply min by minus one", 141, []int32{math.MinInt32, -1}, "22003"},
		{"multiply min by min", 141, []int32{math.MinInt32, math.MinInt32}, "22003"},
		{"multiply square overflow", 141, []int32{46341, 46341}, "22003"},
		{"divide overflow", 154, []int32{math.MinInt32, -1}, "22003"},
		{"divide by zero", 154, []int32{1, 0}, "22012"},
		{"zero divided by zero", 154, []int32{0, 0}, "22012"},
		{"min divided by zero", 154, []int32{math.MinInt32, 0}, "22012"},
		{"modulo by zero", 156, []int32{1, 0}, "22012"},
		{"modulo alias by zero", 941, []int32{math.MinInt32, 0}, "22012"},
		{"negate min", 212, []int32{math.MinInt32}, "22003"},
		{"increment max", 766, []int32{math.MaxInt32}, "22003"},
		{"abs min", 1251, []int32{math.MinInt32}, "22003"},
		{"abs alias min", 1397, []int32{math.MinInt32}, "22003"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := callInt4(t, tc.oid, tc.args...)
			var diag *mterrors.PgDiagnostic
			require.ErrorAs(t, err, &diag)
			assert.Equal(t, tc.code, diag.Code)
			assert.Equal(t, "ERROR", diag.Severity)
			message := "integer out of range"
			if tc.code == "22012" {
				message = "division by zero"
			}
			assert.Equal(t, message, diag.Message)
		})
	}
}

func TestInt4CatalogAndNulls(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		oid   fmgr.Oid
		src   string
		nargs int
		ret   pgoid.Oid
	}{
		{65, "int4eq", 2, pgoid.BOOLOID},
		{144, "int4ne", 2, pgoid.BOOLOID},
		{66, "int4lt", 2, pgoid.BOOLOID},
		{149, "int4le", 2, pgoid.BOOLOID},
		{147, "int4gt", 2, pgoid.BOOLOID},
		{150, "int4ge", 2, pgoid.BOOLOID},
		{177, "int4pl", 2, pgoid.INT4OID},
		{181, "int4mi", 2, pgoid.INT4OID},
		{141, "int4mul", 2, pgoid.INT4OID},
		{154, "int4div", 2, pgoid.INT4OID},
		{156, "int4mod", 2, pgoid.INT4OID},
		{941, "int4mod", 2, pgoid.INT4OID},
		{212, "int4um", 1, pgoid.INT4OID},
		{1912, "int4up", 1, pgoid.INT4OID},
		{766, "int4inc", 1, pgoid.INT4OID},
		{1251, "int4abs", 1, pgoid.INT4OID},
		{1397, "int4abs", 1, pgoid.INT4OID},
	} {
		t.Run(tc.src, func(t *testing.T) {
			proc := pgcatalog.ProcByOid(tc.oid)
			require.NotNil(t, proc)
			assert.Equal(t, tc.src, proc.Src)
			assert.Equal(t, tc.ret, proc.RetType)
			require.Len(t, proc.ArgTypes, tc.nargs)
			for _, typ := range proc.ArgTypes {
				assert.Equal(t, pgoid.INT4OID, typ)
			}
			assert.True(t, fmgr.IsBuiltinRegistered(proc.Src))
			info, err := fmgr.FmgrInfoFor(tc.oid)
			require.NoError(t, err)
			assert.Equal(t, tc.oid, info.FnOid)
			assert.Equal(t, int16(tc.nargs), info.FnNargs)
			require.True(t, info.FnStrict)
			assert.False(t, info.FnRetset)

			fcinfo := fmgr.NewFunctionCallInfo(info, tc.nargs, pgoid.InvalidOid)
			for nullArg := range fcinfo.Args {
				for i := range fcinfo.Args {
					// A division's zero divisor must not run when either arg is NULL.
					fcinfo.Args[i] = datum.NullableDatum{IsNull: i == nullArg}
				}
				err := pgerror.Recover(func() { fmgr.CallFunction(fcinfo) })
				require.NoError(t, err)
				assert.True(t, fcinfo.IsNull)
			}
			// Reusing the frame after a NULL call must reset result nullness.
			for i := range fcinfo.Args {
				fcinfo.Args[i] = datum.NullableDatum{Value: datum.Int32GetDatum(1)}
			}
			require.NoError(t, pgerror.Recover(func() { fmgr.CallFunction(fcinfo) }))
			assert.False(t, fcinfo.IsNull)
		})
	}
}

func BenchmarkInt4pl(b *testing.B) {
	info, err := fmgr.FmgrInfoFor(177)
	require.NoError(b, err)
	fcinfo := fmgr.NewFunctionCallInfo(info, 2, pgoid.InvalidOid)
	fcinfo.Args[0].Value = datum.Int32GetDatum(20)
	fcinfo.Args[1].Value = datum.Int32GetDatum(22)
	b.ReportAllocs()
	for b.Loop() {
		fmgr.CallFunction(fcinfo)
	}
}
