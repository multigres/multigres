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
	"fmt"
	"math"
	"math/big"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/pgoid"
	"github.com/multigres/multigres/go/common/pgcatalog"
	"github.com/multigres/multigres/go/common/pgeval/datum"
	"github.com/multigres/multigres/go/common/pgeval/fmgr"
	"github.com/multigres/multigres/go/common/pgeval/pgerror"
)

func TestScalarRegistrations(t *testing.T) {
	t.Parallel()
	var expected []string
	for _, prefix := range []string{"int2", "int4", "int8", "int24", "int42", "int28", "int82", "int48", "int84"} {
		for _, suffix := range []string{"eq", "ne", "lt", "le", "gt", "ge", "pl", "mi", "mul", "div"} {
			expected = append(expected, prefix+suffix)
		}
		expected = append(expected, "bt"+prefix+"cmp")
	}
	for _, prefix := range []string{"int2", "int4", "int8"} {
		for _, suffix := range []string{"um", "up", "abs", "mod", "larger", "smaller", "and", "or", "xor", "not", "shl", "shr", "in", "out", "send", "recv"} {
			expected = append(expected, prefix+suffix)
		}
	}
	expected = append(expected,
		"i2toi4", "i4toi2", "int28", "int82", "int48", "int84", "int4inc", "int8inc", "int8dec",
		"int4gcd", "int8gcd", "int4lcm", "int8lcm", "boolin", "boolout", "booltext",
		"booleq", "boolne", "boollt", "boolle", "boolgt", "boolge", "int4_bool", "bool_int4",
		"booland_statefunc", "boolor_statefunc", "btboolcmp", "boolsend", "boolrecv",
		"in_range_int2_int2", "in_range_int2_int4", "in_range_int2_int8",
		"in_range_int4_int2", "in_range_int4_int4", "in_range_int4_int8", "in_range_int8_int8")
	for _, typ := range []string{"int2", "int4", "int8", "char"} {
		expected = append(expected, "hash"+typ, "hash"+typ+"extended")
	}
	assert.ElementsMatch(t, expected, fmgr.RegisteredBuiltins())

	seen := map[string]bool{}
	for _, proc := range pgcatalog.Procs {
		if !fmgr.IsBuiltinRegistered(proc.Src) {
			continue
		}
		seen[proc.Src] = true
		t.Run(fmt.Sprintf("%s_%d", proc.Src, proc.Oid), func(t *testing.T) {
			info, err := fmgr.FmgrInfoFor(proc.Oid)
			require.NoError(t, err)
			require.True(t, info.FnStrict)
			assert.False(t, info.FnRetset)
			assert.Equal(t, proc.Oid, info.FnOid)
			assert.Equal(t, int16(len(proc.ArgTypes)), info.FnNargs)
			fc := fmgr.NewFunctionCallInfo(info, len(proc.ArgTypes), pgoid.InvalidOid)
			// Every NULL position must short-circuit, including zero divisors
			// and invalid input in the remaining zero-valued slots.
			for nullArg := range fc.Args {
				for i := range fc.Args {
					fc.Args[i] = datum.NullableDatum{IsNull: i == nullArg}
				}
				require.NoError(t, pgerror.Recover(func() { fmgr.CallFunction(fc) }))
				assert.True(t, fc.IsNull)
			}
		})
	}
	for _, src := range expected {
		assert.True(t, seen[src], "registration %s must correspond to a real catalog entry", src)
	}
}

func integerLimits(typ pgoid.Oid) (int64, int64) {
	switch typ {
	case pgoid.INT2OID:
		return math.MinInt16, math.MaxInt16
	case pgoid.INT4OID:
		return math.MinInt32, math.MaxInt32
	default:
		return math.MinInt64, math.MaxInt64
	}
}

func integerSamples(typ pgoid.Oid) []int64 {
	lo, hi := integerLimits(typ)
	values := []int64{lo, lo + 1, -2, -1, 0, 1, 2, hi - 1, hi}
	for _, v := range []int64{181, 182, 32768, -32769, 46340, 46341, 2147483648, -2147483649, 3037000499, 3037000500} {
		if v >= lo && v <= hi {
			values = append(values, v, -v)
		}
	}
	return values
}

func isInteger(typ pgoid.Oid) bool {
	return typ == pgoid.INT2OID || typ == pgoid.INT4OID || typ == pgoid.INT8OID
}

func integerDatum(typ pgoid.Oid, n int64) datum.Datum {
	switch typ {
	case pgoid.INT2OID:
		return datum.Int16GetDatum(int16(n))
	case pgoid.INT4OID:
		return datum.Int32GetDatum(int32(n))
	default:
		return datum.Int64GetDatum(n)
	}
}

// A mathematical big.Int oracle is independent of the production wraparound
// overflow checks. Exercise every integer signature, not just same-width calls.
func TestIntegersAgainstBigInt(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewPCG(2026, 17))
	for _, proc := range pgcatalog.Procs {
		if !fmgr.IsBuiltinRegistered(proc.Src) || !isInteger(proc.ArgTypes[0]) ||
			len(proc.ArgTypes) > 2 || strings.HasPrefix(proc.Src, "hash") ||
			(!isInteger(proc.RetType) && proc.RetType != pgoid.BOOLOID) {
			continue
		}
		t.Run(fmt.Sprintf("%s_%d", proc.Src, proc.Oid), func(t *testing.T) {
			info, err := fmgr.FmgrInfoFor(proc.Oid)
			require.NoError(t, err)
			fc := fmgr.NewFunctionCallInfo(info, len(proc.ArgTypes), pgoid.InvalidOid)
			check := func(a, b int64) {
				t.Helper()
				fc.Args[0].Value = integerDatum(proc.ArgTypes[0], a)
				if len(fc.Args) == 2 {
					fc.Args[1].Value = integerDatum(proc.ArgTypes[1], b)
				}
				var got datum.Datum
				err := pgerror.Recover(func() { got = fmgr.CallFunction(fc) })
				want, code := integerOracle(t, &proc, a, b)
				if code != "" {
					var diag *mterrors.PgDiagnostic
					require.ErrorAs(t, err, &diag, "args: %d, %d", a, b)
					require.Equal(t, code, diag.Code, "args: %d, %d", a, b)
					return
				}
				require.NoError(t, err, "args: %d, %d", a, b)
				require.False(t, fc.IsNull)
				if proc.RetType == pgoid.BOOLOID {
					require.Equal(t, want.Sign() != 0, datum.DatumGetBool(got), "args: %d, %d", a, b)
				} else {
					require.Equal(t, want.Int64(), datum.DatumGetInt64(got), "args: %d, %d", a, b)
				}
			}
			for _, a := range integerSamples(proc.ArgTypes[0]) {
				if len(proc.ArgTypes) == 1 {
					check(a, 0)
				} else {
					for _, b := range integerSamples(proc.ArgTypes[1]) {
						check(a, b)
					}
				}
			}
			for range 100 {
				a := datum.DatumGetInt64(integerDatum(proc.ArgTypes[0], int64(rng.Uint64())))
				var b int64
				if len(proc.ArgTypes) == 2 {
					b = datum.DatumGetInt64(integerDatum(proc.ArgTypes[1], int64(rng.Uint64())))
				}
				check(a, b)
			}
		})
	}
}

func integerOracle(t *testing.T, proc *pgcatalog.Proc, a, b int64) (*big.Int, string) {
	t.Helper()
	x, y, result := big.NewInt(a), big.NewInt(b), new(big.Int)
	op := strings.TrimLeft(strings.TrimPrefix(strings.TrimPrefix(proc.Src, "bt"), "int"), "248")
	switch op {
	case "cmp":
		if proc.Src == "btint2cmp" {
			result.Sub(x, y)
		} else {
			result.SetInt64(int64(x.Cmp(y)))
		}
	case "eq", "ne", "lt", "le", "gt", "ge", "_bool":
		value := map[string]bool{"eq": a == b, "ne": a != b, "lt": a < b, "le": a <= b, "gt": a > b, "ge": a >= b, "_bool": a != 0}[op]
		if value {
			result.SetInt64(1)
		}
		return result, ""
	case "", "i2toi4", "i4toi2", "up":
		result.Set(x)
	case "pl":
		result.Add(x, y)
	case "mi":
		result.Sub(x, y)
	case "mul":
		result.Mul(x, y)
	case "div", "mod":
		if b == 0 {
			return nil, "22012"
		}
		if op == "div" {
			result.Quo(x, y)
		} else {
			result.Rem(x, y)
		}
	case "um":
		result.Neg(x)
	case "abs":
		result.Abs(x)
	case "inc":
		result.Add(x, big.NewInt(1))
	case "dec":
		result.Sub(x, big.NewInt(1))
	case "larger":
		result.SetInt64(max(a, b))
	case "smaller":
		result.SetInt64(min(a, b))
	case "gcd":
		result.GCD(nil, nil, x, y)
	case "lcm":
		if a != 0 && b != 0 {
			gcd := new(big.Int).GCD(nil, nil, x, y)
			result.Abs(result.Quo(result.Mul(x, y), gcd))
		}
	case "and":
		result.And(x, y)
	case "or":
		result.Or(x, y)
	case "xor":
		result.Xor(x, y)
	case "not":
		result.Not(x)
	case "shl", "shr":
		width := uint(32)
		if proc.ArgTypes[0] == pgoid.INT8OID {
			width = 64
		}
		shift := uint(uint32(b)) % width
		if op == "shl" {
			result.Lsh(x, shift)
		} else {
			result.Rsh(x, shift)
		}
		if proc.RetType == pgoid.INT2OID {
			width = 16
		}
		modulus := new(big.Int).Lsh(big.NewInt(1), width)
		result.Mod(result, modulus)
		if result.Bit(int(width-1)) != 0 {
			result.Sub(result, modulus)
		}
	default:
		t.Fatalf("missing oracle for %s", proc.Src)
	}
	lo, hi := integerLimits(proc.RetType)
	if result.Cmp(big.NewInt(lo)) < 0 || result.Cmp(big.NewInt(hi)) > 0 {
		return nil, "22003"
	}
	return result, ""
}

func scalarCall(t testing.TB, src string, args ...datum.Datum) (datum.NullableDatum, error) {
	t.Helper()
	for _, proc := range pgcatalog.Procs {
		if proc.Src != src {
			continue
		}
		info, err := fmgr.FmgrInfoFor(proc.Oid)
		require.NoError(t, err)
		require.Equal(t, len(proc.ArgTypes), len(args))
		fc := fmgr.NewFunctionCallInfo(info, len(args), pgoid.InvalidOid)
		for i, arg := range args {
			fc.Args[i].Value = arg
		}
		var result datum.NullableDatum
		err = pgerror.Recover(func() { result.Value = fmgr.CallFunction(fc) })
		result.IsNull = fc.IsNull
		return result, err
	}
	t.Fatalf("no catalog entry for %s", src)
	return datum.NullableDatum{}, nil
}

func TestBooleanScalars(t *testing.T) {
	t.Parallel()
	for _, a := range []bool{false, true} {
		for _, b := range []bool{false, true} {
			for src, want := range map[string]bool{
				"booleq": a == b, "boolne": a != b, "boollt": !a && b,
				"boolle": !a || b, "boolgt": a && !b, "boolge": a || !b,
				"booland_statefunc": a && b, "boolor_statefunc": a || b,
			} {
				result, err := scalarCall(t, src, datum.BoolGetDatum(a), datum.BoolGetDatum(b))
				require.NoError(t, err)
				assert.Equal(t, want, datum.DatumGetBool(result.Value), "%s(%v,%v)", src, a, b)
			}
		}
		cast, err := scalarCall(t, "bool_int4", datum.BoolGetDatum(a))
		require.NoError(t, err)
		want := int32(0)
		if a {
			want = 1
		}
		assert.Equal(t, want, datum.DatumGetInt32(cast.Value))
		for src, want := range map[string]string{"boolout": "f", "booltext": "false"} {
			if a {
				want = "true"
				if src == "boolout" {
					want = "t"
				}
			}
			result, err := scalarCall(t, src, datum.BoolGetDatum(a))
			require.NoError(t, err)
			assert.Equal(t, want, string(datum.DatumGetBytes(result.Value)))
		}
	}
}

func FuzzIntegerTextRoundTrip(f *testing.F) {
	for _, n := range []int64{math.MinInt64, math.MaxInt64, math.MinInt32, math.MaxInt32, math.MinInt16, math.MaxInt16, -1, 0, 1} {
		f.Add(n)
	}
	f.Fuzz(func(t *testing.T, n int64) {
		for _, typ := range []pgoid.Oid{pgoid.INT2OID, pgoid.INT4OID, pgoid.INT8OID} {
			value := integerDatum(typ, n)
			prefix := pgcatalog.TypeByOid(typ).Name
			out, err := scalarCall(t, prefix+"out", value)
			require.NoError(t, err)
			require.Equal(t, strconv.FormatInt(datum.DatumGetInt64(value), 10), string(datum.DatumGetBytes(out.Value)))
			in, err := scalarCall(t, prefix+"in", out.Value)
			require.NoError(t, err)
			require.Equal(t, value, in.Value)
		}
	})
}
