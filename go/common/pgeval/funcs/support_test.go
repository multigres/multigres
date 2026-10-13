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
	"math/big"
	"math/rand/v2"
	"runtime"
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

func TestBooleanOrdering(t *testing.T) {
	t.Parallel()
	for a := int32(0); a <= 1; a++ {
		for b := int32(0); b <= 1; b++ {
			got, err := scalarCall(t, "btboolcmp", datum.BoolGetDatum(a != 0), datum.BoolGetDatum(b != 0))
			require.NoError(t, err)
			assert.Equal(t, a-b, datum.DatumGetInt32(got.Value))
		}
	}
}

// Mathematical comparison with an unbounded sum, independent of the port's
// overflow detection. Includes every width pair PG supplies and all flag pairs.
func TestInRangeAgainstBigInt(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewPCG(17, 4126))
	for _, proc := range pgcatalog.Procs {
		if !fmgr.IsBuiltinRegistered(proc.Src) || !strings.HasPrefix(proc.Src, "in_range_int") {
			continue
		}
		t.Run(proc.Src, func(t *testing.T) {
			info, err := fmgr.FmgrInfoFor(proc.Oid)
			require.NoError(t, err)
			fc := fmgr.NewFunctionCallInfo(info, 5, pgoid.InvalidOid)
			check := func(a, b, offset int64) {
				t.Helper()
				for i, value := range []int64{a, b, offset} {
					fc.Args[i].Value = integerDatum(proc.ArgTypes[i], value)
				}
				for _, sub := range []bool{false, true} {
					for _, less := range []bool{false, true} {
						fc.Args[3].Value = datum.BoolGetDatum(sub)
						fc.Args[4].Value = datum.BoolGetDatum(less)
						var got datum.Datum
						err := pgerror.Recover(func() { got = fmgr.CallFunction(fc) })
						if offset < 0 {
							var diag *mterrors.PgDiagnostic
							require.ErrorAs(t, err, &diag)
							require.Equal(t, "22013", diag.Code)
							require.Equal(t, "invalid preceding or following size in window function", diag.Message)
							continue
						}
						require.NoError(t, err)
						require.False(t, fc.IsNull)
						sum := big.NewInt(b)
						if sub {
							sum.Sub(sum, big.NewInt(offset))
						} else {
							sum.Add(sum, big.NewInt(offset))
						}
						comparison := big.NewInt(a).Cmp(sum)
						want := (less && comparison <= 0) || (!less && comparison >= 0)
						require.Equal(t, want, datum.DatumGetBool(got), "args: %d %d %d %v %v", a, b, offset, sub, less)
					}
				}
			}
			lo, hi := integerLimits(proc.ArgTypes[0])
			oLo, oHi := integerLimits(proc.ArgTypes[2])
			for _, a := range []int64{lo, lo + 1, -1, 0, 1, hi - 1, hi} {
				for _, b := range []int64{lo, lo + 1, -1, 0, 1, hi - 1, hi} {
					for _, offset := range []int64{oLo, -1, 0, 1, 2, oHi - 1, oHi} {
						check(a, b, offset)
					}
				}
			}
			for range 100 {
				a := datum.DatumGetInt64(integerDatum(proc.ArgTypes[0], int64(rng.Uint64())))
				b := datum.DatumGetInt64(integerDatum(proc.ArgTypes[1], int64(rng.Uint64())))
				offset := datum.DatumGetInt64(integerDatum(proc.ArgTypes[2], int64(rng.Uint64())))
				check(a, b, offset)
			}
		})
	}
}

func TestHashCompatibility(t *testing.T) {
	t.Parallel()
	for _, seed := range []int64{0, 1, -1, 1 << 32, math.MinInt64, math.MaxInt64} {
		for _, n := range integerSamples(pgoid.INT2OID) {
			var previous int64
			for i, typ := range []string{"int2", "int4", "int8"} {
				got, err := scalarCall(t, "hash"+typ+"extended", datum.Int64GetDatum(n), datum.Int64GetDatum(seed))
				require.NoError(t, err)
				value := datum.DatumGetInt64(got.Value)
				if i != 0 {
					assert.Equal(t, previous, value, "%s(%d), seed %d", typ, n, seed)
				}
				previous = value
				if seed == 0 {
					plain, err := scalarCall(t, "hash"+typ, datum.Int64GetDatum(n))
					require.NoError(t, err)
					assert.Equal(t, int32(value), datum.DatumGetInt32(plain.Value))
				}
			}
		}
		// Values outside int4 exercise both halves and sign correction.
		for _, pair := range [][2]int64{
			{math.MinInt64, math.MaxInt32},
			{math.MaxInt64, math.MinInt32},
			{1 << 32, 1},
			{-(1 << 32), 0},
			{(1 << 32) + 1, 0},
		} {
			a, err := scalarCall(t, "hashint8extended", datum.Int64GetDatum(pair[0]), datum.Int64GetDatum(seed))
			require.NoError(t, err)
			b, err := scalarCall(t, "hashint4extended", datum.Int32GetDatum(int32(pair[1])), datum.Int64GetDatum(seed))
			require.NoError(t, err)
			assert.Equal(t, a.Value, b.Value, "values %v, seed %d", pair, seed)
		}
		for n := -128; n <= 127; n++ {
			arg := datum.CharGetDatum(int8(n))
			// PG's boolean hash opclass passes normalized bool Datums to
			// these same hashchar entry points, despite their CHAROID signature.
			if n == 0 || n == 1 {
				arg = datum.BoolGetDatum(n != 0)
			}
			a, err := scalarCall(t, "hashcharextended", arg, datum.Int64GetDatum(seed))
			require.NoError(t, err)
			promoted := int32(n)
			if runtime.GOOS == "linux" && runtime.GOARCH == "arm64" {
				promoted = int32(uint8(n)) // Default AArch64 C ABI, unlike Darwin.
			}
			b, err := scalarCall(t, "hashint4extended", datum.Int32GetDatum(promoted), datum.Int64GetDatum(seed))
			require.NoError(t, err)
			assert.Equal(t, a.Value, b.Value)
			if seed == 0 {
				plain, err := scalarCall(t, "hashchar", arg)
				require.NoError(t, err)
				assert.Equal(t, datum.DatumGetInt32(a.Value), datum.DatumGetInt32(plain.Value))
			}
		}
	}
}
