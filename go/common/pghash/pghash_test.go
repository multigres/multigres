// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pghash

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

// The values themselves are checked against PostgreSQL through the callers:
// go/common/pgeval/funcs (hashint*, every width and seed) and the golden
// vectors of go/common/shardindex. This pins the fold they both rely on.
func TestInt8Key(t *testing.T) {
	t.Parallel()
	// An integer that fits in int4 keeps its int4 bit pattern, so hashint8
	// agrees with hashint4 and hashint2 on it.
	for _, n := range []int64{0, 1, -1, 42, -42, math.MinInt16, math.MaxInt16, math.MinInt32, math.MaxInt32} {
		assert.Equal(t, uint32(int32(n)), Int8Key(n), "%d", n)
	}
	// Outside int4, the high half is folded in: inverted for negatives.
	for n, want := range map[int64]uint32{
		math.MaxInt32 + 1:    0x80000000,
		math.MinInt32 - 1:    0x7FFFFFFF,
		1 << 32:              1,
		-(1 << 32):           0,
		1<<32 + 1:            0,
		math.MaxInt64:        0x80000000,
		math.MinInt64:        0x7FFFFFFF,
		0x1234_5678_9ABC_DEF: 0x89ABCDEF ^ 0x01234567,
	} {
		assert.Equal(t, want, Int8Key(n), "%d", n)
	}
}

func TestInt8Extended(t *testing.T) {
	t.Parallel()
	for _, seed := range []uint64{0, 1, math.MaxUint64, 0x7A5B22367996DCFD} {
		for _, n := range []int64{0, 1, -1, math.MinInt64, math.MaxInt64, 1 << 40} {
			assert.Equal(t, Uint32Extended(Int8Key(n), seed), Int8Extended(n, seed))
		}
		// Numbers with the same key collide, exactly as in PostgreSQL.
		assert.Equal(t, Int8Extended(0, seed), Int8Extended(-(1<<32), seed))
	}
	assert.NotEqual(t, Int8Extended(1, 0), Int8Extended(1, 1))
}
