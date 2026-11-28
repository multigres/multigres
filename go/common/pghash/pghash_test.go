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
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// bytesExtendedGolden is one golden vector of
// testdata/bytes_extended_golden.json: a key in hex, a seed, and
// hash_bytes_extended(key, seed) as 8 bytes of hex.
//
// The hashes come from PostgreSQL 17.6 on a little-endian server, not from
// this package. No SQL function hashes arbitrary bytes directly, so the server
// computes them as
//
//	hash_record_extended(ROW(<key>::bytea), <seed>)
//
// and confirms hashtextextended(<key as text> COLLATE "C", <seed>) for every
// key that is valid text. They were written by the opt-in differential suite,
// which also re-checks them against the server on every run:
//
//	RUN_PGEVAL_DIFFERENTIAL=1 UPDATE_SHARDINDEX_GOLDEN=1 \
//	  /mt-dev integration pgeval TestScalarsAgainstPostgres/ShardIndexHash
//
// Shard indexes place rows by these values. A failure here means the hash
// changed, which must never ship: fix the code, never the file.
type bytesExtendedGolden struct {
	Key  string `json:"input_hex"`
	Seed int64  `json:"seed"`
	Hash string `json:"hash"`
}

const partitionSeed = 0x7A5B22367996DCFD

func TestBytesExtended_Golden(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile("testdata/bytes_extended_golden.json")
	require.NoError(t, err)
	var golden []bytesExtendedGolden
	require.NoError(t, json.Unmarshal(raw, &golden))

	// covered is the key lengths seen with each seed, and which of them had
	// a byte with the high bit set.
	type cover struct{ any, high map[int]bool }
	covered := map[uint64]cover{}
	for _, g := range golden {
		key, err := hex.DecodeString(g.Key)
		require.NoError(t, err)
		seed := uint64(g.Seed)
		assert.Equal(t, g.Hash, fmt.Sprintf("%016x", BytesExtended(key, seed)), "key %s seed %d", g.Key, g.Seed)

		if covered[seed].any == nil {
			covered[seed] = cover{map[int]bool{}, map[int]bool{}}
		}
		covered[seed].any[len(key)] = true
		for _, b := range key {
			if b >= 0x80 {
				covered[seed].high[len(key)] = true
			}
		}
	}
	// The file must keep covering, without a seed, with one and with the
	// shard indexes' seed, every tail length after zero to three 12-byte
	// blocks, and bytes that a signed char would sign-extend at each of them.
	for _, seed := range []uint64{0, 1, partitionSeed} {
		require.Contains(t, covered, seed)
		for n := 0; n <= 40; n++ {
			assert.True(t, covered[seed].any[n], "no golden vector of length %d with seed %d", n, seed)
			if n > 0 {
				assert.True(t, covered[seed].high[n], "no golden vector of length %d with a high byte and seed %d", n, seed)
			}
		}
	}
}

// A uint32 hashes as its four bytes in little-endian order, which ties
// BytesExtended to the integer hashes PostgreSQL pins through Uint32Extended.
func TestBytesExtended_MatchesUint32Extended(t *testing.T) {
	t.Parallel()
	for _, seed := range []uint64{0, 1, math.MaxUint64, partitionSeed} {
		for _, key := range []uint32{0, 1, 0x80, 0xFF000000, 0x01020304, math.MaxUint32} {
			assert.Equal(t, Uint32Extended(key, seed), BytesExtended(binary.LittleEndian.AppendUint32(nil, key), seed),
				"key %#x seed %d", key, seed)
		}
	}
}

func TestBytesExtended_Allocations(t *testing.T) {
	key := make([]byte, 100)
	var sink uint64
	assert.Zero(t, testing.AllocsPerRun(100, func() { sink += BytesExtended(key, partitionSeed) }))
	_ = sink
}

func BenchmarkBytesExtended(b *testing.B) {
	for _, size := range []int{8, 16, 36, 256} {
		key := make([]byte, size)
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ReportAllocs()
			var sink uint64
			for b.Loop() {
				sink += BytesExtended(key, partitionSeed)
			}
			_ = sink
		})
	}
}
