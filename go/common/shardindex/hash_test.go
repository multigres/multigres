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

package shardindex

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/datum"
	"github.com/multigres/multigres/go/common/parser/pgoid"
	"github.com/multigres/multigres/go/common/pghash"
)

// hashGolden is one golden vector of testdata/hash_golden.json: an integer,
// the width in bytes of the column type it is hashed as, and the routing
// identifier in hex.
//
// The routing identifiers come from PostgreSQL 17.6, not from this package:
//
//	SELECT int8send(hashint<width>extended(<value>::int<width>, 8816678312871386365))
//
// They were written by the opt-in differential suite, which also re-checks
// them against the server on every run:
//
//	RUN_PGEVAL_DIFFERENTIAL=1 UPDATE_SHARDINDEX_GOLDEN=1 \
//	  /mt-dev integration pgeval TestScalarsAgainstPostgres/ShardIndexHash
//
// Rows are placed by these values. A failure here means the mapping changed,
// which must never ship: fix the code, never the file. Only add vectors.
type hashGolden struct {
	Value     int64  `json:"value"`
	Width     int    `json:"width"`
	RoutingID string `json:"routing_id"`
}

func loadHashGolden(t *testing.T) []hashGolden {
	t.Helper()
	raw, err := os.ReadFile("testdata/hash_golden.json")
	require.NoError(t, err)
	var golden []hashGolden
	require.NoError(t, json.Unmarshal(raw, &golden))
	return golden
}

func newHash(t *testing.T) *hashIndex {
	t.Helper()
	index, err := New(HashType, "by_id", nil)
	require.NoError(t, err)
	return index.(*hashIndex)
}

// intValue is n as a non-null value of the integer type width bytes wide.
func intValue(t *testing.T, n int64, width int) Value {
	t.Helper()
	switch width {
	case 2:
		require.Equal(t, int64(int16(n)), n)
		return Value{Type: pgoid.INT2OID, Datum: datum.Int16GetDatum(int16(n))}
	case 4:
		require.Equal(t, int64(int32(n)), n)
		return Value{Type: pgoid.INT4OID, Datum: datum.Int32GetDatum(int32(n))}
	case 8:
		return Value{Type: pgoid.INT8OID, Datum: datum.Int64GetDatum(n)}
	}
	t.Fatalf("unknown width %d", width)
	return Value{}
}

func requireSQLState(t *testing.T, err error, state string) {
	t.Helper()
	require.Error(t, err)
	var diag *mterrors.PgDiagnostic
	require.ErrorAs(t, err, &diag)
	assert.Equal(t, state, diag.Code)
}

func TestHash_Golden(t *testing.T) {
	t.Parallel()
	h := newHash(t)
	golden := loadHashGolden(t)

	// The file must keep covering the edges of every width, values just
	// outside each narrower width, and each narrow value at every wider width.
	covered := map[int]map[int64]bool{2: {}, 4: {}, 8: {}}
	for _, g := range golden {
		covered[g.Width][g.Value] = true
	}
	for width, values := range map[int][]int64{
		2: {0, 1, -1, math.MinInt16, math.MaxInt16},
		4: {0, 1, -1, math.MinInt16, math.MaxInt16, math.MinInt16 - 1, math.MaxInt16 + 1, math.MinInt32, math.MaxInt32},
		8: {
			0, 1, -1, math.MinInt16, math.MaxInt16, math.MinInt16 - 1, math.MaxInt16 + 1,
			math.MinInt32, math.MaxInt32, math.MinInt32 - 1, math.MaxInt32 + 1, math.MinInt64, math.MaxInt64,
		},
	} {
		for _, value := range values {
			assert.True(t, covered[width][value], "no golden vector for int%d %d", width, value)
		}
	}

	values := make([]Value, len(golden))
	ids := make([][]byte, len(golden))
	byValue := map[int64]string{}
	for i, g := range golden {
		want, err := hex.DecodeString(g.RoutingID)
		require.NoError(t, err)
		require.Len(t, want, 8, "golden vector %d", i)
		values[i], ids[i] = intValue(t, g.Value, g.Width), want

		got, err := h.Hash(values[i])
		require.NoError(t, err)
		assert.Equal(t, g.RoutingID, hex.EncodeToString(got), "Hash of %d as a %d-byte integer", g.Value, g.Width)

		// Equal integers have one routing identifier, whatever their width.
		if previous, ok := byValue[g.Value]; ok {
			assert.Equal(t, previous, g.RoutingID, "golden vectors for %d disagree across widths", g.Value)
		}
		byValue[g.Value] = g.RoutingID
	}

	destinations, err := h.Map(context.Background(), nil, values)
	require.NoError(t, err)
	require.Len(t, destinations, len(golden))
	for i, d := range destinations {
		assert.Equal(t, KindRoutingID, d.Kind())
		assert.Equal(t, ids[i], d.RoutingID(), "Map of %d", golden[i].Value)
	}

	verified, err := h.Verify(context.Background(), nil, values, ids)
	require.NoError(t, err)
	for i, ok := range verified {
		assert.True(t, ok, "Verify of %d", golden[i].Value)
	}
}

func TestHash_Seed(t *testing.T) {
	t.Parallel()
	// PostgreSQL's HASH_PARTITION_SEED, and the int8 literal the docs and the
	// SQL equivalent spell it as.
	assert.Equal(t, uint64(0x7A5B22367996DCFD), HashSeed)
	assert.Equal(t, int64(8816678312871386365), int64(HashSeed))
}

func TestHash_Encoding(t *testing.T) {
	t.Parallel()
	h := newHash(t)
	rng := rand.New(rand.NewPCG(22, 22))
	hash := func(n int64) (uint64, []byte) {
		id, err := h.Hash(intValue(t, n, 8))
		require.NoError(t, err)
		require.Len(t, id, 8)
		// The identifier is the 64-bit hash, big-endian: it round-trips.
		want := pghash.Int8Extended(n, HashSeed)
		require.Equal(t, want, binary.BigEndian.Uint64(id))
		return want, id
	}
	for range 2000 {
		a, aID := hash(int64(rng.Uint64()))
		b, bID := hash(int64(rng.Uint64()))
		// Byte order is the unsigned numeric order of the hash, which is
		// what lets a key range of byte strings own a range of hashes.
		switch {
		case a < b:
			assert.Negative(t, bytes.Compare(aID, bID))
		case a > b:
			assert.Positive(t, bytes.Compare(aID, bID))
		default:
			assert.Equal(t, aID, bID)
		}
	}
}

func TestHash_WidthsAgree(t *testing.T) {
	t.Parallel()
	h := newHash(t)
	rng := rand.New(rand.NewPCG(2, 4))
	samples := []int64{0, 1, -1, math.MinInt16, math.MaxInt16, math.MinInt32, math.MaxInt32}
	for range 500 {
		samples = append(samples, int64(int16(rng.Uint32())), int64(int32(rng.Uint32())))
	}
	for _, n := range samples {
		wide, err := h.Hash(intValue(t, n, 8))
		require.NoError(t, err)
		for _, width := range []int{2, 4} {
			if width == 2 && n != int64(int16(n)) {
				continue
			}
			narrow, err := h.Hash(intValue(t, n, width))
			require.NoError(t, err)
			assert.Equal(t, wide, narrow, "%d as a %d-byte integer", n, width)
		}
	}
}

// A datum of a narrow type is read at its own width and sign-extended, so
// stray high bits in the datum cannot change the routing identifier.
func TestHash_SignExtendsNarrowDatums(t *testing.T) {
	t.Parallel()
	h := newHash(t)
	want, err := h.Hash(intValue(t, -1, 8))
	require.NoError(t, err)
	for _, v := range []Value{
		{Type: pgoid.INT2OID, Datum: datum.UInt64GetDatum(0xFFFF)},
		{Type: pgoid.INT4OID, Datum: datum.UInt64GetDatum(0xFFFFFFFF)},
		{Type: pgoid.INT2OID, Datum: datum.UInt64GetDatum(0xABCD_0000_FFFF)},
	} {
		got, err := h.Hash(v)
		require.NoError(t, err)
		assert.Equal(t, want, got)
	}
}

func TestHash_Properties(t *testing.T) {
	t.Parallel()
	h := newHash(t)
	assert.Equal(t, "by_id", h.Name())
	assert.Equal(t, "hash", h.Type())
	assert.Equal(t, 1, h.Cost())
	assert.True(t, h.IsUnique())

	for _, typ := range []pgoid.Oid{pgoid.INT2OID, pgoid.INT4OID, pgoid.INT8OID} {
		assert.True(t, h.AcceptsType(typ), "type %d", typ)
	}
	for _, typ := range []pgoid.Oid{pgoid.TEXTOID, pgoid.UUIDOID, pgoid.BYTEAOID, pgoid.BOOLOID, pgoid.OIDOID, pgoid.NUMERICOID, pgoid.InvalidOid} {
		assert.False(t, h.AcceptsType(typ), "type %d", typ)
	}
}

func TestHash_Errors(t *testing.T) {
	t.Parallel()
	h := newHash(t)
	ctx := context.Background()
	good := intValue(t, 7, 8)

	for name, tc := range map[string]struct {
		value Value
		state string
	}{
		"text":    {Value{Type: pgoid.TEXTOID, Datum: datum.BytesGetDatum([]byte("7"))}, mterrors.PgSSFeatureNotSupported},
		"bool":    {Value{Type: pgoid.BOOLOID, Datum: datum.BoolGetDatum(true)}, mterrors.PgSSFeatureNotSupported},
		"oid":     {Value{Type: pgoid.OIDOID, Datum: datum.ObjectIdGetDatum(7)}, mterrors.PgSSFeatureNotSupported},
		"no type": {Value{Datum: datum.Int64GetDatum(7)}, mterrors.PgSSFeatureNotSupported},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := h.Hash(tc.value)
			requireSQLState(t, err, tc.state)
			assert.Contains(t, err.Error(), `"by_id"`)

			// One bad value fails the whole batch.
			_, err = h.Map(ctx, nil, []Value{good, tc.value})
			requireSQLState(t, err, tc.state)
			_, err = h.Verify(ctx, nil, []Value{good, tc.value}, [][]byte{nil, nil})
			requireSQLState(t, err, tc.state)
		})
	}

	// A NULL has no routing identifier, whatever its type.
	for _, typ := range []pgoid.Oid{pgoid.INT8OID, pgoid.TEXTOID} {
		_, err := h.Hash(Value{Type: typ, IsNull: true})
		requireSQLState(t, err, mterrors.PgSSNullValueNotAllowed)
		assert.Contains(t, err.Error(), `"by_id"`)
	}

	_, err := New(HashType, "by_id", map[string]string{"seed": "1"})
	requireSQLState(t, err, mterrors.PgSSInvalidParameterValue)
	assert.Contains(t, err.Error(), `"seed"`)
	// An empty map is no parameters.
	_, err = New(HashType, "by_id", map[string]string{})
	require.NoError(t, err)
}

// The int8 fold makes distinct bigints share a routing identifier, so an
// identifier never identifies a value. These are the pairs the docs quote.
func TestHash_DistinctValuesShareRoutingID(t *testing.T) {
	t.Parallel()
	h := newHash(t)
	for _, tc := range []struct {
		a, b int64
		id   string
	}{
		{1, 4294967296, "52d621058124e6d5"},
		{-1, 4294967295, "ba5fc748095606c2"},
	} {
		a, err := h.Hash(intValue(t, tc.a, 8))
		require.NoError(t, err)
		b, err := h.Hash(intValue(t, tc.b, 8))
		require.NoError(t, err)
		assert.Equal(t, tc.id, hex.EncodeToString(a), "%d", tc.a)
		assert.Equal(t, tc.id, hex.EncodeToString(b), "%d", tc.b)
	}
}

// A NULL is not an error for Map and Verify: col = NULL is true for no row,
// so it maps to no shard, and the other values of the batch still map.
func TestHash_Null(t *testing.T) {
	t.Parallel()
	h := newHash(t)
	ctx := context.Background()
	seven := intValue(t, 7, 8)
	id, err := h.Hash(seven)
	require.NoError(t, err)

	// The NULL's type is not looked at.
	values := []Value{{Type: pgoid.INT8OID, IsNull: true}, seven, {Type: pgoid.TEXTOID, IsNull: true}}
	destinations, err := h.Map(ctx, nil, values)
	require.NoError(t, err)
	require.Len(t, destinations, 3)
	assert.Equal(t, DestinationNone(), destinations[0])
	assert.Equal(t, KindRoutingID, destinations[1].Kind())
	assert.Equal(t, id, destinations[1].RoutingID())
	assert.Equal(t, DestinationNone(), destinations[2])

	verified, err := h.Verify(ctx, nil, values, [][]byte{id, id, nil})
	require.NoError(t, err)
	assert.Equal(t, []bool{false, true, false}, verified)
}

func TestHash_Verify(t *testing.T) {
	t.Parallel()
	h := newHash(t)
	ctx := context.Background()
	seven, err := h.Hash(intValue(t, 7, 4))
	require.NoError(t, err)
	eight, err := h.Hash(intValue(t, 8, 4))
	require.NoError(t, err)

	got, err := h.Verify(ctx, nil,
		[]Value{intValue(t, 7, 2), intValue(t, 7, 8), intValue(t, 7, 8), intValue(t, 7, 8), intValue(t, 7, 8)},
		[][]byte{seven, eight, seven[:7], append(bytes.Clone(seven), 0), nil})
	require.NoError(t, err)
	assert.Equal(t, []bool{true, false, false, false, false}, got)

	_, err = h.Verify(ctx, nil, []Value{intValue(t, 7, 8)}, nil)
	require.Error(t, err)

	got, err = h.Verify(ctx, nil, nil, nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestHash_MapIdentifiersAreIndependent(t *testing.T) {
	t.Parallel()
	h := newHash(t)
	destinations, err := h.Map(context.Background(), nil, []Value{intValue(t, 1, 8), intValue(t, 2, 8)})
	require.NoError(t, err)
	second := bytes.Clone(destinations[1].RoutingID())
	// The identifiers share a backing array; appending to one must not
	// reach into the next.
	_ = append(destinations[0].RoutingID(), 0xFF)
	assert.Equal(t, second, destinations[1].RoutingID())

	empty, err := h.Map(context.Background(), nil, nil)
	require.NoError(t, err)
	assert.Empty(t, empty)
}

// Map and Hash run on the query path: they allocate their result and nothing
// else, however many values are mapped.
func TestHash_Allocations(t *testing.T) {
	h := newHash(t)
	ctx := context.Background()
	values := make([]Value, 64)
	for i := range values {
		values[i] = intValue(t, int64(i)*7919, 8)
	}
	assert.Equal(t, 1.0, testing.AllocsPerRun(100, func() {
		if _, err := h.Hash(values[3]); err != nil {
			t.Fatal(err)
		}
	}), "Hash allocates the identifier only")
	assert.Equal(t, 2.0, testing.AllocsPerRun(100, func() {
		if _, err := h.Map(ctx, nil, values); err != nil {
			t.Fatal(err)
		}
	}), "Map allocates the destinations and one array of identifiers")
}

func BenchmarkHash_Map(b *testing.B) {
	index, err := New(HashType, "by_id", nil)
	require.NoError(b, err)
	h := index.(SingleColumn)
	values := []Value{{Type: pgoid.INT8OID, Datum: datum.Int64GetDatum(42)}}
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := h.Map(ctx, nil, values); err != nil {
			b.Fatal(err)
		}
	}
}
