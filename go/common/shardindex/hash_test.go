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
	"strings"
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

	for _, typ := range []pgoid.Oid{
		pgoid.INT2OID, pgoid.INT4OID, pgoid.INT8OID,
		pgoid.TEXTOID, pgoid.VARCHAROID, pgoid.BPCHAROID, pgoid.UUIDOID, pgoid.BYTEAOID,
	} {
		assert.True(t, h.AcceptsType(typ), "type %d", typ)
	}
	for _, typ := range []pgoid.Oid{pgoid.NAMEOID, pgoid.CHAROID, pgoid.BOOLOID, pgoid.OIDOID, pgoid.NUMERICOID, pgoid.InvalidOid} {
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
		"name":    {Value{Type: pgoid.NAMEOID, Datum: datum.BytesGetDatum([]byte("7"))}, mterrors.PgSSFeatureNotSupported},
		"numeric": {Value{Type: pgoid.NUMERICOID, Datum: datum.BytesGetDatum([]byte("7"))}, mterrors.PgSSFeatureNotSupported},
		// A uuid is exactly 16 bytes.
		"short uuid": {Value{Type: pgoid.UUIDOID, Datum: datum.BytesGetDatum(make([]byte, 15))}, mterrors.PgSSInvalidBinaryRepresentation},
		"long uuid":  {Value{Type: pgoid.UUIDOID, Datum: datum.BytesGetDatum(make([]byte, 17))}, mterrors.PgSSInvalidBinaryRepresentation},
		"empty uuid": {Value{Type: pgoid.UUIDOID}, mterrors.PgSSInvalidBinaryRepresentation},
		// The canonical text form is not the payload.
		"text uuid": {Value{Type: pgoid.UUIDOID, Datum: datum.BytesGetDatum([]byte("123e4567-e89b-12d3-a456-426614174000"))}, mterrors.PgSSInvalidBinaryRepresentation},
		"bool":      {Value{Type: pgoid.BOOLOID, Datum: datum.BoolGetDatum(true)}, mterrors.PgSSFeatureNotSupported},
		"oid":       {Value{Type: pgoid.OIDOID, Datum: datum.ObjectIdGetDatum(7)}, mterrors.PgSSFeatureNotSupported},
		"no type":   {Value{Datum: datum.Int64GetDatum(7)}, mterrors.PgSSFeatureNotSupported},
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

	// An unsupported type is told which types are supported.
	_, err := h.Hash(Value{Type: pgoid.BOOLOID, Datum: datum.BoolGetDatum(true)})
	var diag *mterrors.PgDiagnostic
	require.ErrorAs(t, err, &diag)
	assert.Equal(t, "It supports smallint, integer, bigint, text, varchar, bpchar, uuid and bytea.", diag.Detail)

	// A NULL has no routing identifier, whatever its type.
	for _, typ := range []pgoid.Oid{pgoid.INT8OID, pgoid.TEXTOID, pgoid.UUIDOID, pgoid.BOOLOID} {
		_, err := h.Hash(Value{Type: typ, IsNull: true})
		requireSQLState(t, err, mterrors.PgSSNullValueNotAllowed)
		assert.Contains(t, err.Error(), `"by_id"`)
	}

	_, err = New(HashType, "by_id", map[string]string{"seed": "1"})
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
	values := []Value{{Type: pgoid.INT8OID, IsNull: true}, seven, {Type: pgoid.BOOLOID, IsNull: true}}
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

	// The same holds for the types hashed as a byte string.
	ids := make([][]byte, 0, 64)
	values = values[:0]
	for i, typ := range []pgoid.Oid{pgoid.TEXTOID, pgoid.VARCHAROID, pgoid.BPCHAROID, pgoid.BYTEAOID, pgoid.UUIDOID} {
		for n := range 12 {
			payload := bytes.Repeat([]byte{byte('a' + i), ' '}, 8+n)
			if typ == pgoid.UUIDOID {
				payload = payload[:16]
			}
			value := Value{Type: typ, Datum: datum.BytesGetDatum(payload)}
			id, err := h.Hash(value)
			require.NoError(t, err)
			values, ids = append(values, value), append(ids, id)
		}
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
	assert.Equal(t, 1.0, testing.AllocsPerRun(100, func() {
		if _, err := h.Verify(ctx, nil, values, ids); err != nil {
			t.Fatal(err)
		}
	}), "Verify allocates its result only")
}

// hashBytesGolden is one golden vector of testdata/hash_golden_bytes.json,
// for the types hashed as a byte string: the type's name, the value's payload
// in hex (what Value.Datum carries for the type), and the routing identifier
// in hex. The payload is in hex so that NUL bytes, trailing spaces and bytes
// that are not UTF-8 are unambiguous. Text payloads are UTF-8.
//
// The routing identifiers come from PostgreSQL 17.6 on a little-endian
// server, not from this package:
//
//	text, varchar   int8send(hashtextextended(<value> COLLATE "C", 8816678312871386365))
//	bpchar          int8send(hashbpcharextended(<value> COLLATE "C", 8816678312871386365))
//	uuid            int8send(uuid_hash_extended(<value>, 8816678312871386365))
//	bytea           int8send(hash_record_extended(ROW(<value>), 8816678312871386365))
//
// They were written, and are re-checked, by the same differential suite as
// hashGolden's, and the same rule holds: fix the code, never the file. To add
// a vector, add its type and value with an empty routing_id and run the suite
// with UPDATE_SHARDINDEX_GOLDEN=1.
type hashBytesGolden struct {
	Type      string `json:"type"`
	ValueHex  string `json:"value_hex"`
	RoutingID string `json:"routing_id"`
}

// hashBytesTypes is the type OID of each type name of hashBytesGolden.
var hashBytesTypes = map[string]pgoid.Oid{
	"text":    pgoid.TEXTOID,
	"varchar": pgoid.VARCHAROID,
	"bpchar":  pgoid.BPCHAROID,
	"uuid":    pgoid.UUIDOID,
	"bytea":   pgoid.BYTEAOID,
}

func bytesValue(typ pgoid.Oid, payload []byte) Value {
	return Value{Type: typ, Datum: datum.BytesGetDatum(payload)}
}

func TestHash_GoldenBytes(t *testing.T) {
	t.Parallel()
	h := newHash(t)
	raw, err := os.ReadFile("testdata/hash_golden_bytes.json")
	require.NoError(t, err)
	var golden []hashBytesGolden
	require.NoError(t, json.Unmarshal(raw, &golden))

	values := make([]Value, len(golden))
	ids := make([][]byte, len(golden))
	// byType is each type's routing identifiers by payload.
	byType := map[string]map[string]string{}
	for i, g := range golden {
		typ, ok := hashBytesTypes[g.Type]
		require.True(t, ok, "golden vector %d has unknown type %q", i, g.Type)
		payload, err := hex.DecodeString(g.ValueHex)
		require.NoError(t, err)
		want, err := hex.DecodeString(g.RoutingID)
		require.NoError(t, err)
		require.Len(t, want, 8, "golden vector %d", i)
		values[i], ids[i] = bytesValue(typ, payload), want

		got, err := h.Hash(values[i])
		require.NoError(t, err)
		assert.Equal(t, g.RoutingID, hex.EncodeToString(got), "Hash of %s %s", g.Type, g.ValueHex)

		if byType[g.Type] == nil {
			byType[g.Type] = map[string]string{}
		}
		byType[g.Type][string(payload)] = g.RoutingID
	}

	destinations, err := h.Map(context.Background(), nil, values)
	require.NoError(t, err)
	require.Len(t, destinations, len(golden))
	for i, d := range destinations {
		assert.Equal(t, KindRoutingID, d.Kind())
		assert.Equal(t, ids[i], d.RoutingID(), "Map of %s %s", golden[i].Type, golden[i].ValueHex)
	}
	verified, err := h.Verify(context.Background(), nil, values, ids)
	require.NoError(t, err)
	for i, ok := range verified {
		assert.True(t, ok, "Verify of %s %s", golden[i].Type, golden[i].ValueHex)
	}

	// The file must keep covering what follows. id fails on a missing vector.
	id := func(typ, payload string) string {
		t.Helper()
		got, ok := byType[typ][payload]
		require.True(t, ok, "no golden vector for %s %q", typ, payload)
		return got
	}
	lengths := func(typ string) map[int]bool {
		seen := map[int]bool{}
		for payload := range byType[typ] {
			seen[len(payload)] = true
		}
		return seen
	}
	// Every tail length of the hash, over two 12-byte blocks, and a long value.
	for _, typ := range []string{"text", "bytea"} {
		seen := lengths(typ)
		for n := 0; n <= 30; n++ {
			assert.True(t, seen[n], "no golden vector for a %s of %d bytes", typ, n)
		}
		assert.True(t, seen[1000], "no golden vector for a long %s", typ)
	}
	// UTF-8 sequences of 2, 3 and 4 bytes.
	for _, s := range []string{"é", "€", "😀"} {
		id("text", s)
	}

	// varchar is text.
	require.GreaterOrEqual(t, len(byType["varchar"]), 5)
	for payload, want := range byType["varchar"] {
		assert.Equal(t, id("text", payload), want, "varchar %q", payload)
	}
	// Trailing spaces count in text and varchar...
	assert.NotEqual(t, id("text", "ab"), id("text", "ab  "))
	assert.NotEqual(t, id("varchar", "ab"), id("varchar", "ab  "))
	// ...and are padding in bpchar, which otherwise hashes as text does.
	// Only trailing spaces are: not leading or inner ones, nor a tab.
	for payload, want := range byType["bpchar"] {
		assert.Equal(t, id("text", strings.TrimRight(payload, " ")), want, "bpchar %q", payload)
	}
	assert.Equal(t, id("bpchar", "ab"), id("bpchar", "ab  "))
	assert.Equal(t, id("bpchar", ""), id("bpchar", "   "))
	assert.Equal(t, id("bpchar", "abcdefghijkl"), id("bpchar", "abcdefghijkl    "))
	assert.NotEqual(t, id("bpchar", "ab"), id("bpchar", " ab"))
	assert.NotEqual(t, id("bpchar", "ab"), id("bpchar", "ab\t"))
	assert.NotEqual(t, id("bpchar", "ab\t"), id("bpchar", "ab \t"))

	id("uuid", string(make([]byte, 16)))
	id("uuid", strings.Repeat("\xff", 16))
	require.GreaterOrEqual(t, len(byType["uuid"]), 8)

	// A bytea hashes as the same bytes do as text. Both sides of this are
	// PostgreSQL's answers: hash_record_extended(ROW(b)) for the bytea,
	// hashtextextended for the same bytes as text.
	shared := 0
	for payload, want := range byType["bytea"] {
		text, ok := byType["text"][payload]
		if !ok {
			continue
		}
		shared++
		assert.Equal(t, text, want, "bytea %q", payload)
	}
	assert.GreaterOrEqual(t, shared, 5, "golden vectors shared by bytea and text")
	// NUL bytes are data, embedded or trailing, and bytes need not be UTF-8.
	assert.NotEqual(t, id("bytea", ""), id("bytea", "\x00"))
	assert.NotEqual(t, id("bytea", "\x00"), id("bytea", "\x00\x00"))
	assert.NotEqual(t, id("bytea", "ab"), id("bytea", "ab\x00"))
	assert.NotEqual(t, id("bytea", "ab\x00"), id("bytea", "ab\x00\x00"))
	id("bytea", "a\x00b")
	id("bytea", "\xc3\x28")
	id("bytea", "\xff\xfe\xfd")
	// Unlike bpchar, trailing spaces are data.
	assert.NotEqual(t, id("bytea", "ab"), id("bytea", "ab  "))
}

// An empty text or bytea is a value, not a NULL: BytesGetDatum of an empty
// slice is the zero Datum, whose payload reads back as nil, and that hashes as
// the empty string.
func TestHash_EmptyBytes(t *testing.T) {
	t.Parallel()
	h := newHash(t)
	ctx := context.Background()
	for typ, want := range map[pgoid.Oid]uint64{
		pgoid.TEXTOID:    pghash.BytesExtended(nil, HashSeed),
		pgoid.VARCHAROID: pghash.BytesExtended(nil, HashSeed),
		pgoid.BPCHAROID:  pghash.BytesExtended(nil, HashSeed),
		pgoid.BYTEAOID:   pghash.BytesExtended(nil, HashSeed),
	} {
		wantID := binary.BigEndian.AppendUint64(nil, want)
		for _, value := range []Value{{Type: typ}, bytesValue(typ, nil), bytesValue(typ, []byte{})} {
			require.Equal(t, datum.Datum{}, value.Datum)
			got, err := h.Hash(value)
			require.NoError(t, err)
			assert.Equal(t, wantID, got, "type %d", typ)

			destinations, err := h.Map(ctx, nil, []Value{value})
			require.NoError(t, err)
			assert.Equal(t, KindRoutingID, destinations[0].Kind())
			assert.Equal(t, wantID, destinations[0].RoutingID())

			verified, err := h.Verify(ctx, nil, []Value{value}, [][]byte{wantID})
			require.NoError(t, err)
			assert.Equal(t, []bool{true}, verified)
		}
	}
	// A bpchar of spaces only is empty once its padding is removed.
	spaces, err := h.Hash(bytesValue(pgoid.BPCHAROID, []byte("    ")))
	require.NoError(t, err)
	empty, err := h.Hash(bytesValue(pgoid.BPCHAROID, nil))
	require.NoError(t, err)
	assert.Equal(t, empty, spaces)
}

// The identifier depends on the type: the same bytes map differently as text
// and as bpchar. text, varchar, bytea and uuid hash the bytes they are given.
func TestHash_TypeDecidesTheMapping(t *testing.T) {
	t.Parallel()
	h := newHash(t)
	hash := func(typ pgoid.Oid, payload string) uint64 {
		t.Helper()
		id, err := h.Hash(bytesValue(typ, []byte(payload)))
		require.NoError(t, err)
		return binary.BigEndian.Uint64(id)
	}
	text := hash(pgoid.TEXTOID, "ab ")
	assert.Equal(t, pghash.BytesExtended([]byte("ab "), HashSeed), text)
	assert.Equal(t, text, hash(pgoid.VARCHAROID, "ab "))
	assert.Equal(t, pghash.BytesExtended([]byte("ab"), HashSeed), hash(pgoid.BPCHAROID, "ab "))
	assert.Equal(t, text, hash(pgoid.BYTEAOID, "ab "))

	// A uuid is its 16 bytes.
	sixteen := "0123456789abcdef"
	assert.Equal(t, hash(pgoid.TEXTOID, sixteen), hash(pgoid.UUIDOID, sixteen))
	assert.Equal(t, hash(pgoid.UUIDOID, sixteen), hash(pgoid.BYTEAOID, sixteen))

	// The mapping does not modify or retain the payload.
	payload := []byte("ab   ")
	_, err := h.Hash(bytesValue(pgoid.BPCHAROID, payload))
	require.NoError(t, err)
	assert.Equal(t, "ab   ", string(payload))
}

func BenchmarkHash_MapText(b *testing.B) {
	index, err := New(HashType, "by_id", nil)
	require.NoError(b, err)
	h := index.(SingleColumn)
	values := []Value{bytesValue(pgoid.TEXTOID, []byte("customer-0123456789@example.com"))}
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := h.Map(ctx, nil, values); err != nil {
			b.Fatal(err)
		}
	}
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
