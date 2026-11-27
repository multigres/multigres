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

package pgeval

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser/datum"
	"github.com/multigres/multigres/go/common/parser/pgoid"
	"github.com/multigres/multigres/go/common/pghash"
	"github.com/multigres/multigres/go/common/shardindex"
)

// hashGoldenPath is the hash shard index's golden vectors, which its unit
// tests pin the mapping with.
const hashGoldenPath = "../../../common/shardindex/testdata/hash_golden.json"

// hashGolden is one golden vector: an integer, the width in bytes of the
// column type it is hashed as, and the routing identifier in hex.
type hashGolden struct {
	Value     int64  `json:"value"`
	Width     int    `json:"width"`
	RoutingID string `json:"routing_id"`
}

// compareShardIndexHash checks the hash shard index against the server. The
// query is the documented SQL equivalent of the index, so this also proves
// that a routing identifier can be recomputed on any shard.
//
// Every golden vector must match both the server and the local index. With
// UPDATE_SHARDINDEX_GOLDEN=1 the vectors' routing identifiers are instead
// rewritten from the server's answers; that is how the file was generated, and
// since the mapping is permanent it should never be needed again for existing
// rows, only when adding values. Random values of each width are compared too.
func compareShardIndexHash(t *testing.T, ctx context.Context, conn *pgx.Conn) {
	index, err := shardindex.New(shardindex.HashType, "hash", nil)
	require.NoError(t, err)
	hashing := index.(shardindex.Hashing)

	// server returns PostgreSQL's routing identifier of value as the type of
	// the given width, after checking that it equals the int8 form.
	server := func(value int64, width int) []byte {
		t.Helper()
		typ := fmt.Sprintf("int%d", width)
		var narrow, wide []byte
		var upperHalf bool
		require.NoError(t, conn.QueryRow(ctx,
			"SELECT int8send(hash"+typ+"extended($1::"+typ+", $2::int8)), int8send(hashint8extended($1::"+typ+"::int8, $2::int8)),"+
				` int8send(hashint8extended($1::`+typ+`::int8, $2::int8)) >= '\x80'::bytea`,
			value, int64(shardindex.HashSeed)).Scan(&narrow, &wide, &upperHalf))
		require.Equal(t, wide, narrow, "%s %d", typ, value)
		// A key range is a bytea comparison on the server, as the migrator
		// will filter rows: 80-inf owns exactly the identifiers from 0x80 up.
		require.Equal(t, wide[0] >= 0x80, upperHalf, "%s %d", typ, value)
		return wide
	}
	local := func(value int64, width int) []byte {
		t.Helper()
		v := shardindex.Value{Datum: datum.Int64GetDatum(value)}
		switch width {
		case 2:
			v.Type = pgoid.INT2OID
		case 4:
			v.Type = pgoid.INT4OID
		case 8:
			v.Type = pgoid.INT8OID
		default:
			t.Fatalf("unknown width %d", width)
		}
		id, err := hashing.Hash(v)
		require.NoError(t, err)
		return id
	}

	raw, err := os.ReadFile(hashGoldenPath)
	require.NoError(t, err)
	var golden []hashGolden
	require.NoError(t, json.Unmarshal(raw, &golden))
	require.NotEmpty(t, golden)

	if os.Getenv("UPDATE_SHARDINDEX_GOLDEN") == "1" {
		objects := make([][]string, len(golden))
		for i, g := range golden {
			objects[i] = []string{
				fmt.Sprintf(`"value": %d`, g.Value),
				fmt.Sprintf(`"width": %d`, g.Width),
				fmt.Sprintf(`"routing_id": "%s"`, hex.EncodeToString(server(g.Value, g.Width))),
			}
		}
		writeGoldenObjects(t, hashGoldenPath, objects)
		return
	}

	for _, g := range golden {
		want := hex.EncodeToString(server(g.Value, g.Width))
		require.Equal(t, want, g.RoutingID, "golden vector for int%d %d", g.Width, g.Value)
		require.Equal(t, want, hex.EncodeToString(local(g.Value, g.Width)), "int%d %d", g.Width, g.Value)
	}

	rng := rand.New(rand.NewPCG(17, 6))
	const random = 500
	for range random {
		n := rng.Uint64()
		for width, value := range map[int]int64{2: int64(int16(n)), 4: int64(int32(n)), 8: int64(n)} {
			require.Equal(t, server(value, width), local(value, width), "int%d %d", width, value)
		}
	}
	t.Logf("Compared %d golden vectors and %d random values with PostgreSQL", len(golden), 3*random)
}

// writeGoldenObjects writes a JSON array of flat objects, each given as its
// members, the way Prettier formats it, which CI checks: one object per line,
// broken into one member per line when the line, with its trailing comma,
// would pass 80 columns.
func writeGoldenObjects(t *testing.T, path string, objects [][]string) {
	t.Helper()
	lines := make([]string, len(objects))
	for i, members := range objects {
		lines[i] = "  { " + strings.Join(members, ", ") + " }"
		width := len(lines[i])
		if i < len(objects)-1 {
			width++
		}
		if width > 80 {
			lines[i] = "  {\n    " + strings.Join(members, ",\n    ") + "\n  }"
		}
	}
	require.NoError(t, os.WriteFile(path, []byte("[\n"+strings.Join(lines, ",\n")+"\n]\n"), 0o644))
	t.Logf("Rewrote %d golden vectors in %s from the server", len(objects), path)
}

const (
	// hashBytesGoldenPath is the hash shard index's golden vectors for the
	// types hashed as a byte string.
	hashBytesGoldenPath = "../../../common/shardindex/testdata/hash_golden_bytes.json"
	// bytesExtendedGoldenPath is the golden vectors of pghash.BytesExtended.
	bytesExtendedGoldenPath = "../../../common/pghash/testdata/bytes_extended_golden.json"
)

// hashBytesGolden is one golden vector of hashBytesGoldenPath: a type name,
// the value's bytes in hex (see shardindex.Value for what they are for each
// type), and the routing identifier in hex.
type hashBytesGolden struct {
	Type      string `json:"type"`
	ValueHex  string `json:"value_hex"`
	RoutingID string `json:"routing_id"`
}

// bytesExtendedGolden is one golden vector of bytesExtendedGoldenPath: a key
// in hex, a seed, and hash_bytes_extended(key, seed) as 8 bytes of hex.
type bytesExtendedGolden struct {
	Key  string `json:"input_hex"`
	Seed int64  `json:"seed"`
	Hash string `json:"hash"`
}

// hashBytesTypes is, for each type of hashBytesGoldenPath, how the server
// computes a routing identifier from the value's bytes ($1, a bytea) and the
// seed ($2). value rebuilds the value of that type, so that the server sees
// exactly those bytes, and hash is the documented SQL form applied to it. The
// string types are given the "C" collation, so that the comparison does not
// depend on the test database's locale: every deterministic collation hashes
// the same bytes.
var hashBytesTypes = map[string]struct {
	oid    pgoid.Oid
	pgName string
	value  string
	hash   string
}{
	"text":    {pgoid.TEXTOID, "text", `convert_from($1::bytea, 'UTF8') COLLATE "C"`, "hashtextextended(%s, $2::int8)"},
	"varchar": {pgoid.VARCHAROID, "character varying", `convert_from($1::bytea, 'UTF8')::varchar COLLATE "C"`, "hashtextextended(%s, $2::int8)"},
	"bpchar":  {pgoid.BPCHAROID, "character", `convert_from($1::bytea, 'UTF8')::bpchar COLLATE "C"`, "hashbpcharextended(%s, $2::int8)"},
	"uuid":    {pgoid.UUIDOID, "uuid", `encode($1::bytea, 'hex')::uuid`, "uuid_hash_extended(%s, $2::int8)"},
	"bytea":   {pgoid.BYTEAOID, "bytea", `$1::bytea`, "hash_record_extended(ROW(%s), $2::int8)"},
}

// compareShardIndexHashBytes is compareShardIndexHash for the types the hash
// shard index hashes as a byte string: text, varchar, bpchar, uuid and bytea.
// It also checks pghash.BytesExtended, the primitive under them, against the
// server with seeds other than the index's.
//
// With UPDATE_SHARDINDEX_GOLDEN=1 both golden files are rewritten from the
// server's answers instead. To add a vector, add its input with an empty
// result, and run with that variable set.
func compareShardIndexHashBytes(t *testing.T, ctx context.Context, conn *pgx.Conn) {
	index, err := shardindex.New(shardindex.HashType, "hash", nil)
	require.NoError(t, err)
	hashing := index.(shardindex.Hashing)
	update := os.Getenv("UPDATE_SHARDINDEX_GOLDEN") == "1"

	// The text values are sent as their UTF-8 bytes, which are the server
	// encoding's bytes only in a UTF8 database.
	var encoding string
	require.NoError(t, conn.QueryRow(ctx, "SHOW server_encoding").Scan(&encoding))
	require.Equal(t, "UTF8", encoding)

	server := func(typ string, value []byte) []byte {
		t.Helper()
		spec, ok := hashBytesTypes[typ]
		require.True(t, ok, "unknown type %q", typ)
		id := "int8send(" + fmt.Sprintf(spec.hash, spec.value) + ")"
		if value == nil {
			// pgx sends a nil slice as NULL; this is the empty value.
			value = []byte{}
		}
		var got []byte
		var upperHalf bool
		var pgType string
		require.NoError(t, conn.QueryRow(ctx,
			"SELECT "+id+", "+id+` >= '\x80'::bytea, pg_typeof(`+spec.value+")::text",
			value, int64(shardindex.HashSeed)).Scan(&got, &upperHalf, &pgType), "%s %x", typ, value)
		require.Equal(t, spec.pgName, pgType)
		require.Len(t, got, 8)
		// A key range is a bytea comparison on the server, as the migrator
		// will filter rows: 80-inf owns exactly the identifiers from 0x80 up.
		require.Equal(t, got[0] >= 0x80, upperHalf, "%s %x", typ, value)
		return got
	}
	local := func(typ string, value []byte) []byte {
		t.Helper()
		id, err := hashing.Hash(shardindex.Value{Type: hashBytesTypes[typ].oid, Datum: datum.BytesGetDatum(value)})
		require.NoError(t, err)
		return id
	}

	// serverBytesExtended is hash_bytes_extended(key, seed) on the server.
	// bytea's hashvarlenaextended cannot be called from SQL, so it is taken
	// from the hash of the one-column row holding the key, which is that
	// column's hash unchanged, and, for a key that is valid text, from
	// hashtextextended as well: the two agreeing is the server's own
	// statement that the row adds nothing.
	serverBytesExtended := func(key []byte, seed int64) uint64 {
		t.Helper()
		var row []byte
		require.NoError(t, conn.QueryRow(ctx,
			"SELECT int8send(hash_record_extended(ROW($1::bytea), $2::int8))", key, seed).Scan(&row))
		hash := binary.BigEndian.Uint64(row)
		if utf8.Valid(key) && !bytes.Contains(key, []byte{0}) {
			var text []byte
			require.NoError(t, conn.QueryRow(ctx,
				`SELECT int8send(hashtextextended(convert_from($1::bytea, 'UTF8') COLLATE "C", $2::int8))`, key, seed).Scan(&text))
			require.Equal(t, hash, binary.BigEndian.Uint64(text), "key %x seed %d", key, seed)
		}
		return hash
	}

	var golden []hashBytesGolden
	raw, err := os.ReadFile(hashBytesGoldenPath)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &golden))
	require.NotEmpty(t, golden)
	var primitive []bytesExtendedGolden
	raw, err = os.ReadFile(bytesExtendedGoldenPath)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &primitive))
	require.NotEmpty(t, primitive)

	if update {
		objects := make([][]string, len(golden))
		for i, g := range golden {
			value, err := hex.DecodeString(g.ValueHex)
			require.NoError(t, err)
			objects[i] = []string{
				`"type": ` + strconv.Quote(g.Type),
				`"value_hex": "` + g.ValueHex + `"`,
				`"routing_id": "` + hex.EncodeToString(server(g.Type, value)) + `"`,
			}
		}
		writeGoldenObjects(t, hashBytesGoldenPath, objects)

		objects = make([][]string, len(primitive))
		for i, g := range primitive {
			key, err := hex.DecodeString(g.Key)
			require.NoError(t, err)
			objects[i] = []string{
				`"input_hex": "` + g.Key + `"`,
				`"seed": ` + strconv.FormatInt(g.Seed, 10),
				fmt.Sprintf(`"hash": "%016x"`, serverBytesExtended(key, g.Seed)),
			}
		}
		writeGoldenObjects(t, bytesExtendedGoldenPath, objects)
		return
	}

	for _, g := range golden {
		value, err := hex.DecodeString(g.ValueHex)
		require.NoError(t, err)
		want := hex.EncodeToString(server(g.Type, value))
		require.Equal(t, want, g.RoutingID, "golden vector for %s %s", g.Type, g.ValueHex)
		require.Equal(t, want, hex.EncodeToString(local(g.Type, value)), "%s %s", g.Type, g.ValueHex)
	}
	for _, g := range primitive {
		key, err := hex.DecodeString(g.Key)
		require.NoError(t, err)
		want := serverBytesExtended(key, g.Seed)
		require.Equal(t, fmt.Sprintf("%016x", want), g.Hash, "golden vector for key %s seed %d", g.Key, g.Seed)
		require.Equal(t, want, pghash.BytesExtended(key, uint64(g.Seed)), "key %s seed %d", g.Key, g.Seed)
	}

	rng := rand.New(rand.NewPCG(22, 50))
	randomBytes := func(maxLen int) []byte {
		b := make([]byte, rng.IntN(maxLen+1))
		for i := range b {
			b[i] = byte(rng.Uint32())
		}
		return b
	}
	// randomText is valid UTF-8 with no NUL, which text cannot hold: runes
	// of every encoded length.
	randomText := func() []byte {
		var b []byte
		for range rng.IntN(40) {
			var r rune
			switch rng.IntN(4) {
			case 0:
				r = rune(1 + rng.IntN(0x7F))
			case 1:
				r = rune(0x80 + rng.IntN(0x800-0x80))
			case 2:
				r = rune(0x800 + rng.IntN(0xD800-0x800))
			default:
				r = rune(0x10000 + rng.IntN(0x110000-0x10000))
			}
			b = utf8.AppendRune(b, r)
		}
		return b
	}
	const random = 300
	compared := 0
	check := func(typ string, value []byte) {
		t.Helper()
		compared++
		require.Equal(t, server(typ, value), local(typ, value), "%s %x", typ, value)
	}
	for range random {
		text := randomText()
		check("text", text)
		check("varchar", text)
		check("bpchar", text)
		// Padding does not change a bpchar's identifier.
		padded := append(bytes.Clone(text), strings.Repeat(" ", 1+rng.IntN(4))...)
		check("bpchar", padded)
		check("text", padded)
		check("bytea", randomBytes(64))
		uuid := make([]byte, 16)
		for i := range uuid {
			uuid[i] = byte(rng.Uint32())
		}
		check("uuid", uuid)

		key, seed := randomBytes(64), int64(rng.Uint64())
		require.Equal(t, serverBytesExtended(key, seed), pghash.BytesExtended(key, uint64(seed)), "key %x seed %d", key, seed)
	}
	t.Logf("Compared %d golden vectors, %d random values, and %d golden and %d random keys of BytesExtended with PostgreSQL",
		len(golden), compared, len(primitive), random)
}
