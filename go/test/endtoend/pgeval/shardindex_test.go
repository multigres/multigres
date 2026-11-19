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
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/parser/datum"
	"github.com/multigres/multigres/go/common/parser/pgoid"
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
		lines := make([]string, len(golden))
		for i, g := range golden {
			lines[i] = fmt.Sprintf(`  { "value": %d, "width": %d, "routing_id": "%s" }`,
				g.Value, g.Width, hex.EncodeToString(server(g.Value, g.Width)))
		}
		require.NoError(t, os.WriteFile(hashGoldenPath, []byte("[\n"+strings.Join(lines, ",\n")+"\n]\n"), 0o644))
		t.Logf("Rewrote %d golden vectors in %s from the server", len(golden), hashGoldenPath)
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
