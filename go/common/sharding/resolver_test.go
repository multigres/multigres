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

// These tests are worked examples of the logical->physical sharding model
// (docs/sharding/design.md). Each names a scenario from the design doc and
// asserts the routing invariant that makes the model useful — a point query
// hits one cluster, a keyless query scatters, co-located tables agree, a
// reference table is everywhere, and resharding is just a layout edit that
// preserves coverage.
package sharding

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	shardingpb "github.com/multigres/multigres/go/pb/sharding"
)

// --- small builders so the schemas below read like the design doc ------------

func obj(db, schema, table string) *shardingpb.LogicalObject {
	return &shardingpb.LogicalObject{Database: db, Schema: schema, Table: table}
}

// kr builds a keyspace-id KeyRange. nil start = -inf, nil end = +inf.
func kr(start, end []byte) *clustermetadatapb.KeyRange {
	return &clustermetadatapb.KeyRange{Start: start, End: end}
}

func place(kr *clustermetadatapb.KeyRange, cluster string) *shardingpb.Placement {
	return &shardingpb.Placement{KeyRange: kr, Cluster: cluster}
}

func hashGroup(name string, layout ...*shardingpb.Placement) *shardingpb.ShardingGroup {
	return &shardingpb.ShardingGroup{
		Name:     name,
		Function: &shardingpb.ShardingFunction{Kind: &shardingpb.ShardingFunction_Hash{Hash: &shardingpb.HashFunction{}}},
		Layout:   layout,
	}
}

// userVal encodes a shard-key column value the way a client would send it.
func userVal(id int) map[string][]byte {
	return map[string][]byte{"user_id": []byte(strconv.Itoa(id))}
}

// Example 1 — an unsharded database sits wholly on one cluster.
func TestResolve_UnshardedDatabase(t *testing.T) {
	schema := &shardingpb.ShardingSchema{
		DirectPlacements: []*shardingpb.DirectPlacement{
			{Object: obj("db2", "", ""), Clusters: []string{"A"}},
		},
	}

	// Any table in db2 routes to A, with or without a shard key.
	got, err := Resolve(schema, obj("db2", "sc2", "t"), nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"A"}, got)

	got, err = Resolve(schema, obj("db2", "sc3", "t"), userVal(42))
	require.NoError(t, err)
	assert.Equal(t, []string{"A"}, got)
}

// Example 2 — a hash-sharded table: a point query hits one cluster, a keyless
// query scatters across the whole layout.
func TestResolve_HashSharded(t *testing.T) {
	schema := &shardingpb.ShardingSchema{
		ShardingGroups: []*shardingpb.ShardingGroup{
			hashGroup("by_user",
				place(kr(nil, []byte{0x80}), "B"), // -80
				place(kr([]byte{0x80}, nil), "C"), // 80-
			),
		},
		TableBindings: []*shardingpb.TableBinding{
			{Table: obj("db1", "sc1", "t1"), ShardingGroup: "by_user", Columns: []string{"user_id"}},
		},
	}

	// Point query on the shard key -> exactly one cluster.
	for _, id := range []int{1, 42, 200, 999, 12345} {
		got, err := Resolve(schema, obj("db1", "sc1", "t1"), userVal(id))
		require.NoError(t, err)
		require.Len(t, got, 1, "user_id=%d should target a single cluster", id)
		assert.Contains(t, []string{"B", "C"}, got[0])
	}

	// No usable shard key -> scatter across every cluster in the layout.
	got, err := Resolve(schema, obj("db1", "sc1", "t1"), nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"B", "C"}, got)
}

// Example 3 — two tables bound to the SAME group on the SAME key co-locate:
// for any given key they resolve to the same single cluster, so a join on that
// key stays within one cluster.
func TestResolve_CoLocatedTablesShareCluster(t *testing.T) {
	schema := &shardingpb.ShardingSchema{
		ShardingGroups: []*shardingpb.ShardingGroup{
			hashGroup("by_user",
				place(kr(nil, []byte{0x80}), "B"),
				place(kr([]byte{0x80}, nil), "C"),
			),
		},
		TableBindings: []*shardingpb.TableBinding{
			{Table: obj("db1", "sc1", "t1"), ShardingGroup: "by_user", Columns: []string{"user_id"}},
			{Table: obj("db1", "sc1", "t2"), ShardingGroup: "by_user", Columns: []string{"user_id"}},
		},
	}

	for _, id := range []int{1, 42, 200, 999, 12345} {
		t1, err := Resolve(schema, obj("db1", "sc1", "t1"), userVal(id))
		require.NoError(t, err)
		t2, err := Resolve(schema, obj("db1", "sc1", "t2"), userVal(id))
		require.NoError(t, err)
		assert.Equal(t, t1, t2, "co-located tables must agree for user_id=%d", id)
		require.Len(t, t1, 1)
	}
}

// Example 4 — a reference table replicated to every cluster is just a direct
// placement across many clusters; it needs no dedicated concept.
func TestResolve_ReferenceTable(t *testing.T) {
	schema := &shardingpb.ShardingSchema{
		DirectPlacements: []*shardingpb.DirectPlacement{
			{Object: obj("db1", "sc1", "countries"), Clusters: []string{"A", "B", "C"}},
		},
	}

	got, err := Resolve(schema, obj("db1", "sc1", "countries"), nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"A", "B", "C"}, got)
}

// Example 6 — resharding is a layout edit. Splitting B's -80 into B:-40 and
// D:40-80 must preserve coverage: every key stays a single-cluster route, keys
// formerly on C are untouched, keys formerly on B now land on B or D (only), and
// the new cluster D actually receives traffic.
func TestResolve_ReshardingPreservesCoverage(t *testing.T) {
	binding := []*shardingpb.TableBinding{
		{Table: obj("db1", "sc1", "t1"), ShardingGroup: "by_user", Columns: []string{"user_id"}},
	}
	before := &shardingpb.ShardingSchema{
		ShardingGroups: []*shardingpb.ShardingGroup{
			hashGroup("by_user",
				place(kr(nil, []byte{0x80}), "B"), // -80
				place(kr([]byte{0x80}, nil), "C"), // 80-
			),
		},
		TableBindings: binding,
	}
	after := &shardingpb.ShardingSchema{
		ShardingGroups: []*shardingpb.ShardingGroup{
			hashGroup("by_user",
				place(kr(nil, []byte{0x40}), "B"),          // -40
				place(kr([]byte{0x40}, []byte{0x80}), "D"), // 40-80  (new cluster)
				place(kr([]byte{0x80}, nil), "C"),          // 80-
			),
		},
		TableBindings: binding, // bindings are untouched by resharding
	}

	sawD := false
	for id := range 500 {
		pre, err := Resolve(before, obj("db1", "sc1", "t1"), userVal(id))
		require.NoError(t, err)
		require.Len(t, pre, 1)
		post, err := Resolve(after, obj("db1", "sc1", "t1"), userVal(id))
		require.NoError(t, err)
		require.Len(t, post, 1)

		switch pre[0] {
		case "C":
			assert.Equal(t, "C", post[0], "keys on C are unaffected by the B split (user_id=%d)", id)
		case "B":
			assert.Contains(t, []string{"B", "D"}, post[0], "a B key must stay on B or move to D (user_id=%d)", id)
			if post[0] == "D" {
				sawD = true
			}
		}
	}
	assert.True(t, sawD, "the new cluster D should receive some of B's former keys")
}

// Mixed granularity — a database-level placement is the default, and a table
// binding overrides it for that one table (most-specific-wins precedence).
func TestResolve_PrecedenceTableOverridesDatabase(t *testing.T) {
	schema := &shardingpb.ShardingSchema{
		DirectPlacements: []*shardingpb.DirectPlacement{
			{Object: obj("db1", "", ""), Clusters: []string{"A"}}, // db1 defaults to A
		},
		ShardingGroups: []*shardingpb.ShardingGroup{
			hashGroup("by_user",
				place(kr(nil, []byte{0x80}), "B"),
				place(kr([]byte{0x80}, nil), "C"),
			),
		},
		TableBindings: []*shardingpb.TableBinding{
			{Table: obj("db1", "sc1", "t1"), ShardingGroup: "by_user", Columns: []string{"user_id"}},
		},
	}

	// t1 is bound -> sharded across B/C, not the db-level A.
	got, err := Resolve(schema, obj("db1", "sc1", "t1"), userVal(42))
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Contains(t, []string{"B", "C"}, got[0])

	// Any other table in db1 falls back to the database-level placement.
	got, err = Resolve(schema, obj("db1", "sc1", "other"), userVal(42))
	require.NoError(t, err)
	assert.Equal(t, []string{"A"}, got)
}
