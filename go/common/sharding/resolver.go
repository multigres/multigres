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

// Package sharding is a DRAFT reference resolver for the logical->physical
// sharding model proposed in docs/sharding/design.md. It exists to validate the
// proto data model with runnable examples, not as production routing code. The
// hash function in particular is a placeholder (see keyspaceID).
package sharding

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"sort"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	shardingpb "github.com/multigres/multigres/go/pb/sharding"
)

// Resolve computes the set of storage clusters a query touches for the given
// logical object and shard-key values, using schema as the logical->physical
// map.
//
// values holds the query's shard-key column values keyed by column name. It may
// be empty (or missing some bound columns), which models a query with no usable
// shard-key predicate — that resolves to a scatter across the object's whole
// layout.
//
// The returned cluster names are sorted and de-duplicated so callers (and tests)
// get a stable set. A single-element result is a targeted route; a multi-element
// result is a scatter/gather.
func Resolve(schema *shardingpb.ShardingSchema, obj *shardingpb.LogicalObject, values map[string][]byte) ([]string, error) {
	// A table bound to a sharding group is the most specific rule and wins over
	// any database/schema-level direct placement (see ShardingSchema precedence).
	if binding := findBinding(schema, obj); binding != nil {
		return resolveSharded(schema, binding, values)
	}

	// Otherwise fall back to the most specific direct placement that covers the
	// object (table, then schema, then database).
	if dp := findDirectPlacement(schema, obj); dp != nil {
		return sortedSet(dp.GetClusters()), nil
	}

	return nil, fmt.Errorf("no placement found for %s", objectString(obj))
}

// resolveSharded turns a table binding + shard-key values into target clusters.
// With a usable key it computes the keyspace-id and returns the owning cluster;
// without one it scatters across every cluster in the group's layout.
func resolveSharded(schema *shardingpb.ShardingSchema, binding *shardingpb.TableBinding, values map[string][]byte) ([]string, error) {
	group := findGroup(schema, binding.GetShardingGroup())
	if group == nil {
		return nil, fmt.Errorf("table %s references unknown sharding group %q", objectString(binding.GetTable()), binding.GetShardingGroup())
	}

	keyBytes, haveKey := shardKeyBytes(binding.GetColumns(), values)
	if !haveKey {
		// No usable shard key: scatter across the whole layout.
		return layoutClusters(group), nil
	}

	kid := keyspaceID(group.GetFunction(), keyBytes)

	var hits []string
	for _, p := range group.GetLayout() {
		if keyRangeContains(p.GetKeyRange(), kid) {
			hits = append(hits, p.GetCluster())
		}
	}
	if len(hits) == 0 {
		return nil, fmt.Errorf("keyspace-id %x for %s is not covered by group %q layout", kid, objectString(binding.GetTable()), group.GetName())
	}
	return sortedSet(hits), nil
}

// shardKeyBytes concatenates the bound columns' values in order. It reports
// haveKey=false if any bound column is absent, since a partial key can't be
// hashed to a single keyspace-id.
func shardKeyBytes(columns []string, values map[string][]byte) (key []byte, haveKey bool) {
	if len(columns) == 0 {
		return nil, false
	}
	var buf bytes.Buffer
	for _, c := range columns {
		v, ok := values[c]
		if !ok {
			return nil, false
		}
		// Length-prefix each column so ("a","bc") != ("ab","c").
		fmt.Fprintf(&buf, "%d:", len(v))
		buf.Write(v)
	}
	return buf.Bytes(), true
}

// keyspaceID maps a shard-key value to a keyspace-id in [0x00.., 0xFF..].
//
// PLACEHOLDER. A range function uses the raw key bytes so ordering is preserved;
// everything else (hash, and lookup for now) uses the first 8 bytes of SHA-256
// for even distribution. The real functions (and keyspace-id width) are an open
// design decision — this is only good enough to demonstrate routing behavior
// deterministically.
func keyspaceID(fn *shardingpb.ShardingFunction, key []byte) []byte {
	if fn.GetRange() != nil {
		return key
	}
	sum := sha256.Sum256(key)
	return sum[:8]
}

// keyRangeContains reports whether kid falls in [start, end): start inclusive,
// end exclusive. An empty start is -inf; an empty end is +inf. Comparison is
// lexicographic over bytes, so a short range boundary (e.g. 0x80) compares
// correctly against a wider keyspace-id.
func keyRangeContains(kr *clustermetadatapb.KeyRange, kid []byte) bool {
	start := kr.GetStart()
	end := kr.GetEnd()
	if len(start) > 0 && bytes.Compare(kid, start) < 0 {
		return false
	}
	if len(end) > 0 && bytes.Compare(kid, end) >= 0 {
		return false
	}
	return true
}

// findBinding returns the table binding for obj, if obj names that exact table.
func findBinding(schema *shardingpb.ShardingSchema, obj *shardingpb.LogicalObject) *shardingpb.TableBinding {
	for _, b := range schema.GetTableBindings() {
		if sameObject(b.GetTable(), obj) {
			return b
		}
	}
	return nil
}

// findDirectPlacement returns the most specific direct placement covering obj:
// an exact match first, then a schema-level, then a database-level one.
func findDirectPlacement(schema *shardingpb.ShardingSchema, obj *shardingpb.LogicalObject) *shardingpb.DirectPlacement {
	var bySchema, byDatabase *shardingpb.DirectPlacement
	for _, dp := range schema.GetDirectPlacements() {
		scope := dp.GetObject()
		switch {
		case sameObject(scope, obj):
			return dp // exact match is most specific
		case scope.GetTable() == "" && scope.GetSchema() != "" &&
			scope.GetDatabase() == obj.GetDatabase() && scope.GetSchema() == obj.GetSchema():
			bySchema = dp
		case scope.GetTable() == "" && scope.GetSchema() == "" &&
			scope.GetDatabase() == obj.GetDatabase():
			byDatabase = dp
		}
	}
	if bySchema != nil {
		return bySchema
	}
	return byDatabase
}

func findGroup(schema *shardingpb.ShardingSchema, name string) *shardingpb.ShardingGroup {
	for _, g := range schema.GetShardingGroups() {
		if g.GetName() == name {
			return g
		}
	}
	return nil
}

// layoutClusters returns the sorted, de-duplicated set of clusters in a group's
// layout — the scatter target when there is no usable shard key.
func layoutClusters(group *shardingpb.ShardingGroup) []string {
	var all []string
	for _, p := range group.GetLayout() {
		all = append(all, p.GetCluster())
	}
	return sortedSet(all)
}

func sameObject(a, b *shardingpb.LogicalObject) bool {
	return a.GetDatabase() == b.GetDatabase() &&
		a.GetSchema() == b.GetSchema() &&
		a.GetTable() == b.GetTable()
}

func objectString(o *shardingpb.LogicalObject) string {
	s := o.GetDatabase()
	if o.GetSchema() != "" {
		s += "." + o.GetSchema()
	}
	if o.GetTable() != "" {
		s += "." + o.GetTable()
	}
	return s
}

func sortedSet(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}
