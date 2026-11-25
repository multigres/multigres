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

// Package shardindex is the extension point for shard indexes: named,
// database-wide ways to compute a routing identifier (a byte string) from
// column values. A table's primary shard key passes its columns to a shard
// index, and the routing identifier picks the shard whose key range contains
// it.
//
// # Implementations and instances
//
// An implementation is a type name ("hash") and a Factory, registered once per
// binary. Built-in type names have no dot; a user-provided implementation
// registers with Register under a namespaced name ("acme.region"), so the two
// can never collide. A shard index is an instance of an implementation: a
// catalog row (multigres.shard_index) with a name, the type and parameters,
// built with New. Parameters are a string map; each factory validates its own
// and rejects names it does not know (see CheckParams).
//
// # The mapping is permanent
//
// Rows are placed by the routing identifier, so once an implementation has
// shipped, its mapping from value to routing identifier must never change: not
// for a bug fix, not for a new PostgreSQL version. A different mapping needs a
// new type name. Every implementation should pin its mapping with golden
// vectors, as hash does in testdata/hash_golden.json.
//
// # Capabilities
//
// ShardIndex, the base interface, only describes an index. What an index can
// do is declared by the optional interfaces it also implements, each matching
// a kind of predicate the planner can route, and detected by type assertion:
//
//   - SingleColumn maps one column's values for equality and IN lists. A NULL
//     maps to DestinationNone, because col = NULL is true for no row.
//   - Hashing exposes the raw hash, so the index can be a component of a
//     multi-column index.
//   - TypeChecking says which column types the index accepts.
//
// # Compatibility for implementations
//
// User-provided implementations are compiled against this package, so it
// evolves without breaking them:
//
//   - ShardIndex never gains a method. New abilities (multi-column keys,
//     ranges, prefixes, lookup-entry maintenance, reverse mapping) arrive as
//     new optional interfaces, which existing implementations simply do not
//     implement.
//   - Existing capability interfaces never change their method sets either; a
//     changed contract is a new interface.
//   - Executor is passed to every method that may one day need to query shards.
//     Implementations only call it, never implement it, so it can gain methods
//     without touching them.
//   - Destination is a closed value type built by constructors, so new kinds
//     can be added, and Factory takes parameters as a string map, so new
//     parameters need no signature change.
//
// # Destinations
//
// Map returns one Destination per value: a routing identifier, several, a key
// range, every shard, or none. A caller placing a row must treat none as an
// error: no shard is chosen, so no PostgreSQL would see the row to reject it.
// A destination is independent of any shard
// layout; the caller resolves it against a tablegroup's active shards with
// Destination.Resolve at the moment it routes. Resolution lives here rather
// than in shardingcatalog because the set of destination kinds belongs to the
// extension point: the catalog's read model only has to answer "which shard
// owns this routing identifier" (TableGroup.ShardFor) and stays unaware of
// what planners and indexes do with it.
//
// # Writing an implementation
//
// Implement ShardIndex and the capabilities the index supports, and register
// a Factory from init:
//
//	package regionindex
//
//	func init() { shardindex.Register("acme.region", newIndex) }
//
//	func newIndex(name string, params map[string]string) (shardindex.ShardIndex, error) {
//		if err := shardindex.CheckParams("acme.region", name, params, "default"); err != nil {
//			return nil, err
//		}
//		return &index{name: name, def: params["default"]}, nil
//	}
//
// Then link the package into every binary that routes or validates — the
// gateway and the pooler — with a blank import in a plugin file next to main,
// as go/cmd/multigateway/plugin_topo.go does for topology backends:
//
//	// go/cmd/multigateway/plugin_shardindex_region.go
//	package main
//
//	import _ "example.com/regionindex"
//
// All binaries of a cluster must link the same implementations, and an
// implementation must compute the same routing identifier in each of them.
// Map and Hash run on the query path: they must be safe for concurrent use
// and should not allocate beyond their result.
package shardindex
