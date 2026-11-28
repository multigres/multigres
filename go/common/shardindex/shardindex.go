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
	"context"

	"github.com/multigres/multigres/go/common/parser/datum"
	"github.com/multigres/multigres/go/common/parser/pgoid"
)

// ShardIndex is what every implementation provides. It only describes the
// index; what the index can do is declared by the capability interfaces below.
// This interface never gains a method (see the package doc).
type ShardIndex interface {
	// Name is the name of this instance, unique within the database.
	Name() string

	// Type is the implementation's type name, as registered.
	Type() string

	// Cost lets the planner choose between indexes: 1 when the routing
	// identifier is computed from the value alone, 2 or more when it needs
	// a lookup.
	Cost() int

	// IsUnique reports whether one value maps to at most one routing
	// identifier. Only a unique index can place rows.
	IsUnique() bool
}

// SingleColumn is a shard index over one column. The planner uses it to route
// equality predicates and IN lists.
type SingleColumn interface {
	ShardIndex

	// Map returns one destination per value, in order. A unique index
	// returns a single routing identifier (or a key range, when it cannot
	// narrow further) for each value. A NULL maps to DestinationNone
	// rather than failing: col = NULL is true for no row, so a predicate
	// on it reaches no shard. A caller placing a row must reject
	// DestinationNone itself, since no shard is chosen to reject it.
	Map(ctx context.Context, exec Executor, values []Value) ([]Destination, error)

	// Verify reports, for each value, whether it maps to the routing
	// identifier at the same position. values and routingIDs have the same
	// length. A value that maps to DestinationNone, such as a NULL, is
	// false.
	Verify(ctx context.Context, exec Executor, values []Value, routingIDs [][]byte) ([]bool, error)
}

// Hashing is implemented by indexes whose mapping is a pure function of the
// value, so that a multi-column index can use them as a component.
type Hashing interface {
	// Hash returns the routing identifier of value. A NULL has none and is
	// an error.
	Hash(value Value) ([]byte, error)
}

// TypeChecking is implemented by indexes that restrict the column types they
// accept. Shard-key DDL uses it to validate a key before any row is placed.
type TypeChecking interface {
	// AcceptsType reports whether the index can map values of the type.
	AcceptsType(typ pgoid.Oid) bool
}

// Executor is how a shard index runs queries against the cluster while it
// maps, on behalf of the request being routed. It is for indexes that look a
// routing identifier up in a table rather than compute it from the value. Its
// method set is empty for now: Map and Verify take it already so that lookup
// indexes can be added without changing the signatures every implementation
// compiles against. Implementations must not implement Executor, only call it,
// and must accept a nil Executor unless they use it.
type Executor any

// Value is one column value handed to a shard index. The caller coerces the
// value to the type of the shard key column first: an index maps by Type, and
// the same bytes map differently as different types.
type Value struct {
	// Type is the PostgreSQL type OID of the value.
	Type pgoid.Oid
	// Datum is the value. It is meaningless when IsNull is set.
	//
	// A by-value type is stored as its DatumGet accessor reads it: an
	// integer with datum.Int16GetDatum, Int32GetDatum or Int64GetDatum.
	//
	// A by-reference type is its payload with no varlena header, built with
	// datum.BytesGetDatum and read with datum.DatumGetBytes. An empty
	// payload is the zero Datum:
	//
	//   - text, varchar, bpchar: the characters' bytes in the database's
	//     server encoding, not the client's. bpchar includes its padding.
	//   - bytea: the raw bytes.
	//   - uuid: the 16 bytes in network order, which are the hex digits of
	//     the canonical text form in order.
	Datum datum.Datum
	// IsNull marks SQL NULL.
	IsNull bool
}
