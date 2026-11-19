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
	"encoding/binary"
	"strconv"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/datum"
	"github.com/multigres/multigres/go/common/parser/pgoid"
	"github.com/multigres/multigres/go/common/pghash"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
)

// HashType is the type name of the built-in hash shard index.
const HashType = "hash"

// HashSeed is the seed of the hash shard index: PostgreSQL's
// HASH_PARTITION_SEED (src/include/catalog/partition.h), passed as the int8
// seed argument of hashint8extended. It is part of the mapping and therefore
// permanent: rows are placed by the values it produces.
const HashSeed uint64 = 0x7A5B22367996DCFD

// hashIDLen is the length of a hash routing identifier.
const hashIDLen = 8

// hashIndex is the built-in hash shard index for integer columns. The routing
// identifier of a value is
//
//	hashint8extended(value::int8, HashSeed)
//
// exactly as PostgreSQL computes it, written as 8 bytes big-endian, so that
// byte order is the unsigned numeric order of the hash. int2, int4 and int8
// are sign-extended to int8 first, and so hash identically when equal. The
// same identifier is computable in SQL on any shard as
//
//	int8send(hashint8extended(col::int8, 8816678312871386365))
//
// See docs/sharding/shard-indexes.md. This mapping must never change;
// testdata/hash_golden.json pins it.
type hashIndex struct {
	name string
}

var (
	_ SingleColumn = (*hashIndex)(nil)
	_ Hashing      = (*hashIndex)(nil)
	_ TypeChecking = (*hashIndex)(nil)
)

func init() {
	Register(HashType, newHashIndex)
}

// newHashIndex is the Factory of hash. It takes no parameters.
func newHashIndex(name string, params map[string]string) (ShardIndex, error) {
	if err := CheckParams(HashType, name, params); err != nil {
		return nil, err
	}
	return &hashIndex{name: name}, nil
}

func (h *hashIndex) Name() string { return h.name }

func (h *hashIndex) Type() string { return HashType }

// Cost is 1: the routing identifier is computed from the value alone.
func (h *hashIndex) Cost() int { return 1 }

func (h *hashIndex) IsUnique() bool { return true }

// AcceptsType accepts the integer types.
func (h *hashIndex) AcceptsType(typ pgoid.Oid) bool {
	switch typ {
	case pgoid.INT2OID, pgoid.INT4OID, pgoid.INT8OID:
		return true
	}
	return false
}

// Hash returns the 8-byte routing identifier of value.
func (h *hashIndex) Hash(value Value) ([]byte, error) {
	hash, err := h.hash(value)
	if err != nil {
		return nil, err
	}
	return binary.BigEndian.AppendUint64(make([]byte, 0, hashIDLen), hash), nil
}

// Map returns a single routing identifier for each value.
func (h *hashIndex) Map(_ context.Context, _ Executor, values []Value) ([]Destination, error) {
	// One backing array holds every identifier, so mapping any number of
	// values costs two allocations.
	out := make([]Destination, len(values))
	ids := make([]byte, hashIDLen*len(values))
	for i, value := range values {
		hash, err := h.hash(value)
		if err != nil {
			return nil, err
		}
		// Cap each identifier so appending to one cannot reach the next.
		id := ids[i*hashIDLen : (i+1)*hashIDLen : (i+1)*hashIDLen]
		binary.BigEndian.PutUint64(id, hash)
		out[i] = DestinationRoutingID(id)
	}
	return out, nil
}

// Verify reports whether each value hashes to its routing identifier.
func (h *hashIndex) Verify(_ context.Context, _ Executor, values []Value, routingIDs [][]byte) ([]bool, error) {
	if len(values) != len(routingIDs) {
		return nil, mterrors.Errorf(mtrpcpb.Code_INTERNAL,
			"shard index %q: Verify got %d values and %d routing identifiers", h.name, len(values), len(routingIDs))
	}
	out := make([]bool, len(values))
	for i, value := range values {
		hash, err := h.hash(value)
		if err != nil {
			return nil, err
		}
		id := routingIDs[i]
		out[i] = len(id) == hashIDLen && binary.BigEndian.Uint64(id) == hash
	}
	return out, nil
}

// hash is the mapping itself: hashint8extended(value::int8, HashSeed).
func (h *hashIndex) hash(value Value) (uint64, error) {
	if value.IsNull {
		// Primary shard key columns are NOT NULL, so a NULL has no shard.
		return 0, mterrors.NewPgError("ERROR", mterrors.PgSSNullValueNotAllowed,
			"shard index \""+h.name+"\" cannot map a null value", "")
	}
	var n int64
	switch value.Type {
	case pgoid.INT2OID:
		n = int64(datum.DatumGetInt16(value.Datum))
	case pgoid.INT4OID:
		n = int64(datum.DatumGetInt32(value.Datum))
	case pgoid.INT8OID:
		n = datum.DatumGetInt64(value.Datum)
	default:
		return 0, mterrors.NewPgError("ERROR", mterrors.PgSSFeatureNotSupported,
			"shard index \""+h.name+"\" of type \""+HashType+"\" does not support type with OID "+
				strconv.FormatUint(uint64(value.Type), 10),
			"It supports smallint, integer and bigint.")
	}
	return pghash.Int8Extended(n, HashSeed), nil
}
