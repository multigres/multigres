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
// seed argument of every type's extended hash function. It is part of the
// mapping and therefore permanent: rows are placed by the values it produces.
const HashSeed uint64 = 0x7A5B22367996DCFD

// hashIDLen is the length of a hash routing identifier.
const hashIDLen = 8

// hashIndex is the built-in hash shard index. The routing identifier of a
// value is PostgreSQL's extended hash of the value for its type, seeded with
// HashSeed, read as an unsigned 64-bit number and written as 8 bytes
// big-endian, so that byte order is the numeric order of the hash. The type
// decides the function, and each is computable in SQL on any shard:
//
//	smallint, integer, bigint   int8send(hashint8extended(col::int8, seed))
//	text, varchar               int8send(hashtextextended(col, seed))
//	bpchar                      int8send(hashbpcharextended(col, seed))
//	uuid                        int8send(uuid_hash_extended(col, seed))
//	bytea                       int8send(hash_record_extended(ROW(col), seed))
//
// where seed is 8816678312871386365, HashSeed as an int8.
//
// Integers: int2, int4 and int8 are sign-extended to int8 first, and so hash
// identically when equal. hashint8extended folds the int8 to 32 bits before
// hashing, so distinct bigints share a routing identifier (1 and 4294967296,
// for example).
//
// text and varchar: pghash.BytesExtended of the value's bytes in the server
// encoding. That is hashtextextended under a deterministic collation only. A
// nondeterministic collation makes PostgreSQL hash a locale sort key instead,
// which this index cannot compute, so such a column cannot be a shard key.
//
// bpchar: the same after trailing spaces (0x20) are removed, as
// hashbpcharextended does, so 'ab' and 'ab  ' share an identifier. The same
// bytes hash differently as text, where trailing spaces count.
//
// uuid: pghash.BytesExtended of its 16 bytes.
//
// bytea: pghash.BytesExtended of its bytes, which is what bytea's own extended
// hash function, hashvarlenaextended, returns. That function takes an
// "internal" argument and so cannot be called from SQL, and every routing
// identifier must be computable in SQL, so that a reshard can filter rows by
// key range with it. The SQL form therefore hashes the one-column row holding
// the value. hash_record_extended (rowtypes.c) starts from 0 and folds each column
// in as result*31 + the column's extended hash, and mixes in nothing else, so
// for one column it is that column's hash unchanged. The same bytes therefore
// have one identifier as text and as bytea.
//
// All of these are the values a little-endian PostgreSQL server computes (see
// pghash.BytesExtended). In no case is the mapping injective: an identifier
// places a value, it never identifies one.
//
// See docs/sharding/shard-indexes.md. This mapping must never change. A type
// can be added, because no value already accepted changes its identifier.
// testdata/hash_golden.json and testdata/hash_golden_bytes.json pin it.
type hashIndex struct {
	name string
}

var (
	_ SingleColumn = (*hashIndex)(nil)
	_ Hashing      = (*hashIndex)(nil)
	_ TypeChecking = (*hashIndex)(nil)
)

func init() {
	registerBuiltin(HashType, newHashIndex)
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

// AcceptsType accepts the types hash maps: the integers, text, varchar,
// bpchar, uuid and bytea.
func (h *hashIndex) AcceptsType(typ pgoid.Oid) bool {
	switch typ {
	case pgoid.INT2OID, pgoid.INT4OID, pgoid.INT8OID,
		pgoid.TEXTOID, pgoid.VARCHAROID, pgoid.BPCHAROID,
		pgoid.UUIDOID, pgoid.BYTEAOID:
		return true
	}
	return false
}

// Hash returns the 8-byte routing identifier of value. A NULL has none and is
// an error.
func (h *hashIndex) Hash(value Value) ([]byte, error) {
	hash, err := h.hash(value)
	if err != nil {
		return nil, err
	}
	return binary.BigEndian.AppendUint64(make([]byte, 0, hashIDLen), hash), nil
}

// Map returns a single routing identifier for each value, and
// DestinationNone for a NULL: col = NULL is true for no row.
func (h *hashIndex) Map(_ context.Context, _ Executor, values []Value) ([]Destination, error) {
	// One backing array holds every identifier, so mapping any number of
	// values costs two allocations.
	out := make([]Destination, len(values))
	ids := make([]byte, hashIDLen*len(values))
	for i, value := range values {
		if value.IsNull {
			// The zero Destination is DestinationNone.
			continue
		}
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

// Verify reports whether each value hashes to its routing identifier. A NULL
// maps to none, so it is false.
func (h *hashIndex) Verify(_ context.Context, _ Executor, values []Value, routingIDs [][]byte) ([]bool, error) {
	if len(values) != len(routingIDs) {
		return nil, mterrors.Errorf(mtrpcpb.Code_INTERNAL,
			"shard index %q: Verify got %d values and %d routing identifiers", h.name, len(values), len(routingIDs))
	}
	out := make([]bool, len(values))
	for i, value := range values {
		if value.IsNull {
			continue
		}
		hash, err := h.hash(value)
		if err != nil {
			return nil, err
		}
		id := routingIDs[i]
		out[i] = len(id) == hashIDLen && binary.BigEndian.Uint64(id) == hash
	}
	return out, nil
}

// uuidLen is the length of a uuid's payload.
const uuidLen = 16

// hash is the mapping itself, described on hashIndex. A NULL is an error
// here; Map and Verify handle it before calling. It does not allocate.
func (h *hashIndex) hash(value Value) (uint64, error) {
	if value.IsNull {
		// Primary shard key columns are NOT NULL, so a NULL has no
		// routing identifier.
		return 0, mterrors.NewPgError("ERROR", mterrors.PgSSNullValueNotAllowed,
			"shard index \""+h.name+"\" cannot map a null value", "")
	}
	switch value.Type {
	case pgoid.INT2OID:
		return pghash.Int8Extended(int64(datum.DatumGetInt16(value.Datum)), HashSeed), nil
	case pgoid.INT4OID:
		return pghash.Int8Extended(int64(datum.DatumGetInt32(value.Datum)), HashSeed), nil
	case pgoid.INT8OID:
		return pghash.Int8Extended(datum.DatumGetInt64(value.Datum), HashSeed), nil
	case pgoid.TEXTOID, pgoid.VARCHAROID:
		// An empty value's payload is nil, which hashes as the empty string.
		return pghash.BytesExtended(datum.DatumGetBytes(value.Datum), HashSeed), nil
	case pgoid.BPCHAROID:
		// bcTruelen (varchar.c): trailing spaces are padding, not data.
		b := datum.DatumGetBytes(value.Datum)
		for len(b) > 0 && b[len(b)-1] == ' ' {
			b = b[:len(b)-1]
		}
		return pghash.BytesExtended(b, HashSeed), nil
	case pgoid.UUIDOID:
		b := datum.DatumGetBytes(value.Datum)
		if len(b) != uuidLen {
			return 0, mterrors.NewPgError("ERROR", mterrors.PgSSInvalidBinaryRepresentation,
				"shard index \""+h.name+"\" got a uuid of "+strconv.Itoa(len(b))+" bytes", "A uuid is 16 bytes.")
		}
		return pghash.BytesExtended(b, HashSeed), nil
	case pgoid.BYTEAOID:
		return pghash.BytesExtended(datum.DatumGetBytes(value.Datum), HashSeed), nil
	}
	return 0, mterrors.NewPgError("ERROR", mterrors.PgSSFeatureNotSupported,
		"shard index \""+h.name+"\" of type \""+HashType+"\" does not support type with OID "+
			strconv.FormatUint(uint64(value.Type), 10),
		"It supports smallint, integer, bigint, text, varchar, bpchar, uuid and bytea.")
}
