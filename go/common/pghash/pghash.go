// PostgreSQL Database Management System
// (also known as Postgres, formerly known as Postgres95)
//
//	Portions Copyright (c) 2026, Supabase, Inc
//
//	Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//
//	Portions Copyright (c) 1994, The Regents of the University of California
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written agreement
// is hereby granted, provided that the above copyright notice and this
// paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
// DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
// LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
// DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.
//
// THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
// AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
// ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
// PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.

// Package pghash holds PostgreSQL's hash functions for values, ported
// bit-for-bit. It is a leaf package with no dependencies so that every user of
// a PostgreSQL hash calls the same code: the expression engine's hashint*
// builtins (go/common/pgeval/funcs) and shard indexes (go/common/shardindex),
// which place rows by these values and so can never let them change.
//
// PostgreSQL's hash of a byte string depends on the server's byte order: it
// reads the key as native 32-bit words. BytesExtended is the little-endian
// result on every host. A big-endian PostgreSQL server computes different
// values and is not supported.
package pghash

import (
	"encoding/binary"
	"math/bits"
)

// Int8Key is the 32-bit key PostgreSQL hashes for an int8 (hashint8 and
// hashint8extended, hashfunc.c:45-114, REL_17_6). It folds the high half into
// the low half so numerically equal signed integers hash identically across
// widths: for a sign-extended int2 or int4 (or char) the high half contributes
// zero, just as in their PG bodies.
func Int8Key(value int64) uint32 {
	high := uint32(value >> 32)
	if value < 0 {
		high = ^high
	}
	return uint32(value) ^ high
}

// Int8Extended is PostgreSQL's hashint8extended(value, seed). Because of
// Int8Key's folding it is also hashint2extended and hashint4extended of the
// same number.
func Int8Extended(value int64, seed uint64) uint64 {
	return Uint32Extended(Int8Key(value), seed)
}

// Uint32Extended ports hash_bytes_uint32_extended and its mix/final macros from
// src/common/hashfn.c:82,116,631. With seed zero, the low 32 bits are exactly
// hash_bytes_uint32. This is PG's adaptation of Bob Jenkins' hash, not a Go
// substitute: persisted/distributed hash results must match PostgreSQL.
func Uint32Extended(key uint32, seed uint64) uint64 {
	a := uint32(0x9e3779b9 + 4 + 3923095)
	b, c := a, a
	if seed != 0 {
		a += uint32(seed >> 32)
		b += uint32(seed)
		a, b, c = mix(a, b, c)
	}
	a += key
	return final(a, b, c)
}

// BytesExtended ports hash_bytes_extended (src/common/hashfn.c:372, REL_17_6),
// which hash_any_extended wraps: PostgreSQL's 64-bit seeded hash of a byte
// string. It is what hashtextextended (under a deterministic collation),
// hashbpcharextended (after trailing spaces are stripped), uuid_hash_extended
// and hashvarlenaextended return for the bytes of their value.
//
// The result is the one a little-endian server computes. The C code reads the
// key in 32-bit words: native-order fetches when the pointer is aligned, and
// words assembled from bytes when it is not. Under !WORDS_BIGENDIAN the two
// paths are the same function of the bytes (k[0] is the low byte of the first
// word, and the tail switch adds the same shifted bytes either way), so the
// alignment of a value never shows in its hash. This port is that function,
// with explicit little-endian reads, and so does not depend on the host.
//
// The length is mixed in as a uint32, as in C, where keylen is an int. It does
// not allocate.
func BytesExtended(k []byte, seed uint64) uint64 {
	a := 0x9e3779b9 + uint32(len(k)) + 3923095
	b, c := a, a
	if seed != 0 {
		// The seed is hashed as a 12-byte chunk of its own: its two halves and
		// four zero bytes.
		a += uint32(seed >> 32)
		b += uint32(seed)
		a, b, c = mix(a, b, c)
	}
	for len(k) >= 12 {
		a += binary.LittleEndian.Uint32(k)
		b += binary.LittleEndian.Uint32(k[4:])
		c += binary.LittleEndian.Uint32(k[8:])
		a, b, c = mix(a, b, c)
		k = k[12:]
	}
	// The last 11 bytes. The lowest byte of c is reserved for the length, so
	// bytes 8 to 10 start at bit 8.
	switch len(k) {
	case 11:
		c += uint32(k[10]) << 24
		fallthrough
	case 10:
		c += uint32(k[9]) << 16
		fallthrough
	case 9:
		c += uint32(k[8]) << 8
		fallthrough
	case 8:
		b += uint32(k[7]) << 24
		fallthrough
	case 7:
		b += uint32(k[6]) << 16
		fallthrough
	case 6:
		b += uint32(k[5]) << 8
		fallthrough
	case 5:
		b += uint32(k[4])
		fallthrough
	case 4:
		a += uint32(k[3]) << 24
		fallthrough
	case 3:
		a += uint32(k[2]) << 16
		fallthrough
	case 2:
		a += uint32(k[1]) << 8
		fallthrough
	case 1:
		a += uint32(k[0])
	}
	return final(a, b, c)
}

// mix is the mix() macro of src/common/hashfn.c:82: it mixes three 32-bit
// values reversibly.
func mix(a, b, c uint32) (uint32, uint32, uint32) {
	a -= c
	a ^= bits.RotateLeft32(c, 4)
	c += b
	b -= a
	b ^= bits.RotateLeft32(a, 6)
	a += c
	c -= b
	c ^= bits.RotateLeft32(b, 8)
	b += a
	a -= c
	a ^= bits.RotateLeft32(c, 16)
	c += b
	b -= a
	b ^= bits.RotateLeft32(a, 19)
	a += c
	c -= b
	c ^= bits.RotateLeft32(b, 4)
	b += a
	return a, b, c
}

// final is the final() macro of src/common/hashfn.c:116 followed by the result
// every extended hash reports: b in the high half and c in the low half.
func final(a, b, c uint32) uint64 {
	c ^= b
	c -= bits.RotateLeft32(b, 14)
	a ^= c
	a -= bits.RotateLeft32(c, 11)
	b ^= a
	b -= bits.RotateLeft32(a, 25)
	c ^= b
	c -= bits.RotateLeft32(b, 16)
	a ^= c
	a -= bits.RotateLeft32(c, 4)
	b ^= a
	b -= bits.RotateLeft32(a, 14)
	c ^= b
	c -= bits.RotateLeft32(b, 24)
	return uint64(b)<<32 | uint64(c)
}
