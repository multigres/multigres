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

package funcs

import (
	"math/bits"
	"runtime"

	"github.com/multigres/multigres/go/common/pgeval/datum"
	"github.com/multigres/multigres/go/common/pgeval/fmgr"
)

func init() {
	fmgr.RegisterBuiltin("hashint2", integerHash[int16])
	fmgr.RegisterBuiltin("hashint4", integerHash[int32])
	fmgr.RegisterBuiltin("hashint8", integerHash[int64])
	fmgr.RegisterBuiltin("hashint2extended", integerHashExtended[int16])
	fmgr.RegisterBuiltin("hashint4extended", integerHashExtended[int32])
	fmgr.RegisterBuiltin("hashint8extended", integerHashExtended[int64])
	// PG uses plain C char for this shared char/boolean support routine.
	// Of our linux/darwin × amd64/arm64 targets, Linux ARM64 defaults to
	// unsigned char; the others default to signed char. Bool's 0/1 values
	// hash identically everywhere. Non-default C compiler flags are outside
	// this compatibility target; validate the ABI when adding architectures.
	if runtime.GOOS == "linux" && runtime.GOARCH == "arm64" {
		fmgr.RegisterBuiltin("hashchar", integerHash[uint8])
		fmgr.RegisterBuiltin("hashcharextended", integerHashExtended[uint8])
	} else {
		fmgr.RegisterBuiltin("hashchar", integerHash[int8])
		fmgr.RegisterBuiltin("hashcharextended", integerHashExtended[int8])
	}
}

type hashInteger interface {
	int8 | uint8 | integer
}

// hashfunc.c:45-114, REL_17_6. Fold int8's high half so numerically equal
// signed integers hash identically across widths. For sign-extended int2/int4
// (and char), the high half contributes zero, just as in their PG bodies.
func integerHashKey[T hashInteger](fcinfo fmgr.FunctionCallInfo) uint32 {
	value := int64(T(fcinfo.GetArgInt64(0)))
	high := uint32(value >> 32)
	if value < 0 {
		high = ^high
	}
	return uint32(value) ^ high
}

func integerHash[T hashInteger](fcinfo fmgr.FunctionCallInfo) datum.Datum {
	return datum.Int32GetDatum(int32(hashUint32(integerHashKey[T](fcinfo), 0)))
}

func integerHashExtended[T hashInteger](fcinfo fmgr.FunctionCallInfo) datum.Datum {
	return datum.Int64GetDatum(int64(hashUint32(integerHashKey[T](fcinfo), uint64(fcinfo.GetArgInt64(1)))))
}

// hashUint32 ports hash_bytes_uint32_extended and its mix/final macros from
// src/common/hashfn.c:82,116,631. With seed zero, the low 32 bits are exactly
// hash_bytes_uint32. This is PG's adaptation of Bob Jenkins' hash, not a Go
// substitute: persisted/distributed hash results must match PostgreSQL.
func hashUint32(key uint32, seed uint64) uint64 {
	a := uint32(0x9e3779b9 + 4 + 3923095)
	b, c := a, a
	if seed != 0 {
		a += uint32(seed >> 32)
		b += uint32(seed)
		// mix(a, b, c)
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
	}
	a += key
	// final(a, b, c)
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
