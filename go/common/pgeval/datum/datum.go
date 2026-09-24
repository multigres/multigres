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

// Package datum ports PostgreSQL's Datum value model (postgres/src/include/postgres.h,
// REL_17_6) to Go: the tagless value that every fmgr-callable function consumes
// and produces. It is the foundation of the gateway's evaluation engine.
//
// # Representation
//
// PostgreSQL's Datum is a uintptr_t that is *either* an inline scalar or a hidden
// pointer, distinguished only by the caller's static knowledge of the type. That
// pointer-hidden-in-an-integer trick is illegal under Go's garbage collector,
// which must be able to find every live pointer. So Datum here is a two-word
// struct instead of one machine word:
//
//		type Datum struct { val uint64; ptr unsafe.Pointer }
//
//	  - By-value types (bool, the integers, float4/float8, OIDs) live in val, with
//	    ptr nil. Constructing and reading them touches no heap: DatumGetInt32 and
//	    friends are allocation-free.
//	  - By-reference types (text, bytea, numeric, ...) put their payload pointer in
//	    ptr, which the GC can see and keep alive, and fold the payload length into
//	    val (see BytesGetDatum). This is why the struct is two words rather than
//	    PostgreSQL's one.
//
// On 64-bit PostgreSQL, USE_FLOAT8_BYVAL makes int64 and float8 pass-by-value;
// val is 64 bits wide, so this port is unconditionally byval for every scalar up
// to 64 bits and needs none of upstream's #ifdef USE_FLOAT8_BYVAL branches.
//
// As upstream, NULL is never encoded in a Datum; it rides alongside in a
// NullableDatum or a separate isnull flag. A zero Datum is a legitimate value
// (false, 0, InvalidOid), not a NULL sentinel.
//
// # Divergence from upstream
//
// No TOAST, no varlena headers: a by-reference Datum's ptr addresses bare payload
// bytes, never a 1B/4B-header varlena. The length that a varlena header would
// carry lives in val instead.
//
// The accessor functions below mirror postgres.h one-for-one (same names, same
// semantics) so that ported fmgr bodies read line-for-line against the C.
package datum

import (
	"math"
	"unsafe"

	"github.com/multigres/multigres/go/common/parser/pgoid"
)

// Datum is the tagless value model - postgres.h:64 ("typedef uintptr_t Datum").
// See the package doc for why this is a two-word struct rather than a single
// machine word. The fields are unexported: callers construct and inspect Datums
// only through the XxxGetDatum / DatumGetXxx accessors, mirroring the way
// upstream treats a Datum as opaque outside of postgres.h.
type Datum struct {
	// val holds by-value scalars directly, or, for by-reference values, the
	// payload length (see BytesGetDatum).
	val uint64
	// ptr is nil for by-value scalars and addresses the payload bytes for
	// by-reference values. Keeping it a real Go pointer is what makes
	// by-reference payloads visible to the garbage collector.
	ptr unsafe.Pointer
}

// NullableDatum pairs a Datum with its nullness - postgres.h:72. Nullness lives
// beside the Datum, never inside it: a Datum on its own is always a
// non-NULL value.
type NullableDatum struct {
	Value  Datum // FIELDNO_NULLABLE_DATUM_DATUM 0
	IsNull bool  // FIELDNO_NULLABLE_DATUM_ISNULL 1
}

// Oid is PostgreSQL's object identifier, aliased from the leaf pgoid package so a
// single Oid type flows through the parser, catalog, and eval code -
// postgres_ext.h:31.
type Oid = pgoid.Oid

// TransactionId is a transaction identifier - c.h:652 ("typedef uint32 TransactionId").
type TransactionId uint32

// MultiXactId is a multixact identifier - c.h:662 ("typedef TransactionId
// MultiXactId"). Upstream makes it an alias of TransactionId, so it is one here too.
type MultiXactId = TransactionId

// CommandId is a command identifier within a transaction - c.h:666 ("typedef
// uint32 CommandId").
type CommandId uint32

// Char is the storage form of PostgreSQL's "char" type (a single byte),
// distinguished by name from int8 the way upstream's plain C char is distinct
// from its int8 (== signed char) typedef, even though both are 8-bit signed. It
// is an alias, not a defined type, so it stays assignment-compatible with int8
// and ported fmgr bodies keep reading line-for-line against the C.
//
// The domain is signed: char.c pins char<->int4 conversions to signed char
// (chartoi4 reinterprets via (int8), so byte 0xFF reads as -1; i4tochar rejects
// anything outside SCHAR_MIN..SCHAR_MAX - char.c:184,192). Ordering, by contrast,
// is unsigned: the comparison operators cast to (uint8) at each site (charlt does
// (uint8) arg1 < (uint8) arg2 - char.c:145). Those comparison functions therefore
// must apply the uint8 cast explicitly; Char alone does not carry it.
type Char = int8

// ----------------------------------------------------------------
// By-value accessors. Each mirrors the identically named inline function in
// postgres.h. Signed conversions sign-extend into val and truncate on the way
// out, exactly as the C casts (Datum)X / (intN)X do; the round trip is lossless.
// ----------------------------------------------------------------

// DatumGetBool returns the boolean value of a datum - postgres.h:90. Any nonzero
// value is true.
func DatumGetBool(x Datum) bool { return x.val != 0 }

// BoolGetDatum returns the datum representation of a boolean - postgres.h:102.
func BoolGetDatum(x bool) Datum {
	if x {
		return Datum{val: 1}
	}
	return Datum{val: 0}
}

// DatumGetChar returns the character value of a datum - postgres.h:112. See Char
// for why the "char" type maps to a signed 8-bit value.
func DatumGetChar(x Datum) Char { return Char(x.val) }

// CharGetDatum returns the datum representation of a character - postgres.h:122.
func CharGetDatum(x Char) Datum { return Datum{val: uint64(x)} }

// Int8GetDatum returns the datum representation of an 8-bit integer - postgres.h:132.
// (Upstream has no DatumGetInt8; DatumGetChar covers the read direction.)
func Int8GetDatum(x int8) Datum { return Datum{val: uint64(x)} }

// DatumGetUInt8 returns the 8-bit unsigned integer value of a datum - postgres.h:142.
func DatumGetUInt8(x Datum) uint8 { return uint8(x.val) }

// UInt8GetDatum returns the datum representation of an 8-bit unsigned integer - postgres.h:152.
func UInt8GetDatum(x uint8) Datum { return Datum{val: uint64(x)} }

// DatumGetInt16 returns the 16-bit integer value of a datum - postgres.h:162.
func DatumGetInt16(x Datum) int16 { return int16(x.val) }

// Int16GetDatum returns the datum representation of a 16-bit integer - postgres.h:172.
func Int16GetDatum(x int16) Datum { return Datum{val: uint64(x)} }

// DatumGetUInt16 returns the 16-bit unsigned integer value of a datum - postgres.h:182.
func DatumGetUInt16(x Datum) uint16 { return uint16(x.val) }

// UInt16GetDatum returns the datum representation of a 16-bit unsigned integer - postgres.h:192.
func UInt16GetDatum(x uint16) Datum { return Datum{val: uint64(x)} }

// DatumGetInt32 returns the 32-bit integer value of a datum - postgres.h:202.
func DatumGetInt32(x Datum) int32 { return int32(x.val) }

// Int32GetDatum returns the datum representation of a 32-bit integer - postgres.h:212.
func Int32GetDatum(x int32) Datum { return Datum{val: uint64(x)} }

// DatumGetUInt32 returns the 32-bit unsigned integer value of a datum - postgres.h:222.
func DatumGetUInt32(x Datum) uint32 { return uint32(x.val) }

// UInt32GetDatum returns the datum representation of a 32-bit unsigned integer - postgres.h:232.
func UInt32GetDatum(x uint32) Datum { return Datum{val: uint64(x)} }

// DatumGetObjectId returns the object identifier value of a datum - postgres.h:242.
func DatumGetObjectId(x Datum) Oid { return Oid(x.val) }

// ObjectIdGetDatum returns the datum representation of an object identifier - postgres.h:252.
func ObjectIdGetDatum(x Oid) Datum { return Datum{val: uint64(x)} }

// DatumGetTransactionId returns the transaction identifier value of a datum - postgres.h:262.
func DatumGetTransactionId(x Datum) TransactionId { return TransactionId(x.val) }

// TransactionIdGetDatum returns the datum representation of a transaction identifier - postgres.h:272.
func TransactionIdGetDatum(x TransactionId) Datum { return Datum{val: uint64(x)} }

// MultiXactIdGetDatum returns the datum representation of a multixact identifier - postgres.h:282.
func MultiXactIdGetDatum(x MultiXactId) Datum { return Datum{val: uint64(x)} }

// DatumGetCommandId returns the command identifier value of a datum - postgres.h:292.
func DatumGetCommandId(x Datum) CommandId { return CommandId(x.val) }

// CommandIdGetDatum returns the datum representation of a command identifier - postgres.h:302.
func CommandIdGetDatum(x CommandId) Datum { return Datum{val: uint64(x)} }

// DatumGetInt64 returns the 64-bit integer value of a datum - postgres.h:385.
// Unconditionally pass-by-value here; see the package doc on USE_FLOAT8_BYVAL.
func DatumGetInt64(x Datum) int64 { return int64(x.val) }

// Int64GetDatum returns the datum representation of a 64-bit integer - postgres.h:403.
func Int64GetDatum(x int64) Datum { return Datum{val: uint64(x)} }

// DatumGetUInt64 returns the 64-bit unsigned integer value of a datum - postgres.h:419.
func DatumGetUInt64(x Datum) uint64 { return x.val }

// UInt64GetDatum returns the datum representation of a 64-bit unsigned integer - postgres.h:436.
func UInt64GetDatum(x uint64) Datum { return Datum{val: x} }

// DatumGetFloat4 returns the 4-byte floating point value of a datum - postgres.h:458.
// Upstream reinterprets the datum's low 32 bits as a float4 through a union; the
// Float32frombits pun is the exact analogue and preserves every bit, including
// NaN payloads.
func DatumGetFloat4(x Datum) float32 { return math.Float32frombits(uint32(x.val)) }

// Float4GetDatum returns the datum representation of a 4-byte floating point number - postgres.h:475.
func Float4GetDatum(x float32) Datum { return Datum{val: uint64(math.Float32bits(x))} }

// DatumGetFloat8 returns the 8-byte floating point value of a datum - postgres.h:494.
// Bit-preserving pun, as for float4.
func DatumGetFloat8(x Datum) float64 { return math.Float64frombits(x.val) }

// Float8GetDatum returns the datum representation of an 8-byte floating point number - postgres.h:519.
func Float8GetDatum(x float64) Datum { return Datum{val: math.Float64bits(x)} }

// ----------------------------------------------------------------
// Pointer accessors - postgres.h. PostgreSQL's Pointer/CString/Name are all
// char* under the hood; here the by-reference slot is ptr. A Datum built with
// PointerGetDatum carries no length in val - use BytesGetDatum below when the
// payload has one.
// ----------------------------------------------------------------

// DatumGetPointer returns the pointer value of a datum - postgres.h:312.
func DatumGetPointer(x Datum) unsafe.Pointer { return x.ptr }

// PointerGetDatum returns the datum representation of a pointer - postgres.h:322.
func PointerGetDatum(p unsafe.Pointer) Datum { return Datum{ptr: p} }

// ----------------------------------------------------------------
// By-reference payloads. These have no direct postgres.h analogue:
// upstream would DatumGetPointer to a varlena and read its length out of the 1B/4B
// header. We drop the header and carry the payload's length in val instead, so a
// by-reference Datum is (ptr = &payload[0], val = len(payload)). This is the
// primitive the later text/bytea/numeric working-form accessors build on.
// ----------------------------------------------------------------

// BytesGetDatum builds a by-reference Datum whose payload is b's bytes. The
// backing array is referenced (not copied); ptr keeps it alive for the GC, so the
// caller must not mutate b while the Datum is in use. An empty or nil b yields the
// zero Datum.
func BytesGetDatum(b []byte) Datum {
	if len(b) == 0 {
		return Datum{}
	}
	return Datum{val: uint64(len(b)), ptr: unsafe.Pointer(&b[0])}
}

// DatumGetBytes returns the payload of a by-reference Datum built by BytesGetDatum.
// The result aliases the original backing array; it is not a copy. Calling this on
// a Datum that was not built by BytesGetDatum (e.g. a by-value scalar, or one from
// PointerGetDatum, which sets no length) does not return its payload.
func DatumGetBytes(x Datum) []byte {
	if x.val == 0 || x.ptr == nil {
		return nil
	}
	return unsafe.Slice((*byte)(x.ptr), int(x.val))
}
