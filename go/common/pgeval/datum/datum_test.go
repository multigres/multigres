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

package datum

import (
	"math"
	"runtime"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDatumSizeTwoWords locks the representation invariant: Datum is exactly two
// machine words. If this ever changes, the representation and its benchmarks need
// re-visiting.
func TestDatumSizeTwoWords(t *testing.T) {
	assert.Equal(t, uintptr(16), unsafe.Sizeof(Datum{}), "Datum must be two 64-bit words")
	assert.Equal(t, uintptr(0), unsafe.Sizeof(Datum{})%unsafe.Sizeof(uintptr(0)))
}

// TestZeroDatum documents that the zero Datum is a genuine value (false, 0,
// InvalidOid), not a NULL sentinel - nullness lives in NullableDatum.
func TestZeroDatum(t *testing.T) {
	var d Datum
	assert.False(t, DatumGetBool(d))
	assert.Equal(t, int32(0), DatumGetInt32(d))
	assert.Equal(t, int64(0), DatumGetInt64(d))
	assert.Equal(t, Oid(0), DatumGetObjectId(d))
	assert.Nil(t, DatumGetPointer(d))
}

func TestBoolRoundtrip(t *testing.T) {
	assert.True(t, DatumGetBool(BoolGetDatum(true)))
	assert.False(t, DatumGetBool(BoolGetDatum(false)))
	// Any nonzero value reads as true, matching upstream DatumGetBool.
	assert.True(t, DatumGetBool(Int32GetDatum(42)))
	assert.True(t, DatumGetBool(Int32GetDatum(-1)))
}

func TestSignedIntRoundtrip(t *testing.T) {
	for _, v := range []int8{math.MinInt8, -1, 0, 1, math.MaxInt8} {
		assert.Equal(t, v, DatumGetChar(CharGetDatum(v)), "char %d", v)
		assert.Equal(t, v, DatumGetChar(Int8GetDatum(v)), "int8 %d", v)
	}
	for _, v := range []int16{math.MinInt16, -1, 0, 1, math.MaxInt16} {
		assert.Equal(t, v, DatumGetInt16(Int16GetDatum(v)), "int16 %d", v)
	}
	for _, v := range []int32{math.MinInt32, -1, 0, 1, math.MaxInt32} {
		assert.Equal(t, v, DatumGetInt32(Int32GetDatum(v)), "int32 %d", v)
	}
	for _, v := range []int64{math.MinInt64, -1, 0, 1, math.MaxInt64} {
		assert.Equal(t, v, DatumGetInt64(Int64GetDatum(v)), "int64 %d", v)
	}
}

func TestUnsignedIntRoundtrip(t *testing.T) {
	for _, v := range []uint8{0, 1, math.MaxUint8} {
		assert.Equal(t, v, DatumGetUInt8(UInt8GetDatum(v)), "uint8 %d", v)
	}
	for _, v := range []uint16{0, 1, math.MaxUint16} {
		assert.Equal(t, v, DatumGetUInt16(UInt16GetDatum(v)), "uint16 %d", v)
	}
	for _, v := range []uint32{0, 1, math.MaxUint32} {
		assert.Equal(t, v, DatumGetUInt32(UInt32GetDatum(v)), "uint32 %d", v)
	}
	for _, v := range []uint64{0, 1, math.MaxUint64} {
		assert.Equal(t, v, DatumGetUInt64(UInt64GetDatum(v)), "uint64 %d", v)
	}
}

func TestIdentifierRoundtrip(t *testing.T) {
	for _, v := range []Oid{0, 1, 16, math.MaxUint32} {
		assert.Equal(t, v, DatumGetObjectId(ObjectIdGetDatum(v)), "oid %d", v)
	}
	for _, v := range []TransactionId{0, 1, math.MaxUint32} {
		assert.Equal(t, v, DatumGetTransactionId(TransactionIdGetDatum(v)), "xid %d", v)
		// MultiXactId is an alias of TransactionId; MultiXactIdGetDatum stores the
		// same bits, so DatumGetTransactionId reads them back.
		assert.Equal(t, v, DatumGetTransactionId(MultiXactIdGetDatum(v)), "mxid %d", v)
	}
	for _, v := range []CommandId{0, 1, math.MaxUint32} {
		assert.Equal(t, v, DatumGetCommandId(CommandIdGetDatum(v)), "cid %d", v)
	}
}

// TestFloatRoundtripBitExact checks that the float puns preserve every bit,
// including signed zero, the infinities, and a specific NaN payload. Byte-exact
// float encoding is required for hashing and binary send to match the shards.
func TestFloatRoundtripBitExact(t *testing.T) {
	f8s := []float64{
		0, math.Copysign(0, -1), 1, -1,
		math.MaxFloat64, math.SmallestNonzeroFloat64,
		math.Inf(1), math.Inf(-1),
		math.Float64frombits(0x7FF8000000000001), // quiet NaN with a nonzero payload
		math.Float64frombits(0xFFF0000000000001), // signaling NaN, sign set
	}
	for _, f := range f8s {
		got := DatumGetFloat8(Float8GetDatum(f))
		assert.Equal(t, math.Float64bits(f), math.Float64bits(got), "float8 bits %x", math.Float64bits(f))
	}

	f4s := []float32{
		0, float32(math.Copysign(0, -1)), 1, -1,
		math.MaxFloat32, math.SmallestNonzeroFloat32,
		float32(math.Inf(1)), float32(math.Inf(-1)),
		math.Float32frombits(0x7FC00001), // quiet NaN with payload
		math.Float32frombits(0xFF800001), // signaling NaN, sign set
	}
	for _, f := range f4s {
		got := DatumGetFloat4(Float4GetDatum(f))
		assert.Equal(t, math.Float32bits(f), math.Float32bits(got), "float4 bits %x", math.Float32bits(f))
	}
}

// TestScalarsHaveNilPtr is a white-box check that by-value constructors leave ptr
// nil, so no by-value Datum ever presents a spurious pointer to the GC. This is
// the "scalars are allocation-free" property.
func TestScalarsHaveNilPtr(t *testing.T) {
	assert.Nil(t, BoolGetDatum(true).ptr)
	assert.Nil(t, Int32GetDatum(-5).ptr)
	assert.Nil(t, Int64GetDatum(math.MinInt64).ptr)
	assert.Nil(t, Float8GetDatum(math.Inf(-1)).ptr)
	assert.Nil(t, ObjectIdGetDatum(16).ptr)
}

func TestPointerRoundtrip(t *testing.T) {
	x := 42
	d := PointerGetDatum(unsafe.Pointer(&x))
	assert.Equal(t, unsafe.Pointer(&x), DatumGetPointer(d))

	assert.Nil(t, DatumGetPointer(PointerGetDatum(nil)))
}

func TestBytesRoundtrip(t *testing.T) {
	cases := [][]byte{
		nil,
		{},
		{0},
		[]byte("hello"),
		[]byte("hello\x00world"), // embedded NUL: payloads are length-delimited, not C strings
	}
	for _, b := range cases {
		got := DatumGetBytes(BytesGetDatum(b))
		if len(b) == 0 {
			assert.Nil(t, got, "empty payload reads back nil")
			continue
		}
		assert.Equal(t, b, got, "payload %q", b)
	}

	// A by-reference Datum aliases the caller's backing array rather than copying.
	b := []byte("shared")
	d := BytesGetDatum(b)
	b[0] = 'S'
	assert.Equal(t, []byte("Shared"), DatumGetBytes(d))
}

// TestBytesGCVisible is the empirical check on the risky half of the representation: a
// by-reference payload stays alive and intact after its original slice reference
// is gone and the GC has run, because ptr keeps the backing array reachable.
func TestBytesGCVisible(t *testing.T) {
	d := makeDetachedBytesDatum(1024)

	// No live reference to the original slice remains here; only d.ptr keeps the
	// backing array alive. Run the GC hard to shake out any missed pointer.
	for range 3 {
		runtime.GC()
	}

	got := DatumGetBytes(d)
	require.Len(t, got, 1024)
	for i, c := range got {
		require.Equalf(t, byte(i%251), c, "byte %d corrupted after GC", i)
	}
	runtime.KeepAlive(d)
}

// makeDetachedBytesDatum builds a payload Datum and returns it while dropping every
// other reference to the backing slice, so the returned Datum's ptr is the sole
// thing keeping the bytes alive.
//
//go:noinline
func makeDetachedBytesDatum(n int) Datum {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return BytesGetDatum(b)
}

func TestNullableDatum(t *testing.T) {
	nn := NullableDatum{Value: Int32GetDatum(7), IsNull: false}
	assert.False(t, nn.IsNull)
	assert.Equal(t, int32(7), DatumGetInt32(nn.Value))

	// The zero NullableDatum is a non-NULL zero value, matching the zero Datum.
	var zero NullableDatum
	assert.False(t, zero.IsNull)
	assert.Equal(t, int32(0), DatumGetInt32(zero.Value))

	null := NullableDatum{IsNull: true}
	assert.True(t, null.IsNull)
}
