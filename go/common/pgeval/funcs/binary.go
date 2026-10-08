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
	"encoding/binary"
	"unsafe"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgeval/datum"
	"github.com/multigres/multigres/go/common/pgeval/fmgr"
	"github.com/multigres/multigres/go/common/pgeval/pgerror"
)

func init() {
	fmgr.RegisterBuiltin("int2recv", integerRecv[int16])
	fmgr.RegisterBuiltin("int4recv", integerRecv[int32])
	fmgr.RegisterBuiltin("int8recv", integerRecv[int64])
	fmgr.RegisterBuiltin("int2send", integerSend[int16])
	fmgr.RegisterBuiltin("int4send", integerSend[int32])
	fmgr.RegisterBuiltin("int8send", integerSend[int64])
	fmgr.RegisterBuiltin("boolrecv", boolrecv)
	fmgr.RegisterBuiltin("boolsend", boolsend)
}

// BinaryInput is the read-only portion of PG's StringInfo: bytes plus a cursor.
// recv functions consume their value, leaving any trailing bytes for the caller
// (e.g. a containing value's decoder). A Bind decoder must separately reject a
// nonzero Remaining() count after decoding a complete parameter.
// It is execution-local and must not be shared between concurrent calls.
// No growable output buffer is needed: send functions use encoding/binary.
type BinaryInput struct {
	data   []byte
	cursor int
}

// NewBinaryInput borrows data; callers must not mutate it while it is in use.
// An empty or nil slice is an empty input buffer, not SQL NULL.
func NewBinaryInput(data []byte) *BinaryInput { return &BinaryInput{data: data} }

// Datum returns the opaque INTERNAL argument accepted by recv builtins. The
// pointer is visible to Go's GC, including the buffer's underlying byte slice.
func (b *BinaryInput) Datum() datum.Datum {
	return datum.PointerGetDatum(unsafe.Pointer(b))
}

// Remaining returns the number of unread bytes.
func (b *BinaryInput) Remaining() int { return len(b.data) - b.cursor }

// pqformat.c:508, pq_getmsgbytes. Check before slicing or advancing the cursor.
func (b *BinaryInput) read(n int) []byte {
	if n > b.Remaining() {
		pgerror.Ereportf(mterrors.PgSSProtocolViolation, "insufficient data left in message")
	}
	result := b.data[b.cursor : b.cursor+n]
	b.cursor += n
	return result
}

// int.c:87,311; int8.c:83, REL_17_6. INTERNAL is a BinaryInput pointer, not a
// bytea Datum. Network byte order is big-endian, independent of host endianness.
func integerRecv[T integer](fcinfo fmgr.FunctionCallInfo) datum.Datum {
	input := (*BinaryInput)(datum.DatumGetPointer(fcinfo.Arg(0)))
	_, width := integerType[T]()
	data := input.read(width / 8)
	var value T
	switch width {
	case 16:
		value = T(binary.BigEndian.Uint16(data))
	case 32:
		value = T(binary.BigEndian.Uint32(data))
	default:
		value = T(binary.BigEndian.Uint64(data))
	}
	return datum.Int64GetDatum(int64(value))
}

// int.c:98,322; int8.c:94. Return bare bytea payload, without a varlena header.
func integerSend[T integer](fcinfo fmgr.FunctionCallInfo) datum.Datum {
	value := integerArg[T](fcinfo, 0)
	_, width := integerType[T]()
	var data []byte
	switch width {
	case 16:
		data = binary.BigEndian.AppendUint16(nil, uint16(value))
	case 32:
		data = binary.BigEndian.AppendUint32(nil, uint32(value))
	default:
		data = binary.BigEndian.AppendUint64(nil, uint64(value))
	}
	return datum.BytesGetDatum(data)
}

// bool.c:174. All nonzero bytes mean true, not just the canonical byte 1.
func boolrecv(fcinfo fmgr.FunctionCallInfo) datum.Datum {
	input := (*BinaryInput)(datum.DatumGetPointer(fcinfo.Arg(0)))
	// pq_getmsgbyte has a different diagnostic from pq_copymsgbytes.
	if input.Remaining() == 0 {
		pgerror.Ereportf(mterrors.PgSSProtocolViolation, "no data left in message")
	}
	return datum.BoolGetDatum(input.read(1)[0] != 0)
}

// bool.c:187. Output is canonical even if boolrecv saw another nonzero byte.
func boolsend(fcinfo fmgr.FunctionCallInfo) datum.Datum {
	value := byte(0)
	if fcinfo.GetArgBool(0) {
		value = 1
	}
	return datum.BytesGetDatum([]byte{value})
}
