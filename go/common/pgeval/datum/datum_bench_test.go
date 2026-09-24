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
	"testing"
)

// These benchmarks and the allocation-assertion tests alongside them are the
// empirical validation of the representation: by-value scalars round-trip with zero heap
// allocation, a slice of Datums allocates once for the slice rather than per
// element, and the rejected "any everywhere" alternative boxes on every insert.

// Package-level sinks defeat dead-code elimination so the compiler cannot fold
// the constructor/accessor pairs away.
var (
	sinkInt64 int64
	sinkDatum Datum
	sinkBytes []byte
	sinkAny   any
)

const sliceN = 1024

// TestScalarRoundtripZeroAlloc hard-asserts the core representation claim: constructing a
// Datum from a scalar and reading it back touches no heap.
func TestScalarRoundtripZeroAlloc(t *testing.T) {
	allocs := testing.AllocsPerRun(1000, func() {
		sinkInt64 = DatumGetInt64(Int64GetDatum(sinkInt64 + 1))
	})
	if allocs != 0 {
		t.Fatalf("Int64 Datum roundtrip allocated %v times/op, want 0", allocs)
	}
}

// TestDatumSliceFillZeroPerElemAlloc asserts that filling a preallocated
// []Datum with scalars allocates nothing per element (contrast: a []any would
// box each int).
func TestDatumSliceFillZeroPerElemAlloc(t *testing.T) {
	buf := make([]Datum, sliceN)
	allocs := testing.AllocsPerRun(1000, func() {
		for i := range buf {
			buf[i] = Int64GetDatum(int64(i))
		}
	})
	if allocs != 0 {
		t.Fatalf("filling []Datum allocated %v times/op, want 0", allocs)
	}
	sinkDatum = buf[sliceN-1]
}

func BenchmarkInt64Roundtrip(b *testing.B) {
	b.ReportAllocs()
	var v int64
	for b.Loop() {
		v = DatumGetInt64(Int64GetDatum(v + 1))
	}
	sinkInt64 = v
}

func BenchmarkFloat8Roundtrip(b *testing.B) {
	b.ReportAllocs()
	var d Datum
	for b.Loop() {
		d = Float8GetDatum(DatumGetFloat8(d) + 1)
	}
	sinkDatum = d
}

func BenchmarkDatumSliceFill(b *testing.B) {
	b.ReportAllocs()
	buf := make([]Datum, sliceN)
	for b.Loop() {
		for j := range buf {
			buf[j] = Int64GetDatum(int64(j))
		}
	}
	sinkDatum = buf[sliceN-1]
}

// BenchmarkAnyBoxingFill is the rejected alternative: an int64 in an any
// boxes onto the heap. Kept as a contrast baseline for the PR's numbers.
func BenchmarkAnyBoxingFill(b *testing.B) {
	b.ReportAllocs()
	buf := make([]any, sliceN)
	for b.Loop() {
		for j := range buf {
			buf[j] = int64(j)
		}
	}
	sinkAny = buf[sliceN-1]
}

func BenchmarkBytesRoundtrip(b *testing.B) {
	b.ReportAllocs()
	payload := []byte("the quick brown fox jumps over the lazy dog")
	var out []byte
	for b.Loop() {
		out = DatumGetBytes(BytesGetDatum(payload))
	}
	sinkBytes = out
}
