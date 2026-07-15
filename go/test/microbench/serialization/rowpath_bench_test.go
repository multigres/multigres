// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package serialization holds micro-benchmarks for the gateway<->pooler
// result-row hot path. They measure the cost of moving a batch of result
// rows from the pooler to the client under two representations:
//
//   - Columnar (current): pooler builds sqltypes.Result -> ToProto ->
//     proto.Marshal on the wire -> gateway proto.Unmarshal -> ResultFromProto
//     -> re-emit each row as a PostgreSQL DataRow frame to the client. This is
//     the "triple serialization" described in Problem 2 of
//     docs/query_serving/client_session_stream_design.md.
//
//   - Opaque (proposed): the pooler already holds each row as a raw PostgreSQL
//     DataRow frame (that is what the backend hands it). It copies the frames
//     into one batch blob, marshals a single carrier message, the gateway
//     unmarshals it and writes the blob straight to the client socket. No
//     per-column re-framing on either side.
//
// The opaque side is a faithful model of the proposal, not the eventual
// production code: it reuses the generated query.Row message as the carrier
// (Lengths = per-frame byte lengths, Values = the concatenated frames) so the
// benchmark exercises real protobuf marshalling rather than a toy. Compare the
// two with:
//
//	go test -bench=BenchmarkResultPath -benchmem ./go/test/microbench/serialization/
//
// Watch B/op and allocs/op as much as ns/op: Problem 2's win shows up in
// allocations and copies before it shows up in wall-clock latency.
package serialization

import (
	"encoding/binary"
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/sqltypes"
	querypb "github.com/multigres/multigres/go/pb/query"
)

// rowShapes are representative result-row widths. Each entry is a column-width
// profile in bytes; -1 marks a NULL column. These loosely mirror a mixed OLTP
// row (a few ints, a couple of short strings, a wide text column, a NULL).
var rowShapes = [][]int{
	{4, 4, 12, 24, 64, -1, 8, 128},
}

// makeSQLResult builds an sqltypes.Result with nRows rows following the first
// row shape, filled with deterministic bytes (no randomness -- Math.random and
// time are avoided so the benchmark is reproducible).
func makeSQLResult(nRows int) *sqltypes.Result {
	shape := rowShapes[0]
	fields := make([]*querypb.Field, len(shape))
	for i := range shape {
		fields[i] = &querypb.Field{Name: fmt.Sprintf("c%d", i), Type: "text"}
	}
	rows := make([]*sqltypes.Row, nRows)
	for r := range nRows {
		vals := make([]sqltypes.Value, len(shape))
		for c, w := range shape {
			if w < 0 {
				vals[c] = nil
				continue
			}
			b := make([]byte, w)
			for k := range b {
				b[k] = byte('a' + (r+c+k)%26)
			}
			vals[c] = b
		}
		rows[r] = &sqltypes.Row{Values: vals}
	}
	return &sqltypes.Result{
		Fields:     fields,
		Rows:       rows,
		CommandTag: fmt.Sprintf("SELECT %d", nRows),
	}
}

// encodeDataRow writes one row as a PostgreSQL DataRow ('D') message body,
// matching the layout in go/common/pgprotocol/server/query.go writeDataRow:
// int16 column count, then per column int32 length (-1 for NULL) + bytes. The
// leading 'D' byte and int32 packet length are included so the blob models what
// actually crosses the socket.
func encodeDataRow(dst []byte, values []sqltypes.Value) []byte {
	bodyLen := 4 + 2 // packet length field + column count
	for _, v := range values {
		bodyLen += 4
		if v != nil {
			bodyLen += len(v)
		}
	}
	start := len(dst)
	dst = append(dst, 'D')
	dst = binary.BigEndian.AppendUint32(dst, uint32(bodyLen))
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(values)))
	for _, v := range values {
		if v == nil {
			dst = binary.BigEndian.AppendUint32(dst, ^uint32(0)) // -1
			continue
		}
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(v)))
		dst = append(dst, v...)
	}
	_ = start
	return dst
}

// BenchmarkResultPath_Columnar measures the current path: ToProto, marshal,
// unmarshal, FromProto, then re-emit each row as a DataRow frame to a client
// buffer. This is what a gateway does per result batch today.
func BenchmarkResultPath_Columnar(b *testing.B) {
	for _, nRows := range []int{1, 100, 10000} {
		res := makeSQLResult(nRows)
		b.Run(fmt.Sprintf("rows=%d", nRows), func(b *testing.B) {
			b.ReportAllocs()
			var clientBuf []byte
			for i := 0; i < b.N; i++ {
				// Pooler side: sqltypes.Result -> proto -> wire.
				pr := res.ToProto()
				wire, err := proto.Marshal(pr)
				if err != nil {
					b.Fatal(err)
				}
				// Gateway side: wire -> proto -> sqltypes.Result -> DataRow frames.
				var got querypb.QueryResult
				if err := proto.Unmarshal(wire, &got); err != nil {
					b.Fatal(err)
				}
				back := sqltypes.ResultFromProto(&got)
				clientBuf = clientBuf[:0]
				for _, row := range back.Rows {
					clientBuf = encodeDataRow(clientBuf, row.Values)
				}
			}
			_ = clientBuf
		})
	}
}

// BenchmarkResultPath_Opaque measures the proposed path: the pooler holds raw
// DataRow frames, concatenates them into one batch blob carried by a single
// message, the gateway unmarshals and writes the blob straight through.
func BenchmarkResultPath_Opaque(b *testing.B) {
	for _, nRows := range []int{1, 100, 10000} {
		res := makeSQLResult(nRows)
		// Pre-build the raw frames the pooler would already have from the
		// backend; framing cost is not part of the gateway<->pooler hop.
		frames := make([][]byte, len(res.Rows))
		for i, row := range res.Rows {
			frames[i] = encodeDataRow(nil, row.Values)
		}
		b.Run(fmt.Sprintf("rows=%d", nRows), func(b *testing.B) {
			b.ReportAllocs()
			var clientBuf []byte
			for i := 0; i < b.N; i++ {
				// Pooler side: copy frames into one blob, carry as opaque bytes.
				lengths := make([]int64, len(frames))
				total := 0
				for j, f := range frames {
					lengths[j] = int64(len(f))
					total += len(f)
				}
				blob := make([]byte, 0, total)
				for _, f := range frames {
					blob = append(blob, f...)
				}
				carrier := &querypb.Row{Lengths: lengths, Values: blob}
				wire, err := proto.Marshal(carrier)
				if err != nil {
					b.Fatal(err)
				}
				// Gateway side: unmarshal, write the blob straight to the client.
				var got querypb.Row
				if err := proto.Unmarshal(wire, &got); err != nil {
					b.Fatal(err)
				}
				clientBuf = clientBuf[:0]
				clientBuf = append(clientBuf, got.Values...)
			}
			_ = clientBuf
		})
	}
}
