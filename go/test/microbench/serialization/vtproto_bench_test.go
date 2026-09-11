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

// Package serialization benchmarks the gateway-pooler result serialization
// path. vtproto_bench_test.go isolates protobuf marshalling cost: it compares
// the reflection-based google.golang.org/protobuf marshaller against the
// generated vtprotobuf MarshalVT/UnmarshalVT/SizeVT methods on QueryResult,
// the message that carries every result row across the gRPC boundary.
//
// This measures the ceiling of the vtproto optimization in isolation, before
// wiring it end-to-end through the gRPC codec. Run with:
//
//	go test -run='^$' -bench=BenchmarkQueryResult -benchmem \
//	  ./go/test/microbench/serialization/
package serialization

import (
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/pb/query"
)

// makeQueryResult builds a QueryResult with rowCount rows of three ~16-byte
// text columns, approximating a typical wide-row result set.
func makeQueryResult(rowCount int) *query.QueryResult {
	fields := []*query.Field{
		{Name: "id", Type: "int4", TableOid: 16385},
		{Name: "name", Type: "text", TableOid: 16385},
		{Name: "created_at", Type: "timestamptz", TableOid: 16385},
	}
	const col = "0123456789abcdef" // 16 bytes
	value := []byte(col + col + col)
	lengths := []int64{16, 16, 16}

	rows := make([]*query.Row, rowCount)
	for i := range rows {
		rows[i] = &query.Row{Lengths: lengths, Values: value}
	}
	return &query.QueryResult{
		Fields:       fields,
		Rows:         rows,
		RowsAffected: uint64(rowCount),
		CommandTag:   fmt.Sprintf("SELECT %d", rowCount),
		HasFields:    true,
	}
}

var rowCounts = []int{100, 1000, 10000}

func BenchmarkQueryResultMarshal(b *testing.B) {
	for _, n := range rowCounts {
		qr := makeQueryResult(n)
		b.Run(fmt.Sprintf("rows=%d/standard", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := proto.Marshal(qr); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("rows=%d/vt", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := qr.MarshalVT(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkQueryResultUnmarshal(b *testing.B) {
	for _, n := range rowCounts {
		data, err := makeQueryResult(n).MarshalVT()
		if err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("rows=%d/standard", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := proto.Unmarshal(data, &query.QueryResult{}); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("rows=%d/vt", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				out := &query.QueryResult{}
				if err := out.UnmarshalVT(data); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
