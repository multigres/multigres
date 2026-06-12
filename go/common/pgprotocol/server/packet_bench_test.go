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

package server

import (
	"bufio"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/bufpool"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"
)

// Benchmarks for the pgwire write path in isolation.
//
// These exercise only the packet-construction code (writeRowDescription,
// writeDataRow, writeCommandComplete, writeReadyForQuery, writeError, …)
// against a discard-backed net.Conn so neither real syscalls nor TCP cost
// up. Use them to measure encoder-only changes — pool layout, in-place
// encoding, header coalescing — without dragging in pgbench / multipooler.
//
// Each benchmark uses b.RunParallel with one Conn per goroutine, mimicking
// the production layout where each pgwire client has its own Conn but all
// share the listener-level bufpool / writer pool.

// discardConn is a net.Conn whose Write() drops the bytes and reports them
// as written. We don't want to measure socket syscalls here; the point is
// the encoder and buffered-writer path.
type discardConn struct {
	bytesWritten uint64
}

func (d *discardConn) Read(b []byte) (int, error) { return 0, io.EOF }
func (d *discardConn) Write(b []byte) (int, error) {
	atomic.AddUint64(&d.bytesWritten, uint64(len(b)))
	return len(b), nil
}
func (d *discardConn) Close() error                       { return nil }
func (d *discardConn) LocalAddr() net.Addr                { return nil }
func (d *discardConn) RemoteAddr() net.Addr               { return nil }
func (d *discardConn) SetDeadline(t time.Time) error      { return nil }
func (d *discardConn) SetReadDeadline(t time.Time) error  { return nil }
func (d *discardConn) SetWriteDeadline(t time.Time) error { return nil }

// newBenchListener builds a Listener stub with the same bufpool/writer pool
// configuration as the real one so startPacket / writePacket take their
// real pooling paths.
func newBenchListener() *Listener {
	return &Listener{
		logger: slog.New(slog.DiscardHandler),
		writersPool: &sync.Pool{
			New: func() any { return bufio.NewWriterSize(nil, connBufferSize) },
		},
		readersPool: &sync.Pool{
			New: func() any { return bufio.NewReaderSize(nil, connBufferSize) },
		},
		bufPool: bufpool.New(16*1024, 64*1024*1024),
	}
}

// newBenchConn builds a Conn wired to a discard net.Conn, with a buffered
// writer drawn from the listener pool. Each parallel goroutine gets its
// own Conn to mirror the production "one Conn per client" layout — the
// bufpool and writer pool are shared across goroutines, which is also how
// production runs.
func newBenchConn(l *Listener) *Conn {
	netConn := &discardConn{}
	bw := l.writersPool.Get().(*bufio.Writer)
	bw.Reset(netConn)
	return &Conn{
		conn:           netConn,
		bufferedWriter: bw,
		listener:       l,
		txnStatus:      protocol.TxnStatusIdle,
		logger:         l.logger,
	}
}

// --- Fixtures: representative payloads -------------------------------------

// pgbenchRowDesc mirrors what pgbench's TPC-B "SELECT abalance FROM
// pgbench_accounts WHERE aid = …" returns — one int4 column.
func pgbenchRowDesc() []*query.Field {
	return []*query.Field{
		{
			Name:                 "abalance",
			TableOid:             16384,
			TableAttributeNumber: 4,
			DataTypeOid:          23, // int4
			DataTypeSize:         4,
			TypeModifier:         -1,
			Format:               0,
		},
	}
}

func makeRowDesc(n int) []*query.Field {
	fields := make([]*query.Field, n)
	for i := range fields {
		fields[i] = &query.Field{
			Name:                 "col_" + strconv.Itoa(i),
			TableOid:             uint32(16384 + i),
			TableAttributeNumber: int32(i + 1),
			DataTypeOid:          23,
			DataTypeSize:         4,
			TypeModifier:         -1,
			Format:               0,
		}
	}
	return fields
}

func makeRow(n int) *sqltypes.Row {
	values := make([]sqltypes.Value, n)
	for i := range values {
		// Mix sizes a bit: short integer text reps and a longer string.
		if i%4 == 3 {
			values[i] = []byte("alpha-bravo-charlie")
		} else {
			values[i] = []byte(strconv.Itoa(1_000_000 + i))
		}
	}
	return &sqltypes.Row{Values: values}
}

func benchDiagnostic() *mterrors.PgDiagnostic {
	return mterrors.NewPgError("ERROR", mterrors.PgSSInternalError,
		"relation \"pgbench_accounts\" does not exist", "double-check the schema")
}

// --- Per-packet benchmarks --------------------------------------------------

func BenchmarkWriteRowDescription_1col(b *testing.B) {
	benchRowDescription(b, makeRowDesc(1))
}

func BenchmarkWriteRowDescription_4col(b *testing.B) {
	benchRowDescription(b, makeRowDesc(4))
}

func BenchmarkWriteRowDescription_16col(b *testing.B) {
	benchRowDescription(b, makeRowDesc(16))
}

func benchRowDescription(b *testing.B, fields []*query.Field) {
	l := newBenchListener()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		c := newBenchConn(l)
		for pb.Next() {
			if err := c.writeRowDescription(fields); err != nil {
				b.Fatal(err)
			}
		}
		_ = c.bufferedWriter.Flush()
	})
}

func BenchmarkWriteDataRow_1col(b *testing.B) {
	benchDataRow(b, makeRow(1))
}

func BenchmarkWriteDataRow_4col(b *testing.B) {
	benchDataRow(b, makeRow(4))
}

func BenchmarkWriteDataRow_16col(b *testing.B) {
	benchDataRow(b, makeRow(16))
}

func benchDataRow(b *testing.B, row *sqltypes.Row) {
	l := newBenchListener()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		c := newBenchConn(l)
		for pb.Next() {
			if err := c.writeDataRow(row); err != nil {
				b.Fatal(err)
			}
		}
		_ = c.bufferedWriter.Flush()
	})
}

func BenchmarkWriteCommandComplete(b *testing.B) {
	l := newBenchListener()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		c := newBenchConn(l)
		for pb.Next() {
			if err := c.writeCommandComplete("SELECT 1"); err != nil {
				b.Fatal(err)
			}
		}
		_ = c.bufferedWriter.Flush()
	})
}

func BenchmarkWriteReadyForQuery(b *testing.B) {
	l := newBenchListener()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		c := newBenchConn(l)
		for pb.Next() {
			if err := c.writeReadyForQuery(); err != nil {
				b.Fatal(err)
			}
		}
		_ = c.bufferedWriter.Flush()
	})
}

func BenchmarkWriteParameterDescription(b *testing.B) {
	params := []*query.ParameterDescription{
		{DataTypeOid: 23},
		{DataTypeOid: 25},
		{DataTypeOid: 1043},
	}
	l := newBenchListener()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		c := newBenchConn(l)
		for pb.Next() {
			if err := c.writeParameterDescription(params); err != nil {
				b.Fatal(err)
			}
		}
		_ = c.bufferedWriter.Flush()
	})
}

func BenchmarkWriteErrorResponse(b *testing.B) {
	diag := benchDiagnostic()
	l := newBenchListener()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		c := newBenchConn(l)
		for pb.Next() {
			if err := c.writePgDiagnosticResponse(protocol.MsgErrorResponse, diag); err != nil {
				b.Fatal(err)
			}
		}
		_ = c.bufferedWriter.Flush()
	})
}

func BenchmarkWriteEmptyQueryResponse(b *testing.B) {
	l := newBenchListener()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		c := newBenchConn(l)
		for pb.Next() {
			if err := c.writeEmptyQueryResponse(); err != nil {
				b.Fatal(err)
			}
		}
		_ = c.bufferedWriter.Flush()
	})
}

// --- Composite: full pgbench-shape response --------------------------------

// BenchmarkPgbenchResponse builds the full per-query response that pgbench
// TPC-B sees on the SELECT step: RowDescription + DataRow + CommandComplete
// + ReadyForQuery + flush. This is the unit that matters for pgbench TPS.
func BenchmarkPgbenchResponse_1col(b *testing.B) {
	benchPgbenchResponse(b, 1)
}

func BenchmarkPgbenchResponse_4col(b *testing.B) {
	benchPgbenchResponse(b, 4)
}

func benchPgbenchResponse(b *testing.B, ncols int) {
	fields := makeRowDesc(ncols)
	row := makeRow(ncols)
	if ncols == 1 {
		fields = pgbenchRowDesc()
	}
	l := newBenchListener()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		c := newBenchConn(l)
		for pb.Next() {
			if err := c.writeRowDescription(fields); err != nil {
				b.Fatal(err)
			}
			if err := c.writeDataRow(row); err != nil {
				b.Fatal(err)
			}
			if err := c.writeCommandComplete("SELECT 1"); err != nil {
				b.Fatal(err)
			}
			if err := c.writeReadyForQuery(); err != nil {
				b.Fatal(err)
			}
			if err := c.flush(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkPgbenchResponse_ManyRows exercises a result set with multiple
// DataRows, which is closer to a non-pgbench OLTP read.
func BenchmarkPgbenchResponse_4col_100rows(b *testing.B) {
	fields := makeRowDesc(4)
	row := makeRow(4)
	l := newBenchListener()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		c := newBenchConn(l)
		for pb.Next() {
			if err := c.writeRowDescription(fields); err != nil {
				b.Fatal(err)
			}
			for range 100 {
				if err := c.writeDataRow(row); err != nil {
					b.Fatal(err)
				}
			}
			if err := c.writeCommandComplete("SELECT 100"); err != nil {
				b.Fatal(err)
			}
			if err := c.writeReadyForQuery(); err != nil {
				b.Fatal(err)
			}
			if err := c.flush(); err != nil {
				b.Fatal(err)
			}
		}
	})
}
