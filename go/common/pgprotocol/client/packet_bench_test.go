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

package client

import (
	"bufio"
	"io"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// Benchmarks for the multipooler-side pgwire write path. Symmetric to
// the server-side benchmarks in pgprotocol/server/packet_bench_test.go.
// Each Conn talks to a discardConn so syscalls don't dominate; what we
// measure is encoder + bufio cost.

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

func newBenchConn() *Conn {
	netConn := &discardConn{}
	c := &Conn{
		conn:           netConn,
		bufferedReader: bufio.NewReaderSize(netConn, connBufferSize),
		bufferedWriter: bufio.NewWriterSize(netConn, connBufferSize),
	}
	return c
}

func makeParams(n int) [][]byte {
	params := make([][]byte, n)
	for i := range params {
		if i%4 == 3 {
			params[i] = []byte("alpha-bravo-charlie")
		} else {
			params[i] = []byte(strconv.Itoa(1_000_000 + i))
		}
	}
	return params
}

// --- Per-message benchmarks ------------------------------------------------

func BenchmarkClientWriteQuery_Short(b *testing.B) {
	q := "SELECT abalance FROM pgbench_accounts WHERE aid = 12345"
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		c := newBenchConn()
		for pb.Next() {
			if err := c.writeQueryMessage(q); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkClientWriteParse(b *testing.B) {
	q := "SELECT abalance FROM pgbench_accounts WHERE aid = $1"
	paramTypes := []uint32{23}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		c := newBenchConn()
		for pb.Next() {
			if err := c.writeParse("stmt", q, paramTypes); err != nil {
				b.Fatal(err)
			}
		}
		_ = c.bufferedWriter.Flush()
	})
}

func BenchmarkClientWriteBind_1param(b *testing.B) {
	benchClientBind(b, 1)
}

func BenchmarkClientWriteBind_4param(b *testing.B) {
	benchClientBind(b, 4)
}

func BenchmarkClientWriteBind_16param(b *testing.B) {
	benchClientBind(b, 16)
}

func benchClientBind(b *testing.B, n int) {
	params := makeParams(n)
	paramFormats := []int16{0}
	resultFormats := []int16{0}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		c := newBenchConn()
		for pb.Next() {
			if err := c.writeBind("", "stmt", params, paramFormats, resultFormats); err != nil {
				b.Fatal(err)
			}
		}
		_ = c.bufferedWriter.Flush()
	})
}

func BenchmarkClientWriteExecute(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		c := newBenchConn()
		for pb.Next() {
			if err := c.writeExecute("", 0); err != nil {
				b.Fatal(err)
			}
		}
		_ = c.bufferedWriter.Flush()
	})
}

func BenchmarkClientWriteDescribe(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		c := newBenchConn()
		for pb.Next() {
			if err := c.writeDescribe('S', "stmt"); err != nil {
				b.Fatal(err)
			}
		}
		_ = c.bufferedWriter.Flush()
	})
}

func BenchmarkClientWriteSync(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		c := newBenchConn()
		for pb.Next() {
			if err := c.writeSync(); err != nil {
				b.Fatal(err)
			}
		}
		_ = c.bufferedWriter.Flush()
	})
}

// --- Composite: full extended-protocol pgbench-shape pipeline ---------------

// BenchmarkClientPgbenchPipeline emits the message sequence multipooler
// sends to postgres for a single pgbench prepared-protocol query:
// Parse + Bind + Describe + Execute + Sync, then flush.
func BenchmarkClientPgbenchPipeline(b *testing.B) {
	q := "SELECT abalance FROM pgbench_accounts WHERE aid = $1"
	paramTypes := []uint32{23}
	params := [][]byte{[]byte("12345")}
	paramFormats := []int16{0}
	resultFormats := []int16{0}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		c := newBenchConn()
		for pb.Next() {
			if err := c.writeParse("", q, paramTypes); err != nil {
				b.Fatal(err)
			}
			if err := c.writeBind("", "", params, paramFormats, resultFormats); err != nil {
				b.Fatal(err)
			}
			if err := c.writeDescribe('P', ""); err != nil {
				b.Fatal(err)
			}
			if err := c.writeExecute("", 0); err != nil {
				b.Fatal(err)
			}
			if err := c.writeSync(); err != nil {
				b.Fatal(err)
			}
			if err := c.flush(); err != nil {
				b.Fatal(err)
			}
		}
	})
}
