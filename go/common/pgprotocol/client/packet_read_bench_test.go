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
	"encoding/binary"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
)

// Read-path benchmarks for the multipooler-side client. Symmetric to
// the server-side read-bench suite. Inputs represent realistic
// postgres-side responses: ReadyForQuery, DataRow, RowDescription,
// CommandComplete.

type cyclicConn struct {
	data []byte
	pos  int
}

func (c *cyclicConn) Read(p []byte) (int, error) {
	n := 0
	for n < len(p) {
		if c.pos >= len(c.data) {
			c.pos = 0
		}
		m := copy(p[n:], c.data[c.pos:])
		n += m
		c.pos += m
	}
	return n, nil
}

func (c *cyclicConn) Write(p []byte) (int, error)        { return len(p), nil }
func (c *cyclicConn) Close() error                       { return nil }
func (c *cyclicConn) LocalAddr() net.Addr                { return nil }
func (c *cyclicConn) RemoteAddr() net.Addr               { return nil }
func (c *cyclicConn) SetDeadline(t time.Time) error      { return nil }
func (c *cyclicConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *cyclicConn) SetWriteDeadline(t time.Time) error { return nil }

func newReadBenchConn(data []byte) *Conn {
	netConn := &cyclicConn{data: data}
	return &Conn{
		conn:           netConn,
		bufferedReader: bufio.NewReaderSize(netConn, connBufferSize),
		bufferedWriter: bufio.NewWriterSize(netConn, connBufferSize),
	}
}

// --- Input builders --------------------------------------------------------

func buildPacket(msgType byte, body []byte) []byte {
	out := make([]byte, 5+len(body))
	out[0] = msgType
	binary.BigEndian.PutUint32(out[1:5], uint32(4+len(body)))
	copy(out[5:], body)
	return out
}

// buildDataRowBody constructs a DataRow body with n columns, mixing
// short integer-text values and a longer string value.
func buildDataRowBody(n int) []byte {
	body := make([]byte, 0, 2+n*16)
	body = appendInt16(body, int16(n))
	for i := range n {
		var v []byte
		if i%4 == 3 {
			v = []byte("alpha-bravo-charlie")
		} else {
			v = []byte(strconv.Itoa(1_000_000 + i))
		}
		body = appendInt32(body, int32(len(v)))
		body = append(body, v...)
	}
	return body
}

func appendInt16(b []byte, v int16) []byte {
	var x [2]byte
	binary.BigEndian.PutUint16(x[:], uint16(v))
	return append(b, x[:]...)
}

func appendInt32(b []byte, v int32) []byte {
	var x [4]byte
	binary.BigEndian.PutUint32(x[:], uint32(v))
	return append(b, x[:]...)
}

func repeatTo(packet []byte, minBytes int) []byte {
	repeat := max((minBytes+len(packet)-1)/len(packet), 4)
	out := make([]byte, 0, len(packet)*repeat)
	for range repeat {
		out = append(out, packet...)
	}
	return out
}

// --- Per-message benchmarks -----------------------------------------------

func benchClientReadMessage(b *testing.B, msgType byte, body []byte) {
	pkt := buildPacket(msgType, body)
	data := repeatTo(pkt, 64*1024)

	b.ReportAllocs()
	b.SetBytes(int64(len(pkt)))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		c := newReadBenchConn(data)
		for pb.Next() {
			_, _, err := c.readMessage()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// ReadyForQuery is a 6-byte packet (5-byte header + 1-byte status) —
// the smallest realistic message the client reads.
func BenchmarkClientReadMessage_ReadyForQuery(b *testing.B) {
	benchClientReadMessage(b, protocol.MsgReadyForQuery, []byte{'I'})
}

// CommandComplete carries a short tag like "SELECT 1\0".
func BenchmarkClientReadMessage_CommandComplete(b *testing.B) {
	benchClientReadMessage(b, protocol.MsgCommandComplete, []byte("SELECT 1\x00"))
}

func BenchmarkClientReadMessage_DataRow_1col(b *testing.B) {
	benchClientReadMessage(b, protocol.MsgDataRow, buildDataRowBody(1))
}

func BenchmarkClientReadMessage_DataRow_4col(b *testing.B) {
	benchClientReadMessage(b, protocol.MsgDataRow, buildDataRowBody(4))
}

func BenchmarkClientReadMessage_DataRow_16col(b *testing.B) {
	benchClientReadMessage(b, protocol.MsgDataRow, buildDataRowBody(16))
}

// --- Read + parse combined -------------------------------------------------

// BenchmarkClientReadAndParseDataRow walks every column value with a
// MessageReader, mirroring what query.go does after each DataRow.
func BenchmarkClientReadAndParseDataRow_4col(b *testing.B) {
	body := buildDataRowBody(4)
	pkt := buildPacket(protocol.MsgDataRow, body)
	data := repeatTo(pkt, 64*1024)

	b.ReportAllocs()
	b.SetBytes(int64(len(pkt)))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		c := newReadBenchConn(data)
		for pb.Next() {
			_, body, err := c.readMessage()
			if err != nil {
				b.Fatal(err)
			}
			r := NewMessageReader(body)
			n, _ := r.ReadInt16()
			for range n {
				if _, err := r.ReadByteString(); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}
