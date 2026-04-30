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
	"encoding/binary"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
)

// Benchmarks for the pgwire read path. Symmetric to the write-side
// benchmarks: each goroutine has its own Conn fed by a cyclicReader
// that infinitely returns a pre-built buffer of pgwire bytes, so we
// measure parse/decode cost with no syscall noise.
//
// Inputs are realistic pgwire request packets the server would
// receive: Sync, Bind (1/4/16 params), Query, Parse. The cyclic
// buffer is large enough that the bufio.Reader's 16 KB window never
// stalls the loop.

// cyclicConn is a net.Conn whose Read() returns bytes from a fixed
// buffer in an endless loop. Writes are discarded. Each goroutine
// gets its own cyclicConn (because pos must be unshared); they all
// reference the same read-only data slice.
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

func newReadBenchConn(l *Listener, data []byte) *Conn {
	netConn := &cyclicConn{data: data}
	br := l.readersPool.Get().(*bufio.Reader)
	br.Reset(netConn)
	return &Conn{
		conn:           netConn,
		bufferedReader: br,
		listener:       l,
		txnStatus:      protocol.TxnStatusIdle,
		logger:         l.logger,
	}
}

// --- Input builders --------------------------------------------------------

// buildPacket assembles a single pgwire request packet:
//
//	[type byte][int32 length-including-self][body...]
func buildPacket(msgType byte, body []byte) []byte {
	out := make([]byte, 5+len(body))
	out[0] = msgType
	binary.BigEndian.PutUint32(out[1:5], uint32(4+len(body)))
	copy(out[5:], body)
	return out
}

// buildBindBody constructs the body of a Bind ('B') request with n
// parameters. Format: portal (string), stmt (string), int16
// param-format-count + format codes, int16 param-count + (int32 len +
// bytes)*, int16 result-format-count + format codes.
func buildBindBody(n int) []byte {
	body := make([]byte, 0, 64+n*16)
	body = append(body, 0)                     // empty portal name
	body = append(body, 's', 't', 'm', 't', 0) // stmt = "stmt"
	// param formats: 1 entry, value 0 (text)
	body = appendInt16(body, 1)
	body = appendInt16(body, 0)
	// param values
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
	// result formats: 1 entry, value 0 (text)
	body = appendInt16(body, 1)
	body = appendInt16(body, 0)
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

// repeatTo replicates one packet enough times to comfortably cover the
// bufio reader's window (16 KB), so the benchmark loop doesn't hit
// the cyclic boundary mid-packet on every iteration.
func repeatTo(packet []byte, minBytes int) []byte {
	repeat := max((minBytes+len(packet)-1)/len(packet), 4)
	out := make([]byte, 0, len(packet)*repeat)
	for range repeat {
		out = append(out, packet...)
	}
	return out
}

// --- Per-message benchmarks -----------------------------------------------

// One full message read = ReadMessageType + ReadMessageLength +
// readMessageBody + returnReadBuffer. Mirrors what the dispatcher
// (handleParse / handleBind / handleExecute / …) does.
func benchReadMessage(b *testing.B, msgType byte, body []byte) {
	l := newBenchListener()
	pkt := buildPacket(msgType, body)
	data := repeatTo(pkt, 64*1024)

	b.ReportAllocs()
	b.SetBytes(int64(len(pkt)))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		c := newReadBenchConn(l, data)
		for pb.Next() {
			t, err := c.ReadMessageType()
			if err != nil {
				b.Fatal(err)
			}
			_ = t
			n, err := c.ReadMessageLength()
			if err != nil {
				b.Fatal(err)
			}
			if _, err := c.readMessageBody(n); err != nil {
				b.Fatal(err)
			}
			c.returnReadBuffer()
		}
	})
}

func BenchmarkReadMessage_Sync(b *testing.B) {
	// Sync ('S') has no body — exercises the type+length read with
	// minimum payload, and stresses the per-call overhead.
	benchReadMessage(b, protocol.MsgSync, nil)
}

func BenchmarkReadMessage_Bind_1param(b *testing.B) {
	benchReadMessage(b, protocol.MsgBind, buildBindBody(1))
}

func BenchmarkReadMessage_Bind_4param(b *testing.B) {
	benchReadMessage(b, protocol.MsgBind, buildBindBody(4))
}

func BenchmarkReadMessage_Bind_16param(b *testing.B) {
	benchReadMessage(b, protocol.MsgBind, buildBindBody(16))
}

func BenchmarkReadMessage_Query_Short(b *testing.B) {
	q := append([]byte("SELECT abalance FROM pgbench_accounts WHERE aid = 12345"), 0)
	benchReadMessage(b, protocol.MsgQuery, q)
}

// --- Read + parse combined -------------------------------------------------

// BenchmarkReadAndParseBind exercises the Bind read end-to-end as
// handleBind does it: read the message, walk the body with a
// MessageReader, recycle the buffer.
func BenchmarkReadAndParseBind_4param(b *testing.B) {
	l := newBenchListener()
	body := buildBindBody(4)
	pkt := buildPacket(protocol.MsgBind, body)
	data := repeatTo(pkt, 64*1024)

	b.ReportAllocs()
	b.SetBytes(int64(len(pkt)))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		c := newReadBenchConn(l, data)
		for pb.Next() {
			if _, err := c.ReadMessageType(); err != nil {
				b.Fatal(err)
			}
			n, err := c.ReadMessageLength()
			if err != nil {
				b.Fatal(err)
			}
			buf, err := c.readMessageBody(n)
			if err != nil {
				b.Fatal(err)
			}
			r := NewMessageReader(buf)
			// portal, stmt
			if _, err := r.ReadString(); err != nil {
				b.Fatal(err)
			}
			if _, err := r.ReadString(); err != nil {
				b.Fatal(err)
			}
			// param-format count + formats
			pfc, _ := r.ReadInt16()
			for range pfc {
				if _, err := r.ReadInt16(); err != nil {
					b.Fatal(err)
				}
			}
			// param count + values
			pc, _ := r.ReadInt16()
			for range pc {
				if _, err := r.ReadByteString(); err != nil {
					b.Fatal(err)
				}
			}
			// result-format count + formats
			rfc, _ := r.ReadInt16()
			for range rfc {
				if _, err := r.ReadInt16(); err != nil {
					b.Fatal(err)
				}
			}
			c.returnReadBuffer()
		}
	})
}
