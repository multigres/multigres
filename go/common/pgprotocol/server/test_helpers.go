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
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"sync"
	"time"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
)

// TestConn is a test-only wrapper around Conn that provides access to the write buffer.
type TestConn struct {
	*Conn
	WriteBuf *bytes.Buffer
}

// exportedTestNetConn is a minimal implementation of net.Conn for testing.
type exportedTestNetConn struct {
	readBuf  *bytes.Buffer
	writeBuf *bytes.Buffer
}

func (m *exportedTestNetConn) Read(b []byte) (n int, err error) {
	return m.readBuf.Read(b)
}

func (m *exportedTestNetConn) Write(b []byte) (n int, err error) {
	return m.writeBuf.Write(b)
}

func (m *exportedTestNetConn) Close() error                       { return nil }
func (m *exportedTestNetConn) LocalAddr() net.Addr                { return nil }
func (m *exportedTestNetConn) RemoteAddr() net.Addr               { return nil }
func (m *exportedTestNetConn) SetDeadline(t time.Time) error      { return nil }
func (m *exportedTestNetConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *exportedTestNetConn) SetWriteDeadline(t time.Time) error { return nil }

// TestConnOption configures a TestConn.
type TestConnOption func(*Conn)

// WithTestHandler sets the handler on a test connection.
func WithTestHandler(h Handler) TestConnOption {
	return func(c *Conn) { c.handler = h }
}

// WithTestDatabase sets the database name on a test connection.
func WithTestDatabase(db string) TestConnOption {
	return func(c *Conn) { c.database = db }
}

// WithTestReplicationMode sets the replication mode on a test connection.
func WithTestReplicationMode(mode ReplicationMode) TestConnOption {
	return func(c *Conn) { c.replicationMode = mode }
}

// WithTestUser sets the authenticated user on a test connection.
func WithTestUser(user string) TestConnOption {
	return func(c *Conn) { c.user = user }
}

// WithTestScramKeys sets the SCRAM passthrough keys on a test connection.
func WithTestScramKeys(clientKey, serverKey []byte) TestConnOption {
	return func(c *Conn) {
		c.scramClientKey = clientKey
		c.scramServerKey = serverKey
	}
}

// WithTestNetConn replaces the underlying net.Conn on a test connection. Used
// to drive DetachConn / replication-tunnel tests over a real bidirectional
// pipe instead of the bytes.Buffer-backed default.
func WithTestNetConn(nc net.Conn) TestConnOption {
	return func(c *Conn) { c.conn = nc }
}

// NewTestConn creates a Conn suitable for testing.
// readBuf contains data that will be read by the Conn (simulating client input).
// The returned TestConn includes WriteBuf to inspect what was written.
func NewTestConn(readBuf *bytes.Buffer, opts ...TestConnOption) *TestConn {
	writeBuf := &bytes.Buffer{}
	netConn := &exportedTestNetConn{readBuf: readBuf, writeBuf: writeBuf}

	ctx, cancel := context.WithCancel(context.TODO())
	_ = cancel // caller can cancel via conn.Close() if needed

	conn := &Conn{
		conn:           netConn,
		bufferedReader: bufio.NewReader(readBuf),
		bufferedWriter: bufio.NewWriter(writeBuf),
		txnStatus:      protocol.TxnStatusIdle,
		ctx:            ctx,
		cancel:         cancel,
	}
	for _, opt := range opts {
		opt(conn)
	}

	return &TestConn{
		Conn:     conn,
		WriteBuf: writeBuf,
	}
}

// NewReplicationTestConn builds a Conn suitable for replication-tunnel tests.
// Unlike NewTestConn it does not pre-seed a read buffer; callers supply a real
// net.Conn via WithTestNetConn so DetachConn can hijack a live bidirectional
// pipe. The bufferedReader/bufferedWriter are wired to that net.Conn after the
// options run, and the conn is given a minimal listener with the buffer pools
// DetachConn returns those buffers to.
func NewReplicationTestConn(opts ...TestConnOption) *TestConn {
	ctx, cancel := context.WithCancel(context.TODO())

	l := &Listener{
		readersPool: &sync.Pool{New: func() any { return bufio.NewReaderSize(nil, connBufferSize) }},
		writersPool: &sync.Pool{New: func() any { return bufio.NewWriterSize(nil, connBufferSize) }},
	}

	conn := &Conn{
		listener:  l,
		txnStatus: protocol.TxnStatusIdle,
		ctx:       ctx,
		cancel:    cancel,
	}
	for _, opt := range opts {
		opt(conn)
	}
	if conn.conn != nil {
		conn.bufferedReader = l.readersPool.Get().(*bufio.Reader)
		conn.bufferedReader.Reset(conn.conn)
		conn.bufferedWriter = l.writersPool.Get().(*bufio.Writer)
		conn.bufferedWriter.Reset(conn.conn)
	}

	return &TestConn{Conn: conn}
}

// WriteCopyDataMessage writes a CopyData message to the buffer.
// This simulates a client sending COPY data.
func WriteCopyDataMessage(buf *bytes.Buffer, data []byte) {
	buf.WriteByte(protocol.MsgCopyData)
	length := uint32(4 + len(data)) // length includes itself
	_ = binary.Write(buf, binary.BigEndian, length)
	buf.Write(data)
}

// WriteCopyDoneMessage writes a CopyDone message to the buffer.
// This simulates a client signaling end of COPY data.
func WriteCopyDoneMessage(buf *bytes.Buffer) {
	buf.WriteByte(protocol.MsgCopyDone)
	length := uint32(4) // length includes itself, no body
	_ = binary.Write(buf, binary.BigEndian, length)
}

// WriteCopyFailMessage writes a CopyFail message to the buffer.
// This simulates a client aborting a COPY operation with an error message.
func WriteCopyFailMessage(buf *bytes.Buffer, errMsg string) {
	buf.WriteByte(protocol.MsgCopyFail)
	msgBytes := append([]byte(errMsg), 0) // null-terminated
	length := uint32(4 + len(msgBytes))   // length includes itself
	_ = binary.Write(buf, binary.BigEndian, length)
	buf.Write(msgBytes)
}
