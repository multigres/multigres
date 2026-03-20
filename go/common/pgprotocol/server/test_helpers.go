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

// NewTestConn creates a Conn suitable for testing.
// readBuf contains data that will be read by the Conn (simulating client input).
// The returned TestConn includes WriteBuf to inspect what was written.
func NewTestConn(readBuf *bytes.Buffer) *TestConn {
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

	return &TestConn{
		Conn:     conn,
		WriteBuf: writeBuf,
	}
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
