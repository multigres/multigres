// Copyright 2025 Supabase, Inc.
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
	"encoding/binary"
	"fmt"
	"io"

	"github.com/multigres/multigres/go/common/pgprotocol/bufpool"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
)

// bufPool is the package-level buffer pool used by the slow path of
// startPacket. The client side has no per-listener context (each Conn
// is created standalone via Connect), so the pool lives at package
// scope and is shared across all client connections in a process.
//
// Sized to match the server side: 16 KB minimum (one bufio buffer
// worth), 64 MB maximum. Power-of-two buckets in between.
var bufPool = bufpool.New(16*1024, 64*1024*1024)

// Reading utilities

// readMessageType reads a single byte message type from the connection.
func (c *Conn) readMessageType() (byte, error) {
	return c.bufferedReader.ReadByte()
}

// readMessageLength reads the 4-byte message length from the connection.
// The length includes itself but excludes the message type byte.
// Returns the length of the message body (length - 4).
//
// Uses Peek + Discard to read the 4 bytes directly out of bufio's
// internal buffer — vs the previous io.ReadFull(io.Reader, []byte)
// shape, which forced the stack-local slice header to the heap on
// every call due to the io.Reader interface dispatch.
func (c *Conn) readMessageLength() (int, error) {
	hdr, err := c.bufferedReader.Peek(4)
	if err != nil {
		return 0, err
	}
	length := binary.BigEndian.Uint32(hdr)
	if _, err := c.bufferedReader.Discard(4); err != nil {
		return 0, err
	}
	if length < 4 {
		return 0, fmt.Errorf("invalid message length: %d", length)
	}
	return int(length - 4), nil
}

// readMessageBody reads the message body of the given length.
//
// Note: the client side intentionally does NOT pool body buffers
// (unlike the write path or the server-side read path). The reason
// is that parseDataRow returns sqltypes.Value slices that alias
// into the body buffer, and those values accumulate across multiple
// readMessage calls before being handed up to the user callback.
// Recycling the body would require copying every column value
// individually — strictly more allocations than the current
// single-make-per-body pattern. If we ever change parseDataRow to
// fully copy values, we can pool here too.
func (c *Conn) readMessageBody(length int) ([]byte, error) {
	if length == 0 {
		return nil, nil
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(c.bufferedReader, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// readMessage reads a complete message (type, length, body).
func (c *Conn) readMessage() (byte, []byte, error) {
	msgType, err := c.readMessageType()
	if err != nil {
		return 0, nil, err
	}

	bodyLen, err := c.readMessageLength()
	if err != nil {
		return 0, nil, err
	}

	body, err := c.readMessageBody(bodyLen)
	if err != nil {
		return 0, nil, err
	}

	return msgType, body, nil
}

// ReadRawMessage reads a single protocol message (type byte + body).
// The caller is responsible for message classification and parsing.
// This enables split read/write patterns where the reader goroutine
// handles all incoming message types (e.g., PubSubListener).
func (c *Conn) ReadRawMessage() (byte, []byte, error) {
	return c.readMessage()
}

// Writing utilities

// startPacket reserves space for a single pgwire packet of the given
// body length, with the message type and 4-byte length header pre-
// written. The returned slice has length 5+bodyLen and pos points at
// the first body byte. Callers encode the body in-place via writeXxxAt
// then call writePacket exactly once.
//
// Unlike the server-side equivalent, this does NOT acquire bufmu —
// the high-level caller (Query, Bind, Execute, …) already holds it
// across the full request/response cycle. There is no concurrent
// writer on a client Conn (no async notification pusher analog), so
// no additional synchronization is needed.
//
// Fast path: when the packet fits in bufferedWriter's currently-
// available capacity, the body is written directly into the buffered
// writer's internal byte slice. writePacket commits in place — no
// intermediate buffer, no allocation.
//
// Slow path: when the body wouldn't fit (large Bind packets with big
// bytea params, large Parse query strings), borrow from the package-
// level bufpool. writePacket returns the buffer to the pool. We do
// not keep a per-connection scratch buffer.
func (c *Conn) startPacket(msgType byte, bodyLen int) ([]byte, int) {
	totalLen := 5 + bodyLen

	if c.bufferedWriter != nil {
		avail := c.bufferedWriter.AvailableBuffer()
		if cap(avail) >= totalLen {
			buf := avail[:totalLen]
			buf[0] = msgType
			binary.BigEndian.PutUint32(buf[1:5], uint32(4+bodyLen))
			return buf, 5
		}
	}

	c.outboundPoolBuf = bufPool.Get(totalLen)
	buf := *c.outboundPoolBuf
	buf[0] = msgType
	binary.BigEndian.PutUint32(buf[1:5], uint32(4+bodyLen))
	return buf, 5
}

// writePacket commits the packet started by startPacket. On the fast
// path, buf aliases bufferedWriter's internal storage so Write
// performs a self-copy that just advances the internal cursor. On the
// slow path, this is a real Write of the pool-backed slice; the pool
// buffer is then returned for reuse.
//
// Does NOT flush. The caller is responsible for calling flush() once
// at the end of the request/response cycle (or for not flushing at
// all in the extended-protocol pipelined case, where Sync triggers
// the flush).
func (c *Conn) writePacket(buf []byte) error {
	var err error
	if c.bufferedWriter != nil {
		_, err = c.bufferedWriter.Write(buf)
	} else {
		_, err = c.conn.Write(buf)
	}
	if c.outboundPoolBuf != nil {
		bufPool.Put(c.outboundPoolBuf)
		c.outboundPoolBuf = nil
	}
	return err
}

// In-place packet body encoders. Each writes at buf[pos:] and returns
// the new position. Callers must size the buffer (via startPacket) so
// these never run off the end — out-of-range slice writes panic.

func writeByteAt(buf []byte, pos int, b byte) int {
	buf[pos] = b
	return pos + 1
}

func writeInt16At(buf []byte, pos int, v int16) int {
	binary.BigEndian.PutUint16(buf[pos:], uint16(v))
	return pos + 2
}

func writeInt32At(buf []byte, pos int, v int32) int {
	binary.BigEndian.PutUint32(buf[pos:], uint32(v))
	return pos + 4
}

func writeUint32At(buf []byte, pos int, v uint32) int {
	binary.BigEndian.PutUint32(buf[pos:], v)
	return pos + 4
}

// writeStringAt writes s followed by a single null terminator.
func writeStringAt(buf []byte, pos int, s string) int {
	n := copy(buf[pos:], s)
	buf[pos+n] = 0
	return pos + n + 1
}

// writeBytesAt writes raw bytes (no terminator).
func writeBytesAt(buf []byte, pos int, b []byte) int {
	n := copy(buf[pos:], b)
	return pos + n
}

// writeByteStringAt writes a length-prefixed byte string (4-byte
// length + data). Writes -1 for nil (NULL).
func writeByteStringAt(buf []byte, pos int, b []byte) int {
	if b == nil {
		return writeInt32At(buf, pos, -1)
	}
	pos = writeInt32At(buf, pos, int32(len(b)))
	return writeBytesAt(buf, pos, b)
}

// writeTerminate writes a Terminate message.
func (c *Conn) writeTerminate() error {
	buf, _ := c.startPacket(protocol.MsgTerminate, 0)
	return c.writePacket(buf)
}

// MessageReader provides helper methods for reading message fields.
type MessageReader struct {
	buf []byte
	pos int
}

// NewMessageReader creates a new message reader for the given buffer.
//
// Returns by value (not pointer). Callers do `r := NewMessageReader(body)`
// and the struct lives on their stack — no heap allocation. The
// methods take pointer receivers to mutate `pos`; Go auto-addresses
// the stack-local value when calling them.
//
// IMPORTANT: helpers that walk the SAME MessageReader as their
// caller MUST accept `*MessageReader`, not `MessageReader` by value.
// A by-value parameter copies the struct, and any cursor mutations
// inside the helper vanish on return — silently producing wrong
// reads if the caller continues parsing after the call.
func NewMessageReader(buf []byte) MessageReader {
	return MessageReader{buf: buf, pos: 0}
}

// Remaining returns the number of unread bytes.
func (r *MessageReader) Remaining() int {
	return len(r.buf) - r.pos
}

// ReadByte reads a single byte.
func (r *MessageReader) ReadByte() (byte, error) {
	if r.pos >= len(r.buf) {
		return 0, io.EOF
	}
	b := r.buf[r.pos]
	r.pos++
	return b, nil
}

// ReadUint16 reads a 16-bit unsigned integer in network byte order.
func (r *MessageReader) ReadUint16() (uint16, error) {
	if r.pos+2 > len(r.buf) {
		return 0, io.EOF
	}
	v := binary.BigEndian.Uint16(r.buf[r.pos:])
	r.pos += 2
	return v, nil
}

// ReadUint32 reads a 32-bit unsigned integer in network byte order.
func (r *MessageReader) ReadUint32() (uint32, error) {
	if r.pos+4 > len(r.buf) {
		return 0, io.EOF
	}
	v := binary.BigEndian.Uint32(r.buf[r.pos:])
	r.pos += 4
	return v, nil
}

// ReadInt16 reads a 16-bit signed integer in network byte order.
func (r *MessageReader) ReadInt16() (int16, error) {
	v, err := r.ReadUint16()
	return int16(v), err
}

// ReadInt32 reads a 32-bit signed integer in network byte order.
func (r *MessageReader) ReadInt32() (int32, error) {
	v, err := r.ReadUint32()
	return int32(v), err
}

// ReadString reads a null-terminated string.
func (r *MessageReader) ReadString() (string, error) {
	start := r.pos
	for r.pos < len(r.buf) {
		if r.buf[r.pos] == 0 {
			s := string(r.buf[start:r.pos])
			r.pos++ // Skip null terminator.
			return s, nil
		}
		r.pos++
	}
	return "", io.EOF
}

// ReadBytes reads n bytes.
func (r *MessageReader) ReadBytes(n int) ([]byte, error) {
	if r.pos+n > len(r.buf) {
		return nil, io.EOF
	}
	b := r.buf[r.pos : r.pos+n]
	r.pos += n
	return b, nil
}

// ReadByteString reads a length-prefixed byte string (4-byte length + data).
// Returns nil if length is -1 (NULL).
func (r *MessageReader) ReadByteString() ([]byte, error) {
	length, err := r.ReadInt32()
	if err != nil {
		return nil, err
	}
	if length == -1 {
		return nil, nil // NULL
	}
	if length < 0 {
		return nil, fmt.Errorf("invalid byte string length: %d", length)
	}
	return r.ReadBytes(int(length))
}
