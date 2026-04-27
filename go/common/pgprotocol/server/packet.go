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

package server

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
)

// ReadMessageType reads a single byte message type from the connection.
// Returns 0 and io.EOF if the connection is closed gracefully.
//
// Uses bufferedReader.ReadByte (concrete-type method, no interface
// dispatch) so the compiler can prove nothing escapes — vs the
// previous io.ReadFull(io.Reader, []byte) shape, which forced the
// stack-local slice header to the heap on every call.
func (c *Conn) ReadMessageType() (byte, error) {
	return c.bufferedReader.ReadByte()
}

// ReadMessageLength reads the 4-byte message length from the connection.
// The length includes itself but excludes the message type byte.
// Returns the length of the message body (length - 4).
//
// Uses Peek + Discard to read the 4 bytes directly out of bufio's
// internal buffer with no intermediate slice — same escape-avoidance
// reason as ReadMessageType. Peek returns a slice into bufio's
// storage; we read it before Discarding the bytes so the buffer
// can reuse the space.
func (c *Conn) ReadMessageLength() (int, error) {
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
// Returns a slice that must be released by calling returnReadBuffer.
//
// On the pool path, the underlying *[]byte is stashed in
// c.inboundPoolBuf so returnReadBuffer can return it without taking
// the address of a stack-local slice (which would force the slice
// header to the heap on every call). On the make-fallback path
// (no listener), there's nothing to recycle and inboundPoolBuf
// stays nil.
func (c *Conn) readMessageBody(length int) ([]byte, error) {
	if length == 0 {
		return nil, nil
	}

	var buf []byte
	if c.listener != nil && c.listener.bufPool != nil {
		c.inboundPoolBuf = c.listener.bufPool.Get(length)
		buf = *c.inboundPoolBuf
	} else {
		buf = make([]byte, length)
	}

	if _, err := io.ReadFull(c.bufferedReader, buf); err != nil {
		if c.inboundPoolBuf != nil {
			c.listener.bufPool.Put(c.inboundPoolBuf)
			c.inboundPoolBuf = nil
		}
		return nil, err
	}
	return buf, nil
}

// returnReadBuffer releases the buffer held by inboundPoolBuf back
// to the listener bufpool. No-op when readMessageBody used the make
// fallback (inboundPoolBuf is nil) or returned a zero-length slice.
func (c *Conn) returnReadBuffer() {
	if c.inboundPoolBuf != nil {
		c.listener.bufPool.Put(c.inboundPoolBuf)
		c.inboundPoolBuf = nil
	}
}

// readStartupPacket reads a startup packet (no message type byte).
// Startup packets only have a length field followed by the body.
func (c *Conn) readStartupPacket() ([]byte, error) {
	length, err := c.ReadMessageLength()
	if err != nil {
		return nil, err
	}

	if length > protocol.MaxStartupPacketLength {
		return nil, fmt.Errorf("startup packet too large: %d bytes", length)
	}

	return c.readMessageBody(length)
}

// writeMessage writes a complete message with type, length, and body.
// The length is calculated automatically (includes length field, excludes
// type byte).
//
// Routes through startPacket/writePacket so the header + body land in
// the bufferedWriter as a single Write (a self-copy on the fast path).
// Used by the few callers that already have the body materialized as
// a []byte — body-less control messages like ParseComplete /
// BindComplete / NoData / CloseComplete pass nil here.
func (c *Conn) writeMessage(msgType byte, body []byte) error {
	buf, pos := c.startPacket(msgType, len(body))
	if len(body) > 0 {
		pos = writeBytesAt(buf, pos, body)
	}
	_ = pos
	return c.writePacket(buf)
}

// startPacket reserves space for a single pgwire packet of the given
// body length, with the message type and 4-byte length header pre-
// written. The returned slice has length 5+bodyLen and pos points at
// the first body byte. Callers encode the body in-place via writeXxxAt
// then call writePacket exactly once.
//
// startPacket acquires bufMu and writePacket releases it. The lock is
// held across body encoding so an interleaved write from the async
// notification pusher cannot split the packet.
//
// Fast path: when a bufferedWriter is attached and the packet fits in
// its currently-available capacity, the body is written directly into
// the buffered writer's internal byte slice. writePacket then commits
// the bytes in place — no copy through a separate buffer, no buffer
// pool round-trip, no per-message allocation. This mirrors how postgres
// builds messages straight into PqSendBuffer.
//
// Fallback: when the body wouldn't fit (large packets) or no
// bufferedWriter is set yet (pre-startup writes), borrow a buffer from
// the listener's bufpool — the same pool the read path uses to stage
// inbound message bodies. The read and write sides see the same byte
// stream (multipooler's reads from postgres become multigateway's
// writes to the client), so any workload that exercises the read pool
// will exercise the write pool at a similar rate; routing both sides
// through one pool means a workload of large packets allocates once
// and then reuses the buffer indefinitely. writePacket returns the
// buffer to the pool. We do not keep a per-connection scratch buffer:
// the pool is per-listener, so memory amortizes across all
// connections rather than being pinned per-connection.
func (c *Conn) startPacket(msgType byte, bodyLen int) ([]byte, int) {
	totalLen := 5 + bodyLen
	c.bufMu.Lock()

	if c.bufferedWriter != nil {
		avail := c.bufferedWriter.AvailableBuffer()
		if cap(avail) >= totalLen {
			buf := avail[:totalLen]
			buf[0] = msgType
			binary.BigEndian.PutUint32(buf[1:5], uint32(4+bodyLen))
			return buf, 5
		}
	}

	var buf []byte
	if c.listener != nil && c.listener.bufPool != nil {
		c.outboundPoolBuf = c.listener.bufPool.Get(totalLen)
		buf = *c.outboundPoolBuf
	} else {
		buf = make([]byte, totalLen)
	}
	buf[0] = msgType
	binary.BigEndian.PutUint32(buf[1:5], uint32(4+bodyLen))
	return buf, 5
}

// writePacket commits the packet started by startPacket. Releases
// bufMu (which was acquired by startPacket) and returns any pool
// buffer the slow path borrowed.
//
// On the fast path, buf aliases the bufferedWriter's internal storage,
// so bufio.Writer.Write performs a self-copy that is effectively a no-
// op and just advances the internal write cursor. On the slow path,
// this is a normal Write of the pool-backed slice; afterwards the
// pool buffer is returned to listener.bufPool for reuse.
func (c *Conn) writePacket(buf []byte) error {
	var err error
	if c.bufferedWriter != nil {
		_, err = c.bufferedWriter.Write(buf)
	} else {
		_, err = c.conn.Write(buf)
	}
	if c.outboundPoolBuf != nil {
		c.listener.bufPool.Put(c.outboundPoolBuf)
		c.outboundPoolBuf = nil
	}
	c.bufMu.Unlock()
	return err
}

// In-place packet body encoders. Each writes at buf[pos:] and returns the
// new position. Callers must size the buffer (via startPacket) so that
// these never run off the end — out-of-range slice writes will panic.

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

// writeRawByte writes a single byte that is NOT a pgwire packet —
// just one raw byte on the wire. Used only for the SSL/GSSENC
// negotiation response ('S' or 'N'), which has no length prefix and
// no message-type framing. Goes through the bufferedWriter if one is
// attached, otherwise straight to the conn.
func (c *Conn) writeRawByte(b byte) error {
	c.bufMu.Lock()
	defer c.bufMu.Unlock()

	if c.bufferedWriter != nil {
		return c.bufferedWriter.WriteByte(b)
	}
	_, err := c.conn.Write([]byte{b})
	return err
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
// reads if the caller continues parsing after the call. Pass-by-
// pointer-of-stack-local doesn't escape (the helper just calls
// methods locally), so safety is free.
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

// ReadByteString reads a length-prefixed string (4-byte length + data).
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
		return nil, fmt.Errorf("invalid string length: %d", length)
	}
	return r.ReadBytes(int(length))
}
