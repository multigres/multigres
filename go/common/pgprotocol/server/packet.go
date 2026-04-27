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
func (c *Conn) ReadMessageType() (byte, error) {
	var msgType [1]byte
	_, err := io.ReadFull(c.bufferedReader, msgType[:])
	if err != nil {
		return 0, err
	}
	return msgType[0], nil
}

// ReadMessageLength reads the 4-byte message length from the connection.
// The length includes itself but excludes the message type byte.
// Returns the length of the message body (length - 4).
func (c *Conn) ReadMessageLength() (int, error) {
	var lenBuf [4]byte
	_, err := io.ReadFull(c.bufferedReader, lenBuf[:])
	if err != nil {
		return 0, err
	}

	length := binary.BigEndian.Uint32(lenBuf[:])
	if length < 4 {
		return 0, fmt.Errorf("invalid message length: %d", length)
	}

	// Return body length (excluding the length field itself).
	return int(length - 4), nil
}

// readMessageBody reads the message body of the given length.
// Returns a buffer from the pool that must be returned using returnReadBuffer.
func (c *Conn) readMessageBody(length int) ([]byte, error) {
	if length == 0 {
		return nil, nil
	}

	// Allocate buffer from pool if available.
	var buf []byte
	var pooledBuf *[]byte

	if c.listener != nil && c.listener.bufPool != nil {
		pooledBuf = c.listener.bufPool.Get(length)
		buf = *pooledBuf
	} else {
		buf = make([]byte, length)
	}

	// Read the message body.
	_, err := io.ReadFull(c.bufferedReader, buf)
	if err != nil {
		// Return buffer to pool on error.
		if pooledBuf != nil {
			c.listener.bufPool.Put(pooledBuf)
		}
		return nil, err
	}

	return buf, nil
}

// returnReadBuffer returns a buffer obtained from readMessageBody to the pool.
func (c *Conn) returnReadBuffer(buf []byte) {
	if c.listener != nil && c.listener.bufPool != nil && buf != nil {
		pooledBuf := &buf
		c.listener.bufPool.Put(pooledBuf)
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
// The length is calculated automatically (includes length field, excludes type byte).
//
// Coalesces the type+length header into one Write call to halve the
// number of bufio.Write trips through the buffered writer per message.
func (c *Conn) writeMessage(msgType byte, body []byte) error {
	var hdr [5]byte
	hdr[0] = msgType
	binary.BigEndian.PutUint32(hdr[1:], uint32(4+len(body)))

	writer := c.getWriter()
	if _, err := writer.Write(hdr[:]); err != nil {
		return err
	}
	if len(body) > 0 {
		if _, err := writer.Write(body); err != nil {
			return err
		}
	}
	return nil
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

// writeByte writes a single byte.
func (c *Conn) writeByte(w io.Writer, b byte) error {
	buf := [1]byte{b}
	_, err := w.Write(buf[:])
	return err
}

// writeUint16 writes a 16-bit unsigned integer in network byte order (big-endian).
func (c *Conn) writeUint16(w io.Writer, v uint16) error {
	buf := [2]byte{}
	binary.BigEndian.PutUint16(buf[:], v)
	_, err := w.Write(buf[:])
	return err
}

// writeUint32 writes a 32-bit unsigned integer in network byte order (big-endian).
func (c *Conn) writeUint32(w io.Writer, v uint32) error {
	buf := [4]byte{}
	binary.BigEndian.PutUint32(buf[:], v)
	_, err := w.Write(buf[:])
	return err
}

// writeInt16 writes a 16-bit signed integer in network byte order (big-endian).
func (c *Conn) writeInt16(w io.Writer, v int16) error {
	return c.writeUint16(w, uint16(v))
}

// writeInt32 writes a 32-bit signed integer in network byte order (big-endian).
func (c *Conn) writeInt32(w io.Writer, v int32) error {
	return c.writeUint32(w, uint32(v))
}

// writeString writes a null-terminated string.
func (c *Conn) writeString(w io.Writer, s string) error {
	if _, err := w.Write([]byte(s)); err != nil {
		return err
	}
	return c.writeByte(w, 0)
}

// writeBytes writes a byte slice (not null-terminated).
func (c *Conn) writeBytes(w io.Writer, b []byte) error {
	_, err := w.Write(b)
	return err
}

// MessageReader provides helper methods for reading message fields.
type MessageReader struct {
	buf []byte
	pos int
}

// NewMessageReader creates a new message reader for the given buffer.
func NewMessageReader(buf []byte) *MessageReader {
	return &MessageReader{buf: buf, pos: 0}
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

// MessageWriter provides helper methods for building message bodies.
type MessageWriter struct {
	buf []byte
}

// NewMessageWriter creates a new message writer.
func NewMessageWriter() *MessageWriter {
	return &MessageWriter{buf: make([]byte, 0, 1024)}
}

// Bytes returns the accumulated message bytes.
func (w *MessageWriter) Bytes() []byte {
	return w.buf
}

// WriteByte writes a single byte.
func (w *MessageWriter) WriteByte(b byte) {
	w.buf = append(w.buf, b)
}

// WriteUint16 writes a 16-bit unsigned integer in network byte order.
func (w *MessageWriter) WriteUint16(v uint16) {
	buf := [2]byte{}
	binary.BigEndian.PutUint16(buf[:], v)
	w.buf = append(w.buf, buf[:]...)
}

// WriteUint32 writes a 32-bit unsigned integer in network byte order.
func (w *MessageWriter) WriteUint32(v uint32) {
	buf := [4]byte{}
	binary.BigEndian.PutUint32(buf[:], v)
	w.buf = append(w.buf, buf[:]...)
}

// WriteInt16 writes a 16-bit signed integer in network byte order.
func (w *MessageWriter) WriteInt16(v int16) {
	w.WriteUint16(uint16(v))
}

// WriteInt32 writes a 32-bit signed integer in network byte order.
func (w *MessageWriter) WriteInt32(v int32) {
	w.WriteUint32(uint32(v))
}

// WriteString writes a null-terminated string.
func (w *MessageWriter) WriteString(s string) {
	w.buf = append(w.buf, []byte(s)...)
	w.buf = append(w.buf, 0)
}

// WriteBytes writes raw bytes (not null-terminated).
func (w *MessageWriter) WriteBytes(b []byte) {
	w.buf = append(w.buf, b...)
}

// WriteByteString writes a length-prefixed string (4-byte length + data).
// Writes -1 for nil (NULL).
func (w *MessageWriter) WriteByteString(b []byte) {
	if b == nil {
		w.WriteInt32(-1)
		return
	}
	w.WriteInt32(int32(len(b)))
	w.WriteBytes(b)
}
