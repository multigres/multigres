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

// readMessageType reads a single byte message type from the connection.
// Returns 0 and io.EOF if the connection is closed gracefully.
func (c *Conn) readMessageType() (byte, error) {
	var msgType [1]byte
	_, err := io.ReadFull(c.bufferedReader, msgType[:])
	if err != nil {
		return 0, err
	}
	return msgType[0], nil
}

// readMessageLength reads the 4-byte message length from the connection.
// The length includes itself but excludes the message type byte.
// Returns the length of the message body (length - 4).
func (c *Conn) readMessageLength() (int, error) {
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
	length, err := c.readMessageLength()
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
func (c *Conn) writeMessage(msgType byte, body []byte) error {
	writer := c.getWriter()

	// Write message type.
	if err := c.writeByte(writer, msgType); err != nil {
		return err
	}

	// Write length (4 bytes + body length).
	length := uint32(4 + len(body))
	if err := c.writeUint32(writer, length); err != nil {
		return err
	}

	// Write body.
	if len(body) > 0 {
		_, err := writer.Write(body)
		if err != nil {
			return err
		}
	}

	return nil
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
