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

import "encoding/binary"

// MessageWriter is a test-only helper for constructing synthetic
// pgwire byte buffers (typically representing server responses fed
// into the parser). Production code uses startPacket / writePacket
// and the writeXxxAt encoders directly into bufio.Writer; this type
// exists purely so tests can build expected-bytes fixtures with an
// append-style API.
//
// Only present in *_test.go files — never compiled into the
// production binary.
type MessageWriter struct {
	buf []byte
}

// NewMessageWriter creates a new test-only message writer.
func NewMessageWriter() *MessageWriter {
	return &MessageWriter{buf: make([]byte, 0, 256)}
}

// Bytes returns the accumulated message bytes.
func (w *MessageWriter) Bytes() []byte {
	return w.buf
}

// Len returns the current length of the message.
func (w *MessageWriter) Len() int {
	return len(w.buf)
}

// Reset resets the writer for reuse.
func (w *MessageWriter) Reset() {
	w.buf = w.buf[:0]
}

// AppendByte writes a single byte. Named AppendByte (not WriteByte) to
// avoid shadowing io.ByteWriter's signature, which go vet flags.
func (w *MessageWriter) AppendByte(b byte) {
	w.buf = append(w.buf, b)
}

// WriteBytes writes raw bytes.
func (w *MessageWriter) WriteBytes(b []byte) {
	w.buf = append(w.buf, b...)
}

// WriteUint16 writes a 16-bit unsigned integer in network byte order.
func (w *MessageWriter) WriteUint16(v uint16) {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], v)
	w.buf = append(w.buf, buf[:]...)
}

// WriteUint32 writes a 32-bit unsigned integer in network byte order.
func (w *MessageWriter) WriteUint32(v uint32) {
	var buf [4]byte
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

// WriteByteString writes a length-prefixed byte string (4-byte
// length + data). Writes -1 for nil (NULL).
func (w *MessageWriter) WriteByteString(b []byte) {
	if b == nil {
		w.WriteInt32(-1)
		return
	}
	w.WriteInt32(int32(len(b)))
	w.WriteBytes(b)
}
