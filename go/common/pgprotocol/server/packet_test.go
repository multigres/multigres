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
	"bufio"
	"bytes"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
)

func TestMessageReaderReadByte(t *testing.T) {
	buf := []byte{0x01, 0x02, 0x03}
	r := NewMessageReader(buf)

	b, err := r.ReadByte()
	require.NoError(t, err)
	assert.Equal(t, byte(0x01), b)

	b, err = r.ReadByte()
	require.NoError(t, err)
	assert.Equal(t, byte(0x02), b)
}

func TestMessageReaderReadUint16(t *testing.T) {
	buf := []byte{0x01, 0x02, 0x03, 0x04}
	r := NewMessageReader(buf)

	v, err := r.ReadUint16()
	require.NoError(t, err)
	assert.Equal(t, uint16(0x0102), v)

	v, err = r.ReadUint16()
	require.NoError(t, err)
	assert.Equal(t, uint16(0x0304), v)
}

func TestMessageReaderReadUint32(t *testing.T) {
	buf := []byte{0x01, 0x02, 0x03, 0x04}
	r := NewMessageReader(buf)

	v, err := r.ReadUint32()
	require.NoError(t, err)
	assert.Equal(t, uint32(0x01020304), v)
}

func TestMessageReaderReadString(t *testing.T) {
	buf := []byte{'h', 'e', 'l', 'l', 'o', 0, 'w', 'o', 'r', 'l', 'd', 0}
	r := NewMessageReader(buf)

	s, err := r.ReadString()
	require.NoError(t, err)
	assert.Equal(t, "hello", s)

	s, err = r.ReadString()
	require.NoError(t, err)
	assert.Equal(t, "world", s)
}

func TestMessageReaderReadByteString(t *testing.T) {
	// Create a buffer with length-prefixed string: length=5, data="hello"
	buf := []byte{0x00, 0x00, 0x00, 0x05, 'h', 'e', 'l', 'l', 'o'}
	r := NewMessageReader(buf)

	data, err := r.ReadByteString()
	require.NoError(t, err)
	assert.Equal(t, "hello", string(data))
}

func TestMessageReaderReadByteStringNull(t *testing.T) {
	// Create a buffer with NULL string (length=-1)
	buf := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	r := NewMessageReader(buf)

	data, err := r.ReadByteString()
	require.NoError(t, err)
	assert.Nil(t, data, "expected nil for NULL string")
}

func TestMessageReaderEOF(t *testing.T) {
	buf := []byte{0x01}
	r := NewMessageReader(buf)

	// Read one byte successfully.
	_, err := r.ReadByte()
	require.NoError(t, err)

	// Next read should return EOF.
	_, err = r.ReadByte()
	assert.ErrorIs(t, err, io.EOF)
}

func TestMessageWriterWriteByte(t *testing.T) {
	w := NewMessageWriter()
	w.WriteByte(0x01)
	w.WriteByte(0x02)

	buf := w.Bytes()
	assert.Equal(t, []byte{0x01, 0x02}, buf)
}

func TestMessageWriterWriteUint16(t *testing.T) {
	w := NewMessageWriter()
	w.WriteUint16(0x0102)

	buf := w.Bytes()
	assert.Equal(t, []byte{0x01, 0x02}, buf)
}

func TestMessageWriterWriteUint32(t *testing.T) {
	w := NewMessageWriter()
	w.WriteUint32(0x01020304)

	buf := w.Bytes()
	assert.Equal(t, []byte{0x01, 0x02, 0x03, 0x04}, buf)
}

func TestMessageWriterWriteString(t *testing.T) {
	w := NewMessageWriter()
	w.WriteString("hello")

	buf := w.Bytes()
	expected := []byte{'h', 'e', 'l', 'l', 'o', 0}
	assert.Equal(t, expected, buf)
}

func TestMessageWriterWriteByteString(t *testing.T) {
	w := NewMessageWriter()
	w.WriteByteString([]byte("hello"))

	buf := w.Bytes()
	// Should be: length (4 bytes) + data (5 bytes)
	expected := []byte{0x00, 0x00, 0x00, 0x05, 'h', 'e', 'l', 'l', 'o'}
	assert.Equal(t, expected, buf)
}

func TestMessageWriterWriteByteStringNull(t *testing.T) {
	w := NewMessageWriter()
	w.WriteByteString(nil)

	buf := w.Bytes()
	// Should be: length=-1 (0xFFFFFFFF)
	expected := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	assert.Equal(t, expected, buf)
}

func TestConnWriteAndReadMessage(t *testing.T) {
	// Create a mock connection using a bytes buffer.
	var buf bytes.Buffer

	// Create a minimal conn for testing.
	conn := &Conn{
		conn:           &mockNetConn{buf: &buf},
		bufferedReader: bufio.NewReader(&buf),
	}

	// Write a message.
	body := []byte("test message")
	err := conn.writeMessage(protocol.MsgQuery, body)
	require.NoError(t, err)

	// Reset reader to read what we just wrote.
	conn.bufferedReader.Reset(&buf)

	// Read message type.
	msgType, err := conn.ReadMessageType()
	require.NoError(t, err)
	assert.Equal(t, byte(protocol.MsgQuery), msgType)

	// Read message length.
	length, err := conn.ReadMessageLength()
	require.NoError(t, err)
	assert.Equal(t, len(body), length)

	// Read message body.
	readBody, err := conn.readMessageBody(length)
	require.NoError(t, err)
	assert.Equal(t, body, readBody)
}

// mockNetConn is a minimal implementation of net.Conn for testing.
type mockNetConn struct {
	buf *bytes.Buffer
}

func (m *mockNetConn) Read(b []byte) (n int, err error) {
	return m.buf.Read(b)
}

func (m *mockNetConn) Write(b []byte) (n int, err error) {
	return m.buf.Write(b)
}

func (m *mockNetConn) Close() error {
	return nil
}

// Implement remaining net.Conn methods (not used in tests).
func (m *mockNetConn) LocalAddr() net.Addr                { return nil }
func (m *mockNetConn) RemoteAddr() net.Addr               { return nil }
func (m *mockNetConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockNetConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockNetConn) SetWriteDeadline(t time.Time) error { return nil }
