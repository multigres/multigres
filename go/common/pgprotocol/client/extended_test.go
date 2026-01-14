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
	"bufio"
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
)

func TestWriteParse(t *testing.T) {
	var buf bytes.Buffer
	conn := &Conn{
		conn:           &mockNetConn{buf: &buf},
		bufferedReader: bufio.NewReader(&buf),
		bufferedWriter: bufio.NewWriter(&buf),
	}

	err := conn.writeParse("stmt1", "SELECT $1, $2", []uint32{23, 25})
	require.NoError(t, err)

	err = conn.flush()
	require.NoError(t, err)

	// Read and verify.
	conn.bufferedReader.Reset(&buf)

	msgType, err := conn.readMessageType()
	require.NoError(t, err)
	assert.Equal(t, byte(protocol.MsgParse), msgType)

	length, err := conn.readMessageLength()
	require.NoError(t, err)

	body, err := conn.readMessageBody(length)
	require.NoError(t, err)

	r := NewMessageReader(body)

	// Statement name.
	name, err := r.ReadString()
	require.NoError(t, err)
	assert.Equal(t, "stmt1", name)

	// Query string.
	queryStr, err := r.ReadString()
	require.NoError(t, err)
	assert.Equal(t, "SELECT $1, $2", queryStr)

	// Parameter count.
	paramCount, err := r.ReadInt16()
	require.NoError(t, err)
	assert.Equal(t, int16(2), paramCount)

	// Parameter types.
	oid1, err := r.ReadUint32()
	require.NoError(t, err)
	assert.Equal(t, uint32(23), oid1)

	oid2, err := r.ReadUint32()
	require.NoError(t, err)
	assert.Equal(t, uint32(25), oid2)
}

func TestWriteBind(t *testing.T) {
	var buf bytes.Buffer
	conn := &Conn{
		conn:           &mockNetConn{buf: &buf},
		bufferedReader: bufio.NewReader(&buf),
		bufferedWriter: bufio.NewWriter(&buf),
	}

	params := [][]byte{[]byte("42"), []byte("hello")}
	paramFormats := []int16{0, 0}
	resultFormats := []int16{0}

	err := conn.writeBind("portal1", "stmt1", params, paramFormats, resultFormats)
	require.NoError(t, err)

	err = conn.flush()
	require.NoError(t, err)

	// Read and verify.
	conn.bufferedReader.Reset(&buf)

	msgType, err := conn.readMessageType()
	require.NoError(t, err)
	assert.Equal(t, byte(protocol.MsgBind), msgType)

	length, err := conn.readMessageLength()
	require.NoError(t, err)

	body, err := conn.readMessageBody(length)
	require.NoError(t, err)

	r := NewMessageReader(body)

	// Portal name.
	portalName, err := r.ReadString()
	require.NoError(t, err)
	assert.Equal(t, "portal1", portalName)

	// Statement name.
	stmtName, err := r.ReadString()
	require.NoError(t, err)
	assert.Equal(t, "stmt1", stmtName)

	// Parameter format count.
	formatCount, err := r.ReadInt16()
	require.NoError(t, err)
	assert.Equal(t, int16(2), formatCount)

	// Parameter formats.
	for range 2 {
		format, err := r.ReadInt16()
		require.NoError(t, err)
		assert.Equal(t, int16(0), format)
	}

	// Parameter count.
	paramCount, err := r.ReadInt16()
	require.NoError(t, err)
	assert.Equal(t, int16(2), paramCount)

	// Parameters.
	p1, err := r.ReadByteString()
	require.NoError(t, err)
	assert.Equal(t, []byte("42"), p1)

	p2, err := r.ReadByteString()
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), p2)

	// Result format count.
	resultFormatCount, err := r.ReadInt16()
	require.NoError(t, err)
	assert.Equal(t, int16(1), resultFormatCount)

	// Result format.
	resultFormat, err := r.ReadInt16()
	require.NoError(t, err)
	assert.Equal(t, int16(0), resultFormat)
}

func TestWriteExecute(t *testing.T) {
	var buf bytes.Buffer
	conn := &Conn{
		conn:           &mockNetConn{buf: &buf},
		bufferedReader: bufio.NewReader(&buf),
		bufferedWriter: bufio.NewWriter(&buf),
	}

	err := conn.writeExecute("portal1", 100)
	require.NoError(t, err)

	err = conn.flush()
	require.NoError(t, err)

	// Read and verify.
	conn.bufferedReader.Reset(&buf)

	msgType, err := conn.readMessageType()
	require.NoError(t, err)
	assert.Equal(t, byte(protocol.MsgExecute), msgType)

	length, err := conn.readMessageLength()
	require.NoError(t, err)

	body, err := conn.readMessageBody(length)
	require.NoError(t, err)

	r := NewMessageReader(body)

	// Portal name.
	portalName, err := r.ReadString()
	require.NoError(t, err)
	assert.Equal(t, "portal1", portalName)

	// Max rows.
	maxRows, err := r.ReadInt32()
	require.NoError(t, err)
	assert.Equal(t, int32(100), maxRows)
}

func TestWriteDescribe(t *testing.T) {
	var buf bytes.Buffer
	conn := &Conn{
		conn:           &mockNetConn{buf: &buf},
		bufferedReader: bufio.NewReader(&buf),
		bufferedWriter: bufio.NewWriter(&buf),
	}

	err := conn.writeDescribe('S', "stmt1")
	require.NoError(t, err)

	err = conn.flush()
	require.NoError(t, err)

	// Read and verify.
	conn.bufferedReader.Reset(&buf)

	msgType, err := conn.readMessageType()
	require.NoError(t, err)
	assert.Equal(t, byte(protocol.MsgDescribe), msgType)

	length, err := conn.readMessageLength()
	require.NoError(t, err)

	body, err := conn.readMessageBody(length)
	require.NoError(t, err)

	r := NewMessageReader(body)

	// Describe type.
	descType, err := r.ReadByte()
	require.NoError(t, err)
	assert.Equal(t, byte('S'), descType)

	// Name.
	name, err := r.ReadString()
	require.NoError(t, err)
	assert.Equal(t, "stmt1", name)
}

func TestWriteClose(t *testing.T) {
	var buf bytes.Buffer
	conn := &Conn{
		conn:           &mockNetConn{buf: &buf},
		bufferedReader: bufio.NewReader(&buf),
		bufferedWriter: bufio.NewWriter(&buf),
	}

	err := conn.writeClose('P', "portal1")
	require.NoError(t, err)

	err = conn.flush()
	require.NoError(t, err)

	// Read and verify.
	conn.bufferedReader.Reset(&buf)

	msgType, err := conn.readMessageType()
	require.NoError(t, err)
	assert.Equal(t, byte(protocol.MsgClose), msgType)

	length, err := conn.readMessageLength()
	require.NoError(t, err)

	body, err := conn.readMessageBody(length)
	require.NoError(t, err)

	r := NewMessageReader(body)

	// Close type.
	closeType, err := r.ReadByte()
	require.NoError(t, err)
	assert.Equal(t, byte('P'), closeType)

	// Name.
	name, err := r.ReadString()
	require.NoError(t, err)
	assert.Equal(t, "portal1", name)
}

func TestWriteSync(t *testing.T) {
	var buf bytes.Buffer
	conn := &Conn{
		conn:           &mockNetConn{buf: &buf},
		bufferedReader: bufio.NewReader(&buf),
		bufferedWriter: bufio.NewWriter(&buf),
	}

	err := conn.writeSync()
	require.NoError(t, err)

	err = conn.flush()
	require.NoError(t, err)

	// Read and verify.
	conn.bufferedReader.Reset(&buf)

	msgType, err := conn.readMessageType()
	require.NoError(t, err)
	assert.Equal(t, byte(protocol.MsgSync), msgType)

	length, err := conn.readMessageLength()
	require.NoError(t, err)
	assert.Equal(t, 0, length) // Sync has no body.
}

func TestWriteFlush(t *testing.T) {
	var buf bytes.Buffer
	conn := &Conn{
		conn:           &mockNetConn{buf: &buf},
		bufferedReader: bufio.NewReader(&buf),
		bufferedWriter: bufio.NewWriter(&buf),
	}

	err := conn.writeFlush()
	require.NoError(t, err)

	err = conn.flush()
	require.NoError(t, err)

	// Read and verify.
	conn.bufferedReader.Reset(&buf)

	msgType, err := conn.readMessageType()
	require.NoError(t, err)
	assert.Equal(t, byte(protocol.MsgFlush), msgType)

	length, err := conn.readMessageLength()
	require.NoError(t, err)
	assert.Equal(t, 0, length) // Flush has no body.
}

func TestWriteTerminate(t *testing.T) {
	var buf bytes.Buffer
	conn := &Conn{
		conn:           &mockNetConn{buf: &buf},
		bufferedReader: bufio.NewReader(&buf),
		bufferedWriter: bufio.NewWriter(&buf),
	}

	err := conn.writeTerminate()
	require.NoError(t, err)

	err = conn.flush()
	require.NoError(t, err)

	// Read and verify.
	conn.bufferedReader.Reset(&buf)

	msgType, err := conn.readMessageType()
	require.NoError(t, err)
	assert.Equal(t, byte(protocol.MsgTerminate), msgType)

	length, err := conn.readMessageLength()
	require.NoError(t, err)
	assert.Equal(t, 0, length) // Terminate has no body.
}

func TestWriteBindWithNullParams(t *testing.T) {
	var buf bytes.Buffer
	conn := &Conn{
		conn:           &mockNetConn{buf: &buf},
		bufferedReader: bufio.NewReader(&buf),
		bufferedWriter: bufio.NewWriter(&buf),
	}

	// Include a NULL parameter.
	params := [][]byte{[]byte("42"), nil, []byte("hello")}

	err := conn.writeBind("", "", params, nil, nil)
	require.NoError(t, err)

	err = conn.flush()
	require.NoError(t, err)

	// Read and verify.
	conn.bufferedReader.Reset(&buf)

	msgType, err := conn.readMessageType()
	require.NoError(t, err)
	assert.Equal(t, byte(protocol.MsgBind), msgType)

	length, err := conn.readMessageLength()
	require.NoError(t, err)

	body, err := conn.readMessageBody(length)
	require.NoError(t, err)

	r := NewMessageReader(body)

	// Skip portal and statement names.
	_, _ = r.ReadString()
	_, _ = r.ReadString()

	// Skip parameter format count (0).
	_, _ = r.ReadInt16()

	// Parameter count.
	paramCount, err := r.ReadInt16()
	require.NoError(t, err)
	assert.Equal(t, int16(3), paramCount)

	// Parameters.
	p1, err := r.ReadByteString()
	require.NoError(t, err)
	assert.Equal(t, []byte("42"), p1)

	p2, err := r.ReadByteString()
	require.NoError(t, err)
	assert.Nil(t, p2) // NULL

	p3, err := r.ReadByteString()
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), p3)
}
