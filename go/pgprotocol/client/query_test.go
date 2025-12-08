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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/pgprotocol/protocol"
)

func TestParseRowsAffected(t *testing.T) {
	tests := []struct {
		tag      string
		expected uint64
	}{
		{"SELECT 5", 5},
		{"SELECT 0", 0},
		{"SELECT 100", 100},
		{"INSERT 0 1", 1},
		{"INSERT 0 10", 10},
		{"UPDATE 5", 5},
		{"DELETE 3", 3},
		{"CREATE TABLE", 0},
		{"DROP TABLE", 0},
		{"BEGIN", 0},
		{"COMMIT", 0},
		{"ROLLBACK", 0},
	}

	for _, tt := range tests {
		t.Run(tt.tag, func(t *testing.T) {
			result := parseRowsAffected(tt.tag)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseRowDescription(t *testing.T) {
	// Build a RowDescription message body.
	w := NewMessageWriter()
	w.WriteInt16(2) // 2 fields

	// Field 1
	w.WriteString("id")  // name
	w.WriteUint32(12345) // table OID
	w.WriteInt16(1)      // attribute number
	w.WriteUint32(23)    // data type OID (int4)
	w.WriteInt16(4)      // data type size
	w.WriteInt32(-1)     // type modifier
	w.WriteInt16(0)      // format code (text)

	// Field 2
	w.WriteString("name") // name
	w.WriteUint32(12345)  // table OID
	w.WriteInt16(2)       // attribute number
	w.WriteUint32(25)     // data type OID (text)
	w.WriteInt16(-1)      // data type size (variable)
	w.WriteInt32(-1)      // type modifier
	w.WriteInt16(0)       // format code (text)

	conn := &Conn{}
	result := &query.QueryResult{}
	err := conn.parseRowDescription(w.Bytes(), result)
	require.NoError(t, err)

	require.Len(t, result.Fields, 2)

	assert.Equal(t, "id", result.Fields[0].Name)
	assert.Equal(t, uint32(12345), result.Fields[0].TableOid)
	assert.Equal(t, int32(1), result.Fields[0].TableAttributeNumber)
	assert.Equal(t, uint32(23), result.Fields[0].DataTypeOid)
	assert.Equal(t, int32(4), result.Fields[0].DataTypeSize)
	assert.Equal(t, int32(-1), result.Fields[0].TypeModifier)
	assert.Equal(t, int32(0), result.Fields[0].Format)

	assert.Equal(t, "name", result.Fields[1].Name)
	assert.Equal(t, uint32(12345), result.Fields[1].TableOid)
	assert.Equal(t, int32(2), result.Fields[1].TableAttributeNumber)
	assert.Equal(t, uint32(25), result.Fields[1].DataTypeOid)
	assert.Equal(t, int32(-1), result.Fields[1].DataTypeSize)
}

func TestParseDataRow(t *testing.T) {
	// Build a DataRow message body.
	w := NewMessageWriter()
	w.WriteInt16(3) // 3 columns

	// Column 1: "hello"
	w.WriteByteString([]byte("hello"))

	// Column 2: NULL
	w.WriteByteString(nil)

	// Column 3: "world"
	w.WriteByteString([]byte("world"))

	conn := &Conn{}
	row, err := conn.parseDataRow(w.Bytes())
	require.NoError(t, err)

	require.Len(t, row.Values, 3)
	assert.Equal(t, []byte("hello"), row.Values[0])
	assert.Nil(t, row.Values[1])
	assert.Equal(t, []byte("world"), row.Values[2])
}

func TestParseCommandComplete(t *testing.T) {
	// Build a CommandComplete message body.
	w := NewMessageWriter()
	w.WriteString("SELECT 42")

	conn := &Conn{}
	tag, err := conn.parseCommandComplete(w.Bytes())
	require.NoError(t, err)
	assert.Equal(t, "SELECT 42", tag)
}

func TestParseError(t *testing.T) {
	// Build an ErrorResponse message body.
	w := NewMessageWriter()
	w.WriteByte(protocol.FieldSeverity)
	w.WriteString("ERROR")
	w.WriteByte(protocol.FieldCode)
	w.WriteString("42P01")
	w.WriteByte(protocol.FieldMessage)
	w.WriteString("relation \"foo\" does not exist")
	w.WriteByte(protocol.FieldDetail)
	w.WriteString("Some detail")
	w.WriteByte(protocol.FieldHint)
	w.WriteString("Check your table name")
	w.WriteByte(0) // Terminator

	conn := &Conn{}
	err := conn.parseError(w.Bytes())
	require.Error(t, err)

	var pgErr *Error
	require.True(t, errors.As(err, &pgErr))
	assert.Equal(t, "ERROR", pgErr.Severity)
	assert.Equal(t, "42P01", pgErr.Code)
	assert.Equal(t, "relation \"foo\" does not exist", pgErr.Message)
	assert.Equal(t, "Some detail", pgErr.Detail)
	assert.Equal(t, "Check your table name", pgErr.Hint)
	assert.True(t, pgErr.IsSQLState("42P01"))
}

func TestErrorString(t *testing.T) {
	err := &Error{
		Severity: "ERROR",
		Code:     "42P01",
		Message:  "relation \"foo\" does not exist",
	}
	assert.Equal(t, "ERROR: relation \"foo\" does not exist (SQLSTATE 42P01)", err.Error())

	errWithDetail := &Error{
		Severity: "ERROR",
		Code:     "42P01",
		Message:  "relation \"foo\" does not exist",
		Detail:   "Some additional detail",
	}
	expected := "ERROR: relation \"foo\" does not exist (SQLSTATE 42P01)\nDETAIL: Some additional detail"
	assert.Equal(t, expected, errWithDetail.Error())
}

func TestWriteQueryMessage(t *testing.T) {
	var buf bytes.Buffer
	conn := &Conn{
		conn:           &mockNetConn{buf: &buf},
		bufferedReader: bufio.NewReader(&buf),
		bufferedWriter: bufio.NewWriter(&buf),
	}

	err := conn.writeQueryMessage("SELECT 1")
	require.NoError(t, err)

	// Reset reader to read what we just wrote.
	conn.bufferedReader.Reset(&buf)

	// Read the message.
	msgType, err := conn.readMessageType()
	require.NoError(t, err)
	assert.Equal(t, byte(protocol.MsgQuery), msgType)

	length, err := conn.readMessageLength()
	require.NoError(t, err)

	body, err := conn.readMessageBody(length)
	require.NoError(t, err)

	// Body should be "SELECT 1\0"
	assert.Equal(t, "SELECT 1", string(body[:len(body)-1]))
	assert.Equal(t, byte(0), body[len(body)-1])
}

func TestParseParameterDescription(t *testing.T) {
	// Build a ParameterDescription message body.
	w := NewMessageWriter()
	w.WriteInt16(3)     // 3 parameters
	w.WriteUint32(23)   // int4
	w.WriteUint32(25)   // text
	w.WriteUint32(1700) // numeric

	conn := &Conn{}
	params, err := conn.parseParameterDescription(w.Bytes())
	require.NoError(t, err)

	require.Len(t, params, 3)
	assert.Equal(t, uint32(23), params[0].DataTypeOid)
	assert.Equal(t, uint32(25), params[1].DataTypeOid)
	assert.Equal(t, uint32(1700), params[2].DataTypeOid)
}
