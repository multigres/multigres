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

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/sqltypes"
)

func TestParseRowsAffected(t *testing.T) {
	tests := []struct {
		tag      string
		expected uint64
	}{
		{"SELECT 5", 0},   // SELECT doesn't affect rows, only reads them
		{"SELECT 0", 0},   // SELECT doesn't affect rows
		{"SELECT 100", 0}, // SELECT doesn't affect rows
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
	fields, err := conn.parseRowDescription(w.Bytes())
	require.NoError(t, err)

	require.Len(t, fields, 2)

	assert.Equal(t, "id", fields[0].Name)
	assert.Equal(t, uint32(12345), fields[0].TableOid)
	assert.Equal(t, int32(1), fields[0].TableAttributeNumber)
	assert.Equal(t, uint32(23), fields[0].DataTypeOid)
	assert.Equal(t, int32(4), fields[0].DataTypeSize)
	assert.Equal(t, int32(-1), fields[0].TypeModifier)
	assert.Equal(t, int32(0), fields[0].Format)

	assert.Equal(t, "name", fields[1].Name)
	assert.Equal(t, uint32(12345), fields[1].TableOid)
	assert.Equal(t, int32(2), fields[1].TableAttributeNumber)
	assert.Equal(t, uint32(25), fields[1].DataTypeOid)
	assert.Equal(t, int32(-1), fields[1].DataTypeSize)
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
	assert.Equal(t, []byte("hello"), []byte(row.Values[0]))
	assert.Nil(t, row.Values[1])
	assert.Equal(t, []byte("world"), []byte(row.Values[2]))
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
	// Build an ErrorResponse message body with basic fields.
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

	var diag *sqltypes.PgDiagnostic
	require.True(t, errors.As(err, &diag))
	assert.Equal(t, "ERROR", diag.Severity)
	assert.Equal(t, "42P01", diag.Code)
	assert.Equal(t, "relation \"foo\" does not exist", diag.Message)
	assert.Equal(t, "Some detail", diag.Detail)
	assert.Equal(t, "Check your table name", diag.Hint)
	assert.Equal(t, "42P01", diag.SQLSTATE())
}

func TestParseErrorAllFields(t *testing.T) {
	// Build an ErrorResponse message body with all 14 fields.
	w := NewMessageWriter()
	w.WriteByte(protocol.FieldSeverity)
	w.WriteString("ERROR")
	w.WriteByte(protocol.FieldCode)
	w.WriteString("23505")
	w.WriteByte(protocol.FieldMessage)
	w.WriteString("duplicate key value violates unique constraint \"users_email_key\"")
	w.WriteByte(protocol.FieldDetail)
	w.WriteString("Key (email)=(test@example.com) already exists.")
	w.WriteByte(protocol.FieldHint)
	w.WriteString("Try a different email address")
	w.WriteByte(protocol.FieldPosition)
	w.WriteString("42")
	w.WriteByte(protocol.FieldInternalPosition)
	w.WriteString("15")
	w.WriteByte(protocol.FieldInternalQuery)
	w.WriteString("SELECT * FROM internal_table")
	w.WriteByte(protocol.FieldWhere)
	w.WriteString("PL/pgSQL function check_email() line 5")
	w.WriteByte(protocol.FieldSchema)
	w.WriteString("public")
	w.WriteByte(protocol.FieldTable)
	w.WriteString("users")
	w.WriteByte(protocol.FieldColumn)
	w.WriteString("email")
	w.WriteByte(protocol.FieldDataType)
	w.WriteString("text")
	w.WriteByte(protocol.FieldConstraint)
	w.WriteString("users_email_key")
	w.WriteByte(0) // Terminator

	conn := &Conn{}
	err := conn.parseError(w.Bytes())
	require.Error(t, err)

	var diag *sqltypes.PgDiagnostic
	require.True(t, errors.As(err, &diag))

	// Verify all 14 fields.
	require.NotNil(t, diag)
	assert.Equal(t, "ERROR", diag.Severity)
	assert.Equal(t, "23505", diag.Code)
	assert.Equal(t, "duplicate key value violates unique constraint \"users_email_key\"", diag.Message)
	assert.Equal(t, "Key (email)=(test@example.com) already exists.", diag.Detail)
	assert.Equal(t, "Try a different email address", diag.Hint)
	assert.Equal(t, int32(42), diag.Position)
	assert.Equal(t, int32(15), diag.InternalPosition)
	assert.Equal(t, "SELECT * FROM internal_table", diag.InternalQuery)
	assert.Equal(t, "PL/pgSQL function check_email() line 5", diag.Where)
	assert.Equal(t, "public", diag.Schema)
	assert.Equal(t, "users", diag.Table)
	assert.Equal(t, "email", diag.Column)
	assert.Equal(t, "text", diag.DataType)
	assert.Equal(t, "users_email_key", diag.Constraint)
}

func TestPgDiagnosticFields(t *testing.T) {
	// Build an ErrorResponse message body with all 14 fields.
	w := NewMessageWriter()
	w.WriteByte(protocol.FieldSeverity)
	w.WriteString("ERROR")
	w.WriteByte(protocol.FieldCode)
	w.WriteString("22P02")
	w.WriteByte(protocol.FieldMessage)
	w.WriteString("invalid input syntax for type boolean")
	w.WriteByte(protocol.FieldDetail)
	w.WriteString("Some detail")
	w.WriteByte(protocol.FieldHint)
	w.WriteString("Check your input")
	w.WriteByte(protocol.FieldPosition)
	w.WriteString("25")
	w.WriteByte(protocol.FieldInternalPosition)
	w.WriteString("10")
	w.WriteByte(protocol.FieldInternalQuery)
	w.WriteString("SELECT internal()")
	w.WriteByte(protocol.FieldWhere)
	w.WriteString("function context")
	w.WriteByte(protocol.FieldSchema)
	w.WriteString("public")
	w.WriteByte(protocol.FieldTable)
	w.WriteString("mytable")
	w.WriteByte(protocol.FieldColumn)
	w.WriteString("mycolumn")
	w.WriteByte(protocol.FieldDataType)
	w.WriteString("boolean")
	w.WriteByte(protocol.FieldConstraint)
	w.WriteString("myconstraint")
	w.WriteByte(0) // Terminator

	conn := &Conn{}
	err := conn.parseError(w.Bytes())
	require.Error(t, err)

	var diag *sqltypes.PgDiagnostic
	require.True(t, errors.As(err, &diag))

	// Verify MessageType is set to 'E' (ErrorResponse).
	assert.Equal(t, byte(protocol.MsgErrorResponse), diag.MessageType)
	assert.True(t, diag.IsError())
	assert.False(t, diag.IsNotice())

	// Verify all fields are set correctly.
	assert.Equal(t, "ERROR", diag.Severity)
	assert.Equal(t, "22P02", diag.Code)
	assert.Equal(t, "invalid input syntax for type boolean", diag.Message)
	assert.Equal(t, "Some detail", diag.Detail)
	assert.Equal(t, "Check your input", diag.Hint)
	assert.Equal(t, int32(25), diag.Position)
	assert.Equal(t, int32(10), diag.InternalPosition)
	assert.Equal(t, "SELECT internal()", diag.InternalQuery)
	assert.Equal(t, "function context", diag.Where)
	assert.Equal(t, "public", diag.Schema)
	assert.Equal(t, "mytable", diag.Table)
	assert.Equal(t, "mycolumn", diag.Column)
	assert.Equal(t, "boolean", diag.DataType)
	assert.Equal(t, "myconstraint", diag.Constraint)
}

func TestPgDiagnosticErrorString(t *testing.T) {
	// Build an error without detail.
	w := NewMessageWriter()
	w.WriteByte(protocol.FieldSeverity)
	w.WriteString("ERROR")
	w.WriteByte(protocol.FieldCode)
	w.WriteString("42P01")
	w.WriteByte(protocol.FieldMessage)
	w.WriteString("relation \"foo\" does not exist")
	w.WriteByte(0)

	conn := &Conn{}
	err := conn.parseError(w.Bytes())
	require.Error(t, err)

	// Error() returns PostgreSQL-native format without SQLSTATE
	assert.Equal(t, "ERROR: relation \"foo\" does not exist", err.Error())

	// FullError() includes SQLSTATE for debugging
	var diag *sqltypes.PgDiagnostic
	require.True(t, errors.As(err, &diag))
	assert.Equal(t, "ERROR: relation \"foo\" does not exist (SQLSTATE 42P01)", diag.FullError())

	// Build an error with detail - Error() should still be clean
	w2 := NewMessageWriter()
	w2.WriteByte(protocol.FieldSeverity)
	w2.WriteString("FATAL")
	w2.WriteByte(protocol.FieldCode)
	w2.WriteString("28P01")
	w2.WriteByte(protocol.FieldMessage)
	w2.WriteString("password authentication failed")
	w2.WriteByte(protocol.FieldDetail)
	w2.WriteString("Some additional detail")
	w2.WriteByte(0)

	err2 := conn.parseError(w2.Bytes())
	require.Error(t, err2)

	// Error() still returns clean format (no SQLSTATE, no DETAIL)
	assert.Equal(t, "FATAL: password authentication failed", err2.Error())

	// FullError() includes SQLSTATE
	var diag2 *sqltypes.PgDiagnostic
	require.True(t, errors.As(err2, &diag2))
	assert.Equal(t, "FATAL: password authentication failed (SQLSTATE 28P01)", diag2.FullError())

	// Detail is still accessible directly on the diagnostic
	assert.Equal(t, "Some additional detail", diag2.Detail)
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
