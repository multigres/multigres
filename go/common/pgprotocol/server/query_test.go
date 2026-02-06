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
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createTestConn creates a minimal test connection with the given buffer.
func createTestConn(t *testing.T, buf *bytes.Buffer) *Conn {
	return &Conn{
		conn:           &testNetConn{buf: buf},
		bufferedReader: bufio.NewReader(buf),
		txnStatus:      protocol.TxnStatusIdle,
	}
}

// testNetConn is a minimal implementation of net.Conn for testing.
type testNetConn struct {
	buf *bytes.Buffer
}

func (m *testNetConn) Read(b []byte) (n int, err error) {
	return m.buf.Read(b)
}

func (m *testNetConn) Write(b []byte) (n int, err error) {
	return m.buf.Write(b)
}

func (m *testNetConn) Close() error {
	return nil
}

func (m *testNetConn) LocalAddr() net.Addr                { return nil }
func (m *testNetConn) RemoteAddr() net.Addr               { return nil }
func (m *testNetConn) SetDeadline(t time.Time) error      { return nil }
func (m *testNetConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *testNetConn) SetWriteDeadline(t time.Time) error { return nil }

// TestReadQueryMessage tests parsing of Query messages.
func TestReadQueryMessage(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		expectError bool
	}{
		{
			name:        "simple SELECT query",
			query:       "SELECT * FROM users",
			expectError: false,
		},
		{
			name:        "empty query",
			query:       "",
			expectError: false,
		},
		{
			name:        "query with special characters",
			query:       "SELECT 'hello''world' FROM table",
			expectError: false,
		},
		{
			name:        "multi-line query",
			query:       "SELECT id,\n  name,\n  email\nFROM users",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Build the query message.
			var buf bytes.Buffer

			// Write length (4 bytes for length + query + null terminator).
			length := int32(4 + len(tt.query) + 1)
			err := binary.Write(&buf, binary.BigEndian, length)
			require.NoError(t, err)

			// Write query string.
			buf.WriteString(tt.query)
			buf.WriteByte(0) // Null terminator

			// Create a mock connection.
			conn := createTestConn(t, &buf)

			// Read the query message.
			result, err := conn.readQueryMessage()
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.query, result)
			}
		})
	}
}

// TestWriteRowDescription tests encoding of RowDescription messages.
func TestWriteRowDescription(t *testing.T) {
	tests := []struct {
		name   string
		fields []*query.Field
	}{
		{
			name: "single column",
			fields: []*query.Field{
				{
					Name:                 "id",
					Type:                 "integer",
					TableOid:             16384,
					TableAttributeNumber: 1,
					DataTypeOid:          23, // int4
					DataTypeSize:         4,
					TypeModifier:         -1,
					Format:               0, // text
				},
			},
		},
		{
			name: "multiple columns",
			fields: []*query.Field{
				{
					Name:         "id",
					DataTypeOid:  23,
					DataTypeSize: 4,
					TypeModifier: -1,
					Format:       0,
				},
				{
					Name:         "name",
					DataTypeOid:  25, // text
					DataTypeSize: -1, // variable
					TypeModifier: -1,
					Format:       0,
				},
				{
					Name:         "created_at",
					DataTypeOid:  1114, // timestamp
					DataTypeSize: 8,
					TypeModifier: -1,
					Format:       0,
				},
			},
		},
		{
			name:   "no columns",
			fields: []*query.Field{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			conn := createTestConn(t, &buf)

			err := conn.writeRowDescription(tt.fields)
			if len(tt.fields) == 0 {
				// No fields means no output.
				assert.NoError(t, err)
				assert.Equal(t, 0, buf.Len())
				return
			}

			assert.NoError(t, err)

			// Verify message type.
			msgType, err := buf.ReadByte()
			require.NoError(t, err)
			assert.Equal(t, byte(protocol.MsgRowDescription), msgType)

			// Read and verify length.
			var length int32
			err = binary.Read(&buf, binary.BigEndian, &length)
			require.NoError(t, err)

			// Read field count.
			var fieldCount int16
			err = binary.Read(&buf, binary.BigEndian, &fieldCount)
			require.NoError(t, err)
			assert.Equal(t, int16(len(tt.fields)), fieldCount)

			// Verify each field.
			for i, field := range tt.fields {
				// Read field name.
				name, err := readNullTerminatedString(&buf)
				require.NoError(t, err, "field %d", i)
				assert.Equal(t, field.Name, name, "field %d name", i)

				// Read table OID.
				var tableOid uint32
				err = binary.Read(&buf, binary.BigEndian, &tableOid)
				require.NoError(t, err)
				assert.Equal(t, field.TableOid, tableOid)

				// Read column number.
				var colNum int16
				err = binary.Read(&buf, binary.BigEndian, &colNum)
				require.NoError(t, err)
				assert.Equal(t, int16(field.TableAttributeNumber), colNum)

				// Read type OID.
				var typeOid uint32
				err = binary.Read(&buf, binary.BigEndian, &typeOid)
				require.NoError(t, err)
				assert.Equal(t, field.DataTypeOid, typeOid)

				// Read type size.
				var typeSize int16
				err = binary.Read(&buf, binary.BigEndian, &typeSize)
				require.NoError(t, err)
				assert.Equal(t, int16(field.DataTypeSize), typeSize)

				// Read type modifier.
				var typeMod int32
				err = binary.Read(&buf, binary.BigEndian, &typeMod)
				require.NoError(t, err)
				assert.Equal(t, field.TypeModifier, typeMod)

				// Read format.
				var format int16
				err = binary.Read(&buf, binary.BigEndian, &format)
				require.NoError(t, err)
				assert.Equal(t, int16(field.Format), format)
			}
		})
	}
}

// TestWriteDataRow tests encoding of DataRow messages.
func TestWriteDataRow(t *testing.T) {
	tests := []struct {
		name string
		row  *sqltypes.Row
	}{
		{
			name: "single value",
			row: &sqltypes.Row{
				Values: []sqltypes.Value{
					[]byte("1"),
				},
			},
		},
		{
			name: "multiple values",
			row: &sqltypes.Row{
				Values: []sqltypes.Value{
					[]byte("42"),
					[]byte("John Doe"),
					[]byte("2024-01-01 00:00:00"),
				},
			},
		},
		{
			name: "with NULL values",
			row: &sqltypes.Row{
				Values: []sqltypes.Value{
					[]byte("1"),
					nil, // NULL
					[]byte("test"),
				},
			},
		},
		{
			name: "empty values",
			row: &sqltypes.Row{
				Values: []sqltypes.Value{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			conn := createTestConn(t, &buf)

			err := conn.writeDataRow(tt.row)
			assert.NoError(t, err)

			// Verify message type.
			msgType, err := buf.ReadByte()
			require.NoError(t, err)
			assert.Equal(t, byte(protocol.MsgDataRow), msgType)

			// Read and verify length.
			var length int32
			err = binary.Read(&buf, binary.BigEndian, &length)
			require.NoError(t, err)

			// Read column count.
			var colCount int16
			err = binary.Read(&buf, binary.BigEndian, &colCount)
			require.NoError(t, err)
			assert.Equal(t, int16(len(tt.row.Values)), colCount)

			// Verify each column value.
			for i, expectedValue := range tt.row.Values {
				var valueLen int32
				err = binary.Read(&buf, binary.BigEndian, &valueLen)
				require.NoError(t, err, "column %d", i)

				if expectedValue == nil {
					assert.Equal(t, int32(-1), valueLen, "column %d should be NULL", i)
				} else {
					assert.Equal(t, int32(len(expectedValue)), valueLen, "column %d length", i)
					value := make([]byte, valueLen)
					_, err = io.ReadFull(&buf, value)
					require.NoError(t, err)
					assert.Equal(t, []byte(expectedValue), value, "column %d value", i)
				}
			}
		})
	}
}

// TestWriteCommandComplete tests encoding of CommandComplete messages.
func TestWriteCommandComplete(t *testing.T) {
	tests := []struct {
		name string
		tag  string
	}{
		{
			name: "SELECT",
			tag:  "SELECT 5",
		},
		{
			name: "INSERT",
			tag:  "INSERT 0 1",
		},
		{
			name: "UPDATE",
			tag:  "UPDATE 3",
		},
		{
			name: "DELETE",
			tag:  "DELETE 2",
		},
		{
			name: "CREATE TABLE",
			tag:  "CREATE TABLE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			conn := createTestConn(t, &buf)

			err := conn.writeCommandComplete(tt.tag)
			assert.NoError(t, err)

			// Verify message type.
			msgType, err := buf.ReadByte()
			require.NoError(t, err)
			assert.Equal(t, byte(protocol.MsgCommandComplete), msgType)

			// Read and verify length.
			var length int32
			err = binary.Read(&buf, binary.BigEndian, &length)
			require.NoError(t, err)

			// Read command tag.
			tag, err := readNullTerminatedString(&buf)
			require.NoError(t, err)
			assert.Equal(t, tt.tag, tag)
		})
	}
}

// TestWriteSimpleErrorWithDetail tests encoding of ErrorResponse messages.
func TestWriteSimpleErrorWithDetail(t *testing.T) {
	tests := []struct {
		name     string
		severity string
		sqlState string
		message  string
		detail   string
		hint     string
	}{
		{
			name:     "basic error",
			severity: "ERROR",
			sqlState: "42P01",
			message:  "relation \"users\" does not exist",
			detail:   "",
			hint:     "",
		},
		{
			name:     "error with detail and hint",
			severity: "ERROR",
			sqlState: "23505",
			message:  "duplicate key value violates unique constraint",
			detail:   "Key (id)=(1) already exists.",
			hint:     "Use a different value for the id column.",
		},
		{
			name:     "fatal error",
			severity: "FATAL",
			sqlState: "08P01",
			message:  "protocol violation",
			detail:   "",
			hint:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			conn := createTestConn(t, &buf)

			err := conn.writeSimpleErrorWithDetail(tt.severity, tt.sqlState, tt.message, tt.detail, tt.hint)
			assert.NoError(t, err)

			// Verify message type.
			msgType, err := buf.ReadByte()
			require.NoError(t, err)
			assert.Equal(t, byte(protocol.MsgErrorResponse), msgType)

			// Read and verify length.
			var length int32
			err = binary.Read(&buf, binary.BigEndian, &length)
			require.NoError(t, err)

			// Read and verify fields.
			fields := make(map[byte]string)
			for {
				fieldType, err := buf.ReadByte()
				require.NoError(t, err)

				if fieldType == 0 {
					break // Terminator
				}

				value, err := readNullTerminatedString(&buf)
				require.NoError(t, err)
				fields[fieldType] = value
			}

			// Verify required fields.
			assert.Equal(t, tt.severity, fields[protocol.FieldSeverity])
			assert.Equal(t, tt.severity, fields[protocol.FieldSeverityV])
			assert.Equal(t, tt.sqlState, fields[protocol.FieldCode])
			assert.Equal(t, tt.message, fields[protocol.FieldMessage])

			// Verify optional fields.
			if tt.detail != "" {
				assert.Equal(t, tt.detail, fields[protocol.FieldDetail])
			}
			if tt.hint != "" {
				assert.Equal(t, tt.hint, fields[protocol.FieldHint])
			}
		})
	}
}

// Helper function to read a null-terminated string from a reader.
func readNullTerminatedString(r io.Reader) (string, error) {
	var result []byte
	buf := make([]byte, 1)

	for {
		_, err := r.Read(buf)
		if err != nil {
			return "", err
		}
		if buf[0] == 0 {
			break
		}
		result = append(result, buf[0])
	}

	return string(result), nil
}
