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
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"
)

// readQueryMessage reads a 'Q' (Query) message from the connection.
// The message format is:
//   - Message type: 'Q' (already read)
//   - Length: int32 (includes length field itself)
//   - Query string: null-terminated string
func (c *Conn) readQueryMessage() (string, error) {
	// Read the message length (includes itself, 4 bytes).
	var length int32
	if err := binary.Read(c.bufferedReader, binary.BigEndian, &length); err != nil {
		return "", fmt.Errorf("reading query length: %w", err)
	}

	// Validate length (must be at least 4 for the length field + 1 for null terminator).
	if length < 5 {
		return "", fmt.Errorf("invalid query message length: %d", length)
	}

	// Calculate body size (length - 4 bytes for length field).
	bodySize := length - 4

	// Read the query string.
	queryBytes := make([]byte, bodySize)
	if _, err := io.ReadFull(c.bufferedReader, queryBytes); err != nil {
		return "", fmt.Errorf("reading query body: %w", err)
	}

	// Verify null terminator.
	if queryBytes[len(queryBytes)-1] != 0 {
		return "", errors.New("query string missing null terminator")
	}

	// Convert to string (excluding null terminator).
	queryStr := string(queryBytes[:len(queryBytes)-1])
	return queryStr, nil
}

// writeParameterDescription writes a 't' (ParameterDescription) message.
// Format:
//   - Type: 't'
//   - Length: int32
//   - Parameter count: int16
//   - For each parameter:
//   - Type OID: int32
func (c *Conn) writeParameterDescription(params []*query.ParameterDescription) error {
	bodyLen := 2 + 4*len(params)
	buf, pos := c.startPacket(protocol.MsgParameterDescription, bodyLen)
	pos = writeInt16At(buf, pos, int16(len(params)))
	for _, param := range params {
		pos = writeInt32At(buf, pos, int32(param.DataTypeOid))
	}
	_ = pos
	return c.writePacket(buf)
}

// writeRowDescription writes a 'T' (RowDescription) message.
// Format:
//   - Type: 'T'
//   - Length: int32
//   - Field count: int16
//   - For each field:
//   - Field name: null-terminated string
//   - Table OID: int32
//   - Column number: int16
//   - Type OID: int32
//   - Type size: int16
//   - Type modifier: int32
//   - Format code: int16 (0=text, 1=binary)
func (c *Conn) writeRowDescription(fields []*query.Field) error {
	bodyLen := 2 // field count
	for _, field := range fields {
		bodyLen += len(field.Name) + 1 // name + null terminator
		bodyLen += 4 + 2 + 4 + 2 + 4 + 2
	}

	buf, pos := c.startPacket(protocol.MsgRowDescription, bodyLen)
	pos = writeInt16At(buf, pos, int16(len(fields)))
	for _, field := range fields {
		pos = writeStringAt(buf, pos, field.Name)
		pos = writeInt32At(buf, pos, int32(field.TableOid))
		pos = writeInt16At(buf, pos, int16(field.TableAttributeNumber))
		pos = writeInt32At(buf, pos, int32(field.DataTypeOid))
		pos = writeInt16At(buf, pos, int16(field.DataTypeSize))
		pos = writeInt32At(buf, pos, field.TypeModifier)
		pos = writeInt16At(buf, pos, int16(field.Format))
	}
	_ = pos
	return c.writePacket(buf)
}

// writeDataRow writes a 'D' (DataRow) message.
// Format:
//   - Type: 'D'
//   - Length: int32
//   - Column count: int16
//   - For each column:
//   - Value length: int32 (-1 for NULL)
//   - Value bytes: []byte (if not NULL)
func (c *Conn) writeDataRow(row *sqltypes.Row) error {
	bodyLen := 2 // column count
	for _, value := range row.Values {
		bodyLen += 4
		if value != nil {
			bodyLen += len(value)
		}
	}

	buf, pos := c.startPacket(protocol.MsgDataRow, bodyLen)
	pos = writeInt16At(buf, pos, int16(len(row.Values)))
	for _, value := range row.Values {
		if value == nil {
			pos = writeInt32At(buf, pos, -1)
		} else {
			pos = writeInt32At(buf, pos, int32(len(value)))
			pos = writeBytesAt(buf, pos, value)
		}
	}
	_ = pos
	return c.writePacket(buf)
}

// writeCommandComplete writes a 'C' (CommandComplete) message.
// Format:
//   - Type: 'C'
//   - Length: int32
//   - Command tag: null-terminated string (e.g., "SELECT 5", "INSERT 0 1")
func (c *Conn) writeCommandComplete(tag string) error {
	buf, pos := c.startPacket(protocol.MsgCommandComplete, len(tag)+1)
	pos = writeStringAt(buf, pos, tag)
	_ = pos
	return c.writePacket(buf)
}

// writeReadyForQuery writes a 'Z' (ReadyForQuery) message.
// Format:
//   - Type: 'Z'
//   - Length: int32 (always 5)
//   - Transaction status: byte ('I', 'T', or 'E')
func (c *Conn) writeReadyForQuery() error {
	buf, pos := c.startPacket(protocol.MsgReadyForQuery, 1)
	pos = writeByteAt(buf, pos, byte(c.txnStatus))
	_ = pos
	return c.writePacket(buf)
}

// writeEmptyQueryResponse writes an 'I' (EmptyQueryResponse) message.
// This is sent when the client sends an empty query string.
// Format:
//   - Type: 'I'
//   - Length: int32 (always 4)
func (c *Conn) writeEmptyQueryResponse() error {
	buf, _ := c.startPacket(protocol.MsgEmptyQueryResponse, 0)
	return c.writePacket(buf)
}

// writeNoticeResponse writes an 'N' (NoticeResponse) message.
// Format is identical to ErrorResponse but with different severity levels.
func (c *Conn) writeNoticeResponse(diag *mterrors.PgDiagnostic) error {
	return c.writePgDiagnosticResponse(protocol.MsgNoticeResponse, diag)
}

// writeError writes an error response to the client.
// It handles both PostgreSQL errors (preserving all diagnostic fields)
// and generic errors (creating synthetic PgDiagnostic).
func (c *Conn) writeError(err error) error {
	if err == nil {
		return nil
	}

	// Extract root cause - handles wrapped errors
	rootErr := mterrors.RootCause(err)

	// Check if root cause is a PostgreSQL error
	var diag *mterrors.PgDiagnostic
	if errors.As(rootErr, &diag) {
		return c.writePgDiagnosticResponse(protocol.MsgErrorResponse, diag)
	}

	// Generic error: use outer message for context
	return c.writePgDiagnosticResponse(protocol.MsgErrorResponse,
		mterrors.NewPgError("ERROR", mterrors.PgSSInternalError, err.Error(), ""))
}

// writePgDiagnosticResponse writes a PostgreSQL diagnostic response (error or notice).
// The msgType should be MsgErrorResponse ('E') or MsgNoticeResponse ('N').
// This unified function handles all 14 PostgreSQL diagnostic fields.
func (c *Conn) writePgDiagnosticResponse(msgType byte, diag *mterrors.PgDiagnostic) error {
	fields := make(map[byte]string)
	fields[protocol.FieldSeverity] = diag.Severity
	fields[protocol.FieldSeverityV] = diag.Severity
	fields[protocol.FieldCode] = diag.Code
	fields[protocol.FieldMessage] = diag.Message
	if diag.Detail != "" {
		fields[protocol.FieldDetail] = diag.Detail
	}
	if diag.Hint != "" {
		fields[protocol.FieldHint] = diag.Hint
	}
	if diag.Position != 0 {
		fields[protocol.FieldPosition] = strconv.Itoa(int(diag.Position))
	}
	if diag.InternalPosition != 0 {
		fields[protocol.FieldInternalPosition] = strconv.Itoa(int(diag.InternalPosition))
	}
	if diag.InternalQuery != "" {
		fields[protocol.FieldInternalQuery] = diag.InternalQuery
	}
	if diag.Where != "" {
		fields[protocol.FieldWhere] = diag.Where
	}
	if diag.Schema != "" {
		fields[protocol.FieldSchema] = diag.Schema
	}
	if diag.Table != "" {
		fields[protocol.FieldTable] = diag.Table
	}
	if diag.Column != "" {
		fields[protocol.FieldColumn] = diag.Column
	}
	if diag.DataType != "" {
		fields[protocol.FieldDataType] = diag.DataType
	}
	if diag.Constraint != "" {
		fields[protocol.FieldConstraint] = diag.Constraint
	}

	return c.writeErrorOrNotice(msgType, fields)
}

// writeErrorOrNotice writes an error or notice message with the given fields.
//
// PostgreSQL field order (S, V, C, M, D, H, P, p, q, W, s, t, c, d, n, F, L, R)
// is preserved across the bodyLen pre-pass and the in-place encoding so the
// buffer is sized to exactly the bytes encoded.
var diagFieldOrder = [...]byte{
	protocol.FieldSeverity,
	protocol.FieldSeverityV,
	protocol.FieldCode,
	protocol.FieldMessage,
	protocol.FieldDetail,
	protocol.FieldHint,
	protocol.FieldPosition,
	protocol.FieldInternalPosition,
	protocol.FieldInternalQuery,
	protocol.FieldWhere,
	protocol.FieldSchema,
	protocol.FieldTable,
	protocol.FieldColumn,
	protocol.FieldDataType,
	protocol.FieldConstraint,
	protocol.FieldFile,
	protocol.FieldLine,
	protocol.FieldRoutine,
}

func (c *Conn) writeErrorOrNotice(msgType byte, fields map[byte]string) error {
	bodyLen := 1 // trailing null terminator
	for _, fieldType := range diagFieldOrder {
		if value, ok := fields[fieldType]; ok {
			bodyLen += 1 + len(value) + 1 // type + value + null
		}
	}

	buf, pos := c.startPacket(msgType, bodyLen)
	for _, fieldType := range diagFieldOrder {
		if value, ok := fields[fieldType]; ok {
			pos = writeByteAt(buf, pos, fieldType)
			pos = writeStringAt(buf, pos, value)
		}
	}
	pos = writeByteAt(buf, pos, 0)
	_ = pos
	return c.writePacket(buf)
}

// WriteCopyInResponse writes a CopyInResponse ('G') message to the client
// This tells the client that the server is ready to receive COPY data
func (c *Conn) WriteCopyInResponse(format int16, columnFormats []int16) error {
	bodyLen := 1 + 2 + 2*len(columnFormats)
	buf, pos := c.startPacket(protocol.MsgCopyInResponse, bodyLen)
	pos = writeByteAt(buf, pos, byte(format))
	pos = writeInt16At(buf, pos, int16(len(columnFormats)))
	for _, fmt := range columnFormats {
		pos = writeInt16At(buf, pos, fmt)
	}
	_ = pos
	return c.writePacket(buf)
}

// ReadCopyDataMessage reads a CopyData ('d') message body
// The message type byte has already been read
// length is the body length (already has 4 subtracted by ReadMessageLength)
func (c *Conn) ReadCopyDataMessage(length int) ([]byte, error) {
	if length < 0 {
		return nil, fmt.Errorf("invalid CopyData message length: %d", length)
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(c.bufferedReader, data); err != nil {
		return nil, fmt.Errorf("failed to read CopyData: %w", err)
	}

	return data, nil
}

// ReadCopyDoneMessage reads a CopyDone ('c') message
// The message type byte has already been read
// CopyDone has no body, just validates the length
// length is the body length (already has 4 subtracted by ReadMessageLength)
func (c *Conn) ReadCopyDoneMessage(length int) error {
	// CopyDone has no body, so length should be 0
	if length != 0 {
		return fmt.Errorf("invalid CopyDone message length: %d (expected 0)", length)
	}
	return nil
}

// ReadCopyFailMessage reads a CopyFail ('f') message
// The message type byte has already been read
// Returns the error message string from the client
// length is the body length (already has 4 subtracted by ReadMessageLength)
func (c *Conn) ReadCopyFailMessage(length int) (string, error) {
	if length < 0 {
		return "", fmt.Errorf("invalid CopyFail message length: %d", length)
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(c.bufferedReader, data); err != nil {
		return "", fmt.Errorf("failed to read CopyFail: %w", err)
	}

	// Message should be null-terminated
	if len(data) > 0 && data[len(data)-1] == 0 {
		data = data[:len(data)-1]
	}

	return string(data), nil
}
