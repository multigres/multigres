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
	// Calculate message size: length + param count + (param count * 4 bytes per OID).
	size := 4 + 2 + (len(params) * 4)

	w := c.getWriter()

	// Write message type.
	if err := writeByte(w, protocol.MsgParameterDescription); err != nil {
		return err
	}

	// Write message length.
	if err := writeInt32(w, int32(size)); err != nil {
		return err
	}

	// Write parameter count.
	if err := writeInt16(w, int16(len(params))); err != nil {
		return err
	}

	// Write each parameter OID.
	for _, param := range params {
		if err := writeInt32(w, int32(param.DataTypeOid)); err != nil {
			return err
		}
	}

	return nil
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
	if len(fields) == 0 {
		return nil // No fields to describe
	}

	// Calculate message size.
	size := 4 + 2 // length + field count
	for _, field := range fields {
		size += len(field.Name) + 1 // name + null terminator
		size += 4                   // table OID
		size += 2                   // column number
		size += 4                   // type OID
		size += 2                   // type size
		size += 4                   // type modifier
		size += 2                   // format code
	}

	w := c.getWriter()

	// Write message type.
	if err := writeByte(w, protocol.MsgRowDescription); err != nil {
		return err
	}

	// Write message length.
	if err := writeInt32(w, int32(size)); err != nil {
		return err
	}

	// Write field count.
	if err := writeInt16(w, int16(len(fields))); err != nil {
		return err
	}

	// Write each field.
	for _, field := range fields {
		// Field name (null-terminated).
		if err := writeString(w, field.Name); err != nil {
			return err
		}

		// Table OID.
		if err := writeInt32(w, int32(field.TableOid)); err != nil {
			return err
		}

		// Column number (attribute number).
		if err := writeInt16(w, int16(field.TableAttributeNumber)); err != nil {
			return err
		}

		// Type OID.
		if err := writeInt32(w, int32(field.DataTypeOid)); err != nil {
			return err
		}

		// Type size.
		if err := writeInt16(w, int16(field.DataTypeSize)); err != nil {
			return err
		}

		// Type modifier.
		if err := writeInt32(w, field.TypeModifier); err != nil {
			return err
		}

		// Format code.
		if err := writeInt16(w, int16(field.Format)); err != nil {
			return err
		}
	}

	return nil
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
	// Calculate message size.
	size := 4 + 2 // length + column count
	for _, value := range row.Values {
		size += 4 // value length
		if value != nil {
			size += len(value)
		}
	}

	w := c.getWriter()

	// Write message type.
	if err := writeByte(w, protocol.MsgDataRow); err != nil {
		return err
	}

	// Write message length.
	if err := writeInt32(w, int32(size)); err != nil {
		return err
	}

	// Write column count.
	if err := writeInt16(w, int16(len(row.Values))); err != nil {
		return err
	}

	// Write each column value.
	for _, value := range row.Values {
		if value == nil {
			// NULL value.
			if err := writeInt32(w, -1); err != nil {
				return err
			}
		} else {
			// Non-NULL value.
			if err := writeInt32(w, int32(len(value))); err != nil {
				return err
			}
			if _, err := w.Write(value); err != nil {
				return fmt.Errorf("writing data row value: %w", err)
			}
		}
	}

	return nil
}

// writeCommandComplete writes a 'C' (CommandComplete) message.
// Format:
//   - Type: 'C'
//   - Length: int32
//   - Command tag: null-terminated string (e.g., "SELECT 5", "INSERT 0 1")
func (c *Conn) writeCommandComplete(tag string) error {
	// Calculate message size: length + tag + null terminator.
	size := 4 + len(tag) + 1

	w := c.getWriter()

	// Write message type.
	if err := writeByte(w, protocol.MsgCommandComplete); err != nil {
		return err
	}

	// Write message length.
	if err := writeInt32(w, int32(size)); err != nil {
		return err
	}

	// Write command tag (null-terminated).
	if err := writeString(w, tag); err != nil {
		return err
	}

	return nil
}

// writeReadyForQuery writes a 'Z' (ReadyForQuery) message.
// Format:
//   - Type: 'Z'
//   - Length: int32 (always 5)
//   - Transaction status: byte ('I', 'T', or 'E')
func (c *Conn) writeReadyForQuery() error {
	w := c.getWriter()

	// Write message type.
	if err := writeByte(w, protocol.MsgReadyForQuery); err != nil {
		return err
	}

	// Write message length (always 5: 4 for length + 1 for status).
	if err := writeInt32(w, 5); err != nil {
		return err
	}

	// Write transaction status.
	if err := writeByte(w, c.txnStatus); err != nil {
		return err
	}

	return nil
}

// writeEmptyQueryResponse writes an 'I' (EmptyQueryResponse) message.
// This is sent when the client sends an empty query string.
// Format:
//   - Type: 'I'
//   - Length: int32 (always 4)
func (c *Conn) writeEmptyQueryResponse() error {
	w := c.getWriter()

	// Write message type.
	if err := writeByte(w, protocol.MsgEmptyQueryResponse); err != nil {
		return err
	}

	// Write message length (always 4: just the length field itself).
	if err := writeInt32(w, 4); err != nil {
		return err
	}

	return nil
}

// writeSimpleError writes an 'E' (ErrorResponse) message for non-PostgreSQL errors.
// It creates a minimal PgDiagnostic and uses the unified writePgDiagnosticResponse.
// Use this for internal errors that don't originate from PostgreSQL.
//
// For PostgreSQL errors with full diagnostic information, use writeErrorFromDiagnostic instead.
func (c *Conn) writeSimpleError(sqlState, message string) error {
	diag := &sqltypes.PgDiagnostic{
		MessageType: protocol.MsgErrorResponse,
		Severity:    "ERROR",
		Code:        sqlState,
		Message:     message,
	}
	return c.writePgDiagnosticResponse(protocol.MsgErrorResponse, diag)
}

// writeSimpleErrorWithDetail writes an 'E' (ErrorResponse) message with detail and hint.
// It creates a PgDiagnostic with the provided fields and uses the unified writePgDiagnosticResponse.
// Use this for internal errors that don't originate from PostgreSQL but need additional context.
//
// For PostgreSQL errors with full diagnostic information, use writeErrorFromDiagnostic instead.
func (c *Conn) writeSimpleErrorWithDetail(severity, sqlState, message, detail, hint string) error {
	diag := &sqltypes.PgDiagnostic{
		MessageType: protocol.MsgErrorResponse,
		Severity:    severity,
		Code:        sqlState,
		Message:     message,
		Detail:      detail,
		Hint:        hint,
	}
	return c.writePgDiagnosticResponse(protocol.MsgErrorResponse, diag)
}

// writeNoticeResponse writes an 'N' (NoticeResponse) message.
// Format is identical to ErrorResponse but with different severity levels.
func (c *Conn) writeNoticeResponse(diag *sqltypes.PgDiagnostic) error {
	return c.writePgDiagnosticResponse(protocol.MsgNoticeResponse, diag)
}

// writeErrorFromDiagnostic writes an 'E' (ErrorResponse) message using a PgDiagnostic.
// This preserves all PostgreSQL error fields for native error format output.
func (c *Conn) writeErrorFromDiagnostic(diag *sqltypes.PgDiagnostic) error {
	return c.writePgDiagnosticResponse(protocol.MsgErrorResponse, diag)
}

// writePgDiagnosticResponse writes a PostgreSQL diagnostic response (error or notice).
// The msgType should be MsgErrorResponse ('E') or MsgNoticeResponse ('N').
// This unified function handles all 14 PostgreSQL diagnostic fields.
func (c *Conn) writePgDiagnosticResponse(msgType byte, diag *sqltypes.PgDiagnostic) error {
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
func (c *Conn) writeErrorOrNotice(msgType byte, fields map[byte]string) error {
	// Calculate message size.
	size := 4 + 1 // length + terminator
	for _, value := range fields {
		size += 1              // field type
		size += len(value) + 1 // value + null terminator
	}

	w := c.getWriter()

	// Write message type.
	if err := writeByte(w, msgType); err != nil {
		return err
	}

	// Write message length.
	if err := writeInt32(w, int32(size)); err != nil {
		return err
	}

	// Write fields in a defined order for consistency.
	// Order follows PostgreSQL convention: S, V, C, M, D, H, P, p, q, W, s, t, c, d, n, F, L, R
	fieldOrder := []byte{
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

	for _, fieldType := range fieldOrder {
		if value, ok := fields[fieldType]; ok {
			if err := writeByte(w, fieldType); err != nil {
				return err
			}
			if err := writeString(w, value); err != nil {
				return err
			}
		}
	}

	// Write terminator.
	if err := writeByte(w, 0); err != nil {
		return err
	}

	return nil
}

// WriteCopyInResponse writes a CopyInResponse ('G') message to the client
// This tells the client that the server is ready to receive COPY data
func (c *Conn) WriteCopyInResponse(format int16, columnFormats []int16) error {
	// Calculate message size: 4 (length) + 1 (format as Int8) + 2 (num columns) + 2*numCols (column formats)
	size := 4 + 1 + 2 + (2 * len(columnFormats))

	w := c.getWriter()

	// Write message type
	if err := writeByte(w, protocol.MsgCopyInResponse); err != nil {
		return err
	}

	// Write message length
	if err := writeInt32(w, int32(size)); err != nil {
		return err
	}

	// Write overall format as Int8 (1 byte) - 0=text, 1=binary
	if err := writeByte(w, byte(format)); err != nil {
		return err
	}

	// Write number of columns (Int16, 2 bytes)
	if err := writeInt16(w, int16(len(columnFormats))); err != nil {
		return err
	}

	// Write format code for each column (Int16 each)
	for _, fmt := range columnFormats {
		if err := writeInt16(w, fmt); err != nil {
			return err
		}
	}

	return nil
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

// Helper functions for writing protocol data types.

func writeByte(w io.Writer, b byte) error {
	_, err := w.Write([]byte{b})
	if err != nil {
		return fmt.Errorf("writing byte: %w", err)
	}
	return nil
}

func writeInt16(w io.Writer, n int16) error {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], uint16(n))
	if _, err := w.Write(buf[:]); err != nil {
		return fmt.Errorf("writing int16: %w", err)
	}
	return nil
}

func writeInt32(w io.Writer, n int32) error {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(n))
	if _, err := w.Write(buf[:]); err != nil {
		return fmt.Errorf("writing int32: %w", err)
	}
	return nil
}

func writeString(w io.Writer, s string) error {
	if _, err := w.Write([]byte(s)); err != nil {
		return fmt.Errorf("writing string: %w", err)
	}
	// Write null terminator.
	if err := writeByte(w, 0); err != nil {
		return err
	}
	return nil
}
