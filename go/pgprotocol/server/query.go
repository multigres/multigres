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

	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/pgprotocol/protocol"
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
		return "", fmt.Errorf("query string missing null terminator")
	}

	// Convert to string (excluding null terminator).
	queryStr := string(queryBytes[:len(queryBytes)-1])
	return queryStr, nil
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
func (c *Conn) writeDataRow(row *query.Row) error {
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

// writeErrorResponse writes an 'E' (ErrorResponse) message.
// Format:
//   - Type: 'E'
//   - Length: int32
//   - Fields: series of field-value pairs (each null-terminated)
//   - Terminator: byte(0)
//
// Common fields:
//   - 'S': Severity (ERROR, FATAL, PANIC)
//   - 'V': Severity (non-localized)
//   - 'C': SQLSTATE code (5 characters)
//   - 'M': Primary message
//   - 'D': Detail
//   - 'H': Hint
func (c *Conn) writeErrorResponse(severity, sqlState, message, detail, hint string) error {
	// Build the fields map.
	fields := make(map[byte]string)
	fields[protocol.FieldSeverity] = severity
	fields[protocol.FieldSeverityV] = severity // Non-localized version
	fields[protocol.FieldCode] = sqlState
	fields[protocol.FieldMessage] = message
	if detail != "" {
		fields[protocol.FieldDetail] = detail
	}
	if hint != "" {
		fields[protocol.FieldHint] = hint
	}

	return c.writeErrorOrNotice(protocol.MsgErrorResponse, fields)
}

// writeNoticeResponse writes an 'N' (NoticeResponse) message.
// Format is identical to ErrorResponse but with different severity levels.
func (c *Conn) writeNoticeResponse(severity, sqlState, message, detail, hint string) error {
	fields := make(map[byte]string)
	fields[protocol.FieldSeverity] = severity
	fields[protocol.FieldSeverityV] = severity
	fields[protocol.FieldCode] = sqlState
	fields[protocol.FieldMessage] = message
	if detail != "" {
		fields[protocol.FieldDetail] = detail
	}
	if hint != "" {
		fields[protocol.FieldHint] = hint
	}

	return c.writeErrorOrNotice(protocol.MsgNoticeResponse, fields)
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
	// Order: S, V, C, M, D, H, and others
	fieldOrder := []byte{
		protocol.FieldSeverity,
		protocol.FieldSeverityV,
		protocol.FieldCode,
		protocol.FieldMessage,
		protocol.FieldDetail,
		protocol.FieldHint,
		protocol.FieldPosition,
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
