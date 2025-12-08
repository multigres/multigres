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
	"context"
	"fmt"

	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/pgprotocol/protocol"
)

// Query executes a simple query and returns all results.
// For large result sets, consider using QueryStreaming instead.
func (c *Conn) Query(ctx context.Context, queryStr string) ([]*query.QueryResult, error) {
	var results []*query.QueryResult

	err := c.QueryStreaming(ctx, queryStr, func(ctx context.Context, result *query.QueryResult) error {
		results = append(results, result)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return results, nil
}

// QueryStreaming executes a simple query and streams results via callback.
// The callback is called for each result set (there can be multiple for multi-statement queries).
func (c *Conn) QueryStreaming(ctx context.Context, queryStr string, callback func(ctx context.Context, result *query.QueryResult) error) error {
	c.bufmu.Lock()
	defer c.bufmu.Unlock()

	// Send the Query message.
	if err := c.writeQueryMessage(queryStr); err != nil {
		return fmt.Errorf("failed to send query: %w", err)
	}

	// Process responses.
	return c.processQueryResponses(ctx, callback)
}

// writeQueryMessage writes a 'Q' (Query) message.
func (c *Conn) writeQueryMessage(queryStr string) error {
	w := NewMessageWriter()
	w.WriteString(queryStr)
	return c.writeMessage(protocol.MsgQuery, w.Bytes())
}

// processQueryResponses processes all responses to a query until ReadyForQuery.
func (c *Conn) processQueryResponses(ctx context.Context, callback func(ctx context.Context, result *query.QueryResult) error) error {
	var currentResult *query.QueryResult

	for {
		// Check context.
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Read message.
		msgType, body, err := c.readMessage()
		if err != nil {
			return fmt.Errorf("failed to read message: %w", err)
		}

		switch msgType {
		case protocol.MsgRowDescription:
			// Start of a new result set.
			currentResult = &query.QueryResult{}
			if err := c.parseRowDescription(body, currentResult); err != nil {
				return err
			}

		case protocol.MsgDataRow:
			if currentResult == nil {
				currentResult = &query.QueryResult{}
			}
			row, err := c.parseDataRow(body)
			if err != nil {
				return err
			}
			currentResult.Rows = append(currentResult.Rows, row)

		case protocol.MsgCommandComplete:
			if currentResult == nil {
				currentResult = &query.QueryResult{}
			}
			tag, err := c.parseCommandComplete(body)
			if err != nil {
				return err
			}
			currentResult.CommandTag = tag
			currentResult.RowsAffected = parseRowsAffected(tag)

			// Call the callback with the completed result.
			if callback != nil {
				if err := callback(ctx, currentResult); err != nil {
					return err
				}
			}
			currentResult = nil

		case protocol.MsgEmptyQueryResponse:
			// Empty query, call callback with empty result.
			if callback != nil {
				if err := callback(ctx, &query.QueryResult{}); err != nil {
					return err
				}
			}

		case protocol.MsgReadyForQuery:
			// Query complete.
			c.txnStatus = body[0]
			return nil

		case protocol.MsgErrorResponse:
			return c.parseError(body)

		case protocol.MsgNoticeResponse:
			// Ignore notices for now.

		case protocol.MsgParameterStatus:
			// Handle parameter status updates.
			if err := c.handleParameterStatus(body); err != nil {
				return err
			}

		default:
			return fmt.Errorf("unexpected message type in query response: %c (0x%02x)", msgType, msgType)
		}
	}
}

// parseRowDescription parses a RowDescription message.
func (c *Conn) parseRowDescription(body []byte, result *query.QueryResult) error {
	reader := NewMessageReader(body)

	fieldCount, err := reader.ReadInt16()
	if err != nil {
		return fmt.Errorf("failed to read field count: %w", err)
	}

	result.Fields = make([]*query.Field, fieldCount)

	for i := range fieldCount {
		field := &query.Field{}

		field.Name, err = reader.ReadString()
		if err != nil {
			return fmt.Errorf("failed to read field name: %w", err)
		}

		tableOID, err := reader.ReadUint32()
		if err != nil {
			return fmt.Errorf("failed to read table OID: %w", err)
		}
		field.TableOid = tableOID

		attrNum, err := reader.ReadInt16()
		if err != nil {
			return fmt.Errorf("failed to read attribute number: %w", err)
		}
		field.TableAttributeNumber = int32(attrNum)

		dataTypeOID, err := reader.ReadUint32()
		if err != nil {
			return fmt.Errorf("failed to read data type OID: %w", err)
		}
		field.DataTypeOid = dataTypeOID

		dataTypeSize, err := reader.ReadInt16()
		if err != nil {
			return fmt.Errorf("failed to read data type size: %w", err)
		}
		field.DataTypeSize = int32(dataTypeSize)

		typeMod, err := reader.ReadInt32()
		if err != nil {
			return fmt.Errorf("failed to read type modifier: %w", err)
		}
		field.TypeModifier = typeMod

		formatCode, err := reader.ReadInt16()
		if err != nil {
			return fmt.Errorf("failed to read format code: %w", err)
		}
		field.Format = int32(formatCode)

		result.Fields[i] = field
	}

	return nil
}

// parseDataRow parses a DataRow message.
func (c *Conn) parseDataRow(body []byte) (*query.Row, error) {
	reader := NewMessageReader(body)

	columnCount, err := reader.ReadInt16()
	if err != nil {
		return nil, fmt.Errorf("failed to read column count: %w", err)
	}

	row := &query.Row{
		Values: make([][]byte, columnCount),
	}

	for i := range columnCount {
		value, err := reader.ReadByteString()
		if err != nil {
			return nil, fmt.Errorf("failed to read column value: %w", err)
		}
		row.Values[i] = value
	}

	return row, nil
}

// parseCommandComplete parses a CommandComplete message.
func (c *Conn) parseCommandComplete(body []byte) (string, error) {
	reader := NewMessageReader(body)
	tag, err := reader.ReadString()
	if err != nil {
		return "", fmt.Errorf("failed to read command tag: %w", err)
	}
	return tag, nil
}

// parseRowsAffected extracts the row count from a command tag.
func parseRowsAffected(tag string) uint64 {
	// Command tags have formats like:
	// - "SELECT 5" (5 rows)
	// - "INSERT 0 1" (1 row inserted)
	// - "UPDATE 10" (10 rows updated)
	// - "DELETE 3" (3 rows deleted)

	// Find the last space-separated number.
	var count uint64
	var num uint64
	inNumber := false

	for i := len(tag) - 1; i >= 0; i-- {
		c := tag[i]
		if c >= '0' && c <= '9' {
			if !inNumber {
				inNumber = true
				count = 0
				num = 1
			}
			count += uint64(c-'0') * num
			num *= 10
		} else if c == ' ' {
			if inNumber {
				return count
			}
		} else {
			break
		}
	}

	if inNumber {
		return count
	}
	return 0
}

// parseError parses an ErrorResponse message into an error.
func (c *Conn) parseError(body []byte) error {
	reader := NewMessageReader(body)

	var severity, code, message, detail, hint string

	for reader.Remaining() > 0 {
		fieldType, err := reader.ReadByte()
		if err != nil {
			break
		}
		if fieldType == 0 {
			break // End of fields.
		}

		value, err := reader.ReadString()
		if err != nil {
			break
		}

		switch fieldType {
		case protocol.FieldSeverity:
			severity = value
		case protocol.FieldCode:
			code = value
		case protocol.FieldMessage:
			message = value
		case protocol.FieldDetail:
			detail = value
		case protocol.FieldHint:
			hint = value
		}
	}

	return &Error{
		Severity: severity,
		Code:     code,
		Message:  message,
		Detail:   detail,
		Hint:     hint,
	}
}

// Error represents a PostgreSQL error response.
type Error struct {
	Severity string
	Code     string
	Message  string
	Detail   string
	Hint     string
}

// Error implements the error interface.
func (e *Error) Error() string {
	if e.Detail != "" {
		return fmt.Sprintf("%s: %s (SQLSTATE %s)\nDETAIL: %s", e.Severity, e.Message, e.Code, e.Detail)
	}
	return fmt.Sprintf("%s: %s (SQLSTATE %s)", e.Severity, e.Message, e.Code)
}

// IsSQLState checks if the error has the given SQLSTATE code.
func (e *Error) IsSQLState(code string) bool {
	return e.Code == code
}
