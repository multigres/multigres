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
	"log/slog"
	"strconv"

	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/tools/telemetry"
)

// DefaultStreamingBatchSize is the default size threshold (in bytes) for batching
// rows during streaming. When accumulated row data exceeds this size, the batch
// is flushed via callback. This balances memory efficiency with callback overhead.
const DefaultStreamingBatchSize = 2 * 1024 * 1024 // 2MB

// queryTracingKey is the context key for query tracing configuration.
type queryTracingKey struct{}

// QueryTracingConfig holds optional configuration for query tracing.
// Spans are always created for queries; this config controls optional details.
type QueryTracingConfig struct {
	// OperationName is a semantic name for the operation (e.g., "pg_is_in_recovery").
	// This should describe what the query does, not the SQL itself.
	// If empty, "QUERY" will be used.
	OperationName string

	// IncludeQueryText enables recording the SQL query text in the span.
	//
	// SECURITY WARNING: This should ONLY be enabled for internal system queries where:
	// - The SQL is hardcoded or uses PostgreSQL system functions
	// - No user-provided data appears in the query text
	// - No PII (Personally Identifiable Information) is included
	//
	// Examples of SAFE usage (internal queries):
	// - SELECT pg_is_in_recovery()
	// - SHOW server_version
	// - SELECT setting FROM pg_settings WHERE name = 'max_connections'
	//
	// Examples of UNSAFE usage (NEVER enable for these):
	// - Any query containing user input
	// - SELECT * FROM users WHERE email = 'user@example.com'
	// - Queries with bind parameters that may contain PII
	//
	// Default: false (SQL text is never included in spans)
	IncludeQueryText bool
}

// WithQueryTracing returns a context with query tracing configuration.
// This allows callers to customize the span (e.g., set operation name, include SQL text).
// Spans are created for all queries by default; this just adds configuration.
func WithQueryTracing(ctx context.Context, config QueryTracingConfig) context.Context {
	return context.WithValue(ctx, queryTracingKey{}, config)
}

// getQueryTracingConfig returns the tracing config from context.
// Returns an empty config if none is set (spans are still created).
func getQueryTracingConfig(ctx context.Context) QueryTracingConfig {
	config, _ := ctx.Value(queryTracingKey{}).(QueryTracingConfig)
	return config
}

// defaultOperationName returns a safe default operation name.
// TODO: In the future, could support SELECT, UPDATE, INSERT, DELETE based on
// parsing the first keyword from a fixed allowlist.
func defaultOperationName() string {
	return "QUERY"
}

// Query executes a simple query and returns all results.
// For large result sets, consider using QueryStreaming instead.
func (c *Conn) Query(ctx context.Context, queryStr string) ([]*sqltypes.Result, error) {
	var results []*sqltypes.Result
	var currentResult *sqltypes.Result

	err := c.QueryStreaming(ctx, queryStr, func(ctx context.Context, result *sqltypes.Result) error {
		// Accumulate rows into the current result.
		if currentResult == nil {
			currentResult = result
		} else {
			currentResult.Rows = append(currentResult.Rows, result.Rows...)
		}

		// CommandTag being set signals the end of a result set.
		if result.CommandTag != "" {
			if currentResult == nil {
				currentResult = &sqltypes.Result{}
			}
			currentResult.CommandTag = result.CommandTag
			currentResult.RowsAffected = result.RowsAffected
			if currentResult.Fields == nil {
				currentResult.Fields = result.Fields
			}
			results = append(results, currentResult)
			currentResult = nil
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return results, nil
}

// QueryStreaming executes a simple query and streams results via callback.
// The callback is invoked in a streaming fashion with batched rows:
// - Rows are accumulated until DefaultStreamingBatchSize is exceeded, then flushed with Fields
// - On CommandComplete: remaining rows + CommandTag sent together (signals end of result set)
// For small result sets, this means a single callback with Fields, Rows, and CommandTag.
// For large result sets, multiple callbacks with rows, final one includes CommandTag.
// For multi-statement queries, this pattern repeats for each statement.
//
// A span is always created for query execution with database semantic conventions.
// Use WithQueryTracing to customize the span (operation name, include SQL text).
func (c *Conn) QueryStreaming(ctx context.Context, queryStr string, callback func(ctx context.Context, result *sqltypes.Result) error) error {
	// Create span for query execution
	config := getQueryTracingConfig(ctx)
	opName := config.OperationName
	if opName == "" {
		opName = defaultOperationName()
	}
	attrs := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.DBSystemPostgreSQL,
			semconv.DBOperationName(opName),
		),
	}
	if config.IncludeQueryText {
		attrs = append(attrs, trace.WithAttributes(semconv.DBQueryText(queryStr)))
	}
	ctx, span := telemetry.Tracer().Start(ctx, opName+" postgresql", attrs...)
	defer span.End()

	c.bufmu.Lock()
	defer c.bufmu.Unlock()

	// Send the Query message.
	if err := c.writeQueryMessage(queryStr); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to send query")
		return fmt.Errorf("failed to send query: %w", err)
	}

	// Process responses.
	err := c.processQueryResponses(ctx, callback)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "query failed")
	}
	return err
}

// writeQueryMessage writes a 'Q' (Query) message.
func (c *Conn) writeQueryMessage(queryStr string) error {
	w := NewMessageWriter()
	w.WriteString(queryStr)
	return c.writeMessage(protocol.MsgQuery, w.Bytes())
}

// processQueryResponses processes all responses to a query until ReadyForQuery.
// The callback is invoked in a streaming fashion with batched rows:
// - Rows are accumulated until DefaultStreamingBatchSize is exceeded, then flushed with Fields
// - On CommandComplete: remaining rows + CommandTag sent together (signals end of result set)
// For small result sets, this means a single callback with Fields, Rows, and CommandTag.
// For large result sets, multiple callbacks with rows, final one includes CommandTag.
//
// IMPORTANT: This function always reads until ReadyForQuery to keep the connection
// in a clean state. Callback errors are captured but do not stop message processing.
// Context cancellation should be handled by the caller (e.g., by killing the query
// on the server side) rather than here, to avoid leaving unread messages on the wire.
func (c *Conn) processQueryResponses(ctx context.Context, callback func(ctx context.Context, result *sqltypes.Result) error) error {
	// Track state for current result set.
	var currentFields []*query.Field
	var batchedRows []*sqltypes.Row
	var batchedSize int
	var notices []*sqltypes.PgDiagnostic

	// Track the first error encountered. We continue processing messages to drain
	// the connection, then return this error after ReadyForQuery.
	var firstErr error

	// flushBatch sends accumulated rows via callback and resets the batch.
	// Does not reset currentFields as they may be needed for subsequent batches.
	// Captures errors but does not return them - we continue draining.
	flushBatch := func() {
		if len(batchedRows) == 0 || callback == nil {
			return
		}
		result := &sqltypes.Result{
			Fields:  currentFields,
			Rows:    batchedRows,
			Notices: notices,
		}
		if firstErr == nil {
			firstErr = callback(ctx, result)
		}
		batchedRows = nil
		batchedSize = 0
		notices = nil
	}

	for {
		// Read message.
		msgType, body, err := c.readMessage()
		if err != nil {
			return fmt.Errorf("failed to read message: %w", err)
		}

		switch msgType {
		case protocol.MsgRowDescription:
			// Start of a new result set - parse and store fields.
			// Fields will be included in the first batch callback.
			currentFields, err = c.parseRowDescription(body)
			if err != nil {
				return err
			}

		case protocol.MsgDataRow:
			row, err := c.parseDataRow(body)
			if err != nil {
				return err
			}

			// Add row to batch and track size.
			batchedRows = append(batchedRows, row)
			batchedSize += len(body)

			// Flush batch if size threshold exceeded.
			if batchedSize >= DefaultStreamingBatchSize {
				flushBatch()
			}

		case protocol.MsgCommandComplete:
			tag, err := c.parseCommandComplete(body)
			if err != nil {
				return err
			}

			// Send final batch with CommandTag (signals end of result set).
			// This combines any remaining rows with the command completion.
			if callback != nil && firstErr == nil {
				result := &sqltypes.Result{
					Fields:       currentFields,
					Rows:         batchedRows,
					CommandTag:   tag,
					RowsAffected: parseRowsAffected(tag),
					Notices:      notices,
				}
				firstErr = callback(ctx, result)
			}

			// Reset for next result set.
			currentFields = nil
			batchedRows = nil
			batchedSize = 0
			notices = nil

		case protocol.MsgEmptyQueryResponse:
			// Empty query, call callback with empty result.
			if callback != nil && firstErr == nil {
				firstErr = callback(ctx, &sqltypes.Result{})
			}

		case protocol.MsgReadyForQuery:
			// Query complete. Return any error that was captured.
			c.txnStatus = body[0]
			return firstErr

		case protocol.MsgErrorResponse:
			// Capture the error but continue draining until ReadyForQuery.
			if firstErr == nil {
				firstErr = c.parseError(body)
			}

		case protocol.MsgNoticeResponse:
			// Parse and accumulate notices to be included in the Result.
			notices = append(notices, c.parseNotice(body))

		case protocol.MsgParameterStatus:
			// Handle parameter status updates. Capture error but continue draining.
			if firstErr == nil {
				firstErr = c.handleParameterStatus(body)
			}

		default:
			// Unexpected message type. Capture error but continue draining.
			if firstErr == nil {
				firstErr = fmt.Errorf("unexpected message type in query response: %c (0x%02x)", msgType, msgType)
			}
		}
	}
}

// parseRowDescription parses a RowDescription message.
//
// TODO: Migrate errors in parseRowDescription, parseDataRow, parseCommandComplete, and readMessage
// to use the mterrors package with proper error codes. These errors indicate connection-level failures
// (truncated/incomplete messages) and should be categorized so that isConnectionError() in
// regular_conn.go can detect them using error codes instead of string matching.
func (c *Conn) parseRowDescription(body []byte) ([]*query.Field, error) {
	reader := NewMessageReader(body)

	fieldCount, err := reader.ReadInt16()
	if err != nil {
		return nil, fmt.Errorf("failed to read field count: %w", err)
	}

	fields := make([]*query.Field, fieldCount)

	for i := range fieldCount {
		field := &query.Field{}

		field.Name, err = reader.ReadString()
		if err != nil {
			return nil, fmt.Errorf("failed to read field name: %w", err)
		}

		tableOID, err := reader.ReadUint32()
		if err != nil {
			return nil, fmt.Errorf("failed to read table OID: %w", err)
		}
		field.TableOid = tableOID

		attrNum, err := reader.ReadInt16()
		if err != nil {
			return nil, fmt.Errorf("failed to read attribute number: %w", err)
		}
		field.TableAttributeNumber = int32(attrNum)

		dataTypeOID, err := reader.ReadUint32()
		if err != nil {
			return nil, fmt.Errorf("failed to read data type OID: %w", err)
		}
		field.DataTypeOid = dataTypeOID
		field.Type = ast.Oid(dataTypeOID).String()

		dataTypeSize, err := reader.ReadInt16()
		if err != nil {
			return nil, fmt.Errorf("failed to read data type size: %w", err)
		}
		field.DataTypeSize = int32(dataTypeSize)

		typeMod, err := reader.ReadInt32()
		if err != nil {
			return nil, fmt.Errorf("failed to read type modifier: %w", err)
		}
		field.TypeModifier = typeMod

		formatCode, err := reader.ReadInt16()
		if err != nil {
			return nil, fmt.Errorf("failed to read format code: %w", err)
		}
		field.Format = int32(formatCode)

		fields[i] = field
	}

	return fields, nil
}

// parseDataRow parses a DataRow message.
// Returns sqltypes.Row where nil values represent NULL.
func (c *Conn) parseDataRow(body []byte) (*sqltypes.Row, error) {
	reader := NewMessageReader(body)

	columnCount, err := reader.ReadInt16()
	if err != nil {
		return nil, fmt.Errorf("failed to read column count: %w", err)
	}

	row := &sqltypes.Row{
		Values: make([]sqltypes.Value, columnCount),
	}

	for i := range columnCount {
		value, err := reader.ReadByteString()
		if err != nil {
			return nil, fmt.Errorf("failed to read column value: %w", err)
		}
		// nil for NULL, []byte{} for empty string - preserved correctly
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
// Returns 0 for SELECT statements since they don't "affect" rows (they only read).
func parseRowsAffected(tag string) uint64 {
	// Command tags have formats like:
	// - "SELECT 5" (5 rows returned, but not "affected" since SELECT is read-only)
	// - "INSERT 0 1" (1 row inserted)
	// - "UPDATE 10" (10 rows updated)
	// - "DELETE 3" (3 rows deleted)

	// SELECT doesn't affect rows, only reads them
	if len(tag) >= 6 && tag[:6] == "SELECT" {
		return 0
	}

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

// parseDiagnosticFields parses all 14 PostgreSQL diagnostic fields from the wire format.
// This is a shared helper used by both parseError() and parseNotice() since PostgreSQL
// uses the same field format for ErrorResponse ('E') and NoticeResponse ('N') messages.
// The msgType parameter should be protocol.MsgErrorResponse or protocol.MsgNoticeResponse.
//
// If the parsed diagnostic fails validation (missing required fields), a warning is logged
// but the diagnostic is still returned. This allows lenient handling of malformed messages.
func parseDiagnosticFields(msgType byte, body []byte) *sqltypes.PgDiagnostic {
	reader := NewMessageReader(body)

	diag := &sqltypes.PgDiagnostic{
		MessageType: msgType,
	}

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
			diag.Severity = value
		case protocol.FieldSeverityV:
			// FieldSeverityV ('V') is the non-localized severity.
			// Only use it if FieldSeverity ('S') wasn't already set.
			if diag.Severity == "" {
				diag.Severity = value
			}
		case protocol.FieldCode:
			diag.Code = value
		case protocol.FieldMessage:
			diag.Message = value
		case protocol.FieldDetail:
			diag.Detail = value
		case protocol.FieldHint:
			diag.Hint = value
		case protocol.FieldPosition:
			if pos, err := strconv.ParseInt(value, 10, 32); err == nil {
				diag.Position = int32(pos)
			}
		case protocol.FieldInternalPosition:
			if pos, err := strconv.ParseInt(value, 10, 32); err == nil {
				diag.InternalPosition = int32(pos)
			}
		case protocol.FieldInternalQuery:
			diag.InternalQuery = value
		case protocol.FieldWhere:
			diag.Where = value
		case protocol.FieldSchema:
			diag.Schema = value
		case protocol.FieldTable:
			diag.Table = value
		case protocol.FieldColumn:
			diag.Column = value
		case protocol.FieldDataType:
			diag.DataType = value
		case protocol.FieldConstraint:
			diag.Constraint = value
		}
	}

	// Validate the parsed diagnostic. Log a warning if validation fails,
	// but still return the diagnostic to allow lenient handling.
	if err := diag.Validate(); err != nil {
		slog.Warn("parsed PostgreSQL diagnostic with missing required fields",
			"error", err,
			// Convert single byte to string directly (msgType is 'E' or 'N')
			"message_type", string([]byte{msgType}),
			"severity", diag.Severity,
			"code", diag.Code,
		)
	}

	return diag
}

// parseError parses an ErrorResponse message into a *sqltypes.PgDiagnostic.
// Since sqltypes.PgDiagnostic implements the error interface, it can be returned
// directly as an error. This eliminates the need for a separate wrapper type.
// It captures all 14 PostgreSQL error fields defined in the protocol.
func (c *Conn) parseError(body []byte) error {
	return parseDiagnosticFields(protocol.MsgErrorResponse, body)
}

// parseNotice parses a NoticeResponse message into a sqltypes.PgDiagnostic.
func (c *Conn) parseNotice(body []byte) *sqltypes.PgDiagnostic {
	return parseDiagnosticFields(protocol.MsgNoticeResponse, body)
}
