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
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/query"
)

// Parse sends a Parse message to prepare a statement.
// name is the statement name (empty for unnamed statement).
// queryStr is the SQL query.
// paramTypes are the OIDs of parameter types (0 for unspecified).
func (c *Conn) Parse(ctx context.Context, name, queryStr string, paramTypes []uint32) error {
	c.bufmu.Lock()
	defer c.bufmu.Unlock()

	if err := c.writeParse(name, queryStr, paramTypes); err != nil {
		return fmt.Errorf("failed to write Parse: %w", err)
	}

	// Send Sync to get a response.
	if err := c.writeSync(); err != nil {
		return fmt.Errorf("failed to write Sync: %w", err)
	}

	if err := c.flush(); err != nil {
		return fmt.Errorf("failed to flush: %w", err)
	}

	// Wait for ParseComplete and ReadyForQuery.
	return c.waitForParseComplete(ctx)
}

// BindAndExecute binds parameters to a prepared statement and executes it atomically.
// This sends Bind → Execute → Sync in a single operation, ensuring the portal
// is not cleared before execution (Sync closes the implicit transaction which clears portals).
// stmtName is the prepared statement name - the portal will use the same name.
// params are the parameter values.
// paramFormats are format codes for parameters (0=text, 1=binary).
// resultFormats are format codes for result columns (0=text, 1=binary).
// maxRows is the maximum number of rows to return (0 for unlimited).
// Returns true if the execution completed (CommandComplete), false if suspended (PortalSuspended).
func (c *Conn) BindAndExecute(ctx context.Context, stmtName string, params [][]byte, paramFormats, resultFormats []int16, maxRows int32, callback func(ctx context.Context, result *sqltypes.Result) error) (completed bool, err error) {
	c.bufmu.Lock()
	defer c.bufmu.Unlock()

	// Use the same name for portal as the statement for consistency.
	if err := c.writeBind(stmtName, stmtName, params, paramFormats, resultFormats); err != nil {
		return false, fmt.Errorf("failed to write Bind: %w", err)
	}

	if err := c.writeExecute(stmtName, maxRows); err != nil {
		return false, fmt.Errorf("failed to write Execute: %w", err)
	}

	if err := c.writeSync(); err != nil {
		return false, fmt.Errorf("failed to write Sync: %w", err)
	}

	if err := c.flush(); err != nil {
		return false, fmt.Errorf("failed to flush: %w", err)
	}

	// Process Bind and Execute responses.
	return c.processBindAndExecuteResponses(ctx, callback)
}

// BindAndDescribe binds parameters to a prepared statement and describes the resulting portal.
// This sends Bind → Describe('P') → Sync in a single operation.
// stmtName is the prepared statement name - the portal will use the same name.
// params are the parameter values.
// paramFormats are format codes for parameters (0=text, 1=binary).
// resultFormats are format codes for result columns (0=text, 1=binary).
func (c *Conn) BindAndDescribe(ctx context.Context, stmtName string, params [][]byte, paramFormats, resultFormats []int16) (*query.StatementDescription, error) {
	c.bufmu.Lock()
	defer c.bufmu.Unlock()

	// Use the same name for portal as the statement for consistency.
	if err := c.writeBind(stmtName, stmtName, params, paramFormats, resultFormats); err != nil {
		return nil, fmt.Errorf("failed to write Bind: %w", err)
	}

	if err := c.writeDescribe('P', stmtName); err != nil {
		return nil, fmt.Errorf("failed to write Describe: %w", err)
	}

	if err := c.writeSync(); err != nil {
		return nil, fmt.Errorf("failed to write Sync: %w", err)
	}

	if err := c.flush(); err != nil {
		return nil, fmt.Errorf("failed to flush: %w", err)
	}

	// Process Bind and Describe responses.
	return c.processBindAndDescribeResponses(ctx)
}

// DescribePrepared describes a prepared statement.
// This sends Describe('S') → Sync.
// name is the prepared statement name (empty for unnamed statement).
func (c *Conn) DescribePrepared(ctx context.Context, name string) (*query.StatementDescription, error) {
	c.bufmu.Lock()
	defer c.bufmu.Unlock()

	if err := c.writeDescribe('S', name); err != nil {
		return nil, fmt.Errorf("failed to write Describe: %w", err)
	}

	if err := c.writeSync(); err != nil {
		return nil, fmt.Errorf("failed to write Sync: %w", err)
	}

	if err := c.flush(); err != nil {
		return nil, fmt.Errorf("failed to flush: %w", err)
	}

	// Process describe responses.
	return c.processDescribeResponses(ctx)
}

// CloseStatement sends a Close message to close a prepared statement.
func (c *Conn) CloseStatement(ctx context.Context, name string) error {
	return c.closeTarget(ctx, 'S', name)
}

// ClosePortal sends a Close message to close a portal.
func (c *Conn) ClosePortal(ctx context.Context, name string) error {
	return c.closeTarget(ctx, 'P', name)
}

// closeTarget sends a Close message for a statement or portal.
func (c *Conn) closeTarget(ctx context.Context, typ byte, name string) error {
	c.bufmu.Lock()
	defer c.bufmu.Unlock()

	if err := c.writeClose(typ, name); err != nil {
		return fmt.Errorf("failed to write Close: %w", err)
	}

	// Send Sync to get a response.
	if err := c.writeSync(); err != nil {
		return fmt.Errorf("failed to write Sync: %w", err)
	}

	if err := c.flush(); err != nil {
		return fmt.Errorf("failed to flush: %w", err)
	}

	// Wait for CloseComplete and ReadyForQuery.
	return c.waitForCloseComplete(ctx)
}

// Sync sends a Sync message to synchronize the extended query protocol.
func (c *Conn) Sync(ctx context.Context) error {
	c.bufmu.Lock()
	defer c.bufmu.Unlock()

	if err := c.writeSync(); err != nil {
		return fmt.Errorf("failed to write Sync: %w", err)
	}

	if err := c.flush(); err != nil {
		return fmt.Errorf("failed to flush: %w", err)
	}

	// Wait for ReadyForQuery.
	return c.waitForReadyForQuery(ctx)
}

// Flush sends a Flush message to request the server to flush its output buffer.
func (c *Conn) Flush(ctx context.Context) error {
	c.bufmu.Lock()
	defer c.bufmu.Unlock()

	if err := c.writeFlush(); err != nil {
		return fmt.Errorf("failed to write Flush: %w", err)
	}

	return c.flush()
}

// PrepareAndExecute is a convenience method that prepares and executes a statement.
// This performs Parse, Bind, Execute, and Sync in a single round trip.
// name is the statement/portal name (use "" for unnamed, which is cleared after Sync).
// A named statement persists until explicitly closed or the session ends.
func (c *Conn) PrepareAndExecute(ctx context.Context, name, queryStr string, params [][]byte, callback func(ctx context.Context, result *sqltypes.Result) error) error {
	c.bufmu.Lock()
	defer c.bufmu.Unlock()

	// Write all messages without flushing.
	if err := c.writeParse(name, queryStr, nil); err != nil {
		return fmt.Errorf("failed to write Parse: %w", err)
	}

	// Use text format for all parameters and results.
	// Use the same name for portal as the statement for consistency.
	if err := c.writeBind(name, name, params, nil, nil); err != nil {
		return fmt.Errorf("failed to write Bind: %w", err)
	}

	if err := c.writeExecute(name, 0); err != nil {
		return fmt.Errorf("failed to write Execute: %w", err)
	}

	if err := c.writeSync(); err != nil {
		return fmt.Errorf("failed to write Sync: %w", err)
	}

	if err := c.flush(); err != nil {
		return fmt.Errorf("failed to flush: %w", err)
	}

	// Process all responses.
	return c.processPrepareAndExecuteResponses(ctx, callback)
}

// QueryArgs executes a parameterized query using the extended query protocol.
// This is a convenience method that accepts Go values as arguments and converts
// them to the appropriate text format for PostgreSQL.
// Supported argument types: nil, string, []byte, int, int32, int64, uint32, uint64,
// float32, float64, bool, and time.Time.
func (c *Conn) QueryArgs(ctx context.Context, queryStr string, args ...any) ([]*sqltypes.Result, error) {
	// Convert args to [][]byte
	params, err := argsToParams(args)
	if err != nil {
		return nil, fmt.Errorf("failed to convert args: %w", err)
	}

	var results []*sqltypes.Result
	var currentResult *sqltypes.Result

	// Use unnamed statement (empty name) for one-shot queries.
	err = c.PrepareAndExecute(ctx, "", queryStr, params, func(ctx context.Context, result *sqltypes.Result) error {
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

// Execute continues execution of a previously bound portal.
// This is used to fetch more rows from a portal that was executed with maxRows > 0
// and returned PortalSuspended.
// portalName is the name of the portal to execute (empty for unnamed portal).
// maxRows is the maximum number of rows to return (0 for unlimited).
// Returns true if the portal completed (CommandComplete), false if suspended (PortalSuspended).
func (c *Conn) Execute(ctx context.Context, portalName string, maxRows int32, callback func(ctx context.Context, result *sqltypes.Result) error) (completed bool, err error) {
	c.bufmu.Lock()
	defer c.bufmu.Unlock()

	if err := c.writeExecute(portalName, maxRows); err != nil {
		return false, fmt.Errorf("failed to write Execute: %w", err)
	}

	if err := c.writeSync(); err != nil {
		return false, fmt.Errorf("failed to write Sync: %w", err)
	}

	if err := c.flush(); err != nil {
		return false, fmt.Errorf("failed to flush: %w", err)
	}

	// Process execute responses.
	return c.processExecuteResponses(ctx, callback)
}

// argsToParams converts Go values to PostgreSQL text format parameters.
func argsToParams(args []any) ([][]byte, error) {
	params := make([][]byte, len(args))
	for i, arg := range args {
		param, err := argToParam(arg)
		if err != nil {
			return nil, fmt.Errorf("arg %d: %w", i, err)
		}
		params[i] = param
	}
	return params, nil
}

// argToParam converts a single Go value to PostgreSQL text format.
func argToParam(arg any) ([]byte, error) {
	if arg == nil {
		return nil, nil // NULL is represented as nil
	}

	switch v := arg.(type) {
	case string:
		return []byte(v), nil
	case []byte:
		return v, nil
	case int:
		return []byte(strconv.FormatInt(int64(v), 10)), nil
	case int32:
		return []byte(strconv.FormatInt(int64(v), 10)), nil
	case int64:
		return []byte(strconv.FormatInt(v, 10)), nil
	case uint32:
		return []byte(strconv.FormatUint(uint64(v), 10)), nil
	case uint64:
		return []byte(strconv.FormatUint(v, 10)), nil
	case float32:
		return []byte(strconv.FormatFloat(float64(v), 'f', -1, 32)), nil
	case float64:
		return []byte(strconv.FormatFloat(v, 'f', -1, 64)), nil
	case bool:
		if v {
			return []byte("true"), nil
		}
		return []byte("false"), nil
	case time.Time:
		// Use RFC3339 format which PostgreSQL understands.
		return []byte(v.Format(time.RFC3339Nano)), nil
	default:
		return nil, fmt.Errorf("unsupported type: %T", arg)
	}
}

// processExecuteResponses processes responses to an Execute command.
// Returns true if the execution completed (CommandComplete), false if suspended (PortalSuspended).
//
// IMPORTANT: This function always reads until ReadyForQuery to keep the connection
// in a clean state. Errors are captured but do not stop message processing.
func (c *Conn) processExecuteResponses(ctx context.Context, callback func(ctx context.Context, result *sqltypes.Result) error) (completed bool, err error) {
	var currentFields []*query.Field
	var batchedRows []*sqltypes.Row
	var batchedSize int
	var notices []*sqltypes.PgDiagnostic
	var firstErr error

	// flushBatch sends accumulated rows via callback and resets the batch.
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
		msgType, body, err := c.readMessage()
		if err != nil {
			return false, fmt.Errorf("failed to read message: %w", err)
		}

		switch msgType {
		case protocol.MsgRowDescription:
			// Start of a new result set - parse and store fields.
			fields, err := c.parseRowDescription(body)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
			} else {
				currentFields = fields
			}

		case protocol.MsgDataRow:
			row, err := c.parseDataRow(body)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
			} else {
				batchedRows = append(batchedRows, row)
				batchedSize += len(body)

				if batchedSize >= DefaultStreamingBatchSize {
					flushBatch()
				}
			}

		case protocol.MsgCommandComplete:
			tag, err := c.parseCommandComplete(body)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
			} else if callback != nil && firstErr == nil {
				// Send final batch with CommandTag.
				result := &sqltypes.Result{
					Fields:       currentFields,
					Rows:         batchedRows,
					CommandTag:   tag,
					RowsAffected: parseRowsAffected(tag),
					Notices:      notices,
				}
				firstErr = callback(ctx, result)
			}
			// Don't return yet - wait for ReadyForQuery.
			completed = true
			currentFields = nil
			batchedRows = nil
			batchedSize = 0
			notices = nil

		case protocol.MsgEmptyQueryResponse:
			if callback != nil && firstErr == nil {
				firstErr = callback(ctx, &sqltypes.Result{})
			}
			completed = true

		case protocol.MsgPortalSuspended:
			// Portal execution was suspended (partial results).
			flushBatch()
			// Don't return yet - wait for ReadyForQuery.
			completed = false

		case protocol.MsgReadyForQuery:
			c.txnStatus = body[0]
			return completed, firstErr

		case protocol.MsgErrorResponse:
			if firstErr == nil {
				firstErr = c.parseError(body)
			}

		case protocol.MsgNoticeResponse:
			// Parse and accumulate notices to be included in the Result.
			notices = append(notices, c.parseNotice(body))

		case protocol.MsgParameterStatus:
			if firstErr == nil {
				firstErr = c.handleParameterStatus(body)
			}

		default:
			if firstErr == nil {
				firstErr = fmt.Errorf("unexpected message type: %c (0x%02x)", msgType, msgType)
			}
		}
	}
}

// Write methods for extended protocol messages.

// writeParse writes a Parse message.
func (c *Conn) writeParse(name, queryStr string, paramTypes []uint32) error {
	w := NewMessageWriter()
	w.WriteString(name)
	w.WriteString(queryStr)
	w.WriteInt16(int16(len(paramTypes)))
	for _, oid := range paramTypes {
		w.WriteUint32(oid)
	}
	return c.writeMessageNoFlush(protocol.MsgParse, w.Bytes())
}

// writeBind writes a Bind message.
func (c *Conn) writeBind(portalName, stmtName string, params [][]byte, paramFormats, resultFormats []int16) error {
	w := NewMessageWriter()
	w.WriteString(portalName)
	w.WriteString(stmtName)

	// Parameter format codes.
	w.WriteInt16(int16(len(paramFormats)))
	for _, f := range paramFormats {
		w.WriteInt16(f)
	}

	// Parameter values.
	w.WriteInt16(int16(len(params)))
	for _, p := range params {
		w.WriteByteString(p)
	}

	// Result format codes.
	w.WriteInt16(int16(len(resultFormats)))
	for _, f := range resultFormats {
		w.WriteInt16(f)
	}

	return c.writeMessageNoFlush(protocol.MsgBind, w.Bytes())
}

// writeExecute writes an Execute message.
func (c *Conn) writeExecute(portalName string, maxRows int32) error {
	w := NewMessageWriter()
	w.WriteString(portalName)
	w.WriteInt32(maxRows)
	return c.writeMessageNoFlush(protocol.MsgExecute, w.Bytes())
}

// writeDescribe writes a Describe message.
func (c *Conn) writeDescribe(typ byte, name string) error {
	w := NewMessageWriter()
	w.WriteByte(typ)
	w.WriteString(name)
	return c.writeMessageNoFlush(protocol.MsgDescribe, w.Bytes())
}

// writeClose writes a Close message.
func (c *Conn) writeClose(typ byte, name string) error {
	w := NewMessageWriter()
	w.WriteByte(typ)
	w.WriteString(name)
	return c.writeMessageNoFlush(protocol.MsgClose, w.Bytes())
}

// writeSync writes a Sync message.
func (c *Conn) writeSync() error {
	return c.writeMessageNoFlush(protocol.MsgSync, nil)
}

// writeFlush writes a Flush message.
func (c *Conn) writeFlush() error {
	return c.writeMessageNoFlush(protocol.MsgFlush, nil)
}

// Response processing methods.

// waitForParseComplete waits for ParseComplete and ReadyForQuery.
// Always reads until ReadyForQuery to keep the connection in a clean state.
func (c *Conn) waitForParseComplete(_ context.Context) error {
	gotParseComplete := false
	var firstErr error

	for {
		msgType, body, err := c.readMessage()
		if err != nil {
			return fmt.Errorf("failed to read message: %w", err)
		}

		switch msgType {
		case protocol.MsgParseComplete:
			gotParseComplete = true

		case protocol.MsgReadyForQuery:
			c.txnStatus = body[0]
			if firstErr != nil {
				return firstErr
			}
			if !gotParseComplete {
				return errors.New("did not receive ParseComplete")
			}
			return nil

		case protocol.MsgErrorResponse:
			if firstErr == nil {
				firstErr = c.parseError(body)
			}

		case protocol.MsgNoticeResponse:
			// Parse notice (no result to attach to in this context).
			_ = c.parseNotice(body)

		case protocol.MsgParameterStatus:
			if firstErr == nil {
				firstErr = c.handleParameterStatus(body)
			}

		default:
			if firstErr == nil {
				firstErr = fmt.Errorf("unexpected message type: %c (0x%02x)", msgType, msgType)
			}
		}
	}
}

// waitForCloseComplete waits for CloseComplete and ReadyForQuery.
// Always reads until ReadyForQuery to keep the connection in a clean state.
func (c *Conn) waitForCloseComplete(_ context.Context) error {
	gotCloseComplete := false
	var firstErr error

	for {
		msgType, body, err := c.readMessage()
		if err != nil {
			return fmt.Errorf("failed to read message: %w", err)
		}

		switch msgType {
		case protocol.MsgCloseComplete:
			gotCloseComplete = true

		case protocol.MsgReadyForQuery:
			c.txnStatus = body[0]
			if firstErr != nil {
				return firstErr
			}
			if !gotCloseComplete {
				return errors.New("did not receive CloseComplete")
			}
			return nil

		case protocol.MsgErrorResponse:
			if firstErr == nil {
				firstErr = c.parseError(body)
			}

		case protocol.MsgNoticeResponse:
			// Parse notice (no result to attach to in this context).
			_ = c.parseNotice(body)

		case protocol.MsgParameterStatus:
			if firstErr == nil {
				firstErr = c.handleParameterStatus(body)
			}

		default:
			if firstErr == nil {
				firstErr = fmt.Errorf("unexpected message type: %c (0x%02x)", msgType, msgType)
			}
		}
	}
}

// waitForReadyForQuery waits for ReadyForQuery.
// Always reads until ReadyForQuery to keep the connection in a clean state.
func (c *Conn) waitForReadyForQuery(_ context.Context) error {
	var firstErr error

	for {
		msgType, body, err := c.readMessage()
		if err != nil {
			return fmt.Errorf("failed to read message: %w", err)
		}

		switch msgType {
		case protocol.MsgReadyForQuery:
			c.txnStatus = body[0]
			return firstErr

		case protocol.MsgErrorResponse:
			if firstErr == nil {
				firstErr = c.parseError(body)
			}

		case protocol.MsgNoticeResponse:
			// Parse notice (no result to attach to in this context).
			_ = c.parseNotice(body)

		case protocol.MsgParameterStatus:
			if firstErr == nil {
				firstErr = c.handleParameterStatus(body)
			}

		default:
			if firstErr == nil {
				firstErr = fmt.Errorf("unexpected message type: %c (0x%02x)", msgType, msgType)
			}
		}
	}
}

// processDescribeResponses processes responses to a Describe('S') command.
// This only expects ParameterDescription and RowDescription (no BindComplete).
// Always reads until ReadyForQuery to keep the connection in a clean state.
func (c *Conn) processDescribeResponses(_ context.Context) (*query.StatementDescription, error) {
	desc := &query.StatementDescription{}
	var firstErr error

	for {
		msgType, body, err := c.readMessage()
		if err != nil {
			return nil, fmt.Errorf("failed to read message: %w", err)
		}

		switch msgType {
		case protocol.MsgParameterDescription:
			params, err := c.parseParameterDescription(body)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
			} else {
				desc.Parameters = params
			}

		case protocol.MsgRowDescription:
			fields, err := c.parseRowDescription(body)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
			} else {
				desc.Fields = fields
			}

		case protocol.MsgNoData:
			// No data to return (e.g., for non-SELECT statements).

		case protocol.MsgReadyForQuery:
			c.txnStatus = body[0]
			if firstErr != nil {
				return nil, firstErr
			}
			return desc, nil

		case protocol.MsgErrorResponse:
			if firstErr == nil {
				firstErr = c.parseError(body)
			}

		case protocol.MsgNoticeResponse:
			// Parse notice (no result to attach to in this context).
			_ = c.parseNotice(body)

		case protocol.MsgParameterStatus:
			if firstErr == nil {
				firstErr = c.handleParameterStatus(body)
			}

		default:
			if firstErr == nil {
				firstErr = fmt.Errorf("unexpected message type: %c (0x%02x)", msgType, msgType)
			}
		}
	}
}

// processBindAndExecuteResponses processes responses to BindAndExecute.
// Expects: BindComplete, then execute results (RowDescription, DataRow, CommandComplete), then ReadyForQuery.
// The callback is invoked in a streaming fashion with batched rows:
// - Rows are accumulated until DefaultStreamingBatchSize is exceeded, then flushed with Fields
// - On CommandComplete: remaining rows + CommandTag sent together (signals end of result set)
// For small result sets, this means a single callback with Fields, Rows, and CommandTag.
// Returns true if the execution completed (CommandComplete), false if suspended (PortalSuspended).
// Always reads until ReadyForQuery to keep the connection in a clean state.
func (c *Conn) processBindAndExecuteResponses(ctx context.Context, callback func(ctx context.Context, result *sqltypes.Result) error) (completed bool, err error) {
	gotBindComplete := false
	var currentFields []*query.Field
	var batchedRows []*sqltypes.Row
	var batchedSize int
	var notices []*sqltypes.PgDiagnostic
	var firstErr error

	// flushBatch sends accumulated rows via callback and resets the batch.
	// Does not reset currentFields as they may be needed for subsequent batches.
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
		msgType, body, err := c.readMessage()
		if err != nil {
			return false, fmt.Errorf("failed to read message: %w", err)
		}

		switch msgType {
		case protocol.MsgBindComplete:
			gotBindComplete = true

		case protocol.MsgRowDescription:
			// Start of a new result set - parse and store fields.
			// Fields will be included in the first batch callback.
			fields, err := c.parseRowDescription(body)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
			} else {
				currentFields = fields
			}

		case protocol.MsgDataRow:
			row, err := c.parseDataRow(body)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
			} else {
				// Add row to batch and track size.
				batchedRows = append(batchedRows, row)
				batchedSize += len(body)

				// Flush batch if size threshold exceeded.
				if batchedSize >= DefaultStreamingBatchSize {
					flushBatch()
				}
			}

		case protocol.MsgCommandComplete:
			tag, err := c.parseCommandComplete(body)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
			} else if callback != nil && firstErr == nil {
				// Send final batch with CommandTag (signals end of result set).
				// This combines any remaining rows with the command completion.
				result := &sqltypes.Result{
					Fields:       currentFields,
					Rows:         batchedRows,
					CommandTag:   tag,
					RowsAffected: parseRowsAffected(tag),
					Notices:      notices,
				}
				firstErr = callback(ctx, result)
			}

			// Don't return yet - wait for ReadyForQuery.
			completed = true
			currentFields = nil
			batchedRows = nil
			batchedSize = 0
			notices = nil

		case protocol.MsgEmptyQueryResponse:
			if callback != nil && firstErr == nil {
				firstErr = callback(ctx, &sqltypes.Result{})
			}
			completed = true

		case protocol.MsgPortalSuspended:
			// Portal execution was suspended (partial results).
			// Flush any batched rows.
			flushBatch()
			// Don't return yet - wait for ReadyForQuery.
			completed = false

		case protocol.MsgReadyForQuery:
			c.txnStatus = body[0]
			if firstErr != nil {
				return false, firstErr
			}
			if !gotBindComplete {
				return false, errors.New("did not receive BindComplete")
			}
			return completed, nil

		case protocol.MsgErrorResponse:
			if firstErr == nil {
				firstErr = c.parseError(body)
			}

		case protocol.MsgNoticeResponse:
			// Parse and accumulate notices to be included in the Result.
			notices = append(notices, c.parseNotice(body))

		case protocol.MsgParameterStatus:
			if firstErr == nil {
				firstErr = c.handleParameterStatus(body)
			}

		default:
			if firstErr == nil {
				firstErr = fmt.Errorf("unexpected message type: %c (0x%02x)", msgType, msgType)
			}
		}
	}
}

// processBindAndDescribeResponses processes responses to BindAndDescribe.
// Expects: BindComplete, then describe results (RowDescription or NoData), then ReadyForQuery.
// Note: Describe('P') for a portal does NOT return ParameterDescription, only RowDescription/NoData.
// Always reads until ReadyForQuery to keep the connection in a clean state.
func (c *Conn) processBindAndDescribeResponses(_ context.Context) (*query.StatementDescription, error) {
	gotBindComplete := false
	desc := &query.StatementDescription{}
	var firstErr error

	for {
		msgType, body, err := c.readMessage()
		if err != nil {
			return nil, fmt.Errorf("failed to read message: %w", err)
		}

		switch msgType {
		case protocol.MsgBindComplete:
			gotBindComplete = true

		case protocol.MsgRowDescription:
			fields, err := c.parseRowDescription(body)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
			} else {
				desc.Fields = fields
			}

		case protocol.MsgNoData:
			// No data to return (e.g., for non-SELECT statements).

		case protocol.MsgReadyForQuery:
			c.txnStatus = body[0]
			if firstErr != nil {
				return nil, firstErr
			}
			if !gotBindComplete {
				return nil, errors.New("did not receive BindComplete")
			}
			return desc, nil

		case protocol.MsgErrorResponse:
			if firstErr == nil {
				firstErr = c.parseError(body)
			}

		case protocol.MsgNoticeResponse:
			// Parse notice (no result to attach to in this context).
			_ = c.parseNotice(body)

		case protocol.MsgParameterStatus:
			if firstErr == nil {
				firstErr = c.handleParameterStatus(body)
			}

		default:
			if firstErr == nil {
				firstErr = fmt.Errorf("unexpected message type: %c (0x%02x)", msgType, msgType)
			}
		}
	}
}

// parseParameterDescription parses a ParameterDescription message.
func (c *Conn) parseParameterDescription(body []byte) ([]*query.ParameterDescription, error) {
	reader := NewMessageReader(body)

	paramCount, err := reader.ReadInt16()
	if err != nil {
		return nil, fmt.Errorf("failed to read parameter count: %w", err)
	}

	params := make([]*query.ParameterDescription, paramCount)
	for i := range paramCount {
		oid, err := reader.ReadUint32()
		if err != nil {
			return nil, fmt.Errorf("failed to read parameter OID: %w", err)
		}
		params[i] = &query.ParameterDescription{
			DataTypeOid: oid,
		}
	}

	return params, nil
}

// processPrepareAndExecuteResponses processes responses for PrepareAndExecute.
// The callback is invoked in a streaming fashion with batched rows:
// - Rows are accumulated until DefaultStreamingBatchSize is exceeded, then flushed with Fields
// - On CommandComplete: remaining rows + CommandTag sent together (signals end of result set)
// For small result sets, this means a single callback with Fields, Rows, and CommandTag.
// Always reads until ReadyForQuery to keep the connection in a clean state.
func (c *Conn) processPrepareAndExecuteResponses(ctx context.Context, callback func(ctx context.Context, result *sqltypes.Result) error) error {
	gotParseComplete := false
	gotBindComplete := false
	var currentFields []*query.Field
	var batchedRows []*sqltypes.Row
	var batchedSize int
	var notices []*sqltypes.PgDiagnostic
	var firstErr error

	// flushBatch sends accumulated rows via callback and resets the batch.
	// Does not reset currentFields as they may be needed for subsequent batches.
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
		msgType, body, err := c.readMessage()
		if err != nil {
			return fmt.Errorf("failed to read message: %w", err)
		}

		switch msgType {
		case protocol.MsgParseComplete:
			gotParseComplete = true

		case protocol.MsgBindComplete:
			gotBindComplete = true

		case protocol.MsgRowDescription:
			// Start of a new result set - parse and store fields.
			// Fields will be included in the first batch callback.
			fields, err := c.parseRowDescription(body)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
			} else {
				currentFields = fields
			}

		case protocol.MsgDataRow:
			row, err := c.parseDataRow(body)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
			} else {
				// Add row to batch and track size.
				batchedRows = append(batchedRows, row)
				batchedSize += len(body)

				// Flush batch if size threshold exceeded.
				if batchedSize >= DefaultStreamingBatchSize {
					flushBatch()
				}
			}

		case protocol.MsgCommandComplete:
			tag, err := c.parseCommandComplete(body)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
			} else if callback != nil && firstErr == nil {
				// Send final batch with CommandTag (signals end of result set).
				// This combines any remaining rows with the command completion.
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
			if callback != nil && firstErr == nil {
				firstErr = callback(ctx, &sqltypes.Result{})
			}

		case protocol.MsgReadyForQuery:
			c.txnStatus = body[0]
			if firstErr != nil {
				return firstErr
			}
			if !gotParseComplete {
				return errors.New("did not receive ParseComplete")
			}
			if !gotBindComplete {
				return errors.New("did not receive BindComplete")
			}
			return nil

		case protocol.MsgErrorResponse:
			if firstErr == nil {
				firstErr = c.parseError(body)
			}

		case protocol.MsgNoticeResponse:
			// Parse and accumulate notices to be included in the Result.
			notices = append(notices, c.parseNotice(body))

		case protocol.MsgParameterStatus:
			if firstErr == nil {
				firstErr = c.handleParameterStatus(body)
			}

		default:
			if firstErr == nil {
				firstErr = fmt.Errorf("unexpected message type: %c (0x%02x)", msgType, msgType)
			}
		}
	}
}
