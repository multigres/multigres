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
// stmtName is the prepared statement name (empty for unnamed statement).
// params are the parameter values.
// paramFormats are format codes for parameters (0=text, 1=binary).
// resultFormats are format codes for result columns (0=text, 1=binary).
// maxRows is the maximum number of rows to return (0 for unlimited).
func (c *Conn) BindAndExecute(ctx context.Context, stmtName string, params [][]byte, paramFormats, resultFormats []int16, maxRows int32, callback func(ctx context.Context, result *query.QueryResult) error) error {
	c.bufmu.Lock()
	defer c.bufmu.Unlock()

	// Use unnamed portal since it will be consumed immediately.
	if err := c.writeBind("", stmtName, params, paramFormats, resultFormats); err != nil {
		return fmt.Errorf("failed to write Bind: %w", err)
	}

	if err := c.writeExecute("", maxRows); err != nil {
		return fmt.Errorf("failed to write Execute: %w", err)
	}

	if err := c.writeSync(); err != nil {
		return fmt.Errorf("failed to write Sync: %w", err)
	}

	if err := c.flush(); err != nil {
		return fmt.Errorf("failed to flush: %w", err)
	}

	// Process Bind and Execute responses.
	return c.processBindAndExecuteResponses(ctx, callback)
}

// BindAndDescribe binds parameters to a prepared statement and describes the resulting portal.
// This sends Bind → Describe('P') → Sync in a single operation.
// stmtName is the prepared statement name (empty for unnamed statement).
// params are the parameter values.
// paramFormats are format codes for parameters (0=text, 1=binary).
// resultFormats are format codes for result columns (0=text, 1=binary).
func (c *Conn) BindAndDescribe(ctx context.Context, stmtName string, params [][]byte, paramFormats, resultFormats []int16) (*query.StatementDescription, error) {
	c.bufmu.Lock()
	defer c.bufmu.Unlock()

	// Use unnamed portal since it will be described and then cleared by Sync.
	if err := c.writeBind("", stmtName, params, paramFormats, resultFormats); err != nil {
		return nil, fmt.Errorf("failed to write Bind: %w", err)
	}

	if err := c.writeDescribe('P', ""); err != nil {
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
func (c *Conn) PrepareAndExecute(ctx context.Context, queryStr string, params [][]byte, callback func(ctx context.Context, result *query.QueryResult) error) error {
	c.bufmu.Lock()
	defer c.bufmu.Unlock()

	// Write all messages without flushing.
	if err := c.writeParse("", queryStr, nil); err != nil {
		return fmt.Errorf("failed to write Parse: %w", err)
	}

	// Use text format for all parameters and results.
	if err := c.writeBind("", "", params, nil, nil); err != nil {
		return fmt.Errorf("failed to write Bind: %w", err)
	}

	if err := c.writeExecute("", 0); err != nil {
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
func (c *Conn) waitForParseComplete(ctx context.Context) error {
	gotParseComplete := false

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msgType, body, err := c.readMessage()
		if err != nil {
			return fmt.Errorf("failed to read message: %w", err)
		}

		switch msgType {
		case protocol.MsgParseComplete:
			gotParseComplete = true

		case protocol.MsgReadyForQuery:
			c.txnStatus = body[0]
			if !gotParseComplete {
				return fmt.Errorf("did not receive ParseComplete")
			}
			return nil

		case protocol.MsgErrorResponse:
			return c.handleErrorAndWaitForReady(body)

		case protocol.MsgNoticeResponse:
			// Ignore notices.

		case protocol.MsgParameterStatus:
			if err := c.handleParameterStatus(body); err != nil {
				return err
			}

		default:
			return fmt.Errorf("unexpected message type: %c (0x%02x)", msgType, msgType)
		}
	}
}

// waitForCloseComplete waits for CloseComplete and ReadyForQuery.
func (c *Conn) waitForCloseComplete(ctx context.Context) error {
	gotCloseComplete := false

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msgType, body, err := c.readMessage()
		if err != nil {
			return fmt.Errorf("failed to read message: %w", err)
		}

		switch msgType {
		case protocol.MsgCloseComplete:
			gotCloseComplete = true

		case protocol.MsgReadyForQuery:
			c.txnStatus = body[0]
			if !gotCloseComplete {
				return fmt.Errorf("did not receive CloseComplete")
			}
			return nil

		case protocol.MsgErrorResponse:
			return c.handleErrorAndWaitForReady(body)

		case protocol.MsgNoticeResponse:
			// Ignore notices.

		case protocol.MsgParameterStatus:
			if err := c.handleParameterStatus(body); err != nil {
				return err
			}

		default:
			return fmt.Errorf("unexpected message type: %c (0x%02x)", msgType, msgType)
		}
	}
}

// waitForReadyForQuery waits for ReadyForQuery.
func (c *Conn) waitForReadyForQuery(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msgType, body, err := c.readMessage()
		if err != nil {
			return fmt.Errorf("failed to read message: %w", err)
		}

		switch msgType {
		case protocol.MsgReadyForQuery:
			c.txnStatus = body[0]
			return nil

		case protocol.MsgErrorResponse:
			return c.handleErrorAndWaitForReady(body)

		case protocol.MsgNoticeResponse:
			// Ignore notices.

		case protocol.MsgParameterStatus:
			if err := c.handleParameterStatus(body); err != nil {
				return err
			}

		default:
			return fmt.Errorf("unexpected message type: %c (0x%02x)", msgType, msgType)
		}
	}
}

// processDescribeResponses processes responses to a Describe('S') command.
// This only expects ParameterDescription and RowDescription (no BindComplete).
func (c *Conn) processDescribeResponses(ctx context.Context) (*query.StatementDescription, error) {
	desc := &query.StatementDescription{}

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		msgType, body, err := c.readMessage()
		if err != nil {
			return nil, fmt.Errorf("failed to read message: %w", err)
		}

		switch msgType {
		case protocol.MsgParameterDescription:
			params, err := c.parseParameterDescription(body)
			if err != nil {
				return nil, err
			}
			desc.Parameters = params

		case protocol.MsgRowDescription:
			result := &query.QueryResult{}
			if err := c.parseRowDescription(body, result); err != nil {
				return nil, err
			}
			desc.Fields = result.Fields

		case protocol.MsgNoData:
			// No data to return (e.g., for non-SELECT statements).

		case protocol.MsgReadyForQuery:
			c.txnStatus = body[0]
			return desc, nil

		case protocol.MsgErrorResponse:
			return nil, c.handleErrorAndWaitForReady(body)

		case protocol.MsgNoticeResponse:
			// Ignore notices.

		case protocol.MsgParameterStatus:
			if err := c.handleParameterStatus(body); err != nil {
				return nil, err
			}

		default:
			return nil, fmt.Errorf("unexpected message type: %c (0x%02x)", msgType, msgType)
		}
	}
}

// processBindAndExecuteResponses processes responses to BindAndExecute.
// Expects: BindComplete, then execute results (RowDescription, DataRow, CommandComplete), then ReadyForQuery.
// The callback is invoked in a streaming fashion with batched rows:
// - Rows are accumulated until DefaultStreamingBatchSize is exceeded, then flushed with Fields
// - On CommandComplete: remaining rows + CommandTag sent together (signals end of result set)
// For small result sets, this means a single callback with Fields, Rows, and CommandTag.
func (c *Conn) processBindAndExecuteResponses(ctx context.Context, callback func(ctx context.Context, result *query.QueryResult) error) error {
	gotBindComplete := false
	var currentFields []*query.Field
	var batchedRows []*query.Row
	var batchedSize int

	// flushBatch sends accumulated rows via callback and resets the batch.
	// Does not reset currentFields as they may be needed for subsequent batches.
	flushBatch := func() error {
		if len(batchedRows) == 0 || callback == nil {
			return nil
		}
		result := &query.QueryResult{
			Fields: currentFields,
			Rows:   batchedRows,
		}
		if err := callback(ctx, result); err != nil {
			return err
		}
		batchedRows = nil
		batchedSize = 0
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msgType, body, err := c.readMessage()
		if err != nil {
			return fmt.Errorf("failed to read message: %w", err)
		}

		switch msgType {
		case protocol.MsgBindComplete:
			gotBindComplete = true

		case protocol.MsgRowDescription:
			// Start of a new result set - parse and store fields.
			// Fields will be included in the first batch callback.
			result := &query.QueryResult{}
			if err := c.parseRowDescription(body, result); err != nil {
				return err
			}
			currentFields = result.Fields

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
				if err := flushBatch(); err != nil {
					return err
				}
			}

		case protocol.MsgCommandComplete:
			tag, err := c.parseCommandComplete(body)
			if err != nil {
				return err
			}

			// Send final batch with CommandTag (signals end of result set).
			// This combines any remaining rows with the command completion.
			if callback != nil {
				result := &query.QueryResult{
					Fields:       currentFields,
					Rows:         batchedRows,
					CommandTag:   tag,
					RowsAffected: parseRowsAffected(tag),
				}
				if err := callback(ctx, result); err != nil {
					return err
				}
			}

			// Reset for next result set.
			currentFields = nil
			batchedRows = nil
			batchedSize = 0

		case protocol.MsgEmptyQueryResponse:
			if callback != nil {
				if err := callback(ctx, &query.QueryResult{}); err != nil {
					return err
				}
			}

		case protocol.MsgPortalSuspended:
			// Portal execution was suspended (partial results).
			// Flush any batched rows.
			if err := flushBatch(); err != nil {
				return err
			}

		case protocol.MsgReadyForQuery:
			c.txnStatus = body[0]
			if !gotBindComplete {
				return fmt.Errorf("did not receive BindComplete")
			}
			return nil

		case protocol.MsgErrorResponse:
			return c.handleErrorAndWaitForReady(body)

		case protocol.MsgNoticeResponse:
			// Ignore notices.

		case protocol.MsgParameterStatus:
			if err := c.handleParameterStatus(body); err != nil {
				return err
			}

		default:
			return fmt.Errorf("unexpected message type: %c (0x%02x)", msgType, msgType)
		}
	}
}

// processBindAndDescribeResponses processes responses to BindAndDescribe.
// Expects: BindComplete, then describe results (RowDescription or NoData), then ReadyForQuery.
// Note: Describe('P') for a portal does NOT return ParameterDescription, only RowDescription/NoData.
func (c *Conn) processBindAndDescribeResponses(ctx context.Context) (*query.StatementDescription, error) {
	gotBindComplete := false
	desc := &query.StatementDescription{}

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		msgType, body, err := c.readMessage()
		if err != nil {
			return nil, fmt.Errorf("failed to read message: %w", err)
		}

		switch msgType {
		case protocol.MsgBindComplete:
			gotBindComplete = true

		case protocol.MsgRowDescription:
			result := &query.QueryResult{}
			if err := c.parseRowDescription(body, result); err != nil {
				return nil, err
			}
			desc.Fields = result.Fields

		case protocol.MsgNoData:
			// No data to return (e.g., for non-SELECT statements).

		case protocol.MsgReadyForQuery:
			c.txnStatus = body[0]
			if !gotBindComplete {
				return nil, fmt.Errorf("did not receive BindComplete")
			}
			return desc, nil

		case protocol.MsgErrorResponse:
			return nil, c.handleErrorAndWaitForReady(body)

		case protocol.MsgNoticeResponse:
			// Ignore notices.

		case protocol.MsgParameterStatus:
			if err := c.handleParameterStatus(body); err != nil {
				return nil, err
			}

		default:
			return nil, fmt.Errorf("unexpected message type: %c (0x%02x)", msgType, msgType)
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
func (c *Conn) processPrepareAndExecuteResponses(ctx context.Context, callback func(ctx context.Context, result *query.QueryResult) error) error {
	gotParseComplete := false
	gotBindComplete := false
	var currentFields []*query.Field
	var batchedRows []*query.Row
	var batchedSize int

	// flushBatch sends accumulated rows via callback and resets the batch.
	// Does not reset currentFields as they may be needed for subsequent batches.
	flushBatch := func() error {
		if len(batchedRows) == 0 || callback == nil {
			return nil
		}
		result := &query.QueryResult{
			Fields: currentFields,
			Rows:   batchedRows,
		}
		if err := callback(ctx, result); err != nil {
			return err
		}
		batchedRows = nil
		batchedSize = 0
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

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
			result := &query.QueryResult{}
			if err := c.parseRowDescription(body, result); err != nil {
				return err
			}
			currentFields = result.Fields

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
				if err := flushBatch(); err != nil {
					return err
				}
			}

		case protocol.MsgCommandComplete:
			tag, err := c.parseCommandComplete(body)
			if err != nil {
				return err
			}

			// Send final batch with CommandTag (signals end of result set).
			// This combines any remaining rows with the command completion.
			if callback != nil {
				result := &query.QueryResult{
					Fields:       currentFields,
					Rows:         batchedRows,
					CommandTag:   tag,
					RowsAffected: parseRowsAffected(tag),
				}
				if err := callback(ctx, result); err != nil {
					return err
				}
			}

			// Reset for next result set.
			currentFields = nil
			batchedRows = nil
			batchedSize = 0

		case protocol.MsgEmptyQueryResponse:
			if callback != nil {
				if err := callback(ctx, &query.QueryResult{}); err != nil {
					return err
				}
			}

		case protocol.MsgReadyForQuery:
			c.txnStatus = body[0]
			if !gotParseComplete {
				return fmt.Errorf("did not receive ParseComplete")
			}
			if !gotBindComplete {
				return fmt.Errorf("did not receive BindComplete")
			}
			return nil

		case protocol.MsgErrorResponse:
			return c.handleErrorAndWaitForReady(body)

		case protocol.MsgNoticeResponse:
			// Ignore notices.

		case protocol.MsgParameterStatus:
			if err := c.handleParameterStatus(body); err != nil {
				return err
			}

		default:
			return fmt.Errorf("unexpected message type: %c (0x%02x)", msgType, msgType)
		}
	}
}
