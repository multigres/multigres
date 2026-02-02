// Copyright 2026 Supabase, Inc.
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

package engine

import (
	"context"
	"fmt"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/multigateway/handler"
	"github.com/multigres/multigres/go/parser/ast"
)

// CopyStatement implements the Primitive interface for executing COPY statements.
// Currently supports COPY FROM STDIN; extensible for COPY TO STDOUT and other COPY variants.
type CopyStatement struct {
	TableGroup string
	Query      string
	CopyStmt   *ast.CopyStmt
}

// NewCopyStatement creates a new CopyStatement primitive.
func NewCopyStatement(tableGroup, query string, copyStmt *ast.CopyStmt) *CopyStatement {
	return &CopyStatement{
		TableGroup: tableGroup,
		Query:      query,
		CopyStmt:   copyStmt,
	}
}

// StreamExecute implements the Primitive interface.
// Orchestrates COPY operations (currently COPY FROM STDIN).
func (c *CopyStatement) StreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	// For now, shard is empty (unsharded). When sharding is supported,
	// this will need to be determined from the COPY target table.
	shard := ""

	// Phase 1: INITIATE - Send COPY command to pooler
	// CopyInitiate stores reserved connection info in state.ShardStates internally
	format, columnFormats, err := exec.CopyInitiate(ctx, conn, c.TableGroup, shard, c.Query, state, func(ctx context.Context, result *sqltypes.Result) error {
		return nil
	})
	if err != nil {
		// Initiation failed - clean up any partial stream state
		state.ClearCopyStream()
		return fmt.Errorf("failed to initiate COPY: %w", err)
	}

	// Send CopyInResponse to client
	if err := conn.WriteCopyInResponse(format, columnFormats); err != nil {
		// Failed to send response to client - abort and clean up
		c.abort(ctx, exec, conn, state, shard)
		state.ClearCopyStream()
		return fmt.Errorf("failed to write CopyInResponse: %w", err)
	}
	if err := conn.Flush(); err != nil {
		c.abort(ctx, exec, conn, state, shard)
		state.ClearCopyStream()
		return fmt.Errorf("failed to flush CopyInResponse: %w", err)
	}

	// Phase 2: DATA - Read from client and send chunks to pooler
	for {
		msgType, err := conn.ReadMessageType()
		if err != nil {
			c.abort(ctx, exec, conn, state, shard)
			return fmt.Errorf("failed to read message: %w", err)
		}

		length, err := conn.ReadMessageLength()
		if err != nil {
			c.abort(ctx, exec, conn, state, shard)
			return fmt.Errorf("failed to read message length: %w", err)
		}

		switch msgType {
		case protocol.MsgCopyData:
			if err := c.handleData(ctx, exec, conn, state, shard, length); err != nil {
				c.abort(ctx, exec, conn, state, shard)
				return err
			}

		case protocol.MsgCopyDone:
			if err := conn.ReadCopyDoneMessage(length); err != nil {
				c.abort(ctx, exec, conn, state, shard)
				return err
			}
			// Phase 3: DONE - Finalize
			return c.finalize(ctx, exec, conn, state, shard, callback)

		case protocol.MsgCopyFail:
			errMsg, err := conn.ReadCopyFailMessage(length)
			if err != nil {
				c.abort(ctx, exec, conn, state, shard)
				return err
			}
			c.abort(ctx, exec, conn, state, shard)
			return fmt.Errorf("COPY failed: %s", errMsg)

		default:
			c.abort(ctx, exec, conn, state, shard)
			return fmt.Errorf("unexpected message type during COPY: %c", msgType)
		}
	}
}

// handleData processes CopyData messages from client.
func (c *CopyStatement) handleData(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	shard string,
	length int,
) error {
	data, err := conn.ReadCopyDataMessage(length)
	if err != nil {
		return err
	}

	// Stream data immediately to pooler
	if err := exec.CopySendData(ctx, conn, c.TableGroup, shard, state, data); err != nil {
		return fmt.Errorf("failed to send COPY data: %w", err)
	}

	return nil
}

// finalize sends CopyDone and receives result.
// With streaming, there should be no buffered data.
func (c *CopyStatement) finalize(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	shard string,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	// Call executor's CopyFinalize with no buffered data (streaming mode)
	// CopyFinalize looks up reserved connection from state internally
	return exec.CopyFinalize(ctx, conn, c.TableGroup, shard, state, nil, callback)
}

// abort sends CopyFail to pooler.
func (c *CopyStatement) abort(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	shard string,
) {
	_ = exec.CopyAbort(ctx, conn, c.TableGroup, shard, state)
}

// GetTableGroup implements the Primitive interface.
func (c *CopyStatement) GetTableGroup() string {
	return c.TableGroup
}

// GetQuery implements the Primitive interface.
func (c *CopyStatement) GetQuery() string {
	return c.Query
}

// String implements the Primitive interface.
func (c *CopyStatement) String() string {
	direction := "FROM STDIN"
	if !c.CopyStmt.IsFrom {
		direction = "TO STDOUT"
	}
	return fmt.Sprintf("CopyStatement(%s %s)", c.CopyStmt.Relation.RelName, direction)
}

// Ensure CopyStatement implements Primitive interface.
var _ Primitive = (*CopyStatement)(nil)
