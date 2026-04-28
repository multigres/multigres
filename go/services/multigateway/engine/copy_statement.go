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

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multigateway/handler"
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
	_ []*ast.A_Const,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	// For now, use DefaultShard (unsharded). When sharding is supported,
	// this will need to be determined from the COPY target table.
	shard := constants.DefaultShard

	// Phase 1: INITIATE - Send COPY command to pooler
	// CopyInitiate stores reserved connection info in state.ShardStates internally
	format, columnFormats, err := exec.CopyInitiate(ctx, conn, c.TableGroup, shard, c.Query, state, func(ctx context.Context, result *sqltypes.Result) error {
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to initiate COPY: %w", err)
	}

	// Ensure cleanup on any exit path after successful initiate
	completed := false
	defer func() {
		if !completed {
			_ = exec.CopyAbort(ctx, conn, c.TableGroup, shard, state)
		}
	}()

	// Send CopyInResponse to client
	if err := conn.WriteCopyInResponse(format, columnFormats); err != nil {
		return fmt.Errorf("failed to write CopyInResponse: %w", err)
	}
	if err := conn.Flush(); err != nil {
		return fmt.Errorf("failed to flush CopyInResponse: %w", err)
	}

	// Phase 2: DATA - Read from client and send chunks to pooler
	for {
		msgType, err := conn.ReadMessageType()
		if err != nil {
			return fmt.Errorf("failed to read message: %w", err)
		}

		length, err := conn.ReadMessageLength()
		if err != nil {
			return fmt.Errorf("failed to read message length: %w", err)
		}

		switch msgType {
		case protocol.MsgCopyData:
			data, err := conn.ReadCopyDataMessage(length)
			if err != nil {
				return err
			}
			if err := exec.CopySendData(ctx, conn, c.TableGroup, shard, state, data); err != nil {
				return fmt.Errorf("failed to send COPY data: %w", err)
			}

		case protocol.MsgCopyDone:
			if err := conn.ReadCopyDoneMessage(length); err != nil {
				return err
			}
			// Phase 3: DONE - Finalize (no buffered data in streaming mode)
			if err := exec.CopyFinalize(ctx, conn, c.TableGroup, shard, state, nil, callback); err != nil {
				return err
			}
			completed = true // Mark after successful completion
			return nil

		case protocol.MsgCopyFail:
			errMsg, err := conn.ReadCopyFailMessage(length)
			if err != nil {
				return err
			}
			return mterrors.NewPgError("ERROR", mterrors.PgSSProtocolViolation,
				"COPY failed: "+errMsg, "")

		default:
			return mterrors.NewPgError("ERROR", mterrors.PgSSProtocolViolation,
				fmt.Sprintf("unexpected message type during COPY: %c", msgType), "")
		}
	}
}

// PortalStreamExecute satisfies the Primitive interface for the
// extended-protocol path. COPY FROM STDIN is simple-protocol only —
// PlanPortal returns nil for CopyStmt and isCacheable rejects it — so
// the executor never reaches this method in practice. The delegate to
// StreamExecute keeps the contract uniform without inventing portal
// semantics for a primitive that doesn't have any.
func (c *CopyStatement) PortalStreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	_ *preparedstatement.PortalInfo,
	_ int32,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	return c.StreamExecute(ctx, exec, conn, state, nil, callback)
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
