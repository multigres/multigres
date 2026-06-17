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
	pgClient "github.com/multigres/multigres/go/common/pgprotocol/client"
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
// Orchestrates COPY operations: COPY FROM STDIN (client → server data)
// and COPY TO STDOUT (server → client data) are both streamed through this
// primitive. The branch is taken on c.CopyStmt.IsFrom.
func (c *CopyStatement) StreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	_ []*ast.A_Const,
	_ PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	// For now, use DefaultShard (unsharded). When sharding is supported,
	// this will need to be determined from the COPY target table.
	shard := constants.DefaultShard

	if !c.CopyStmt.IsFrom {
		return c.streamCopyOut(ctx, exec, conn, shard, state, callback)
	}

	// Phase 1: INITIATE - Send COPY command to pooler
	// CopyInitiate stores reserved connection info in state.ShardStates internally.
	// PG errors (e.g. "column does not exist") are surfaced un-wrapped so the
	// gateway re-emits a verbatim ErrorResponse to the client — wrapping with
	// "failed to initiate COPY:" here would corrupt regression-test fixtures
	// that match against bare upstream PG ERROR text.
	format, columnFormats, err := exec.CopyInitiate(ctx, conn, c.TableGroup, shard, c.Query, state, func(ctx context.Context, result *sqltypes.Result) error {
		return nil
	})
	if err != nil {
		return err
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
				// PG errors are returned un-wrapped — see comment in
				// StreamExecute's CopyInitiate error path.
				return err
			}

		case protocol.MsgCopyDone:
			if err := conn.ReadCopyDoneMessage(length); err != nil {
				return err
			}
			// Phase 3: DONE - Finalize (no buffered data in streaming mode).
			// CopyFinalize owns the full lifecycle of the COPY's tail end:
			// it sends CopyDone to PG, reads the response, and on a PG-level
			// error (e.g., constraint violation) drains ReadyForQuery and
			// updates the gateway's reserved-state tracking with whatever
			// state the multipooler returned. Mark the COPY completed before
			// returning the error so the deferred CopyAbort doesn't run on
			// top of an already-cleaned-up flow — running it would clear
			// gateway state for a connection the multipooler intentionally
			// kept alive (e.g., because a transaction reason remains).
			finalizeErr := exec.CopyFinalize(ctx, conn, c.TableGroup, shard, state, nil, callback)
			completed = true
			return finalizeErr

		case protocol.MsgCopyFail:
			errMsg, err := conn.ReadCopyFailMessage(length)
			if err != nil {
				return err
			}
			// Match PostgreSQL's response to a client CopyFail: copyfromparse.c
			// raises ERRCODE_QUERY_CANCELED (57014) with "COPY from stdin
			// failed: <msg>".
			return mterrors.NewPgError("ERROR", mterrors.PgSSQueryCanceled,
				"COPY from stdin failed: "+errMsg, "")

		default:
			return mterrors.NewPgError("ERROR", mterrors.PgSSProtocolViolation,
				fmt.Sprintf("unexpected message type during COPY: %c", msgType), "")
		}
	}
}

// PortalStreamExecute satisfies the Primitive interface for the
// extended-protocol path. COPY is effectively simple-protocol only —
// PostgreSQL rejects COPY in the extended query protocol — so the executor
// does not reach this method in practice. The delegate to StreamExecute keeps
// the contract uniform without inventing portal semantics for a primitive that
// doesn't have any.
func (c *CopyStatement) PortalStreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	_ *preparedstatement.PortalInfo,
	_ int32,
	_ bool,
	_ PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	return c.StreamExecute(ctx, exec, conn, state, nil, PlanExecInfo{}, callback)
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
	// COPY (SELECT ...) TO STDOUT has Relation == nil and Query set
	// instead; keep this nil-safe so debug logging / planner traces don't
	// crash on subquery COPYs.
	target := "(query)"
	if c.CopyStmt.Relation != nil {
		target = c.CopyStmt.Relation.RelName
	}
	return fmt.Sprintf("CopyStatement(%s %s)", target, direction)
}

// streamCopyOut drives a COPY ... TO STDOUT.
//
// Flow:
//  1. CopyOutInitiate → reserves a backend conn and returns the
//     CopyOutResponse format/columnFormats plus any pre-CopyOutResponse
//     notices (BEFORE STATEMENT triggers, etc.).
//  2. Write CopyOutResponse to the client; flush any initiation notices.
//  3. CopyOutStream pumps each CopyData / Notice frame from the
//     multipooler via callback: data is written as CopyData('d') to the
//     client; notices are written as NoticeResponse('N').
//  4. Final RESULT carries the CommandTag + post-data Notices. Notices
//     are written first (between the last CopyData and CopyDone), then
//     CopyDone('c') is sent, then the higher-level callback consumes
//     the Result (which writes CommandComplete).
func (c *CopyStatement) streamCopyOut(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	shard string,
	state *handler.MultiGatewayConnectionState,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	format, columnFormats, initNotices, err := exec.CopyOutInitiate(ctx, conn, c.TableGroup, shard, c.Query, state)
	if err != nil {
		// Surface PG errors un-wrapped so the gateway re-emits a verbatim
		// ErrorResponse — see StreamExecute's FROM-STDIN path comment.
		return err
	}

	// CopyOutInitiate succeeded — the multipooler has reserved a backend
	// conn and added the ReasonCopy reservation reason. If anything below
	// returns before CopyOutStream removes it (notice callback,
	// WriteCopyOutResponse, Flush, or a ReadCopyOutMessage error that
	// the stream pump turns into a connection-level fail), we must run
	// CopyAbort so the reservation is released and the backend isn't left
	// stuck in COPY OUT pushing CopyData with no reader. CopyAbort sends
	// CopyFail which isn't a legal frontend message during COPY OUT, but
	// the resulting protocol error makes the multipooler release the
	// reserved conn with ReleaseError — which is the cleanup we want.
	//
	// Mirrors the FROM-STDIN guard in StreamExecute. The flag is cleared
	// once CopyOutStream returns successfully (at which point ReasonCopy
	// is already removed inside the executor); anything failing after that
	// is gateway-to-client write failure and the reservation is gone.
	streamCompleted := false
	defer func() {
		if !streamCompleted {
			_ = exec.CopyAbort(ctx, conn, c.TableGroup, shard, state)
		}
	}()

	// Forward any pre-CopyOutResponse notices to the client as standalone
	// NoticeResponse frames via the callback (the per-result Conn writer
	// emits NoticeResponse for every entry in result.Notices).
	if len(initNotices) > 0 {
		if err := callback(ctx, &sqltypes.Result{Notices: initNotices}); err != nil {
			return err
		}
	}

	if err := conn.WriteCopyOutResponse(format, columnFormats); err != nil {
		return fmt.Errorf("failed to write CopyOutResponse: %w", err)
	}
	if err := conn.Flush(); err != nil {
		return fmt.Errorf("failed to flush CopyOutResponse: %w", err)
	}

	// Pump data + interleaved notices to the client.
	result, err := exec.CopyOutStream(ctx, conn, c.TableGroup, shard, state, func(msg pgClient.CopyOutMessage) error {
		if msg.Notice != nil {
			if cbErr := callback(ctx, &sqltypes.Result{Notices: []*mterrors.PgDiagnostic{msg.Notice}}); cbErr != nil {
				return cbErr
			}
			return nil
		}
		if err := conn.WriteCopyData(msg.Data); err != nil {
			return fmt.Errorf("failed to write CopyData: %w", err)
		}
		// Don't flush per chunk — bufferedWriter will flush in
		// reasonably large batches, which is the usual server behavior.
		return nil
	})
	if err != nil {
		// CopyOutStream's own error paths already drop ReasonCopy and
		// release the conn (PG error → ReleasePortalComplete or kept-state;
		// connection error → ReleaseError). Mark the stream complete so
		// the deferred CopyAbort above doesn't run a second cleanup on
		// top of an already-released conn.
		streamCompleted = true
		return err
	}
	// Success path — executor's CopyOutStream removed ReasonCopy and
	// released (or kept-state) the conn. No deferred abort needed.
	streamCompleted = true

	// Wire ordering matches upstream PG for COPY ... TO STDOUT:
	//   CopyData* → CopyDone → NoticeResponse* → CommandComplete → ReadyForQuery
	//
	// result.Notices holds diagnostics PG sent between CopyDone and
	// CommandComplete (AFTER STATEMENT trigger output, COPY progress
	// finalization, etc.), so they go AFTER WriteCopyDone but BEFORE the
	// callback that emits CommandComplete. NoticeResponse is technically
	// valid at any point on the wire, so any client (libpq, pgx) will
	// accept it either way — but matching PG ordering avoids surprising
	// protocol-level diff tools / wire fuzzers.
	if err := conn.WriteCopyDone(); err != nil {
		return fmt.Errorf("failed to write CopyDone: %w", err)
	}

	if len(result.Notices) > 0 {
		if cbErr := callback(ctx, &sqltypes.Result{Notices: result.Notices}); cbErr != nil {
			return cbErr
		}
	}

	// Hand the Result (with CommandTag) to the higher-level callback which
	// will write CommandComplete and update transaction bookkeeping.
	resultForCallback := &sqltypes.Result{
		CommandTag:   result.CommandTag,
		RowsAffected: result.RowsAffected,
	}
	if err := callback(ctx, resultForCallback); err != nil {
		return err
	}
	return nil
}

// Ensure CopyStatement implements Primitive interface.
var _ Primitive = (*CopyStatement)(nil)
