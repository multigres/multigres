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
	"time"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/parser/ast"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/sqltypes"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// TransactionPrimitive handles transaction control statements (BEGIN, COMMIT, ROLLBACK).
//
// Key behaviors:
//   - BEGIN: Deferred execution - sets TxState to InTransaction and returns a synthetic
//     result without making a backend call. The actual BEGIN is sent with the first
//     real query via StreamExecute with reservation options.
//   - COMMIT: Concludes the transaction on all shards (removes the transaction
//     reservation reason; connections are fully released only if no other reasons
//     remain), syncs pending LISTEN/UNLISTEN subscriptions.
//   - ROLLBACK: Concludes the transaction on all shards (same reservation semantics
//     as COMMIT), discards pending LISTEN/UNLISTEN subscriptions.
type TransactionPrimitive struct {
	// Kind is the type of transaction statement (BEGIN, COMMIT, ROLLBACK, etc.)
	Kind ast.TransactionStmtKind

	// Query is the original Query string for this statement.
	Query string

	// TableGroup is the target tablegroup (used for COMMIT/ROLLBACK routing).
	TableGroup string

	// metrics records transaction duration and count. Nil-safe: if nil,
	// metrics are silently skipped (e.g., in tests without OTel setup).
	metrics *TransactionMetrics
}

// NewTransactionPrimitive creates a new TransactionPrimitive.
func NewTransactionPrimitive(kind ast.TransactionStmtKind, sql, tableGroup string, metrics *TransactionMetrics) *TransactionPrimitive {
	return &TransactionPrimitive{
		Kind:       kind,
		Query:      sql,
		TableGroup: tableGroup,
		metrics:    metrics,
	}
}

// StreamExecute executes the transaction primitive.
func (t *TransactionPrimitive) StreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	_ []*ast.A_Const,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	switch t.Kind {
	case ast.TRANS_STMT_BEGIN, ast.TRANS_STMT_START:
		return t.executeBegin(ctx, conn, state, callback)

	case ast.TRANS_STMT_COMMIT:
		return t.executeCommit(ctx, exec, conn, state, callback)

	case ast.TRANS_STMT_ROLLBACK:
		return t.executeRollback(ctx, exec, conn, state, callback)

	case ast.TRANS_STMT_ROLLBACK_TO:
		return t.executeRollbackToSavepoint(ctx, exec, conn, state, callback)

	default:
		// For other transaction statements (SAVEPOINT, RELEASE SAVEPOINT), pass through to backend
		return exec.StreamExecute(ctx, conn, t.TableGroup, constants.DefaultShard, t.Query, nil, state, callback)
	}
}

// executeBegin handles BEGIN/START TRANSACTION with deferred execution.
// Sets transaction state but doesn't send to backend - the actual BEGIN
// will be sent atomically with the first real query.
// Stores the original query text so isolation level and access mode options
// (e.g., "BEGIN ISOLATION LEVEL SERIALIZABLE") are preserved.
func (t *TransactionPrimitive) executeBegin(
	ctx context.Context,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	// Set transaction state (deferred - no backend call yet)
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	// Store the original BEGIN query so the multipooler can replay it with
	// the correct isolation level and access mode when creating the reserved connection.
	state.PendingBeginQuery = t.Query

	// Record transaction start time for duration tracking.
	state.TxnStartTime = time.Now()

	// Return synthetic result to client
	return callback(ctx, &sqltypes.Result{
		CommandTag: "BEGIN",
	})
}

// executeCommit handles COMMIT by concluding the transaction on all reserved connections.
// Pending LISTEN/UNLISTEN subscriptions are synced before the COMMIT result is sent
// to the client, ensuring subscriptions are active before the client is told the
// transaction committed.
func (t *TransactionPrimitive) executeCommit(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	// Clear pending begin query — transaction is ending.
	state.PendingBeginQuery = ""

	// Record transaction metrics before clearing state.
	t.recordTxnMetrics(ctx, conn, state, TxnOutcomeCommit)

	// If no reserved connections, or if the reserved connections don't have
	// an active transaction (e.g., temp-table-reserved session where BEGIN
	// was deferred but never sent to PG), return synthetic result.
	hasActiveTransaction := false
	for _, ss := range state.ShardStates {
		if ss.ReservedState != nil && ss.ReservedState.ReservationReasons&protoutil.ReasonTransaction != 0 {
			hasActiveTransaction = true
			break
		}
	}
	if len(state.ShardStates) == 0 || !hasActiveTransaction {
		conn.SetTxnStatus(protocol.TxnStatusIdle)
		syncPendingSubscriptions(conn, state)
		return callback(ctx, &sqltypes.Result{
			CommandTag: "COMMIT",
		})
	}

	// Wrap the callback to sync subscriptions after the backend confirms COMMIT
	// but before the CommandComplete is sent to the client.
	commitCallback := callback
	if state.HasPendingListens() {
		commitCallback = func(cbCtx context.Context, result *sqltypes.Result) error {
			if result != nil && result.CommandTag != "" {
				syncPendingSubscriptions(conn, state)
			}
			return callback(cbCtx, result)
		}
	}

	// Conclude the transaction on all shards via the ConcludeTransaction RPC.
	// ConcludeTransaction clears shard state entries where the connection was
	// fully released (remainingReasons == 0) and keeps entries where the
	// connection is still reserved for other reasons (e.g., temp tables).
	err := exec.ConcludeTransaction(ctx, conn, state,
		multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_COMMIT, commitCallback)

	// Reset transaction state regardless of error.
	conn.SetTxnStatus(protocol.TxnStatusIdle)

	return err
}

// syncPendingSubscriptions applies buffered LISTEN/UNLISTEN changes via the
// connection's SubscriptionSync. Called during COMMIT to ensure subscriptions
// are active before the client is told the transaction committed.
func syncPendingSubscriptions(conn *server.Conn, state *handler.MultiGatewayConnectionState) {
	if !state.HasPendingListens() {
		return
	}
	subs, unsubs, unsubAll := state.CommitPendingListens()
	state.SubSync.SyncSubscriptions(conn.Context(), conn, state, subs, unsubs, unsubAll)
}

// executeRollback handles ROLLBACK by concluding the transaction on all reserved connections.
func (t *TransactionPrimitive) executeRollback(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	// Clear pending begin query — transaction is ending.
	state.PendingBeginQuery = ""
	// Discard any pending LISTEN/UNLISTEN changes — ROLLBACK cancels them.
	state.DiscardPendingListens()

	// Record transaction metrics before clearing state.
	t.recordTxnMetrics(ctx, conn, state, TxnOutcomeRollback)

	// If no reserved connections, or if the reserved connections don't have
	// an active transaction, return synthetic result.
	hasActiveRbTransaction := false
	for _, ss := range state.ShardStates {
		if ss.ReservedState != nil && ss.ReservedState.ReservationReasons&protoutil.ReasonTransaction != 0 {
			hasActiveRbTransaction = true
			break
		}
	}
	if len(state.ShardStates) == 0 || !hasActiveRbTransaction {
		conn.SetTxnStatus(protocol.TxnStatusIdle)
		return callback(ctx, &sqltypes.Result{
			CommandTag: "ROLLBACK",
		})
	}

	// Conclude the transaction on all shards via the ConcludeTransaction RPC.
	// ConcludeTransaction clears shard state entries where the connection was
	// fully released (remainingReasons == 0) and keeps entries where the
	// connection is still reserved for other reasons (e.g., temp tables).
	err := exec.ConcludeTransaction(ctx, conn, state,
		multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_ROLLBACK, callback)

	// Reset transaction state regardless of error.
	conn.SetTxnStatus(protocol.TxnStatusIdle)

	return err
}

// executeRollbackToSavepoint handles ROLLBACK TO SAVEPOINT by passing through
// to the backend. On success, transitions TxnStatus from Failed back to InBlock
// so subsequent statements can execute normally. This matches PostgreSQL's
// behavior where ROLLBACK TO SAVEPOINT is the primary mechanism for recovering
// from errors within a transaction.
func (t *TransactionPrimitive) executeRollbackToSavepoint(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	wasFailed := conn.TxnStatus() == protocol.TxnStatusFailed

	err := exec.StreamExecute(ctx, conn, t.TableGroup, constants.DefaultShard, t.Query, nil, state, callback)
	if err != nil {
		return err
	}

	// On success, restore transaction state if we were in the aborted state.
	// The backend has rolled back to the savepoint and is now in a normal
	// in-transaction state.
	if wasFailed {
		conn.SetTxnStatus(protocol.TxnStatusInBlock)
	}

	return nil
}

// recordTxnMetrics records transaction duration and count if TxnStartTime
// was set (i.e., a BEGIN was executed). Clears TxnStartTime after recording.
func (t *TransactionPrimitive) recordTxnMetrics(
	ctx context.Context,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	outcome string,
) {
	if state.TxnStartTime.IsZero() {
		return
	}
	txnDuration := time.Since(state.TxnStartTime)
	state.TxnStartTime = time.Time{}
	t.metrics.RecordCompletion(ctx, txnDuration.Seconds(), conn.Database(), outcome)
}

// PortalStreamExecute satisfies the Primitive interface for the
// extended-protocol path. BEGIN / COMMIT / ROLLBACK / SAVEPOINT carry
// no parameter binds; PlanPortal explicitly funnels TransactionStmt
// through Plan() so this primitive runs locally rather than being
// portal-forwarded. Delegate.
func (t *TransactionPrimitive) PortalStreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	_ *preparedstatement.PortalInfo,
	_ int32,
	_ bool,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	return t.StreamExecute(ctx, exec, conn, state, nil, callback)
}

// GetTableGroup returns the target tablegroup.
func (t *TransactionPrimitive) GetTableGroup() string {
	return t.TableGroup
}

// GetQuery returns the Query query.
func (t *TransactionPrimitive) GetQuery() string {
	return t.Query
}

// String returns a description of the primitive for debugging.
func (t *TransactionPrimitive) String() string {
	return fmt.Sprintf("Transaction(%s)", t.Kind.String())
}

// Ensure TransactionPrimitive implements Primitive interface.
var _ Primitive = (*TransactionPrimitive)(nil)
