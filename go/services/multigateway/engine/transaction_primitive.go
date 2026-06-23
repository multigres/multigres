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
	"github.com/multigres/multigres/go/common/mterrors"
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
//   - SAVEPOINT / RELEASE / ROLLBACK TO: Pass through to backend; on success, push
//     or pop the gateway's per-savepoint snapshot stack so SessionSettings and
//     gateway-managed variables track PostgreSQL's GUC stack semantics.
type TransactionPrimitive struct {
	// Kind is the type of transaction statement (BEGIN, COMMIT, ROLLBACK, etc.)
	Kind ast.TransactionStmtKind

	// SavepointName is the savepoint identifier for SAVEPOINT / RELEASE /
	// ROLLBACK TO statements. Empty for other kinds.
	SavepointName string

	// Chain is true for COMMIT AND CHAIN / ROLLBACK AND CHAIN.
	Chain bool

	// Query is the original Query string for this statement.
	Query string

	// TableGroup is the target tablegroup (used for COMMIT/ROLLBACK routing).
	TableGroup string

	// metrics records transaction duration and count. Nil-safe: if nil,
	// metrics are silently skipped (e.g., in tests without OTel setup).
	metrics *TransactionMetrics
}

// NewTransactionPrimitive creates a new TransactionPrimitive.
func NewTransactionPrimitive(kind ast.TransactionStmtKind, savepointName string, chain bool, sql, tableGroup string, metrics *TransactionMetrics) *TransactionPrimitive {
	return &TransactionPrimitive{
		Kind:          kind,
		SavepointName: savepointName,
		Chain:         chain,
		Query:         sql,
		TableGroup:    tableGroup,
		metrics:       metrics,
	}
}

// StreamExecute executes the transaction primitive.
func (t *TransactionPrimitive) StreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	_ []*ast.A_Const,
	info PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	switch t.Kind {
	case ast.TRANS_STMT_BEGIN, ast.TRANS_STMT_START:
		return t.executeBegin(ctx, conn, state, callback)

	case ast.TRANS_STMT_COMMIT:
		return t.executeCommit(ctx, exec, conn, state, callback)

	case ast.TRANS_STMT_ROLLBACK:
		return t.executeRollback(ctx, exec, conn, state, callback)

	case ast.TRANS_STMT_SAVEPOINT:
		return t.executeSavepoint(ctx, exec, conn, state, callback)

	case ast.TRANS_STMT_RELEASE:
		return t.executeReleaseSavepoint(ctx, exec, conn, state, callback)

	case ast.TRANS_STMT_ROLLBACK_TO:
		return t.executeRollbackToSavepoint(ctx, exec, conn, state, callback)

	case ast.TRANS_STMT_PREPARE:
		return t.executePrepareTransaction(ctx, exec, conn, state, info, callback)

	default:
		return exec.StreamExecute(ctx, conn, t.TableGroup, constants.DefaultShard, t.Query, nil, state, info, callback)
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
	state.ActiveTransactionBeginQuery = t.Query

	// Record transaction start time for duration tracking.
	state.TxnStartTime = time.Now()

	// Push a BEGIN-level snapshot of SessionSettings and gateway-managed variables
	// so a subsequent ROLLBACK can revert any SET / RESET issued in the transaction.
	state.BeginTransaction()

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
	if t.Chain && !conn.IsInTransaction() {
		return chainOutsideTransactionError("COMMIT")
	}

	chainBeginQuery := inheritedBeginQuery(state)

	// Clear pending begin query — transaction is ending. COMMIT AND CHAIN may
	// restore it below when the old transaction never reached a backend.
	state.PendingBeginQuery = ""
	// PostgreSQL converts COMMIT into ROLLBACK when the transaction is in a
	// failed state, so SET / RESET issued before the failure must revert rather
	// than persist, every open cursor (including WITH HOLD) is closed, and the
	// wire-level command tag returned to the client is `ROLLBACK` rather than
	// `COMMIT`. For a healthy COMMIT, drop the savepoint stack and clear SET
	// LOCAL overrides — current values of non-LOCAL SETs become persistent
	// session state.
	implicitRollback := conn.TxnStatus() == protocol.TxnStatusFailed
	// Capture the HOLD-cursor diff BEFORE state is mutated. PG closes cursors
	// declared inside the transaction at the implicit ROLLBACK boundary; cursors
	// declared before BEGIN (under autocommit) survive.
	var rollbackPortalReleases []string
	if implicitRollback {
		rollbackPortalReleases = state.HoldCursorsDeclaredInTxn()
		state.RestoreOpenHoldCursorsToBeginSnapshot()
		state.RollbackTransaction()
	} else {
		state.CommitTransaction()
	}

	// Record transaction metrics before starting the chained transaction's timer.
	outcome := TxnOutcomeCommit
	if implicitRollback {
		outcome = TxnOutcomeRollback
	}
	t.recordTxnMetrics(ctx, conn, state, outcome)

	if t.Chain {
		state.BeginTransaction()
		state.ActiveTransactionBeginQuery = chainBeginQuery
		state.TxnStartTime = time.Now()
	} else {
		state.ActiveTransactionBeginQuery = ""
	}

	// Choose the conclusion + synthetic command tag based on whether PG will
	// honour or rewrite this COMMIT. Implicit ROLLBACK uses the multipooler's
	// ROLLBACK path so ReleaseAllPortals fires and any pinned HOLD cursors are
	// unpinned alongside the txn reason.
	conclusion := multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_COMMIT
	commandTag := "COMMIT"
	if implicitRollback {
		conclusion = multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_ROLLBACK
		commandTag = "ROLLBACK"
	}

	// If no reserved connections, or if the reserved connections don't have an
	// active transaction (e.g., temp-table-reserved session where BEGIN was
	// deferred but never sent to PG), return synthetic result. For AND CHAIN,
	// keep the gateway in a transaction and preserve the inherited BEGIN query so
	// the eventual backend transaction starts with the same characteristics.
	hasActiveTransaction := hasTransactionReservation(state)
	if len(state.ShardStates) == 0 || !hasActiveTransaction {
		if t.Chain {
			conn.SetTxnStatus(protocol.TxnStatusInBlock)
			state.PendingBeginQuery = chainBeginQuery
		} else {
			conn.SetTxnStatus(protocol.TxnStatusIdle)
		}
		// Pending LISTEN/UNLISTEN buffered inside a failed transaction must be
		// discarded — PG's implicit ROLLBACK invalidates them. A healthy COMMIT
		// promotes them as usual.
		if implicitRollback {
			state.DiscardPendingListens()
		} else {
			syncPendingSubscriptions(conn, state)
		}
		return callback(ctx, &sqltypes.Result{CommandTag: commandTag})
	}

	// Wrap the callback to sync subscriptions after the backend confirms the
	// conclusion but before the CommandComplete is sent to the client.
	commitCallback := callback
	if !implicitRollback && state.HasPendingListens() {
		commitCallback = func(cbCtx context.Context, result *sqltypes.Result) error {
			if result != nil && result.CommandTag != "" {
				syncPendingSubscriptions(conn, state)
			}
			return callback(cbCtx, result)
		}
	} else if implicitRollback {
		// Implicit ROLLBACK invalidates any pending LISTEN/UNLISTEN — drop them
		// silently so they don't leak into the next transaction.
		state.DiscardPendingListens()
	}

	// Conclude the transaction on all shards via the ConcludeTransaction RPC.
	// With AND CHAIN, the multipooler executes COMMIT/ROLLBACK AND CHAIN and
	// keeps the transaction reservation on the same backend; without it, the
	// transaction reason is removed as before.
	err := exec.ConcludeTransaction(ctx, conn, state, conclusion,
		rollbackPortalReleases, false /* releaseAllPortals */, t.Chain, commitCallback)

	if t.Chain {
		conn.SetTxnStatus(protocol.TxnStatusInBlock)
		// If the chained backend reservation was lost (RPC failure, destroyed
		// connection, or rollback recovery), the gateway still owes the client a
		// chained transaction. Preserve the inherited BEGIN query so the next
		// statement creates a replacement backend transaction with the same
		// isolation/read-only/deferrable characteristics. When the backend stayed
		// reserved, it already executed COMMIT/ROLLBACK AND CHAIN, so no deferred
		// BEGIN is needed.
		if !hasTransactionReservation(state) {
			state.PendingBeginQuery = chainBeginQuery
		}
	} else {
		conn.SetTxnStatus(protocol.TxnStatusIdle)
	}

	return err
}

func chainOutsideTransactionError(command string) error {
	return mterrors.NewPgError("ERROR", "25P01",
		command+" AND CHAIN can only be used in transaction blocks", "")
}

func inheritedBeginQuery(state *handler.MultiGatewayConnectionState) string {
	if state.ActiveTransactionBeginQuery != "" {
		return state.ActiveTransactionBeginQuery
	}
	if state.PendingBeginQuery != "" {
		return state.PendingBeginQuery
	}
	return "BEGIN"
}

func hasTransactionReservation(state *handler.MultiGatewayConnectionState) bool {
	for _, ss := range state.ShardStates {
		if ss.ReservedState != nil && ss.ReservedState.ReservationReasons&protoutil.ReasonTransaction != 0 {
			return true
		}
	}
	return false
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
	if t.Chain && !conn.IsInTransaction() {
		return chainOutsideTransactionError("ROLLBACK")
	}

	chainBeginQuery := inheritedBeginQuery(state)

	// Clear pending begin query — transaction is ending. ROLLBACK AND CHAIN may
	// restore it below when the old transaction never reached a backend.
	state.PendingBeginQuery = ""
	// Discard any pending LISTEN/UNLISTEN changes — ROLLBACK cancels them.
	state.DiscardPendingListens()
	// Capture the HOLD-cursor diff before the snapshot stack collapses. PG closes
	// cursors declared inside this transaction; cursors declared before BEGIN
	// (autocommit) survive. Forward the in-txn list to the multipooler so it
	// unpins exactly those names while preserving pre-existing pins.
	rollbackPortalReleases := state.HoldCursorsDeclaredInTxn()
	state.RestoreOpenHoldCursorsToBeginSnapshot()
	// Restore SessionSettings and gateway-managed variables from the BEGIN-level
	// snapshot so any SET / RESET issued in the transaction is reverted.
	state.RollbackTransaction()

	// Record transaction metrics before starting the chained transaction's timer.
	t.recordTxnMetrics(ctx, conn, state, TxnOutcomeRollback)

	if t.Chain {
		state.BeginTransaction()
		state.ActiveTransactionBeginQuery = chainBeginQuery
		state.TxnStartTime = time.Now()
	} else {
		state.ActiveTransactionBeginQuery = ""
	}

	// If no reserved connections, or if the reserved connections don't have an
	// active transaction, return synthetic result. For AND CHAIN, keep the gateway
	// in a transaction and preserve the inherited BEGIN query so the eventual
	// backend transaction starts with the same characteristics.
	hasActiveRbTransaction := hasTransactionReservation(state)
	if len(state.ShardStates) == 0 || !hasActiveRbTransaction {
		if t.Chain {
			conn.SetTxnStatus(protocol.TxnStatusInBlock)
			state.PendingBeginQuery = chainBeginQuery
		} else {
			conn.SetTxnStatus(protocol.TxnStatusIdle)
		}
		return callback(ctx, &sqltypes.Result{CommandTag: "ROLLBACK"})
	}

	// Conclude the transaction on all shards via the ConcludeTransaction RPC.
	// With AND CHAIN, the multipooler executes ROLLBACK AND CHAIN and keeps the
	// transaction reservation on the same backend; without it, the transaction
	// reason is removed as before.
	err := exec.ConcludeTransaction(ctx, conn, state,
		multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_ROLLBACK,
		rollbackPortalReleases, false /* releaseAllPortals */, t.Chain, callback)

	if t.Chain {
		conn.SetTxnStatus(protocol.TxnStatusInBlock)
		// If the chained backend reservation was lost (RPC failure, destroyed
		// connection, or rollback recovery), the gateway still owes the client a
		// chained transaction. Preserve the inherited BEGIN query so the next
		// statement creates a replacement backend transaction with the same
		// isolation/read-only/deferrable characteristics. When the backend stayed
		// reserved, it already executed COMMIT/ROLLBACK AND CHAIN, so no deferred
		// BEGIN is needed.
		if !hasTransactionReservation(state) {
			state.PendingBeginQuery = chainBeginQuery
		}
	} else {
		conn.SetTxnStatus(protocol.TxnStatusIdle)
	}

	return err
}

// executePrepareTransaction handles PREPARE TRANSACTION as a transaction-ending
// statement. PostgreSQL ends the current transaction even when PREPARE fails
// during end-of-transaction checks (for example max_prepared_transactions=0 or
// temporary-object usage). If the gateway treated that backend error like a
// normal in-transaction statement failure, it would leave the client in failed
// transaction state and keep the multipooler reservation marked as in a
// transaction, causing every following statement to cascade with "current
// transaction is aborted" despite the backend already being idle.
func (t *TransactionPrimitive) executePrepareTransaction(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	info PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	state.PendingBeginQuery = ""

	var preparedResult *sqltypes.Result
	err := exec.StreamExecute(ctx, conn, t.TableGroup, constants.DefaultShard, t.Query, nil, state, info,
		func(cbCtx context.Context, result *sqltypes.Result) error {
			// Hold CommandComplete until the reservation cleanup below succeeds, so
			// the client is not told PREPARE completed while the gateway still thinks
			// it has an open transaction. PREPARE TRANSACTION produces only a final
			// command tag; preserve any notices defensively.
			preparedResult = result
			return nil
		})
	if err != nil {
		// Failed PREPARE behaves like a rollback of the transaction being
		// prepared: transaction-local/gateway-tracked state reverts, pending
		// LISTEN/UNLISTEN changes are discarded, and the connection is idle.
		state.DiscardPendingListens()
		state.RollbackTransaction()
		t.recordTxnMetrics(ctx, conn, state, TxnOutcomeRollback)
		_ = t.cleanupPreparedTransactionReservation(ctx, exec, conn, state,
			multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_ROLLBACK)
		conn.SetTxnStatus(protocol.TxnStatusIdle)
		return err
	}

	// Successful PREPARE ends the transaction without committing it yet; from the
	// session's perspective the transaction block is over and the backend is idle.
	// Keep committed session state, clear transaction-local state, and release the
	// transaction reservation without sending a client-visible COMMIT tag.
	state.CommitTransaction()
	t.recordTxnMetrics(ctx, conn, state, TxnOutcomeCommit)
	if err := t.cleanupPreparedTransactionReservation(ctx, exec, conn, state,
		multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_COMMIT); err != nil {
		conn.SetTxnStatus(protocol.TxnStatusIdle)
		return err
	}
	conn.SetTxnStatus(protocol.TxnStatusIdle)

	if preparedResult != nil {
		return callback(ctx, preparedResult)
	}
	return nil
}

// cleanupPreparedTransactionReservation drops the multipooler's transaction
// reservation after PREPARE TRANSACTION has already driven the backend to idle.
// We intentionally reuse ConcludeTransaction with a silent callback so the
// reserved.Conn bookkeeping (transaction reason, rollback snapshot, release vs
// remaining temp/portal reasons) stays centralized. The COMMIT/ROLLBACK sent by
// that cleanup runs against an idle backend and is used only to select whether
// reserved.Conn keeps or restores its cached session-state snapshot.
func (t *TransactionPrimitive) cleanupPreparedTransactionReservation(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	conclusion multipoolerpb.TransactionConclusion,
) error {
	hasActiveTransaction := false
	for _, ss := range state.ShardStates {
		if ss.ReservedState != nil && ss.ReservedState.ReservationReasons&protoutil.ReasonTransaction != 0 {
			hasActiveTransaction = true
			break
		}
	}
	if !hasActiveTransaction {
		return nil
	}
	return exec.ConcludeTransaction(ctx, conn, state, conclusion, nil, false, false,
		func(context.Context, *sqltypes.Result) error { return nil })
}

// executeSavepoint handles SAVEPOINT by passing through to the backend, then
// pushing a frame onto the gateway's savepoint stack so SessionSettings and
// gateway-managed variables can be reverted on a later ROLLBACK TO. State is
// only mutated on backend success.
func (t *TransactionPrimitive) executeSavepoint(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	if err := exec.StreamExecute(ctx, conn, t.TableGroup, constants.DefaultShard, t.Query, nil, state, PlanExecInfo{}, callback); err != nil {
		return err
	}
	state.PushSavepoint(t.SavepointName)
	return nil
}

// executeReleaseSavepoint handles RELEASE SAVEPOINT by passing through to the
// backend, then dropping the named frame (and any nested ones) from the gateway
// stack. Per PostgreSQL semantics, current SessionSettings and variable values
// are kept — RELEASE merges sub-transaction state into the parent scope.
func (t *TransactionPrimitive) executeReleaseSavepoint(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	if err := exec.StreamExecute(ctx, conn, t.TableGroup, constants.DefaultShard, t.Query, nil, state, PlanExecInfo{}, callback); err != nil {
		return err
	}
	state.ReleaseSavepoint(t.SavepointName)
	return nil
}

// executeRollbackToSavepoint handles ROLLBACK TO SAVEPOINT by passing through
// to the backend. On success, transitions TxnStatus from Failed back to InBlock
// so subsequent statements can execute normally, and restores SessionSettings
// + gateway-managed variables from the named savepoint's snapshot. This matches
// PostgreSQL's behavior where ROLLBACK TO SAVEPOINT is the primary mechanism
// for recovering from errors within a transaction and reverts SET / RESET
// commands issued under the savepoint.
func (t *TransactionPrimitive) executeRollbackToSavepoint(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	wasFailed := conn.TxnStatus() == protocol.TxnStatusFailed

	// PG closes any cursor (including WITH HOLD) declared in the
	// rolled-back sub-transaction. Pass those names as the ROLLBACK TO
	// statement's release-portal intent so ScatterConn forwards them as
	// release_portal_names on the same RPC and the multipooler's portal pin set
	// matches what the server keeps.
	//
	// NOTE: CLOSE itself is *not* transactional in PostgreSQL — a
	// cursor explicitly CLOSE'd inside the sub-transaction stays
	// closed after ROLLBACK TO. We do not attempt to "revive" such
	// cursors; the savepoint snapshot is intersected with the current
	// open set (see RollbackToSavepoint) so the gateway's tracking
	// drops them too.
	lostHoldCursors := state.HoldCursorsDeclaredAfterSavepoint(t.SavepointName)

	// PostgreSQL reverts session GUCs (and role) set after the savepoint when it
	// rolls back to it, but the pooler's connstate cache does not observe the
	// exact reverted values. Signal the multipooler to mark the reserved
	// connection's session state untrusted so it force-reconciles before the
	// next reserved user SQL or at release, rather than trusting a stale
	// connstate pointer. Set before exec so the same RPC carries the flag.
	state.PendingMarkSessionStateUntrusted = true

	err := exec.StreamExecute(ctx, conn, t.TableGroup, constants.DefaultShard, t.Query, nil, state,
		PlanExecInfo{ReleasePortals: lostHoldCursors}, callback)
	if err != nil {
		return err
	}

	// On success, restore transaction state if we were in the aborted state.
	// The backend has rolled back to the savepoint and is now in a normal
	// in-transaction state.
	if wasFailed {
		conn.SetTxnStatus(protocol.TxnStatusInBlock)
	}

	// Revert SessionSettings, gateway-managed variables, and the
	// OpenHoldCursors set to the snapshot captured when this savepoint was
	// opened. The named frame stays on the stack — PostgreSQL leaves the
	// savepoint active after ROLLBACK TO.
	state.RollbackToSavepoint(t.SavepointName)

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
// no parameter binds; Plan routes every TransactionStmt through
// planTransactionStmt on both protocols so this primitive runs locally
// rather than being portal-forwarded. Delegate.
func (t *TransactionPrimitive) PortalStreamExecute(
	ctx context.Context,
	exec IExecute,
	conn *server.Conn,
	state *handler.MultiGatewayConnectionState,
	_ *preparedstatement.PortalInfo,
	_ int32,
	_ bool,
	info PlanExecInfo,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	return t.StreamExecute(ctx, exec, conn, state, nil, info, callback)
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
