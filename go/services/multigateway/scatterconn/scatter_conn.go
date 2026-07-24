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

// Package scatterconn handles coordinated query execution across multiple
// multipooler instances. It implements the IExecute interface from the engine
// package and is responsible for:
// - Selecting appropriate poolers for a given tablegroup
// - Executing queries via gRPC
// - Streaming results back
// - Handling failures and retries
package scatterconn

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/multigres/multigres/go/common/mterrors"
	pgClient "github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	querypb "github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/engine"
	"github.com/multigres/multigres/go/services/multigateway/handler"
	"github.com/multigres/multigres/go/services/multigateway/poolergateway"
	"github.com/multigres/multigres/go/tools/telemetry"
)

// ScatterConn coordinates query execution across multiple multipooler instances.
// It implements the engine.IExecute interface.
type ScatterConn struct {
	logger *slog.Logger

	// gateway is used for executing queries (typically a PoolerGateway)
	gateway poolergateway.Gateway

	metrics *ScatterMetrics
}

// NewScatterConn creates a new ScatterConn instance.
func NewScatterConn(gateway poolergateway.Gateway, logger *slog.Logger) *ScatterConn {
	metrics, err := NewScatterMetrics()
	if err != nil {
		logger.Warn("failed to initialise some scatter metrics", "error", err)
	}
	return &ScatterConn{
		logger:  logger,
		gateway: gateway,
		metrics: metrics,
	}
}

// userAuthFrom builds the outbound UserAuth payload from the session's captured
// SCRAM passthrough keys. Returns nil for sessions that did not authenticate via
// SCRAM (e.g. trust in tests), so this field stays absent on the wire instead of
// carrying empty slices.
//
// Keys are copied rather than referenced: Conn.Close zeroizes the accessor's
// backing slice in place, and gRPC may marshal lazily (stream init) or re-
// marshal on transient retry. A detached copy guarantees the proto carries
// live bytes for the full lifetime of the RPC.
func userAuthFrom(conn *server.Conn) *querypb.UserAuth {
	clientKey := conn.ScramClientKey()
	serverKey := conn.ScramServerKey()
	if clientKey == nil && serverKey == nil {
		return nil
	}
	return &querypb.UserAuth{
		ClientKey: append([]byte(nil), clientKey...),
		ServerKey: append([]byte(nil), serverKey...),
	}
}

func attachPostQuerySessionSettings(eo *querypb.ExecuteOptions, info engine.PlanExecInfo) {
	if eo == nil || !info.HasPostQuerySessionSettings {
		return
	}
	eo.HasPostQuerySessionSettings = true
	eo.PostQuerySessionSettings = info.PostQuerySessionSettings
}

// buildTarget constructs a routing target for the given (database,
// tableGroup, shard). The database comes from the connection's bound
// database (conn.Database()) so the gateway routes within the database
// the client authenticated to.
//
// When the connection arrived on the replica-reads port (state.TargetReplica()),
// the mode is INCONSISTENT (any replica within lag tolerance); otherwise
// WRITABLE (must hit the leader). Callers that want CONSISTENT must
// surface that explicitly — today the SQL layer doesn't distinguish
// read-your-writes from stale.
func (sc *ScatterConn) buildTarget(database, tableGroup, shard string, state *handler.MultigatewayConnectionState) *querypb.Target {
	mode := querypb.Mode_MODE_WRITABLE
	if state.TargetReplica() {
		mode = querypb.Mode_MODE_INCONSISTENT
	}
	return &querypb.Target{
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   database,
			TableGroup: tableGroup,
			Shard:      shard,
		},
		Mode: mode,
	}
}

// isCancellationError reports whether err is a query cancellation: an explicit
// cancel or statement_timeout (PostgreSQL SQLSTATE 57014, query_canceled) or a
// context cancellation/deadline. After such a cancellation the backend has
// rolled back the cancelled statement and returned to an idle, reusable state,
// so a reserved connection that hit it is still valid and must not be dropped.
func isCancellationError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var pgDiag *mterrors.PgDiagnostic
	if errors.As(err, &pgDiag) {
		return pgDiag.Code == mterrors.PgSSQueryCanceled
	}
	return false
}

// applyReservedState replaces independent bookkeeping with the authoritative reservation
// state from the multipooler. If the reserved connection ID is zero, the connection was
// destroyed or released — clear the shard state. Otherwise, update the reservation reasons.
//
// When a reserved connection is destroyed while in a transaction, the transaction is marked
// as failed (TxnStatusFailed) so subsequent queries are rejected until ROLLBACK. This is
// defense-in-depth — handler.go also sets TxnStatusFailed on query errors, but setting it
// here ensures coverage for all code paths (COPY, portal, etc.).
func (sc *ScatterConn) applyReservedState(
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	target *querypb.Target,
	rs *querypb.ReservedState,
) {
	if rs.GetReservedConnectionId() == 0 {
		state.ClearReservedConnection(target)
		if conn.TxnStatus() == protocol.TxnStatusInBlock {
			conn.SetTxnStatus(protocol.TxnStatusFailed)
		}
	} else {
		state.SetReservedConnection(target, rs)
	}
}

// StreamExecute executes a query on the specified tablegroup and streams results.
// This is the implementation of engine.IExecute.StreamExecute().
//
// Implements 3-case reservation logic for transactions:
//   - Case 1: Has reserved connection → use it
//   - Case 2: In transaction, no reserved conn → call StreamExecute with reservation options
//   - Case 3: Not in transaction → use regular pooled connection
//
// If executeSQLPreparedStatement is non-nil, it is attached to the
// ExecuteOptions so the multipooler can resolve the prepared statement through
// pooler-level consolidation and materialize the SQL EXECUTE wrapper before
// running the query.
// wantPassthroughRow reports whether this statement should use opaque row passthrough.
// Opaque is the default: the multipooler returns rows as raw DataRow blocks the
// multigateway writes straight to the client, skipping per-row marshalling and
// re-framing. A route opts out via keepStructured (Route.KeepStructured) when
// the gateway consumes the result rows itself (for example resolve_set_config)
// rather than streaming them to the client.
func wantPassthroughRow(keepStructured bool) bool {
	return !keepStructured
}

func (sc *ScatterConn) StreamExecute(
	ctx context.Context,
	conn *server.Conn,
	tableGroup string,
	shard string,
	sql string,
	executeSQLPreparedStatement *querypb.ExecuteSqlPreparedStatement,
	state *handler.MultigatewayConnectionState,
	info engine.PlanExecInfo,
	keepStructured bool,
	callback func(context.Context, *sqltypes.Result) error,
) (retErr error) {
	ctx, span := telemetry.Tracer().Start(ctx, "shard.execute",
		trace.WithAttributes(
			attribute.String("tablegroup", tableGroup),
			attribute.String("shard", shard),
			attribute.String("db.namespace", conn.Database()),
		),
	)
	defer span.End()
	start := time.Now()
	defer sc.endAction(ctx, span, start, conn.Database(), tableGroup, shard, &retErr)

	sc.logger.DebugContext(ctx, "scatter conn executing query",
		"tablegroup", tableGroup,
		"shard", shard,
		"query", sql,
		"user", conn.User(),
		"database", conn.Database(),
		"connection_id", conn.ConnectionID(),
		"in_transaction", conn.IsInTransaction())

	target := sc.buildTarget(conn.Database(), tableGroup, shard, state)

	eo := &querypb.ExecuteOptions{
		UserAuth:                    userAuthFrom(conn),
		User:                        conn.User(),
		ClientConnectionId:          conn.ConnectionID(),
		SessionSettings:             state.GetSessionSettings(),
		ExecuteSqlPreparedStatement: executeSQLPreparedStatement,
		PassthroughRow:              wantPassthroughRow(keepStructured),
	}
	attachPostQuerySessionSettings(eo, info)

	ss := state.GetMatchingShardState(target)

	// This statement may touch a session-level advisory lock (acquire or
	// release). When it does, ask the multipooler to re-probe pg_locks afterward
	// and unpin if none remain — keeping that probe off the per-statement hot
	// path. Attached to whichever reservation path runs below (Case 3 has no
	// reserved connection, so there's nothing to recheck).
	recheckAdvisory := info.RecheckAdvisoryLocks

	// One-shot: a successful ROLLBACK TO SAVEPOINT reverted session GUCs on the
	// backend without the pooler observing the exact values. Forward it so the
	// multipooler marks the reserved connection's session state untrusted.
	markUntrusted := state.PendingMarkSessionStateUntrusted
	state.PendingMarkSessionStateUntrusted = false

	// Case 1: Already have reserved connection - use it
	if ss != nil && ss.ReservedState.GetReservedConnectionId() != 0 {
		sc.logger.DebugContext(ctx, "using existing reserved connection",
			"reserved_conn_id", ss.ReservedState.GetReservedConnectionId())

		eo.ReservedConnectionId = ss.ReservedState.GetReservedConnectionId()
		qs, err := sc.gateway.QueryServiceByID(ctx, ss.ReservedState.GetPoolerId(), target)
		if err != nil {
			return err
		}

		// Build reservation options for any reasons we need to add to the
		// existing reserved connection.
		var reservationOpts *querypb.ReservationOptions

		// For any already-reserved backend and a deferred BEGIN, promote the
		// reservation to a transaction before running the user's first statement.
		// Temp-table reservations were the original case, but session advisory locks
		// and holdable portals also pin a backend; once the client sends BEGIN, the
		// transaction must start on that same reserved backend.
		if state.PendingBeginQuery != "" && !protoutil.HasTransactionReason(ss.ReservedState.GetReservationReasons()) {
			sc.logger.DebugContext(ctx, "adding deferred BEGIN via reservation options",
				"pending_begin", state.PendingBeginQuery,
				"existing_reasons", protoutil.ReasonsString(ss.ReservedState.GetReservationReasons()))
			reservationOpts = &querypb.ReservationOptions{
				Reasons:    protoutil.ReasonTransaction,
				BeginQuery: state.PendingBeginQuery,
			}
			state.PendingBeginQuery = ""
		}

		// If this query creates a temp table, add the reason so the
		// multipooler tracks it on the reserved connection.
		if info.TempTable {
			if reservationOpts == nil {
				reservationOpts = &querypb.ReservationOptions{}
			}
			reservationOpts.Reasons |= protoutil.ReasonTempTable
		}

		// If this query acquires a session-level advisory lock, add the reason
		// so the multipooler keeps the backend pinned until the lock is
		// released. Promotes an existing reservation (e.g. an open transaction)
		// to also hold the advisory-lock reason.
		if info.AdvisoryLock {
			if reservationOpts == nil {
				reservationOpts = &querypb.ReservationOptions{}
			}
			reservationOpts.Reasons |= protoutil.ReasonSessionAdvisoryLock
		}

		// If this query declares a `WITH HOLD` cursor, pin the cursor name on
		// the reserved backend so the cursor survives COMMIT.
		if pinNames := info.PinPortals; len(pinNames) > 0 {
			if reservationOpts == nil {
				reservationOpts = &querypb.ReservationOptions{}
			}
			reservationOpts.Reasons |= protoutil.ReasonPortal
			reservationOpts.PinPortalNames = append(reservationOpts.PinPortalNames, pinNames...)
		}

		// If this query closes a `WITH HOLD` cursor, unpin it after the CLOSE
		// runs on the backend. The multipooler will drop the reservation
		// (returning ReservedConnectionId=0) when the last reason clears.
		if releaseNames := info.ReleasePortals; len(releaseNames) > 0 {
			if reservationOpts == nil {
				reservationOpts = &querypb.ReservationOptions{}
			}
			reservationOpts.ReleasePortalNames = append(reservationOpts.ReleasePortalNames, releaseNames...)
		}

		// If this statement touched an advisory lock, ask for a post-statement
		// pg_locks recheck so the multipooler unpins once the last lock is gone.
		if recheckAdvisory {
			if reservationOpts == nil {
				reservationOpts = &querypb.ReservationOptions{}
			}
			reservationOpts.RecheckAdvisoryLocks = true
		}

		// ROLLBACK TO SAVEPOINT reverted session state invisibly to the pooler.
		if markUntrusted {
			if reservationOpts == nil {
				reservationOpts = &querypb.ReservationOptions{}
			}
			reservationOpts.MarkSessionStateUntrusted = true
		}

		reservedState, err := qs.StreamExecute(ctx, target, sql, eo, reservationOpts, callback)

		// A query error on an existing reserved connection does not, by itself, mean
		// the reserved backend is gone. The pooler returns a non-zero reserved state
		// whenever it can still vouch for the connection (the common error case). But
		// the gateway enforces statement_timeout with a context deadline, and when
		// that fires mid-stream the RPC can return before the reserved-state trailer
		// arrives, yielding a zero reserved state even though the backend — and the
		// session's temp tables — are still alive. Clearing the reservation here would
		// route the next statement to a different pooled backend and lose those temp
		// tables (the PostGIS interrupt tests' CREATE TEMP TABLE ... AS + statement
		// timeout reproduce this).
		//
		// Keep the existing reservation only when all of these hold: the query failed,
		// no fresh reserved state came back, we are NOT inside an explicit transaction,
		// and the failure was a cancellation (statement_timeout / context cancel, after
		// which the backend has returned to an idle, reusable state). Every other case
		// — all in-transaction behaviour, genuine connection loss, and ordinary query
		// errors — is handled exactly as before via applyReservedState.
		keepReservation := err != nil &&
			reservedState.GetReservedConnectionId() == 0 &&
			conn.TxnStatus() != protocol.TxnStatusInBlock &&
			isCancellationError(err)
		if !keepReservation {
			sc.applyReservedState(conn, state, target, reservedState)
		}

		if err != nil {
			return fmt.Errorf("query execution failed: %w", err)
		}
		return nil
	}

	// Case 2: Need a new reserved connection — for transaction, temp table,
	// portal pin (DECLARE WITH HOLD), or any combination.
	pinPortalNames := info.PinPortals
	if conn.IsInTransaction() || info.TempTable || info.AdvisoryLock || len(pinPortalNames) > 0 {
		reasons := uint32(0)
		if conn.IsInTransaction() {
			reasons |= protoutil.ReasonTransaction
		}
		if info.TempTable {
			reasons |= protoutil.ReasonTempTable
		}
		// If the session already has a temp table reservation on another shard,
		// include the temp table reason so the connection survives COMMIT.
		if state.HasTempTableReservation() {
			reasons |= protoutil.ReasonTempTable
		}
		if info.AdvisoryLock {
			reasons |= protoutil.ReasonSessionAdvisoryLock
		}
		if len(pinPortalNames) > 0 {
			reasons |= protoutil.ReasonPortal
		}

		sc.logger.DebugContext(ctx, "creating reserved connection",
			"reasons", protoutil.ReasonsString(reasons))

		reservationOpts := &querypb.ReservationOptions{Reasons: reasons}
		if len(pinPortalNames) > 0 {
			reservationOpts.PinPortalNames = pinPortalNames
		}
		// Pass the original BEGIN query (e.g., "BEGIN ISOLATION LEVEL SERIALIZABLE")
		// so the multipooler preserves transaction options instead of using plain "BEGIN".
		if state.PendingBeginQuery != "" {
			reservationOpts.BeginQuery = state.PendingBeginQuery
			state.PendingBeginQuery = ""
		}
		// A new reservation created by an advisory acquire also wants a recheck
		// (so a failed pg_try_advisory_lock unpins immediately).
		reservationOpts.RecheckAdvisoryLocks = recheckAdvisory
		reservedState, err := sc.gateway.StreamExecute(ctx, target, sql, eo, reservationOpts, callback)
		if err != nil {
			// The multipooler can return a surviving ReservedState when the first
			// statement in an explicit transaction fails with a PostgreSQL error. Keep
			// tracking it so ROLLBACK is routed to the backend that is now in failed
			// transaction state.
			if reservedState.GetReservedConnectionId() != 0 {
				sc.applyReservedState(conn, state, target, reservedState)
			}
			return fmt.Errorf("query execution failed: %w", err)
		}

		sc.applyReservedState(conn, state, target, reservedState)

		sc.logger.DebugContext(ctx, "reserved connection created",
			"reserved_conn_id", reservedState.GetReservedConnectionId())
		return nil
	}

	// Case 3: Not in transaction, no temp table — use regular pooled connection
	sc.logger.DebugContext(ctx, "executing query via regular pooled connection",
		"tablegroup", tableGroup,
		"shard", shard,
		"mode", target.GetMode().String())

	if _, err := sc.gateway.StreamExecute(ctx, target, sql, eo, nil, callback); err != nil {
		// If it's a PostgreSQL error, don't wrap it - pass through unchanged
		var pgDiag *mterrors.PgDiagnostic
		if errors.As(err, &pgDiag) {
			return err
		}
		return fmt.Errorf("query execution failed: %w", err)
	}

	sc.logger.DebugContext(ctx, "query execution completed successfully",
		"tablegroup", tableGroup,
		"shard", shard)

	return nil
}

// PortalStreamExecute executes a portal (bound prepared statement) and streams results.
// This is the implementation of engine.IExecute.PortalStreamExecute().
func (sc *ScatterConn) PortalStreamExecute(
	ctx context.Context,
	tableGroup string,
	shard string,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	portalInfo *preparedstatement.PortalInfo,
	maxRows int32,
	includeDescribe bool,
	info engine.PlanExecInfo,
	keepStructured bool,
	callback func(context.Context, *sqltypes.Result) error,
) (retErr error) {
	ctx, span := telemetry.Tracer().Start(ctx, "shard.execute",
		trace.WithAttributes(
			attribute.String("tablegroup", tableGroup),
			attribute.String("shard", shard),
			attribute.String("db.namespace", conn.Database()),
		),
	)
	defer span.End()
	start := time.Now()
	defer sc.endAction(ctx, span, start, conn.Database(), tableGroup, shard, &retErr)

	sc.logger.DebugContext(ctx, "scatter conn executing portal",
		"tablegroup", tableGroup,
		"shard", shard,
		"portal", portalInfo.Portal.Name,
		"max_rows", maxRows,
		"user", conn.User(),
		"database", conn.Database(),
		"connection_id", conn.ConnectionID())

	// Create target for routing
	target := sc.buildTarget(conn.Database(), tableGroup, shard, state)

	eo := &querypb.ExecuteOptions{
		UserAuth:           userAuthFrom(conn),
		User:               conn.User(),
		ClientConnectionId: conn.ConnectionID(),
		MaxRows:            uint64(maxRows),
		SessionSettings:    state.GetSessionSettings(),
		PassthroughRow:     wantPassthroughRow(keepStructured),
	}
	attachPostQuerySessionSettings(eo, info)

	// When the protocol layer folded a Describe('P') into this Execute, ask
	// the multipooler to fuse Bind+Describe(P)+Execute+Sync into one
	// backend round trip. The portal RowDescription rides back through
	// the streaming callback's Fields on the first chunk; pgwire-server's
	// handleExecute writes it to the wire before any DataRow.
	var portalOpts *multipoolerpb.PortalExecuteOptions
	if includeDescribe {
		portalOpts = &multipoolerpb.PortalExecuteOptions{IncludeDescribe: true}
	}

	var qs queryservice.QueryService = sc.gateway
	var err error

	// reservationOpts carries the reservation reasons for this portal. When the
	// portal needs a fresh reservation (Case 2), the multipooler reserves a
	// backend with these reasons and runs the portal on it atomically — no
	// separate no-op "SELECT 1" reserve round trip.
	var reservationOpts *querypb.ReservationOptions

	// Advisory recheck signal — attached to reservationOpts after the
	// reservation path below is chosen (see the end of the if/else).
	recheckAdvisory := info.RecheckAdvisoryLocks

	ss := state.GetMatchingShardState(target)
	// If we have a reserved connection, we have to ensure
	// we are routing the query to the pooler where we got the reserved
	// connection from. If a reparent happened, then we will get an error
	// back.
	if ss != nil && ss.ReservedState.GetReservedConnectionId() != 0 {
		eo.ReservedConnectionId = ss.ReservedState.GetReservedConnectionId()
		qs, err = sc.gateway.QueryServiceByID(ctx, ss.ReservedState.GetPoolerId(), target)

		// If this portal acquires a session-level advisory lock, OR the reason
		// onto the existing reservation so the lock keeps the backend pinned and
		// survives the other reason ending (e.g. a COMMIT). The multipooler
		// applies the reason atomically, before running the portal, so unlike a
		// separate promotion step there's no window for the unpin probe to see
		// no lock and tear the reservation down — see portalExecuteWithReserved.
		if info.AdvisoryLock {
			reservationOpts = &querypb.ReservationOptions{Reasons: protoutil.ReasonSessionAdvisoryLock}
		}
	} else if conn.IsInTransaction() || info.TempTable || info.AdvisoryLock {
		// Case 2: Need a new reserved connection — for transaction, temp table,
		// advisory lock, or a combination. Build reservation options the same way
		// the simple StreamExecute path does and pass them on the portal RPC; the
		// multipooler reserves-and-runs atomically.
		reasons := uint32(0)
		if conn.IsInTransaction() {
			reasons |= protoutil.ReasonTransaction
		}
		if info.TempTable {
			reasons |= protoutil.ReasonTempTable
		}
		if state.HasTempTableReservation() {
			reasons |= protoutil.ReasonTempTable
		}
		if info.AdvisoryLock {
			reasons |= protoutil.ReasonSessionAdvisoryLock
		}

		sc.logger.DebugContext(ctx, "reserving connection for portal via reservation options",
			"reasons", protoutil.ReasonsString(reasons))

		reservationOpts = &querypb.ReservationOptions{Reasons: reasons}
		// Pass the original BEGIN query (e.g., "BEGIN ISOLATION LEVEL SERIALIZABLE")
		// so the multipooler preserves transaction options instead of plain "BEGIN".
		if state.PendingBeginQuery != "" {
			reservationOpts.BeginQuery = state.PendingBeginQuery
			state.PendingBeginQuery = ""
		}
	}

	// If this portal touched an advisory lock, ask for a post-statement
	// pg_locks recheck (covers both the existing-reservation and new-reservation
	// paths above). A bare release on an already-pinned session needs options
	// allocated just to carry the flag.
	if recheckAdvisory {
		if reservationOpts == nil {
			reservationOpts = &querypb.ReservationOptions{}
		}
		reservationOpts.RecheckAdvisoryLocks = true
	}

	if err != nil {
		return err
	}

	// Execute portal via QueryService (PoolerGateway) and stream results
	sc.logger.DebugContext(ctx, "executing portal via query service",
		"tablegroup", tableGroup,
		"shard", shard,
		"portal", portalInfo.Portal.Name,
		"mode", target.GetMode().String())

	// Use the query from the prepared statement
	reservedState, err := qs.PortalStreamExecute(ctx, target, portalInfo.PreparedStatementInfo.PreparedStatement, portalInfo.Portal, eo, portalOpts, reservationOpts, callback)
	if err != nil {
		// PostgreSQL-level portal errors can leave an existing reserved backend
		// alive (for example an explicit transaction that is now aborted). Apply any
		// state the multipooler managed to send before the gRPC error so gateway
		// cleanup/ROLLBACK stays routed to the same backend.
		keepReservation := reservedState.GetReservedConnectionId() == 0 &&
			conn.TxnStatus() != protocol.TxnStatusInBlock &&
			isCancellationError(err)
		if !keepReservation {
			sc.applyReservedState(conn, state, target, reservedState)
		}

		// If it's a PostgreSQL error, don't wrap it - pass through unchanged
		var pgDiag *mterrors.PgDiagnostic
		if errors.As(err, &pgDiag) {
			return err
		}
		return fmt.Errorf("portal execution failed: %w", err)
	}
	// Use authoritative state from multipooler. The multipooler already OR'd in
	// the portal reason (if suspended) or removed it (if completed). If no reasons
	// remain (ReservedConnectionId == 0) the connection was released.
	sc.applyReservedState(conn, state, target, reservedState)

	sc.logger.DebugContext(ctx, "portal execution completed successfully",
		"tablegroup", tableGroup,
		"shard", shard,
		"portal", portalInfo.Portal.Name,
		"reserved_connection_id", reservedState.GetReservedConnectionId())

	return nil
}

// Describe returns metadata about a prepared statement or portal.
// This is the implementation of engine.IExecute.Describe().
func (sc *ScatterConn) Describe(
	ctx context.Context,
	tableGroup string,
	shard string,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	portalInfo *preparedstatement.PortalInfo,
	preparedStatementInfo *preparedstatement.PreparedStatementInfo,
) (*querypb.StatementDescription, error) {
	sc.logger.DebugContext(ctx, "scatter conn describing",
		"tablegroup", tableGroup,
		"shard", shard,
		"user", conn.User(),
		"database", conn.Database(),
		"connection_id", conn.ConnectionID())

	// Create target for routing
	target := sc.buildTarget(conn.Database(), tableGroup, shard, state)

	eo := &querypb.ExecuteOptions{
		UserAuth:           userAuthFrom(conn),
		User:               conn.User(),
		ClientConnectionId: conn.ConnectionID(),
		SessionSettings:    state.GetSessionSettings(),
	}
	var preparedStatement *querypb.PreparedStatement
	var portal *querypb.Portal
	if portalInfo != nil {
		preparedStatement = portalInfo.PreparedStatementInfo.PreparedStatement
		portal = portalInfo.Portal
	} else if preparedStatementInfo != nil {
		preparedStatement = preparedStatementInfo.PreparedStatement
	}

	var qs queryservice.QueryService = sc.gateway
	var err error

	ss := state.GetMatchingShardState(target)
	// If we have a reserved connection, use it
	if ss != nil && ss.ReservedState.GetReservedConnectionId() != 0 {
		eo.ReservedConnectionId = ss.ReservedState.GetReservedConnectionId()
		qs, err = sc.gateway.QueryServiceByID(ctx, ss.ReservedState.GetPoolerId(), target)
	}
	if err != nil {
		return nil, err
	}

	// Call Describe on the query service
	sc.logger.DebugContext(ctx, "describing via query service",
		"tablegroup", tableGroup,
		"shard", shard,
		"mode", target.GetMode().String())

	description, err := qs.Describe(ctx, target, preparedStatement, portal, eo)
	if err != nil {
		// If it's a PostgreSQL error, don't wrap it - pass through unchanged
		var pgDiag *mterrors.PgDiagnostic
		if errors.As(err, &pgDiag) {
			return nil, err
		}
		return nil, fmt.Errorf("describe failed: %w", err)
	}

	sc.logger.DebugContext(ctx, "describe completed successfully",
		"tablegroup", tableGroup,
		"shard", shard)

	return description, nil
}

// ConcludeTransaction concludes a transaction on reserved connections that have
// the transaction reason set. Shards reserved for other reasons only (e.g., temp
// tables, portals) are left untouched. Based on the returned remainingReasons from
// the multipooler, shard state entries are either cleared (connection fully released)
// or updated with the new reason bitmask.
func (sc *ScatterConn) ConcludeTransaction(
	ctx context.Context,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	conclusion multipoolerpb.TransactionConclusion,
	releasePortalNames []string,
	releaseAllPortals bool,
	chain bool,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	ctx, span := telemetry.Tracer().Start(ctx, "shard.conclude_transaction",
		trace.WithAttributes(
			attribute.String("db.namespace", conn.Database()),
		),
	)
	defer span.End()

	// Collect shard state updates to apply after iteration.
	// We cannot call ClearReservedConnection during iteration because it
	// uses swap-and-truncate which mutates the underlying slice.
	type shardUpdate struct {
		target        *querypb.Target
		clear         bool                   // true = remove entry, false = set state
		reservedState *querypb.ReservedState // only used when clear == false
	}
	var updates []shardUpdate
	var errs []error
	var callbackResult *sqltypes.Result

	// Count shards with a transaction reason — multi-shard transactions are not
	// yet supported (distributed transactions). Log a warning as a sentinel so
	// unexpected multi-shard cases are visible before DT is implemented.
	var txnShardCount int
	for _, ss := range state.ShardStates {
		if ss.ReservedState.GetReservedConnectionId() != 0 && protoutil.HasTransactionReason(ss.ReservedState.GetReservationReasons()) {
			txnShardCount++
		}
	}
	if txnShardCount > 1 {
		sc.logger.WarnContext(ctx, "multi-shard transaction detected — distributed transactions not yet supported",
			"shard_count", txnShardCount)
	}

	// Iterate over all shard states with reserved connections.
	// Only conclude on shards that have the transaction reason.
	// Continue on errors so all shards get concluded.
	for _, ss := range state.ShardStates {
		if ss.ReservedState.GetReservedConnectionId() == 0 {
			continue
		}
		if !protoutil.HasTransactionReason(ss.ReservedState.GetReservationReasons()) {
			continue
		}

		eo := &querypb.ExecuteOptions{
			UserAuth:             userAuthFrom(conn),
			User:                 conn.User(),
			ClientConnectionId:   conn.ConnectionID(),
			SessionSettings:      state.GetSessionSettings(),
			ReservedConnectionId: ss.ReservedState.GetReservedConnectionId(),
		}

		qs, err := sc.gateway.QueryServiceByID(ctx, ss.ReservedState.GetPoolerId(), ss.Target)
		if err != nil {
			// Connection lost — mark for clearing, continue to other shards
			updates = append(updates, shardUpdate{target: ss.Target, clear: true})
			errs = append(errs, fmt.Errorf("conclude transaction: pooler lookup failed for %s: %w", ss.Target, err))
			continue
		}

		result, reservedState, err := qs.ConcludeTransaction(ctx, ss.Target, eo, conclusion, releasePortalNames, releaseAllPortals, chain)
		if err != nil {
			if reservedState.GetReservedConnectionId() != 0 {
				// The multipooler reported the backend is still healthy and
				// reserved for a surviving non-transaction reason (e.g. a temp
				// table) even though the conclusion itself failed (e.g. COMMIT
				// hit a deferred constraint violation). Keep tracking it
				// instead of assuming the whole reservation is gone.
				updates = append(updates, shardUpdate{target: ss.Target, reservedState: reservedState})
			} else {
				updates = append(updates, shardUpdate{target: ss.Target, clear: true})
			}
			// Plain ROLLBACK on a destroyed connection is graceful recovery — don't
			// propagate error. ROLLBACK AND CHAIN is different: PostgreSQL promises a
			// new transaction on the same backend, so losing that backend must fail
			// closed rather than silently moving the chained transaction elsewhere.
			if conclusion == multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_ROLLBACK && !chain {
				callbackResult = &sqltypes.Result{CommandTag: "ROLLBACK"}
				continue
			}
			errs = append(errs, fmt.Errorf("conclude transaction failed for %s: %w", ss.Target, err))
			continue
		}

		if reservedState.GetReservedConnectionId() == 0 {
			// Connection fully released by multipooler
			updates = append(updates, shardUpdate{target: ss.Target, clear: true})
		} else {
			// Connection still reserved for other reasons — update our local tracking
			updates = append(updates, shardUpdate{target: ss.Target, reservedState: reservedState})
		}

		// Keep the last successful result for the callback
		callbackResult = result
	}

	// Apply collected updates outside the iteration loop.
	for _, u := range updates {
		if u.clear {
			state.ClearReservedConnection(u.target)
		} else {
			state.SetReservedConnection(u.target, u.reservedState)
		}
	}

	if len(errs) > 0 {
		// Return only the first error. PostgreSQL clients expect a single ErrorResponse
		// with a SQLSTATE — a joined multi-line error confuses ORMs and connection poolers.
		if len(errs) > 1 {
			sc.logger.ErrorContext(ctx, "multiple shard errors during conclude transaction",
				"error_count", len(errs),
				"errors", errors.Join(errs...))
		}
		return errs[0]
	}

	// Send the result to the client (COMMIT/ROLLBACK command tag)
	if callbackResult != nil {
		if err := callback(ctx, callbackResult); err != nil {
			return err
		}
	}

	return nil
}

// DiscardTempTables sends DISCARD TEMP on reserved connections that have the
// temp table reason set. Based on the returned state from the multipooler,
// shard state entries are either cleared (connection fully released) or updated.
func (sc *ScatterConn) DiscardTempTables(
	ctx context.Context,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	ctx, span := telemetry.Tracer().Start(ctx, "shard.discard_temp_tables",
		trace.WithAttributes(
			attribute.String("db.namespace", conn.Database()),
		),
	)
	defer span.End()

	// Collect shard state updates to apply after iteration.
	type shardUpdate struct {
		target        *querypb.Target
		clear         bool
		reservedState *querypb.ReservedState
	}
	var updates []shardUpdate
	var errs []error
	var callbackResult *sqltypes.Result

	// Iterate over all shard states with reserved connections.
	// Only discard on shards that have the temp table reason.
	for _, ss := range state.ShardStates {
		if ss.ReservedState.GetReservedConnectionId() == 0 {
			continue
		}
		if !protoutil.HasTempTableReason(ss.ReservedState.GetReservationReasons()) {
			continue
		}

		eo := &querypb.ExecuteOptions{
			UserAuth:             userAuthFrom(conn),
			User:                 conn.User(),
			ClientConnectionId:   conn.ConnectionID(),
			SessionSettings:      state.GetSessionSettings(),
			ReservedConnectionId: ss.ReservedState.GetReservedConnectionId(),
		}

		qs, err := sc.gateway.QueryServiceByID(ctx, ss.ReservedState.GetPoolerId(), ss.Target)
		if err != nil {
			updates = append(updates, shardUpdate{target: ss.Target, clear: true})
			errs = append(errs, fmt.Errorf("discard temp tables: pooler lookup failed for %s: %w", ss.Target, err))
			continue
		}

		result, reservedState, err := qs.DiscardTempTables(ctx, ss.Target, eo)
		if err != nil {
			updates = append(updates, shardUpdate{target: ss.Target, clear: true})
			errs = append(errs, fmt.Errorf("discard temp tables failed for %s: %w", ss.Target, err))
			continue
		}

		if reservedState.GetReservedConnectionId() == 0 {
			// Connection fully released by multipooler
			updates = append(updates, shardUpdate{target: ss.Target, clear: true})
		} else {
			// Connection still reserved for other reasons — update our local tracking
			updates = append(updates, shardUpdate{target: ss.Target, reservedState: reservedState})
		}

		// Keep the last successful result for the callback
		callbackResult = result
	}

	// Apply collected updates outside the iteration loop.
	for _, u := range updates {
		if u.clear {
			state.ClearReservedConnection(u.target)
		} else {
			state.SetReservedConnection(u.target, u.reservedState)
		}
	}

	if len(errs) > 0 {
		if len(errs) > 1 {
			sc.logger.ErrorContext(ctx, "multiple shard errors during discard temp tables",
				"error_count", len(errs),
				"errors", errors.Join(errs...))
		}
		return errs[0]
	}

	// Send the result to the client
	if callbackResult != nil {
		if err := callback(ctx, callbackResult); err != nil {
			return err
		}
	}

	return nil
}

// --- COPY FROM STDIN methods ---

// CopyOutInitiate initiates a COPY ... TO STDOUT operation using
// bidirectional streaming. Stores reserved connection info in
// state.ShardStates for the given tableGroup/shard. Returns format,
// columnFormats, and any NoticeResponse diagnostics that arrived before
// the CopyOutResponse (e.g. BEFORE STATEMENT trigger output). The caller
// then drives the data stream with CopyOutStream.
func (sc *ScatterConn) CopyOutInitiate(
	ctx context.Context,
	conn *server.Conn,
	tableGroup string,
	shard string,
	queryStr string,
	state *handler.MultigatewayConnectionState,
) (int16, []int16, []*mterrors.PgDiagnostic, error) {
	ctx, span := telemetry.Tracer().Start(ctx, "shard.copy_out_initiate",
		trace.WithAttributes(
			attribute.String("tablegroup", tableGroup),
			attribute.String("shard", shard),
			attribute.String("db.namespace", conn.Database()),
		),
	)
	defer span.End()

	target := &querypb.Target{
		ShardKey: &clustermetadatapb.ShardKey{Database: conn.Database(), TableGroup: tableGroup, Shard: shard},
		Mode:     querypb.Mode_MODE_WRITABLE,
	}

	execOptions := &querypb.ExecuteOptions{
		UserAuth:           userAuthFrom(conn),
		User:               conn.User(),
		ClientConnectionId: conn.ConnectionID(),
		SessionSettings:    state.GetSessionSettings(),
	}

	// Reuse an existing reserved connection (e.g. one already held by a
	// transaction or temp-table reservation) so the COPY runs on the same
	// backend that owns the in-flight session state. Otherwise pass deferred
	// BEGIN options so the new connection enters the transaction before
	// starting COPY. Mirrors CopyInitiate (FROM STDIN).
	var reservationOpts *querypb.ReservationOptions
	ss := state.GetMatchingShardState(target)
	if ss != nil && ss.ReservedState.GetReservedConnectionId() != 0 {
		execOptions.ReservedConnectionId = ss.ReservedState.GetReservedConnectionId()
	} else if conn.IsInTransaction() {
		reservationOpts = protoutil.NewTransactionReservationOptions()
		if state.HasTempTableReservation() {
			reservationOpts.Reasons |= protoutil.ReasonTempTable
		}
		if state.PendingBeginQuery != "" {
			reservationOpts.BeginQuery = state.PendingBeginQuery
			state.PendingBeginQuery = ""
		}
	}

	format, columnFormats, notices, reservedState, err := sc.gateway.CopyOutReady(ctx, target, queryStr, execOptions, reservationOpts)
	if err != nil {
		sc.applyReservedState(conn, state, target, reservedState)
		// Surface the PG error un-wrapped — see the comment on CopyInitiate
		// below for the rationale.
		return 0, nil, notices, err
	}

	sc.applyReservedState(conn, state, target, reservedState)
	return format, columnFormats, notices, nil
}

// CopyOutStream drives the COPY ... TO STDOUT data stream, invoking
// onMessage for each CopyData chunk / NoticeResponse pumped by the
// multipooler. Returns the final Result (CommandTag, RowsAffected, plus any
// trailing notices in result.Notices) and applies the authoritative
// ReservedState to shard tracking on completion. PG ErrorResponse mid-stream
// surfaces un-wrapped so the gateway re-emits a verbatim ErrorResponse.
func (sc *ScatterConn) CopyOutStream(
	ctx context.Context,
	conn *server.Conn,
	tableGroup string,
	shard string,
	state *handler.MultigatewayConnectionState,
	onMessage func(pgClient.CopyOutMessage) error,
) (*sqltypes.Result, error) {
	ctx, span := telemetry.Tracer().Start(ctx, "shard.copy_out_stream",
		trace.WithAttributes(
			attribute.String("tablegroup", tableGroup),
			attribute.String("shard", shard),
			attribute.String("db.namespace", conn.Database()),
		),
	)
	defer span.End()

	target := &querypb.Target{
		ShardKey: &clustermetadatapb.ShardKey{Database: conn.Database(), TableGroup: tableGroup, Shard: shard},
		Mode:     querypb.Mode_MODE_WRITABLE,
	}

	ss := state.GetMatchingShardState(target)
	if ss == nil || ss.ReservedState.GetReservedConnectionId() == 0 {
		return nil, errors.New("no active COPY connection")
	}

	copyOptions := &querypb.ExecuteOptions{
		UserAuth:             userAuthFrom(conn),
		User:                 conn.User(),
		ClientConnectionId:   conn.ConnectionID(),
		SessionSettings:      state.GetSessionSettings(),
		ReservedConnectionId: ss.ReservedState.GetReservedConnectionId(),
	}

	result, reservedState, err := sc.gateway.CopyOutStream(ctx, target, copyOptions, onMessage)
	sc.applyReservedState(conn, state, target, reservedState)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// StreamReplication opens a replication tunnel to the PRIMARY pooler for the
// given tablegroup/shard. It fills the init's Target from buildTarget (the
// gateway forces PoolerType=PRIMARY regardless) and forwards to the gateway,
// which performs the Init/Ready handshake and returns the live bidi stream.
//
// The User and UserAuth fields on init are populated by the caller before the
// client socket is detached (DetachConn zeroizes the SCRAM keys), so they are
// not touched here.
func (sc *ScatterConn) StreamReplication(
	ctx context.Context,
	conn *server.Conn,
	tableGroup string,
	shard string,
	state *handler.MultigatewayConnectionState,
	init *multipoolerpb.StreamReplicationInit,
) (multipoolerpb.MultipoolerService_StreamReplicationClient, error) {
	ctx, span := telemetry.Tracer().Start(ctx, "shard.stream_replication",
		trace.WithAttributes(
			attribute.String("tablegroup", tableGroup),
			attribute.String("shard", shard),
			attribute.String("db.namespace", conn.Database()),
		),
	)
	defer span.End()

	init.Target = sc.buildTarget(conn.Database(), tableGroup, shard, state)
	return sc.gateway.StreamReplication(ctx, init)
}

// CopyInitiate initiates a COPY FROM STDIN operation using bidirectional streaming.
// Stores reserved connection info in state.ShardStates for the given tableGroup/shard.
// Returns: format, columnFormats, error
func (sc *ScatterConn) CopyInitiate(
	ctx context.Context,
	conn *server.Conn,
	tableGroup string,
	shard string,
	queryStr string,
	state *handler.MultigatewayConnectionState,
	callback func(ctx context.Context, result *sqltypes.Result) error,
) (int16, []int16, error) {
	ctx, span := telemetry.Tracer().Start(ctx, "shard.copy_initiate",
		trace.WithAttributes(
			attribute.String("tablegroup", tableGroup),
			attribute.String("shard", shard),
			attribute.String("db.namespace", conn.Database()),
		),
	)
	defer span.End()

	sc.logger.DebugContext(ctx, "initiating COPY FROM STDIN",
		"query", queryStr,
		"tablegroup", tableGroup,
		"shard", shard,
		"user", conn.User(),
		"database", conn.Database())

	// Create target for routing - COPY always goes to PRIMARY
	target := &querypb.Target{
		ShardKey: &clustermetadatapb.ShardKey{Database: conn.Database(), TableGroup: tableGroup, Shard: shard},
		Mode:     querypb.Mode_MODE_WRITABLE,
	}

	// Create execute options
	execOptions := &querypb.ExecuteOptions{
		UserAuth:           userAuthFrom(conn),
		User:               conn.User(),
		ClientConnectionId: conn.ConnectionID(),
		SessionSettings:    state.GetSessionSettings(),
	}

	// If there's already a reserved connection for this target (e.g., in a transaction),
	// pass its ID so CopyReady reuses it instead of creating a new one.
	// If we're in a transaction but no reserved connection exists yet (deferred BEGIN),
	// pass ReservationOptions so CopyReady creates a connection with the pending BEGIN.
	var reservationOpts *querypb.ReservationOptions
	ss := state.GetMatchingShardState(target)
	if ss != nil && ss.ReservedState.GetReservedConnectionId() != 0 {
		execOptions.ReservedConnectionId = ss.ReservedState.GetReservedConnectionId()
	} else if conn.IsInTransaction() {
		// Deferred BEGIN: pass transaction reservation options so the executor
		// executes BEGIN on the new connection before initiating COPY.
		reservationOpts = protoutil.NewTransactionReservationOptions()
		if state.HasTempTableReservation() {
			reservationOpts.Reasons |= protoutil.ReasonTempTable
		}
		if state.PendingBeginQuery != "" {
			reservationOpts.BeginQuery = state.PendingBeginQuery
			state.PendingBeginQuery = ""
		}
	}

	// Call CopyReady on gateway to initiate the COPY and get format info
	format, columnFormats, reservedState, err := sc.gateway.CopyReady(ctx, target, queryStr, execOptions, reservationOpts)
	if err != nil {
		// When init fails, the multipooler may still have a live reserved
		// connection (e.g., it was already held for a transaction or temp
		// tables and only the COPY query itself was rejected). Apply whatever
		// state came back: a non-nil ReservedState keeps the gateway pointed
		// at the surviving conn, while a nil/zero state clears the tracking
		// so we don't end up sending future statements to a connection that
		// no longer exists.
		sc.applyReservedState(conn, state, target, reservedState)
		// Surface the PG error un-wrapped: the gateway re-emits a verbatim
		// ErrorResponse to the client. Wrapping here with
		// "failed to initiate COPY:" would diverge from upstream PG output
		// that regression-test fixtures match against.
		return 0, nil, err
	}

	// Use authoritative state from multipooler (reasons already include copy + transaction if applicable)
	sc.applyReservedState(conn, state, target, reservedState)

	sc.logger.DebugContext(ctx, "COPY initiated successfully",
		"reserved_conn_id", reservedState.GetReservedConnectionId(),
		"format", format,
		"num_columns", len(columnFormats))

	return format, columnFormats, nil
}

// CopySendData sends a chunk of COPY data via bidirectional stream.
// Looks up reserved connection from state.ShardStates based on tableGroup/shard.
func (sc *ScatterConn) CopySendData(
	ctx context.Context,
	conn *server.Conn,
	tableGroup string,
	shard string,
	state *handler.MultigatewayConnectionState,
	data []byte,
) error {
	sc.logger.DebugContext(ctx, "sending COPY data chunk",
		"size", len(data),
		"tablegroup", tableGroup,
		"shard", shard)

	// Create target for routing
	target := &querypb.Target{
		ShardKey: &clustermetadatapb.ShardKey{Database: conn.Database(), TableGroup: tableGroup, Shard: shard},
		Mode:     querypb.Mode_MODE_WRITABLE,
	}

	// Get the reserved connection ID from shard state
	ss := state.GetMatchingShardState(target)
	if ss == nil || ss.ReservedState.GetReservedConnectionId() == 0 {
		return errors.New("no active COPY connection")
	}

	// Build options with reserved connection ID
	copyOptions := &querypb.ExecuteOptions{
		UserAuth:             userAuthFrom(conn),
		User:                 conn.User(),
		ClientConnectionId:   conn.ConnectionID(),
		SessionSettings:      state.GetSessionSettings(),
		ReservedConnectionId: ss.ReservedState.GetReservedConnectionId(),
	}

	// Send data via gateway
	if err := sc.gateway.CopySendData(ctx, target, data, copyOptions); err != nil {
		// Surface PG errors un-wrapped — see CopyInitiate comment above.
		return err
	}

	sc.logger.DebugContext(ctx, "sent COPY data", "size", len(data))

	return nil
}

// CopyFinalize sends the final chunk and CopyDone via bidirectional stream.
// Looks up reserved connection from state.ShardStates based on tableGroup/shard.
func (sc *ScatterConn) CopyFinalize(
	ctx context.Context,
	conn *server.Conn,
	tableGroup string,
	shard string,
	state *handler.MultigatewayConnectionState,
	finalData []byte,
	callback func(ctx context.Context, result *sqltypes.Result) error,
) error {
	ctx, span := telemetry.Tracer().Start(ctx, "shard.copy_finalize",
		trace.WithAttributes(
			attribute.String("tablegroup", tableGroup),
			attribute.String("shard", shard),
			attribute.String("db.namespace", conn.Database()),
		),
	)
	defer span.End()

	sc.logger.DebugContext(ctx, "finalizing COPY",
		"final_chunk_size", len(finalData),
		"tablegroup", tableGroup,
		"shard", shard)

	// Create target for routing
	target := &querypb.Target{
		ShardKey: &clustermetadatapb.ShardKey{Database: conn.Database(), TableGroup: tableGroup, Shard: shard},
		Mode:     querypb.Mode_MODE_WRITABLE,
	}

	// Get the reserved connection ID from shard state
	ss := state.GetMatchingShardState(target)
	if ss == nil || ss.ReservedState.GetReservedConnectionId() == 0 {
		return errors.New("no active COPY connection")
	}

	// Build options with reserved connection ID
	copyOptions := &querypb.ExecuteOptions{
		UserAuth:             userAuthFrom(conn),
		User:                 conn.User(),
		ClientConnectionId:   conn.ConnectionID(),
		SessionSettings:      state.GetSessionSettings(),
		ReservedConnectionId: ss.ReservedState.GetReservedConnectionId(),
	}

	// Finalize the COPY operation via gateway
	result, reservedState, err := sc.gateway.CopyFinalize(ctx, target, finalData, copyOptions)
	if err != nil {
		// Forward notices PostgreSQL sent before the ErrorResponse, so clients see
		// NOTICE before ERROR like they would against PostgreSQL directly.
		if result != nil && len(result.Notices) > 0 {
			if cbErr := callback(ctx, &sqltypes.Result{Notices: result.Notices}); cbErr != nil {
				sc.applyReservedState(conn, state, target, reservedState)
				return cbErr
			}
		}
		// CopyFinalize returns a non-nil ReservedState when a PG-level error
		// (e.g., constraint violation) left the underlying reserved connection
		// alive because it is still holding another reason such as a
		// transaction. A nil state means the connection was released. Either
		// way, applyReservedState does the right thing: keep tracking if the
		// state has a non-zero conn ID, clear it and mark the transaction
		// failed if not.
		sc.applyReservedState(conn, state, target, reservedState)
		// Surface PG errors un-wrapped — see CopyInitiate comment above.
		return err
	}

	sc.logger.DebugContext(ctx, "COPY finalized successfully",
		"command_tag", result.CommandTag,
		"rows_affected", result.RowsAffected)

	// Call callback with result
	if err := callback(ctx, result); err != nil {
		sc.logger.ErrorContext(ctx, "callback error in CopyFinalize", "error", err)
		sc.applyReservedState(conn, state, target, reservedState)
		return err
	}

	// Update shard state with authoritative state from multipooler
	sc.applyReservedState(conn, state, target, reservedState)

	return nil
}

// CopyAbort aborts the COPY operation via bidirectional stream.
// Looks up reserved connection from state.ShardStates based on tableGroup/shard.
func (sc *ScatterConn) CopyAbort(
	ctx context.Context,
	conn *server.Conn,
	tableGroup string,
	shard string,
	state *handler.MultigatewayConnectionState,
) error {
	sc.logger.DebugContext(ctx, "aborting COPY",
		"tablegroup", tableGroup,
		"shard", shard)

	// Create target for routing
	target := &querypb.Target{
		ShardKey: &clustermetadatapb.ShardKey{Database: conn.Database(), TableGroup: tableGroup, Shard: shard},
		Mode:     querypb.Mode_MODE_WRITABLE,
	}

	// Get the reserved connection ID from shard state
	ss := state.GetMatchingShardState(target)
	if ss == nil || ss.ReservedState.GetReservedConnectionId() == 0 {
		// Already cleaned up
		sc.logger.DebugContext(ctx, "COPY already cleaned up")
		return nil
	}

	// Build options with reserved connection ID
	copyOptions := &querypb.ExecuteOptions{
		UserAuth:             userAuthFrom(conn),
		User:                 conn.User(),
		ClientConnectionId:   conn.ConnectionID(),
		SessionSettings:      state.GetSessionSettings(),
		ReservedConnectionId: ss.ReservedState.GetReservedConnectionId(),
	}

	// Abort the COPY operation via gateway
	reservedState, err := sc.gateway.CopyAbort(ctx, target, "operation aborted by client", copyOptions)
	if err != nil {
		sc.logger.WarnContext(ctx, "error during COPY abort", "error", err)
	}

	// Update shard state with authoritative state from multipooler
	sc.applyReservedState(conn, state, target, reservedState)

	sc.logger.DebugContext(ctx, "COPY aborted")

	return nil
}

// ReleaseAllReservedConnections forcefully releases all reserved connections.
// Iterates all shard states and calls ReleaseReservedConnection on the multipooler
// for each one. Errors are logged and collected but do not stop the iteration
// (best-effort). After the loop, all local shard state is cleared.
func (sc *ScatterConn) ReleaseAllReservedConnections(
	ctx context.Context,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
) error {
	var errs []error
	for _, ss := range state.ShardStates {
		if ss.ReservedState.GetReservedConnectionId() == 0 {
			continue
		}

		eo := &querypb.ExecuteOptions{
			UserAuth:             userAuthFrom(conn),
			User:                 conn.User(),
			ClientConnectionId:   conn.ConnectionID(),
			SessionSettings:      state.GetSessionSettings(),
			ReservedConnectionId: ss.ReservedState.GetReservedConnectionId(),
		}

		qs, err := sc.gateway.QueryServiceByID(ctx, ss.ReservedState.GetPoolerId(), ss.Target)
		if err != nil {
			sc.logger.ErrorContext(ctx, "release: pooler lookup failed",
				"target", ss.Target, "error", err)
			errs = append(errs, err)
			continue
		}

		if err := qs.ReleaseReservedConnection(ctx, ss.Target, eo); err != nil {
			sc.logger.ErrorContext(ctx, "release: RPC failed",
				"target", ss.Target,
				"reserved_conn_id", ss.ReservedState.GetReservedConnectionId(),
				"error", err)
			errs = append(errs, err)
		}
	}

	state.ClearAllReservedConnections()
	return errors.Join(errs...)
}

// endAction records shard-level metrics and span status for both success and
// error outcomes. Designed to be called via defer with a pointer to the named
// error return.
func (sc *ScatterConn) endAction(ctx context.Context, span trace.Span, start time.Time, dbNamespace, tableGroup, shard string, err *error) {
	duration := time.Since(start).Seconds()
	if *err != nil {
		sqlstate := mterrors.ExtractSQLSTATE(*err)
		span.RecordError(*err)
		span.SetStatus(codes.Error, (*err).Error())
		if sqlstate != "" {
			span.SetAttributes(attribute.String("db.response.status_code", sqlstate))
		}
		sc.metrics.executeDuration.Record(ctx, duration, dbNamespace, tableGroup, shard, ScatterStatusError)
		// TODO: Consider filtering out client-caused errors (e.g. unique constraint violations)
		// from the counter to avoid inflating error rates. We currently count all errors and
		// rely on the error.type label for dashboard filtering. Revisit if counters get noisy.
		sc.metrics.executeErrors.Add(ctx, tableGroup, shard, sqlstate)
		return
	}
	sc.metrics.executeDuration.Record(ctx, duration, dbNamespace, tableGroup, shard, ScatterStatusOK)
}

// Ensure ScatterConn implements engine.IExecute interface.
// This will be checked at compile time.
var _ engine.IExecute = (*ScatterConn)(nil)
