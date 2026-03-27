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
	state *handler.MultiGatewayConnectionState,
	target *querypb.Target,
	rs *querypb.ReservedState,
) {
	if rs.GetReservedConnectionId() == 0 {
		state.ClearReservedConnection(target)
		if conn.TxnStatus() == protocol.TxnStatusInBlock {
			conn.SetTxnStatus(protocol.TxnStatusFailed)
		}
		// If the destroyed connection had temp tables, clear the pin.
		// The temp tables are irrecoverably lost with the backend session.
		// Subsequent queries will use regular pooled connections and get
		// "relation does not exist" if they reference the temp table,
		// which is the correct PostgreSQL error for this situation.
		if state.SessionPinned {
			state.SessionPinned = false
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
//   - Case 2: In transaction, no reserved conn → call ReserveStreamExecute with BEGIN
//   - Case 3: Not in transaction → use regular pooled connection
func (sc *ScatterConn) StreamExecute(
	ctx context.Context,
	conn *server.Conn,
	tableGroup string,
	shard string,
	sql string,
	state *handler.MultiGatewayConnectionState,
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

	// Create target for routing
	// TODO: Add query analysis to determine if this is a read or write query
	// For now, always route to PRIMARY (safe default)
	target := &querypb.Target{
		TableGroup: tableGroup,
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		Shard:      shard,
	}

	eo := &querypb.ExecuteOptions{
		User:            conn.User(),
		SessionSettings: state.GetSessionSettings(),
	}

	ss := state.GetMatchingShardState(target)

	// Case 1: Already have reserved connection - use it
	if ss != nil && ss.ReservedState.GetReservedConnectionId() != 0 {
		sc.logger.DebugContext(ctx, "using existing reserved connection",
			"reserved_conn_id", ss.ReservedState.GetReservedConnectionId())

		eo.ReservedConnectionId = ss.ReservedState.GetReservedConnectionId()
		qs, err := sc.gateway.QueryServiceByID(ctx, ss.ReservedState.GetPoolerId(), target)
		if err != nil {
			return err
		}

		// For pinned sessions with deferred BEGIN: prepend the BEGIN to the
		// SQL so PG enters a transaction block on this reserved connection.
		beganTransaction := false
		if state.PendingBeginQuery != "" && state.SessionPinned {
			sc.logger.DebugContext(ctx, "prepending deferred BEGIN to pinned query",
				"pending_begin", state.PendingBeginQuery)
			sql = state.PendingBeginQuery + "; " + sql
			state.PendingBeginQuery = ""
			beganTransaction = true
		}

		reservedState, err := qs.StreamExecute(ctx, target, sql, eo, callback)
		sc.applyReservedState(conn, state, target, reservedState)

		// After prepending BEGIN or when gateway knows we're in a transaction,
		// ensure the shard state has ReasonTransaction so ConcludeTransaction
		// can send COMMIT/ROLLBACK to the multipooler.
		if (beganTransaction || (conn.IsInTransaction() && state.SessionPinned)) && err == nil {
			updatedSS := state.GetMatchingShardState(target)
			if updatedSS != nil && updatedSS.ReservedState != nil {
				updatedSS.ReservedState.ReservationReasons |= protoutil.ReasonTransaction
			}
		}
		if err != nil {
			return fmt.Errorf("query execution failed: %w", err)
		}
		return nil
	}

	// Case 2: In transaction but no reserved connection - create one with BEGIN
	if conn.IsInTransaction() {
		sc.logger.DebugContext(ctx, "creating reserved connection with BEGIN for transaction")

		reservationOpts := protoutil.NewTransactionReservationOptions()
		// If the session is pinned for temp tables, include the temp table reason
		// so the connection survives COMMIT (only released when all reasons clear).
		if state.SessionPinned {
			reservationOpts.Reasons |= protoutil.ReasonTempTable
		}
		// Pass the original BEGIN query (e.g., "BEGIN ISOLATION LEVEL SERIALIZABLE")
		// so the multipooler preserves transaction options instead of using plain "BEGIN".
		if state.PendingBeginQuery != "" {
			reservationOpts.BeginQuery = state.PendingBeginQuery
			// NOTE: PendingBeginQuery is consumed by the first shard. In a future
			// multi-shard transaction, subsequent shards would get a plain BEGIN
			// instead of preserving the isolation level. When we add distributed
			// transactions, preserve this for the transaction lifetime and only
			// clear on commit/rollback.
			state.PendingBeginQuery = ""
		}
		reservedState, err := sc.gateway.ReserveStreamExecute(ctx, target, sql, eo, reservationOpts, callback)
		if err != nil {
			return fmt.Errorf("reserve stream execute failed: %w", err)
		}

		// Use authoritative state from multipooler (reasons already include transaction)
		sc.applyReservedState(conn, state, target, reservedState)

		sc.logger.DebugContext(ctx, "reserved connection created",
			"reserved_conn_id", reservedState.GetReservedConnectionId())
		return nil
	}

	// Case 2b: Session pinned (temp tables) but no reserved connection yet - create one
	if state.SessionPinned {
		sc.logger.DebugContext(ctx, "creating reserved connection for session pin (temp tables)")

		reservationOpts := protoutil.NewTempTableReservationOptions()
		reservedState, err := sc.gateway.ReserveStreamExecute(ctx, target, sql, eo, reservationOpts, callback)
		if err != nil {
			return fmt.Errorf("reserve stream execute failed: %w", err)
		}

		sc.applyReservedState(conn, state, target, reservedState)
		return nil
	}

	// Case 3: Not in transaction - use regular pooled connection
	sc.logger.DebugContext(ctx, "executing query via regular pooled connection",
		"tablegroup", tableGroup,
		"shard", shard,
		"pooler_type", target.PoolerType.String())

	if _, err := sc.gateway.StreamExecute(ctx, target, sql, eo, callback); err != nil {
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
	state *handler.MultiGatewayConnectionState,
	portalInfo *preparedstatement.PortalInfo,
	maxRows int32,
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
	target := &querypb.Target{
		TableGroup: tableGroup,
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		Shard:      shard,
	}

	eo := &querypb.ExecuteOptions{
		User:            conn.User(),
		MaxRows:         uint64(maxRows),
		SessionSettings: state.GetSessionSettings(),
	}

	var qs queryservice.QueryService = sc.gateway
	var err error

	ss := state.GetMatchingShardState(target)
	// If we have a reserved connection, we have to ensure
	// we are routing the query to the pooler where we got the reserved
	// connection from. If a reparent happened, then we will get an error
	// back.
	if ss != nil && ss.ReservedState.GetReservedConnectionId() != 0 {
		eo.ReservedConnectionId = ss.ReservedState.GetReservedConnectionId()
		qs, err = sc.gateway.QueryServiceByID(ctx, ss.ReservedState.GetPoolerId(), target)
	} else if conn.IsInTransaction() {
		// Case 2: In transaction but no reserved connection — create one with BEGIN.
		// Same deferred-BEGIN pattern as StreamExecute and CopyInitiate.
		//
		// We reuse ReserveStreamExecute with a no-op "SELECT 1" query rather than
		// adding a dedicated ReservePortalStreamExecute RPC. A new RPC would require
		// changes to the QueryService interface, proto definitions, all implementations,
		// and the gRPC service, far more invasive for the same result.
		sc.logger.DebugContext(ctx, "creating reserved connection with BEGIN for portal transaction")

		reservationOpts := protoutil.NewTransactionReservationOptions()
		if state.SessionPinned {
			reservationOpts.Reasons |= protoutil.ReasonTempTable
		}
		if state.PendingBeginQuery != "" {
			reservationOpts.BeginQuery = state.PendingBeginQuery
			state.PendingBeginQuery = ""
		}
		noopCallback := func(context.Context, *sqltypes.Result) error { return nil }
		reservedState, reserveErr := sc.gateway.ReserveStreamExecute(
			ctx, target, "SELECT 1", eo, reservationOpts, noopCallback)
		if reserveErr != nil {
			return fmt.Errorf("reserve connection for portal failed: %w", reserveErr)
		}
		sc.applyReservedState(conn, state, target, reservedState)
		eo.ReservedConnectionId = reservedState.GetReservedConnectionId()
		qs, err = sc.gateway.QueryServiceByID(ctx, reservedState.GetPoolerId(), target)
	} else if state.SessionPinned {
		// Session pinned (temp tables) but no reserved connection yet.
		// Fall back to ReserveStreamExecute with the query text to create
		// the reservation. Subsequent portal executions will use Case 1.
		sc.logger.DebugContext(ctx, "creating reserved connection for session pin via portal execution")
		sql := portalInfo.PreparedStatementInfo.Query
		reservationOpts := protoutil.NewTempTableReservationOptions()
		reservedState, rsErr := sc.gateway.ReserveStreamExecute(ctx, target, sql, eo, reservationOpts, callback)
		if rsErr != nil {
			return fmt.Errorf("reserve stream execute failed: %w", rsErr)
		}
		sc.applyReservedState(conn, state, target, reservedState)
		return nil
	}
	if err != nil {
		return err
	}

	// Execute portal via QueryService (PoolerGateway) and stream results
	sc.logger.DebugContext(ctx, "executing portal via query service",
		"tablegroup", tableGroup,
		"shard", shard,
		"portal", portalInfo.Portal.Name,
		"pooler_type", target.PoolerType.String())

	// Use the query from the prepared statement
	reservedState, err := qs.PortalStreamExecute(ctx, target, portalInfo.PreparedStatementInfo.PreparedStatement, portalInfo.Portal, eo, callback)
	if err != nil {
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
	state *handler.MultiGatewayConnectionState,
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
	target := &querypb.Target{
		TableGroup: tableGroup,
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		Shard:      shard,
	}

	eo := &querypb.ExecuteOptions{
		User:            conn.User(),
		SessionSettings: state.GetSessionSettings(),
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
		"pooler_type", target.PoolerType.String())

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
	state *handler.MultiGatewayConnectionState,
	conclusion multipoolerpb.TransactionConclusion,
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
			User:                 conn.User(),
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

		result, reservedState, err := qs.ConcludeTransaction(ctx, ss.Target, eo, conclusion)
		if err != nil {
			updates = append(updates, shardUpdate{target: ss.Target, clear: true})
			// ROLLBACK on a destroyed connection is graceful recovery — don't propagate error
			if conclusion == multipoolerpb.TransactionConclusion_TRANSACTION_CONCLUSION_ROLLBACK {
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
	state *handler.MultiGatewayConnectionState,
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
			User:                 conn.User(),
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

// CopyInitiate initiates a COPY FROM STDIN operation using bidirectional streaming.
// Stores reserved connection info in state.ShardStates for the given tableGroup/shard.
// Returns: format, columnFormats, error
func (sc *ScatterConn) CopyInitiate(
	ctx context.Context,
	conn *server.Conn,
	tableGroup string,
	shard string,
	queryStr string,
	state *handler.MultiGatewayConnectionState,
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
		TableGroup: tableGroup,
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		Shard:      shard,
	}

	// Create execute options
	execOptions := &querypb.ExecuteOptions{
		User:            conn.User(),
		SessionSettings: state.GetSessionSettings(),
	}

	// If there's already a reserved connection for this target (e.g., in a transaction),
	// pass its ID so CopyReady reuses it instead of creating a new one.
	// If we're in a transaction but no reserved connection exists yet (deferred BEGIN),
	// pass ReservationOptions so CopyReady creates a connection with the pending BEGIN.
	var reservationOpts *multipoolerpb.ReservationOptions
	ss := state.GetMatchingShardState(target)
	if ss != nil && ss.ReservedState.GetReservedConnectionId() != 0 {
		execOptions.ReservedConnectionId = ss.ReservedState.GetReservedConnectionId()
	} else if conn.IsInTransaction() {
		// Deferred BEGIN: pass transaction reservation options so the executor
		// executes BEGIN on the new connection before initiating COPY.
		reservationOpts = protoutil.NewTransactionReservationOptions()
		if state.SessionPinned {
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
		return 0, nil, fmt.Errorf("failed to initiate COPY: %w", err)
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
	state *handler.MultiGatewayConnectionState,
	data []byte,
) error {
	sc.logger.DebugContext(ctx, "sending COPY data chunk",
		"size", len(data),
		"tablegroup", tableGroup,
		"shard", shard)

	// Create target for routing
	target := &querypb.Target{
		TableGroup: tableGroup,
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		Shard:      shard,
	}

	// Get the reserved connection ID from shard state
	ss := state.GetMatchingShardState(target)
	if ss == nil || ss.ReservedState.GetReservedConnectionId() == 0 {
		return errors.New("no active COPY connection")
	}

	// Build options with reserved connection ID
	copyOptions := &querypb.ExecuteOptions{
		User:                 conn.User(),
		SessionSettings:      state.GetSessionSettings(),
		ReservedConnectionId: ss.ReservedState.GetReservedConnectionId(),
	}

	// Send data via gateway
	if err := sc.gateway.CopySendData(ctx, target, data, copyOptions); err != nil {
		return fmt.Errorf("failed to send COPY data: %w", err)
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
	state *handler.MultiGatewayConnectionState,
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
		TableGroup: tableGroup,
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		Shard:      shard,
	}

	// Get the reserved connection ID from shard state
	ss := state.GetMatchingShardState(target)
	if ss == nil || ss.ReservedState.GetReservedConnectionId() == 0 {
		return errors.New("no active COPY connection")
	}

	// Build options with reserved connection ID
	copyOptions := &querypb.ExecuteOptions{
		User:                 conn.User(),
		SessionSettings:      state.GetSessionSettings(),
		ReservedConnectionId: ss.ReservedState.GetReservedConnectionId(),
	}

	// Finalize the COPY operation via gateway
	result, reservedState, err := sc.gateway.CopyFinalize(ctx, target, finalData, copyOptions)
	if err != nil {
		// Every error path in executor.CopyFinalize destroys the connection via
		// Release(ReleaseError), so the multipooler no longer has it. Pass a zero
		// ReservedState to applyReservedState so it clears state and (if in a transaction)
		// marks TxnStatusFailed as defense-in-depth.
		sc.applyReservedState(conn, state, target, nil)
		return fmt.Errorf("failed to finalize COPY: %w", err)
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
	state *handler.MultiGatewayConnectionState,
) error {
	sc.logger.DebugContext(ctx, "aborting COPY",
		"tablegroup", tableGroup,
		"shard", shard)

	// Create target for routing
	target := &querypb.Target{
		TableGroup: tableGroup,
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		Shard:      shard,
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
		User:                 conn.User(),
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
	state *handler.MultiGatewayConnectionState,
) error {
	var errs []error
	for _, ss := range state.ShardStates {
		if ss.ReservedState.GetReservedConnectionId() == 0 {
			continue
		}

		eo := &querypb.ExecuteOptions{
			User:                 conn.User(),
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
