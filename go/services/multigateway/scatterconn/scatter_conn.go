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

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/protoutil"
	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/engine"
	"github.com/multigres/multigres/go/services/multigateway/handler"
	"github.com/multigres/multigres/go/services/multigateway/poolergateway"
)

// ScatterConn coordinates query execution across multiple multipooler instances.
// It implements the engine.IExecute interface.
type ScatterConn struct {
	logger *slog.Logger

	// gateway is used for executing queries (typically a PoolerGateway)
	gateway poolergateway.Gateway
}

// NewScatterConn creates a new ScatterConn instance.
func NewScatterConn(gateway poolergateway.Gateway, logger *slog.Logger) *ScatterConn {
	return &ScatterConn{
		logger:  logger,
		gateway: gateway,
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
) error {
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
	target := &query.Target{
		TableGroup: tableGroup,
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		Shard:      shard,
	}

	eo := &query.ExecuteOptions{
		User:            conn.User(),
		SessionSettings: state.GetSessionSettings(),
	}

	ss := state.GetMatchingShardState(target)

	// Case 1: Already have reserved connection - use it
	if ss != nil && ss.ReservedConnectionId != 0 {
		sc.logger.DebugContext(ctx, "using existing reserved connection",
			"reserved_conn_id", ss.ReservedConnectionId)

		eo.ReservedConnectionId = uint64(ss.ReservedConnectionId)
		qs, err := sc.gateway.QueryServiceByID(ctx, ss.PoolerID, target)
		if err != nil {
			return err
		}

		if err := qs.StreamExecute(ctx, target, sql, eo, callback); err != nil {
			return fmt.Errorf("query execution failed: %w", err)
		}
		return nil
	}

	// Case 2: In transaction but no reserved connection - create one with BEGIN
	if conn.IsInTransaction() {
		sc.logger.DebugContext(ctx, "creating reserved connection with BEGIN for transaction")

		reservationOpts := protoutil.NewTransactionReservationOptions()
		// Pass the original BEGIN query (e.g., "BEGIN ISOLATION LEVEL SERIALIZABLE")
		// so the multipooler preserves transaction options instead of using plain "BEGIN".
		if state.PendingBeginQuery != "" {
			reservationOpts.BeginQuery = state.PendingBeginQuery
			state.PendingBeginQuery = ""
		}
		reservedState, err := sc.gateway.ReserveStreamExecute(ctx, target, sql, eo, reservationOpts, callback)
		if err != nil {
			return fmt.Errorf("reserve stream execute failed: %w", err)
		}

		// Store the reserved connection for subsequent queries with transaction reason
		state.StoreReservedConnection(target, reservedState, protoutil.ReasonTransaction)

		sc.logger.DebugContext(ctx, "reserved connection created",
			"reserved_conn_id", reservedState.ReservedConnectionId)
		return nil
	}

	// Case 3: Not in transaction - use regular pooled connection
	sc.logger.DebugContext(ctx, "executing query via regular pooled connection",
		"tablegroup", tableGroup,
		"shard", shard,
		"pooler_type", target.PoolerType.String())

	if err := sc.gateway.StreamExecute(ctx, target, sql, eo, callback); err != nil {
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
) error {
	sc.logger.DebugContext(ctx, "scatter conn executing portal",
		"tablegroup", tableGroup,
		"shard", shard,
		"portal", portalInfo.Portal.Name,
		"max_rows", maxRows,
		"user", conn.User(),
		"database", conn.Database(),
		"connection_id", conn.ConnectionID())

	// Create target for routing
	target := &query.Target{
		TableGroup: tableGroup,
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		Shard:      shard,
	}

	eo := &query.ExecuteOptions{
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
	if ss != nil && ss.ReservedConnectionId != 0 {
		eo.ReservedConnectionId = uint64(ss.ReservedConnectionId)
		qs, err = sc.gateway.QueryServiceByID(ctx, ss.PoolerID, target)
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
	if reservedState.ReservedConnectionId != 0 {
		// Portal is suspended (not all rows fetched) - store reservation
		state.StoreReservedConnection(target, reservedState, protoutil.ReasonPortal)
	} else {
		// Portal completed - remove portal reason (if any previous suspension was stored).
		// If no other reasons remain (no transaction, no COPY), the entry is cleaned up.
		state.RemoveReservationReason(target, protoutil.ReasonPortal)
	}

	sc.logger.DebugContext(ctx, "portal execution completed successfully",
		"tablegroup", tableGroup,
		"shard", shard,
		"portal", portalInfo.Portal.Name,
		"reserved_connection_id", reservedState.ReservedConnectionId)

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
) (*query.StatementDescription, error) {
	sc.logger.DebugContext(ctx, "scatter conn describing",
		"tablegroup", tableGroup,
		"shard", shard,
		"user", conn.User(),
		"database", conn.Database(),
		"connection_id", conn.ConnectionID())

	// Create target for routing
	target := &query.Target{
		TableGroup: tableGroup,
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		Shard:      shard,
	}

	eo := &query.ExecuteOptions{
		User:            conn.User(),
		SessionSettings: state.GetSessionSettings(),
	}
	var preparedStatement *query.PreparedStatement
	var portal *query.Portal
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
	if ss != nil && ss.ReservedConnectionId != 0 {
		eo.ReservedConnectionId = uint64(ss.ReservedConnectionId)
		qs, err = sc.gateway.QueryServiceByID(ctx, ss.PoolerID, target)
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
	// Collect shard state updates to apply after iteration.
	// We cannot call ClearReservedConnection during iteration because it
	// uses swap-and-truncate which mutates the underlying slice.
	type shardUpdate struct {
		target           *query.Target
		clear            bool   // true = remove entry, false = update reasons
		remainingReasons uint32 // only used when clear == false
	}
	var updates []shardUpdate
	var errs []error
	var callbackResult *sqltypes.Result

	// Count shards with a transaction reason — multi-shard transactions are not
	// yet supported (distributed transactions). Log a warning as a sentinel so
	// unexpected multi-shard cases are visible before DT is implemented.
	var txnShardCount int
	for _, ss := range state.ShardStates {
		if ss.ReservedConnectionId != 0 && protoutil.HasTransactionReason(ss.ReservationReasons) {
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
		if ss.ReservedConnectionId == 0 {
			continue
		}
		if !protoutil.HasTransactionReason(ss.ReservationReasons) {
			continue
		}

		eo := &query.ExecuteOptions{
			User:                 conn.User(),
			SessionSettings:      state.GetSessionSettings(),
			ReservedConnectionId: uint64(ss.ReservedConnectionId),
		}

		qs, err := sc.gateway.QueryServiceByID(ctx, ss.PoolerID, ss.Target)
		if err != nil {
			// Connection lost — mark for clearing, continue to other shards
			updates = append(updates, shardUpdate{target: ss.Target, clear: true})
			errs = append(errs, fmt.Errorf("conclude transaction: pooler lookup failed for %s: %w", ss.Target, err))
			continue
		}

		result, remainingReasons, err := qs.ConcludeTransaction(ctx, ss.Target, eo, conclusion)
		if err != nil {
			updates = append(updates, shardUpdate{target: ss.Target, clear: true})
			errs = append(errs, fmt.Errorf("conclude transaction failed for %s: %w", ss.Target, err))
			continue
		}

		if remainingReasons == 0 {
			// Connection fully released by multipooler
			updates = append(updates, shardUpdate{target: ss.Target, clear: true})
		} else {
			// Connection still reserved for other reasons — update our local tracking
			updates = append(updates, shardUpdate{target: ss.Target, remainingReasons: remainingReasons})
		}

		// Keep the last successful result for the callback
		callbackResult = result
	}

	// Apply collected updates outside the iteration loop.
	for _, u := range updates {
		if u.clear {
			state.ClearReservedConnection(u.target)
		} else {
			state.UpdateReservationReasons(u.target, u.remainingReasons)
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
	sc.logger.DebugContext(ctx, "initiating COPY FROM STDIN",
		"query", queryStr,
		"tablegroup", tableGroup,
		"shard", shard,
		"user", conn.User(),
		"database", conn.Database())

	// Create target for routing - COPY always goes to PRIMARY
	target := &query.Target{
		TableGroup: tableGroup,
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		Shard:      shard,
	}

	// Create execute options
	execOptions := &query.ExecuteOptions{
		User:            conn.User(),
		SessionSettings: state.GetSessionSettings(),
	}

	// Call CopyReady on gateway to initiate the COPY and get format info
	format, columnFormats, reservedState, err := sc.gateway.CopyReady(ctx, target, queryStr, execOptions)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to initiate COPY: %w", err)
	}

	// Store reserved connection with COPY reason. The reason is removed by CopyFinalize/CopyAbort.
	state.StoreReservedConnection(target, reservedState, protoutil.ReasonCopy)

	sc.logger.DebugContext(ctx, "COPY initiated successfully",
		"reserved_conn_id", reservedState.ReservedConnectionId,
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
	target := &query.Target{
		TableGroup: tableGroup,
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		Shard:      shard,
	}

	// Get the reserved connection ID from shard state
	ss := state.GetMatchingShardState(target)
	if ss == nil || ss.ReservedConnectionId == 0 {
		return errors.New("no active COPY connection")
	}

	// Build options with reserved connection ID
	copyOptions := &query.ExecuteOptions{
		User:                 conn.User(),
		SessionSettings:      state.GetSessionSettings(),
		ReservedConnectionId: uint64(ss.ReservedConnectionId),
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
	sc.logger.DebugContext(ctx, "finalizing COPY",
		"final_chunk_size", len(finalData),
		"tablegroup", tableGroup,
		"shard", shard)

	// Create target for routing
	target := &query.Target{
		TableGroup: tableGroup,
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		Shard:      shard,
	}

	// Get the reserved connection ID from shard state
	ss := state.GetMatchingShardState(target)
	if ss == nil || ss.ReservedConnectionId == 0 {
		return errors.New("no active COPY connection")
	}

	// Build options with reserved connection ID
	copyOptions := &query.ExecuteOptions{
		User:                 conn.User(),
		SessionSettings:      state.GetSessionSettings(),
		ReservedConnectionId: uint64(ss.ReservedConnectionId),
	}

	// Finalize the COPY operation via gateway
	result, err := sc.gateway.CopyFinalize(ctx, target, finalData, copyOptions)
	if err != nil {
		// Remove COPY reason on error. If no other reasons remain, the entry is cleared.
		state.RemoveReservationReason(target, protoutil.ReasonCopy)
		return fmt.Errorf("failed to finalize COPY: %w", err)
	}

	sc.logger.DebugContext(ctx, "COPY finalized successfully",
		"command_tag", result.CommandTag,
		"rows_affected", result.RowsAffected)

	// Call callback with result
	if err := callback(ctx, result); err != nil {
		sc.logger.ErrorContext(ctx, "callback error in CopyFinalize", "error", err)
		state.RemoveReservationReason(target, protoutil.ReasonCopy)
		return err
	}

	// Remove COPY reason. If the connection is also reserved for a transaction, it stays.
	state.RemoveReservationReason(target, protoutil.ReasonCopy)

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
	target := &query.Target{
		TableGroup: tableGroup,
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		Shard:      shard,
	}

	// Get the reserved connection ID from shard state
	ss := state.GetMatchingShardState(target)
	if ss == nil || ss.ReservedConnectionId == 0 {
		// Already cleaned up
		sc.logger.DebugContext(ctx, "COPY already cleaned up")
		return nil
	}

	// Build options with reserved connection ID
	copyOptions := &query.ExecuteOptions{
		User:                 conn.User(),
		SessionSettings:      state.GetSessionSettings(),
		ReservedConnectionId: uint64(ss.ReservedConnectionId),
	}

	// Abort the COPY operation via gateway
	if err := sc.gateway.CopyAbort(ctx, target, "operation aborted by client", copyOptions); err != nil {
		sc.logger.WarnContext(ctx, "error during COPY abort", "error", err)
	}

	// Remove COPY reason. If the connection is also reserved for a transaction, it stays.
	state.RemoveReservationReason(target, protoutil.ReasonCopy)

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
		if ss.ReservedConnectionId == 0 {
			continue
		}

		eo := &query.ExecuteOptions{
			User:                 conn.User(),
			SessionSettings:      state.GetSessionSettings(),
			ReservedConnectionId: uint64(ss.ReservedConnectionId),
		}

		qs, err := sc.gateway.QueryServiceByID(ctx, ss.PoolerID, ss.Target)
		if err != nil {
			sc.logger.ErrorContext(ctx, "release: pooler lookup failed",
				"target", ss.Target, "error", err)
			errs = append(errs, err)
			continue
		}

		if err := qs.ReleaseReservedConnection(ctx, ss.Target, eo); err != nil {
			sc.logger.ErrorContext(ctx, "release: RPC failed",
				"target", ss.Target,
				"reserved_conn_id", ss.ReservedConnectionId,
				"error", err)
			errs = append(errs, err)
		}
	}

	state.ClearAllReservedConnections()
	return errors.Join(errs...)
}

// Ensure ScatterConn implements engine.IExecute interface.
// This will be checked at compile time.
var _ engine.IExecute = (*ScatterConn)(nil)
