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

	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
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
// - Creates Target with tablegroup, shard, and PRIMARY pooler type
// - Uses PoolerGateway to select matching pooler
// - Executes query via gRPC to the pooler
// - Streams actual results back via callback
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
		"connection_id", conn.ConnectionID())

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

	// Execute query via QueryService (PoolerGateway) and stream results
	// PoolerGateway will use the target to find the right pooler
	sc.logger.DebugContext(ctx, "executing query via query service",
		"tablegroup", tableGroup,
		"shard", shard,
		"pooler_type", target.PoolerType.String())

	if err := qs.StreamExecute(ctx, target, sql, eo, callback); err != nil {
		// If it's a PostgreSQL error, don't wrap it - pass through unchanged
		var pgDiag *sqltypes.PgDiagnostic
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
		var pgDiag *sqltypes.PgDiagnostic
		if errors.As(err, &pgDiag) {
			return err
		}
		return fmt.Errorf("portal execution failed: %w", err)
	}
	state.StoreReservedConnection(target, reservedState)

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
		var pgDiag *sqltypes.PgDiagnostic
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

	// Store reserved connection in ShardStates (same pattern as regular queries)
	state.StoreReservedConnection(target, reservedState)

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
		// Clear state even on error - the reserved connection has been released/closed on the server side
		state.ClearReservedConnection(target)
		return fmt.Errorf("failed to finalize COPY: %w", err)
	}

	sc.logger.DebugContext(ctx, "COPY finalized successfully",
		"command_tag", result.CommandTag,
		"rows_affected", result.RowsAffected)

	// Call callback with result
	if err := callback(ctx, result); err != nil {
		sc.logger.ErrorContext(ctx, "callback error in CopyFinalize", "error", err)
		// Clear state even on callback error - the reserved connection has already been released
		state.ClearReservedConnection(target)
		return err
	}

	// Clear reserved connection since it's been released
	state.ClearReservedConnection(target)

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

	// Clear reserved connection since it's been released
	state.ClearReservedConnection(target)

	sc.logger.DebugContext(ctx, "COPY aborted")

	return nil
}

// Ensure ScatterConn implements engine.IExecute interface.
// This will be checked at compile time.
var _ engine.IExecute = (*ScatterConn)(nil)
