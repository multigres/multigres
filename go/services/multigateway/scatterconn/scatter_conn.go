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
	"github.com/multigres/multigres/go/pb/multipoolerservice"
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
		return nil, fmt.Errorf("describe failed: %w", err)
	}

	sc.logger.DebugContext(ctx, "describe completed successfully",
		"tablegroup", tableGroup,
		"shard", shard)

	return description, nil
}

// --- COPY FROM STDIN methods ---

// CopyInitiate initiates a COPY FROM STDIN operation using bidirectional streaming.
// Returns: reservedConnID, poolerID, format, columnFormats, error
func (sc *ScatterConn) CopyInitiate(
	ctx context.Context,
	conn *server.Conn,
	queryStr string,
	callback func(ctx context.Context, result *sqltypes.Result) error,
) (uint64, *clustermetadatapb.ID, int16, []int16, error) {
	sc.logger.DebugContext(ctx, "initiating COPY FROM STDIN",
		"query", queryStr,
		"user", conn.User(),
		"database", conn.Database())

	// For COPY operations, we need to get the table group from somewhere
	// Since ScatterConn doesn't have access to the planner, we'll use a default
	// In practice, this will be called by CopyStatement which knows the table group
	// but we need to extract it from context or pass it differently
	// For now, hardcode to "default" - this matches what Executor does
	const defaultTableGroup = "default"

	// Create target for routing - COPY always goes to PRIMARY
	target := &query.Target{
		TableGroup: defaultTableGroup,
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		Shard:      "",
	}

	// Get connection state
	state := conn.GetConnectionState().(*handler.MultiGatewayConnectionState)

	// Create bidirectional stream
	stream, err := sc.gateway.GetBidirectionalExecuteStream(ctx, target)
	if err != nil {
		return 0, nil, 0, nil, fmt.Errorf("failed to create bidirectional execute stream: %w", err)
	}

	// Send INITIATE message (but don't store stream yet)
	execOptions := &query.ExecuteOptions{
		User:            conn.User(),
		SessionSettings: state.GetSessionSettings(),
	}

	initiateReq := &multipoolerservice.BidirectionalExecuteRequest{
		Phase:   multipoolerservice.BidirectionalExecuteRequest_INITIATE,
		Query:   queryStr,
		Target:  target,
		Options: execOptions,
	}

	if err := stream.Send(initiateReq); err != nil {
		return 0, nil, 0, nil, fmt.Errorf("failed to send INITIATE: %w", err)
	}

	sc.logger.DebugContext(ctx, "sent INITIATE message")

	// Receive READY response (CopyInResponse for COPY FROM STDIN)
	resp, err := stream.Recv()
	if err != nil {
		return 0, nil, 0, nil, fmt.Errorf("failed to receive READY response: %w", err)
	}

	// Check for ERROR response
	if resp.Phase == multipoolerservice.BidirectionalExecuteResponse_ERROR {
		return 0, nil, 0, nil, fmt.Errorf("operation failed: %s", resp.Error)
	}

	// Validate READY response
	if resp.Phase != multipoolerservice.BidirectionalExecuteResponse_READY {
		return 0, nil, 0, nil, fmt.Errorf("expected READY, got %v", resp.Phase)
	}

	// SUCCESS - Store stream ONLY after initiation succeeds
	state.SetCopyStream(stream)

	// Convert columnFormats from []int32 to []int16
	columnFormats := make([]int16, len(resp.ColumnFormats))
	for i, f := range resp.ColumnFormats {
		columnFormats[i] = int16(f)
	}

	sc.logger.DebugContext(ctx, "received READY response",
		"reserved_conn_id", resp.ReservedConnectionId,
		"format", resp.Format,
		"num_columns", len(columnFormats))

	return resp.ReservedConnectionId, resp.PoolerId, int16(resp.Format), columnFormats, nil
}

// CopySendData sends a chunk of COPY data via bidirectional stream.
func (sc *ScatterConn) CopySendData(
	ctx context.Context,
	conn *server.Conn,
	reservedConnID uint64,
	poolerID *clustermetadatapb.ID,
	data []byte,
) error {
	sc.logger.DebugContext(ctx, "sending COPY data chunk",
		"size", len(data),
		"reserved_conn_id", reservedConnID)

	// Get the stream from connection state
	state := conn.GetConnectionState().(*handler.MultiGatewayConnectionState)
	stream := state.GetCopyStream()
	if stream == nil {
		return errors.New("no active bidirectional execute stream")
	}

	// Send DATA message
	dataReq := &multipoolerservice.BidirectionalExecuteRequest{
		Phase: multipoolerservice.BidirectionalExecuteRequest_DATA,
		Data:  data,
	}

	if err := stream.Send(dataReq); err != nil {
		return fmt.Errorf("failed to send DATA: %w", err)
	}

	sc.logger.DebugContext(ctx, "sent DATA message", "size", len(data))

	return nil
}

// CopyFinalize sends the final chunk and CopyDone via bidirectional stream.
func (sc *ScatterConn) CopyFinalize(
	ctx context.Context,
	conn *server.Conn,
	reservedConnID uint64,
	poolerID *clustermetadatapb.ID,
	finalData []byte,
	callback func(ctx context.Context, result *sqltypes.Result) error,
) error {
	sc.logger.DebugContext(ctx, "finalizing COPY",
		"final_chunk_size", len(finalData),
		"reserved_conn_id", reservedConnID)

	// Get the stream from connection state
	state := conn.GetConnectionState().(*handler.MultiGatewayConnectionState)
	stream := state.GetCopyStream()
	if stream == nil {
		return errors.New("no active bidirectional execute stream")
	}

	// Send DONE message with final data
	doneReq := &multipoolerservice.BidirectionalExecuteRequest{
		Phase: multipoolerservice.BidirectionalExecuteRequest_DONE,
		Data:  finalData,
	}

	if err := stream.Send(doneReq); err != nil {
		return fmt.Errorf("failed to send DONE: %w", err)
	}

	// Close our side of the stream
	if err := stream.CloseSend(); err != nil {
		return fmt.Errorf("failed to close send: %w", err)
	}

	sc.logger.DebugContext(ctx, "sent DONE message, waiting for result")

	// Receive RESULT message
	resp, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("failed to receive RESULT: %w", err)
	}

	if resp.Phase == multipoolerservice.BidirectionalExecuteResponse_ERROR {
		return fmt.Errorf("operation failed: %s", resp.Error)
	}

	if resp.Phase != multipoolerservice.BidirectionalExecuteResponse_RESULT {
		return fmt.Errorf("expected RESULT, got %v", resp.Phase)
	}

	sc.logger.DebugContext(ctx, "received RESULT message")

	// Convert result and call callback
	result := sqltypes.ResultFromProto(resp.Result)
	if err := callback(ctx, result); err != nil {
		return err
	}

	return nil
}

// CopyAbort aborts the COPY operation via bidirectional stream.
func (sc *ScatterConn) CopyAbort(
	ctx context.Context,
	conn *server.Conn,
	reservedConnID uint64,
	poolerID *clustermetadatapb.ID,
) error {
	sc.logger.DebugContext(ctx, "aborting COPY",
		"reserved_conn_id", reservedConnID)

	// Get the stream from connection state
	state := conn.GetConnectionState().(*handler.MultiGatewayConnectionState)
	stream := state.GetCopyStream()
	if stream == nil {
		// Already cleaned up
		return nil
	}

	// Send FAIL message
	failReq := &multipoolerservice.BidirectionalExecuteRequest{
		Phase:        multipoolerservice.BidirectionalExecuteRequest_FAIL,
		ErrorMessage: "operation aborted by client",
	}

	if err := stream.Send(failReq); err != nil {
		sc.logger.WarnContext(ctx, "failed to send FAIL message", "error", err)
	}

	// Close our side of the stream
	if err := stream.CloseSend(); err != nil {
		sc.logger.WarnContext(ctx, "failed to close send", "error", err)
	}

	// Drain any remaining responses until stream ends
	// We intentionally ignore errors here as stream closure is expected
	for {
		_, recvErr := stream.Recv()
		if recvErr != nil {
			// Expected to fail with io.EOF or stream closed
			// This is the normal exit condition, not an error to return
			sc.logger.DebugContext(ctx, "stream closed as expected", "error", recvErr)
			break
		}
	}

	sc.logger.DebugContext(ctx, "operation aborted")

	return nil
}

// Ensure ScatterConn implements engine.IExecute interface.
// This will be checked at compile time.
var _ engine.IExecute = (*ScatterConn)(nil)
