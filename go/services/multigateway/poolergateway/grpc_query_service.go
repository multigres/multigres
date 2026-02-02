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

package poolergateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"

	"google.golang.org/grpc"
)

// grpcQueryService implements queryservice.QueryService using gRPC to communicate with a multipooler instance.
// This is a private implementation used internally by PoolerGateway.
type grpcQueryService struct {
	// conn is the gRPC connection to the multipooler
	conn *grpc.ClientConn

	// client is the generated gRPC client
	client multipoolerservice.MultiPoolerServiceClient

	// logger for debugging
	logger *slog.Logger

	// poolerID for logging
	poolerID string
}

// newGRPCQueryService creates a new QueryService that uses gRPC to communicate
// with a multipooler instance.
func newGRPCQueryService(
	conn *grpc.ClientConn,
	poolerID string,
	logger *slog.Logger,
) queryservice.QueryService {
	return &grpcQueryService{
		conn:     conn,
		client:   multipoolerservice.NewMultiPoolerServiceClient(conn),
		logger:   logger,
		poolerID: poolerID,
	}
}

// StreamExecute executes a query and streams results back via callback.
func (g *grpcQueryService) StreamExecute(
	ctx context.Context,
	target *query.Target,
	sql string,
	options *query.ExecuteOptions,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	g.logger.DebugContext(ctx, "streaming query execution",
		"pooler_id", g.poolerID,
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"query", sql)

	// Create the request
	req := &multipoolerservice.StreamExecuteRequest{
		Query:   sql,
		Target:  target,
		Options: options,
		// TODO: Add caller_id when we have authentication
	}

	// Call the gRPC StreamExecute
	stream, err := g.client.StreamExecute(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to start stream execute: %w", err)
	}

	// Stream results back via callback
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			// Stream completed successfully
			g.logger.DebugContext(ctx, "stream completed", "pooler_id", g.poolerID)
			return nil
		}
		if err != nil {
			return fmt.Errorf("stream receive error: %w", err)
		}

		// Extract result from response
		if response.Result == nil {
			g.logger.WarnContext(ctx, "received response with nil result", "pooler_id", g.poolerID)
			continue
		}

		// Convert proto result to sqltypes (preserves NULL vs empty string)
		result := sqltypes.ResultFromProto(response.Result)

		// Call the callback with the result
		if err := callback(ctx, result); err != nil {
			// Callback returned error, stop streaming
			g.logger.DebugContext(ctx, "callback returned error, stopping stream",
				"pooler_id", g.poolerID,
				"error", err)
			return err
		}
	}
}

// ExecuteQuery implements queryservice.QueryService.
// This should be used sparingly only when we know the result set is small,
// otherwise StreamExecute should be used.
func (g *grpcQueryService) ExecuteQuery(ctx context.Context, target *query.Target, sql string, options *query.ExecuteOptions) (*sqltypes.Result, error) {
	g.logger.DebugContext(ctx, "Executing query",
		"pooler_id", g.poolerID,
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"query", sql)

	// Create the request
	req := &multipoolerservice.ExecuteQueryRequest{
		Query:   sql,
		Target:  target,
		Options: options,
		// TODO: Add caller_id when we have authentication
	}

	// Call the gRPC ExecuteQuery
	res, err := g.client.ExecuteQuery(ctx, req)
	if err != nil {
		return nil, err
	}
	// Convert proto result to sqltypes (preserves NULL vs empty string)
	return sqltypes.ResultFromProto(res.GetResult()), nil
}

// PortalStreamExecute executes a portal (bound prepared statement) and streams results back via callback.
// Returns ReservedState containing information about the reserved connection used for this execution.
func (g *grpcQueryService) PortalStreamExecute(
	ctx context.Context,
	target *query.Target,
	preparedStatement *query.PreparedStatement,
	portal *query.Portal,
	options *query.ExecuteOptions,
	callback func(context.Context, *sqltypes.Result) error,
) (queryservice.ReservedState, error) {
	g.logger.DebugContext(ctx, "portal stream execute",
		"pooler_id", g.poolerID,
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"portal", portal.Name)

	// Create the request
	req := &multipoolerservice.PortalStreamExecuteRequest{
		Target:            target,
		PreparedStatement: preparedStatement,
		Portal:            portal,
		Options:           options,
		// TODO: Add caller_id when we have authentication
	}

	// Call the gRPC PortalStreamExecute
	stream, err := g.client.PortalStreamExecute(ctx, req)
	if err != nil {
		return queryservice.ReservedState{}, fmt.Errorf("failed to start portal stream execute: %w", err)
	}

	var reservedState queryservice.ReservedState

	// Stream results back via callback
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			// Stream completed successfully
			g.logger.DebugContext(ctx, "portal stream completed", "pooler_id", g.poolerID)
			return reservedState, nil
		}
		if err != nil {
			return reservedState, fmt.Errorf("portal stream receive error: %w", err)
		}

		// Extract reserved state if present
		if response.ReservedConnectionId != 0 {
			reservedState.ReservedConnectionId = response.ReservedConnectionId
			reservedState.PoolerID = response.PoolerId
			g.logger.DebugContext(ctx, "received reserved connection",
				"reserved_connection_id", response.ReservedConnectionId,
				"pooler_id", response.PoolerId.String())
		}

		// Extract result from response
		if response.Result == nil {
			g.logger.WarnContext(ctx, "received response with nil result", "pooler_id", g.poolerID)
			continue
		}

		// Convert proto result to sqltypes (preserves NULL vs empty string)
		result := sqltypes.ResultFromProto(response.Result)

		// Call the callback with the result
		if err := callback(ctx, result); err != nil {
			// Callback returned error, stop streaming
			g.logger.DebugContext(ctx, "callback returned error, stopping stream",
				"pooler_id", g.poolerID,
				"error", err)
			return reservedState, err
		}
	}
}

// Describe returns metadata about a prepared statement or portal.
func (g *grpcQueryService) Describe(
	ctx context.Context,
	target *query.Target,
	preparedStatement *query.PreparedStatement,
	portal *query.Portal,
	options *query.ExecuteOptions,
) (*query.StatementDescription, error) {
	g.logger.DebugContext(ctx, "describing statement/portal",
		"pooler_id", g.poolerID,
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String())

	// Create the request
	req := &multipoolerservice.DescribeRequest{
		Target:            target,
		PreparedStatement: preparedStatement,
		Portal:            portal,
		Options:           options,
		// TODO: Add caller_id when we have authentication
	}

	// Call the gRPC Describe
	response, err := g.client.Describe(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("describe failed: %w", err)
	}

	g.logger.DebugContext(ctx, "describe completed successfully", "pooler_id", g.poolerID)
	return response.Description, nil
}

// Close closes the gRPC connection.
func (g *grpcQueryService) Close(ctx context.Context) error {
	g.logger.DebugContext(ctx, "closing gRPC query service", "pooler_id", g.poolerID)
	if g.conn != nil {
		return g.conn.Close()
	}
	return nil
}

// CopyStream creates a bidirectional stream for COPY operations.
func (g *grpcQueryService) CopyStream(
	ctx context.Context,
	target *query.Target,
	copyQuery string,
	options *query.ExecuteOptions,
) (queryservice.CopyStreamClient, error) {
	g.logger.DebugContext(ctx, "creating COPY stream",
		"pooler_id", g.poolerID,
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"query", copyQuery)

	// Start the bidirectional stream
	stream, err := g.client.BidirectionalExecute(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start bidirectional execute stream: %w", err)
	}

	// Return wrapper that implements CopyStreamClient
	return &grpcCopyStreamClient{
		stream:    stream,
		target:    target,
		copyQuery: copyQuery,
		options:   options,
		logger:    g.logger,
		poolerID:  g.poolerID,
	}, nil
}

// grpcCopyStreamClient implements queryservice.CopyStreamClient using gRPC bidirectional streaming.
type grpcCopyStreamClient struct {
	stream    multipoolerservice.MultiPoolerService_BidirectionalExecuteClient
	target    *query.Target
	copyQuery string
	options   *query.ExecuteOptions
	logger    *slog.Logger
	poolerID  string
}

// Ready initiates the COPY operation and returns format information.
func (c *grpcCopyStreamClient) Ready() (int16, []int16, queryservice.ReservedState, error) {
	// Send INITIATE message
	initiateReq := &multipoolerservice.BidirectionalExecuteRequest{
		Phase:   multipoolerservice.BidirectionalExecuteRequest_INITIATE,
		Query:   c.copyQuery,
		Target:  c.target,
		Options: c.options,
	}

	if err := c.stream.Send(initiateReq); err != nil {
		return 0, nil, queryservice.ReservedState{}, fmt.Errorf("failed to send INITIATE: %w", err)
	}

	c.logger.Debug("sent INITIATE message", "pooler_id", c.poolerID)

	// Receive READY response
	resp, err := c.stream.Recv()
	if err != nil {
		return 0, nil, queryservice.ReservedState{}, fmt.Errorf("failed to receive READY response: %w", err)
	}

	// Check for ERROR response
	if resp.Phase == multipoolerservice.BidirectionalExecuteResponse_ERROR {
		return 0, nil, queryservice.ReservedState{}, fmt.Errorf("COPY initiation failed: %s", resp.Error)
	}

	// Validate READY response
	if resp.Phase != multipoolerservice.BidirectionalExecuteResponse_READY {
		return 0, nil, queryservice.ReservedState{}, fmt.Errorf("expected READY, got %v", resp.Phase)
	}

	// Convert columnFormats from []int32 to []int16
	columnFormats := make([]int16, len(resp.ColumnFormats))
	for i, f := range resp.ColumnFormats {
		columnFormats[i] = int16(f)
	}

	reservedState := queryservice.ReservedState{
		ReservedConnectionId: resp.ReservedConnectionId,
		PoolerID:             resp.PoolerId,
	}

	c.logger.Debug("received READY response",
		"pooler_id", c.poolerID,
		"reserved_conn_id", resp.ReservedConnectionId,
		"format", resp.Format,
		"num_columns", len(columnFormats))

	return int16(resp.Format), columnFormats, reservedState, nil
}

// SendData sends a chunk of COPY data to the server.
func (c *grpcCopyStreamClient) SendData(data []byte) error {
	dataReq := &multipoolerservice.BidirectionalExecuteRequest{
		Phase: multipoolerservice.BidirectionalExecuteRequest_DATA,
		Data:  data,
	}

	if err := c.stream.Send(dataReq); err != nil {
		return fmt.Errorf("failed to send DATA: %w", err)
	}

	c.logger.Debug("sent DATA message", "pooler_id", c.poolerID, "size", len(data))

	return nil
}

// Finalize completes the COPY operation.
func (c *grpcCopyStreamClient) Finalize(finalData []byte) (*sqltypes.Result, error) {
	// Send DONE message with final data
	doneReq := &multipoolerservice.BidirectionalExecuteRequest{
		Phase: multipoolerservice.BidirectionalExecuteRequest_DONE,
		Data:  finalData,
	}

	if err := c.stream.Send(doneReq); err != nil {
		return nil, fmt.Errorf("failed to send DONE: %w", err)
	}

	// Close send direction
	if err := c.stream.CloseSend(); err != nil {
		return nil, fmt.Errorf("failed to close send: %w", err)
	}

	c.logger.Debug("sent DONE message", "pooler_id", c.poolerID, "final_data_size", len(finalData))

	// Receive RESULT response
	resp, err := c.stream.Recv()
	if err != nil {
		return nil, fmt.Errorf("failed to receive RESULT response: %w", err)
	}

	// Check for ERROR response
	if resp.Phase == multipoolerservice.BidirectionalExecuteResponse_ERROR {
		return nil, fmt.Errorf("COPY finalization failed: %s", resp.Error)
	}

	// Validate RESULT response
	if resp.Phase != multipoolerservice.BidirectionalExecuteResponse_RESULT {
		return nil, fmt.Errorf("expected RESULT, got %v", resp.Phase)
	}

	result := sqltypes.ResultFromProto(resp.Result)

	c.logger.Debug("received RESULT response", "pooler_id", c.poolerID)

	return result, nil
}

// Abort aborts the COPY operation.
func (c *grpcCopyStreamClient) Abort(errorMsg string) error {
	// Send FAIL message
	failReq := &multipoolerservice.BidirectionalExecuteRequest{
		Phase:        multipoolerservice.BidirectionalExecuteRequest_FAIL,
		ErrorMessage: errorMsg,
	}

	if err := c.stream.Send(failReq); err != nil {
		// Log but don't return error - we're already aborting
		c.logger.Debug("failed to send FAIL message", "pooler_id", c.poolerID, "error", err)
	}

	// Close send direction
	if err := c.stream.CloseSend(); err != nil {
		c.logger.Debug("failed to close send after FAIL", "pooler_id", c.poolerID, "error", err)
	}

	// Try to receive response (may be ERROR)
	_, err := c.stream.Recv()
	if err != nil && !errors.Is(err, io.EOF) {
		c.logger.Debug("error receiving response after FAIL (expected)", "pooler_id", c.poolerID, "error", err)
	}

	c.logger.Debug("COPY aborted", "pooler_id", c.poolerID)

	return nil
}

// Ensure grpcCopyStreamClient implements queryservice.CopyStreamClient
var _ queryservice.CopyStreamClient = (*grpcCopyStreamClient)(nil)

// Ensure grpcQueryService implements queryservice.QueryService
var _ queryservice.QueryService = (*grpcQueryService)(nil)
