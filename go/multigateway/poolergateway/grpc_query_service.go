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
	"fmt"
	"io"
	"log/slog"

	"github.com/multigres/multigres/go/multipooler/queryservice"
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
	callback func(context.Context, *query.QueryResult) error,
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
		if err == io.EOF {
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

		// Call the callback with the result
		if err := callback(ctx, response.Result); err != nil {
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
func (g *grpcQueryService) ExecuteQuery(ctx context.Context, target *query.Target, sql string, options *query.ExecuteOptions) (*query.QueryResult, error) {
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
	return res.GetResult(), err
}

// PortalStreamExecute executes a portal (bound prepared statement) and streams results back via callback.
// Returns ReservedState containing information about the reserved connection used for this execution.
func (g *grpcQueryService) PortalStreamExecute(
	ctx context.Context,
	target *query.Target,
	preparedStatement *query.PreparedStatement,
	portal *query.Portal,
	options *query.ExecuteOptions,
	callback func(context.Context, *query.QueryResult) error,
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
	firstResponse := true

	// Stream results back via callback
	for {
		response, err := stream.Recv()
		if err == io.EOF {
			// Stream completed successfully
			g.logger.DebugContext(ctx, "portal stream completed", "pooler_id", g.poolerID)
			return reservedState, nil
		}
		if err != nil {
			return reservedState, fmt.Errorf("portal stream receive error: %w", err)
		}

		// Extract reserved state from first response
		if firstResponse {
			reservedState.ReservedConnectionId = response.ReservedConnectionId
			if response.PoolerId != nil {
				reservedState.PoolerID = response.PoolerId
				g.logger.DebugContext(ctx, "received reserved connection",
					"reserved_connection_id", response.ReservedConnectionId,
					"pooler_id", response.PoolerId.String())
			}
			firstResponse = false
		}

		// Extract result from response
		if response.Result == nil {
			g.logger.WarnContext(ctx, "received response with nil result", "pooler_id", g.poolerID)
			continue
		}

		// Call the callback with the result
		if err := callback(ctx, response.Result); err != nil {
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

// Ensure grpcQueryService implements queryservice.QueryService
var _ queryservice.QueryService = (*grpcQueryService)(nil)
