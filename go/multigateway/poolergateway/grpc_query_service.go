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
	callback func(*query.QueryResult) error,
) error {
	g.logger.Debug("streaming query execution",
		"pooler_id", g.poolerID,
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"query", sql)

	// Create the request
	req := &multipoolerservice.StreamExecuteRequest{
		Query:  []byte(sql),
		Target: target,
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
			g.logger.Debug("stream completed", "pooler_id", g.poolerID)
			return nil
		}
		if err != nil {
			return fmt.Errorf("stream receive error: %w", err)
		}

		// Extract result from response
		if response.Result == nil {
			g.logger.Warn("received response with nil result", "pooler_id", g.poolerID)
			continue
		}

		// Call the callback with the result
		if err := callback(response.Result); err != nil {
			// Callback returned error, stop streaming
			g.logger.Debug("callback returned error, stopping stream",
				"pooler_id", g.poolerID,
				"error", err)
			return err
		}
	}
}

// Close closes the gRPC connection.
func (g *grpcQueryService) Close(ctx context.Context) error {
	g.logger.Debug("closing gRPC query service", "pooler_id", g.poolerID)
	if g.conn != nil {
		return g.conn.Close()
	}
	return nil
}

// Ensure grpcQueryService implements queryservice.QueryService
var _ queryservice.QueryService = (*grpcQueryService)(nil)
