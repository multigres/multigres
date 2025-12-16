// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package grpcpoolerservice implements the gRPC server for MultiPooler
package grpcpoolerservice

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/multipooler/poolerserver"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	querypb "github.com/multigres/multigres/go/pb/query"
)

// poolerService is the gRPC wrapper for MultiPooler
type poolerService struct {
	multipoolerpb.UnimplementedMultiPoolerServiceServer
	pooler *poolerserver.QueryPoolerServer
}

func RegisterPoolerServices(senv *servenv.ServEnv, grpc *servenv.GrpcServer) {
	// Register ourselves to be invoked when the pooler starts
	poolerserver.RegisterPoolerServices = append(poolerserver.RegisterPoolerServices, func(p *poolerserver.QueryPoolerServer) {
		if grpc.CheckServiceMap("pooler", senv) {
			srv := &poolerService{
				pooler: p,
			}
			multipoolerpb.RegisterMultiPoolerServiceServer(grpc.Server, srv)
		}
	})
}

// StreamExecute executes a SQL query and streams the results back to the client.
// This is the main execution method used by multigateway.
func (s *poolerService) StreamExecute(req *multipoolerpb.StreamExecuteRequest, stream multipoolerpb.MultiPoolerService_StreamExecuteServer) error {
	// Get the executor from the pooler
	executor, err := s.pooler.Executor()
	if err != nil {
		return err
	}

	// Execute the query and stream results
	err = executor.StreamExecute(stream.Context(), req.Target, req.Query, nil, func(ctx context.Context, result *querypb.QueryResult) error {
		// Send the result back to the client
		response := &multipoolerpb.StreamExecuteResponse{
			Result: result,
		}
		return stream.Send(response)
	})

	return err
}

// ExecuteQuery executes a SQL query and returns the result
// This should be used sparingly only when we know the result set is small,
// otherwise StreamExecute should be used.
func (s *poolerService) ExecuteQuery(ctx context.Context, req *multipoolerpb.ExecuteQueryRequest) (*multipoolerpb.ExecuteQueryResponse, error) {
	// Get the executor from the pooler
	executor, err := s.pooler.Executor()
	if err != nil {
		return nil, fmt.Errorf("executor not initialized")
	}

	// Execute the query and stream results
	options := &querypb.ExecuteOptions{MaxRows: req.MaxRows}
	res, err := executor.ExecuteQuery(ctx, req.Target, req.Query, options)
	if err != nil {
		return nil, err
	}
	return &multipoolerpb.ExecuteQueryResponse{
		Result: res,
	}, nil
}

// GetAuthCredentials retrieves authentication credentials (SCRAM hash) for a PostgreSQL user.
// This is used by multigateway to authenticate clients using SCRAM-SHA-256.
func (s *poolerService) GetAuthCredentials(ctx context.Context, req *multipoolerpb.GetAuthCredentialsRequest) (*multipoolerpb.GetAuthCredentialsResponse, error) {
	// Validate request.
	if req.Username == "" {
		return nil, status.Error(codes.InvalidArgument, "username is required")
	}
	if req.Database == "" {
		return nil, status.Error(codes.InvalidArgument, "database is required")
	}

	// Check if pooler is initialized.
	if s.pooler == nil {
		return nil, status.Error(codes.Unavailable, "pooler not initialized")
	}

	// Get the executor from the pooler.
	executor, err := s.pooler.Executor()
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "executor not initialized: %v", err)
	}

	// Query pg_authid for the user's password hash.
	// Note: This requires superuser access to read pg_authid.
	query := fmt.Sprintf("SELECT rolpassword FROM pg_catalog.pg_authid WHERE rolname = '%s'",
		escapeString(req.Username))

	options := &querypb.ExecuteOptions{MaxRows: 1}
	result, err := executor.ExecuteQuery(ctx, nil, query, options)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to query credentials: %v", err)
	}

	// Check if user exists.
	if len(result.Rows) == 0 {
		return nil, status.Errorf(codes.NotFound, "user %q not found", req.Username)
	}

	// Extract the password hash.
	var scramHash string
	if len(result.Rows[0].Values) > 0 {
		scramHash = string(result.Rows[0].Values[0])
	}

	return &multipoolerpb.GetAuthCredentialsResponse{
		ScramHash: scramHash,
	}, nil
}

// escapeString escapes a string for safe use in SQL queries.
// This doubles single quotes for PostgreSQL.
func escapeString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// Describe returns metadata about a prepared statement or portal.
// Used by multigateway for the Extended Query Protocol.
func (s *poolerService) Describe(ctx context.Context, req *multipoolerpb.DescribeRequest) (*multipoolerpb.DescribeResponse, error) {
	// Get the executor from the pooler
	executor, err := s.pooler.Executor()
	if err != nil {
		return nil, fmt.Errorf("executor not initialized")
	}

	// Call the executor's Describe method
	desc, err := executor.Describe(ctx, req.Target, req.PreparedStatement, req.Portal, req.Options)
	if err != nil {
		return nil, err
	}

	return &multipoolerpb.DescribeResponse{
		Description: desc,
	}, nil
}

// PortalStreamExecute executes a portal (bound prepared statement) and streams results.
// Used by multigateway for the Extended Query Protocol.
func (s *poolerService) PortalStreamExecute(req *multipoolerpb.PortalStreamExecuteRequest, stream multipoolerpb.MultiPoolerService_PortalStreamExecuteServer) error {
	// Get the executor from the pooler
	executor, err := s.pooler.Executor()
	if err != nil {
		return err
	}

	// Execute the portal and stream results
	reservedState, err := executor.PortalStreamExecute(
		stream.Context(),
		req.Target,
		req.PreparedStatement,
		req.Portal,
		req.Options,
		func(ctx context.Context, result *querypb.QueryResult) error {
			// Send the result back to the client
			response := &multipoolerpb.PortalStreamExecuteResponse{
				Result: result,
			}
			return stream.Send(response)
		},
	)
	if err != nil {
		// Note: When PortalStreamExecute returns an error, it also releases any reserved
		// connection and returns an empty ReservedState. So we don't need to send a
		// reserved connection ID in the error case.
		return err
	}

	// Send final response with reserved connection ID if one was created
	if reservedState.ReservedConnectionId > 0 {
		return stream.Send(&multipoolerpb.PortalStreamExecuteResponse{
			ReservedConnectionId: reservedState.ReservedConnectionId,
		})
	}

	return nil
}
