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

	"github.com/multigres/multigres/go/multipooler/poolerserver"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	querypb "github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/servenv"
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
		return fmt.Errorf("executor not initialized")
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
