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

	"github.com/multigres/multigres/go/multipooler/poolerserver"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/servenv"
)

// poolerService is the gRPC wrapper for MultiPooler
type poolerService struct {
	multipoolerpb.UnimplementedMultiPoolerServiceServer
	pooler *poolerserver.MultiPooler
}

func RegisterPoolerServices(senv *servenv.ServEnv, grpc *servenv.GrpcServer) {
	// Register ourselves to be invoked when the pooler starts
	poolerserver.RegisterPoolerServices = append(poolerserver.RegisterPoolerServices, func(p *poolerserver.MultiPooler) {
		if grpc.CheckServiceMap("pooler", senv) {
			srv := &poolerService{
				pooler: p,
			}
			multipoolerpb.RegisterMultiPoolerServiceServer(grpc.Server, srv)
		}
	})
}

// ExecuteQuery executes a SQL query and returns the result
func (s *poolerService) ExecuteQuery(ctx context.Context, req *multipoolerpb.ExecuteQueryRequest) (*multipoolerpb.ExecuteQueryResponse, error) {
	result, err := s.pooler.ExecuteQuery(ctx, req.Query, req.MaxRows)
	if err != nil {
		return nil, err
	}
	return &multipoolerpb.ExecuteQueryResponse{
		Result: result,
	}, nil
}
