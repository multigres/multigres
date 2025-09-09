/*
Copyright 2025 The Multigres Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MultiAdminServer implements the MultiAdminService gRPC interface
type MultiAdminServer struct {
	multiadminpb.UnimplementedMultiAdminServiceServer

	// ts is the topology store for querying cluster metadata
	ts topo.Store

	// logger for structured logging
	logger *slog.Logger

	// grpcServer is the gRPC server instance
	grpcServer *grpc.Server

	// listener is the network listener
	listener net.Listener
}

// NewMultiAdminServer creates a new MultiAdminServer instance
func NewMultiAdminServer(ts topo.Store, logger *slog.Logger) *MultiAdminServer {
	return &MultiAdminServer{
		ts:     ts,
		logger: logger,
	}
}

// Start starts the gRPC server on the specified port
func (s *MultiAdminServer) Start(port int) error {
	// Create listener
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %w", port, err)
	}
	s.listener = lis

	// Create gRPC server
	s.grpcServer = grpc.NewServer()

	// Register the service
	multiadminpb.RegisterMultiAdminServiceServer(s.grpcServer, s)

	s.logger.Info("MultiAdmin gRPC server starting", "port", port)

	// Start serving (this blocks)
	if err := s.grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("failed to serve gRPC: %w", err)
	}

	return nil
}

// Stop gracefully stops the gRPC server
func (s *MultiAdminServer) Stop() {
	if s.grpcServer != nil {
		s.logger.Info("Stopping MultiAdmin gRPC server")
		s.grpcServer.GracefulStop()
	}
	if s.listener != nil {
		s.listener.Close()
	}
}

// GetCell retrieves information about a specific cell
func (s *MultiAdminServer) GetCell(ctx context.Context, req *multiadminpb.GetCellRequest) (*multiadminpb.GetCellResponse, error) {
	s.logger.Debug("GetCell request received", "cell_name", req.Name)

	// Validate request
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "cell name cannot be empty")
	}

	// Get cell from topology
	cell, err := s.ts.GetCell(ctx, req.Name)
	if err != nil {
		s.logger.Error("Failed to get cell from topology", "cell_name", req.Name, "error", err)

		// Check if it's a not found error
		if errors.Is(err, &topo.TopoError{Code: topo.NoNode}) {
			return nil, status.Errorf(codes.NotFound, "cell '%s' not found", req.Name)
		}

		return nil, status.Errorf(codes.Internal, "failed to retrieve cell: %v", err)
	}

	// Return the response
	response := &multiadminpb.GetCellResponse{
		Cell: cell,
	}

	s.logger.Debug("GetCell request completed successfully", "cell_name", req.Name)
	return response, nil
}

// GetDatabase retrieves information about a specific database
func (s *MultiAdminServer) GetDatabase(ctx context.Context, req *multiadminpb.GetDatabaseRequest) (*multiadminpb.GetDatabaseResponse, error) {
	s.logger.Debug("GetDatabase request received", "database_name", req.Name)

	// Validate request
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "database name cannot be empty")
	}

	// Get database from topology
	database, err := s.ts.GetDatabase(ctx, req.Name)
	if err != nil {
		s.logger.Error("Failed to get database from topology", "database_name", req.Name, "error", err)

		// Check if it's a not found error
		if errors.Is(err, &topo.TopoError{Code: topo.NoNode}) {
			return nil, status.Errorf(codes.NotFound, "database '%s' not found", req.Name)
		}

		return nil, status.Errorf(codes.Internal, "failed to retrieve database: %v", err)
	}

	// Return the response
	response := &multiadminpb.GetDatabaseResponse{
		Database: database,
	}

	s.logger.Debug("GetDatabase request completed successfully", "database_name", req.Name)
	return response, nil
}
