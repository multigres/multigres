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

// Package server implements the MultiAdmin gRPC service for multigres cluster administration.
// It provides administrative operations for managing and querying cluster components including
// cells, databases, gateways, poolers, and orchestrators through a unified gRPC interface.
package multiadmin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MultiAdminServer implements the MultiAdminService gRPC interface
type MultiAdminServer struct {
	multiadminpb.UnimplementedMultiAdminServiceServer

	// ts is the topology store for querying cluster metadata
	ts topoclient.Store

	// logger for structured logging
	logger *slog.Logger

	// backupJobTracker manages async backup/restore jobs
	backupJobTracker *BackupJobTracker

	// rpcClient is the client for communicating with multipooler nodes
	rpcClient rpcclient.MultiPoolerClient
}

// NewMultiAdminServer creates a new MultiAdminServer instance
func NewMultiAdminServer(ts topoclient.Store, logger *slog.Logger) *MultiAdminServer {
	return &MultiAdminServer{
		ts:               ts,
		logger:           logger,
		backupJobTracker: NewBackupJobTracker(),
		rpcClient:        rpcclient.NewMultiPoolerClient(100),
	}
}

// RegisterWithGRPCServer registers the MultiAdmin service with the provided gRPC server
func (s *MultiAdminServer) RegisterWithGRPCServer(grpcServer *grpc.Server) {
	multiadminpb.RegisterMultiAdminServiceServer(grpcServer, s)
	s.logger.Info("MultiAdmin service registered with gRPC server")
}

// Stop stops background goroutines and releases resources
func (s *MultiAdminServer) Stop() {
	s.backupJobTracker.Stop()
}

// SetRPCClient sets the RPC client for communicating with multipoolers.
// This is primarily used for testing to inject a fake client.
func (s *MultiAdminServer) SetRPCClient(client rpcclient.MultiPoolerClient) {
	s.rpcClient = client
}

// GetCell retrieves information about a specific cell
func (s *MultiAdminServer) GetCell(ctx context.Context, req *multiadminpb.GetCellRequest) (*multiadminpb.GetCellResponse, error) {
	s.logger.DebugContext(ctx, "GetCell request received", "cell_name", req.Name)

	// Validate request
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "cell name cannot be empty")
	}

	// Get cell from topology
	cell, err := s.ts.GetCell(ctx, req.Name)
	if err != nil {
		s.logger.ErrorContext(ctx, "Failed to get cell from topology", "cell_name", req.Name, "error", err)

		// Check if it's a not found error
		if errors.Is(err, &topoclient.TopoError{Code: topoclient.NoNode}) {
			return nil, status.Errorf(codes.NotFound, "cell '%s' not found", req.Name)
		}

		return nil, status.Errorf(codes.Internal, "failed to retrieve cell: %v", err)
	}

	// Return the response
	response := &multiadminpb.GetCellResponse{
		Cell: cell,
	}

	s.logger.DebugContext(ctx, "GetCell request completed successfully", "cell_name", req.Name)
	return response, nil
}

// GetDatabase retrieves information about a specific database
func (s *MultiAdminServer) GetDatabase(ctx context.Context, req *multiadminpb.GetDatabaseRequest) (*multiadminpb.GetDatabaseResponse, error) {
	s.logger.DebugContext(ctx, "GetDatabase request received", "database_name", req.Name)

	// Validate request
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "database name cannot be empty")
	}

	// Get database from topology
	database, err := s.ts.GetDatabase(ctx, req.Name)
	if err != nil {
		s.logger.ErrorContext(ctx, "Failed to get database from topology", "database_name", req.Name, "error", err)

		// Check if it's a not found error
		if errors.Is(err, &topoclient.TopoError{Code: topoclient.NoNode}) {
			return nil, status.Errorf(codes.NotFound, "database '%s' not found", req.Name)
		}

		return nil, status.Errorf(codes.Internal, "failed to retrieve database: %v", err)
	}

	// Return the response
	response := &multiadminpb.GetDatabaseResponse{
		Database: database,
	}

	s.logger.DebugContext(ctx, "GetDatabase request completed successfully", "database_name", req.Name)
	return response, nil
}

// GetCellNames retrieves all cell names in the cluster
func (s *MultiAdminServer) GetCellNames(ctx context.Context, req *multiadminpb.GetCellNamesRequest) (*multiadminpb.GetCellNamesResponse, error) {
	s.logger.DebugContext(ctx, "GetCellNames request received")

	names, err := s.ts.GetCellNames(ctx)
	if err != nil {
		s.logger.ErrorContext(ctx, "Failed to get cell names from topology", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to retrieve cell names: %v", err)
	}

	response := &multiadminpb.GetCellNamesResponse{
		Names: names,
	}

	s.logger.DebugContext(ctx, "GetCellNames request completed successfully", "count", len(names))
	return response, nil
}

// GetDatabaseNames retrieves all database names in the cluster
func (s *MultiAdminServer) GetDatabaseNames(ctx context.Context, req *multiadminpb.GetDatabaseNamesRequest) (*multiadminpb.GetDatabaseNamesResponse, error) {
	s.logger.DebugContext(ctx, "GetDatabaseNames request received")

	names, err := s.ts.GetDatabaseNames(ctx)
	if err != nil {
		s.logger.ErrorContext(ctx, "Failed to get database names from topology", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to retrieve database names: %v", err)
	}

	response := &multiadminpb.GetDatabaseNamesResponse{
		Names: names,
	}

	s.logger.DebugContext(ctx, "GetDatabaseNames request completed successfully", "count", len(names))
	return response, nil
}

// GetGateways retrieves gateways filtered by cells
func (s *MultiAdminServer) GetGateways(ctx context.Context, req *multiadminpb.GetGatewaysRequest) (*multiadminpb.GetGatewaysResponse, error) {
	s.logger.DebugContext(ctx, "GetGateways request received", "cells", req.Cells)

	// Determine which cells to query
	cellsToQuery := req.Cells
	if len(cellsToQuery) == 0 {
		// If no cells specified, get all cells
		allCells, err := s.ts.GetCellNames(ctx)
		if err != nil {
			s.logger.ErrorContext(ctx, "Failed to get all cell names", "error", err)
			return nil, status.Errorf(codes.Internal, "failed to retrieve cell names: %v", err)
		}
		cellsToQuery = allCells
	}

	var allGateways []*clustermetadatapb.MultiGateway
	var errors []error

	// Query each cell for gateways
	for _, cellName := range cellsToQuery {
		gatewayInfos, err := s.ts.GetMultiGatewaysByCell(ctx, cellName)
		if err != nil {
			s.logger.ErrorContext(ctx, "Failed to get gateways for cell", "cell", cellName, "error", err)
			errors = append(errors, fmt.Errorf("failed to get gateways for cell %s: %w", cellName, err))
			continue
		}

		// Convert to protobuf
		for _, info := range gatewayInfos {
			gateway := info.MultiGateway
			allGateways = append(allGateways, gateway)
		}
	}

	response := &multiadminpb.GetGatewaysResponse{
		Gateways: allGateways,
	}

	// Return partial results with error if some cells failed
	if len(errors) > 0 {
		s.logger.DebugContext(ctx, "GetGateways request completed with partial results", "count", len(allGateways), "errors", len(errors))
		return response, fmt.Errorf("partial results returned due to errors in %d cell(s): %v", len(errors), errors)
	}

	s.logger.DebugContext(ctx, "GetGateways request completed successfully", "count", len(allGateways))
	return response, nil
}

// GetPoolers retrieves poolers filtered by cells and/or database
func (s *MultiAdminServer) GetPoolers(ctx context.Context, req *multiadminpb.GetPoolersRequest) (*multiadminpb.GetPoolersResponse, error) {
	s.logger.DebugContext(ctx, "GetPoolers request received", "cells", req.Cells, "database", req.Database)

	// Determine which cells to query
	cellsToQuery := req.Cells
	if len(cellsToQuery) == 0 {
		// If no cells specified, get all cells
		allCells, err := s.ts.GetCellNames(ctx)
		if err != nil {
			s.logger.ErrorContext(ctx, "Failed to get all cell names", "error", err)
			return nil, status.Errorf(codes.Internal, "failed to retrieve cell names: %v", err)
		}
		cellsToQuery = allCells
	}

	var allPoolers []*clustermetadatapb.MultiPooler
	var errors []error

	// Query each cell for poolers
	for _, cellName := range cellsToQuery {
		var opts *topoclient.GetMultiPoolersByCellOptions
		// filter by database and shard if specified
		if req.Database != "" {
			opts = &topoclient.GetMultiPoolersByCellOptions{
				DatabaseShard: &topoclient.DatabaseShard{
					Database: req.Database,
					Shard:    req.Shard,
				},
			}
		}
		poolerInfos, err := s.ts.GetMultiPoolersByCell(ctx, cellName, opts)
		if err != nil {
			s.logger.ErrorContext(ctx, "Failed to get poolers for cell", "cell", cellName, "error", err)
			errors = append(errors, fmt.Errorf("failed to get poolers for cell %s: %w", cellName, err))
			continue
		}

		// Convert to protobuf
		for _, info := range poolerInfos {
			pooler := info.MultiPooler
			allPoolers = append(allPoolers, pooler)
		}
	}

	response := &multiadminpb.GetPoolersResponse{
		Poolers: allPoolers,
	}

	// Return partial results with error if some cells failed
	if len(errors) > 0 {
		s.logger.DebugContext(ctx, "GetPoolers request completed with partial results", "count", len(allPoolers), "errors", len(errors))
		return response, fmt.Errorf("partial results returned due to errors in %d cell(s): %v", len(errors), errors)
	}

	s.logger.DebugContext(ctx, "GetPoolers request completed successfully", "count", len(allPoolers))
	return response, nil
}

// GetOrchs retrieves orchestrators filtered by cells
func (s *MultiAdminServer) GetOrchs(ctx context.Context, req *multiadminpb.GetOrchsRequest) (*multiadminpb.GetOrchsResponse, error) {
	s.logger.DebugContext(ctx, "GetOrchs request received", "cells", req.Cells)

	// Determine which cells to query
	cellsToQuery := req.Cells
	if len(cellsToQuery) == 0 {
		// If no cells specified, get all cells
		allCells, err := s.ts.GetCellNames(ctx)
		if err != nil {
			s.logger.ErrorContext(ctx, "Failed to get all cell names", "error", err)
			return nil, status.Errorf(codes.Internal, "failed to retrieve cell names: %v", err)
		}
		cellsToQuery = allCells
	}

	var allOrchs []*clustermetadatapb.MultiOrch
	var errors []error

	// Query each cell for orchestrators
	for _, cellName := range cellsToQuery {
		orchInfos, err := s.ts.GetMultiOrchsByCell(ctx, cellName)
		if err != nil {
			s.logger.ErrorContext(ctx, "Failed to get orchestrators for cell", "cell", cellName, "error", err)
			errors = append(errors, fmt.Errorf("failed to get orchestrators for cell %s: %w", cellName, err))
			continue
		}

		// Convert to protobuf
		for _, info := range orchInfos {
			orch := info.MultiOrch
			allOrchs = append(allOrchs, orch)
		}
	}

	response := &multiadminpb.GetOrchsResponse{
		Orchs: allOrchs,
	}

	// Return partial results with error if some cells failed
	if len(errors) > 0 {
		s.logger.DebugContext(ctx, "GetOrchs request completed with partial results", "count", len(allOrchs), "errors", len(errors))
		return response, fmt.Errorf("partial results returned due to errors in %d cell(s): %v", len(errors), errors)
	}

	s.logger.DebugContext(ctx, "GetOrchs request completed successfully", "count", len(allOrchs))
	return response, nil
}

// GetPoolerStatus retrieves the unified status of a specific pooler by proxying
// the request to the target pooler's MultiPoolerManager.Status RPC.
func (s *MultiAdminServer) GetPoolerStatus(ctx context.Context, req *multiadminpb.GetPoolerStatusRequest) (*multiadminpb.GetPoolerStatusResponse, error) {
	// Validate request
	if req.PoolerId == nil {
		return nil, status.Error(codes.InvalidArgument, "pooler_id cannot be empty")
	}
	if req.PoolerId.Cell == "" || req.PoolerId.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "pooler_id must have both cell and name")
	}

	// Create a fully-qualified pooler ID for topology lookup
	poolerID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      req.PoolerId.Cell,
		Name:      req.PoolerId.Name,
	}

	// Get pooler from topology
	poolerInfo, err := s.ts.GetMultiPooler(ctx, poolerID)
	if err != nil {
		s.logger.ErrorContext(ctx, "Failed to get pooler from topology", "pooler_id", req.PoolerId, "error", err)

		if errors.Is(err, &topoclient.TopoError{Code: topoclient.NoNode}) {
			return nil, status.Errorf(codes.NotFound, "pooler '%s/%s' not found", req.PoolerId.Cell, req.PoolerId.Name)
		}

		return nil, status.Errorf(codes.Internal, "failed to retrieve pooler: %v", err)
	}

	// Call Status RPC on the pooler
	statusResp, err := s.rpcClient.Status(ctx, poolerInfo.MultiPooler, &multipoolermanagerdatapb.StatusRequest{})
	if err != nil {
		s.logger.ErrorContext(ctx, "Failed to get status from pooler", "pooler_id", req.PoolerId, "error", err)
		return nil, status.Errorf(codes.Unavailable, "failed to get status from pooler: %v", err)
	}

	return &multiadminpb.GetPoolerStatusResponse{
		Status: statusResp.Status,
	}, nil
}

// EnablePostgresMonitor enables PostgreSQL monitoring on a specific pooler by proxying
// the request to the target pooler's MultiPoolerManager.EnableMonitor RPC.
func (s *MultiAdminServer) EnablePostgresMonitor(ctx context.Context, req *multiadminpb.EnablePostgresMonitorRequest) (*multiadminpb.EnablePostgresMonitorResponse, error) {
	// Validate request
	if req.PoolerId == nil {
		return nil, status.Error(codes.InvalidArgument, "pooler_id cannot be empty")
	}
	if req.PoolerId.Cell == "" || req.PoolerId.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "pooler_id must have both cell and name")
	}

	// Create a fully-qualified pooler ID for topology lookup
	poolerID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      req.PoolerId.Cell,
		Name:      req.PoolerId.Name,
	}

	// Get pooler from topology
	poolerInfo, err := s.ts.GetMultiPooler(ctx, poolerID)
	if err != nil {
		s.logger.ErrorContext(ctx, "Failed to get pooler from topology", "pooler_id", req.PoolerId, "error", err)

		if errors.Is(err, &topoclient.TopoError{Code: topoclient.NoNode}) {
			return nil, status.Errorf(codes.NotFound, "pooler '%s/%s' not found", req.PoolerId.Cell, req.PoolerId.Name)
		}

		return nil, status.Errorf(codes.Internal, "failed to retrieve pooler: %v", err)
	}

	// Call EnableMonitor RPC on the pooler
	_, err = s.rpcClient.EnableMonitor(ctx, poolerInfo.MultiPooler, &multipoolermanagerdatapb.EnableMonitorRequest{})
	if err != nil {
		s.logger.ErrorContext(ctx, "Failed to enable monitor on pooler", "pooler_id", req.PoolerId, "error", err)
		return nil, status.Errorf(codes.Unavailable, "failed to enable PostgreSQL monitoring on pooler: %v", err)
	}

	return &multiadminpb.EnablePostgresMonitorResponse{}, nil
}

// DisablePostgresMonitor disables PostgreSQL monitoring on a specific pooler by proxying
// the request to the target pooler's MultiPoolerManager.DisableMonitor RPC.
func (s *MultiAdminServer) DisablePostgresMonitor(ctx context.Context, req *multiadminpb.DisablePostgresMonitorRequest) (*multiadminpb.DisablePostgresMonitorResponse, error) {
	// Validate request
	if req.PoolerId == nil {
		return nil, status.Error(codes.InvalidArgument, "pooler_id cannot be empty")
	}
	if req.PoolerId.Cell == "" || req.PoolerId.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "pooler_id must have both cell and name")
	}

	// Create a fully-qualified pooler ID for topology lookup
	poolerID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      req.PoolerId.Cell,
		Name:      req.PoolerId.Name,
	}

	// Get pooler from topology
	poolerInfo, err := s.ts.GetMultiPooler(ctx, poolerID)
	if err != nil {
		s.logger.ErrorContext(ctx, "Failed to get pooler from topology", "pooler_id", req.PoolerId, "error", err)

		if errors.Is(err, &topoclient.TopoError{Code: topoclient.NoNode}) {
			return nil, status.Errorf(codes.NotFound, "pooler '%s/%s' not found", req.PoolerId.Cell, req.PoolerId.Name)
		}

		return nil, status.Errorf(codes.Internal, "failed to retrieve pooler: %v", err)
	}

	// Call DisableMonitor RPC on the pooler
	_, err = s.rpcClient.DisableMonitor(ctx, poolerInfo.MultiPooler, &multipoolermanagerdatapb.DisableMonitorRequest{})
	if err != nil {
		s.logger.ErrorContext(ctx, "Failed to disable monitor on pooler", "pooler_id", req.PoolerId, "error", err)
		return nil, status.Errorf(codes.Unavailable, "failed to disable PostgreSQL monitoring on pooler: %v", err)
	}

	return &multiadminpb.DisablePostgresMonitorResponse{}, nil
}
