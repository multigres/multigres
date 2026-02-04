// Copyright 2026 Supabase, Inc.
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

// TODO: Add PoolerConnection tests. This requires either:
// - A mock queryservice.QueryService implementation for unit tests
// - Integration tests with a real multipooler instance
// Tests should cover: connection lifecycle, query delegation, error handling

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/tools/grpccommon"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// PoolerConnection manages a single gRPC connection to a multipooler instance.
// It wraps a QueryService and provides access to pooler metadata.
//
// A PoolerConnection exists if and only if we are actively connected to the pooler.
// The LoadBalancer creates and destroys PoolerConnections based on discovery events.
type PoolerConnection struct {
	// mu protects poolerInfo from concurrent access
	mu sync.Mutex

	// poolerInfo contains the pooler metadata from discovery
	poolerInfo *topoclient.MultiPoolerInfo

	// conn is the underlying gRPC connection
	conn *grpc.ClientConn

	// queryService handles query execution over gRPC
	queryService queryservice.QueryService

	// serviceClient is the gRPC client for admin operations (auth, health, etc.)
	serviceClient multipoolerpb.MultiPoolerServiceClient

	// logger for debugging
	logger *slog.Logger
}

// NewPoolerConnection creates a new connection to a multipooler instance.
// Returns an error if the gRPC connection cannot be established.
func NewPoolerConnection(
	pooler *clustermetadatapb.MultiPooler,
	logger *slog.Logger,
) (*PoolerConnection, error) {
	poolerInfo := &topoclient.MultiPoolerInfo{MultiPooler: pooler}
	poolerID := topoclient.MultiPoolerIDString(pooler.Id)
	addr := poolerInfo.Addr()

	logger.Debug("creating pooler connection",
		"pooler_id", poolerID,
		"addr", addr,
		"type", pooler.Type.String())

	// Create gRPC connection with telemetry attributes
	conn, err := grpccommon.NewClient(addr,
		grpccommon.WithAttributes(rpcclient.PoolerSpanAttributes(pooler.Id)...),
		grpccommon.WithDialOptions(grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC client for pooler %s at %s: %w", poolerID, addr, err)
	}

	// Create QueryService wrapper
	queryService := newGRPCQueryService(conn, poolerID, logger)

	// Create service client for admin operations
	serviceClient := multipoolerpb.NewMultiPoolerServiceClient(conn)

	pc := &PoolerConnection{
		poolerInfo:    poolerInfo,
		conn:          conn,
		queryService:  queryService,
		serviceClient: serviceClient,
		logger:        logger,
	}

	return pc, nil
}

// ID returns the unique identifier for this pooler connection.
func (pc *PoolerConnection) ID() string {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return topoclient.MultiPoolerIDString(pc.poolerInfo.Id)
}

// Cell returns the cell where this pooler is located.
func (pc *PoolerConnection) Cell() string {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.poolerInfo.Id.GetCell()
}

// Type returns the pooler type (PRIMARY or REPLICA).
func (pc *PoolerConnection) Type() clustermetadatapb.PoolerType {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.poolerInfo.Type
}

// PoolerInfo returns the underlying pooler metadata.
func (pc *PoolerConnection) PoolerInfo() *topoclient.MultiPoolerInfo {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.poolerInfo
}

// ServiceClient returns the MultiPoolerServiceClient for admin operations.
// This can be used for authentication, health checks, and other system-level operations.
func (pc *PoolerConnection) ServiceClient() multipoolerpb.MultiPoolerServiceClient {
	return pc.serviceClient
}

// QueryService returns the QueryService for executing queries on this pooler.
// This is the primary interface for query execution and should be used by callers
// who need to route queries to this specific pooler instance.
func (pc *PoolerConnection) QueryService() queryservice.QueryService {
	return pc.queryService
}

// UpdateMetadata updates the pooler metadata without recreating the gRPC connection.
// This is used when pooler properties change (e.g., type changes from REPLICA to PRIMARY during promotion).
func (pc *PoolerConnection) UpdateMetadata(pooler *clustermetadatapb.MultiPooler) {
	pc.mu.Lock()
	pc.poolerInfo = &topoclient.MultiPoolerInfo{MultiPooler: pooler}
	poolerID := topoclient.MultiPoolerIDString(pooler.Id)
	poolerType := pooler.Type.String()
	pc.mu.Unlock()

	pc.logger.Debug("updated pooler metadata",
		"pooler_id", poolerID,
		"type", poolerType)
}

// Close closes the gRPC connection to the pooler.
func (pc *PoolerConnection) Close() error {
	poolerID := pc.ID()
	pc.logger.Debug("closing pooler connection", "pooler_id", poolerID)

	if err := pc.queryService.Close(); err != nil {
		return fmt.Errorf("failed to close query service for pooler %s: %w", poolerID, err)
	}
	return nil
}
