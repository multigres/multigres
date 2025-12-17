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

// Package poolergateway handles selection and communication with multipooler instances.
// It is responsible for:
// - Discovering available poolers via PoolerDiscovery
// - Selecting healthy poolers for a given tablegroup
// - Managing gRPC connections to poolers
// - Providing QueryService instances for query execution
//
// This is analogous to Vitess's TabletGateway component.
package poolergateway

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/tools/grpccommon"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// PoolerDiscovery is the interface for discovering multipooler instances.
// This abstracts the PoolerDiscovery implementation for easier testing.
type PoolerDiscovery interface {
	// GetPooler returns a pooler matching the target specification.
	// Target specifies the tablegroup, shard, and pooler type to route to.
	// Returns nil if no matching pooler is found.
	GetPooler(target *query.Target) *clustermetadatapb.MultiPooler

	// PoolerCount returns the total number of discovered poolers.
	PoolerCount() int
}

// A Gateway is the query processing module for each shard,
// which is used by ScatterConn.
type Gateway interface {
	// the query service that this Gateway wraps around
	queryservice.QueryService

	// QueryServiceByID returns a QueryService
	QueryServiceByID(ctx context.Context, id *clustermetadatapb.ID, target *query.Target) (queryservice.QueryService, error)
}

// PoolerGateway selects and manages connections to multipooler instances.
type PoolerGateway struct {
	// discovery is used to find available poolers
	discovery PoolerDiscovery

	// logger for debugging
	logger *slog.Logger

	// connections maintains gRPC connections to poolers
	// Key is pooler ID (hostname:port)
	mu          sync.Mutex
	connections map[string]*poolerConnection
}

// poolerConnection represents a connection to a single multipooler instance
type poolerConnection struct {
	// poolerInfo contains the pooler metadata
	poolerInfo *topoclient.MultiPoolerInfo

	// conn is the gRPC connection
	conn *grpc.ClientConn

	// queryService is the QueryService implementation for query execution
	queryService queryservice.QueryService

	// serviceClient is the gRPC client for admin operations (auth, health, etc.)
	serviceClient multipoolerpb.MultiPoolerServiceClient

	// lastUsed tracks when this connection was last used
	lastUsed time.Time
}

// NewPoolerGateway creates a new PoolerGateway.
func NewPoolerGateway(
	discovery PoolerDiscovery,
	logger *slog.Logger,
) *PoolerGateway {
	return &PoolerGateway{
		discovery:   discovery,
		logger:      logger,
		connections: make(map[string]*poolerConnection),
	}
}

// QueryServiceByID implements Gateway.
func (pg *PoolerGateway) QueryServiceByID(ctx context.Context, id *clustermetadatapb.ID, target *query.Target) (queryservice.QueryService, error) {
	// TODO: IMPLEMENT queryservicebyid
	return pg, nil
}

// StreamExecute implements queryservice.QueryService.
// It routes the query to the appropriate multipooler instance based on the target.
//
// This method:
// 1. Uses discovery to find a pooler matching the target specification
// 2. Establishes gRPC connection if needed
// 3. Delegates to the pooler's QueryService for execution
//
// The target specifies:
// - TableGroup: Required
// - PoolerType: PRIMARY (writes), REPLICA (reads), etc. Defaults to PRIMARY if not set.
// - Shard: Optional, empty matches any shard
func (pg *PoolerGateway) StreamExecute(
	ctx context.Context,
	target *query.Target,
	sql string,
	options *query.ExecuteOptions,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	// Get a pooler matching the target
	queryService, err := pg.getQueryServiceForTarget(ctx, target)
	if err != nil {
		return err
	}

	// Delegate to the pooler's QueryService
	return queryService.StreamExecute(ctx, target, sql, options, callback)
}

func (pg *PoolerGateway) getQueryServiceForTarget(ctx context.Context, target *query.Target) (queryservice.QueryService, error) {
	pooler := pg.discovery.GetPooler(target)
	if pooler == nil {
		return nil, fmt.Errorf("no pooler found for target: tablegroup=%s, shard=%s, type=%s",
			target.TableGroup, target.Shard, target.PoolerType.String())
	}

	poolerID := topoclient.MultiPoolerIDString(pooler.Id)

	pg.logger.DebugContext(ctx, "selected pooler for target",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"pooler_id", poolerID,
		"actual_pooler_type", pooler.Type.String())

	// Get or create connection to this pooler
	queryService, err := pg.getOrCreateConnection(ctx, pooler)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection to pooler %s: %w", poolerID, err)
	}
	return queryService, nil
}

// ExecuteQuery implements queryservice.QueryService.
// It routes the query to the appropriate multipooler instance based on the target.
// This should be used sparingly only when we know the result set is small,
// otherwise StreamExecute should be used.
func (pg *PoolerGateway) ExecuteQuery(ctx context.Context, target *query.Target, sql string, options *query.ExecuteOptions) (*sqltypes.Result, error) {
	// Get a pooler matching the target
	queryService, err := pg.getQueryServiceForTarget(ctx, target)
	if err != nil {
		return nil, err
	}

	// Delegate to the pooler's QueryService
	return queryService.ExecuteQuery(ctx, target, sql, options)
}

// getOrCreateGRPCConn returns an existing gRPC connection or creates a new one.
// This is the core connection management that can be used for any gRPC service.
func (pg *PoolerGateway) getOrCreateGRPCConn(
	ctx context.Context,
	pooler *clustermetadatapb.MultiPooler,
) (*grpc.ClientConn, error) {
	poolerID := topoclient.MultiPoolerIDString(pooler.Id)

	// Check if we already have a connection
	pg.mu.Lock()
	if conn, ok := pg.connections[poolerID]; ok {
		pg.mu.Unlock()
		conn.lastUsed = time.Now()
		return conn.conn, nil
	}
	pg.mu.Unlock()

	// Need to create a new connection
	pg.mu.Lock()
	defer pg.mu.Unlock()

	// Double-check after acquiring write lock
	if conn, ok := pg.connections[poolerID]; ok {
		conn.lastUsed = time.Now()
		return conn.conn, nil
	}

	// Create new gRPC connection
	poolerInfo := &topoclient.MultiPoolerInfo{MultiPooler: pooler}
	addr := poolerInfo.Addr()

	pg.logger.InfoContext(ctx, "creating new gRPC connection to pooler",
		"pooler_id", poolerID,
		"addr", addr)

	// Create gRPC connection (non-blocking in newer gRPC)
	conn, err := grpccommon.NewClient(addr,
		grpccommon.WithAttributes(rpcclient.PoolerSpanAttributes(pooler.Id)...),
		grpccommon.WithDialOptions(grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create client for pooler %s at %s: %w", poolerID, addr, err)
	}

	// Create QueryService for the connection
	queryService := newGRPCQueryService(conn, poolerID, pg.logger)

	// Create service client for admin operations
	serviceClient := multipoolerpb.NewMultiPoolerServiceClient(conn)

	// Store connection
	pg.connections[poolerID] = &poolerConnection{
		poolerInfo:    poolerInfo,
		conn:          conn,
		queryService:  queryService,
		serviceClient: serviceClient,
		lastUsed:      time.Now(),
	}

	pg.logger.InfoContext(ctx, "gRPC connection established",
		"pooler_id", poolerID,
		"addr", addr)

	return conn, nil
}

// getOrCreateConnection returns an existing QueryService or creates a new one.
func (pg *PoolerGateway) getOrCreateConnection(
	ctx context.Context,
	pooler *clustermetadatapb.MultiPooler,
) (queryservice.QueryService, error) {
	poolerID := topoclient.MultiPoolerIDString(pooler.Id)

	// Ensure we have a connection
	_, err := pg.getOrCreateGRPCConn(ctx, pooler)
	if err != nil {
		return nil, err
	}

	// Return the QueryService from the cached connection
	pg.mu.Lock()
	defer pg.mu.Unlock()
	return pg.connections[poolerID].queryService, nil
}

// PortalStreamExecute implements queryservice.QueryService.
// It executes a portal and returns reservation information.
func (pg *PoolerGateway) PortalStreamExecute(
	ctx context.Context,
	target *query.Target,
	preparedStatement *query.PreparedStatement,
	portal *query.Portal,
	options *query.ExecuteOptions,
	callback func(context.Context, *sqltypes.Result) error,
) (queryservice.ReservedState, error) {
	// Get a pooler matching the target
	queryService, err := pg.getQueryServiceForTarget(ctx, target)
	if err != nil {
		return queryservice.ReservedState{}, err
	}

	// Delegate to the pooler's QueryService
	return queryService.PortalStreamExecute(ctx, target, preparedStatement, portal, options, callback)
}

// Describe implements queryservice.QueryService.
// It returns metadata about a prepared statement or portal.
func (pg *PoolerGateway) Describe(
	ctx context.Context,
	target *query.Target,
	preparedStatement *query.PreparedStatement,
	portal *query.Portal,
	options *query.ExecuteOptions,
) (*query.StatementDescription, error) {
	// Get a pooler matching the target
	queryService, err := pg.getQueryServiceForTarget(ctx, target)
	if err != nil {
		return nil, err
	}

	// Delegate to the pooler's QueryService
	return queryService.Describe(ctx, target, preparedStatement, portal, options)
}

// Close implements queryservice.QueryService.
// It closes all connections to poolers.
func (pg *PoolerGateway) Close(ctx context.Context) error {
	pg.mu.Lock()
	defer pg.mu.Unlock()

	pg.logger.InfoContext(ctx, "closing all pooler connections", "count", len(pg.connections))

	var lastErr error
	for poolerID, conn := range pg.connections {
		pg.logger.DebugContext(ctx, "closing connection", "pooler_id", poolerID)
		if err := conn.queryService.Close(ctx); err != nil {
			pg.logger.ErrorContext(ctx, "failed to close connection",
				"pooler_id", poolerID,
				"error", err)
			lastErr = err
		}
	}

	// Clear connections map
	pg.connections = make(map[string]*poolerConnection)

	return lastErr
}

// Ensure PoolerGateway implements Gateway
var _ Gateway = (*PoolerGateway)(nil)

// getPoolerServiceClient returns a MultiPoolerServiceClient for the given database.
// This can be used for authentication or other admin operations.
// It finds any available pooler and returns a client connected to it.
func (pg *PoolerGateway) getPoolerServiceClient(ctx context.Context, database string) (multipoolerpb.MultiPoolerServiceClient, error) {
	// Find any pooler - for authentication we just need access to pg_authid
	// which is available from any pooler connected to this database.
	// Use PRIMARY type as it's always available.
	target := &query.Target{
		TableGroup: "default", // TODO: Make configurable or discover from database
		PoolerType: clustermetadatapb.PoolerType_PRIMARY,
	}

	pooler := pg.discovery.GetPooler(target)
	if pooler == nil {
		return nil, fmt.Errorf("no pooler found for database %q", database)
	}

	poolerID := topoclient.MultiPoolerIDString(pooler.Id)

	// Get or create the gRPC connection (this also caches the service client)
	_, err := pg.getOrCreateGRPCConn(ctx, pooler)
	if err != nil {
		return nil, err
	}

	// Return the cached service client
	pg.mu.Lock()
	defer pg.mu.Unlock()
	return pg.connections[poolerID].serviceClient, nil
}

// PoolerClientFunc returns a function that can be used with auth.PoolerHashProvider.
// The returned function discovers an available pooler and returns a client for it.
func (pg *PoolerGateway) PoolerClientFunc() func(ctx context.Context, database string) (multipoolerpb.MultiPoolerServiceClient, error) {
	return pg.getPoolerServiceClient
}

// Stats returns statistics about the gateway.
func (pg *PoolerGateway) Stats() map[string]any {
	pg.mu.Lock()
	defer pg.mu.Unlock()

	return map[string]any{
		"active_connections": len(pg.connections),
		"poolers_discovered": pg.discovery.PoolerCount(),
	}
}
