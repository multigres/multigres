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

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/multipooler/queryservice"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/query"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// PoolerDiscovery is the interface for discovering multipooler instances.
// This abstracts the PoolerDiscovery implementation for easier testing.
type PoolerDiscovery interface {
	// GetPoolers returns all discovered poolers
	GetPoolers() []*clustermetadatapb.MultiPooler

	// GetPooler returns a pooler matching the target specification.
	// Target specifies the tablegroup, shard, and pooler type to route to.
	// Returns nil if no matching pooler is found.
	GetPooler(target *query.Target) *clustermetadatapb.MultiPooler
}

// PoolerGateway selects and manages connections to multipooler instances.
type PoolerGateway struct {
	// discovery is used to find available poolers
	discovery PoolerDiscovery

	// logger for debugging
	logger *slog.Logger

	// connections maintains gRPC connections to poolers
	// Key is pooler ID (hostname:port)
	mu          sync.RWMutex
	connections map[string]*poolerConnection
}

// poolerConnection represents a connection to a single multipooler instance
type poolerConnection struct {
	// poolerInfo contains the pooler metadata
	poolerInfo *topo.MultiPoolerInfo

	// conn is the gRPC connection
	conn *grpc.ClientConn

	// queryService is the QueryService implementation
	queryService queryservice.QueryService

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
	callback func(*query.QueryResult) error,
) error {
	// Get a pooler matching the target
	pooler := pg.discovery.GetPooler(target)
	if pooler == nil {
		return fmt.Errorf("no pooler found for target: tablegroup=%s, shard=%s, type=%s",
			target.TableGroup, target.Shard, target.PoolerType.String())
	}

	poolerID := topo.MultiPoolerIDString(pooler.Id)

	pg.logger.Debug("selected pooler for target",
		"tablegroup", target.TableGroup,
		"shard", target.Shard,
		"pooler_type", target.PoolerType.String(),
		"pooler_id", poolerID,
		"actual_pooler_type", pooler.Type.String())

	// Get or create connection to this pooler
	queryService, err := pg.getOrCreateConnection(ctx, pooler)
	if err != nil {
		return fmt.Errorf("failed to get connection to pooler %s: %w", poolerID, err)
	}

	// Delegate to the pooler's QueryService
	return queryService.StreamExecute(ctx, target, sql, callback)
}

// getOrCreateConnection returns an existing connection or creates a new one.
func (pg *PoolerGateway) getOrCreateConnection(
	ctx context.Context,
	pooler *clustermetadatapb.MultiPooler,
) (queryservice.QueryService, error) {
	poolerID := topo.MultiPoolerIDString(pooler.Id)

	// Check if we already have a connection
	pg.mu.RLock()
	if conn, ok := pg.connections[poolerID]; ok {
		pg.mu.RUnlock()
		conn.lastUsed = time.Now()
		return conn.queryService, nil
	}
	pg.mu.RUnlock()

	// Need to create a new connection
	pg.mu.Lock()
	defer pg.mu.Unlock()

	// Double-check after acquiring write lock
	if conn, ok := pg.connections[poolerID]; ok {
		conn.lastUsed = time.Now()
		return conn.queryService, nil
	}

	// Create new gRPC connection
	poolerInfo := &topo.MultiPoolerInfo{MultiPooler: pooler}
	addr := poolerInfo.Addr()

	pg.logger.Info("creating new gRPC connection to pooler",
		"pooler_id", poolerID,
		"addr", addr)

	// Create gRPC connection (non-blocking in newer gRPC)
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create client for pooler %s at %s: %w", poolerID, addr, err)
	}

	// Create QueryService
	queryService := newGRPCQueryService(conn, poolerID, pg.logger)

	// Store connection
	pg.connections[poolerID] = &poolerConnection{
		poolerInfo:   poolerInfo,
		conn:         conn,
		queryService: queryService,
		lastUsed:     time.Now(),
	}

	pg.logger.Info("gRPC connection established",
		"pooler_id", poolerID,
		"addr", addr)

	return queryService, nil
}

// Close implements queryservice.QueryService.
// It closes all connections to poolers.
func (pg *PoolerGateway) Close(ctx context.Context) error {
	pg.mu.Lock()
	defer pg.mu.Unlock()

	pg.logger.Info("closing all pooler connections", "count", len(pg.connections))

	var lastErr error
	for poolerID, conn := range pg.connections {
		pg.logger.Debug("closing connection", "pooler_id", poolerID)
		if err := conn.queryService.Close(ctx); err != nil {
			pg.logger.Error("failed to close connection",
				"pooler_id", poolerID,
				"error", err)
			lastErr = err
		}
	}

	// Clear connections map
	pg.connections = make(map[string]*poolerConnection)

	return lastErr
}

// Ensure PoolerGateway implements queryservice.QueryService
var _ queryservice.QueryService = (*PoolerGateway)(nil)

// Stats returns statistics about the gateway.
func (pg *PoolerGateway) Stats() map[string]any {
	pg.mu.RLock()
	defer pg.mu.RUnlock()

	return map[string]any{
		"active_connections": len(pg.connections),
		"poolers_discovered": len(pg.discovery.GetPoolers()),
	}
}
