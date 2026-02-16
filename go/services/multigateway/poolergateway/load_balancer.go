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

// Package poolergateway implements load balancing and connection management
// for multipooler instances. The LoadBalancer selects poolers based on target
// specifications (tablegroup, shard, type) with cell locality optimization.
//
// Design documentation: docs/query_serving/load_balancing.md
package poolergateway

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/pb/query"
)

// LoadBalancer manages PoolerConnections and selects connections for queries.
// It creates connections based on discovery events and destroys them when poolers
// are removed from discovery.
type LoadBalancer struct {
	// localCell is the cell where this gateway is running
	localCell string

	// logger for debugging
	logger *slog.Logger

	// mu protects the connections map
	mu sync.Mutex

	// connections maps pooler ID to PoolerConnection
	connections map[string]*PoolerConnection
}

// NewLoadBalancer creates a new LoadBalancer.
func NewLoadBalancer(localCell string, logger *slog.Logger) *LoadBalancer {
	return &LoadBalancer{
		localCell:   localCell,
		logger:      logger,
		connections: make(map[string]*PoolerConnection),
	}
}

// AddPooler creates or updates a PoolerConnection for the given pooler.
// If a connection already exists, its metadata is updated rather than reconnecting.
// This optimization is important during failover: when a replica is promoted to primary,
// we want to reuse the existing gRPC connection rather than creating a new one.
func (lb *LoadBalancer) AddPooler(pooler *clustermetadatapb.MultiPooler) error {
	poolerID := topoclient.MultiPoolerIDString(pooler.Id)

	lb.mu.Lock()
	defer lb.mu.Unlock()

	// Check if already exists - if so, update its metadata
	// This handles the case where a pooler's type changes (REPLICA -> PRIMARY during promotion)
	if existingConn, exists := lb.connections[poolerID]; exists {
		lb.logger.Debug("pooler connection already exists, updating metadata",
			"pooler_id", poolerID,
			"old_type", existingConn.Type().String(),
			"new_type", pooler.Type.String())
		existingConn.UpdateMetadata(pooler)
		return nil
	}

	// Create new connection
	conn, err := NewPoolerConnection(pooler, lb.logger)
	if err != nil {
		return fmt.Errorf("failed to create connection to pooler %s: %w", poolerID, err)
	}

	lb.connections[poolerID] = conn
	lb.logger.Debug("added pooler connection",
		"pooler_id", poolerID,
		"type", pooler.Type.String(),
		"cell", pooler.Id.GetCell())

	return nil
}

// RemovePooler closes and removes the PoolerConnection for the given pooler ID.
// If no connection exists for this pooler, it is a no-op.
func (lb *LoadBalancer) RemovePooler(poolerID *clustermetadatapb.ID) {
	if poolerID == nil {
		return
	}

	idStr := topoclient.MultiPoolerIDString(poolerID)

	lb.mu.Lock()
	conn, exists := lb.connections[idStr]
	if !exists {
		lb.mu.Unlock()
		return
	}
	delete(lb.connections, idStr)
	lb.mu.Unlock()

	// Close outside the lock
	if err := conn.Close(); err != nil {
		lb.logger.Error("error closing pooler connection",
			"pooler_id", idStr,
			"error", err)
	} else {
		lb.logger.Debug("removed pooler connection", "pooler_id", idStr)
	}
}

// GetConnection returns a PoolerConnection matching the target specification.
// Returns an error immediately if no suitable connection is available (fail-fast).
//
// Selection logic:
// - For PRIMARY: returns the primary pooler (any cell)
// - For REPLICA: prefers local cell, falls back to other cells
//
// This method never waits or retries. Retry logic should be implemented
// at a higher level in the stack (e.g., PoolerGateway).
func (lb *LoadBalancer) GetConnection(target *query.Target) (*PoolerConnection, error) {
	if target == nil {
		return nil, errors.New("target cannot be nil")
	}

	lb.mu.Lock()
	defer lb.mu.Unlock()

	// Collect candidates matching the target
	var candidates []*PoolerConnection
	for _, conn := range lb.connections {
		if matchesTarget(conn, target) {
			candidates = append(candidates, conn)
		}
	}

	if len(candidates) == 0 {
		// Return UNAVAILABLE error (like Vitess does for "no healthy tablet")
		// This makes the error retryable by the retry logic
		return nil, mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE,
			"no pooler found for target: tablegroup=%s, shard=%s, type=%s",
			target.TableGroup, target.Shard, target.PoolerType.String())
	}

	// Select best candidate
	return lb.selectConnection(candidates, target), nil
}

// GetConnectionByID returns a PoolerConnection for a specific pooler ID.
// This is used for reserved connections where queries need to be routed to
// a specific pooler instance (e.g., for session affinity with prepared statements).
// Returns an error immediately if the pooler connection doesn't exist (fail-fast).
func (lb *LoadBalancer) GetConnectionByID(poolerID *clustermetadatapb.ID) (*PoolerConnection, error) {
	if poolerID == nil {
		return nil, errors.New("pooler ID cannot be nil")
	}

	idStr := topoclient.MultiPoolerIDString(poolerID)

	lb.mu.Lock()
	defer lb.mu.Unlock()

	conn, exists := lb.connections[idStr]
	if !exists {
		return nil, mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE,
			"no connection found for pooler ID: %s", idStr)
	}

	return conn, nil
}

// selectConnection chooses the best connection from candidates.
// For replicas, prefers local cell. For primaries, just returns the first one
// (there should only be one primary).
func (lb *LoadBalancer) selectConnection(candidates []*PoolerConnection, target *query.Target) *PoolerConnection {
	// For PRIMARY, just return the first (there should be only one)
	if target.PoolerType == clustermetadatapb.PoolerType_PRIMARY {
		return candidates[0]
	}

	// For REPLICA, prefer local cell
	for _, conn := range candidates {
		if conn.Cell() == lb.localCell {
			return conn
		}
	}

	// No local replica, return any
	return candidates[0]
}

// matchesTarget checks if a connection matches the target specification.
func matchesTarget(conn *PoolerConnection, target *query.Target) bool {
	poolerInfo := conn.PoolerInfo()

	// Check tablegroup match
	if target.TableGroup != poolerInfo.GetTableGroup() {
		return false
	}

	// Check shard match (must be exact)
	if target.Shard != poolerInfo.GetShard() {
		return false
	}

	// Check type match
	targetType := target.PoolerType
	if targetType == clustermetadatapb.PoolerType_UNKNOWN {
		targetType = clustermetadatapb.PoolerType_PRIMARY
	}

	poolerType := conn.Type()
	switch targetType {
	case clustermetadatapb.PoolerType_PRIMARY:
		return poolerType == clustermetadatapb.PoolerType_PRIMARY
	case clustermetadatapb.PoolerType_REPLICA:
		return poolerType == clustermetadatapb.PoolerType_REPLICA
	default:
		return false
	}
}

// ConnectionCount returns the number of active connections.
func (lb *LoadBalancer) ConnectionCount() int {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	return len(lb.connections)
}

// Close closes all connections.
func (lb *LoadBalancer) Close() error {
	lb.mu.Lock()
	connections := lb.connections
	lb.connections = make(map[string]*PoolerConnection)
	lb.mu.Unlock()

	lb.logger.Info("closing all pooler connections", "count", len(connections))

	var lastErr error
	for poolerID, conn := range connections {
		if err := conn.Close(); err != nil {
			lb.logger.Error("error closing pooler connection",
				"pooler_id", poolerID,
				"error", err)
			lastErr = err
		}
	}
	return lastErr
}

// LoadBalancerListener wraps a LoadBalancer to implement multigateway.PoolerChangeListener.
type LoadBalancerListener struct {
	lb *LoadBalancer
}

// NewLoadBalancerListener creates a listener adapter for the given LoadBalancer.
func NewLoadBalancerListener(lb *LoadBalancer) *LoadBalancerListener {
	return &LoadBalancerListener{lb: lb}
}

// OnPoolerChanged implements multigateway.PoolerChangeListener.
func (l *LoadBalancerListener) OnPoolerChanged(pooler *clustermetadatapb.MultiPooler) {
	if err := l.lb.AddPooler(pooler); err != nil {
		l.lb.logger.Error("failed to add pooler on change event",
			"pooler_id", topoclient.MultiPoolerIDString(pooler.Id),
			"error", err)
	}
}

// OnPoolerRemoved implements multigateway.PoolerChangeListener.
func (l *LoadBalancerListener) OnPoolerRemoved(pooler *clustermetadatapb.MultiPooler) {
	l.lb.RemovePooler(pooler.Id)
}
