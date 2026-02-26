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

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/query"
)

// GetConnectionOptions specifies options for connection selection.
type GetConnectionOptions struct {
	// ExcludePoolers is a list of pooler IDs to exclude from selection.
	// This is used for retry logic to avoid retrying on a pooler that just failed.
	ExcludePoolers []string
}

// LoadBalancer manages PoolerConnections and selects connections for queries.
// It creates connections based on discovery events and destroys them when poolers
// are removed from discovery.
type LoadBalancer struct {
	// localCell is the cell where this gateway is running
	localCell string

	// logger for debugging
	logger *slog.Logger

	// ctx is the service-lifetime context for child goroutines (health streams)
	ctx context.Context

	// mu protects the connections map
	mu sync.RWMutex

	// connections maps pooler ID to PoolerConnection
	connections map[string]*PoolerConnection
}

// NewLoadBalancer creates a new LoadBalancer.
func NewLoadBalancer(ctx context.Context, localCell string, logger *slog.Logger) *LoadBalancer {
	return &LoadBalancer{
		localCell:   localCell,
		logger:      logger,
		ctx:         ctx,
		connections: make(map[string]*PoolerConnection),
	}
}

// AddPooler creates a new PoolerConnection for the given pooler.
// If a connection already exists for this pooler, it updates the pooler info
// (e.g., when type changes from UNKNOWN to PRIMARY).
func (lb *LoadBalancer) AddPooler(pooler *clustermetadatapb.MultiPooler) error {
	poolerID := poolerIDString(pooler.Id)

	lb.mu.Lock()
	defer lb.mu.Unlock()

	// Check if already exists - update info instead of skipping
	if conn, exists := lb.connections[poolerID]; exists {
		conn.UpdatePoolerInfo(pooler)
		return nil
	}

	// Create new connection.
	// Pass nil for onHealthUpdate - the LoadBalancer currently uses Health()
	// to check serving state on demand rather than maintaining a separate list.
	// TODO: Consider adding a callback to proactively update routing when health changes.
	conn, err := NewPoolerConnection(lb.ctx, pooler, lb.logger, nil)
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
func (lb *LoadBalancer) RemovePooler(poolerID string) {
	lb.mu.Lock()
	conn, exists := lb.connections[poolerID]
	if !exists {
		lb.mu.Unlock()
		return
	}
	delete(lb.connections, poolerID)
	lb.mu.Unlock()

	// Close outside the lock
	if err := conn.Close(); err != nil {
		lb.logger.Error("error closing pooler connection",
			"pooler_id", poolerID,
			"error", err)
	} else {
		lb.logger.Debug("removed pooler connection", "pooler_id", poolerID)
	}
}

// GetConnection returns a PoolerConnection matching the target specification.
// Returns an error immediately if no suitable connection is available.
//
// Selection logic:
// - For PRIMARY: uses term-based reconciliation across all shard poolers
// - For REPLICA: prefers local cell serving replicas, with randomization
func (lb *LoadBalancer) GetConnection(target *query.Target, opts *GetConnectionOptions) (*PoolerConnection, error) {
	if target == nil {
		return nil, errors.New("target cannot be nil")
	}

	lb.mu.RLock()
	defer lb.mu.RUnlock()

	excludeSet := makeExcludeSet(opts)
	targetType := target.PoolerType
	if targetType == clustermetadatapb.PoolerType_UNKNOWN {
		targetType = clustermetadatapb.PoolerType_PRIMARY
	}

	if targetType == clustermetadatapb.PoolerType_PRIMARY {
		// For PRIMARY: collect ALL poolers in the shard (regardless of type)
		// because any pooler can report PrimaryObservation about who the primary is.
		// We include excluded poolers for their observations but won't return them.
		var shardPoolers []*PoolerConnection
		for _, conn := range lb.connections {
			if matchesShardTarget(conn, target) {
				shardPoolers = append(shardPoolers, conn)
			}
		}

		if len(shardPoolers) == 0 {
			return nil, fmt.Errorf("no pooler found for target: tablegroup=%s, shard=%s, type=%s",
				target.TableGroup, target.Shard, target.PoolerType.String())
		}

		conn := lb.selectPrimaryByTerm(shardPoolers, excludeSet)
		if conn == nil {
			return nil, fmt.Errorf("no pooler found for target: tablegroup=%s, shard=%s, type=%s (no PRIMARY type)",
				target.TableGroup, target.Shard, target.PoolerType.String())
		}
		return conn, nil
	}

	// For REPLICA: collect only replica-type poolers
	var candidates []*PoolerConnection
	for _, conn := range lb.connections {
		if excludeSet[conn.ID()] {
			continue
		}
		if matchesTarget(conn, target) {
			candidates = append(candidates, conn)
		}
	}

	if len(candidates) == 0 {
		return nil, fmt.Errorf("no pooler found for target: tablegroup=%s, shard=%s, type=%s",
			target.TableGroup, target.Shard, target.PoolerType.String())
	}

	return lb.selectReplicaConnection(candidates), nil
}

// GetConnectionContext returns a PoolerConnection matching the target specification.
// If no suitable connection is immediately available, it waits until one becomes
// available or the context is cancelled.
//
// This is useful during failover when waiting for a new primary to be elected.
func (lb *LoadBalancer) GetConnectionContext(ctx context.Context, target *query.Target, opts *GetConnectionOptions) (*PoolerConnection, error) {
	// For now, just delegate to GetConnection.
	// Waiting logic will be added when we implement health streaming.
	return lb.GetConnection(target, opts)
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

	lb.mu.RLock()
	defer lb.mu.RUnlock()

	conn, exists := lb.connections[idStr]
	if !exists {
		return nil, fmt.Errorf("no connection found for pooler ID: %s", idStr)
	}

	return conn, nil
}

// selectReplicaConnection chooses the best replica connection from candidates.
// Prefers serving connections in local cell, with randomization within each tier
// to distribute load across replicas (following Vitess pattern).
func (lb *LoadBalancer) selectReplicaConnection(candidates []*PoolerConnection) *PoolerConnection {
	// Categorize by locality and serving status
	var localServing, remoteServing, localNotServing []*PoolerConnection
	for _, conn := range candidates {
		isLocal := conn.Cell() == lb.localCell
		isServing := conn.Health().IsServing()

		switch {
		case isLocal && isServing:
			localServing = append(localServing, conn)
		case isServing:
			remoteServing = append(remoteServing, conn)
		case isLocal:
			localNotServing = append(localNotServing, conn)
		}
	}

	// Select from tiers in preference order, with randomization within each tier
	if len(localServing) > 0 {
		return localServing[rand.IntN(len(localServing))]
	}
	if len(remoteServing) > 0 {
		return remoteServing[rand.IntN(len(remoteServing))]
	}
	if len(localNotServing) > 0 {
		return localNotServing[rand.IntN(len(localNotServing))]
	}
	// Fall back to any candidate
	return candidates[rand.IntN(len(candidates))]
}

// selectPrimaryByTerm finds the primary by looking at all poolers' PrimaryObservation.
// The observation with the highest term indicates the most recently elected primary.
// Returns the connection for the primary_id specified in that observation.
//
// The excludeSet contains pooler IDs that should not be returned (e.g., recently failed).
// Excluded poolers are still consulted for their observations since they may have
// the most up-to-date view of who the primary is.
//
// This handles split-brain scenarios: during failover, multiple poolers may have
// different views of who the primary is. The highest term wins.
//
// TODO: Consider caching the best primary observation per-shard and updating it via
// onHealthUpdate callbacks, rather than recomputing on every GetConnection call.
// For now, the dynamic approach is simpler and n (poolers per shard) is typically small.
func (lb *LoadBalancer) selectPrimaryByTerm(shardPoolers []*PoolerConnection, excludeSet map[string]bool) *PoolerConnection {
	if len(shardPoolers) == 0 {
		return nil
	}

	// Find the observation with the highest term across all poolers (including excluded ones)
	var bestObservation *PoolerConnection
	var bestTerm int64 = -1
	var bestPrimaryID string

	for _, conn := range shardPoolers {
		health := conn.Health()
		if health == nil || health.PrimaryObservation == nil {
			continue
		}

		term := health.PrimaryObservation.Term
		if term > bestTerm {
			bestTerm = term
			bestObservation = conn
			bestPrimaryID = poolerIDString(health.PrimaryObservation.PrimaryId)
		}
	}

	// If no observations found, fall back to pooler type.
	// Only return PRIMARY-type poolers - never route PRIMARY requests to UNKNOWN
	// (uninitialized) poolers since they might be replicas. Multi-cell discovery
	// will find the PRIMARY when multiorch assigns the type.
	if bestObservation == nil {
		for _, conn := range shardPoolers {
			if excludeSet[conn.ID()] {
				continue
			}
			if conn.Type() == clustermetadatapb.PoolerType_PRIMARY {
				return conn
			}
		}
		// No PRIMARY pooler found - return nil.
		// Caller will handle as "no pooler found" error.
		return nil
	}

	// Find the connection for the primary identified by the best observation
	// Skip excluded poolers
	for _, conn := range shardPoolers {
		if excludeSet[conn.ID()] {
			continue
		}
		if conn.ID() == bestPrimaryID {
			return conn
		}
	}

	// Primary identified by observation not found or excluded.
	// Fall back to the observer if not excluded.
	if !excludeSet[bestObservation.ID()] {
		lb.logger.Warn("primary from highest-term observation not found in connections",
			"primary_id", bestPrimaryID,
			"term", bestTerm,
			"observer_pooler", bestObservation.ID())
		return bestObservation
	}

	// Observer is excluded too. Find any non-excluded PRIMARY-type pooler.
	// We only return PRIMARY type - never fall back to other types.
	for _, conn := range shardPoolers {
		if excludeSet[conn.ID()] {
			continue
		}
		if conn.Type() == clustermetadatapb.PoolerType_PRIMARY {
			return conn
		}
	}

	// No eligible PRIMARY pooler found
	return nil
}

// matchesShardTarget checks if a connection matches the tablegroup and shard,
// regardless of pooler type. Used for primary selection where we need to consult
// all poolers in the shard for their PrimaryObservation.
func matchesShardTarget(conn *PoolerConnection, target *query.Target) bool {
	poolerInfo := conn.PoolerInfo()

	// Check tablegroup match
	if target.TableGroup != poolerInfo.GetTableGroup() {
		return false
	}

	// Check shard match (empty target shard matches any)
	if target.Shard != "" && target.Shard != poolerInfo.GetShard() {
		return false
	}

	return true
}

// matchesTarget checks if a connection matches the target specification.
func matchesTarget(conn *PoolerConnection, target *query.Target) bool {
	if !matchesShardTarget(conn, target) {
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

// makeExcludeSet creates a set from the ExcludePoolers list for O(1) lookup.
func makeExcludeSet(opts *GetConnectionOptions) map[string]bool {
	if opts == nil || len(opts.ExcludePoolers) == 0 {
		return nil
	}
	set := make(map[string]bool, len(opts.ExcludePoolers))
	for _, id := range opts.ExcludePoolers {
		set[id] = true
	}
	return set
}

// poolerIDString returns the string ID for a pooler.
// Uses the same format as PoolerConnection.ID() for consistency.
func poolerIDString(id *clustermetadatapb.ID) string {
	return topoclient.MultiPoolerIDString(id)
}

// ConnectionCount returns the number of active connections.
func (lb *LoadBalancer) ConnectionCount() int {
	lb.mu.RLock()
	defer lb.mu.RUnlock()
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
			"pooler_id", poolerIDString(pooler.Id),
			"error", err)
	}
}

// OnPoolerRemoved implements multigateway.PoolerChangeListener.
func (l *LoadBalancerListener) OnPoolerRemoved(pooler *clustermetadatapb.MultiPooler) {
	l.lb.RemovePooler(poolerIDString(pooler.Id))
}
