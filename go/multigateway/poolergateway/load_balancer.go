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

package poolergateway

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/tools/retry"
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

// AddPooler creates a new PoolerConnection for the given pooler.
// If a connection already exists for this pooler, it is a no-op.
func (lb *LoadBalancer) AddPooler(pooler *clustermetadatapb.MultiPooler) error {
	poolerID := poolerIDString(pooler.Id)

	lb.mu.Lock()
	defer lb.mu.Unlock()

	// Check if already exists
	if _, exists := lb.connections[poolerID]; exists {
		lb.logger.Debug("pooler connection already exists", "pooler_id", poolerID)
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
// - For PRIMARY: returns the primary pooler (any cell)
// - For REPLICA: prefers local cell, falls back to other cells
func (lb *LoadBalancer) GetConnection(target *query.Target, opts *GetConnectionOptions) (*PoolerConnection, error) {
	if target == nil {
		return nil, errors.New("target cannot be nil")
	}

	lb.mu.Lock()
	defer lb.mu.Unlock()

	excludeSet := makeExcludeSet(opts)

	// Collect candidates matching the target
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

	// Select best candidate
	return lb.selectConnection(candidates, target), nil
}

// GetConnectionContext returns a PoolerConnection matching the target specification.
// If no suitable connection is immediately available, it waits until one becomes
// available or the context is cancelled.
//
// This is useful during failover when waiting for a new primary to be elected.
// Maximum wait time is 30 seconds (similar to Vitess's initialTabletTimeout).
func (lb *LoadBalancer) GetConnectionContext(ctx context.Context, target *query.Target, opts *GetConnectionOptions) (*PoolerConnection, error) {
	// Try once immediately
	conn, err := lb.GetConnection(target, opts)
	if err == nil {
		return conn, nil
	}

	// If no connection found, retry with backoff up to 30 seconds total
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	r := retry.New(10*time.Millisecond, 100*time.Millisecond)
	for _, retryErr := range r.Attempts(ctx) {
		if retryErr != nil {
			return nil, fmt.Errorf("no pooler found for target after 30s: tablegroup=%s, shard=%s, type=%s",
				target.TableGroup, target.Shard, target.PoolerType.String())
		}

		conn, connErr := lb.GetConnection(target, opts)
		if connErr == nil {
			return conn, nil
		}
		// Continue retrying on next iteration
	}

	// This should be unreachable since r.Attempts will return context error
	return nil, errors.New("no pooler found for target after retry")
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

	// Check shard match (empty target shard matches any)
	if target.Shard != "" && target.Shard != poolerInfo.GetShard() {
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
func poolerIDString(id *clustermetadatapb.ID) string {
	return fmt.Sprintf("%s/%s", id.GetCell(), id.GetName())
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
			"pooler_id", poolerIDString(pooler.Id),
			"error", err)
	}
}

// OnPoolerRemoved implements multigateway.PoolerChangeListener.
func (l *LoadBalancerListener) OnPoolerRemoved(pooler *clustermetadatapb.MultiPooler) {
	l.lb.RemovePooler(poolerIDString(pooler.Id))
}
