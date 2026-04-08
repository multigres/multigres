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

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/pb/query"
)

// shardKey identifies a shard for primary caching.
type shardKey struct {
	tableGroup string
	shard      string
}

// cachedPrimary holds the cached primary connection for a shard.
type cachedPrimary struct {
	conn *PoolerConnection
	term int64
}

// LoadBalancer manages PoolerConnections and selects connections for queries.
// It creates connections based on discovery events and destroys them when poolers
// are removed from discovery.
//
// For PRIMARY targets, the LoadBalancer caches the primary connection per-shard.
// The cache is updated via health stream callbacks, so the hot path (GetConnection)
// is a simple map lookup rather than iterating all poolers.
type LoadBalancer struct {
	// localCell is the cell where this gateway is running
	localCell string

	// logger for debugging
	logger *slog.Logger

	// ctx is the service-lifetime context for child goroutines (health streams)
	ctx context.Context

	// mu protects connections and cachedPrimaries
	mu sync.Mutex

	// connections maps pooler ID to PoolerConnection
	connections map[string]*PoolerConnection

	// cachedPrimaries maps shard key to the cached primary connection.
	// Updated by onPoolerHealthUpdate when health streams report PrimaryObservation.
	cachedPrimaries map[shardKey]*cachedPrimary

	// onPrimaryServing is called when a new primary is detected via health stream.
	// Used to stop failover buffering for the shard. May be nil.
	onPrimaryServing func(tableGroup, shard string)

	// onSchemaVersionChanged is called when the schema version increases for a
	// shard, indicating that one or more DDL statements have been executed.
	// Consumers (e.g. plan cache) should invalidate cached plans for the shard.
	// May be nil.
	onSchemaVersionChanged func(tableGroup, shard string)

	// lastSchemaVersions tracks the most recently seen SchemaVersion per pooler
	// connection ID. Used to detect version increases across health messages.
	// Protected by mu.
	lastSchemaVersions map[string]int64
}

// NewLoadBalancer creates a new LoadBalancer.
func NewLoadBalancer(ctx context.Context, localCell string, logger *slog.Logger) *LoadBalancer {
	return &LoadBalancer{
		localCell:          localCell,
		logger:             logger,
		ctx:                ctx,
		connections:        make(map[string]*PoolerConnection),
		cachedPrimaries:    make(map[shardKey]*cachedPrimary),
		lastSchemaVersions: make(map[string]int64),
	}
}

// SetOnPrimaryServing sets a callback invoked when a new primary is detected
// via the streaming health check. This is used to stop failover buffering
// when a new primary becomes available, replacing the topology-based approach.
// Must be called before any poolers are added.
func (lb *LoadBalancer) SetOnPrimaryServing(fn func(tableGroup, shard string)) {
	lb.onPrimaryServing = fn
}

// SetOnSchemaVersionChanged sets a callback that is invoked whenever the schema
// version for a shard increases. Consumers such as the plan cache should
// invalidate cached plans for the affected shard when this fires.
// Must be called before any poolers are added.
func (lb *LoadBalancer) SetOnSchemaVersionChanged(fn func(tableGroup, shard string)) {
	lb.onSchemaVersionChanged = fn
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
		oldType := conn.Type()
		conn.UpdatePoolerInfo(pooler)
		newType := conn.Type()

		// Invalidate seed if type changed away from PRIMARY
		if oldType == clustermetadatapb.PoolerType_PRIMARY && newType != clustermetadatapb.PoolerType_PRIMARY {
			key := shardKey{tableGroup: pooler.GetTableGroup(), shard: pooler.GetShard()}
			if cached, ok := lb.cachedPrimaries[key]; ok && cached.conn == conn && cached.term == 0 {
				delete(lb.cachedPrimaries, key)
				lb.logger.Debug("invalidated seeded primary cache on type change",
					"pooler_id", poolerID, "old_type", oldType, "new_type", newType)
			}
		}

		// Seed if type changed to PRIMARY
		if oldType != clustermetadatapb.PoolerType_PRIMARY && newType == clustermetadatapb.PoolerType_PRIMARY {
			key := shardKey{tableGroup: pooler.GetTableGroup(), shard: pooler.GetShard()}
			if existing, ok := lb.cachedPrimaries[key]; !ok || existing.term == 0 {
				lb.cachedPrimaries[key] = &cachedPrimary{conn: conn, term: 0}
				lb.logger.Debug("seeded primary cache on type change",
					"pooler_id", poolerID, "old_type", oldType, "new_type", newType)
			}
		}

		return nil
	}

	conn, err := NewPoolerConnection(lb.ctx, pooler, lb.logger, lb.onPoolerHealthUpdate)
	if err != nil {
		return fmt.Errorf("failed to create connection to pooler %s: %w", poolerID, err)
	}

	lb.connections[poolerID] = conn

	// Seed primary cache from discovery type (term 0 = unconfirmed).
	// Health stream callbacks will overwrite with real term data.
	if pooler.Type == clustermetadatapb.PoolerType_PRIMARY {
		key := shardKey{tableGroup: pooler.GetTableGroup(), shard: pooler.GetShard()}
		if existing, ok := lb.cachedPrimaries[key]; !ok || existing.term == 0 {
			lb.cachedPrimaries[key] = &cachedPrimary{conn: conn, term: 0}
			lb.logger.Debug("seeded primary cache from discovery",
				"pooler_id", poolerID, "tablegroup", key.tableGroup, "shard", key.shard)
		}
	}

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
	delete(lb.lastSchemaVersions, poolerID)
	// Invalidate cached primary if the removed pooler was the cached primary.
	for key, cached := range lb.cachedPrimaries {
		if cached.conn == conn {
			delete(lb.cachedPrimaries, key)
		}
	}
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
// - For PRIMARY: uses cached primary (updated by health stream callbacks)
// - For REPLICA: prefers local cell serving replicas, with randomization
func (lb *LoadBalancer) GetConnection(target *query.Target) (*PoolerConnection, error) {
	if target == nil {
		return nil, errors.New("target cannot be nil")
	}

	lb.mu.Lock()
	defer lb.mu.Unlock()

	targetType := target.PoolerType

	if targetType == clustermetadatapb.PoolerType_PRIMARY {
		// Use cached primary (populated by health stream callbacks).
		// The health stream's PrimaryObservation is the authoritative source
		// for primary identity — no type-based fallback.
		key := shardKey{tableGroup: target.TableGroup, shard: target.Shard}
		if cached, ok := lb.cachedPrimaries[key]; ok {
			return cached.conn, nil
		}

		return nil, mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE,
			"no primary cached for target: tablegroup=%s, shard=%s (waiting for health stream)",
			target.TableGroup, target.Shard)
	}

	// For REPLICA: collect only replica-type poolers
	var candidates []*PoolerConnection
	for _, conn := range lb.connections {
		if matchesTarget(conn, target) {
			candidates = append(candidates, conn)
		}
	}

	if len(candidates) == 0 {
		return nil, mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE,
			"no pooler found for target: tablegroup=%s, shard=%s, type=%s",
			target.TableGroup, target.Shard, target.PoolerType.String())
	}

	return lb.selectReplicaConnection(candidates), nil
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

// onPoolerHealthUpdate is the callback invoked by PoolerConnection when health
// state changes. It updates the cached primary for the connection's shard
// based on PrimaryObservation term reconciliation.
//
// This is safe to call concurrently: both processHealthResponse and setHealthError
// release healthMu before invoking this callback.
func (lb *LoadBalancer) onPoolerHealthUpdate(conn *PoolerConnection) {
	health := conn.Health()
	if health == nil || health.PrimaryObservation == nil {
		return
	}

	key := shardKey{
		tableGroup: health.Target.GetTableGroup(),
		shard:      health.Target.GetShard(),
	}
	term := health.PrimaryObservation.PrimaryTerm
	primaryID := poolerIDString(health.PrimaryObservation.PrimaryId)

	lb.mu.Lock()
	defer lb.mu.Unlock()

	cached := lb.cachedPrimaries[key]

	switch {
	case cached == nil || term > cached.term:
		// New primary or higher term — update the cached primary.
		primaryConn, exists := lb.connections[primaryID]
		if !exists {
			return
		}
		lb.cachedPrimaries[key] = &cachedPrimary{conn: primaryConn, term: term}
		lb.logger.Debug("cached primary updated",
			"tablegroup", key.tableGroup,
			"shard", key.shard,
			"primary_id", primaryID,
			"term", term)

		// Only stop failover buffering when the primary is confirmed to be
		// PRIMARY type and SERVING. The PrimaryObservation can arrive before
		// the pooler has transitioned its query server to PRIMARY/SERVING
		// (e.g., during Promote, UpdatePrimaryObservation fires before
		// changeTypeLocked). Draining buffered requests too early would send
		// them to a pooler that still rejects PRIMARY traffic.
		lb.notifyIfPrimaryServingLocked(key, primaryConn)

	case term == cached.term:
		// Same term — the primary is already cached but may not have been
		// SERVING when we first saw the observation. Re-check now so that
		// StopBuffering fires once the primary transitions to PRIMARY/SERVING.
		lb.notifyIfPrimaryServingLocked(key, cached.conn)

	default:
		// Stale term — ignore.
		return
	}

	// Check for schema version changes from the primary. Only the primary
	// multipooler runs schema tracking, so replicas always report version 0
	// and can be ignored.
	//
	// On the first message from a given primary (!seen) we record a baseline
	// without firing — the gateway has no cached plans to invalidate yet.
	// On subsequent messages we fire if the version differs in either direction:
	//   - increased: schema changed normally
	//   - decreased: multipooler restarted and its in-memory counter reset;
	//     treat conservatively as a possible schema change
	if lb.onSchemaVersionChanged != nil &&
		health.Target.GetPoolerType() == clustermetadatapb.PoolerType_PRIMARY {
		connID := poolerIDString(health.PoolerID)
		lastVersion, seen := lb.lastSchemaVersions[connID]
		if seen && health.SchemaVersion != lastVersion {
			lb.onSchemaVersionChanged(key.tableGroup, key.shard)
		}
		lb.lastSchemaVersions[connID] = health.SchemaVersion
	}
}

// notifyIfPrimaryServingLocked calls onPrimaryServing if the primary connection
// is confirmed to be PRIMARY type and SERVING. StopBuffering is idempotent, so
// calling this on every health update is safe and ensures buffering stops
// promptly once the primary is ready. Caller must hold lb.mu.
func (lb *LoadBalancer) notifyIfPrimaryServingLocked(key shardKey, primaryConn *PoolerConnection) {
	primaryHealth := primaryConn.Health()
	if lb.onPrimaryServing != nil && primaryHealth != nil &&
		primaryHealth.Target.GetPoolerType() == clustermetadatapb.PoolerType_PRIMARY &&
		primaryHealth.IsServing() {
		lb.onPrimaryServing(key.tableGroup, key.shard)
	}
}

// matchesShardTarget checks if a connection matches the tablegroup and shard,
// regardless of pooler type.
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
	poolerType := conn.Type()
	switch target.PoolerType {
	case clustermetadatapb.PoolerType_PRIMARY:
		return poolerType == clustermetadatapb.PoolerType_PRIMARY
	case clustermetadatapb.PoolerType_REPLICA:
		return poolerType == clustermetadatapb.PoolerType_REPLICA
	default:
		return false
	}
}

// poolerIDString returns the string ID for a pooler.
// Uses the same format as PoolerConnection.ID() for consistency.
func poolerIDString(id *clustermetadatapb.ID) string {
	return topoclient.MultiPoolerIDString(id)
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
	lb.cachedPrimaries = make(map[shardKey]*cachedPrimary)
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
