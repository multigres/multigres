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
	"time"

	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
)

// shardKey identifies a shard for leader tracking.
type shardKey struct {
	tableGroup string
	shard      string
}

// LoadBalancer manages PoolerConnections and selects connections for queries.
// It creates connections based on discovery events and destroys them when poolers
// are removed from discovery.
//
// Leader identity is tracked per-shard via the leaders map, populated only from
// LeaderObservation messages on health streams. The pooler.Type field from etcd
// topology is a hint about intended role — never the source of truth for which
// pooler leads a shard. A leaders entry survives connection lifecycle events:
// removing a pooler does not erase consensus's choice of leader.
type LoadBalancer struct {
	// localCell is the cell where this gateway is running
	localCell string

	// logger for debugging
	logger *slog.Logger

	// ctx is the service-lifetime context for child goroutines (health streams)
	ctx context.Context

	// mu protects connections and leaders
	mu sync.Mutex

	// connections maps pooler ID to PoolerConnection
	connections map[string]*PoolerConnection

	// leaders maps shard key to the highest-term LeaderObservation seen for that
	// shard. Populated only from health-stream observations; never from topology.
	// The leader_id field of the observation may name a pooler we are not
	// currently connected to — GetConnection handles that case at read time.
	leaders map[shardKey]*multipoolerservice.LeaderObservation

	// onPrimaryServing is called when a new primary is detected via health stream.
	// Used to stop failover buffering for the shard. May be nil.
	onPrimaryServing func(tableGroup, shard string)

	// grpcDialOpt configures transport credentials (TLS or insecure) for pooler connections.
	grpcDialOpt grpc.DialOption

	// lowReplicationLagNs is the preferred replication lag threshold in nanoseconds.
	// Replicas at or below this lag are considered "healthy" and preferred.
	// If all replicas exceed this but are under highReplicationLagToleranceNs,
	// they are still eligible. Zero disables the preferred tier (all replicas equal).
	lowReplicationLagNs int64

	// highReplicationLagToleranceNs is the absolute maximum replication lag in
	// nanoseconds. Replicas above this are never selected. Zero means no upper
	// bound — any replica is eligible regardless of lag.
	highReplicationLagToleranceNs int64
}

// NewLoadBalancer creates a new LoadBalancer.
// The grpcDialOpt configures transport credentials for gRPC connections to poolers.
func NewLoadBalancer(ctx context.Context, localCell string, logger *slog.Logger, grpcDialOpt grpc.DialOption) *LoadBalancer {
	return &LoadBalancer{
		localCell:   localCell,
		logger:      logger,
		ctx:         ctx,
		connections: make(map[string]*PoolerConnection),
		leaders:     make(map[shardKey]*multipoolerservice.LeaderObservation),
		grpcDialOpt: grpcDialOpt,
	}
}

// SetOnPrimaryServing sets a callback invoked when a new primary is detected
// via the streaming health check. This is used to stop failover buffering
// when a new primary becomes available, replacing the topology-based approach.
// Must be called before any poolers are added.
func (lb *LoadBalancer) SetOnPrimaryServing(fn func(tableGroup, shard string)) {
	lb.onPrimaryServing = fn
}

// SetReplicationLagThresholds configures two-tier replication lag filtering.
//
//   - lowLag: replicas at or below this lag are preferred ("healthy" tier).
//     If all replicas exceed this but are under highTolerance, they are still used.
//     Zero disables the preferred tier — all replicas are treated equally.
//   - highTolerance: absolute maximum lag. Replicas above this are never selected.
//     Zero means no upper bound.
func (lb *LoadBalancer) SetReplicationLagThresholds(lowLag, highTolerance time.Duration) {
	lb.lowReplicationLagNs = lowLag.Nanoseconds()
	lb.highReplicationLagToleranceNs = highTolerance.Nanoseconds()
}

// AddPooler creates a new PoolerConnection for the given pooler.
// If a connection already exists for this pooler, it updates the pooler info.
//
// AddPooler does not touch leader state — that comes exclusively from
// LeaderObservation. When a newly-added connection happens to be the leader
// already-known via prior observations, the buffer is drained promptly via
// notifyIfLeaderServingLocked.
func (lb *LoadBalancer) AddPooler(pooler *clustermetadatapb.MultiPooler) error {
	poolerID := poolerIDString(pooler.Id)

	lb.mu.Lock()
	defer lb.mu.Unlock()

	key := shardKey{tableGroup: pooler.GetShardKey().GetTableGroup(), shard: pooler.GetShardKey().GetShard()}

	// Existing pooler — just refresh topology metadata. No leader-state effects.
	if conn, exists := lb.connections[poolerID]; exists {
		conn.UpdatePoolerInfo(pooler)
		lb.notifyIfLeaderServingLocked(key, conn)
		return nil
	}

	conn, err := NewPoolerConnection(lb.ctx, pooler, lb.logger, lb.grpcDialOpt, lb.onPoolerHealthUpdate)
	if err != nil {
		return fmt.Errorf("failed to create connection to pooler %s: %w", poolerID, err)
	}

	lb.connections[poolerID] = conn

	// If a prior LeaderObservation already named this pooler the leader, the
	// first health update may be slow — trigger buffer drain now so traffic
	// waiting on the leader's arrival proceeds as soon as it reports SERVING.
	lb.notifyIfLeaderServingLocked(key, conn)

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
//   - For PRIMARY: consults `leaders` (highest-term LeaderObservation) to identify
//     the leader, then looks up its connection. Two distinct error cases: no
//     leader observed yet, vs. leader known but not connected.
//   - For REPLICA: any connected pooler in the shard that is not the known
//     leader. If no leader is known yet (cold start), falls back to topology
//     pooler.Type == REPLICA to avoid serving reads from a pooler that may
//     turn out to be the leader.
func (lb *LoadBalancer) GetConnection(target *query.Target) (*PoolerConnection, error) {
	if target == nil {
		return nil, errors.New("target cannot be nil")
	}

	lb.mu.Lock()
	defer lb.mu.Unlock()

	key := shardKey{tableGroup: target.TableGroup, shard: target.Shard}
	leaderObs := lb.leaders[key]

	if target.PoolerType == clustermetadatapb.PoolerType_PRIMARY {
		if leaderObs == nil {
			return nil, mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE,
				"no leader observed yet for tablegroup=%s, shard=%s",
				target.TableGroup, target.Shard)
		}
		leaderID := poolerIDString(leaderObs.LeaderId)
		conn, ok := lb.connections[leaderID]
		if !ok {
			return nil, mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE,
				"leader %s known but not connected for tablegroup=%s, shard=%s",
				leaderID, target.TableGroup, target.Shard)
		}
		return conn, nil
	}

	// REPLICA: collect every connection in the shard except the known leader.
	leaderID := ""
	if leaderObs != nil {
		leaderID = poolerIDString(leaderObs.LeaderId)
	}
	var candidates []*PoolerConnection
	for id, conn := range lb.connections {
		if !matchesShardTarget(conn, target) {
			continue
		}
		if leaderObs != nil {
			if id == leaderID {
				continue
			}
		} else if conn.Type() != clustermetadatapb.PoolerType_REPLICA {
			// Cold start: no LeaderObservation yet. Fall back to topology hint
			// so we don't accidentally serve reads from a future leader.
			continue
		}
		candidates = append(candidates, conn)
	}

	if len(candidates) == 0 {
		return nil, mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE,
			"no pooler found for target: tablegroup=%s, shard=%s, type=%s",
			target.TableGroup, target.Shard, target.PoolerType.String())
	}

	selected := lb.selectReplicaConnection(candidates)
	if selected == nil {
		// All replicas exceeded the replication lag threshold.
		return nil, mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE,
			"no replica with acceptable replication lag for target: tablegroup=%s, shard=%s",
			target.TableGroup, target.Shard)
	}
	return selected, nil
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

// selectReplicaConnection chooses the best replica connection from candidates
// using two-tier replication lag filtering:
//
//  1. Exclude replicas above highReplicationLagToleranceNs (absolute max).
//  2. Prefer replicas at or below lowReplicationLagNs ("healthy" tier).
//  3. If no healthy replicas, fall back to any that passed the high tolerance.
//  4. If none pass either threshold, return nil (error to client).
//
// Within each tier, selection prefers local-cell serving replicas with
// randomization to distribute load.
func (lb *LoadBalancer) selectReplicaConnection(candidates []*PoolerConnection) *PoolerConnection {
	lowThreshold := lb.lowReplicationLagNs
	highThreshold := lb.highReplicationLagToleranceNs
	hasLagFilter := lowThreshold > 0 || highThreshold > 0

	// Single-pass: lag filtering + locality categorization combined.
	// "healthy" = lag within lowThreshold (or unknown). "tolerable" = between
	// low and high thresholds. Each bucket is further split by locality.
	type bucket struct {
		localServing, remoteServing, localNotServing []*PoolerConnection
	}
	var healthy, tolerable bucket

	for _, conn := range candidates {
		health := conn.Health()

		if hasLagFilter {
			lagNs := int64(0)
			if health != nil {
				lagNs = health.ReplicationLagNs
			}

			// Exclude replicas above the absolute maximum.
			if highThreshold > 0 && lagNs > 0 && lagNs > highThreshold {
				continue
			}

			isLocal := conn.Cell() == lb.localCell
			isServing := health.IsServing()

			// Lag unknown (0) or within the low threshold → healthy.
			if lowThreshold == 0 || lagNs == 0 || lagNs <= lowThreshold {
				switch {
				case isLocal && isServing:
					healthy.localServing = append(healthy.localServing, conn)
				case isServing:
					healthy.remoteServing = append(healthy.remoteServing, conn)
				case isLocal:
					healthy.localNotServing = append(healthy.localNotServing, conn)
				}
			} else {
				// Between low and high thresholds → tolerable fallback.
				switch {
				case isLocal && isServing:
					tolerable.localServing = append(tolerable.localServing, conn)
				case isServing:
					tolerable.remoteServing = append(tolerable.remoteServing, conn)
				case isLocal:
					tolerable.localNotServing = append(tolerable.localNotServing, conn)
				}
			}
		} else {
			// No lag filtering — all candidates go into "healthy".
			isLocal := conn.Cell() == lb.localCell
			isServing := health.IsServing()

			switch {
			case isLocal && isServing:
				healthy.localServing = append(healthy.localServing, conn)
			case isServing:
				healthy.remoteServing = append(healthy.remoteServing, conn)
			case isLocal:
				healthy.localNotServing = append(healthy.localNotServing, conn)
			}
		}
	}

	// Pick the best bucket: prefer healthy, fall back to tolerable.
	b := &healthy
	if len(b.localServing)+len(b.remoteServing)+len(b.localNotServing) == 0 {
		b = &tolerable
	}
	if len(b.localServing)+len(b.remoteServing)+len(b.localNotServing) == 0 {
		if hasLagFilter {
			// All replicas exceed the high tolerance threshold.
			return nil
		}
		// No lag filter and no candidates categorized — shouldn't happen
		// because candidates is non-empty, but be safe.
		return candidates[rand.IntN(len(candidates))]
	}

	// Select from tiers in preference order, with randomization within each tier.
	if len(b.localServing) > 0 {
		return b.localServing[rand.IntN(len(b.localServing))]
	}
	if len(b.remoteServing) > 0 {
		return b.remoteServing[rand.IntN(len(b.remoteServing))]
	}
	return b.localNotServing[rand.IntN(len(b.localNotServing))]
}

// onPoolerHealthUpdate is the callback invoked by PoolerConnection when health
// state changes. It updates `leaders` based on LeaderObservation term
// reconciliation — recording the named leader unconditionally, regardless of
// whether we currently have a connection to that leader. The connection
// lookup happens at GetConnection time, not at observation time, so identity
// survives any connection lifecycle event.
//
// This is safe to call concurrently: both processHealthResponse and setHealthError
// release healthMu before invoking this callback.
func (lb *LoadBalancer) onPoolerHealthUpdate(conn *PoolerConnection) {
	health := conn.Health()
	if health == nil || health.LeaderObservation == nil {
		return
	}

	key := shardKey{
		tableGroup: health.Target.GetTableGroup(),
		shard:      health.Target.GetShard(),
	}
	obs := health.LeaderObservation

	lb.mu.Lock()
	defer lb.mu.Unlock()

	existing := lb.leaders[key]

	switch {
	case existing == nil || obs.LeaderTerm > existing.LeaderTerm:
		// New leader or higher term — record identity unconditionally. The
		// named pooler may not be in `connections` yet; that's fine.
		lb.leaders[key] = obs
		lb.logger.Debug("leader observation recorded",
			"tablegroup", key.tableGroup,
			"shard", key.shard,
			"leader_id", poolerIDString(obs.LeaderId),
			"term", obs.LeaderTerm)

		// If the named leader is connected and serving, drain the buffer.
		// (Only stop failover buffering when the leader is SERVING — the
		// LeaderObservation can arrive before the pooler has transitioned
		// its query server to PRIMARY/SERVING, e.g. during Promote where
		// UpdateLeaderObservation fires before changeTypeLocked. Draining
		// buffered requests too early would send them to a pooler that
		// still rejects PRIMARY traffic.)
		if leaderConn, ok := lb.connections[poolerIDString(obs.LeaderId)]; ok {
			lb.notifyIfLeaderServingLocked(key, leaderConn)
		}

	case obs.LeaderTerm == existing.LeaderTerm:
		// Same term — the leader is already known but may not have been
		// SERVING when we first saw the observation. Re-check now so that
		// StopBuffering fires once the leader transitions to SERVING.
		if leaderConn, ok := lb.connections[poolerIDString(existing.LeaderId)]; ok {
			lb.notifyIfLeaderServingLocked(key, leaderConn)
		}

	default:
		// Stale term — ignore.
		return
	}
}

// notifyIfLeaderServingLocked calls onPrimaryServing if the given connection is
// the known leader of the shard and is serving. StopBuffering is idempotent,
// so calling this on every health update is safe and ensures buffering stops
// promptly once the leader is ready. Caller must hold lb.mu.
func (lb *LoadBalancer) notifyIfLeaderServingLocked(key shardKey, conn *PoolerConnection) {
	if lb.onPrimaryServing == nil {
		return
	}
	leader := lb.leaders[key]
	if leader == nil || poolerIDString(leader.LeaderId) != conn.ID() {
		return
	}
	if !conn.Health().IsServing() {
		return
	}
	lb.onPrimaryServing(key.tableGroup, key.shard)
}

// matchesShardTarget checks if a connection matches the tablegroup and shard,
// regardless of pooler type.
func matchesShardTarget(conn *PoolerConnection, target *query.Target) bool {
	poolerInfo := conn.PoolerInfo()

	// Check tablegroup match
	if target.TableGroup != poolerInfo.GetShardKey().GetTableGroup() {
		return false
	}

	// Check shard match (empty target shard matches any)
	if target.Shard != "" && target.Shard != poolerInfo.GetShardKey().GetShard() {
		return false
	}

	return true
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
	lb.leaders = make(map[shardKey]*multipoolerservice.LeaderObservation)
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
