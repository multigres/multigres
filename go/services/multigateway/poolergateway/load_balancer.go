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

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
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
// Leader identity is tracked per-shard via the leaders map, populated from two
// sources that both carry a rule number: a pooler's self_leadership in its
// topology record (read on discovery) and LeaderObservation messages on health
// streams. The most-authoritative observation (highest rule number) wins,
// regardless of source. The pooler.Type field from etcd topology is never
// consulted for leader identity — only self_leadership is. A leaders entry
// survives connection lifecycle events: removing a pooler does not erase
// consensus's choice of leader.
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
	connections map[topoclient.ComponentID]*PoolerConnection

	// leaders maps shard key to the most-authoritative LeaderObservation seen
	// for that shard (highest rule number wins). Populated from a pooler's
	// self_leadership topology record and from LeaderObservation messages on
	// health streams. The leader_id field of the observation may name a pooler
	// we are not currently connected to — GetConnection handles that case at
	// read time.
	leaders map[shardKey]*clustermetadatapb.LeaderObservation

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
		connections: make(map[topoclient.ComponentID]*PoolerConnection),
		leaders:     make(map[shardKey]*clustermetadatapb.LeaderObservation),
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
// If the pooler's topology record carries a self_leadership observation (the
// leader publishes one naming itself under the rule it leads), AddPooler folds
// it into `leaders[shard]`. This is a real consensus observation, not a Type
// heuristic: a demoted pooler clears its self_leadership, so only the current
// leader advertises one. It competes with health-stream observations on rule
// number, so the most-authoritative view wins regardless of source. This lets
// the gateway route to the leader as soon as it is discovered in etcd, before
// any health stream has reported.
func (lb *LoadBalancer) AddPooler(pooler *clustermetadatapb.MultiPooler) error {
	poolerID := topoclient.ComponentIDString(pooler.Id)

	lb.mu.Lock()
	defer lb.mu.Unlock()

	key := shardKey{tableGroup: pooler.GetShardKey().GetTableGroup(), shard: pooler.GetShardKey().GetShard()}

	// Existing pooler — refresh topology metadata. The self_leadership merge
	// below also fires here: a pooler first discovered as a non-leader is
	// re-discovered with self_leadership set once multiorch promotes it. That
	// republished record is often the first signal of who leads, before any
	// health stream reports an observation.
	if conn, exists := lb.connections[poolerID]; exists {
		conn.UpdatePoolerInfo(pooler)
		lb.mergeTopologyLeaderLocked(key, pooler)
		lb.notifyIfLeaderServingLocked(key, conn)
		return nil
	}

	conn, err := NewPoolerConnection(lb.ctx, pooler, lb.logger, lb.grpcDialOpt, lb.onPoolerHealthUpdate)
	if err != nil {
		return fmt.Errorf("failed to create connection to pooler %s: %w", poolerID, err)
	}

	lb.connections[poolerID] = conn
	lb.mergeTopologyLeaderLocked(key, pooler)

	// If a prior LeaderObservation already named this pooler the leader, the
	// first health update may be slow — trigger buffer drain now so traffic
	// waiting on the leader's arrival proceeds as soon as it reports SERVING.
	lb.notifyIfLeaderServingLocked(key, conn)

	lb.logger.Debug("added pooler connection",
		"pooler_id", poolerID,
		"is_leader", pooler.GetSelfLeadership() != nil,
		"cell", pooler.Id.GetCell())

	return nil
}

// mergeTopologyLeaderLocked folds a pooler's self_leadership observation (from
// its topology record) into `leaders[key]`, keeping whichever observation has
// the higher rule number. A pooler that is not the leader carries no
// self_leadership, so this is a no-op for it — it never clears an existing
// entry, since a higher observation from any source is what supersedes a
// leader. Caller must hold lb.mu.
func (lb *LoadBalancer) mergeTopologyLeaderLocked(key shardKey, pooler *clustermetadatapb.MultiPooler) {
	obs := pooler.GetSelfLeadership()
	if obs == nil {
		return
	}
	merged := commonconsensus.MostAuthoritativeObservation(lb.leaders[key], obs)
	if merged == lb.leaders[key] {
		return
	}
	lb.leaders[key] = merged
	lb.logger.Debug("leader observation from topology self_leadership",
		"pooler_id", topoclient.ComponentIDString(pooler.Id),
		"tablegroup", key.tableGroup,
		"shard", key.shard,
		"rule", commonconsensus.FormatRuleNumber(obs.GetLeaderRuleNumber()))
}

// RemovePooler closes and removes the PoolerConnection for the given pooler ID.
// If no connection exists for this pooler, it is a no-op.
func (lb *LoadBalancer) RemovePooler(poolerID topoclient.ComponentID) {
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
//   - For PRIMARY: consults `leaders` (most-authoritative LeaderObservation) to
//     identify the leader, then looks up its connection. Two distinct error
//     cases: no leader observed yet, vs. leader known but not connected.
//   - For REPLICA: any connected pooler in the shard that does not believe
//     itself the leader, judged per-connection from self_leadership and
//     health-stream observations (see matchesReplicaTarget). This excludes both
//     the current leader and a stale leader, never consulting pooler.Type.
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
		leaderID := topoclient.ComponentIDString(leaderObs.LeaderId)
		conn, ok := lb.connections[leaderID]
		if !ok {
			return nil, mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE,
				"leader %s known but not connected for tablegroup=%s, shard=%s",
				leaderID, target.TableGroup, target.Shard)
		}
		return conn, nil
	}

	// REPLICA: collect every connection eligible to serve reads in the shard.
	// A pooler that believes itself the leader — the current leader or a stale
	// leader — is excluded; see matchesReplicaTarget.
	var candidates []*PoolerConnection
	for _, conn := range lb.connections {
		if matchesReplicaTarget(conn, target) {
			candidates = append(candidates, conn)
		}
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

	idStr := topoclient.ComponentIDString(poolerID)

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

	// CompareRuleNumbers treats a nil rule as the smallest value, so a first
	// observation (existing == nil) compares strictly greater and is recorded.
	existing := lb.leaders[key]
	cmp := commonconsensus.CompareRuleNumbers(obs.GetLeaderRuleNumber(), existing.GetLeaderRuleNumber())

	switch {
	case cmp > 0:
		// New leader or higher rule — record identity unconditionally. The
		// named pooler may not be in `connections` yet; that's fine.
		lb.leaders[key] = obs
		lb.logger.Debug("leader observation recorded",
			"tablegroup", key.tableGroup,
			"shard", key.shard,
			"leader_id", topoclient.ComponentIDString(obs.LeaderId),
			"rule", commonconsensus.FormatRuleNumber(obs.GetLeaderRuleNumber()))

		// If the named leader is connected and serving, drain the buffer.
		// (Only stop failover buffering when the leader is SERVING — the
		// LeaderObservation can arrive before the pooler has transitioned
		// its query server to PRIMARY/SERVING, e.g. during Promote where
		// UpdateLeaderObservation fires before changeTypeLocked. Draining
		// buffered requests too early would send them to a pooler that
		// still rejects PRIMARY traffic.)
		if leaderConn, ok := lb.connections[topoclient.ComponentIDString(obs.LeaderId)]; ok {
			lb.notifyIfLeaderServingLocked(key, leaderConn)
		}

	case cmp == 0:
		// Same rule — the leader is already known but may not have been
		// SERVING when we first saw the observation. Re-check now so that
		// StopBuffering fires once the leader transitions to SERVING.
		if leaderConn, ok := lb.connections[topoclient.ComponentIDString(existing.LeaderId)]; ok {
			lb.notifyIfLeaderServingLocked(key, leaderConn)
		}

	default:
		// Stale term — ignore.
		return
	}
}

// notifyIfLeaderServingLocked calls onPrimaryServing if the given connection
// is the known leader of the shard and is both SERVING and self-reporting
// PoolerType_PRIMARY on its health stream. StopBuffering is idempotent, so
// calling this on every health update is safe and ensures buffering stops
// promptly once the leader is ready.
//
// The PoolerType_PRIMARY check is critical: a LeaderObservation can arrive
// before the named pooler has finished transitioning its query server to
// PRIMARY/SERVING (e.g. during Promote, UpdateLeaderObservation fires before
// changeTypeLocked). Draining buffered requests too early would send them
// to a pooler that still rejects PRIMARY traffic.
//
// Caller must hold lb.mu.
func (lb *LoadBalancer) notifyIfLeaderServingLocked(key shardKey, conn *PoolerConnection) {
	if lb.onPrimaryServing == nil {
		return
	}
	leader := lb.leaders[key]
	if leader == nil || topoclient.ComponentIDString(leader.LeaderId) != conn.ID() {
		return
	}
	health := conn.Health()
	if health == nil || !health.IsServing() {
		return
	}
	if health.Target.GetPoolerType() != clustermetadatapb.PoolerType_PRIMARY {
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

// matchesReplicaTarget reports whether conn is eligible to serve REPLICA
// traffic for target: it is in the shard and does not believe itself the shard
// leader. Leadership is judged per-connection from the better of the pooler's
// etcd self_leadership and its health-stream observation (see
// believesSelfLeader), never from the topology Type label.
//
// Excluding self-believed leaders covers both the current leader and a stale
// leader — a pooler whose own view still names it the leader even though a
// newer leader has been observed. Either would serve reads as a primary or from
// a diverged timeline, so neither is a replica candidate.
//
// TODO(replica-quality): eligibility is otherwise binary. Longer term, treat a
// replica as degraded when it is not actively replicating or has fallen behind,
// and prefer non-degraded replicas when several are available. That belongs in
// selectReplicaConnection (which already tiers by lag), with the per-pooler
// signal carried on the health observation.
func matchesReplicaTarget(conn *PoolerConnection, target *query.Target) bool {
	if !matchesShardTarget(conn, target) {
		return false
	}
	return !conn.believesSelfLeader()
}

// ConnectionCount returns the number of active connections.
func (lb *LoadBalancer) ConnectionCount() int {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	return len(lb.connections)
}

// Consensus leadership roles reported by LeadershipByID for the status page.
const (
	// LeadershipLeader marks the shard's current consensus leader.
	LeadershipLeader = "leader"
	// LeadershipStaleLeader marks a pooler that still believes itself the leader
	// (per its own self_leadership and health observation) even though consensus
	// has moved on to a higher-rule leader. Such a pooler is excluded from
	// replica reads; see matchesReplicaTarget.
	LeadershipStaleLeader = "stale-leader"
	// LeadershipFollower marks any other connected pooler.
	LeadershipFollower = "follower"
)

// LeadershipByID returns the consensus leadership role of each connected pooler,
// keyed by serialized pooler ID, for the admin/status page. The role reflects
// the gateway's merged view — the per-shard leader map (self_leadership combined
// with health-stream observations) and the pooler's own belief — never the
// topology Type label. Poolers the gateway is not connected to are absent from
// the map; the caller fills those in from discovery (e.g. as a lifecycle state).
func (lb *LoadBalancer) LeadershipByID() map[topoclient.ComponentID]string {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	roles := make(map[topoclient.ComponentID]string, len(lb.connections))
	for _, conn := range lb.connections {
		info := conn.PoolerInfo()
		key := shardKey{
			tableGroup: info.GetShardKey().GetTableGroup(),
			shard:      info.GetShardKey().GetShard(),
		}

		// The shard's consensus leader (merged across every pooler's
		// self_leadership and health-stream reports) takes precedence, so a
		// leader whose own record lags is still reported as the leader.
		// Otherwise a pooler that still believes itself leader is stale.
		role := LeadershipFollower
		switch obs := lb.leaders[key]; {
		case obs != nil && topoclient.ComponentIDString(obs.LeaderId) == conn.ID():
			role = LeadershipLeader
		case conn.believesSelfLeader():
			role = LeadershipStaleLeader
		}
		roles[conn.ID()] = role
	}
	return roles
}

// Close closes all connections.
func (lb *LoadBalancer) Close() error {
	lb.mu.Lock()
	connections := lb.connections
	lb.connections = make(map[topoclient.ComponentID]*PoolerConnection)
	lb.leaders = make(map[shardKey]*clustermetadatapb.LeaderObservation)
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
