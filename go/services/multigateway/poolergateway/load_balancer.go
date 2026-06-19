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
	"log/slog"
	"maps"
	"math/rand/v2"
	"sync"
	"time"

	"google.golang.org/grpc"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/poolerwatch"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/pb/query"
)

// shardKey identifies a shard for leader tracking.
type shardKey struct {
	tableGroup string
	shard      string
}

// LoadBalancer selects PoolerConnections for queries. The set of tracked
// poolers and each one's *PoolerConnection rider lives in the pooler cache
// (poolerwatch.PoolerCache[*PoolerConnection]) — the cache's OnLive hook
// constructs the connection, and OnGone closes it. LoadBalancer owns no
// per-pooler registry of its own.
//
// Leader identity is tracked per-shard via the leaders map, populated from two
// sources that both carry a rule number: a pooler's self_leadership in its
// topology record (folded in when the hook calls MergeTopologyLeader) and
// LeaderObservation messages on health streams (delivered via
// OnPoolerHealthUpdate). The most-authoritative observation (highest rule
// number) wins, regardless of source. The pooler.Type field from etcd
// topology is never consulted for leader identity — only self_leadership is.
// A leaders entry survives connection lifecycle events: removing a pooler
// does not erase consensus's choice of leader.
//
// Concurrency: lb.mu and the cache's internal mutex are separate. Methods
// here take lb.mu only while reading/writing the leaders map; cache reads
// happen after lb.mu has been released, so the cache OnLive/OnGone hooks
// (which call MergeTopologyLeader / NotifyIfLeaderServing → lb.mu) can never
// deadlock against an LB method holding lb.mu and waiting on the cache.
type LoadBalancer struct {
	// localCell is the cell where this gateway is running
	localCell string

	// logger for debugging
	logger *slog.Logger

	// ctx is the service-lifetime context for child goroutines (health streams)
	ctx context.Context

	// mu protects leaders.
	mu sync.Mutex

	// leaders maps shard key to the most-authoritative LeaderObservation seen
	// for that shard (highest rule number wins). Populated from a pooler's
	// self_leadership topology record (via MergeTopologyLeader) and from
	// LeaderObservation messages on health streams (via OnPoolerHealthUpdate).
	// The leader_id field of the observation may name a pooler we are not
	// currently connected to — GetConnection handles that case at read time
	// by consulting the cache.
	leaders map[shardKey]*clustermetadatapb.LeaderObservation

	// cache is the pooler cache that owns the per-pooler *PoolerConnection
	// riders. Supplied at construction via LoadBalancerOpts.Cache; callers
	// build the cache first (without hooks), pass it in here, then call
	// cache.Start with hooks that close over the LB.
	cache *poolerwatch.PoolerCache[*PoolerConnection]

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

// LoadBalancerOpts groups the construction parameters for a LoadBalancer.
// All required fields must be set; optional fields default to zero/nil.
type LoadBalancerOpts struct {
	// Ctx is the service-lifetime context; cancelled on Shutdown. Required.
	Ctx context.Context
	// LocalCell is the cell where this gateway runs. Required.
	LocalCell string
	// Logger is used for all diagnostic logging. Required.
	Logger *slog.Logger
	// DialOpt configures transport credentials for pooler gRPC connections.
	// Required.
	DialOpt grpc.DialOption

	// LowLag is the preferred replication-lag threshold. Replicas at or below
	// this lag are preferred. Zero disables the preferred tier.
	LowLag time.Duration
	// HighTolerance is the absolute maximum replication lag. Replicas above
	// this are never selected. Zero means no upper bound.
	HighTolerance time.Duration

	// OnPrimaryServing fires when a new primary is observed serving on its
	// health stream; used to drain the failover buffer. Optional.
	OnPrimaryServing func(tableGroup, shard string)

	// Cache is the pooler cache that owns the per-pooler *PoolerConnection
	// riders. Callers construct the cache without hooks, pass it here, then
	// call cache.Start with hooks that close over the resulting LB.
	Cache *poolerwatch.PoolerCache[*PoolerConnection]
}

// NewLoadBalancer creates a LoadBalancer. The cache supplied via opts.Cache
// must be started by the caller after construction (via cache.Start with
// hooks that close over this LB).
func NewLoadBalancer(opts LoadBalancerOpts) *LoadBalancer {
	return &LoadBalancer{
		localCell:                     opts.LocalCell,
		logger:                        opts.Logger,
		ctx:                           opts.Ctx,
		leaders:                       make(map[shardKey]*clustermetadatapb.LeaderObservation),
		grpcDialOpt:                   opts.DialOpt,
		onPrimaryServing:              opts.OnPrimaryServing,
		lowReplicationLagNs:           opts.LowLag.Nanoseconds(),
		highReplicationLagToleranceNs: opts.HighTolerance.Nanoseconds(),
		cache:                         opts.Cache,
	}
}

// MergeTopologyLeader folds a pooler's self_leadership observation (from its
// topology record) into `leaders[shard]`, keeping whichever observation has
// the higher rule number. A pooler that is not the leader carries no
// self_leadership, so this is a no-op for it — it never clears an existing
// entry, since a higher observation from any source is what supersedes a
// leader.
//
// Called by the pooler cache's OnLive and OnUpdate hooks. Acquires lb.mu
// internally; must not be called while holding lb.mu.
func (lb *LoadBalancer) MergeTopologyLeader(pooler *clustermetadatapb.MultiPooler) {
	obs := pooler.GetSelfLeadership()
	if obs == nil {
		return
	}
	key := shardKey{
		tableGroup: pooler.GetShardKey().GetTableGroup(),
		shard:      pooler.GetShardKey().GetShard(),
	}

	lb.mu.Lock()
	merged := commonconsensus.MostAuthoritativeObservation(lb.leaders[key], obs)
	if merged == lb.leaders[key] {
		lb.mu.Unlock()
		return
	}
	lb.leaders[key] = merged
	lb.mu.Unlock()

	lb.logger.Debug("leader observation from topology self_leadership",
		"pooler_id", topoclient.ComponentIDString(pooler.Id),
		"tablegroup", key.tableGroup,
		"shard", key.shard,
		"rule", commonconsensus.FormatRuleNumber(obs.GetLeaderRuleNumber()))
}

// NotifyIfLeaderServing calls onPrimaryServing if conn is the known leader of
// its shard and is both SERVING and self-reporting PoolerType_PRIMARY on its
// health stream. StopBuffering is idempotent, so calling this on every
// lifecycle / health update is safe and ensures buffering stops promptly once
// the leader is ready.
//
// The PoolerType_PRIMARY check is critical: a LeaderObservation can arrive
// before the named pooler has finished transitioning its query server to
// PRIMARY/SERVING (e.g. during Promote, UpdateLeaderObservation fires before
// changeTypeLocked). Draining buffered requests too early would send them to
// a pooler that still rejects PRIMARY traffic.
//
// Called by the cache OnLive/OnUpdate hooks and internally by
// OnPoolerHealthUpdate. Acquires lb.mu briefly to read the leader; must not
// be called while holding lb.mu.
func (lb *LoadBalancer) NotifyIfLeaderServing(pooler *clustermetadatapb.MultiPooler, conn *PoolerConnection) {
	if lb.onPrimaryServing == nil || conn == nil {
		return
	}
	key := shardKey{
		tableGroup: pooler.GetShardKey().GetTableGroup(),
		shard:      pooler.GetShardKey().GetShard(),
	}
	lb.notifyIfLeaderServing(key, conn)
}

// notifyIfLeaderServing is the internal helper shared by NotifyIfLeaderServing
// and OnPoolerHealthUpdate. Snapshots the leader under lb.mu, releases it,
// then inspects the connection's health to avoid holding lb.mu while reading
// cross-mutex state.
func (lb *LoadBalancer) notifyIfLeaderServing(key shardKey, conn *PoolerConnection) {
	if lb.onPrimaryServing == nil {
		return
	}
	lb.mu.Lock()
	leader := lb.leaders[key]
	lb.mu.Unlock()
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

// GetConnection returns a PoolerConnection matching the target specification.
// Returns an error immediately if no suitable connection is available.
//
// Selection logic:
//   - For PRIMARY: consults `leaders` (most-authoritative LeaderObservation) to
//     identify the leader, then looks up its connection in the cache. Two
//     distinct error cases: no leader observed yet, vs. leader known but not
//     connected.
//   - For REPLICA: any connected pooler in the shard that does not believe
//     itself the leader, judged per-connection from self_leadership and
//     health-stream observations (see matchesReplicaTarget). This excludes both
//     the current leader and a stale leader, never consulting pooler.Type.
func (lb *LoadBalancer) GetConnection(target *query.Target) (*PoolerConnection, error) {
	if target == nil {
		return nil, errors.New("target cannot be nil")
	}

	key := shardKey{tableGroup: target.TableGroup, shard: target.Shard}

	// Snapshot the leader observation under lb.mu, release it, then talk to
	// the cache (which has its own mutex).
	lb.mu.Lock()
	leaderObs := lb.leaders[key]
	lb.mu.Unlock()

	if target.PoolerType == clustermetadatapb.PoolerType_PRIMARY {
		if leaderObs == nil {
			return nil, mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE,
				"no leader observed yet for tablegroup=%s, shard=%s",
				target.TableGroup, target.Shard)
		}
		leaderID := topoclient.ComponentIDString(leaderObs.LeaderId)
		if lb.cache == nil {
			return nil, mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE,
				"leader %s known but not connected for tablegroup=%s, shard=%s",
				leaderID, target.TableGroup, target.Shard)
		}
		conn, ok := lb.cache.GetRider(leaderID)
		if !ok || conn == nil {
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
	if lb.cache != nil {
		for _, entry := range lb.cache.All() {
			conn := entry.Rider
			if conn == nil {
				continue
			}
			if matchesReplicaTarget(conn, target) {
				candidates = append(candidates, conn)
			}
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
	if lb.cache != nil {
		if conn, ok := lb.cache.GetRider(idStr); ok && conn != nil {
			return conn, nil
		}
	}
	return nil, mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE,
		"no connection found for pooler ID: %s", idStr)
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

// OnPoolerHealthUpdate is the callback invoked by PoolerConnection when health
// state changes. It updates `leaders` based on LeaderObservation rule
// reconciliation — recording the named leader unconditionally, regardless of
// whether we currently have a connection to that leader. The connection
// lookup happens at GetConnection time via the cache, so identity survives
// any connection lifecycle event.
//
// Concurrency: holds lb.mu only to mutate the leaders map; cache lookups for
// the SERVING-leader notification happen after lb.mu has been released, so
// this never blocks a cache OnLive/OnGone hook on lb.mu.
//
// Safe to call concurrently: both processHealthResponse and setHealthError
// release healthMu before invoking this callback.
func (lb *LoadBalancer) OnPoolerHealthUpdate(conn *PoolerConnection) {
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
	existing := lb.leaders[key]
	cmp := commonconsensus.CompareRuleNumbers(obs.GetLeaderRuleNumber(), existing.GetLeaderRuleNumber())

	var leaderIDForLookup topoclient.ComponentID
	switch {
	case cmp > 0:
		// New leader or higher rule — record identity unconditionally. The
		// named pooler may not be in the cache yet; that's fine.
		lb.leaders[key] = obs
		leaderIDForLookup = topoclient.ComponentIDString(obs.LeaderId)
		lb.mu.Unlock()
		lb.logger.Debug("leader observation recorded",
			"tablegroup", key.tableGroup,
			"shard", key.shard,
			"leader_id", leaderIDForLookup,
			"rule", commonconsensus.FormatRuleNumber(obs.GetLeaderRuleNumber()))

	case cmp == 0:
		// Same rule — the leader is already known but may not have been
		// SERVING when we first saw the observation. Re-check now so that
		// StopBuffering fires once the leader transitions to SERVING.
		leaderIDForLookup = topoclient.ComponentIDString(existing.LeaderId)
		lb.mu.Unlock()

	default:
		// Stale rule — ignore.
		lb.mu.Unlock()
		return
	}

	// If the named leader is connected and serving, drain the buffer. Only
	// stop failover buffering when the leader is SERVING — the
	// LeaderObservation can arrive before the pooler has transitioned its
	// query server to PRIMARY/SERVING (e.g. during Promote where
	// UpdateLeaderObservation fires before changeTypeLocked). Draining
	// buffered requests too early would send them to a pooler that still
	// rejects PRIMARY traffic.
	if lb.cache == nil {
		return
	}
	if leaderConn, ok := lb.cache.GetRider(leaderIDForLookup); ok && leaderConn != nil {
		lb.notifyIfLeaderServing(key, leaderConn)
	}
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

// ConnectionCount returns the number of active connections, sourced from the
// pooler cache.
func (lb *LoadBalancer) ConnectionCount() int {
	if lb.cache == nil {
		return 0
	}
	return lb.cache.Len()
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
	if lb.cache == nil {
		return map[topoclient.ComponentID]string{}
	}

	// Snapshot leaders under lb.mu, then read the cache (separate mutex).
	lb.mu.Lock()
	leadersSnap := maps.Clone(lb.leaders)
	lb.mu.Unlock()

	entries := lb.cache.All()
	roles := make(map[topoclient.ComponentID]string, len(entries))
	for _, entry := range entries {
		conn := entry.Rider
		if conn == nil {
			continue
		}
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
		switch obs := leadersSnap[key]; {
		case obs != nil && topoclient.ComponentIDString(obs.LeaderId) == conn.ID():
			role = LeadershipLeader
		case conn.believesSelfLeader():
			role = LeadershipStaleLeader
		}
		roles[conn.ID()] = role
	}
	return roles
}

// Close clears the leaders map. Per-pooler connections are owned by the
// pooler cache: shutting the cache down (which fires OnGone for every entry)
// is what closes them. Close is a no-op aside from the leader map reset and
// remains as a convenience for symmetry with the old API.
func (lb *LoadBalancer) Close() error {
	lb.mu.Lock()
	lb.leaders = make(map[shardKey]*clustermetadatapb.LeaderObservation)
	lb.mu.Unlock()
	lb.logger.Info("load balancer leader state cleared")
	return nil
}
