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

// shardKey identifies a shard for leader tracking. Database is included
// so two databases that share a (tableGroup, shard) name don't collide in
// lb.shards — see commit history for the cross-database bug this fixes.
type shardKey struct {
	database   string
	tableGroup string
	shard      string
}

// shardKeyOf builds an internal shardKey from a clustermetadata.ShardKey.
func shardKeyOf(sk *clustermetadatapb.ShardKey) shardKey {
	return shardKey{
		database:   sk.GetDatabase(),
		tableGroup: sk.GetTableGroup(),
		shard:      sk.GetShard(),
	}
}

// shardSummary holds the gateway's per-shard routing state.
// One shardSummary exists per (tableGroup, shard) pair while any pooler
// from that shard is tracked in the cache. It is auto-created on the
// first observation for the shard and auto-cleared once no poolers from
// the shard remain.
//
// The state is the SET of poolers currently claiming to be the writable
// (routing) primary — membership means "this pooler is a routing primary right
// now," keyed by pooler id. This is distinct from consensus: orch reasons about
// the highest-known rule over consensus statuses; the gateway only cares which
// pooler(s) are writable primaries. The observation's rule number is used only
// as a tiebreaker to pick among members during the brief failover-overlap window
// when two poolers both claim primary. A pooler is added when it advertises
// itself as a writable primary and removed when it stops (demoted, no longer
// writable) or leaves — so a stale primary retracts rather than sticking.
//
// Concurrency: primaries is protected by mu. Read the elected leader via
// leader(), mutate via setPrimary/clearPrimary. shardKey is immutable.
type shardSummary struct {
	// shardKey identifies the shard — database, tablegroup, shard.
	shardKey *clustermetadatapb.ShardKey

	mu        sync.Mutex
	primaries primarySet
}

// primarySet is the set of poolers currently claiming to be the writable
// routing primary for a shard, keyed by pooler id, each carrying the rule under
// which it claimed. It exposes only what the gateway needs — add/remove by id
// and "the highest-rule member" — behind a named type so the storage can change
// without touching callers. No heap or sorted structure is warranted: the number
// of simultaneous primary claims per shard is expected to be very low in practice
// (normally one, briefly two during a failover overlap), so a plain map with a
// linear highest() scan is the right call. Not safe for concurrent use —
// shardSummary.mu guards it.
type primarySet struct {
	byID map[topoclient.ComponentID]*clustermetadatapb.RoutingState
}

func (p *primarySet) set(id topoclient.ComponentID, rs *clustermetadatapb.RoutingState) {
	if p.byID == nil {
		p.byID = make(map[topoclient.ComponentID]*clustermetadatapb.RoutingState)
	}
	p.byID[id] = rs
}

func (p *primarySet) clear(id topoclient.ComponentID) { delete(p.byID, id) }

func (p *primarySet) has(id topoclient.ComponentID) bool { _, ok := p.byID[id]; return ok }

// highest returns the id of the highest-rule member and true, or ("", false) if
// the set is empty. The member's identity is the map key — a RoutingState carries
// no id, since a pooler only ever reports its own routing state.
func (p *primarySet) highest() (topoclient.ComponentID, bool) {
	var (
		bestID   topoclient.ComponentID
		bestRule *clustermetadatapb.RuleNumber
		found    bool
	)
	for id, rs := range p.byID {
		if !found || commonconsensus.CompareRuleNumbers(rs.GetRule(), bestRule) > 0 {
			bestID, bestRule, found = id, rs.GetRule(), true
		}
	}
	return bestID, found
}

// leaderID returns the id of the routing primary with the highest rule and true,
// or ("", false) if the shard currently has no writable leader. Safe to call
// concurrently.
func (s *shardSummary) leaderID() (topoclient.ComponentID, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.primaries.highest()
}

// setPrimary records poolerID as a routing primary at rs's rule (adds it to the
// set or updates its rule). Returns true if the elected leader (the highest-rule
// member's id) changed.
func (s *shardSummary) setPrimary(poolerID topoclient.ComponentID, rs *clustermetadatapb.RoutingState) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	before, _ := s.primaries.highest()
	s.primaries.set(poolerID, rs)
	after, _ := s.primaries.highest()
	return before != after
}

// clearPrimary removes poolerID from the routing-primary set (retraction).
// Returns true if the elected leader changed. Safe on an absent pooler.
func (s *shardSummary) clearPrimary(poolerID topoclient.ComponentID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.primaries.has(poolerID) {
		return false
	}
	before, _ := s.primaries.highest()
	s.primaries.clear(poolerID)
	after, _ := s.primaries.highest()
	return before != after
}

// hasPrimary reports whether poolerID currently claims to be a routing primary
// for this shard — i.e. its latest health observation advertised role PRIMARY.
// This is the single authority for "believes itself the writable primary": both
// stale-leader detection and replica-read exclusion read it, rather than
// re-deriving the same signal from each connection's live health.
func (s *shardSummary) hasPrimary(poolerID topoclient.ComponentID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.primaries.has(poolerID)
}

// loadBalancer selects poolerConnections for queries. The set of tracked
// poolers and each one's *poolerConnection rider lives in the pooler cache
// (poolerwatch.PoolerCache[*poolerConnection]) — the cache's OnLive hook
// constructs the connection, and OnGone closes it. loadBalancer owns no
// per-pooler registry of its own.
//
// Per-shard state is tracked via the shards map. Each shardSummary holds the
// SET of poolers currently claiming to be the writable routing primary, fed
// entirely by live health-stream observations (via onPoolerHealthUpdate): a
// pooler is added when its own observation names itself and retracted when it
// stops claiming or leaves. The etcd topology — pooler.Type and self_leadership
// alike — is never consulted for leader identity; only a live, self-attested
// health observation counts. A shardSummary survives connection lifecycle
// events until the last pooler in the shard leaves (see onPoolerGone).
//
// Concurrency: a two-lock model is in play.
//   - lb.mu protects ONLY the lb.shards map (lookups, inserts, deletes).
//   - Each *shardSummary has its own mu protecting its routing-primary set;
//     reads go through leader()/hasPrimary(), mutations through
//     setPrimary/clearPrimary.
//
// Lock ordering: take lb.mu first (if needed), then summary.mu. No method
// holds both at once — summaryForPooler() releases lb.mu before returning the
// *shardSummary, and callers then invoke summary methods which self-lock.
// Cache reads happen after lb.mu has been released, so the cache OnLive/
// OnGone hooks (which call onPoolerHealthUpdate / onPoolerGone → lb.mu) can
// never deadlock against an LB method holding lb.mu and waiting on the cache.
type loadBalancer struct {
	// localCell is the cell where this gateway is running
	localCell string

	// logger for debugging
	logger *slog.Logger

	// ctx is the service-lifetime context for child goroutines (health streams)
	ctx context.Context

	// mu protects shards.
	mu sync.Mutex

	// shards maps shard key to the gateway's per-shard summary — the set of
	// poolers currently claiming to be the writable routing primary for that
	// shard. Populated solely from LeaderObservation messages on health streams
	// (via onPoolerHealthUpdate); the elected leader (highest-rule claimant) may
	// name a pooler we are not currently connected to — GetConnection handles
	// that at read time by consulting the cache. Entries are auto-cleared by
	// onPoolerGone once no poolers remain in the shard.
	shards map[shardKey]*shardSummary

	// cache is the pooler cache that owns the per-pooler *poolerConnection
	// riders. Supplied at construction via loadBalancerOpts.Cache; callers
	// build the cache first (without hooks), pass it in here, then call
	// cache.Start with hooks that close over the LB.
	cache *poolerwatch.PoolerCache[*poolerConnection]

	// onLeaderServing is called when a new consensus leader is observed
	// serving via its health stream. Used to stop failover buffering for
	// the shard. May be nil.
	onLeaderServing func(*clustermetadatapb.ShardKey)

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

// loadBalancerOpts groups the construction parameters for a loadBalancer.
// All required fields must be set; optional fields default to zero/nil.
type loadBalancerOpts struct {
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

	// OnLeaderServing fires when a new consensus leader is observed
	// serving on its health stream; used to drain the failover buffer.
	// Optional. The ShardKey carries database + tableGroup + shard so the
	// buffer can scope failover state correctly across databases.
	OnLeaderServing func(*clustermetadatapb.ShardKey)

	// Cache is the pooler cache that owns the per-pooler *poolerConnection
	// riders. Callers construct the cache without hooks, pass it here, then
	// call cache.Start with hooks that close over the resulting LB.
	Cache *poolerwatch.PoolerCache[*poolerConnection]
}

// newLoadBalancer creates a loadBalancer. The cache supplied via opts.Cache
// must be started by the caller after construction (via cache.Start with
// hooks that close over this LB).
func newLoadBalancer(opts loadBalancerOpts) *loadBalancer {
	return &loadBalancer{
		localCell:                     opts.LocalCell,
		logger:                        opts.Logger,
		ctx:                           opts.Ctx,
		shards:                        make(map[shardKey]*shardSummary),
		grpcDialOpt:                   opts.DialOpt,
		onLeaderServing:               opts.OnLeaderServing,
		lowReplicationLagNs:           opts.LowLag.Nanoseconds(),
		highReplicationLagToleranceNs: opts.HighTolerance.Nanoseconds(),
		cache:                         opts.Cache,
	}
}

// shardSummary returns the existing summary for the shard, creating one if
// absent. The returned *shardSummary is owned by the cache; callers mutate
// via its methods (which self-lock). Briefly holds lb.mu; releases it before
// returning, so callers can freely invoke summary methods without nesting
// locks.
func (lb *loadBalancer) summaryForPooler(p *clustermetadatapb.Multipooler) *shardSummary {
	key := shardKeyOf(p.GetShardKey())
	lb.mu.Lock()
	defer lb.mu.Unlock()
	summary, ok := lb.shards[key]
	if !ok {
		summary = &shardSummary{shardKey: p.GetShardKey()}
		lb.shards[key] = summary
	}
	return summary
}

// notifyIfLeaderServing calls onLeaderServing if conn is the known leader of
// its shard, is SERVING on its health stream, AND the most recent broadcast
// names this pooler itself as leader. StopBuffering is idempotent, so calling
// this on every lifecycle / health update is safe and ensures buffering stops
// promptly once the leader is ready.
//
// The self-named-leader check is the buffer-drain race guard: a
// LeaderObservation can arrive (via etcd self_leadership or via another
// pooler's health stream) before the named pooler has itself acknowledged
// being leader. Until this pooler's own broadcast names itself as leader,
// draining the buffer toward it would route writes to a queryServer that
// still rejects WRITABLE traffic with MTF01.
//
// Called by the cache OnLive/OnUpdate hooks and internally by
// onPoolerHealthUpdate. Acquires lb.mu briefly to look up the summary; must
// not be called while holding lb.mu.
func (lb *loadBalancer) notifyIfLeaderServing(pooler *clustermetadatapb.Multipooler, conn *poolerConnection) {
	if lb.onLeaderServing == nil || conn == nil {
		return
	}
	key := shardKeyOf(pooler.GetShardKey())
	lb.mu.Lock()
	summary := lb.shards[key]
	lb.mu.Unlock()
	lb.notifyLeaderServingFromSummary(summary, conn)
}

// notifyLeaderServingFromSummary is the internal helper shared by
// notifyIfLeaderServing and onPoolerHealthUpdate. The summary may be nil if no
// shard summary has been created yet (no leader observed); callers obtain it
// from lb.shards or via shardSummary().
func (lb *loadBalancer) notifyLeaderServingFromSummary(summary *shardSummary, conn *poolerConnection) {
	if lb.onLeaderServing == nil || summary == nil {
		return
	}
	leaderID, ok := summary.leaderID()
	connID := conn.PoolerInfo().GetId()
	if !ok || leaderID != topoclient.ComponentIDString(connID) {
		return
	}
	health := conn.Health()
	if health == nil || !health.isServing() {
		return
	}
	// The pooler's own broadcast must advertise role PRIMARY — it is the elected
	// routing primary and it is attesting to that itself. Role PRIMARY implies
	// writability (a pooler advertises PRIMARY only once it is the writable routing
	// primary), so draining the failover buffer toward it is safe without a
	// separate writability check.
	if health.RoutingState.GetRole() != clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY {
		return
	}
	lb.onLeaderServing(summary.shardKey)
}

// getConnection returns a poolerConnection matching the target specification.
// Returns an error immediately if no suitable connection is available.
//
// Routing by Mode:
//   - WRITABLE / CONSISTENT: route to the consensus leader. Uses the
//     shard's most-authoritative LeaderObservation, then looks up the
//     leader's connection in the cache. Two distinct error cases: no
//     leader observed yet, vs. leader known but not connected. (CONSISTENT
//     today resolves to the leader; future routing work may downgrade to
//     a sync standby caught up to the leader's LSN — see Mode docs.)
//   - INCONSISTENT (and UNSPECIFIED, treated as INCONSISTENT for backward
//     compatibility with old REPLICA callers): any connected pooler in
//     the shard that does not believe itself the leader, judged per
//     connection from self_leadership and health-stream observations
//     (see matchesReplicaTarget). Excludes both the current leader and a
//     stale leader.
func (lb *loadBalancer) getConnection(target *query.Target) (*poolerConnection, error) {
	if target == nil {
		return nil, errors.New("target cannot be nil")
	}

	sk := target.GetShardKey()
	key := shardKeyOf(sk)

	// Look up the shard summary under lb.mu, release it, then read the elected
	// leader id via the summary's own lock.
	lb.mu.Lock()
	summary := lb.shards[key]
	lb.mu.Unlock()
	var (
		leaderID   topoclient.ComponentID
		haveLeader bool
	)
	if summary != nil {
		leaderID, haveLeader = summary.leaderID()
	}

	mode := target.GetMode()
	routesToLeader := mode == query.Mode_MODE_WRITABLE || mode == query.Mode_MODE_CONSISTENT
	if routesToLeader {
		if !haveLeader {
			return nil, newNoWritablePrimaryError(
				"no leader observed yet for database=%s, tablegroup=%s, shard=%s",
				sk.GetDatabase(), sk.GetTableGroup(), sk.GetShard())
		}
		if lb.cache == nil {
			return nil, newNoWritablePrimaryError(
				"leader %s known but not connected for database=%s, tablegroup=%s, shard=%s",
				leaderID, sk.GetDatabase(), sk.GetTableGroup(), sk.GetShard())
		}
		conn, ok := lb.cache.GetRider(leaderID)
		if !ok || conn == nil {
			return nil, newNoWritablePrimaryError(
				"leader %s known but not connected for database=%s, tablegroup=%s, shard=%s",
				leaderID, sk.GetDatabase(), sk.GetTableGroup(), sk.GetShard())
		}
		return conn, nil
	}

	// INCONSISTENT (or unspecified): collect every connection eligible to
	// serve reads in this shard. A pooler that believes itself the leader
	// — the current leader or a stale leader — is excluded; see
	// matchesReplicaTarget.
	//
	// We scan cache.All() rather than calling GetByShard because gateway
	// callers still send empty-shard targets for unsharded routing, and a
	// hash lookup would miss them. matchesReplicaTarget filters by the full
	// (database, tableGroup, shard) tuple — database is required to keep
	// shards in different databases from colliding when they share a
	// (tableGroup, shard) name.
	//
	// TODO: switch to cache.GetByShard once gateway routing populates
	// Target.ShardKey.Shard for every request.
	var candidates []*poolerConnection
	if lb.cache != nil {
		for _, entry := range lb.cache.All() {
			conn := entry.Rider
			if conn == nil {
				continue
			}
			if lb.matchesReplicaTarget(conn, target) {
				candidates = append(candidates, conn)
			}
		}
	}

	if len(candidates) == 0 {
		return nil, mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE,
			"no pooler found for target: database=%s, tablegroup=%s, shard=%s, mode=%s",
			sk.GetDatabase(), sk.GetTableGroup(), sk.GetShard(), mode.String())
	}

	selected := lb.selectReplicaConnection(candidates)
	if selected == nil {
		// All replicas exceeded the replication lag threshold.
		return nil, mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE,
			"no replica with acceptable replication lag for target: tablegroup=%s, shard=%s",
			target.GetShardKey().GetTableGroup(), target.GetShardKey().GetShard())
	}
	return selected, nil
}

// getConnectionByID returns a poolerConnection for a specific pooler ID.
// This is used for reserved connections where queries need to be routed to
// a specific pooler instance (e.g., for session affinity with prepared statements).
// Returns an error immediately if the pooler connection doesn't exist (fail-fast).
func (lb *loadBalancer) getConnectionByID(poolerID *clustermetadatapb.ID) (*poolerConnection, error) {
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
func (lb *loadBalancer) selectReplicaConnection(candidates []*poolerConnection) *poolerConnection {
	lowThreshold := lb.lowReplicationLagNs
	highThreshold := lb.highReplicationLagToleranceNs
	hasLagFilter := lowThreshold > 0 || highThreshold > 0

	// Single-pass: lag filtering + locality categorization combined.
	// "healthy" = lag within lowThreshold (or unknown). "tolerable" = between
	// low and high thresholds. Each bucket is further split by locality.
	type bucket struct {
		localServing, remoteServing, localNotServing []*poolerConnection
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
			isServing := health.isServing()

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
			isServing := health.isServing()

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

// onPoolerHealthUpdate is the callback invoked by poolerConnection when health
// state changes. It folds the LeaderObservation into the shard's
// shardSummary — recording the named leader unconditionally, regardless of
// whether we currently have a connection to that leader. The connection
// lookup happens at GetConnection time via the cache, so identity survives
// any connection lifecycle event.
//
// After folding the observation in, the SERVING-leader notification fires
// unconditionally: notifyIfLeaderServing is idempotent (StopBuffering is
// idempotent and the SERVING check is a pure read), so this covers both the
// "new rule installed" case and the previously-distinct "same rule but the
// leader has just become SERVING" case in one path.
//
// Concurrency: shardSummary briefly holds lb.mu, then releases it; the
// summary's own mu protects its leader field. Cache lookups for the
// SERVING-leader notification happen with no LB locks held.
//
// Safe to call concurrently: both processHealthResponse and setHealthError
// release healthMu before invoking this callback.
func (lb *loadBalancer) onPoolerHealthUpdate(conn *poolerConnection) {
	health := conn.Health()
	if health == nil {
		return
	}

	poolerID := topoclient.ComponentIDString(conn.PoolerInfo().GetId())
	summary := lb.summaryForPooler(conn.PoolerInfo().Multipooler)

	// A pooler is a routing primary iff its broadcast advertises role PRIMARY.
	// That implies writability: a pooler only advertises PRIMARY once it is the
	// writable routing primary (out of recovery and the active committed leader),
	// so the gateway needs no separate writability check. Anything else — a
	// replica (role REPLICA / UNKNOWN) or no routing state — retracts any prior
	// claim (demotion / no-longer-primary).
	rs := health.RoutingState
	if rs.GetRole() == clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY {
		if summary.setPrimary(poolerID, rs) {
			lb.logger.Debug("routing primary recorded",
				"tablegroup", summary.shardKey.GetTableGroup(),
				"shard", summary.shardKey.GetShard(),
				"leader_id", poolerID,
				"rule", commonconsensus.FormatRuleNumber(rs.GetRule()))
		}
	} else {
		summary.clearPrimary(poolerID)
	}

	// Re-check the SERVING-leader notification: if the elected routing primary is
	// this (or any) connected pooler and it is serving, drain the failover buffer.
	// Idempotent and cheap, so always calling it is simpler than tracking deltas.
	leaderID, ok := summary.leaderID()
	if !ok || lb.cache == nil {
		return
	}
	if leaderConn, ok := lb.cache.GetRider(leaderID); ok && leaderConn != nil {
		lb.notifyLeaderServingFromSummary(summary, leaderConn)
	}
}

// matchesShardTarget checks if a connection matches the target's
// (database, tableGroup, shard), regardless of pooler type. Database and
// tableGroup must match exactly. An empty target Shard matches any shard
// within the matched (database, tableGroup) — the unsharded-routing
// fallback used while the gateway does not yet populate shard.
func matchesShardTarget(conn *poolerConnection, target *query.Target) bool {
	poolerKey := conn.PoolerInfo().GetShardKey()
	targetKey := target.GetShardKey()

	if targetKey.GetDatabase() != poolerKey.GetDatabase() {
		return false
	}
	if targetKey.GetTableGroup() != poolerKey.GetTableGroup() {
		return false
	}
	if targetKey.GetShard() != "" && targetKey.GetShard() != poolerKey.GetShard() {
		return false
	}
	return true
}

// matchesReplicaTarget reports whether conn is eligible to serve REPLICA
// traffic for target: it is in the shard and does not claim to be a routing
// primary. Leadership is judged from the shard summary's routing-primary set
// (fed by health-stream observations, see claimsPrimary), never from the
// topology Type label.
//
// Any primary claimant is excluded, not just the elected one. The reason is
// forward-looking, not about stale timelines: a pooler that claims primary but
// is not (or is no longer) the winning leader is headed for demotion, and
// demoting it back to a replica restarts postgres — often with a pg_rewind to
// discard diverged WAL. That restart drains gracefully (in-flight queries get a
// grace period to finish), so it is not an abrupt kill; but any query slow
// enough to outlast the grace period is interrupted. Sending fresh replica reads
// to a pooler we expect to restart soon needlessly exposes them to that risk, so
// a lower-rule (stale) claimant is excluded too, until it stops claiming primary.
//
// TODO(replica-quality): eligibility is otherwise binary. Longer term, treat a
// replica as degraded when it is not actively replicating or has fallen behind,
// and prefer non-degraded replicas when several are available. That belongs in
// selectReplicaConnection (which already tiers by lag), with the per-pooler
// signal carried on the health observation.
func (lb *loadBalancer) matchesReplicaTarget(conn *poolerConnection, target *query.Target) bool {
	if !matchesShardTarget(conn, target) {
		return false
	}
	return !lb.claimsPrimary(conn)
}

// claimsPrimary reports whether conn currently claims to be a routing primary
// for its shard, per the shard summary's routing-primary set. It is the single
// authority for the "believes itself the writable primary" signal — the same
// signal onPoolerHealthUpdate folds into the summary — consulted by both
// leadershipFor (stale-leader label) and matchesReplicaTarget (replica
// exclusion).
func (lb *loadBalancer) claimsPrimary(conn *poolerConnection) bool {
	if conn == nil {
		return false
	}
	key := shardKeyOf(conn.PoolerInfo().GetShardKey())
	lb.mu.Lock()
	summary := lb.shards[key]
	lb.mu.Unlock()
	return summary != nil && summary.hasPrimary(conn.ID())
}

// connectionCount returns the number of active connections, sourced from the
// pooler cache.
func (lb *loadBalancer) connectionCount() int {
	if lb.cache == nil {
		return 0
	}
	return lb.cache.Len()
}

// Consensus leadership roles reported by LeadershipFor for the status page.
const (
	// leadershipLeader marks the shard's current consensus leader.
	leadershipLeader = "leader"
	// leadershipStaleLeader marks a pooler that still believes itself the leader
	// (per its own self_leadership and health observation) even though consensus
	// has moved on to a higher-rule leader. Such a pooler is excluded from
	// replica reads; see matchesReplicaTarget.
	leadershipStaleLeader = "stale-leader"
	// leadershipFollower marks any other connected pooler.
	leadershipFollower = "follower"
)

// leadershipFor returns the consensus leadership role of a single connected
// pooler for the admin/status page. The role reflects the gateway's live view —
// the shard's shardSummary routing-primary set, fed by health-stream
// observations — never the topology Type label.
func (lb *loadBalancer) leadershipFor(conn *poolerConnection) string {
	if conn == nil {
		return leadershipFollower
	}
	info := conn.PoolerInfo()
	key := shardKeyOf(info.GetShardKey())

	lb.mu.Lock()
	summary := lb.shards[key]
	lb.mu.Unlock()
	if summary == nil {
		return leadershipFollower
	}
	leaderID, ok := summary.leaderID()

	// The shard's elected routing primary (highest-rule claimant across all
	// health-stream reports) takes precedence. A pooler that still claims to be
	// a routing primary but is not the elected one is stale — consensus has
	// moved on to a higher rule.
	switch {
	case ok && leaderID == conn.ID():
		return leadershipLeader
	case summary.hasPrimary(conn.ID()):
		return leadershipStaleLeader
	}
	return leadershipFollower
}

// onPoolerGone is the LB's reaction to OnGone in the cache. If no poolers
// remain in this shard after the eviction, the shard's shardSummary is
// removed. Called by the init.go OnGone hook AFTER the cache has already
// removed the rider.
//
// Concurrency: the cache is queried WITHOUT holding lb.mu, then lb.mu is
// acquired briefly to delete the entry if the shard is empty. There is a
// benign TOCTOU: a concurrent OnLive for the same shard between the cache
// check and the lb.mu acquisition could race the delete, leaving a stale
// shardSummary or removing a freshly-created one. In the worst case we keep
// a shardSummary one cycle longer; the next onPoolerGone (or first
// observation for a re-added pooler) will reconcile it.
//
// TODO: shardKey is currently database-agnostic — if two databases share a
// (tableGroup, shard) name, draining poolers from one database can drop the
// summary used by another. The next observation from the other database
// will re-populate it. Tolerable in practice but resolves cleanly once
// query.Target carries a Database field and shardKey gains a database
// component.
func (lb *loadBalancer) onPoolerGone(p *clustermetadatapb.Multipooler) {
	if p == nil || lb.cache == nil {
		return
	}
	sk := p.GetShardKey()
	key := shardKeyOf(sk)

	lb.mu.Lock()
	summary := lb.shards[key]
	lb.mu.Unlock()

	// Retract the departing pooler's routing-primary claim even when other
	// poolers remain — otherwise a leader that left the topology would stick in
	// the set as a phantom primary.
	if summary != nil {
		summary.clearPrimary(topoclient.ComponentIDString(p.GetId()))
	}

	// Drop the summary entirely once no poolers remain in the shard.
	if len(lb.cache.GetByShard(sk.GetDatabase(), sk.GetTableGroup(), sk.GetShard())) > 0 {
		return
	}
	lb.mu.Lock()
	delete(lb.shards, key)
	lb.mu.Unlock()
}
