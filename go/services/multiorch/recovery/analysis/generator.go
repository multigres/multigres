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

package analysis

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/topoclient"
	commontypes "github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// DefaultReplicaLagThreshold is the threshold above which a replica is considered lagging.
const DefaultReplicaLagThreshold = 10 * time.Second

// replicationHeartbeatStalenessMultiplier is applied to wal_receiver_status_interval
// to compute the heartbeat staleness threshold. The replica sends a status message
// to the primary every wal_receiver_status_interval; the primary echoes a keepalive
// reply. Three missed intervals means the primary has gone silent well before the
// wal_receiver_timeout (60s) would disconnect the WAL receiver.
const replicationHeartbeatStalenessMultiplier = 3

// defaultReplicationHeartbeatStalenessThreshold is the fallback threshold used
// when wal_receiver_status_interval is not available in the replica's health
// state. Equals replicationHeartbeatStalenessMultiplier × the default
// wal_receiver_status_interval (10s).
const defaultReplicationHeartbeatStalenessThreshold = 30 * time.Second

// PoolersByShard is a structured map for efficient lookups.
// Structure: [database][tablegroup][shard][pooler_id] -> PoolerHealthState
type PoolersByShard map[string]map[string]map[string]map[topoclient.ComponentID]*store.Pooler

// AnalysisGenerator creates ReplicationAnalysis from the pooler store.
type AnalysisGenerator struct {
	poolerStore    *store.PoolerCache
	poolersByShard PoolersByShard
	// tombstoneIDs is the set of pooler IDs the cache currently tracks as
	// SHUTDOWN tombstones — riders evicted by OnGone but retained for cleanup.
	// Surfaced to analyzers via ShardAnalysis.TombstoneIDs.
	tombstoneIDs map[topoclient.ComponentID]struct{}
	// policyLookup returns the bootstrap durability policy for a database name.
	// May be nil; when nil, ShardAnalysis.BootstrapDurabilityPolicy is left nil.
	policyLookup func(database string) *clustermetadatapb.DurabilityPolicy
	now          func() time.Time
	// availability holds the freshness thresholds (and, over time, other
	// decision knobs) used when judging pooler liveness. Defaults to
	// DefaultAvailabilityPolicy; tests may override.
	availability AvailabilityPolicy
}

// NewAnalysisGenerator creates a new analysis generator.
// It eagerly builds the poolersByShard map and tombstone set from the current
// store state. policyLookup is optional; pass nil if the bootstrap policy
// is unavailable.
func NewAnalysisGenerator(poolerStore *store.PoolerCache, policyLookup func(database string) *clustermetadatapb.DurabilityPolicy) *AnalysisGenerator {
	g := &AnalysisGenerator{
		poolerStore:  poolerStore,
		policyLookup: policyLookup,
		now:          time.Now,
		availability: DefaultAvailabilityPolicy(),
	}
	g.poolersByShard = g.buildPoolersByShard()
	if poolerStore != nil {
		tombstones := poolerStore.Tombstones()
		g.tombstoneIDs = make(map[topoclient.ComponentID]struct{}, len(tombstones))
		for _, gh := range tombstones {
			g.tombstoneIDs[topoclient.ComponentIDString(gh.ID)] = struct{}{}
		}
	}
	return g
}

// GenerateShardAnalyses groups per-pooler analyses into one ShardAnalysis per shard.
func (g *AnalysisGenerator) GenerateShardAnalyses() []*ShardAnalysis {
	type shardEntry struct {
		key     *clustermetadatapb.ShardKey
		poolers map[topoclient.ComponentID]*store.Pooler
	}
	byKey := make(map[string]*shardEntry)

	for database, tableGroups := range g.poolersByShard {
		for tableGroup, shards := range tableGroups {
			for shard, poolers := range shards {
				key := &clustermetadatapb.ShardKey{Database: database, TableGroup: tableGroup, Shard: shard}
				byKey[string(commontypes.FormatShardKey(key))] = &shardEntry{key: key, poolers: poolers}
			}
		}
	}

	result := make([]*ShardAnalysis, 0, len(byKey))
	for _, entry := range byKey {
		result = append(result, g.buildShardAnalysis(entry.key, entry.poolers))
	}
	return result
}

// GenerateShardAnalysis returns a ShardAnalysis for a specific shard.
// Returns an error if no poolers for that shard are found in the store.
func (g *AnalysisGenerator) GenerateShardAnalysis(shardKey *clustermetadatapb.ShardKey) (*ShardAnalysis, error) {
	poolers, ok := g.poolersByShard[shardKey.Database][shardKey.TableGroup][shardKey.Shard]
	if !ok || len(poolers) == 0 {
		return nil, fmt.Errorf("shard not found: %s", commontypes.FormatShardKey(shardKey))
	}
	return g.buildShardAnalysis(shardKey, poolers), nil
}

// buildShardAnalysis constructs a ShardAnalysis for a shard, including shard-level aggregates.
func (g *AnalysisGenerator) buildShardAnalysis(shardKey *clustermetadatapb.ShardKey, poolers map[topoclient.ComponentID]*store.Pooler) *ShardAnalysis {
	sa := &ShardAnalysis{
		ShardKey:     shardKey,
		TombstoneIDs: g.tombstoneIDs,
		Now:          g.now(),
		Policy:       g.availability,
	}
	for _, pooler := range poolers {
		sa.Analyses = append(sa.Analyses, pooler)
	}
	g.computeShardLevelFields(sa, poolers)
	return sa
}

// buildPoolersByShard creates a structured map by iterating the cache once.
func (g *AnalysisGenerator) buildPoolersByShard() PoolersByShard {
	poolersByShard := make(PoolersByShard)

	for _, entry := range g.poolerStore.All() {
		pooler := entry.Rider
		if pooler == nil || pooler.Health().MultiPooler == nil || pooler.Health().MultiPooler.Id == nil {
			continue
		}

		database := pooler.Health().MultiPooler.GetShardKey().GetDatabase()
		tableGroup := pooler.Health().MultiPooler.GetShardKey().GetTableGroup()
		shard := pooler.Health().MultiPooler.GetShardKey().GetShard()

		if poolersByShard[database] == nil {
			poolersByShard[database] = make(map[string]map[string]map[topoclient.ComponentID]*store.Pooler)
		}
		if poolersByShard[database][tableGroup] == nil {
			poolersByShard[database][tableGroup] = make(map[string]map[topoclient.ComponentID]*store.Pooler)
		}
		if poolersByShard[database][tableGroup][shard] == nil {
			poolersByShard[database][tableGroup][shard] = make(map[topoclient.ComponentID]*store.Pooler)
		}

		poolersByShard[database][tableGroup][shard][topoclient.ComponentIDString(pooler.Health().MultiPooler.Id)] = pooler
	}

	return poolersByShard
}

// GetPoolersInShard returns all pooler IDs in the same shard as the given pooler.
// Uses the cached poolersByShard for efficient lookup.
func (g *AnalysisGenerator) GetPoolersInShard(poolerIDStr topoclient.ComponentID) ([]topoclient.ComponentID, error) {
	// Get pooler from store to determine its shard
	pooler, ok := g.poolerStore.GetRider(poolerIDStr)
	if !ok {
		return nil, fmt.Errorf("pooler not found in store: %s", poolerIDStr)
	}

	if pooler == nil || pooler.Health().MultiPooler == nil || pooler.Health().MultiPooler.Id == nil {
		return nil, fmt.Errorf("pooler or ID is nil: %s", poolerIDStr)
	}

	database := pooler.Health().MultiPooler.GetShardKey().GetDatabase()
	tableGroup := pooler.Health().MultiPooler.GetShardKey().GetTableGroup()
	shard := pooler.Health().MultiPooler.GetShardKey().GetShard()

	// Use cached poolersByShard for efficient lookup
	poolers, ok := g.poolersByShard[database][tableGroup][shard]
	if !ok {
		return []topoclient.ComponentID{}, nil
	}

	poolerIDs := make([]topoclient.ComponentID, 0, len(poolers))
	for id := range poolers {
		poolerIDs = append(poolerIDs, id)
	}

	return poolerIDs, nil
}

// GenerateAnalysisForPooler generates and returns the ShardAnalysis for the shard containing
// the given pooler ID. Used primarily in tests to inspect shard-level fields like
// ReplicasConnectedToLeader without running the full analysis loop.
//
// TODO: remove this method. It has no production callers (production uses
// GenerateShardAnalyses / GenerateShardAnalysis) and is exported only for tests.
// Migrate the test call sites to GenerateShardAnalysis(shardKey) — they already
// know the shard key — and delete this poolerID→shardKey convenience wrapper.
func (g *AnalysisGenerator) GenerateAnalysisForPooler(poolerIDStr topoclient.ComponentID) (*ShardAnalysis, error) {
	pooler, ok := g.poolerStore.GetRider(poolerIDStr)
	if !ok {
		return nil, fmt.Errorf("pooler not found in store: %s", poolerIDStr)
	}
	if pooler == nil || pooler.Health().MultiPooler == nil || pooler.Health().MultiPooler.Id == nil {
		return nil, fmt.Errorf("pooler or ID is nil: %s", poolerIDStr)
	}

	database := pooler.Health().MultiPooler.GetShardKey().GetDatabase()
	tableGroup := pooler.Health().MultiPooler.GetShardKey().GetTableGroup()
	shard := pooler.Health().MultiPooler.GetShardKey().GetShard()

	poolers, ok := g.poolersByShard[database][tableGroup][shard]
	if !ok || len(poolers) == 0 {
		return nil, fmt.Errorf("shard not found for pooler: %s", poolerIDStr)
	}

	shardKey := &clustermetadatapb.ShardKey{Database: database, TableGroup: tableGroup, Shard: shard}
	return g.buildShardAnalysis(shardKey, poolers), nil
}

// poolerByID returns the pooler with the given ID, or nil if id is nil or no
// pooler matches.
func poolerByID(poolers map[topoclient.ComponentID]*store.Pooler, id *clustermetadatapb.ID) *store.Pooler {
	if id == nil {
		return nil
	}
	for _, pooler := range poolers {
		if proto.Equal(pooler.Health().GetMultiPooler().GetId(), id) {
			return pooler
		}
	}
	return nil
}

// observationFresh reports whether the pooler's most recent successful health
// snapshot is recent enough (within maxAge, measured against now on the
// orchestrator clock) to be trusted as a live observation.
//
// A pooler that has never recorded a snapshot is NOT fresh: with no observation
// we have no evidence it is alive. Because LastSeen advances only on a
// successful snapshot (never on a disconnect), freshness alone captures
// "recently, successfully observed" — so callers need not separately consult a
// connection flag or a snapshot counter, and a brief stream interruption does
// not flip liveness until the observation genuinely ages out.
func observationFresh(p *store.Pooler, now time.Time, maxAge time.Duration) bool {
	age, ok := p.ObservationAge(now)
	if !ok {
		return false
	}
	return age <= maxAge
}

// computeShardLevelFields populates shard-level aggregates on sa after all per-pooler
// analyses have been built. These fields describe the shard as a whole rather than
// any individual pooler, so they are computed once here rather than per-pooler.
func (g *AnalysisGenerator) computeShardLevelFields(sa *ShardAnalysis, poolers map[topoclient.ComponentID]*store.Pooler) {
	// Bootstrap durability policy lookup.
	if g.policyLookup != nil {
		sa.BootstrapDurabilityPolicy = g.policyLookup(sa.ShardKey.Database)
	}

	// Count reachable, initialized poolers for bootstrap analysis.
	for _, pa := range sa.Analyses {
		if pa.Health().IsLastCheckValid && pa.IsInitialized() {
			sa.NumInitialized++
		}
	}

	// The shard's single leader is named by the highest known consensus rule
	// across all poolers — consensus is the source of truth, never the PoolerType
	// label. HighestShardRule.GetLeaderId() is the leader; GetCohortMembers() is its
	// recorded cohort. Whether that leader is reachable is captured separately below.
	statuses := make([]*clustermetadatapb.ConsensusStatus, 0, len(poolers))
	for _, pooler := range poolers {
		if cs := pooler.Health().GetConsensusStatus(); cs != nil {
			statuses = append(statuses, cs)
		}
	}
	sa.HighestShardRule = commonconsensus.HighestKnownRule(statuses)

	topologyPrimary := poolerByID(poolers, sa.HighestShardRule.GetLeaderId())
	sa.Leader = topologyPrimary
	if topologyPrimary != nil {
		sa.LeaderPostgresReady = topologyPrimary.Health().GetStatus().GetPostgresReady()
		sa.LeaderPostgresRunning = topologyPrimary.Health().GetStatus().GetPostgresRunning()
		// LeaderHasResigned: AvailabilityStatus and ConsensusTerm are populated from
		// StatusResponse on every health stream snapshot, so LeaderNeedsReplacement
		// correctly detects REQUESTING_DEMOTION signals without a separate RPC.
		sa.LeaderHasResigned = types.LeaderNeedsReplacement(topologyPrimary.Health())
		// LeaderReachable is the leader-led-change gate (Q3), used by the cohort
		// and replication-repair analyzers: the leader's pooler check is valid,
		// its postgres is serving, and it has not resigned. Liveness for failover
		// *detection* (Q1) is a different, freshness-aware question judged inside
		// LeaderIsDeadAnalyzer from the leader rider + sa.Now + sa.Policy, not from
		// this field.
		sa.LeaderReachable = topologyPrimary.Health().IsLastCheckValid &&
			topologyPrimary.Health().GetStatus().GetPostgresReady() &&
			!sa.LeaderHasResigned
		if topologyPrimary.Health().LastPostgresReadyTime != nil {
			sa.LeaderLastPostgresReadyTime = topologyPrimary.Health().LastPostgresReadyTime.AsTime()
		}

		// Detect pg_promote transition: multipooler explicitly signals promotion is running.
		if topologyPrimary.Health().GetStatus().GetPostgresStatus() == multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_PROMOTING {
			sa.PromotingPrimaryID = topologyPrimary.Health().MultiPooler.Id
		}
	}

	// HasInitializedReplica: any non-primary, reachable, initialized pooler.
	for _, pa := range sa.Analyses {
		if !namesSelfAsLeader(pa) && pa.Health().IsLastCheckValid && pa.IsInitialized() {
			sa.HasInitializedReplica = true
			break
		}
	}
}
