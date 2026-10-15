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

	"github.com/multigres/multigres/go/common/topoclient"
	commontypes "github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

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
type PoolersByShard map[string]map[string]map[string]map[topoclient.ComponentID]*multiorchdatapb.PoolerHealthState

// AnalysisGenerator creates ReplicationAnalysis from the pooler store.
type AnalysisGenerator struct {
	// poolersByShard holds each pooler's health snapshot, captured once in
	// NewAnalysisGenerator so every analysis built from this generator reads
	// the same immutable state.
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
// It captures every pooler's current health snapshot and the tombstone set
// from the store once, so analyses built from it are internally consistent.
// policyLookup is optional; pass nil if the bootstrap policy is unavailable.
func NewAnalysisGenerator(poolerStore *store.PoolerCache, policyLookup func(database string) *clustermetadatapb.DurabilityPolicy) *AnalysisGenerator {
	g := &AnalysisGenerator{
		policyLookup: policyLookup,
		now:          time.Now,
		availability: DefaultAvailabilityPolicy(),
	}
	g.poolersByShard = buildPoolersByShard(poolerStore)
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
		poolers map[topoclient.ComponentID]*multiorchdatapb.PoolerHealthState
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
func (g *AnalysisGenerator) buildShardAnalysis(shardKey *clustermetadatapb.ShardKey, poolers map[topoclient.ComponentID]*multiorchdatapb.PoolerHealthState) *ShardAnalysis {
	sa := &ShardAnalysis{
		ShardKey:     shardKey,
		TombstoneIDs: g.tombstoneIDs,
		Now:          g.now(),
		Policy:       g.availability,
	}
	for _, pooler := range poolers {
		sa.Poolers = append(sa.Poolers, pooler)
	}
	if g.policyLookup != nil {
		sa.BootstrapDurabilityPolicy = g.policyLookup(shardKey.Database)
	}
	return sa
}

// buildPoolersByShard captures each cached pooler's current health snapshot,
// grouped by shard, in a single pass over the cache.
func buildPoolersByShard(poolerStore *store.PoolerCache) PoolersByShard {
	poolersByShard := make(PoolersByShard)
	if poolerStore == nil {
		return poolersByShard
	}

	for _, entry := range poolerStore.All() {
		if entry.Rider == nil {
			continue
		}
		health := entry.Rider.Health()
		if health.GetMultipooler().GetId() == nil {
			continue
		}

		database := health.GetMultipooler().GetShardKey().GetDatabase()
		tableGroup := health.GetMultipooler().GetShardKey().GetTableGroup()
		shard := health.GetMultipooler().GetShardKey().GetShard()

		if poolersByShard[database] == nil {
			poolersByShard[database] = make(map[string]map[string]map[topoclient.ComponentID]*multiorchdatapb.PoolerHealthState)
		}
		if poolersByShard[database][tableGroup] == nil {
			poolersByShard[database][tableGroup] = make(map[string]map[topoclient.ComponentID]*multiorchdatapb.PoolerHealthState)
		}
		if poolersByShard[database][tableGroup][shard] == nil {
			poolersByShard[database][tableGroup][shard] = make(map[topoclient.ComponentID]*multiorchdatapb.PoolerHealthState)
		}

		poolersByShard[database][tableGroup][shard][topoclient.ComponentIDString(health.GetMultipooler().GetId())] = health
	}

	return poolersByShard
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
func observationFresh(h *multiorchdatapb.PoolerHealthState, now time.Time, maxAge time.Duration) bool {
	age, ok := store.ObservationAge(h, now)
	if !ok {
		return false
	}
	return age <= maxAge
}
