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
	"slices"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	commontypes "github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	"github.com/multigres/multigres/go/services/multiorch/store"
	"github.com/multigres/multigres/go/tools/pgutil"
)

// PoolersByShard is a structured map for efficient lookups.
// Structure: [database][tablegroup][shard][pooler_id] -> PoolerHealthState
type PoolersByShard map[string]map[string]map[string]map[string]*multiorchdatapb.PoolerHealthState

// AnalysisGenerator creates ReplicationAnalysis from the pooler store.
type AnalysisGenerator struct {
	poolerStore    *store.PoolerStore
	poolersByShard PoolersByShard
}

// NewAnalysisGenerator creates a new analysis generator.
// It eagerly builds the poolersByShard map from the current store state.
func NewAnalysisGenerator(poolerStore *store.PoolerStore) *AnalysisGenerator {
	g := &AnalysisGenerator{
		poolerStore: poolerStore,
	}
	g.poolersByShard = g.buildPoolersByShard()
	return g
}

// GenerateShardAnalyses groups per-pooler analyses into one ShardAnalysis per shard.
func (g *AnalysisGenerator) GenerateShardAnalyses() []*ShardAnalysis {
	type shardEntry struct {
		key     commontypes.ShardKey
		poolers map[string]*multiorchdatapb.PoolerHealthState
	}
	byKey := make(map[commontypes.ShardKey]*shardEntry)

	for database, tableGroups := range g.poolersByShard {
		for tableGroup, shards := range tableGroups {
			for shard, poolers := range shards {
				key := commontypes.ShardKey{Database: database, TableGroup: tableGroup, Shard: shard}
				byKey[key] = &shardEntry{key: key, poolers: poolers}
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
func (g *AnalysisGenerator) GenerateShardAnalysis(shardKey commontypes.ShardKey) (*ShardAnalysis, error) {
	poolers, ok := g.poolersByShard[shardKey.Database][shardKey.TableGroup][shardKey.Shard]
	if !ok || len(poolers) == 0 {
		return nil, fmt.Errorf("shard not found: %s", shardKey)
	}
	return g.buildShardAnalysis(shardKey, poolers), nil
}

// buildShardAnalysis constructs a ShardAnalysis for a shard, including shard-level aggregates.
func (g *AnalysisGenerator) buildShardAnalysis(shardKey commontypes.ShardKey, poolers map[string]*multiorchdatapb.PoolerHealthState) *ShardAnalysis {
	sa := &ShardAnalysis{ShardKey: shardKey}
	for _, pooler := range poolers {
		sa.Analyses = append(sa.Analyses, g.generateAnalysisForPooler(pooler, shardKey))
	}
	g.computeShardLevelFields(sa, poolers)
	return sa
}

// buildPoolersByShard creates a structured map by iterating the store once.
// Since ProtoStore.Range() returns clones, we don't need explicit DeepCopy.
func (g *AnalysisGenerator) buildPoolersByShard() PoolersByShard {
	poolersByShard := make(PoolersByShard)

	g.poolerStore.Range(func(poolerID string, pooler *multiorchdatapb.PoolerHealthState) bool {
		if pooler == nil || pooler.MultiPooler == nil || pooler.MultiPooler.Id == nil {
			return true // skip nil entries
		}

		database := pooler.MultiPooler.Database
		tableGroup := pooler.MultiPooler.TableGroup
		shard := pooler.MultiPooler.Shard

		// Initialize nested maps if needed
		if poolersByShard[database] == nil {
			poolersByShard[database] = make(map[string]map[string]map[string]*multiorchdatapb.PoolerHealthState)
		}
		if poolersByShard[database][tableGroup] == nil {
			poolersByShard[database][tableGroup] = make(map[string]map[string]*multiorchdatapb.PoolerHealthState)
		}
		if poolersByShard[database][tableGroup][shard] == nil {
			poolersByShard[database][tableGroup][shard] = make(map[string]*multiorchdatapb.PoolerHealthState)
		}

		// Store the pooler (already a clone from Range)
		poolersByShard[database][tableGroup][shard][poolerID] = pooler
		return true // continue
	})

	return poolersByShard
}

// GetPoolersInShard returns all pooler IDs in the same shard as the given pooler.
// Uses the cached poolersByShard for efficient lookup.
func (g *AnalysisGenerator) GetPoolersInShard(poolerIDStr string) ([]string, error) {
	// Get pooler from store to determine its shard
	pooler, ok := g.poolerStore.Get(poolerIDStr)
	if !ok {
		return nil, fmt.Errorf("pooler not found in store: %s", poolerIDStr)
	}

	if pooler == nil || pooler.MultiPooler == nil || pooler.MultiPooler.Id == nil {
		return nil, fmt.Errorf("pooler or ID is nil: %s", poolerIDStr)
	}

	database := pooler.MultiPooler.Database
	tableGroup := pooler.MultiPooler.TableGroup
	shard := pooler.MultiPooler.Shard

	// Use cached poolersByShard for efficient lookup
	poolers, ok := g.poolersByShard[database][tableGroup][shard]
	if !ok {
		return []string{}, nil
	}

	poolerIDs := make([]string, 0, len(poolers))
	for id := range poolers {
		poolerIDs = append(poolerIDs, id)
	}

	return poolerIDs, nil
}

// generateAnalysisForPooler creates a ReplicationAnalysis for a single pooler.
func (g *AnalysisGenerator) generateAnalysisForPooler(
	pooler *multiorchdatapb.PoolerHealthState,
	shardKey commontypes.ShardKey,
) *PoolerAnalysis {
	// Determine pooler type from health check (PoolerType).
	// Nodes are never created with topology type PRIMARY, so health check is authoritative.
	// Fall back to topology type only if health check type is UNKNOWN.
	poolerType := pooler.PoolerType
	if poolerType == clustermetadatapb.PoolerType_UNKNOWN {
		poolerType = pooler.MultiPooler.Type
	}

	analysis := &PoolerAnalysis{
		PoolerID:         pooler.MultiPooler.Id,
		ShardKey:         shardKey,
		PoolerType:       poolerType,
		IsPrimary:        poolerType == clustermetadatapb.PoolerType_PRIMARY,
		LastCheckValid:   pooler.IsLastCheckValid,
		IsInitialized:    store.IsInitialized(pooler),
		HasDataDirectory: pooler.HasDataDirectory,
		AnalyzedAt:       time.Now(),
	}

	// Compute staleness
	analysis.IsStale = !pooler.IsUpToDate

	// Store consensus term for stale primary detection
	if pooler.ConsensusStatus != nil {
		analysis.ConsensusTerm = pooler.ConsensusStatus.CurrentTerm
	}

	// Store primary term (term when this pooler was promoted to primary)
	if pooler.ConsensusTerm != nil {
		analysis.PrimaryTerm = pooler.ConsensusTerm.PrimaryTerm
	}

	// Store WAL position for timeline comparison (LSN is a secondary tiebreaker;
	// ignore parse errors — zero value is safe).
	// Primaries use their write LSN; replicas use their last applied (replay) LSN
	// since only applied data is readable (e.g. for cohort/coordinator term lookups).
	if analysis.IsPrimary && pooler.PrimaryStatus != nil {
		analysis.LSN, _ = pgutil.ParseLSN(pooler.PrimaryStatus.Lsn)
	} else if !analysis.IsPrimary && pooler.ReplicationStatus != nil {
		analysis.LSN, _ = pgutil.ParseLSN(pooler.ReplicationStatus.LastReplayLsn)
	}

	// If this is a REPLICA, populate replica-specific fields
	if !analysis.IsPrimary {
		if pooler.ReplicationStatus != nil {
			rs := pooler.ReplicationStatus
			analysis.ReplicationStopped = rs.IsWalReplayPaused

			// Extract primary connection info
			if rs.PrimaryConnInfo != nil {
				analysis.PrimaryConnInfoHost = rs.PrimaryConnInfo.Host
			}
		}
	}

	return analysis
}

// findHighestTermRawPooler returns the raw PoolerHealthState with the highest PrimaryTerm
// among all PRIMARY-typed poolers, regardless of reachability. Returns nil if none found.
func findHighestTermRawPooler(poolers map[string]*multiorchdatapb.PoolerHealthState) *multiorchdatapb.PoolerHealthState {
	// TODO: If multiple poolers claim to be primary at the same term, we should surface an error that
	// manual intervention is needed.

	var best *multiorchdatapb.PoolerHealthState
	var bestTerm int64
	for _, pooler := range poolers {
		if pooler == nil || pooler.MultiPooler == nil || pooler.MultiPooler.Id == nil {
			continue
		}
		if pooler.PoolerType != clustermetadatapb.PoolerType_PRIMARY {
			continue
		}
		var term int64
		if pooler.ConsensusTerm != nil {
			term = pooler.ConsensusTerm.PrimaryTerm
		}
		if best == nil || term > bestTerm {
			best = pooler
			bestTerm = term
		}
	}
	return best
}

// allReplicasConnectedToPrimary checks if ALL replicas in the shard are connected to the primary.
// A replica is considered connected if:
// 1. Its health check is valid (IsLastCheckValid)
// 2. It has PrimaryConnInfo configured pointing to this primary
// 3. It has received WAL (LastReceiveLsn is not empty)
//
// Returns true only if all replicas meet these criteria.
// Returns false if there are no replicas or any replica is disconnected.
func (g *AnalysisGenerator) allReplicasConnectedToPrimary(
	primary *multiorchdatapb.PoolerHealthState,
	poolers map[string]*multiorchdatapb.PoolerHealthState,
) bool {
	primaryIDStr := topoclient.MultiPoolerIDString(primary.MultiPooler.Id)
	primaryHost := primary.MultiPooler.Hostname
	primaryPort := primary.MultiPooler.PortMap["postgres"]

	replicaCount := 0
	connectedCount := 0

	for poolerID, pooler := range poolers {
		if pooler == nil || pooler.MultiPooler == nil || pooler.MultiPooler.Id == nil {
			continue
		}

		// Skip the primary itself
		if poolerID == primaryIDStr {
			continue
		}

		// Skip non-replicas
		replicaType := pooler.PoolerType
		if replicaType == clustermetadatapb.PoolerType_UNKNOWN {
			replicaType = pooler.MultiPooler.Type
		}
		if replicaType != clustermetadatapb.PoolerType_REPLICA {
			continue
		}

		replicaCount++

		// Check if replica is connected to the primary
		if !g.isReplicaConnectedToPrimary(pooler, primaryHost, primaryPort) {
			continue
		}

		connectedCount++
	}

	// All replicas must be connected (and there must be at least one replica)
	return replicaCount > 0 && connectedCount == replicaCount
}

// isReplicaConnectedToPrimary checks if a single replica is connected to the primary.
//
// TODO: Check heartbeat data timestamp to verify writes are actively flowing through replication.
// The multigres.heartbeat table is updated periodically on the primary, so checking if the
// replica's heartbeat timestamp is recent would prove the replication connection is active.
// Currently we check that LastReceiveLsn is non-empty, but this doesn't prove active connectivity.
func (g *AnalysisGenerator) isReplicaConnectedToPrimary(
	replica *multiorchdatapb.PoolerHealthState,
	primaryHost string,
	primaryPort int32,
) bool {
	// Replica must be reachable
	if !replica.IsLastCheckValid {
		return false
	}

	// Replica must have replication status
	if replica.ReplicationStatus == nil {
		return false
	}

	// Replica must have PrimaryConnInfo pointing to the primary
	connInfo := replica.ReplicationStatus.PrimaryConnInfo
	if connInfo == nil || connInfo.Host == "" {
		return false
	}

	// Verify the replica is pointing to the correct primary
	if connInfo.Host != primaryHost || connInfo.Port != primaryPort {
		return false
	}

	// Replica must have received WAL (indicates connection was established)
	if replica.ReplicationStatus.LastReceiveLsn == "" {
		return false
	}

	return true
}

// computeShardLevelFields populates shard-level aggregates on sa after all per-pooler
// analyses have been built. These fields describe the shard as a whole rather than
// any individual pooler, so they are computed once here rather than per-pooler.
func (g *AnalysisGenerator) computeShardLevelFields(sa *ShardAnalysis, poolers map[string]*multiorchdatapb.PoolerHealthState) {
	// Collect all reachable primaries in the shard.
	for _, pa := range sa.Analyses {
		if pa.IsPrimary && pa.LastCheckValid {
			sa.Primaries = append(sa.Primaries, pa)
		}
	}

	// Determine the highest-term primary (used for stale-primary detection).
	sa.HighestTermReachablePrimary = findHighestTermPooler(sa.Primaries)

	// Compute topology primary: the highest-term primary in the shard regardless of reachability.
	// This may differ from HighestTermPrimary when the primary pooler is down.
	topologyPrimary := findHighestTermRawPooler(poolers)
	if topologyPrimary != nil {
		sa.HighestTermDiscoveredPrimaryID = topologyPrimary.MultiPooler.Id
		sa.PrimaryPoolerReachable = topologyPrimary.IsLastCheckValid
		sa.PrimaryReachable = topologyPrimary.IsLastCheckValid && topologyPrimary.IsPostgresRunning

		// Populate the standby list from the topology primary (used by IsInStandbyList).
		if topologyPrimary.PrimaryStatus != nil && topologyPrimary.PrimaryStatus.SyncReplicationConfig != nil {
			sa.PrimaryStandbyIDs = topologyPrimary.PrimaryStatus.SyncReplicationConfig.StandbyIds
		}
	}

	// HasInitializedReplica: any non-primary, reachable, initialized pooler.
	for _, pa := range sa.Analyses {
		if !pa.IsPrimary && pa.LastCheckValid && pa.IsInitialized {
			sa.HasInitializedReplica = true
			break
		}
	}

	// Determine if all replicas are still connected to the primary Postgres.
	// Use the topology primary (which may be unreachable) so we can detect the
	// "pooler down but Postgres still running" scenario that ReplicasConnectedToPrimary
	// is designed to catch.
	if topologyPrimary != nil {
		sa.ReplicasConnectedToPrimary = g.allReplicasConnectedToPrimary(topologyPrimary, poolers)
	}
}

// findHighestTermPooler returns the primary PoolerAnalysis with the highest PrimaryTerm.
// Returns nil if primaries is empty, all have PrimaryTerm=0, or there is a tie.
//
// Invariant: In a properly initialized shard, PrimaryTerm is always >0 for PRIMARY poolers.
// PrimaryTerm is set during promotion and only cleared during demotion. This function
// is defensive and returns nil if all primaries have PrimaryTerm=0, but this should
// never happen in a properly initialized shard.
func findHighestTermPooler(primaries []*PoolerAnalysis) *PoolerAnalysis {
	if len(primaries) == 0 {
		return nil
	}

	mostAdvanced := slices.MaxFunc(primaries, comparePrimaryTimeline)

	// Defensive: should not happen in initialized shards, but guard against invalid state
	if mostAdvanced.PrimaryTerm == 0 {
		return nil
	}

	// Tie detection: multiple primaries with the same PrimaryTerm indicates a consensus bug.
	// PrimaryTerm should be unique per primary and monotonically increasing. If two primaries
	// claim the same PrimaryTerm, something went wrong in the consensus protocol (bug in
	// promotion logic, data corruption, or split-brain).
	//
	// TODO: Rather than requiring manual intervention, multiorch could automatically resolve
	// this by starting a new term and reappointing one of the primaries, which would update
	// its primary_term and make the others stale. For now, we skip automatic demotion to
	// avoid making the situation worse without understanding the root cause.
	for _, p := range primaries {
		if p != mostAdvanced && comparePrimaryTimeline(p, mostAdvanced) == 0 {
			return nil
		}
	}

	return mostAdvanced
}
