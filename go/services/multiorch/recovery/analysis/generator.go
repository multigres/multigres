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

// PoolersByShard is a structured map for efficient lookups.
// Structure: [database][tablegroup][shard][pooler_id] -> PoolerHealthState
type PoolersByShard map[string]map[string]map[string]map[string]*multiorchdatapb.PoolerHealthState

// AnalysisGenerator creates ReplicationAnalysis from the pooler store.
type AnalysisGenerator struct {
	poolerStore    *store.PoolerStore
	poolersByShard PoolersByShard
	// policyLookup returns the bootstrap durability policy for a database name.
	// May be nil; when nil, ShardAnalysis.BootstrapDurabilityPolicy is left nil.
	policyLookup func(database string) *clustermetadatapb.DurabilityPolicy
}

// NewAnalysisGenerator creates a new analysis generator.
// It eagerly builds the poolersByShard map from the current store state.
// policyLookup is optional; pass nil if the bootstrap policy is unavailable.
func NewAnalysisGenerator(poolerStore *store.PoolerStore, policyLookup func(database string) *clustermetadatapb.DurabilityPolicy) *AnalysisGenerator {
	g := &AnalysisGenerator{
		poolerStore:  poolerStore,
		policyLookup: policyLookup,
	}
	g.poolersByShard = g.buildPoolersByShard()
	return g
}

// GenerateShardAnalyses groups per-pooler analyses into one ShardAnalysis per shard.
func (g *AnalysisGenerator) GenerateShardAnalyses() []*ShardAnalysis {
	// Group analyses by shard key.
	type shardEntry struct {
		key      commontypes.ShardKey
		analyses []*PoolerAnalysis
	}
	byKey := make(map[commontypes.ShardKey]*shardEntry)

	for database, tableGroups := range g.poolersByShard {
		for tableGroup, shards := range tableGroups {
			for shard, poolers := range shards {
				key := commontypes.ShardKey{Database: database, TableGroup: tableGroup, Shard: shard}
				entry := &shardEntry{key: key}
				for _, pooler := range poolers {
					entry.analyses = append(entry.analyses, g.generateAnalysisForPooler(pooler, key))
				}
				byKey[key] = entry
			}
		}
	}

	result := make([]*ShardAnalysis, 0, len(byKey))
	for _, entry := range byKey {
		var policy *clustermetadatapb.DurabilityPolicy
		if g.policyLookup != nil {
			policy = g.policyLookup(entry.key.Database)
		}

		numInitialized := 0
		for _, pa := range entry.analyses {
			if pa.LastCheckValid && pa.IsInitialized {
				numInitialized++
			}
		}

		result = append(result, &ShardAnalysis{
			ShardKey:                  entry.key,
			Analyses:                  entry.analyses,
			NumInitialized:            numInitialized,
			BootstrapDurabilityPolicy: policy,
		})
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

	var analyses []*PoolerAnalysis
	for _, pooler := range poolers {
		analyses = append(analyses, g.generateAnalysisForPooler(pooler, shardKey))
	}

	var policy *clustermetadatapb.DurabilityPolicy
	if g.policyLookup != nil {
		policy = g.policyLookup(shardKey.Database)
	}

	numInitialized := 0
	for _, pa := range analyses {
		if pa.LastCheckValid && pa.IsInitialized {
			numInitialized++
		}
	}

	return &ShardAnalysis{
		ShardKey:                  shardKey,
		Analyses:                  analyses,
		NumInitialized:            numInitialized,
		BootstrapDurabilityPolicy: policy,
	}, nil
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

// GenerateAnalysisForPooler generates analysis for a single pooler using the cached poolersByShard.
// If fresh data is needed (e.g., after re-polling the store), create a new AnalysisGenerator.
func (g *AnalysisGenerator) GenerateAnalysisForPooler(poolerIDStr string) (*PoolerAnalysis, error) {
	// Get pooler from store
	pooler, ok := g.poolerStore.Get(poolerIDStr)
	if !ok {
		return nil, fmt.Errorf("pooler not found in store: %s", poolerIDStr)
	}

	if pooler == nil || pooler.MultiPooler == nil || pooler.MultiPooler.Id == nil {
		return nil, fmt.Errorf("pooler or ID is nil: %s", poolerIDStr)
	}

	// Generate analysis for this specific pooler using the cached poolersByShard.
	// Note: If fresh data is needed (e.g., after re-polling), create a new AnalysisGenerator.
	shardKey := commontypes.ShardKey{
		Database:   pooler.MultiPooler.Database,
		TableGroup: pooler.MultiPooler.TableGroup,
		Shard:      pooler.MultiPooler.Shard,
	}
	analysis := g.generateAnalysisForPooler(pooler, shardKey)

	return analysis, nil
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
		CohortMembers:    pooler.CohortMembers,
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

	// If this is a PRIMARY, check for stale primaries by looking for other PRIMARYs in the shard
	if analysis.IsPrimary {
		g.detectOtherPrimary(analysis, shardKey, pooler)
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

		// Lookup primary info
		g.populatePrimaryInfo(analysis, shardKey)
	}

	return analysis
}

// populatePrimaryInfo looks up the primary this replica is replicating from.
func (g *AnalysisGenerator) populatePrimaryInfo(
	analysis *PoolerAnalysis,
	shardKey commontypes.ShardKey,
) {
	poolers, ok := g.poolersByShard[shardKey.Database][shardKey.TableGroup][shardKey.Shard]
	if !ok {
		return
	}

	// Find the primary with the highest PrimaryTerm in the same shard.
	// During a failover, two primaries may transiently coexist: the stale old primary
	// (postgres dead) and the newly elected one (postgres running). Selecting by
	// PrimaryTerm ensures we always use the most recently elected primary, preventing
	// PrimaryIsDeadAnalyzer from falsely triggering on the stale one.
	var primary *multiorchdatapb.PoolerHealthState
	var highestPrimaryTerm int64
	for _, pooler := range poolers {
		if pooler == nil || pooler.MultiPooler == nil || pooler.MultiPooler.Id == nil {
			continue
		}

		if pooler.PoolerType != clustermetadatapb.PoolerType_PRIMARY {
			continue
		}

		var primaryTerm int64
		if pooler.ConsensusTerm != nil {
			primaryTerm = pooler.ConsensusTerm.PrimaryTerm
		}
		if primary == nil || primaryTerm > highestPrimaryTerm {
			primary = pooler
			highestPrimaryTerm = primaryTerm
		}
	}

	if primary == nil {
		return // no primary found
	}

	// Found the primary - populate basic fields
	analysis.PrimaryPoolerID = primary.MultiPooler.Id

	// Track primary health details separately (for distinguishing pooler-down vs postgres-down)
	analysis.PrimaryPoolerReachable = primary.IsLastCheckValid
	analysis.PrimaryPostgresRunning = primary.IsPostgresRunning

	// Primary is reachable only if both pooler is reachable AND Postgres is running
	analysis.PrimaryReachable = analysis.PrimaryPoolerReachable && analysis.PrimaryPostgresRunning

	// Check if this replica is in the primary's synchronous standby list
	analysis.IsInPrimaryStandbyList = g.isInStandbyList(analysis.PoolerID, primary)

	// Compute ReplicasConnectedToPrimary: true only if ALL replicas are connected to primary.
	// When the primary pooler is down but Postgres is still running, replicas remain connected
	// and we should NOT trigger failover. Instead, the operator should restart the pooler process.
	analysis.ReplicasConnectedToPrimary = g.allReplicasConnectedToPrimary(primary, poolers)
}

// isInStandbyList checks if the given pooler ID is in the primary's synchronous standby list.
func (g *AnalysisGenerator) isInStandbyList(
	replicaID *clustermetadatapb.ID,
	primary *multiorchdatapb.PoolerHealthState,
) bool {
	if primary.PrimaryStatus == nil || primary.PrimaryStatus.SyncReplicationConfig == nil {
		return false
	}

	for _, standbyID := range primary.PrimaryStatus.SyncReplicationConfig.StandbyIds {
		if standbyID.Cell == replicaID.Cell && standbyID.Name == replicaID.Name {
			return true
		}
	}

	return false
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

// detectOtherPrimary checks for all other PRIMARYs in the same shard.
// Populates OtherPrimariesInShard and determines HighestTermPrimary based on PrimaryTerm.
// This is used to detect stale primaries that came back online after failover.
func (g *AnalysisGenerator) detectOtherPrimary(
	analysis *PoolerAnalysis,
	shardKey commontypes.ShardKey,
	thisPooler *multiorchdatapb.PoolerHealthState,
) {
	poolers, ok := g.poolersByShard[shardKey.Database][shardKey.TableGroup][shardKey.Shard]
	if !ok {
		return
	}

	thisIDStr := topoclient.MultiPoolerIDString(thisPooler.MultiPooler.Id)
	var otherPrimaries []*PrimaryInfo

	// Collect ALL other primaries (not just first one)
	for poolerID, pooler := range poolers {
		// Skip self
		if poolerID == thisIDStr {
			continue
		}

		// Skip if not reachable (can't trust stale data)
		if !pooler.IsLastCheckValid {
			continue
		}

		// Check if this pooler also thinks it's PRIMARY
		poolerType := pooler.PoolerType
		if poolerType == clustermetadatapb.PoolerType_UNKNOWN && pooler.MultiPooler != nil {
			poolerType = pooler.MultiPooler.Type
		}

		if poolerType == clustermetadatapb.PoolerType_PRIMARY {
			// Extract consensus term and primary term
			var consensusTerm, primaryTerm int64
			if pooler.ConsensusStatus != nil {
				consensusTerm = pooler.ConsensusStatus.CurrentTerm
			}
			if pooler.ConsensusTerm != nil {
				primaryTerm = pooler.ConsensusTerm.PrimaryTerm
			}

			otherPrimaries = append(otherPrimaries, &PrimaryInfo{
				ID:            pooler.MultiPooler.Id,
				ConsensusTerm: consensusTerm,
				PrimaryTerm:   primaryTerm,
			})
		}
	}

	// Populate other primaries list (empty if none detected)
	analysis.OtherPrimariesInShard = otherPrimaries

	// Find most advanced primary (include THIS pooler in comparison)
	// This should always be set for PRIMARY poolers, even if there are no other primaries
	allPrimaries := []*PrimaryInfo{
		{
			ID:            thisPooler.MultiPooler.Id,
			ConsensusTerm: analysis.ConsensusTerm,
			PrimaryTerm:   analysis.PrimaryTerm,
		},
	}
	allPrimaries = append(allPrimaries, otherPrimaries...)
	analysis.HighestTermPrimary = findHighestTermPrimary(allPrimaries)
}

// findHighestTermPrimary returns the primary with the highest PrimaryTerm.
// Returns nil if there's a tie between primaries with the same highest PrimaryTerm.
//
// Invariant: In a properly initialized shard, PrimaryTerm is always >0 for PRIMARY poolers.
// PrimaryTerm is set during promotion and only cleared during demotion. This function
// is defensive and returns nil if all primaries have PrimaryTerm=0, but this should
// never happen in a properly initialized shard.
func findHighestTermPrimary(primaries []*PrimaryInfo) *PrimaryInfo {
	var mostAdvanced *PrimaryInfo
	maxPrimaryTerm := int64(0)
	tieDetected := false

	for _, p := range primaries {
		if p.PrimaryTerm > maxPrimaryTerm {
			maxPrimaryTerm = p.PrimaryTerm
			mostAdvanced = p
			tieDetected = false
		} else if p.PrimaryTerm == maxPrimaryTerm && p.PrimaryTerm > 0 {
			tieDetected = true
		}
	}

	// Defensive: should not happen in initialized shards, but guard against invalid state
	if maxPrimaryTerm == 0 {
		return nil
	}

	// Tie detected: multiple primaries with same PrimaryTerm indicates a consensus bug.
	// PrimaryTerm should be unique per primary and monotonically increasing. If two primaries
	// claim the same PrimaryTerm, something went wrong in the consensus protocol (bug in
	// promotion logic, data corruption, or split-brain).
	//
	// TODO: Rather than requiring manual intervention, multiorch could automatically resolve
	// this by starting a new term and reappointing one of the primaries, which would update
	// its primary_term and make the others stale. For now, we skip automatic demotion to
	// avoid making the situation worse without understanding the root cause.
	if tieDetected {
		return nil
	}

	return mostAdvanced
}
