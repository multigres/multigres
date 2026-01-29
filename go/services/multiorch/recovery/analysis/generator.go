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

// DefaultReplicaLagThreshold is the threshold above which a replica is considered lagging.
const DefaultReplicaLagThreshold = 10 * time.Second

// PoolersByShard is a structured map for efficient lookups.
// Structure: [database][tablegroup][shard][pooler_id] -> PoolerHealthState
type PoolersByShard map[string]map[string]map[string]map[string]*multiorchdatapb.PoolerHealthState

// AnalysisGenerator creates ReplicationAnalysis from the pooler store.
type AnalysisGenerator struct {
	poolerStore    *store.PoolerHealthStore
	poolersByShard PoolersByShard
}

// NewAnalysisGenerator creates a new analysis generator.
// It eagerly builds the poolersByShard map from the current store state.
func NewAnalysisGenerator(poolerStore *store.PoolerHealthStore) *AnalysisGenerator {
	g := &AnalysisGenerator{
		poolerStore: poolerStore,
	}
	g.poolersByShard = g.buildPoolersByShard()
	return g
}

// GenerateAnalyses creates one ReplicationAnalysis per pooler in the store.
// This examines the current state and computes derived fields.
func (g *AnalysisGenerator) GenerateAnalyses() []*store.ReplicationAnalysis {
	analyses := []*store.ReplicationAnalysis{}

	for database, tableGroups := range g.poolersByShard {
		for tableGroup, shards := range tableGroups {
			for shard, poolers := range shards {
				shardKey := commontypes.ShardKey{
					Database:   database,
					TableGroup: tableGroup,
					Shard:      shard,
				}
				for _, pooler := range poolers {
					analysis := g.generateAnalysisForPooler(pooler, shardKey)
					analyses = append(analyses, analysis)
				}
			}
		}
	}

	return analyses
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
func (g *AnalysisGenerator) GenerateAnalysisForPooler(poolerIDStr string) (*store.ReplicationAnalysis, error) {
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
) *store.ReplicationAnalysis {
	// Determine pooler type from health check (PoolerType).
	// Nodes are never created with topology type PRIMARY, so health check is authoritative.
	// Fall back to topology type only if health check type is UNKNOWN.
	poolerType := pooler.PoolerType
	if poolerType == clustermetadatapb.PoolerType_UNKNOWN {
		poolerType = pooler.MultiPooler.Type
	}

	analysis := &store.ReplicationAnalysis{
		PoolerID:             pooler.MultiPooler.Id,
		ShardKey:             shardKey,
		PoolerType:           poolerType,
		CurrentServingStatus: pooler.MultiPooler.ServingStatus,
		IsPrimary:            poolerType == clustermetadatapb.PoolerType_PRIMARY,
		LastCheckValid:       pooler.IsLastCheckValid,
		IsInitialized:        store.IsInitialized(pooler),
		HasDataDirectory:     pooler.HasDataDirectory,
		AnalyzedAt:           time.Now(),
	}

	// Compute staleness
	analysis.IsStale = !pooler.IsUpToDate

	// Store consensus term for stale primary detection
	if pooler.ConsensusStatus != nil {
		analysis.ConsensusTerm = pooler.ConsensusStatus.CurrentTerm
	}

	// If this is a PRIMARY, populate primary-specific fields and aggregate replica stats
	if analysis.IsPrimary {
		if pooler.PrimaryStatus != nil {
			analysis.PrimaryLSN = pooler.PrimaryStatus.Lsn
			analysis.ReadOnly = !pooler.PrimaryStatus.Ready // Primary not ready = read-only
		}

		// Aggregate replica stats
		g.aggregateReplicaStats(pooler, analysis, shardKey)

		// Check for stale primary: look for other PRIMARYs in the same shard
		g.detectOtherPrimary(analysis, shardKey, pooler)
	}

	// If this is a REPLICA, populate replica-specific fields
	if !analysis.IsPrimary {
		if pooler.ReplicationStatus != nil {
			rs := pooler.ReplicationStatus
			analysis.ReplicationStopped = rs.IsWalReplayPaused
			analysis.IsLagging = rs.Lag != nil && rs.Lag.AsDuration() > DefaultReplicaLagThreshold
			if rs.Lag != nil {
				analysis.ReplicaLagMillis = rs.Lag.AsDuration().Milliseconds()
			}
			analysis.ReplicaReplayLSN = rs.LastReplayLsn
			analysis.ReplicaReceiveLSN = rs.LastReceiveLsn
			analysis.IsWalReplayPaused = rs.IsWalReplayPaused
			analysis.WalReplayPauseState = rs.WalReplayPauseState

			// Extract primary connection info
			if rs.PrimaryConnInfo != nil {
				analysis.PrimaryConnInfoHost = rs.PrimaryConnInfo.Host
				analysis.PrimaryConnInfoPort = rs.PrimaryConnInfo.Port
			}
		}

		// Lookup primary info
		g.populatePrimaryInfo(analysis, shardKey)
	}

	return analysis
}

// aggregateReplicaStats counts replicas pointing to this primary.
func (g *AnalysisGenerator) aggregateReplicaStats(
	primary *multiorchdatapb.PoolerHealthState,
	analysis *store.ReplicationAnalysis,
	shardKey commontypes.ShardKey,
) {
	var countReplicas uint
	var countReachable uint
	var countReplicating uint
	var countLagging uint

	primaryIDStr := topoclient.MultiPoolerIDString(primary.MultiPooler.Id)

	// Get connected followers from primary status
	var connectedFollowers []*clustermetadatapb.ID
	if primary.PrimaryStatus != nil {
		connectedFollowers = primary.PrimaryStatus.ConnectedFollowers
	}

	// Iterate only over poolers in the same shard (efficient lookup)
	if poolers, ok := g.poolersByShard[shardKey.Database][shardKey.TableGroup][shardKey.Shard]; ok {
		for poolerID, pooler := range poolers {
			if pooler == nil || pooler.MultiPooler == nil || pooler.MultiPooler.Id == nil {
				continue
			}

			// Skip the primary itself
			if poolerID == primaryIDStr {
				continue
			}

			// Skip if not a replica - check health check type, fall back to topology
			replicaType := pooler.PoolerType
			if replicaType == clustermetadatapb.PoolerType_UNKNOWN {
				replicaType = pooler.MultiPooler.Type
			}
			if replicaType != clustermetadatapb.PoolerType_REPLICA {
				continue
			}

			// Check if this replica is pointing to our primary
			// We do this by checking if primary is in the replica's connected followers
			// OR by checking primary_conninfo host/port match
			isPointingToPrimary := false
			for _, followerID := range connectedFollowers {
				if topoclient.MultiPoolerIDString(followerID) == poolerID {
					isPointingToPrimary = true
					break
				}
			}

			// Also check via primary_conninfo if we didn't find it in connected followers
			if !isPointingToPrimary && pooler.ReplicationStatus != nil && pooler.ReplicationStatus.PrimaryConnInfo != nil {
				connInfo := pooler.ReplicationStatus.PrimaryConnInfo
				primaryPort := primary.MultiPooler.PortMap["postgres"]
				if connInfo.Host == primary.MultiPooler.Hostname && connInfo.Port == primaryPort {
					isPointingToPrimary = true
				}
			}

			if !isPointingToPrimary {
				continue // not pointing to this primary
			}

			countReplicas++

			if pooler.IsLastCheckValid {
				countReachable++
			}

			// Check if actively replicating (not paused and lag is reasonable)
			if pooler.IsLastCheckValid && pooler.ReplicationStatus != nil && !pooler.ReplicationStatus.IsWalReplayPaused {
				countReplicating++
			}

			// Check if lagging
			if pooler.ReplicationStatus != nil && pooler.ReplicationStatus.Lag != nil {
				if pooler.ReplicationStatus.Lag.AsDuration() > DefaultReplicaLagThreshold {
					countLagging++
				}
			}
		}
	}

	analysis.CountReplicas = countReplicas
	analysis.CountReachableReplicas = countReachable
	analysis.CountReplicatingReplicas = countReplicating
	analysis.CountLaggingReplicas = countLagging
}

// populatePrimaryInfo looks up the primary this replica is replicating from.
func (g *AnalysisGenerator) populatePrimaryInfo(
	analysis *store.ReplicationAnalysis,
	shardKey commontypes.ShardKey,
) {
	poolers, ok := g.poolersByShard[shardKey.Database][shardKey.TableGroup][shardKey.Shard]
	if !ok {
		return
	}

	// Find the primary in the same shard
	var primary *multiorchdatapb.PoolerHealthState
	for _, pooler := range poolers {
		if pooler == nil || pooler.MultiPooler == nil || pooler.MultiPooler.Id == nil {
			continue
		}

		// Look for primary in same shard - check health check type
		// Nodes are never created with topology type PRIMARY
		if pooler.PoolerType != clustermetadatapb.PoolerType_PRIMARY {
			continue
		}

		primary = pooler
		break
	}

	if primary == nil {
		return // no primary found
	}

	// Found the primary - populate basic fields
	analysis.PrimaryPoolerID = primary.MultiPooler.Id
	if primary.LastSeen != nil {
		analysis.PrimaryTimestamp = primary.LastSeen.AsTime()
	}

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

// detectOtherPrimary checks if there's another PRIMARY in the same shard.
// If found, populates OtherPrimaryInShard and OtherPrimaryTerm on the analysis.
// This is used to detect stale primaries that came back online after failover.
func (g *AnalysisGenerator) detectOtherPrimary(
	analysis *store.ReplicationAnalysis,
	shardKey commontypes.ShardKey,
	thisPooler *multiorchdatapb.PoolerHealthState,
) {
	poolers, ok := g.poolersByShard[shardKey.Database][shardKey.TableGroup][shardKey.Shard]
	if !ok {
		return
	}

	thisIDStr := topoclient.MultiPoolerIDString(thisPooler.MultiPooler.Id)

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
			// Found another PRIMARY - one of them is stale!
			analysis.OtherPrimaryInShard = pooler.MultiPooler.Id
			if pooler.ConsensusStatus != nil {
				analysis.OtherPrimaryTerm = pooler.ConsensusStatus.CurrentTerm
			}
			return // Found one, that's enough to trigger recovery
		}
	}
}
