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
	"github.com/multigres/multigres/go/multiorch/store"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

// PoolersByShard is a structured map for efficient lookups.
// Structure: [database][tablegroup][shard][pooler_id] -> PoolerHealthState
type PoolersByShard map[string]map[string]map[string]map[string]*multiorchdatapb.PoolerHealthState

// AnalysisGenerator creates ReplicationAnalysis from the pooler store.
type AnalysisGenerator struct {
	poolerStore    *store.ProtoStore[string, *multiorchdatapb.PoolerHealthState]
	poolersByShard PoolersByShard
}

// NewAnalysisGenerator creates a new analysis generator.
// It eagerly builds the poolersByShard map from the current store state.
func NewAnalysisGenerator(poolerStore *store.ProtoStore[string, *multiorchdatapb.PoolerHealthState]) *AnalysisGenerator {
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
				for _, pooler := range poolers {
					analysis := g.generateAnalysisForPooler(pooler, database, tableGroup, shard)
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

	var poolerIDs []string

	// Iterate the store to find all poolers in the same shard
	// Note: We can't use the cached poolersByShard here because the store may have been updated
	g.poolerStore.Range(func(id string, p *multiorchdatapb.PoolerHealthState) bool {
		if p == nil || p.MultiPooler == nil || p.MultiPooler.Id == nil {
			return true
		}

		if p.MultiPooler.Database == database &&
			p.MultiPooler.TableGroup == tableGroup &&
			p.MultiPooler.Shard == shard {
			poolerIDs = append(poolerIDs, id)
		}

		return true
	})

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
	analysis := g.generateAnalysisForPooler(pooler, pooler.MultiPooler.Database, pooler.MultiPooler.TableGroup, pooler.MultiPooler.Shard)

	return analysis, nil
}

// generateAnalysisForPooler creates a ReplicationAnalysis for a single pooler.
func (g *AnalysisGenerator) generateAnalysisForPooler(
	pooler *multiorchdatapb.PoolerHealthState,
	database string,
	tableGroup string,
	shard string,
) *store.ReplicationAnalysis {
	analysis := &store.ReplicationAnalysis{
		PoolerID:             pooler.MultiPooler.Id,
		Database:             pooler.MultiPooler.Database,
		TableGroup:           pooler.MultiPooler.TableGroup,
		Shard:                pooler.MultiPooler.Shard,
		PoolerType:           pooler.MultiPooler.Type,
		CurrentServingStatus: pooler.MultiPooler.ServingStatus,
		IsPrimary:            pooler.MultiPooler.Type == clustermetadatapb.PoolerType_PRIMARY,
		LastCheckValid:       pooler.IsLastCheckValid,
		AnalyzedAt:           time.Now(),
	}

	// Compute staleness
	analysis.IsStale = !pooler.IsUpToDate
	analysis.IsUnreachable = !pooler.IsLastCheckValid

	// If this is a PRIMARY, populate primary-specific fields and aggregate replica stats
	if analysis.IsPrimary {
		if pooler.PrimaryStatus != nil {
			analysis.PrimaryLSN = pooler.PrimaryStatus.Lsn
			analysis.ReadOnly = !pooler.PrimaryStatus.Ready // Primary not ready = read-only
		}

		// Aggregate replica stats
		g.aggregateReplicaStats(pooler, analysis, database, tableGroup, shard)
	}

	// If this is a REPLICA, populate replica-specific fields
	if !analysis.IsPrimary {
		if pooler.ReplicationStatus != nil {
			rs := pooler.ReplicationStatus
			analysis.ReplicationStopped = rs.IsWalReplayPaused
			analysis.IsLagging = rs.Lag != nil && rs.Lag.AsDuration().Milliseconds() > 10000 // 10 seconds threshold
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
		g.populatePrimaryInfo(pooler, analysis, database, tableGroup, shard)
	}

	return analysis
}

// aggregateReplicaStats counts replicas pointing to this primary.
func (g *AnalysisGenerator) aggregateReplicaStats(
	primary *multiorchdatapb.PoolerHealthState,
	analysis *store.ReplicationAnalysis,
	database string,
	tableGroup string,
	shard string,
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
	if poolers, ok := g.poolersByShard[database][tableGroup][shard]; ok {
		for poolerID, pooler := range poolers {
			if pooler == nil || pooler.MultiPooler == nil || pooler.MultiPooler.Id == nil {
				continue
			}

			// Skip the primary itself
			if poolerID == primaryIDStr {
				continue
			}

			// Skip if not a replica
			if pooler.MultiPooler.Type != clustermetadatapb.PoolerType_REPLICA {
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
				if pooler.ReplicationStatus.PrimaryConnInfo.Host == primary.MultiPooler.Hostname {
					// TODO: More robust check would compare port as well
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

			// Check if lagging (> 10 seconds)
			if pooler.ReplicationStatus != nil && pooler.ReplicationStatus.Lag != nil {
				if pooler.ReplicationStatus.Lag.AsDuration().Milliseconds() > 10000 {
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
	replica *multiorchdatapb.PoolerHealthState,
	analysis *store.ReplicationAnalysis,
	database string,
	tableGroup string,
	shard string,
) {
	// Find the primary in the same tablegroup (efficient lookup)
	if poolers, ok := g.poolersByShard[database][tableGroup][shard]; ok {
		for _, pooler := range poolers {
			if pooler == nil || pooler.MultiPooler == nil || pooler.MultiPooler.Id == nil {
				continue
			}

			// Look for primary in same tablegroup
			if pooler.MultiPooler.Type != clustermetadatapb.PoolerType_PRIMARY {
				continue
			}

			// Found the primary
			analysis.PrimaryPoolerID = pooler.MultiPooler.Id
			analysis.PrimaryReachable = pooler.IsLastCheckValid
			if pooler.LastSeen != nil {
				analysis.PrimaryTimestamp = pooler.LastSeen.AsTime()
			}
			return // found it
		}
	}
}
