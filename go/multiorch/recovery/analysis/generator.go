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

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/multiorch/store"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// TableGroupKey uniquely identifies a tablegroup.
type TableGroupKey struct {
	Database   string
	TableGroup string
	Shard      string
}

// PoolersByTableGroup is a structured map for efficient lookups.
// Structure: [database][tablegroup][shard][pooler_id] -> PoolerHealth
type PoolersByTableGroup map[string]map[string]map[string]map[string]*store.PoolerHealth

// AnalysisGenerator creates ReplicationAnalysis from the pooler store.
// This is analogous to VTOrc's GetReplicationAnalysis() SQL query,
// but done in-memory with Go code.
type AnalysisGenerator struct {
	poolerStore *store.Store[string, *store.PoolerHealth]
}

// NewAnalysisGenerator creates a new analysis generator.
func NewAnalysisGenerator(poolerStore *store.Store[string, *store.PoolerHealth]) *AnalysisGenerator {
	return &AnalysisGenerator{
		poolerStore: poolerStore,
	}
}

// GenerateAnalyses creates one ReplicationAnalysis per pooler in the store.
// This examines the current state and computes derived fields.
func (g *AnalysisGenerator) GenerateAnalyses() []*store.ReplicationAnalysis {
	// Build structured map - iterate store ONCE
	poolersByTG := g.buildPoolersByTableGroup()

	// Generate analyses for all poolers
	analyses := []*store.ReplicationAnalysis{}

	for database, tableGroups := range poolersByTG {
		for tableGroup, shards := range tableGroups {
			for shard, poolers := range shards {
				for poolerID, pooler := range poolers {
					analysis := g.generateAnalysisForPooler(poolerID, pooler, poolersByTG, database, tableGroup, shard)
					analyses = append(analyses, analysis)
				}
			}
		}
	}

	return analyses
}

// buildPoolersByTableGroup creates a structured map by iterating the store once.
func (g *AnalysisGenerator) buildPoolersByTableGroup() PoolersByTableGroup {
	poolersByTG := make(PoolersByTableGroup)

	g.poolerStore.Range(func(poolerID string, pooler *store.PoolerHealth) bool {
		if pooler == nil || pooler.ID == nil {
			return true // skip nil entries
		}

		database := pooler.Database
		tableGroup := pooler.TableGroup
		shard := pooler.Shard

		// Initialize nested maps if needed
		if poolersByTG[database] == nil {
			poolersByTG[database] = make(map[string]map[string]map[string]*store.PoolerHealth)
		}
		if poolersByTG[database][tableGroup] == nil {
			poolersByTG[database][tableGroup] = make(map[string]map[string]*store.PoolerHealth)
		}
		if poolersByTG[database][tableGroup][shard] == nil {
			poolersByTG[database][tableGroup][shard] = make(map[string]*store.PoolerHealth)
		}

		// Create a deep copy to avoid concurrent access issues
		poolersByTG[database][tableGroup][shard][poolerID] = pooler.DeepCopy()
		return true // continue
	})

	return poolersByTG
}

// GetPoolersInTableGroup returns all pooler IDs in the same tablegroup as the given pooler.
// This uses the cached poolersByTG map to avoid iterating the store again.
func (g *AnalysisGenerator) GetPoolersInTableGroup(poolerIDStr string) ([]string, error) {
	// Get pooler from store to determine its tablegroup
	pooler, ok := g.poolerStore.Get(poolerIDStr)
	if !ok {
		return nil, fmt.Errorf("pooler not found in store: %s", poolerIDStr)
	}

	if pooler == nil || pooler.ID == nil {
		return nil, fmt.Errorf("pooler or ID is nil: %s", poolerIDStr)
	}

	database := pooler.Database
	tableGroup := pooler.TableGroup
	shard := pooler.Shard

	var poolerIDs []string

	// Iterate the store to find all poolers in the same tablegroup
	// Note: We can't use the cached poolersByTG here because the store may have been updated
	g.poolerStore.Range(func(id string, p *store.PoolerHealth) bool {
		if p == nil || p.ID == nil {
			return true
		}

		if p.Database == database &&
			p.TableGroup == tableGroup &&
			p.Shard == shard {
			poolerIDs = append(poolerIDs, id)
		}

		return true
	})

	return poolerIDs, nil
}

// GenerateAnalysisForPooler generates analysis for a single pooler.
// This rebuilds the poolersByTG map from the current store state to ensure fresh data.
func (g *AnalysisGenerator) GenerateAnalysisForPooler(poolerIDStr string) (*store.ReplicationAnalysis, error) {
	// Get pooler from store
	pooler, ok := g.poolerStore.Get(poolerIDStr)
	if !ok {
		return nil, fmt.Errorf("pooler not found in store: %s", poolerIDStr)
	}

	if pooler == nil || pooler.ID == nil {
		return nil, fmt.Errorf("pooler or ID is nil: %s", poolerIDStr)
	}

	// Rebuild the map with current store data (store may have been updated by re-polling)
	poolersByTG := g.buildPoolersByTableGroup()

	database := pooler.Database
	tableGroup := pooler.TableGroup
	shard := pooler.Shard

	// Generate analysis for this specific pooler
	analysis := g.generateAnalysisForPooler(poolerIDStr, pooler, poolersByTG, database, tableGroup, shard)

	return analysis, nil
}

// generateAnalysisForPooler creates a ReplicationAnalysis for a single pooler.
func (g *AnalysisGenerator) generateAnalysisForPooler(
	poolerID string,
	pooler *store.PoolerHealth,
	poolersByTG PoolersByTableGroup,
	database string,
	tableGroup string,
	shard string,
) *store.ReplicationAnalysis {
	analysis := &store.ReplicationAnalysis{
		PoolerID:             pooler.ID,
		Database:             pooler.Database,
		TableGroup:           pooler.TableGroup,
		Shard:                pooler.Shard,
		PoolerType:           pooler.TopoPoolerType,
		CurrentServingStatus: pooler.ServingStatus,
		IsPrimary:            pooler.TopoPoolerType == clustermetadatapb.PoolerType_PRIMARY,
		LastCheckValid:       pooler.IsLastCheckValid,
		AnalyzedAt:           time.Now(),
	}

	// Compute staleness
	analysis.IsStale = !pooler.IsUpToDate
	analysis.IsUnreachable = !pooler.IsLastCheckValid

	// If this is a PRIMARY, populate primary-specific fields and aggregate replica stats
	if analysis.IsPrimary {
		analysis.PrimaryLSN = pooler.PrimaryLSN
		analysis.ReadOnly = !pooler.PrimaryReady // Primary not ready = read-only

		// Aggregate replica stats
		g.aggregateReplicaStats(pooler, analysis, poolersByTG, database, tableGroup, shard)
	}

	// If this is a REPLICA, populate replica-specific fields
	if !analysis.IsPrimary {
		analysis.ReplicationStopped = pooler.ReplicaIsWalReplayPaused
		analysis.ReplicaLagMillis = pooler.ReplicaLagMillis
		analysis.IsLagging = pooler.ReplicaLagMillis > 10000 // 10 seconds threshold
		analysis.ReplicaReplayLSN = pooler.ReplicaLastReplayLSN
		analysis.ReplicaReceiveLSN = pooler.ReplicaLastReceiveLSN
		analysis.IsWalReplayPaused = pooler.ReplicaIsWalReplayPaused
		analysis.WalReplayPauseState = pooler.ReplicaWalReplayPauseState

		// Extract primary connection info
		if pooler.ReplicaPrimaryConnInfo != nil {
			analysis.PrimaryConnInfoHost = pooler.ReplicaPrimaryConnInfo.Host
			analysis.PrimaryConnInfoPort = pooler.ReplicaPrimaryConnInfo.Port
		}

		// Lookup primary info
		g.populatePrimaryInfo(pooler, analysis, poolersByTG, database, tableGroup, shard)
	}

	return analysis
}

// aggregateReplicaStats counts replicas pointing to this primary.
func (g *AnalysisGenerator) aggregateReplicaStats(
	primary *store.PoolerHealth,
	analysis *store.ReplicationAnalysis,
	poolersByTG PoolersByTableGroup,
	database string,
	tableGroup string,
	shard string,
) {
	var countReplicas uint
	var countReachable uint
	var countReplicating uint
	var countLagging uint

	primaryIDStr := topo.MultiPoolerIDString(primary.ID)

	// Iterate only over poolers in the same tablegroup (efficient lookup)
	if poolers, ok := poolersByTG[database][tableGroup][shard]; ok {
		for poolerID, pooler := range poolers {
			if pooler == nil || pooler.ID == nil {
				continue
			}

			// Skip the primary itself
			if poolerID == primaryIDStr {
				continue
			}

			// Skip if not a replica
			if pooler.TopoPoolerType != clustermetadatapb.PoolerType_REPLICA {
				continue
			}

			// Check if this replica is pointing to our primary
			// We do this by checking if primary is in the replica's connected followers
			// OR by checking primary_conninfo host/port match
			isPointingToPrimary := false
			for _, followerID := range primary.PrimaryConnectedFollowers {
				if topo.MultiPoolerIDString(followerID) == poolerID {
					isPointingToPrimary = true
					break
				}
			}

			// Also check via primary_conninfo if we didn't find it in connected followers
			if !isPointingToPrimary && pooler.ReplicaPrimaryConnInfo != nil {
				if pooler.ReplicaPrimaryConnInfo.Host == primary.Hostname {
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
			if pooler.IsLastCheckValid && !pooler.ReplicaIsWalReplayPaused {
				countReplicating++
			}

			// Check if lagging (> 10 seconds)
			if pooler.ReplicaLagMillis > 10000 {
				countLagging++
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
	replica *store.PoolerHealth,
	analysis *store.ReplicationAnalysis,
	poolersByTG PoolersByTableGroup,
	database string,
	tableGroup string,
	shard string,
) {
	// Find the primary in the same tablegroup (efficient lookup)
	if poolers, ok := poolersByTG[database][tableGroup][shard]; ok {
		for _, pooler := range poolers {
			if pooler == nil || pooler.ID == nil {
				continue
			}

			// Look for primary in same tablegroup
			if pooler.TopoPoolerType != clustermetadatapb.PoolerType_PRIMARY {
				continue
			}

			// Found the primary
			analysis.PrimaryPoolerID = pooler.ID
			analysis.PrimaryReachable = pooler.IsLastCheckValid
			analysis.PrimaryTimestamp = pooler.LastSeen
			return // found it
		}
	}
}
