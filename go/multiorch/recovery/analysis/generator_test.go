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
	"testing"
	"time"

	"github.com/multigres/multigres/go/multiorch/store"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestAnalysisGenerator_GenerateAnalyses_EmptyStore(t *testing.T) {
	poolerStore := store.NewStore[string, *store.PoolerHealth]()
	generator := NewAnalysisGenerator(poolerStore)

	analyses := generator.GenerateAnalyses()

	assert.Empty(t, analyses, "should return empty slice for empty store")
}

func TestAnalysisGenerator_GenerateAnalyses_SinglePrimary(t *testing.T) {
	poolerStore := store.NewStore[string, *store.PoolerHealth]()

	// Add a single primary pooler
	primaryID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "primary-1",
	}

	poolerStore.Set("multipooler-cell1-primary-1", &store.PoolerHealth{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         primaryID,
			Database:   "testdb",
			TableGroup: "testtg",
			Shard:      "0",
			Type:       clustermetadatapb.PoolerType_PRIMARY,
		},
		IsLastCheckValid: true,
		IsUpToDate:       true,
		PrimaryLSN:       "0/1234567",
		PrimaryReady:     true,
		LastSeen:         time.Now(),
	})

	generator := NewAnalysisGenerator(poolerStore)
	analyses := generator.GenerateAnalyses()

	require.Len(t, analyses, 1, "should generate one analysis")

	analysis := analyses[0]
	assert.Equal(t, "testdb", analysis.Database)
	assert.Equal(t, "testtg", analysis.TableGroup)
	assert.Equal(t, "0", analysis.Shard)
	assert.True(t, analysis.IsPrimary)
	assert.True(t, analysis.LastCheckValid)
	assert.Equal(t, "0/1234567", analysis.PrimaryLSN)
	assert.Equal(t, uint(0), analysis.CountReplicas, "should have no replicas")
}

func TestAnalysisGenerator_GenerateAnalyses_PrimaryWithReplicas(t *testing.T) {
	poolerStore := store.NewStore[string, *store.PoolerHealth]()

	primaryID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "primary-1",
	}

	replica1ID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "replica-1",
	}

	replica2ID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "replica-2",
	}

	// Add primary
	poolerStore.Set("multipooler-cell1-primary-1", &store.PoolerHealth{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         primaryID,
			Database:   "testdb",
			TableGroup: "testtg",
			Shard:      "0",
			Type:       clustermetadatapb.PoolerType_PRIMARY,
			Hostname:   "primary.example.com",
		},
		IsLastCheckValid:          true,
		IsUpToDate:                true,
		PrimaryLSN:                "0/1234567",
		PrimaryReady:              true,
		PrimaryConnectedFollowers: []*clustermetadatapb.ID{replica1ID, replica2ID},
		LastSeen:                  time.Now(),
	})

	// Add replica 1 (replicating)
	poolerStore.Set("multipooler-cell1-replica-1", &store.PoolerHealth{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         replica1ID,
			Database:   "testdb",
			TableGroup: "testtg",
			Shard:      "0",
			Type:       clustermetadatapb.PoolerType_REPLICA,
		},
		IsLastCheckValid:         true,
		IsUpToDate:               true,
		ReplicaIsWalReplayPaused: false,
		ReplicaLagMillis:         100, // 100ms lag
		LastSeen:                 time.Now(),
	})

	// Add replica 2 (lagging)
	poolerStore.Set("multipooler-cell1-replica-2", &store.PoolerHealth{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         replica2ID,
			Database:   "testdb",
			TableGroup: "testtg",
			Shard:      "0",
			Type:       clustermetadatapb.PoolerType_REPLICA,
		},
		IsLastCheckValid:         true,
		IsUpToDate:               true,
		ReplicaIsWalReplayPaused: false,
		ReplicaLagMillis:         15000, // 15s lag (> 10s threshold)
		LastSeen:                 time.Now(),
	})

	generator := NewAnalysisGenerator(poolerStore)
	analyses := generator.GenerateAnalyses()

	require.Len(t, analyses, 3, "should generate three analyses")

	// Find the primary analysis
	var primaryAnalysis *store.ReplicationAnalysis
	for _, a := range analyses {
		if a.IsPrimary {
			primaryAnalysis = a
			break
		}
	}

	require.NotNil(t, primaryAnalysis, "should find primary analysis")
	assert.Equal(t, uint(2), primaryAnalysis.CountReplicas)
	assert.Equal(t, uint(2), primaryAnalysis.CountReachableReplicas)
	assert.Equal(t, uint(2), primaryAnalysis.CountReplicatingReplicas)
	assert.Equal(t, uint(1), primaryAnalysis.CountLaggingReplicas)
}

func TestAnalysisGenerator_GenerateAnalyses_Replica(t *testing.T) {
	poolerStore := store.NewStore[string, *store.PoolerHealth]()

	primaryID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "primary-1",
	}

	replicaID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "replica-1",
	}

	// Add primary
	poolerStore.Set("multipooler-cell1-primary-1", &store.PoolerHealth{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         primaryID,
			Database:   "testdb",
			TableGroup: "testtg",
			Shard:      "0",
			Type:       clustermetadatapb.PoolerType_PRIMARY,
		},
		IsLastCheckValid: true,
		IsUpToDate:       true,
		LastSeen:         time.Now(),
	})

	// Add replica
	poolerStore.Set("multipooler-cell1-replica-1", &store.PoolerHealth{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         replicaID,
			Database:   "testdb",
			TableGroup: "testtg",
			Shard:      "0",
			Type:       clustermetadatapb.PoolerType_REPLICA,
		},
		IsLastCheckValid:         true,
		IsUpToDate:               true,
		ReplicaIsWalReplayPaused: false,
		ReplicaLagMillis:         500,
		ReplicaLastReplayLSN:     "0/1234567",
		LastSeen:                 time.Now(),
	})

	generator := NewAnalysisGenerator(poolerStore)
	analyses := generator.GenerateAnalyses()

	require.Len(t, analyses, 2, "should generate two analyses")

	// Find the replica analysis
	var replicaAnalysis *store.ReplicationAnalysis
	for _, a := range analyses {
		if !a.IsPrimary {
			replicaAnalysis = a
			break
		}
	}

	require.NotNil(t, replicaAnalysis, "should find replica analysis")
	assert.False(t, replicaAnalysis.IsPrimary)
	assert.Equal(t, int64(500), replicaAnalysis.ReplicaLagMillis)
	assert.False(t, replicaAnalysis.IsLagging, "500ms should not be considered lagging")
	assert.Equal(t, "0/1234567", replicaAnalysis.ReplicaReplayLSN)
	assert.NotNil(t, replicaAnalysis.PrimaryPoolerID, "should have primary ID populated")
	assert.True(t, replicaAnalysis.PrimaryReachable)
}

func TestAnalysisGenerator_GenerateAnalyses_MultipleTableGroups(t *testing.T) {
	poolerStore := store.NewStore[string, *store.PoolerHealth]()

	// Add poolers from two different table groups
	poolerStore.Set("multipooler-cell1-tg1-primary", &store.PoolerHealth{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "tg1-primary",
			},
			Database:   "testdb",
			TableGroup: "tg1",
			Shard:      "0",
			Type:       clustermetadatapb.PoolerType_PRIMARY,
		},
		IsLastCheckValid: true,
		IsUpToDate:       true,
		LastSeen:         time.Now(),
	})

	poolerStore.Set("multipooler-cell1-tg2-primary", &store.PoolerHealth{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "tg2-primary",
			},
			Database:   "testdb",
			TableGroup: "tg2",
			Shard:      "0",
			Type:       clustermetadatapb.PoolerType_PRIMARY,
		},
		IsLastCheckValid: true,
		IsUpToDate:       true,
		LastSeen:         time.Now(),
	})

	generator := NewAnalysisGenerator(poolerStore)
	analyses := generator.GenerateAnalyses()

	require.Len(t, analyses, 2, "should generate two analyses")

	// Verify both table groups are present
	tableGroups := make(map[string]bool)
	for _, a := range analyses {
		tableGroups[a.TableGroup] = true
	}

	assert.True(t, tableGroups["tg1"])
	assert.True(t, tableGroups["tg2"])
}
