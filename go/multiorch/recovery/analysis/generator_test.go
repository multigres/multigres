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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/multiorch/store"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestAnalysisGenerator_GenerateAnalyses_EmptyStore(t *testing.T) {
	poolerStore := store.NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()
	generator := NewAnalysisGenerator(poolerStore)

	analyses := generator.GenerateAnalyses()

	assert.Empty(t, analyses, "should return empty slice for empty store")
}

func TestAnalysisGenerator_GenerateAnalyses_SinglePrimary(t *testing.T) {
	poolerStore := store.NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()

	// Add a single primary pooler
	primaryID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "primary-1",
	}

	primary := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         primaryID,
			Database:   "testdb",
			TableGroup: "testtg",
			Shard:      "0",
			Type:       clustermetadatapb.PoolerType_PRIMARY,
		},
		IsLastCheckValid: true,
		IsUpToDate:       true,
		LastSeen:         timestamppb.Now(),
		PoolerType:       clustermetadatapb.PoolerType_PRIMARY,
		PrimaryStatus: &multipoolermanagerdatapb.PrimaryStatus{
			Lsn:   "0/1234567",
			Ready: true,
		},
	}
	poolerStore.Set("multipooler-cell1-primary-1", primary)

	generator := NewAnalysisGenerator(poolerStore)
	analyses := generator.GenerateAnalyses()

	require.Len(t, analyses, 1, "should generate one analysis")

	analysis := analyses[0]
	assert.Equal(t, "testdb", analysis.ShardKey.Database)
	assert.Equal(t, "testtg", analysis.ShardKey.TableGroup)
	assert.Equal(t, "0", analysis.ShardKey.Shard)
	assert.True(t, analysis.IsPrimary)
	assert.True(t, analysis.LastCheckValid)
	assert.Equal(t, "0/1234567", analysis.PrimaryLSN)
	assert.Equal(t, uint(0), analysis.CountReplicas, "should have no replicas")
}

func TestAnalysisGenerator_GenerateAnalyses_PrimaryWithReplicas(t *testing.T) {
	poolerStore := store.NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()

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
	primary := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         primaryID,
			Database:   "testdb",
			TableGroup: "testtg",
			Shard:      "0",
			Type:       clustermetadatapb.PoolerType_PRIMARY,
			Hostname:   "primary.example.com",
		},
		IsLastCheckValid: true,
		IsUpToDate:       true,
		LastSeen:         timestamppb.Now(),
		PoolerType:       clustermetadatapb.PoolerType_PRIMARY,
		PrimaryStatus: &multipoolermanagerdatapb.PrimaryStatus{
			Lsn:                "0/1234567",
			Ready:              true,
			ConnectedFollowers: []*clustermetadatapb.ID{replica1ID, replica2ID},
		},
	}
	poolerStore.Set("multipooler-cell1-primary-1", primary)

	// Add replica 1 (replicating)
	replica1 := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         replica1ID,
			Database:   "testdb",
			TableGroup: "testtg",
			Shard:      "0",
			Type:       clustermetadatapb.PoolerType_REPLICA,
		},
		IsLastCheckValid: true,
		IsUpToDate:       true,
		LastSeen:         timestamppb.Now(),
		PoolerType:       clustermetadatapb.PoolerType_REPLICA,
		ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
			IsWalReplayPaused: false,
			Lag:               durationpb.New(100 * time.Millisecond), // 100ms lag
		},
	}
	poolerStore.Set("multipooler-cell1-replica-1", replica1)

	// Add replica 2 (lagging)
	replica2 := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         replica2ID,
			Database:   "testdb",
			TableGroup: "testtg",
			Shard:      "0",
			Type:       clustermetadatapb.PoolerType_REPLICA,
		},
		IsLastCheckValid: true,
		IsUpToDate:       true,
		LastSeen:         timestamppb.Now(),
		PoolerType:       clustermetadatapb.PoolerType_REPLICA,
		ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
			IsWalReplayPaused: false,
			Lag:               durationpb.New(15 * time.Second), // 15s lag (> 10s threshold)
		},
	}
	poolerStore.Set("multipooler-cell1-replica-2", replica2)

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
	poolerStore := store.NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()

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
	primary := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         primaryID,
			Database:   "testdb",
			TableGroup: "testtg",
			Shard:      "0",
			Type:       clustermetadatapb.PoolerType_PRIMARY,
		},
		IsLastCheckValid:  true,
		IsUpToDate:        true,
		IsPostgresRunning: true,
		LastSeen:          timestamppb.Now(),
		PoolerType:        clustermetadatapb.PoolerType_PRIMARY,
	}
	poolerStore.Set("multipooler-cell1-primary-1", primary)

	// Add replica
	replica := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         replicaID,
			Database:   "testdb",
			TableGroup: "testtg",
			Shard:      "0",
			Type:       clustermetadatapb.PoolerType_REPLICA,
		},
		IsLastCheckValid: true,
		IsUpToDate:       true,
		LastSeen:         timestamppb.Now(),
		PoolerType:       clustermetadatapb.PoolerType_REPLICA,
		ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
			IsWalReplayPaused: false,
			Lag:               durationpb.New(500 * time.Millisecond),
			LastReplayLsn:     "0/1234567",
		},
	}
	poolerStore.Set("multipooler-cell1-replica-1", replica)

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
	assert.Equal(t, int64(500), replicaAnalysis.ReplicaLagMillis) // generator converts Duration to millis
	assert.False(t, replicaAnalysis.IsLagging, "500ms should not be considered lagging")
	assert.Equal(t, "0/1234567", replicaAnalysis.ReplicaReplayLSN)
	assert.NotNil(t, replicaAnalysis.PrimaryPoolerID, "should have primary ID populated")
	assert.True(t, replicaAnalysis.PrimaryReachable)
}

func TestAnalysisGenerator_GenerateAnalyses_MultipleTableGroups(t *testing.T) {
	poolerStore := store.NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()

	// Add poolers from two different table groups
	tg1Primary := &multiorchdatapb.PoolerHealthState{
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
		LastSeen:         timestamppb.Now(),
		PoolerType:       clustermetadatapb.PoolerType_PRIMARY,
	}
	poolerStore.Set("multipooler-cell1-tg1-primary", tg1Primary)

	tg2Primary := &multiorchdatapb.PoolerHealthState{
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
		LastSeen:         timestamppb.Now(),
		PoolerType:       clustermetadatapb.PoolerType_PRIMARY,
	}
	poolerStore.Set("multipooler-cell1-tg2-primary", tg2Primary)

	generator := NewAnalysisGenerator(poolerStore)
	analyses := generator.GenerateAnalyses()

	require.Len(t, analyses, 2, "should generate two analyses")

	// Verify both table groups are present
	tableGroups := make(map[string]bool)
	for _, a := range analyses {
		tableGroups[a.ShardKey.TableGroup] = true
	}

	assert.True(t, tableGroups["tg1"])
	assert.True(t, tableGroups["tg2"])
}

func TestAggregateReplicaStats_MatchesByHostAndPort(t *testing.T) {
	// Create a store with primary and replica on same host but different ports
	poolerStore := store.NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()

	primaryID := "multipooler-cell1-node1"
	replicaID := "multipooler-cell1-node2"

	// Primary on host1:5432
	poolerStore.Set(primaryID, &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "node1",
			},
			Database:   "db1",
			TableGroup: "tg1",
			Shard:      "shard1",
			Hostname:   "host1",
			PortMap:    map[string]int32{"postgres": 5432},
		},
		PoolerType:       clustermetadatapb.PoolerType_PRIMARY,
		IsLastCheckValid: true,
		PrimaryStatus:    &multipoolermanagerdatapb.PrimaryStatus{Lsn: "0/1234"},
	})

	// Replica pointing to host1:5433 (wrong port - different primary)
	poolerStore.Set(replicaID, &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "node2",
			},
			Database:   "db1",
			TableGroup: "tg1",
			Shard:      "shard1",
			Hostname:   "host2",
		},
		PoolerType:       clustermetadatapb.PoolerType_REPLICA,
		IsLastCheckValid: true,
		ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
			LastReplayLsn: "0/1234",
			PrimaryConnInfo: &multipoolermanagerdatapb.PrimaryConnInfo{
				Host: "host1",
				Port: 5433, // Different port!
			},
		},
	})

	gen := NewAnalysisGenerator(poolerStore)
	analysis, err := gen.GenerateAnalysisForPooler(primaryID)
	require.NoError(t, err)

	// Should NOT count this replica since port doesn't match
	assert.Equal(t, uint(0), analysis.CountReplicas, "replica with wrong port should not be counted")
}

// Task 6: Test for skipping nil entries
func TestGenerateAnalyses_SkipsNilEntries(t *testing.T) {
	poolerStore := store.NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()

	// Add a nil entry
	poolerStore.Set("nil-pooler", nil)

	// Add a valid pooler
	poolerStore.Set("valid-pooler", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "valid",
			},
			Database:   "db1",
			TableGroup: "tg1",
			Shard:      "shard1",
		},
		PoolerType:       clustermetadatapb.PoolerType_REPLICA,
		IsLastCheckValid: true,
	})

	gen := NewAnalysisGenerator(poolerStore)
	analyses := gen.GenerateAnalyses()

	// Should only generate one analysis for the valid pooler, skipping the nil entry
	assert.Len(t, analyses, 1)
	assert.Equal(t, "db1", analyses[0].ShardKey.Database)
}

// Task 7: Test for no primary in shard
func TestPopulatePrimaryInfo_NoPrimaryInShard(t *testing.T) {
	poolerStore := store.NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()

	replicaID := "multipooler-cell1-replica"
	poolerStore.Set(replicaID, &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "replica",
			},
			Database:   "db1",
			TableGroup: "tg1",
			Shard:      "shard1",
		},
		PoolerType:       clustermetadatapb.PoolerType_REPLICA,
		IsLastCheckValid: true,
		ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
			LastReplayLsn: "0/1234",
		},
	})

	gen := NewAnalysisGenerator(poolerStore)
	analysis, err := gen.GenerateAnalysisForPooler(replicaID)
	require.NoError(t, err)

	// When no primary exists in the shard, PrimaryPoolerID should be nil
	assert.Nil(t, analysis.PrimaryPoolerID)
	assert.False(t, analysis.PrimaryReachable)
}

// Task 7: Test for primary with postgres down
func TestPopulatePrimaryInfo_PrimaryPostgresDown(t *testing.T) {
	poolerStore := store.NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()

	primaryID := "multipooler-cell1-primary"
	replicaID := "multipooler-cell1-replica"

	// Primary with IsPostgresRunning: false (postgres is down)
	poolerStore.Set(primaryID, &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "primary",
			},
			Database:   "db1",
			TableGroup: "tg1",
			Shard:      "shard1",
		},
		PoolerType:        clustermetadatapb.PoolerType_PRIMARY,
		IsLastCheckValid:  true,
		IsPostgresRunning: false, // Postgres is down!
		PrimaryStatus:     &multipoolermanagerdatapb.PrimaryStatus{Lsn: "0/1234"},
	})

	poolerStore.Set(replicaID, &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "replica",
			},
			Database:   "db1",
			TableGroup: "tg1",
			Shard:      "shard1",
		},
		PoolerType:       clustermetadatapb.PoolerType_REPLICA,
		IsLastCheckValid: true,
		ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
			LastReplayLsn: "0/1234",
		},
	})

	gen := NewAnalysisGenerator(poolerStore)
	analysis, err := gen.GenerateAnalysisForPooler(replicaID)
	require.NoError(t, err)

	// PrimaryPoolerID should be set
	assert.NotNil(t, analysis.PrimaryPoolerID)
	// But PrimaryReachable should be false because postgres is down
	assert.False(t, analysis.PrimaryReachable, "primary should NOT be reachable when postgres is down")
}

func TestIsInStandbyList(t *testing.T) {
	poolerStore := store.NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()

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
		Cell:      "cell2",
		Name:      "replica-2",
	}

	tests := []struct {
		name          string
		replicaID     *clustermetadatapb.ID
		primaryStatus *multipoolermanagerdatapb.PrimaryStatus
		expected      bool
	}{
		{
			name:      "replica in standby list",
			replicaID: replica1ID,
			primaryStatus: &multipoolermanagerdatapb.PrimaryStatus{
				Lsn:   "0/1234567",
				Ready: true,
				SyncReplicationConfig: &multipoolermanagerdatapb.SynchronousReplicationConfiguration{
					StandbyIds: []*clustermetadatapb.ID{replica1ID},
				},
			},
			expected: true,
		},
		{
			name:      "replica not in standby list",
			replicaID: replica2ID,
			primaryStatus: &multipoolermanagerdatapb.PrimaryStatus{
				Lsn:   "0/1234567",
				Ready: true,
				SyncReplicationConfig: &multipoolermanagerdatapb.SynchronousReplicationConfiguration{
					StandbyIds: []*clustermetadatapb.ID{replica1ID},
				},
			},
			expected: false,
		},
		{
			name:      "empty standby list",
			replicaID: replica1ID,
			primaryStatus: &multipoolermanagerdatapb.PrimaryStatus{
				Lsn:   "0/1234567",
				Ready: true,
				SyncReplicationConfig: &multipoolermanagerdatapb.SynchronousReplicationConfiguration{
					StandbyIds: []*clustermetadatapb.ID{},
				},
			},
			expected: false,
		},
		{
			name:      "nil sync replication config",
			replicaID: replica1ID,
			primaryStatus: &multipoolermanagerdatapb.PrimaryStatus{
				Lsn:                   "0/1234567",
				Ready:                 true,
				SyncReplicationConfig: nil,
			},
			expected: false,
		},
		{
			name:          "nil primary status",
			replicaID:     replica1ID,
			primaryStatus: nil,
			expected:      false,
		},
		{
			name:      "multiple standbys - replica present",
			replicaID: replica2ID,
			primaryStatus: &multipoolermanagerdatapb.PrimaryStatus{
				Lsn:   "0/1234567",
				Ready: true,
				SyncReplicationConfig: &multipoolermanagerdatapb.SynchronousReplicationConfiguration{
					StandbyIds: []*clustermetadatapb.ID{replica1ID, replica2ID},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up pooler store with primary
			poolerStore.Set("multipooler-cell1-primary-1", &multiorchdatapb.PoolerHealthState{
				MultiPooler: &clustermetadatapb.MultiPooler{
					Id:         primaryID,
					Database:   "testdb",
					TableGroup: "testtg",
					Shard:      "0",
					Type:       clustermetadatapb.PoolerType_PRIMARY,
				},
				IsLastCheckValid:  true,
				IsUpToDate:        true,
				IsPostgresRunning: true,
				LastSeen:          timestamppb.Now(),
				PoolerType:        clustermetadatapb.PoolerType_PRIMARY,
				PrimaryStatus:     tt.primaryStatus,
			})

			generator := NewAnalysisGenerator(poolerStore)

			primary, _ := poolerStore.Get("multipooler-cell1-primary-1")
			result := generator.isInStandbyList(tt.replicaID, primary)

			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPopulatePrimaryInfo_IsInPrimaryStandbyList(t *testing.T) {
	poolerStore := store.NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()

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
		Cell:      "cell2",
		Name:      "replica-2",
	}

	// Add primary with replica1 in standby list
	poolerStore.Set("multipooler-cell1-primary-1", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         primaryID,
			Database:   "testdb",
			TableGroup: "testtg",
			Shard:      "0",
			Type:       clustermetadatapb.PoolerType_PRIMARY,
		},
		IsLastCheckValid:  true,
		IsUpToDate:        true,
		IsPostgresRunning: true,
		LastSeen:          timestamppb.Now(),
		PoolerType:        clustermetadatapb.PoolerType_PRIMARY,
		PrimaryStatus: &multipoolermanagerdatapb.PrimaryStatus{
			Lsn:   "0/1234567",
			Ready: true,
			SyncReplicationConfig: &multipoolermanagerdatapb.SynchronousReplicationConfiguration{
				StandbyIds: []*clustermetadatapb.ID{replica1ID},
			},
		},
	})

	// Add replica1 (in standby list)
	poolerStore.Set("multipooler-cell1-replica-1", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         replica1ID,
			Database:   "testdb",
			TableGroup: "testtg",
			Shard:      "0",
			Type:       clustermetadatapb.PoolerType_REPLICA,
		},
		IsLastCheckValid: true,
		IsUpToDate:       true,
		LastSeen:         timestamppb.Now(),
		PoolerType:       clustermetadatapb.PoolerType_REPLICA,
		ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
			IsWalReplayPaused: false,
			Lag:               durationpb.New(100 * time.Millisecond),
		},
	})

	// Add replica2 (not in standby list)
	poolerStore.Set("multipooler-cell2-replica-2", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:         replica2ID,
			Database:   "testdb",
			TableGroup: "testtg",
			Shard:      "0",
			Type:       clustermetadatapb.PoolerType_REPLICA,
		},
		IsLastCheckValid: true,
		IsUpToDate:       true,
		LastSeen:         timestamppb.Now(),
		PoolerType:       clustermetadatapb.PoolerType_REPLICA,
		ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
			IsWalReplayPaused: false,
			Lag:               durationpb.New(100 * time.Millisecond),
		},
	})

	generator := NewAnalysisGenerator(poolerStore)

	t.Run("replica in standby list", func(t *testing.T) {
		analysis, err := generator.GenerateAnalysisForPooler("multipooler-cell1-replica-1")
		require.NoError(t, err)
		assert.True(t, analysis.IsInPrimaryStandbyList, "replica1 should be in standby list")
	})

	t.Run("replica not in standby list", func(t *testing.T) {
		analysis, err := generator.GenerateAnalysisForPooler("multipooler-cell2-replica-2")
		require.NoError(t, err)
		assert.False(t, analysis.IsInPrimaryStandbyList, "replica2 should not be in standby list")
	})
}
