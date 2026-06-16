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

	"github.com/multigres/multigres/go/common/topoclient"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// primaryConsensusStatus builds a ConsensusStatus that names id as the leader
// in its current rule with the given coordinator term. This is the minimal
// fixture required for commonconsensus.IsLeader to return true for a given pooler.
func primaryConsensusStatus(id *clustermetadatapb.ID, term int64) *clustermetadatapb.ConsensusStatus {
	return &clustermetadatapb.ConsensusStatus{
		Id: id,
		CurrentPosition: &clustermetadatapb.PoolerPosition{
			Rule: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term},
				LeaderId:   id,
			},
		},
	}
}

func TestAnalysisGenerator_GenerateShardAnalyses_EmptyStore(t *testing.T) {
	generator := NewAnalysisGenerator(store.NewPoolerStore(), nil)

	analyses := flattenShardAnalyses(generator.GenerateShardAnalyses())

	assert.Empty(t, analyses, "should return empty slice for empty store")
}

func TestAnalysisGenerator_GenerateShardAnalyses_SinglePrimary(t *testing.T) {
	ps := store.NewPoolerStore()

	// Add a single primary pooler
	primaryID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "primary-1",
	}

	primary := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: primaryID,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "testtg",
				Shard:      "0",
			},
			Type: clustermetadatapb.PoolerType_PRIMARY,
		},
		IsLastCheckValid: true,
		IsUpToDate:       true,
		LastSeen:         timestamppb.Now(),
		ConsensusStatus:  primaryConsensusStatus(primaryID, 1),
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_PRIMARY,
			PrimaryStatus: &multipoolermanagerdatapb.PrimaryStatus{
				Lsn:   "0/1234567",
				Ready: true,
			},
		},
	}
	ps.Set("multipooler-cell1-primary-1", primary)

	generator := NewAnalysisGenerator(ps, nil)
	analyses := flattenShardAnalyses(generator.GenerateShardAnalyses())

	require.Len(t, analyses, 1, "should generate one analysis")

	analysis := analyses[0]
	assert.Equal(t, "testdb", analysis.ShardKey.Database)
	assert.Equal(t, "testtg", analysis.ShardKey.TableGroup)
	assert.Equal(t, "0", analysis.ShardKey.Shard)
	assert.True(t, analysis.IsLeader)
	assert.True(t, analysis.LastCheckValid)
}

func TestAnalysisGenerator_GenerateShardAnalyses_PrimaryWithReplicas(t *testing.T) {
	ps := store.NewPoolerStore()

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
			Id: primaryID,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "testtg",
				Shard:      "0",
			},
			Type:     clustermetadatapb.PoolerType_PRIMARY,
			Hostname: "primary.example.com",
		},
		IsLastCheckValid: true,
		IsUpToDate:       true,
		LastSeen:         timestamppb.Now(),
		ConsensusStatus:  primaryConsensusStatus(primaryID, 1),
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_PRIMARY,
			PrimaryStatus: &multipoolermanagerdatapb.PrimaryStatus{
				Lsn:                "0/1234567",
				Ready:              true,
				ConnectedFollowers: []*clustermetadatapb.ID{replica1ID, replica2ID},
			},
		},
	}
	ps.Set("multipooler-cell1-primary-1", primary)

	// Add replica 1 (replicating)
	replica1 := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: replica1ID,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "testtg",
				Shard:      "0",
			},
			Type: clustermetadatapb.PoolerType_REPLICA,
		},
		IsLastCheckValid: true,
		IsUpToDate:       true,
		LastSeen:         timestamppb.Now(),
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_REPLICA,
			ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
				IsWalReplayPaused: false,
				Lag:               durationpb.New(100 * time.Millisecond), // 100ms lag
			},
		},
	}
	ps.Set("multipooler-cell1-replica-1", replica1)

	// Add replica 2 (lagging)
	replica2 := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: replica2ID,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "testtg",
				Shard:      "0",
			},
			Type: clustermetadatapb.PoolerType_REPLICA,
		},
		IsLastCheckValid: true,
		IsUpToDate:       true,
		LastSeen:         timestamppb.Now(),
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_REPLICA,
			ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
				IsWalReplayPaused: false,
				Lag:               durationpb.New(15 * time.Second), // 15s lag (> 10s threshold)
			},
		},
	}
	ps.Set("multipooler-cell1-replica-2", replica2)

	generator := NewAnalysisGenerator(ps, nil)
	analyses := flattenShardAnalyses(generator.GenerateShardAnalyses())

	require.Len(t, analyses, 3, "should generate three analyses")

	// Find the primary analysis
	var primaryAnalysis *PoolerAnalysis
	for _, a := range analyses {
		if a.IsLeader {
			primaryAnalysis = a
			break
		}
	}

	require.NotNil(t, primaryAnalysis, "should find primary analysis")
	assert.True(t, primaryAnalysis.IsLeader)
}

func TestAnalysisGenerator_GenerateShardAnalyses_Replica(t *testing.T) {
	ps := store.NewPoolerStore()

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
			Id: primaryID,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "testtg",
				Shard:      "0",
			},
			Type: clustermetadatapb.PoolerType_PRIMARY,
		},
		IsLastCheckValid: true,
		IsUpToDate:       true,
		LastSeen:         timestamppb.Now(),
		ConsensusStatus:  primaryConsensusStatus(primaryID, 1),
		Status: &multipoolermanagerdatapb.Status{
			PoolerType:    clustermetadatapb.PoolerType_PRIMARY,
			PostgresReady: true,
		},
	}
	ps.Set("multipooler-cell1-primary-1", primary)

	// Add replica
	replica := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: replicaID,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "testtg",
				Shard:      "0",
			},
			Type: clustermetadatapb.PoolerType_REPLICA,
		},
		IsLastCheckValid: true,
		IsUpToDate:       true,
		LastSeen:         timestamppb.Now(),
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_REPLICA,
			ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
				IsWalReplayPaused: false,
				Lag:               durationpb.New(500 * time.Millisecond),
				LastReplayLsn:     "0/1234567",
			},
		},
	}
	ps.Set("multipooler-cell1-replica-1", replica)

	generator := NewAnalysisGenerator(ps, nil)
	shards := generator.GenerateShardAnalyses()

	require.Len(t, shards, 1, "should generate one shard analysis")
	sa := shards[0]
	require.Len(t, sa.Analyses, 2, "should generate two pooler analyses")

	// Find the replica analysis
	replicaAnalysis := sa.Replicas()
	require.Len(t, replicaAnalysis, 1, "should find one replica")
	assert.False(t, replicaAnalysis[0].IsLeader)

	// Primary health is now a shard-level field
	assert.NotNil(t, sa.HighestShardRule.GetLeaderId(), "should have topology primary ID populated")
	assert.True(t, sa.LeaderReachable)
}

func TestAnalysisGenerator_GenerateShardAnalyses_MultipleTableGroups(t *testing.T) {
	ps := store.NewPoolerStore()

	// Add poolers from two different table groups
	tg1Primary := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "tg1-primary",
			},
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "tg1",
				Shard:      "0",
			},
			Type: clustermetadatapb.PoolerType_PRIMARY,
		},
		IsLastCheckValid: true,
		IsUpToDate:       true,
		LastSeen:         timestamppb.Now(),
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		},
	}
	ps.Set("multipooler-cell1-tg1-primary", tg1Primary)

	tg2Primary := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "tg2-primary",
			},
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "tg2",
				Shard:      "0",
			},
			Type: clustermetadatapb.PoolerType_PRIMARY,
		},
		IsLastCheckValid: true,
		IsUpToDate:       true,
		LastSeen:         timestamppb.Now(),
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		},
	}
	ps.Set("multipooler-cell1-tg2-primary", tg2Primary)

	generator := NewAnalysisGenerator(ps, nil)
	analyses := flattenShardAnalyses(generator.GenerateShardAnalyses())

	require.Len(t, analyses, 2, "should generate two analyses")

	// Verify both table groups are present
	tableGroups := make(map[string]bool)
	for _, a := range analyses {
		tableGroups[a.ShardKey.TableGroup] = true
	}

	assert.True(t, tableGroups["tg1"])
	assert.True(t, tableGroups["tg2"])
}

// Task 6: Test for skipping nil entries
func TestGenerateShardAnalyses_SkipsNilEntries(t *testing.T) {
	ps := store.NewPoolerStore()

	// Add a nil entry
	ps.Set("nil-pooler", nil)

	// Add a valid pooler
	ps.Set("valid-pooler", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "valid",
			},
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "db1",
				TableGroup: "tg1",
				Shard:      "shard1",
			},
		},
		IsLastCheckValid: true,
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_REPLICA,
		},
	})

	gen := NewAnalysisGenerator(ps, nil)
	analyses := flattenShardAnalyses(gen.GenerateShardAnalyses())

	// Should only generate one analysis for the valid pooler, skipping the nil entry
	assert.Len(t, analyses, 1)
	assert.Equal(t, "db1", analyses[0].ShardKey.Database)
}

// Task 7: Test for no primary in shard
func TestPopulatePrimaryInfo_NoPrimaryInShard(t *testing.T) {
	ps := store.NewPoolerStore()

	replicaID := topoclient.ComponentID("multipooler-cell1-replica")
	ps.Set(replicaID, &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "replica",
			},
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "db1",
				TableGroup: "tg1",
				Shard:      "shard1",
			},
		},
		IsLastCheckValid: true,
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_REPLICA,
			ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
				LastReplayLsn: "0/1234",
			},
		},
	})

	gen := NewAnalysisGenerator(ps, nil)
	sa, err := gen.GenerateShardAnalysis(&clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "shard1"})
	require.NoError(t, err)

	// When no primary exists in the shard, topology primary fields should be nil/false
	assert.Nil(t, sa.HighestShardRule.GetLeaderId())
	assert.False(t, sa.LeaderReachable)
}

// Task 7: Test for primary with postgres down
func TestPopulatePrimaryInfo_PrimaryPostgresDown(t *testing.T) {
	ps := store.NewPoolerStore()

	primaryID := topoclient.ComponentID("multipooler-cell1-primary")
	replicaID := topoclient.ComponentID("multipooler-cell1-replica")

	// Primary with PostgresReady: false (postgres is down)
	ps.Set(primaryID, &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "primary",
			},
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "db1",
				TableGroup: "tg1",
				Shard:      "shard1",
			},
		},
		IsLastCheckValid: true,
		ConsensusStatus:  primaryConsensusStatus(&clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"}, 1),
		Status: &multipoolermanagerdatapb.Status{
			PoolerType:    clustermetadatapb.PoolerType_PRIMARY,
			PostgresReady: false, // Postgres is down!
			PrimaryStatus: &multipoolermanagerdatapb.PrimaryStatus{Lsn: "0/1234"},
		},
	})

	ps.Set(replicaID, &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "replica",
			},
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "db1",
				TableGroup: "tg1",
				Shard:      "shard1",
			},
		},
		IsLastCheckValid: true,
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_REPLICA,
			ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
				LastReplayLsn: "0/1234",
			},
		},
	})

	gen := NewAnalysisGenerator(ps, nil)
	sa, err := gen.GenerateShardAnalysis(&clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "shard1"})
	require.NoError(t, err)
	analysis := findPoolerByName(sa, "replica")
	require.NotNil(t, analysis)

	// HighestTermDiscoveredPrimaryID should be set even when postgres is down
	assert.NotNil(t, sa.HighestShardRule.GetLeaderId())
	// But PrimaryReachable should be false because postgres is down
	assert.False(t, sa.LeaderReachable, "primary should NOT be reachable when postgres is down")
}

// TestPopulatePrimaryInfo_DemotedViaRecruit covers the scenario where a primary is
// demoted via Recruit and restarted as a standby (emergencyDemoteLocked behavior).
// After restart, PoolerType=REPLICA in the health snapshot and primary_term stays > 0
// (only SetPrimary clears it). PrimaryReachable must be false so PrimaryIsDead
// triggers and a new primary can be elected.
func TestPopulatePrimaryInfo_DemotedViaRecruit(t *testing.T) {
	replica := &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "replica",
			},
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "db1",
				TableGroup: "tg1",
				Shard:      "shard1",
			},
			Type: clustermetadatapb.PoolerType_REPLICA,
		},
		IsLastCheckValid: true,
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_REPLICA,
		},
	}

	shardKey := &clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "shard1"}

	t.Run("topology type PRIMARY, PoolerType REPLICA, primary term > 0 via ConsensusStatus", func(t *testing.T) {
		// Former primary promoted at term 4; etcd topology updated to PRIMARY.
		// After REVOKE, postgres restarts as standby → PoolerType=REPLICA.
		// The committed rule still names this node as primary (before new rule replicates),
		// so IsPrimary(ConsensusStatus) remains true and term > 0.
		ps := store.NewPoolerStore()
		formerPrimaryID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "cell1",
			Name:      "primary",
		}
		ps.Set("multipooler-cell1-primary", &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: formerPrimaryID,
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "db1",
					TableGroup: "tg1",
					Shard:      "shard1",
				},
				Type: clustermetadatapb.PoolerType_PRIMARY, // etcd updated when promoted
			},
			IsLastCheckValid: true,
			ConsensusStatus:  primaryConsensusStatus(formerPrimaryID, 4),
			// Recruit marked the former leader resigned at its term; this is what
			// makes it not LeaderReachable now that postgres restarted as a standby.
			AvailabilityStatus: &clustermetadatapb.AvailabilityStatus{
				LeadershipStatus: &clustermetadatapb.LeadershipStatus{
					Signal:     clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION,
					LeaderTerm: 4,
				},
			},
			Status: &multipoolermanagerdatapb.Status{
				PoolerType:      clustermetadatapb.PoolerType_REPLICA, // running as standby after REVOKE
				PostgresReady:   true,
				PostgresRunning: true,
			},
		})
		ps.Set("multipooler-cell1-replica", replica)

		gen := NewAnalysisGenerator(ps, nil)
		sa, err := gen.GenerateShardAnalysis(shardKey)
		require.NoError(t, err)
		assert.NotNil(t, sa.HighestShardRule.GetLeaderId(), "demoted primary should still be tracked (primary term > 0)")
		assert.Equal(t, "primary", sa.HighestShardRule.GetLeaderId().Name)
		assert.False(t, sa.LeaderReachable, "demoted primary reporting REPLICA should not be LeaderReachable")
		assert.True(t, sa.LeaderPoolerReachable)
	})

	t.Run("topology type REPLICA, PoolerType REPLICA, primary term > 0 via ConsensusStatus (stale etcd)", func(t *testing.T) {
		// Simulates etcd being stopped before the promotion: topology still shows this node
		// as REPLICA (initial assignment), but it was promoted later (term=4) and then
		// revoked. HighestTermDiscoveredPrimaryID must still be found via ConsensusStatus.
		ps := store.NewPoolerStore()
		formerPrimaryID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "cell1",
			Name:      "former-primary",
		}
		ps.Set("multipooler-cell1-former-primary", &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: formerPrimaryID,
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "db1",
					TableGroup: "tg1",
					Shard:      "shard1",
				},
				Type: clustermetadatapb.PoolerType_REPLICA, // stale etcd: never updated
			},
			IsLastCheckValid: true,
			ConsensusStatus:  primaryConsensusStatus(formerPrimaryID, 4),
			// Recruit marked the former leader resigned at its term; this is what
			// makes it not LeaderReachable now that postgres restarted as a standby.
			AvailabilityStatus: &clustermetadatapb.AvailabilityStatus{
				LeadershipStatus: &clustermetadatapb.LeadershipStatus{
					Signal:     clustermetadatapb.LeadershipSignal_LEADERSHIP_SIGNAL_REQUESTING_DEMOTION,
					LeaderTerm: 4,
				},
			},
			Status: &multipoolermanagerdatapb.Status{
				PoolerType:      clustermetadatapb.PoolerType_REPLICA, // running as standby after REVOKE
				PostgresReady:   true,
				PostgresRunning: true,
			},
		})
		ps.Set("multipooler-cell1-replica", replica)

		gen := NewAnalysisGenerator(ps, nil)
		sa, err := gen.GenerateShardAnalysis(shardKey)
		require.NoError(t, err)
		assert.NotNil(t, sa.HighestShardRule.GetLeaderId(), "stale-topology former primary should be found via ConsensusStatus")
		assert.Equal(t, "former-primary", sa.HighestShardRule.GetLeaderId().Name)
		assert.False(t, sa.LeaderReachable, "demoted primary reporting REPLICA should not be LeaderReachable")
		assert.True(t, sa.LeaderPoolerReachable)
	})
}

// TestGenerateShardAnalysis_LeaderNamedButAbsentFromStore covers the invariant
// that justifies keeping HighestShardRule (consensus identity) and Leader (health)
// as separate fields: a follower can carry a rule naming a leader whose own health
// state is not in the store. The leader is then identified by consensus but has no
// health to attach, so Leader is nil and the leader is treated as unreachable.
func TestGenerateShardAnalysis_LeaderNamedButAbsentFromStore(t *testing.T) {
	shardKey := &clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "shard1"}

	absentLeaderID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "absent-leader",
	}
	followerID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "follower",
	}

	ps := store.NewPoolerStore()
	// Only the follower is in the store. Its replication primary rule names the
	// leader at term 5, so consensus identifies a leader the store has no health for.
	ps.Set("multipooler-cell1-follower", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:       followerID,
			ShardKey: shardKey,
			Type:     clustermetadatapb.PoolerType_REPLICA,
		},
		IsLastCheckValid: true,
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id: followerID,
			ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{
				Rule: &clustermetadatapb.ShardRule{
					RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
					LeaderId:   absentLeaderID,
				},
			},
		},
	})

	gen := NewAnalysisGenerator(ps, nil)
	sa, err := gen.GenerateShardAnalysis(shardKey)
	require.NoError(t, err)

	require.NotNil(t, sa.HighestShardRule.GetLeaderId(), "consensus must still identify the leader")
	assert.Equal(t, "absent-leader", sa.HighestShardRule.GetLeaderId().Name)
	assert.Nil(t, sa.Leader, "no health state exists for the named leader")
	assert.False(t, sa.LeaderReachable, "a leader with no health cannot be reachable")
	assert.False(t, sa.LeaderPoolerReachable)
}

// TestGenerateShardAnalysis_StaleLeaderSupersededViaFollowerRule is a regression
// test for the post-failover window where the newly elected leader has not yet
// reported a direct health update. A stale leader still self-claims leadership
// at the old term, while a follower already replicates from the new leader at a
// higher term. Leader identity must come from who the poolers say the leader is
// (the rule's LeaderId, including via the replication primary), not from which
// pooler claims leadership for itself — otherwise the stale leader would be
// mistaken for the primary and failover suppressed.
func TestGenerateShardAnalysis_StaleLeaderSupersededViaFollowerRule(t *testing.T) {
	shardKey := &clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "shard1"}

	staleLeaderID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "stale-leader"}
	newLeaderID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "new-leader"}
	followerID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "follower"}

	ps := store.NewPoolerStore()
	// Stale leader: still self-claims leadership at the old term (term 5) and is
	// reachable and ready. Under the old self-claim-only logic this would be
	// picked as the leader.
	ps.Set("multipooler-cell1-stale-leader", &multiorchdatapb.PoolerHealthState{
		MultiPooler:      &clustermetadatapb.MultiPooler{Id: staleLeaderID, ShardKey: shardKey, Type: clustermetadatapb.PoolerType_PRIMARY},
		IsLastCheckValid: true,
		ConsensusStatus:  primaryConsensusStatus(staleLeaderID, 5),
		Status:           &multipoolermanagerdatapb.Status{PoolerType: clustermetadatapb.PoolerType_PRIMARY, PostgresReady: true, PostgresRunning: true},
	})
	// Follower: already replicating from the new leader at the higher term (6).
	// The new leader itself has not reported health yet (absent from the store).
	ps.Set("multipooler-cell1-follower", &multiorchdatapb.PoolerHealthState{
		MultiPooler:      &clustermetadatapb.MultiPooler{Id: followerID, ShardKey: shardKey, Type: clustermetadatapb.PoolerType_REPLICA},
		IsLastCheckValid: true,
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id: followerID,
			ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{
				Rule: &clustermetadatapb.ShardRule{
					RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 6},
					LeaderId:   newLeaderID,
				},
			},
		},
	})

	gen := NewAnalysisGenerator(ps, nil)
	sa, err := gen.GenerateShardAnalysis(shardKey)
	require.NoError(t, err)

	require.NotNil(t, sa.HighestShardRule.GetLeaderId())
	assert.Equal(t, "new-leader", sa.HighestShardRule.GetLeaderId().Name,
		"the leader named by the follower's higher-term rule must win over the stale self-claiming leader")
	assert.Equal(t, int64(6), sa.HighestShardRule.GetRuleNumber().GetCoordinatorTerm())
	assert.Nil(t, sa.Leader, "the new leader has no health state yet, so Leader is nil")
}

func TestIsInStandbyList(t *testing.T) {
	ps := store.NewPoolerStore()

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
			ps.Set("multipooler-cell1-primary-1", &multiorchdatapb.PoolerHealthState{
				MultiPooler: &clustermetadatapb.MultiPooler{
					Id: primaryID,
					ShardKey: &clustermetadatapb.ShardKey{
						Database:   "testdb",
						TableGroup: "testtg",
						Shard:      "0",
					},
					Type: clustermetadatapb.PoolerType_PRIMARY,
				},
				IsLastCheckValid: true,
				IsUpToDate:       true,
				LastSeen:         timestamppb.Now(),
				ConsensusStatus: &clustermetadatapb.ConsensusStatus{
					Id: primaryID,
					CurrentPosition: &clustermetadatapb.PoolerPosition{
						Rule: &clustermetadatapb.ShardRule{
							RuleNumber:    &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
							LeaderId:      primaryID,
							CohortMembers: tt.primaryStatus.GetSyncReplicationConfig().GetStandbyIds(),
						},
					},
				},
				Status: &multipoolermanagerdatapb.Status{
					PoolerType:    clustermetadatapb.PoolerType_PRIMARY,
					PostgresReady: true,
					PrimaryStatus: tt.primaryStatus,
				},
			})

			generator := NewAnalysisGenerator(ps, nil)
			sa, err := generator.GenerateShardAnalysis(&clustermetadatapb.ShardKey{Database: "testdb", TableGroup: "testtg", Shard: "0"})
			require.NoError(t, err)

			result := sa.IsInStandbyList(tt.replicaID)

			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPopulatePrimaryInfo_PrimaryHealthFields(t *testing.T) {
	t.Run("sets PrimaryPoolerReachable and PrimaryPostgresReady correctly", func(t *testing.T) {
		ps := store.NewPoolerStore()

		primaryID := topoclient.ComponentID("multipooler-cell1-primary")
		replicaID := topoclient.ComponentID("multipooler-cell1-replica")

		// Primary with pooler reachable and postgres running
		respondedAt := time.Now().Add(-3 * time.Second)
		ps.Set(primaryID, &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "cell1",
					Name:      "primary",
				},
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "db1",
					TableGroup: "tg1",
					Shard:      "shard1",
				},
				Hostname: "primary-host",
				PortMap:  map[string]int32{"postgres": 5432},
			},
			ConsensusStatus:       primaryConsensusStatus(&clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"}, 1),
			IsLastCheckValid:      true,
			LastPostgresReadyTime: timestamppb.New(respondedAt),
			Status: &multipoolermanagerdatapb.Status{
				PoolerType:    clustermetadatapb.PoolerType_PRIMARY,
				PostgresReady: true,
			},
		})

		ps.Set(replicaID, &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "cell1",
					Name:      "replica",
				},
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "db1",
					TableGroup: "tg1",
					Shard:      "shard1",
				},
			},
			IsLastCheckValid: true,
			Status: &multipoolermanagerdatapb.Status{
				PoolerType: clustermetadatapb.PoolerType_REPLICA,
			},
		})

		gen := NewAnalysisGenerator(ps, nil)
		sa, err := gen.GenerateShardAnalysis(&clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "shard1"})
		require.NoError(t, err)

		assert.True(t, sa.LeaderPoolerReachable)
		assert.True(t, sa.LeaderPostgresReady)
		assert.True(t, sa.LeaderReachable)
		assert.WithinDuration(t, respondedAt, sa.LeaderLastPostgresReadyTime, time.Second,
			"PrimaryLastPostgresReadyTime should be propagated from primary's LastPostgresReadyTime")
	})

	t.Run("sets PrimaryPoolerReachable false when pooler unreachable", func(t *testing.T) {
		ps := store.NewPoolerStore()

		primaryID := topoclient.ComponentID("multipooler-cell1-primary")
		replicaID := topoclient.ComponentID("multipooler-cell1-replica")

		// Primary with pooler unreachable
		ps.Set(primaryID, &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "cell1",
					Name:      "primary",
				},
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "db1",
					TableGroup: "tg1",
					Shard:      "shard1",
				},
				Hostname: "primary-host",
				PortMap:  map[string]int32{"postgres": 5432},
			},
			ConsensusStatus:  primaryConsensusStatus(&clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"}, 1),
			IsLastCheckValid: false, // Pooler unreachable
			Status: &multipoolermanagerdatapb.Status{
				PoolerType:    clustermetadatapb.PoolerType_PRIMARY,
				PostgresReady: false,
			},
		})

		ps.Set(replicaID, &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "cell1",
					Name:      "replica",
				},
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "db1",
					TableGroup: "tg1",
					Shard:      "shard1",
				},
			},
			IsLastCheckValid: true,
			Status: &multipoolermanagerdatapb.Status{
				PoolerType: clustermetadatapb.PoolerType_REPLICA,
			},
		})

		gen := NewAnalysisGenerator(ps, nil)
		sa, err := gen.GenerateShardAnalysis(&clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "shard1"})
		require.NoError(t, err)

		assert.False(t, sa.LeaderPoolerReachable)
		assert.False(t, sa.LeaderPostgresReady)
		assert.False(t, sa.LeaderReachable)
	})
}

func TestAllReplicasConnectedToLeader(t *testing.T) {
	t.Run("returns true when all replicas connected", func(t *testing.T) {
		ps := store.NewPoolerStore()

		primaryID := topoclient.ComponentID("multipooler-cell1-primary")
		replica1ID := topoclient.ComponentID("multipooler-cell1-replica1")
		replica2ID := topoclient.ComponentID("multipooler-cell1-replica2")

		ps.Set(primaryID, &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "cell1",
					Name:      "primary",
				},
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "db1",
					TableGroup: "tg1",
					Shard:      "shard1",
				},
				Hostname: "primary-host",
				PortMap:  map[string]int32{"postgres": 5432},
			},
			ConsensusStatus:  primaryConsensusStatus(&clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"}, 1),
			IsLastCheckValid: false, // Primary pooler is down
			Status: &multipoolermanagerdatapb.Status{
				PoolerType:    clustermetadatapb.PoolerType_PRIMARY,
				PostgresReady: false,
			},
		})

		now := time.Now()

		// Replica 1 - connected to primary
		ps.Set(replica1ID, &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "cell1",
					Name:      "replica1",
				},
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "db1",
					TableGroup: "tg1",
					Shard:      "shard1",
				},
			},
			IsLastCheckValid:        true,
			StreamSnapshotsReceived: 1,
			Status: &multipoolermanagerdatapb.Status{
				PoolerType: clustermetadatapb.PoolerType_REPLICA,
				ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
					LastReceiveLsn:     "0/1234567",
					WalReceiverStatus:  "streaming",
					LastMsgReceiveTime: timestamppb.New(now.Add(-5 * time.Second)),
					PrimaryConnInfo: &multipoolermanagerdatapb.PrimaryConnInfo{
						Host: "primary-host",
						Port: 5432,
					},
				},
			},
		})

		// Replica 2 - also connected to primary
		ps.Set(replica2ID, &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "cell1",
					Name:      "replica2",
				},
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "db1",
					TableGroup: "tg1",
					Shard:      "shard1",
				},
			},
			IsLastCheckValid:        true,
			StreamSnapshotsReceived: 1,
			Status: &multipoolermanagerdatapb.Status{
				PoolerType: clustermetadatapb.PoolerType_REPLICA,
				ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
					LastReceiveLsn:     "0/1234567",
					WalReceiverStatus:  "streaming",
					LastMsgReceiveTime: timestamppb.New(now.Add(-5 * time.Second)),
					PrimaryConnInfo: &multipoolermanagerdatapb.PrimaryConnInfo{
						Host: "primary-host",
						Port: 5432,
					},
				},
			},
		})

		gen := NewAnalysisGenerator(ps, nil)
		sa, err := gen.GenerateShardAnalysis(&clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "shard1"})
		require.NoError(t, err)

		assert.True(t, sa.ReplicasConnectedToLeader, "should be true when all replicas are connected")
	})

	t.Run("returns false when one replica disconnected", func(t *testing.T) {
		ps := store.NewPoolerStore()

		primaryID := topoclient.ComponentID("multipooler-cell1-primary")
		replica1ID := topoclient.ComponentID("multipooler-cell1-replica1")
		replica2ID := topoclient.ComponentID("multipooler-cell1-replica2")

		ps.Set(primaryID, &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "cell1",
					Name:      "primary",
				},
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "db1",
					TableGroup: "tg1",
					Shard:      "shard1",
				},
				Hostname: "primary-host",
				PortMap:  map[string]int32{"postgres": 5432},
			},
			ConsensusStatus:  primaryConsensusStatus(&clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"}, 1),
			IsLastCheckValid: false,
			Status: &multipoolermanagerdatapb.Status{
				PoolerType:    clustermetadatapb.PoolerType_PRIMARY,
				PostgresReady: false,
			},
		})

		// Replica 1 - connected
		ps.Set(replica1ID, &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "cell1",
					Name:      "replica1",
				},
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "db1",
					TableGroup: "tg1",
					Shard:      "shard1",
				},
			},
			IsLastCheckValid:        true,
			StreamSnapshotsReceived: 1,
			Status: &multipoolermanagerdatapb.Status{
				PoolerType: clustermetadatapb.PoolerType_REPLICA,
				ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
					LastReceiveLsn:     "0/1234567",
					WalReceiverStatus:  "streaming",
					LastMsgReceiveTime: timestamppb.New(time.Now().Add(-5 * time.Second)),
					PrimaryConnInfo: &multipoolermanagerdatapb.PrimaryConnInfo{
						Host: "primary-host",
						Port: 5432,
					},
				},
			},
		})

		// Replica 2 - disconnected (no PrimaryConnInfo)
		ps.Set(replica2ID, &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "cell1",
					Name:      "replica2",
				},
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "db1",
					TableGroup: "tg1",
					Shard:      "shard1",
				},
			},
			IsLastCheckValid:        true,
			StreamSnapshotsReceived: 1,
			Status: &multipoolermanagerdatapb.Status{
				PoolerType:        clustermetadatapb.PoolerType_REPLICA,
				ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
					// No PrimaryConnInfo - replica is disconnected
				},
			},
		})

		gen := NewAnalysisGenerator(ps, nil)
		sa, err := gen.GenerateShardAnalysis(&clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "shard1"})
		require.NoError(t, err)

		assert.False(t, sa.ReplicasConnectedToLeader, "should be false when any replica is disconnected")
	})

	t.Run("returns false when replica unreachable", func(t *testing.T) {
		ps := store.NewPoolerStore()

		primaryID := topoclient.ComponentID("multipooler-cell1-primary")
		replica1ID := topoclient.ComponentID("multipooler-cell1-replica1")

		ps.Set(primaryID, &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "cell1",
					Name:      "primary",
				},
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "db1",
					TableGroup: "tg1",
					Shard:      "shard1",
				},
				Hostname: "primary-host",
				PortMap:  map[string]int32{"postgres": 5432},
			},
			ConsensusStatus:  primaryConsensusStatus(&clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"}, 1),
			IsLastCheckValid: false,
			Status: &multipoolermanagerdatapb.Status{
				PoolerType:    clustermetadatapb.PoolerType_PRIMARY,
				PostgresReady: false,
			},
		})

		// Replica was previously healthy but is now unreachable (stream disconnected).
		// StreamSnapshotsReceived > 0 distinguishes this from a brand-new pooler.
		ps.Set(replica1ID, &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "cell1",
					Name:      "replica1",
				},
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "db1",
					TableGroup: "tg1",
					Shard:      "shard1",
				},
			},
			IsLastCheckValid:        false, // Replica unreachable
			StreamSnapshotsReceived: 1,     // Was previously healthy
			Status: &multipoolermanagerdatapb.Status{
				PoolerType: clustermetadatapb.PoolerType_REPLICA,
			},
		})

		gen := NewAnalysisGenerator(ps, nil)
		sa, err := gen.GenerateShardAnalysis(&clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "shard1"})
		require.NoError(t, err)

		assert.False(t, sa.ReplicasConnectedToLeader, "should be false when replica is unreachable")
	})

	t.Run("returns false when no replicas exist", func(t *testing.T) {
		ps := store.NewPoolerStore()

		primaryID := topoclient.ComponentID("multipooler-cell1-primary")

		// Only primary, no replicas
		ps.Set(primaryID, &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "cell1",
					Name:      "primary",
				},
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "db1",
					TableGroup: "tg1",
					Shard:      "shard1",
				},
				Hostname: "primary-host",
				PortMap:  map[string]int32{"postgres": 5432},
			},
			ConsensusStatus:  primaryConsensusStatus(&clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"}, 1),
			IsLastCheckValid: true,
			Status: &multipoolermanagerdatapb.Status{
				PoolerType:    clustermetadatapb.PoolerType_PRIMARY,
				PostgresReady: true,
			},
		})

		gen := NewAnalysisGenerator(ps, nil)
		sa, err := gen.GenerateShardAnalysis(&clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "shard1"})
		require.NoError(t, err)

		// Primary-only shard: ReplicasConnectedToLeader should be false (no replicas)
		assert.False(t, sa.ReplicasConnectedToLeader)
	})

	t.Run("returns false when replica pointing to wrong primary", func(t *testing.T) {
		ps := store.NewPoolerStore()

		primaryID := topoclient.ComponentID("multipooler-cell1-primary")
		replicaID := topoclient.ComponentID("multipooler-cell1-replica")

		ps.Set(primaryID, &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "cell1",
					Name:      "primary",
				},
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "db1",
					TableGroup: "tg1",
					Shard:      "shard1",
				},
				Hostname: "primary-host",
				PortMap:  map[string]int32{"postgres": 5432},
			},
			ConsensusStatus:  primaryConsensusStatus(&clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"}, 1),
			IsLastCheckValid: false,
			Status: &multipoolermanagerdatapb.Status{
				PoolerType:    clustermetadatapb.PoolerType_PRIMARY,
				PostgresReady: false,
			},
		})

		// Replica pointing to different host
		ps.Set(replicaID, &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "cell1",
					Name:      "replica",
				},
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "db1",
					TableGroup: "tg1",
					Shard:      "shard1",
				},
			},
			IsLastCheckValid:        true,
			StreamSnapshotsReceived: 1,
			Status: &multipoolermanagerdatapb.Status{
				PoolerType: clustermetadatapb.PoolerType_REPLICA,
				ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
					LastReceiveLsn:     "0/1234567",
					WalReceiverStatus:  "streaming",
					LastMsgReceiveTime: timestamppb.New(time.Now().Add(-5 * time.Second)),
					PrimaryConnInfo: &multipoolermanagerdatapb.PrimaryConnInfo{
						Host: "different-host", // Wrong host!
						Port: 5432,
					},
				},
			},
		})

		gen := NewAnalysisGenerator(ps, nil)
		sa, err := gen.GenerateShardAnalysis(&clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "shard1"})
		require.NoError(t, err)

		assert.False(t, sa.ReplicasConnectedToLeader, "should be false when replica points to wrong primary")
	})

	t.Run("returns false when WAL receiver is not streaming", func(t *testing.T) {
		ps := store.NewPoolerStore()

		primaryID := topoclient.ComponentID("multipooler-cell1-primary")
		replicaID := topoclient.ComponentID("multipooler-cell1-replica")

		ps.Set(primaryID, &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"},
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "db1",
					TableGroup: "tg1",
					Shard:      "shard1",
				},
				Hostname: "primary-host",
				PortMap:  map[string]int32{"postgres": 5432},
			},
			ConsensusStatus: primaryConsensusStatus(&clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"}, 1),
			Status: &multipoolermanagerdatapb.Status{
				PoolerType: clustermetadatapb.PoolerType_PRIMARY,
			},
		})

		for _, status := range []string{"", "starting", "waiting", "stopping"} {
			ps.Set(replicaID, &multiorchdatapb.PoolerHealthState{
				MultiPooler: &clustermetadatapb.MultiPooler{
					Id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica"},
					ShardKey: &clustermetadatapb.ShardKey{
						Database:   "db1",
						TableGroup: "tg1",
						Shard:      "shard1",
					},
				},
				IsLastCheckValid:        true,
				StreamSnapshotsReceived: 1,
				Status: &multipoolermanagerdatapb.Status{
					PoolerType: clustermetadatapb.PoolerType_REPLICA,
					ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
						LastReceiveLsn:     "0/1234567",
						WalReceiverStatus:  status, // not "streaming"
						LastMsgReceiveTime: timestamppb.New(time.Now().Add(-5 * time.Second)),
						PrimaryConnInfo: &multipoolermanagerdatapb.PrimaryConnInfo{
							Host: "primary-host",
							Port: 5432,
						},
					},
				},
			})

			gen := NewAnalysisGenerator(ps, nil)
			analysis, err := gen.GenerateAnalysisForPooler(replicaID)
			require.NoError(t, err)
			assert.False(t, analysis.ReplicasConnectedToLeader, "should be false when wal_receiver_status=%q", status)
		}
	})

	t.Run("returns false when last_msg_receive_time is stale (default threshold)", func(t *testing.T) {
		// No WalReceiverStatusInterval supplied — falls back to defaultReplicationHeartbeatStalenessThreshold.
		ps := store.NewPoolerStore()

		primaryID := topoclient.ComponentID("multipooler-cell1-primary")
		replicaID := topoclient.ComponentID("multipooler-cell1-replica")

		ps.Set(primaryID, &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"},
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "db1",
					TableGroup: "tg1",
					Shard:      "shard1",
				},
				Hostname: "primary-host",
				PortMap:  map[string]int32{"postgres": 5432},
			},
			ConsensusStatus: primaryConsensusStatus(&clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"}, 1),
			Status: &multipoolermanagerdatapb.Status{
				PoolerType: clustermetadatapb.PoolerType_PRIMARY,
			},
		})

		fixedNow := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
		staleTime := fixedNow.Add(-(defaultReplicationHeartbeatStalenessThreshold + time.Second))

		ps.Set(replicaID, &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica"},
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "db1",
					TableGroup: "tg1",
					Shard:      "shard1",
				},
			},
			IsLastCheckValid:        true,
			StreamSnapshotsReceived: 1,
			Status: &multipoolermanagerdatapb.Status{
				PoolerType: clustermetadatapb.PoolerType_REPLICA,
				ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
					LastReceiveLsn:     "0/1234567",
					WalReceiverStatus:  "streaming",
					LastMsgReceiveTime: timestamppb.New(staleTime),
					// WalReceiverStatusInterval intentionally nil — exercises fallback path
					PrimaryConnInfo: &multipoolermanagerdatapb.PrimaryConnInfo{
						Host: "primary-host",
						Port: 5432,
					},
				},
			},
		})

		gen := NewAnalysisGenerator(ps, nil)
		gen.now = func() time.Time { return fixedNow }
		analysis, err := gen.GenerateAnalysisForPooler(replicaID)
		require.NoError(t, err)

		assert.False(t, analysis.ReplicasConnectedToLeader, "should be false when last_msg_receive_time is stale")
	})

	t.Run("returns false when last_msg_receive_time is stale (dynamic threshold)", func(t *testing.T) {
		// WalReceiverStatusInterval supplied — threshold is multiplier × interval.
		ps := store.NewPoolerStore()

		primaryID := topoclient.ComponentID("multipooler-cell1-primary")
		replicaID := topoclient.ComponentID("multipooler-cell1-replica")

		ps.Set(primaryID, &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"},
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "db1",
					TableGroup: "tg1",
					Shard:      "shard1",
				},
				Hostname: "primary-host",
				PortMap:  map[string]int32{"postgres": 5432},
			},
			ConsensusStatus: primaryConsensusStatus(&clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"}, 1),
			Status: &multipoolermanagerdatapb.Status{
				PoolerType: clustermetadatapb.PoolerType_PRIMARY,
			},
		})

		fixedNow := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
		interval := 5 * time.Second
		dynamicThreshold := replicationHeartbeatStalenessMultiplier * interval // 15s
		staleTime := fixedNow.Add(-(dynamicThreshold + time.Second))

		ps.Set(replicaID, &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica"},
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "db1",
					TableGroup: "tg1",
					Shard:      "shard1",
				},
			},
			IsLastCheckValid:        true,
			StreamSnapshotsReceived: 1,
			Status: &multipoolermanagerdatapb.Status{
				PoolerType: clustermetadatapb.PoolerType_REPLICA,
				ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
					LastReceiveLsn:            "0/1234567",
					WalReceiverStatus:         "streaming",
					LastMsgReceiveTime:        timestamppb.New(staleTime),
					WalReceiverStatusInterval: durationpb.New(interval),
					PrimaryConnInfo: &multipoolermanagerdatapb.PrimaryConnInfo{
						Host: "primary-host",
						Port: 5432,
					},
				},
			},
		})

		gen := NewAnalysisGenerator(ps, nil)
		gen.now = func() time.Time { return fixedNow }
		analysis, err := gen.GenerateAnalysisForPooler(replicaID)
		require.NoError(t, err)

		assert.False(t, analysis.ReplicasConnectedToLeader, "should be false when last_msg_receive_time exceeds dynamic threshold")
	})

	t.Run("returns false when last_msg_receive_time exceeds wal_receiver_timeout", func(t *testing.T) {
		// Even if the delay is below the staleness threshold, if it exceeds the
		// WAL receiver timeout the connection is effectively dead.
		ps := store.NewPoolerStore()

		primaryID := topoclient.ComponentID("multipooler-cell1-primary")
		replicaID := topoclient.ComponentID("multipooler-cell1-replica")

		ps.Set(primaryID, &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"},
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "db1",
					TableGroup: "tg1",
					Shard:      "shard1",
				},
				Hostname: "primary-host",
				PortMap:  map[string]int32{"postgres": 5432},
			},
			ConsensusStatus: primaryConsensusStatus(&clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"}, 1),
			Status: &multipoolermanagerdatapb.Status{
				PoolerType: clustermetadatapb.PoolerType_PRIMARY,
			},
		})

		fixedNow := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
		walReceiverTimeout := 60 * time.Second
		// last_msg_receive_time is 61s ago — exceeds wal_receiver_timeout (60s) but
		// is still within the staleness threshold (3×10s = 30s would be fine, but
		// the hard deadline fires first).
		lastMsgReceiveTime := fixedNow.Add(-(walReceiverTimeout + time.Second))

		ps.Set(replicaID, &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica"},
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "db1",
					TableGroup: "tg1",
					Shard:      "shard1",
				},
			},
			IsLastCheckValid:        true,
			StreamSnapshotsReceived: 1,
			Status: &multipoolermanagerdatapb.Status{
				PoolerType: clustermetadatapb.PoolerType_REPLICA,
				ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
					LastReceiveLsn:            "0/1234567",
					WalReceiverStatus:         "streaming",
					LastMsgReceiveTime:        timestamppb.New(lastMsgReceiveTime),
					WalReceiverStatusInterval: durationpb.New(10 * time.Second),
					WalReceiverTimeout:        durationpb.New(walReceiverTimeout),
					PrimaryConnInfo: &multipoolermanagerdatapb.PrimaryConnInfo{
						Host: "primary-host",
						Port: 5432,
					},
				},
			},
		})

		gen := NewAnalysisGenerator(ps, nil)
		gen.now = func() time.Time { return fixedNow }
		analysis, err := gen.GenerateAnalysisForPooler(replicaID)
		require.NoError(t, err)

		assert.False(t, analysis.ReplicasConnectedToLeader, "should be false when delay exceeds wal_receiver_timeout")
	})

	t.Run("returns true when last_msg_receive_time is nil", func(t *testing.T) {
		// Backward compatibility: replicas that don't report last_msg_receive_time
		// (e.g. running an older version) should still be considered connected if
		// the WAL receiver is streaming.
		ps := store.NewPoolerStore()

		primaryID := topoclient.ComponentID("multipooler-cell1-primary")
		replicaID := topoclient.ComponentID("multipooler-cell1-replica")

		ps.Set(primaryID, &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"},
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "db1",
					TableGroup: "tg1",
					Shard:      "shard1",
				},
				Hostname: "primary-host",
				PortMap:  map[string]int32{"postgres": 5432},
			},
			ConsensusStatus: primaryConsensusStatus(&clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"}, 1),
			Status: &multipoolermanagerdatapb.Status{
				PoolerType: clustermetadatapb.PoolerType_PRIMARY,
			},
		})

		ps.Set(replicaID, &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica"},
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "db1",
					TableGroup: "tg1",
					Shard:      "shard1",
				},
			},
			IsLastCheckValid:        true,
			StreamSnapshotsReceived: 1,
			Status: &multipoolermanagerdatapb.Status{
				PoolerType: clustermetadatapb.PoolerType_REPLICA,
				ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
					LastReceiveLsn:    "0/1234567",
					WalReceiverStatus: "streaming",
					// LastMsgReceiveTime intentionally nil
					PrimaryConnInfo: &multipoolermanagerdatapb.PrimaryConnInfo{
						Host: "primary-host",
						Port: 5432,
					},
				},
			},
		})

		gen := NewAnalysisGenerator(ps, nil)
		analysis, err := gen.GenerateAnalysisForPooler(replicaID)
		require.NoError(t, err)

		assert.True(t, analysis.ReplicasConnectedToLeader, "should be true when last_msg_receive_time is nil")
	})

	t.Run("new pooler with no health data does not count against connected replicas", func(t *testing.T) {
		// Regression test for the startup race: PoolerWatcher adds a pooler to the
		// store before its health stream delivers the first snapshot
		// (StreamSnapshotsReceived==0). If the recovery tick fires in that window,
		// the new pooler must not be counted as a "disconnected replica" —
		// otherwise it would make allReplicasConnectedToLeader return false and
		// disable the LeaderIsDeadAnalyzer suppression window, risking a
		// premature failover.
		ps := store.NewPoolerStore()

		now := time.Now()

		// Leader is transiently unreachable (pooler down, postgres still running).
		ps.Set("multipooler-cell1-primary", &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"},
				ShardKey: &clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "shard1"},
				Hostname: "primary-host",
				PortMap:  map[string]int32{"postgres": 5432},
			},
			ConsensusStatus:         primaryConsensusStatus(&clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"}, 1),
			IsLastCheckValid:        false,
			StreamSnapshotsReceived: 5,
			Status: &multipoolermanagerdatapb.Status{
				PoolerType:    clustermetadatapb.PoolerType_PRIMARY,
				PostgresReady: false,
			},
		})

		// Existing replica: healthy and actively streaming WAL from the leader.
		ps.Set("multipooler-cell1-replica1", &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica1"},
				ShardKey: &clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "shard1"},
			},
			IsLastCheckValid:        true,
			StreamSnapshotsReceived: 10,
			Status: &multipoolermanagerdatapb.Status{
				PoolerType: clustermetadatapb.PoolerType_REPLICA,
				ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
					LastReceiveLsn:     "0/1234567",
					WalReceiverStatus:  "streaming",
					LastMsgReceiveTime: timestamppb.New(now.Add(-5 * time.Second)),
					PrimaryConnInfo:    &multipoolermanagerdatapb.PrimaryConnInfo{Host: "primary-host", Port: 5432},
				},
			},
		})

		// New pooler just added by PoolerWatcher: no health data yet
		// (StreamSnapshotsReceived==0, IsLastCheckValid==false).
		ps.Set("multipooler-cell2-replica2", &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell2", Name: "replica2"},
				ShardKey: &clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "shard1"},
				Type:     clustermetadatapb.PoolerType_REPLICA,
			},
			// IsLastCheckValid and StreamSnapshotsReceived are both zero — brand new pooler.
		})

		gen := NewAnalysisGenerator(ps, nil)
		sa, err := gen.GenerateShardAnalysis(&clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "shard1"})
		require.NoError(t, err)

		// replica1 is connected; replica2 has never reported health and must not
		// count against the connected-replicas check.
		assert.True(t, sa.ReplicasConnectedToLeader,
			"new pooler with StreamSnapshotsReceived==0 must not count as a disconnected replica")
	})
}

func TestPopulatePrimaryInfo_IsInPrimaryStandbyList(t *testing.T) {
	ps := store.NewPoolerStore()

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
	ps.Set("multipooler-cell1-primary-1", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: primaryID,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "testtg",
				Shard:      "0",
			},
			Type: clustermetadatapb.PoolerType_PRIMARY,
		},
		IsLastCheckValid: true,
		IsUpToDate:       true,
		LastSeen:         timestamppb.Now(),
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id: primaryID,
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Rule: &clustermetadatapb.ShardRule{
					RuleNumber:    &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
					LeaderId:      primaryID,
					CohortMembers: []*clustermetadatapb.ID{replica1ID},
				},
			},
		},
		Status: &multipoolermanagerdatapb.Status{
			PoolerType:    clustermetadatapb.PoolerType_PRIMARY,
			PostgresReady: true,
			PrimaryStatus: &multipoolermanagerdatapb.PrimaryStatus{
				Lsn:   "0/1234567",
				Ready: true,
				SyncReplicationConfig: &multipoolermanagerdatapb.SynchronousReplicationConfiguration{
					StandbyIds: []*clustermetadatapb.ID{replica1ID},
				},
			},
		},
	})

	// Add replica1 (in standby list)
	ps.Set("multipooler-cell1-replica-1", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: replica1ID,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "testtg",
				Shard:      "0",
			},
			Type: clustermetadatapb.PoolerType_REPLICA,
		},
		IsLastCheckValid: true,
		IsUpToDate:       true,
		LastSeen:         timestamppb.Now(),
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_REPLICA,
			ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
				IsWalReplayPaused: false,
				Lag:               durationpb.New(100 * time.Millisecond),
			},
		},
	})

	// Add replica2 (not in standby list)
	ps.Set("multipooler-cell2-replica-2", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: replica2ID,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "testtg",
				Shard:      "0",
			},
			Type: clustermetadatapb.PoolerType_REPLICA,
		},
		IsLastCheckValid: true,
		IsUpToDate:       true,
		LastSeen:         timestamppb.Now(),
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_REPLICA,
			ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
				IsWalReplayPaused: false,
				Lag:               durationpb.New(100 * time.Millisecond),
			},
		},
	})

	generator := NewAnalysisGenerator(ps, nil)
	shardKey := &clustermetadatapb.ShardKey{Database: "testdb", TableGroup: "testtg", Shard: "0"}

	t.Run("replica in standby list", func(t *testing.T) {
		sa, err := generator.GenerateShardAnalysis(shardKey)
		require.NoError(t, err)
		analysis := findPoolerByName(sa, "replica-1")
		require.NotNil(t, analysis)
		assert.True(t, sa.IsInStandbyList(analysis.PoolerID), "replica1 should be in standby list")
	})

	t.Run("replica not in standby list", func(t *testing.T) {
		sa, err := generator.GenerateShardAnalysis(shardKey)
		require.NoError(t, err)
		analysis := findPoolerByName(sa, "replica-2")
		require.NotNil(t, analysis)
		assert.False(t, sa.IsInStandbyList(analysis.PoolerID), "replica2 should not be in standby list")
	})
}

// TestPopulatePrimaryInfo_PicksHighestPrimaryTerm verifies that when two primaries transiently
// coexist (e.g. during failover), the replica's analysis references the one with the higher
// PrimaryTerm — not an arbitrary one from non-deterministic map iteration.
func TestPopulatePrimaryInfo_PicksHighestPrimaryTerm(t *testing.T) {
	ps := store.NewPoolerStore()

	newPrimaryID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "new-primary",
	}
	stalePrimaryID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "stale-primary",
	}
	replicaID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "replica-1",
	}

	shardConfig := func(id *clustermetadatapb.ID) *clustermetadatapb.MultiPooler {
		return &clustermetadatapb.MultiPooler{
			Id:       id,
			ShardKey: &clustermetadatapb.ShardKey{Database: "testdb", TableGroup: "default", Shard: "0"},
			Type:     clustermetadatapb.PoolerType_PRIMARY,
		}
	}

	// New (correct) primary: higher PrimaryTerm, postgres running.
	ps.Set("multipooler-cell1-new-primary", &multiorchdatapb.PoolerHealthState{
		MultiPooler:      shardConfig(newPrimaryID),
		IsLastCheckValid: true,
		LastSeen:         timestamppb.Now(),
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id:             newPrimaryID,
			TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 11},
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Rule: &clustermetadatapb.ShardRule{
					RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 6},
					LeaderId:   newPrimaryID,
				},
			},
		},
		Status: &multipoolermanagerdatapb.Status{
			PoolerType:    clustermetadatapb.PoolerType_PRIMARY,
			PostgresReady: true,
		},
	})

	// Stale primary: lower primary term, postgres NOT running (just came back after being killed).
	ps.Set("multipooler-cell1-stale-primary", &multiorchdatapb.PoolerHealthState{
		MultiPooler:      shardConfig(stalePrimaryID),
		IsLastCheckValid: true,
		LastSeen:         timestamppb.Now(),
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id:             stalePrimaryID,
			TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 10},
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Rule: &clustermetadatapb.ShardRule{
					RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
					LeaderId:   stalePrimaryID,
				},
			},
		},
		Status: &multipoolermanagerdatapb.Status{
			PoolerType:    clustermetadatapb.PoolerType_PRIMARY,
			PostgresReady: false,
		},
	})

	// Replica.
	ps.Set("multipooler-cell1-replica-1", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:       replicaID,
			ShardKey: &clustermetadatapb.ShardKey{Database: "testdb", TableGroup: "default", Shard: "0"},
			Type:     clustermetadatapb.PoolerType_REPLICA,
		},
		IsLastCheckValid: true,
		LastSeen:         timestamppb.Now(),
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_REPLICA,
		},
	})

	generator := NewAnalysisGenerator(ps, nil)
	sa, err := generator.GenerateShardAnalysis(&clustermetadatapb.ShardKey{Database: "testdb", TableGroup: "default", Shard: "0"})
	require.NoError(t, err)
	analysis := findPoolerByName(sa, "replica-1")
	require.NotNil(t, analysis)

	// The shard-level topology primary must point to the new (correct) primary, not the stale one.
	// If it pointed to the stale primary (postgres dead), PrimaryReachable would be false
	// and LeaderIsDeadAnalyzer would falsely trigger a new election.
	require.NotNil(t, sa.HighestShardRule.GetLeaderId())
	assert.Equal(t, "new-primary", sa.HighestShardRule.GetLeaderId().Name,
		"should pick primary with highest PrimaryTerm")
	assert.True(t, sa.LeaderReachable,
		"primary must appear reachable when new primary has postgres running")
}

func TestDetectOtherPrimary(t *testing.T) {
	shardKey := &clustermetadatapb.ShardKey{Database: "testdb", TableGroup: "default", Shard: "0"}

	// leaderName returns the shard leader's name and rule term from the generated
	// analysis. Leadership is the highest known consensus rule, regardless of
	// reachability.
	leaderOf := func(sa *ShardAnalysis) (string, int64) {
		return sa.HighestShardRule.GetLeaderId().GetName(), sa.HighestShardRule.GetRuleNumber().GetCoordinatorTerm()
	}

	t.Run("highest-rule primary wins among two", func(t *testing.T) {
		store := setupMultiplePrimariesStore(t, []primaryConfig{
			{id: "primary-1", primaryTerm: 5, consensusTerm: 10},
			{id: "primary-2", primaryTerm: 6, consensusTerm: 11},
		})
		sa, err := NewAnalysisGenerator(store, nil).GenerateShardAnalysis(shardKey)
		require.NoError(t, err)
		name, term := leaderOf(sa)
		assert.Equal(t, "primary-2", name)
		assert.Equal(t, int64(6), term)
	})

	t.Run("highest-rule primary wins among many (rule, not consensus term)", func(t *testing.T) {
		store := setupMultiplePrimariesStore(t, []primaryConfig{
			{id: "primary-1", primaryTerm: 5, consensusTerm: 11},
			{id: "primary-2", primaryTerm: 4, consensusTerm: 10},
			{id: "primary-3", primaryTerm: 6, consensusTerm: 9},
		})
		sa, err := NewAnalysisGenerator(store, nil).GenerateShardAnalysis(shardKey)
		require.NoError(t, err)
		// primary-3 has the highest rule term (6) even though primary-1 has the
		// highest consensus term (11) — ranking is by rule, not consensus term.
		name, term := leaderOf(sa)
		assert.Equal(t, "primary-3", name)
		assert.Equal(t, int64(6), term)
	})

	t.Run("single primary is the leader", func(t *testing.T) {
		store := setupMultiplePrimariesStore(t, []primaryConfig{
			{id: "primary-1", primaryTerm: 5, consensusTerm: 10},
		})
		sa, err := NewAnalysisGenerator(store, nil).GenerateShardAnalysis(shardKey)
		require.NoError(t, err)
		name, term := leaderOf(sa)
		assert.Equal(t, "primary-1", name)
		assert.Equal(t, int64(5), term)
	})

	t.Run("highest-rule leader wins even when unreachable", func(t *testing.T) {
		// The highest-rule leader (primary-2) is unreachable; we must NOT fall back
		// to the lower-rule reachable primary-1. The leader is still primary-2, but
		// its health is absent/unreachable.
		store := setupMultiplePrimariesStoreWithReachability(t, []primaryConfigWithReachability{
			{primaryConfig: primaryConfig{id: "primary-1", primaryTerm: 5, consensusTerm: 10}, reachable: true},
			{primaryConfig: primaryConfig{id: "primary-2", primaryTerm: 6, consensusTerm: 11}, reachable: false},
		})
		sa, err := NewAnalysisGenerator(store, nil).GenerateShardAnalysis(shardKey)
		require.NoError(t, err)
		name, term := leaderOf(sa)
		assert.Equal(t, "primary-2", name)
		assert.Equal(t, int64(6), term)
		assert.False(t, sa.LeaderPoolerReachable, "the highest-rule leader is unreachable")
	})
}

// Helper types and functions for multiple primaries tests

type primaryConfig struct {
	id            string
	primaryTerm   int64
	consensusTerm int64
}

type primaryConfigWithReachability struct {
	primaryConfig
	reachable bool
}

func setupMultiplePrimariesStore(t *testing.T, primaries []primaryConfig) *store.PoolerStore {
	configs := make([]primaryConfigWithReachability, len(primaries))
	for i, p := range primaries {
		configs[i] = primaryConfigWithReachability{
			primaryConfig: p,
			reachable:     true,
		}
	}
	return setupMultiplePrimariesStoreWithReachability(t, configs)
}

func setupMultiplePrimariesStoreWithReachability(_ *testing.T, primaries []primaryConfigWithReachability) *store.PoolerStore {
	ps := store.NewPoolerStore()

	for _, p := range primaries {
		poolerID := topoclient.ComponentID("multipooler-cell1-" + p.id)
		id := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "cell1",
			Name:      p.id,
		}
		poolerState := &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: id,
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "testdb",
					TableGroup: "default",
					Shard:      "0",
				},
				Type:     clustermetadatapb.PoolerType_PRIMARY,
				Hostname: "localhost",
			},
			IsLastCheckValid: p.reachable,
			IsUpToDate:       true,
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				Id:             id,
				TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: p.consensusTerm},
				CurrentPosition: &clustermetadatapb.PoolerPosition{
					Rule: &clustermetadatapb.ShardRule{
						RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: p.primaryTerm},
						LeaderId:   id,
					},
				},
			},
			Status: &multipoolermanagerdatapb.Status{
				PoolerType: clustermetadatapb.PoolerType_PRIMARY,
			},
		}
		ps.Set(poolerID, poolerState)
	}

	return ps
}

func TestGenerateShardAnalyses_GroupsByShardKey(t *testing.T) {
	ps := store.NewPoolerStore()

	makePooler := func(name, db, tg, shard string, typ clustermetadatapb.PoolerType) *multiorchdatapb.PoolerHealthState {
		return &multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "c1", Name: name},
				ShardKey: &clustermetadatapb.ShardKey{Database: db, TableGroup: tg, Shard: shard},
				Type:     typ,
			},
			Status: &multipoolermanagerdatapb.Status{
				PoolerType: typ,
			},
		}
	}

	// Two poolers in shard db/tg/0 and one in db/tg/1
	ps.Set("multipooler-c1-p0a", makePooler("p0a", "db", "tg", "0", clustermetadatapb.PoolerType_PRIMARY))
	ps.Set("multipooler-c1-p0b", makePooler("p0b", "db", "tg", "0", clustermetadatapb.PoolerType_REPLICA))
	ps.Set("multipooler-c1-p1a", makePooler("p1a", "db", "tg", "1", clustermetadatapb.PoolerType_PRIMARY))

	gen := NewAnalysisGenerator(ps, nil)
	shards := gen.GenerateShardAnalyses()

	require.Len(t, shards, 2, "should produce one ShardAnalysis per shard")

	countByShard := make(map[string]int)
	for _, sa := range shards {
		countByShard[sa.ShardKey.Shard] = len(sa.Analyses)
	}
	assert.Equal(t, 2, countByShard["0"], "shard 0 should have 2 analyses")
	assert.Equal(t, 1, countByShard["1"], "shard 1 should have 1 analysis")
}

func TestGenerateShardAnalysis_ErrorOnMissingShard(t *testing.T) {
	ps := store.NewPoolerStore()
	gen := NewAnalysisGenerator(ps, nil)

	shardKey := &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}
	_, err := gen.GenerateShardAnalysis(shardKey)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "shard not found")
}

func TestGenerateShardAnalysis_ReturnsAllPoolersInShard(t *testing.T) {
	ps := store.NewPoolerStore()

	ps.Set("multipooler-c1-primary", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "c1", Name: "primary"},
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "db",
				TableGroup: "tg",
				Shard:      "0",
			},
			Type: clustermetadatapb.PoolerType_PRIMARY,
		},
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		},
	})
	ps.Set("multipooler-c1-replica", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "c1", Name: "replica"},
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "db",
				TableGroup: "tg",
				Shard:      "0",
			},
			Type: clustermetadatapb.PoolerType_REPLICA,
		},
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_REPLICA,
		},
	})

	gen := NewAnalysisGenerator(ps, nil)
	sa, err := gen.GenerateShardAnalysis(&clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"})
	require.NoError(t, err)
	assert.Len(t, sa.Analyses, 2)
}
