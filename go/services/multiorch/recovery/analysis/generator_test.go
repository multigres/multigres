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

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// primaryConsensusStatus builds a ConsensusStatus that names id as the leader
// in its current rule with the given coordinator term. This is the minimal
// fixture required for SelfConsensusRole to report ConsensusRoleLeader for a given pooler.
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
	generator := NewAnalysisGenerator(store.NewTestCache(t), nil)

	analyses := flattenShardAnalyses(generator.GenerateShardAnalyses())

	assert.Empty(t, analyses, "should return empty slice for empty store")
}

func TestAnalysisGenerator_GenerateShardAnalyses_SinglePrimary(t *testing.T) {
	ps := store.NewTestCache(t)

	// Add a single primary pooler
	primaryID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      "cell1",
		Name:      "primary-1",
	}

	primary := &multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id: primaryID,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "testtg",
				Shard:      "0",
			},
			Type: clustermetadatapb.PoolerType_PRIMARY,
		},
		IsLastCheckValid: true,
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
	store.SeedCache(t, ps, store.NewPooler(primary, nil))

	generator := NewAnalysisGenerator(ps, nil)
	analyses := flattenShardAnalyses(generator.GenerateShardAnalyses())

	require.Len(t, analyses, 1, "should generate one analysis")

	analysis := analyses[0]
	assert.Equal(t, "testdb", analysis.Health().GetMultipooler().GetShardKey().GetDatabase())
	assert.Equal(t, "testtg", analysis.Health().GetMultipooler().GetShardKey().GetTableGroup())
	assert.Equal(t, "0", analysis.Health().GetMultipooler().GetShardKey().GetShard())
	assert.True(t, commonconsensus.SelfConsensusRole(analysis.Health().GetConsensusStatus()) == commonconsensus.ConsensusRoleLeader)
	assert.True(t, analysis.Health().IsLastCheckValid)
}

func TestAnalysisGenerator_GenerateShardAnalyses_PrimaryWithReplicas(t *testing.T) {
	ps := store.NewTestCache(t)

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
		Multipooler: &clustermetadatapb.Multipooler{
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
	store.SeedCache(t, ps, store.NewPooler(primary, nil))

	// Add replica 1 (replicating)
	replica1 := &multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id: replica1ID,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "testtg",
				Shard:      "0",
			},
			Type: clustermetadatapb.PoolerType_REPLICA,
		},
		IsLastCheckValid: true,
		LastSeen:         timestamppb.Now(),
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_REPLICA,
			ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
				IsWalReplayPaused: false,
				Lag:               durationpb.New(100 * time.Millisecond), // 100ms lag
			},
		},
	}
	store.SeedCache(t, ps, store.NewPooler(replica1, nil))

	// Add replica 2 (lagging)
	replica2 := &multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id: replica2ID,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "testtg",
				Shard:      "0",
			},
			Type: clustermetadatapb.PoolerType_REPLICA,
		},
		IsLastCheckValid: true,
		LastSeen:         timestamppb.Now(),
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_REPLICA,
			ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
				IsWalReplayPaused: false,
				Lag:               durationpb.New(15 * time.Second), // 15s lag (> 10s threshold)
			},
		},
	}
	store.SeedCache(t, ps, store.NewPooler(replica2, nil))

	generator := NewAnalysisGenerator(ps, nil)
	analyses := flattenShardAnalyses(generator.GenerateShardAnalyses())

	require.Len(t, analyses, 3, "should generate three analyses")

	// Find the primary analysis
	var primaryAnalysis *store.Pooler
	for _, a := range analyses {
		if commonconsensus.SelfConsensusRole(a.Health().GetConsensusStatus()) == commonconsensus.ConsensusRoleLeader {
			primaryAnalysis = a
			break
		}
	}

	require.NotNil(t, primaryAnalysis, "should find primary analysis")
	assert.True(t, commonconsensus.SelfConsensusRole(primaryAnalysis.Health().GetConsensusStatus()) == commonconsensus.ConsensusRoleLeader)
}

func TestAnalysisGenerator_GenerateShardAnalyses_Replica(t *testing.T) {
	ps := store.NewTestCache(t)

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
		Multipooler: &clustermetadatapb.Multipooler{
			Id: primaryID,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "testtg",
				Shard:      "0",
			},
			Type: clustermetadatapb.PoolerType_PRIMARY,
		},
		IsLastCheckValid: true,
		LastSeen:         timestamppb.Now(),
		ConsensusStatus:  primaryConsensusStatus(primaryID, 1),
		Status: &multipoolermanagerdatapb.Status{
			PoolerType:    clustermetadatapb.PoolerType_PRIMARY,
			PostgresReady: true,
		},
	}
	store.SeedCache(t, ps, store.NewPooler(primary, nil))

	// Add replica
	replica := &multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id: replicaID,
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "testdb",
				TableGroup: "testtg",
				Shard:      "0",
			},
			Type: clustermetadatapb.PoolerType_REPLICA,
		},
		IsLastCheckValid: true,
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
	store.SeedCache(t, ps, store.NewPooler(replica, nil))

	generator := NewAnalysisGenerator(ps, nil)
	shards := generator.GenerateShardAnalyses()

	require.Len(t, shards, 1, "should generate one shard analysis")
	sa := shards[0]
	require.Len(t, sa.Analyses, 2, "should generate two pooler analyses")

	// Find the replica analysis
	replicaAnalysis := sa.Replicas()
	require.Len(t, replicaAnalysis, 1, "should find one replica")
	assert.NotEqual(t, commonconsensus.ConsensusRoleLeader, commonconsensus.SelfConsensusRole(replicaAnalysis[0].Health().GetConsensusStatus()))

	// Primary health is now a shard-level field
	assert.NotNil(t, sa.HighestShardRule.GetLeaderId(), "should have topology primary ID populated")
}

func TestAnalysisGenerator_GenerateShardAnalyses_MultipleTableGroups(t *testing.T) {
	ps := store.NewTestCache(t)

	// Add poolers from two different table groups
	tg1Primary := &multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
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
		LastSeen:         timestamppb.Now(),
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		},
	}
	store.SeedCache(t, ps, store.NewPooler(tg1Primary, nil))

	tg2Primary := &multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
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
		LastSeen:         timestamppb.Now(),
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_PRIMARY,
		},
	}
	store.SeedCache(t, ps, store.NewPooler(tg2Primary, nil))

	generator := NewAnalysisGenerator(ps, nil)
	analyses := flattenShardAnalyses(generator.GenerateShardAnalyses())

	require.Len(t, analyses, 2, "should generate two analyses")

	// Verify both table groups are present
	tableGroups := make(map[string]bool)
	for _, a := range analyses {
		tableGroups[a.Health().GetMultipooler().GetShardKey().GetTableGroup()] = true
	}

	assert.True(t, tableGroups["tg1"])
	assert.True(t, tableGroups["tg2"])
}

// Task 6: Test for skipping nil entries
func TestGenerateShardAnalyses_SkipsNilEntries(t *testing.T) {
	ps := store.NewTestCache(t)

	// Add a nil entry
	store.SeedCache(t, ps, nil)

	// Add a valid pooler
	store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
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
	}, nil))

	gen := NewAnalysisGenerator(ps, nil)
	analyses := flattenShardAnalyses(gen.GenerateShardAnalyses())

	// Should only generate one analysis for the valid pooler, skipping the nil entry
	assert.Len(t, analyses, 1)
	assert.Equal(t, "db1", analyses[0].Health().GetMultipooler().GetShardKey().GetDatabase())
}

// Task 7: Test for no primary in shard
func TestPopulatePrimaryInfo_NoPrimaryInShard(t *testing.T) {
	ps := store.NewTestCache(t)

	store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
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
	}, nil))

	gen := NewAnalysisGenerator(ps, nil)
	sa, err := gen.GenerateShardAnalysis(&clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "shard1"})
	require.NoError(t, err)

	// When no primary exists in the shard, topology primary fields should be nil.
	assert.Nil(t, sa.HighestShardRule.GetLeaderId())
}

// Task 7: Test for primary with postgres down
func TestPopulatePrimaryInfo_PrimaryPostgresDown(t *testing.T) {
	ps := store.NewTestCache(t)

	// Primary with PostgresReady: false (postgres is down)
	store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
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
	}, nil))

	store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
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
	}, nil))

	gen := NewAnalysisGenerator(ps, nil)
	sa, err := gen.GenerateShardAnalysis(&clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "shard1"})
	require.NoError(t, err)
	analysis := findPoolerByName(sa, "replica")
	require.NotNil(t, analysis)

	// HighestTermDiscoveredPrimaryID should be set even when postgres is down
	assert.NotNil(t, sa.HighestShardRule.GetLeaderId())
	// But PrimaryReachable should be false because postgres is down
	assert.False(t, leaderServing(sa), "primary should NOT be serving when postgres is down")
}

// TestPopulatePrimaryInfo_DemotedViaRecruit covers the scenario where a primary is
// demoted via Recruit and restarted as a standby (emergencyDemoteLocked behavior).
// After restart, PoolerType=REPLICA in the health snapshot and primary_term stays > 0
// (only SetPrimary clears it). PrimaryReachable must be false so PrimaryIsDead
// triggers and a new primary can be elected.
func TestPopulatePrimaryInfo_DemotedViaRecruit(t *testing.T) {
	replica := &multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
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
		ps := store.NewTestCache(t)
		formerPrimaryID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "cell1",
			Name:      "primary",
		}
		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{
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
		}, nil))
		store.SeedCache(t, ps, store.NewPooler(replica, nil))

		gen := NewAnalysisGenerator(ps, nil)
		sa, err := gen.GenerateShardAnalysis(shardKey)
		require.NoError(t, err)
		assert.NotNil(t, sa.HighestShardRule.GetLeaderId(), "demoted primary should still be tracked (primary term > 0)")
		assert.Equal(t, "primary", sa.HighestShardRule.GetLeaderId().Name)
		assert.False(t, leaderServing(sa), "demoted primary reporting REPLICA should not be serving")
	})

	t.Run("topology type REPLICA, PoolerType REPLICA, primary term > 0 via ConsensusStatus (stale etcd)", func(t *testing.T) {
		// Simulates etcd being stopped before the promotion: topology still shows this node
		// as REPLICA (initial assignment), but it was promoted later (term=4) and then
		// revoked. HighestTermDiscoveredPrimaryID must still be found via ConsensusStatus.
		ps := store.NewTestCache(t)
		formerPrimaryID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "cell1",
			Name:      "former-primary",
		}
		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{
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
		}, nil))
		store.SeedCache(t, ps, store.NewPooler(replica, nil))

		gen := NewAnalysisGenerator(ps, nil)
		sa, err := gen.GenerateShardAnalysis(shardKey)
		require.NoError(t, err)
		assert.NotNil(t, sa.HighestShardRule.GetLeaderId(), "stale-topology former primary should be found via ConsensusStatus")
		assert.Equal(t, "former-primary", sa.HighestShardRule.GetLeaderId().Name)
		assert.False(t, leaderServing(sa), "demoted primary reporting REPLICA should not be serving")
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

	ps := store.NewTestCache(t)
	// Only the follower is in the store. Its replication primary rule names the
	// leader at term 5, so consensus identifies a leader the store has no health for.
	store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
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
	}, nil))

	gen := NewAnalysisGenerator(ps, nil)
	sa, err := gen.GenerateShardAnalysis(shardKey)
	require.NoError(t, err)

	require.NotNil(t, sa.HighestShardRule.GetLeaderId(), "consensus must still identify the leader")
	assert.Equal(t, "absent-leader", sa.HighestShardRule.GetLeaderId().Name)
	assert.Nil(t, sa.Leader, "no health state exists for the named leader")
	assert.False(t, leaderServing(sa), "a leader with no health cannot be serving")
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

	ps := store.NewTestCache(t)
	// Stale leader: still self-claims leadership at the old term (term 5) and is
	// reachable and ready. Under the old self-claim-only logic this would be
	// picked as the leader.
	store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler:      &clustermetadatapb.Multipooler{Id: staleLeaderID, ShardKey: shardKey, Type: clustermetadatapb.PoolerType_PRIMARY},
		IsLastCheckValid: true,
		ConsensusStatus:  primaryConsensusStatus(staleLeaderID, 5),
		Status:           &multipoolermanagerdatapb.Status{PoolerType: clustermetadatapb.PoolerType_PRIMARY, PostgresReady: true, PostgresRunning: true},
	}, nil))
	// Follower: already replicating from the new leader at the higher term (6).
	// The new leader itself has not reported health yet (absent from the store).
	store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler:      &clustermetadatapb.Multipooler{Id: followerID, ShardKey: shardKey, Type: clustermetadatapb.PoolerType_REPLICA},
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
	}, nil))

	gen := NewAnalysisGenerator(ps, nil)
	sa, err := gen.GenerateShardAnalysis(shardKey)
	require.NoError(t, err)

	require.NotNil(t, sa.HighestShardRule.GetLeaderId())
	assert.Equal(t, "new-leader", sa.HighestShardRule.GetLeaderId().Name,
		"the leader named by the follower's higher-term rule must win over the stale self-claiming leader")
	assert.Equal(t, int64(6), sa.HighestShardRule.GetRuleNumber().GetCoordinatorTerm())
	assert.Nil(t, sa.Leader, "the new leader has no health state yet, so Leader is nil")
}

// TestPopulatePrimaryInfo_PicksHighestPrimaryTerm verifies that when two primaries transiently
// coexist (e.g. during failover), the replica's analysis references the one with the higher
// PrimaryTerm — not an arbitrary one from non-deterministic map iteration.
func TestPopulatePrimaryInfo_PicksHighestPrimaryTerm(t *testing.T) {
	ps := store.NewTestCache(t)

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

	shardConfig := func(id *clustermetadatapb.ID) *clustermetadatapb.Multipooler {
		return &clustermetadatapb.Multipooler{
			Id:       id,
			ShardKey: &clustermetadatapb.ShardKey{Database: "testdb", TableGroup: "default", Shard: "0"},
			Type:     clustermetadatapb.PoolerType_PRIMARY,
		}
	}

	// New (correct) primary: higher PrimaryTerm, postgres running.
	store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler:      shardConfig(newPrimaryID),
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
	}, nil))

	// Stale primary: lower primary term, postgres NOT running (just came back after being killed).
	store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler:      shardConfig(stalePrimaryID),
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
	}, nil))

	// Replica.
	store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id:       replicaID,
			ShardKey: &clustermetadatapb.ShardKey{Database: "testdb", TableGroup: "default", Shard: "0"},
			Type:     clustermetadatapb.PoolerType_REPLICA,
		},
		IsLastCheckValid: true,
		LastSeen:         timestamppb.Now(),
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_REPLICA,
		},
	}, nil))

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
	assert.True(t, leaderServing(sa),
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

func setupMultiplePrimariesStore(t *testing.T, primaries []primaryConfig) *store.PoolerCache {
	configs := make([]primaryConfigWithReachability, len(primaries))
	for i, p := range primaries {
		configs[i] = primaryConfigWithReachability{
			primaryConfig: p,
			reachable:     true,
		}
	}
	return setupMultiplePrimariesStoreWithReachability(t, configs)
}

func setupMultiplePrimariesStoreWithReachability(t *testing.T, primaries []primaryConfigWithReachability) *store.PoolerCache {
	ps := store.NewTestCache(t)

	for _, p := range primaries {
		id := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "cell1",
			Name:      p.id,
		}
		poolerState := &multiorchdatapb.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{
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
		store.SeedCache(t, ps, store.NewPooler(poolerState, nil))
	}

	return ps
}

func TestGenerateShardAnalyses_GroupsByShardKey(t *testing.T) {
	ps := store.NewTestCache(t)

	makePooler := func(name, db, tg, shard string, typ clustermetadatapb.PoolerType) *store.Pooler {
		return store.NewPooler(&multiorchdatapb.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{
				Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "c1", Name: name},
				ShardKey: &clustermetadatapb.ShardKey{Database: db, TableGroup: tg, Shard: shard},
				Type:     typ,
			},
			Status: &multipoolermanagerdatapb.Status{
				PoolerType: typ,
			},
		}, nil)
	}

	// Two poolers in shard db/tg/0 and one in db/tg/1
	store.SeedCache(t, ps, makePooler("p0a", "db", "tg", "0", clustermetadatapb.PoolerType_PRIMARY))
	store.SeedCache(t, ps, makePooler("p0b", "db", "tg", "0", clustermetadatapb.PoolerType_REPLICA))
	store.SeedCache(t, ps, makePooler("p1a", "db", "tg", "1", clustermetadatapb.PoolerType_PRIMARY))

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
	ps := store.NewTestCache(t)
	gen := NewAnalysisGenerator(ps, nil)

	shardKey := &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}
	_, err := gen.GenerateShardAnalysis(shardKey)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "shard not found")
}

func TestGenerateShardAnalysis_ReturnsAllPoolersInShard(t *testing.T) {
	ps := store.NewTestCache(t)

	store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
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
	}, nil))
	store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
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
	}, nil))

	gen := NewAnalysisGenerator(ps, nil)
	sa, err := gen.GenerateShardAnalysis(&clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"})
	require.NoError(t, err)
	assert.Len(t, sa.Analyses, 2)
}
