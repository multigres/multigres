// Copyright 2026 Supabase, Inc.
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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// Test for primary with postgres down
func TestIntendedLeaderServing_PostgresDown(t *testing.T) {
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
		ConsensusStatus: primaryConsensusStatus(&clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"}, 1),
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
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_REPLICA,
			ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
				LastReplayLsn: "0/1234",
			},
		},
	}, nil))

	gen := newTestCapture(ps)
	sa, err := gen.shard(&clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "shard1"})
	require.NoError(t, err)
	analysis := findPoolerByName(sa, "replica")
	require.NotNil(t, analysis)

	// HighestTermDiscoveredPrimaryID should be set even when postgres is down
	assert.NotNil(t, sa.HighestIntendedRule().IntendedLeaderID())
	// But PrimaryReachable should be false because postgres is down
	assert.False(t, intendedLeaderServing(sa), "primary should NOT be serving when postgres is down")
}

// TestIntendedLeaderServing_DemotedViaRecruit covers the scenario where a primary is
// demoted via Recruit and restarted as a standby (emergencyDemoteLocked behavior).
// After restart, PoolerType=REPLICA in the health snapshot and primary_term stays > 0
// (only SetPrimary clears it). PrimaryReachable must be false so PrimaryIsDead
// triggers and a new primary can be elected.
func TestIntendedLeaderServing_DemotedViaRecruit(t *testing.T) {
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
			ConsensusStatus: primaryConsensusStatus(formerPrimaryID, 4),
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

		gen := newTestCapture(ps)
		sa, err := gen.shard(shardKey)
		require.NoError(t, err)
		assert.NotNil(t, sa.HighestIntendedRule().IntendedLeaderID(), "demoted primary should still be tracked (primary term > 0)")
		assert.Equal(t, "primary", sa.HighestIntendedRule().IntendedLeaderID().Name)
		assert.False(t, intendedLeaderServing(sa), "demoted primary reporting REPLICA should not be serving")
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
			ConsensusStatus: primaryConsensusStatus(formerPrimaryID, 4),
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

		gen := newTestCapture(ps)
		sa, err := gen.shard(shardKey)
		require.NoError(t, err)
		assert.NotNil(t, sa.HighestIntendedRule().IntendedLeaderID(), "stale-topology former primary should be found via ConsensusStatus")
		assert.Equal(t, "former-primary", sa.HighestIntendedRule().IntendedLeaderID().Name)
		assert.False(t, intendedLeaderServing(sa), "demoted primary reporting REPLICA should not be serving")
	})
}

// TestIntendedLeaderServing_LeaderAbsentFromStore covers the invariant
// that justifies keeping HighestShardRule (consensus identity) and Leader (health)
// as separate fields: a follower can carry a rule naming a leader whose own health
// state is not in the store. The leader is then identified by consensus but has no
// health to attach, so Leader is nil and the leader is treated as unreachable.
func TestIntendedLeaderServing_LeaderAbsentFromStore(t *testing.T) {
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
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id: followerID,
			ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{
				Position: &clustermetadatapb.RulePosition{
					Decision: &clustermetadatapb.ShardRule{
						RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
						LeaderId:   absentLeaderID,
					},
				},
			},
		},
	}, nil))

	gen := newTestCapture(ps)
	sa, err := gen.shard(shardKey)
	require.NoError(t, err)

	require.NotNil(t, sa.HighestIntendedRule().IntendedLeaderID(), "consensus must still identify the leader")
	assert.Equal(t, "absent-leader", sa.HighestIntendedRule().IntendedLeaderID().Name)
	assert.Nil(t, sa.IntendedLeaderHealth(), "no health state exists for the named leader")
	assert.False(t, intendedLeaderServing(sa), "a leader with no health cannot be serving")
}

// TestIntendedLeaderServing_HighestRuleLeader verifies that when two primaries transiently
// coexist (e.g. during failover), the replica's analysis references the one with the higher
// PrimaryTerm — not an arbitrary one from non-deterministic map iteration.
func TestIntendedLeaderServing_HighestRuleLeader(t *testing.T) {
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
		Multipooler: shardConfig(newPrimaryID),
		LastSeen:    timestamppb.Now(),
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id:             newPrimaryID,
			TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 11},
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
					RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 6},
					LeaderId:   newPrimaryID,
				}},
			},
		},
		Status: &multipoolermanagerdatapb.Status{
			PoolerType:    clustermetadatapb.PoolerType_PRIMARY,
			PostgresReady: true,
		},
	}, nil))

	// Stale primary: lower primary term, postgres NOT running (just came back after being killed).
	store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler: shardConfig(stalePrimaryID),
		LastSeen:    timestamppb.Now(),
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			Id:             stalePrimaryID,
			TermRevocation: &clustermetadatapb.TermRevocation{RevokedBelowTerm: 10},
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
					RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
					LeaderId:   stalePrimaryID,
				}},
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
		LastSeen: timestamppb.Now(),
		Status: &multipoolermanagerdatapb.Status{
			PoolerType: clustermetadatapb.PoolerType_REPLICA,
		},
	}, nil))

	capture := newTestCapture(ps)
	sa, err := capture.shard(&clustermetadatapb.ShardKey{Database: "testdb", TableGroup: "default", Shard: "0"})
	require.NoError(t, err)
	analysis := findPoolerByName(sa, "replica-1")
	require.NotNil(t, analysis)

	// The shard-level topology primary must point to the new (correct) primary, not the stale one.
	// If it pointed to the stale primary (postgres dead), PrimaryReachable would be false
	// and LeaderIsDeadAnalyzer would falsely trigger a new election.
	require.NotNil(t, sa.HighestIntendedRule().IntendedLeaderID())
	assert.Equal(t, "new-primary", sa.HighestIntendedRule().IntendedLeaderID().Name,
		"should pick primary with highest PrimaryTerm")
	assert.True(t, intendedLeaderServing(sa),
		"primary must appear reachable when new primary has postgres running")
}
