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

// Tests for the call-time predicate helpers on ShardAnalysis (leaderServing,
// leaderPostgresReady, replicasStreamingFromLeader). These derive verdicts
// from an already-built ShardAnalysis; the analyzers consume them, so they
// live alongside the helpers rather than with the generator that only
// assembles the ShardAnalysis.

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

func TestLeaderServing(t *testing.T) {
	t.Run("sets PrimaryPoolerReachable and PrimaryPostgresReady correctly", func(t *testing.T) {
		ps := store.NewTestCache(t)

		// Primary with pooler reachable and postgres running
		respondedAt := time.Now().Add(-3 * time.Second)
		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
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
			LastSeen:              timestamppb.Now(),
			LastPostgresReadyTime: timestamppb.New(respondedAt),
			Status: &multipoolermanagerdatapb.Status{
				PoolerType:    clustermetadatapb.PoolerType_PRIMARY,
				PostgresReady: true,
			},
		}, nil))

		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
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
		}, nil))

		gen := NewAnalysisGenerator(ps, nil)
		sa, err := gen.GenerateShardAnalysis(&clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "shard1"})
		require.NoError(t, err)

		assert.True(t, leaderPostgresReady(sa))
		assert.True(t, leaderServing(sa))
	})

	t.Run("sets PrimaryPoolerReachable false when pooler unreachable", func(t *testing.T) {
		ps := store.NewTestCache(t)

		// Primary with pooler unreachable
		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
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
		}, nil))

		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
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
		}, nil))

		gen := NewAnalysisGenerator(ps, nil)
		sa, err := gen.GenerateShardAnalysis(&clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "shard1"})
		require.NoError(t, err)

		assert.False(t, leaderPostgresReady(sa))
		assert.False(t, leaderServing(sa))
	})
}

func TestReplicasStreamingFromLeader(t *testing.T) {
	t.Run("returns true when all replicas connected", func(t *testing.T) {
		ps := store.NewTestCache(t)

		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
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
		}, nil))

		now := time.Now()

		// Replica 1 - connected to primary
		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
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
			LastSeen:                timestamppb.New(time.Now()),
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
		}, nil))

		// Replica 2 - also connected to primary
		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
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
			LastSeen:                timestamppb.New(time.Now()),
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
		}, nil))

		gen := NewAnalysisGenerator(ps, nil)
		sa, err := gen.GenerateShardAnalysis(&clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "shard1"})
		require.NoError(t, err)

		assert.True(t, replicasStreamingFromLeader(sa), "should be true when all replicas are connected")
	})

	t.Run("returns false when one replica disconnected", func(t *testing.T) {
		ps := store.NewTestCache(t)

		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
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
		}, nil))

		// Replica 1 - connected
		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
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
			LastSeen:                timestamppb.New(time.Now()),
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
		}, nil))

		// Replica 2 - disconnected (no PrimaryConnInfo)
		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
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
			LastSeen:                timestamppb.New(time.Now()),
			StreamSnapshotsReceived: 1,
			Status: &multipoolermanagerdatapb.Status{
				PoolerType:        clustermetadatapb.PoolerType_REPLICA,
				ReplicationStatus: &multipoolermanagerdatapb.StandbyReplicationStatus{
					// No PrimaryConnInfo - replica is disconnected
				},
			},
		}, nil))

		gen := NewAnalysisGenerator(ps, nil)
		sa, err := gen.GenerateShardAnalysis(&clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "shard1"})
		require.NoError(t, err)

		assert.False(t, replicasStreamingFromLeader(sa), "should be false when any replica is disconnected")
	})

	t.Run("returns false when replica unreachable", func(t *testing.T) {
		ps := store.NewTestCache(t)

		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
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
		}, nil))

		// Replica was previously healthy but is now unreachable (stream disconnected).
		// StreamSnapshotsReceived > 0 distinguishes this from a brand-new pooler.
		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
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
		}, nil))

		gen := NewAnalysisGenerator(ps, nil)
		sa, err := gen.GenerateShardAnalysis(&clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "shard1"})
		require.NoError(t, err)

		assert.False(t, replicasStreamingFromLeader(sa), "should be false when replica is unreachable")
	})

	t.Run("returns false when no replicas exist", func(t *testing.T) {
		ps := store.NewTestCache(t)

		// Only primary, no replicas
		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
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
			LastSeen:         timestamppb.New(time.Now()),
			Status: &multipoolermanagerdatapb.Status{
				PoolerType:    clustermetadatapb.PoolerType_PRIMARY,
				PostgresReady: true,
			},
		}, nil))

		gen := NewAnalysisGenerator(ps, nil)
		sa, err := gen.GenerateShardAnalysis(&clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "shard1"})
		require.NoError(t, err)

		// Primary-only shard: ReplicasConnectedToLeader should be false (no replicas)
		assert.False(t, replicasStreamingFromLeader(sa))
	})

	t.Run("returns false when replica pointing to wrong primary", func(t *testing.T) {
		ps := store.NewTestCache(t)

		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
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
		}, nil))

		// Replica pointing to different host
		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
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
			LastSeen:                timestamppb.New(time.Now()),
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
		}, nil))

		gen := NewAnalysisGenerator(ps, nil)
		sa, err := gen.GenerateShardAnalysis(&clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "shard1"})
		require.NoError(t, err)

		assert.False(t, replicasStreamingFromLeader(sa), "should be false when replica points to wrong primary")
	})

	t.Run("returns false when WAL receiver is not streaming", func(t *testing.T) {
		ps := store.NewTestCache(t)

		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
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
		}, nil))

		for _, status := range []string{"", "starting", "waiting", "stopping"} {
			store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
				MultiPooler: &clustermetadatapb.MultiPooler{
					Id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica"},
					ShardKey: &clustermetadatapb.ShardKey{
						Database:   "db1",
						TableGroup: "tg1",
						Shard:      "shard1",
					},
				},
				IsLastCheckValid:        true,
				LastSeen:                timestamppb.New(time.Now()),
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
			}, nil))

			gen := NewAnalysisGenerator(ps, nil)
			analysis, err := gen.GenerateAnalysisForPooler(topoclient.ComponentID("multipooler-cell1-replica"))
			require.NoError(t, err)
			assert.False(t, replicasStreamingFromLeader(analysis), "should be false when wal_receiver_status=%q", status)
		}
	})

	t.Run("returns false when last_msg_receive_time is stale (default threshold)", func(t *testing.T) {
		// No WalReceiverStatusInterval supplied — falls back to defaultReplicationHeartbeatStalenessThreshold.
		ps := store.NewTestCache(t)

		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
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
		}, nil))

		fixedNow := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
		staleTime := fixedNow.Add(-(defaultReplicationHeartbeatStalenessThreshold + time.Second))

		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica"},
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "db1",
					TableGroup: "tg1",
					Shard:      "shard1",
				},
			},
			IsLastCheckValid:        true,
			LastSeen:                timestamppb.New(time.Now()),
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
		}, nil))

		gen := NewAnalysisGenerator(ps, nil)
		gen.now = func() time.Time { return fixedNow }
		analysis, err := gen.GenerateAnalysisForPooler(topoclient.ComponentID("multipooler-cell1-replica"))
		require.NoError(t, err)

		assert.False(t, replicasStreamingFromLeader(analysis), "should be false when last_msg_receive_time is stale")
	})

	t.Run("returns false when last_msg_receive_time is stale (dynamic threshold)", func(t *testing.T) {
		// WalReceiverStatusInterval supplied — threshold is multiplier × interval.
		ps := store.NewTestCache(t)

		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
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
		}, nil))

		fixedNow := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
		interval := 5 * time.Second
		dynamicThreshold := replicationHeartbeatStalenessMultiplier * interval // 15s
		staleTime := fixedNow.Add(-(dynamicThreshold + time.Second))

		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica"},
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "db1",
					TableGroup: "tg1",
					Shard:      "shard1",
				},
			},
			IsLastCheckValid:        true,
			LastSeen:                timestamppb.New(time.Now()),
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
		}, nil))

		gen := NewAnalysisGenerator(ps, nil)
		gen.now = func() time.Time { return fixedNow }
		analysis, err := gen.GenerateAnalysisForPooler(topoclient.ComponentID("multipooler-cell1-replica"))
		require.NoError(t, err)

		assert.False(t, replicasStreamingFromLeader(analysis), "should be false when last_msg_receive_time exceeds dynamic threshold")
	})

	t.Run("returns false when last_msg_receive_time exceeds wal_receiver_timeout", func(t *testing.T) {
		// Even if the delay is below the staleness threshold, if it exceeds the
		// WAL receiver timeout the connection is effectively dead.
		ps := store.NewTestCache(t)

		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
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
		}, nil))

		fixedNow := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
		walReceiverTimeout := 60 * time.Second
		// last_msg_receive_time is 61s ago — exceeds wal_receiver_timeout (60s) but
		// is still within the staleness threshold (3×10s = 30s would be fine, but
		// the hard deadline fires first).
		lastMsgReceiveTime := fixedNow.Add(-(walReceiverTimeout + time.Second))

		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica"},
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "db1",
					TableGroup: "tg1",
					Shard:      "shard1",
				},
			},
			IsLastCheckValid:        true,
			LastSeen:                timestamppb.New(time.Now()),
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
		}, nil))

		gen := NewAnalysisGenerator(ps, nil)
		gen.now = func() time.Time { return fixedNow }
		analysis, err := gen.GenerateAnalysisForPooler(topoclient.ComponentID("multipooler-cell1-replica"))
		require.NoError(t, err)

		assert.False(t, replicasStreamingFromLeader(analysis), "should be false when delay exceeds wal_receiver_timeout")
	})

	t.Run("returns true when last_msg_receive_time is nil", func(t *testing.T) {
		// Backward compatibility: replicas that don't report last_msg_receive_time
		// (e.g. running an older version) should still be considered connected if
		// the WAL receiver is streaming.
		ps := store.NewTestCache(t)

		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
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
		}, nil))

		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica"},
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   "db1",
					TableGroup: "tg1",
					Shard:      "shard1",
				},
			},
			IsLastCheckValid:        true,
			LastSeen:                timestamppb.New(time.Now()),
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
		}, nil))

		gen := NewAnalysisGenerator(ps, nil)
		analysis, err := gen.GenerateAnalysisForPooler(topoclient.ComponentID("multipooler-cell1-replica"))
		require.NoError(t, err)

		assert.True(t, replicasStreamingFromLeader(analysis), "should be true when last_msg_receive_time is nil")
	})

	t.Run("new pooler with no health data does not count against connected replicas", func(t *testing.T) {
		// Regression test for the startup race: PoolerWatcher adds a pooler to the
		// store before its health stream delivers the first snapshot
		// (StreamSnapshotsReceived==0). If the recovery tick fires in that window,
		// the new pooler must not be counted as a "disconnected replica" —
		// otherwise it would make allReplicasConnectedToLeader return false and
		// disable the LeaderIsDeadAnalyzer suppression window, risking a
		// premature failover.
		ps := store.NewTestCache(t)

		now := time.Now()

		// Leader is transiently unreachable (pooler down, postgres still running).
		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
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
		}, nil))

		// Existing replica: healthy and actively streaming WAL from the leader.
		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica1"},
				ShardKey: &clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "shard1"},
			},
			IsLastCheckValid:        true,
			LastSeen:                timestamppb.New(time.Now()),
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
		}, nil))

		// New pooler just added by PoolerWatcher: no health data yet
		// (StreamSnapshotsReceived==0, IsLastCheckValid==false).
		store.SeedCache(t, ps, store.NewPooler(&multiorchdatapb.PoolerHealthState{
			MultiPooler: &clustermetadatapb.MultiPooler{
				Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell2", Name: "replica2"},
				ShardKey: &clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "shard1"},
				Type:     clustermetadatapb.PoolerType_REPLICA,
			},
			// IsLastCheckValid and StreamSnapshotsReceived are both zero — brand new pooler.
		}, nil))

		gen := NewAnalysisGenerator(ps, nil)
		sa, err := gen.GenerateShardAnalysis(&clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "shard1"})
		require.NoError(t, err)

		// replica1 is connected; replica2 has never reported health and must not
		// count against the connected-replicas check.
		assert.True(t, replicasStreamingFromLeader(sa),
			"new pooler with StreamSnapshotsReceived==0 must not count as a disconnected replica")
	})
}
