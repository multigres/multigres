// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/topoclient"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

func TestPoolerStore_FindPoolersInShard(t *testing.T) {
	poolerStore := NewPoolerStore()

	// Add poolers to different shards
	poolerStore.Set("pooler1", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "db1",
				TableGroup: "tg1",
				Shard:      "0",
			},
		},
	})
	poolerStore.Set("pooler2", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{Cell: "cell1", Name: "pooler2"},
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "db1",
				TableGroup: "tg1",
				Shard:      "0",
			},
		},
	})
	poolerStore.Set("pooler3", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{Cell: "cell2", Name: "pooler3"},
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "db1",
				TableGroup: "tg1",
				Shard:      "1",
			}, // different shard
		},
	})

	t.Run("finds poolers in shard", func(t *testing.T) {
		shardKey := &clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "0"}
		poolers := poolerStore.FindPoolersInShard(shardKey)

		assert.Len(t, poolers, 2)
		names := []string{poolers[0].MultiPooler.Id.Name, poolers[1].MultiPooler.Id.Name}
		assert.Contains(t, names, "pooler1")
		assert.Contains(t, names, "pooler2")
	})

	t.Run("returns empty for non-existent shard", func(t *testing.T) {
		shardKey := &clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "999"}
		poolers := poolerStore.FindPoolersInShard(shardKey)

		assert.Empty(t, poolers)
	})

	t.Run("skips nil entries", func(t *testing.T) {
		poolerStore.Set("nil-pooler", nil)
		poolerStore.Set("nil-multipooler", &multiorchdatapb.PoolerHealthState{MultiPooler: nil})

		shardKey := &clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "0"}
		poolers := poolerStore.FindPoolersInShard(shardKey)

		// Should still find the 2 valid poolers
		assert.Len(t, poolers, 2)
	})
}

func TestPoolerStore_FindPoolerByID(t *testing.T) {
	poolerStore := NewPoolerStore()

	poolerStore.Set("pooler1", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"},
		},
	})
	poolerStore.Set("pooler2", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{Cell: "cell2", Name: "pooler2"},
		},
	})

	t.Run("finds pooler by ID", func(t *testing.T) {
		id := &clustermetadatapb.ID{Cell: "cell1", Name: "pooler1"}
		found, err := poolerStore.FindPoolerByID(id)

		require.NoError(t, err)
		assert.Equal(t, "pooler1", found.MultiPooler.Id.Name)
		assert.Equal(t, "cell1", found.MultiPooler.Id.Cell)
	})

	t.Run("returns error for non-existent pooler", func(t *testing.T) {
		id := &clustermetadatapb.ID{Cell: "cell1", Name: "non-existent"}
		found, err := poolerStore.FindPoolerByID(id)

		assert.Error(t, err)
		assert.Nil(t, found)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("matches both cell and name", func(t *testing.T) {
		// Same name, different cell - should not match
		id := &clustermetadatapb.ID{Cell: "cell2", Name: "pooler1"}
		found, err := poolerStore.FindPoolerByID(id)

		assert.Error(t, err)
		assert.Nil(t, found)
	})
}

func TestPoolerStore_FindShardMembers(t *testing.T) {
	shardKey := &clustermetadatapb.ShardKey{Database: "testdb", TableGroup: "default", Shard: "0"}
	primaryID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"}
	replicaID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica"}

	// set stores a pooler in shardKey with the given consensus status (cs may be nil).
	set := func(ps *PoolerStore, id *clustermetadatapb.ID, cs *clustermetadatapb.ConsensusStatus) {
		ps.Set(topoclient.ComponentID(id.Name), &multiorchdatapb.PoolerHealthState{
			MultiPooler:     &clustermetadatapb.MultiPooler{Id: id, ShardKey: shardKey, Type: clustermetadatapb.PoolerType_REPLICA},
			ConsensusStatus: cs,
		})
	}

	t.Run("names the leader from its own current-position rule", func(t *testing.T) {
		ps := NewPoolerStore()
		set(ps, primaryID, leaderConsensusStatus(primaryID, 1))
		set(ps, replicaID, nil)

		shard := ps.FindShardMembers(shardKey)

		assert.Len(t, shard.Poolers, 2)
		require.NotNil(t, shard.Leader)
		assert.Equal(t, "primary", shard.Leader.MultiPooler.Id.Name)
		assert.True(t, proto.Equal(primaryID, shard.HighestKnownRule.GetLeaderId()))
	})

	t.Run("names the leader via a follower's replication rule when its own snapshot is absent", func(t *testing.T) {
		// The leader carries no consensus status of its own, but a follower's
		// replication rule still names it — leadership is found from that rule.
		ps := NewPoolerStore()
		set(ps, primaryID, nil)
		set(ps, replicaID, replicationStatus(replicaID, primaryID, 1))

		shard := ps.FindShardMembers(shardKey)

		require.NotNil(t, shard.Leader)
		assert.Equal(t, "primary", shard.Leader.MultiPooler.Id.Name)
	})

	t.Run("no rule known yields a nil leader and rule", func(t *testing.T) {
		ps := NewPoolerStore()
		set(ps, primaryID, nil)
		set(ps, replicaID, nil)

		shard := ps.FindShardMembers(shardKey)

		assert.Len(t, shard.Poolers, 2)
		assert.Nil(t, shard.HighestKnownRule)
		assert.Nil(t, shard.Leader)
	})

	t.Run("leader named by a rule but absent from the store yields a nil leader", func(t *testing.T) {
		// A follower names primary, but primary itself is not tracked. The rule is
		// still reported; Leader is nil so callers can detect the gap.
		ps := NewPoolerStore()
		set(ps, replicaID, replicationStatus(replicaID, primaryID, 1))

		shard := ps.FindShardMembers(shardKey)

		require.NotNil(t, shard.HighestKnownRule)
		assert.True(t, proto.Equal(primaryID, shard.HighestKnownRule.GetLeaderId()))
		assert.Nil(t, shard.Leader)
	})

	t.Run("REGRESSION: picks the leader named by a replica's rule over a stale self-claiming primary", func(t *testing.T) {
		// Failover window: the old leader (term 5) has not been demoted yet, so it
		// still self-claims leadership. The new leader (term 6) has not yet published
		// its own snapshot — its leadership is known only from a replica that already
		// replicates from it. Ranking by rule number names exactly one leader (the
		// term-6 node), never the stale self-claim, with no multiple-primary
		// ambiguity.
		staleID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "stale-leader"}
		newID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "new-leader"}

		ps := NewPoolerStore()
		set(ps, staleID, leaderConsensusStatus(staleID, 5)) // stale self-claim
		set(ps, newID, nil)                                 // new leader: no snapshot yet
		set(ps, replicaID, replicationStatus(replicaID, newID, 6))

		shard := ps.FindShardMembers(shardKey)

		require.NotNil(t, shard.Leader)
		assert.Equal(t, "new-leader", shard.Leader.MultiPooler.Id.Name,
			"the leader named by the replica's higher-term rule must be selected, not the stale self-claim")
		assert.Equal(t, int64(6), shard.HighestKnownRule.GetRuleNumber().GetCoordinatorTerm())
	})
}

// leaderConsensusStatus builds a consensus status for a pooler that names
// itself as the leader at the given coordinator term.
func leaderConsensusStatus(id *clustermetadatapb.ID, term int64) *clustermetadatapb.ConsensusStatus {
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

// replicationStatus builds a consensus status for a follower whose replication
// primary names leader at the given coordinator term.
func replicationStatus(self, leader *clustermetadatapb.ID, term int64) *clustermetadatapb.ConsensusStatus {
	return &clustermetadatapb.ConsensusStatus{
		Id: self,
		ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{
			Rule: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term},
				LeaderId:   leader,
			},
		},
	}
}

// TestPoolerStore_DoUpdateRange verifies that DoUpdateRange atomically resets fields
// on qualifying poolers while leaving others unchanged — mirroring the
// queuePoolersHealthCheck use case.
func TestPoolerStore_DoUpdateRange(t *testing.T) {
	store := NewPoolerStore()

	// pooler1: IsUpToDate=true — should be reset to false
	store.Set("pooler1", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"},
		},
		IsUpToDate: true,
	})
	// pooler2: IsUpToDate=false — should remain false and not be written back
	store.Set("pooler2", &multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler2"},
		},
		IsUpToDate: false,
	})

	writeCount := 0
	store.DoUpdateRange(func(key topoclient.ComponentID, value *multiorchdatapb.PoolerHealthState) (*multiorchdatapb.PoolerHealthState, bool) {
		if value.IsUpToDate {
			value.IsUpToDate = false
			writeCount++
			return value, true // write and continue
		}
		return nil, true // no write, continue
	})

	// Only pooler1 should have triggered a write-back
	require.Equal(t, 1, writeCount)

	p1, ok := store.Get("pooler1")
	require.True(t, ok)
	require.False(t, p1.IsUpToDate, "pooler1 IsUpToDate should have been reset to false")

	p2, ok := store.Get("pooler2")
	require.True(t, ok)
	require.False(t, p2.IsUpToDate, "pooler2 IsUpToDate should remain false")
}
