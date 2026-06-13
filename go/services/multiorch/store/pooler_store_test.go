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
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/rpcclient"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestPoolerStore_FindPoolersInShard(t *testing.T) {
	poolerStore := NewPoolerStore(nil, slog.Default())

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
	poolerStore := NewPoolerStore(nil, slog.Default())

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

func TestPoolerStore_FindHealthyPrimary(t *testing.T) {
	ctx := context.Background()

	primaryID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"}
	replicaID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica"}

	t.Run("finds consensus leader verified by its live status", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		fakeClient.SetStatusResponse("multipooler-cell1-primary", &multipoolermanagerdatapb.StatusResponse{
			ConsensusStatus: leaderConsensusStatus(primaryID, 1),
		})
		poolerStore := NewPoolerStore(fakeClient, slog.Default())

		poolers := []*multiorchdatapb.PoolerHealthState{
			{MultiPooler: &clustermetadatapb.MultiPooler{Id: replicaID, Type: clustermetadatapb.PoolerType_REPLICA}},
			{
				MultiPooler:     &clustermetadatapb.MultiPooler{Id: primaryID, Type: clustermetadatapb.PoolerType_PRIMARY},
				ConsensusStatus: leaderConsensusStatus(primaryID, 1),
			},
		}

		primary, err := poolerStore.FindHealthyPrimary(ctx, poolers)

		require.NoError(t, err)
		assert.Equal(t, "primary", primary.MultiPooler.Id.Name)
	})

	t.Run("finds leader named by a follower's rule even when its own snapshot is absent", func(t *testing.T) {
		// The leader's own health snapshot carries no consensus status (stale),
		// but a follower's replication rule still names it. The leader must be
		// found and verified via its live Status RPC.
		fakeClient := rpcclient.NewFakeClient()
		fakeClient.SetStatusResponse("multipooler-cell1-primary", &multipoolermanagerdatapb.StatusResponse{
			ConsensusStatus: leaderConsensusStatus(primaryID, 1),
		})
		poolerStore := NewPoolerStore(fakeClient, slog.Default())

		poolers := []*multiorchdatapb.PoolerHealthState{
			{MultiPooler: &clustermetadatapb.MultiPooler{Id: primaryID, Type: clustermetadatapb.PoolerType_PRIMARY}},
			{
				MultiPooler: &clustermetadatapb.MultiPooler{Id: replicaID, Type: clustermetadatapb.PoolerType_REPLICA},
				ConsensusStatus: &clustermetadatapb.ConsensusStatus{
					Id: replicaID,
					ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{
						Rule: &clustermetadatapb.ShardRule{
							RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1},
							LeaderId:   primaryID,
						},
					},
				},
			},
		}

		primary, err := poolerStore.FindHealthyPrimary(ctx, poolers)

		require.NoError(t, err)
		assert.Equal(t, "primary", primary.MultiPooler.Id.Name)
	})

	t.Run("returns error when no consensus leader is known", func(t *testing.T) {
		poolerStore := NewPoolerStore(&rpcclient.FakeClient{}, slog.Default())

		poolers := []*multiorchdatapb.PoolerHealthState{
			{MultiPooler: &clustermetadatapb.MultiPooler{Id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica1"}, Type: clustermetadatapb.PoolerType_REPLICA}},
			{MultiPooler: &clustermetadatapb.MultiPooler{Id: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica2"}, Type: clustermetadatapb.PoolerType_REPLICA}},
		}

		primary, err := poolerStore.FindHealthyPrimary(ctx, poolers)

		assert.Error(t, err)
		assert.Nil(t, primary)
		assert.Contains(t, err.Error(), "no consensus leader known")
	})

	t.Run("returns error when consensus leader is unreachable", func(t *testing.T) {
		fakeClient := rpcclient.NewFakeClient()
		fakeClient.Errors["multipooler-cell1-primary"] = errors.New("connection refused")
		poolerStore := NewPoolerStore(fakeClient, slog.Default())

		poolers := []*multiorchdatapb.PoolerHealthState{
			{
				MultiPooler:     &clustermetadatapb.MultiPooler{Id: primaryID, Type: clustermetadatapb.PoolerType_PRIMARY},
				ConsensusStatus: leaderConsensusStatus(primaryID, 1),
			},
		}

		primary, err := poolerStore.FindHealthyPrimary(ctx, poolers)

		assert.Error(t, err)
		assert.Nil(t, primary)
		assert.Contains(t, err.Error(), "unreachable during health check")
	})

	t.Run("returns error when leader no longer reports itself as leader", func(t *testing.T) {
		// The rule still names primary, but its live status shows it has resigned
		// (no longer names itself as leader) — e.g. demoted into recovery.
		fakeClient := rpcclient.NewFakeClient()
		fakeClient.SetStatusResponse("multipooler-cell1-primary", &multipoolermanagerdatapb.StatusResponse{
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{Id: primaryID},
		})
		poolerStore := NewPoolerStore(fakeClient, slog.Default())

		poolers := []*multiorchdatapb.PoolerHealthState{
			{
				MultiPooler:     &clustermetadatapb.MultiPooler{Id: primaryID, Type: clustermetadatapb.PoolerType_PRIMARY},
				ConsensusStatus: leaderConsensusStatus(primaryID, 1),
			},
		}

		primary, err := poolerStore.FindHealthyPrimary(ctx, poolers)

		assert.Error(t, err)
		assert.Nil(t, primary)
		assert.Contains(t, err.Error(), "no longer reports itself as the leader")
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

// TestPoolerStore_DoUpdateRange verifies that DoUpdateRange atomically resets fields
// on qualifying poolers while leaving others unchanged — mirroring the
// queuePoolersHealthCheck use case.
func TestPoolerStore_DoUpdateRange(t *testing.T) {
	store := NewPoolerStore(nil, slog.Default())

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
	store.DoUpdateRange(func(key string, value *multiorchdatapb.PoolerHealthState) (*multiorchdatapb.PoolerHealthState, bool) {
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
