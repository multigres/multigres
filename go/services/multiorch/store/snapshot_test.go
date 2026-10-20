// Copyright 2026 Supabase, Inc.
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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/poolerwatch"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	"github.com/multigres/multigres/go/services/multiorch/internal/snapshot"
)

func seedPooler(t *testing.T, cache *PoolerCache, name string, shardKey *clustermetadatapb.ShardKey) {
	t.Helper()
	SeedCache(t, cache, NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: name},
			ShardKey: shardKey,
		},
	}, nil))
}

func TestSnapshotShard_PoolersAreInStableOrder(t *testing.T) {
	cache := NewTestCache(t)
	key := &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}
	for _, name := range []string{"c", "a", "b"} {
		seedPooler(t, cache, name, key)
	}

	shard, err := SnapshotShard(cache, key, SnapshotOptions{Now: time.Now(), AvailabilityPolicy: snapshot.AvailabilityPolicy{}})
	require.NoError(t, err)
	var names []string
	for _, p := range shard.Poolers {
		names = append(names, p.GetMultipooler().GetId().GetName())
	}
	assert.Equal(t, []string{"a", "b", "c"}, names)
}

func TestSnapshotShard_UnknownShardIsNotFound(t *testing.T) {
	_, err := SnapshotShard(NewTestCache(t), &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}, SnapshotOptions{})
	require.Error(t, err)
	assert.Equal(t, mtrpcpb.Code_NOT_FOUND, mterrors.Code(err))
}

func TestSnapshotShards_GroupsPoolersByShard(t *testing.T) {
	cache := NewTestCache(t)
	shard0 := &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}
	shard1 := &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "1"}
	seedPooler(t, cache, "a", shard0)
	seedPooler(t, cache, "b", shard0)
	seedPooler(t, cache, "c", shard1)

	sizes := map[string]int{}
	for _, s := range SnapshotShards(cache, SnapshotOptions{Now: time.Now()}) {
		sizes[s.ShardKey.GetShard()] = len(s.Poolers)
	}
	assert.Equal(t, map[string]int{"0": 2, "1": 1}, sizes)
}

func TestSnapshotShards_CarriesTheOptions(t *testing.T) {
	cache := NewTestCache(t)
	key := &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}
	seedPooler(t, cache, "a", key)
	now := time.Now()
	policy := &clustermetadatapb.DurabilityPolicy{RequiredCount: 2}

	shards := SnapshotShards(cache, SnapshotOptions{
		Now:                now,
		AvailabilityPolicy: snapshot.AvailabilityPolicy{ObservationFreshness: time.Minute},
		BootstrapDurabilityPolicy: func(database string) *clustermetadatapb.DurabilityPolicy {
			assert.Equal(t, "db", database)
			return policy
		},
	})
	require.Len(t, shards, 1)
	assert.Equal(t, now, shards[0].Now)
	assert.Equal(t, time.Minute, shards[0].AvailabilityPolicy.ObservationFreshness)
	assert.Same(t, policy, shards[0].BootstrapDurabilityPolicy)
}

func TestHealthOf(t *testing.T) {
	id := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "a"}

	assert.Nil(t, healthOf(nil), "no rider")
	assert.Nil(t, healthOf(NewPooler(&multiorchdatapb.PoolerHealthState{}, nil)), "rider with no identified pooler")

	health := &multiorchdatapb.PoolerHealthState{Multipooler: &clustermetadatapb.Multipooler{Id: id}}
	got := healthOf(NewPooler(health, nil))
	require.NotNil(t, got)
	assert.Equal(t, "a", got.GetMultipooler().GetId().GetName())
}

func TestSnapshotShards_CarriesTombstones(t *testing.T) {
	cache := NewTestCache(t)
	key := &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}
	seedPooler(t, cache, "live", key)
	gone := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "gone"}
	poolerwatch.SeedTombstoneForTest(t, cache, gone, time.Now())

	shard, err := SnapshotShard(cache, key, SnapshotOptions{Now: time.Now()})
	require.NoError(t, err)
	assert.Equal(t, map[topoclient.ComponentID]struct{}{topoclient.ComponentIDString(gone): {}}, shard.TombstoneIDs)
	assert.Len(t, shard.Poolers, 1, "a tombstoned pooler is not in the live set")
}
