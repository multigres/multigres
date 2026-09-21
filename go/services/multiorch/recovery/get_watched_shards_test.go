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

package recovery

import (
	"testing"

	"github.com/stretchr/testify/require"

	commontypes "github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

func TestEngine_GetWatchedShards(t *testing.T) {
	cache := store.NewTestCache(t)
	engine := &Engine{poolerCache: cache}

	shardA := &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}
	shardB := &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "1"}

	seed := func(name string, sk *clustermetadatapb.ShardKey) {
		store.SeedCache(t, cache, store.NewPooler(&multiorchdatapb.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{
				Id:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: name},
				ShardKey: sk,
			},
		}, nil))
	}
	// Two poolers on shardA (must still produce one entry, not two) and one on shardB.
	seed("pooler-1", shardA)
	seed("pooler-2", shardA)
	seed("pooler-3", shardB)

	shardKeys := engine.GetWatchedShards()
	require.Len(t, shardKeys, 2, "distinct shards must be deduplicated across their poolers")

	got := make(map[commontypes.ShardKeyString]bool)
	for _, sk := range shardKeys {
		got[commontypes.FormatShardKey(sk)] = true
	}
	require.True(t, got[commontypes.FormatShardKey(shardA)])
	require.True(t, got[commontypes.FormatShardKey(shardB)])
}

func TestEngine_GetWatchedShards_Empty(t *testing.T) {
	engine := &Engine{poolerCache: store.NewTestCache(t)}
	require.Empty(t, engine.GetWatchedShards())
}
