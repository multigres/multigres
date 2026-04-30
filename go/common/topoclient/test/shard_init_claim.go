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

package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// checkShardInitClaim verifies ClaimShardInitialization semantics:
// first caller wins, different caller loses, same caller after crash wins
// with the originally committed cohort.
func checkShardInitClaim(t *testing.T, ctx context.Context, ts topoclient.Store) {
	shardKey := &clustermetadatapb.ShardKey{Database: "claimdb", TableGroup: "tg0", Shard: "0"}

	coord1 := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Cell: "cell1", Name: "orch-1"}
	coord2 := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Cell: "cell2", Name: "orch-2"}
	cohort := []*clustermetadatapb.ID{
		{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "pooler-1"},
		{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "pooler-2"},
	}

	t.Log("===      first caller wins the claim")
	won, committed, err := ts.ClaimShardInitialization(ctx, shardKey, coord1, cohort)
	require.NoError(t, err)
	assert.True(t, won, "first caller should win")
	require.Len(t, committed, 2)

	t.Log("===      same caller wins again (crash recovery) with committed cohort")
	differentCohort := []*clustermetadatapb.ID{
		{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "pooler-3"},
	}
	won, committed, err = ts.ClaimShardInitialization(ctx, shardKey, coord1, differentCohort)
	require.NoError(t, err)
	assert.True(t, won, "same caller should win on retry")
	// Must get back the originally committed cohort, not the new proposal.
	require.Len(t, committed, 2)
	assert.Equal(t, "pooler-1", committed[0].Name)
	assert.Equal(t, "pooler-2", committed[1].Name)

	t.Log("===      different caller loses")
	won, _, err = ts.ClaimShardInitialization(ctx, shardKey, coord2, cohort)
	require.NoError(t, err)
	assert.False(t, won, "different caller should lose")

	t.Log("===      independent shard can be claimed separately")
	shardKey2 := &clustermetadatapb.ShardKey{Database: "claimdb", TableGroup: "tg0", Shard: "1"}
	won, committed, err = ts.ClaimShardInitialization(ctx, shardKey2, coord2, cohort)
	require.NoError(t, err)
	assert.True(t, won, "different shard should be claimable independently")
	require.Len(t, committed, 2)
}
