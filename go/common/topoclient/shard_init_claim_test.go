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

package topoclient_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestClaimShardInitialization(t *testing.T) {
	shardKey := types.ShardKey{Database: "mydb", TableGroup: "tg0", Shard: "0"}
	coord := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Cell: "cell1", Name: "orch-1"}
	cohort := []*clustermetadatapb.ID{
		{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "p1"},
		{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "p2"},
	}

	t.Run("first caller wins the claim", func(t *testing.T) {
		ctx := t.Context()
		ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
		defer ts.Close()

		won, committed, err := ts.ClaimShardInitialization(ctx, shardKey, coord, cohort)
		require.NoError(t, err)
		require.True(t, won)
		require.Len(t, committed, 2)
	})

	t.Run("second caller with different ID loses", func(t *testing.T) {
		ctx := t.Context()
		ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
		defer ts.Close()

		won, _, err := ts.ClaimShardInitialization(ctx, shardKey, coord, cohort)
		require.NoError(t, err)
		require.True(t, won)

		coord2 := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Cell: "cell2", Name: "orch-2"}
		won, _, err = ts.ClaimShardInitialization(ctx, shardKey, coord2, cohort)
		require.NoError(t, err)
		require.False(t, won)
	})

	t.Run("same caller after crash gets committed cohort", func(t *testing.T) {
		ctx := t.Context()
		ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
		defer ts.Close()

		won, _, err := ts.ClaimShardInitialization(ctx, shardKey, coord, cohort)
		require.NoError(t, err)
		require.True(t, won)

		// Retry with a different proposed cohort — should get back the original.
		newCohort := []*clustermetadatapb.ID{
			{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "p3"},
		}
		won, committed, err := ts.ClaimShardInitialization(ctx, shardKey, coord, newCohort)
		require.NoError(t, err)
		require.True(t, won)
		require.Len(t, committed, 2)
		require.Equal(t, "p1", committed[0].Name)
		require.Equal(t, "p2", committed[1].Name)
	})
}
