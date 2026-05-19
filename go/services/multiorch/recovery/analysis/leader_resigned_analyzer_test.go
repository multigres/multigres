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

package analysis

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/consensus"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

func TestLeaderResignedAnalyzer_Analyze(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()
	rpcClient := &rpcclient.FakeClient{}
	poolerStore := store.NewPoolerStore(rpcClient, slog.Default())
	coordID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIORCH,
		Cell:      "cell1",
		Name:      "test-coord",
	}
	coord := consensus.NewCoordinator(coordID, ts, rpcClient, slog.Default(), false)
	cfg := config.NewTestConfig()
	factory := NewRecoveryActionFactory(cfg, poolerStore, rpcClient, ts, coord, slog.Default())

	analyzer := &LeaderResignedAnalyzer{factory: factory}

	leaderID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "leader-1"}
	shardKey := &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}

	t.Run("fires when leader has resigned", func(t *testing.T) {
		sa := &ShardAnalysis{
			ShardKey:                      shardKey,
			HighestTermDiscoveredLeaderID: leaderID,
			LeaderHasResigned:             true,
		}
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1)
		require.Equal(t, types.ProblemLeaderResigned, problems[0].Code)
		require.Equal(t, types.CheckName("LeaderResigned"), problems[0].CheckName)
		require.Equal(t, types.ScopeShard, problems[0].Scope)
		require.Equal(t, types.PriorityEmergency, problems[0].Priority)
		require.Equal(t, leaderID, problems[0].PoolerID)
		require.Equal(t, shardKey, problems[0].ShardKey)
	})

	t.Run("does not fire when leader has not resigned", func(t *testing.T) {
		sa := &ShardAnalysis{
			ShardKey:                      shardKey,
			HighestTermDiscoveredLeaderID: leaderID,
			LeaderHasResigned:             false,
		}
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems)
	})

	t.Run("does not fire when no leader exists", func(t *testing.T) {
		sa := &ShardAnalysis{
			ShardKey:          shardKey,
			LeaderHasResigned: true,
		}
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems)
	})
}
