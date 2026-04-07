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

package analysis

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	commontypes "github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/consensus"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

func TestPrimaryIsDeadAnalyzer_Analyze(t *testing.T) {
	// Set up factory for tests
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
	coord := consensus.NewCoordinator(coordID, ts, rpcClient, slog.Default())
	factory := NewRecoveryActionFactory(nil, poolerStore, rpcClient, ts, coord, slog.Default())

	analyzer := &PrimaryIsDeadAnalyzer{factory: factory}

	primaryID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"}
	shardKey := commontypes.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}

	// deadPrimaryShardAnalysis builds a ShardAnalysis that has a dead primary and an
	// initialized replica — the base case for PrimaryIsDead detection.
	deadPrimaryShardAnalysis := func(overrides ...func(*ShardAnalysis)) *ShardAnalysis {
		sa := &ShardAnalysis{
			ShardKey:                       shardKey,
			HighestTermDiscoveredPrimaryID: primaryID,
			PrimaryReachable:               false,
			HasInitializedReplica:          true,
			Analyses: []*PoolerAnalysis{
				{
					PoolerID:      &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica1"},
					ShardKey:      shardKey,
					IsPrimary:     false,
					IsInitialized: true,
				},
			},
		}
		for _, o := range overrides {
			o(sa)
		}
		return sa
	}

	t.Run("detects dead primary (primary exists in topology but unreachable)", func(t *testing.T) {
		sa := deadPrimaryShardAnalysis()

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1)
		problem := problems[0]
		require.Equal(t, types.ProblemPrimaryIsDead, problem.Code)
		require.Equal(t, types.ScopeShard, problem.Scope)
		require.Equal(t, types.PriorityEmergency, problem.Priority)
		require.Equal(t, primaryID, problem.PoolerID)
		require.NotNil(t, problem.RecoveryAction)
	})

	t.Run("ignores healthy primary (reachable)", func(t *testing.T) {
		sa := deadPrimaryShardAnalysis(func(sa *ShardAnalysis) {
			sa.PrimaryReachable = true
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems)
	})

	t.Run("ignores when no primary exists in topology (future analysis)", func(t *testing.T) {
		sa := deadPrimaryShardAnalysis(func(sa *ShardAnalysis) {
			sa.HighestTermDiscoveredPrimaryID = nil
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems)
	})

	t.Run("ignores when no initialized replica can confirm the primary is dead", func(t *testing.T) {
		sa := deadPrimaryShardAnalysis(func(sa *ShardAnalysis) {
			sa.HasInitializedReplica = false
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems)
	})

	t.Run("ignores when primary pooler down but all replicas still connected to postgres", func(t *testing.T) {
		sa := deadPrimaryShardAnalysis(func(sa *ShardAnalysis) {
			sa.PrimaryPoolerReachable = false
			sa.ReplicasConnectedToPrimary = true
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems, "should not trigger failover when pooler is down but replicas are connected")
	})

	t.Run("triggers failover when primary pooler up but postgres down", func(t *testing.T) {
		sa := deadPrimaryShardAnalysis(func(sa *ShardAnalysis) {
			sa.PrimaryPoolerReachable = true // Pooler is up
			// PrimaryReachable remains false (postgres down)
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1)
		require.Equal(t, types.ProblemPrimaryIsDead, problems[0].Code)
		require.Equal(t, primaryID, problems[0].PoolerID)
	})

	t.Run("triggers failover when both pooler and replicas disconnected", func(t *testing.T) {
		sa := deadPrimaryShardAnalysis(func(sa *ShardAnalysis) {
			sa.PrimaryPoolerReachable = false
			sa.ReplicasConnectedToPrimary = false
		})

		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1)
		require.Equal(t, types.ProblemPrimaryIsDead, problems[0].Code)
		require.Equal(t, primaryID, problems[0].PoolerID)
	})

	t.Run("analyzer name is correct", func(t *testing.T) {
		require.Equal(t, types.CheckName("PrimaryIsDead"), analyzer.Name())
	})
}
