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
	commontypes "github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	"github.com/multigres/multigres/go/services/multiorch/consensus"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

func TestPrimaryAndReplicasDeadAnalyzer_Analyze(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()
	poolerStore := store.NewProtoStore[string, *multiorchdatapb.PoolerHealthState]()
	rpcClient := &rpcclient.FakeClient{}
	coordID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIORCH,
		Cell:      "cell1",
		Name:      "test-coord",
	}
	coord := consensus.NewCoordinator(coordID, ts, rpcClient, slog.Default())
	factory := NewRecoveryActionFactory(nil, poolerStore, rpcClient, ts, coord, slog.Default())

	analyzer := &PrimaryAndReplicasDeadAnalyzer{factory: factory}

	t.Run("detects when no poolers reachable", func(t *testing.T) {
		primaryID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"}
		analysis := &store.ReplicationAnalysis{
			PoolerID:                            &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica1"},
			ShardKey:                            commontypes.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
			IsPrimary:                           false,
			IsInitialized:                       false, // unreachable replicas have IsInitialized=false
			PrimaryPoolerID:                     primaryID,
			PrimaryReachable:                    false,
			AllReplicasConfirmPrimaryAlive:      false,
			CountReplicaPoolersInShard:          2,
			CountReachableReplicaPoolersInShard: 0,
			CountReplicasConfirmingPrimaryAliveInShard: 0,
		}

		problem, err := analyzer.Analyze(analysis)
		require.NoError(t, err)
		require.NotNil(t, problem)
		require.Equal(t, types.ProblemPrimaryAndReplicasDead, problem.Code)
		require.Equal(t, types.PriorityNormal, problem.Priority)
		require.IsType(t, &types.NoOpAction{}, problem.RecoveryAction, "non-actionable when no replicas reachable")
	})

	t.Run("does not trigger when some poolers reachable", func(t *testing.T) {
		primaryID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"}
		analysis := &store.ReplicationAnalysis{
			PoolerID:                            &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica1"},
			ShardKey:                            commontypes.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
			IsPrimary:                           false,
			IsInitialized:                       true,
			PrimaryPoolerID:                     primaryID,
			PrimaryReachable:                    false,
			AllReplicasConfirmPrimaryAlive:      false,
			CountReplicaPoolersInShard:          3,
			CountReachableReplicaPoolersInShard: 1,
			CountReplicasConfirmingPrimaryAliveInShard: 0,
		}

		problem, err := analyzer.Analyze(analysis)
		require.NoError(t, err)
		require.Nil(t, problem)
	})

	t.Run("does not trigger when all poolers reachable", func(t *testing.T) {
		primaryID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"}
		analysis := &store.ReplicationAnalysis{
			PoolerID:                            &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica1"},
			ShardKey:                            commontypes.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
			IsPrimary:                           false,
			IsInitialized:                       true,
			PrimaryPoolerID:                     primaryID,
			PrimaryReachable:                    false,
			AllReplicasConfirmPrimaryAlive:      false,
			CountReplicaPoolersInShard:          2,
			CountReachableReplicaPoolersInShard: 2,
			CountReplicasConfirmingPrimaryAliveInShard: 0,
		}

		problem, err := analyzer.Analyze(analysis)
		require.NoError(t, err)
		require.Nil(t, problem)
	})

	t.Run("analyzer name is correct", func(t *testing.T) {
		require.Equal(t, types.CheckName("PrimaryAndReplicasDead"), analyzer.Name())
	})

	t.Run("recovery action is no-op", func(t *testing.T) {
		require.IsType(t, &types.NoOpAction{}, analyzer.RecoveryAction())
	})
}
