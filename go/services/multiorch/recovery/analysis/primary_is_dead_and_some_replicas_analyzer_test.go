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

func TestPrimaryIsDeadAndSomeReplicasAnalyzer_Analyze(t *testing.T) {
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

	analyzer := &PrimaryIsDeadAndSomeReplicasAnalyzer{factory: factory}

	t.Run("triggers failover when some replicas reachable", func(t *testing.T) {
		primaryID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"}
		analysis := &store.ReplicationAnalysis{
			PoolerID:                      &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica1"},
			ShardKey:                      commontypes.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
			IsPrimary:                     false,
			IsInitialized:                 true,
			PrimaryPoolerID:               primaryID,
			PrimaryReachable:              false,
			ReplicasConnectedToPrimary:    false,
			CountReplicasInShard:          3,
			CountReachableReplicasInShard: 2, // 2 of 3 reachable
		}

		problem, err := analyzer.Analyze(analysis)
		require.NoError(t, err)
		require.NotNil(t, problem)
		require.Equal(t, types.ProblemPrimaryIsDeadAndSomeReplicas, problem.Code)
		require.Equal(t, types.PriorityEmergency, problem.Priority)
		require.NotNil(t, problem.RecoveryAction)
	})

	t.Run("does not trigger when all replicas reachable", func(t *testing.T) {
		primaryID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"}
		analysis := &store.ReplicationAnalysis{
			PoolerID:                      &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica1"},
			ShardKey:                      commontypes.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
			IsPrimary:                     false,
			IsInitialized:                 true,
			PrimaryPoolerID:               primaryID,
			PrimaryReachable:              false,
			ReplicasConnectedToPrimary:    false,
			CountReplicasInShard:          2,
			CountReachableReplicasInShard: 2, // All reachable → PrimaryIsDead, not this analyzer
		}

		problem, err := analyzer.Analyze(analysis)
		require.NoError(t, err)
		require.Nil(t, problem)
	})

	t.Run("does not trigger when no replicas reachable", func(t *testing.T) {
		primaryID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"}
		analysis := &store.ReplicationAnalysis{
			PoolerID:                      &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica1"},
			ShardKey:                      commontypes.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
			IsPrimary:                     false,
			IsInitialized:                 true,
			PrimaryPoolerID:               primaryID,
			PrimaryReachable:              false,
			ReplicasConnectedToPrimary:    false,
			CountReplicasInShard:          2,
			CountReachableReplicasInShard: 0, // None reachable → PrimaryAndReplicasDead, not this
		}

		problem, err := analyzer.Analyze(analysis)
		require.NoError(t, err)
		require.Nil(t, problem)
	})

	t.Run("analyzer name is correct", func(t *testing.T) {
		require.Equal(t, types.CheckName("PrimaryIsDeadAndSomeReplicas"), analyzer.Name())
	})
}
