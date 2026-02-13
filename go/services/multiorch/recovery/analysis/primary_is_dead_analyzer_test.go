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
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	"github.com/multigres/multigres/go/services/multiorch/consensus"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

func TestPrimaryIsDeadAnalyzer_Analyze(t *testing.T) {
	// Set up factory for tests
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

	analyzer := &PrimaryIsDeadAnalyzer{factory: factory}

	t.Run("detects dead primary when all replicas reachable", func(t *testing.T) {
		primaryID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"}
		analysis := &store.ReplicationAnalysis{
			PoolerID:                      &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica1"},
			ShardKey:                      commontypes.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
			IsPrimary:                     false,
			IsInitialized:                 true,
			PrimaryPoolerID:               primaryID,
			PrimaryReachable:              false,
			CountReplicasInShard:          2,
			CountReachableReplicasInShard: 2,
		}

		problem, err := analyzer.Analyze(analysis)
		require.NoError(t, err)
		require.NotNil(t, problem)
		require.Equal(t, types.ProblemPrimaryIsDead, problem.Code)
		require.Equal(t, types.ScopeShard, problem.Scope)
		require.Equal(t, types.PriorityEmergency, problem.Priority)
		require.NotNil(t, problem.RecoveryAction)
	})

	t.Run("ignores healthy primary (reachable)", func(t *testing.T) {
		primaryID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"}
		analysis := &store.ReplicationAnalysis{
			IsPrimary:        false,
			IsInitialized:    true,
			PrimaryPoolerID:  primaryID,
			PrimaryReachable: true,
		}

		problem, err := analyzer.Analyze(analysis)
		require.NoError(t, err)
		require.Nil(t, problem)
	})

	t.Run("ignores no primary scenario", func(t *testing.T) {
		analysis := &store.ReplicationAnalysis{
			IsPrimary:        false,
			IsInitialized:    true,
			PrimaryPoolerID:  nil,
			PrimaryReachable: false,
		}

		problem, err := analyzer.Analyze(analysis)
		require.NoError(t, err)
		require.Nil(t, problem)
	})

	t.Run("ignores primary itself", func(t *testing.T) {
		analysis := &store.ReplicationAnalysis{
			IsPrimary:        true,
			IsInitialized:    true,
			PrimaryPoolerID:  nil,
			PrimaryReachable: false,
		}

		problem, err := analyzer.Analyze(analysis)
		require.NoError(t, err)
		require.Nil(t, problem)
	})

	t.Run("ignores uninitialized replica", func(t *testing.T) {
		primaryID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"}
		analysis := &store.ReplicationAnalysis{
			IsPrimary:        false,
			IsInitialized:    false,
			PrimaryPoolerID:  primaryID,
			PrimaryReachable: false,
		}

		problem, err := analyzer.Analyze(analysis)
		require.NoError(t, err)
		require.Nil(t, problem)
	})

	t.Run("analyzer name is correct", func(t *testing.T) {
		require.Equal(t, types.CheckName("PrimaryIsDead"), analyzer.Name())
	})

	t.Run("ignores when primary pooler down but replicas connected", func(t *testing.T) {
		primaryID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"}
		analysis := &store.ReplicationAnalysis{
			PoolerID:                   &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica1"},
			ShardKey:                   commontypes.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
			IsPrimary:                  false,
			IsInitialized:              true,
			PrimaryPoolerID:            primaryID,
			PrimaryReachable:           false,
			PrimaryPoolerReachable:     false,
			ReplicasConnectedToPrimary: true,
		}

		problem, err := analyzer.Analyze(analysis)
		require.NoError(t, err)
		require.Nil(t, problem, "should not trigger failover when pooler is down but replicas are connected")
	})

	t.Run("triggers failover when primary pooler up but postgres down", func(t *testing.T) {
		primaryID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"}
		analysis := &store.ReplicationAnalysis{
			PoolerID:                      &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica1"},
			ShardKey:                      commontypes.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
			IsPrimary:                     false,
			IsInitialized:                 true,
			PrimaryPoolerID:               primaryID,
			PrimaryReachable:              false,
			PrimaryPoolerReachable:        true,
			PrimaryPostgresRunning:        false,
			ReplicasConnectedToPrimary:    false,
			CountReplicasInShard:          2,
			CountReachableReplicasInShard: 2,
		}

		problem, err := analyzer.Analyze(analysis)
		require.NoError(t, err)
		require.NotNil(t, problem, "should trigger failover when pooler is up but postgres is down")
		require.Equal(t, types.ProblemPrimaryIsDead, problem.Code)
	})

	t.Run("does not trigger when some replicas unreachable", func(t *testing.T) {
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
			CountReachableReplicasInShard: 2, // Only 2 of 3 reachable
		}

		problem, err := analyzer.Analyze(analysis)
		require.NoError(t, err)
		require.Nil(t, problem, "PrimaryIsDead requires all replicas reachable")
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
			CountReachableReplicasInShard: 0,
		}

		problem, err := analyzer.Analyze(analysis)
		require.NoError(t, err)
		require.Nil(t, problem, "PrimaryIsDead requires all replicas reachable")
	})
}
