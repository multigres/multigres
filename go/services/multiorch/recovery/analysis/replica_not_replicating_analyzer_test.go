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

func TestReplicaNotReplicatingAnalyzer_Analyze(t *testing.T) {
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

	analyzer := &ReplicaNotReplicatingAnalyzer{factory: factory}

	t.Run("detects replica with no primary_conninfo", func(t *testing.T) {
		analysis := &store.ReplicationAnalysis{
			PoolerID:            &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica1"},
			ShardKey:            commontypes.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
			IsPrimary:           false,
			IsInitialized:       true,
			PrimaryPoolerID:     &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"},
			PrimaryReachable:    true,
			PrimaryConnInfoHost: "", // No primary_conninfo configured
			ReplicationStopped:  false,
		}

		problem, err := analyzer.Analyze(analysis)
		require.NoError(t, err)
		require.NotNil(t, problem)
		require.Equal(t, types.ProblemReplicaNotReplicating, problem.Code)
		require.Equal(t, types.ScopePooler, problem.Scope)
		require.Equal(t, types.PriorityHigh, problem.Priority)
		require.NotNil(t, problem.RecoveryAction)
	})

	t.Run("detects replica with replication stopped", func(t *testing.T) {
		analysis := &store.ReplicationAnalysis{
			PoolerID:            &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica1"},
			ShardKey:            commontypes.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
			IsPrimary:           false,
			IsInitialized:       true,
			PrimaryPoolerID:     &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"},
			PrimaryReachable:    true,
			PrimaryConnInfoHost: "primary.example.com",
			ReplicationStopped:  true, // Replication stopped
		}

		problem, err := analyzer.Analyze(analysis)
		require.NoError(t, err)
		require.NotNil(t, problem)
		require.Equal(t, types.ProblemReplicaNotReplicating, problem.Code)
	})

	t.Run("ignores replica with healthy replication", func(t *testing.T) {
		analysis := &store.ReplicationAnalysis{
			PoolerID:            &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica1"},
			ShardKey:            commontypes.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
			IsPrimary:           false,
			IsInitialized:       true,
			PrimaryPoolerID:     &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"},
			PrimaryReachable:    true,
			PrimaryConnInfoHost: "primary.example.com",
			ReplicationStopped:  false,
		}

		problem, err := analyzer.Analyze(analysis)
		require.NoError(t, err)
		require.Nil(t, problem)
	})

	t.Run("ignores primary nodes", func(t *testing.T) {
		analysis := &store.ReplicationAnalysis{
			PoolerID:            &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"},
			ShardKey:            commontypes.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
			IsPrimary:           true,
			IsInitialized:       true,
			PrimaryConnInfoHost: "", // Primaries don't have primary_conninfo
		}

		problem, err := analyzer.Analyze(analysis)
		require.NoError(t, err)
		require.Nil(t, problem)
	})

	t.Run("ignores uninitialized replica", func(t *testing.T) {
		analysis := &store.ReplicationAnalysis{
			PoolerID:            &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica1"},
			ShardKey:            commontypes.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
			IsPrimary:           false,
			IsInitialized:       false, // Not initialized
			PrimaryConnInfoHost: "",
		}

		problem, err := analyzer.Analyze(analysis)
		require.NoError(t, err)
		require.Nil(t, problem)
	})

	t.Run("ignores replica when primary is unreachable", func(t *testing.T) {
		analysis := &store.ReplicationAnalysis{
			PoolerID:            &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica1"},
			ShardKey:            commontypes.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
			IsPrimary:           false,
			IsInitialized:       true,
			PrimaryPoolerID:     &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"},
			PrimaryReachable:    false, // Primary unreachable - PrimaryIsDead handles this
			PrimaryConnInfoHost: "",
		}

		problem, err := analyzer.Analyze(analysis)
		require.NoError(t, err)
		require.Nil(t, problem)
	})

	t.Run("analyzer name is correct", func(t *testing.T) {
		require.Equal(t, types.CheckName("ReplicaNotReplicating"), analyzer.Name())
	})

	t.Run("returns error when factory is nil", func(t *testing.T) {
		nilFactoryAnalyzer := &ReplicaNotReplicatingAnalyzer{factory: nil}
		analysis := &store.ReplicationAnalysis{
			PoolerID:            &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica1"},
			ShardKey:            commontypes.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
			IsPrimary:           false,
			IsInitialized:       true,
			PrimaryPoolerID:     &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"},
			PrimaryReachable:    true,
			PrimaryConnInfoHost: "",
		}

		problem, err := nilFactoryAnalyzer.Analyze(analysis)
		require.Error(t, err)
		require.Nil(t, problem)
		require.Contains(t, err.Error(), "factory not initialized")
	})
}
