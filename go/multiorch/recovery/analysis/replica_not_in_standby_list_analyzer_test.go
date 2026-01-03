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
	"github.com/multigres/multigres/go/multiorch/coordinator"
	"github.com/multigres/multigres/go/multiorch/recovery/types"
	"github.com/multigres/multigres/go/multiorch/store"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

func TestReplicaNotInStandbyListAnalyzer_Analyze(t *testing.T) {
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
	coord := coordinator.NewCoordinator(coordID, ts, rpcClient, slog.Default())
	factory := NewRecoveryActionFactory(poolerStore, rpcClient, ts, coord, slog.Default())

	analyzer := &ReplicaNotInStandbyListAnalyzer{factory: factory}

	require.Equal(t, types.CheckName("ReplicaNotInStandbyList"), analyzer.Name())

	tests := []struct {
		name          string
		analysis      *store.ReplicationAnalysis
		expectProblem bool
		expectedCode  types.ProblemCode
		expectedScope types.ProblemScope
		expectedPrio  types.Priority
	}{
		{
			name: "detects replica not in standby list",
			analysis: &store.ReplicationAnalysis{
				PoolerID:               &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica1"},
				ShardKey:               commontypes.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
				PoolerType:             clustermetadatapb.PoolerType_REPLICA,
				IsPrimary:              false,
				IsInitialized:          true,
				PrimaryPoolerID:        &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"},
				PrimaryReachable:       true,
				PrimaryConnInfoHost:    "primary.example.com",
				IsInPrimaryStandbyList: false,
			},
			expectProblem: true,
			expectedCode:  types.ProblemReplicaNotInStandbyList,
			expectedScope: types.ScopePooler,
			expectedPrio:  types.PriorityNormal,
		},
		{
			name: "ignores replica already in standby list",
			analysis: &store.ReplicationAnalysis{
				PoolerID:               &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica1"},
				ShardKey:               commontypes.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
				PoolerType:             clustermetadatapb.PoolerType_REPLICA,
				IsPrimary:              false,
				IsInitialized:          true,
				PrimaryPoolerID:        &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"},
				PrimaryReachable:       true,
				PrimaryConnInfoHost:    "primary.example.com",
				IsInPrimaryStandbyList: true,
			},
			expectProblem: false,
		},
		{
			name: "ignores primary nodes",
			analysis: &store.ReplicationAnalysis{
				PoolerID:               &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"},
				ShardKey:               commontypes.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
				PoolerType:             clustermetadatapb.PoolerType_PRIMARY,
				IsPrimary:              true,
				IsInitialized:          true,
				IsInPrimaryStandbyList: false,
			},
			expectProblem: false,
		},
		{
			name: "ignores uninitialized replica",
			analysis: &store.ReplicationAnalysis{
				PoolerID:               &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica1"},
				ShardKey:               commontypes.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
				PoolerType:             clustermetadatapb.PoolerType_REPLICA,
				IsPrimary:              false,
				IsInitialized:          false,
				IsInPrimaryStandbyList: false,
			},
			expectProblem: false,
		},
		{
			name: "ignores replica when primary is unreachable",
			analysis: &store.ReplicationAnalysis{
				PoolerID:               &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica1"},
				ShardKey:               commontypes.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
				PoolerType:             clustermetadatapb.PoolerType_REPLICA,
				IsPrimary:              false,
				IsInitialized:          true,
				PrimaryPoolerID:        &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"},
				PrimaryReachable:       false,
				PrimaryConnInfoHost:    "primary.example.com",
				IsInPrimaryStandbyList: false,
			},
			expectProblem: false,
		},
		{
			name: "ignores replica with no replication configured",
			analysis: &store.ReplicationAnalysis{
				PoolerID:               &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica1"},
				ShardKey:               commontypes.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
				PoolerType:             clustermetadatapb.PoolerType_REPLICA,
				IsPrimary:              false,
				IsInitialized:          true,
				PrimaryPoolerID:        &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"},
				PrimaryReachable:       true,
				PrimaryConnInfoHost:    "",
				IsInPrimaryStandbyList: false,
			},
			expectProblem: false,
		},
		{
			name: "ignores UNKNOWN pooler type",
			analysis: &store.ReplicationAnalysis{
				PoolerID:               &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "unknown1"},
				ShardKey:               commontypes.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
				PoolerType:             clustermetadatapb.PoolerType_UNKNOWN,
				IsPrimary:              false,
				IsInitialized:          true,
				PrimaryPoolerID:        &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"},
				PrimaryReachable:       true,
				PrimaryConnInfoHost:    "primary.example.com",
				IsInPrimaryStandbyList: false,
			},
			expectProblem: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			problems, err := analyzer.Analyze(tt.analysis)
			require.NoError(t, err)

			if tt.expectProblem {
				require.Len(t, problems, 1)
				require.Equal(t, tt.expectedCode, problems[0].Code)
				require.Equal(t, tt.expectedScope, problems[0].Scope)
				require.Equal(t, tt.expectedPrio, problems[0].Priority)
				require.NotNil(t, problems[0].RecoveryAction)
			} else {
				require.Len(t, problems, 0)
			}
		})
	}

	t.Run("returns error when factory is nil", func(t *testing.T) {
		nilFactoryAnalyzer := &ReplicaNotInStandbyListAnalyzer{factory: nil}
		analysis := &store.ReplicationAnalysis{
			PoolerID:               &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica1"},
			ShardKey:               commontypes.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
			PoolerType:             clustermetadatapb.PoolerType_REPLICA,
			IsPrimary:              false,
			IsInitialized:          true,
			PrimaryPoolerID:        &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"},
			PrimaryReachable:       true,
			PrimaryConnInfoHost:    "primary.example.com",
			IsInPrimaryStandbyList: false,
		}

		problems, err := nilFactoryAnalyzer.Analyze(analysis)
		require.Error(t, err)
		require.Nil(t, problems)
		require.Contains(t, err.Error(), "factory not initialized")
	})
}
