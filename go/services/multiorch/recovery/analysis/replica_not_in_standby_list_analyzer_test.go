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

func TestReplicaNotInStandbyListAnalyzer_Analyze(t *testing.T) {
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

	analyzer := &ReplicaNotInStandbyListAnalyzer{factory: factory}

	require.Equal(t, types.CheckName("ReplicaNotInStandbyList"), analyzer.Name())

	primaryID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"}
	replicaID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica1"}
	shardKey := commontypes.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}

	tests := []struct {
		name          string
		buildShard    func() *ShardAnalysis
		expectProblem bool
		expectedCode  types.ProblemCode
		expectedScope types.ProblemScope
		expectedPrio  types.Priority
	}{
		{
			name: "detects replica not in standby list",
			buildShard: func() *ShardAnalysis {
				return &ShardAnalysis{
					ShardKey:                       shardKey,
					HighestTermDiscoveredPrimaryID: primaryID,
					PrimaryReachable:               true,
					// PrimaryStandbyIDs is empty — replica is not in standby list
					Analyses: []*PoolerAnalysis{{
						PoolerID:            replicaID,
						ShardKey:            shardKey,
						PoolerType:          clustermetadatapb.PoolerType_REPLICA,
						IsPrimary:           false,
						IsInitialized:       true,
						PrimaryConnInfoHost: "primary.example.com",
					}},
				}
			},
			expectProblem: true,
			expectedCode:  types.ProblemReplicaNotInStandbyList,
			expectedScope: types.ScopePooler,
			expectedPrio:  types.PriorityNormal,
		},
		{
			name: "ignores replica already in standby list",
			buildShard: func() *ShardAnalysis {
				return &ShardAnalysis{
					ShardKey:                       shardKey,
					HighestTermDiscoveredPrimaryID: primaryID,
					PrimaryReachable:               true,
					PrimaryStandbyIDs:              []*clustermetadatapb.ID{replicaID},
					Analyses: []*PoolerAnalysis{{
						PoolerID:            replicaID,
						ShardKey:            shardKey,
						PoolerType:          clustermetadatapb.PoolerType_REPLICA,
						IsPrimary:           false,
						IsInitialized:       true,
						PrimaryConnInfoHost: "primary.example.com",
					}},
				}
			},
			expectProblem: false,
		},
		{
			name: "ignores primary nodes",
			buildShard: func() *ShardAnalysis {
				return &ShardAnalysis{
					ShardKey: shardKey,
					Analyses: []*PoolerAnalysis{{
						PoolerID:      primaryID,
						ShardKey:      shardKey,
						PoolerType:    clustermetadatapb.PoolerType_PRIMARY,
						IsPrimary:     true,
						IsInitialized: true,
					}},
				}
			},
			expectProblem: false,
		},
		{
			name: "ignores uninitialized replica",
			buildShard: func() *ShardAnalysis {
				return &ShardAnalysis{
					ShardKey: shardKey,
					Analyses: []*PoolerAnalysis{{
						PoolerID:      replicaID,
						ShardKey:      shardKey,
						PoolerType:    clustermetadatapb.PoolerType_REPLICA,
						IsPrimary:     false,
						IsInitialized: false,
					}},
				}
			},
			expectProblem: false,
		},
		{
			name: "ignores replica when primary is unreachable",
			buildShard: func() *ShardAnalysis {
				return &ShardAnalysis{
					ShardKey:                       shardKey,
					HighestTermDiscoveredPrimaryID: primaryID,
					PrimaryReachable:               false,
					Analyses: []*PoolerAnalysis{{
						PoolerID:            replicaID,
						ShardKey:            shardKey,
						PoolerType:          clustermetadatapb.PoolerType_REPLICA,
						IsPrimary:           false,
						IsInitialized:       true,
						PrimaryConnInfoHost: "primary.example.com",
					}},
				}
			},
			expectProblem: false,
		},
		{
			name: "ignores replica with no replication configured",
			buildShard: func() *ShardAnalysis {
				return &ShardAnalysis{
					ShardKey:                       shardKey,
					HighestTermDiscoveredPrimaryID: primaryID,
					PrimaryReachable:               true,
					Analyses: []*PoolerAnalysis{{
						PoolerID:            replicaID,
						ShardKey:            shardKey,
						PoolerType:          clustermetadatapb.PoolerType_REPLICA,
						IsPrimary:           false,
						IsInitialized:       true,
						PrimaryConnInfoHost: "", // No replication configured
					}},
				}
			},
			expectProblem: false,
		},
		{
			name: "ignores UNKNOWN pooler type",
			buildShard: func() *ShardAnalysis {
				unknownID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "unknown1"}
				return &ShardAnalysis{
					ShardKey:                       shardKey,
					HighestTermDiscoveredPrimaryID: primaryID,
					PrimaryReachable:               true,
					Analyses: []*PoolerAnalysis{{
						PoolerID:            unknownID,
						ShardKey:            shardKey,
						PoolerType:          clustermetadatapb.PoolerType_UNKNOWN,
						IsPrimary:           false,
						IsInitialized:       true,
						PrimaryConnInfoHost: "primary.example.com",
					}},
				}
			},
			expectProblem: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sa := tt.buildShard()
			problems, err := analyzer.Analyze(sa)
			require.NoError(t, err)

			if tt.expectProblem {
				require.Len(t, problems, 1)
				require.Equal(t, tt.expectedCode, problems[0].Code)
				require.Equal(t, tt.expectedScope, problems[0].Scope)
				require.Equal(t, tt.expectedPrio, problems[0].Priority)
				require.NotNil(t, problems[0].RecoveryAction)
			} else {
				require.Empty(t, problems)
			}
		})
	}

	t.Run("returns error when factory is nil", func(t *testing.T) {
		nilFactoryAnalyzer := &ReplicaNotInStandbyListAnalyzer{factory: nil}
		sa := &ShardAnalysis{
			ShardKey:                       shardKey,
			HighestTermDiscoveredPrimaryID: primaryID,
			PrimaryReachable:               true,
			Analyses: []*PoolerAnalysis{{
				PoolerID:            replicaID,
				ShardKey:            shardKey,
				PoolerType:          clustermetadatapb.PoolerType_REPLICA,
				IsPrimary:           false,
				IsInitialized:       true,
				PrimaryConnInfoHost: "primary.example.com",
			}},
		}
		problems, err := nilFactoryAnalyzer.Analyze(sa)
		require.Error(t, err)
		require.Nil(t, problems)
		require.Contains(t, err.Error(), "factory not initialized")
	})
}
