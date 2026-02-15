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

func TestPrimaryRevokedAnalyzer_Analyze(t *testing.T) {
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

	analyzer := &PrimaryRevokedAnalyzer{factory: factory}

	primaryID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"}
	replicaID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica1"}
	shardKey := commontypes.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}

	tests := []struct {
		name     string
		analysis *store.ReplicationAnalysis
		wantNil  bool
		reason   string
	}{
		{
			name: "detects revoked primary: term mismatch and no replicas replicating",
			analysis: &store.ReplicationAnalysis{
				PoolerID:                            replicaID,
				ShardKey:                            shardKey,
				IsPrimary:                           false,
				IsInitialized:                       true,
				PrimaryPoolerID:                     primaryID,
				PrimaryReachable:                    true,
				ConsensusTerm:                       2,
				PrimaryConsensusTerm:                1,
				CountReplicaPoolersInShard:          2,
				CountReachableReplicaPoolersInShard: 2,
				CountReplicasConfirmingPrimaryAliveInShard: 0,
			},
			wantNil: false,
		},
		{
			name: "does not trigger when primary is unreachable",
			analysis: &store.ReplicationAnalysis{
				PoolerID:                            replicaID,
				ShardKey:                            shardKey,
				IsPrimary:                           false,
				IsInitialized:                       true,
				PrimaryPoolerID:                     primaryID,
				PrimaryReachable:                    false,
				ConsensusTerm:                       2,
				PrimaryConsensusTerm:                1,
				CountReplicaPoolersInShard:          2,
				CountReachableReplicaPoolersInShard: 2,
				CountReplicasConfirmingPrimaryAliveInShard: 0,
			},
			wantNil: true,
			reason:  "PrimaryIsDead handles unreachable primary, not PrimaryRevoked",
		},
		{
			name: "does not trigger when some replicas still replicating",
			analysis: &store.ReplicationAnalysis{
				PoolerID:                            replicaID,
				ShardKey:                            shardKey,
				IsPrimary:                           false,
				IsInitialized:                       true,
				PrimaryPoolerID:                     primaryID,
				PrimaryReachable:                    true,
				ConsensusTerm:                       2,
				PrimaryConsensusTerm:                1,
				CountReplicaPoolersInShard:          2,
				CountReachableReplicaPoolersInShard: 2,
				CountReplicasConfirmingPrimaryAliveInShard: 1,
			},
			wantNil: true,
			reason:  "some replicas still connected",
		},
		{
			name: "does not trigger when consensus terms match",
			analysis: &store.ReplicationAnalysis{
				PoolerID:                            replicaID,
				ShardKey:                            shardKey,
				IsPrimary:                           false,
				IsInitialized:                       true,
				PrimaryPoolerID:                     primaryID,
				PrimaryReachable:                    true,
				ConsensusTerm:                       1,
				PrimaryConsensusTerm:                1,
				CountReplicaPoolersInShard:          2,
				CountReachableReplicaPoolersInShard: 2,
				CountReplicasConfirmingPrimaryAliveInShard: 0,
			},
			wantNil: true,
			reason:  "same term means no REVOKE occurred (e.g. bootstrap)",
		},
		{
			name: "does not trigger when some replicas unreachable",
			analysis: &store.ReplicationAnalysis{
				PoolerID:                            replicaID,
				ShardKey:                            shardKey,
				IsPrimary:                           false,
				IsInitialized:                       true,
				PrimaryPoolerID:                     primaryID,
				PrimaryReachable:                    true,
				ConsensusTerm:                       2,
				PrimaryConsensusTerm:                1,
				CountReplicaPoolersInShard:          3,
				CountReachableReplicaPoolersInShard: 2,
				CountReplicasConfirmingPrimaryAliveInShard: 0,
			},
			wantNil: true,
			reason:  "requires full visibility — all replicas must be reachable",
		},
		{
			name: "does not trigger on primary node",
			analysis: &store.ReplicationAnalysis{
				PoolerID:      primaryID,
				IsPrimary:     true,
				IsInitialized: true,
			},
			wantNil: true,
		},
		{
			name: "does not trigger on uninitialized replica",
			analysis: &store.ReplicationAnalysis{
				PoolerID:             replicaID,
				IsPrimary:            false,
				IsInitialized:        false,
				PrimaryPoolerID:      primaryID,
				PrimaryReachable:     true,
				ConsensusTerm:        2,
				PrimaryConsensusTerm: 1,
			},
			wantNil: true,
		},
		{
			name: "does not trigger with zero replicas",
			analysis: &store.ReplicationAnalysis{
				PoolerID:                            replicaID,
				IsPrimary:                           false,
				IsInitialized:                       true,
				PrimaryPoolerID:                     primaryID,
				PrimaryReachable:                    true,
				ConsensusTerm:                       2,
				PrimaryConsensusTerm:                1,
				CountReplicaPoolersInShard:          0,
				CountReachableReplicaPoolersInShard: 0,
			},
			wantNil: true,
			reason:  "no replicas to failover to",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			problem, err := analyzer.Analyze(tt.analysis)
			require.NoError(t, err)
			if tt.wantNil {
				require.Nil(t, problem, tt.reason)
			} else {
				require.NotNil(t, problem)
				require.Equal(t, types.ProblemPrimaryRevoked, problem.Code)
				require.Equal(t, types.ScopeShard, problem.Scope)
				require.Equal(t, types.PriorityEmergency, problem.Priority)
				require.NotNil(t, problem.RecoveryAction)
			}
		})
	}

	t.Run("analyzer name is correct", func(t *testing.T) {
		require.Equal(t, types.CheckName("PrimaryRevoked"), analyzer.Name())
	})
}
