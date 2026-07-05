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
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multiorch/consensus"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

func TestShardNeedsInitializationAnalyzer_Analyze(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	defer ts.Close()
	rpcClient := &rpcclient.FakeClient{}
	poolerStore := store.NewTestCache(t)
	coordID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIORCH,
		Cell:      "cell1",
		Name:      "test-coord",
	}
	coord := consensus.NewCoordinator(coordID, ts, rpcClient, slog.Default())
	factory := NewRecoveryActionFactory(nil, poolerStore, rpcClient, ts, coord, slog.Default())

	analyzer := &ShardNeedsInitializationAnalyzer{factory: factory}
	shardKey := &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}
	policy := topoclient.AtLeastN(2)

	poolerIDFor := func(name string) *clustermetadatapb.ID {
		return &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: name}
	}
	initialized := func(name string) *store.Pooler {
		return newRider(&multiorchdatapb.PoolerHealthState{
			Multipooler:      &clustermetadatapb.Multipooler{Id: poolerIDFor(name), ShardKey: shardKey},
			IsLastCheckValid: true,
			Status:           &multipoolermanagerdatapb.Status{IsInitialized: true},
		})
	}

	t.Run("fires when quorum of initialized poolers present with no cohort or primary", func(t *testing.T) {
		sa := &ShardAnalysis{
			ShardKey:                  shardKey,
			BootstrapDurabilityPolicy: policy,
			Analyses:                  []*store.Pooler{initialized("pooler-1"), initialized("pooler-2")},
		}
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Len(t, problems, 1)
		require.Equal(t, types.ProblemShardNeedsInitialization, problems[0].Code)
		require.Equal(t, types.ScopeShard, problems[0].Scope)
		require.Equal(t, types.PriorityShardBootstrap, problems[0].Priority)
		require.NotNil(t, problems[0].RecoveryAction)
	})

	t.Run("does not fire when not enough initialized poolers for quorum", func(t *testing.T) {
		sa := &ShardAnalysis{
			ShardKey:                  shardKey,
			BootstrapDurabilityPolicy: policy,
			Analyses:                  []*store.Pooler{initialized("pooler-1")},
		}
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems)
	})

	t.Run("suppresses entire shard when any pooler has cohort members", func(t *testing.T) {
		withCohort := newRider(&multiorchdatapb.PoolerHealthState{
			Multipooler:      &clustermetadatapb.Multipooler{Id: poolerIDFor("pooler-2"), ShardKey: shardKey},
			IsLastCheckValid: true,
			Status: &multipoolermanagerdatapb.Status{
				IsInitialized: true,
				CohortMembers: []*clustermetadatapb.ID{poolerIDFor("pooler-2")},
			},
		})
		sa := &ShardAnalysis{
			ShardKey:                  shardKey,
			BootstrapDurabilityPolicy: policy,
			Analyses:                  []*store.Pooler{initialized("pooler-1"), withCohort},
		}
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems)
	})

	t.Run("suppresses when any pooler is a primary (has cohort members)", func(t *testing.T) {
		// A genuine primary always has cohort members; the cohort-members check covers this case.
		withCohortAndPrimary := newRider(&multiorchdatapb.PoolerHealthState{
			Multipooler:      &clustermetadatapb.Multipooler{Id: poolerIDFor("pooler-1"), ShardKey: shardKey},
			IsLastCheckValid: true,
			ConsensusStatus:  primaryConsensusStatus(poolerIDFor("pooler-1"), 1),
			Status: &multipoolermanagerdatapb.Status{
				IsInitialized: true,
				CohortMembers: []*clustermetadatapb.ID{poolerIDFor("pooler-1")},
			},
		})
		sa := &ShardAnalysis{
			ShardKey:                  shardKey,
			BootstrapDurabilityPolicy: policy,
			Analyses:                  []*store.Pooler{withCohortAndPrimary, initialized("pooler-2")},
		}
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems)
	})

	t.Run("does not fire when bootstrap durability policy is unknown", func(t *testing.T) {
		sa := &ShardAnalysis{
			ShardKey:                  shardKey,
			BootstrapDurabilityPolicy: nil,
			Analyses:                  []*store.Pooler{initialized("pooler-1"), initialized("pooler-2")},
		}
		problems, err := analyzer.Analyze(sa)
		require.NoError(t, err)
		require.Empty(t, problems)
	})
}
