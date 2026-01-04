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

func TestShardNeedsBootstrapAnalyzer_Analyze(t *testing.T) {
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
	coord := coordinator.NewCoordinator(coordID, ts, rpcClient, slog.Default())
	factory := NewRecoveryActionFactory(poolerStore, rpcClient, ts, coord, slog.Default())

	analyzer := &ShardNeedsBootstrapAnalyzer{factory: factory}

	t.Run("detects uninitialized shard", func(t *testing.T) {
		analysis := &store.ReplicationAnalysis{
			PoolerID:         &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"},
			ShardKey:         commontypes.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
			IsInitialized:    false,
			HasDataDirectory: false, // Explicitly set - no data directory
			PrimaryPoolerID:  nil,   // no primary exists
		}

		problems, err := analyzer.Analyze(analysis)
		require.NoError(t, err)
		require.Len(t, problems, 1)
		require.Equal(t, types.ProblemShardNeedsBootstrap, problems[0].Code)
		require.Equal(t, types.ScopeShard, problems[0].Scope)
		require.Equal(t, types.PriorityShardBootstrap, problems[0].Priority)
		require.NotNil(t, problems[0].RecoveryAction)
	})

	t.Run("ignores initialized pooler", func(t *testing.T) {
		analysis := &store.ReplicationAnalysis{
			IsInitialized:   true,
			PrimaryPoolerID: nil,
		}

		problems, err := analyzer.Analyze(analysis)
		require.NoError(t, err)
		require.Len(t, problems, 0)
	})

	t.Run("ignores uninitialized pooler if primary exists", func(t *testing.T) {
		analysis := &store.ReplicationAnalysis{
			IsInitialized:   false,
			PrimaryPoolerID: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"},
		}

		problems, err := analyzer.Analyze(analysis)
		require.NoError(t, err)
		require.Len(t, problems, 0)
	})

	t.Run("detects bootstrap needed for REPLICA type without data directory", func(t *testing.T) {
		analysis := &store.ReplicationAnalysis{
			PoolerID:         &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"},
			ShardKey:         commontypes.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
			PoolerType:       clustermetadatapb.PoolerType_REPLICA, // REPLICA type
			IsInitialized:    false,
			HasDataDirectory: false, // No data directory
			PrimaryPoolerID:  nil,   // no primary exists
		}

		problems, err := analyzer.Analyze(analysis)
		require.NoError(t, err)
		require.Len(t, problems, 1)
		require.Equal(t, types.ProblemShardNeedsBootstrap, problems[0].Code)
	})

	t.Run("skips REPLICA type with data directory", func(t *testing.T) {
		analysis := &store.ReplicationAnalysis{
			PoolerID:         &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "pooler1"},
			ShardKey:         commontypes.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"},
			PoolerType:       clustermetadatapb.PoolerType_REPLICA, // REPLICA type
			IsInitialized:    false,                                // might be temporarily down
			HasDataDirectory: true,                                 // Has data directory
			PrimaryPoolerID:  nil,
		}

		problems, err := analyzer.Analyze(analysis)
		require.NoError(t, err)
		require.Len(t, problems, 0)
	})

	t.Run("analyzer name is correct", func(t *testing.T) {
		require.Equal(t, types.CheckName("ShardNeedsBootstrap"), analyzer.Name())
	})
}
