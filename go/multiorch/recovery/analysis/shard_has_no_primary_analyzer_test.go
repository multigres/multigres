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

	"github.com/multigres/multigres/go/clustermetadata/topo/memorytopo"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/multiorch/coordinator"
	"github.com/multigres/multigres/go/multiorch/store"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

func TestShardHasNoPrimaryAnalyzer_Analyze(t *testing.T) {
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
	SetRecoveryActionFactory(factory)
	defer SetRecoveryActionFactory(nil) // Clean up after test

	analyzer := &ShardHasNoPrimaryAnalyzer{}

	t.Run("detects initialized shard with no primary", func(t *testing.T) {
		analysis := &store.ReplicationAnalysis{
			PoolerID:        &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica1"},
			Database:        "db",
			TableGroup:      "tg",
			Shard:           "0",
			IsPrimary:       false,
			IsInitialized:   true,
			PrimaryPoolerID: nil, // no primary exists
		}

		problems := analyzer.Analyze(analysis)
		require.Len(t, problems, 1)
		require.Equal(t, ProblemShardHasNoPrimary, problems[0].Code)
		require.Equal(t, ScopeShard, problems[0].Scope)
		require.Equal(t, PriorityShardBootstrap, problems[0].Priority)
		require.NotNil(t, problems[0].RecoveryAction)
	})

	t.Run("ignores primary", func(t *testing.T) {
		analysis := &store.ReplicationAnalysis{
			IsPrimary:       true,
			IsInitialized:   true,
			PrimaryPoolerID: nil,
		}

		problems := analyzer.Analyze(analysis)
		require.Len(t, problems, 0)
	})

	t.Run("ignores uninitialized replica", func(t *testing.T) {
		analysis := &store.ReplicationAnalysis{
			IsPrimary:       false,
			IsInitialized:   false,
			PrimaryPoolerID: nil,
		}

		problems := analyzer.Analyze(analysis)
		require.Len(t, problems, 0)
	})

	t.Run("ignores replica with primary", func(t *testing.T) {
		analysis := &store.ReplicationAnalysis{
			IsPrimary:       false,
			IsInitialized:   true,
			PrimaryPoolerID: &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"},
		}

		problems := analyzer.Analyze(analysis)
		require.Len(t, problems, 0)
	})

	t.Run("analyzer name is correct", func(t *testing.T) {
		require.Equal(t, CheckName("ShardHasNoPrimary"), analyzer.Name())
	})
}
