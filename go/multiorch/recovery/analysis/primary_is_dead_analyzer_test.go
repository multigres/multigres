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
	"github.com/multigres/multigres/go/multiorch/coordinator"
	"github.com/multigres/multigres/go/multiorch/recovery/types"
	"github.com/multigres/multigres/go/multiorch/store"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
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
	coord := coordinator.NewCoordinator(coordID, ts, rpcClient, slog.Default())
	factory := NewRecoveryActionFactory(poolerStore, rpcClient, ts, coord, slog.Default())
	SetRecoveryActionFactory(factory)
	defer SetRecoveryActionFactory(nil) // Clean up after test

	analyzer := &PrimaryIsDeadAnalyzer{}

	t.Run("detects dead primary (primary exists in topology but unreachable)", func(t *testing.T) {
		primaryID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"}
		analysis := &store.ReplicationAnalysis{
			PoolerID:         &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "replica1"},
			Database:         "db",
			TableGroup:       "tg",
			Shard:            "0",
			IsPrimary:        false,
			IsInitialized:    true,
			PrimaryPoolerID:  primaryID, // Primary exists in topology
			PrimaryReachable: false,     // But is unreachable (DEAD)
		}

		problems := analyzer.Analyze(analysis)
		require.Len(t, problems, 1)
		require.Equal(t, types.ProblemPrimaryIsDead, problems[0].Code)
		require.Equal(t, types.ScopeShard, problems[0].Scope)
		require.Equal(t, types.PriorityEmergency, problems[0].Priority)
		require.NotNil(t, problems[0].RecoveryAction)
	})

	t.Run("ignores healthy primary (reachable)", func(t *testing.T) {
		primaryID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"}
		analysis := &store.ReplicationAnalysis{
			IsPrimary:        false,
			IsInitialized:    true,
			PrimaryPoolerID:  primaryID, // Primary exists
			PrimaryReachable: true,      // And is reachable (HEALTHY)
		}

		problems := analyzer.Analyze(analysis)
		require.Len(t, problems, 0)
	})

	t.Run("ignores no primary scenario (future analysis)", func(t *testing.T) {
		analysis := &store.ReplicationAnalysis{
			IsPrimary:        false,
			IsInitialized:    true,
			PrimaryPoolerID:  nil,   // No primary exists in topology
			PrimaryReachable: false, // N/A
		}

		problems := analyzer.Analyze(analysis)
		require.Len(t, problems, 0) // Future analyzer will handle this case
	})

	t.Run("ignores primary itself", func(t *testing.T) {
		analysis := &store.ReplicationAnalysis{
			IsPrimary:        true, // This is the primary node
			IsInitialized:    true,
			PrimaryPoolerID:  nil,
			PrimaryReachable: false,
		}

		problems := analyzer.Analyze(analysis)
		require.Len(t, problems, 0) // Primaries don't report themselves as dead
	})

	t.Run("ignores uninitialized replica", func(t *testing.T) {
		primaryID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "primary1"}
		analysis := &store.ReplicationAnalysis{
			IsPrimary:        false,
			IsInitialized:    false, // Uninitialized
			PrimaryPoolerID:  primaryID,
			PrimaryReachable: false,
		}

		problems := analyzer.Analyze(analysis)
		require.Len(t, problems, 0) // ShardNeedsBootstrap handles uninitialized nodes
	})

	t.Run("analyzer name is correct", func(t *testing.T) {
		require.Equal(t, types.CheckName("PrimaryIsDead"), analyzer.Name())
	})
}
