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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commontypes "github.com/multigres/multigres/go/common/types"
	"github.com/multigres/multigres/go/multiorch/recovery/types"
	"github.com/multigres/multigres/go/multiorch/store"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestStalePrimaryAnalyzer_Analyze(t *testing.T) {
	factory := &RecoveryActionFactory{}

	t.Run("detects stale primary when this node has lower term", func(t *testing.T) {
		analyzer := &StalePrimaryAnalyzer{factory: factory}
		analysis := &store.ReplicationAnalysis{
			PoolerID: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "stale-primary",
			},
			ShardKey:      commontypes.ShardKey{Database: "db", TableGroup: "default", Shard: "0"},
			IsPrimary:     true,
			IsInitialized: true,
			ConsensusTerm: 5,
			OtherPrimaryInShard: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "new-primary",
			},
			OtherPrimaryTerm: 6, // Higher term = this node is stale
		}

		problems, err := analyzer.Analyze(analysis)

		require.NoError(t, err)
		require.Len(t, problems, 1)
		assert.Equal(t, types.ProblemStalePrimary, problems[0].Code)
		assert.Equal(t, types.ScopeShard, problems[0].Scope)
		assert.Equal(t, types.PriorityEmergency, problems[0].Priority)
		assert.Contains(t, problems[0].Description, "stale-primary")
		assert.Contains(t, problems[0].Description, "term 5")
	})

	t.Run("ignores when this node has higher term (other is stale)", func(t *testing.T) {
		analyzer := &StalePrimaryAnalyzer{factory: factory}
		analysis := &store.ReplicationAnalysis{
			PoolerID: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "new-primary",
			},
			ShardKey:      commontypes.ShardKey{Database: "db", TableGroup: "default", Shard: "0"},
			IsPrimary:     true,
			IsInitialized: true,
			ConsensusTerm: 6, // Higher term = this node is NOT stale
			OtherPrimaryInShard: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "stale-primary",
			},
			OtherPrimaryTerm: 5,
		}

		problems, err := analyzer.Analyze(analysis)

		require.NoError(t, err)
		require.Len(t, problems, 0, "should not flag this node when it has higher term")
	})

	t.Run("does not demote when terms are equal (prevents double demotion)", func(t *testing.T) {
		// When terms are equal, neither node should demote to avoid both nodes
		// demoting simultaneously and leaving the shard without a primary.
		// This unusual state should be resolved through normal failover or manual intervention.
		analyzer := &StalePrimaryAnalyzer{factory: factory}
		analysis := &store.ReplicationAnalysis{
			PoolerID: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "primary-a",
			},
			ShardKey:      commontypes.ShardKey{Database: "db", TableGroup: "default", Shard: "0"},
			IsPrimary:     true,
			IsInitialized: true,
			ConsensusTerm: 5,
			OtherPrimaryInShard: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "primary-b",
			},
			OtherPrimaryTerm: 5, // Same term - neither should demote
		}

		problems, err := analyzer.Analyze(analysis)

		require.NoError(t, err)
		require.Len(t, problems, 0, "should NOT demote when terms are equal to prevent double demotion")
	})

	t.Run("ignores replicas", func(t *testing.T) {
		analyzer := &StalePrimaryAnalyzer{factory: factory}
		analysis := &store.ReplicationAnalysis{
			PoolerID: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "replica1",
			},
			ShardKey:      commontypes.ShardKey{Database: "db", TableGroup: "default", Shard: "0"},
			IsPrimary:     false,
			IsInitialized: true,
		}

		problems, err := analyzer.Analyze(analysis)

		require.NoError(t, err)
		require.Len(t, problems, 0)
	})

	t.Run("ignores when no other primary detected", func(t *testing.T) {
		analyzer := &StalePrimaryAnalyzer{factory: factory}
		analysis := &store.ReplicationAnalysis{
			PoolerID: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "primary",
			},
			ShardKey:            commontypes.ShardKey{Database: "db", TableGroup: "default", Shard: "0"},
			IsPrimary:           true,
			IsInitialized:       true,
			ConsensusTerm:       5,
			OtherPrimaryInShard: nil, // No other primary
		}

		problems, err := analyzer.Analyze(analysis)

		require.NoError(t, err)
		require.Len(t, problems, 0)
	})

	t.Run("ignores uninitialized primary", func(t *testing.T) {
		analyzer := &StalePrimaryAnalyzer{factory: factory}
		analysis := &store.ReplicationAnalysis{
			PoolerID: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "primary",
			},
			ShardKey:      commontypes.ShardKey{Database: "db", TableGroup: "default", Shard: "0"},
			IsPrimary:     true,
			IsInitialized: false, // Not initialized
			OtherPrimaryInShard: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "other-primary",
			},
		}

		problems, err := analyzer.Analyze(analysis)

		require.NoError(t, err)
		require.Len(t, problems, 0)
	})

	t.Run("returns error when factory is nil", func(t *testing.T) {
		analyzer := &StalePrimaryAnalyzer{factory: nil}
		analysis := &store.ReplicationAnalysis{IsPrimary: true}

		_, err := analyzer.Analyze(analysis)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "factory not initialized")
	})

	t.Run("analyzer name is correct", func(t *testing.T) {
		analyzer := &StalePrimaryAnalyzer{factory: factory}
		assert.Equal(t, types.CheckName("StalePrimary"), analyzer.Name())
	})
}
