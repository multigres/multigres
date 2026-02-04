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
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

func TestStalePrimaryAnalyzer_Analyze(t *testing.T) {
	factory := &RecoveryActionFactory{}

	t.Run("detects stale primary when this pooler has lower primary_term", func(t *testing.T) {
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
			PrimaryTerm:   5,
			ConsensusTerm: 10,
			OtherPrimariesInShard: []*store.PrimaryInfo{
				{
					ID: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      "cell1",
						Name:      "new-primary",
					},
					ConsensusTerm: 11,
					PrimaryTerm:   6,
				},
			},
			MostAdvancedPrimary: &store.PrimaryInfo{
				ID: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "cell1",
					Name:      "new-primary",
				},
				ConsensusTerm: 11,
				PrimaryTerm:   6,
			},
		}

		problem, err := analyzer.Analyze(analysis)

		require.NoError(t, err)
		require.NotNil(t, problem)
		assert.Equal(t, types.ProblemStalePrimary, problem.Code)
		assert.Equal(t, types.ScopeShard, problem.Scope)
		assert.Equal(t, types.PriorityEmergency, problem.Priority)
		assert.Contains(t, problem.Description, "stale-primary")
		assert.Contains(t, problem.Description, "stale_primary_term 5")
		assert.Contains(t, problem.Description, "most_advanced_primary_term 6")
	})

	t.Run("detects other primary as stale when this pooler has higher primary_term", func(t *testing.T) {
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
			PrimaryTerm:   6,
			ConsensusTerm: 11,
			OtherPrimariesInShard: []*store.PrimaryInfo{
				{
					ID: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      "cell1",
						Name:      "stale-primary",
					},
					ConsensusTerm: 10,
					PrimaryTerm:   5,
				},
			},
			MostAdvancedPrimary: &store.PrimaryInfo{
				ID: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "cell1",
					Name:      "new-primary",
				},
				ConsensusTerm: 11,
				PrimaryTerm:   6,
			},
		}

		problem, err := analyzer.Analyze(analysis)

		require.NoError(t, err)
		require.NotNil(t, problem, "should detect other primary as stale")
		assert.Equal(t, types.ProblemStalePrimary, problem.Code)
		assert.Equal(t, "stale-primary", problem.PoolerID.Name, "should report the stale primary")
		assert.Contains(t, problem.Description, "stale-primary (stale_primary_term 5) is stale")
		assert.Contains(t, problem.Description, "new-primary (most_advanced_primary_term 6)")
	})

	t.Run("does not demote when primary_terms are equal (prevents double demotion)", func(t *testing.T) {
		// When primary_terms are equal, neither pooler should demote to avoid both poolers
		// demoting simultaneously and leaving the shard without a primary.
		// This unusual state should be resolved through manual intervention.
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
			PrimaryTerm:   5,
			ConsensusTerm: 10,
			OtherPrimariesInShard: []*store.PrimaryInfo{
				{
					ID: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      "cell1",
						Name:      "primary-b",
					},
					ConsensusTerm: 10,
					PrimaryTerm:   5,
				},
			},
			MostAdvancedPrimary: nil, // Tie detected, so nil
		}

		problem, err := analyzer.Analyze(analysis)

		require.NoError(t, err)
		require.Nil(t, problem, "should NOT demote when primary_terms are equal to prevent double demotion")
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

		problem, err := analyzer.Analyze(analysis)

		require.NoError(t, err)
		require.Nil(t, problem)
	})

	t.Run("ignores when no other primary detected", func(t *testing.T) {
		analyzer := &StalePrimaryAnalyzer{factory: factory}
		analysis := &store.ReplicationAnalysis{
			PoolerID: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "primary",
			},
			ShardKey:              commontypes.ShardKey{Database: "db", TableGroup: "default", Shard: "0"},
			IsPrimary:             true,
			IsInitialized:         true,
			PrimaryTerm:           5,
			ConsensusTerm:         10,
			OtherPrimariesInShard: nil, // No other primaries
		}

		problem, err := analyzer.Analyze(analysis)

		require.NoError(t, err)
		require.Nil(t, problem)
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
			OtherPrimariesInShard: []*store.PrimaryInfo{
				{
					ID: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      "cell1",
						Name:      "other-primary",
					},
					ConsensusTerm: 10,
					PrimaryTerm:   5,
				},
			},
		}

		problem, err := analyzer.Analyze(analysis)

		require.NoError(t, err)
		require.Nil(t, problem)
	})

	t.Run("returns error when factory is nil", func(t *testing.T) {
		analyzer := &StalePrimaryAnalyzer{factory: nil}
		analysis := &store.ReplicationAnalysis{IsPrimary: true}

		_, err := analyzer.Analyze(analysis)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "factory not initialized")
	})

	t.Run("handles multiple other primaries", func(t *testing.T) {
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
			PrimaryTerm:   6,
			ConsensusTerm: 11,
			OtherPrimariesInShard: []*store.PrimaryInfo{
				{
					ID: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      "cell1",
						Name:      "stale-primary-1",
					},
					ConsensusTerm: 9,
					PrimaryTerm:   4,
				},
				{
					ID: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      "cell1",
						Name:      "stale-primary-2",
					},
					ConsensusTerm: 10,
					PrimaryTerm:   5,
				},
			},
			MostAdvancedPrimary: &store.PrimaryInfo{
				ID: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "cell1",
					Name:      "new-primary",
				},
				ConsensusTerm: 11,
				PrimaryTerm:   6,
			},
		}

		problem, err := analyzer.Analyze(analysis)

		require.NoError(t, err)
		require.NotNil(t, problem)
		// Should report first stale primary (they'll be demoted one at a time)
		assert.Equal(t, "stale-primary-1", problem.PoolerID.Name)
	})

	t.Run("skips when this pooler has primary_term zero (invalid state)", func(t *testing.T) {
		// Note: This tests the invariant check. In a properly initialized shard,
		// PRIMARY poolers should never have PrimaryTerm=0.
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
			PrimaryTerm:   0, // Invalid: initialized PRIMARY should never have PrimaryTerm=0
			ConsensusTerm: 10,
			OtherPrimariesInShard: []*store.PrimaryInfo{
				{
					ID: &clustermetadatapb.ID{
						Component: clustermetadatapb.ID_MULTIPOOLER,
						Cell:      "cell1",
						Name:      "primary-b",
					},
					ConsensusTerm: 10,
					PrimaryTerm:   5,
				},
			},
			MostAdvancedPrimary: &store.PrimaryInfo{
				ID: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "cell1",
					Name:      "primary-b",
				},
				ConsensusTerm: 10,
				PrimaryTerm:   5,
			},
		}

		problem, err := analyzer.Analyze(analysis)

		require.NoError(t, err)
		require.Nil(t, problem, "should skip when this pooler's PrimaryTerm=0 (invalid state)")
	})

	t.Run("analyzer name is correct", func(t *testing.T) {
		analyzer := &StalePrimaryAnalyzer{factory: factory}
		assert.Equal(t, types.CheckName("StalePrimary"), analyzer.Name())
	})
}
