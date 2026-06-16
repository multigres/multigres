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

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// primaryRuleStatus builds a ConsensusStatus that names id as the primary
// with the given coordinator term — shorthand for wiring PoolerAnalysis so
// commonconsensus.LeaderTerm returns term.
func primaryRuleStatus(id *clustermetadatapb.ID, term int64) *clustermetadatapb.ConsensusStatus {
	return &clustermetadatapb.ConsensusStatus{
		Id: id,
		CurrentPosition: &clustermetadatapb.PoolerPosition{
			Rule: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term},
				LeaderId:   id,
			},
		},
	}
}

func TestStaleLeaderAnalyzer_Analyze(t *testing.T) {
	factory := &RecoveryActionFactory{poolerStore: store.NewPoolerStore()}

	t.Run("detects stale primary when this pooler has lower primary_term", func(t *testing.T) {
		analyzer := &StaleLeaderAnalyzer{factory: factory}
		staleID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "stale-primary"}
		newID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "new-primary"}
		stalePA := &PoolerAnalysis{
			PoolerID:          staleID,
			ShardKey:          &clustermetadatapb.ShardKey{Database: "db", TableGroup: "default", Shard: "0"},
			NamesSelfAsLeader: true,
			IsInitialized:     true,
			LastCheckValid:    true,
			ConsensusStatus:   primaryRuleStatus(staleID, 5),
			ConsensusTerm:     10,
		}
		sa := &ShardAnalysis{
			ShardKey: stalePA.ShardKey,
			Analyses: []*PoolerAnalysis{stalePA},
			HighestShardRule: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 6},
				LeaderId:   newID,
			},
		}

		problems, err := analyzer.Analyze(sa)

		require.NoError(t, err)
		require.Len(t, problems, 1)
		problem := problems[0]
		assert.Equal(t, types.ProblemStaleLeader, problem.Code)
		assert.Equal(t, types.ScopeShard, problem.Scope)
		assert.Equal(t, types.PriorityEmergency, problem.Priority, "single stale primary should get PriorityEmergency")
		assert.Contains(t, problem.Description, "stale-primary")
		assert.Contains(t, problem.Description, "stale_leader_term 5")
		assert.Contains(t, problem.Description, "leader_term 6")
	})

	t.Run("detects other primary as stale when this pooler has higher primary_term", func(t *testing.T) {
		analyzer := &StaleLeaderAnalyzer{factory: factory}
		newID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "new-primary"}
		staleID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "stale-primary"}
		newPA := &PoolerAnalysis{
			PoolerID:          newID,
			ShardKey:          &clustermetadatapb.ShardKey{Database: "db", TableGroup: "default", Shard: "0"},
			NamesSelfAsLeader: true,
			IsInitialized:     true,
			ConsensusStatus:   primaryRuleStatus(newID, 6),
			ConsensusTerm:     11,
		}
		stalePA := &PoolerAnalysis{
			PoolerID:        staleID,
			ShardKey:        newPA.ShardKey,
			LastCheckValid:  true,
			ConsensusStatus: primaryRuleStatus(staleID, 5),
			ConsensusTerm:   10,
		}
		sa := &ShardAnalysis{
			ShardKey: newPA.ShardKey,
			Analyses: []*PoolerAnalysis{newPA, stalePA},
			HighestShardRule: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 6},
				LeaderId:   newID,
			},
		}

		problems, err := analyzer.Analyze(sa)

		require.NoError(t, err)
		require.Len(t, problems, 1, "should detect other primary as stale")
		problem := problems[0]
		assert.Equal(t, types.ProblemStaleLeader, problem.Code)
		assert.Equal(t, "stale-primary", problem.PoolerID.Name, "should report the stale primary")
		assert.Contains(t, problem.Description, "stale-primary (stale_leader_term 5) is stale")
		assert.Contains(t, problem.Description, "new-primary (leader_term 6)")
	})

	t.Run("ignores replicas", func(t *testing.T) {
		analyzer := &StaleLeaderAnalyzer{factory: factory}
		analysis := &PoolerAnalysis{
			PoolerID: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "cell1",
				Name:      "replica1",
			},
			ShardKey:          &clustermetadatapb.ShardKey{Database: "db", TableGroup: "default", Shard: "0"},
			NamesSelfAsLeader: false,
			IsInitialized:     true,
		}

		problem, err := analyzeOne(analyzer, analysis)

		require.NoError(t, err)
		require.Nil(t, problem)
	})

	t.Run("ignores when no other primary detected", func(t *testing.T) {
		analyzer := &StaleLeaderAnalyzer{factory: factory}
		primaryID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "primary"}
		pa := &PoolerAnalysis{
			PoolerID:          primaryID,
			ShardKey:          &clustermetadatapb.ShardKey{Database: "db", TableGroup: "default", Shard: "0"},
			NamesSelfAsLeader: true,
			IsInitialized:     true,
			ConsensusStatus:   primaryRuleStatus(primaryID, 5),
			ConsensusTerm:     10,
		}
		sa := &ShardAnalysis{
			ShardKey: pa.ShardKey,
			Analyses: []*PoolerAnalysis{pa}, // Only one primary — it is the leader, no stale primary to detect
			HighestShardRule: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
				LeaderId:   primaryID,
			},
		}

		problems, err := analyzer.Analyze(sa)

		require.NoError(t, err)
		require.Empty(t, problems)
	})

	t.Run("returns error when factory is nil", func(t *testing.T) {
		analyzer := &StaleLeaderAnalyzer{factory: nil}
		analysis := &PoolerAnalysis{NamesSelfAsLeader: true}

		_, err := analyzeOne(analyzer, analysis)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "factory not initialized")
	})

	t.Run("handles multiple other primaries", func(t *testing.T) {
		analyzer := &StaleLeaderAnalyzer{factory: factory}
		newID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "new-primary"}
		stale1ID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "stale-primary-1"}
		stale2ID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "stale-primary-2"}
		newPA := &PoolerAnalysis{
			PoolerID:          newID,
			ShardKey:          &clustermetadatapb.ShardKey{Database: "db", TableGroup: "default", Shard: "0"},
			NamesSelfAsLeader: true,
			IsInitialized:     true,
			ConsensusStatus:   primaryRuleStatus(newID, 6),
			ConsensusTerm:     11,
		}
		stale1PA := &PoolerAnalysis{
			PoolerID:        stale1ID,
			ShardKey:        newPA.ShardKey,
			LastCheckValid:  true,
			ConsensusStatus: primaryRuleStatus(stale1ID, 4),
		}
		stale2PA := &PoolerAnalysis{
			PoolerID:        stale2ID,
			ShardKey:        newPA.ShardKey,
			LastCheckValid:  true,
			ConsensusStatus: primaryRuleStatus(stale2ID, 5),
		}
		sa := &ShardAnalysis{
			ShardKey: newPA.ShardKey,
			Analyses: []*PoolerAnalysis{newPA, stale1PA, stale2PA},
			HighestShardRule: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 6},
				LeaderId:   newID,
			},
		}

		problems, err := analyzer.Analyze(sa)

		require.NoError(t, err)
		require.Len(t, problems, 2)
		// Most stale (lowest PrimaryTerm) should be first with highest priority
		assert.Equal(t, "stale-primary-1", problems[0].PoolerID.Name)
		assert.Equal(t, types.PriorityEmergency, problems[0].Priority)
		assert.Equal(t, "stale-primary-2", problems[1].PoolerID.Name)
		assert.Equal(t, types.PriorityEmergency-1, problems[1].Priority)
	})

	t.Run("analyzer name is correct", func(t *testing.T) {
		analyzer := &StaleLeaderAnalyzer{factory: factory}
		assert.Equal(t, types.CheckName("StaleLeader"), analyzer.Name())
	})
}
