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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

func TestShardAnalysis_DerivedLookups(t *testing.T) {
	podID := func(name string) *clustermetadatapb.ID {
		return &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: name}
	}
	pooler := func(name string) *multiorchdatapb.PoolerHealthState {
		return &multiorchdatapb.PoolerHealthState{Multipooler: &clustermetadatapb.Multipooler{Id: podID(name)}}
	}
	leader, follower := pooler("leader"), pooler("follower")

	t.Run("no rule means no leader", func(t *testing.T) {
		sa := &ShardAnalysis{Poolers: []*multiorchdatapb.PoolerHealthState{leader, follower}}
		assert.Nil(t, sa.HighestIntendedRule())
		assert.Nil(t, sa.IntendedLeaderID())
		assert.Nil(t, sa.IntendedLeader())
	})

	t.Run("looks up poolers by ID", func(t *testing.T) {
		sa := &ShardAnalysis{Poolers: []*multiorchdatapb.PoolerHealthState{leader, follower}}
		assert.Same(t, follower, sa.Pooler(podID("follower")))
		assert.Nil(t, sa.Pooler(podID("absent")))
		assert.Nil(t, sa.Pooler(nil))
	})

	t.Run("leader is the pooler the rule names", func(t *testing.T) {
		sa := newShardAnalysis(&ShardAnalysis{Poolers: []*multiorchdatapb.PoolerHealthState{follower}},
			&clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{LeaderId: podID("leader")}}, leader)
		assert.Equal(t, "leader", sa.IntendedLeaderID().GetName())
		assert.Same(t, leader, sa.IntendedLeader())
	})

	t.Run("a named leader we have no health for yields a nil Leader", func(t *testing.T) {
		sa := newShardAnalysis(&ShardAnalysis{Poolers: []*multiorchdatapb.PoolerHealthState{follower}},
			&clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{LeaderId: podID("absent")}}, nil)
		require.NotNil(t, sa.IntendedLeaderID())
		assert.Nil(t, sa.IntendedLeader())
	})

	t.Run("the outstanding proposal names the leader over the decision", func(t *testing.T) {
		sa := newShardAnalysis(&ShardAnalysis{Poolers: []*multiorchdatapb.PoolerHealthState{leader, follower}},
			&clustermetadatapb.RulePosition{
				Decision: &clustermetadatapb.ShardRule{LeaderId: podID("follower"), RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1}},
				Proposal: &clustermetadatapb.ShardRule{LeaderId: podID("leader"), RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 2}},
			}, nil)
		assert.Equal(t, int64(2), commonconsensus.PossiblyUndecidedRule(sa.HighestIntendedRule()).GetRuleNumber().GetCoordinatorTerm())
		assert.Same(t, leader, sa.IntendedLeader())
	})
}
