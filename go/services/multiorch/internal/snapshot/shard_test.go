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

package snapshot

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

func podID(name string) *clustermetadatapb.ID {
	return &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: name}
}

func pooler(name string) *multiorchdatapb.PoolerHealthState {
	return &multiorchdatapb.PoolerHealthState{Multipooler: &clustermetadatapb.Multipooler{Id: podID(name)}}
}

// withPosition gives p the rule position pos as its own WAL position.
func withPosition(p *multiorchdatapb.PoolerHealthState, pos *clustermetadatapb.RulePosition) *multiorchdatapb.PoolerHealthState {
	p.ConsensusStatus = &clustermetadatapb.ConsensusStatus{
		Id:              p.GetMultipooler().GetId(),
		CurrentPosition: &clustermetadatapb.PoolerPosition{Position: pos},
	}
	return p
}

func ruleNumber(term int64) *clustermetadatapb.RuleNumber {
	return &clustermetadatapb.RuleNumber{CoordinatorTerm: term}
}

func TestShard_NoRuleMeansNoLeader(t *testing.T) {
	s := &Shard{Poolers: []*multiorchdatapb.PoolerHealthState{pooler("leader"), pooler("follower")}}
	assert.True(t, s.HighestIntendedRule().IsEmpty())
	assert.Nil(t, s.HighestIntendedRule().PossiblyUndecidedRule().GetLeaderId())
	assert.Nil(t, s.IntendedLeaderHealth())
}

func TestShard_PoolerLooksUpByID(t *testing.T) {
	follower := pooler("follower")
	s := &Shard{Poolers: []*multiorchdatapb.PoolerHealthState{pooler("leader"), follower}}
	assert.Same(t, follower, s.PoolerHealth(podID("follower")))
	assert.Nil(t, s.PoolerHealth(podID("absent")))
	assert.Nil(t, s.PoolerHealth(nil))
}

func TestShard_IntendedLeaderIsThePoolerTheRuleNames(t *testing.T) {
	leader := pooler("leader")
	rule := &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{RuleNumber: ruleNumber(1), LeaderId: podID("leader")}}
	s := &Shard{Poolers: []*multiorchdatapb.PoolerHealthState{withPosition(pooler("follower"), rule), leader}}
	assert.Equal(t, "leader", s.HighestIntendedRule().PossiblyUndecidedRule().GetLeaderId().GetName())
	assert.Same(t, leader, s.IntendedLeaderHealth())
}

func TestShard_IntendedLeaderWithoutHealthIsNil(t *testing.T) {
	rule := &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{RuleNumber: ruleNumber(1), LeaderId: podID("absent")}}
	s := &Shard{Poolers: []*multiorchdatapb.PoolerHealthState{withPosition(pooler("follower"), rule)}}
	require.NotNil(t, s.HighestIntendedRule().PossiblyUndecidedRule().GetLeaderId())
	assert.Nil(t, s.IntendedLeaderHealth())
}

func TestShard_OutstandingProposalNamesTheIntendedLeader(t *testing.T) {
	leader, follower := pooler("leader"), pooler("follower")
	rule := &clustermetadatapb.RulePosition{
		Decision: &clustermetadatapb.ShardRule{RuleNumber: ruleNumber(1), LeaderId: podID("follower")},
		Proposal: &clustermetadatapb.ShardRule{RuleNumber: ruleNumber(2), LeaderId: podID("leader")},
	}
	s := &Shard{Poolers: []*multiorchdatapb.PoolerHealthState{leader, withPosition(follower, rule)}}
	assert.False(t, s.HighestIntendedRule().IsDecided())
	assert.Same(t, leader, s.IntendedLeaderHealth())
}

func TestShard_AdvertisedRuleCountsAsIntended(t *testing.T) {
	// The follower learned of a newer rule only through its replication primary.
	decided := &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{RuleNumber: ruleNumber(1), LeaderId: podID("old")}}
	advertised := &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{RuleNumber: ruleNumber(2), LeaderId: podID("new")}}
	follower := withPosition(pooler("follower"), decided)
	follower.ConsensusStatus.ReplicationPrimary = &clustermetadatapb.ReplicationPrimary{Position: advertised}
	s := &Shard{Poolers: []*multiorchdatapb.PoolerHealthState{follower}}
	assert.Equal(t, "new", s.HighestIntendedRule().PossiblyUndecidedRule().GetLeaderId().GetName())
}
