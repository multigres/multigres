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

package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"

	"github.com/multigres/multigres/go/common/mterrors"
)

// shard returns the ShardKey used across this file's fixtures.
func shard() *clustermetadatapb.ShardKey {
	return &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}
}

// poolerID builds a stable component ID for a fixture pooler.
func poolerID(cell, name string) *clustermetadatapb.ID {
	return &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: cell, Name: name}
}

// withRule returns a *Pooler whose health carries a consensus rule with the
// given coordinator_term/leader_subterm naming leaderID as the rule's leader.
// Passing leaderID=nil yields a rule with no leader (still bumps the rule
// number).
func withRule(id *clustermetadatapb.ID, coordinatorTerm, leaderSubterm int64, leaderID *clustermetadatapb.ID) *Pooler {
	return NewPooler(&multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{Id: id, ShardKey: shard()},
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{
					RuleNumber: &clustermetadatapb.RuleNumber{
						CoordinatorTerm: coordinatorTerm,
						LeaderSubterm:   leaderSubterm,
					},
					LeaderId: leaderID,
				}},
			},
		},
	}, nil)
}

// withoutRule returns a *Pooler whose health has MultiPooler set but no
// ConsensusStatus — i.e. a pooler the cache knows about but who hasn't
// reported consensus state yet.
func withoutRule(id *clustermetadatapb.ID) *Pooler {
	return NewPooler(&multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{Id: id, ShardKey: shard()},
	}, nil)
}

// withProposal builds a Pooler whose decision names decisionLeaderID but
// which also carries an outstanding proposal beyond it naming
// proposalLeaderID — e.g. a self-promotion whose fresh proposal reached this
// node's WAL but was never marked decided.
func withProposal(id *clustermetadatapb.ID, decisionTerm int64, decisionLeaderID *clustermetadatapb.ID, proposalTerm int64, proposalLeaderID *clustermetadatapb.ID) *Pooler {
	return NewPooler(&multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{Id: id, ShardKey: shard()},
		ConsensusStatus: &clustermetadatapb.ConsensusStatus{
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Position: &clustermetadatapb.RulePosition{
					Decision: &clustermetadatapb.ShardRule{
						RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: decisionTerm},
						LeaderId:   decisionLeaderID,
					},
					Proposal: &clustermetadatapb.ShardRule{
						RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: proposalTerm},
						LeaderId:   proposalLeaderID,
					},
				},
			},
		},
	}, nil)
}

func TestFindPoolerByID_PresentAndAbsent(t *testing.T) {
	cache := NewTestCache(t)
	defer cache.Shutdown()

	want := withoutRule(poolerID("zone1", "p1"))
	SeedCache(t, cache, want)

	got, err := FindPoolerByID(cache, want.Health().MultiPooler.Id)
	require.NoError(t, err)
	assert.Equal(t, want.Health().MultiPooler.Id.Name, got.Health().MultiPooler.Id.Name)

	_, err = FindPoolerByID(cache, poolerID("zone1", "missing"))
	require.Error(t, err)
	assert.Equal(t, mtrpcpb.Code_NOT_FOUND, mterrors.Code(err),
		"missing pooler must return NOT_FOUND, got %v", err)
}

func TestFindShardMembers_EmptyCache(t *testing.T) {
	cache := NewTestCache(t)
	defer cache.Shutdown()

	members := FindShardMembers(cache, shard())
	assert.Empty(t, members.Poolers)
	assert.Nil(t, members.HighestKnownPosition)
	assert.Nil(t, members.Leader)
}

func TestFindShardMembers_NoConsensusStatus(t *testing.T) {
	// Poolers exist but none has reported a consensus rule yet — typical
	// shortly after discovery, before the first health stream snapshot.
	cache := NewTestCache(t)
	defer cache.Shutdown()

	SeedCache(t, cache, withoutRule(poolerID("zone1", "p1")))
	SeedCache(t, cache, withoutRule(poolerID("zone1", "p2")))

	members := FindShardMembers(cache, shard())
	assert.Len(t, members.Poolers, 2)
	assert.Nil(t, members.HighestKnownPosition, "no rule → HighestKnownRule must be nil")
	assert.Nil(t, members.Leader)
}

func TestFindShardMembers_LeaderResolvesFromHighestRule(t *testing.T) {
	// Two poolers report the same rule naming p1 as leader. Expect Leader=p1.
	cache := NewTestCache(t)
	defer cache.Shutdown()

	p1ID := poolerID("zone1", "p1")
	SeedCache(t, cache, withRule(p1ID, 3 /*term*/, 0 /*subterm*/, p1ID))
	SeedCache(t, cache, withRule(poolerID("zone1", "p2"), 3, 0, p1ID))

	members := FindShardMembers(cache, shard())
	require.NotNil(t, members.HighestKnownPosition)
	assert.Equal(t, int64(3), members.HighestKnownPosition.GetDecision().GetRuleNumber().GetCoordinatorTerm())
	require.NotNil(t, members.Leader)
	assert.Equal(t, "p1", members.Leader.Health().MultiPooler.Id.Name)
}

func TestFindShardMembers_LeaderResolvesFromUndecidedProposal(t *testing.T) {
	// p1's decision still names p2 as leader, but p1 also carries an
	// outstanding proposal (undecided) naming itself. FindShardMembers
	// resolves Leader via PossiblyUndecidedRule, so it should follow the
	// proposal, not the stale decision.
	cache := NewTestCache(t)
	defer cache.Shutdown()

	p1ID := poolerID("zone1", "p1")
	p2ID := poolerID("zone1", "p2")
	SeedCache(t, cache, withProposal(p1ID, 3 /*decisionTerm*/, p2ID, 4 /*proposalTerm*/, p1ID))
	SeedCache(t, cache, withRule(p2ID, 3, 0, p2ID))

	members := FindShardMembers(cache, shard())
	require.NotNil(t, members.HighestKnownPosition)
	require.NotNil(t, members.Leader)
	assert.Equal(t, "p1", members.Leader.Health().MultiPooler.Id.Name,
		"leader should resolve via p1's undecided proposal, not its stale decision")
}

func TestFindShardMembers_HigherRuleSupersedes(t *testing.T) {
	// p2 carries a strictly-higher rule than p1 (later coordinator term).
	// Expect that rule wins and the leader resolves to whoever it names.
	cache := NewTestCache(t)
	defer cache.Shutdown()

	p1ID := poolerID("zone1", "p1")
	p2ID := poolerID("zone1", "p2")
	SeedCache(t, cache, withRule(p1ID, 2, 0, p1ID))
	SeedCache(t, cache, withRule(p2ID, 3, 0, p2ID))

	members := FindShardMembers(cache, shard())
	require.NotNil(t, members.HighestKnownPosition)
	assert.Equal(t, int64(3), members.HighestKnownPosition.GetDecision().GetRuleNumber().GetCoordinatorTerm(),
		"higher coordinator_term wins")
	require.NotNil(t, members.Leader)
	assert.Equal(t, "p2", members.Leader.Health().MultiPooler.Id.Name)
}

func TestFindShardMembers_LeaderNamedButNotInCache(t *testing.T) {
	// A common state during failover: a follower reports a rule naming a
	// new leader the cache hasn't observed yet. HighestKnownRule must still
	// surface so callers can act on the term — but Leader is nil because
	// we can't dereference a pooler we don't have.
	cache := NewTestCache(t)
	defer cache.Shutdown()

	p1ID := poolerID("zone1", "p1")
	notInCache := poolerID("zone1", "ghost-leader")
	SeedCache(t, cache, withRule(p1ID, 5, 0, notInCache))

	members := FindShardMembers(cache, shard())
	require.NotNil(t, members.HighestKnownPosition)
	assert.Equal(t, int64(5), members.HighestKnownPosition.GetDecision().GetRuleNumber().GetCoordinatorTerm())
	assert.Equal(t, "ghost-leader", members.HighestKnownPosition.GetDecision().GetLeaderId().GetName())
	assert.Nil(t, members.Leader, "leader named by rule but not in cache → nil Leader")
}

func TestFindShardMembers_RuleWithNoLeader(t *testing.T) {
	// A rule with no LeaderId (e.g. pre-promotion or post-resignation).
	// HighestKnownRule is the rule; Leader is nil.
	cache := NewTestCache(t)
	defer cache.Shutdown()

	SeedCache(t, cache, withRule(poolerID("zone1", "p1"), 4, 0, nil /*no leader*/))

	members := FindShardMembers(cache, shard())
	require.NotNil(t, members.HighestKnownPosition)
	assert.Equal(t, int64(4), members.HighestKnownPosition.GetDecision().GetRuleNumber().GetCoordinatorTerm())
	assert.Nil(t, members.HighestKnownPosition.GetDecision().GetLeaderId())
	assert.Nil(t, members.Leader)
}

func TestFindShardMembers_OnlyShardScoped(t *testing.T) {
	// Poolers in a different shard must not appear in the result.
	cache := NewTestCache(t)
	defer cache.Shutdown()

	inShard := withoutRule(poolerID("zone1", "in"))
	otherShard := NewPooler(&multiorchdatapb.PoolerHealthState{
		MultiPooler: &clustermetadatapb.MultiPooler{
			Id:       poolerID("zone1", "other"),
			ShardKey: &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "other"},
		},
	}, nil)
	SeedCache(t, cache, inShard)
	SeedCache(t, cache, otherShard)

	members := FindShardMembers(cache, shard())
	require.Len(t, members.Poolers, 1)
	assert.Equal(t, "in", members.Poolers[0].Health().MultiPooler.Id.Name)
}
