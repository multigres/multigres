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

package consensus

import (
	"testing"

	"github.com/stretchr/testify/assert"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func rn(term, subterm int64) *clustermetadatapb.RuleNumber {
	return &clustermetadatapb.RuleNumber{CoordinatorTerm: term, LeaderSubterm: subterm}
}

func TestRuleNamesLeader(t *testing.T) {
	id := func(cell, name string) *clustermetadatapb.ID {
		return &clustermetadatapb.ID{Cell: cell, Name: name}
	}
	self := id("zone1", "pooler-1")

	tests := []struct {
		name string
		rule *clustermetadatapb.ShardRule
		id   *clustermetadatapb.ID
		want bool
	}{
		{name: "nil rule", rule: nil, id: self, want: false},
		{name: "nil leader id", rule: &clustermetadatapb.ShardRule{}, id: self, want: false},
		{name: "nil leader id and nil id", rule: &clustermetadatapb.ShardRule{}, id: nil, want: false},
		{name: "leader present but nil id", rule: &clustermetadatapb.ShardRule{LeaderId: self}, id: nil, want: false},
		{name: "leader matches", rule: &clustermetadatapb.ShardRule{LeaderId: self}, id: self, want: true},
		{name: "leader differs", rule: &clustermetadatapb.ShardRule{LeaderId: id("zone1", "pooler-2")}, id: self, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, RuleNamesLeader(tt.rule, tt.id))
		})
	}
}

func pos(term int64, lsn string) *clustermetadatapb.PoolerPosition {
	return &clustermetadatapb.PoolerPosition{
		Position:   &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{RuleNumber: rn(term, 0)}},
		FlushedLsn: lsn,
	}
}

func TestCompareRuleNumbers(t *testing.T) {
	tests := []struct {
		name string
		a, b *clustermetadatapb.RuleNumber
		want int
	}{
		{"equal", rn(5, 3), rn(5, 3), 0},
		{"higher term wins", rn(6, 0), rn(5, 99), 1},
		{"lower term loses", rn(4, 99), rn(5, 0), -1},
		{"same term higher subterm", rn(5, 4), rn(5, 3), 1},
		{"same term lower subterm", rn(5, 2), rn(5, 3), -1},
		{"nil treated as zero", nil, rn(0, 0), 0},
		{"nil less than non-zero", nil, rn(1, 0), -1},
		{"both nil", nil, nil, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CompareRuleNumbers(tt.a, tt.b)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestMostAdvancedPosition(t *testing.T) {
	mkID := func(name string) *clustermetadatapb.ID {
		return &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      name,
		}
	}
	status := func(name string, p *clustermetadatapb.PoolerPosition) *clustermetadatapb.ConsensusStatus {
		return &clustermetadatapb.ConsensusStatus{Id: mkID(name), CurrentPosition: p}
	}

	t.Run("empty input returns nil", func(t *testing.T) {
		got := MostAdvancedPosition(nil)
		assert.Nil(t, got)
	})

	t.Run("all statuses have unparsable LSN: returns nil", func(t *testing.T) {
		got := MostAdvancedPosition([]*clustermetadatapb.ConsensusStatus{
			status("a", pos(3, "")),
			status("b", pos(3, "bad-lsn")),
		})
		assert.Nil(t, got)
	})

	t.Run("highest rule wins regardless of LSN", func(t *testing.T) {
		got := MostAdvancedPosition([]*clustermetadatapb.ConsensusStatus{
			status("a", pos(2, "0/9000000")),
			status("b", pos(4, "0/100")),
			status("c", pos(3, "0/500000")),
		})
		assert.Equal(t, int64(4), got.GetPosition().GetDecision().GetRuleNumber().GetCoordinatorTerm())
		assert.Equal(t, "0/100", got.GetFlushedLsn())
	})

	t.Run("same rule highest LSN wins", func(t *testing.T) {
		got := MostAdvancedPosition([]*clustermetadatapb.ConsensusStatus{
			status("a", pos(3, "0/100")),
			status("b", pos(3, "0/300")),
			status("c", pos(3, "0/200")),
		})
		assert.Equal(t, "0/300", got.GetFlushedLsn())
	})

	t.Run("skips statuses with unparsable LSN", func(t *testing.T) {
		got := MostAdvancedPosition([]*clustermetadatapb.ConsensusStatus{
			status("a", pos(5, "bad-lsn")),
			status("b", pos(3, "0/100")),
		})
		// pooler-a's rule is higher (5) but its LSN is unparsable, so it's
		// filtered out and pooler-b wins despite the lower rule.
		assert.Equal(t, int64(3), got.GetPosition().GetDecision().GetRuleNumber().GetCoordinatorTerm())
		assert.Equal(t, "0/100", got.GetFlushedLsn())
	})
}

func TestReplicationPrimaryMatches(t *testing.T) {
	mkID := func(name string) *clustermetadatapb.ID {
		return &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "zone1",
			Name:      name,
		}
	}
	mkAddr := func(name, host string, pgPort int32) *clustermetadatapb.PoolerAddress {
		return &clustermetadatapb.PoolerAddress{
			Id:           mkID(name),
			Host:         host,
			PostgresPort: pgPort,
		}
	}
	mkRule := func(term int64) *clustermetadatapb.ShardRule {
		return &clustermetadatapb.ShardRule{RuleNumber: rn(term, 0)}
	}
	mkPosition := func(rule *clustermetadatapb.ShardRule) *clustermetadatapb.RulePosition {
		return &clustermetadatapb.RulePosition{Decision: rule}
	}
	mkRP := func(rule *clustermetadatapb.ShardRule, primary *clustermetadatapb.PoolerAddress) *clustermetadatapb.ReplicationPrimary {
		return &clustermetadatapb.ReplicationPrimary{Position: mkPosition(rule), Primary: primary}
	}

	target := mkAddr("primary-1", "host-a", 5432)
	targetRule := mkRule(3)
	targetPosition := mkPosition(targetRule)

	t.Run("nil rp returns false", func(t *testing.T) {
		assert.False(t, ReplicationPrimaryMatches(nil, target, targetPosition))
	})

	t.Run("nil target returns false", func(t *testing.T) {
		rp := mkRP(targetRule, target)
		assert.False(t, ReplicationPrimaryMatches(rp, nil, targetPosition))
	})

	t.Run("nil targetRule returns false", func(t *testing.T) {
		rp := mkRP(targetRule, target)
		assert.False(t, ReplicationPrimaryMatches(rp, target, nil))
	})

	t.Run("published rule older than targetRule returns false", func(t *testing.T) {
		rp := mkRP(mkRule(2), target)
		assert.False(t, ReplicationPrimaryMatches(rp, target, targetPosition))
	})

	t.Run("published primary missing returns false", func(t *testing.T) {
		rp := mkRP(targetRule, nil)
		assert.False(t, ReplicationPrimaryMatches(rp, target, targetPosition))
	})

	t.Run("primary id mismatch returns false", func(t *testing.T) {
		rp := mkRP(targetRule, mkAddr("primary-2", "host-a", 5432))
		assert.False(t, ReplicationPrimaryMatches(rp, target, targetPosition))
	})

	t.Run("primary id different cell returns false", func(t *testing.T) {
		other := mkAddr("primary-1", "host-a", 5432)
		other.Id.Cell = "zone2"
		rp := mkRP(targetRule, other)
		assert.False(t, ReplicationPrimaryMatches(rp, target, targetPosition))
	})

	t.Run("primary id different component returns false", func(t *testing.T) {
		other := mkAddr("primary-1", "host-a", 5432)
		other.Id.Component = clustermetadatapb.ID_MULTIGATEWAY
		rp := mkRP(targetRule, other)
		assert.False(t, ReplicationPrimaryMatches(rp, target, targetPosition))
	})

	t.Run("primary hostname mismatch returns false", func(t *testing.T) {
		rp := mkRP(targetRule, mkAddr("primary-1", "host-b", 5432))
		assert.False(t, ReplicationPrimaryMatches(rp, target, targetPosition))
	})

	t.Run("primary port mismatch returns false", func(t *testing.T) {
		rp := mkRP(targetRule, mkAddr("primary-1", "host-a", 5433))
		assert.False(t, ReplicationPrimaryMatches(rp, target, targetPosition))
	})

	t.Run("exact match returns true", func(t *testing.T) {
		rp := mkRP(targetRule, target)
		assert.True(t, ReplicationPrimaryMatches(rp, target, targetPosition))
	})

	t.Run("published rule newer than targetRule still matches", func(t *testing.T) {
		// Coordinator's targetRule may lag the pooler's published rule —
		// "no older than" means published >= target.
		rp := mkRP(mkRule(5), target)
		assert.True(t, ReplicationPrimaryMatches(rp, target, targetPosition))
	})
}

func TestComparePosition(t *testing.T) {
	tests := []struct {
		name string
		a, b *clustermetadatapb.PoolerPosition
		want int
	}{
		// Rule number takes absolute precedence over LSN.
		{"higher rule wins regardless of LSN", pos(4, "0/100"), pos(3, "0/9999999"), 1},
		{"lower rule loses regardless of LSN", pos(3, "0/9999999"), pos(4, "0/100"), -1},

		// Within the same rule, LSN is the tiebreaker.
		{"same rule higher LSN wins", pos(3, "0/200"), pos(3, "0/100"), 1},
		{"same rule lower LSN loses", pos(3, "0/100"), pos(3, "0/200"), -1},
		{"same rule equal LSNs", pos(3, "0/100"), pos(3, "0/100"), 0},

		// A valid LSN is more authoritative than an invalid or missing one at the
		// same rule: the node with no LSN cannot prove its position.
		{"same rule valid LSN beats empty LSN", pos(3, "0/100"), pos(3, ""), 1},
		{"same rule empty LSN loses to valid LSN", pos(3, ""), pos(3, "0/100"), -1},
		{"same rule valid LSN beats unparsable LSN", pos(3, "0/100"), pos(3, "not-an-lsn"), 1},

		// Both positions invalid at the same rule — neither is ahead.
		{"same rule both empty LSN", pos(3, ""), pos(3, ""), 0},
		{"same rule both unparsable LSN", pos(3, "bad"), pos(3, "also-bad"), 0},

		// Nil positions are treated as the minimum.
		{"both nil", nil, nil, 0},
		{"nil less than non-nil", nil, pos(1, "0/100"), -1},
		{"non-nil greater than nil", pos(1, "0/100"), nil, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ComparePoolerPosition(tt.a, tt.b)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestFormatRulePosition(t *testing.T) {
	decisionOnly := &clustermetadatapb.ShardRule{RuleNumber: rn(5, 0)}
	proposal := &clustermetadatapb.ShardRule{RuleNumber: rn(6, 0)}

	tests := []struct {
		name string
		pos  *clustermetadatapb.RulePosition
		want string
	}{
		{"nil position", nil, "none"},
		{"decision only", &clustermetadatapb.RulePosition{Decision: decisionOnly}, "5.0"},
		{"decision and proposal", &clustermetadatapb.RulePosition{Decision: decisionOnly, Proposal: proposal}, "5.0 proposal=6.0"},
		{
			// A zero-valued (unset-sentinel) Proposal must not render as a
			// phantom "proposal=0.0" suffix.
			"decision with zero-valued proposal",
			&clustermetadatapb.RulePosition{Decision: decisionOnly, Proposal: &clustermetadatapb.ShardRule{}},
			"5.0",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, FormatRulePosition(tt.pos))
		})
	}
}

func TestIsRuleDecided(t *testing.T) {
	decision := &clustermetadatapb.ShardRule{RuleNumber: rn(5, 0)}
	proposal := &clustermetadatapb.ShardRule{RuleNumber: rn(6, 0)}

	assert.True(t, IsRuleDecided(&clustermetadatapb.RulePosition{Decision: decision}), "no proposal at all")
	assert.True(t, IsRuleDecided(&clustermetadatapb.RulePosition{Decision: decision, Proposal: &clustermetadatapb.ShardRule{}}), "zero-valued proposal is the unset sentinel")
	assert.False(t, IsRuleDecided(&clustermetadatapb.RulePosition{Decision: decision, Proposal: proposal}), "real outstanding proposal")
}

func TestPossiblyUndecidedRule(t *testing.T) {
	decision := &clustermetadatapb.ShardRule{RuleNumber: rn(5, 0)}
	proposal := &clustermetadatapb.ShardRule{RuleNumber: rn(6, 0)}

	assert.Nil(t, PossiblyUndecidedRule(nil))
	assert.Equal(t, decision, PossiblyUndecidedRule(&clustermetadatapb.RulePosition{Decision: decision}), "no proposal: returns decision")
	assert.Equal(t, proposal, PossiblyUndecidedRule(&clustermetadatapb.RulePosition{Decision: decision, Proposal: proposal}), "outstanding proposal: prefers proposal")
}

func TestCompareRulePosition(t *testing.T) {
	decision := func(term int64) *clustermetadatapb.RulePosition {
		return &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{RuleNumber: rn(term, 0)}}
	}
	withProposal := func(decisionTerm, proposalTerm int64) *clustermetadatapb.RulePosition {
		return &clustermetadatapb.RulePosition{
			Decision: &clustermetadatapb.ShardRule{RuleNumber: rn(decisionTerm, 0)},
			Proposal: &clustermetadatapb.ShardRule{RuleNumber: rn(proposalTerm, 0)},
		}
	}

	tests := []struct {
		name string
		a, b *clustermetadatapb.RulePosition
		want int
	}{
		{"higher decision wins regardless of proposal", withProposal(3, 100), decision(4), -1},
		{"higher decision other wins regardless of proposal", decision(4), withProposal(3, 100), 1},
		{"decisions tie, proposals tiebreak: a ahead", withProposal(4, 6), withProposal(4, 5), 1},
		{"decisions tie, proposals tiebreak: b ahead", withProposal(4, 5), withProposal(4, 6), -1},
		{"decisions tie, no proposals: equal", decision(4), decision(4), 0},
		{"decisions tie, one has a proposal: proposal wins", withProposal(4, 5), decision(4), 1},
		{"decisions tie, other has a proposal: proposal wins", decision(4), withProposal(4, 5), -1},
		{"decisions and proposals both tie: equal", withProposal(4, 5), withProposal(4, 5), 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, CompareRulePosition(tt.a, tt.b))
		})
	}
}

func leaderID(name string) *clustermetadatapb.ID {
	return &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: name}
}

func TestHighestKnownRule(t *testing.T) {
	// status with a current_position rule.
	posStatus := func(term int64, leader string) *clustermetadatapb.ConsensusStatus {
		return &clustermetadatapb.ConsensusStatus{
			CurrentPosition: &clustermetadatapb.PoolerPosition{
				Position: &clustermetadatapb.RulePosition{
					Decision: &clustermetadatapb.ShardRule{RuleNumber: rn(term, 0), LeaderId: leaderID(leader)},
				},
			},
		}
	}
	// status whose replication primary holds leadership at a given rule.
	replStatus := func(term int64, leader string) *clustermetadatapb.ConsensusStatus {
		return &clustermetadatapb.ConsensusStatus{
			ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{
				Position: &clustermetadatapb.RulePosition{
					Decision: &clustermetadatapb.ShardRule{RuleNumber: rn(term, 0), LeaderId: leaderID(leader)},
				},
			},
		}
	}

	t.Run("nil when no statuses carry a rule", func(t *testing.T) {
		assert.Nil(t, HighestKnownRule(nil))
		assert.Nil(t, PossiblyUndecidedRule(HighestKnownRule([]*clustermetadatapb.ConsensusStatus{{}})))
	})

	t.Run("highest rule number across positions wins", func(t *testing.T) {
		got := PossiblyUndecidedRule(HighestKnownRule([]*clustermetadatapb.ConsensusStatus{posStatus(5, "a"), posStatus(7, "b"), posStatus(6, "c")}))
		assert.Equal(t, "b", got.GetLeaderId().GetName())
	})

	t.Run("replication primary rule is considered, not just position", func(t *testing.T) {
		// A follower positioned at rule 5 but replicating from a leader at rule 8.
		follower := posStatus(5, "old")
		follower.ReplicationPrimary = replStatus(8, "new").ReplicationPrimary
		got := PossiblyUndecidedRule(HighestKnownRule([]*clustermetadatapb.ConsensusStatus{follower, posStatus(5, "old")}))
		assert.Equal(t, "new", got.GetLeaderId().GetName(), "newer leader via replication primary should win")
	})

	t.Run("phantom 0/0 replication primary is ignored", func(t *testing.T) {
		// A pooler positioned at a real rule (term 5) but carrying a zero-valued
		// replication primary (rule 0/0, no leader) — the fresh/never-established
		// shape. The phantom entry must not shadow the real position rule.
		phantom := posStatus(5, "leader")
		phantom.ReplicationPrimary = &clustermetadatapb.ReplicationPrimary{
			Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{RuleNumber: rn(0, 0)}},
		}
		got := PossiblyUndecidedRule(HighestKnownRule([]*clustermetadatapb.ConsensusStatus{phantom}))
		assert.Equal(t, "leader", got.GetLeaderId().GetName(), "real position rule should win over phantom 0/0 replication primary")
	})
}

func TestReplicationPrimaryOrNil(t *testing.T) {
	t.Run("nil status", func(t *testing.T) {
		assert.Nil(t, ReplicationPrimaryOrNil(nil))
	})

	t.Run("unset replication primary", func(t *testing.T) {
		assert.Nil(t, ReplicationPrimaryOrNil(&clustermetadatapb.ConsensusStatus{}))
	})

	t.Run("zero-valued 0/0 rule is treated as absent", func(t *testing.T) {
		cs := &clustermetadatapb.ConsensusStatus{
			ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{
				Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{RuleNumber: rn(0, 0)}},
			},
		}
		assert.Nil(t, ReplicationPrimaryOrNil(cs))
	})

	t.Run("empty rule (no rule number) is treated as absent", func(t *testing.T) {
		cs := &clustermetadatapb.ConsensusStatus{
			ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{
				Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{}},
			},
		}
		assert.Nil(t, ReplicationPrimaryOrNil(cs))
	})

	t.Run("established replication primary is returned", func(t *testing.T) {
		rp := &clustermetadatapb.ReplicationPrimary{
			Position: &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{RuleNumber: rn(1, 0), LeaderId: leaderID("leader")}},
		}
		cs := &clustermetadatapb.ConsensusStatus{ReplicationPrimary: rp}
		assert.Same(t, rp, ReplicationPrimaryOrNil(cs))
	})
}
