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
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func transitionRule(term int64, leader string) *clustermetadatapb.ShardRule {
	return &clustermetadatapb.ShardRule{
		RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term},
		LeaderId:   &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: leader},
	}
}

func TestRuleTransition_ZeroValueIsEmpty(t *testing.T) {
	var zero RuleTransition
	assert.True(t, zero.IsEmpty())
	assert.True(t, zero.IsDecided())
	assert.Nil(t, zero.Position())
	assert.Nil(t, zero.Decision())
	assert.Nil(t, zero.Proposal())
	assert.Nil(t, zero.PossiblyUndecidedRule())
	assert.Nil(t, zero.IntendedLeaderID())
	assert.Equal(t, "none", zero.String())
}

func TestRuleTransition_PhantomRuleNumbersAreEmpty(t *testing.T) {
	phantom := &clustermetadatapb.ShardRule{RuleNumber: &clustermetadatapb.RuleNumber{}}
	for _, tt := range []struct {
		name  string
		pos   *clustermetadatapb.RulePosition
		empty bool
	}{
		{"nil position", nil, true},
		{"no rules", &clustermetadatapb.RulePosition{}, true},
		{"phantom 0.0 decision", &clustermetadatapb.RulePosition{Decision: phantom}, true},
		{"phantom decision and proposal", &clustermetadatapb.RulePosition{Decision: phantom, Proposal: phantom}, true},
		{"decision without a rule number", &clustermetadatapb.RulePosition{Decision: &clustermetadatapb.ShardRule{}}, true},
		{"real decision", &clustermetadatapb.RulePosition{Decision: transitionRule(1, "a")}, false},
		{"real proposal only", &clustermetadatapb.RulePosition{Proposal: transitionRule(1, "a")}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.empty, NewRuleTransition(tt.pos).IsEmpty())
		})
	}
}

func TestRuleTransition_Decided(t *testing.T) {
	pos := &clustermetadatapb.RulePosition{Decision: transitionRule(5, "a")}
	tr := NewRuleTransition(pos)

	assert.False(t, tr.IsEmpty())
	assert.True(t, tr.IsDecided())
	assert.Same(t, pos, tr.Position())
	assert.Same(t, pos.Decision, tr.Decision())
	assert.Nil(t, tr.Proposal())
	assert.Same(t, pos.Decision, tr.PossiblyUndecidedRule())
	assert.Equal(t, "a", tr.IntendedLeaderID().GetName())
	assert.Equal(t, "5.0", tr.String())
}

func TestRuleTransition_UndecidedNamesTheProposal(t *testing.T) {
	pos := &clustermetadatapb.RulePosition{Decision: transitionRule(5, "old"), Proposal: transitionRule(6, "new")}
	tr := NewRuleTransition(pos)

	assert.False(t, tr.IsDecided())
	assert.Same(t, pos.Decision, tr.Decision())
	assert.Same(t, pos.Proposal, tr.Proposal())
	assert.Same(t, pos.Proposal, tr.PossiblyUndecidedRule())
	assert.Equal(t, "new", tr.IntendedLeaderID().GetName())
	assert.Equal(t, "old", tr.Decision().GetLeaderId().GetName())
	assert.Equal(t, "5.0 proposal=6.0", tr.String())
}

func TestRuleTransition_PhantomProposalCountsAsDecided(t *testing.T) {
	// A proposal whose rule number is the {0,0} sentinel is not a real proposal.
	pos := &clustermetadatapb.RulePosition{
		Decision: transitionRule(5, "a"),
		Proposal: &clustermetadatapb.ShardRule{RuleNumber: &clustermetadatapb.RuleNumber{}},
	}
	tr := NewRuleTransition(pos)
	assert.True(t, tr.IsDecided())
	assert.Same(t, pos.Decision, tr.PossiblyUndecidedRule())
}

func TestRuleTransition_Compare(t *testing.T) {
	decided5 := NewRuleTransition(&clustermetadatapb.RulePosition{Decision: transitionRule(5, "a")})
	decided6 := NewRuleTransition(&clustermetadatapb.RulePosition{Decision: transitionRule(6, "a")})
	decided5Proposing6 := NewRuleTransition(&clustermetadatapb.RulePosition{Decision: transitionRule(5, "a"), Proposal: transitionRule(6, "b")})

	assert.Negative(t, decided5.Compare(decided6))
	assert.Positive(t, decided6.Compare(decided5))
	assert.Zero(t, decided5.Compare(decided5))
	// The same decision with a proposal is further along than the bare decision.
	assert.Positive(t, decided5Proposing6.Compare(decided5))
	// A settled decision outranks another position's proposal alone.
	assert.Positive(t, decided6.Compare(decided5Proposing6))
}

func TestRuleTransition_IsRevokedByMatchesIsRuleRevoked(t *testing.T) {
	revocation := &clustermetadatapb.TermRevocation{
		RevokedBelowTerm: 6,
		OutgoingRule:     &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
	}
	for _, tt := range []struct {
		name string
		pos  *clustermetadatapb.RulePosition
	}{
		{"nil", nil},
		{"below the term", &clustermetadatapb.RulePosition{Decision: transitionRule(5, "a")}},
		{"at the term", &clustermetadatapb.RulePosition{Decision: transitionRule(6, "a")}},
		{"proposal at term", &clustermetadatapb.RulePosition{Decision: transitionRule(5, "a"), Proposal: transitionRule(6, "b")}},
		{"proposal below", &clustermetadatapb.RulePosition{Decision: transitionRule(4, "a"), Proposal: transitionRule(5, "b")}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, IsRuleRevoked(tt.pos, revocation), NewRuleTransition(tt.pos).IsRevokedBy(revocation))
		})
	}
	assert.False(t, NewRuleTransition(&clustermetadatapb.RulePosition{Decision: transitionRule(5, "a")}).IsRevokedBy(nil))
}
