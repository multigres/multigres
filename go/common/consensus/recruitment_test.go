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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
)

// makeID builds a pooler ID for tests.
func makeID(cell, name string) *clustermetadatapb.ID {
	return &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      cell,
		Name:      name,
	}
}

// makeRule builds a ShardRule at the given coordinator term with the given cohort.
func makeRule(coordTerm int64, cohort []*clustermetadatapb.ID) *clustermetadatapb.ShardRule {
	return &clustermetadatapb.ShardRule{
		RuleNumber: &clustermetadatapb.RuleNumber{
			CoordinatorTerm: coordTerm,
		},
		PrimaryId:        cohort[0],
		CohortMembers:    cohort,
		DurabilityPolicy: topoclient.AtLeastN(2),
	}
}

// makeStatus builds a ConsensusStatus for a recruited node.
func makeStatus(id *clustermetadatapb.ID, rule *clustermetadatapb.ShardRule, revocation *clustermetadatapb.TermRevocation) *clustermetadatapb.ConsensusStatus {
	return makeStatusWithLSN(id, rule, revocation, "0/1000000")
}

// makeStatusWithLSN builds a ConsensusStatus with an explicit LSN.
func makeStatusWithLSN(id *clustermetadatapb.ID, rule *clustermetadatapb.ShardRule, revocation *clustermetadatapb.TermRevocation, lsn string) *clustermetadatapb.ConsensusStatus {
	return &clustermetadatapb.ConsensusStatus{
		Id:             id,
		TermRevocation: revocation,
		CurrentPosition: &clustermetadatapb.PoolerPosition{
			Rule: rule,
			Lsn:  lsn,
		},
	}
}

// simpleProposal returns a buildProposal callback that proposes the first
// eligible leader with the given proposed rule.
func simpleProposal(proposedRule *clustermetadatapb.ShardRule) func(RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
	return func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
		leader := r.EligibleLeaders[0]
		return &consensusdatapb.CoordinatorProposal{
			TermRevocation: r.TermRevocation,
			ProposalLeader: &consensusdatapb.ProposalLeader{
				Id:   leader.GetId(),
				Host: "localhost",
			},
			ProposedRule: proposedRule,
		}, nil
	}
}

var testRevocation = &clustermetadatapb.TermRevocation{RevokedBelowTerm: 5}

func TestCheckSufficientRecruitment_NormalPromotion(t *testing.T) {
	a := makeID("z1", "pooler-a")
	b := makeID("z1", "pooler-b")
	c := makeID("z1", "pooler-c")
	cohort := []*clustermetadatapb.ID{a, b, c}
	rule := makeRule(3, cohort)

	statuses := []*clustermetadatapb.ConsensusStatus{
		makeStatus(a, rule, testRevocation),
		makeStatus(b, rule, testRevocation),
		makeStatus(c, rule, testRevocation),
	}

	// Propose same cohort, pick a as leader.
	proposal, err := CheckSufficientRecruitment(statuses, simpleProposal(rule))

	require.NoError(t, err)
	require.NotNil(t, proposal)
	assert.Equal(t, "pooler-a", proposal.GetProposalLeader().GetId().GetName())
}

func TestCheckSufficientRecruitment_Empty(t *testing.T) {
	_, err := CheckSufficientRecruitment(nil, simpleProposal(nil))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "no recruitment statuses provided")
}

func TestCheckSufficientRecruitment_NoCommittedRule(t *testing.T) {
	a := makeID("z1", "pooler-a")
	statuses := []*clustermetadatapb.ConsensusStatus{
		{Id: a, TermRevocation: testRevocation}, // no current_position
	}

	_, err := CheckSufficientRecruitment(statuses, simpleProposal(nil))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "no committed rule found")
}

func TestCheckSufficientRecruitment_InsufficientQuorum(t *testing.T) {
	a := makeID("z1", "pooler-a")
	b := makeID("z1", "pooler-b")
	c := makeID("z1", "pooler-c")
	cohort := []*clustermetadatapb.ID{a, b, c}
	rule := makeRule(3, cohort)

	// Only one of three nodes recruited — not enough for AT_LEAST_2 with majority.
	statuses := []*clustermetadatapb.ConsensusStatus{
		makeStatus(a, rule, testRevocation),
	}

	_, err := CheckSufficientRecruitment(statuses, simpleProposal(rule))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "insufficient recruitment")
}

func TestCheckSufficientRecruitment_InvalidLeader(t *testing.T) {
	a := makeID("z1", "pooler-a")
	b := makeID("z1", "pooler-b")
	c := makeID("z1", "pooler-c")
	outsider := makeID("z1", "outsider")
	cohort := []*clustermetadatapb.ID{a, b, c}
	rule := makeRule(3, cohort)

	statuses := []*clustermetadatapb.ConsensusStatus{
		makeStatus(a, rule, testRevocation),
		makeStatus(b, rule, testRevocation),
		makeStatus(c, rule, testRevocation),
	}

	// Callback proposes a node that was not recruited.
	buildProposal := func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
		return &consensusdatapb.CoordinatorProposal{
			TermRevocation: r.TermRevocation,
			ProposalLeader: &consensusdatapb.ProposalLeader{Id: outsider},
			ProposedRule:   rule,
		}, nil
	}

	_, err := CheckSufficientRecruitment(statuses, buildProposal)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "not among eligible leaders")
}

func TestCheckSufficientRecruitment_NonRecruitedCohortMember(t *testing.T) {
	a := makeID("z1", "pooler-a")
	b := makeID("z1", "pooler-b")
	c := makeID("z1", "pooler-c")
	outsider := makeID("z1", "outsider")
	cohort := []*clustermetadatapb.ID{a, b, c}
	rule := makeRule(3, cohort)

	statuses := []*clustermetadatapb.ConsensusStatus{
		makeStatus(a, rule, testRevocation),
		makeStatus(b, rule, testRevocation),
		makeStatus(c, rule, testRevocation),
	}

	// Proposed rule includes a node that was not recruited.
	proposedRule := makeRule(4, []*clustermetadatapb.ID{a, b, outsider})
	buildProposal := func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
		return &consensusdatapb.CoordinatorProposal{
			TermRevocation: r.TermRevocation,
			ProposalLeader: &consensusdatapb.ProposalLeader{Id: a},
			ProposedRule:   proposedRule,
		}, nil
	}

	_, err := CheckSufficientRecruitment(statuses, buildProposal)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "was not recruited")
}

func TestCheckSufficientRecruitment_BuildProposalError(t *testing.T) {
	a := makeID("z1", "pooler-a")
	b := makeID("z1", "pooler-b")
	c := makeID("z1", "pooler-c")
	cohort := []*clustermetadatapb.ID{a, b, c}
	rule := makeRule(3, cohort)

	statuses := []*clustermetadatapb.ConsensusStatus{
		makeStatus(a, rule, testRevocation),
		makeStatus(b, rule, testRevocation),
		makeStatus(c, rule, testRevocation),
	}

	buildProposal := func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
		return nil, errors.New("no suitable candidate")
	}

	_, err := CheckSufficientRecruitment(statuses, buildProposal)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "no suitable candidate")
}

func TestCheckSufficientRecruitment_BestRuleSelected(t *testing.T) {
	// One node is behind; the others are at the higher rule.
	// The higher rule's cohort and policy must govern quorum.
	a := makeID("z1", "pooler-a")
	b := makeID("z1", "pooler-b")
	c := makeID("z1", "pooler-c")
	cohort := []*clustermetadatapb.ID{a, b, c}
	oldRule := makeRule(2, cohort)
	newRule := makeRule(3, cohort)

	statuses := []*clustermetadatapb.ConsensusStatus{
		makeStatus(a, newRule, testRevocation),
		makeStatus(b, newRule, testRevocation),
		makeStatus(c, oldRule, testRevocation), // behind
	}

	// Only a and b are eligible (at bestRule); callback picks a.
	var gotResult RecruitmentResult
	buildProposal := func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
		gotResult = r
		return &consensusdatapb.CoordinatorProposal{
			TermRevocation: r.TermRevocation,
			ProposalLeader: &consensusdatapb.ProposalLeader{Id: r.EligibleLeaders[0].GetId()},
			ProposedRule:   newRule,
		}, nil
	}

	proposal, err := CheckSufficientRecruitment(statuses, buildProposal)

	require.NoError(t, err)
	require.NotNil(t, proposal)
	assert.Equal(t, int64(3), gotResult.BestRule.GetRuleNumber().GetCoordinatorTerm())
	assert.Len(t, gotResult.EligibleLeaders, 2, "only nodes at bestRule are eligible")
	// Leader must be a or b (both at newRule), not c.
	leaderName := proposal.GetProposalLeader().GetId().GetName()
	assert.NotEqual(t, "pooler-c", leaderName)
	assert.Equal(t, topoclient.ClusterIDString(gotResult.EligibleLeaders[0].GetId()), topoclient.ClusterIDString(proposal.GetProposalLeader().GetId()))
}

func TestCheckSufficientRecruitment_BuildProposalNil(t *testing.T) {
	a := makeID("z1", "pooler-a")
	b := makeID("z1", "pooler-b")
	c := makeID("z1", "pooler-c")
	cohort := []*clustermetadatapb.ID{a, b, c}
	rule := makeRule(3, cohort)

	statuses := []*clustermetadatapb.ConsensusStatus{
		makeStatus(a, rule, testRevocation),
		makeStatus(b, rule, testRevocation),
		makeStatus(c, rule, testRevocation),
	}

	_, err := CheckSufficientRecruitment(statuses, func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
		return nil, nil
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "buildProposal returned nil")
}

func TestCheckSufficientRecruitment_ProposedPolicyNotAchievable(t *testing.T) {
	a := makeID("z1", "pooler-a")
	b := makeID("z1", "pooler-b")
	c := makeID("z1", "pooler-c")
	cohort := []*clustermetadatapb.ID{a, b, c}
	rule := makeRule(3, cohort)

	statuses := []*clustermetadatapb.ConsensusStatus{
		makeStatus(a, rule, testRevocation),
		makeStatus(b, rule, testRevocation),
		makeStatus(c, rule, testRevocation),
	}

	// Proposed rule has AT_LEAST_2 but only one cohort member — not achievable.
	tinyRule := &clustermetadatapb.ShardRule{
		RuleNumber:       rule.GetRuleNumber(),
		CohortMembers:    []*clustermetadatapb.ID{a},
		DurabilityPolicy: topoclient.AtLeastN(2),
	}
	buildProposal := func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
		return &consensusdatapb.CoordinatorProposal{
			TermRevocation: r.TermRevocation,
			ProposalLeader: &consensusdatapb.ProposalLeader{Id: a},
			ProposedRule:   tinyRule,
		}, nil
	}

	_, err := CheckSufficientRecruitment(statuses, buildProposal)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "not achievable")
}

func TestCheckSufficientRecruitment_DuplicateStatusIgnoredForQuorum(t *testing.T) {
	// The same pooler appears twice in the statuses (e.g. two RPC responses
	// for the same node). It must count only once toward quorum.
	a := makeID("z1", "pooler-a")
	b := makeID("z1", "pooler-b")
	c := makeID("z1", "pooler-c")
	cohort := []*clustermetadatapb.ID{a, b, c}
	rule := makeRule(3, cohort)

	// Only a responds, but we see its response twice — still only 1 recruited.
	statuses := []*clustermetadatapb.ConsensusStatus{
		makeStatus(a, rule, testRevocation),
		makeStatus(a, rule, testRevocation), // duplicate
	}

	_, err := CheckSufficientRecruitment(statuses, simpleProposal(rule))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "insufficient recruitment")
}

func TestCheckSufficientRecruitment_DuplicateBestPositionKept(t *testing.T) {
	// The same pooler appears twice with different positions (e.g. a retry
	// returned a fresher snapshot). The entry with the higher position must win,
	// regardless of which appeared first in the input.
	//
	// Specifically this tests that rule number takes precedence: a's stale
	// response has a higher LSN but an older rule, so the fresh lower-LSN
	// response at the newer rule must win.
	a := makeID("z1", "pooler-a")
	b := makeID("z1", "pooler-b")
	c := makeID("z1", "pooler-c")
	cohort := []*clustermetadatapb.ID{a, b, c}
	oldRule := makeRule(2, cohort)
	newRule := makeRule(3, cohort)

	// a appears twice: stale at oldRule with a high LSN, fresh at newRule with
	// a lower LSN. Rule number wins, so the newRule entry must be kept.
	// Result: a ends up as the sole eligible leader (highest LSN at newRule).
	statuses := []*clustermetadatapb.ConsensusStatus{
		makeStatusWithLSN(a, oldRule, testRevocation, "0/3000000"), // stale, high LSN
		makeStatusWithLSN(a, newRule, testRevocation, "0/2000000"), // fresh, lower LSN
		makeStatusWithLSN(b, newRule, testRevocation, "0/1000000"),
		makeStatusWithLSN(c, newRule, testRevocation, "0/1000000"),
	}

	var gotResult RecruitmentResult
	_, err := CheckSufficientRecruitment(statuses, func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
		gotResult = r
		return &consensusdatapb.CoordinatorProposal{
			TermRevocation: r.TermRevocation,
			ProposalLeader: &consensusdatapb.ProposalLeader{Id: r.EligibleLeaders[0].GetId()},
			ProposedRule:   newRule,
		}, nil
	})

	require.NoError(t, err)
	// a is at newRule after deduplication and has the highest LSN among newRule
	// nodes, so it is the sole eligible leader.
	require.Len(t, gotResult.EligibleLeaders, 1)
	assert.Equal(t, "pooler-a", gotResult.EligibleLeaders[0].GetId().GetName())
}

func TestCheckSufficientRecruitment_EligibleLeadersOrderDeterministic(t *testing.T) {
	// EligibleLeaders must be in the same order regardless of which order
	// statuses arrive. Coordinators that pick by index need a stable list.
	a := makeID("z1", "pooler-a")
	b := makeID("z1", "pooler-b")
	c := makeID("z1", "pooler-c")
	cohort := []*clustermetadatapb.ID{a, b, c}
	rule := makeRule(3, cohort)

	forward := []*clustermetadatapb.ConsensusStatus{
		makeStatus(a, rule, testRevocation),
		makeStatus(b, rule, testRevocation),
		makeStatus(c, rule, testRevocation),
	}
	reversed := []*clustermetadatapb.ConsensusStatus{
		makeStatus(c, rule, testRevocation),
		makeStatus(b, rule, testRevocation),
		makeStatus(a, rule, testRevocation),
	}

	collect := func(statuses []*clustermetadatapb.ConsensusStatus) []string {
		var names []string
		_, err := CheckSufficientRecruitment(statuses, func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
			for _, cs := range r.EligibleLeaders {
				names = append(names, cs.GetId().GetName())
			}
			return &consensusdatapb.CoordinatorProposal{
				TermRevocation: r.TermRevocation,
				ProposalLeader: &consensusdatapb.ProposalLeader{Id: r.EligibleLeaders[0].GetId()},
				ProposedRule:   rule,
			}, nil
		})
		require.NoError(t, err)
		return names
	}

	assert.Equal(t, collect(forward), collect(reversed), "eligible leader order must not depend on input order")
}

func TestCheckSufficientRecruitment_ExtraNodeOutsideCohortIgnored(t *testing.T) {
	// An extra node that is not in the bestRule cohort was also recruited
	// (e.g. a node being removed from the cohort). It must not inflate the
	// quorum count for the current cohort's policy.
	a := makeID("z1", "pooler-a")
	b := makeID("z1", "pooler-b")
	c := makeID("z1", "pooler-c")
	extra := makeID("z1", "extra-node") // not in current cohort
	cohort := []*clustermetadatapb.ID{a, b, c}
	rule := makeRule(3, cohort)

	// Only a recruited from the actual cohort, plus the extra node.
	statuses := []*clustermetadatapb.ConsensusStatus{
		makeStatus(a, rule, testRevocation),
		makeStatus(extra, rule, testRevocation),
	}

	_, err := CheckSufficientRecruitment(statuses, simpleProposal(rule))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "insufficient recruitment",
		"extra node outside cohort should not count toward quorum")
}

func TestCheckSufficientRecruitment_LSNTiebreaker(t *testing.T) {
	// All three nodes are at the same rule number but different LSNs.
	// Only the node with the highest LSN should be eligible as leader.
	a := makeID("z1", "pooler-a")
	b := makeID("z1", "pooler-b")
	c := makeID("z1", "pooler-c")
	cohort := []*clustermetadatapb.ID{a, b, c}
	rule := makeRule(3, cohort)

	statuses := []*clustermetadatapb.ConsensusStatus{
		makeStatusWithLSN(a, rule, testRevocation, "0/3000000"), // highest
		makeStatusWithLSN(b, rule, testRevocation, "0/2000000"),
		makeStatusWithLSN(c, rule, testRevocation, "0/1000000"), // lowest
	}

	var gotResult RecruitmentResult
	_, err := CheckSufficientRecruitment(statuses, func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
		gotResult = r
		return &consensusdatapb.CoordinatorProposal{
			TermRevocation: r.TermRevocation,
			ProposalLeader: &consensusdatapb.ProposalLeader{Id: r.EligibleLeaders[0].GetId()},
			ProposedRule:   rule,
		}, nil
	})

	require.NoError(t, err)
	require.Len(t, gotResult.EligibleLeaders, 1, "only the node with the highest LSN should be eligible")
	assert.Equal(t, "pooler-a", gotResult.EligibleLeaders[0].GetId().GetName())
}

func TestCheckSufficientRecruitment_LSNTiedAtMax(t *testing.T) {
	// Two nodes share the highest LSN — both should be eligible.
	a := makeID("z1", "pooler-a")
	b := makeID("z1", "pooler-b")
	c := makeID("z1", "pooler-c")
	cohort := []*clustermetadatapb.ID{a, b, c}
	rule := makeRule(3, cohort)

	statuses := []*clustermetadatapb.ConsensusStatus{
		makeStatusWithLSN(a, rule, testRevocation, "0/3000000"),
		makeStatusWithLSN(b, rule, testRevocation, "0/3000000"), // tied with a
		makeStatusWithLSN(c, rule, testRevocation, "0/1000000"),
	}

	var gotResult RecruitmentResult
	_, err := CheckSufficientRecruitment(statuses, func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
		gotResult = r
		return &consensusdatapb.CoordinatorProposal{
			TermRevocation: r.TermRevocation,
			ProposalLeader: &consensusdatapb.ProposalLeader{Id: r.EligibleLeaders[0].GetId()},
			ProposedRule:   rule,
		}, nil
	})

	require.NoError(t, err)
	require.Len(t, gotResult.EligibleLeaders, 2, "both nodes at the highest LSN should be eligible")
	names := []string{gotResult.EligibleLeaders[0].GetId().GetName(), gotResult.EligibleLeaders[1].GetId().GetName()}
	assert.ElementsMatch(t, []string{"pooler-a", "pooler-b"}, names)
}

func TestCheckSufficientRecruitment_NodeWithHigherRuleButLowerLSNNotEligible(t *testing.T) {
	// Node a is at bestRule with a high LSN.
	// Node b is at bestRule with a lower LSN — should be ineligible as leader
	// even though it is at the correct rule number.
	// Node c is at an older rule — also ineligible.
	// Quorum is satisfied (all 3 recruited from a 3-node cohort).
	a := makeID("z1", "pooler-a")
	b := makeID("z1", "pooler-b")
	c := makeID("z1", "pooler-c")
	cohort := []*clustermetadatapb.ID{a, b, c}
	oldRule := makeRule(2, cohort)
	newRule := makeRule(3, cohort)

	statuses := []*clustermetadatapb.ConsensusStatus{
		makeStatusWithLSN(a, newRule, testRevocation, "0/5000000"),
		makeStatusWithLSN(b, newRule, testRevocation, "0/3000000"), // same rule, lower LSN
		makeStatusWithLSN(c, oldRule, testRevocation, "0/4000000"), // old rule
	}

	var gotResult RecruitmentResult
	_, err := CheckSufficientRecruitment(statuses, func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
		gotResult = r
		return &consensusdatapb.CoordinatorProposal{
			TermRevocation: r.TermRevocation,
			ProposalLeader: &consensusdatapb.ProposalLeader{Id: r.EligibleLeaders[0].GetId()},
			ProposedRule:   newRule,
		}, nil
	})

	require.NoError(t, err)
	require.Len(t, gotResult.EligibleLeaders, 1)
	assert.Equal(t, "pooler-a", gotResult.EligibleLeaders[0].GetId().GetName())
}

func TestCheckSufficientRecruitment_NoEligibleLeaderDueToMissingLSN(t *testing.T) {
	// All nodes at bestRule have empty LSNs — none can be verified as eligible.
	a := makeID("z1", "pooler-a")
	b := makeID("z1", "pooler-b")
	c := makeID("z1", "pooler-c")
	cohort := []*clustermetadatapb.ID{a, b, c}
	rule := makeRule(3, cohort)

	statuses := []*clustermetadatapb.ConsensusStatus{
		makeStatusWithLSN(a, rule, testRevocation, ""), // no LSN
		makeStatusWithLSN(b, rule, testRevocation, ""),
		makeStatusWithLSN(c, rule, testRevocation, ""),
	}

	_, err := CheckSufficientRecruitment(statuses, simpleProposal(rule))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "no eligible leaders found")
}

func TestCheckSufficientRecruitment_CohortExpansionNewMembersOnly(t *testing.T) {
	// Scenario: 3-node cohort [A, B, C] with AT_LEAST_2. A coordinator begins
	// expanding to a 5-node cohort [A, B, C, D, E]. The new rule reaches only
	// D and E before the original primary A crashes. B and C are also
	// unreachable. Only D and E respond to Recruit.
	//
	// The coordinator wants to stabilize by promoting D with a new cohort [D, E].
	//
	// This correctly fails: AT_LEAST_2 with the 5-node bestRule cohort requires
	// recruiting at least 4 nodes (revocation: 5-2+1=4, majority: 3 — max=4).
	// Only 2 are recruited, so quorum is rejected.
	//
	// SAFETY NOTE: We are safe here because the 5-node cohort's revocation
	// requirements happen to block this case. However, this is NOT because we
	// correctly enforce the generalized consensus rule that recruitment must
	// satisfy quorum under the PREVIOUS rule's cohort [A, B, C] as well.
	// If 4 nodes had responded (e.g. B, C, D, E), the quorum check would
	// pass even though promoting D or E would still be unsafe — D and E may
	// not have replicated all of A's committed WAL. That case is a known
	// limitation documented in CheckSufficientRecruitment's TODO.
	a := makeID("z1", "pooler-a") // unreachable — old primary
	b := makeID("z1", "pooler-b") // unreachable
	c := makeID("z1", "pooler-c") // unreachable
	d := makeID("z1", "pooler-d") // new cohort member, responds
	e := makeID("z1", "pooler-e") // new cohort member, responds

	oldCohort := []*clustermetadatapb.ID{a, b, c}
	newCohort := []*clustermetadatapb.ID{a, b, c, d, e}
	reducedCohort := []*clustermetadatapb.ID{d, e}

	_ = oldCohort // documented above for clarity
	newRule := makeRule(4, newCohort)
	proposedRule := makeRule(5, reducedCohort)

	statuses := []*clustermetadatapb.ConsensusStatus{
		makeStatusWithLSN(d, newRule, testRevocation, "0/4000000"),
		makeStatusWithLSN(e, newRule, testRevocation, "0/4000000"),
	}

	_, err := CheckSufficientRecruitment(statuses, func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
		return &consensusdatapb.CoordinatorProposal{
			TermRevocation: r.TermRevocation,
			ProposalLeader: &consensusdatapb.ProposalLeader{Id: d},
			ProposedRule:   proposedRule,
		}, nil
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "insufficient recruitment")
}

func TestCheckSufficientRecruitment_CohortReplacementSplitBrain(t *testing.T) {
	// KNOWN LIMITATION: documents a split-brain scenario that CheckSufficientRecruitment
	// cannot detect. See the TODO in CheckSufficientRecruitment.
	//
	// A is the primary for cohort [A, B, C] (AT_LEAST_2). A coordinator sends a
	// Propose to replace the cohort with [D, E, F]. A writes the new rule to its
	// rule_history; D and E stream that WAL from A and apply it. But B and C never
	// receive the new rule, and A crashes before the coordinator gets a Propose
	// response — so the new rule was never durably decided. Under the outgoing
	// cohort's policy (AT_LEAST_2 on [A, B, C]), only A applied the rule change;
	// it needed at least one of B or C to commit. The new rule is a phantom: it
	// exists in D and E's WAL but was never agreed to by the outgoing cohort.
	//
	// Two coordinators now independently recruit disjoint sets of nodes:
	//
	//   Coordinator 1 sees B and C (both at old rule, cohort [A,B,C]):
	//     - bestRule = old rule, cohort = [A,B,C], recruited 2 of 3 → AT_LEAST_2 ✓
	//     - promotes B with cohort [B,C]
	//
	//   Coordinator 2 sees D and E (both at new rule, cohort [D,E,F]):
	//     - bestRule = new rule (phantom), cohort = [D,E,F], recruited 2 of 3 → AT_LEAST_2 ✓
	//     - promotes D with cohort [D,E]
	//
	// The two recruited sets share no nodes. Both promotions succeed, yielding
	// two independent primaries — split brain.
	//
	// A correct implementation would reject the new rule as bestRule when it has
	// not achieved quorum under the outgoing cohort's policy. We don't yet have
	// enough information from Recruit responses to enforce this. When the TODO is
	// resolved, at least one of the two calls below should return an error.
	a := makeID("z1", "pooler-a") // old primary, crashed — not recruited
	b := makeID("z1", "pooler-b") // old cohort, responds to coord 1
	c := makeID("z1", "pooler-c") // old cohort, responds to coord 1
	d := makeID("z1", "pooler-d") // new cohort, responds to coord 2
	e := makeID("z1", "pooler-e") // new cohort, responds to coord 2
	f := makeID("z1", "pooler-f") // new cohort, unreachable

	_ = a
	_ = f

	oldRule := makeRule(3, []*clustermetadatapb.ID{a, b, c})
	newRule := makeRule(4, []*clustermetadatapb.ID{d, e, f})

	// Coordinator 1: B and C at old rule.
	statusesCoord1 := []*clustermetadatapb.ConsensusStatus{
		makeStatusWithLSN(b, oldRule, testRevocation, "0/3000000"),
		makeStatusWithLSN(c, oldRule, testRevocation, "0/3000000"),
	}
	// Coordinator 2: D and E at new rule.
	statusesCoord2 := []*clustermetadatapb.ConsensusStatus{
		makeStatusWithLSN(d, newRule, testRevocation, "0/4000000"),
		makeStatusWithLSN(e, newRule, testRevocation, "0/4000000"),
	}

	proposal1, err1 := CheckSufficientRecruitment(statusesCoord1, func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
		return &consensusdatapb.CoordinatorProposal{
			TermRevocation: r.TermRevocation,
			ProposalLeader: &consensusdatapb.ProposalLeader{Id: b},
			ProposedRule:   makeRule(5, []*clustermetadatapb.ID{b, c}),
		}, nil
	})
	proposal2, err2 := CheckSufficientRecruitment(statusesCoord2, func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
		return &consensusdatapb.CoordinatorProposal{
			TermRevocation: r.TermRevocation,
			ProposalLeader: &consensusdatapb.ProposalLeader{Id: d},
			ProposedRule:   makeRule(5, []*clustermetadatapb.ID{d, e}),
		}, nil
	})

	require.NoError(t, err1)
	assert.NotNil(t, proposal1)
	require.NoError(t, err2)
	assert.NotNil(t, proposal2)
}

func TestSameCohort(t *testing.T) {
	a := makeID("z1", "a")
	b := makeID("z1", "b")
	c := makeID("z1", "c")

	assert.True(t, sameCohort([]*clustermetadatapb.ID{a, b}, []*clustermetadatapb.ID{b, a}), "order should not matter")
	assert.False(t, sameCohort([]*clustermetadatapb.ID{a, b}, []*clustermetadatapb.ID{a, c}))
	assert.False(t, sameCohort([]*clustermetadatapb.ID{a, b}, []*clustermetadatapb.ID{a, b, c}))
	assert.True(t, sameCohort(nil, nil))
}
