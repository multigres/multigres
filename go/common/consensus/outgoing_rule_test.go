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
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
)

// Safety is judged against the decision's cohort, the one every competing
// coordinator recruits, even when the outgoing rule is an undecided proposal.
func TestObservedOutgoingRule_Revoke_UndecidedProposal(t *testing.T) {
	zone1 := poolerIDs.zone1
	rev := revocation(5, ruleNum(3, 1))
	decision := makeRule(ruleNum(3, 0), atLeast(2), zone1.a, zone1.b, zone1.c)
	grow := &clustermetadatapb.RulePosition{
		Decision: decision,
		Proposal: makeRule(ruleNum(3, 1), atLeast(2), zone1.a, zone1.b, zone1.c, zone1.d, zone1.e),
	}
	statuses := func(ids ...*clustermetadatapb.ID) []*clustermetadatapb.ConsensusStatus {
		var out []*clustermetadatapb.ConsensusStatus
		for _, id := range ids {
			out = append(out, makeStatusWithProposal(id, grow, rev, "0/1000000"))
		}
		return out
	}
	revoke := func(recruited []*clustermetadatapb.ConsensusStatus) error {
		observed, err := discoverOutgoingRule(recruited, ruleNum(3, 1), revocationBasis{})
		require.NoError(t, err)
		_, err = observed.revoke(recruited)
		return err
	}

	t.Run("recruiting only the proposal's new members is rejected", func(t *testing.T) {
		err := revoke(statuses(zone1.d, zone1.e))
		require.EqualError(t, err, "insufficient outgoing cohort recruitment: majority not satisfied: recruited 0 of 3 cohort poolers, need at least 2")
	})

	t.Run("a majority of the decision's cohort is enough", func(t *testing.T) {
		// A majority of the proposal's five-member cohort is not required.
		require.NoError(t, revoke(statuses(zone1.a, zone1.b)))
	})
}

// Promote must decide an undecided outgoing proposal first, so the recruits have
// to be able to finish it. That is liveness, separate from revoking the rule.
func TestRevokedOutgoingRule_CheckRecruitsCanFinalizeProposal(t *testing.T) {
	zone1 := poolerIDs.zone1
	rev := revocation(5, ruleNum(3, 1))
	check := func(position *clustermetadatapb.RulePosition, basis revocationBasis, ids ...*clustermetadatapb.ID) error {
		var recruited []*clustermetadatapb.ConsensusStatus
		for _, id := range ids {
			recruited = append(recruited, makeStatusWithProposal(id, position, rev, "0/1000000"))
		}
		expected := position.GetDecision().GetRuleNumber()
		if position.GetProposal() != nil {
			expected = position.GetProposal().GetRuleNumber()
		}
		observed, err := discoverOutgoingRule(recruited, expected, basis)
		require.NoError(t, err)
		_, err = revokedOutgoingRule{observed}.checkRecruitsCanFinalizeProposal(recruited)
		return err
	}
	decision := makeRule(ruleNum(3, 0), atLeast(2), zone1.a, zone1.b, zone1.c)
	invalid := &clustermetadatapb.DurabilityPolicy{QuorumType: clustermetadatapb.QuorumType_QUORUM_TYPE_UNKNOWN}

	t.Run("a removal the recruits can finalize", func(t *testing.T) {
		shrink := &clustermetadatapb.RulePosition{Decision: decision, Proposal: makeRule(ruleNum(3, 1), atLeast(2), zone1.b, zone1.c)}
		require.NoError(t, check(shrink, revocationBasis{}, zone1.b, zone1.c))
	})

	t.Run("an addition the recruits can finalize", func(t *testing.T) {
		grow := &clustermetadatapb.RulePosition{Decision: decision, Proposal: makeRule(ruleNum(3, 1), atLeast(2), zone1.a, zone1.b, zone1.c, zone1.d)}
		require.NoError(t, check(grow, revocationBasis{}, zone1.a, zone1.b))
	})

	t.Run("recruits that cannot satisfy the Both policy are rejected", func(t *testing.T) {
		// AT_LEAST_3 over three members: two recruited satisfy the safety
		// checks, but deciding the proposal needs all three.
		strict := makeRule(ruleNum(3, 0), atLeast(3), zone1.a, zone1.b, zone1.c)
		position := &clustermetadatapb.RulePosition{Decision: strict, Proposal: makeRule(ruleNum(3, 1), atLeast(3), zone1.a, zone1.b, zone1.c)}

		err := check(position, revocationBasis{}, zone1.a, zone1.b)

		require.EqualError(t, err, "outgoing proposal cannot be finalized: recruited members cannot satisfy its policy: durability not satisfied: 2 poolers, required 3")
	})

	t.Run("recruits outside the Both cohort do not count", func(t *testing.T) {
		strict := makeRule(ruleNum(3, 0), atLeast(3), zone1.a, zone1.b, zone1.c)
		position := &clustermetadatapb.RulePosition{Decision: strict, Proposal: makeRule(ruleNum(3, 1), atLeast(3), zone1.a, zone1.b, zone1.c)}

		// d records the proposal but is not in either cohort.
		err := check(position, revocationBasis{}, zone1.a, zone1.b, zone1.d)

		require.EqualError(t, err, "outgoing proposal cannot be finalized: recruited members cannot satisfy its policy: durability not satisfied: 2 poolers, required 3")
	})

	t.Run("a lowered policy is judged by the stricter Both policy", func(t *testing.T) {
		strict := makeRule(ruleNum(3, 0), atLeast(3), zone1.a, zone1.b, zone1.c)
		position := &clustermetadatapb.RulePosition{Decision: strict, Proposal: makeRule(ruleNum(3, 1), atLeast(2), zone1.a, zone1.b, zone1.c)}

		err := check(position, revocationBasis{}, zone1.a, zone1.b)

		require.EqualError(t, err, "outgoing proposal cannot be finalized: recruited members cannot satisfy its policy: durability not satisfied: 2 poolers, required 3")
	})

	t.Run("a transition the pooler cannot finalize is rejected", func(t *testing.T) {
		disjoint := &clustermetadatapb.RulePosition{Decision: decision, Proposal: makeRule(ruleNum(3, 1), atLeast(2), zone1.c, zone1.d, zone1.e)}

		err := check(disjoint, revocationBasis{}, zone1.a, zone1.b, zone1.c)

		require.EqualError(t, err, "outgoing proposal cannot be finalized: unsupported leader-led rule change: neither cohort is a subset of the other")
	})

	t.Run("a malformed proposal fails closed", func(t *testing.T) {
		position := &clustermetadatapb.RulePosition{Decision: decision, Proposal: makeRule(ruleNum(3, 1), invalid, zone1.a, zone1.b, zone1.c)}

		err := check(position, revocationBasis{}, zone1.a, zone1.b)

		require.ErrorContains(t, err, "failed to parse durability policy from rule")
	})

	t.Run("a malformed decision fails closed", func(t *testing.T) {
		position := &clustermetadatapb.RulePosition{
			Decision: makeRule(ruleNum(3, 0), invalid, zone1.a, zone1.b, zone1.c),
			Proposal: makeRule(ruleNum(3, 1), atLeast(2), zone1.a, zone1.b, zone1.c),
		}

		err := check(position, revocationBasis{}, zone1.a, zone1.b)

		require.ErrorContains(t, err, "failed to parse durability policy from decision")
	})

	t.Run("not checked when a cert revokes the rule", func(t *testing.T) {
		strict := makeRule(ruleNum(3, 0), atLeast(3), zone1.a, zone1.b, zone1.c)
		position := &clustermetadatapb.RulePosition{Decision: strict, Proposal: makeRule(ruleNum(3, 1), atLeast(3), zone1.a, zone1.b, zone1.c)}

		require.NoError(t, check(position, revocationBasis{cert: &certBasis{}}, zone1.a, zone1.b))
	})

	t.Run("not needed for a decided rule", func(t *testing.T) {
		require.NoError(t, check(&clustermetadatapb.RulePosition{Decision: decision}, revocationBasis{}, zone1.a))
	})
}

// Through an undecided outgoing rule a proposal changes only the leader.
func TestBuildSafeProposal_UndecidedRuleKeepsCohortAndPolicy(t *testing.T) {
	zone1 := poolerIDs.zone1
	rev := revocation(5, ruleNum(3, 1))
	decision := makeRule(ruleNum(3, 0), atLeast(2), zone1.a, zone1.b, zone1.c)
	position := &clustermetadatapb.RulePosition{Decision: decision, Proposal: makeRule(ruleNum(3, 1), atLeast(2), zone1.a, zone1.b, zone1.c)}
	statuses := []*clustermetadatapb.ConsensusStatus{
		makeStatusWithProposal(zone1.a, position, rev, "0/1000000"),
		makeStatusWithProposal(zone1.b, position, rev, "0/1000000"),
		makeStatusWithProposal(zone1.c, position, rev, "0/1000000"),
	}
	propose := func(policy *clustermetadatapb.DurabilityPolicy, cohort ...*clustermetadatapb.ID) func(RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
		return proposeFirstEligible(makeRule(ruleNum(5, 0), policy, cohort...))
	}
	const wantErr = "proposal validation: proposed rule must keep the undecided outgoing rule's cohort and durability policy, changing only the leader"

	_, err := BuildSafeProposal(rev, statuses, propose(atLeast(2), zone1.a, zone1.b, zone1.c))
	require.NoError(t, err)

	t.Run("a different cohort", func(t *testing.T) {
		_, err := BuildSafeProposal(rev, statuses, propose(atLeast(2), zone1.a, zone1.b))
		require.EqualError(t, err, wantErr)
	})

	t.Run("a different policy", func(t *testing.T) {
		_, err := BuildSafeProposal(rev, statuses, propose(atLeast(3), zone1.a, zone1.b, zone1.c))
		require.EqualError(t, err, wantErr)
	})
}

func TestObservedOutgoingRule_Revoke_InvalidPolicies(t *testing.T) {
	zone1 := poolerIDs.zone1
	cohort := []*clustermetadatapb.ID{zone1.a, zone1.b, zone1.c}
	invalid := &clustermetadatapb.DurabilityPolicy{QuorumType: clustermetadatapb.QuorumType_QUORUM_TYPE_UNKNOWN}
	rev := revocation(5, ruleNum(3, 1))

	t.Run("a decided outgoing rule", func(t *testing.T) {
		rule := makeRule(ruleNum(3, 1), invalid, cohort...)
		statuses := []*clustermetadatapb.ConsensusStatus{makeStatus(zone1.a, rule, rev), makeStatus(zone1.b, rule, rev)}
		observed, err := discoverOutgoingRule(statuses, ruleNum(3, 1), revocationBasis{})
		require.NoError(t, err)

		_, err = observed.revoke(statuses)

		require.ErrorContains(t, err, "failed to parse durability policy from decision")
	})

	t.Run("the decision of an undecided outgoing rule", func(t *testing.T) {
		position := &clustermetadatapb.RulePosition{
			Decision: makeRule(ruleNum(3, 0), invalid, cohort...),
			Proposal: makeRule(ruleNum(3, 1), atLeast(2), cohort...),
		}
		statuses := []*clustermetadatapb.ConsensusStatus{
			makeStatusWithProposal(zone1.a, position, rev, "0/1000000"),
			makeStatusWithProposal(zone1.b, position, rev, "0/1000000"),
		}
		observed, err := discoverOutgoingRule(statuses, ruleNum(3, 1), revocationBasis{})
		require.NoError(t, err)

		_, err = observed.revoke(statuses)

		require.ErrorContains(t, err, "failed to parse durability policy from decision")
	})
}

// A cert revokes the outgoing rule without recruiting its cohort, and its frozen
// LSN bounds which recruits can lead.
func TestRevokedOutgoingRule_RevokeByCert_EligibleLeaders(t *testing.T) {
	zone1 := poolerIDs.zone1
	cohort := []*clustermetadatapb.ID{zone1.a, zone1.b, zone1.c}
	rule := makeRule(ruleNum(0, 1), atLeast(2), cohort...)
	rev := revocation(5, ruleNum(0, 1))
	// Only a is recruited: a coordinator-led revocation would reject this.
	statuses := []*clustermetadatapb.ConsensusStatus{makeStatusWithLSN(zone1.a, rule, rev, "0/2000000")}

	t.Run("a recruit at or above the frozen LSN can lead", func(t *testing.T) {
		observed, err := discoverOutgoingRule(statuses, ruleNum(0, 1), revocationBasis{cert: &certBasis{frozenLSN: 0x2000000}})
		require.NoError(t, err)
		revoked, err := observed.revoke(statuses)
		require.NoError(t, err)

		leaders, err := revoked.eligibleLeaders()

		require.NoError(t, err)
		require.Len(t, leaders, 1)
		assert.Equal(t, zone1.a.GetName(), leaders[0].GetId().GetName())
	})

	t.Run("none at or above the frozen LSN", func(t *testing.T) {
		observed, err := discoverOutgoingRule(statuses, ruleNum(0, 1), revocationBasis{cert: &certBasis{frozenLSN: 0x3000000}})
		require.NoError(t, err)
		revoked, err := observed.revoke(statuses)
		require.NoError(t, err)

		_, err = revoked.eligibleLeaders()

		require.EqualError(t, err, "no recruited nodes at or above the required LSN 0/3000000")
	})

	t.Run("a coordinator-led revocation of the same recruits is rejected", func(t *testing.T) {
		observed, err := discoverOutgoingRule(statuses, ruleNum(0, 1), revocationBasis{})
		require.NoError(t, err)

		_, err = observed.revoke(statuses)

		require.EqualError(t, err, "insufficient outgoing cohort recruitment: majority not satisfied: recruited 1 of 3 cohort poolers, need at least 2")
	})
}

func TestDiscoverOutgoingRule_InconsistentRecruits(t *testing.T) {
	zone1 := poolerIDs.zone1
	rev := revocation(5, ruleNum(3, 0))
	// Same rule number, different cohort: a data inconsistency.
	statuses := []*clustermetadatapb.ConsensusStatus{
		makeStatus(zone1.a, makeRule(ruleNum(3, 0), atLeast(2), zone1.a, zone1.b, zone1.c), rev),
		makeStatus(zone1.b, makeRule(ruleNum(3, 0), atLeast(2), zone1.a, zone1.b), rev),
	}

	_, err := discoverOutgoingRule(statuses, ruleNum(3, 0), revocationBasis{})

	require.EqualError(t, err, "recruits zone1_pooler-a and zone1_pooler-b agree on rule number but disagree on content; inconsistent discovery result")
}

func TestDiscoverOutgoingRulePosition_NoRecruits(t *testing.T) {
	recruits, position, err := discoverOutgoingRulePosition(nil)

	require.NoError(t, err)
	assert.Empty(t, recruits)
	assert.Nil(t, position)
}

func TestNarrowByLSN_NoCandidates(t *testing.T) {
	_, err := narrowByLSN(nil, 0)

	require.EqualError(t, err, "no eligible leaders found among recruited nodes")
}

func TestCheckUnrecruitedCannotCommit_RecruitedPoolerOutsideCohort(t *testing.T) {
	zone1 := poolerIDs.zone1
	cohort := []*clustermetadatapb.ID{zone1.a, zone1.b, zone1.c}

	err := checkUnrecruitedCannotCommit(AtLeastNPolicy{N: 2}, cohort, []*clustermetadatapb.ID{zone1.d})

	require.EqualError(t, err, "recruited pooler zone1_pooler-d is not in cohort")
}

func TestFilterCohortStatuses_SkipsStatusWithoutID(t *testing.T) {
	zone1 := poolerIDs.zone1
	cohort := []*clustermetadatapb.ID{zone1.a}
	rule := makeRule(ruleNum(3, 0), atLeast(1), cohort...)
	rev := revocation(5, ruleNum(3, 0))
	noID := makeStatus(zone1.a, rule, rev)
	noID.Id = nil

	got := filterCohortStatuses(cohort, []*clustermetadatapb.ConsensusStatus{noID, makeStatus(zone1.a, rule, rev)})

	require.Len(t, got, 1)
	assert.Equal(t, zone1.a.GetName(), got[0].GetId().GetName())
}

// validateProposal refuses a proposed rule that drops the attestation fields.
func TestBuildSafeProposal_ProposedRuleMissingAttestation(t *testing.T) {
	zone1 := poolerIDs.zone1
	cohort := []*clustermetadatapb.ID{zone1.a, zone1.b, zone1.c}
	rule := makeRule(ruleNum(3, 0), atLeast(2), cohort...)
	rev := revocation(5, ruleNum(3, 0))
	statuses := []*clustermetadatapb.ConsensusStatus{
		makeStatus(zone1.a, rule, rev), makeStatus(zone1.b, rule, rev), makeStatus(zone1.c, rule, rev),
	}
	strip := func(mutate func(*clustermetadatapb.ShardRule)) func(RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
		return func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
			p, err := proposeFirstEligible(makeRule(ruleNum(5, 0), atLeast(2), cohort...))(r)
			if err != nil {
				return nil, err
			}
			mutate(p.GetProposedTransition().GetProposal())
			return p, nil
		}
	}

	_, err := BuildSafeProposal(rev, statuses, strip(func(r *clustermetadatapb.ShardRule) { r.CoordinatorId = nil }))
	require.EqualError(t, err, "proposal validation: proposed rule has no coordinator_id")

	_, err = BuildSafeProposal(rev, statuses, strip(func(r *clustermetadatapb.ShardRule) { r.CreationTime = nil }))
	require.EqualError(t, err, "proposal validation: proposed rule has no creation_time")
}

// A failover through an undecided proposal that the recruits cannot decide is
// rejected before any proposal is built, from both the real and the pre-vote path.
func TestBuildSafeProposal_UndecidedProposalNotFinalizable(t *testing.T) {
	zone1 := poolerIDs.zone1
	cohort := []*clustermetadatapb.ID{zone1.a, zone1.b, zone1.c}
	// AT_LEAST_3 over three members: two recruited satisfy the safety checks, but
	// deciding the stuck proposal needs all three.
	position := &clustermetadatapb.RulePosition{
		Decision: makeRule(ruleNum(3, 0), atLeast(3), cohort...),
		Proposal: makeRule(ruleNum(3, 1), atLeast(3), cohort...),
	}
	rev := revocation(5, ruleNum(3, 1))
	statuses := []*clustermetadatapb.ConsensusStatus{
		makeStatusWithProposal(zone1.a, position, rev, "0/1000000"),
		makeStatusWithProposal(zone1.b, position, rev, "0/1000000"),
	}
	build := func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
		t.Fatal("no proposal should be built")
		return nil, nil
	}
	const wantErr = "outgoing proposal cannot be finalized: recruited members cannot satisfy its policy: durability not satisfied: 2 poolers, required 3"

	_, err := BuildSafeProposal(rev, statuses, build)
	require.EqualError(t, err, wantErr)

	err = CheckProposalPossible(coordRevocation(5, ruleNum(3, 1)), []*clustermetadatapb.ConsensusStatus{
		makeStatusWithProposal(zone1.a, position, nil, "0/1000000"),
		makeStatusWithProposal(zone1.b, position, nil, "0/1000000"),
	}, build)
	require.EqualError(t, err, wantErr)
}
