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

// An undecided outgoing rule whose cohort is disjoint from its decision's must
// be recruited on both: the proposal's cohort freezes it, and the decision's
// cohort excludes competitors that have not seen the proposal.
func TestObservedOutgoingRule_Revoke_UndecidedDisjointCohorts(t *testing.T) {
	zone1 := poolerIDs.zone1
	decision := makeRule(ruleNum(3, 0), atLeast(2), zone1.a, zone1.b, zone1.c)
	proposal := makeRule(ruleNum(3, 1), atLeast(2), zone1.c, zone1.d, zone1.e)
	rev := revocation(5, ruleNum(3, 1))
	position := &clustermetadatapb.RulePosition{Decision: decision, Proposal: proposal}
	recording := func(id *clustermetadatapb.ID) *clustermetadatapb.ConsensusStatus {
		return makeStatusWithProposal(id, position, rev, "0/1000000")
	}

	t.Run("a majority of the proposal's cohort alone is not enough", func(t *testing.T) {
		statuses := []*clustermetadatapb.ConsensusStatus{recording(zone1.d), recording(zone1.e)}
		observed, err := discoverOutgoingRule(statuses, ruleNum(3, 1), revocationBasis{})
		require.NoError(t, err)

		_, err = observed.revoke(statuses)

		require.EqualError(t, err, "insufficient outgoing decision cohort recruitment: majority not satisfied: recruited 0 of 3 cohort poolers, need at least 2")
	})

	t.Run("a majority of the decision's cohort alone is not enough", func(t *testing.T) {
		statuses := []*clustermetadatapb.ConsensusStatus{recording(zone1.a), recording(zone1.b)}
		observed, err := discoverOutgoingRule(statuses, ruleNum(3, 1), revocationBasis{})
		require.NoError(t, err)

		_, err = observed.revoke(statuses)

		require.EqualError(t, err, "insufficient outgoing cohort recruitment: majority not satisfied: recruited 0 of 3 cohort poolers, need at least 2")
	})

	t.Run("both majorities succeed", func(t *testing.T) {
		statuses := []*clustermetadatapb.ConsensusStatus{recording(zone1.a), recording(zone1.c), recording(zone1.d)}
		observed, err := discoverOutgoingRule(statuses, ruleNum(3, 1), revocationBasis{})
		require.NoError(t, err)

		_, err = observed.revoke(statuses)

		require.NoError(t, err)
	})
}

func TestObservedOutgoingRule_Revoke_InvalidPolicies(t *testing.T) {
	zone1 := poolerIDs.zone1
	cohort := []*clustermetadatapb.ID{zone1.a, zone1.b, zone1.c}
	invalid := &clustermetadatapb.DurabilityPolicy{QuorumType: clustermetadatapb.QuorumType_QUORUM_TYPE_UNKNOWN}
	rev := revocation(5, ruleNum(3, 1))

	t.Run("the outgoing rule's policy", func(t *testing.T) {
		rule := makeRule(ruleNum(3, 1), invalid, cohort...)
		statuses := []*clustermetadatapb.ConsensusStatus{makeStatus(zone1.a, rule, rev), makeStatus(zone1.b, rule, rev)}
		observed, err := discoverOutgoingRule(statuses, ruleNum(3, 1), revocationBasis{})
		require.NoError(t, err)

		_, err = observed.revoke(statuses)

		require.ErrorContains(t, err, "failed to parse durability policy from rule")
	})

	t.Run("the decision's policy when the outgoing rule is undecided", func(t *testing.T) {
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
