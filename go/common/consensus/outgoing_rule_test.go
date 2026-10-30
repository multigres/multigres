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
	"google.golang.org/protobuf/proto"

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
	proveRevoked := func(recruited []*clustermetadatapb.ConsensusStatus) error {
		observed, err := discoverOutgoingRule(recruited, ruleNum(3, 1), revocationBasis{})
		require.NoError(t, err)
		_, err = observed.proveRevoked()
		return err
	}

	t.Run("recruiting only the proposal's new members is rejected", func(t *testing.T) {
		err := proveRevoked(statuses(zone1.d, zone1.e))
		require.EqualError(t, err, "insufficient outgoing cohort recruitment: majority not satisfied: recruited 0 of 3 cohort poolers, need at least 2")
	})

	t.Run("a majority of the decision's cohort is enough", func(t *testing.T) {
		// A majority of the proposal's five-member cohort is not required.
		require.NoError(t, proveRevoked(statuses(zone1.a, zone1.b)))
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
		_, err = revokedOutgoingRule{observed}.checkRecruitsCanFinalizeProposal()
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

		_, err = observed.proveRevoked()

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

		_, err = observed.proveRevoked()

		require.ErrorContains(t, err, "failed to parse durability policy from decision")
	})
}

// mustProveRevoked builds the revoked outgoing rule found among recruits.
func mustProveRevoked(t *testing.T, recruits []*clustermetadatapb.ConsensusStatus, expected *clustermetadatapb.RuleNumber, basis revocationBasis) revokedOutgoingRule {
	t.Helper()
	observed, err := discoverOutgoingRule(recruits, expected, basis)
	require.NoError(t, err)
	revoked, err := observed.proveRevoked()
	require.NoError(t, err)
	return revoked
}

// The recruits that can lead are those at or above the minimum durable position.
// Positions compare by rule first, then by LSN.
func TestRevokedOutgoingRule_MinimumDurablePositionAndRecruitsWithAllDurableWrites(t *testing.T) {
	zone1 := poolerIDs.zone1
	cohort := []*clustermetadatapb.ID{zone1.a, zone1.b, zone1.c}
	decision := makeRule(ruleNum(3, 0), atLeast(2), cohort...)
	olderRule := makeRule(ruleNum(2, 0), atLeast(2), cohort...)
	proposal := makeRule(ruleNum(4, 0), atLeast(2), cohort...)
	withProposal := &clustermetadatapb.RulePosition{Decision: decision, Proposal: proposal}
	rev := revocation(5, ruleNum(3, 0))
	revAtProposal := revocation(5, ruleNum(4, 0))

	tests := []struct {
		name         string
		recruits     []*clustermetadatapb.ConsensusStatus
		outgoing     *clustermetadatapb.RuleNumber
		basis        revocationBasis
		wantPosition *clustermetadatapb.RulePosition
		wantLSN      string
		wantEligible []string
		wantErr      string // set when no recruit can lead
	}{
		{
			name: "the most advanced recruit sets the minimum; those behind it cannot lead",
			recruits: []*clustermetadatapb.ConsensusStatus{
				makeStatusWithLSN(zone1.a, decision, rev, "0/1000000"),
				makeStatusWithLSN(zone1.b, decision, rev, "0/2000000"),
				makeStatusWithLSN(zone1.c, decision, rev, "0/1000000"),
			},
			outgoing:     ruleNum(3, 0),
			wantPosition: &clustermetadatapb.RulePosition{Decision: decision},
			wantLSN:      "0/2000000",
			wantEligible: []string{zone1.b.GetName()},
		},
		{
			name: "recruits tied at the minimum can all lead",
			recruits: []*clustermetadatapb.ConsensusStatus{
				makeStatusWithLSN(zone1.a, decision, rev, "0/2000000"),
				makeStatusWithLSN(zone1.b, decision, rev, "0/2000000"),
				makeStatusWithLSN(zone1.c, decision, rev, "0/1000000"),
			},
			outgoing:     ruleNum(3, 0),
			wantPosition: &clustermetadatapb.RulePosition{Decision: decision},
			wantLSN:      "0/2000000",
			wantEligible: []string{zone1.a.GetName(), zone1.b.GetName()},
		},
		{
			name: "an older rule loses to the outgoing rule even with a far higher LSN",
			recruits: []*clustermetadatapb.ConsensusStatus{
				makeStatusWithLSN(zone1.a, decision, rev, "0/1000000"),
				makeStatusWithLSN(zone1.b, decision, rev, "0/2000000"),
				makeStatusWithLSN(zone1.c, olderRule, rev, "0/9000000"),
			},
			outgoing:     ruleNum(3, 0),
			wantPosition: &clustermetadatapb.RulePosition{Decision: decision},
			wantLSN:      "0/2000000",
			wantEligible: []string{zone1.b.GetName()},
		},
		{
			name: "an undecided proposal outranks a bare decision even with a far higher LSN",
			recruits: []*clustermetadatapb.ConsensusStatus{
				makeStatusWithProposal(zone1.a, withProposal, revAtProposal, "0/2000000"),
				makeStatusWithProposal(zone1.b, withProposal, revAtProposal, "0/1000000"),
				makeStatusWithLSN(zone1.c, decision, revAtProposal, "0/9000000"),
			},
			outgoing:     ruleNum(4, 0),
			wantPosition: withProposal,
			wantLSN:      "0/2000000",
			wantEligible: []string{zone1.a.GetName()},
		},
		{
			name: "a cert's frozen LSN above every recruit raises the minimum, leaving no eligible leader",
			recruits: []*clustermetadatapb.ConsensusStatus{
				makeStatusWithLSN(zone1.a, decision, rev, "0/1000000"),
				makeStatusWithLSN(zone1.b, decision, rev, "0/2000000"),
			},
			outgoing:     ruleNum(3, 0),
			basis:        mustCertBasis(rev, "0/3000000", cohort, atLeast(2)),
			wantPosition: &clustermetadatapb.RulePosition{Decision: decision},
			wantLSN:      "0/3000000",
			wantErr:      "no recruited nodes at or above the required LSN 0/3000000",
		},
		{
			name: "a recruit exactly at a cert's frozen LSN can lead",
			recruits: []*clustermetadatapb.ConsensusStatus{
				makeStatusWithLSN(zone1.a, decision, rev, "0/2000000"),
			},
			outgoing:     ruleNum(3, 0),
			basis:        mustCertBasis(rev, "0/2000000", []*clustermetadatapb.ID{zone1.a}, atLeast(1)),
			wantPosition: &clustermetadatapb.RulePosition{Decision: decision},
			wantLSN:      "0/2000000",
			wantEligible: []string{zone1.a.GetName()},
		},
		{
			// Conservative: a cert would allow any recruit at or above its frozen LSN.
			name: "a cert's frozen LSN below the most advanced recruit does not lower the minimum",
			recruits: []*clustermetadatapb.ConsensusStatus{
				makeStatusWithLSN(zone1.a, decision, rev, "0/1000000"),
				makeStatusWithLSN(zone1.b, decision, rev, "0/2000000"),
				makeStatusWithLSN(zone1.c, decision, rev, "0/3000000"),
			},
			outgoing:     ruleNum(3, 0),
			basis:        mustCertBasis(rev, "0/1000000", cohort, atLeast(2)),
			wantPosition: &clustermetadatapb.RulePosition{Decision: decision},
			wantLSN:      "0/3000000",
			wantEligible: []string{zone1.c.GetName()},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			revoked := mustProveRevoked(t, tt.recruits, tt.outgoing, tt.basis)

			minimum, err := revoked.minimumDurablePosition()
			require.NoError(t, err)
			assert.True(t, proto.Equal(tt.wantPosition, minimum.GetPosition()), "minimum rule position: got %s", FormatRulePosition(minimum.GetPosition()))
			assert.Equal(t, tt.wantLSN, minimum.GetLsn())

			eligible, err := revoked.recruitsWithAllDurableWrites()
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			var names []string
			for _, cs := range eligible {
				names = append(names, cs.GetId().GetName())
			}
			assert.ElementsMatch(t, tt.wantEligible, names)
		})
	}
}

// A cert revokes the outgoing rule without recruiting its cohort; coordinator-led
// revocation of the same lone recruit is rejected.
func TestObservedOutgoingRule_Revoke_CertAcceptsWhatRecruitmentRejects(t *testing.T) {
	zone1 := poolerIDs.zone1
	incoming := []*clustermetadatapb.ID{zone1.a, zone1.b, zone1.c}
	// The outgoing cohort is never recruited.
	rule := makeRule(ruleNum(0, 1), atLeast(2), zone1.d, zone1.e, zone1.f)
	rev := revocation(5, ruleNum(0, 1))
	statuses := []*clustermetadatapb.ConsensusStatus{
		makeStatusWithLSN(zone1.a, rule, rev, "0/2000000"),
		makeStatusWithLSN(zone1.b, rule, rev, "0/1000000"),
	}

	t.Run("recruits at or above the frozen LSN can lead", func(t *testing.T) {
		observed, err := discoverOutgoingRule(statuses, ruleNum(0, 1), mustCertBasis(rev, "0/2000000", incoming, atLeast(2)))
		require.NoError(t, err)
		revoked, err := observed.proveRevoked()
		require.NoError(t, err)

		leaders, err := revoked.recruitsWithAllDurableWrites()

		require.NoError(t, err)
		require.Len(t, leaders, 1)
		assert.Equal(t, zone1.a.GetName(), leaders[0].GetId().GetName())
	})

	t.Run("none at or above the frozen LSN", func(t *testing.T) {
		observed, err := discoverOutgoingRule(statuses, ruleNum(0, 1), mustCertBasis(rev, "0/3000000", incoming, atLeast(2)))
		require.NoError(t, err)
		revoked, err := observed.proveRevoked()
		require.NoError(t, err)

		_, err = revoked.recruitsWithAllDurableWrites()

		require.EqualError(t, err, "no recruited nodes at or above the required LSN 0/3000000")
	})

	t.Run("a coordinator-led revocation recruits the outgoing cohort instead", func(t *testing.T) {
		observed, err := discoverOutgoingRule(statuses, ruleNum(0, 1), revocationBasis{})
		require.NoError(t, err)

		_, err = observed.proveRevoked()

		require.EqualError(t, err, "insufficient outgoing cohort recruitment: majority not satisfied: recruited 0 of 3 cohort poolers, need at least 2")
	})
}

// A cert revokes the outgoing rule only together with recruitment of the
// incoming cohort, which forces any retries of the same term to overlap.
func TestObservedOutgoingRule_RevokeByCert_IncomingRecruitment(t *testing.T) {
	zone1 := poolerIDs.zone1
	cohort := []*clustermetadatapb.ID{zone1.a, zone1.b, zone1.c, zone1.d, zone1.e}
	rule := makeRule(ruleNum(0, 1), atLeast(2), cohort...)
	rev := revocation(5, ruleNum(0, 1))
	basis := mustCertBasis(rev, "0/0", cohort, atLeast(2))
	recruiting := func(ids ...*clustermetadatapb.ID) []*clustermetadatapb.ConsensusStatus {
		var statuses []*clustermetadatapb.ConsensusStatus
		for _, id := range ids {
			statuses = append(statuses, makeStatus(id, rule, rev))
		}
		return statuses
	}

	t.Run("a minority of the incoming cohort is rejected", func(t *testing.T) {
		statuses := recruiting(zone1.a, zone1.b)
		observed, err := discoverOutgoingRule(statuses, ruleNum(0, 1), basis)
		require.NoError(t, err)

		_, err = observed.proveRevoked()

		require.EqualError(t, err, "insufficient proposed cohort recruitment: majority not satisfied: recruited 2 of 5 cohort poolers, need at least 3")
	})

	t.Run("leftovers that could still reach the policy are rejected", func(t *testing.T) {
		// AT_LEAST_2 over 5: recruiting 3 leaves 2, which could satisfy it alone.
		statuses := recruiting(zone1.a, zone1.b, zone1.c)
		observed, err := discoverOutgoingRule(statuses, ruleNum(0, 1), basis)
		require.NoError(t, err)

		_, err = observed.proveRevoked()

		require.EqualError(t, err, "insufficient proposed cohort recruitment: revocation not satisfied: un-recruited cohort poolers [zone1_pooler-d, zone1_pooler-e] could independently satisfy AT_LEAST_N(N=2)")
	})

	t.Run("enough recruits succeed", func(t *testing.T) {
		statuses := recruiting(zone1.a, zone1.b, zone1.c, zone1.d)
		observed, err := discoverOutgoingRule(statuses, ruleNum(0, 1), basis)
		require.NoError(t, err)

		_, err = observed.proveRevoked()

		require.NoError(t, err)
	})

	t.Run("recruits outside the incoming cohort are ignored", func(t *testing.T) {
		// f is recruited and records the rule but is not in the incoming cohort.
		statuses := recruiting(zone1.a, zone1.b, zone1.c, zone1.d, zone1.f)
		observed, err := discoverOutgoingRule(statuses, ruleNum(0, 1), basis)
		require.NoError(t, err)

		_, err = observed.proveRevoked()

		require.NoError(t, err)
	})
}

// The incoming-cohort recruitment only protects a proposal for the cohort and
// policy it was made against, so the built proposal must carry exactly those.
func TestBuildExternallyCertifiedProposal_MustMatchProvenCohortAndPolicy(t *testing.T) {
	zone1 := poolerIDs.zone1
	provenCohort := []*clustermetadatapb.ID{zone1.a, zone1.b, zone1.c}
	rule := makeRule(ruleNum(0, 1), atLeast(2), provenCohort...)
	rev := revocation(5, ruleNum(0, 1))
	cert := &clustermetadatapb.ExternallyCertifiedRevocation{TermRevocation: rev, FrozenLsn: "0/0"}
	statuses := []*clustermetadatapb.ConsensusStatus{
		makeStatus(zone1.a, rule, rev), makeStatus(zone1.b, rule, rev), makeStatus(zone1.c, rule, rev),
	}
	propose := func(policy *clustermetadatapb.DurabilityPolicy, cohort ...*clustermetadatapb.ID) func(RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
		return func(r RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
			return &consensusdatapb.CoordinatorProposal{
				TermRevocation: r.TermRevocation,
				ProposalLeader: &clustermetadatapb.PoolerAddress{Id: zone1.a},
				ProposedTransition: &clustermetadatapb.RulePosition{
					Decision: &clustermetadatapb.ShardRule{RuleNumber: r.TermRevocation.GetOutgoingRule()},
					Proposal: makeRule(ruleNum(5, 0), policy, cohort...),
				},
				SkipOutgoingQuorum: true,
			}, nil
		}
	}
	const wantErr = "proposal validation: proposed rule's cohort or durability policy differs from the incoming cohort and policy the revocation was proven against"

	_, err := BuildExternallyCertifiedProposal(cert, provenCohort, atLeast(2), statuses, propose(atLeast(2), zone1.a, zone1.b, zone1.c))
	require.NoError(t, err)

	t.Run("a different cohort", func(t *testing.T) {
		_, err := BuildExternallyCertifiedProposal(cert, provenCohort, atLeast(2), statuses, propose(atLeast(2), zone1.a, zone1.b))
		require.EqualError(t, err, wantErr)
	})

	t.Run("a different policy over the same cohort", func(t *testing.T) {
		_, err := BuildExternallyCertifiedProposal(cert, provenCohort, atLeast(2), statuses, propose(atLeast(3), zone1.a, zone1.b, zone1.c))
		require.EqualError(t, err, wantErr)
	})
}

func TestNewCertBasis_Invalid(t *testing.T) {
	zone1 := poolerIDs.zone1
	cohort := []*clustermetadatapb.ID{zone1.a, zone1.b, zone1.c}

	t.Run("invalid incoming policy", func(t *testing.T) {
		cert := &clustermetadatapb.ExternallyCertifiedRevocation{TermRevocation: revocation(5, ruleNum(0, 1)), FrozenLsn: "0/0"}
		_, err := newCertBasis(cert, cohort, &clustermetadatapb.DurabilityPolicy{QuorumType: clustermetadatapb.QuorumType_QUORUM_TYPE_UNKNOWN})
		require.ErrorContains(t, err, "invalid incoming durability policy")
	})

	t.Run("cert without a frozen LSN", func(t *testing.T) {
		_, err := newCertBasis(&clustermetadatapb.ExternallyCertifiedRevocation{TermRevocation: revocation(5, ruleNum(0, 1))}, cohort, atLeast(2))
		require.EqualError(t, err, "cert is missing frozen_lsn")
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

func TestRevokedOutgoingRule_MinimumDurablePosition_NoRecruitRecordsRule(t *testing.T) {
	revoked := revokedOutgoingRule{observedOutgoingRule{transition: NewRuleTransition(&clustermetadatapb.RulePosition{})}}

	_, err := revoked.minimumDurablePosition()

	require.EqualError(t, err, "no recruit records the outgoing rule")
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
