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
	"fmt"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/tools/pgutil"
)

// observedOutgoingRule is the rule a coordinator-led or certified proposal
// transitions from: the highest rule position any recruit records, confirmed to
// be the rule the revocation expects.
type observedOutgoingRule struct {
	// recruited are the recruits the outgoing rule was found among.
	recruited []*clustermetadatapb.ConsensusStatus
	// position is the highest rule position the recruits record. The outgoing rule
	// is PossiblyUndecidedRule(position): the decision, or the undecided proposal
	// on top of it. Trusting an undecided rule needs no separate verification:
	// UpdateRule always finalizes it, exactly as it stands, before applying
	// anything new.
	position *clustermetadatapb.RulePosition
	// basis is how this rule is to be revoked; see revoke.
	basis revocationBasis
}

// recruitsRecordingRule are all recruits that record the outgoing position,
// whether or not they are members of the rule's cohort.
func (o observedOutgoingRule) recruitsRecordingRule() []*clustermetadatapb.ConsensusStatus {
	var recording []*clustermetadatapb.ConsensusStatus
	for _, cs := range o.recruited {
		if CompareRulePosition(cs.GetCurrentPosition().GetPosition(), o.position) == 0 {
			recording = append(recording, cs)
		}
	}
	return recording
}

// discoverOutgoingRule finds the outgoing rule among recruited and checks that
// it is the rule number the revocation expects.
func discoverOutgoingRule(
	recruited []*clustermetadatapb.ConsensusStatus,
	expected *clustermetadatapb.RuleNumber,
	basis revocationBasis,
) (observedOutgoingRule, error) {
	recruitsRecordingRule, position, err := discoverOutgoingRulePosition(recruited)
	if err != nil {
		return observedOutgoingRule{}, err
	}
	observed := observedOutgoingRule{recruited: recruited, position: position, basis: basis}
	outgoing := PossiblyUndecidedRule(position)
	cmp := CompareRuleNumbers(outgoing.GetRuleNumber(), expected)
	if cmp == 0 {
		return observed, nil
	}
	if basis.cert != nil {
		// The externally-certified path has no coordinator "view" to
		// re-discover — the caller's cert is simply stale relative to what's
		// actually observed now.
		return observedOutgoingRule{}, fmt.Errorf(
			"recruit %s reports rule %v, expected outgoing rule %v; cert no longer matches observed state",
			topoclient.ClusterIDString(recruitsRecordingRule[0].GetId()), outgoing.GetRuleNumber(), expected,
		)
	}
	if cmp > 0 {
		return observedOutgoingRule{}, fmt.Errorf(
			"recruit %s reports position %s ahead of expected outgoing rule %v; coordinator view is stale, re-discover",
			topoclient.ClusterIDString(recruitsRecordingRule[0].GetId()), FormatRulePosition(position), expected,
		)
	}
	return observedOutgoingRule{}, fmt.Errorf(
		"no recruit reports the expected outgoing rule %v; cannot determine cohort for quorum check",
		expected,
	)
}

// revokedOutgoingRule is an observedOutgoingRule that is known to be revoked:
// no write can become durable under it that the recruits cannot see. It can only
// be built by revoke, so anything derived from it rests on that proof.
type revokedOutgoingRule struct {
	observedOutgoingRule
}

// revoke establishes that the outgoing rule is revoked, by the evidence its
// basis names: recruitment of the outgoing cohort, or an external cert.
func (o observedOutgoingRule) revoke() (revokedOutgoingRule, error) {
	if o.basis.cert == nil {
		if err := o.revokeByRecruitment(); err != nil {
			return revokedOutgoingRule{}, err
		}
	}
	// Revoking by cert needs no recruitment of the outgoing cohort: the cert
	// promises nothing durable lies beyond its frozen LSN under the outgoing
	// rule.
	return revokedOutgoingRule{o}, nil
}

// revokeByRecruitment requires the outgoing decision's cohort to be sufficiently
// recruited (see CheckSufficientRecruitment). This is what makes the proposal
// safe.
//
// That is the cohort every competing coordinator recruits for this term,
// whichever rule it believes is outgoing, so a recruited majority forces
// overlap with all of them. Its leftovers also cannot commit, so no write that
// is durable under the outgoing rule is invisible to the recruits: while a
// proposal is undecided, a write needs the "Both" policy of the decision and the
// proposal, and the cohort of that policy lies within the decision's.
func (o observedOutgoingRule) revokeByRecruitment() error {
	decision := o.position.GetDecision()
	policy, err := NewPolicyFromProto(decision.GetDurabilityPolicy())
	if err != nil {
		return fmt.Errorf("failed to parse durability policy from decision: %w", err)
	}
	cohort := decision.GetCohortMembers()
	if err := CheckSufficientRecruitment(policy, cohort, statusesToIDs(filterCohortStatuses(cohort, o.recruited))); err != nil {
		return fmt.Errorf("insufficient outgoing cohort recruitment: %w", err)
	}
	return nil
}

// finalizableOutgoingRule is a revokedOutgoingRule that the recruits can also
// finish: Promote can decide it, if it is an undecided proposal. It can only be
// built by checkRecruitsCanFinalizeProposal, and a RecruitmentResult can only be
// built from one, so no proposal is built on a stuck proposal that could never be
// decided.
type finalizableOutgoingRule struct {
	revokedOutgoingRule
}

// checkRecruitsCanFinalizeProposal predicts that Promote can finish a
// coordinator-led change through an undecided outgoing rule. This is about
// liveness, not safety.
//
// Promote writes twice. It first decides the stuck proposal, under the "Both"
// policy of the decision and the proposal: the recruited members of that
// policy's cohort must be able to satisfy it, or the proposal could never be
// decided. This check covers that write, which depends only on the outgoing rule
// and the recruits. Promote then writes the new rule, which depends on the
// proposal built next, so validateProposal checks that write. A transition the
// pooler cannot compute a "Both" policy for could never be decided either, so it
// is rejected here with a clear error. A malformed proposal fails closed.
//
// Nothing is required for a decided outgoing rule, or when a cert revokes the
// rule: the pooler then decides the proposal under the incoming policy instead.
func (r revokedOutgoingRule) checkRecruitsCanFinalizeProposal() (finalizableOutgoingRule, error) {
	if IsRuleDecided(r.position) || r.basis.cert != nil {
		return finalizableOutgoingRule{r}, nil
	}
	decision := r.position.GetDecision()
	decisionPolicy, err := NewPolicyFromProto(decision.GetDurabilityPolicy())
	if err != nil {
		return finalizableOutgoingRule{}, fmt.Errorf("failed to parse durability policy from decision: %w", err)
	}
	proposal := PossiblyUndecidedRule(r.position)
	proposalPolicy, err := NewPolicyFromProto(proposal.GetDurabilityPolicy())
	if err != nil {
		return finalizableOutgoingRule{}, fmt.Errorf("failed to parse durability policy from rule: %w", err)
	}
	transition, err := BuildPolicyTransition(
		PolicyWithCohort{Policy: decisionPolicy, Cohort: decision.GetCohortMembers()},
		PolicyWithCohort{Policy: proposalPolicy, Cohort: proposal.GetCohortMembers()},
	)
	if err != nil {
		return finalizableOutgoingRule{}, fmt.Errorf("outgoing proposal cannot be finalized: %w", err)
	}
	reachable := statusesToIDs(filterCohortStatuses(transition.Both.Cohort, r.recruited))
	if err := transition.Both.Policy.SatisfiedBy(reachable); err != nil {
		return finalizableOutgoingRule{}, fmt.Errorf("outgoing proposal cannot be finalized: recruited members cannot satisfy its policy: %w", err)
	}
	return finalizableOutgoingRule{r}, nil
}

// eligibleLeaders returns the recruits that can lead: those recording the
// outgoing rule that are tied at the highest LSN at or above the basis's frozen
// LSN (zero when revoking by recruitment).
func (r revokedOutgoingRule) eligibleLeaders() ([]*clustermetadatapb.ConsensusStatus, error) {
	var minLSN pgutil.LSN
	if r.basis.cert != nil {
		minLSN = r.basis.cert.frozenLSN
	}
	return narrowByLSN(r.recruitsRecordingRule(), minLSN)
}
