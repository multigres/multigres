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
	"fmt"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// observedOutgoingRule is the rule a coordinator-led or certified proposal
// transitions from: the highest rule position any recruit records, confirmed to
// be the rule the revocation expects.
type observedOutgoingRule struct {
	// recruited are the recruits the outgoing rule was found among.
	recruited []*clustermetadatapb.ConsensusStatus
	// transition is the highest rule position the recruits record, read as a
	// transition. The outgoing rule is its PossiblyUndecidedRule(): the decision, or
	// the undecided proposal on top of it. Trusting an undecided rule needs no
	// separate verification: UpdateRule always finalizes it, exactly as it stands,
	// before applying anything new.
	transition RuleTransition
	// basis is how this rule is to be revoked; see proveRevoked.
	basis revocationBasis
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
	transition := NewRuleTransition(position)
	observed := observedOutgoingRule{recruited: recruited, transition: transition, basis: basis}
	outgoing := transition.PossiblyUndecidedRule()
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
			topoclient.ClusterIDString(recruitsRecordingRule[0].GetId()), transition, expected,
		)
	}
	return observedOutgoingRule{}, fmt.Errorf(
		"no recruit reports the expected outgoing rule %v; cannot determine cohort for quorum check",
		expected,
	)
}

// revokedOutgoingRule is an observedOutgoingRule that is known to be revoked:
// no write can become durable under it that the recruits cannot see. It can only
// be built by proveRevoked, so anything derived from it rests on that proof.
type revokedOutgoingRule struct {
	observedOutgoingRule
}

// proveRevoked establishes that the outgoing rule is revoked, by the evidence its
// basis names: recruitment of the outgoing decision's cohort, or an external cert
// plus recruitment of the incoming cohort.
func (o observedOutgoingRule) proveRevoked() (revokedOutgoingRule, error) {
	var err error
	if o.basis.cert != nil {
		err = o.proveRevokedByCert()
	} else {
		err = o.proveRevokedByRecruitment()
	}
	if err != nil {
		return revokedOutgoingRule{}, err
	}
	return revokedOutgoingRule{o}, nil
}

// proveRevokedByCert relies on the cert's two promises about the outgoing rule:
// nothing durable lies beyond its frozen LSN, and its term is unique. The
// outgoing cohort is not recruited, so the term can still be retried by the same
// coordinator, for example repeated bootstrap attempts with different proposed
// leaders. A majority of the incoming cohort forces any two such attempts to
// overlap, so they cannot both succeed. This relies on retries proposing the same
// cohort, which nothing enforces. The unrecruited-cannot-commit half also rejects
// a stale cert: if an earlier attempt's rule became durable on this cohort, the
// recruits must overlap its quorum, and holders of that rule refuse the new
// revocation.
func (o observedOutgoingRule) proveRevokedByCert() error {
	cert := o.basis.cert
	recruitedIncoming := statusesToIDs(filterCohortStatuses(cert.incomingCohort, o.recruited))
	if err := CheckSufficientRecruitment(cert.incomingPolicy, cert.incomingCohort, recruitedIncoming); err != nil {
		return fmt.Errorf("insufficient proposed cohort recruitment: %w", err)
	}
	return nil
}

// proveRevokedByRecruitment requires the outgoing decision's cohort to be sufficiently
// recruited (see CheckSufficientRecruitment). This is what makes the proposal
// safe.
//
// That is the cohort every competing coordinator recruits for this term,
// whichever rule it believes is outgoing, so a recruited majority forces
// overlap with all of them. Its leftovers also cannot commit, so no write that
// is durable under the outgoing rule is invisible to the recruits: while a
// proposal is undecided, a write needs the "Both" policy of the decision and the
// proposal, and the cohort of that policy lies within the decision's.
func (o observedOutgoingRule) proveRevokedByRecruitment() error {
	decision := o.transition.Decision()
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
	if r.transition.IsDecided() || r.basis.cert != nil {
		return finalizableOutgoingRule{r}, nil
	}
	decision := r.transition.Decision()
	decisionPolicy, err := NewPolicyFromProto(decision.GetDurabilityPolicy())
	if err != nil {
		return finalizableOutgoingRule{}, fmt.Errorf("failed to parse durability policy from decision: %w", err)
	}
	proposal := r.transition.PossiblyUndecidedRule()
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

// minimumDurablePosition is the lowest position a leader may have: one at or above
// it holds every write the outgoing rule could have made durable. It is the most
// advanced recruit's position (always at the outgoing rule, the highest any recruit
// records), raised to the cert's frozen LSN under a cert.
//
// TODO: take it from recruited members of the outgoing decision's cohort (the
// incoming cohort under a cert), so a further-ahead non-member does not raise it.
// TODO: relax it to the highest position the policy could have made durable, or
// to exactly the frozen LSN under a cert.
func (r revokedOutgoingRule) minimumDurablePosition() (*clustermetadatapb.PoolerPosition, error) {
	var minimum *clustermetadatapb.PoolerPosition
	for _, cs := range r.recruited {
		if position := cs.GetCurrentPosition(); minimum == nil || ComparePoolerPosition(position, minimum) > 0 {
			minimum = position
		}
	}
	if minimum == nil {
		return nil, errors.New("no recruit records the outgoing rule")
	}
	if cert := r.basis.cert; cert != nil {
		frozen := &clustermetadatapb.PoolerPosition{Position: r.transition.Position(), Lsn: cert.frozenLSN.String()}
		if ComparePoolerPosition(frozen, minimum) > 0 {
			minimum = frozen
		}
	}
	return minimum, nil
}

// recruitsWithAllDurableWrites returns the recruits at or above the minimum
// durable position. Only recruits can lead: Promote needs a recruit still in
// force, which proves the pooler is reachable and fenced and which later events,
// such as a restore, invalidate.
func (r revokedOutgoingRule) recruitsWithAllDurableWrites() ([]*clustermetadatapb.ConsensusStatus, error) {
	minimum, err := r.minimumDurablePosition()
	if err != nil {
		return nil, err
	}
	var eligible []*clustermetadatapb.ConsensusStatus
	for _, cs := range r.recruited {
		if ComparePoolerPosition(cs.GetCurrentPosition(), minimum) >= 0 {
			eligible = append(eligible, cs)
		}
	}
	if len(eligible) == 0 {
		// Only a cert's frozen LSN above every recruit leaves none: otherwise the
		// minimum is itself a recruit's position.
		return nil, fmt.Errorf("no recruited nodes at or above the required LSN %s", minimum.GetLsn())
	}
	return eligible, nil
}
