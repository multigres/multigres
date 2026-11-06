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

	"google.golang.org/protobuf/proto"

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

// recruitsRecordingRule are all recruits that record the outgoing rule, whether
// or not they know it is decided and whether or not they are members of its
// cohort.
func (o observedOutgoingRule) recruitsRecordingRule() []*clustermetadatapb.ConsensusStatus {
	latest := o.transition.PossiblyUndecidedRule().GetRuleNumber()
	var recording []*clustermetadatapb.ConsensusStatus
	for _, cs := range o.recruited {
		if CompareRuleNumbers(RuleNumberPositionOf(cs.GetCurrentPosition().GetPosition()).Latest(), latest) == 0 {
			recording = append(recording, cs)
		}
	}
	return recording
}

// outgoingView is what a set of statuses collectively says about the rule a
// rule change transitions from. Two independent facts are joined, not read from
// any one status: which rules are decided (a decision is never forgotten, so
// this is a maximum) and which rule the WAL records last (what a node holds does
// not depend on what it knows is decided).
type outgoingView struct {
	// base is the highest decision any status knows.
	base *clustermetadatapb.ShardRule
	// latest is the newest rule any status records: an undecided proposal beyond
	// base, or base itself.
	latest *clustermetadatapb.ShardRule
	// holders are the statuses that record latest.
	holders []*clustermetadatapb.ConsensusStatus
}

// discoverOutgoing joins statuses into an outgoingView. It fails if statuses
// disagree on the content of a rule they share the number of: that is a data
// inconsistency the caller cannot safely proceed past.
func discoverOutgoing(statuses []*clustermetadatapb.ConsensusStatus) (outgoingView, error) {
	var view outgoingView
	var baseSource, latestSource *clustermetadatapb.ConsensusStatus
	for _, cs := range statuses {
		position := cs.GetCurrentPosition().GetPosition()
		if baseSource == nil || CompareRuleNumbers(position.GetDecision().GetRuleNumber(), view.base.GetRuleNumber()) > 0 {
			baseSource, view.base = cs, position.GetDecision()
		}
		if latestSource == nil || CompareRuleNumbers(PossiblyUndecidedRule(position).GetRuleNumber(), view.latest.GetRuleNumber()) > 0 {
			latestSource, view.latest = cs, PossiblyUndecidedRule(position)
		}
	}
	for _, cs := range statuses {
		position := cs.GetCurrentPosition().GetPosition()
		if CompareRuleNumbers(position.GetDecision().GetRuleNumber(), view.base.GetRuleNumber()) == 0 &&
			!sameRuleContent(position.GetDecision(), view.base) {
			return outgoingView{}, disagreeError(baseSource, cs)
		}
		if CompareRuleNumbers(PossiblyUndecidedRule(position).GetRuleNumber(), view.latest.GetRuleNumber()) == 0 {
			if !sameRuleContent(PossiblyUndecidedRule(position), view.latest) {
				return outgoingView{}, disagreeError(latestSource, cs)
			}
			view.holders = append(view.holders, cs)
		}
	}
	if CompareRuleNumbers(view.base.GetRuleNumber(), view.latest.GetRuleNumber()) > 0 {
		return outgoingView{}, fmt.Errorf("decision %s is beyond every recorded rule %s; inconsistent discovery result",
			FormatRuleNumber(view.base.GetRuleNumber()), FormatRuleNumber(view.latest.GetRuleNumber()))
	}
	return view, nil
}

func disagreeError(a, b *clustermetadatapb.ConsensusStatus) error {
	return fmt.Errorf("recruits %s and %s agree on rule number but disagree on content; inconsistent discovery result",
		topoclient.ClusterIDString(a.GetId()), topoclient.ClusterIDString(b.GetId()))
}

// sameRuleContent reports whether two rules with the same number agree on what
// the protocol acts on: leader, cohort and policy. The coordinator and creation
// time are bookkeeping that deciding a proposal can rewrite.
func sameRuleContent(a, b *clustermetadatapb.ShardRule) bool {
	return idsEqual(a.GetLeaderId(), b.GetLeaderId()) &&
		sameCohort(a.GetCohortMembers(), b.GetCohortMembers()) &&
		proto.Equal(a.GetDurabilityPolicy(), b.GetDurabilityPolicy())
}

// transition reads the view as a RuleTransition: the base decision, plus the
// latest rule as an undecided proposal when it is beyond the base.
func (v outgoingView) transition() RuleTransition {
	position := &clustermetadatapb.RulePosition{Decision: v.base}
	if CompareRuleNumbers(v.latest.GetRuleNumber(), v.base.GetRuleNumber()) > 0 {
		position.Proposal = v.latest
	}
	return NewRuleTransition(position)
}

// discoverOutgoingRule finds the outgoing rule among recruited and checks that
// it is the rule number the revocation expects.
func discoverOutgoingRule(
	recruited []*clustermetadatapb.ConsensusStatus,
	expected *clustermetadatapb.RuleNumber,
	basis revocationBasis,
) (observedOutgoingRule, error) {
	view, err := discoverOutgoing(recruited)
	if err != nil {
		return observedOutgoingRule{}, err
	}
	recruitsRecordingRule := view.holders
	transition := view.transition()
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
// overlap with all of them.
//
// An undecided proposal may in fact be decided elsewhere, after which writes
// commit under its policy alone, so unless the recruits show it cannot have
// been decided, its cohort must be frozen as well.
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
	if o.transition.IsDecided() {
		return nil
	}
	return o.proveProposalFrozen(policy)
}

// proveProposalFrozen requires the unrecruited members of the undecided
// proposal's cohort to be unable to commit under its policy, unless the
// proposal cannot have been decided.
func (o observedOutgoingRule) proveProposalFrozen(decisionPolicy DurabilityPolicy) error {
	decision := o.transition.Decision()
	proposal := o.transition.Proposal()
	proposalPolicy, err := NewPolicyFromProto(proposal.GetDurabilityPolicy())
	if err != nil {
		return fmt.Errorf("failed to parse durability policy from rule: %w", err)
	}
	transition, err := BuildPolicyTransition(
		PolicyWithCohort{Policy: decisionPolicy, Cohort: decision.GetCohortMembers()},
		PolicyWithCohort{Policy: proposalPolicy, Cohort: proposal.GetCohortMembers()},
	)
	if err != nil {
		// Promote can never decide such a proposal; checkRecruitsCanFinalizeProposal rejects it.
		return nil //nolint:nilerr // rejected as unfinalizable, not by this safety check
	}
	if !o.proposalMayBeDecided(transition.Both) {
		return nil
	}
	cohort := proposal.GetCohortMembers()
	recruited := statusesToIDs(filterCohortStatuses(cohort, o.recruited))
	if err := checkUnrecruitedCannotCommit(proposalPolicy, cohort, recruited); err != nil {
		return fmt.Errorf("insufficient proposed cohort recruitment: %w", err)
	}
	return nil
}

// proposalMayBeDecided reports whether the proposal could have been decided
// under both: only the recruits that record it and the unrecruited members of
// both's cohort could have acknowledged it. Other recruits are fenced.
func (o observedOutgoingRule) proposalMayBeDecided(both PolicyWithCohort) bool {
	recruited := statusesToIDs(filterCohortStatuses(both.Cohort, o.recruited))
	holders := statusesToIDs(filterCohortStatuses(both.Cohort, o.recruitsRecordingRule()))
	possible := append(holders, unrecruitedOf(normalizeIDs(both.Cohort), normalizeIDs(recruited))...)
	return both.Policy.SatisfiedBy(possible) == nil
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
// A decided outgoing rule can still be undecided on the leader that is chosen;
// see checkLeaderCanFinalizeProposal.
func (r revokedOutgoingRule) checkRecruitsCanFinalizeProposal() (finalizableOutgoingRule, error) {
	if r.transition.IsDecided() || r.basis.cert != nil {
		return finalizableOutgoingRule{r}, nil
	}
	if err := r.canFinalize(r.transition); err != nil {
		return finalizableOutgoingRule{}, err
	}
	return finalizableOutgoingRule{r}, nil
}

// checkLeaderCanFinalizeProposal is checkRecruitsCanFinalizeProposal for the
// rule as the chosen leader records it. A recruit can lead while recording the
// outgoing rule as an undecided proposal even though another recruit knows it is
// decided, and Promote on that leader must finish deciding it first.
func (r revokedOutgoingRule) checkLeaderCanFinalizeProposal(leader *clustermetadatapb.ConsensusStatus) error {
	leaderTransition := NewRuleTransition(leader.GetCurrentPosition().GetPosition())
	if leaderTransition.IsDecided() || r.basis.cert != nil {
		return nil
	}
	return r.canFinalize(leaderTransition)
}

// canFinalize checks that the recruits can satisfy the "Both" policy of t's
// decision and proposal, which Promote needs to decide the proposal.
func (r revokedOutgoingRule) canFinalize(t RuleTransition) error {
	decision := t.Decision()
	decisionPolicy, err := NewPolicyFromProto(decision.GetDurabilityPolicy())
	if err != nil {
		return fmt.Errorf("failed to parse durability policy from decision: %w", err)
	}
	proposal := t.PossiblyUndecidedRule()
	proposalPolicy, err := NewPolicyFromProto(proposal.GetDurabilityPolicy())
	if err != nil {
		return fmt.Errorf("failed to parse durability policy from rule: %w", err)
	}
	transition, err := BuildPolicyTransition(
		PolicyWithCohort{Policy: decisionPolicy, Cohort: decision.GetCohortMembers()},
		PolicyWithCohort{Policy: proposalPolicy, Cohort: proposal.GetCohortMembers()},
	)
	if err != nil {
		return fmt.Errorf("outgoing proposal cannot be finalized: %w", err)
	}
	reachable := statusesToIDs(filterCohortStatuses(transition.Both.Cohort, r.recruited))
	if err := transition.Both.Policy.SatisfiedBy(reachable); err != nil {
		return fmt.Errorf("outgoing proposal cannot be finalized: recruited members cannot satisfy its policy: %w", err)
	}
	return nil
}

// minimumLeaderPosition is the lowest position a leader may have: one at or above
// it holds every write a quorum under the outgoing rule could hold. Such a write
// may never have been decided, so this is not a commit point. It is the most
// advanced recruit's position (always at the outgoing rule, the highest any recruit
// records), raised to the cert's frozen LSN under a cert.
//
// TODO: take it from recruited members only, so a further-ahead non-member does not
// raise it. When revoking by recruitment, those are the outgoing decision's cohort
// plus, if an undecided proposal may be decided, the proposal's cohort. Under a
// cert, it is the incoming cohort.
// TODO: relax it to the highest position the policy could have made quorum-held,
// or to exactly the frozen LSN under a cert.
func (r revokedOutgoingRule) minimumLeaderPosition() (*clustermetadatapb.PoolerPosition, error) {
	var minimum *clustermetadatapb.PoolerPosition
	for _, cs := range r.recruited {
		if position := cs.GetCurrentPosition(); minimum == nil || CompareWALProgress(position, minimum) > 0 {
			minimum = position
		}
	}
	if minimum == nil {
		return nil, errors.New("no recruit records the outgoing rule")
	}
	if cert := r.basis.cert; cert != nil {
		frozen := &clustermetadatapb.PoolerPosition{Position: r.transition.Position(), Lsn: cert.frozenLSN.String()}
		if CompareWALProgress(frozen, minimum) > 0 {
			minimum = frozen
		}
	}
	return minimum, nil
}

// eligibleLeaders returns the recruits at or above the minimum leader position,
// preferring those that need not decide (finalize) the rule first. Only recruits
// can lead: Promote needs a recruit still in force, which proves the pooler is
// reachable and fenced and which later events, such as a restore, invalidate.
func (r revokedOutgoingRule) eligibleLeaders() ([]*clustermetadatapb.ConsensusStatus, error) {
	minimum, err := r.minimumLeaderPosition()
	if err != nil {
		return nil, err
	}
	var eligible []*clustermetadatapb.ConsensusStatus
	for _, cs := range r.recruited {
		if CompareWALProgress(cs.GetCurrentPosition(), minimum) >= 0 {
			eligible = append(eligible, cs)
		}
	}
	if len(eligible) == 0 {
		// Only a cert's frozen LSN above every recruit leaves none: otherwise the
		// minimum is itself a recruit's position.
		return nil, fmt.Errorf("no recruited nodes at or above the required LSN %s", minimum.GetLsn())
	}
	// Candidates at or above the minimum hold the same WAL, so prefer those that
	// need not decide a rule they hold undecided.
	var finalizers []*clustermetadatapb.ConsensusStatus
	for _, cs := range eligible {
		if r.checkLeaderCanFinalizeProposal(cs) == nil {
			finalizers = append(finalizers, cs)
		}
	}
	if len(finalizers) > 0 {
		return finalizers, nil
	}
	return eligible, nil
}
