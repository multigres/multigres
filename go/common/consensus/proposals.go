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
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	"github.com/multigres/multigres/go/tools/pgutil"
	"github.com/multigres/multigres/go/tools/sortedmaps"
)

// RecruitmentResult holds the interpreted outcome of a successful recruitment
// round. It is passed to the buildProposal callback in BuildSafeProposal,
// CheckProposalPossible, CheckExternallyCertifiedProposalPossible, and
// BuildExternallyCertifiedProposal.
type RecruitmentResult struct {
	// TermRevocation is the revocation the coordinator requested. For
	// Build*Proposal functions, all nodes in this result have accepted it. For
	// Check*ProposalPossible, nodes have not yet accepted it — they have only
	// been filtered by whether they could accept it via ValidateRevocation.
	TermRevocation *clustermetadatapb.TermRevocation

	// OutgoingRule is the ShardRule at the outgoing rule number, shared by
	// every entry in EligibleLeaders. It may be an undecided proposal rather
	// than a decision — see OutgoingRuleDecided. In practice this is never
	// nil: every node always carries at least the initial {0,1} row written
	// during initdb, so the public constructors (NewTermRevocation, cert
	// authors) never produce the sentinel RuleNumber{0,0} that would leave
	// it unset. Revoking by cert still knows this rule — it just does not
	// recruit its cohort, relying instead on the cert and the incoming-cohort
	// check.
	OutgoingRule *clustermetadatapb.ShardRule

	// OutgoingRuleDecided reports whether OutgoingRule came from a decision
	// (WAL-backed, authoritative) rather than an undecided proposal. An
	// undecided OutgoingRule is trusted without a separate quorum-proof step
	// (see buildProposalCore) — that's safe because UpdateRule always
	// finalizes it (deciding it exactly as it already stands, under its own
	// Both policy) before applying anything new, regardless of whether the
	// new proposal changes cohort or policy.
	OutgoingRuleDecided bool

	// EligibleLeaders are the recruited nodes with the best WAL position.
	// For nodes with a recorded rule, this is those matching OutgoingRule's rule
	// number with the highest LSN. For bootstrap (no rule), this is all nodes
	// tied at the highest LSN. The buildProposal callback must choose its
	// leader from this set.
	EligibleLeaders []*clustermetadatapb.ConsensusStatus

	// outgoing is the typed outgoing rule that OutgoingRule exposes to builders:
	// revoked, and finalizable by the recruits. EligibleLeaders rest on it, and
	// validateProposal reads the revocation basis from it.
	outgoing finalizableOutgoingRule
}

// newRecruitmentResult derives the eligible leaders from an outgoing rule that is
// revoked and finalizable. Production code builds a RecruitmentResult only with
// it, so its recruits are backed by that proof.
func newRecruitmentResult(
	revocation *clustermetadatapb.TermRevocation,
	outgoing finalizableOutgoingRule,
) (RecruitmentResult, error) {
	eligible, err := outgoing.eligibleLeaders()
	if err != nil {
		return RecruitmentResult{}, err
	}
	return RecruitmentResult{
		TermRevocation:      revocation,
		OutgoingRule:        PossiblyUndecidedRule(outgoing.position),
		OutgoingRuleDecided: IsRuleDecided(outgoing.position),
		EligibleLeaders:     eligible,
		outgoing:            outgoing,
	}, nil
}

// ReplicationPrimaryFromProposal builds the ReplicationPrimary a cohort
// member should be told from a CoordinatorProposal, shared by Promote
// (leader) and SetPrimary (followers) so they can't drift apart.
//
// rewindReady is explicit rather than defaulted, since a silently-omitted
// zero value here is exactly the kind of gap this exists to prevent.
func ReplicationPrimaryFromProposal(proposal *consensusdatapb.CoordinatorProposal, rewindReady bool) *clustermetadatapb.ReplicationPrimary {
	return &clustermetadatapb.ReplicationPrimary{
		Position:    proposal.GetProposedTransition(),
		Primary:     proposal.GetProposalLeader(),
		RewindReady: rewindReady,
	}
}

// revocationBasis says how the outgoing rule is revoked before buildProposal is
// called. The zero value revokes by recruiting the outgoing cohort, the standard
// safety check for normal failover: the existing cohort must consent to the
// leadership transition.
type revocationBasis struct {
	// cert, when set, revokes the outgoing rule by an external cert instead. It
	// is used for bootstrap and externally certified recovery, where the
	// outgoing cohort cannot be consulted.
	cert *certBasis
}

// certBasis is what an externally certified revocation rests on.
type certBasis struct {
	// frozenLSN is the cert's promise that nothing durable lies beyond it under
	// the outgoing rule.
	frozenLSN pgutil.LSN
}

// newCertBasis validates cert and returns the basis it provides.
func newCertBasis(cert *clustermetadatapb.ExternallyCertifiedRevocation) (revocationBasis, error) {
	frozenLSN, err := certMinLSN(cert)
	if err != nil {
		return revocationBasis{}, err
	}
	return revocationBasis{cert: &certBasis{frozenLSN: frozenLSN}}, nil
}

// BuildSafeProposal validates that the recruited nodes allow a safe
// leadership transition, calls buildProposal to obtain a proposal, then
// validates the proposal against the recruitment constraints.
//
// revocation is the TermRevocation the coordinator sent in its RecruitRequest.
// statuses are ALL ConsensusStatus values known for the shard — including from
// nodes that did not yet apply the revocation. A node counts as recruited only
// if its status carries a TermRevocation that exactly matches revocation.
// Nodes at a higher term or pledged to a different coordinator are filtered out.
//
// The OUTGOING cohort (identified from the highest recorded rule across
// recruited nodes) must have sufficient recruited members to form a quorum.
// The INCOMING cohort (proposed in the returned proposal) is additionally
// validated by validateProposal.
//
// The highest recorded rule may itself be an undecided proposal (a cohort or
// leader change interrupted mid-write) rather than a decision — see
// RecruitmentResult.OutgoingRuleDecided. buildProposalCore trusts it as the
// outgoing baseline either way. Safety is judged against the decision's cohort,
// the one every competing coordinator recruits (see
// observedOutgoingRule.revokeByRecruitment). An undecided proposal must also be
// finalizable by the recruits (see revokedOutgoingRule.checkRecruitsCanFinalizeProposal), and
// the new proposal may then change only its leader (see validateProposal).
func BuildSafeProposal(
	revocation *clustermetadatapb.TermRevocation,
	statuses []*clustermetadatapb.ConsensusStatus,
	buildProposal func(RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error),
) (*consensusdatapb.CoordinatorProposal, error) {
	recruited := filterByRevocation(revocation, statuses)
	if len(recruited) == 0 {
		return nil, errors.New("no nodes accepted the requested term revocation")
	}
	return buildProposalCore(revocation, recruited, revocationBasis{}, buildProposal)
}

// CheckProposalPossible checks whether a safe leadership proposal is possible
// given the current observed statuses, without requiring nodes to have already
// accepted the revocation. It filters statuses to those that could accept the
// proposed revocation (using ValidateRevocation), then applies the same
// outgoing-cohort quorum and eligible-leader checks as BuildSafeProposal.
//
// Returns an error if no viable proposal exists; the proposal itself is not
// returned since nodes have not yet committed to the revocation.
//
// Intended for pre-vote feasibility checks before committing to a Recruit round.
func CheckProposalPossible(
	revocation *clustermetadatapb.TermRevocation,
	statuses []*clustermetadatapb.ConsensusStatus,
	buildProposal func(RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error),
) error {
	candidates := filterByPotentialRevocation(revocation, statuses)
	if len(candidates) == 0 {
		return errors.New("no nodes could accept the proposed revocation")
	}
	_, err := buildProposalCore(revocation, candidates, revocationBasis{}, buildProposal)
	return err
}

// CheckExternallyCertifiedProposalPossible checks whether an externally certified
// proposal is possible given the current observed statuses, without requiring
// nodes to have already accepted the revocation. It is the externally-certified
// counterpart of CheckProposalPossible: it revokes by cert so that
// bootstrap scenarios with no recorded rule (no OutgoingRule) are handled correctly.
//
// Returns an error if no viable proposal exists; the proposal itself is not
// returned since nodes have not yet committed to the revocation.
//
// Intended for pre-vote feasibility checks in bootstrap or externally-certified
// recovery paths.
func CheckExternallyCertifiedProposalPossible(
	cert *clustermetadatapb.ExternallyCertifiedRevocation,
	statuses []*clustermetadatapb.ConsensusStatus,
	buildProposal func(RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error),
) error {
	revocation := cert.GetTermRevocation()
	candidates := filterByPotentialRevocation(revocation, statuses)
	if len(candidates) == 0 {
		return errors.New("no nodes could accept the proposed revocation")
	}
	basis, err := newCertBasis(cert)
	if err != nil {
		return err
	}
	_, err = buildProposalCore(revocation, candidates, basis, buildProposal)
	return err
}

// BuildExternallyCertifiedProposal constructs a proposal for scenarios where
// the outgoing cohort's revocation has been established by an external agent
// rather than through normal Recruit RPCs. The incoming cohort is still
// recruited normally: nodes must have accepted the term revocation in cert
// before this function is called.
//
// Unlike BuildSafeProposal, the outgoing-cohort quorum check is skipped —
// the caller certifies via cert that the outgoing cohort is frozen. Instead,
// validateProposal verifies that the INCOMING (proposed) cohort has sufficient
// recruited members, ensuring the new cluster can make durable writes.
//
// Two additional checks enforce the cert's guarantees:
//   - No recruited node may have a rule beyond revocation.outgoing_rule; if
//     one does, the outgoing cohort was not actually frozen at the certified
//     point. (Enforced inside buildProposalCore.)
//   - Only nodes with an LSN at or above cert.frozen_lsn are eligible to be
//     leader. Nodes below that threshold still count toward quorum — they can
//     endorse the proposal — but cannot be selected as the new leader.
//
// The buildProposal callback is responsible for specifying the full new cohort
// and durability policy, since there may be no recorded rule to derive them from.
func BuildExternallyCertifiedProposal(
	cert *clustermetadatapb.ExternallyCertifiedRevocation,
	statuses []*clustermetadatapb.ConsensusStatus,
	buildProposal func(RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error),
) (*consensusdatapb.CoordinatorProposal, error) {
	revocation := cert.GetTermRevocation()
	recruited := filterByRevocation(revocation, statuses)
	if len(recruited) == 0 {
		return nil, errors.New("no nodes accepted the requested term revocation")
	}
	basis, err := newCertBasis(cert)
	if err != nil {
		return nil, err
	}
	return buildProposalCore(revocation, recruited, basis, buildProposal)
}

// certMinLSN validates cert and returns the LSN floor (cert.frozen_lsn)
// buildProposalCore should use for leader eligibility.
//
// frozen_lsn is required on the cert: without it, the cert provides no real
// guarantee about what the outgoing cohort was frozen at. Bootstrap callers
// must set it explicitly (e.g. "0/0").
//
// Safety contract: the cert externally certifies that no durable writes
// occurred beyond frozen_lsn under term_revocation.outgoing_rule. Any node
// whose LSN ≥ frozen_lsn therefore holds every committed transaction up to
// that frozen point — which is why buildProposalCore is safe to skip the
// outgoing-cohort quorum check when revoking by cert.
func certMinLSN(
	cert *clustermetadatapb.ExternallyCertifiedRevocation,
) (pgutil.LSN, error) {
	if cert.GetTermRevocation().GetOutgoingRule() == nil {
		return 0, errors.New("cert.term_revocation.outgoing_rule is required")
	}
	if cert.GetFrozenLsn() == "" {
		return 0, errors.New("cert is missing frozen_lsn")
	}
	minLSN, err := pgutil.ParseLSN(cert.GetFrozenLsn())
	if err != nil {
		return 0, fmt.Errorf("invalid frozen_lsn in cert: %w", err)
	}
	return minLSN, nil
}

// buildProposalCore is the shared implementation for BuildSafeProposal,
// CheckProposalPossible, and BuildExternallyCertifiedProposal. Callers are responsible
// for pre-filtering and deduplicating statuses before calling this.
//
// basis says how the outgoing rule is revoked: by recruiting the outgoing
// cohort (the zero value), or by an external cert, in which case
// validateProposal verifies the proposed (incoming) cohort has sufficient
// recruited members.
func buildProposalCore(
	revocation *clustermetadatapb.TermRevocation,
	recruitedStatuses []*clustermetadatapb.ConsensusStatus,
	basis revocationBasis,
	buildProposal func(RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error),
) (*consensusdatapb.CoordinatorProposal, error) {
	if len(recruitedStatuses) == 0 {
		return nil, errors.New("empty list of statuses")
	}

	recruitedStatuses = deduplicateStatuses(recruitedStatuses)
	recruitedStatuses = filterByValidPosition(recruitedStatuses)
	if len(recruitedStatuses) == 0 {
		return nil, errors.New("all recruited nodes reported an invalid or missing WAL position")
	}

	// The revocation's outgoing_rule is the rule number the coordinator
	// committed to transitioning from when it authored the revocation. The
	// outgoing rule must match it: no recruit may be more advanced (whether by
	// decision or by an undecided proposal), and the matching rule must be the
	// one recruits actually record.
	expectedOutgoingRule := revocation.GetOutgoingRule()
	if expectedOutgoingRule == nil {
		return nil, errors.New("revocation.outgoing_rule is required (use NewTermRevocation to construct revocations)")
	}
	outgoing, err := discoverOutgoingRule(recruitedStatuses, expectedOutgoingRule, basis)
	if err != nil {
		return nil, err
	}
	revoked, err := outgoing.revoke()
	if err != nil {
		return nil, err
	}
	finalizable, err := revoked.checkRecruitsCanFinalizeProposal()
	if err != nil {
		return nil, err
	}
	result, err := newRecruitmentResult(revocation, finalizable)
	if err != nil {
		return nil, err
	}

	proposal, err := buildProposal(result)
	if err != nil {
		return nil, fmt.Errorf("buildProposal: %w", err)
	}
	if proposal == nil {
		return nil, errors.New("buildProposal returned nil proposal")
	}

	recruitedIncomingCohortMembers := statusesToIDs(filterCohortStatuses(proposal.GetProposedTransition().GetProposal().GetCohortMembers(), recruitedStatuses))
	if err := validateProposal(proposal, result, recruitedIncomingCohortMembers); err != nil {
		return nil, fmt.Errorf("proposal validation: %w", err)
	}

	return proposal, nil
}

// discoverOutgoingRulePosition returns every recruit that holds the outgoing
// rule position (the highest RulePosition among recruitedStatuses), followed
// by that position itself. Decision takes precedence; an undecided proposal
// only breaks ties among recruits sharing the same decision (see
// CompareRulePosition) — a recruit's own undecided proposal never outranks
// another recruit's higher decision.
//
// Returns an error if the recruits recording the position disagree on its
// content (decision or proposal) — different nodes agreeing on a rule
// number but disagreeing on cohort members or durability policy signals a
// data inconsistency the caller cannot safely proceed past.
func discoverOutgoingRulePosition(
	recruitedStatuses []*clustermetadatapb.ConsensusStatus,
) ([]*clustermetadatapb.ConsensusStatus, *clustermetadatapb.RulePosition, error) {
	var outgoingPosition *clustermetadatapb.RulePosition
	var recruitsRecordingRule []*clustermetadatapb.ConsensusStatus
	for _, cs := range recruitedStatuses {
		position := cs.GetCurrentPosition().GetPosition()
		switch {
		case outgoingPosition == nil || CompareRulePosition(position, outgoingPosition) > 0:
			outgoingPosition = position
			recruitsRecordingRule = []*clustermetadatapb.ConsensusStatus{cs}
		case CompareRulePosition(position, outgoingPosition) == 0:
			recruitsRecordingRule = append(recruitsRecordingRule, cs)
		}
	}
	if len(recruitsRecordingRule) == 0 {
		return recruitsRecordingRule, nil, nil
	}
	for _, cs := range recruitsRecordingRule[1:] {
		if !proto.Equal(cs.GetCurrentPosition().GetPosition(), outgoingPosition) {
			return nil, nil, fmt.Errorf(
				"recruits %s and %s agree on rule number but disagree on content; inconsistent discovery result",
				topoclient.ClusterIDString(recruitsRecordingRule[0].GetId()), topoclient.ClusterIDString(cs.GetId()),
			)
		}
	}
	return recruitsRecordingRule, outgoingPosition, nil
}

// narrowByLSN narrows mostAdvanced (recruits already confirmed to share the
// same RulePosition, decided, matching the coordinator's expected outgoing
// rule — see buildProposalCore) to those additionally tied at the highest
// LSN at or above minLSN: RulePosition alone doesn't capture ordinary WAL
// streaming progress within the same decided rule, so the buildProposal
// callback still needs an LSN-based choice among them.
//
// minLSN (see certMinLSN) narrows leader eligibility only — a recruit below
// it still counted toward the position check above, since that check must
// catch a stale/unfrozen view regardless of LSN, but it can't itself be
// selected as leader here.
func narrowByLSN(
	mostAdvanced []*clustermetadatapb.ConsensusStatus,
	minLSN pgutil.LSN,
) ([]*clustermetadatapb.ConsensusStatus, error) {
	var bestLSN pgutil.LSN
	var eligibleLeaders []*clustermetadatapb.ConsensusStatus
	for _, cs := range mostAdvanced {
		lsn, err := pgutil.ParseLSN(cs.GetCurrentPosition().GetLsn())
		if err != nil || lsn < minLSN {
			// The caller's filterByValidPosition guarantees all surviving statuses
			// have parseable LSNs, so a parse failure here is unreachable in practice.
			continue
		}
		if lsn > bestLSN {
			bestLSN = lsn
			eligibleLeaders = eligibleLeaders[:0]
		}
		if lsn >= bestLSN {
			// There may be multiple poolers that have the most advanced LSN.
			eligibleLeaders = append(eligibleLeaders, cs)
		}
	}

	if len(eligibleLeaders) == 0 {
		if minLSN > 0 {
			return nil, fmt.Errorf("no recruited nodes at or above the required LSN %s", minLSN)
		}
		// Unreachable: mostAdvanced is non-empty (checked by the caller) and
		// every entry in it came from recruitedStatuses, which
		// filterByValidPosition guarantees has a parseable LSN.
		return nil, errors.New("no eligible leaders found among recruited nodes")
	}

	return eligibleLeaders, nil
}

// validateProposal checks structural validity and cohort quorum for the proposal.
// The proposed leader must be among the eligible leaders, the proposed rule and
// durability policy must be valid, and the cohort must satisfy achievability and
// (when revoking by cert) sufficient-recruitment constraints.
func validateProposal(
	proposal *consensusdatapb.CoordinatorProposal,
	result RecruitmentResult,
	recruitedInProposedCohort []*clustermetadatapb.ID,
) error {
	if !proto.Equal(proposal.GetTermRevocation(), result.TermRevocation) {
		return errors.New("proposal term revocation does not match the recruitment revocation")
	}

	// skip_outgoing_quorum may only be set by the externally-certified path:
	// regular safe proposals always have a known outgoing cohort to drain.
	cert := result.outgoing.basis.cert
	if proposal.GetSkipOutgoingQuorum() && cert == nil {
		return errors.New("skip_outgoing_quorum is only valid for externally-certified proposals")
	}

	leaderID := proposal.GetProposalLeader().GetId()
	if leaderID == nil {
		return errors.New("proposal has no leader ID")
	}
	leaderKey := topoclient.ClusterIDString(leaderID)
	foundLeader := false
	for _, cs := range result.EligibleLeaders {
		if topoclient.ClusterIDString(cs.GetId()) == leaderKey {
			foundLeader = true
			break
		}
	}
	if !foundLeader {
		eligibleKeys := make([]string, len(result.EligibleLeaders))
		for i, cs := range result.EligibleLeaders {
			eligibleKeys[i] = topoclient.ClusterIDString(cs.GetId())
		}
		return fmt.Errorf("proposed leader %s is not among eligible leaders %v", leaderKey, eligibleKeys)
	}

	proposedRule := proposal.GetProposedTransition().GetProposal()
	if proposedRule == nil {
		return errors.New("no proposed rule")
	}

	// TermRevocation.outgoing_rule is still just a bare RuleNumber, so compare
	// by number, not proto.Equal — the proposal's decision is a full ShardRule,
	// a different message type that would never compare equal via proto.Equal
	// regardless of content.
	//
	// TODO: switch revocation to a full ShardRule and use proto.Equal() here.
	if CompareRuleNumbers(proposal.GetProposedTransition().GetDecision().GetRuleNumber(), result.TermRevocation.GetOutgoingRule()) != 0 {
		return errors.New("proposal outgoing decision doesn't match revocation outgoing rule")
	}

	// Identity and timing fields are caller-supplied attestations. We refuse
	// to install a rule that drops them: a downstream consumer (rule_history,
	// audit log, time-based ordering against external systems) would have no
	// way to tell a missing field from a deliberate zero value.
	if proposedRule.GetCoordinatorId() == nil {
		return errors.New("proposed rule has no coordinator_id")
	}
	if proposedRule.GetCreationTime() == nil {
		return errors.New("proposed rule has no creation_time")
	}

	// TODO: relax this to support re-proposing/propagating stuck rule changes.
	// In that case a coordinator must recruit at a higher term and re-propagate
	// a potentially lower-numbered pre-existing rule.
	if proposedTerm := proposedRule.GetRuleNumber().GetCoordinatorTerm(); proposedTerm < result.TermRevocation.GetRevokedBelowTerm() {
		return fmt.Errorf("proposed rule term %d is below the recruitment revocation term %d",
			proposedTerm, result.TermRevocation.GetRevokedBelowTerm())
	}
	if proposedTerm := proposedRule.GetRuleNumber().GetCoordinatorTerm(); proposedTerm > result.TermRevocation.GetRevokedBelowTerm() {
		return fmt.Errorf("proposed rule term %d is above the recruitment revocation term %d",
			proposedTerm, result.TermRevocation.GetRevokedBelowTerm())
	}

	p, err := NewPolicyFromProto(proposedRule.GetDurabilityPolicy())
	if err != nil {
		return fmt.Errorf("invalid durability policy in proposal: %w", err)
	}

	if !result.OutgoingRuleDecided && cert == nil {
		// Promote decides the stuck proposal first, so a proposal built on it
		// changes only the leader. Any cohort or policy change waits for a
		// decided rule.
		if !sameCohort(proposedRule.GetCohortMembers(), result.OutgoingRule.GetCohortMembers()) ||
			!proto.Equal(proposedRule.GetDurabilityPolicy(), result.OutgoingRule.GetDurabilityPolicy()) {
			return errors.New("proposed rule must keep the undecided outgoing rule's cohort and durability policy, changing only the leader")
		}
	}

	// Promote's second write, the new rule, needs the recruits to satisfy the
	// proposed policy. The first write, deciding an undecided outgoing proposal,
	// was checked before the builder ran (see checkRecruitsCanFinalizeProposal).
	if err := p.SatisfiedBy(recruitedInProposedCohort); err != nil {
		return fmt.Errorf("recruited proposed cohort cannot achieve durability: %w", err)
	}
	if cert != nil {
		if err := checkCertifiedRetryGuard(p, proposedRule.GetCohortMembers(), recruitedInProposedCohort); err != nil {
			return err
		}
	}

	return nil
}

// checkCertifiedRetryGuard requires the proposed cohort to be sufficiently
// recruited under the proposed policy. The cert promises the term is unique,
// but a coordinator may still retry the same term, for example repeated
// bootstrap attempts with different proposed leaders. A majority of the
// proposed cohort makes any two such attempts overlap, so they cannot both
// succeed. This relies on retries proposing the same cohort, which nothing
// enforces. The unrecruited-cannot-commit half also rejects a stale cert: if an
// earlier attempt's rule became durable on this cohort, the recruits must
// overlap its quorum, and holders of that rule refuse the new revocation.
func checkCertifiedRetryGuard(
	policy DurabilityPolicy,
	proposedCohort, recruitedInProposedCohort []*clustermetadatapb.ID,
) error {
	if err := CheckSufficientRecruitment(policy, proposedCohort, recruitedInProposedCohort); err != nil {
		return fmt.Errorf("insufficient proposed cohort recruitment: %w", err)
	}
	return nil
}

// filterByValidPosition returns only the statuses whose current_position
// carries a parseable LSN and a recorded decision. A node missing either
// cannot contribute to quorum or leader discovery — we have no way to
// verify its timeline is consistent with the rest of the cohort — so it's
// treated like an outage of that node: dropped before quorum counting and
// discovery, not merely deprioritized as a candidate.
func filterByValidPosition(statuses []*clustermetadatapb.ConsensusStatus) []*clustermetadatapb.ConsensusStatus {
	result := make([]*clustermetadatapb.ConsensusStatus, 0, len(statuses))
	for _, cs := range statuses {
		if ruleNumberIsUnset(cs.GetCurrentPosition().GetPosition().GetDecision().GetRuleNumber()) {
			continue
		}
		if _, err := pgutil.ParseLSN(cs.GetCurrentPosition().GetLsn()); err == nil {
			result = append(result, cs)
		}
	}
	return result
}

// filterByRevocation returns only the statuses whose TermRevocation exactly
// matches revocation. Nodes at a higher term or pledged to a different
// coordinator are excluded because they did not accept this recruitment.
func filterByRevocation(revocation *clustermetadatapb.TermRevocation, statuses []*clustermetadatapb.ConsensusStatus) []*clustermetadatapb.ConsensusStatus {
	result := make([]*clustermetadatapb.ConsensusStatus, 0, len(statuses))
	for _, cs := range statuses {
		if proto.Equal(cs.GetTermRevocation(), revocation) {
			result = append(result, cs)
		}
	}
	return result
}

// filterByPotentialRevocation returns only the statuses that could accept the
// given revocation — i.e. ValidateRevocation returns nil. Used by
// CheckProposalPossible and CheckExternallyCertifiedProposalPossible for
// pre-vote feasibility checks where nodes have not yet been recruited.
func filterByPotentialRevocation(revocation *clustermetadatapb.TermRevocation, statuses []*clustermetadatapb.ConsensusStatus) []*clustermetadatapb.ConsensusStatus {
	result := make([]*clustermetadatapb.ConsensusStatus, 0, len(statuses))
	for _, cs := range statuses {
		if ValidateRevocation(cs, revocation) == nil {
			result = append(result, cs)
		}
	}
	return result
}

// deduplicateStatuses returns a deduplicated, ID-sorted copy of statuses.
// When the same node ID appears more than once, the entry with the highest
// current_position (rule number, then LSN) is kept.
func deduplicateStatuses(statuses []*clustermetadatapb.ConsensusStatus) []*clustermetadatapb.ConsensusStatus {
	best := make(map[string]*clustermetadatapb.ConsensusStatus, len(statuses))
	for _, cs := range statuses {
		if cs.GetId() == nil {
			continue
		}
		key := topoclient.ClusterIDString(cs.GetId())
		if prev, exists := best[key]; !exists || ComparePoolerPosition(cs.GetCurrentPosition(), prev.GetCurrentPosition()) > 0 {
			best[key] = cs
		}
	}
	return sortedmaps.Values(best)
}

// filterCohortStatuses returns the subset of statuses whose node ID is a
// member of cohort.
func filterCohortStatuses(cohort []*clustermetadatapb.ID, statuses []*clustermetadatapb.ConsensusStatus) []*clustermetadatapb.ConsensusStatus {
	cohortKeys := poolerKeysOf(cohort)
	result := make([]*clustermetadatapb.ConsensusStatus, 0, len(cohort))
	for _, cs := range statuses {
		id := cs.GetId()
		if id == nil {
			// Defensive: deduplicateStatuses drops nil-ID entries before any
			// caller passes statuses here, so this branch is unreachable in
			// practice. Guards against future callers that don't dedupe first.
			continue
		}
		if _, inCohort := cohortKeys[topoclient.ClusterIDString(id)]; inCohort {
			result = append(result, cs)
		}
	}
	return result
}

// statusesToIDs extracts the ID from each status.
func statusesToIDs(statuses []*clustermetadatapb.ConsensusStatus) []*clustermetadatapb.ID {
	ids := make([]*clustermetadatapb.ID, 0, len(statuses))
	for _, cs := range statuses {
		ids = append(ids, cs.GetId())
	}
	return ids
}
