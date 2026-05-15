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
	"sort"

	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	"github.com/multigres/multigres/go/tools/pgutil"
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

	// OutgoingRule is the highest recorded ShardRule across all recruited nodes,
	// determined from current_position.rule (WAL-backed, authoritative).
	// May be nil for BuildExternallyCertifiedProposal when nodes have no
	// recorded rule (e.g. fresh bootstrap before any rule has been written).
	OutgoingRule *clustermetadatapb.ShardRule

	// EligibleLeaders are the recruited nodes with the best WAL position.
	// For nodes with a recorded rule, this is those matching OutgoingRule's rule
	// number with the highest LSN. For bootstrap (no rule), this is all nodes
	// tied at the highest LSN. The buildProposal callback must choose its
	// leader from this set.
	EligibleLeaders []*clustermetadatapb.ConsensusStatus
}

// discoverer reduces a recruited set to the eligible-leader candidates.
// Each proposal-building flow supplies its own discoverer; the strategy
// encapsulates which nodes are safe to elect for that flow:
//
//   - discoverMostAdvancedTimeline (default for BuildSafeProposal /
//     CheckProposalPossible): nodes tied at the highest (rule, LSN) among
//     recruits. Safe because buildProposalCore's outgoing-cohort quorum check
//     (run before discovery) guarantees the non-recruited set is too small to
//     have committed anything beyond the most-advanced recruit.
//   - newExternallyCertifiedDiscoverer(cert) (for BuildExternallyCertifiedProposal /
//     CheckExternallyCertifiedProposalPossible): nodes whose LSN ≥
//     cert.frozen_lsn, then tie-broken by LSN. Safe because the cert
//     externally certifies the freezing point of the outgoing cohort.
//
// The discoverer's safety contract lives with its implementation, not its
// callers. buildProposalCore is agnostic to which strategy is in use.
type discoverer func(
	recruited []*clustermetadatapb.ConsensusStatus,
	outgoingRule *clustermetadatapb.ShardRule,
) []*clustermetadatapb.ConsensusStatus

// quorumMode controls which quorum requirements are enforced before calling
// buildProposal. Both modes require an incoming-cohort quorum after the
// proposal is built (so the new cohort can make durable writes); they differ
// in whether they additionally require a transition quorum from the outgoing
// cohort.
type quorumMode int

const (
	// requireTransitionQuorum requires that enough members of the
	// current/outgoing cohort (identified from the highest recorded rule
	// across the recruited nodes) have accepted the term revocation. This is
	// the standard safety check for normal failover: the existing cohort must
	// consent to the leadership transition. validateProposal additionally
	// verifies the proposed (incoming) cohort has sufficient recruited members.
	requireTransitionQuorum quorumMode = iota

	// onlyRequireIncomingQuorum skips the outgoing-cohort transition check and
	// relies solely on validateProposal to verify that enough members of the
	// PROPOSED (incoming) cohort have been recruited. Used for bootstrap and
	// externally certified recovery where the outgoing cohort cannot be
	// consulted (so transition consent comes from a cert or is unnecessary
	// because there is no prior cohort).
	onlyRequireIncomingQuorum
)

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
// TODO: This assumes OutgoingRule is already durably recorded. If a cohort change
// was in progress when the leader failed, some nodes may hold OutgoingRule in WAL
// while others do not, and the previous cohort's policy may apply instead. We
// don't yet have enough information from the Recruit responses alone to detect
// this safely, so for now we proceed optimistically.
func BuildSafeProposal(
	revocation *clustermetadatapb.TermRevocation,
	statuses []*clustermetadatapb.ConsensusStatus,
	buildProposal func(RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error),
) (*consensusdatapb.CoordinatorProposal, error) {
	recruited := filterByRevocation(revocation, statuses)
	if len(recruited) == 0 {
		return nil, errors.New("no nodes accepted the requested term revocation")
	}
	return buildProposalCore(revocation, recruited, requireTransitionQuorum, discoverMostAdvancedTimeline, buildProposal)
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
	_, err := buildProposalCore(revocation, candidates, requireTransitionQuorum, discoverMostAdvancedTimeline, buildProposal)
	return err
}

// CheckExternallyCertifiedProposalPossible checks whether an externally certified
// proposal is possible given the current observed statuses, without requiring
// nodes to have already accepted the revocation. It is the externally-certified
// counterpart of CheckProposalPossible: it uses onlyRequireIncomingQuorum so that
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
	discover, err := newExternallyCertifiedDiscoverer(cert, candidates)
	if err != nil {
		return err
	}
	_, err = buildProposalCore(revocation, candidates, onlyRequireIncomingQuorum, discover, buildProposal)
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
//   - No recruited node may have a rule beyond cert.outgoing_rule_number; if
//     one does, the outgoing cohort was not actually frozen at the certified point.
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
	discover, err := newExternallyCertifiedDiscoverer(cert, recruited)
	if err != nil {
		return nil, err
	}
	return buildProposalCore(revocation, recruited, onlyRequireIncomingQuorum, discover, buildProposal)
}

// newExternallyCertifiedDiscoverer validates cert against the given nodes and returns a
// discoverer for use in buildProposalCore. The returned discoverer enforces
// cert.frozen_lsn as a floor on leader eligibility, then delegates the
// most-advanced-LSN tie-break to discoverMostAdvancedTimeline.
//
// Both outgoing_rule_number and frozen_lsn are required on the cert: without
// them, the cert provides no real guarantee about what the outgoing cohort
// was frozen at. Bootstrap callers must set them explicitly (e.g. term 0
// and "0/0"). The factory also rejects the cert if any of the given nodes
// has advanced past cert.outgoing_rule_number — the cert is stale or wrong
// in that case, and the factory surfaces it as a specific error rather than
// returning a discoverer that would yield "no eligible leaders".
//
// Safety contract: the cert externally certifies that no durable writes
// occurred beyond frozen_lsn under cert.outgoing_rule_number. Any node whose
// LSN ≥ frozen_lsn therefore holds every committed transaction up to that
// frozen point — which is why buildProposalCore is safe to skip the
// outgoing-cohort quorum check for this discoverer (onlyRequireIncomingQuorum).
func newExternallyCertifiedDiscoverer(
	cert *clustermetadatapb.ExternallyCertifiedRevocation,
	nodes []*clustermetadatapb.ConsensusStatus,
) (discoverer, error) {
	certRule := cert.GetOutgoingRuleNumber()
	if certRule == nil {
		return nil, errors.New("cert is missing outgoing_rule_number")
	}
	if cert.GetFrozenLsn() == "" {
		return nil, errors.New("cert is missing frozen_lsn")
	}
	for _, cs := range nodes {
		rule := cs.GetCurrentPosition().GetRule()
		if rule == nil {
			// Recruited nodes should always carry a rule.
			return nil, fmt.Errorf("node %s has no recorded rule; consensus state may be uninitialized",
				topoclient.ClusterIDString(cs.GetId()))
		}
		if CompareRuleNumbers(rule.GetRuleNumber(), certRule) > 0 {
			return nil, fmt.Errorf("node %s is at rule term %d but certified outgoing rule is term %d",
				topoclient.ClusterIDString(cs.GetId()), rule.GetRuleNumber().GetCoordinatorTerm(), certRule.GetCoordinatorTerm())
		}
	}
	minLSN, err := pgutil.ParseLSN(cert.GetFrozenLsn())
	if err != nil {
		return nil, fmt.Errorf("invalid frozen_lsn in cert: %w", err)
	}
	return func(recruited []*clustermetadatapb.ConsensusStatus, outgoingRule *clustermetadatapb.ShardRule) []*clustermetadatapb.ConsensusStatus {
		qualified := make([]*clustermetadatapb.ConsensusStatus, 0, len(recruited))
		for _, cs := range recruited {
			lsn, err := pgutil.ParseLSN(cs.GetCurrentPosition().GetLsn())
			if err == nil && lsn >= minLSN {
				qualified = append(qualified, cs)
			}
		}
		return discoverMostAdvancedTimeline(qualified, outgoingRule)
	}, nil
}

// buildProposalCore is the shared implementation for BuildSafeProposal,
// CheckProposalPossible, and BuildExternallyCertifiedProposal. Callers are responsible
// for pre-filtering and deduplicating statuses before calling this.
//
// With requireTransitionQuorum: validates that the current/outgoing cohort has
// sufficient recruited members before building the proposal.
// With onlyRequireIncomingQuorum: skips that check and relies on validateProposal to
// verify the proposed (incoming) cohort has sufficient recruited members.
func buildProposalCore(
	revocation *clustermetadatapb.TermRevocation,
	recruitedStatuses []*clustermetadatapb.ConsensusStatus,
	mode quorumMode,
	discover discoverer,
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

	// Find the best recorded rule (highest RuleNumber) across all statuses,
	// from their WAL-backed current_position.rule.
	var outgoingRule *clustermetadatapb.ShardRule
	for _, cs := range recruitedStatuses {
		rule := cs.GetCurrentPosition().GetRule()
		if rule != nil && (outgoingRule == nil || CompareRuleNumbers(rule.GetRuleNumber(), outgoingRule.GetRuleNumber()) > 0) {
			outgoingRule = rule
		}
	}

	switch mode {
	case requireTransitionQuorum:
		// Validate revocation of the outgoing cohort: no parallel quorum can still
		// form among the non-recruited nodes. outgoingRule must be known to identify
		// the cohort.
		if outgoingRule == nil {
			return nil, errors.New("no recorded rule found among recruited nodes; cannot determine cohort for quorum check")
		}
		outgoingPolicy, err := NewPolicyFromProto(outgoingRule.GetDurabilityPolicy())
		if err != nil {
			return nil, fmt.Errorf("failed to parse durability policy from rule: %w", err)
		}
		cohort := outgoingRule.GetCohortMembers()
		if err := outgoingPolicy.CheckSufficientRecruitment(cohort, statusesToIDs(filterCohortStatuses(cohort, recruitedStatuses))); err != nil {
			return nil, fmt.Errorf("insufficient outgoing cohort recruitment: %w", err)
		}
	case onlyRequireIncomingQuorum:
		// Outgoing-cohort quorum is not required. The incoming cohort is checked
		// below after the proposal is built.
	}

	eligibleLeaders := discover(recruitedStatuses, outgoingRule)
	if len(eligibleLeaders) == 0 {
		// Unreachable: filterByValidPosition ensures at least one status survives
		// with a parseable LSN, and outgoingRule (if set) is always derived from one of
		// those statuses, so eligibleLeaders is always non-empty here.
		return nil, errors.New("no eligible leaders found among recruited nodes")
	}

	result := RecruitmentResult{
		TermRevocation:  revocation,
		OutgoingRule:    outgoingRule,
		EligibleLeaders: eligibleLeaders,
	}

	proposal, err := buildProposal(result)
	if err != nil {
		return nil, fmt.Errorf("buildProposal: %w", err)
	}
	if proposal == nil {
		return nil, errors.New("buildProposal returned nil proposal")
	}

	recruitedIncomingCohortMembers := statusesToIDs(filterCohortStatuses(proposal.GetProposedRule().GetCohortMembers(), recruitedStatuses))
	if err := validateProposal(proposal, result, recruitedIncomingCohortMembers, mode); err != nil {
		return nil, fmt.Errorf("proposal validation: %w", err)
	}

	return proposal, nil
}

// discoverMostAdvancedTimeline is the default discoverer for safe proposals.
// It returns the recruited nodes tied at the highest LSN — at outgoingRule's
// rule number if one is known, otherwise across all recruits (fresh bootstrap).
//
// Safety contract: buildProposalCore's outgoing-cohort quorum check (run with
// requireTransitionQuorum) guarantees recruits intersect every committing
// N-subset of the outgoing cohort, so the most-advanced recruit holds every
// committed transaction. With onlyRequireIncomingQuorum (no outgoing quorum
// guarantee), this discoverer is unsafe — use newExternallyCertifiedDiscoverer instead.
//
// Callers are expected to pre-filter via filterByValidPosition; nodes with
// unparsable LSNs are skipped here as a defensive fallback.
func discoverMostAdvancedTimeline(
	recruitedStatuses []*clustermetadatapb.ConsensusStatus,
	outgoingRule *clustermetadatapb.ShardRule,
) []*clustermetadatapb.ConsensusStatus {
	var bestLSN pgutil.LSN
	var eligibleLeaders []*clustermetadatapb.ConsensusStatus
	for _, cs := range recruitedStatuses {
		if outgoingRule != nil {
			ruleNum := cs.GetCurrentPosition().GetRule().GetRuleNumber()
			if CompareRuleNumbers(ruleNum, outgoingRule.GetRuleNumber()) != 0 {
				continue
			}
		}
		lsn, err := pgutil.ParseLSN(cs.GetCurrentPosition().GetLsn())
		if err != nil {
			// The caller's filterByValidPosition guarantees all surviving statuses
			// have parseable LSNs, so this branch is unreachable in practice.
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
	return eligibleLeaders
}

// validateProposal checks structural validity and cohort quorum for the proposal.
// The proposed leader must be among the eligible leaders, the proposed rule and
// durability policy must be valid, and the cohort must satisfy achievability and
// (in onlyRequireIncomingQuorum) sufficient-recruitment constraints.
func validateProposal(
	proposal *consensusdatapb.CoordinatorProposal,
	result RecruitmentResult,
	recruitedInProposedCohort []*clustermetadatapb.ID,
	mode quorumMode,
) error {
	if !proto.Equal(proposal.GetTermRevocation(), result.TermRevocation) {
		return errors.New("proposal term revocation does not match the recruitment revocation")
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
		return fmt.Errorf("proposed leader %s is not among eligible leaders", leaderKey)
	}

	r := proposal.GetProposedRule()
	if r == nil {
		return errors.New("no proposed rule")
	}

	// TODO: relax this to support re-proposing/propagating stuck rule changes.
	// In that case a coordinator must recruit at a higher term and re-propagate
	// a potentially lower-numbered pre-existing rule.
	if proposedTerm := r.GetRuleNumber().GetCoordinatorTerm(); proposedTerm < result.TermRevocation.GetRevokedBelowTerm() {
		return fmt.Errorf("proposed rule term %d is below the recruitment revocation term %d",
			proposedTerm, result.TermRevocation.GetRevokedBelowTerm())
	}
	if proposedTerm := r.GetRuleNumber().GetCoordinatorTerm(); proposedTerm > result.TermRevocation.GetRevokedBelowTerm() {
		return fmt.Errorf("proposed rule term %d is above the recruitment revocation term %d",
			proposedTerm, result.TermRevocation.GetRevokedBelowTerm())
	}

	p, err := NewPolicyFromProto(r.GetDurabilityPolicy())
	if err != nil {
		return fmt.Errorf("invalid durability policy in proposal: %w", err)
	}

	if err := p.CheckAchievable(recruitedInProposedCohort); err != nil {
		return fmt.Errorf("recruited proposed cohort cannot achieve durability: %w", err)
	}
	if mode == onlyRequireIncomingQuorum {
		// Coordinator-retry safety: multiple coordinators may attempt externally
		// certified proposals (e.g. repeated bootstrap attempts with different proposed
		// leaders). Sufficient recruitment (majority overlap) ensures any two
		// concurrent recruitments of the same cohort and durability policy must
		// overlap and therefore cannot both independently succeed or cause split brain.
		if err := p.CheckSufficientRecruitment(r.GetCohortMembers(), recruitedInProposedCohort); err != nil {
			return fmt.Errorf("insufficient proposed cohort recruitment: %w", err)
		}
	}

	return nil
}

// filterByValidPosition returns only the statuses whose current_position
// carries a parseable LSN. A node that cannot report a valid WAL position
// cannot contribute to quorum or leader discovery: we have no way to verify
// its timeline is consistent with the rest of the cohort.
func filterByValidPosition(statuses []*clustermetadatapb.ConsensusStatus) []*clustermetadatapb.ConsensusStatus {
	result := make([]*clustermetadatapb.ConsensusStatus, 0, len(statuses))
	for _, cs := range statuses {
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
		if prev, exists := best[key]; !exists || comparePosition(cs.GetCurrentPosition(), prev.GetCurrentPosition()) > 0 {
			best[key] = cs
		}
	}
	result := make([]*clustermetadatapb.ConsensusStatus, 0, len(best))
	for _, cs := range best {
		result = append(result, cs)
	}
	sort.Slice(result, func(i, j int) bool {
		return topoclient.ClusterIDString(result[i].GetId()) < topoclient.ClusterIDString(result[j].GetId())
	})
	return result
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
