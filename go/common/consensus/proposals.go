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
// CheckProposalPossible, CheckForcedProposalPossible, and BuildForcedProposal.
type RecruitmentResult struct {
	// TermRevocation is the revocation the coordinator requested. For
	// Build*Proposal functions, all nodes in this result have accepted it. For
	// Check*ProposalPossible, nodes have not yet accepted it — they have only
	// been filtered by whether they could accept it via ValidateRevocation.
	TermRevocation *clustermetadatapb.TermRevocation

	// BestRule is the highest committed ShardRule across all recruited nodes,
	// determined from current_position.rule (WAL-backed, authoritative).
	// May be nil for BuildForcedProposal when nodes have no committed rule
	// (e.g. fresh bootstrap before any rule has been written).
	BestRule *clustermetadatapb.ShardRule

	// EligibleLeaders are the recruited nodes with the best WAL position.
	// For nodes with a committed rule, this is those matching BestRule's rule
	// number with the highest LSN. For bootstrap (no rule), this is all nodes
	// tied at the highest LSN. The buildProposal callback must choose its
	// leader from this set.
	EligibleLeaders []*clustermetadatapb.ConsensusStatus
}

// cohortQuorumMode controls which cohort's quorum requirements are enforced
// before calling buildProposal. The mode makes explicit whether we are
// validating the OUTGOING cohort's consent (normal failover) or relying on the
// INCOMING cohort's coverage (bootstrap and forced recovery).
type cohortQuorumMode int

const (
	// outgoingCohortMode requires that enough members of the current/outgoing
	// cohort (identified from the highest committed rule across the recruited
	// nodes) have accepted the term revocation. This is the standard safety
	// check for normal failover: the existing cohort must consent to the
	// leadership transition. validateProposal additionally verifies the
	// proposed (incoming) cohort has sufficient recruited members.
	outgoingCohortMode cohortQuorumMode = iota

	// incomingCohortMode skips the outgoing-cohort quorum check and instead
	// relies on validateProposal to verify that enough members of the
	// PROPOSED (incoming) cohort have been recruited. Used for bootstrap and
	// forced recovery where the outgoing cohort cannot be consulted.
	incomingCohortMode
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
// The OUTGOING cohort (identified from the highest committed rule across
// recruited nodes) must have sufficient recruited members to form a quorum.
// The INCOMING cohort (proposed in the returned proposal) is additionally
// validated by validateProposal.
//
// TODO: This assumes BestRule is already durably committed. If a cohort change
// was in progress when the primary failed, some nodes may hold BestRule in WAL
// while others do not, and the previous cohort's policy may apply instead. We
// don't yet have enough information from the Recruit responses alone to detect
// this safely, so for now we proceed optimistically.
func BuildSafeProposal(
	revocation *clustermetadatapb.TermRevocation,
	statuses []*clustermetadatapb.ConsensusStatus,
	buildProposal func(RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error),
) (*consensusdatapb.CoordinatorProposal, error) {
	recruited := deduplicateStatuses(filterByRevocation(revocation, statuses))
	if len(recruited) == 0 {
		return nil, errors.New("no nodes accepted the requested term revocation")
	}
	return buildProposalCore(revocation, recruited, outgoingCohortMode, buildProposal)
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
	candidates := deduplicateStatuses(filterByPotentialRevocation(revocation, statuses))
	if len(candidates) == 0 {
		return errors.New("no nodes could accept the proposed revocation")
	}
	_, err := buildProposalCore(revocation, candidates, outgoingCohortMode, buildProposal)
	return err
}

// CheckForcedProposalPossible checks whether a forced leadership proposal is
// possible given the current observed statuses, without requiring nodes to have
// already accepted the revocation. It is the forced-recovery counterpart of
// CheckProposalPossible: it uses incomingCohortMode so that bootstrap scenarios
// with no committed rule (no BestRule) are handled correctly.
//
// Returns an error if no viable proposal exists; the proposal itself is not
// returned since nodes have not yet committed to the revocation.
//
// Intended for pre-vote feasibility checks in bootstrap or forced-recovery paths.
func CheckForcedProposalPossible(
	revocation *clustermetadatapb.TermRevocation,
	statuses []*clustermetadatapb.ConsensusStatus,
	buildProposal func(RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error),
) error {
	candidates := deduplicateStatuses(filterByPotentialRevocation(revocation, statuses))
	if len(candidates) == 0 {
		return errors.New("no nodes could accept the proposed revocation")
	}
	_, err := buildProposalCore(revocation, candidates, incomingCohortMode, buildProposal)
	return err
}

// BuildForcedProposal constructs a proposal for scenarios where the outgoing
// cohort's quorum cannot be obtained — specifically bootstrap and stuck-quorum
// recovery. Like BuildSafeProposal it requires nodes to have accepted the term
// revocation, but it skips the outgoing-cohort quorum check. Instead,
// validateProposal verifies that the INCOMING (proposed) cohort has sufficient
// recruited members, ensuring the new cluster can make durable writes.
//
// The buildProposal callback is responsible for specifying the full new cohort
// and durability policy, since there may be no committed rule to derive them from.
func BuildForcedProposal(
	revocation *clustermetadatapb.TermRevocation,
	statuses []*clustermetadatapb.ConsensusStatus,
	buildProposal func(RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error),
) (*consensusdatapb.CoordinatorProposal, error) {
	recruited := deduplicateStatuses(filterByRevocation(revocation, statuses))
	if len(recruited) == 0 {
		return nil, errors.New("no nodes accepted the requested term revocation")
	}
	return buildProposalCore(revocation, recruited, incomingCohortMode, buildProposal)
}

// buildProposalCore is the shared implementation for BuildSafeProposal,
// CheckProposalPossible, and BuildForcedProposal. Callers are responsible
// for pre-filtering and deduplicating statuses before calling this.
//
// With outgoingCohortMode: validates that the current/outgoing cohort has
// sufficient recruited members before building the proposal.
// With incomingCohortMode: skips that check and relies on validateProposal to
// verify the proposed (incoming) cohort has sufficient recruited members.
func buildProposalCore(
	revocation *clustermetadatapb.TermRevocation,
	statuses []*clustermetadatapb.ConsensusStatus,
	mode cohortQuorumMode,
	buildProposal func(RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error),
) (*consensusdatapb.CoordinatorProposal, error) {
	if len(statuses) == 0 {
		return nil, errors.New("empty list of statuses")
	}

	statuses = filterByValidPosition(statuses)
	if len(statuses) == 0 {
		return nil, errors.New("all recruited nodes reported an invalid or missing WAL position")
	}

	// Find the best committed rule (highest RuleNumber) across all statuses,
	// from their WAL-backed current_position.rule.
	var bestRule *clustermetadatapb.ShardRule
	for _, cs := range statuses {
		rule := cs.GetCurrentPosition().GetRule()
		if rule != nil && (bestRule == nil || CompareRuleNumbers(rule.GetRuleNumber(), bestRule.GetRuleNumber()) > 0) {
			bestRule = rule
		}
	}

	switch mode {
	case outgoingCohortMode:
		// Validate revocation of the outgoing cohort: no parallel quorum can still
		// form among the non-recruited nodes. bestRule must be known to identify
		// the cohort.
		if bestRule == nil {
			return nil, errors.New("no committed rule found among recruited nodes; cannot determine cohort for quorum check")
		}
		outgoingPolicy, err := NewPolicyFromProto(bestRule.GetDurabilityPolicy())
		if err != nil {
			return nil, fmt.Errorf("failed to parse durability policy from rule: %w", err)
		}
		cohort := bestRule.GetCohortMembers()
		if err := outgoingPolicy.CheckSufficientRecruitment(cohort, cohortIntersect(cohort, statuses)); err != nil {
			return nil, fmt.Errorf("insufficient outgoing cohort recruitment: %w", err)
		}
	case incomingCohortMode:
		// Outgoing-cohort quorum is not required. The incoming cohort is checked
		// below after the proposal is built.
	}

	// Build the eligible leader set.
	//
	// When a bestRule is known: prefer nodes at bestRule's rule number (highest
	// committed WAL), then break ties by LSN.
	// When no bestRule exists (fresh bootstrap): all nodes are candidates;
	// highest LSN wins.
	//
	// Nodes with unparsable LSNs are excluded — we cannot verify their position.
	var bestLSN pgutil.LSN
	var eligibleLeaders []*clustermetadatapb.ConsensusStatus
	for _, cs := range statuses {
		if bestRule != nil {
			ruleNum := cs.GetCurrentPosition().GetRule().GetRuleNumber()
			if CompareRuleNumbers(ruleNum, bestRule.GetRuleNumber()) != 0 {
				continue
			}
		}
		lsn, err := pgutil.ParseLSN(cs.GetCurrentPosition().GetLsn())
		if err != nil {
			continue
		}
		if lsn > bestLSN {
			bestLSN = lsn
			eligibleLeaders = eligibleLeaders[:0]
		}
		if lsn >= bestLSN {
			eligibleLeaders = append(eligibleLeaders, cs)
		}
	}
	if len(eligibleLeaders) == 0 {
		return nil, errors.New("no eligible leaders found among recruited nodes")
	}

	result := RecruitmentResult{
		TermRevocation:  revocation,
		BestRule:        bestRule,
		EligibleLeaders: eligibleLeaders,
	}

	proposal, err := buildProposal(result)
	if err != nil {
		return nil, fmt.Errorf("buildProposal: %w", err)
	}
	if proposal == nil {
		return nil, errors.New("buildProposal returned nil proposal")
	}

	// Validate the proposed (incoming) cohort.
	if r := proposal.GetProposedRule(); r != nil && r.GetDurabilityPolicy() != nil {
		incomingPolicy, err := NewPolicyFromProto(r.GetDurabilityPolicy())
		if err != nil {
			return nil, fmt.Errorf("invalid durability policy in proposed rule: %w", err)
		}
		recruitedInProposedCohort := cohortIntersect(r.GetCohortMembers(), statuses)
		if mode == incomingCohortMode {
			// Coordinator-retry safety: multiple coordinators may attempt forced
			// proposals (e.g. repeated bootstrap attempts with different proposed
			// leaders). Sufficient recruitment (majority overlap) ensures any two
			// concurrent recruitments of the same cohort and durability policy must
			// overlap therefore cannot both independently succeed or have split brain.
			if err := incomingPolicy.CheckSufficientRecruitment(r.GetCohortMembers(), recruitedInProposedCohort); err != nil {
				return nil, fmt.Errorf("insufficient proposed cohort recruitment: %w", err)
			}
		}
		// Candidacy: the recruited set must be able to form a quorum so the new
		// leader can immediately make durable writes.
		if err := incomingPolicy.CheckAchievable(recruitedInProposedCohort); err != nil {
			return nil, fmt.Errorf("recruited proposed cohort cannot achieve durability: %w", err)
		}
	}

	if err := validateProposal(proposal, result); err != nil {
		return nil, fmt.Errorf("proposal validation: %w", err)
	}

	return proposal, nil
}

// validateProposal checks that the returned proposal is structurally consistent
// with the recruitment result: the proposed leader must be among the eligible
// leaders, and the proposed cohort must be achievable under its durability policy.
// Recruited-subset quorum checks are handled in buildProposalCore.
func validateProposal(
	proposal *consensusdatapb.CoordinatorProposal,
	result RecruitmentResult,
) error {
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
	p, err := NewPolicyFromProto(r.GetDurabilityPolicy())
	if err != nil {
		return fmt.Errorf("invalid durability policy in proposal: %w", err)
	}
	// The full proposed cohort must be large enough to ever satisfy the policy.
	if err := p.CheckAchievable(r.GetCohortMembers()); err != nil {
		return fmt.Errorf("proposed durability policy not achievable with proposed cohort: %w", err)
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
// CheckProposalPossible and CheckForcedProposalPossible for pre-vote feasibility
// checks where nodes have not yet been recruited.
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
