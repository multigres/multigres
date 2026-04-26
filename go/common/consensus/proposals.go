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
// round. It is passed to the buildProposal callback in BuildSafeProposal.
type RecruitmentResult struct {
	// TermRevocation is the revocation the coordinator requested, which all
	// nodes in this result have accepted.
	TermRevocation *clustermetadatapb.TermRevocation

	// BestRule is the highest committed ShardRule across all recruited nodes,
	// determined from current_position.rule (WAL-backed, authoritative).
	BestRule *clustermetadatapb.ShardRule

	// EligibleLeaders are the recruited nodes whose committed rule number
	// matches BestRule. The buildProposal callback must choose its leader
	// from this set.
	EligibleLeaders []*clustermetadatapb.ConsensusStatus
}

// BuildSafeProposal validates that the recruited nodes allow a safe
// leadership transition, calls buildProposal to obtain a proposal, then
// validates the proposal against the recruitment constraints.
//
// revocation is the TermRevocation the coordinator sent in its RecruitRequest.
// statuses are all ConsensusStatus values returned from Recruit() RPCs,
// including from nodes that did not apply the revocation. A node counts as
// recruited only if its status carries a TermRevocation that exactly matches
// revocation (same term and same coordinator). Nodes at a higher term or
// pledged to a different coordinator are filtered out.
//
// # Quorum validation
//
// The highest committed rule (BestRule) across the recruited nodes determines
// the cohort and durability policy for the quorum check. Recruited nodes in
// that cohort must satisfy the policy.
//
// TODO: This assumes BestRule is already durably committed. If a cohort change
// was in progress when the primary failed, some nodes may hold BestRule in WAL
// while others do not, and the previous cohort's policy may apply instead. We
// don't yet have enough information from the Recruit responses alone to detect
// this safely, so for now we proceed optimistically.
//
// # Post-callback proposal validation
//
//  1. proposal_leader must be among EligibleLeaders.
//  2. The proposed durability policy must be achievable with the proposed cohort.
//  3. Enough proposed cohort members must have been recruited to satisfy the policy.
func BuildSafeProposal(
	revocation *clustermetadatapb.TermRevocation,
	statuses []*clustermetadatapb.ConsensusStatus,
	buildProposal func(RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error),
) (*consensusdatapb.CoordinatorProposal, error) {
	statuses = deduplicateStatuses(filterByRevocation(revocation, statuses))
	if len(statuses) == 0 {
		return nil, errors.New("no nodes accepted the requested term revocation")
	}
	statuses = filterByValidPosition(statuses)
	if len(statuses) == 0 {
		return nil, errors.New("all recruited nodes reported an invalid or missing WAL position")
	}

	// Step 1: Find the best committed rule (highest RuleNumber) across all
	// recruited nodes from their WAL-backed current_position.rule.
	var bestRule *clustermetadatapb.ShardRule
	for _, cs := range statuses {
		rule := cs.GetCurrentPosition().GetRule()
		if rule != nil && (bestRule == nil || CompareRuleNumbers(rule.GetRuleNumber(), bestRule.GetRuleNumber()) > 0) {
			bestRule = rule
		}
	}
	if bestRule == nil {
		return nil, errors.New("no committed rule found among recruited nodes; cannot determine cohort")
	}

	// Step 2: Validate that the recruited nodes cover sufficient quorum under
	// the best rule's cohort and policy.
	policy, err := NewPolicyFromProto(bestRule.GetDurabilityPolicy())
	if err != nil {
		return nil, fmt.Errorf("failed to parse durability policy from rule: %w", err)
	}
	cohort := bestRule.GetCohortMembers()
	recruitedInCohort := cohortIntersect(cohort, statuses)
	if err := policy.CheckSufficientRecruitment(cohort, recruitedInCohort); err != nil {
		return nil, fmt.Errorf("insufficient recruitment: %w", err)
	}

	// Step 3: Build the eligible leader set — nodes at bestRule's rule number
	// with the highest LSN. Rule number is the primary criterion; LSN breaks
	// ties within the same rule (a node at the same rule but a lower LSN has
	// less WAL and is strictly worse). Nodes with unparseable or empty LSNs
	// are excluded — we can't verify their position.
	var bestLSN pgutil.LSN
	var eligibleLeaders []*clustermetadatapb.ConsensusStatus
	for _, cs := range statuses {
		if CompareRuleNumbers(cs.GetCurrentPosition().GetRule().GetRuleNumber(), bestRule.GetRuleNumber()) != 0 {
			continue
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

	// Step 4: Ask the caller to build the proposal.
	proposal, err := buildProposal(result)
	if err != nil {
		return nil, fmt.Errorf("buildProposal: %w", err)
	}
	if proposal == nil {
		return nil, errors.New("buildProposal returned nil proposal")
	}

	// Step 5: Validate the proposal against the recruitment constraints.
	if err := validateProposal(proposal, result, statuses); err != nil {
		return nil, fmt.Errorf("proposal validation: %w", err)
	}

	return proposal, nil
}

// validateProposal checks that the returned proposal is consistent with the
// recruitment result and the set of recruited nodes.
func validateProposal(
	proposal *consensusdatapb.CoordinatorProposal,
	result RecruitmentResult,
	statuses []*clustermetadatapb.ConsensusStatus,
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

	if r := proposal.GetProposedRule(); r != nil && r.GetDurabilityPolicy() != nil {
		p, err := NewPolicyFromProto(r.GetDurabilityPolicy())
		if err != nil {
			return fmt.Errorf("invalid durability policy in proposal: %w", err)
		}
		// The full proposed cohort must be large enough to ever satisfy the policy.
		if err := p.CheckAchievable(r.GetCohortMembers()); err != nil {
			return fmt.Errorf("proposed durability policy not achievable with proposed cohort: %w", err)
		}
		// Not all cohort members need to be recruited — for example, the dead
		// primary may remain in the cohort so it can rejoin as a standby. But we
		// must have recruited enough of the proposed cohort to satisfy the policy,
		// otherwise the new leader cannot make durable writes immediately.
		recruitedInProposedCohort := cohortIntersect(r.GetCohortMembers(), statuses)
		if err := p.CheckAchievable(recruitedInProposedCohort); err != nil {
			return fmt.Errorf("insufficient recruitment from proposed cohort to achieve durability: %w", err)
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
