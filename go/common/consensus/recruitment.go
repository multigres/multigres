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

	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	"github.com/multigres/multigres/go/tools/pgutil"
)

// RecruitmentResult holds the interpreted outcome of a successful recruitment
// round. It is passed to the buildProposal callback in CheckSufficientRecruitment.
type RecruitmentResult struct {
	// TermRevocation is the revocation the recruited nodes accepted, taken
	// from the first status (all recruited nodes accept the same one).
	TermRevocation *clustermetadatapb.TermRevocation

	// BestRule is the highest committed ShardRule across all recruited nodes,
	// determined from current_position.rule (WAL-backed, authoritative).
	BestRule *clustermetadatapb.ShardRule

	// EligibleLeaders are the recruited nodes whose committed rule number
	// matches BestRule. The buildProposal callback must choose its leader
	// from this set.
	EligibleLeaders []*clustermetadatapb.ConsensusStatus
}

// CheckSufficientRecruitment validates that the recruited nodes allow a safe
// leadership transition, calls buildProposal to obtain a proposal, then
// validates the proposal against the recruitment constraints.
//
// statuses are the ConsensusStatus values returned from Recruit() RPCs. All
// statuses are assumed to have accepted the same TermRevocation.
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
//  2. All proposed_rule cohort members must have been recruited.
//  3. The proposed durability policy must be achievable with the proposed cohort.
func CheckSufficientRecruitment(
	statuses []*clustermetadatapb.ConsensusStatus,
	buildProposal func(RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error),
) (*consensusdatapb.CoordinatorProposal, error) {
	statuses = deduplicateStatuses(statuses)
	if len(statuses) == 0 {
		return nil, errors.New("no recruitment statuses provided")
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
		lsn, ok := parseLSN(cs.GetCurrentPosition().GetLsn())
		if !ok {
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
		TermRevocation:  statuses[0].GetTermRevocation(),
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

	// All proposed cohort members must have been recruited.
	recruitedKeys := make(map[string]struct{}, len(statuses))
	for _, cs := range statuses {
		if id := cs.GetId(); id != nil {
			recruitedKeys[topoclient.ClusterIDString(id)] = struct{}{}
		}
	}
	for _, member := range proposal.GetProposedRule().GetCohortMembers() {
		key := topoclient.ClusterIDString(member)
		if _, ok := recruitedKeys[key]; !ok {
			return fmt.Errorf("proposed cohort member %s was not recruited", key)
		}
	}

	// The proposed durability policy must be achievable with the proposed cohort.
	if r := proposal.GetProposedRule(); r != nil && r.GetDurabilityPolicy() != nil {
		p, err := NewPolicyFromProto(r.GetDurabilityPolicy())
		if err != nil {
			return fmt.Errorf("invalid durability policy in proposal: %w", err)
		}
		if err := p.CheckAchievable(r.GetCohortMembers()); err != nil {
			return fmt.Errorf("proposed durability policy not achievable with proposed cohort: %w", err)
		}
	}

	return nil
}

// sameCohort reports whether a and b represent the same set of pooler IDs.
func sameCohort(a, b []*clustermetadatapb.ID) bool {
	if len(a) != len(b) {
		return false
	}
	aKeys := poolerKeysOf(a)
	for _, id := range b {
		if _, ok := aKeys[topoclient.ClusterIDString(id)]; !ok {
			return false
		}
	}
	return true
}

// parseLSN parses a PostgreSQL LSN string, returning (0, false) for empty or
// unparseable values.
func parseLSN(s string) (pgutil.LSN, bool) {
	if s == "" {
		return 0, false
	}
	lsn, err := pgutil.ParseLSN(s)
	if err != nil {
		return 0, false
	}
	return lsn, true
}

// cohortIntersect returns the IDs of recruited nodes (from statuses) that are
// members of cohort. statuses is assumed to be already deduplicated by ID.
func cohortIntersect(cohort []*clustermetadatapb.ID, statuses []*clustermetadatapb.ConsensusStatus) []*clustermetadatapb.ID {
	cohortKeys := poolerKeysOf(cohort)
	result := make([]*clustermetadatapb.ID, 0, len(cohort))
	for _, cs := range statuses {
		id := cs.GetId()
		if id == nil {
			continue
		}
		if _, inCohort := cohortKeys[topoclient.ClusterIDString(id)]; inCohort {
			result = append(result, id)
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

// comparePosition returns negative, zero, or positive based on whether a is
// behind, equal to, or ahead of b. Rule number takes precedence; LSN breaks
// ties within the same rule. A missing LSN is treated as less than any valid LSN.
func comparePosition(a, b *clustermetadatapb.PoolerPosition) int {
	if cmp := CompareRuleNumbers(a.GetRule().GetRuleNumber(), b.GetRule().GetRuleNumber()); cmp != 0 {
		return cmp
	}
	lsnA, okA := parseLSN(a.GetLsn())
	lsnB, okB := parseLSN(b.GetLsn())
	switch {
	case !okA && !okB:
		return 0
	case !okA:
		return -1
	case !okB:
		return 1
	case lsnA < lsnB:
		return -1
	case lsnA > lsnB:
		return 1
	default:
		return 0
	}
}
