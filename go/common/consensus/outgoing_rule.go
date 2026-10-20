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
	position *clustermetadatapb.RulePosition
	// rule is the decision, or the undecided proposal on top of it. Trusting an
	// undecided rule needs no separate verification: UpdateRule always finalizes
	// it, exactly as it stands, before applying anything new.
	rule    *clustermetadatapb.ShardRule
	decided bool
	// recruitsRecordingRule are all recruits that record position, whether or
	// not they are members of rule's cohort.
	recruitsRecordingRule []*clustermetadatapb.ConsensusStatus
	// basis is how this rule is to be revoked; see revoke.
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
	observed := observedOutgoingRule{
		position:              position,
		rule:                  PossiblyUndecidedRule(position),
		decided:               IsRuleDecided(position),
		recruitsRecordingRule: recruitsRecordingRule,
		basis:                 basis,
	}
	cmp := CompareRuleNumbers(observed.rule.GetRuleNumber(), expected)
	if cmp == 0 {
		return observed, nil
	}
	if basis.cert != nil {
		// The externally-certified path has no coordinator "view" to
		// re-discover — the caller's cert is simply stale relative to what's
		// actually observed now.
		return observedOutgoingRule{}, fmt.Errorf(
			"recruit %s reports rule %v, expected outgoing rule %v; cert no longer matches observed state",
			topoclient.ClusterIDString(recruitsRecordingRule[0].GetId()), observed.rule.GetRuleNumber(), expected,
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
func (o observedOutgoingRule) revoke(recruited []*clustermetadatapb.ConsensusStatus) (revokedOutgoingRule, error) {
	if o.basis.cert == nil {
		if err := o.revokeByRecruitment(recruited); err != nil {
			return revokedOutgoingRule{}, err
		}
	}
	// Revoking by cert needs no recruitment of the outgoing cohort: the cert
	// promises nothing durable lies beyond its frozen LSN under the outgoing
	// rule.
	return revokedOutgoingRule{o}, nil
}

// revokeByRecruitment proves from the recruits two properties of a
// coordinator-led rule change, each against the cohort that serves it:
//
//   - freeze: no write can become durable under the outgoing rule that the
//     recruits cannot see.
//   - exclusion: no competing coordinator can win the same term. Competitors
//     all recruit the outgoing cohort, so a majority of it forces overlap.
//
// For a decided outgoing rule one cohort serves both. For an undecided one the
// outgoing rule's cohort freezes it, while a competitor that has not yet seen
// the proposal finds the decision and recruits its cohort, so that cohort must
// be checked for exclusion too.
func (o observedOutgoingRule) revokeByRecruitment(recruited []*clustermetadatapb.ConsensusStatus) error {
	if err := o.freeze(recruited); err != nil {
		return err
	}
	return o.excludeCompetingCoordinators(recruited)
}

// freeze checks the outgoing rule's own cohort: sufficiently recruited, so its
// leftovers cannot commit under it. While the rule is an undecided proposal,
// writes must satisfy the "Both" policy of the decision and the proposal, so
// any durable write has an acker among these recruited members.
func (o observedOutgoingRule) freeze(recruited []*clustermetadatapb.ConsensusStatus) error {
	policy, err := NewPolicyFromProto(o.rule.GetDurabilityPolicy())
	if err != nil {
		return fmt.Errorf("failed to parse durability policy from rule: %w", err)
	}
	cohort := o.rule.GetCohortMembers()
	if err := CheckSufficientRecruitment(policy, cohort, statusesToIDs(filterCohortStatuses(cohort, recruited))); err != nil {
		return fmt.Errorf("insufficient outgoing cohort recruitment: %w", err)
	}
	return nil
}

// excludeCompetingCoordinators checks the decision's cohort when the outgoing
// rule is an undecided proposal on top of it. When the outgoing rule is
// decided, freeze's cohort already serves exclusion.
func (o observedOutgoingRule) excludeCompetingCoordinators(recruited []*clustermetadatapb.ConsensusStatus) error {
	if o.decided {
		return nil
	}
	decision := o.position.GetDecision()
	policy, err := NewPolicyFromProto(decision.GetDurabilityPolicy())
	if err != nil {
		return fmt.Errorf("failed to parse durability policy from decision: %w", err)
	}
	cohort := decision.GetCohortMembers()
	if err := CheckSufficientRecruitment(policy, cohort, statusesToIDs(filterCohortStatuses(cohort, recruited))); err != nil {
		return fmt.Errorf("insufficient outgoing decision cohort recruitment: %w", err)
	}
	return nil
}

// eligibleLeaders returns the recruits that can lead: those recording the
// outgoing rule that are tied at the highest LSN at or above the basis's frozen
// LSN (zero when revoking by recruitment).
func (r revokedOutgoingRule) eligibleLeaders() ([]*clustermetadatapb.ConsensusStatus, error) {
	var minLSN pgutil.LSN
	if r.basis.cert != nil {
		minLSN = r.basis.cert.frozenLSN
	}
	return narrowByLSN(r.recruitsRecordingRule, minLSN)
}
