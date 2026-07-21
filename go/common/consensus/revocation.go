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
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/tools/pgutil"
)

// NewTermRevocation constructs a TermRevocation for a coordinator-led safe
// transition. Use this for failover-style rule changes where the
// coordinator's view of the outgoing rule comes from discovery over the
// cohort. The revocation term is derived from the highest term observed
// across the provided ConsensusStatus values — the maximum of each node's
// accepted revocation term and its recorded rule's coordinator term —
// incremented by one. outgoing_rule is the highest RuleNumber recorded
// across the cohort.
//
// statuses must be non-empty and at least one status must carry a recorded
// rule. An empty list or a cohort with no recorded rules indicates the
// caller has nothing meaningful to transition from — that's a fresh-cluster
// or bootstrap scenario where the agent (e.g. multiorch's
// AppointInitialLeader) constructs the TermRevocation directly with an
// explicit outgoing_rule, rather than going through this helper.
func NewTermRevocation(
	statuses []*clustermetadatapb.ConsensusStatus,
	coordinatorID *clustermetadatapb.ID,
	initiatedAt *timestamppb.Timestamp,
) (*clustermetadatapb.TermRevocation, error) {
	if len(statuses) == 0 {
		return nil, errors.New("NewTermRevocation: statuses must be non-empty")
	}
	if initiatedAt == nil {
		return nil, errors.New("NewTermRevocation: initiatedAt must be non-nil")
	}
	// Discovery: the cohort's most advanced position anchors this
	// revocation's outgoing_rule. Propagation (deriving it from a
	// quorum-verified but undecided proposal) is not yet supported — the
	// most advanced position must already be decided.
	// TODO: Once propagation is implemented, revocations will be relative
	// to an outgoing decision and we can also calculate the max revocation
	// in this loop.
	var maxPosition *clustermetadatapb.RulePosition
	for _, cs := range statuses {
		// Capture the first position we see (even an explicit zero-valued
		// one, e.g. a freshly bootstrapped node); bump only on strictly
		// greater thereafter.
		position := cs.GetCurrentPosition().GetPosition()
		if maxPosition == nil || CompareRulePosition(position, maxPosition) > 0 {
			maxPosition = position
		}
	}
	if maxPosition == nil {
		return nil, errors.New("NewTermRevocation: no cohort member reports a recorded rule; agent should construct revocation directly with explicit outgoing_rule")
	}
	if !IsRuleDecided(maxPosition) {
		return nil, fmt.Errorf("cohort's most advanced position is an undecided proposal at rule %s; propagation is not yet supported",
			FormatRuleNumber(maxPosition.GetProposal().GetRuleNumber()))
	}
	outgoingRule := maxPosition.GetDecision().GetRuleNumber()

	// replaceDecision is the highest MARKED decision across the cohort — the
	// baseline the collective failover backoff counts attempts against. It is
	// computed independently of outgoingRule on purpose: on this base they are
	// equal (the guard above requires the most-advanced position to be decided),
	// but once propagation lands and outgoingRule may be an undecided
	// (quorum-verified) proposal, the attempt count must stay keyed on a settled
	// decision — otherwise a stuck proposal, which never advances the decision,
	// would reset the backoff. Scoping to the decision keeps churn escalating.
	var replaceDecision *clustermetadatapb.RuleNumber

	// The new revocation term must exceed every term any cohort member has
	// already accepted or decided. The same pass tracks the highest marked
	// decision and the most recent prior revocation (highest revoked_below_term),
	// which the backoff carry/reset below is relative to.
	// TODO: once propagation is implemented, we only need to consider statuses where the
	// outgoing decision matches the match decision we've found.
	maxTerm := outgoingRule.GetCoordinatorTerm()
	var latestRevocation *clustermetadatapb.TermRevocation
	for _, cs := range statuses {
		if d := cs.GetCurrentPosition().GetPosition().GetDecision().GetRuleNumber(); replaceDecision == nil || CompareRuleNumbers(d, replaceDecision) > 0 {
			replaceDecision = d
		}
		rev := cs.GetTermRevocation()
		if t := rev.GetRevokedBelowTerm(); t > maxTerm {
			maxTerm = t
		}
		if rev.GetRevokedBelowTerm() > 0 && (latestRevocation == nil || rev.GetRevokedBelowTerm() > latestRevocation.GetRevokedBelowTerm()) {
			latestRevocation = rev
		}
	}

	// Backoff attempt count, keyed on replace_decision. Carry forward (+1) while
	// replace_decision is unchanged from the most recent prior revocation, and
	// reset to 1 only when it advances — i.e. only when the cohort has committed a
	// newer decision, which is real, durable progress. That is the point: a run of
	// successive *undecided* attempts to move past the same decision keeps
	// escalating the backoff (see go/common/ha) instead of firing a burst of stuck
	// failovers in fast sequence, because a proposal that never gets decided never
	// advances replace_decision. Aggressive-first for a genuinely fresh failover
	// comes from the coordinator_initiated_at time anchor, not from resetting this
	// count.
	attempt := int64(1)
	if latestRevocation != nil &&
		CompareRuleNumbers(replaceDecision, latestRevocation.GetRecruitIntent().GetReplaceDecision()) == 0 {
		attempt = latestRevocation.GetRecruitIntent().GetAttempt() + 1
	}

	return &clustermetadatapb.TermRevocation{
		RevokedBelowTerm:       maxTerm + 1,
		AcceptedCoordinatorId:  coordinatorID,
		CoordinatorInitiatedAt: initiatedAt,
		OutgoingRule:           outgoingRule,
		RecruitIntent: &clustermetadatapb.RecruitIntent{
			ReplaceDecision: replaceDecision,
			Attempt:         attempt,
		},
	}, nil
}

// IsRuleRevoked reports whether the pooler's recorded revocation forbids
// applying a position (e.g. one delivered by a follower-side
// rule-propagation RPC such as SetPrimary). The position represents durable
// WAL state the cohort has reached; the revocation is this pooler's promise
// to refuse work below a given coordinator term. The predicate is a pure
// function of the two consensus messages; callers handle storage I/O,
// locking, and any logging.
//
// Comparison is two-tiered, mirroring CompareRulePosition — decision
// dominates outright, and only a tie falls through to the second tier:
//
//   - If revocation.outgoing_rule strictly exceeds position.decision, the
//     position is revoked outright: the revocation is anchored on a more
//     advanced confirmed decision than this position has confirmed, and an
//     unconfirmed proposal on the position's side cannot override that.
//   - If position.decision strictly exceeds revocation.outgoing_rule, the
//     position is never revoked (the runaway-recruit override: durable WAL
//     already moved past the rule the revocation was authored to transition
//     away from, so the promise is moot).
//   - If they tie, the position is revoked iff revocation.revoked_below_term
//     exceeds position.proposal's coordinator_term — an outstanding
//     proposal is real WAL content, so it counts against the revocation just
//     as a decision would.
//
// revocation may be nil (treated as no revocation). A revocation with no
// outgoing_rule (nil or the {0,0} unset sentinel — see ruleNumberIsUnset) is
// invalid regardless of revoked_below_term: a revocation is only ever
// authoritative relative to the specific rule it's transitioning from, so
// one that doesn't name it revokes nothing.
func IsRuleRevoked(position *clustermetadatapb.RulePosition, revocation *clustermetadatapb.TermRevocation) bool {
	revokedBelow := revocation.GetRevokedBelowTerm()
	if revokedBelow == 0 {
		return false
	}
	outgoing := revocation.GetOutgoingRule()
	if ruleNumberIsUnset(outgoing) {
		return false
	}
	decisionNum := position.GetDecision().GetRuleNumber()
	if decisionNum.GetCoordinatorTerm() >= revokedBelow {
		return false
	}
	switch cmp := CompareRuleNumbers(decisionNum, outgoing); {
	case cmp > 0:
		return false
	case cmp == 0:
		return revokedBelow > position.GetProposal().GetRuleNumber().GetCoordinatorTerm()
	}
	return true
}

// ValidateRevocation reports whether the given revocation is safe for a node
// with the provided status to honor. It returns nil if the revocation should be
// accepted, or a descriptive error explaining why it was refused.
//
// Three conditions are checked:
//
//  1. WAL position safety: the node's recorded rule coordinator term must be
//     strictly less than revoked_below_term. If the node has already applied
//     WAL at or beyond the revocation term, the revocation has no authority
//     over those writes.
//
//  2. Coordinator idempotency: if the node already accepted a revocation at
//     the same term, the request must come from the same coordinator. A
//     different coordinator at the same term is refused; the same coordinator
//     is re-accepted (idempotent success).
//
//  3. Recruitment idempotency: when the coordinator and term match, the
//     coordinator_initiated_at timestamp must also match. A differing
//     timestamp means a distinct recruitment round at the same term number,
//     which is treated as a conflict. The timestamp is protection against
//     a stateless coordinator restarting and forgetting what it was trying
//     to do -- any previous promises made to it prior to the restart should
//     be disregarded.
//
// The revocation must have a non-empty accepted_coordinator_id and a non-nil
// coordinator_initiated_at; both are required fields.
//
// A nil term_revocation in status means the node has not previously accepted
// any revocation, so conditions 2 and 3 pass for any incoming revocation.
func ValidateRevocation(status *clustermetadatapb.ConsensusStatus, revocation *clustermetadatapb.TermRevocation) error {
	if revocation == nil {
		return errors.New("cannot accept revocation: revocation is nil")
	}
	if revocation.GetAcceptedCoordinatorId().GetName() == "" {
		return errors.New("cannot accept revocation: accepted_coordinator_id is required")
	}
	if revocation.GetCoordinatorInitiatedAt() == nil {
		return errors.New("cannot accept revocation: coordinator_initiated_at is required")
	}
	// outgoing_rule must be a real, established position — {0,0} (or nil) is
	// reserved codebase-wide as the "no rule recorded" sentinel (see
	// ruleNumberIsUnset) and never a legitimate transition point. A
	// revocation without one isn't authoritative over anything and revokes
	// nothing.
	outgoingRule := revocation.GetOutgoingRule()
	if ruleNumberIsUnset(outgoingRule) {
		return errors.New("cannot accept revocation: outgoing_rule is required")
	}
	revokedBelowTerm := revocation.GetRevokedBelowTerm()
	// Invariant: outgoing_rule represents the rule the coordinator is
	// transitioning from. Its coordinator_term must be strictly less than
	// revoked_below_term — the new term is by construction max(observed) + 1,
	// so outgoing_rule.coordinator_term <= max(observed) < revoked_below_term.
	// A violation indicates a malformed revocation (or future code paths
	// constructing revocations by hand without using NewTermRevocation).
	if outTerm := outgoingRule.GetCoordinatorTerm(); outTerm >= revokedBelowTerm {
		return fmt.Errorf(
			"cannot accept revocation: outgoing_rule coordinator_term %d >= revoked_below_term %d",
			outTerm, revokedBelowTerm,
		)
	}

	// Condition 1: WAL position safety. This is exactly the question
	// IsRuleRevoked answers — does this revocation dominate the node's own
	// recorded position — so delegate to it rather than duplicating (and
	// risking drifting from) its three-tier decision/proposal comparison:
	// decision dominates outright in both directions, and only when
	// decisions tie does the proposal break it. In particular, a position
	// whose decision is behind outgoing_rule is revoked regardless of how
	// advanced its own (unconfirmed) proposal is — an unconfirmed proposal
	// never overrides a decision-level comparison.
	pos := status.GetCurrentPosition()
	if pos == nil {
		return errors.New("cannot accept revocation: unknown WAL position")
	}
	if _, err := pgutil.ParseLSN(pos.Lsn); err != nil {
		return mterrors.Wrap(err, "cannot accept revocation")
	}
	if !IsRuleRevoked(pos.GetPosition(), revocation) {
		return fmt.Errorf(
			"cannot accept revocation: recorded position %s is not revoked by outgoing_rule %s / revoked_below_term %d",
			FormatRulePosition(pos.GetPosition()), FormatRuleNumber(outgoingRule), revokedBelowTerm,
		)
	}
	// TODO: reject revocations whose outgoing_rule is known to be obsolete.
	// This may require us to first have more clarity about what's a proposal vs
	// what's a decision.

	// Conditions 2 and 3: stored-revocation consistency.
	stored := status.GetTermRevocation()
	if stored != nil {
		storedTerm := stored.GetRevokedBelowTerm()
		if storedTerm > revokedBelowTerm {
			return fmt.Errorf(
				"cannot accept revocation: already accepted term %d > requested %d",
				storedTerm, revokedBelowTerm,
			)
		}
		if storedTerm == revokedBelowTerm {
			storedCoord := topoclient.ClusterIDString(stored.GetAcceptedCoordinatorId())
			reqCoord := topoclient.ClusterIDString(revocation.GetAcceptedCoordinatorId())
			if storedCoord != reqCoord {
				return fmt.Errorf(
					"cannot accept revocation: already accepted term %d from coordinator %s, requested by %s",
					storedTerm, storedCoord, reqCoord,
				)
			}
			// Same coordinator, same term: verify the recruitment round matches.
			if !proto.Equal(stored.GetCoordinatorInitiatedAt(), revocation.GetCoordinatorInitiatedAt()) {
				return fmt.Errorf(
					"cannot accept revocation: coordinator %s reused term %d with a different coordinator_initiated_at",
					storedCoord, storedTerm,
				)
			}
			// All fields match: idempotent acceptance.
		}
	}

	return nil
}
