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

	// The new revocation term must exceed every term any cohort member has
	// already accepted or decided.
	// TODO: once propagation is implemented, we only need to consider statuses where the
	// outgoing decision matches the match decision we've found.
	maxTerm := outgoingRule.GetCoordinatorTerm()
	for _, cs := range statuses {
		if t := cs.GetTermRevocation().GetRevokedBelowTerm(); t > maxTerm {
			maxTerm = t
		}
	}

	return &clustermetadatapb.TermRevocation{
		RevokedBelowTerm:       maxTerm + 1,
		AcceptedCoordinatorId:  coordinatorID,
		CoordinatorInitiatedAt: initiatedAt,
		OutgoingRule:           outgoingRule,
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
// revocation may be nil (treated as no revocation).
//
// A revocation with revoked_below_term > 0 but no outgoing_rule should not
// occur in practice: every real construction path populates both together
// (NewTermRevocation always derives outgoing_rule from cohort statuses
// before returning; cert/bootstrap paths set it explicitly). The nil/{0,0}
// outgoing_rule branch below exists only to handle that malformed input
// defensively, falling straight through to the term-only check rather than
// crashing or silently misranking against the {0,0} sentinel.
func IsRuleRevoked(position *clustermetadatapb.RulePosition, revocation *clustermetadatapb.TermRevocation) bool {
	revokedBelow := revocation.GetRevokedBelowTerm()
	if revokedBelow == 0 {
		return false
	}
	decisionNum := position.GetDecision().GetRuleNumber()
	if decisionNum.GetCoordinatorTerm() >= revokedBelow {
		return false
	}
	// A nil or {0,0} OutgoingRule means the revocation carries no position to
	// compare against (e.g. a bare term-based revocation) — fall straight
	// through to the term check below. {0,0} is reserved codebase-wide as the
	// "no rule recorded" sentinel (see ruleNumberIsUnset), so it must be
	// treated the same as nil here, not compared as if it outranked nothing.
	if outgoing := revocation.GetOutgoingRule(); !ruleNumberIsUnset(outgoing) {
		switch cmp := CompareRuleNumbers(decisionNum, outgoing); {
		case cmp > 0:
			return false
		case cmp == 0:
			return revokedBelow > position.GetProposal().GetRuleNumber().GetCoordinatorTerm()
		}
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
	if revocation.GetOutgoingRule() == nil {
		return errors.New("cannot accept revocation: outgoing_rule is required")
	}
	revokedBelowTerm := revocation.GetRevokedBelowTerm()
	// Invariant: outgoing_rule represents the rule the coordinator is
	// transitioning from. Its coordinator_term must be strictly less than
	// revoked_below_term — the new term is by construction max(observed) + 1,
	// so outgoing_rule.coordinator_term <= max(observed) < revoked_below_term.
	// A violation indicates a malformed revocation (or future code paths
	// constructing revocations by hand without using NewTermRevocation).
	if outTerm := revocation.GetOutgoingRule().GetCoordinatorTerm(); outTerm >= revokedBelowTerm {
		return fmt.Errorf(
			"cannot accept revocation: outgoing_rule coordinator_term %d >= revoked_below_term %d",
			outTerm, revokedBelowTerm,
		)
	}

	// Condition 1: WAL position safety.
	pos := status.GetCurrentPosition()
	if pos == nil {
		return errors.New("cannot accept revocation: unknown WAL position")
	}
	if _, err := pgutil.ParseLSN(pos.Lsn); err != nil {
		return mterrors.Wrap(err, "cannot accept revocation")
	}
	// Propagation is not yet supported — a node with an outstanding
	// undecided proposal cannot yet safely evaluate a revocation.
	// TODO: Implement propagation
	if !IsRuleDecided(pos.GetPosition()) {
		return fmt.Errorf("cannot accept revocation: node has an undecided proposal at rule %s; propagation is not yet supported",
			FormatRuleNumber(pos.GetPosition().GetProposal().GetRuleNumber()))
	}
	ruleCoordTerm := pos.GetPosition().GetDecision().GetRuleNumber().GetCoordinatorTerm()
	if ruleCoordTerm >= revokedBelowTerm {
		return fmt.Errorf(
			"cannot accept revocation: recorded rule is at coordinator term %d >= revoked_below_term %d",
			ruleCoordTerm, revokedBelowTerm,
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
