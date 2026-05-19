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
) (*clustermetadatapb.TermRevocation, error) {
	if len(statuses) == 0 {
		return nil, errors.New("NewTermRevocation: statuses must be non-empty")
	}
	var maxTerm int64
	var maxRule *clustermetadatapb.RuleNumber
	for _, cs := range statuses {
		if t := cs.GetTermRevocation().GetRevokedBelowTerm(); t > maxTerm {
			maxTerm = t
		}
		ruleNum := cs.GetCurrentPosition().GetRule().GetRuleNumber()
		if ruleNum == nil {
			continue
		}
		if t := ruleNum.GetCoordinatorTerm(); t > maxTerm {
			maxTerm = t
		}
		// Capture the first non-nil RuleNumber we see; bump only on strictly
		// greater. The "first non-nil" path matters when the recorded rule
		// is the zero RuleNumber — CompareRuleNumbers treats zero == nil, so
		// without the explicit nil check we'd never lift maxRule above nil.
		if maxRule == nil || CompareRuleNumbers(ruleNum, maxRule) > 0 {
			maxRule = ruleNum
		}
	}
	if maxRule == nil {
		return nil, errors.New("NewTermRevocation: no cohort member reports a recorded rule; agent should construct revocation directly with explicit outgoing_rule")
	}
	return &clustermetadatapb.TermRevocation{
		RevokedBelowTerm:       maxTerm + 1,
		AcceptedCoordinatorId:  coordinatorID,
		CoordinatorInitiatedAt: timestamppb.Now(),
		OutgoingRule:           maxRule,
	}, nil
}

// IsRuleRevoked reports whether the pooler's recorded revocation forbids
// applying a rule (e.g. one delivered by a follower-side rule-propagation
// RPC such as SetTermPrimary). The rule represents durable WAL state the
// cohort has reached; the revocation is this pooler's promise to refuse
// work below a given coordinator term. The predicate is a pure function of
// the two consensus messages; callers handle storage I/O, locking, and any
// logging.
//
// A rule is revoked when:
//   - revocation.revoked_below_term is non-zero and exceeds the rule's
//     coordinator_term, AND
//   - the rule does not strictly exceed revocation.outgoing_rule.
//
// The second clause is the runaway-recruit override: if durable WAL exists
// for a rule strictly newer than outgoing_rule, the cohort has demonstrably
// moved past the rule the revocation was authored to transition away from,
// so the promise is moot. See the TermRevocation proto for the full safety
// argument.
//
// revocation may be nil (treated as no revocation). outgoing_rule may be nil
// on revocations written by older code that predates the field; nil means
// "no override available" and the override branch cannot fire.
func IsRuleRevoked(rule *clustermetadatapb.ShardRule, revocation *clustermetadatapb.TermRevocation) bool {
	revokedBelow := revocation.GetRevokedBelowTerm()
	if revokedBelow == 0 {
		return false
	}
	if rule.GetRuleNumber().GetCoordinatorTerm() >= revokedBelow {
		return false
	}
	outgoing := revocation.GetOutgoingRule()
	if outgoing != nil && CompareRuleNumbers(rule.GetRuleNumber(), outgoing) > 0 {
		return false
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
	ruleCoordTerm := pos.GetRule().GetRuleNumber().GetCoordinatorTerm()
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
