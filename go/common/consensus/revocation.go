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

// NewTermRevocation constructs a TermRevocation for a new coordinator-led rule
// change. The revocation term is derived from the highest term observed across
// the provided ConsensusStatus values — the maximum of each node's accepted
// revocation term and its recorded rule's coordinator term — incremented by
// one. If the cohort has no prior elections, term 1 is used.
//
// outgoing_rule is set to the highest RuleNumber recorded across the cohort
// statuses. On a fresh cluster with no recorded rule anywhere, this is a
// zero-valued RuleNumber rather than nil, so any real future rule compares
// strictly greater. See the TermRevocation proto definition and
// checkInformAgainstRevocation for how this is used as an override key during
// Inform.
//
// statuses must be non-empty: a revocation is a promise the coordinator makes
// to a cohort it has actually observed. An empty list indicates zero cohort
// visibility, which is a programming error, not a "fresh cluster" semantic.
// A fresh cluster still has cohort members in statuses; their TermRevocation
// and CurrentPosition fields are simply empty.
//
// TODO: unify "outgoing rule" across the consensus flow. Today this function
// derives outgoing_rule from cached cohort statuses, while buildProposalCore
// independently re-derives a (full) outgoing ShardRule from recruited
// statuses. A follow-up should make the coordinator pick one canonical
// outgoing rule up front, have Recruit reject poolers whose recorded rule is
// strictly newer than that expected outgoing rule, and have Propose accept
// the outgoing rule as an input rather than re-deriving. That refactor is
// out of scope here.
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
		if t := ruleNum.GetCoordinatorTerm(); t > maxTerm {
			maxTerm = t
		}
		if CompareRuleNumbers(ruleNum, maxRule) > 0 {
			maxRule = ruleNum
		}
	}
	if maxRule == nil {
		maxRule = &clustermetadatapb.RuleNumber{}
	}
	return &clustermetadatapb.TermRevocation{
		RevokedBelowTerm:       maxTerm + 1,
		AcceptedCoordinatorId:  coordinatorID,
		CoordinatorInitiatedAt: timestamppb.Now(),
		OutgoingRule:           maxRule,
	}, nil
}

// IsRuleRevoked reports whether the pooler's recorded revocation forbids
// applying a rule received via Inform. The rule represents durable WAL state
// the cohort has reached; the revocation is this pooler's promise to refuse
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
