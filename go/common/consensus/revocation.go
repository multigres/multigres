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
)

// ValidateRevocation reports whether the given revocation is safe for a node
// with the provided status to honor. It returns nil if the revocation should be
// accepted, or a descriptive error explaining why it was refused.
//
// Three conditions are checked:
//
//  1. WAL position safety: the node's committed rule coordinator term must be
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
//     which is treated as a conflict.
//
// A nil status or nil current_position means there is no WAL position to
// check, so condition 1 passes. A nil term_revocation in status means the
// node has not previously accepted any revocation, so conditions 2 and 3
// pass for any incoming revocation.
func ValidateRevocation(status *clustermetadatapb.ConsensusStatus, revocation *clustermetadatapb.TermRevocation) error {
	if revocation == nil {
		return errors.New("cannot accept revocation: revocation is nil")
	}
	revokedBelowTerm := revocation.GetRevokedBelowTerm()

	// Condition 1: WAL position safety.
	if pos := status.GetCurrentPosition(); pos != nil {
		ruleCoordTerm := pos.GetRule().GetRuleNumber().GetCoordinatorTerm()
		if ruleCoordTerm >= revokedBelowTerm {
			return fmt.Errorf(
				"cannot accept revocation: committed rule is at coordinator term %d >= revoked_below_term %d",
				ruleCoordTerm, revokedBelowTerm,
			)
		}
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
					"cannot accept revocation: coordinator %s re-used term %d with a different coordinator_initiated_at",
					storedCoord, storedTerm,
				)
			}
			// All fields match: idempotent acceptance.
		}
	}

	return nil
}
