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

package manager

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// CheckClaim validates a CoordinatorClaim against the pooler's current consensus
// state without persisting anything. It is the read-only preflight used by
// RPCs that must run their side-effects before committing a term bump
// (e.g., EmergencyDemote, DemoteStalePrimary). Pre-op callers should use
// ApplyClaim directly.
//
// Requires the action lock (ctx must be an action-lock context).
func (cs *ConsensusState) CheckClaim(ctx context.Context, claim *consensusdatapb.CoordinatorClaim) error {
	if err := AssertActionLockHeld(ctx); err != nil {
		return err
	}
	if err := validateClaimFieldsNonNil(claim); err != nil {
		return err
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.checkClaimLocked(claim)
}

// ApplyClaim validates a CoordinatorClaim and, if admissible, atomically persists
// the triple (term, coordinator_id, coordinator_initiated_at) to disk.
// Idempotent on exact retries.
//
// Requires the action lock.
func (cs *ConsensusState) ApplyClaim(ctx context.Context, claim *consensusdatapb.CoordinatorClaim) error {
	if err := AssertActionLockHeld(ctx); err != nil {
		return err
	}
	if err := validateClaimFieldsNonNil(claim); err != nil {
		return err
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if err := cs.checkClaimLocked(claim); err != nil {
		return err
	}

	// If the claim exactly matches stored state, short-circuit (no disk write).
	if cs.term != nil &&
		cs.term.TermNumber == claim.Term &&
		proto.Equal(cs.term.AcceptedTermFromCoordinatorId, claim.CoordinatorId) &&
		proto.Equal(cs.term.CoordinatorInitiatedAt, claim.CoordinatorInitiatedAt) {
		return nil
	}

	// Preserve primary_term across term increase.
	primaryTerm := int64(0)
	if cs.term != nil {
		primaryTerm = cs.term.GetPrimaryTerm()
	}
	newTerm := &multipoolermanagerdatapb.ConsensusTerm{
		TermNumber:                    claim.Term,
		AcceptedTermFromCoordinatorId: proto.Clone(claim.CoordinatorId).(*clustermetadatapb.ID),
		CoordinatorInitiatedAt:        claim.CoordinatorInitiatedAt,
		LeaderId:                      nil,
		PrimaryTerm:                   primaryTerm,
	}
	return cs.saveAndUpdateLocked(newTerm)
}

// ApplyClaimExactTerm is the variant used by RPCs that require an exact
// term match (today only Promote, which must run at the recruited term and
// must not implicitly bump). Rejects both lower and higher terms; accepts
// only when claim.term == stored.term with matching coord + initiated_at.
func (cs *ConsensusState) ApplyClaimExactTerm(ctx context.Context, claim *consensusdatapb.CoordinatorClaim) error {
	if err := AssertActionLockHeld(ctx); err != nil {
		return err
	}
	if err := validateClaimFieldsNonNil(claim); err != nil {
		return err
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.term == nil {
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			"consensus term not initialized; exact term match required")
	}
	if claim.Term != cs.term.TermNumber {
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"exact term match required: stored=%d, claim=%d", cs.term.TermNumber, claim.Term)
	}
	return cs.checkClaimLocked(claim)
}

func validateClaimFieldsNonNil(claim *consensusdatapb.CoordinatorClaim) error {
	if claim == nil {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "term claim is required")
	}
	if claim.GetTerm() <= 0 {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "term claim must have positive term, got %d", claim.GetTerm())
	}
	if claim.GetCoordinatorId() == nil {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "term claim must have coordinator_id")
	}
	if claim.GetCoordinatorInitiatedAt() == nil {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "term claim must have coordinator_initiated_at")
	}
	return nil
}

// checkClaimLocked runs the same-vs-different-term safety checks. Caller
// must hold cs.mu. Returns nil if the claim is admissible.
func (cs *ConsensusState) checkClaimLocked(claim *consensusdatapb.CoordinatorClaim) error {
	current := int64(0)
	if cs.term != nil {
		current = cs.term.GetTermNumber()
	}
	if claim.Term < current {
		return fmt.Errorf("cannot admit claim: stored term %d is newer than claim term %d", current, claim.Term)
	}
	if claim.Term > current || cs.term == nil {
		return nil // new term, no conflict possible
	}
	// Same term: enforce coord + initiated_at.
	if cs.term.AcceptedTermFromCoordinatorId != nil {
		if !proto.Equal(cs.term.AcceptedTermFromCoordinatorId, claim.CoordinatorId) {
			return fmt.Errorf("already accepted term from %s in term %d",
				cs.term.AcceptedTermFromCoordinatorId.GetName(), cs.term.TermNumber)
		}
		if !proto.Equal(cs.term.CoordinatorInitiatedAt, claim.CoordinatorInitiatedAt) {
			return fmt.Errorf(
				"term %d already recruited by coordinator %s with a different coordinator_initiated_at (coordinator likely restarted, bump term)",
				cs.term.TermNumber, cs.term.AcceptedTermFromCoordinatorId.GetName())
		}
	}
	return nil
}
