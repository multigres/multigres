// Copyright 2025 Supabase, Inc.
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
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// ConsensusState manages the in-memory and on-disk consensus state for this node.
// It provides thread-safe access to consensus state and ensures that memory is only
// updated after successful disk writes (pessimistic approach).
type ConsensusState struct {
	poolerDir string
	serviceID *clustermetadatapb.ID

	mu         sync.Mutex
	revocation *clustermetadatapb.TermRevocation // cached revocation from disk
}

// NewConsensusState creates a new ConsensusState manager.
// It does not load state from disk - call Load() to initialize.
func NewConsensusState(poolerDir string, serviceID *clustermetadatapb.ID) *ConsensusState {
	return &ConsensusState{
		poolerDir:  poolerDir,
		serviceID:  serviceID,
		revocation: nil,
	}
}

// Load loads consensus state from disk into memory.
// If the file doesn't exist, initializes with default values (term 0, no accepted coordinator).
// This method is idempotent - subsequent calls will reload from disk.
func (cs *ConsensusState) Load() (int64, error) {
	revocation, err := cs.getRevocation()
	if err != nil {
		return 0, fmt.Errorf("failed to load consensus term: %w", err)
	}

	cs.mu.Lock()
	cs.revocation = revocation
	cs.mu.Unlock()

	return revocation.RevokedBelowTerm, nil
}

// GetCurrentTermNumber returns the current term.
// Returns 0 if state has not been loaded.
func (cs *ConsensusState) GetCurrentTermNumber(ctx context.Context) (int64, error) {
	if err := AssertActionLockHeld(ctx); err != nil {
		return 0, err
	}
	return cs.GetInconsistentCurrentTermNumber()
}

// GetInconsistentCurrentTermNumber returns the current term for monitoring.
// It doesn't require the action lock to be held, so the value returned may
// be outdated by the time it's used. Use GetCurrentTermNumber() as part of
// any action workflow to protect against race conditions.
// Returns 0 if state has not been loaded.
func (cs *ConsensusState) GetInconsistentCurrentTermNumber() (int64, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.revocation == nil {
		return 0, nil
	}
	return cs.revocation.GetRevokedBelowTerm(), nil
}

// GetInconsistentRevocation returns a copy of the current term revocation for monitoring.
// It doesn't require the action lock to be held, so the value returned may
// be outdated by the time it's used. Use GetRevocation() as part of any action
// workflow to protect against race conditions.
// Returns nil if state has not been loaded.
func (cs *ConsensusState) GetInconsistentRevocation() (*clustermetadatapb.TermRevocation, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.revocation == nil {
		return nil, nil
	}

	// Return a copy to prevent external modifications
	return cloneRevocation(cs.revocation), nil
}

// GetAcceptedLeader returns the coordinator ID this pooler accepted the term from.
// Returns empty string if no coordinator was accepted.
func (cs *ConsensusState) GetAcceptedLeader(ctx context.Context) (string, error) {
	if err := AssertActionLockHeld(ctx); err != nil {
		return "", err
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.revocation == nil || cs.revocation.AcceptedCoordinatorId == nil {
		return "", nil
	}
	return cs.revocation.AcceptedCoordinatorId.GetName(), nil
}

// GetRevocation returns a copy of the current term revocation.
// Returns nil if state has not been loaded.
func (cs *ConsensusState) GetRevocation(ctx context.Context) (*clustermetadatapb.TermRevocation, error) {
	if err := AssertActionLockHeld(ctx); err != nil {
		return nil, err
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.revocation == nil {
		return nil, nil
	}

	// Return a copy to prevent external modifications
	return cloneRevocation(cs.revocation), nil
}

// AcceptCandidateAndSave atomically records acceptance of the term from a coordinator.
// This is called when a node accepts the term during BeginTerm.
// Returns error if already accepted from a different coordinator in this term.
// Idempotent: succeeds if already accepted from the same coordinator.
func (cs *ConsensusState) AcceptCandidateAndSave(ctx context.Context, candidateID *clustermetadatapb.ID) error {
	if err := AssertActionLockHeld(ctx); err != nil {
		return err
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.revocation == nil {
		return errors.New("consensus term not initialized")
	}

	if candidateID == nil {
		return errors.New("candidate ID cannot be nil")
	}

	// If already accepted from this coordinator, idempotent success
	if cs.revocation.AcceptedCoordinatorId != nil && proto.Equal(cs.revocation.AcceptedCoordinatorId, candidateID) {
		return nil
	}

	// Check if already accepted from someone else in this term
	if cs.revocation.AcceptedCoordinatorId != nil {
		return fmt.Errorf("already accepted term from %s in term %d",
			cs.revocation.AcceptedCoordinatorId.GetName(), cs.revocation.RevokedBelowTerm)
	}

	// Prepare acceptance
	newRevocation := cloneRevocation(cs.revocation)
	// Update acceptance - use proto.Clone to ensure deep copy
	newRevocation.AcceptedCoordinatorId = proto.Clone(candidateID).(*clustermetadatapb.ID)
	// Update last acceptance time
	newRevocation.CoordinatorInitiatedAt = timestamppb.New(time.Now())

	// Save and update under lock
	return cs.saveAndUpdateLocked(newRevocation)
}

// UpdateTermAndAcceptCandidate atomically updates the term and accepts a candidate in one file write.
// This is used by BeginTerm to avoid two separate file writes.
// If newTerm > currentTerm, updates term and resets acceptance, then sets the candidate.
// If newTerm == currentTerm, just accepts the candidate (same as AcceptCandidateAndSave).
// Returns error if newTerm < currentTerm.
func (cs *ConsensusState) UpdateTermAndAcceptCandidate(ctx context.Context, newTerm int64, candidateID *clustermetadatapb.ID) error {
	if err := AssertActionLockHeld(ctx); err != nil {
		return err
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if candidateID == nil {
		return errors.New("candidate ID cannot be nil")
	}

	currentTerm := int64(0)
	if cs.revocation != nil {
		currentTerm = cs.revocation.GetRevokedBelowTerm()
	}

	if newTerm < currentTerm {
		return fmt.Errorf("cannot update to older term: current=%d, new=%d", currentTerm, newTerm)
	}

	var newRevocation *clustermetadatapb.TermRevocation

	if newTerm > currentTerm {
		// Higher term: create new revocation with the candidate already set
		newRevocation = &clustermetadatapb.TermRevocation{
			RevokedBelowTerm:       newTerm,
			AcceptedCoordinatorId:  proto.Clone(candidateID).(*clustermetadatapb.ID),
			CoordinatorInitiatedAt: timestamppb.New(time.Now()),
		}
	} else {
		// Same term: just update acceptance (idempotent check first)
		if cs.revocation == nil {
			return errors.New("consensus term not initialized")
		}

		// If already accepted from this coordinator, idempotent success
		if cs.revocation.AcceptedCoordinatorId != nil && proto.Equal(cs.revocation.AcceptedCoordinatorId, candidateID) {
			return nil
		}

		// Check if already accepted from someone else in this term
		if cs.revocation.AcceptedCoordinatorId != nil {
			return fmt.Errorf("already accepted term from %s in term %d",
				cs.revocation.AcceptedCoordinatorId.GetName(), cs.revocation.RevokedBelowTerm)
		}

		// Prepare acceptance
		newRevocation = cloneRevocation(cs.revocation)
		newRevocation.AcceptedCoordinatorId = proto.Clone(candidateID).(*clustermetadatapb.ID)
		newRevocation.CoordinatorInitiatedAt = timestamppb.New(time.Now())
	}

	// Single file write
	return cs.saveAndUpdateLocked(newRevocation)
}

// UpdateTermAndSave atomically updates the term number, resetting accepted coordinator.
// This is called when discovering a newer term from another node.
// Returns error if newTerm < currentTerm.
// Idempotent: succeeds without changes if newTerm == currentTerm.
func (cs *ConsensusState) UpdateTermAndSave(ctx context.Context, newTerm int64) error {
	if err := AssertActionLockHeld(ctx); err != nil {
		return err
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()

	currentTerm := int64(0)
	if cs.revocation != nil {
		currentTerm = cs.revocation.GetRevokedBelowTerm()
	}

	if newTerm < currentTerm {
		return fmt.Errorf("cannot update to older term: current=%d, new=%d", currentTerm, newTerm)
	}

	// If same term, nothing to do (idempotent success)
	if newTerm == currentTerm {
		return nil
	}

	// Only if newTerm > currentTerm: create new revocation with reset acceptance
	newRevocation := &clustermetadatapb.TermRevocation{
		RevokedBelowTerm:       newTerm,
		AcceptedCoordinatorId:  nil,
		CoordinatorInitiatedAt: nil,
	}

	// Save and update under lock
	return cs.saveAndUpdateLocked(newRevocation)
}

// saveAndUpdateLocked saves the revocation to disk and updates memory.
// MUST be called with cs.mu held.
// This is the key method that ensures memory never diverges from disk.
// If the save fails, memory remains unchanged and the error is returned.
func (cs *ConsensusState) saveAndUpdateLocked(newRevocation *clustermetadatapb.TermRevocation) error {
	// Save to disk (lock still held)
	if err := cs.setRevocation(newRevocation); err != nil {
		// Save failed - don't update memory, propagate error
		return fmt.Errorf("failed to save consensus term: %w", err)
	}

	// Save succeeded - NOW update memory
	cs.revocation = cloneRevocation(newRevocation)
	return nil
}

// DeleteTermFile removes the consensus term file from disk and resets the
// in-memory state to uninitialized (term 0, no accepted coordinator).
// Called after a pgBackRest restore so the node re-joins consensus from
// scratch; the cluster's current term will be propagated by multiorch on
// first contact via BeginTerm.
// If the file does not exist this is a no-op. Returns an error only if
// the file exists but cannot be removed.
func (cs *ConsensusState) DeleteTermFile() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if err := os.Remove(cs.consensusTermPath()); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete consensus term file after restore: %w", err)
	}

	cs.revocation = &clustermetadatapb.TermRevocation{}
	return nil
}

// cloneRevocation creates a deep copy of a TermRevocation.
func cloneRevocation(revocation *clustermetadatapb.TermRevocation) *clustermetadatapb.TermRevocation {
	if revocation == nil {
		return nil
	}
	return proto.Clone(revocation).(*clustermetadatapb.TermRevocation)
}
