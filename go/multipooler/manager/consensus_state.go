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
	"fmt"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// ConsensusState manages the in-memory and on-disk consensus state for this node.
// It provides thread-safe access to consensus state and ensures that memory is only
// updated after successful disk writes (pessimistic approach).
type ConsensusState struct {
	poolerDir string
	serviceID *clustermetadatapb.ID

	mu   sync.Mutex
	term *multipoolermanagerdatapb.ConsensusTerm // cached term from disk
}

// NewConsensusState creates a new ConsensusState manager.
// It does not load state from disk - call Load() to initialize.
func NewConsensusState(poolerDir string, serviceID *clustermetadatapb.ID) *ConsensusState {
	return &ConsensusState{
		poolerDir: poolerDir,
		serviceID: serviceID,
		term:      nil,
	}
}

// Load loads consensus state from disk into memory.
// If the file doesn't exist, initializes with default values (term 0, no accepted coordinator).
// This method is idempotent - subsequent calls will reload from disk.
func (cs *ConsensusState) Load() (int64, error) {
	term, err := getConsensusTerm(cs.poolerDir)
	if err != nil {
		return 0, fmt.Errorf("failed to load consensus term: %w", err)
	}

	cs.mu.Lock()
	cs.term = term
	cs.mu.Unlock()

	return term.TermNumber, nil
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

	if cs.term == nil {
		return 0, nil
	}
	return cs.term.GetTermNumber(), nil
}

// GetAcceptedLeader returns the coordinator ID this pooler accepted the term from.
// Returns empty string if no coordinator was accepted.
func (cs *ConsensusState) GetAcceptedLeader(ctx context.Context) (string, error) {
	if err := AssertActionLockHeld(ctx); err != nil {
		return "", err
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.term == nil || cs.term.AcceptedTermFromCoordinatorId == nil {
		return "", nil
	}
	return cs.term.AcceptedTermFromCoordinatorId.GetName(), nil
}

// GetTerm returns a copy of the current consensus term.
// Returns nil if state has not been loaded.
func (cs *ConsensusState) GetTerm(ctx context.Context) (*multipoolermanagerdatapb.ConsensusTerm, error) {
	if err := AssertActionLockHeld(ctx); err != nil {
		return nil, err
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.term == nil {
		return nil, nil
	}

	// Return a copy to prevent external modifications
	return cloneTerm(cs.term), nil
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

	if cs.term == nil {
		return fmt.Errorf("consensus term not initialized")
	}

	if candidateID == nil {
		return fmt.Errorf("candidate ID cannot be nil")
	}

	// If already accepted from this coordinator, idempotent success
	if cs.term.AcceptedTermFromCoordinatorId != nil && proto.Equal(cs.term.AcceptedTermFromCoordinatorId, candidateID) {
		return nil
	}

	// Check if already accepted from someone else in this term
	if cs.term.AcceptedTermFromCoordinatorId != nil {
		return fmt.Errorf("already accepted term from %s in term %d",
			cs.term.AcceptedTermFromCoordinatorId.GetName(), cs.term.TermNumber)
	}

	// Prepare acceptance
	newTerm := cloneTerm(cs.term)

	// Update acceptance - use proto.Clone to ensure deep copy
	newTerm.AcceptedTermFromCoordinatorId = proto.Clone(candidateID).(*clustermetadatapb.ID)

	// Update last acceptance time
	now := time.Now()
	newTerm.LastAcceptanceTime = timestamppb.New(now)

	// Save and update under lock
	return cs.saveAndUpdateLocked(newTerm)
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
		return fmt.Errorf("candidate ID cannot be nil")
	}

	currentTerm := int64(0)
	if cs.term != nil {
		currentTerm = cs.term.GetTermNumber()
	}

	if newTerm < currentTerm {
		return fmt.Errorf("cannot update to older term: current=%d, new=%d", currentTerm, newTerm)
	}

	var newTermProto *multipoolermanagerdatapb.ConsensusTerm

	if newTerm > currentTerm {
		// Higher term: create new term with the candidate already set
		newTermProto = &multipoolermanagerdatapb.ConsensusTerm{
			TermNumber:                    newTerm,
			AcceptedTermFromCoordinatorId: proto.Clone(candidateID).(*clustermetadatapb.ID),
			LastAcceptanceTime:            timestamppb.New(time.Now()),
			LeaderId:                      nil,
		}
	} else {
		// Same term: just update acceptance (idempotent check first)
		if cs.term == nil {
			return fmt.Errorf("consensus term not initialized")
		}

		// If already accepted from this coordinator, idempotent success
		if cs.term.AcceptedTermFromCoordinatorId != nil && proto.Equal(cs.term.AcceptedTermFromCoordinatorId, candidateID) {
			return nil
		}

		// Check if already accepted from someone else in this term
		if cs.term.AcceptedTermFromCoordinatorId != nil {
			return fmt.Errorf("already accepted term from %s in term %d",
				cs.term.AcceptedTermFromCoordinatorId.GetName(), cs.term.TermNumber)
		}

		// Prepare acceptance
		newTermProto = cloneTerm(cs.term)
		newTermProto.AcceptedTermFromCoordinatorId = proto.Clone(candidateID).(*clustermetadatapb.ID)
		newTermProto.LastAcceptanceTime = timestamppb.New(time.Now())
	}

	// Single file write
	return cs.saveAndUpdateLocked(newTermProto)
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
	if cs.term != nil {
		currentTerm = cs.term.GetTermNumber()
	}

	if newTerm < currentTerm {
		return fmt.Errorf("cannot update to older term: current=%d, new=%d", currentTerm, newTerm)
	}

	// If same term, nothing to do (idempotent success)
	if newTerm == currentTerm {
		return nil
	}

	// Only if newTerm > currentTerm: create new term with reset acceptance
	term := &multipoolermanagerdatapb.ConsensusTerm{
		TermNumber:                    newTerm,
		AcceptedTermFromCoordinatorId: nil,
		LastAcceptanceTime:            nil,
		LeaderId:                      nil,
	}

	// Save and update under lock
	return cs.saveAndUpdateLocked(term)
}

// saveAndUpdateLocked saves the term to disk and updates memory.
// MUST be called with cs.mu held.
// This is the key method that ensures memory never diverges from disk.
// If the save fails, memory remains unchanged and the error is returned.
func (cs *ConsensusState) saveAndUpdateLocked(newTerm *multipoolermanagerdatapb.ConsensusTerm) error {
	// Save to disk (lock still held)
	if err := setConsensusTerm(cs.poolerDir, newTerm); err != nil {
		// Save failed - don't update memory, propagate error
		return fmt.Errorf("failed to save consensus term: %w", err)
	}

	// Save succeeded - NOW update memory
	cs.term = cloneTerm(newTerm)
	return nil
}

// cloneTerm creates a deep copy of a ConsensusTerm
func cloneTerm(term *multipoolermanagerdatapb.ConsensusTerm) *multipoolermanagerdatapb.ConsensusTerm {
	if term == nil {
		return nil
	}
	return proto.Clone(term).(*multipoolermanagerdatapb.ConsensusTerm)
}
