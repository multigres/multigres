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
	"fmt"
	"sync"
	"time"

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
// If the file doesn't exist, initializes with default values (term 0, no accepted leader).
// This method is idempotent - subsequent calls will reload from disk.
func (cs *ConsensusState) Load() error {
	term, err := getConsensusTerm(cs.poolerDir)
	if err != nil {
		return fmt.Errorf("failed to load consensus term: %w", err)
	}

	cs.mu.Lock()
	cs.term = term
	cs.mu.Unlock()

	return nil
}

// GetCurrentTermNumber returns the current term.
// Returns 0 if state has not been loaded.
func (cs *ConsensusState) GetCurrentTermNumber() int64 {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.term == nil {
		return 0
	}
	return cs.term.GetTermNumber()
}

// GetAcceptedLeader returns the candidate ID this pooler accepted as leader in the current term.
// Returns empty string if no leader was accepted.
func (cs *ConsensusState) GetAcceptedLeader() string {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.term == nil || cs.term.AcceptedLeader == nil {
		return ""
	}
	return cs.term.AcceptedLeader.GetName()
}

// GetTerm returns a copy of the current consensus term.
// Returns nil if state has not been loaded.
func (cs *ConsensusState) GetTerm() *multipoolermanagerdatapb.ConsensusTerm {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.term == nil {
		return nil
	}

	// Return a copy to prevent external modifications
	return cloneTerm(cs.term)
}

// PrepareAcceptance creates a new term with the acceptance recorded.
// This does NOT modify in-memory state - the caller must call SaveAndUpdate.
// Returns an error if already accepted a different candidate in this term.
func (cs *ConsensusState) PrepareAcceptance(candidateID string) (*multipoolermanagerdatapb.ConsensusTerm, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.term == nil {
		return nil, fmt.Errorf("consensus term not initialized")
	}

	// Check if already accepted someone else in this term
	if cs.term.AcceptedLeader != nil && cs.term.AcceptedLeader.GetName() != candidateID {
		return nil, fmt.Errorf("already accepted %s as leader in term %d",
			cs.term.AcceptedLeader.GetName(), cs.term.TermNumber)
	}

	// Create NEW term (don't modify existing in-memory state)
	newTerm := cloneTerm(cs.term)

	// Update acceptance
	if newTerm.AcceptedLeader == nil {
		newTerm.AcceptedLeader = &clustermetadatapb.ID{
			Component: cs.serviceID.Component,
			Cell:      cs.serviceID.Cell,
			Name:      candidateID,
		}
	} else {
		newTerm.AcceptedLeader.Name = candidateID
	}

	// Update last acceptance time
	now := time.Now()
	newTerm.LastAcceptanceTime = timestamppb.New(now)

	return newTerm, nil
}

// PrepareTermUpdate creates a new term with updated term number.
// This does NOT modify in-memory state - the caller must call SaveAndUpdate.
// If newTerm > currentTerm, resets accepted leader to nil.
// Returns an error if newTerm < currentTerm.
func (cs *ConsensusState) PrepareTermUpdate(newTerm int64) (*multipoolermanagerdatapb.ConsensusTerm, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	currentTerm := int64(0)
	if cs.term != nil {
		currentTerm = cs.term.GetTermNumber()
	}

	if newTerm < currentTerm {
		return nil, fmt.Errorf("cannot update to older term: current=%d, new=%d", currentTerm, newTerm)
	}

	// Create new term with reset acceptance
	term := &multipoolermanagerdatapb.ConsensusTerm{
		TermNumber:         newTerm,
		AcceptedLeader:     nil,
		LastAcceptanceTime: nil,
		LeaderId:           nil,
	}

	return term, nil
}

// SaveAndUpdate atomically saves the term to disk, then updates memory ONLY on success.
// This is the key method that ensures memory never diverges from disk.
// If the save fails, memory remains unchanged and the error is returned.
func (cs *ConsensusState) SaveAndUpdate(newTerm *multipoolermanagerdatapb.ConsensusTerm) error {
	// Save to disk FIRST (outside of lock to allow concurrent reads)
	if err := setConsensusTerm(cs.poolerDir, newTerm); err != nil {
		// Save failed - don't update memory, propagate error
		return fmt.Errorf("failed to save consensus term: %w", err)
	}

	// Save succeeded - NOW update memory
	cs.mu.Lock()
	cs.term = cloneTerm(newTerm)
	cs.mu.Unlock()

	return nil
}

// cloneTerm creates a deep copy of a ConsensusTerm
func cloneTerm(term *multipoolermanagerdatapb.ConsensusTerm) *multipoolermanagerdatapb.ConsensusTerm {
	if term == nil {
		return nil
	}

	clone := &multipoolermanagerdatapb.ConsensusTerm{
		TermNumber:         term.TermNumber,
		LastAcceptanceTime: term.LastAcceptanceTime,
		LeaderId:           term.LeaderId,
	}

	if term.AcceptedLeader != nil {
		clone.AcceptedLeader = &clustermetadatapb.ID{
			Component: term.AcceptedLeader.Component,
			Cell:      term.AcceptedLeader.Cell,
			Name:      term.AcceptedLeader.Name,
		}
	}

	return clone
}
