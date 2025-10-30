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
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// ConsensusState manages the in-memory and on-disk consensus state for this node.
// It provides thread-safe access to voting state and caches the state in memory
// to avoid repeated disk reads.
type ConsensusState struct {
	poolerDir string
	serviceID *clustermetadatapb.ID

	mu    sync.RWMutex
	term  *pgctldpb.ConsensusTerm
	dirty bool // true if in-memory state differs from disk
}

// NewConsensusState creates a new ConsensusState manager.
// It does not load state from disk - call Load() to initialize.
func NewConsensusState(poolerDir string, serviceID *clustermetadatapb.ID) *ConsensusState {
	return &ConsensusState{
		poolerDir: poolerDir,
		serviceID: serviceID,
		term:      nil,
		dirty:     false,
	}
}

// Load loads consensus state from disk into memory.
// If the file doesn't exist, initializes with default values (term 0, no vote).
// This method is idempotent - subsequent calls will reload from disk.
func (cs *ConsensusState) Load() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	term, err := GetTerm(cs.poolerDir)
	if err != nil {
		return fmt.Errorf("failed to load consensus term: %w", err)
	}

	cs.term = term
	cs.dirty = false
	return nil
}

// Save persists the current in-memory state to disk using atomic write.
// Only writes if the state has been modified (dirty flag is set).
func (cs *ConsensusState) Save() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if !cs.dirty {
		// No changes to persist
		return nil
	}

	if cs.term == nil {
		return fmt.Errorf("cannot save nil consensus term")
	}

	if err := SetTerm(cs.poolerDir, cs.term); err != nil {
		return fmt.Errorf("failed to save consensus term: %w", err)
	}

	cs.dirty = false
	return nil
}

// GetCurrentTerm returns the current term.
// Returns 0 if state has not been loaded.
func (cs *ConsensusState) GetCurrentTerm() int64 {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	if cs.term == nil {
		return 0
	}
	return cs.term.GetCurrentTerm()
}

// GetAcceptedLeader returns the candidate ID this node voted for in the current term.
// Returns empty string if no vote has been cast.
func (cs *ConsensusState) GetAcceptedLeader() string {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	if cs.term == nil || cs.term.AcceptedLeader == nil {
		return ""
	}
	return cs.term.AcceptedLeader.GetName()
}

// GetTerm returns a copy of the current consensus term.
// Returns nil if state has not been loaded.
func (cs *ConsensusState) GetTerm() *pgctldpb.ConsensusTerm {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	if cs.term == nil {
		return nil
	}

	// Return a copy to prevent external modifications
	return cloneTerm(cs.term)
}

// UpdateTerm updates the current term and optionally the acceptedLeader field.
// If newTerm > currentTerm, resets acceptedLeader to nil.
// If newTerm == currentTerm and acceptedLeader is provided, updates acceptedLeader.
// If newTerm < currentTerm, returns an error.
// Changes are marked dirty but not persisted - call Save() to persist.
func (cs *ConsensusState) UpdateTerm(newTerm int64, acceptedLeader string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.term == nil {
		cs.term = &pgctldpb.ConsensusTerm{}
	}

	currentTerm := cs.term.GetCurrentTerm()

	// Reject outdated terms
	if newTerm < currentTerm {
		return fmt.Errorf("cannot update to older term: current=%d, new=%d", currentTerm, newTerm)
	}

	// If term is newer, reset vote
	if newTerm > currentTerm {
		cs.term.CurrentTerm = newTerm
		cs.term.AcceptedLeader = nil
		cs.term.LastAcceptanceTime = nil
		cs.dirty = true
	}

	// Update acceptedLeader if provided
	if acceptedLeader != "" {
		if cs.term.AcceptedLeader == nil {
			cs.term.AcceptedLeader = &clustermetadatapb.ID{
				Component: cs.serviceID.Component,
				Cell:      cs.serviceID.Cell,
				Name:      acceptedLeader,
			}
		} else {
			cs.term.AcceptedLeader.Name = acceptedLeader
		}
		// Update last vote time
		now := time.Now()
		cs.term.LastAcceptanceTime = timestamppb.New(now)
		cs.dirty = true
	}

	return nil
}

// SetTerm replaces the entire consensus term state.
// Changes are marked dirty but not persisted - call Save() to persist.
func (cs *ConsensusState) SetTerm(term *pgctldpb.ConsensusTerm) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	cs.term = cloneTerm(term)
	cs.dirty = true
}

// AcceptAppointment records a vote for the specified candidate in the current term.
// Returns error if already voted for a different candidate in this term.
// Changes are marked dirty but not persisted - call Save() to persist.
func (cs *ConsensusState) AcceptAppointment(leaderID string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.term == nil {
		return fmt.Errorf("consensus term not initialized")
	}

	// Check if already voted for someone else
	if cs.term.AcceptedLeader != nil && cs.term.AcceptedLeader.GetName() != leaderID {
		return fmt.Errorf("already voted for %s in term %d", cs.term.AcceptedLeader.GetName(), cs.term.CurrentTerm)
	}

	// Grant vote
	if cs.term.AcceptedLeader == nil {
		cs.term.AcceptedLeader = &clustermetadatapb.ID{
			Component: cs.serviceID.Component,
			Cell:      cs.serviceID.Cell,
			Name:      leaderID,
		}
	} else {
		cs.term.AcceptedLeader.Name = leaderID
	}

	// Update last vote time
	now := time.Now()
	cs.term.LastAcceptanceTime = timestamppb.New(now)
	cs.dirty = true

	return nil
}

// cloneTerm creates a deep copy of a ConsensusTerm
func cloneTerm(term *pgctldpb.ConsensusTerm) *pgctldpb.ConsensusTerm {
	if term == nil {
		return nil
	}

	clone := &pgctldpb.ConsensusTerm{
		CurrentTerm:        term.CurrentTerm,
		LastAcceptanceTime: term.LastAcceptanceTime,
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
