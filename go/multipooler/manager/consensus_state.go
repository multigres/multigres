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
func (lcs *ConsensusState) Load() error {
	lcs.mu.Lock()
	defer lcs.mu.Unlock()

	term, err := GetTerm(lcs.poolerDir)
	if err != nil {
		return fmt.Errorf("failed to load consensus term: %w", err)
	}

	lcs.term = term
	lcs.dirty = false
	return nil
}

// Save persists the current in-memory state to disk using atomic write.
// Only writes if the state has been modified (dirty flag is set).
func (lcs *ConsensusState) Save() error {
	lcs.mu.Lock()
	defer lcs.mu.Unlock()

	if !lcs.dirty {
		// No changes to persist
		return nil
	}

	if lcs.term == nil {
		return fmt.Errorf("cannot save nil consensus term")
	}

	if err := SetTerm(lcs.poolerDir, lcs.term); err != nil {
		return fmt.Errorf("failed to save consensus term: %w", err)
	}

	lcs.dirty = false
	return nil
}

// GetCurrentTerm returns the current term.
// Returns 0 if state has not been loaded.
func (lcs *ConsensusState) GetCurrentTerm() int64 {
	lcs.mu.RLock()
	defer lcs.mu.RUnlock()

	if lcs.term == nil {
		return 0
	}
	return lcs.term.GetCurrentTerm()
}

// GetVotedFor returns the candidate ID this node voted for in the current term.
// Returns empty string if no vote has been cast.
func (lcs *ConsensusState) GetVotedFor() string {
	lcs.mu.RLock()
	defer lcs.mu.RUnlock()

	if lcs.term == nil || lcs.term.VotedFor == nil {
		return ""
	}
	return lcs.term.VotedFor.GetName()
}

// GetTerm returns a copy of the current consensus term.
// Returns nil if state has not been loaded.
func (lcs *ConsensusState) GetTerm() *pgctldpb.ConsensusTerm {
	lcs.mu.RLock()
	defer lcs.mu.RUnlock()

	if lcs.term == nil {
		return nil
	}

	// Return a copy to prevent external modifications
	return cloneTerm(lcs.term)
}

// UpdateTerm updates the current term and optionally the votedFor field.
// If newTerm > currentTerm, resets votedFor to nil.
// If newTerm == currentTerm and votedFor is provided, updates votedFor.
// If newTerm < currentTerm, returns an error.
// Changes are marked dirty but not persisted - call Save() to persist.
func (lcs *ConsensusState) UpdateTerm(newTerm int64, votedFor string) error {
	lcs.mu.Lock()
	defer lcs.mu.Unlock()

	if lcs.term == nil {
		lcs.term = &pgctldpb.ConsensusTerm{}
	}

	currentTerm := lcs.term.GetCurrentTerm()

	// Reject outdated terms
	if newTerm < currentTerm {
		return fmt.Errorf("cannot update to older term: current=%d, new=%d", currentTerm, newTerm)
	}

	// If term is newer, reset vote
	if newTerm > currentTerm {
		lcs.term.CurrentTerm = newTerm
		lcs.term.VotedFor = nil
		lcs.term.LastVoteTime = nil
		lcs.dirty = true
	}

	// Update votedFor if provided
	if votedFor != "" {
		if lcs.term.VotedFor == nil {
			lcs.term.VotedFor = &clustermetadatapb.ID{
				Component: lcs.serviceID.Component,
				Cell:      lcs.serviceID.Cell,
				Name:      votedFor,
			}
		} else {
			lcs.term.VotedFor.Name = votedFor
		}
		// Update last vote time
		now := time.Now()
		lcs.term.LastVoteTime = timestamppb.New(now)
		lcs.dirty = true
	}

	return nil
}

// SetTerm replaces the entire consensus term state.
// Changes are marked dirty but not persisted - call Save() to persist.
func (lcs *ConsensusState) SetTerm(term *pgctldpb.ConsensusTerm) {
	lcs.mu.Lock()
	defer lcs.mu.Unlock()

	lcs.term = cloneTerm(term)
	lcs.dirty = true
}

// GrantVote records a vote for the specified candidate in the current term.
// Returns error if already voted for a different candidate in this term.
// Changes are marked dirty but not persisted - call Save() to persist.
func (lcs *ConsensusState) GrantVote(candidateID string) error {
	lcs.mu.Lock()
	defer lcs.mu.Unlock()

	if lcs.term == nil {
		return fmt.Errorf("consensus term not initialized")
	}

	// Check if already voted for someone else
	if lcs.term.VotedFor != nil && lcs.term.VotedFor.GetName() != candidateID {
		return fmt.Errorf("already voted for %s in term %d", lcs.term.VotedFor.GetName(), lcs.term.CurrentTerm)
	}

	// Grant vote
	if lcs.term.VotedFor == nil {
		lcs.term.VotedFor = &clustermetadatapb.ID{
			Component: lcs.serviceID.Component,
			Cell:      lcs.serviceID.Cell,
			Name:      candidateID,
		}
	} else {
		lcs.term.VotedFor.Name = candidateID
	}

	// Update last vote time
	now := time.Now()
	lcs.term.LastVoteTime = timestamppb.New(now)
	lcs.dirty = true

	return nil
}

// cloneTerm creates a deep copy of a ConsensusTerm
func cloneTerm(term *pgctldpb.ConsensusTerm) *pgctldpb.ConsensusTerm {
	if term == nil {
		return nil
	}

	clone := &pgctldpb.ConsensusTerm{
		CurrentTerm:  term.CurrentTerm,
		LastVoteTime: term.LastVoteTime,
	}

	if term.VotedFor != nil {
		clone.VotedFor = &clustermetadatapb.ID{
			Component: term.VotedFor.Component,
			Cell:      term.VotedFor.Cell,
			Name:      term.VotedFor.Name,
		}
	}

	return clone
}
