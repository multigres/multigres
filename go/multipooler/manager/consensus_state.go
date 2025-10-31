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
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// ConsensusState manages the in-memory and on-disk consensus state for this node.
// It provides thread-safe access to voting state and caches the state in memory
// to avoid repeated disk reads. It also runs a background goroutine to persist
// dirty state to disk asynchronously with automatic retries.
type ConsensusState struct {
	poolerDir string
	serviceID *clustermetadatapb.ID

	mu    sync.Mutex
	term  *multipoolermanagerdatapb.ConsensusTerm
	dirty bool // true if in-memory state differs from disk

	persistCh chan struct{} // Signal that persistence is needed
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

// NewConsensusState creates a new ConsensusState manager.
// It does not load state from disk - call Load() to initialize.
// It does not start the background persister - call StartPersister() after Load().
func NewConsensusState(poolerDir string, serviceID *clustermetadatapb.ID) *ConsensusState {
	ctx, cancel := context.WithCancel(context.Background())
	return &ConsensusState{
		poolerDir: poolerDir,
		serviceID: serviceID,
		term:      nil,
		dirty:     false,
		persistCh: make(chan struct{}, 1), // Buffered to avoid blocking
		ctx:       ctx,
		cancel:    cancel,
	}
}

// Load loads consensus state from disk into memory.
// If the file doesn't exist, initializes with default values (term 0, no accepted leader).
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

// Save signals the background persister to persist dirty state to disk.
// This method is non-blocking and returns immediately.
// The actual persistence happens asynchronously in the background with automatic retries.
func (cs *ConsensusState) Save() error {
	// Signal persistence needed (non-blocking due to buffered channel)
	select {
	case cs.persistCh <- struct{}{}:
	default:
		// Already signaled, no need to signal again
	}
	return nil
}

// WaitForPersistence waits until all dirty state has been persisted to disk.
// This is primarily useful for testing. Returns when dirty flag is cleared or context is cancelled.
func (cs *ConsensusState) WaitForPersistence(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		cs.mu.Lock()
		isDirty := cs.dirty
		cs.mu.Unlock()

		if !isDirty {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Continue checking
		}
	}
}

// StartPersister starts the background goroutine that persists dirty state to disk.
// Call this after Load() to enable automatic persistence.
func (cs *ConsensusState) StartPersister() {
	cs.wg.Add(1)
	go cs.persistLoop()
}

// Close stops the background persister and waits for it to finish.
func (cs *ConsensusState) Close() {
	cs.cancel()
	cs.wg.Wait()
}

// persistLoop runs in the background and periodically persists dirty state to disk.
func (cs *ConsensusState) persistLoop() {
	defer cs.wg.Done()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-cs.ctx.Done():
			// Final persist attempt before shutdown
			cs.tryPersist()
			return
		case <-ticker.C:
			cs.tryPersist()
		case <-cs.persistCh:
			cs.tryPersist()
		}
	}
}

// tryPersist attempts to persist dirty state to disk with indefinite retries for transient errors.
// It copies the term while holding the lock, then releases the lock during I/O.
// For permanent errors (like "data directory not initialized"), it gives up immediately.
func (cs *ConsensusState) tryPersist() {
	cs.mu.Lock()
	if !cs.dirty {
		cs.mu.Unlock()
		return
	}
	termCopy := cloneTerm(cs.term)
	cs.mu.Unlock()

	// Retry indefinitely for transient errors, give up immediately for permanent errors
	for {
		select {
		case <-cs.ctx.Done():
			// Shutdown requested, give up
			return
		default:
		}

		err := SetTerm(cs.poolerDir, termCopy)
		if err == nil {
			// Success - clear dirty flag
			cs.mu.Lock()
			// Only clear dirty if term hasn't changed since we copied it
			if cs.dirty && termEquals(cs.term, termCopy) {
				cs.dirty = false
			}
			cs.mu.Unlock()
			return
		}

		// Check if this is a permanent error (data directory not initialized)
		if isPermanentError(err) {
			// Don't retry permanent errors - just leave dirty flag set
			// The state will be persisted later when the data directory is initialized
			return
		}

		// Failed with transient error, wait and retry
		select {
		case <-cs.ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
			// Continue retry loop
		}
	}
}

// isPermanentError checks if an error is permanent and should not be retried
func isPermanentError(err error) bool {
	if err == nil {
		return false
	}
	// Check for "data directory not initialized" error
	errMsg := err.Error()
	return strings.Contains(errMsg, "data directory not initialized")
}

// GetCurrentTerm returns the current term.
// Returns 0 if state has not been loaded.
func (cs *ConsensusState) GetCurrentTerm() int64 {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.term == nil {
		return 0
	}
	return cs.term.GetCurrentTerm()
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

// UpdateTerm updates the current term and optionally the acceptedLeader field.
// If newTerm > currentTerm, resets acceptedLeader to nil.
// If newTerm == currentTerm and acceptedLeader is provided, updates acceptedLeader.
// If newTerm < currentTerm, returns an error.
// Changes are marked dirty but not persisted - call Save() to persist.
func (cs *ConsensusState) UpdateTerm(newTerm int64, acceptedLeader string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.term == nil {
		cs.term = &multipoolermanagerdatapb.ConsensusTerm{}
	}

	currentTerm := cs.term.GetCurrentTerm()

	// Reject outdated terms
	if newTerm < currentTerm {
		return fmt.Errorf("cannot update to older term: current=%d, new=%d", currentTerm, newTerm)
	}

	// If term is newer, reset AcceptedLeader to nil
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
		// Update last acceptance time
		now := time.Now()
		cs.term.LastAcceptanceTime = timestamppb.New(now)
		cs.dirty = true
	}

	return nil
}

// SetTerm replaces the entire consensus term state.
// Changes are marked dirty but not persisted - call Save() to persist.
func (cs *ConsensusState) SetTerm(term *multipoolermanagerdatapb.ConsensusTerm) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	cs.term = cloneTerm(term)
	cs.dirty = true
}

// AcceptAppointment records acceptance of the specified candidate as leader in the current term.
// Returns error if already accepted a different candidate in this term.
// Changes are marked dirty but not persisted - call Save() to persist.
func (cs *ConsensusState) AcceptAppointment(leaderID string) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.term == nil {
		return fmt.Errorf("consensus term not initialized")
	}

	// Check if already accepted someone else
	if cs.term.AcceptedLeader != nil && cs.term.AcceptedLeader.GetName() != leaderID {
		return fmt.Errorf("already accepted %s as leader in term %d", cs.term.AcceptedLeader.GetName(), cs.term.CurrentTerm)
	}

	// Grant acceptance
	if cs.term.AcceptedLeader == nil {
		cs.term.AcceptedLeader = &clustermetadatapb.ID{
			Component: cs.serviceID.Component,
			Cell:      cs.serviceID.Cell,
			Name:      leaderID,
		}
	} else {
		cs.term.AcceptedLeader.Name = leaderID
	}

	// Update last acceptance time
	now := time.Now()
	cs.term.LastAcceptanceTime = timestamppb.New(now)
	cs.dirty = true

	return nil
}

// cloneTerm creates a deep copy of a ConsensusTerm
func cloneTerm(term *multipoolermanagerdatapb.ConsensusTerm) *multipoolermanagerdatapb.ConsensusTerm {
	if term == nil {
		return nil
	}

	clone := &multipoolermanagerdatapb.ConsensusTerm{
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

// termEquals checks if two ConsensusTerm instances are equal
func termEquals(a, b *multipoolermanagerdatapb.ConsensusTerm) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	if a.CurrentTerm != b.CurrentTerm {
		return false
	}

	// Compare AcceptedLeader
	if a.AcceptedLeader == nil && b.AcceptedLeader == nil {
		return true
	}
	if a.AcceptedLeader == nil || b.AcceptedLeader == nil {
		return false
	}

	return a.AcceptedLeader.Component == b.AcceptedLeader.Component &&
		a.AcceptedLeader.Cell == b.AcceptedLeader.Cell &&
		a.AcceptedLeader.Name == b.AcceptedLeader.Name
}
