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
	"context"
	"errors"
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/multigres/multigres/go/common/consensus"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
)

// ConsensusPromises manages the in-memory and on-disk consensus state for this node.
// It provides thread-safe access to consensus state and ensures that memory is only
// updated after successful disk writes (pessimistic approach).
type ConsensusPromises struct {
	poolerDir string
	serviceID *clustermetadatapb.ID

	mu sync.Mutex
	// revocation is persisted to disk; see term_storage.go.
	revocation *clustermetadatapb.TermRevocation
	// minRecruitPosition, if non-nil, is the minimum position (rule numbers +
	// LSN) this pooler must reach before Recruit() may succeed — set
	// immediately before an operation (restore-from-backup, pg_rewind) that
	// can silently break WAL continuity.
	//
	// TODO: not yet persisted — a process restart silently clears it. Once
	// durability is needed, wrap it alongside TermRevocation in a new
	// ConsensusPromisesState proto message, persisted in the same file
	// term_storage.go already writes (replacing today's bare TermRevocation
	// JSON), with a migration fallback for existing on-disk files that
	// predate the wrapper (protojson.Unmarshal into the new shape will error
	// on an old file's fields; fall back to unmarshaling the old bare
	// TermRevocation shape).
	//
	// TODO: not yet enforced by Recruit() — refuse Recruit() unless either
	// this pooler's current position has reached minRecruitPosition, or its
	// highest known rule shows it as an observer (WAL continuity only
	// matters for cohort members). Clear the floor once either condition is
	// met.
	minRecruitPosition *clustermetadatapb.LsnPosition
}

// NewConsensusPromises creates a new ConsensusPromises manager.
// It does not load state from disk - call Load() to initialize.
func NewConsensusPromises(poolerDir string, serviceID *clustermetadatapb.ID) *ConsensusPromises {
	return &ConsensusPromises{
		poolerDir:  poolerDir,
		serviceID:  serviceID,
		revocation: nil,
	}
}

// Load loads consensus state from disk into memory.
// If the file doesn't exist, initializes with default values (term 0, no accepted coordinator).
// This method is idempotent - subsequent calls will reload from disk.
func (cs *ConsensusPromises) Load() (int64, error) {
	revocation, err := cs.getRevocation()
	if err != nil {
		return 0, fmt.Errorf("failed to load consensus term: %w", err)
	}

	cs.mu.Lock()
	cs.revocation = revocation
	cs.mu.Unlock()

	return revocation.RevokedBelowTerm, nil
}

// GetMinRecruitPosition returns the minimum position this pooler must reach
// before Recruit() may succeed, or nil if no floor is set.
func (cs *ConsensusPromises) GetMinRecruitPosition(ctx context.Context) (*clustermetadatapb.LsnPosition, error) {
	if err := actionlock.AssertActionLockHeld(ctx); err != nil {
		return nil, err
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cloneMinRecruitPosition(cs.minRecruitPosition), nil
}

// SetMinRecruitPosition records the minimum position this pooler must reach
// before Recruit() may succeed. Requires the action lock (ctx must be an
// action-lock context).
func (cs *ConsensusPromises) SetMinRecruitPosition(ctx context.Context, pos *clustermetadatapb.LsnPosition) error {
	if err := actionlock.AssertActionLockHeld(ctx); err != nil {
		return err
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.minRecruitPosition = cloneMinRecruitPosition(pos)
	return nil
}

// cloneMinRecruitPosition creates a deep copy of an LsnPosition.
func cloneMinRecruitPosition(pos *clustermetadatapb.LsnPosition) *clustermetadatapb.LsnPosition {
	if pos == nil {
		return nil
	}
	return proto.Clone(pos).(*clustermetadatapb.LsnPosition)
}

// GetInconsistentRevocation returns a copy of the current term revocation for monitoring.
// It doesn't require the action lock to be held, so the value returned may
// be outdated by the time it's used. Use GetRevocation() as part of any action
// workflow to protect against race conditions.
// Returns nil if state has not been loaded.
func (cs *ConsensusPromises) GetInconsistentRevocation() *clustermetadatapb.TermRevocation {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.revocation == nil {
		return nil
	}

	// Return a copy to prevent external modifications
	return cloneRevocation(cs.revocation)
}

// GetRevocation returns a copy of the current term revocation.
// Returns nil if state has not been loaded.
func (cs *ConsensusPromises) GetRevocation(ctx context.Context) (*clustermetadatapb.TermRevocation, error) {
	if err := actionlock.AssertActionLockHeld(ctx); err != nil {
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

// AcceptRevocation validates and persists a TermRevocation in one atomic step.
// It builds the validation status from the observed position in status combined
// with the current in-memory revocation (read under the mutex), so the check
// reflects the actual locked state rather than a potentially stale snapshot.
func (cs *ConsensusPromises) AcceptRevocation(ctx context.Context, status *clustermetadatapb.ConsensusStatus, revocation *clustermetadatapb.TermRevocation) error {
	if err := actionlock.AssertActionLockHeld(ctx); err != nil {
		return err
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if !proto.Equal(status.TermRevocation, cs.revocation) {
		return errors.New("status parameter is out of date")
	}

	if err := consensus.ValidateRevocation(status, revocation); err != nil {
		return err
	}

	return cs.saveAndUpdateLocked(cloneRevocation(revocation))
}

// saveAndUpdateLocked saves the revocation to disk and updates memory.
// MUST be called with cs.mu held.
// This is the key method that ensures memory never diverges from disk.
// If the save fails, memory remains unchanged and the error is returned.
func (cs *ConsensusPromises) saveAndUpdateLocked(newRevocation *clustermetadatapb.TermRevocation) error {
	// Save to disk (lock still held)
	if err := cs.setRevocation(newRevocation); err != nil {
		// Save failed - don't update memory, propagate error
		return fmt.Errorf("failed to save consensus term: %w", err)
	}

	// Save succeeded - NOW update memory
	cs.revocation = cloneRevocation(newRevocation)
	return nil
}

// cloneRevocation creates a deep copy of a TermRevocation.
func cloneRevocation(revocation *clustermetadatapb.TermRevocation) *clustermetadatapb.TermRevocation {
	if revocation == nil {
		return nil
	}
	return proto.Clone(revocation).(*clustermetadatapb.TermRevocation)
}
