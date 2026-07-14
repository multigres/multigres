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
	// promises is the full on-disk state, held in memory; see
	// promise_storage.go. Never nil after construction. Every mutator clones
	// it, updates only the field it owns, persists the clone, then swaps it
	// in — so a field's zero value is never mistaken for "leave unchanged."
	promises *clustermetadatapb.ConsensusPromises
}

// NewConsensusPromises creates a new ConsensusPromises manager.
// It does not load state from disk - call Load() to initialize; promises is
// nil until then (proto3 getters are nil-safe, so reads before Load() behave
// like an all-default message).
func NewConsensusPromises(poolerDir string, serviceID *clustermetadatapb.ID) *ConsensusPromises {
	return &ConsensusPromises{
		poolerDir: poolerDir,
		serviceID: serviceID,
	}
}

// Load loads consensus state from disk into memory.
// If the file doesn't exist, initializes with default values (term 0, no accepted coordinator).
// This method is idempotent - subsequent calls will reload from disk.
func (cs *ConsensusPromises) Load() (int64, error) {
	file, err := cs.readPromisesFromDisk()
	if err != nil {
		return 0, fmt.Errorf("failed to load consensus promises: %w", err)
	}

	cs.mu.Lock()
	cs.promises = file
	cs.mu.Unlock()

	return file.GetTermRevocation().GetRevokedBelowTerm(), nil
}

// SetRecruitBlockedUntil records the minimum position this pooler must reach
// before Recruit() may succeed. Requires the action lock (ctx must be an
// action-lock context).
func (cs *ConsensusPromises) SetRecruitBlockedUntil(ctx context.Context, pos *clustermetadatapb.LsnPosition) error {
	if err := actionlock.AssertActionLockHeld(ctx); err != nil {
		return err
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()

	updated := cloneConsensusPromises(cs.promises)
	updated.RecruitBlockedUntil = pos
	if err := cs.writePromisesToDisk(updated); err != nil {
		return fmt.Errorf("failed to save recruit position floor: %w", err)
	}
	cs.promises = updated
	return nil
}

// cloneConsensusPromises creates a deep copy of a ConsensusPromises message,
// or an empty one if p is nil — so a caller that clones-then-mutates (the
// Set*/Accept* methods below) never dereferences a nil result. Reads may
// safely pass a nil-valued clone straight back out: proto3 getters are
// nil-safe, so a nil-vs-not-yet-loaded message behaves like an all-default
// one to callers.
func cloneConsensusPromises(p *clustermetadatapb.ConsensusPromises) *clustermetadatapb.ConsensusPromises {
	if p == nil {
		return &clustermetadatapb.ConsensusPromises{}
	}
	return proto.Clone(p).(*clustermetadatapb.ConsensusPromises)
}

// GetInconsistent returns a copy of the current promises (term revocation,
// recruit-blocked floor, recruit-observed LSN) without requiring the action
// lock, so the result may be stale by the time it's used. For status
// snapshots and monitoring only (e.g. ConsensusStatus, the recruit-position
// floor check) — never for gating a consensus decision; use GetConsistent
// for that.
func (cs *ConsensusPromises) GetInconsistent() *clustermetadatapb.ConsensusPromises {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cloneConsensusPromises(cs.promises)
}

// GetConsistent returns a copy of the current promises. Requires the action
// lock: callers gating a consensus decision (Promote, SetPrimary, Recruit)
// must read this under the same lock they'll act within, so the check
// reflects the actual locked state rather than a potentially stale snapshot.
func (cs *ConsensusPromises) GetConsistent(ctx context.Context) (*clustermetadatapb.ConsensusPromises, error) {
	if err := actionlock.AssertActionLockHeld(ctx); err != nil {
		return nil, err
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cloneConsensusPromises(cs.promises), nil
}

// AcceptRevocation validates and persists a TermRevocation in one atomic step,
// snapshotting the LSN observed in status (the freshly re-read, post-stabilize
// position) as the new recruit-observed baseline. It builds the validation
// status from the observed position in status combined with the current
// in-memory revocation (read under the mutex), so the check reflects the
// actual locked state rather than a potentially stale snapshot.
func (cs *ConsensusPromises) AcceptRevocation(ctx context.Context, status *clustermetadatapb.ConsensusStatus, revocation *clustermetadatapb.TermRevocation) error {
	if err := actionlock.AssertActionLockHeld(ctx); err != nil {
		return err
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if !proto.Equal(status.TermRevocation, cs.promises.GetTermRevocation()) {
		return errors.New("status parameter is out of date")
	}

	if err := consensus.ValidateRevocation(status, revocation); err != nil {
		return err
	}

	updated := cloneConsensusPromises(cs.promises)
	updated.TermRevocation = proto.Clone(revocation).(*clustermetadatapb.TermRevocation)
	updated.RecruitObservedLsn = status.GetCurrentPosition().GetLsn()
	if err := cs.writePromisesToDisk(updated); err != nil {
		return fmt.Errorf("failed to save consensus term: %w", err)
	}
	cs.promises = updated
	return nil
}
