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

package consensus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

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
	// replicationPrimary is held in memory only — see HighestKnownRule's proto
	// comment. Populated by RPCs that inform this pooler about the cluster
	// state (SetPrimary today; Promote can be wired in via the same RecordTermPrimary
	// entry point). Restarts reset it to nil; coordinators re-inform the
	// pooler.
	replicationPrimary *clustermetadatapb.ReplicationPrimary
	// leaderObservedAt is when RecordTermPrimary last saw the leader ID change
	// (a new primary was recorded). Zero until the first leader is recorded. Used
	// to measure how long a diverged follower's pg_rewind waited for that leader
	// to become rewind-ready: the delay is (rewind start) - leaderObservedAt, which
	// is ~0 when the same SetPrimary that learned the new leader could also rewind.
	leaderObservedAt time.Time
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

// RecordTermPrimary updates the highest-known (rule, primary, rewind_ready)
// based on what this pooler has been told. A nil argument (or nil rule) is a
// no-op. Bump semantics:
//
//   - rule: updated only when strictly greater than the current value
//     (comparison by RuleNumber: coordinator_term then leader_subterm).
//
//   - primary: updated when the rule advances, OR when the supplied rule
//     equals the current rule but the primary's contact info has changed.
//     The same-rule-different-contact case covers a primary pooler being
//     re-homed (host or port changes) without a new election.
//
//   - rewind_ready: recorded alongside, and a change to it is itself enough to
//     record even at the same rule and contact — the leader completing its
//     post-promotion checkpoint flips rewind_ready false->true without a new
//     election, and a diverged follower gates its pg_rewind on that flip.
//
// Safe to call on no-op SetPrimary paths so the recorded values reflect
// everything the pooler has been told, regardless of whether postgres-side
// changes were applied.
func (cs *ConsensusPromises) RecordTermPrimary(rp *clustermetadatapb.ReplicationPrimary) {
	rule := rp.GetRule()
	if rule == nil {
		return
	}
	primary := rp.GetPrimary()
	rewindReady := rp.GetRewindReady()
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cmp := consensus.CompareRuleNumbers(rule.GetRuleNumber(), cs.replicationPrimary.GetRule().GetRuleNumber())
	if cmp < 0 {
		return
	}
	if cmp == 0 &&
		(primary == nil || proto.Equal(primary, cs.replicationPrimary.GetPrimary())) &&
		rewindReady == cs.replicationPrimary.GetRewindReady() {
		return
	}
	// Build the next value by starting from the existing fields (via getters,
	// which are nil-safe) and overlaying the updates we want to apply.
	next := &clustermetadatapb.ReplicationPrimary{
		Rule:        cs.replicationPrimary.GetRule(),
		Primary:     cs.replicationPrimary.GetPrimary(),
		RewindReady: rewindReady,
	}
	if cmp > 0 {
		next.Rule = proto.Clone(rule).(*clustermetadatapb.ShardRule)
	}
	// Update primary only when one is supplied; an RPC that advances the rule
	// without re-stating the primary (or a future WAL-observation path) leaves
	// the previously-recorded contact info in place.
	if primary != nil {
		next.Primary = proto.Clone(primary).(*clustermetadatapb.PoolerAddress)
	}
	// Stamp the time the leader identity changed, so a subsequent rewind toward
	// this leader can report how long it waited for the leader to become
	// rewind-ready (~0 when this same call already advances to a rewind-ready
	// leader).
	if !proto.Equal(cs.replicationPrimary.GetPrimary().GetId(), next.GetPrimary().GetId()) {
		cs.leaderObservedAt = time.Now()
	}
	cs.replicationPrimary = next
}

// LeaderObservedAt returns when RecordTermPrimary last recorded a change of leader
// identity, or the zero time if no leader has been recorded yet.
func (cs *ConsensusPromises) LeaderObservedAt() time.Time {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.leaderObservedAt
}

// GetReplicationPrimary returns a copy of the primary + rule this pooler has
// been told to use, or nil if it has never been informed. The returned message
// is the same one exposed in ConsensusStatus.
func (cs *ConsensusPromises) GetReplicationPrimary() *clustermetadatapb.ReplicationPrimary {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if cs.replicationPrimary == nil {
		return nil
	}
	return proto.Clone(cs.replicationPrimary).(*clustermetadatapb.ReplicationPrimary)
}

// MarkSelfRewindReady records that this pooler is safe to pg_rewind from: it has
// checkpointed onto its current timeline. The postgres monitor calls this once it
// observes the condition (and the async post-promotion checkpoint can flip it as
// soon as it completes). It is one-way: the flag auto-clears whenever
// RecordTermPrimary installs a fresh ReplicationPrimary — a new coordinator term,
// or a SetPrimary naming a different leader (resignation / restart-as-replica) —
// since the new record defaults rewind_ready to false. Returns true if the value
// changed, so callers can broadcast the new health promptly.
//
// selfID and expectedCoordinatorTerm guard against marking the wrong record:
// only the pooler the record names as primary may declare itself rewind-ready,
// and only at the coordinator term it expects. The term check matters for the
// async post-promotion checkpoint — by the time it completes a newer term (a
// re-promotion or a different leader) may have replaced the record, and we must
// not stamp rewind_ready onto that newer term whose checkpoint hasn't completed.
// Both checks happen under the lock so the decision is atomic. (Whether this
// pooler currently holds non-resigned leadership is the caller's concern —
// resignation state lives in the manager, not here.) No-op if no
// ReplicationPrimary has been recorded yet.
func (cs *ConsensusPromises) MarkSelfRewindReady(selfID *clustermetadatapb.ID, expectedCoordinatorTerm int64) bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if cs.replicationPrimary == nil || cs.replicationPrimary.GetRewindReady() {
		return false
	}
	if !proto.Equal(cs.replicationPrimary.GetPrimary().GetId(), selfID) {
		return false
	}
	if cs.replicationPrimary.GetRule().GetRuleNumber().GetCoordinatorTerm() != expectedCoordinatorTerm {
		return false
	}
	next := proto.Clone(cs.replicationPrimary).(*clustermetadatapb.ReplicationPrimary)
	next.RewindReady = true
	cs.replicationPrimary = next
	return true
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

// GetCurrentTermNumber returns the current term.
// Returns 0 if state has not been loaded.
func (cs *ConsensusPromises) GetCurrentTermNumber(ctx context.Context) (int64, error) {
	if err := actionlock.AssertActionLockHeld(ctx); err != nil {
		return 0, err
	}
	return cs.GetInconsistentCurrentTermNumber()
}

// GetInconsistentCurrentTermNumber returns the current term for monitoring.
// It doesn't require the action lock to be held, so the value returned may
// be outdated by the time it's used. Use GetCurrentTermNumber() as part of
// any action workflow to protect against race conditions.
// Returns 0 if state has not been loaded.
func (cs *ConsensusPromises) GetInconsistentCurrentTermNumber() (int64, error) {
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
func (cs *ConsensusPromises) GetInconsistentRevocation() *clustermetadatapb.TermRevocation {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.revocation == nil {
		return nil
	}

	// Return a copy to prevent external modifications
	return cloneRevocation(cs.revocation)
}

// GetAcceptedLeader returns the coordinator ID this pooler accepted the term from.
// Returns empty string if no coordinator was accepted.
func (cs *ConsensusPromises) GetAcceptedLeader(ctx context.Context) (string, error) {
	if err := actionlock.AssertActionLockHeld(ctx); err != nil {
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

// UpdateTermAndSave atomically updates the term number, resetting accepted coordinator.
// This is called when discovering a newer term from another node.
// Returns error if newTerm < currentTerm.
// Idempotent: succeeds without changes if newTerm == currentTerm.
func (cs *ConsensusPromises) UpdateTermAndSave(ctx context.Context, newTerm int64) error {
	if err := actionlock.AssertActionLockHeld(ctx); err != nil {
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
