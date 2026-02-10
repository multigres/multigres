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
	"errors"
	"fmt"
	"sync"
	"time"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// RecoveryActionState manages the in-memory and on-disk recovery state.
// It provides thread-safe access and ensures that memory is only updated after
// successful disk writes (pessimistic approach).
type RecoveryActionState struct {
	poolerDir string
	serviceID *clustermetadatapb.ID

	mu     sync.Mutex
	action *RecoveryAction // cached action from disk, nil means no action pending
}

// NewRecoveryActionState creates a new RecoveryActionState manager.
// It does not load state from disk - call Load() to initialize.
func NewRecoveryActionState(poolerDir string, serviceID *clustermetadatapb.ID) *RecoveryActionState {
	return &RecoveryActionState{
		poolerDir: poolerDir,
		serviceID: serviceID,
		action:    nil,
	}
}

// Load loads recovery state from disk into memory.
// If the file doesn't exist, initializes with nil (no action needed).
// This method is idempotent - subsequent calls will reload from disk.
func (ras *RecoveryActionState) Load() error {
	action, err := getRecoveryState(ras.poolerDir)
	if err != nil {
		return fmt.Errorf("failed to load recovery state: %w", err)
	}

	ras.mu.Lock()
	ras.action = action
	ras.mu.Unlock()

	return nil
}

// GetRecoveryAction returns a copy of the current recovery action.
// REQUIRES: action lock must be held (verified via context).
// Returns nil if no recovery action is pending.
func (ras *RecoveryActionState) GetRecoveryAction(ctx context.Context) (*RecoveryAction, error) {
	if err := AssertActionLockHeld(ctx); err != nil {
		return nil, err
	}

	ras.mu.Lock()
	defer ras.mu.Unlock()

	if ras.action == nil {
		return nil, nil
	}

	// Return a copy to prevent external modifications
	return copyRecoveryAction(ras.action), nil
}

// GetInconsistentRecoveryAction returns the current recovery action for monitoring.
// It doesn't require the action lock to be held, so the value returned may be
// outdated by the time it's used. Use GetRecoveryAction() as part of any action
// workflow to protect against race conditions.
// Returns nil if no recovery action is pending.
func (ras *RecoveryActionState) GetInconsistentRecoveryAction() (*RecoveryAction, error) {
	ras.mu.Lock()
	defer ras.mu.Unlock()

	if ras.action == nil {
		return nil, nil
	}

	// Return a copy to prevent external modifications
	return copyRecoveryAction(ras.action), nil
}

// SetRecoveryAction saves a recovery action to disk and updates memory.
// REQUIRES: action lock must be held (verified via context).
func (ras *RecoveryActionState) SetRecoveryAction(ctx context.Context, actionType RecoveryActionType, reason string, consensusTerm int64) error {
	if err := AssertActionLockHeld(ctx); err != nil {
		return err
	}

	if actionType == RecoveryActionTypeNone {
		return errors.New("use ClearRecoveryAction to clear the recovery action")
	}

	ras.mu.Lock()
	defer ras.mu.Unlock()

	// Create new recovery action
	action := &RecoveryAction{
		ActionType:    actionType,
		Reason:        reason,
		SetTime:       time.Now(),
		ConsensusTerm: consensusTerm,
	}

	// Save to disk first (lock still held)
	if err := setRecoveryState(ras.poolerDir, action); err != nil {
		// Save failed - don't update memory, propagate error
		return fmt.Errorf("failed to save recovery state: %w", err)
	}

	// Save succeeded - NOW update memory
	ras.action = copyRecoveryAction(action)
	return nil
}

// ClearRecoveryAction removes the recovery action from disk and memory.
// REQUIRES: action lock must be held (verified via context).
// Only clears if the current action type matches expectedType.
// Returns an error if the types don't match (indicating a different action was set).
// Idempotent: succeeds if no recovery action exists.
func (ras *RecoveryActionState) ClearRecoveryAction(ctx context.Context, expectedType RecoveryActionType) error {
	if err := AssertActionLockHeld(ctx); err != nil {
		return err
	}

	ras.mu.Lock()
	defer ras.mu.Unlock()

	// If no action pending, clearing is a no-op (idempotent)
	if ras.action == nil || ras.action.ActionType == RecoveryActionTypeNone {
		return nil
	}

	// Verify we're clearing the expected action type
	if ras.action.ActionType != expectedType {
		return fmt.Errorf(
			"recovery action type mismatch: expected %s, current is %s (set at %s for term %d). Not clearing to avoid removing wrong action",
			expectedType,
			ras.action.ActionType,
			ras.action.SetTime.Format(time.RFC3339),
			ras.action.ConsensusTerm,
		)
	}

	// Clear from disk first (lock still held)
	if err := clearRecoveryState(ras.poolerDir); err != nil {
		// Clear failed - don't update memory, propagate error
		return fmt.Errorf("failed to clear recovery state: %w", err)
	}

	// Clear succeeded - NOW update memory
	ras.action = nil
	return nil
}

// HasPendingRecoveryAction checks if a recovery action is pending.
// This is a convenience method for monitoring that doesn't require the action lock.
func (ras *RecoveryActionState) HasPendingRecoveryAction() bool {
	ras.mu.Lock()
	defer ras.mu.Unlock()
	return ras.action != nil && ras.action.ActionType != RecoveryActionTypeNone
}

// copyRecoveryAction creates a copy of a RecoveryAction
func copyRecoveryAction(action *RecoveryAction) *RecoveryAction {
	if action == nil {
		return nil
	}
	// Simple value copy for Go struct
	actionCopy := *action
	return &actionCopy
}
