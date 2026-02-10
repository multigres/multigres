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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// initializeTestDataDir creates the minimal data directory structure needed for tests.
// This simulates what pgctld init does - creates pg_data with PG_VERSION file.
func initializeTestDataDir(t *testing.T, poolerDir string) {
	t.Helper()
	dataDir := postgresDataDir(poolerDir)
	err := os.MkdirAll(dataDir, 0o755)
	require.NoError(t, err)

	// Create PG_VERSION file to mark directory as initialized
	pgVersionFile := filepath.Join(dataDir, "PG_VERSION")
	err = os.WriteFile(pgVersionFile, []byte("16\n"), 0o644)
	require.NoError(t, err)
}

// TestRecoveryActionPersistence verifies that recovery actions persist to disk
// and survive manager restarts (simulated crashes).
func TestRecoveryActionPersistence(t *testing.T) {
	ctx := context.Background()

	// Create first manager
	pm1 := newTestManager(t)

	// Initialize data directory structure
	initializeTestDataDir(t, pm1.multipooler.PoolerDir)

	lockCtx, err := pm1.actionLock.Acquire(ctx, "test")
	require.NoError(t, err)

	// Set recovery action
	err = pm1.recoveryActionState.SetRecoveryAction(
		lockCtx,
		RecoveryActionTypeNeedsRewind,
		"test emergency demote",
		123,
	)
	require.NoError(t, err)

	// Verify state file exists
	poolerDir := pm1.multipooler.PoolerDir
	statePath := recoveryStatePath(poolerDir)
	_, err = os.Stat(statePath)
	require.NoError(t, err, "recovery state file should exist")

	pm1.actionLock.Release(lockCtx)

	// Simulate crash: create new manager with same directory
	pm2 := newTestManager(t, func(pm *MultiPoolerManager) {
		pm.multipooler.PoolerDir = poolerDir
		pm.recoveryActionState = NewRecoveryActionState(poolerDir, pm.serviceID)
		pm.consensusState = NewConsensusState(poolerDir, pm.serviceID)
	})

	// Load state from disk (simulates what happens at startup)
	err = pm2.recoveryActionState.Load()
	require.NoError(t, err)

	// Verify state persisted across "crash"
	action, err := pm2.recoveryActionState.GetInconsistentRecoveryAction()
	require.NoError(t, err)
	require.NotNil(t, action)
	assert.Equal(t, RecoveryActionTypeNeedsRewind, action.ActionType)
	assert.Equal(t, "test emergency demote", action.Reason)
	assert.Equal(t, int64(123), action.ConsensusTerm)
	assert.False(t, action.SetTime.IsZero())
}

// TestRecoveryActionClearPersistence verifies that clearing recovery action
// removes the state file and survives restarts.
func TestRecoveryActionClearPersistence(t *testing.T) {
	ctx := context.Background()

	// Create first manager
	pm1 := newTestManager(t)

	// Initialize data directory structure
	initializeTestDataDir(t, pm1.multipooler.PoolerDir)

	lockCtx, err := pm1.actionLock.Acquire(ctx, "test")
	require.NoError(t, err)

	// Set recovery action
	err = pm1.recoveryActionState.SetRecoveryAction(
		lockCtx,
		RecoveryActionTypeNeedsRewind,
		"test",
		1,
	)
	require.NoError(t, err)

	// Clear recovery action
	err = pm1.recoveryActionState.ClearRecoveryAction(lockCtx, RecoveryActionTypeNeedsRewind)
	require.NoError(t, err)

	// Verify state file removed
	poolerDir := pm1.multipooler.PoolerDir
	statePath := recoveryStatePath(poolerDir)
	_, err = os.Stat(statePath)
	assert.True(t, os.IsNotExist(err), "recovery state file should not exist after clear")

	pm1.actionLock.Release(lockCtx)

	// Simulate crash: create new manager with same directory
	pm2 := newTestManager(t, func(pm *MultiPoolerManager) {
		pm.multipooler.PoolerDir = poolerDir
		pm.recoveryActionState = NewRecoveryActionState(poolerDir, pm.serviceID)
		pm.consensusState = NewConsensusState(poolerDir, pm.serviceID)
	})

	// Load state from disk
	err = pm2.recoveryActionState.Load()
	require.NoError(t, err)

	// Verify no recovery action after restart
	action, err := pm2.recoveryActionState.GetInconsistentRecoveryAction()
	require.NoError(t, err)
	assert.Nil(t, action)
}

// TestRecoveryActionSetRequiresActionLock verifies that SetRecoveryAction
// fails when called without the action lock held.
func TestRecoveryActionSetRequiresActionLock(t *testing.T) {
	ctx := context.Background()

	pm := newTestManager(t)

	// Try to set without lock - should fail
	err := pm.recoveryActionState.SetRecoveryAction(
		ctx,
		RecoveryActionTypeNeedsRewind,
		"test",
		1,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "action lock")
}

// TestRecoveryActionGetRequiresActionLock verifies that GetRecoveryAction
// fails when called without the action lock held.
func TestRecoveryActionGetRequiresActionLock(t *testing.T) {
	ctx := context.Background()

	pm := newTestManager(t)

	// Try to get without lock - should fail
	_, err := pm.recoveryActionState.GetRecoveryAction(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "action lock")
}

// TestRecoveryActionClearRequiresActionLock verifies that ClearRecoveryAction
// fails when called without the action lock held.
func TestRecoveryActionClearRequiresActionLock(t *testing.T) {
	ctx := context.Background()

	pm := newTestManager(t)

	// Try to clear without lock - should fail
	err := pm.recoveryActionState.ClearRecoveryAction(ctx, RecoveryActionTypeNeedsRewind)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "action lock")
}

// TestRecoveryActionGetInconsistentDoesNotRequireLock verifies that
// GetInconsistentRecoveryAction can be called without the action lock.
func TestRecoveryActionGetInconsistentDoesNotRequireLock(t *testing.T) {
	pm := newTestManager(t)

	// Should succeed without lock
	action, err := pm.recoveryActionState.GetInconsistentRecoveryAction()
	require.NoError(t, err)
	assert.Nil(t, action) // No action set yet
}

// TestRecoveryActionHasPendingDoesNotRequireLock verifies that
// HasPendingRecoveryAction can be called without the action lock.
func TestRecoveryActionHasPendingDoesNotRequireLock(t *testing.T) {
	pm := newTestManager(t)

	// Should succeed without lock
	hasPending := pm.recoveryActionState.HasPendingRecoveryAction()
	assert.False(t, hasPending)
}

// TestRecoveryActionClearIdempotent verifies that clearing a non-existent
// recovery action succeeds (idempotent operation).
func TestRecoveryActionClearIdempotent(t *testing.T) {
	ctx := context.Background()

	pm := newTestManager(t)

	lockCtx, err := pm.actionLock.Acquire(ctx, "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	// Clear when nothing exists - should succeed
	err = pm.recoveryActionState.ClearRecoveryAction(lockCtx, RecoveryActionTypeNeedsRewind)
	require.NoError(t, err)

	// Clear again - should still succeed
	err = pm.recoveryActionState.ClearRecoveryAction(lockCtx, RecoveryActionTypeNeedsRewind)
	require.NoError(t, err)
}

// TestRecoveryActionRejectsNoneType verifies that SetRecoveryAction
// rejects RecoveryActionTypeNone and requires using ClearRecoveryAction instead.
func TestRecoveryActionRejectsNoneType(t *testing.T) {
	ctx := context.Background()

	pm := newTestManager(t)

	lockCtx, err := pm.actionLock.Acquire(ctx, "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	// Try to set RecoveryActionTypeNone - should fail
	err = pm.recoveryActionState.SetRecoveryAction(
		lockCtx,
		RecoveryActionTypeNone,
		"test",
		1,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "use ClearRecoveryAction")
}

// TestRecoveryActionLoadNoFile verifies that loading when no state file
// exists initializes with nil (no action pending).
func TestRecoveryActionLoadNoFile(t *testing.T) {
	pm := newTestManager(t)

	// Load when no file exists
	err := pm.recoveryActionState.Load()
	require.NoError(t, err)

	// Verify no action
	action, err := pm.recoveryActionState.GetInconsistentRecoveryAction()
	require.NoError(t, err)
	assert.Nil(t, action)
}

// TestRecoveryActionGetReturnsDeepCopy verifies that GetRecoveryAction
// returns a copy that can be modified without affecting internal state.
func TestRecoveryActionGetReturnsDeepCopy(t *testing.T) {
	ctx := context.Background()

	pm := newTestManager(t)

	// Initialize data directory structure
	initializeTestDataDir(t, pm.multipooler.PoolerDir)

	lockCtx, err := pm.actionLock.Acquire(ctx, "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	// Set recovery action
	err = pm.recoveryActionState.SetRecoveryAction(
		lockCtx,
		RecoveryActionTypeNeedsRewind,
		"original reason",
		100,
	)
	require.NoError(t, err)

	// Get action and modify it
	action1, err := pm.recoveryActionState.GetRecoveryAction(lockCtx)
	require.NoError(t, err)
	require.NotNil(t, action1)
	originalReason := action1.Reason
	action1.Reason = "MODIFIED"
	action1.ConsensusTerm = 999

	// Get again and verify internal state wasn't affected
	action2, err := pm.recoveryActionState.GetRecoveryAction(lockCtx)
	require.NoError(t, err)
	require.NotNil(t, action2)
	assert.Equal(t, originalReason, action2.Reason)
	assert.Equal(t, int64(100), action2.ConsensusTerm)
}

// TestRecoveryActionStorageCorruptedJSON verifies behavior when the state file
// contains invalid JSON.
func TestRecoveryActionStorageCorruptedJSON(t *testing.T) {
	tmpDir := t.TempDir()

	// Initialize data directory structure
	dataDir := postgresDataDir(tmpDir)
	recoveryDir := filepath.Join(dataDir, "recovery")
	err := os.MkdirAll(recoveryDir, 0o755)
	require.NoError(t, err)

	// Write corrupted JSON
	statePath := filepath.Join(recoveryDir, "recovery_state.json")
	err = os.WriteFile(statePath, []byte("not valid json {{{"), 0o644)
	require.NoError(t, err)

	// Try to load - should fail with unmarshal error
	_, err = getRecoveryState(tmpDir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

// TestRecoveryActionHasPendingWhenSet verifies HasPendingRecoveryAction
// returns true when a recovery action is set.
func TestRecoveryActionHasPendingWhenSet(t *testing.T) {
	ctx := context.Background()

	pm := newTestManager(t)

	// Initialize data directory structure
	initializeTestDataDir(t, pm.multipooler.PoolerDir)

	// Initially no pending action
	assert.False(t, pm.recoveryActionState.HasPendingRecoveryAction())

	lockCtx, err := pm.actionLock.Acquire(ctx, "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	// Set recovery action
	err = pm.recoveryActionState.SetRecoveryAction(
		lockCtx,
		RecoveryActionTypeNeedsRewind,
		"test",
		1,
	)
	require.NoError(t, err)

	// Now should have pending action
	assert.True(t, pm.recoveryActionState.HasPendingRecoveryAction())
}

// TestRecoveryActionSetTimeRecorded verifies that SetTime is recorded
// and is recent.
func TestRecoveryActionSetTimeRecorded(t *testing.T) {
	ctx := context.Background()

	pm := newTestManager(t)

	// Initialize data directory structure
	initializeTestDataDir(t, pm.multipooler.PoolerDir)

	lockCtx, err := pm.actionLock.Acquire(ctx, "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	beforeSet := time.Now()

	err = pm.recoveryActionState.SetRecoveryAction(
		lockCtx,
		RecoveryActionTypeNeedsRewind,
		"test",
		1,
	)
	require.NoError(t, err)

	afterSet := time.Now()

	action, err := pm.recoveryActionState.GetRecoveryAction(lockCtx)
	require.NoError(t, err)
	require.NotNil(t, action)

	// Verify SetTime is within the expected range
	assert.False(t, action.SetTime.IsZero())
	assert.True(t, action.SetTime.After(beforeSet) || action.SetTime.Equal(beforeSet))
	assert.True(t, action.SetTime.Before(afterSet) || action.SetTime.Equal(afterSet))
}

// TestRecoveryActionClearTypeMismatch verifies that ClearRecoveryAction
// fails when the expected action type doesn't match the current action type.
// This is a critical safety feature to prevent clearing the wrong recovery action.
func TestRecoveryActionClearTypeMismatch(t *testing.T) {
	ctx := context.Background()

	pm := newTestManager(t)

	// Initialize data directory structure
	initializeTestDataDir(t, pm.multipooler.PoolerDir)

	lockCtx, err := pm.actionLock.Acquire(ctx, "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	// Set recovery action of type NeedsRewind
	err = pm.recoveryActionState.SetRecoveryAction(
		lockCtx,
		RecoveryActionTypeNeedsRewind,
		"test rewind action",
		1,
	)
	require.NoError(t, err)

	// Try to clear with wrong expected type (simulating a future recovery action type)
	// For now we use RecoveryActionTypeNone to test the mismatch logic
	// In the future when we have more types (e.g., NeedsPITR), this test will be even more relevant
	err = pm.recoveryActionState.ClearRecoveryAction(lockCtx, RecoveryActionTypeNone)
	require.Error(t, err, "should fail when expected type doesn't match current type")
	assert.Contains(t, err.Error(), "recovery action type mismatch")
	assert.Contains(t, err.Error(), "expected none")
	assert.Contains(t, err.Error(), "current is needs_rewind")

	// Verify the action was NOT cleared
	action, err := pm.recoveryActionState.GetRecoveryAction(lockCtx)
	require.NoError(t, err)
	require.NotNil(t, action, "action should still be set after failed clear")
	assert.Equal(t, RecoveryActionTypeNeedsRewind, action.ActionType)

	// Now clear with the CORRECT expected type - should succeed
	err = pm.recoveryActionState.ClearRecoveryAction(lockCtx, RecoveryActionTypeNeedsRewind)
	require.NoError(t, err, "should succeed when expected type matches current type")

	// Verify the action was cleared
	action, err = pm.recoveryActionState.GetRecoveryAction(lockCtx)
	require.NoError(t, err)
	assert.Nil(t, action, "action should be cleared after successful clear")
}

// TODO(endtoend): Add end-to-end tests that simulate crashes in the middle of flows:
// - EmergencyDemote sets recovery action, multipooler crashes before pg_rewind
// - Verify on restart that postgres is blocked from starting
// - Complete pg_rewind flow and verify recovery action is cleared
// - Verify postgres can start after recovery action cleared
