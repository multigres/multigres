// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package handler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// --- GatewayManagedVariable snapshot stack ---

func TestGatewayManagedVariable_SnapshotRestoreAllFields(t *testing.T) {
	v := NewGatewayManagedVariable(30 * time.Second)
	v.Set(5 * time.Second)
	v.SetLocal(100 * time.Millisecond)

	v.Snapshot()

	// Mutate after snapshot.
	v.Set(10 * time.Second)
	v.SetLocal(50 * time.Millisecond)
	require.Equal(t, 50*time.Millisecond, v.GetEffective())

	v.RestoreFromDepth(0)
	require.True(t, v.IsSet())
	require.True(t, v.IsLocalSet())
	require.Equal(t, 100*time.Millisecond, v.GetEffective(), "local should win")

	// Snapshot frame at depth 0 stays after RestoreFromDepth.
	require.Equal(t, 1, v.SnapshotDepth())
}

func TestGatewayManagedVariable_RestoreUnsetIsSetAndLocal(t *testing.T) {
	v := NewGatewayManagedVariable(30 * time.Second)

	// Snapshot the pristine state (no overrides).
	v.Snapshot()

	v.Set(5 * time.Second)
	v.SetLocal(100 * time.Millisecond)
	require.Equal(t, 100*time.Millisecond, v.GetEffective())

	v.RestoreFromDepth(0)
	require.False(t, v.IsSet())
	require.False(t, v.IsLocalSet())
	require.Equal(t, 30*time.Second, v.GetEffective(), "default should be in effect")
}

func TestGatewayManagedVariable_NestedSnapshotsRestoreToDepth(t *testing.T) {
	v := NewGatewayManagedVariable(30 * time.Second)
	v.Set(1 * time.Second) // depth 0 base

	v.Snapshot() // depth 0 captures Set=1s
	v.Set(2 * time.Second)

	v.Snapshot() // depth 1 captures Set=2s
	v.Set(3 * time.Second)

	v.Snapshot() // depth 2 captures Set=3s
	v.Set(4 * time.Second)

	require.Equal(t, 3, v.SnapshotDepth())

	// Restore from depth 1 (which captured 2s). Depth 2 frame must be popped.
	v.RestoreFromDepth(1)
	require.Equal(t, 2*time.Second, v.GetEffective())
	require.Equal(t, 2, v.SnapshotDepth(), "depth 0 and 1 frames remain; 2 is gone")
}

func TestGatewayManagedVariable_PopFromDoesNotChangeValues(t *testing.T) {
	v := NewGatewayManagedVariable(30 * time.Second)
	v.Set(1 * time.Second)
	v.Snapshot()
	v.Set(2 * time.Second)
	v.Snapshot()
	v.Set(3 * time.Second)

	v.PopFrom(1) // drop frame 1 and above; current value (3s) untouched
	require.Equal(t, 3*time.Second, v.GetEffective())
	require.Equal(t, 1, v.SnapshotDepth())
}

func TestGatewayManagedVariable_ClearSnapshots(t *testing.T) {
	v := NewGatewayManagedVariable(30 * time.Second)
	v.Snapshot()
	v.Snapshot()
	require.Equal(t, 2, v.SnapshotDepth())
	v.ClearSnapshots()
	require.Equal(t, 0, v.SnapshotDepth())
}

// --- MultiGatewayConnectionState transaction lifecycle ---

func TestConnectionState_BeginRollback_RevertsSessionSettings(t *testing.T) {
	s := NewMultiGatewayConnectionState()
	s.SetSessionVariable("datestyle", "ISO, MDY")

	s.BeginTransaction()
	s.SetSessionVariable("datestyle", "German")

	s.RollbackTransaction()

	v, ok := s.GetSessionVariable("datestyle")
	require.True(t, ok)
	require.Equal(t, "ISO, MDY", v, "rollback must revert SessionSettings to pre-BEGIN")
	require.Equal(t, 0, s.SavepointDepth())
}

func TestConnectionState_BeginCommit_PersistsNonLocalSet(t *testing.T) {
	s := NewMultiGatewayConnectionState()
	s.BeginTransaction()
	s.SetSessionVariable("datestyle", "German")
	s.CommitTransaction()

	v, ok := s.GetSessionVariable("datestyle")
	require.True(t, ok)
	require.Equal(t, "German", v, "commit must keep current values")
	require.Equal(t, 0, s.SavepointDepth())
}

func TestConnectionState_BeginRollback_RevertsResetToOriginal(t *testing.T) {
	s := NewMultiGatewayConnectionState()
	s.SetSessionVariable("search_path", "public")

	s.BeginTransaction()
	s.ResetSessionVariable("search_path")
	_, ok := s.GetSessionVariable("search_path")
	require.False(t, ok, "RESET inside txn removes the variable")

	s.RollbackTransaction()

	v, ok := s.GetSessionVariable("search_path")
	require.True(t, ok, "rollback re-adds the RESET-removed variable")
	require.Equal(t, "public", v)
}

func TestConnectionState_SavepointRollbackTo_RevertsSessionSettings(t *testing.T) {
	s := NewMultiGatewayConnectionState()
	s.BeginTransaction()
	s.SetSessionVariable("datestyle", "MDY")

	s.PushSavepoint("sp")
	s.SetSessionVariable("datestyle", "German")

	s.RollbackToSavepoint("sp")

	v, _ := s.GetSessionVariable("datestyle")
	require.Equal(t, "MDY", v, "rollback to sp must revert to pre-savepoint value")
	require.Equal(t, 2, s.SavepointDepth(), "BEGIN frame and sp frame remain")
}

func TestConnectionState_SavepointRelease_KeepsCurrent(t *testing.T) {
	s := NewMultiGatewayConnectionState()
	s.BeginTransaction()
	s.PushSavepoint("sp")
	s.SetSessionVariable("datestyle", "German")
	s.ReleaseSavepoint("sp")

	v, _ := s.GetSessionVariable("datestyle")
	require.Equal(t, "German", v, "RELEASE merges sub-tx changes into outer scope")
	require.Equal(t, 1, s.SavepointDepth(), "BEGIN frame remains; sp frame gone")
}

func TestConnectionState_NestedSavepoints_RollbackToOuter(t *testing.T) {
	s := NewMultiGatewayConnectionState()
	s.BeginTransaction()
	s.SetSessionVariable("datestyle", "MDY")

	s.PushSavepoint("a")
	s.SetSessionVariable("datestyle", "German")

	s.PushSavepoint("b")
	s.SetSessionVariable("datestyle", "SQL")

	s.RollbackToSavepoint("a")

	v, _ := s.GetSessionVariable("datestyle")
	require.Equal(t, "MDY", v, "rollback to a discards b's changes too")
	require.Equal(t, 2, s.SavepointDepth(), "BEGIN frame and a frame remain")
}

func TestConnectionState_RollbackToSavepoint_MultipleTimes(t *testing.T) {
	s := NewMultiGatewayConnectionState()
	s.BeginTransaction()
	s.SetSessionVariable("datestyle", "MDY")
	s.PushSavepoint("a")

	// First mutation + rollback.
	s.SetSessionVariable("datestyle", "German")
	s.RollbackToSavepoint("a")
	v, _ := s.GetSessionVariable("datestyle")
	require.Equal(t, "MDY", v)

	// Second mutation + rollback should still work — `a` stays active.
	s.SetSessionVariable("datestyle", "SQL")
	s.RollbackToSavepoint("a")
	v, _ = s.GetSessionVariable("datestyle")
	require.Equal(t, "MDY", v)
}

func TestConnectionState_StatementTimeoutRollbackToSavepoint(t *testing.T) {
	s := NewMultiGatewayConnectionState()
	s.InitStatementTimeout(30 * time.Second)
	s.BeginTransaction()
	s.SetLocalStatementTimeout(100 * time.Millisecond)

	s.PushSavepoint("sp")
	s.SetLocalStatementTimeout(50 * time.Millisecond)
	require.Equal(t, 50*time.Millisecond, s.GetStatementTimeout())

	s.RollbackToSavepoint("sp")
	require.Equal(t, 100*time.Millisecond, s.GetStatementTimeout(),
		"ROLLBACK TO sp must restore the pre-savepoint LOCAL value")
}

func TestConnectionState_StatementTimeoutCommitClearsLocal(t *testing.T) {
	s := NewMultiGatewayConnectionState()
	s.InitStatementTimeout(30 * time.Second)
	s.BeginTransaction()
	s.SetStatementTimeout(5 * time.Second)
	s.SetLocalStatementTimeout(100 * time.Millisecond)
	require.Equal(t, 100*time.Millisecond, s.GetStatementTimeout())

	s.CommitTransaction()

	require.Equal(t, 5*time.Second, s.GetStatementTimeout(),
		"after COMMIT, session-level SET persists; LOCAL is cleared")
}

func TestConnectionState_StatementTimeoutRollbackRevertsNonLocal(t *testing.T) {
	s := NewMultiGatewayConnectionState()
	s.InitStatementTimeout(30 * time.Second)
	s.SetStatementTimeout(5 * time.Second)

	s.BeginTransaction()
	s.SetStatementTimeout(10 * time.Second)
	require.Equal(t, 10*time.Second, s.GetStatementTimeout())

	s.RollbackTransaction()

	require.Equal(t, 5*time.Second, s.GetStatementTimeout(),
		"ROLLBACK must revert non-LOCAL SET to pre-BEGIN session value")
}

func TestConnectionState_RollbackWithoutBegin_NoOp(t *testing.T) {
	s := NewMultiGatewayConnectionState()
	s.InitStatementTimeout(30 * time.Second)
	s.SetLocalStatementTimeout(100 * time.Millisecond)

	// Rollback without a prior BEGIN: should not panic, should clear LOCAL.
	s.RollbackTransaction()
	require.Equal(t, 30*time.Second, s.GetStatementTimeout(),
		"defensive: ResetLocal on rollback without active txn")
	require.Equal(t, 0, s.SavepointDepth())
}

func TestConnectionState_BeginIsIdempotent(t *testing.T) {
	s := NewMultiGatewayConnectionState()
	s.BeginTransaction()
	s.BeginTransaction()
	require.Equal(t, 1, s.SavepointDepth(),
		"nested BEGIN must not push a duplicate frame (PG warns 'transaction in progress')")
}

func TestConnectionState_RollbackToUnknown_NoOp(t *testing.T) {
	s := NewMultiGatewayConnectionState()
	s.BeginTransaction()
	s.SetSessionVariable("datestyle", "MDY")
	s.PushSavepoint("a")
	s.SetSessionVariable("datestyle", "German")

	// Backend would have errored before reaching us; defend gracefully anyway.
	s.RollbackToSavepoint("nonexistent")

	v, _ := s.GetSessionVariable("datestyle")
	require.Equal(t, "German", v, "no-op preserves current state")
	require.Equal(t, 2, s.SavepointDepth())
}

// If a SAVEPOINT-style frame somehow lands on the stack outside a
// transaction (e.g. driver issued SAVEPOINT before BEGIN, or implicit-txn
// edge case), BeginTransaction must discard the stale frame and start a
// fresh BEGIN-level frame so a later ROLLBACK reverts to the pre-BEGIN
// value, not the pre-stale-savepoint value.
func TestConnectionState_BeginAfterStaleSavepointFrame_DiscardsStale(t *testing.T) {
	s := NewMultiGatewayConnectionState()
	s.SetSessionVariable("datestyle", "ISO, MDY")

	// Simulate a stray savepoint frame outside any transaction.
	s.PushSavepoint("stale")
	require.Equal(t, 1, s.SavepointDepth())

	// Mutate after the stale frame but before BEGIN.
	s.SetSessionVariable("datestyle", "German")

	s.BeginTransaction()
	require.Equal(t, 1, s.SavepointDepth(), "stale frame must be discarded; only BEGIN-level frame remains")
	require.Equal(t, 1, s.statementTimeout.SnapshotDepth(), "variable snapshot stack stays in lockstep")

	// SET inside the txn, then ROLLBACK.
	s.SetSessionVariable("datestyle", "Postgres")
	s.RollbackTransaction()

	// Must revert to pre-BEGIN value ("German"), not pre-stale-savepoint ("ISO, MDY").
	v, ok := s.GetSessionVariable("datestyle")
	require.True(t, ok)
	require.Equal(t, "German", v, "rollback must revert to value at BEGIN, not at stale savepoint")
	require.Equal(t, 0, s.SavepointDepth())
}

// Genuine nested BEGIN must remain a no-op so we don't lose the original
// BEGIN-level snapshot.
func TestConnectionState_NestedBegin_PreservesOriginalFrame(t *testing.T) {
	s := NewMultiGatewayConnectionState()
	s.SetSessionVariable("datestyle", "ISO, MDY")

	s.BeginTransaction()
	s.SetSessionVariable("datestyle", "German")

	s.BeginTransaction() // PG would emit a warning; we must not lose the frame.
	require.Equal(t, 1, s.SavepointDepth())

	s.RollbackTransaction()
	v, _ := s.GetSessionVariable("datestyle")
	require.Equal(t, "ISO, MDY", v, "rollback after nested BEGIN must still revert to original pre-BEGIN value")
}
