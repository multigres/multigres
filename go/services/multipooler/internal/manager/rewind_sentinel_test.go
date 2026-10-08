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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
	backupengine "github.com/multigres/multigres/go/services/multipooler/internal/manager/backup"
)

// newSentinelTestManager builds a manager whose pooler directory is a writable
// temp dir, so the on-disk rewind-sentinel helpers can be exercised.
func newSentinelTestManager(t *testing.T) *MultipoolerManager {
	t.Helper()
	return newTestManager(t, withRecord(newRecordFromProto(&clustermetadatapb.Multipooler{
		Type:          clustermetadatapb.PoolerType_REPLICA,
		ServingStatus: clustermetadatapb.PoolerServingStatus_DISABLED,
		PoolerDir:     t.TempDir(),
	})))
}

func TestRewindSentinel_RoundTrip(t *testing.T) {
	pm := newSentinelTestManager(t)

	present, err := pm.hasRewindSentinel()
	require.NoError(t, err)
	assert.False(t, present, "no sentinel initially")

	require.NoError(t, pm.writeRewindSentinel())
	present, err = pm.hasRewindSentinel()
	require.NoError(t, err)
	assert.True(t, present, "sentinel present after write")

	// Writing again is idempotent.
	require.NoError(t, pm.writeRewindSentinel())

	require.NoError(t, pm.removeRewindSentinel())
	present, err = pm.hasRewindSentinel()
	require.NoError(t, err)
	assert.False(t, present, "sentinel gone after remove")

	// Removing a missing sentinel is not an error.
	require.NoError(t, pm.removeRewindSentinel())
}

// TestDiscoverPostgresState_RewindSentinel verifies discoverPostgresState surfaces
// the on-disk rewind sentinel, the durable signal the monitor gates on.
func TestDiscoverPostgresState_RewindSentinel(t *testing.T) {
	ctx := t.Context()

	pm := NewTestMultipoolerManager(t)
	pm.pgctldClient = &mockPgctldClient{
		statusResponse: &pgctldpb.StatusResponse{Status: pgctldpb.ServerStatus_STOPPED},
	}

	state, err := pm.discoverPostgresState(ctx)
	require.NoError(t, err)
	assert.False(t, state.rewindSentinelPresent, "no rewind sentinel initially")

	require.NoError(t, pm.writeRewindSentinel())

	state, err = pm.discoverPostgresState(ctx)
	require.NoError(t, err)
	assert.True(t, state.rewindSentinelPresent, "rewind sentinel surfaced once written")
}

// TestDetermineRemedialAction_RewindSentinelRearmsDivergence verifies that a
// rewind sentinel re-arms the (in-memory, restart-lost) suspectedDivergence flag,
// routing the node through the rewind-repair path rather than a blind start.
func TestDetermineRemedialAction_RewindSentinelRearmsDivergence(t *testing.T) {
	ctx := t.Context()
	pm := newTestManager(t)

	// Sentinel present, divergence not yet suspected: re-arm.
	state := postgresState{pgctldAvailable: true, rewindSentinelPresent: true}
	assert.Equal(t, remedialActionMarkRewindInterrupted, pm.determineRemedialAction(ctx, state),
		"a rewind sentinel with divergence unset must re-arm suspected divergence")

	// Once divergence is already suspected, the re-arm does not fire again (it is a
	// one-shot per incident; the rewind path takes over).
	withLock(t, pm, func(lockCtx context.Context) {
		_, err := pm.consensusMgr.SetSuspectedDivergence(lockCtx, true)
		require.NoError(t, err)
	})
	assert.NotEqual(t, remedialActionMarkRewindInterrupted, pm.determineRemedialAction(ctx, state),
		"re-arm must not fire once suspected divergence is already set")
}

// TestTakeRemedialAction_MarkRewindInterruptedSetsDivergence verifies the action
// actually sets the flag the rewind path gates on.
func TestTakeRemedialAction_MarkRewindInterruptedSetsDivergence(t *testing.T) {
	pm := newTestManager(t)
	require.False(t, pm.consensusMgr.SuspectedDivergence())

	withLock(t, pm, func(ctx context.Context) {
		require.NoError(t, pm.takeRemedialAction(ctx, remedialActionMarkRewindInterrupted, postgresState{}))
	})

	assert.True(t, pm.consensusMgr.SuspectedDivergence(),
		"marking an interrupted rewind must set suspected divergence")
}

// TestTrackRecoveryOutcome_RewindSentinelCountsWhileRunning is the core of the
// fix: a half-rewound node that starts into recovery and then waits forever for
// unreachable WAL reports postgresRunning=true. Without the sentinel that would
// reset the unrecoverable streak every tick and the node would spin forever. With
// the sentinel present, failed rewind-repair attempts must keep counting so the
// node is eventually quarantined for replacement.
func TestTrackRecoveryOutcome_RewindSentinelCountsWhileRunning(t *testing.T) {
	pm, clock := newQuarantineTestManager(t, 30*time.Second)

	// "Running but mid-rewind": postgresRunning=true AND the sentinel is present.
	running := postgresState{postgresRunning: true, rewindSentinelPresent: true}
	failRewind := func(ctx context.Context) {
		pm.trackRecoveryOutcome(ctx, remedialActionRewindToLeader, running, assert.AnError)
	}

	withLock(t, pm, func(ctx context.Context) {
		failRewind(ctx) // attempt 1, elapsed 0
		assert.Equal(t, 1, pm.unrecoverableFailedAttempts,
			"a running-but-mid-rewind node must not reset the streak")
		clock.advance(15 * time.Second)
		failRewind(ctx) // attempt 2, elapsed 15s
		clock.advance(20 * time.Second)
		failRewind(ctx) // attempt 3 (floor met), elapsed 35s (>= 30s)
	})

	quarantined, reason, _ := quarantineState(pm)
	assert.True(t, quarantined, "an unrecoverable interrupted rewind must quarantine despite postgres appearing to run")
	assert.NotEmpty(t, reason)
}

// TestRewindDetachOutlivesCallerCancellation verifies the point-of-no-return
// contract used by restartAsStandbyLocked: once the destructive stop -> pg_rewind
// -> restart sequence is detached from the caller's context (an incoming
// SetPrimary RPC that carries multiorch's action deadline, e.g. FixReplication's
// 45s), the caller's cancellation neither cancels the operation nor drops the
// action lock it holds. That is what lets a started pg_rewind run to completion
// rather than be SIGKILLed mid-write when the RPC times out.
func TestRewindDetachOutlivesCallerCancellation(t *testing.T) {
	pm := newTestManager(t)

	lockCtx, err := pm.actionLock.Acquire(t.Context(), "test")
	require.NoError(t, err)
	defer pm.actionLock.Release(lockCtx)

	// The caller's RPC context (carrying a deadline in production), derived from
	// the locked context the way an incoming RPC handler holds the action lock.
	callerCtx, cancel := context.WithCancel(lockCtx)

	// The exact detach restartAsStandbyLocked performs at the point of no return.
	opCtx, opCancel := pm.detachRewindOpContext(callerCtx)
	defer opCancel()

	// The detached sequence is bounded by its own backstop deadline.
	if _, ok := opCtx.Deadline(); !ok {
		t.Fatal("detached rewind context should carry a backstop deadline")
	}

	// The caller's deadline fires.
	cancel()
	require.Error(t, callerCtx.Err(), "precondition: caller context is cancelled")

	// The detached operation continues unaffected...
	assert.NoError(t, opCtx.Err(),
		"a started rewind must not be cancelled when the caller's RPC deadline fires")
	// ...and still holds the action lock, so pgctld's protected Stop/Restart/PgRewind
	// calls (which assert ownership) still succeed and the monitor stays blocked.
	assert.NoError(t, actionlock.AssertActionLockHeld(opCtx),
		"the detached rewind context must keep proving action-lock ownership")
}

// TestRunPgRewind_SentinelBracket verifies runPgRewind writes the rewind sentinel
// before the mutating pg_rewind on the divergence path, and writes nothing when
// there is no divergence (so a no-op dry-run leaves no stale sentinel).
func TestRunPgRewind_SentinelBracket(t *testing.T) {
	t.Run("writes sentinel when servers diverged", func(t *testing.T) {
		pm := newSentinelTestManager(t)
		pm.pgctldClient = &mockPgctldClient{
			pgRewindResponse: &pgctldpb.PgRewindResponse{Output: "servers diverged at 0/5000000 on timeline 2"},
		}
		performed, err := pm.runPgRewind(t.Context(), "leader", 5432)
		require.NoError(t, err)
		assert.True(t, performed, "a diverged rewind is performed")
		present, err := pm.hasRewindSentinel()
		require.NoError(t, err)
		assert.True(t, present, "sentinel must be written before the mutating pg_rewind")
	})

	t.Run("no sentinel when not diverged", func(t *testing.T) {
		pm := newSentinelTestManager(t)
		pm.pgctldClient = &mockPgctldClient{
			pgRewindResponse: &pgctldpb.PgRewindResponse{Output: "no rewind required"},
		}
		performed, err := pm.runPgRewind(t.Context(), "leader", 5432)
		require.NoError(t, err)
		assert.False(t, performed, "no divergence means no rewind")
		present, err := pm.hasRewindSentinel()
		require.NoError(t, err)
		assert.False(t, present, "no sentinel written when there is no divergence")
	})
}

// TestRunPgRewind_RestoreTargetWAL checks that both pg_rewind runs get
// --restore-target-wal and a temporary restore_command that is always removed,
// and that without a pgbackrest config no -c is passed.
func TestRunPgRewind_RestoreTargetWAL(t *testing.T) {
	const diverged = "servers diverged at 0/5000000 on timeline 2"

	setup := func(t *testing.T, withBackupConfig bool) (*MultipoolerManager, *mockPgctldClient, string) {
		t.Helper()
		pgdata := t.TempDir()
		t.Setenv(constants.PgDataDirEnvVar, pgdata)
		autoConf := filepath.Join(pgdata, "postgresql.auto.conf")
		require.NoError(t, os.WriteFile(autoConf, []byte("primary_conninfo = 'host=x'\n"), 0o600))

		pm := newSentinelTestManager(t)
		pm.backup = backupengine.NewEngine(pm.logger, pm.runLongCommand, pm.record, backupengine.Settings{})
		if withBackupConfig {
			pm.backup.SetConfigPath("/etc/pgbackrest.conf")
		}
		return pm, &mockPgctldClient{pgRewindResponse: &pgctldpb.PgRewindResponse{Output: diverged}}, autoConf
	}
	read := func(t *testing.T, path string) string {
		t.Helper()
		b, err := os.ReadFile(path)
		require.NoError(t, err)
		return string(b)
	}

	t.Run("passes -c and sets restore_command for dry-run and real run", func(t *testing.T) {
		pm, client, autoConf := setup(t, true)
		var confAtCall []string
		client.pgRewindOnCall = func(*pgctldpb.PgRewindRequest) { confAtCall = append(confAtCall, read(t, autoConf)) }
		pm.pgctldClient = client

		performed, err := pm.runPgRewind(t.Context(), "leader", 5432)
		require.NoError(t, err)
		assert.True(t, performed)

		require.Len(t, client.pgRewindReqs, 2)
		assert.True(t, client.pgRewindReqs[0].DryRun)
		assert.Equal(t, []string{"--restore-target-wal"}, client.pgRewindReqs[0].ExtraArgs)
		assert.False(t, client.pgRewindReqs[1].DryRun)
		assert.Equal(t, []string{"-R", "--restore-target-wal"}, client.pgRewindReqs[1].ExtraArgs)

		for _, conf := range confAtCall {
			assert.Contains(t, conf, "restore_command = ")
			assert.Contains(t, conf, "pgctld restore-wrapper")
			assert.Contains(t, conf, "archive-get")
			assert.Contains(t, conf, constants.RestoreCommandPIDFile)
		}
		assert.Equal(t, "primary_conninfo = 'host=x'\n", read(t, autoConf), "restore_command must be removed after the rewind")
	})

	t.Run("removes restore_command when pg_rewind fails", func(t *testing.T) {
		pm, client, autoConf := setup(t, true)
		client.pgRewindError = errors.New("pg_rewind boom")
		pm.pgctldClient = client

		_, err := pm.runPgRewind(t.Context(), "leader", 5432)
		require.Error(t, err)
		assert.False(t, strings.Contains(read(t, autoConf), "restore_command"))
	})

	t.Run("removes restore_command when dry-run finds no divergence", func(t *testing.T) {
		pm, client, autoConf := setup(t, true)
		client.pgRewindResponse = &pgctldpb.PgRewindResponse{Output: "no rewind required"}
		pm.pgctldClient = client

		performed, err := pm.runPgRewind(t.Context(), "leader", 5432)
		require.NoError(t, err)
		assert.False(t, performed)
		require.Len(t, client.pgRewindReqs, 1)
		assert.Equal(t, []string{"--restore-target-wal"}, client.pgRewindReqs[0].ExtraArgs)
		assert.NotContains(t, read(t, autoConf), "restore_command")
	})

	t.Run("falls back to no -c without a pgbackrest config", func(t *testing.T) {
		pm, client, autoConf := setup(t, false)
		var confAtCall []string
		client.pgRewindOnCall = func(*pgctldpb.PgRewindRequest) { confAtCall = append(confAtCall, read(t, autoConf)) }
		pm.pgctldClient = client

		performed, err := pm.runPgRewind(t.Context(), "leader", 5432)
		require.NoError(t, err)
		assert.True(t, performed)

		require.Len(t, client.pgRewindReqs, 2)
		assert.Empty(t, client.pgRewindReqs[0].ExtraArgs)
		assert.Equal(t, []string{"-R"}, client.pgRewindReqs[1].ExtraArgs)
		for _, conf := range confAtCall {
			assert.NotContains(t, conf, "restore_command")
		}
	})
}

// TestRewindSentinel_ErrorPaths covers the sentinel helpers' error branches so a
// failure to record/clear the marker is surfaced rather than silently ignored.
func TestRewindSentinel_ErrorPaths(t *testing.T) {
	// fsyncPath surfaces an open error for a path that does not exist.
	require.Error(t, fsyncPath(filepath.Join(t.TempDir(), "does-not-exist")))

	// writeRewindSentinel surfaces a write error when the pooler dir is missing.
	missingDir := newTestManager(t, withRecord(newRecordFromProto(&clustermetadatapb.Multipooler{
		Type:      clustermetadatapb.PoolerType_REPLICA,
		PoolerDir: filepath.Join(t.TempDir(), "no", "such", "dir"),
	})))
	require.Error(t, missingDir.writeRewindSentinel(), "write to a nonexistent pooler dir should error")

	// removeRewindSentinel surfaces a non-NotExist error: a non-empty directory at
	// the sentinel path cannot be removed by os.Remove.
	pm := newSentinelTestManager(t)
	sentinelPath := pm.rewindSentinelPath()
	require.NoError(t, os.Mkdir(sentinelPath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(sentinelPath, "child"), []byte("x"), 0o644))
	require.Error(t, pm.removeRewindSentinel(), "removing a non-empty directory at the sentinel path should error")
}

// TestTrackRecoveryOutcome_ResetsWhenRunningWithoutSentinel guards the boundary:
// a genuinely healthy running node (no sentinel) still breaks the streak.
func TestTrackRecoveryOutcome_ResetsWhenRunningWithoutSentinel(t *testing.T) {
	pm, clock := newQuarantineTestManager(t, 30*time.Second)

	withLock(t, pm, func(ctx context.Context) {
		pm.trackRecoveryOutcome(ctx, remedialActionRewindToLeader,
			postgresState{postgresRunning: true, rewindSentinelPresent: true}, assert.AnError)
		require.Equal(t, 1, pm.unrecoverableFailedAttempts)

		clock.advance(time.Second)
		// A clean, sentinel-free running node: the incident is over.
		pm.trackRecoveryOutcome(ctx, remedialActionNone,
			postgresState{postgresRunning: true}, nil)
		assert.Equal(t, 0, pm.unrecoverableFailedAttempts,
			"a healthy running node without a sentinel resets the streak")
	})
}
