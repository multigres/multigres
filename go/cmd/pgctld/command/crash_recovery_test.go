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

package command

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/services/pgctld"
	"github.com/multigres/multigres/go/tools/retry"
)

// testPgCtldService builds a minimal PgCtldService for exercising crash-recovery
// helpers directly, without the full NewPgCtldService setup (ports, pgbackrest, etc.)
// that production callers need but these unit tests don't.
func testPgCtldService(dataDir string) *PgCtldService {
	return &PgCtldService{
		logger:   testLogger(),
		pgConfig: &pgctld.PostgresCtlConfig{PostgresDataDir: dataDir},
	}
}

// fastRetry returns a retry.Retry whose delays are short enough to make tests
// effectively instant while still exercising the iterator-driven control flow.
func fastRetry() *retry.Retry {
	return retry.New(time.Millisecond, time.Millisecond)
}

// TestPostgresAlreadyRunningPattern verifies the regex pattern matches the actual error
func TestPostgresAlreadyRunningPattern(t *testing.T) {
	testCases := []struct {
		name     string
		output   string
		expected bool
	}{
		{
			name:     "actual postgres error",
			output:   `FATAL:  lock file "postmaster.pid" already exists`,
			expected: true,
		},
		{
			name:     "full error with hint",
			output:   `FATAL:  lock file "postmaster.pid" already exists\nHINT:  Is another postmaster (PID 12345) running in data directory "/data"?`,
			expected: true,
		},
		{
			name:     "other lock file",
			output:   `FATAL:  lock file "other.lock" already exists`,
			expected: true,
		},
		{
			name:     "different error",
			output:   `FATAL:  database system is in recovery mode`,
			expected: false,
		},
		{
			name:     "similar but not exact",
			output:   `lock file is missing`,
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := postgresAlreadyRunningPattern.MatchString(tc.output)
			if result != tc.expected {
				t.Errorf("Pattern match = %v, want %v for output: %q",
					result, tc.expected, tc.output)
			}
		})
	}
}

// lockHeldOutput is the FATAL message Postgres emits (from both `postgres
// --single` and `pg_ctl start`) when the postmaster.pid lock is still held —
// both during the orphan-cleanup window after a crash and when postgres is
// genuinely running.
var lockHeldOutput = []byte(`FATAL:  lock file "postmaster.pid" already exists
HINT:  Is another postmaster (PID 12345) running in data directory "/data"?`)

// TestRetryWhileLockHeld_RetriesDuringOrphanCleanupWindow simulates a postmaster
// crash where orphaned workers keep the lock file held for the first few attempts
// and then release it. The primitive should succeed once the lock clears.
// runCrashRecoveryAttempts and startPostgreSQLWithConfig both delegate directly to
// this loop, so its retry mechanics are tested once here rather than through each
// of those thin, caller-specific wrappers.
func TestRetryWhileLockHeld_RetriesDuringOrphanCleanupWindow(t *testing.T) {
	const holdAttempts = 3
	s := testPgCtldService("")

	calls := 0
	run := func(ctx context.Context) ([]byte, error) {
		calls++
		if calls <= holdAttempts {
			return lockHeldOutput, errors.New("exit status 1")
		}
		return []byte("recovery complete"), nil
	}

	output, err := s.retryWhileLockHeld(context.Background(), run, constants.OrphanCleanupMaxAttempts, fastRetry())
	require.NoError(t, err)
	assert.Equal(t, holdAttempts+1, calls,
		"run should be retried until the lock clears, then succeed")
	assert.Equal(t, "recovery complete", string(output))
}

// TestRetryWhileLockHeld_FirstAttemptSucceeds covers the common case: no stale
// lock, no retries.
func TestRetryWhileLockHeld_FirstAttemptSucceeds(t *testing.T) {
	s := testPgCtldService("")
	calls := 0
	run := func(ctx context.Context) ([]byte, error) {
		calls++
		return []byte("recovery complete"), nil
	}

	_, err := s.retryWhileLockHeld(context.Background(), run, constants.OrphanCleanupMaxAttempts, fastRetry())
	require.NoError(t, err)
	assert.Equal(t, 1, calls)
}

// TestRetryWhileLockHeld_NonLockErrorReturnsImmediately verifies that failures
// unrelated to the orphan-cleanup race (e.g. a genuine config error) fail fast
// and do not consume the retry budget.
func TestRetryWhileLockHeld_NonLockErrorReturnsImmediately(t *testing.T) {
	s := testPgCtldService("")
	calls := 0
	run := func(ctx context.Context) ([]byte, error) {
		calls++
		return []byte("FATAL:  could not access data directory"), errors.New("exit status 1")
	}

	_, err := s.retryWhileLockHeld(context.Background(), run, constants.OrphanCleanupMaxAttempts, fastRetry())
	require.Error(t, err)
	assert.Equal(t, 1, calls, "non-lock errors must not be retried")
}

// TestRetryWhileLockHeld_BudgetExhaustedReturnsRealError verifies that once the
// retry budget runs out, the primitive returns the last attempt's real output
// and error untouched. It does not decide on the caller's behalf whether a
// still-held lock means "give up" (a plain start) or "must already be running"
// (single-user crash recovery, which layers that interpretation on top in
// runCrashRecoveryAttempts).
func TestRetryWhileLockHeld_BudgetExhaustedReturnsRealError(t *testing.T) {
	s := testPgCtldService("")
	calls := 0
	run := func(ctx context.Context) ([]byte, error) {
		calls++
		return lockHeldOutput, errors.New("exit status 1")
	}

	output, err := s.retryWhileLockHeld(context.Background(), run, constants.OrphanCleanupMaxAttempts, fastRetry())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exit status 1")
	assert.Equal(t, string(lockHeldOutput), string(output))
	assert.Equal(t, constants.OrphanCleanupMaxAttempts, calls,
		"run should be retried up to the max-attempts bound")
}

// TestRetryWhileLockHeld_ContextCancelledDuringBackoff verifies that a cancelled
// context aborts the retry loop without further run invocations.
func TestRetryWhileLockHeld_ContextCancelledDuringBackoff(t *testing.T) {
	s := testPgCtldService("")
	ctx, cancel := context.WithCancel(context.Background())

	calls := 0
	run := func(ctx context.Context) ([]byte, error) {
		calls++
		cancel()
		return lockHeldOutput, errors.New("exit status 1")
	}

	_, err := s.retryWhileLockHeld(ctx, run, constants.OrphanCleanupMaxAttempts, retry.New(time.Hour, time.Hour))
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, calls)
}

// TestRunCrashRecovery_LockNeverClearsReturnsNil locks in the historical behavior:
// if the lock is held for the full retry window, treat postgres as already running
// and return nil rather than surfacing an error.
func TestRunCrashRecovery_LockNeverClearsReturnsNil(t *testing.T) {
	calls := 0
	s := testPgCtldService("")
	runner := func(ctx context.Context) ([]byte, error) {
		calls++
		return lockHeldOutput, errors.New("exit status 1")
	}

	err := s.runCrashRecoveryAttempts(context.Background(), runner, fastRetry())
	require.NoError(t, err)
	assert.Equal(t, constants.OrphanCleanupMaxAttempts, calls,
		"runner should be retried up to the max-attempts bound")
}

// TestRunCrashRecovery_FirstAttemptSucceeds covers the cleanly-shut-down hot path:
// no retries, no sleeps.
func TestRunCrashRecovery_FirstAttemptSucceeds(t *testing.T) {
	calls := 0
	s := testPgCtldService("")
	runner := func(ctx context.Context) ([]byte, error) {
		calls++
		return []byte("recovery complete"), nil
	}

	err := s.runCrashRecoveryAttempts(context.Background(), runner, fastRetry())
	require.NoError(t, err)
	assert.Equal(t, 1, calls)
}

// TestRunCrashRecovery_NonLockErrorReturnsImmediately verifies the wrapper's
// own contribution beyond the shared retry loop: a genuine (non-lock) failure
// is wrapped as a crash-recovery failure, not just returned bare.
func TestRunCrashRecovery_NonLockErrorReturnsImmediately(t *testing.T) {
	s := testPgCtldService("")
	calls := 0
	runner := func(ctx context.Context) ([]byte, error) {
		calls++
		return []byte("FATAL:  could not access data directory"), errors.New("exit status 1")
	}

	err := s.runCrashRecoveryAttempts(context.Background(), runner, fastRetry())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "crash recovery failed")
	assert.Equal(t, 1, calls, "non-lock errors must not be retried")
}

// TestRunCrashRecovery_ContextCancelledDuringBackoff verifies the wrapper's own
// contribution: a cancelled context aborts the retry loop and propagates bare,
// without being dressed up as a "crash recovery failed" error.
func TestRunCrashRecovery_ContextCancelledDuringBackoff(t *testing.T) {
	s := testPgCtldService("")
	ctx, cancel := context.WithCancel(context.Background())

	calls := 0
	runner := func(ctx context.Context) ([]byte, error) {
		calls++
		cancel()
		return lockHeldOutput, errors.New("exit status 1")
	}

	err := s.runCrashRecoveryAttempts(ctx, runner, retry.New(time.Hour, time.Hour))
	require.ErrorIs(t, err, context.Canceled)
	assert.NotContains(t, err.Error(), "crash recovery failed",
		"cancellation must propagate bare, not wrapped as a recovery failure")
	assert.Equal(t, 1, calls)
}

func fileExists(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Stat(path)
	return err == nil
}

// TestRunCrashRecoveryInDir_RemovesAndRestoresStandbySignal verifies the
// standby.signal is absent while postgres --single runs (single-user mode
// refuses to start with one present) and is recreated afterwards so the node
// stays a standby.
func TestRunCrashRecoveryInDir_RemovesAndRestoresStandbySignal(t *testing.T) {
	s := testPgCtldService(t.TempDir())
	signalPath, err1 := s.createStandbySignal()
	require.NoError(t, err1)

	var signalPresentDuringRun bool
	runner := func(ctx context.Context) ([]byte, error) {
		signalPresentDuringRun = fileExists(t, signalPath)
		return []byte("recovery complete"), nil
	}

	err := s.runCrashRecoveryInDir(context.Background(), runner, fastRetry())
	require.NoError(t, err)
	assert.False(t, signalPresentDuringRun, "standby.signal must be removed while postgres --single runs")
	assert.True(t, fileExists(t, signalPath), "standby.signal must be recreated after recovery")
}

// TestRunCrashRecoveryInDir_RestoresStandbySignalOnFailure verifies the signal is
// recreated even when recovery fails, so a failed recovery does not silently
// convert a standby into a primary on its next start.
func TestRunCrashRecoveryInDir_RestoresStandbySignalOnFailure(t *testing.T) {
	s := testPgCtldService(t.TempDir())
	signalPath, err1 := s.createStandbySignal()
	require.NoError(t, err1)

	runner := func(ctx context.Context) ([]byte, error) {
		return []byte("FATAL:  could not access data directory"), errors.New("exit status 1")
	}

	err := s.runCrashRecoveryInDir(context.Background(), runner, fastRetry())
	require.Error(t, err)
	assert.True(t, fileExists(t, signalPath), "standby.signal must be recreated even when recovery fails")
}

// TestRunCrashRecoveryInDir_NoStandbySignal_LeavesNoneBehind verifies a primary
// (no standby.signal) is crash-recovered without a spurious signal being created.
func TestRunCrashRecoveryInDir_NoStandbySignal_LeavesNoneBehind(t *testing.T) {
	s := testPgCtldService(t.TempDir())

	signalPath := s.standbySignalPath()

	calls := 0
	runner := func(ctx context.Context) ([]byte, error) {
		calls++
		return []byte("recovery complete"), nil
	}

	err := s.runCrashRecoveryInDir(context.Background(), runner, fastRetry())
	require.NoError(t, err)
	assert.Equal(t, 1, calls)
	assert.False(t, fileExists(t, signalPath), "no standby.signal should be created for a non-standby node")
}

// TestRunCrashRecoveryInDir_RemoveFailureSkipsRecovery verifies that when the
// standby.signal cannot be removed, recovery is not attempted (running
// postgres --single with the signal present would fail). standby.signal is made
// un-removable by making it a non-empty directory, so os.Stat sees it present
// but os.Remove fails.
func TestRunCrashRecoveryInDir_RemoveFailureSkipsRecovery(t *testing.T) {
	s := testPgCtldService(t.TempDir())

	signalPath := s.standbySignalPath()
	require.NoError(t, os.Mkdir(signalPath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(signalPath, "child"), []byte(""), 0o644))

	called := false
	runner := func(ctx context.Context) ([]byte, error) {
		called = true
		return []byte("recovery complete"), nil
	}

	err := s.runCrashRecoveryInDir(context.Background(), runner, fastRetry())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to remove standby.signal")
	assert.False(t, called, "recovery must not run when standby.signal could not be removed")
}

// TestRunCrashRecoveryInDir_SkipsWhenPostgresRunning verifies the guard that
// crash recovery never disturbs a live postmaster: it must not touch
// standby.signal and must not run recovery. A live postmaster is simulated by a
// postmaster.pid pointing at this test process (which is, by definition, running).
func TestRunCrashRecoveryInDir_SkipsWhenPostgresRunning(t *testing.T) {
	dir := t.TempDir()
	s := testPgCtldService(dir)
	signalPath, err1 := s.createStandbySignal()
	require.NoError(t, err1)

	// postmaster.pid's first line is the PID; point it at ourselves so
	// isPostgreSQLRunning sees a live process.
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "postmaster.pid"),
		[]byte(strconv.Itoa(os.Getpid())+"\n"),
		0o644,
	))

	called := false
	runner := func(ctx context.Context) ([]byte, error) {
		called = true
		return []byte("recovery complete"), nil
	}

	err := s.runCrashRecoveryInDir(context.Background(), runner, fastRetry())
	require.NoError(t, err)
	assert.False(t, called, "recovery must not run against a live postmaster")
	assert.True(t, fileExists(t, signalPath), "standby.signal must be left untouched while postgres is running")
}
