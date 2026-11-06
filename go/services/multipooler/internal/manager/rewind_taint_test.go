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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// newAgedDataDir creates a data directory whose every entry has a modification
// time well before any marker written during the test, standing in for a data
// directory that postgres has long since stopped writing to.
func newAgedDataDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for _, d := range []string{"base/1", "global", "pg_wal"} {
		require.NoError(t, os.MkdirAll(filepath.Join(dir, d), 0o700))
	}
	for _, f := range []string{"base/1/16384", "global/pg_control", "pg_wal/000000010000000000000001", autoConfFileName} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, f), []byte("x"), 0o600))
	}
	ageTree(t, dir)
	t.Setenv(constants.PgDataDirEnvVar, dir)
	return dir
}

// ageTree sets every entry under root (root included) back an hour.
func ageTree(t *testing.T, root string) {
	t.Helper()
	old := time.Now().Add(-time.Hour)
	var paths []string
	require.NoError(t, filepath.WalkDir(root, func(p string, _ os.DirEntry, err error) error {
		paths = append(paths, p)
		return err
	}))
	for _, p := range paths {
		require.NoError(t, os.Chtimes(p, old, old))
	}
}

func TestFirstModifiedSince(t *testing.T) {
	// The reference is a minute ago, far enough back that the filesystem's coarse
	// timestamps cannot place a write made now before it.
	ref := time.Now().Add(-time.Minute)

	scan := func(t *testing.T, dir string) string {
		t.Helper()
		got, err := firstModifiedSince(dir, ref, filepath.Join(dir, autoConfFileName))
		require.NoError(t, err)
		return got
	}

	t.Run("untouched directory", func(t *testing.T) {
		assert.Empty(t, scan(t, newAgedDataDir(t)))
	})

	t.Run("a missing root is an error, not proof that nothing changed", func(t *testing.T) {
		gone := filepath.Join(t.TempDir(), "gone")
		_, err := firstModifiedSince(gone, ref, filepath.Join(gone, autoConfFileName))
		require.Error(t, err)
	})

	t.Run("a symlink whose target is gone is an error, not proof that nothing changed", func(t *testing.T) {
		// A tablespace or pg_wal volume that dropped out after the rewind wrote to it.
		dir := newAgedDataDir(t)
		require.NoError(t, os.Symlink(filepath.Join(dir, "missing"), filepath.Join(dir, "dangling")))
		old := time.Now().Add(-time.Hour)
		require.NoError(t, os.Chtimes(dir, old, old))
		_, err := firstModifiedSince(dir, ref, filepath.Join(dir, autoConfFileName))
		require.Error(t, err)
	})

	t.Run("a symlink target that cannot be inspected is an error", func(t *testing.T) {
		dir := newAgedDataDir(t)
		locked := filepath.Join(t.TempDir(), "locked")
		require.NoError(t, os.Mkdir(locked, 0o700))
		require.NoError(t, os.Symlink(filepath.Join(locked, "target"), filepath.Join(dir, "tblspc")))
		require.NoError(t, os.Chmod(locked, 0o000))
		t.Cleanup(func() { _ = os.Chmod(locked, 0o700) })
		if _, err := os.Stat(filepath.Join(locked, "target")); err == nil || errors.Is(err, os.ErrNotExist) {
			t.Skip("directory permissions are not enforced for this user")
		}
		old := time.Now().Add(-time.Hour)
		require.NoError(t, os.Chtimes(dir, old, old))
		_, err := firstModifiedSince(dir, ref, filepath.Join(dir, autoConfFileName))
		require.Error(t, err)
	})

	t.Run("overwritten file", func(t *testing.T) {
		dir := newAgedDataDir(t)
		target := filepath.Join(dir, "base/1/16384")
		require.NoError(t, os.WriteFile(target, []byte("changed"), 0o600))
		assert.Equal(t, target, scan(t, dir))
	})

	t.Run("truncated file", func(t *testing.T) {
		dir := newAgedDataDir(t)
		target := filepath.Join(dir, "global/pg_control")
		require.NoError(t, os.Truncate(target, 0))
		assert.Equal(t, target, scan(t, dir))
	})

	t.Run("removed file is seen through its directory", func(t *testing.T) {
		dir := newAgedDataDir(t)
		require.NoError(t, os.Remove(filepath.Join(dir, "base/1/16384")))
		assert.Equal(t, filepath.Join(dir, "base/1"), scan(t, dir))
	})

	t.Run("created file", func(t *testing.T) {
		dir := newAgedDataDir(t)
		require.NoError(t, os.WriteFile(filepath.Join(dir, "backup_label"), []byte("x"), 0o600))
		assert.NotEmpty(t, scan(t, dir))
	})

	t.Run("the pooler's own postgresql.auto.conf edits are ignored", func(t *testing.T) {
		dir := newAgedDataDir(t)
		require.NoError(t, os.WriteFile(filepath.Join(dir, autoConfFileName), []byte("restore_command = 'x'\n"), 0o600))
		assert.Empty(t, scan(t, dir))
	})

	t.Run("symlinks that do not point to a directory are not followed", func(t *testing.T) {
		dir := newAgedDataDir(t)
		require.NoError(t, os.Symlink(filepath.Join(dir, "base/1/16384"), filepath.Join(dir, "filelink")))
		old := time.Now().Add(-time.Hour)
		require.NoError(t, os.Chtimes(dir, old, old)) // creating the links touched it
		assert.Empty(t, scan(t, dir))
	})

	t.Run("an unreadable symlinked directory is an error", func(t *testing.T) {
		dir := newAgedDataDir(t)
		walDir := t.TempDir()
		locked := filepath.Join(walDir, "locked")
		require.NoError(t, os.Mkdir(locked, 0o700))
		ageTree(t, walDir)
		require.NoError(t, os.RemoveAll(filepath.Join(dir, "pg_wal")))
		require.NoError(t, os.Symlink(walDir, filepath.Join(dir, "pg_wal")))
		old := time.Now().Add(-time.Hour)
		require.NoError(t, os.Chtimes(dir, old, old))
		require.NoError(t, os.Chmod(locked, 0o000))
		t.Cleanup(func() { _ = os.Chmod(locked, 0o700) })
		if _, err := os.ReadDir(locked); err == nil {
			t.Skip("directory permissions are not enforced for this user")
		}
		_, err := firstModifiedSince(dir, ref, filepath.Join(dir, autoConfFileName))
		require.Error(t, err)
	})

	t.Run("a symlinked pg_wal is scanned", func(t *testing.T) {
		dir := newAgedDataDir(t)
		walDir := t.TempDir()
		segment := filepath.Join(walDir, "000000010000000000000001")
		require.NoError(t, os.WriteFile(segment, []byte("x"), 0o600))
		ageTree(t, walDir)
		require.NoError(t, os.RemoveAll(filepath.Join(dir, "pg_wal")))
		require.NoError(t, os.Symlink(walDir, filepath.Join(dir, "pg_wal")))
		ageTree(t, dir)
		require.Empty(t, scan(t, dir), "an untouched symlinked directory must not be reported")

		require.NoError(t, os.WriteFile(segment, []byte("changed"), 0o600))
		assert.Equal(t, filepath.Join(dir, "pg_wal", filepath.Base(segment)), scan(t, dir), "reported through the symlink")
	})
}

// rewindRunSetup returns a manager with an aged data directory and a mock pgctld
// whose second PgRewind call (the mutating one) runs mutate and then fails with
// failErr (nil means success).
func rewindRunSetup(t *testing.T, mutate func(dir string), failErr error) (*MultipoolerManager, *mockPgctldClient) {
	t.Helper()
	dir := newAgedDataDir(t)
	pm := newSentinelTestManager(t)
	client := &mockPgctldClient{
		pgRewindResponse: &pgctldpb.PgRewindResponse{Output: "servers diverged at 0/5000000 on timeline 2"},
	}
	client.pgRewindOnCall = func(*pgctldpb.PgRewindRequest) {
		if client.pgRewindCalls != 2 {
			return
		}
		if mutate != nil {
			mutate(dir)
		}
		client.pgRewindError = failErr
	}
	pm.pgctldClient = client
	return pm, client
}

func requirePresent(t *testing.T, s sentinel, want bool, msg string) {
	t.Helper()
	got, err := s.present()
	require.NoError(t, err)
	assert.Equal(t, want, got, msg)
}

func TestRunPgRewind_TaintVerdict(t *testing.T) {
	overwrite := func(dir string) {
		require.NoError(t, os.WriteFile(filepath.Join(dir, "base/1/16384"), []byte("changed"), 0o600))
	}

	t.Run("failure after the directory was modified records the taint and is not retryable", func(t *testing.T) {
		pm, _ := rewindRunSetup(t, overwrite, errors.New("source went away mid-copy"))
		_, err := pm.runPgRewind(t.Context(), "leader", 5432)
		require.ErrorContains(t, err, "source went away mid-copy")
		require.ErrorContains(t, err, "did not complete")
		requirePresent(t, pm.rewindTainted(), true, "modified directory must be recorded as tainted")
		requirePresent(t, pm.rewindAttempt(), false, "the attempt marker is consumed by the verdict")
		requirePresent(t, pm.rewindSentinel(), true, "the rewind sentinel stays")

		// A later attempt must not start another rewind on the tainted directory.
		withLock(t, pm, func(ctx context.Context) {
			client := &mockPgctldClient{}
			pm.pgctldClient = client
			_, err := pm.restartAsStandbyLocked(ctx, "leader", 5432)
			require.Error(t, err)
			assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
			assert.Zero(t, client.pgRewindCalls)
			assert.False(t, client.restartCalled)
		})
	})

	t.Run("failure before the directory was modified stays retryable", func(t *testing.T) {
		pm, client := rewindRunSetup(t, nil, errors.New("source demoted while planning"))
		_, err := pm.runPgRewind(t.Context(), "leader", 5432)
		require.ErrorContains(t, err, "source demoted while planning")
		assert.NotContains(t, err.Error(), "did not complete")
		requirePresent(t, pm.rewindTainted(), false, "an untouched directory must not be tainted")
		requirePresent(t, pm.rewindAttempt(), false, "the attempt marker must be cleared so later postgres writes are not attributed to the rewind")
		requirePresent(t, pm.rewindSentinel(), true, "repair stays armed")

		// The retry runs: a new attempt starts from a fresh reference.
		client.pgRewindError = nil
		client.pgRewindOnCall = nil
		performed, err := pm.runPgRewind(t.Context(), "leader", 5432)
		require.NoError(t, err)
		assert.True(t, performed)
	})

	t.Run("failed dry-run is retryable even though crash recovery wrote to the directory", func(t *testing.T) {
		pm, client := rewindRunSetup(t, nil, nil)
		// pgctld crash-recovers a target that was not cleanly stopped before the
		// dry-run, which writes to the directory, and then the dry-run fails.
		client.pgRewindOnCall = func(*pgctldpb.PgRewindRequest) {
			overwrite(postgresDataDir())
			client.pgRewindError = errors.New("dry-run boom")
		}
		_, err := pm.runPgRewind(t.Context(), "leader", 5432)
		require.ErrorContains(t, err, "dry-run boom")
		assert.NotContains(t, err.Error(), "did not complete")
		requirePresent(t, pm.rewindTainted(), false, "crash recovery before the marker is not a rewind")
		requirePresent(t, pm.rewindAttempt(), false, "no attempt was started")
		requirePresent(t, pm.rewindSentinel(), true, "repair stays armed")
	})

	t.Run("crash recovery in the dry-run is not mistaken for the rewind", func(t *testing.T) {
		pm, client := rewindRunSetup(t, nil, errors.New("source demoted while planning"))
		// The first call is the dry-run, which crash-recovers and succeeds; the
		// mutating call then fails before pg_rewind wrote anything.
		hook := client.pgRewindOnCall
		client.pgRewindOnCall = func(req *pgctldpb.PgRewindRequest) {
			if client.pgRewindCalls == 1 {
				overwrite(postgresDataDir())
				// Coarse filesystem timestamps would otherwise let the write and the
				// marker written right after it share a timestamp.
				time.Sleep(50 * time.Millisecond)
			}
			hook(req)
		}
		_, err := pm.runPgRewind(t.Context(), "leader", 5432)
		require.ErrorContains(t, err, "source demoted while planning")
		assert.NotContains(t, err.Error(), "did not complete")
		requirePresent(t, pm.rewindTainted(), false, "")
		requirePresent(t, pm.rewindAttempt(), false, "")
	})

	t.Run("success clears the attempt marker", func(t *testing.T) {
		pm, _ := rewindRunSetup(t, overwrite, nil)
		performed, err := pm.runPgRewind(t.Context(), "leader", 5432)
		require.NoError(t, err)
		assert.True(t, performed)
		requirePresent(t, pm.rewindTainted(), false, "a rewind that modified the directory and succeeded is not tainted")
		requirePresent(t, pm.rewindAttempt(), false, "")
	})

	t.Run("no divergence clears the attempt marker", func(t *testing.T) {
		pm, client := rewindRunSetup(t, nil, nil)
		client.pgRewindResponse = &pgctldpb.PgRewindResponse{Output: "no rewind required"}
		performed, err := pm.runPgRewind(t.Context(), "leader", 5432)
		require.NoError(t, err)
		assert.False(t, performed)
		requirePresent(t, pm.rewindAttempt(), false, "")
	})

	t.Run("an unassessed earlier run is settled before another is launched", func(t *testing.T) {
		pm, client := rewindRunSetup(t, nil, nil)
		client.pgRewindOnCall = nil
		require.NoError(t, pm.rewindAttempt().write())
		overwrite(postgresDataDir())

		_, err := pm.runPgRewind(t.Context(), "leader", 5432)
		require.ErrorContains(t, err, "earlier pg_rewind modified the data directory")
		assert.Zero(t, client.pgRewindCalls, "no rewind may run against the tainted directory")
		requirePresent(t, pm.rewindTainted(), true, "")
	})
}

func TestSettleRewindAttempt(t *testing.T) {
	t.Run("no marker means nothing to settle even if files are newer", func(t *testing.T) {
		dir := newAgedDataDir(t)
		pm := newSentinelTestManager(t)
		// Postgres legitimately wrote after a clean failure cleared the marker.
		require.NoError(t, os.WriteFile(filepath.Join(dir, "base/1/16384"), []byte("x"), 0o600))
		tainted, err := pm.settleRewindAttempt(t.Context())
		require.NoError(t, err)
		assert.False(t, tainted)
		requirePresent(t, pm.rewindTainted(), false, "")
	})

	t.Run("a data directory that has disappeared is treated as modified", func(t *testing.T) {
		newAgedDataDir(t)
		pm := newSentinelTestManager(t)
		require.NoError(t, pm.rewindAttempt().write())
		t.Setenv(constants.PgDataDirEnvVar, filepath.Join(t.TempDir(), "gone"))

		withLock(t, pm, func(ctx context.Context) {
			tainted, err := pm.settleRewindAttempt(ctx)
			require.NoError(t, err)
			assert.True(t, tainted)
		})
		requirePresent(t, pm.rewindAttempt(), true, "the evidence is kept until a verdict can be recorded")
	})

	t.Run("an unreadable data directory is treated as modified", func(t *testing.T) {
		dir := newAgedDataDir(t)
		pm := newSentinelTestManager(t)
		require.NoError(t, pm.rewindAttempt().write())
		require.NoError(t, os.Chmod(filepath.Join(dir, "base"), 0o000))
		t.Cleanup(func() { _ = os.Chmod(filepath.Join(dir, "base"), 0o700) })
		if _, err := os.ReadDir(filepath.Join(dir, "base")); err == nil {
			t.Skip("directory permissions are not enforced for this user")
		}

		tainted, err := pm.settleRewindAttempt(t.Context())
		require.NoError(t, err)
		assert.True(t, tainted)
	})
}

// blockSentinel makes the sentinel impossible to write or remove by putting a
// non-empty directory where its file would go.
func blockSentinel(t *testing.T, s sentinel) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Join(s.path, "occupied"), 0o700))
}

func TestRewindAttemptErrors(t *testing.T) {
	t.Run("a verdict that cannot be recorded is logged, not an error, and quarantines the pooler", func(t *testing.T) {
		dir := newAgedDataDir(t)
		pm := newSentinelTestManager(t)
		require.NoError(t, pm.rewindAttempt().write())
		require.NoError(t, os.WriteFile(filepath.Join(dir, "base/1/16384"), []byte("x"), 0o600))
		blockSentinel(t, pm.rewindTainted())

		withLock(t, pm, func(ctx context.Context) {
			tainted, err := pm.settleRewindAttempt(ctx)
			require.NoError(t, err)
			assert.True(t, tainted)
		})
		quarantined, reason, _ := quarantineState(pm)
		assert.True(t, quarantined, "the quarantine is durable without the marker")
		assert.Equal(t, rewindTaintedReason, reason)
		requirePresent(t, pm.rewindAttempt(), true, "kept so the verdict is derived again")

		withLock(t, pm, func(ctx context.Context) {
			runErr := errors.New("copy failed")
			got := pm.failRewindRun(ctx, runErr)
			require.ErrorIs(t, got, runErr)
			require.ErrorContains(t, got, "did not complete")
		})
	})

	t.Run("a taint recorded but an attempt marker that cannot be removed is only logged", func(t *testing.T) {
		dir := newAgedDataDir(t)
		pm := newSentinelTestManager(t)
		require.NoError(t, pm.rewindAttempt().write())
		// A read-only pooler directory: the marker can be read but not removed.
		poolerDir := pm.record.PoolerDir()
		require.NoError(t, os.Chmod(poolerDir, 0o500))
		t.Cleanup(func() { _ = os.Chmod(poolerDir, 0o700) })
		if err := os.WriteFile(filepath.Join(poolerDir, "probe"), nil, 0o600); err == nil {
			t.Skip("directory permissions are not enforced for this user")
		}
		time.Sleep(10 * time.Millisecond)
		require.NoError(t, os.WriteFile(filepath.Join(dir, "base/1/16384"), []byte("x"), 0o600))

		tainted, err := pm.settleRewindAttempt(t.Context())
		require.NoError(t, err)
		assert.True(t, tainted)
		requirePresent(t, pm.rewindTainted(), true, "")
	})

	t.Run("a pg_rewind run is not launched on a directory an earlier run modified", func(t *testing.T) {
		dir := newAgedDataDir(t)
		pm := newSentinelTestManager(t)
		client := &mockPgctldClient{
			pgRewindResponse: &pgctldpb.PgRewindResponse{Output: "servers diverged at 0/5000000 on timeline 2"},
		}
		pm.pgctldClient = client
		require.NoError(t, pm.rewindAttempt().write())
		require.NoError(t, os.WriteFile(filepath.Join(dir, "base/1/16384"), []byte("x"), 0o600))
		blockSentinel(t, pm.rewindTainted())

		withLock(t, pm, func(ctx context.Context) {
			_, err := pm.runPgRewind(ctx, "leader", 5432)
			require.Error(t, err)
			assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err))
			assert.Zero(t, client.pgRewindCalls)
		})
	})

	t.Run("a rewind is refused when the taint marker cannot be read", func(t *testing.T) {
		newAgedDataDir(t)
		pm := newSentinelTestManager(t)
		// A data directory below a regular file makes stat fail with something
		// other than not-exist.
		blocker := filepath.Join(t.TempDir(), "blocker")
		require.NoError(t, os.WriteFile(blocker, nil, 0o600))
		t.Setenv(constants.PgDataDirEnvVar, filepath.Join(blocker, "pg_data"))
		client := &mockPgctldClient{}
		pm.pgctldClient = client
		withLock(t, pm, func(ctx context.Context) {
			_, err := pm.restartAsStandbyLocked(ctx, "leader", 5432)
			require.ErrorContains(t, err, "failed to check rewind taint marker")
			assert.Zero(t, client.pgRewindCalls)
		})
	})

	t.Run("a failed unassessed marker check is an error", func(t *testing.T) {
		newAgedDataDir(t)
		pm := newSentinelTestManager(t)
		// A path component that is a file makes stat fail with something other
		// than not-exist.
		require.NoError(t, os.WriteFile(filepath.Join(pm.record.PoolerDir(), "blocker"), nil, 0o600))
		pm.record = newRecordFromProto(&clustermetadatapb.Multipooler{
			Type:      clustermetadatapb.PoolerType_REPLICA,
			PoolerDir: filepath.Join(pm.record.PoolerDir(), "blocker", "sub"),
		})
		_, err := pm.settleRewindAttempt(t.Context())
		require.Error(t, err)
	})

	t.Run("an attempt marker that cannot be removed after a pass is only logged", func(t *testing.T) {
		newAgedDataDir(t)
		pm := newSentinelTestManager(t)
		blockSentinel(t, pm.rewindAttempt())
		assert.NotPanics(t, func() { pm.passRewindRun(t.Context()) })
	})

	t.Run("the monitor quarantines an interrupted run whose verdict cannot be recorded", func(t *testing.T) {
		dir := newAgedDataDir(t)
		pm := newSentinelTestManager(t)
		require.NoError(t, pm.rewindSentinel().write())
		require.NoError(t, pm.rewindAttempt().write())
		require.NoError(t, os.WriteFile(filepath.Join(dir, "base/1/16384"), []byte("half copied"), 0o600))
		blockSentinel(t, pm.rewindTainted())

		withLock(t, pm, func(ctx context.Context) {
			require.NoError(t, pm.takeRemedialAction(ctx, remedialActionMarkRewindInterrupted, postgresState{}))
		})
		quarantined, _, _ := quarantineState(pm)
		assert.True(t, quarantined)
		requirePresent(t, pm.rewindAttempt(), true, "the evidence is kept")
		assert.False(t, pm.consensusMgr.SuspectedDivergence(), "no repair may be armed for a modified directory")
	})
}

func TestSettleRewindAttempt_WaitsForPgRewindToExit(t *testing.T) {
	setWait := func(t *testing.T, wait time.Duration) {
		t.Helper()
		oldWait, oldPoll := rewindRunningWait, rewindRunningPoll
		rewindRunningWait, rewindRunningPoll = wait, 10*time.Millisecond
		t.Cleanup(func() { rewindRunningWait, rewindRunningPoll = oldWait, oldPoll })
	}

	t.Run("a write made while pg_rewind is still being killed is seen", func(t *testing.T) {
		dir := newAgedDataDir(t)
		pm := newSentinelTestManager(t)
		setWait(t, 5*time.Second)
		require.NoError(t, pm.rewindAttempt().write())
		running := pm.newSentinel(constants.RewindRunningFile, "running\n")
		require.NoError(t, running.write())

		// The call failed on a timeout and returned while pg_rewind was still alive:
		// it writes after the pooler started settling and then exits.
		go func() {
			time.Sleep(100 * time.Millisecond)
			_ = os.WriteFile(filepath.Join(dir, "base/1/16384"), []byte("late write"), 0o600)
			_ = running.remove()
		}()

		tainted, err := pm.settleRewindAttempt(t.Context())
		require.NoError(t, err)
		assert.True(t, tainted, "the scan must wait until pg_rewind has exited")
	})

	t.Run("a pg_rewind that cannot be confirmed to have exited counts as modified", func(t *testing.T) {
		newAgedDataDir(t)
		pm := newSentinelTestManager(t)
		setWait(t, 50*time.Millisecond)
		require.NoError(t, pm.rewindAttempt().write())
		require.NoError(t, pm.newSentinel(constants.RewindRunningFile, "running\n").write())

		tainted, err := pm.settleRewindAttempt(t.Context())
		require.NoError(t, err)
		assert.True(t, tainted)
		requirePresent(t, pm.rewindTainted(), true, "")
		requirePresent(t, pm.rewindAttempt(), true, "kept: a pg_rewind that is still running can delete the verdict")

		// The lingering pg_rewind deletes the verdict, as it does any file only the
		// target has, and a later settle derives and records it again.
		require.NoError(t, pm.rewindTainted().remove())
		tainted, err = pm.settleRewindAttempt(t.Context())
		require.NoError(t, err)
		assert.True(t, tainted)
		requirePresent(t, pm.rewindTainted(), true, "derived again from the kept attempt marker")
	})

	t.Run("a confirmed exit consumes the attempt marker after recording the verdict", func(t *testing.T) {
		dir := newAgedDataDir(t)
		pm := newSentinelTestManager(t)
		setWait(t, 50*time.Millisecond)
		require.NoError(t, pm.rewindAttempt().write())
		require.NoError(t, os.WriteFile(filepath.Join(dir, "base/1/16384"), []byte("x"), 0o600))

		tainted, err := pm.settleRewindAttempt(t.Context())
		require.NoError(t, err)
		assert.True(t, tainted)
		requirePresent(t, pm.rewindAttempt(), false, "the verdict is durable, so the marker is consumed")
	})

	t.Run("a cancelled caller context does not cut the wait short", func(t *testing.T) {
		dir := newAgedDataDir(t)
		pm := newSentinelTestManager(t)
		setWait(t, 5*time.Second)
		require.NoError(t, pm.rewindAttempt().write())
		running := pm.newSentinel(constants.RewindRunningFile, "running\n")
		require.NoError(t, running.write())
		go func() {
			time.Sleep(100 * time.Millisecond)
			_ = os.WriteFile(filepath.Join(dir, "base/1/16384"), []byte("late write"), 0o600)
			_ = running.remove()
		}()

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		tainted, err := pm.settleRewindAttempt(ctx)
		require.NoError(t, err)
		assert.True(t, tainted)
	})
}

func TestRewindTainted_Sentinel(t *testing.T) {
	t.Run("lives in the data directory, so replacing the directory removes it", func(t *testing.T) {
		dir := newAgedDataDir(t)
		pm := newSentinelTestManager(t)
		s := pm.rewindTainted()
		assert.Equal(t, filepath.Join(dir, constants.RewindTaintedFile), s.path)
		assert.NotEqual(t, pm.record.PoolerDir(), filepath.Dir(s.path))

		requirePresent(t, s, false, "no verdict initially")
		require.NoError(t, s.write())
		requirePresent(t, s, true, "present after write")

		// A fresh data directory in the same place (restore, re-init) has no verdict.
		require.NoError(t, os.RemoveAll(dir))
		require.NoError(t, os.MkdirAll(dir, 0o700))
		requirePresent(t, s, false, "the verdict goes with the directory it describes")
	})

	t.Run("a write to a missing data directory is an error", func(t *testing.T) {
		newAgedDataDir(t)
		pm := newSentinelTestManager(t)
		t.Setenv(constants.PgDataDirEnvVar, filepath.Join(t.TempDir(), "no", "such", "dir"))
		require.Error(t, pm.rewindTainted().write())
	})
}

func TestMonitor_TaintedRewind(t *testing.T) {
	t.Run("the verdict is rediscovered from disk and decides before everything else", func(t *testing.T) {
		newAgedDataDir(t)
		pm := newSentinelTestManager(t)
		pm.pgctldClient = &mockPgctldClient{
			statusResponse: &pgctldpb.StatusResponse{Status: pgctldpb.ServerStatus_STOPPED},
		}
		state, err := pm.discoverPostgresState(t.Context())
		require.NoError(t, err)
		assert.False(t, state.rewindTaintedPresent)

		require.NoError(t, pm.rewindTainted().write())
		state, err = pm.discoverPostgresState(t.Context())
		require.NoError(t, err)
		require.True(t, state.rewindTaintedPresent)

		// Even with the sentinel present and pgctld unreachable, the verdict wins.
		state.rewindSentinelPresent = true
		state.pgctldAvailable = false
		assert.Equal(t, remedialActionQuarantineTaintedRewind, pm.determineRemedialAction(t.Context(), state))
	})

	t.Run("the action quarantines the pooler", func(t *testing.T) {
		pm := newSentinelTestManager(t)
		withLock(t, pm, func(ctx context.Context) {
			require.NoError(t, pm.takeRemedialAction(ctx, remedialActionQuarantineTaintedRewind, postgresState{}))
		})
		quarantined, reason, _ := quarantineState(pm)
		assert.True(t, quarantined)
		assert.Equal(t, rewindTaintedReason, reason)
		assert.True(t, pm.postgresRestartsDisabled.Load())
	})

	t.Run("an interrupted run that modified the directory is quarantined, not repaired", func(t *testing.T) {
		dir := newAgedDataDir(t)
		pm := newSentinelTestManager(t)
		require.NoError(t, pm.rewindSentinel().write())
		require.NoError(t, pm.rewindAttempt().write())
		require.NoError(t, os.WriteFile(filepath.Join(dir, "base/1/16384"), []byte("half copied"), 0o600))

		withLock(t, pm, func(ctx context.Context) {
			require.NoError(t, pm.takeRemedialAction(ctx, remedialActionMarkRewindInterrupted, postgresState{}))
		})
		quarantined, _, _ := quarantineState(pm)
		assert.True(t, quarantined)
		requirePresent(t, pm.rewindTainted(), true, "")
		assert.False(t, pm.consensusMgr.SuspectedDivergence(), "no repair may be armed for a tainted directory")
	})

	t.Run("an interrupted run that left the directory untouched is repaired", func(t *testing.T) {
		newAgedDataDir(t)
		pm := newSentinelTestManager(t)
		require.NoError(t, pm.rewindSentinel().write())
		require.NoError(t, pm.rewindAttempt().write())

		withLock(t, pm, func(ctx context.Context) {
			require.NoError(t, pm.takeRemedialAction(ctx, remedialActionMarkRewindInterrupted, postgresState{}))
		})
		quarantined, _, _ := quarantineState(pm)
		assert.False(t, quarantined)
		requirePresent(t, pm.rewindTainted(), false, "")
		requirePresent(t, pm.rewindAttempt(), false, "settled, so a held start is not mistaken for a rewind")
		assert.True(t, pm.consensusMgr.SuspectedDivergence(), "repair is armed as before")
	})
}

// TestRewindSettleFailures covers the callers of settleRewindAttempt when the
// attempt marker cannot be resolved. An attempt marker that is a non-empty
// directory is present but cannot be removed, so settling an untouched data
// directory fails.
func TestRewindSettleFailures(t *testing.T) {
	t.Run("failRewindRun returns the run error together with the settle error", func(t *testing.T) {
		newAgedDataDir(t)
		pm := newSentinelTestManager(t)
		blockSentinel(t, pm.rewindAttempt())

		withLock(t, pm, func(ctx context.Context) {
			runErr := errors.New("copy failed")
			got := pm.failRewindRun(ctx, runErr)
			require.ErrorIs(t, got, runErr)
			require.ErrorContains(t, got, "failed to settle rewind attempt marker")
		})
	})

	t.Run("a rewind is not launched while an earlier run cannot be settled", func(t *testing.T) {
		newAgedDataDir(t)
		pm := newSentinelTestManager(t)
		client := &mockPgctldClient{
			pgRewindResponse: &pgctldpb.PgRewindResponse{Output: "servers diverged at 0/5000000 on timeline 2"},
		}
		pm.pgctldClient = client
		blockSentinel(t, pm.rewindAttempt())

		withLock(t, pm, func(ctx context.Context) {
			_, err := pm.runPgRewind(ctx, "leader", 5432)
			require.ErrorContains(t, err, "failed to settle earlier pg_rewind run")
			assert.Zero(t, client.pgRewindCalls)
		})
	})

	t.Run("a rewind is not launched when the attempt marker cannot be written", func(t *testing.T) {
		newAgedDataDir(t)
		pm := newSentinelTestManager(t)
		client := &mockPgctldClient{
			pgRewindResponse: &pgctldpb.PgRewindResponse{Output: "servers diverged at 0/5000000 on timeline 2"},
		}
		// The dry-run succeeds and then something occupies the marker's path.
		client.pgRewindOnCall = func(*pgctldpb.PgRewindRequest) {
			blockSentinel(t, pm.rewindAttempt())
		}
		pm.pgctldClient = client

		withLock(t, pm, func(ctx context.Context) {
			_, err := pm.runPgRewind(ctx, "leader", 5432)
			require.ErrorContains(t, err, "failed to write rewind attempt marker")
			assert.Equal(t, 1, client.pgRewindCalls, "only the dry-run ran")
		})
	})

	t.Run("the monitor retries later when an interrupted run cannot be settled", func(t *testing.T) {
		newAgedDataDir(t)
		pm := newSentinelTestManager(t)
		require.NoError(t, pm.rewindSentinel().write())
		blockSentinel(t, pm.rewindAttempt())

		withLock(t, pm, func(ctx context.Context) {
			require.NoError(t, pm.takeRemedialAction(ctx, remedialActionMarkRewindInterrupted, postgresState{}))
		})
		quarantined, _, _ := quarantineState(pm)
		assert.False(t, quarantined)
		assert.False(t, pm.consensusMgr.SuspectedDivergence(), "nothing is armed until the run is settled")
	})

	t.Run("discovery skips the tick when the taint marker cannot be read", func(t *testing.T) {
		newAgedDataDir(t)
		pm := newSentinelTestManager(t)
		pm.pgctldClient = &mockPgctldClient{
			statusResponse: &pgctldpb.StatusResponse{Status: pgctldpb.ServerStatus_STOPPED},
		}
		blocker := filepath.Join(t.TempDir(), "blocker")
		require.NoError(t, os.WriteFile(blocker, nil, 0o600))
		t.Setenv(constants.PgDataDirEnvVar, filepath.Join(blocker, "pg_data"))

		_, err := pm.discoverPostgresState(t.Context())
		require.ErrorContains(t, err, "check rewind taint marker")
	})

	t.Run("discovery reports the verdict when pgctld is unreachable", func(t *testing.T) {
		newAgedDataDir(t)
		pm := newSentinelTestManager(t)
		pm.pgctldClient = &mockPgctldClient{statusError: errors.New("pgctld unreachable")}
		require.NoError(t, pm.rewindTainted().write())

		state, err := pm.discoverPostgresState(t.Context())
		require.NoError(t, err, "the verdict needs no pgctld")
		require.True(t, state.rewindTaintedPresent)
		assert.Equal(t, remedialActionQuarantineTaintedRewind, pm.determineRemedialAction(t.Context(), state))
	})

	t.Run("discovery reports the verdict when there is no pgctld client", func(t *testing.T) {
		newAgedDataDir(t)
		pm := newSentinelTestManager(t)
		pm.pgctldClient = nil
		require.NoError(t, pm.rewindTainted().write())

		state, err := pm.discoverPostgresState(t.Context())
		require.NoError(t, err)
		assert.True(t, state.rewindTaintedPresent)
	})
}
