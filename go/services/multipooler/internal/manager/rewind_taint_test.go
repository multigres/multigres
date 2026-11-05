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

	t.Run("missing root has nothing to protect", func(t *testing.T) {
		assert.Empty(t, scan(t, filepath.Join(t.TempDir(), "gone")))
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
		require.NoError(t, os.Symlink(filepath.Join(dir, "missing"), filepath.Join(dir, "dangling")))
		require.NoError(t, os.Symlink(filepath.Join(dir, "base/1/16384"), filepath.Join(dir, "filelink")))
		old := time.Now().Add(-time.Hour)
		require.NoError(t, os.Chtimes(dir, old, old)) // creating the links touched it
		assert.Empty(t, scan(t, dir))
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

	t.Run("failed dry-run leaves the directory untouched", func(t *testing.T) {
		pm, client := rewindRunSetup(t, nil, nil)
		client.pgRewindOnCall = nil
		client.pgRewindError = errors.New("dry-run boom")
		_, err := pm.runPgRewind(t.Context(), "leader", 5432)
		require.ErrorContains(t, err, "dry-run boom")
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
	t.Run("a verdict that cannot be recorded is an error, not a silent retry", func(t *testing.T) {
		dir := newAgedDataDir(t)
		pm := newSentinelTestManager(t)
		require.NoError(t, pm.rewindAttempt().write())
		require.NoError(t, os.WriteFile(filepath.Join(dir, "base/1/16384"), []byte("x"), 0o600))
		blockSentinel(t, pm.rewindTainted())

		tainted, err := pm.settleRewindAttempt(t.Context())
		require.Error(t, err)
		assert.True(t, tainted)

		runErr := errors.New("copy failed")
		got := pm.failRewindRun(t.Context(), runErr)
		require.ErrorIs(t, got, runErr)
		require.ErrorContains(t, got, "failed to settle rewind attempt marker")
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

	t.Run("the monitor retries on the next tick when the interrupted run cannot be settled", func(t *testing.T) {
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
		assert.False(t, quarantined, "nothing is decided until the verdict is durable")
		requirePresent(t, pm.rewindAttempt(), true, "the evidence is kept for the retry")
		assert.False(t, pm.consensusMgr.SuspectedDivergence())
	})
}

func TestMonitor_TaintedRewind(t *testing.T) {
	t.Run("the verdict is rediscovered from disk and decides before everything else", func(t *testing.T) {
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
