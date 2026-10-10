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
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
	backupengine "github.com/multigres/multigres/go/services/multipooler/internal/manager/backup"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

func TestRestoreSentinel_RoundTrip(t *testing.T) {
	pm := newSentinelTestManager(t)

	present, err := pm.hasRestoreSentinel()
	require.NoError(t, err)
	assert.False(t, present, "no sentinel initially")

	require.NoError(t, pm.writeRestoreSentinel())
	present, err = pm.hasRestoreSentinel()
	require.NoError(t, err)
	assert.True(t, present, "sentinel present after write")

	// Writing again is idempotent.
	require.NoError(t, pm.writeRestoreSentinel())

	require.NoError(t, pm.removeRestoreSentinel())
	present, err = pm.hasRestoreSentinel()
	require.NoError(t, err)
	assert.False(t, present, "sentinel gone after remove")

	// Removing a missing sentinel is not an error.
	require.NoError(t, pm.removeRestoreSentinel())
}

// TestRestoreSentinel_ErrorPaths covers the sentinel helpers' error branches so a
// failure to check/record/clear the marker is surfaced rather than silently
// ignored.
func TestRestoreSentinel_ErrorPaths(t *testing.T) {
	// hasRestoreSentinel surfaces a non-NotExist stat error: a pooler dir that is
	// a regular file makes the stat fail with ENOTDIR.
	notADir := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(notADir, []byte("x"), 0o644))
	fileDir := newTestManager(t, withRecord(newRecordFromProto(&clustermetadatapb.Multipooler{
		Type:      clustermetadatapb.PoolerType_REPLICA,
		PoolerDir: notADir,
	})))
	_, err := fileDir.hasRestoreSentinel()
	require.Error(t, err, "a stat failure other than not-exist should error")

	// writeRestoreSentinel surfaces a write error when the pooler dir is missing.
	missingDir := newTestManager(t, withRecord(newRecordFromProto(&clustermetadatapb.Multipooler{
		Type:      clustermetadatapb.PoolerType_REPLICA,
		PoolerDir: filepath.Join(t.TempDir(), "no", "such", "dir"),
	})))
	require.Error(t, missingDir.writeRestoreSentinel(), "write to a nonexistent pooler dir should error")

	// removeRestoreSentinel surfaces a non-NotExist error: a non-empty directory
	// at the sentinel path cannot be removed by os.Remove.
	pm := newSentinelTestManager(t)
	sentinelPath := pm.restoreSentinelPath()
	require.NoError(t, os.Mkdir(sentinelPath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(sentinelPath, "child"), []byte("x"), 0o644))
	require.Error(t, pm.removeRestoreSentinel(), "removing a non-empty directory at the sentinel path should error")
}

// TestDiscoverPostgresState_RestoreSentinel verifies discoverPostgresState
// surfaces the on-disk restore sentinel, the durable signal the monitor gates on.
func TestDiscoverPostgresState_RestoreSentinel(t *testing.T) {
	ctx := t.Context()

	pm := NewTestMultipoolerManager(t)
	pm.pgctldClient = &mockPgctldClient{
		statusResponse: &pgctldpb.StatusResponse{Status: pgctldpb.ServerStatus_STOPPED},
	}

	state, err := pm.discoverPostgresState(ctx)
	require.NoError(t, err)
	assert.False(t, state.restoreSentinelPresent, "no restore sentinel initially")

	require.NoError(t, pm.writeRestoreSentinel())

	state, err = pm.discoverPostgresState(ctx)
	require.NoError(t, err)
	assert.True(t, state.restoreSentinelPresent, "restore sentinel surfaced once written")
}

// TestDiscoverPostgresState_UnreadableRestoreSentinel verifies an unreadable
// sentinel makes the monitor skip the tick rather than treating it as absent and
// possibly starting postgres on a partial restore.
func TestDiscoverPostgresState_UnreadableRestoreSentinel(t *testing.T) {
	pm := NewTestMultipoolerManager(t)
	pm.pgctldClient = &mockPgctldClient{
		statusResponse: &pgctldpb.StatusResponse{Status: pgctldpb.ServerStatus_STOPPED},
	}
	// A self-referencing symlink makes the stat fail with ELOOP, which is not a
	// not-exist error.
	require.NoError(t, os.Symlink(pm.restoreSentinelPath(), pm.restoreSentinelPath()))

	_, err := pm.discoverPostgresState(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "check restore sentinel")
}

// diskStatusPgctldClient reports STOPPED or NOT_INITIALIZED based on whether
// PG_VERSION is on disk, like the real pgctld (IsDataDirInitialized), so a test
// can tell whether a partial data directory was removed before the status
// re-check in restoreAndStartPostgres.
type diskStatusPgctldClient struct {
	mockPgctldClient
}

func (c *diskStatusPgctldClient) Status(context.Context, *pgctldpb.StatusRequest, ...grpc.CallOption) (*pgctldpb.StatusResponse, error) {
	if _, err := os.Stat(filepath.Join(postgresDataDir(), "PG_VERSION")); err == nil {
		return &pgctldpb.StatusResponse{Status: pgctldpb.ServerStatus_STOPPED}, nil
	}
	return &pgctldpb.StatusResponse{Status: pgctldpb.ServerStatus_NOT_INITIALIZED}, nil
}

// plantPartialRestore creates the on-disk state an interrupted restore leaves
// behind: a PGDATA that already contains PG_VERSION (so it looks initialized)
// plus some other restored file. It returns the PGDATA path.
func plantPartialRestore(t *testing.T, pm *MultipoolerManager) string {
	t.Helper()
	dataDir := filepath.Join(pm.record.PoolerDir(), "pg_data")
	t.Setenv(constants.PgDataDirEnvVar, dataDir)
	require.NoError(t, os.MkdirAll(dataDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dataDir, "PG_VERSION"), []byte("17\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dataDir, "partial_marker"), []byte("x"), 0o644))
	return dataDir
}

// TestRestoreAndStartPostgres_RestoreSentinelRemovesPartialDataDir is the core
// of the fix: after a crash mid-restore, the partial PGDATA must be removed
// before the pgctld status re-check. That check treats PG_VERSION as
// "initialized", so removing it any later would make restoreAndStartPostgres
// skip the restore and leave the monitor starting postgres on the partial
// directory forever.
func TestRestoreAndStartPostgres_RestoreSentinelRemovesPartialDataDir(t *testing.T) {
	pm := newSentinelTestManager(t)
	dataDir := plantPartialRestore(t, pm)
	require.NoError(t, pm.writeRestoreSentinel())
	pm.pgctldClient = &diskStatusPgctldClient{}
	// No config path set on the backup engine, so ListBackups fails right after
	// the cleanup and the status re-check, without needing pgbackrest.
	pm.backup = backupengine.NewEngine(pm.logger, pm.runLongCommand, pm.record, backupengine.Settings{})

	withLock(t, pm, func(ctx context.Context) {
		err := pm.restoreAndStartPostgres(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to list backups",
			"the restore must proceed past the status re-check instead of being skipped")
	})

	assert.NoDirExists(t, dataDir, "the partial data directory must be removed")
	present, err := pm.hasRestoreSentinel()
	require.NoError(t, err)
	assert.True(t, present, "the sentinel must stay until a restore completes")
}

// TestRestoreAndStartPostgres_NoSentinelKeepsInitializedDataDir guards the
// boundary: without a sentinel, an initialized data directory is left alone and
// the restore is skipped, as before.
func TestRestoreAndStartPostgres_NoSentinelKeepsInitializedDataDir(t *testing.T) {
	pm := newSentinelTestManager(t)
	dataDir := plantPartialRestore(t, pm)
	pm.pgctldClient = &diskStatusPgctldClient{}
	pm.backup = backupengine.NewEngine(pm.logger, pm.runLongCommand, pm.record, backupengine.Settings{})

	withLock(t, pm, func(ctx context.Context) {
		require.NoError(t, pm.restoreAndStartPostgres(ctx), "an initialized data directory skips the restore")
	})

	assert.FileExists(t, filepath.Join(dataDir, "PG_VERSION"), "the data directory must not be touched")
}

// TestRestoreAndStartPostgres_PartialDataDirRemovalFails verifies a partial data
// directory that cannot be removed fails the attempt instead of restoring or
// starting postgres on top of it.
func TestRestoreAndStartPostgres_PartialDataDirRemovalFails(t *testing.T) {
	pm := newSentinelTestManager(t)
	dataDir := plantPartialRestore(t, pm)
	require.NoError(t, pm.writeRestoreSentinel())
	// removeDataDirectory refuses to delete $HOME, which makes the removal fail.
	t.Setenv("HOME", dataDir)

	withLock(t, pm, func(ctx context.Context) {
		err := pm.restoreAndStartPostgres(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to remove partial data directory from interrupted restore")
	})

	assert.FileExists(t, filepath.Join(dataDir, "PG_VERSION"), "the data directory removal was refused")
}

// TestRestoreAndStartPostgres_UnreadableRestoreSentinel verifies an unreadable
// sentinel fails the attempt rather than being treated as absent.
func TestRestoreAndStartPostgres_UnreadableRestoreSentinel(t *testing.T) {
	notADir := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(notADir, []byte("x"), 0o644))
	pm := newTestManager(t, withRecord(newRecordFromProto(&clustermetadatapb.Multipooler{
		Type:      clustermetadatapb.PoolerType_REPLICA,
		PoolerDir: notADir,
	})))

	withLock(t, pm, func(ctx context.Context) {
		err := pm.restoreAndStartPostgres(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to check restore sentinel")
	})
}

// newRestoreTestManager builds a standby manager whose backup engine can attempt
// a restore into poolerDir/pg_data.
func newRestoreTestManager(t *testing.T, poolerDir string) *MultipoolerManager {
	t.Helper()
	pm := &MultipoolerManager{
		logger:     slog.Default(),
		actionLock: actionlock.NewActionLock(),
		record: newRecordFromProto(&clustermetadatapb.Multipooler{
			PoolerDir: poolerDir,
		}),
	}
	pm.backup = backupengine.NewEngine(pm.logger, pm.runLongCommand, pm.record, backupengine.Settings{})
	pm.stateManager = NewStateManager(pm.logger, pm.record, func() *clustermetadatapb.ConsensusStatus { return nil })
	return pm
}

// TestRestoreFromBackupLocked_KeepsSentinelWhenPartialDataDirRemovalFails
// verifies the sentinel outlives a failed restore whose partial data directory
// could not be removed, so the next attempt removes it instead of starting
// postgres on it.
func TestRestoreFromBackupLocked_KeepsSentinelWhenPartialDataDirRemovalFails(t *testing.T) {
	poolerDir := t.TempDir()
	dataDir := filepath.Join(poolerDir, "pg_data")
	t.Setenv(constants.PgDataDirEnvVar, dataDir)
	require.NoError(t, os.MkdirAll(dataDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dataDir, "partial_marker"), []byte("x"), 0o644))
	// removeDataDirectory refuses to delete $HOME, which makes the cleanup fail.
	t.Setenv("HOME", dataDir)

	pm := newRestoreTestManager(t, poolerDir)
	withLock(t, pm, func(ctx context.Context) {
		// No config path on the backup engine, so Restore() fails immediately.
		require.Error(t, pm.restoreFromBackupLocked(ctx, "some-backup-id"))
	})

	assert.DirExists(t, dataDir, "the data directory removal was refused")
	assert.FileExists(t, filepath.Join(poolerDir, constants.RestoreSentinelFile),
		"the sentinel must stay while the partial data directory is still on disk")
}

// TestRestoreFromBackupLocked_SentinelWriteFailureAbortsRestore verifies the
// restore never runs unprotected: if the sentinel can't be written, it aborts
// before pgbackrest touches PGDATA.
func TestRestoreFromBackupLocked_SentinelWriteFailureAbortsRestore(t *testing.T) {
	t.Setenv(constants.PgDataDirEnvVar, filepath.Join(t.TempDir(), "pg_data"))
	pm := newRestoreTestManager(t, filepath.Join(t.TempDir(), "no", "such", "dir"))

	withLock(t, pm, func(ctx context.Context) {
		err := pm.restoreFromBackupLocked(ctx, "some-backup-id")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to write restore sentinel")
	})
}

// newMockPgbackrestRestoreManager builds a restore test manager whose backup
// engine runs a mock pgbackrest. restoreScript is the bash run for the restore
// command; SENTINEL (the restore sentinel path) and PGDATA are in its
// environment. It returns the manager and its PGDATA path.
func newMockPgbackrestRestoreManager(t *testing.T, restoreScript string) (*MultipoolerManager, string) {
	t.Helper()
	tmpDir := t.TempDir()
	poolerDir := filepath.Join(tmpDir, "pooler")
	require.NoError(t, os.MkdirAll(poolerDir, 0o755))
	dataDir := filepath.Join(poolerDir, "pg_data")
	t.Setenv(constants.PgDataDirEnvVar, dataDir)
	t.Setenv("SENTINEL", filepath.Join(poolerDir, constants.RestoreSentinelFile))

	binDir := filepath.Join(tmpDir, "bin")
	require.NoError(t, os.MkdirAll(binDir, 0o755))
	mockScript := "#!/bin/bash\nif [[ \"$*\" == *\"restore\"* ]]; then\n" + restoreScript + "\nfi\nexit 0\n"
	require.NoError(t, os.WriteFile(filepath.Join(binDir, "pgbackrest"), []byte(mockScript), 0o755))
	t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))

	configPath := filepath.Join(tmpDir, "pgbackrest.conf")
	require.NoError(t, os.WriteFile(configPath, []byte("[global]\n"), 0o600))

	pm := newRestoreTestManager(t, poolerDir)
	pm.backup.SetConfigPath(configPath)
	return pm, dataDir
}

// TestRestoreFromBackupLocked_SentinelCoversOnlyTheRestore verifies the sentinel
// is on disk while pgbackrest restore runs and is cleared as soon as it
// succeeds, so a failure in a later step can't get the restored data discarded.
func TestRestoreFromBackupLocked_SentinelCoversOnlyTheRestore(t *testing.T) {
	observedFile := filepath.Join(t.TempDir(), "sentinel_during_restore")
	t.Setenv("OBSERVED", observedFile)
	// Records whether the sentinel exists while the restore runs, then
	// "restores" by writing PG_VERSION into PGDATA.
	pm, dataDir := newMockPgbackrestRestoreManager(t, `
[[ -f "$SENTINEL" ]] && echo present > "$OBSERVED"
mkdir -p "$PGDATA" && echo 17 > "$PGDATA/PG_VERSION"`)

	withLock(t, pm, func(ctx context.Context) {
		// The engine has no data directory configured, so the step after the
		// restore (reconfiguring the archive settings) fails.
		err := pm.restoreFromBackupLocked(ctx, "some-backup-id")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to remove old archive configuration")
	})

	assert.FileExists(t, observedFile, "the sentinel must be on disk while pgbackrest restore runs")
	assert.NoFileExists(t, pm.restoreSentinelPath(), "the sentinel must be cleared once the restore succeeds")
	assert.FileExists(t, filepath.Join(dataDir, "PG_VERSION"),
		"a failure after a successful restore must not discard the restored data")
}

// TestRestoreFromBackupLocked_SentinelRemovalFails covers a sentinel that can't
// be cleared. The mock pgbackrest swaps the sentinel for a non-empty directory,
// which os.Remove can't delete.
func TestRestoreFromBackupLocked_SentinelRemovalFails(t *testing.T) {
	const swapSentinel = `rm -f "$SENTINEL" && mkdir "$SENTINEL" && touch "$SENTINEL/child"`

	t.Run("after a failed restore the restore error is returned", func(t *testing.T) {
		pm, _ := newMockPgbackrestRestoreManager(t, swapSentinel+"\nexit 1")

		withLock(t, pm, func(ctx context.Context) {
			err := pm.restoreFromBackupLocked(ctx, "some-backup-id")
			require.Error(t, err)
			assert.Contains(t, err.Error(), "pgbackrest restore failed")
		})

		assert.DirExists(t, pm.restoreSentinelPath(), "the sentinel could not be removed")
	})

	t.Run("after a successful restore the attempt fails so it is redone", func(t *testing.T) {
		pm, dataDir := newMockPgbackrestRestoreManager(t, swapSentinel+"\n"+
			`mkdir -p "$PGDATA" && echo 17 > "$PGDATA/PG_VERSION"`)

		withLock(t, pm, func(ctx context.Context) {
			err := pm.restoreFromBackupLocked(ctx, "some-backup-id")
			require.Error(t, err)
			assert.Contains(t, err.Error(), "failed to remove restore sentinel after restore")
		})

		assert.FileExists(t, filepath.Join(dataDir, "PG_VERSION"), "the restored data is kept")
	})
}
