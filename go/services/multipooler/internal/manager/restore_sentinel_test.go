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
	"log/slog"
	"os"
	"path/filepath"
	"strings"
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

// TestDiscoverPostgresState_RestoreSentinel verifies discoverPostgresState
// surfaces the on-disk restore sentinel, the durable signal the monitor gates on,
// and checks the backup repository for an interrupted restore just as it does
// for a missing data directory.
func TestDiscoverPostgresState_RestoreSentinel(t *testing.T) {
	t.Run("no sentinel", func(t *testing.T) {
		pm := NewTestMultipoolerManager(t)
		pm.pgctldClient = &mockPgctldClient{
			statusResponse: &pgctldpb.StatusResponse{Status: pgctldpb.ServerStatus_STOPPED},
		}

		state, err := pm.discoverPostgresState(t.Context())
		require.NoError(t, err)
		assert.False(t, state.restoreSentinelPresent)
	})

	t.Run("sentinel does not read the repository", func(t *testing.T) {
		// The sidecar closes info while its restore runs, so the read would fail
		// and the tick would be skipped, hiding a wait that must age toward
		// quarantine. restoreAndStartPostgres lists the backups itself once the
		// sidecar has confirmed that no restore runs.
		pm, _, _ := newAutoRestoreTestManager(t, `echo "ERROR: [104]: unable to reach repository"; exit 1`, "")

		state, err := pm.discoverPostgresState(t.Context())
		require.NoError(t, err)
		assert.True(t, state.dirInitialized, "the partial restore holds PG_VERSION")
		assert.True(t, state.restoreSentinelPresent)
		assert.False(t, state.backupsAvailable)
	})

	t.Run("sentinel with postgres running does not check backups", func(t *testing.T) {
		// This manager's backup repository can't be read, so checking it would
		// fail the tick and hold up remediation of a running postgres.
		pm := newRunningStandbyManagerForTest(t, &fakeRuleStore{pos: makeRulePosition(1)})
		require.NoError(t, pm.restoreSentinel().write())

		state, err := pm.discoverPostgresState(t.Context())
		require.NoError(t, err)
		assert.True(t, state.postgresRunning)
		assert.True(t, state.restoreSentinelPresent)
		assert.False(t, state.backupsAvailable)
	})
}

// TestDiscoverPostgresState_UnreadableRestoreSentinel verifies an unreadable
// sentinel is treated as present (an unsettled restore), never as absent, so
// postgres is not started on a possibly partial restore, and the tick is not
// skipped, so the wait ages toward quarantine.
func TestDiscoverPostgresState_UnreadableRestoreSentinel(t *testing.T) {
	pm := NewTestMultipoolerManager(t)
	pm.pgctldClient = &mockPgctldClient{
		statusResponse: &pgctldpb.StatusResponse{Status: pgctldpb.ServerStatus_STOPPED},
	}
	// A self-referencing symlink makes the stat fail with ELOOP, which is not a
	// not-exist error.
	require.NoError(t, os.Symlink(pm.restoreSentinel().path, pm.restoreSentinel().path))

	state, err := pm.discoverPostgresState(t.Context())
	require.NoError(t, err)
	assert.True(t, state.restoreSentinelPresent)
}

// diskStatusPgctldClient reports STOPPED or NOT_INITIALIZED based on whether
// PG_VERSION is on disk, like the real pgctld (IsDataDirInitialized), so the
// status re-check in restoreAndStartPostgres sees a partial data directory as
// initialized, as it would in production.
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

// completeBackupInfo is pgbackrest info output listing one complete backup of
// the default shard, so ListBackups finds a backup to restore from.
const completeBackupInfo = `[{"backup":[{"label":"20260101-000000F","error":false,"timestamp":{"start":1735689600,"stop":1735689660},"annotation":{"job_id":"test-job-id","multipooler_id":"test-multipooler","pooler_type":"REPLICA","table_group":"default","shard":"0-inf"}}]}]`

// printCompleteBackupInfo is a mock pgbackrest info script that prints
// completeBackupInfo.
const printCompleteBackupInfo = "cat <<'JSONEOF'\n" + completeBackupInfo + "\nJSONEOF"

// newAutoRestoreTestManager builds a standby manager for restoreAndStartPostgres
// with the on-disk state an interrupted restore leaves behind: a partial PGDATA
// and the restore sentinel. Its mock pgbackrest runs infoScript for info and
// restoreScript for restore, with PGDATA and OBSERVED (a file for the scripts
// to record what they saw) in their environment. It returns the manager, its
// PGDATA path and the OBSERVED path.
func newAutoRestoreTestManager(t *testing.T, infoScript, restoreScript string) (*MultipoolerManager, string, string) {
	t.Helper()
	tmpDir := t.TempDir()
	poolerDir := filepath.Join(tmpDir, "pooler")
	require.NoError(t, os.MkdirAll(poolerDir, 0o755))
	observed := filepath.Join(tmpDir, "observed")
	t.Setenv("OBSERVED", observed)

	binDir := filepath.Join(tmpDir, "bin")
	require.NoError(t, os.MkdirAll(binDir, 0o755))
	mockScript := "#!/bin/bash\ncase \"${@: -1}\" in\ninfo)\n" + infoScript + "\n;;\nrestore)\n" + restoreScript + "\n;;\nesac\nexit 0\n"
	require.NoError(t, os.WriteFile(filepath.Join(binDir, "pgbackrest"), []byte(mockScript), 0o755))
	t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))

	pm := createTestManagerWithBackupLocation(t, poolerDir, "", "", clustermetadatapb.PoolerType_REPLICA, filepath.Join(tmpDir, "backups"))
	pm.pgctldClient = &diskStatusPgctldClient{}

	dataDir := plantPartialRestore(t, pm)
	require.NoError(t, pm.restoreSentinel().write())
	return pm, dataDir, observed
}

// TestRestoreAndStartPostgres_RestoreSentinelRemovesPartialDataDir is the core
// of the fix: after a crash mid-restore, the restore must go ahead even though
// the partial PGDATA already contains PG_VERSION, which the pgctld status
// re-check reads as "initialized", and the partial PGDATA must be gone before
// pgbackrest restores into it.
func TestRestoreAndStartPostgres_RestoreSentinelRemovesPartialDataDir(t *testing.T) {
	// The mock restore records whether the partial data was still there, then
	// fails so the test stops before postgres would be started.
	pm, dataDir, observed := newAutoRestoreTestManager(t, printCompleteBackupInfo, `
[[ -e "$PGDATA/partial_marker" ]] && echo stale >> "$OBSERVED"
echo ran >> "$OBSERVED"
exit 1`)

	withLock(t, pm, func(ctx context.Context) {
		err := pm.restoreAndStartPostgres(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "pgbackrest restore failed",
			"the restore must run instead of being skipped by the status re-check")
	})

	data, err := os.ReadFile(observed)
	require.NoError(t, err, "pgbackrest restore must have run")
	assert.Equal(t, "ran\n", string(data), "the partial data directory must be removed before restoring into it")
	assert.NoDirExists(t, dataDir, "the failed restore is cleaned up")
}

// TestRestoreAndStartPostgres_RestoreSentinelKeepsDataDirWithoutBackup verifies
// the partial data directory is only removed once there is a backup to restore
// from. If the repository can't be read or holds no complete backup, the node
// keeps both its data directory and the sentinel, and the next attempt retries.
func TestRestoreAndStartPostgres_RestoreSentinelKeepsDataDirWithoutBackup(t *testing.T) {
	tests := []struct {
		name       string
		infoScript string
		wantErr    string
	}{
		{
			name:       "repository unreachable",
			infoScript: `echo "ERROR: [104]: unable to reach repository"; exit 1`,
			wantErr:    "failed to list backups",
		},
		{
			name:       "no complete backup",
			infoScript: strings.Replace(printCompleteBackupInfo, `"error":false`, `"error":true`, 1),
			wantErr:    "no complete backups available",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm, dataDir, observed := newAutoRestoreTestManager(t, tt.infoScript, `echo ran >> "$OBSERVED"`)

			withLock(t, pm, func(ctx context.Context) {
				err := pm.restoreAndStartPostgres(ctx)
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			})

			assert.FileExists(t, filepath.Join(dataDir, "partial_marker"),
				"the data directory must be kept while there is nothing to restore from")
			present, err := pm.restoreSentinel().present()
			require.NoError(t, err)
			assert.True(t, present, "the sentinel must stay until a restore completes")
			assert.NoFileExists(t, observed, "pgbackrest restore must not run")
		})
	}
}

// TestRestoreAndStartPostgres_RestoreSentinelKeepsDataDirUnderRunningPostgres
// verifies the partial data directory is never removed while postgres might be
// running on it: not when pgctld reports it running, not when its status can't
// be read, and not when postmaster.pid shows a postmaster that pgctld reports as
// STOPPED because it is still starting up.
func TestRestoreAndStartPostgres_RestoreSentinelKeepsDataDirUnderRunningPostgres(t *testing.T) {
	tests := []struct {
		name    string
		client  *mockPgctldClient       // nil keeps the STOPPED status of the partial restore
		pidFile func(path string) error // plants postmaster.pid
		wantErr string
	}{
		{
			name:    "postgres running",
			client:  &mockPgctldClient{statusResponse: &pgctldpb.StatusResponse{Status: pgctldpb.ServerStatus_RUNNING}},
			wantErr: "postgres is running on the data directory of an interrupted restore",
		},
		{
			name:    "status unavailable",
			client:  &mockPgctldClient{statusError: errors.New("pgctld unavailable")},
			wantErr: "failed to check postgres status",
		},
		{
			name:    "postmaster still starting up",
			pidFile: func(path string) error { return os.WriteFile(path, []byte("12345\n"), 0o600) },
			wantErr: "postmaster.pid exists in the data directory of an interrupted restore",
		},
		{
			// A self-referencing symlink makes the stat fail with ELOOP, which
			// must not be mistaken for "no postmaster.pid".
			name:    "postmaster.pid unreadable",
			pidFile: func(path string) error { return os.Symlink(path, path) },
			wantErr: "failed to check for",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm, dataDir, observed := newAutoRestoreTestManager(t, printCompleteBackupInfo, `echo ran >> "$OBSERVED"`)
			if tt.client != nil {
				pm.pgctldClient = tt.client
			}
			if tt.pidFile != nil {
				require.NoError(t, tt.pidFile(filepath.Join(dataDir, constants.PostmasterPIDFile)))
			}

			withLock(t, pm, func(ctx context.Context) {
				err := pm.restoreAndStartPostgres(ctx)
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			})

			assert.FileExists(t, filepath.Join(dataDir, "partial_marker"), "the data directory must be kept")
			assert.NoFileExists(t, observed, "pgbackrest restore must not run")
		})
	}
}

// TestRestoreAndStartPostgres_NoSentinelKeepsInitializedDataDir guards the
// boundary: without a sentinel, an initialized data directory is left alone and
// the restore is skipped, as before.
func TestRestoreAndStartPostgres_NoSentinelKeepsInitializedDataDir(t *testing.T) {
	pm := newSentinelTestManager(t)
	dataDir := plantPartialRestore(t, pm)
	pm.pgctldClient = &diskStatusPgctldClient{}
	pm.backup = newStubEngine(pm.logger, pm.record, backupengine.Settings{})

	withLock(t, pm, func(ctx context.Context) {
		require.NoError(t, pm.restoreAndStartPostgres(ctx), "an initialized data directory skips the restore")
	})

	assert.FileExists(t, filepath.Join(dataDir, "PG_VERSION"), "the data directory must not be touched")
}

// TestRestoreAndStartPostgres_PartialDataDirRemovalFails verifies a partial data
// directory that cannot be removed fails the attempt instead of restoring or
// starting postgres on top of it.
func TestRestoreAndStartPostgres_PartialDataDirRemovalFails(t *testing.T) {
	pm, dataDir, observed := newAutoRestoreTestManager(t, printCompleteBackupInfo, `echo ran >> "$OBSERVED"`)
	// removeDataDirectory refuses to delete $HOME, which makes the removal fail.
	t.Setenv("HOME", dataDir)

	withLock(t, pm, func(ctx context.Context) {
		err := pm.restoreAndStartPostgres(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to remove partial data directory from interrupted restore")
	})

	assert.FileExists(t, filepath.Join(dataDir, "PG_VERSION"), "the data directory removal was refused")
	assert.NoFileExists(t, observed, "pgbackrest restore must not run on top of the partial data directory")
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
		assert.Contains(t, err.Error(), "cannot read the restore marker")
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
	pm.backup = newStubEngine(pm.logger, pm.record, backupengine.Settings{})
	pm.stateManager = NewStateManager(pm.logger, pm.record, func() *clustermetadatapb.ConsensusStatus { return nil })
	useAnsweringSidecar(t, pm)
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
// restore never runs unprotected: if the sentinel can't be read or written, it
// aborts before pgbackrest touches PGDATA.
func TestRestoreFromBackupLocked_SentinelWriteFailureAbortsRestore(t *testing.T) {
	t.Setenv(constants.PgDataDirEnvVar, filepath.Join(t.TempDir(), "pg_data"))
	pm := newRestoreTestManager(t, filepath.Join(t.TempDir(), "no", "such", "dir"))

	withLock(t, pm, func(ctx context.Context) {
		err := pm.restoreFromBackupLocked(ctx, "some-backup-id")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "restore marker")
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

	pm := newRestoreTestManager(t, poolerDir)
	return pm, dataDir
}

// TestRestoreFromBackupLocked_SentinelCoversOnlyTheRestore verifies the sentinel
// is on disk while pgbackrest restore runs and stays until the steps after it
// ran, so a failure in a later step costs a redundant restore on the next
// attempt and never a postgres that skipped the archive reconfiguration, and
// does not get the restored data discarded.
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
	assert.FileExists(t, pm.restoreSentinel().path, "the sentinel stays until the post-restore steps completed")
	assert.FileExists(t, filepath.Join(dataDir, "PG_VERSION"),
		"a failure after a successful restore must not discard the restored data")
}

// TestRestoreFromBackupLocked_SentinelRemovalFails covers a failed restore whose
// sentinel is not a plain file: nothing is deleted and the restore error is
// returned. The mock pgbackrest swaps the sentinel for a non-empty directory.
func TestRestoreFromBackupLocked_SentinelRemovalFails(t *testing.T) {
	const swapSentinel = `rm -f "$SENTINEL" && mkdir "$SENTINEL" && touch "$SENTINEL/child"`

	t.Run("after a failed restore the restore error is returned", func(t *testing.T) {
		pm, _ := newMockPgbackrestRestoreManager(t, swapSentinel+"\nexit 1")

		withLock(t, pm, func(ctx context.Context) {
			err := pm.restoreFromBackupLocked(ctx, "some-backup-id")
			require.Error(t, err)
			assert.Contains(t, err.Error(), "pgbackrest restore failed")
		})

		assert.DirExists(t, pm.restoreSentinel().path, "the sentinel could not be removed")
	})
}
