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

package manager

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/parser/ast"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

func TestStartMigratesLegacyWALCommands(t *testing.T) {
	dir := t.TempDir()
	data := t.TempDir()
	t.Setenv("PGDATA", data)
	pm := newTestManager(t, withRecord(newRecordFromProto(&clustermetadatapb.Multipooler{PoolerDir: dir})))
	pm.config = &Config{WALCommandWrappers: true}
	pm.pgctldClient = &mockPgctldClient{statusResponse: stoppedWALStatus(dir, data)}
	const other = "shared_buffers = '128MB'\n"
	const archive = "archive_command = '" + backup.ArchiveWALCommand + "'"
	const restore = "restore_command = '" + backup.RestoreWALCommand + "'"
	old := fmt.Sprintf("archive_command = 'pgbackrest --stanza=multigres --config=%s/pgbackrest/pgbackrest.conf archive-push %%p'", dir)
	oldRestore := fmt.Sprintf(`restore_command = 'pgctld restore-wrapper ''%[1]s/restore_command.pid'' -- pgbackrest --stanza=''multigres'' --config=''%[1]s/pgbackrest/pgbackrest.conf'' archive-get %%f "%%p"'`, dir)
	released := `restore_command = 'pgbackrest --config=/former/root/pgbackrest/pgbackrest.conf --stanza=multigres archive-get %f "%p"'`
	custom := strings.ReplaceAll(oldRestore, "--config=''"+dir, "--config=''/different")
	for _, tc := range []struct{ name, before, after string }{
		{"relocated", strings.ReplaceAll(old+"\n"+oldRestore, dir, "/former/root"), archive + "\n" + restore},
		{"released", released, restore},
		{"different roots", custom, custom},
		{"legacy", old + "\n" + oldRestore, archive + "\n" + restore},
		{"streaming", old, archive},
		{"custom", "archive_command = 'custom'\nrestore_command = ''", "archive_command = 'custom'\nrestore_command = ''"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(data, "postgresql.auto.conf")
			require.NoError(t, os.WriteFile(path, []byte(other+tc.before+"\n"), 0o600))
			require.NoError(t, pm.startPostgres(t.Context()))
			got, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, other+tc.after+"\n", string(got))
			stamp := time.Unix(100, 0)
			require.NoError(t, os.Chtimes(path, stamp, stamp))
			require.NoError(t, pm.startPostgres(t.Context()))
			fi, err := os.Stat(path)
			require.NoError(t, err)
			require.True(t, stamp.Equal(fi.ModTime()))
		})
	}
}

func TestOldPgctldLeavesWALCommandsAlone(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("PGDATA", dir)
	pm := newTestManager(t, withRecord(newRecordFromProto(&clustermetadatapb.Multipooler{PoolerDir: dir})))
	pm.config = &Config{WALCommandWrappers: true}
	pm.pgctldClient = &mockPgctldClient{statusResponse: &pgctldpb.StatusResponse{}}
	file := filepath.Join(dir, "postgresql.auto.conf")
	legacy, _ := backup.LegacyWALCommands(backup.ClientConfigPath(dir), dir)
	content := "archive_command = '" + legacy + "'\n"
	require.NoError(t, os.WriteFile(file, []byte(content), 0o600))
	require.NoError(t, pm.startPostgres(t.Context()))
	got, err := os.ReadFile(file)
	require.NoError(t, err)
	require.Equal(t, content, string(got))
}

// A later failed or inconclusive observation cannot reuse an earlier success.
func TestWALWritesRequireFreshMatchingCapability(t *testing.T) {
	dir := t.TempDir()
	pm := newTestManager(t, withRecord(newRecordFromProto(&clustermetadatapb.Multipooler{PoolerDir: dir})))
	pm.config = &Config{WALCommandWrappers: true}
	pg := &walStatusClient{response: stoppedWALStatus(dir, dir)}
	pm.pgctldClient = pg
	require.True(t, pm.supportsWALWrappers(t.Context()))
	require.Equal(t, []bool{true}, pg.forced)

	pg.err = errors.New("status unavailable")
	require.False(t, pm.supportsWALWrappers(t.Context()))
	pg.err = nil
	pg.response.WalWrapperProbeUnknown = true // Even a nonzero version is inconclusive.
	require.False(t, pm.supportsWALWrappers(t.Context()))
	pg.response = stoppedWALStatus("", dir)
	require.False(t, pm.supportsWALWrappers(t.Context()))
	pg.response = stoppedWALStatus(t.TempDir(), dir)
	require.False(t, pm.supportsWALWrappers(t.Context()))
	pg.response = stoppedWALStatus(dir, dir)
	require.True(t, pm.supportsWALWrappers(t.Context()))
	require.Equal(t, []bool{true, true, true, true, true, true}, pg.forced)
}

func stoppedWALStatus(dir, data string) *pgctldpb.StatusResponse {
	return &pgctldpb.StatusResponse{WalWrapperVersion: backup.WALWrapperVersion, PoolerDir: dir, DataDir: data, Status: pgctldpb.ServerStatus_STOPPED}
}

type walStatusClient struct {
	mockPgctldClient
	response *pgctldpb.StatusResponse
	err      error
	forced   []bool
	onStatus func()
}

func (p *walStatusClient) Status(_ context.Context, req *pgctldpb.StatusRequest, _ ...grpc.CallOption) (*pgctldpb.StatusResponse, error) {
	p.forced = append(p.forced, req.GetRecheckWalWrappers())
	if p.onStatus != nil {
		p.onStatus()
	}
	return p.response, p.err
}

func TestLivePostmasterDoesNotScheduleWALConversion(t *testing.T) {
	pm := newTestManager(t)
	pm.config = &Config{WALCommandWrappers: true}
	state := postgresState{pgctldAvailable: true, dirInitialized: true, postgresRunning: true, postgresReady: true}
	require.Equal(t, remedialActionNone, pm.determineRemedialAction(t.Context(), state))
}

func TestStartDoesNotMigrateAmbiguousOrRunningPostgres(t *testing.T) {
	for _, tc := range []string{"running", "ready with stopped status", "pid with stopped status", "wrong PGDATA", "stale pid file", "malformed pid file", "dangling pid link", "pid appears during status"} {
		t.Run(tc, func(t *testing.T) {
			dir, data := t.TempDir(), t.TempDir()
			t.Setenv("PGDATA", data)
			pm := newTestManager(t, withRecord(newRecordFromProto(&clustermetadatapb.Multipooler{PoolerDir: dir})))
			pm.config = &Config{WALCommandWrappers: true}
			pg := &walStatusClient{response: stoppedWALStatus(dir, data)}
			pm.pgctldClient = pg
			pid := filepath.Join(data, "postmaster.pid")
			path := filepath.Join(data, "postgresql.auto.conf")
			legacy, _ := backup.LegacyWALCommands(backup.ClientConfigPath(dir), dir)
			content := "archive_command = '" + legacy + "'\n"
			require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
			switch tc {
			case "running":
				pg.response.Status = pgctldpb.ServerStatus_RUNNING
			case "ready with stopped status":
				pg.response.Ready = true
			case "pid with stopped status":
				pg.response.Pid = 123
			case "wrong PGDATA":
				pg.response.DataDir = t.TempDir()
			case "stale pid file":
				require.NoError(t, os.WriteFile(pid, []byte("2147483647\n"), 0o600))
			case "malformed pid file":
				require.NoError(t, os.WriteFile(pid, nil, 0o600))
			case "dangling pid link":
				require.NoError(t, os.Symlink(filepath.Join(data, "missing"), pid))
			case "pid appears during status":
				pg.onStatus = func() { require.NoError(t, os.WriteFile(pid, nil, 0o600)) }
			}
			require.NoError(t, pm.startPostgres(t.Context()))
			require.True(t, pg.startCalled)
			got, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, content, string(got))
		})
	}
}

type rolloutBootstrapPgctld struct {
	*stepRecordingPgctldClient
	version uint32
}

func (p *rolloutBootstrapPgctld) Status(context.Context, *pgctldpb.StatusRequest, ...grpc.CallOption) (*pgctldpb.StatusResponse, error) {
	return &pgctldpb.StatusResponse{WalWrapperVersion: p.version}, nil
}

func TestLegacyWALCommandRollout(t *testing.T) {
	for _, tc := range []struct {
		name    string
		enabled bool
		version uint32
	}{{"enabled but unavailable", true, 0}, {"disabled but available", false, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			captured := filepath.Join(t.TempDir(), "captured")
			t.Setenv("CAPTURE", captured)
			h := newBootstrapStepHarness(t, func(conf, _ string) string { return fmt.Sprintf("#!/bin/sh\ncp %q \"$CAPTURE\"\n", conf) })
			h.pm.config.WALCommandWrappers = tc.enabled
			h.pm.pgctldClient = &rolloutBootstrapPgctld{h.pgctld, tc.version}
			require.ErrorContains(t, h.run(t), "failed to start PostgreSQL")
			data, err := os.ReadFile(captured)
			require.NoError(t, err)
			archive, _ := backup.LegacyWALCommands(backup.ClientConfigPath(h.poolerDir), h.poolerDir)
			require.Contains(t, string(data), "archive_command = '"+archive+"'")
			pm, pgdata := newMockPgbackrestRestoreManager(t, `printf '%s\n' "$@" > "$CAPTURE"; exit 1`)
			pm.config = &Config{WALCommandWrappers: tc.enabled}
			pm.pgctldClient = &mockPgctldClient{statusResponse: &pgctldpb.StatusResponse{WalWrapperVersion: tc.version}}
			withLock(t, pm, func(ctx context.Context) {
				require.ErrorContains(t, pm.restoreFromBackupLocked(ctx, "backup"), "pgbackrest restore failed")
				data, err := os.ReadFile(captured)
				require.NoError(t, err)
				dir := pm.record.PoolerDir()
				_, restore := backup.LegacyWALCommands(filepath.Join(filepath.Dir(dir), "pgbackrest.conf"), dir)
				require.Contains(t, string(data), "--recovery-option=restore_command="+strings.ReplaceAll(restore, "'", "''")+"\n")
				require.NoError(t, os.MkdirAll(pgdata, 0o700))
				conf := filepath.Join(pgdata, "postgresql.auto.conf")
				require.NoError(t, os.WriteFile(conf, nil, 0o600))
				require.Equal(t, []string{"--restore-target-wal"}, pm.prepareRewindRestoreSource(ctx))
				data, err = os.ReadFile(conf)
				require.NoError(t, err)
				quoted, err := ast.QuoteConfValue(restore)
				require.NoError(t, err)
				require.Contains(t, string(data), quoted)
			})
		})
	}
}

func TestOfflineWALMigrationFailureDoesNotBlockStart(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("PGDATA", dir)
	path := filepath.Join(dir, "postgresql.auto.conf")
	target := filepath.Join(dir, "real.conf")
	archive, _ := backup.LegacyWALCommands(backup.ClientConfigPath(dir), dir)
	content := "archive_command = '" + archive + "'\n"
	require.NoError(t, os.WriteFile(target, []byte(content), 0o600))
	require.NoError(t, os.Symlink(target, path))
	pm := newTestManager(t, withRecord(newRecordFromProto(&clustermetadatapb.Multipooler{PoolerDir: dir})))
	pm.config = &Config{WALCommandWrappers: true}
	pg := &mockPgctldClient{statusResponse: stoppedWALStatus(dir, dir)}
	pm.pgctldClient = pg
	var logs bytes.Buffer
	pm.logger = slog.New(slog.NewTextHandler(&logs, nil))
	require.NoError(t, pm.startPostgres(t.Context()))
	require.True(t, pg.startCalled)
	require.Contains(t, logs.String(), "continuing with existing settings")
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, content, string(got))
}

func TestDisabledWALCommandsDoNotMigrate(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("PGDATA", dir)
	pm := newTestManager(t, withRecord(newRecordFromProto(&clustermetadatapb.Multipooler{PoolerDir: dir})))
	pm.config = &Config{}
	pm.pgctldClient = &mockPgctldClient{statusResponse: stoppedWALStatus(dir, dir)}
	archive, _ := backup.LegacyWALCommands(backup.ClientConfigPath(dir), dir)
	path := filepath.Join(dir, "postgresql.auto.conf")
	content := "archive_command = '" + archive + "'\n"
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	require.NoError(t, pm.startPostgres(t.Context()))
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, content, string(got))
}

func TestOfflineWALReadinessInventory(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("PGDATA", dir)
	pm := newTestManager(t, withRecord(newRecordFromProto(&clustermetadatapb.Multipooler{PoolerDir: dir})))
	p := filepath.Join(dir, "postgresql.auto.conf")
	pm.config = &Config{}
	write := func(s string) { require.NoError(t, os.WriteFile(p, []byte(s), 0o600)) }
	write("archive_command = 'pgbackrest custom push'\n")
	require.Equal(t, "wal_command_unrecognized", pm.walCommandReadiness(t.Context()))
	write("restore_command = '" + backup.RestoreWALCommand + "'\n")
	pm.pgctldClient = &mockPgctldClient{statusResponse: &pgctldpb.StatusResponse{Status: pgctldpb.ServerStatus_STOPPED}}
	require.Equal(t, "wal_wrapper_unavailable", pm.walCommandReadiness(t.Context()))
	pm.pgctldClient = &mockPgctldClient{statusError: errors.New("unreachable")}
	require.Equal(t, "wal_wrapper_unverified", pm.walCommandReadiness(t.Context()))
	got, err := os.ReadFile(p)
	require.NoError(t, err)
	require.Equal(t, "restore_command = '"+backup.RestoreWALCommand+"'\n", string(got))
}

func TestWALReadinessBoundsStatusAndDoesNotForceHandshake(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("PGDATA", dir)
	pm := newTestManager(t, withRecord(newRecordFromProto(&clustermetadatapb.Multipooler{PoolerDir: dir})))
	path := filepath.Join(dir, "postgresql.auto.conf")
	require.NoError(t, os.WriteFile(path, []byte("archive_command = '"+backup.ArchiveWALCommand+"'\n"), 0o600))
	pg := &walStatusClient{response: stoppedWALStatus(dir, dir)}
	pm.pgctldClient = pg
	require.Empty(t, pm.walCommandReadiness(t.Context()))
	require.Equal(t, []bool{false}, pg.forced)
	pg.response.WalWrapperProbeUnknown = true
	require.Equal(t, "wal_wrapper_unverified", pm.walCommandReadiness(t.Context()))
	pg.response.WalWrapperProbeUnknown = false
	pg.response.Status = pgctldpb.ServerStatus_RUNNING
	pg.response.Ready = true
	// The admin query service is unavailable even though pgctld sees Postgres.
	require.Equal(t, "wal_wrapper_unverified", pm.walCommandReadiness(t.Context()))

	pm.pgctldClient = &mockPgctldClient{statusBlockForever: true}
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		require.Equal(t, "wal_wrapper_unverified", pm.walCommandReadiness(t.Context()))
		require.Equal(t, walReadinessTimeout, time.Since(start))
	})

	require.NoError(t, os.Remove(path))
	require.Empty(t, pm.walCommandReadiness(t.Context()))
	require.NoError(t, os.Mkdir(path, 0o700))
	require.Equal(t, "wal_wrapper_unverified", pm.walCommandReadiness(t.Context()))
}

func TestReadWALInventoryRejectsNonRegularAndOversize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "postgresql.auto.conf")
	require.NoError(t, syscall.Mkfifo(path, 0o600))
	_, err := readWALInventory(path)
	require.Error(t, err)
	require.NoError(t, os.Remove(path))
	require.NoError(t, os.WriteFile(path, nil, 0o600))
	require.NoError(t, os.Truncate(path, 2<<20))
	_, err = readWALInventory(path)
	require.Error(t, err)
}
