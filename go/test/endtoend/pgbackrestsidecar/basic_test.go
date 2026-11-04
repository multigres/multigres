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

package pgbackrestsidecar

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/pb/pgbackrestdservice"
)

// TestSidecar_SidecarDownNoWALLost stops the sidecar while WAL is produced.
// Archiving must fail (postgres keeps the segment .ready and retries), and once
// the sidecar is back every segment up to the current LSN is in the repository.
//
// It fails if archive_command ever reports success without the segment being in
// the repository (a push that returns early, or a fallback that drops the
// segment): postgres would recycle the WAL and the segment would be missing
// from the repository below.
func TestSidecar_SidecarDownNoWALLost(t *testing.T) {
	c := newSidecarCluster(t, 2)
	primary := c.setup.GetPrimary(t)
	db := openSocketDB(t, primary)

	waitArchived(t, c, primary, writeAndSwitch(t, db, "before"), 2*time.Minute)

	var failedBefore int64
	require.NoError(t, db.QueryRow("SELECT failed_count FROM pg_stat_archiver").Scan(&failedBefore))

	primary.PgBackRestd.Terminate(t.Logf, 10*time.Second)
	require.False(t, primary.PgBackRestd.IsRunning(), "the sidecar should be stopped")

	var down []string
	for range 3 {
		down = append(down, writeAndSwitch(t, db, "while-down"))
	}
	require.Eventually(t, func() bool {
		var failed int64
		return db.QueryRow("SELECT failed_count FROM pg_stat_archiver").Scan(&failed) == nil && failed > failedBefore
	}, 3*time.Minute, time.Second, "archive_command should fail while the sidecar is down")
	archiveStatus := filepath.Join(primary.Pgctld.PoolerDir, "pg_data", "pg_wal", "archive_status")
	for _, seg := range down {
		require.FileExists(t, filepath.Join(archiveStatus, seg+".ready"), "%s must stay queued", seg)
		require.False(t, repoSegments(c.s3)[seg], "%s cannot be in the repository: nothing could push it", seg)
	}

	require.NoError(t, primary.PgBackRestd.Start(c.setup.Context(), t))
	for _, seg := range down {
		waitArchived(t, c, primary, seg, 3*time.Minute)
	}
	last := writeAndSwitch(t, db, "after")
	waitArchived(t, c, primary, last, 2*time.Minute)

	// Everything postgres still holds below the current segment is in the repository.
	rows, err := db.Query(`SELECT name FROM pg_ls_waldir()
		WHERE name ~ '^[0-9A-F]{24}$' AND name < pg_walfile_name(pg_current_wal_lsn()) ORDER BY name`)
	require.NoError(t, err)
	defer rows.Close()
	inRepo := repoSegments(c.s3)
	n := 0
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		n++
		assert.True(t, inRepo[name], "segment %s is missing from the repository", name)
	}
	require.NoError(t, rows.Err())
	require.Positive(t, n)
	ready, err := filepath.Glob(filepath.Join(archiveStatus, "*.ready"))
	require.NoError(t, err)
	assert.Empty(t, ready, "no segment should still be queued")
}

// TestSidecar_OlderConfigureRejected sends a Configure whose revision is below
// the one the sidecar holds. It is refused, reports the held revision, and
// changes nothing, including across a sidecar restart.
//
// It fails if the revision check is dropped or compared the wrong way: the
// older list would be accepted and Status would move backwards.
func TestSidecar_OlderConfigureRejected(t *testing.T) {
	c := newSidecarCluster(t, 2)
	inst := c.setup.GetPrimary(t)

	before := sidecarStatus(t, inst)
	require.True(t, before.Configured, "multipooler configures the sidecar at startup")
	require.NotEmpty(t, before.Repos)

	// A different repo number, so an accepted request would be visible in Status.
	older := &pgbackrestdservice.ConfigureRequest{
		Revision: before.Revision - 1,
		Repos:    []*pgbackrestdservice.RepoGeneration{{Generation: before.Repos[0].Generation + 7, RepoNumber: before.Repos[0].RepoNumber}},
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	resp, err := controlClient(inst).Configure(ctx, connect.NewRequest(older))
	require.NoError(t, err)
	assert.False(t, resp.Msg.Accepted, "an older revision must be refused")
	assert.Equal(t, before.Revision, resp.Msg.HeldRevision)

	after := sidecarStatus(t, inst)
	assert.Equal(t, before.Revision, after.Revision)
	assert.Equal(t, before.Repos, after.Repos)

	inst.PgBackRestd.Terminate(t.Logf, 10*time.Second)
	require.NoError(t, inst.PgBackRestd.Start(c.setup.Context(), t))
	restarted := sidecarStatus(t, inst)
	assert.GreaterOrEqual(t, restarted.Revision, before.Revision, "a restart must not move the sidecar backwards")
	if restarted.Configured {
		assert.Equal(t, before.Repos, restarted.Repos)
	}
}

// legacyArchiveCommand and legacyRestoreCommand are what a pod that runs
// pgBackRest inside postgres's container leaves in postgresql.auto.conf, with
// the paths of another pooler.
func legacyArchiveCommand(poolerDir string) string {
	return "pgbackrest --stanza=multigres --config=" + poolerDir + "/pgbackrest/pgbackrest.conf archive-push %p"
}

func legacyRestoreCommand(poolerDir string) string {
	return "pgctld restore-wrapper '" + poolerDir + "/restore_command.pid' -- pgbackrest --stanza='multigres' --config='" +
		poolerDir + "/pgbackrest/pgbackrest.conf' archive-get %f \"%p\""
}

func confQuote(v string) string { return "'" + strings.ReplaceAll(v, "'", "''") + "'" }

// setAutoConfCommands rewrites archive_command and restore_command in a
// stopped postgres's postgresql.auto.conf, replacing any existing lines.
func setAutoConfCommands(t *testing.T, poolerDir, archive, restore string) {
	t.Helper()
	path := filepath.Join(poolerDir, "pg_data", "postgresql.auto.conf")
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var kept []string
	for line := range strings.SplitSeq(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "archive_command") || strings.HasPrefix(trimmed, "restore_command") {
			continue
		}
		if trimmed != "" {
			kept = append(kept, line)
		}
	}
	kept = append(kept, "archive_command = "+confQuote(archive), "restore_command = "+confQuote(restore))
	require.NoError(t, os.WriteFile(path, []byte(strings.Join(kept, "\n")+"\n"), 0o600))
}

func readAutoConf(t *testing.T, poolerDir string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(poolerDir, "pg_data", "postgresql.auto.conf"))
	require.NoError(t, err)
	return string(data)
}

// requireOwnSidecarCommands checks that auto.conf and the running server carry
// the sidecar commands for socket, and nothing else.
func requireOwnSidecarCommands(t *testing.T, conf, socket string) {
	t.Helper()
	assert.Contains(t, conf, "--unix-socket "+socket+" ", "commands must name this pooler's own WAL socket")
	assert.Contains(t, conf, "http://pgbackrest/wal/%f")
	assert.NotContains(t, conf, "archive-push", "the in-process archive_command must be gone")
	assert.NotContains(t, conf, "archive-get", "the in-process restore_command must be gone")
	assert.NotContains(t, conf, "/pgbackrest/pgbackrest.conf")
}

// TestSidecar_LegacyAutoConfConverges gives a pooler the auto.conf a pod that
// ran pgBackRest in-process leaves behind (pgBackRest commands naming another
// pooler's pgbackrest.conf) and starts it. Both commands are rewritten to the
// sidecar form with this pooler's own WAL socket before postgres starts.
//
// The in-process file is planted in the data directory while postgres is
// stopped: that is the state a volume carried over from such a pod is in when
// the multipooler starts it. It fails if normalisation only handles the sidecar
// form: the in-process archive_command would survive and every WAL push would
// exit 127.
func TestSidecar_LegacyAutoConfConverges(t *testing.T) {
	c := newSidecarCluster(t, 2)
	standby := c.setup.GetStandbys()[0]
	resume := c.setup.StopPostgres(t, standby.Name, "fast")

	foreign := filepath.Join(c.setup.TempDir, "legacy-pod", "pooler-9", "data")
	setAutoConfCommands(t, standby.Pgctld.PoolerDir, legacyArchiveCommand(foreign), legacyRestoreCommand(foreign))
	require.Contains(t, readAutoConf(t, standby.Pgctld.PoolerDir), "archive-push", "the legacy state must be in place")

	resume()
	require.Eventually(t, func() bool {
		conf := readAutoConf(t, standby.Pgctld.PoolerDir)
		return !strings.Contains(conf, "archive-push") && !strings.Contains(conf, "archive-get")
	}, 3*time.Minute, time.Second, "the commands should be rewritten when the monitor starts postgres")
	requireOwnSidecarCommands(t, readAutoConf(t, standby.Pgctld.PoolerDir), standby.PgBackRestd.Paths.WALSocket)

	db := openSocketDB(t, standby)
	var archive, restore string
	require.NoError(t, db.QueryRow("SHOW archive_command").Scan(&archive))
	require.NoError(t, db.QueryRow("SHOW restore_command").Scan(&restore))
	requireOwnSidecarCommands(t, archive+"\n"+restore, standby.PgBackRestd.Paths.WALSocket)
	assert.Contains(t, restore, "--rename", "restore_command keeps the wrapper that never leaves a partial file")
}
