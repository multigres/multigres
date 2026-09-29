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

package backupfaults

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/s3mock"
	"github.com/multigres/multigres/go/test/utils"
)

// TestAsyncWALArchive_PushRetryAndArchiveReplay exercises asynchronous WAL
// archiving (archive-async=y, the only mode multigres renders) end to end
// against the s3mock, on the durability properties that matter:
//
//  1. Successful archival: a forced WAL switch on the primary lands the
//     segment in the repo through pgbackrest's async archive-push process
//     and postgres marks it done.
//  2. Retry after a storage outage: while WAL PUTs fail, archive_command
//     fails and the segment stays queued in pg_wal/archive_status with no
//     acknowledgement in the spool (never reported archived, never dropped);
//     once storage recovers the queued segment is pushed with no operator
//     action.
//  3. Restore and replay: a fresh pooler restores from the bootstrap backup
//     and replays every archived segment, including the one that was
//     retried, through async archive-get.
func TestAsyncWALArchive_PushRetryAndArchiveReplay(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end test (no postgres binaries available)")
	}

	// WAL-segment PUTs fail (HTTP 500) while the outage flag is set; stanza
	// metadata and backup writes always pass so bootstrap is unaffected.
	var walOutage atomic.Bool
	s3Server, err := s3mock.NewServer(0, s3mock.WithPutCallback(
		func(_ context.Context, _ string, key string) error {
			if walOutage.Load() && strings.Contains(key, "/archive/") && walSegmentKey.MatchString(key) {
				return fmt.Errorf("injected archive storage outage for %s", key)
			}
			return nil
		},
	))
	require.NoError(t, err)
	defer func() { _ = s3Server.Stop() }()
	require.NoError(t, s3Server.CreateBucket("multigres"))

	// pgBackRest demands AWS credentials; s3mock does not validate them.
	t.Setenv("AWS_ACCESS_KEY_ID", "test-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
	os.Unsetenv("AWS_SESSION_TOKEN")

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(2),
		shardsetup.WithS3Backup("multigres", "us-east-1", s3Server.Endpoint()),
	)
	defer cleanup()

	primary := setup.GetPrimary(t)
	require.NotNil(t, primary)
	pgbackrestDir := filepath.Join(primary.Pgctld.PoolerDir, "pgbackrest")

	// The rendered conf is the contract under test.
	conf, err := os.ReadFile(filepath.Join(pgbackrestDir, "pgbackrest.conf"))
	require.NoError(t, err)
	require.Regexp(t, `(?m)^archive-async=y$`, string(conf))
	require.NotRegexp(t, `(?m)^archive-push-queue-max=`, string(conf), "a push queue limit would let pgbackrest drop WAL")

	primaryDB := connectToPostgresViaSocket(t, filepath.Join(primary.Pgctld.PoolerDir, "pg_sockets"), primary.Pgctld.PgPort)
	defer primaryDB.Close()
	_, err = primaryDB.Exec("CREATE TABLE async_archive_test (id serial PRIMARY KEY, phase text NOT NULL)")
	require.NoError(t, err)

	archiveStatus := filepath.Join(primary.Pgctld.PoolerDir, "pg_data", "pg_wal", "archive_status")
	spoolOut := filepath.Join(pgbackrestDir, "spool", "archive", "multigres", "out")

	// --- 1. Successful archival through the async process. In async mode the
	// foreground archive-push logs only to its console, which postgres
	// captures into its own log.
	seg1 := writeAndSwitchWAL(t, primaryDB, "archived")
	waitForSegmentArchived(t, s3Server, archiveStatus, seg1)
	requireLogLine(t, filepath.Join(primary.Pgctld.PoolerDir, "pg_data", "postgresql.log"),
		fmt.Sprintf("pushed WAL file '%s' to the archive asynchronously", seg1),
		"archive_command must have gone through the async path")
	t.Logf("Segment %s archived asynchronously", seg1)

	// --- 2. Storage outage: archive_command must fail, and the segment must
	// stay queued in archive_status with no ack in the spool.
	walOutage.Store(true)
	seg2 := writeAndSwitchWAL(t, primaryDB, "queued-during-outage")
	require.Eventually(t, func() bool {
		var lastFailed sql.NullString
		if err := primaryDB.QueryRow("SELECT last_failed_wal FROM pg_stat_archiver").Scan(&lastFailed); err != nil {
			return false
		}
		return lastFailed.String == seg2
	}, 90*time.Second, 1*time.Second, "archive_command should fail for %s while WAL PUTs are rejected", seg2)
	require.FileExists(t, filepath.Join(archiveStatus, seg2+".ready"),
		"an unarchived segment must stay queued in archive_status")
	require.NoFileExists(t, filepath.Join(archiveStatus, seg2+".done"),
		"postgres must not have been told the segment was archived")
	require.NoFileExists(t, filepath.Join(spoolOut, seg2+".ok"),
		"no acknowledgement may exist in the spool for a segment the repo never received")
	require.Empty(t, walKeysFor(s3Server, seg2))
	t.Logf("Segment %s correctly held back during the outage", seg2)

	// Storage recovers. The next WAL switch wakes the archiver so the retry
	// does not have to wait out its 60s backoff; it retries the oldest
	// .ready first, so seg2 goes before seg3.
	walOutage.Store(false)
	seg3 := writeAndSwitchWAL(t, primaryDB, "after-outage")
	waitForSegmentArchived(t, s3Server, archiveStatus, seg2)
	waitForSegmentArchived(t, s3Server, archiveStatus, seg3)
	t.Logf("Segments %s and %s archived after the outage cleared", seg2, seg3)

	// --- 3. A fresh pooler restores from the bootstrap backup and replays
	// the archive. Nothing wires it to a streaming primary (no multiorch),
	// so every row it sees came through restore_command = async archive-get.
	const restoredName = "pooler-3"
	restored := setup.CreateMultipoolerInstance(t, restoredName,
		utils.GetFreePort(t), utils.GetFreePort(t), utils.GetFreePort(t))
	require.NoError(t, restored.Pgctld.Start(t.Context(), t))
	require.NoError(t, restored.Multipooler.Start(t.Context(), t))
	shardsetup.WaitForEvent(t, restored.Multipooler.LogFile, "restore.attempt", "success", 120*time.Second)
	shardsetup.WaitForManagerReady(t, restored.Multipooler)

	restoredDB := connectToPostgresViaSocket(t, filepath.Join(restored.Pgctld.PoolerDir, "pg_sockets"), restored.Pgctld.PgPort)
	defer restoredDB.Close()
	require.Eventually(t, func() bool {
		var n int
		if err := restoredDB.QueryRow("SELECT count(*) FROM async_archive_test").Scan(&n); err != nil {
			return false
		}
		return n == 3
	}, 120*time.Second, 1*time.Second, "restored pooler should replay every archived segment")

	requireLogLine(t, filepath.Join(restored.Pgctld.PoolerDir, "pg_data", "postgresql.log"),
		fmt.Sprintf("found %s in the archive asynchronously", seg2),
		"the retried segment must have been replayed through async archive-get")
	t.Logf("%s replayed the archive, including retried segment %s, through async archive-get", restoredName, seg2)
}

// requireLogLine waits briefly for line to appear in the log at path: the
// process that wrote it has already finished its work, but its output may
// not have hit the file yet.
func requireLogLine(t *testing.T, path, line, msg string) {
	t.Helper()
	require.Eventually(t, func() bool {
		data, err := os.ReadFile(path)
		return err == nil && strings.Contains(string(data), line)
	}, 30*time.Second, 500*time.Millisecond, "%s: %q not found in %s", msg, line, path)
}

// writeAndSwitchWAL commits one row, then forces a WAL switch so the segment
// holding that row becomes eligible for archive_command. Returns that
// segment's name.
func writeAndSwitchWAL(t *testing.T, db *sql.DB, phase string) string {
	t.Helper()
	_, err := db.Exec("INSERT INTO async_archive_test (phase) VALUES ($1)", phase)
	require.NoError(t, err)
	var segment string
	require.NoError(t, db.QueryRow("SELECT pg_walfile_name(pg_switch_wal())").Scan(&segment))
	return segment
}

// waitForSegmentArchived waits until postgres has marked the segment done
// (archive_command returned success) and the segment is in the repo.
func waitForSegmentArchived(t *testing.T, s3Server *s3mock.Server, archiveStatus, segment string) {
	t.Helper()
	require.Eventually(t, func() bool {
		if _, err := os.Stat(filepath.Join(archiveStatus, segment+".done")); err != nil {
			return false
		}
		return len(walKeysFor(s3Server, segment)) > 0
	}, 120*time.Second, 1*time.Second, "segment %s should be archived", segment)
}

// walKeysFor returns the archive keys in the repo for the given WAL segment.
func walKeysFor(s3Server *s3mock.Server, segment string) []string {
	var keys []string
	for _, key := range s3Server.ListKeys("multigres", "") {
		if strings.Contains(key, "/archive/") && strings.Contains(key, segment) {
			keys = append(keys, key)
		}
	}
	return keys
}
