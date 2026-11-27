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

package shardsetup

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/test/endtoend/clustersetup"
	"github.com/multigres/multigres/go/test/s3mock"
	"github.com/multigres/multigres/go/test/utils"
)

const sidecarSmokeBucket = "multigres"

// requireSidecarHost skips a test that cannot run in this invocation, and says
// why, then applies the sidecar's host requirement (see
// clustersetup.RequireSidecarHost): a skipped sidecar test proves nothing.
func requireSidecarHost(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end test (no postgres binaries available)")
	}
	clustersetup.RequireSidecarHost(t)
}

// TestSidecarSmoke runs a one-shard, three-pooler cluster with pgBackRest in a
// pgbackrestd sidecar per pooler and an s3mock repository that only accepts
// short-lived session credentials, then takes a backup from a replica and
// pushes WAL through the sidecar.
func TestSidecarSmoke(t *testing.T) {
	requireSidecarHost(t)

	s3Server, err := s3mock.NewServer(0)
	require.NoError(t, err)
	defer func() { _ = s3Server.Stop() }()
	require.NoError(t, s3Server.CreateBucket(sidecarSmokeBucket))
	// Only a request signed by an issued session passes. No AWS_* variable is
	// set for this process: the sidecar fetches its credentials itself.
	s3Server.RequireSessions()

	setup, cleanup := NewIsolated(t,
		WithMultipoolerCount(3),
		WithS3Backup(sidecarSmokeBucket, "us-east-1", s3Server.Endpoint()),
		WithS3CAPEM(s3Server.CertPEM()),
	)
	defer cleanup()

	for name, inst := range setup.Multipoolers {
		require.NotNil(t, inst.PgBackRestd, "%s should run a sidecar", name)
		assert.True(t, inst.PgBackRestd.IsRunning(), "%s sidecar should be running", name)
		assert.NotContains(t, inst.Pgctld.PoolerDir, "pgbackrestd",
			"the sidecar's state must not be inside the data volume")
	}

	primary := setup.GetPrimary(t)
	standbys := setup.GetStandbys()
	require.Len(t, standbys, 2)

	// Bootstrap already archived WAL and took the first backup through the
	// sidecar. Now take one from a replica.
	replica := standbys[0]
	client, err := NewMultipoolerClient(replica.Multipooler.GrpcPort)
	require.NoError(t, err)
	defer client.Close()

	backupCtx, cancelBackup := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancelBackup()
	resp, err := client.Manager.Backup(backupCtx, &multipoolermanagerdatapb.BackupRequest{Type: "full"})
	require.NoError(t, err, "full backup from replica %s through the sidecar", replica.Name)
	require.NotEmpty(t, resp.BackupId)
	t.Logf("Replica %s took backup %s", replica.Name, resp.BackupId)

	// `info` is what GetBackups reports.
	listCtx, cancelList := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancelList()
	list, err := client.Manager.GetBackups(listCtx, &multipoolermanagerdatapb.GetBackupsRequest{})
	require.NoError(t, err)
	var found *multipoolermanagerdatapb.BackupMetadata
	for _, b := range list.Backups {
		if b.BackupId == resp.BackupId {
			found = b
		}
	}
	require.NotNil(t, found, "backup %s should appear in info", resp.BackupId)
	assert.Equal(t, multipoolermanagerdatapb.BackupMetadata_COMPLETE, found.Status)

	require.Eventually(t, func() bool {
		return keysContaining(s3Server, "/backup/", resp.BackupId) > 0
	}, 30*time.Second, 500*time.Millisecond, "backup %s should be in s3mock", resp.BackupId)

	// Push WAL from the primary: archive_command POSTs to the WAL socket and
	// returns success only once the segment is in the repository.
	db := openSocketDB(t, primary)
	defer db.Close()
	_, err = db.Exec("CREATE TABLE sidecar_smoke (id serial PRIMARY KEY, note text NOT NULL)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO sidecar_smoke (note) VALUES ('archived through the sidecar')")
	require.NoError(t, err)
	var segment string
	require.NoError(t, db.QueryRow("SELECT pg_walfile_name(pg_switch_wal())").Scan(&segment))

	archiveStatus := filepath.Join(primary.Pgctld.PoolerDir, "pg_data", "pg_wal", "archive_status")
	require.Eventually(t, func() bool {
		if _, err := os.Stat(filepath.Join(archiveStatus, segment+".done")); err != nil {
			return false
		}
		return keysContaining(s3Server, "/archive/", segment) > 0
	}, 2*time.Minute, time.Second, "segment %s should be archived in s3mock and marked done", segment)
	t.Logf("Segment %s archived through the sidecar", segment)
}

// keysContaining counts the s3mock keys that contain both substrings.
func keysContaining(s *s3mock.Server, first, second string) int {
	n := 0
	for _, key := range s.ListKeys(sidecarSmokeBucket, "") {
		if strings.Contains(key, first) && strings.Contains(key, second) {
			n++
		}
	}
	return n
}

func openSocketDB(t *testing.T, inst *MultipoolerInstance) *sql.DB {
	t.Helper()
	dsn := fmt.Sprintf("host=%s port=%d user=postgres dbname=postgres sslmode=disable password=%s",
		filepath.Join(inst.Pgctld.PoolerDir, "pg_sockets"), inst.Pgctld.PgPort, TestPostgresPassword)
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	require.NoError(t, db.Ping())
	return db
}
