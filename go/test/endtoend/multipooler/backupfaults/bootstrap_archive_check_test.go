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
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/s3mock"
	"github.com/multigres/multigres/go/test/utils"
)

// walSegmentKey matches pgBackRest WAL-segment archive PUTs: a key under
// .../archive/... whose name contains a 24-hex WAL segment name. This
// deliberately excludes the stanza's archive.info / backup.info metadata
// writes (no 24-hex run) so stanza-create still succeeds and the failure
// surfaces specifically at the `pgbackrest check` archive probe.
var walSegmentKey = regexp.MustCompile(`[0-9A-F]{24}`)

// TestBootstrapArchiveCheck_FailsWhenWalPushFails proves the bootstrap-time
// `pgbackrest check` (rpc_check.go) catches a broken WAL archive pipeline and
// aborts cluster creation instead of producing a cluster whose WAL silently
// never archives.
//
// Setup: a single pooler self-bootstraps (initdb → stanza-create → check →
// first backup). The s3mock fails every WAL-segment archive PUT but lets the
// stanza-create metadata writes through, so stanza-create succeeds and the
// forced WAL switch inside `check` is the first thing to hit the broken
// archive. The check fails, createFirstBackupAndInitializeLocked returns an
// error, and the shard never produces a complete backup.
func TestBootstrapArchiveCheck_FailsWhenWalPushFails(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end test (no postgres binaries available)")
	}

	// s3mock that rejects WAL-segment archive PUTs with an error (HTTP 500),
	// simulating a misconfigured/broken archive destination. Metadata writes
	// (archive.info, backup.info) pass through so stanza-create succeeds.
	s3Server, err := s3mock.NewServer(0, s3mock.WithPutCallback(
		func(_ context.Context, _ string, key string) error {
			if strings.Contains(key, "/archive/") && walSegmentKey.MatchString(key) {
				return fmt.Errorf("injected archive-push failure for %s", key)
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

	// Single pooler with pgctld started but the multipooler deferred, so the
	// setup helper does not drive (and block on) bootstrap. Once we start the
	// multipooler it bootstraps itself via the monitor loop.
	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(1),
		shardsetup.WithS3Backup("multigres", "us-east-1", s3Server.Endpoint()),
		shardsetup.WithDeferredMultipoolerStart(),
	)
	defer cleanup()

	require.Len(t, setup.Multipoolers, 1)
	var inst *shardsetup.MultipoolerInstance
	for _, v := range setup.Multipoolers {
		inst = v
	}

	require.NoError(t, inst.Multipooler.Start(t.Context(), t))

	// The first bootstrap attempt must fail at the archive check. The check
	// forces a WAL switch and waits out pgBackRest's default 60s archive-timeout
	// before failing, so the log line cannot appear sooner than ~60s plus
	// bootstrap overhead (initdb + stanza-create). 90s covers that with margin
	// while staying well under the CheckTimeout = 2m per-check bound.
	require.Eventually(t, func() bool {
		data, readErr := os.ReadFile(inst.Multipooler.LogFile)
		if readErr != nil {
			return false
		}
		return strings.Contains(string(data), "pgbackrest archive check failed")
	}, 90*time.Second, 2*time.Second,
		"expected bootstrap to abort at the pgbackrest archive check when WAL pushes fail")
	t.Log("Bootstrap aborted at the pgbackrest archive check as expected")

	// With the archive pipeline broken, no complete backup may ever appear.
	backupClient := createBackupClient(t, inst.Multipooler.GrpcPort)
	require.Never(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resp, listErr := backupClient.GetBackups(ctx, &multipoolermanagerdata.GetBackupsRequest{Limit: 10})
		if listErr != nil {
			return false
		}
		for _, b := range resp.Backups {
			if b.Status == multipoolermanagerdata.BackupMetadata_COMPLETE {
				return true
			}
		}
		return false
	}, 15*time.Second, 3*time.Second,
		"no complete backup should be produced while WAL archiving is broken")
}
