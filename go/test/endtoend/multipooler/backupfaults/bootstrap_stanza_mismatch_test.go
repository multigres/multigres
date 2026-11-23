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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/s3mock"
	"github.com/multigres/multigres/go/test/utils"
)

// pgBackRestInfoMismatch is the text of pgBackRest error [028]: the stanza's
// system id differs from the cluster being backed up.
const pgBackRestInfoMismatch = "backup and archive info files exist but do not match the database"

// TestBootstrapRecoversAfterFailureFollowingStanzaCreate checks that a bootstrap
// attempt failing after `stanza-create` does not wedge the shard once the cause
// is gone.
//
// A single pooler bootstraps against an s3mock that rejects WAL archive PUTs but
// lets stanza metadata through, so `stanza-create` succeeds and `pgbackrest
// check` fails. The test then lifts the fault and requires a later attempt to
// produce a complete backup. Without the fix every retry fails with [028].
func TestBootstrapRecoversAfterFailureFollowingStanzaCreate(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end test (no postgres binaries available)")
	}

	const bucket = "multigres"

	var failWALPush atomic.Bool
	failWALPush.Store(true)

	s3Server, err := s3mock.NewServer(0, s3mock.WithPutCallback(
		func(_ context.Context, _ string, key string) error {
			if failWALPush.Load() && strings.Contains(key, "/archive/") && walSegmentKey.MatchString(key) {
				return fmt.Errorf("injected archive-push failure for %s", key)
			}
			return nil
		},
	))
	require.NoError(t, err)
	defer func() { _ = s3Server.Stop() }()

	require.NoError(t, s3Server.CreateBucket(bucket))

	t.Setenv("AWS_ACCESS_KEY_ID", "test-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
	os.Unsetenv("AWS_SESSION_TOKEN")

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(1),
		shardsetup.WithS3Backup(bucket, "us-east-1", s3Server.Endpoint()),
		shardsetup.WithDeferredMultipoolerStart(),
	)
	defer cleanup()

	require.Len(t, setup.Multipoolers, 1)
	var inst *shardsetup.MultipoolerInstance
	for _, v := range setup.Multipoolers {
		inst = v
	}

	readLog := func() string {
		data, readErr := os.ReadFile(inst.Multipooler.LogFile)
		if readErr != nil {
			return ""
		}
		return string(data)
	}

	require.NoError(t, inst.Multipooler.Start(t.Context(), t))

	// The first attempt creates the stanza, then fails at the archive check
	// (60s archive-timeout).
	require.Eventually(t, func() bool {
		return strings.Contains(readLog(), "pgbackrest archive check failed")
	}, 90*time.Second, 500*time.Millisecond,
		"expected the first bootstrap attempt to abort at the pgbackrest archive check")

	shardRef := &clustermetadatapb.ShardRef{
		Database:   "postgres",
		TableGroup: "default",
		Shard:      "0-inf",
	}
	bootstrapMarked := func() bool {
		marked, markErr := setup.TopoServer.BootstrapInProgress(t.Context(), shardRef)
		require.NoError(t, markErr)
		return marked
	}
	require.True(t, bootstrapMarked(), "the aborted attempt must leave the bootstrap marker")

	failWALPush.Store(false)
	t.Log("First attempt aborted after stanza-create, WAL archiving is healthy again")

	hasInfoFile := func() bool {
		for _, key := range s3Server.ListKeys(bucket, "") {
			if strings.HasSuffix(key, "/backup.info") {
				return true
			}
		}
		return false
	}
	require.True(t, hasInfoFile(),
		"precondition: the failed attempt must have left a stanza (backup.info) in the repo")

	backupClient := createBackupClient(t, inst.Multipooler.GrpcPort)
	hasCompleteBackup := func() bool {
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
	}

	recovered := pollUntil(t, hasCompleteBackup, 3*time.Minute, 2*time.Second)
	if !recovered {
		mismatches := strings.Count(readLog(), pgBackRestInfoMismatch)
		t.Fatalf("bootstrap never produced a complete backup after the fault was removed: "+
			"%d later stanza-create attempts failed with %q, because each attempt runs initdb "+
			"and gets a system id that does not match the stanza left by the failed attempt",
			mismatches, pgBackRestInfoMismatch)
	}
	require.True(t, pollUntil(t, func() bool { return !bootstrapMarked() }, 30*time.Second, time.Second),
		"the bootstrap marker must be cleared once a complete backup exists")
}

// pollUntil polls cond until it is true or timeout elapses, and reports which.
func pollUntil(t *testing.T, cond func() bool, timeout, tick time.Duration) bool {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(tick)
	}
	return cond()
}
