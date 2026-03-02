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

package multipooler

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/s3mock"
)

// TestBackup_FailsDuringPrimaryFailover verifies that pgBackRest fails cleanly when
// the primary PostgreSQL instance disappears while a backup is in progress.
//
// The test uses s3mock's PutCallback to pause pgBackRest mid-upload, giving us a
// deterministic point to trigger the failover before unblocking.
func TestBackup_FailsDuringPrimaryFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	// Channels for coordinating with s3mock
	uploadStarted := make(chan struct{}, 1)
	unblock := make(chan struct{})

	// Create a test-local s3mock with PutCallback — not the shared instance
	s3Server, err := s3mock.NewServer(0, s3mock.WithPutCallback(
		func(ctx context.Context, bucket, key string) error {
			// Signal once when the first upload arrives
			select {
			case uploadStarted <- struct{}{}:
			default:
			}
			// Block until the test says to proceed (or request is cancelled)
			select {
			case <-unblock:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	))
	require.NoError(t, err)
	defer func() { _ = s3Server.Stop() }()

	require.NoError(t, s3Server.CreateBucket("multigres"))

	// Set AWS credentials required by pgBackRest (s3mock does not validate them)
	t.Setenv("AWS_ACCESS_KEY_ID", "test-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
	os.Unsetenv("AWS_SESSION_TOKEN") // ensure no stale session token

	// Isolated shard: 2 multipoolers + 1 multiorch for failover, S3 backend
	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(2),
		shardsetup.WithMultiOrchCount(1),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
		shardsetup.WithS3Backup("multigres", "us-east-1", s3Server.Endpoint()),
		shardsetup.WithPrimaryFailoverGracePeriod("8s", "4s"),
	)
	defer cleanup()

	setup.StartMultiOrchs(t.Context(), t)

	// Record original primary before we kill it
	primary := setup.GetPrimary(t)
	require.NotNil(t, primary)
	originalPrimaryName := setup.PrimaryName
	t.Logf("Primary before backup: %s", originalPrimaryName)

	// Create backup client pointing at the primary's multipooler
	backupClient := createBackupClient(t, primary.Multipooler.GrpcPort)

	// Start a full backup on the primary in a goroutine (it will block inside s3mock)
	backupErrCh := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		_, err := backupClient.Backup(ctx, &multipoolermanagerdata.BackupRequest{
			ForcePrimary: true,
			Type:         "full",
		})
		backupErrCh <- err
	}()

	// Wait for pgBackRest to reach the upload phase
	t.Log("Waiting for pgBackRest to start uploading to s3mock...")
	select {
	case <-uploadStarted:
		t.Log("pgBackRest is mid-upload — triggering failover now")
	case <-time.After(1 * time.Minute):
		t.Fatal("timed out waiting for pgBackRest to start uploading")
	}

	// Kill postgres on the primary to trigger failover
	setup.KillPostgres(t, originalPrimaryName)

	// Unblock s3mock — pgBackRest resumes and discovers postgres is gone
	close(unblock)

	// Wait for backup goroutine to return
	backupErr := <-backupErrCh

	// Assertion 1: backup must fail
	require.Error(t, backupErr, "backup must fail when primary postgres disappears mid-backup")
	t.Logf("Backup failed as expected: %v", backupErr)

	// Assertion 2: no backup should be marked complete in the repo
	listCtx := utils.WithTimeout(t, 30*time.Second)
	listResp, err := backupClient.GetBackups(listCtx, &multipoolermanagerdata.GetBackupsRequest{Limit: 10})
	require.NoError(t, err)
	for _, b := range listResp.Backups {
		assert.NotEqual(t, multipoolermanagerdata.BackupMetadata_COMPLETE, b.Status,
			"no backup should be marked complete after a mid-backup primary failover (id=%s status=%s)",
			b.BackupId, b.Status)
	}

	// Assertion 3: cluster is healthy — a new primary is elected and postgres is running
	t.Log("Waiting for a new primary to be elected...")
	require.Eventually(t, func() bool {
		for name, inst := range setup.Multipoolers {
			if name == originalPrimaryName {
				continue
			}
			client, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
			if err != nil {
				continue
			}
			resp, err := client.Manager.Status(utils.WithShortDeadline(t), &multipoolermanagerdata.StatusRequest{})
			client.Close()
			if err != nil {
				continue
			}
			if resp.Status.IsInitialized &&
				resp.Status.PoolerType == clustermetadatapb.PoolerType_PRIMARY &&
				resp.Status.PostgresRunning {
				t.Logf("New primary elected: %s", name)
				return true
			}
		}
		return false
	}, 30*time.Second, 500*time.Millisecond, "a new primary should be elected and running after failover")
}
