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
	"path/filepath"
	"sync/atomic"
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

	// blockPuts gates the callback: false during bootstrap, true once backup starts.
	// This prevents the callback from interfering with bootstrap-time pgBackRest operations.
	var blockPuts atomic.Bool

	// Create a test-local s3mock with PutCallback — not the shared instance
	s3Server, err := s3mock.NewServer(0, s3mock.WithPutCallback(
		func(ctx context.Context, bucket, key string) error {
			if !blockPuts.Load() {
				return nil
			}
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

	// Isolated shard: 3 multipoolers + 1 multiorch for failover, S3 backend.
	// 3 nodes are required so that AT_LEAST_2 quorum can be satisfied after the primary is killed.
	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiOrchCount(1),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
		shardsetup.WithS3Backup("multigres", "us-east-1", s3Server.Endpoint()),
		shardsetup.WithPrimaryFailoverGracePeriod("8s", "4s"),
	)
	defer cleanup()

	setup.StartMultiOrchs(t.Context(), t)

	// Bootstrap is complete — enable PUT blocking so the backup will be intercepted.
	blockPuts.Store(true)

	// Record original primary before we kill it
	primary := setup.GetPrimary(t)
	require.NotNil(t, primary)
	originalPrimaryName := setup.PrimaryName
	t.Logf("Primary before backup: %s", originalPrimaryName)

	// Find the standby instance for the backup client.
	// Backing up from a standby exercises the pgBackRest TLS server code path:
	// shardsetup always generates pgBackRest TLS certs, so the standby's pgBackRest
	// connects to the primary via --pg2-host-type=tls.
	var standbyInst *shardsetup.MultipoolerInstance
	for name, inst := range setup.Multipoolers {
		if name != originalPrimaryName {
			standbyInst = inst
			break
		}
	}
	require.NotNil(t, standbyInst, "expected a standby instance")
	backupClient := createBackupClient(t, standbyInst.Multipooler.GrpcPort)

	// Disable postgres restarts on all nodes so that multipooler does not
	// automatically restart postgres after the kill — multiorch must orchestrate recovery.
	for name, inst := range setup.Multipoolers {
		mc := createBackupClient(t, inst.Multipooler.GrpcPort)
		_, err := mc.SetPostgresRestartsEnabled(t.Context(), &multipoolermanagerdata.SetPostgresRestartsEnabledRequest{Enabled: false})
		require.NoError(t, err, "failed to disable postgres restarts on %s", name)
		t.Logf("Disabled postgres restarts on %s", name)
	}

	// Tag the backup so we can check its specific status after failure.
	const testJobID = "failover-test-backup"

	// pg2-path is required even in TLS mode (pgBackRest 2.58+).
	// Pass the primary's pg_data directory so pgBackRest can connect to it remotely.
	primaryPg2Path := filepath.Join(primary.Pgctld.PoolerDir, "pg_data")

	// Start a full backup from the standby in a goroutine (it will block inside s3mock).
	// The standby connects to the primary's pgBackRest TLS server (pg2-host-type=tls).
	backupErrCh := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		_, err := backupClient.Backup(ctx, &multipoolermanagerdata.BackupRequest{
			Type:      "full",
			JobId:     testJobID,
			Overrides: map[string]string{"pg2_path": primaryPg2Path},
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
	require.Error(t, backupErr, "standby backup must fail when primary postgres disappears mid-backup")
	t.Logf("Backup failed as expected: %v", backupErr)

	// Assertion 2: the specific backup we started must not be marked complete
	jobCtx := utils.WithTimeout(t, 30*time.Second)
	jobResp, err := backupClient.GetBackupByJobId(jobCtx, &multipoolermanagerdata.GetBackupByJobIdRequest{JobId: testJobID})
	if err == nil && jobResp.GetBackup() != nil {
		assert.NotEqual(t, multipoolermanagerdata.BackupMetadata_COMPLETE, jobResp.Backup.Status,
			"the failed backup must not be marked complete (id=%s status=%s)",
			jobResp.Backup.BackupId, jobResp.Backup.Status)
	}
	// If GetBackupByJobId returns an error (backup not found), that is also acceptable —
	// it means pgBackRest never completed the backup and left no record.

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
				resp.Status.PostgresReady {
				t.Logf("New primary elected: %s", name)
				return true
			}
		}
		return false
	}, 30*time.Second, 500*time.Millisecond, "a new primary should be elected and running after failover")
}
