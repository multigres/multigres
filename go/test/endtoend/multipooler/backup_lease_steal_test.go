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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/tools/s3mock"
)

// TestBackup_LeaseStealingOnConcurrentBackup verifies that when a second backup
// is requested while a first backup is in progress, the second backup steals the
// lease from the first. The first backup should fail (lease stolen, process killed)
// and the second backup should succeed.
func TestBackup_LeaseStealingOnConcurrentBackup(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	// Phase tracking: 0=bootstrap, 1=first backup (block), 2=second backup (allow)
	var phase atomic.Int32

	// Channel to signal when the first backup's upload has started
	uploadStarted := make(chan struct{}, 1)

	// Create s3mock with a PutCallback that blocks the first backup's data uploads
	// permanently, and allows the second backup's uploads through.
	// WAL archive uploads (keys containing "/archive/") are always allowed through
	// to prevent pgbackrest from failing with a WAL archive timeout.
	s3Server, err := s3mock.NewServer(0, s3mock.WithPutCallback(
		func(ctx context.Context, bucket, key string) error {
			// Always allow WAL archive uploads through
			if strings.Contains(key, "/archive/") {
				return nil
			}

			switch phase.Load() {
			case 0:
				// Bootstrap phase — allow through
				return nil
			case 1:
				// First backup — signal that upload started, then block forever.
				// The only way to unblock is context cancellation (when the
				// process is killed after its lease is stolen).
				select {
				case uploadStarted <- struct{}{}:
				default:
				}
				<-ctx.Done()
				return ctx.Err()
			default:
				// Second backup (phase 2+) — allow through
				return nil
			}
		},
	))
	require.NoError(t, err)
	defer func() { _ = s3Server.Stop() }()

	require.NoError(t, s3Server.CreateBucket("multigres"))

	// Set AWS credentials required by pgBackRest (s3mock does not validate them)
	t.Setenv("AWS_ACCESS_KEY_ID", "test-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
	os.Unsetenv("AWS_SESSION_TOKEN")

	// Isolated shard: 2 multipoolers, S3 backend
	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(2),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
		shardsetup.WithS3Backup("multigres", "us-east-1", s3Server.Endpoint()),
	)
	defer cleanup()

	// Find primary and standby
	primary := setup.GetPrimary(t)
	require.NotNil(t, primary)
	primaryName := setup.PrimaryName
	t.Logf("Primary: %s", primaryName)

	var standbyInst *shardsetup.MultipoolerInstance
	for name, inst := range setup.Multipoolers {
		if name != primaryName {
			standbyInst = inst
			break
		}
	}
	require.NotNil(t, standbyInst, "expected a standby instance")

	// Create backup clients on different poolers so they have separate ActionLocks.
	// The distributed backup lease is the shared lock being tested.
	standbyClient := createBackupClient(t, standbyInst.Multipooler.GrpcPort)
	primaryClient := createBackupClient(t, primary.Multipooler.GrpcPort)

	// pg2-path override for standby backups
	primaryPg2Path := filepath.Join(primary.Pgctld.PoolerDir, "pg_data")
	standbyOverrides := map[string]string{"pg2_path": primaryPg2Path}

	// === Phase 1: Start first backup from standby (will be blocked in s3mock) ===
	phase.Store(1)

	const firstJobID = "lease-steal-backup-1"
	const secondJobID = "lease-steal-backup-2"

	firstBackupErrCh := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		_, err := standbyClient.Backup(ctx, &multipoolermanagerdata.BackupRequest{
			Type:      "full",
			JobId:     firstJobID,
			Overrides: standbyOverrides,
		})
		firstBackupErrCh <- err
	}()

	// Wait for the first backup to reach the upload phase
	t.Log("Waiting for first backup to start uploading to s3mock...")
	select {
	case <-uploadStarted:
		t.Log("First backup is mid-upload (blocked in s3mock)")
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for first backup to start uploading")
	}

	// === Phase 2: Start second backup from primary (will steal the lease) ===
	phase.Store(2)

	t.Log("Starting second backup from primary (should steal lease from first)...")
	secondBackupErrCh := make(chan error, 1)
	var secondBackupID string
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		resp, err := primaryClient.Backup(ctx, &multipoolermanagerdata.BackupRequest{
			Type:         "full",
			JobId:        secondJobID,
			ForcePrimary: true,
		})
		if err == nil && resp != nil {
			secondBackupID = resp.BackupId
		}
		secondBackupErrCh <- err
	}()

	// Wait for both backups to complete
	t.Log("Waiting for first backup to fail...")
	firstErr := <-firstBackupErrCh
	t.Log("Waiting for second backup to complete...")
	secondErr := <-secondBackupErrCh

	// === Assertions ===

	// First backup must fail (lease was stolen)
	require.Error(t, firstErr, "first backup must fail when its lease is stolen")
	t.Logf("First backup failed as expected: %v", firstErr)

	// Second backup must succeed
	require.NoError(t, secondErr, "second backup must succeed after stealing the lease")
	require.NotEmpty(t, secondBackupID, "second backup should return a backup ID")
	t.Logf("Second backup succeeded with ID: %s", secondBackupID)

	// Verify the second backup is marked complete in metadata (query from primary).
	// Poll because pgbackrest metadata may not be immediately visible after completion.
	var jobResp *multipoolermanagerdata.GetBackupByJobIdResponse
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var err error
		jobResp, err = primaryClient.GetBackupByJobId(ctx, &multipoolermanagerdata.GetBackupByJobIdRequest{JobId: secondJobID})
		return err == nil && jobResp.GetBackup() != nil
	}, 30*time.Second, 1*time.Second, "second backup should exist in metadata")
	assert.Equal(t, multipoolermanagerdata.BackupMetadata_COMPLETE, jobResp.Backup.Status,
		"second backup should be marked COMPLETE")

	// Verify the first backup is NOT marked complete (if it exists at all)
	firstJobCtx, firstJobCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer firstJobCancel()
	firstJobResp, err := standbyClient.GetBackupByJobId(firstJobCtx, &multipoolermanagerdata.GetBackupByJobIdRequest{JobId: firstJobID})
	if err == nil && firstJobResp.GetBackup() != nil {
		assert.NotEqual(t, multipoolermanagerdata.BackupMetadata_COMPLETE, firstJobResp.Backup.Status,
			"first backup must not be marked COMPLETE")
	}
	// If first backup is not found, that's also acceptable — pgBackRest may not have
	// created any record before being killed.
}
