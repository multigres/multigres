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
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/tools/s3mock"
)

// TestBackup_ConcurrentMatrix verifies the distributed backup lease's
// behavior when a second backup is requested while a first is in flight.
// The matrix varies the source of each backup:
//
//   - Std-then-Pri: first from a standby, second from the primary
//     (the canonical cross-node lease-steal scenario)
//   - Pri-then-Std: first from the primary, second from a standby (reversed)
//   - SameNode    : both backups go through the same multipooler. Empirically
//     the local ActionLock serializes them BEFORE either reaches the lease
//     layer, so no lease steal happens — the second backup waits for the
//     first, then runs to completion.
//
// The original design proposed a fourth case (Std→Pri@Start) where the
// second backup arrives during pg_start_backup. That collapses to @Upload —
// pg_start_backup is a SQL operation that doesn't write to S3, so we can't
// distinguish it via s3mock matchers. See TestBackup_FailureMatrix_PrimaryFailover
// for the full discussion.
//
// Each sub-case runs in its own isolated cluster, but the sub-cases run
// SERIALLY rather than in parallel. Lease stealing involves a race between
// the killed pgBackRest releasing its S3 stanza lock and the new pgBackRest
// acquiring it. Under parallel load (3 backups happening at once across
// different clusters but on the same host), the macOS test runner can be
// slow enough that the new pgBackRest hits "lock held" (pgBackRest exit 56)
// before the dying one releases. The CI shard-level parallelism in Phase 5
// already runs tests across machines, so we don't need sub-test parallelism.
func TestBackup_ConcurrentMatrix(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	t.Parallel()

	cases := []struct {
		name      string
		firstSrc  string // "Primary" | "Standby"
		secondSrc string // "Primary" | "Standby" | "SameNode"
	}{
		{"Std-then-Pri", "Standby", "Primary"},
		{"Pri-then-Std", "Primary", "Standby"},
		{"SameNode", "Standby", "SameNode"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			runConcurrentCase(t, c.firstSrc, c.secondSrc)
		})
	}
}

// runConcurrentCase implements one matrix cell: bring up a 2-node cluster,
// pause the first backup mid-upload, kick off a second backup that should
// steal the lease, and verify (a) the first backup fails, (b) the second
// backup completes, (c) the failed first backup is not marked COMPLETE.
func runConcurrentCase(t *testing.T, firstSrc, secondSrc string) {
	tlog := newBackupTestLogger(t)
	tlog.Log("matrix case First=%s Second=%s", firstSrc, secondSrc)

	// Phase-aware gate: parks first-backup data PUTs while letting the second
	// backup pass through. Never Released — first backup unblocks via ctx
	// cancellation when the lease steal kills its pgBackRest process.
	var phase s3mock.Phase
	gate := s3mock.NewGate(s3mock.PhaseMatcher(&phase, "first-backup", s3mock.MatchDataUpload))
	s3Server, err := s3mock.NewServer(0, s3mock.WithGate(gate))
	require.NoError(t, err)
	defer func() { _ = s3Server.Stop() }()
	require.NoError(t, s3Server.CreateBucket("multigres"))

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(2),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
		shardsetup.WithS3Backup("multigres", "us-east-1", s3Server.Endpoint()),
	)
	defer cleanup()

	primary := setup.GetPrimary(t)
	require.NotNil(t, primary)
	primaryName := setup.PrimaryName

	var standbyInst *shardsetup.MultipoolerInstance
	for name, inst := range setup.Multipoolers {
		if name != primaryName {
			standbyInst = inst
			break
		}
	}
	require.NotNil(t, standbyInst, "expected a standby instance")
	tlog.Log("primary=%s standby=%s", primaryName, standbyInst.Name)

	standbyClient := createBackupClient(t, standbyInst.Multipooler.GrpcPort)
	primaryClient := createBackupClient(t, primary.Multipooler.GrpcPort)
	primaryPg2Path := filepath.Join(primary.Pgctld.PoolerDir, "pg_data")
	standbyOverrides := map[string]string{"pg2_path": primaryPg2Path}

	// Pick clients/flags per source.
	type backupTarget struct {
		client       multipoolermanagerpb.MultiPoolerManagerClient
		forcePrimary bool
		overrides    map[string]string
	}
	targetFor := func(src string) backupTarget {
		switch src {
		case "Primary":
			return backupTarget{primaryClient, true, nil}
		case "Standby":
			return backupTarget{standbyClient, false, standbyOverrides}
		case "SameNode":
			// Same multipooler as the first backup — used only as the second.
			return backupTarget{standbyClient, false, standbyOverrides}
		default:
			t.Fatalf("unknown source %q", src)
			return backupTarget{}
		}
	}

	first := targetFor(firstSrc)
	second := targetFor(secondSrc)

	// === Phase 1: launch first backup (will park in gate). ===
	gate.Arm()
	phase.Set("first-backup")

	firstJobID := "concurrent-" + firstSrc + "-" + secondSrc + "-1"
	secondJobID := "concurrent-" + firstSrc + "-" + secondSrc + "-2"

	firstErrCh := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		_, err := first.client.Backup(ctx, &multipoolermanagerdata.BackupRequest{
			Type:         "full",
			JobId:        firstJobID,
			ForcePrimary: first.forcePrimary,
			Overrides:    first.overrides,
		})
		firstErrCh <- err
	}()

	gateCtx, gateCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer gateCancel()
	hit, err := gate.Wait(gateCtx)
	require.NoError(t, err, "timed out waiting for first backup to start uploading")
	tlog.Log("first paused at %s", hit.Key)

	// === Phase 2: launch second backup (steals lease). ===
	phase.Set("second-backup")

	secondErrCh := make(chan error, 1)
	var secondBackupID string
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		resp, err := second.client.Backup(ctx, &multipoolermanagerdata.BackupRequest{
			Type:         "full",
			JobId:        secondJobID,
			ForcePrimary: second.forcePrimary,
			Overrides:    second.overrides,
		})
		if err == nil && resp != nil {
			secondBackupID = resp.BackupId
		}
		secondErrCh <- err
	}()

	// SameNode takes longer because pgBackRest's HTTP-client retries are what
	// eventually unblock the first backup once the gate phase has switched.
	waitTimeout := 90 * time.Second
	if secondSrc == "SameNode" {
		waitTimeout = 3 * time.Minute
	}

	tlog.Log("waiting for first backup")
	firstErr := waitChanWithTimeout(t, firstErrCh, waitTimeout, "first backup did not return")
	tlog.Log("first backup returned: %v", firstErr)

	tlog.Log("waiting for second backup")
	secondErr := waitChanWithTimeout(t, secondErrCh, waitTimeout, "second backup did not return")
	tlog.Log("second backup returned: err=%v id=%s", secondErr, secondBackupID)

	// === Assertions ===
	if secondSrc == "SameNode" {
		// ActionLock serializes both backups on the same multipooler. No lease
		// steal happens — the second backup waits for the first to finish.
		require.NoError(t, firstErr, "SameNode: first backup should complete (ActionLock serializes; no lease steal)")
		require.NoError(t, secondErr, "SameNode: second backup should complete after the first releases the ActionLock")
		require.NotEmpty(t, secondBackupID, "SameNode: second backup should return a backup ID")
		// Both jobs should be COMPLETE.
		assertJobComplete(t, primaryClient, firstJobID)
		assertJobComplete(t, primaryClient, secondJobID)
		return
	}

	// Cross-node: lease steal kicks in. First fails, second succeeds.
	require.Error(t, firstErr, "first backup must fail when its lease is stolen (First=%s Second=%s)", firstSrc, secondSrc)
	require.NoError(t, secondErr, "second backup must succeed after stealing the lease (First=%s Second=%s)", firstSrc, secondSrc)
	require.NotEmpty(t, secondBackupID, "second backup should return a backup ID")

	assertJobComplete(t, primaryClient, secondJobID)

	// First is NOT COMPLETE (or not found).
	firstJobCtx, firstJobCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer firstJobCancel()
	firstJobResp, err := first.client.GetBackupByJobId(firstJobCtx,
		&multipoolermanagerdata.GetBackupByJobIdRequest{JobId: firstJobID})
	if err == nil && firstJobResp.GetBackup() != nil {
		assert.NotEqual(t, multipoolermanagerdata.BackupMetadata_COMPLETE, firstJobResp.Backup.Status,
			"first backup must not be marked COMPLETE")
	}
}

// assertJobComplete polls until the backup with jobID is visible and asserts
// it's in COMPLETE state.
func assertJobComplete(t *testing.T, client multipoolermanagerpb.MultiPoolerManagerClient, jobID string) {
	t.Helper()
	var jobResp *multipoolermanagerdata.GetBackupByJobIdResponse
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var err error
		jobResp, err = client.GetBackupByJobId(ctx, &multipoolermanagerdata.GetBackupByJobIdRequest{JobId: jobID})
		return err == nil && jobResp.GetBackup() != nil
	}, 30*time.Second, 1*time.Second, "backup %s should exist in metadata", jobID)
	assert.Equal(t, multipoolermanagerdata.BackupMetadata_COMPLETE, jobResp.Backup.Status,
		"backup %s should be marked COMPLETE", jobID)
}

// waitChanWithTimeout reads from ch with the given timeout; on timeout it
// fails the test. Returns the value read.
func waitChanWithTimeout(t *testing.T, ch <-chan error, timeout time.Duration, msg string) error {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(timeout):
		t.Fatalf("%s (timeout=%v)", msg, timeout)
		return nil
	}
}
