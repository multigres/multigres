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
	"fmt"
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

// TestBackup_FailureMatrix_PrimaryFailover exercises pgBackRest's response when
// the primary postgres is killed mid-backup. The matrix covers backup source:
// either from the primary itself (ForcePrimary) or from a standby.
//
// The original design proposed three pause points (PgStart / Upload / Finalize).
// Empirically, only Upload is a useful pause point for the "kill primary
// postgres" fault model:
//
//   - PgStart is not a distinct S3 event. pg_start_backup is a SQL operation
//     that writes nothing to S3; pgBackRest's first S3 write is the first data
//     bundle, which is already what Upload captures.
//   - Finalize (paused at backup.manifest write) happens AFTER pg_stop_backup
//     has already returned. Killing postgres at that point does not fail the
//     backup — the data is uploaded, the WAL stop position is captured, and
//     pgBackRest just needs to write the manifest. That fault is covered
//     elsewhere (lease-steal test) under a different fault model.
//
// Each sub-case runs in its own isolated cluster so they can run in parallel.
func TestBackup_FailureMatrix_PrimaryFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	t.Parallel()

	type pausePoint struct {
		name    string
		matcher s3mock.Matcher
	}
	pausePoints := []pausePoint{
		{"Upload", s3mock.MatchDataUpload},
	}
	sources := []string{"Primary", "Standby"}

	for _, src := range sources {
		for _, pp := range pausePoints {
			t.Run(fmt.Sprintf("Source=%s/Pause=%s", src, pp.name), func(t *testing.T) {
				t.Parallel()
				runFailoverMatrixCase(t, src, pp.name, pp.matcher)
			})
		}
	}
}

// runFailoverMatrixCase implements one matrix cell: bring up a 3-node cluster,
// arm the s3mock gate at the chosen pause point, start a backup from the chosen
// source, kill the primary postgres while the backup is paused, and verify
// (a) the backup fails, (b) it leaves no orphan COMPLETE entry, (c) a new
// primary is elected.
func runFailoverMatrixCase(t *testing.T, source, pauseName string, matcher s3mock.Matcher) {
	tlog := newBackupTestLogger(t)
	tlog.Log("matrix case Source=%s Pause=%s", source, pauseName)

	gate := s3mock.NewGate(matcher)
	s3Server, err := s3mock.NewServer(0, s3mock.WithGate(gate))
	require.NoError(t, err)
	defer func() { _ = s3Server.Stop() }()
	require.NoError(t, s3Server.CreateBucket("multigres"))

	// AWS credentials for pgBackRest are set process-wide in TestMain so they
	// survive t.Parallel sub-tests without each test calling t.Setenv.

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
	gate.Arm()

	primary := setup.GetPrimary(t)
	require.NotNil(t, primary)
	originalPrimaryName := setup.PrimaryName
	tlog.Log("primary=%s before backup", originalPrimaryName)

	// Pick the backup client based on source. Standby case mirrors the
	// long-standing failover test: a standby running pgBackRest connects to
	// the primary's pgBackRest TLS server.
	backupClient := createBackupClient(t, primary.Multipooler.GrpcPort)
	forcePrimary := true
	if source == "Standby" {
		var standbyInst *shardsetup.MultipoolerInstance
		for name, inst := range setup.Multipoolers {
			if name != originalPrimaryName {
				standbyInst = inst
				break
			}
		}
		require.NotNil(t, standbyInst, "expected a standby instance")
		backupClient = createBackupClient(t, standbyInst.Multipooler.GrpcPort)
		forcePrimary = false
	}

	// Disable postgres restarts so multiorch is the only path back to a primary.
	for name, inst := range setup.Multipoolers {
		mc := createBackupClient(t, inst.Multipooler.GrpcPort)
		_, err := mc.SetPostgresRestartsEnabled(t.Context(),
			&multipoolermanagerdata.SetPostgresRestartsEnabledRequest{Enabled: false})
		require.NoError(t, err, "failed to disable postgres restarts on %s", name)
	}

	jobID := fmt.Sprintf("failover-%s-%s", source, pauseName)
	backupErrCh := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		_, err := backupClient.Backup(ctx, &multipoolermanagerdata.BackupRequest{
			Type:         "full",
			JobId:        jobID,
			ForcePrimary: forcePrimary,
		})
		backupErrCh <- err
	}()

	tlog.Log("waiting for gate hit at pause=%s", pauseName)
	gateCtx, gateCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer gateCancel()
	hit, err := gate.Wait(gateCtx)
	require.NoError(t, err, "timed out waiting for pgBackRest at pause=%s", pauseName)
	tlog.Log("paused at key=%s", hit.Key)

	setup.KillPostgres(t, originalPrimaryName)
	tlog.Log("killed postgres on %s", originalPrimaryName)
	gate.Release()
	tlog.Log("released gate")

	backupErr := <-backupErrCh
	require.Error(t, backupErr, "backup must fail when primary postgres dies mid-backup (Source=%s Pause=%s)", source, pauseName)
	tlog.Log("backup failed as expected: %v", backupErr)

	// Assertion: the failed backup must not be marked COMPLETE.
	jobCtx := utils.WithTimeout(t, 30*time.Second)
	jobResp, getErr := backupClient.GetBackupByJobId(jobCtx,
		&multipoolermanagerdata.GetBackupByJobIdRequest{JobId: jobID})
	if getErr == nil && jobResp.GetBackup() != nil {
		assert.NotEqual(t, multipoolermanagerdata.BackupMetadata_COMPLETE, jobResp.Backup.Status,
			"failed backup must not be marked complete (id=%s status=%s)",
			jobResp.Backup.BackupId, jobResp.Backup.Status)
	}

	// Assertion: a new primary is elected.
	tlog.Log("waiting for new primary to be elected")
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
				tlog.Log("new primary elected: %s", name)
				return true
			}
		}
		return false
	}, 60*time.Second, 500*time.Millisecond, "a new primary should be elected after failover")
}
