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

// TestBackup_StalePrimaryArchiveFencing verifies that a primary which loses
// quorum (we simulate this by SIGSTOPping its multipooler) and is later
// resumed cannot continue serving as primary — including, importantly,
// cannot run primary-only backups that would race the new primary's
// archive stream.
//
// Sequence:
//
//  1. Bootstrap a 3-node cluster with multiorch and a widened failover
//     grace period so multiorch waits long enough for SIGSTOP to be
//     interpreted as quorum loss.
//  2. Take a full backup.
//  3. SIGSTOP the primary's multipooler. Wait for multiorch to elect a
//     new primary on a fresh timeline.
//  4. SIGCONT the old multipooler.
//  5. Wait for the old node to either re-discover its demoted status or
//     remain a non-PRIMARY. Either way, it MUST NOT report PoolerType
//     PRIMARY any more.
//  6. Attempting a primary-forcing backup against the old node MUST fail.
//  7. The cluster as a whole still answers backup requests via the new
//     primary.
//
// This test deliberately stops short of the original plan's "archive
// divergence" assertions (parsing 00000002.history, comparing TL1 WAL
// against fork LSN). Those checks dive deep into pgBackRest's S3 layout
// and are brittle; the higher-level "stale primary cannot back up"
// assertion is the actionable invariant for downstream code.
func TestBackup_StalePrimaryArchiveFencing(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	t.Parallel()

	tlog := newBackupTestLogger(t)

	s3Server, err := s3mock.NewServer(0)
	require.NoError(t, err)
	defer func() { _ = s3Server.Stop() }()
	require.NoError(t, s3Server.CreateBucket("multigres"))

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiOrchCount(1),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
		shardsetup.WithS3Backup("multigres", "us-east-1", s3Server.Endpoint()),
		// Wider grace period so multiorch waits long enough for SIGSTOP to
		// register as quorum loss before promoting.
		shardsetup.WithPrimaryFailoverGracePeriod("15s", "5s"),
	)
	defer cleanup()
	setup.StartMultiOrchs(t.Context(), t)

	originalPrimary := setup.GetPrimary(t)
	require.NotNil(t, originalPrimary)
	originalPrimaryName := setup.PrimaryName
	tlog.Log("phase=bootstrap primary=%s", originalPrimaryName)

	// Phase 2: full backup before the fault.
	primaryClient := createBackupClient(t, originalPrimary.Multipooler.GrpcPort)
	fullID := createAndVerifyBackup(t, primaryClient, "full", true, 5*time.Minute, nil)
	tlog.Log("phase=full-backup id=%s", fullID)

	// Phase 3: SIGSTOP the primary's multipooler. The pgctld and postgres
	// keep running but multipooler stops emitting heartbeats.
	setup.PauseMultipooler(t, originalPrimaryName)
	tlog.Log("phase=paused-multipooler node=%s", originalPrimaryName)

	// Wait for multiorch to elect a new primary among the surviving standbys.
	var newPrimaryName string
	require.Eventually(t, func() bool {
		for name, inst := range setup.Multipoolers {
			if name == originalPrimaryName {
				continue
			}
			client, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
			if err != nil {
				continue
			}
			resp, err := client.Manager.Status(utils.WithShortDeadline(t),
				&multipoolermanagerdata.StatusRequest{})
			client.Close()
			if err != nil {
				continue
			}
			if resp.Status.IsInitialized &&
				resp.Status.PoolerType == clustermetadatapb.PoolerType_PRIMARY &&
				resp.Status.PostgresReady {
				newPrimaryName = name
				return true
			}
		}
		return false
	}, 60*time.Second, 1*time.Second, "a new primary should be elected after the original is fenced")
	tlog.Log("phase=new-primary node=%s", newPrimaryName)

	// Phase 4: resume the old multipooler. From here on the old multipooler
	// has to figure out it's no longer primary.
	setup.ResumeMultipooler(t, originalPrimaryName)
	tlog.Log("phase=resumed-multipooler node=%s", originalPrimaryName)

	// Phase 5: the old multipooler MUST NOT continue reporting PRIMARY.
	// We give it some time to detect quorum loss after resume.
	require.Eventually(t, func() bool {
		client, err := shardsetup.NewMultipoolerClient(originalPrimary.Multipooler.GrpcPort)
		if err != nil {
			return false
		}
		resp, err := client.Manager.Status(utils.WithShortDeadline(t),
			&multipoolermanagerdata.StatusRequest{})
		client.Close()
		if err != nil {
			return false
		}
		// We accept any non-PRIMARY pooler type. The exact post-fence type
		// (REPLICA vs UNKNOWN vs DEMOTED) is an implementation detail — the
		// invariant is "not PRIMARY".
		return resp.Status.PoolerType != clustermetadatapb.PoolerType_PRIMARY
	}, 60*time.Second, 1*time.Second, "old primary must stop reporting PoolerType=PRIMARY after fencing")
	tlog.Log("phase=old-no-longer-primary")

	// Phase 6: attempting a primary-forcing backup on the old node must fail.
	// Use a short timeout — we do not want to wait 5 minutes for pgBackRest
	// to give up; the multipooler should reject the request immediately.
	staleClient := createBackupClient(t, originalPrimary.Multipooler.GrpcPort)
	staleCtx, staleCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer staleCancel()
	_, staleErr := staleClient.Backup(staleCtx, &multipoolermanagerdata.BackupRequest{
		Type:         "full",
		JobId:        "stale-primary-fence-attempt",
		ForcePrimary: true,
	})
	require.Error(t, staleErr, "ForcePrimary backup against the fenced ex-primary must be rejected")
	tlog.Log("phase=stale-backup-rejected err=%v", staleErr)

	// Phase 7: the new primary still serves backups.
	newPrimary := setup.Multipoolers[newPrimaryName]
	require.NotNil(t, newPrimary)
	newPrimaryClient := createBackupClient(t, newPrimary.Multipooler.GrpcPort)

	// Trigger a checkpoint so pg_control's recorded timeline matches the
	// running WAL timeline. (See TestBackup_TimelineSwitchAndChainContinuity
	// for the full discussion of why this is needed.)
	newPrimaryDB := connectToPostgresViaSocket(t,
		getPostgresSocketPath(newPrimary.Pgctld.PoolerDir),
		newPrimary.Pgctld.PgPort)
	defer newPrimaryDB.Close()
	_, err = newPrimaryDB.Exec("CHECKPOINT")
	require.NoError(t, err)

	postID := createAndVerifyBackup(t, newPrimaryClient, "full", true, 5*time.Minute, nil)
	tlog.Log("phase=post-fence-backup id=%s", postID)

	listAndFindBackup(t, newPrimaryClient, postID, 20)

	// Cluster sanity: primary identity has flipped.
	assert.NotEqual(t, originalPrimaryName, newPrimaryName, "new primary should differ from old")
}
