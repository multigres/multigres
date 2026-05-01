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

// TestPrimaryCrashWithUnarchivedWAL_NoDataLoss proves Multigres' synchronous-commit
// durability contract under a primary postgres crash where committed WAL has not
// been pushed to the pgBackRest archive yet, AND verifies that the post-failover
// pgBackRest archive remains usable for provisioning fresh standbys.
//
// Scenario:
//  1. Healthy 3-node cluster with synchronous_commit + synchronous_standby_names
//     wired by multiorch. Three nodes are required because multiorch's election
//     refuses to appoint a leader without majority recruitment of the cohort —
//     a 2-node cluster cannot survive a primary kill.
//  2. WriterValidator commits rows via multigateway. Under sync_commit, every
//     successful COMMIT is durable on at least one standby's WAL even if the
//     primary's archive is behind.
//  3. The s3mock Gate (MatchWalArchive) deterministically blocks WAL archive PUTs.
//     gate.Wait confirms we are in the "committed-but-unarchived WAL" state.
//  4. SetPostgresRestartsEnabled(false) on the primary, then KillPostgres SIGKILLs
//     postgres. Disabling restarts before the kill prevents the multipooler's
//     postgres monitor from respawning postgres before multiorch sets rewindPending.
//  5. Multiorch elects a standby as the new primary.
//  6. Every successful pre-kill commit must be present on the new primary. If any
//     are missing, the durability contract has been violated.
//  7. After re-enabling restarts and forcing recovery, multiorch demotes the stale
//     primary (pg_rewind + reconfigure as standby). Both replicas — the surviving
//     original standby and the demoted-and-rejoined old primary — must stream on
//     the new primary's term and hold the full set of committed ids.
//  8. A fresh pooler-4 is provisioned via RestoreFromBackup against the baseline
//     backup taken before the kill. RestoreFromBackup returning success is the
//     load-bearing assertion that the post-failover archive is still usable for
//     new-standby provisioning across the timeline change. Multiorch's recovery
//     loop (FixReplication) wires replication to the new primary; we then assert
//     pooler-4 streams on the current term and catches up to every committed id.
func TestPrimaryCrashWithUnarchivedWAL_NoDataLoss(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end test (no postgres binaries available)")
	}

	// s3mock with a Gate that blocks WAL archive PUTs once armed. The Gate is not
	// armed during bootstrap or the baseline backup so they archive freely.
	gate := s3mock.NewGate(s3mock.MatchWalArchive)
	s3Server, err := s3mock.NewServer(0, s3mock.WithGate(gate))
	require.NoError(t, err)
	defer func() { _ = s3Server.Stop() }()

	require.NoError(t, s3Server.CreateBucket("multigres"))

	// pgBackRest demands AWS credentials; s3mock does not validate them.
	t.Setenv("AWS_ACCESS_KEY_ID", "test-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
	os.Unsetenv("AWS_SESSION_TOKEN")

	// Three multipoolers so that majority recruitment can be achieved after the
	// primary is killed (multiorch refuses to appoint a leader if it cannot
	// recruit a majority of the cohort). A fourth multipooler is provisioned
	// from the backup repo later in the test to verify archive usability across
	// the timeline change.
	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiOrchCount(1),
		shardsetup.WithMultigateway(),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
		shardsetup.WithS3Backup("multigres", "us-east-1", s3Server.Endpoint()),
		shardsetup.WithLeaderFailoverGracePeriod("8s", "4s"),
	)
	defer cleanup()

	setup.StartMultiOrchs(t.Context(), t)
	setup.WaitForMultigatewayQueryServing(t)

	primary := setup.GetPrimary(t)
	require.NotNil(t, primary)
	originalPrimaryName := setup.PrimaryName
	t.Logf("Initial primary: %s", originalPrimaryName)

	// --- Pre-flight: the cluster must be configured for the durability contract.
	// Without sync_commit + synchronous_standby_names, this test would prove
	// nothing — a clean run could simply mean the archive happened to be caught up.
	primarySocketDir := filepath.Join(primary.Pgctld.PoolerDir, "pg_sockets")
	primaryDB := connectToPostgresViaSocket(t, primarySocketDir, primary.Pgctld.PgPort)
	defer primaryDB.Close()

	var syncCommit, syncStandbyNames string
	require.NoError(t, primaryDB.QueryRow("SHOW synchronous_commit").Scan(&syncCommit))
	require.NoError(t, primaryDB.QueryRow("SHOW synchronous_standby_names").Scan(&syncStandbyNames))
	t.Logf("Pre-flight: synchronous_commit=%q synchronous_standby_names=%q", syncCommit, syncStandbyNames)
	require.NotEqual(t, "off", syncCommit, "cluster is not configured for the durability contract under test")
	require.NotEqual(t, "local", syncCommit, "cluster is not configured for the durability contract under test")
	require.NotEmpty(t, syncStandbyNames, "synchronous_standby_names is empty — multiorch did not wire sync replication")

	var syncReplicaCount int
	require.NoError(t, primaryDB.QueryRow(
		"SELECT count(*) FROM pg_stat_replication WHERE sync_state IN ('sync','quorum')",
	).Scan(&syncReplicaCount))
	require.GreaterOrEqual(t, syncReplicaCount, 1,
		"no sync standby is currently registered — promoting a standby may lose data")
	t.Logf("Pre-flight: %d sync standbys registered", syncReplicaCount)

	// --- Baseline full backup so the stanza exists and the archive has real
	// content. Without this, the "archive lag" condition we force later would
	// be trivially true (archive empty) instead of meaningful.
	//
	// Backup is taken from a standby because backups from the primary are
	// disallowed by default (would require ForcePrimary). Source choice does
	// not affect what this test proves.
	var standbyInst *shardsetup.MultipoolerInstance
	for name, inst := range setup.Multipoolers {
		if name != originalPrimaryName {
			standbyInst = inst
			break
		}
	}
	require.NotNil(t, standbyInst, "expected a standby instance for the baseline backup")

	backupClient := createBackupClient(t, standbyInst.Multipooler.GrpcPort)
	baselineCtx, baselineCancel := context.WithTimeout(t.Context(), 5*time.Minute)
	baselineResp, err := backupClient.Backup(baselineCtx, &multipoolermanagerdata.BackupRequest{
		Type:  "full",
		JobId: "baseline-full",
	})
	baselineCancel()
	require.NoError(t, err, "baseline full backup should succeed")
	t.Logf("Baseline backup id=%s (taken from %s)", baselineResp.GetBackupId(), standbyInst.Multipooler.Name)

	// --- Arm the gate. From now on the next WAL archive PUT will block.
	gate.Arm()
	t.Log("Gate armed on MatchWalArchive — next archive_command PUT will block")

	// --- Start the writer. WriterValidator records id only after a successful
	// COMMIT, which under sync_commit means at least one standby has the WAL.
	connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")
	gatewayDB, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	defer gatewayDB.Close()
	require.NoError(t, gatewayDB.Ping())

	validator, validatorCleanup, err := shardsetup.NewWriterValidator(t, gatewayDB,
		shardsetup.WithWorkerCount(4),
		shardsetup.WithWriteInterval(20*time.Millisecond),
	)
	require.NoError(t, err)
	t.Cleanup(validatorCleanup)

	t.Logf("Starting WriterValidator (table=%s)", validator.TableName())
	validator.Start(t)

	// Wait until the writer has produced enough commits that the next WAL
	// segment will contain real client traffic — not just the switch record.
	const preSwitchCommits = 20
	require.Eventually(t, func() bool {
		s, _ := validator.Stats()
		return s >= preSwitchCommits
	}, 15*time.Second, 50*time.Millisecond,
		"writer should accumulate >= %d successful commits before forcing WAL switch", preSwitchCommits)

	// Force a WAL segment switch on the primary to trigger archive_command.
	// Without this the writer's modest throughput (~50 commits/sec at small
	// row sizes) will not fill a 16MB segment within the test budget, so no
	// archive PUT would ever happen.
	switchCtx, switchCancel := context.WithTimeout(t.Context(), 10*time.Second)
	var switchedLSN string
	require.NoError(t, primaryDB.QueryRowContext(switchCtx, "SELECT pg_switch_wal()::text").Scan(&switchedLSN))
	switchCancel()
	t.Logf("Forced pg_switch_wal at LSN %s — archive_command should fire", switchedLSN)

	// --- Wait for the first WAL archive PUT to be intercepted. This is the
	// proof point that primary has committed WAL not yet uploaded to the repo.
	waitCtx, waitCancel := context.WithTimeout(t.Context(), 90*time.Second)
	hit, err := gate.Wait(waitCtx)
	waitCancel()
	require.NoError(t, err, "timed out waiting for a WAL archive PUT to be intercepted")
	t.Logf("Gate hit: bucket=%s key=%s", hit.Bucket, hit.Key)

	// Let more commits accumulate against a primary whose archive is now
	// pinned behind. Each successful commit during this window is durable on a
	// sync standby per the sync_commit contract — that is the durability claim
	// this test proves the cluster honors after the kill. We wait for a
	// meaningful number of *post-archive-block* commits rather than sleeping a
	// fixed wall-clock amount.
	gateHitSuccess, _ := validator.Stats()
	const postBlockCommits = 100
	targetCommits := gateHitSuccess + postBlockCommits
	require.Eventually(t, func() bool {
		s, _ := validator.Stats()
		return s >= targetCommits
	}, 30*time.Second, 100*time.Millisecond,
		"writer should accumulate >= %d more commits after the archive was blocked (target=%d)", postBlockCommits, targetCommits)
	preKillSuccess, preKillFailed := validator.Stats()
	t.Logf("At kill: %d successful commits (%d after gate hit), %d failed",
		preKillSuccess, preKillSuccess-gateHitSuccess, preKillFailed)

	// --- Capture WAL state on each node for the diagnostic log.
	logWALState(t, setup, originalPrimaryName)

	// --- Disable the primary's postgres monitor so it cannot respawn postgres
	// between the kill and when multiorch's emergency demote sets rewindPending.
	// Mirrors dead_primary_recovery_test's pattern.
	primaryManagerClient, err := shardsetup.NewMultipoolerClient(primary.Multipooler.GrpcPort)
	require.NoError(t, err)
	_, err = primaryManagerClient.Manager.SetPostgresRestartsEnabled(utils.WithShortDeadline(t),
		&multipoolermanagerdata.SetPostgresRestartsEnabledRequest{Enabled: false})
	require.NoError(t, err)

	// --- Kill postgres on the primary (multipooler stays running to report
	// unhealthy status to multiorch).
	setup.KillPostgres(t, originalPrimaryName)

	// Release the gate so any goroutines parked inside s3mock unwind cleanly,
	// and so the new primary's archive_command can flow once promoted.
	gate.Release()

	// Stop the writer. In-flight inserts to the dead primary will error and be
	// recorded as failed; that is fine — only successful commits define the
	// durability invariant.
	validator.Stop()

	// --- Wait for multiorch to elect a new primary.
	newPrimaryName := shardsetup.WaitForNewPrimary(t, setup, originalPrimaryName, 30*time.Second)
	require.NotEmpty(t, newPrimaryName, "no new primary elected within timeout")
	t.Logf("New primary elected: %s", newPrimaryName)

	// --- Re-enable postgres restarts on the old primary: emergencyDemoteLocked
	// has already set rewindPending, so the monitor will not restart postgres
	// before DemoteStalePrimary runs.
	_, err = primaryManagerClient.Manager.SetPostgresRestartsEnabled(utils.WithShortDeadline(t),
		&multipoolermanagerdata.SetPostgresRestartsEnabledRequest{Enabled: true})
	require.NoError(t, err)
	primaryManagerClient.Close()

	newPrimary := setup.GetMultipoolerInstance(newPrimaryName)
	require.NotNil(t, newPrimary)

	// --- Verify the durability contract: every committed id must be present
	// on the new primary. Connect directly to the new primary's postgres so
	// the assertion does not depend on multigateway's reroute timing.
	newPrimarySocketDir := filepath.Join(newPrimary.Pgctld.PoolerDir, "pg_sockets")
	newPrimaryDB := connectToPostgresViaSocket(t, newPrimarySocketDir, newPrimary.Pgctld.PgPort)
	defer newPrimaryDB.Close()

	committedIDs := validator.SuccessfulWrites()
	require.NotEmpty(t, committedIDs, "no successful commits to verify against")
	t.Logf("Verifying %d committed ids against new primary %s", len(committedIDs), newPrimaryName)

	missing := findMissingIDs(t, newPrimaryDB, validator.TableName(), committedIDs)
	if len(missing) > 0 {
		showCount := min(len(missing), 20)
		t.Fatalf("DURABILITY VIOLATION: %d of %d committed ids are missing from new primary %s; first %d missing: %v",
			len(missing), len(committedIDs), newPrimaryName, showCount, missing[:showCount])
	}
	t.Logf("All %d committed ids present on new primary — sync_commit durability contract honored", len(committedIDs))

	// --- Force multiorch to fully resolve recovery: the old primary needs to be
	// demoted (DemoteStalePrimary: 5s drain + ~15s pg_rewind + ~5s pg restart) and
	// rejoined as a standby on the new primary's term. RequireRecovery blocks
	// until all pending problems are resolved.
	setup.RequireRecovery(t, "multiorch", 60*time.Second)

	// --- Verify both non-primary nodes (the surviving original standby and the
	// demoted-and-rejoined old primary) are healthy streaming replicas of the
	// new primary AND have the full set of committed ids. This proves the
	// failover completed end-to-end before we provision a fresh standby from
	// the backup repo below.
	newPrimaryClient, err := shardsetup.NewMultipoolerClient(newPrimary.Multipooler.GrpcPort)
	require.NoError(t, err)
	newPrimaryStatus, err := newPrimaryClient.Manager.Status(utils.WithShortDeadline(t), &multipoolermanagerdata.StatusRequest{})
	require.NoError(t, err)
	newPrimaryClient.Close()
	newPrimaryTerm := newPrimaryStatus.ConsensusStatus.GetTermRevocation().GetRevokedBelowTerm()
	t.Logf("New primary %s on term %d — verifying rejoined old primary", newPrimaryName, newPrimaryTerm)

	verifyReplicasStreamingOnTerm(t, setup, newPrimaryName, newPrimaryTerm, 60*time.Second)
	verifyReplicasHaveAllIDs(t, setup, newPrimaryName, validator.TableName(), committedIDs, 30*time.Second)

	// --- Provision a fresh standby (pooler-4) and let it bootstrap itself from
	// the backup repo. The multipooler's MonitorPostgres detects an empty
	// PGDATA + an available backup and auto-runs RestoreFromBackup. The
	// "restore.attempt outcome=success" event is the load-bearing assertion:
	// if the post-failover archive's WAL chain is broken across the timeline
	// change, the auto-restore fails and this WaitForEvent times out.
	const newReplicaName = "pooler-4"
	t.Logf("Provisioning %s — multipooler will auto-restore from latest backup", newReplicaName)
	newReplica := setup.CreateMultipoolerInstance(t, newReplicaName,
		utils.GetFreePort(t), utils.GetFreePort(t), utils.GetFreePort(t))
	require.NoError(t, newReplica.Pgctld.Start(t.Context(), t))
	require.NoError(t, newReplica.Multipooler.Start(t.Context(), t))

	shardsetup.WaitForEvent(t, newReplica.Multipooler.LogFile, "restore.attempt", "success", 90*time.Second)
	t.Logf("%s auto-restored from backup — post-failover archive is usable", newReplicaName)

	shardsetup.WaitForManagerReady(t, newReplica.Multipooler)

	// Drive multiorch's recovery loop synchronously so FixReplication wires
	// pooler-4's replication to the current primary on the current term.
	setup.RequireRecovery(t, "multiorch", 60*time.Second)

	// --- Verify the freshly-provisioned pooler-4 streams on the new primary's
	// term AND has every committed id. The previous block already proved the
	// other two replicas are healthy; running the same checks now covers
	// pooler-4's catch-up.
	verifyReplicasStreamingOnTerm(t, setup, newPrimaryName, newPrimaryTerm, 60*time.Second)
	verifyReplicasHaveAllIDs(t, setup, newPrimaryName, validator.TableName(), committedIDs, 60*time.Second)

	// --- Sanity: timeline incremented. Cheap evidence that failover actually
	// happened. pg_control_checkpoint() reflects the last checkpoint's
	// timeline — stale immediately after promotion until the next checkpoint
	// runs. The current WAL filename's timeline prefix updates the moment
	// promotion completes, so use that.
	var walfile string
	require.NoError(t, newPrimaryDB.QueryRow("SELECT pg_walfile_name(pg_current_wal_lsn())").Scan(&walfile))
	require.GreaterOrEqual(t, len(walfile), 8, "unexpected pg_walfile_name format: %q", walfile)
	tlHex := walfile[:8]
	var newTimeline int64
	_, scanErr := fmt.Sscanf(tlHex, "%x", &newTimeline)
	require.NoError(t, scanErr, "failed to parse timeline from walfile %q", walfile)
	assert.Greater(t, newTimeline, int64(1), "expected timeline > 1 after failover, got %d (walfile=%s)", newTimeline, walfile)
	t.Logf("New primary on timeline %d (walfile=%s)", newTimeline, walfile)
}

// logWALState writes a one-shot snapshot of WAL state across the cluster to
// the test log. Captured before the kill so a failure narrative can show that
// standbys were actually receiving WAL via streaming at the moment of crash.
func logWALState(t *testing.T, setup *shardsetup.ShardSetup, primaryName string) {
	t.Helper()

	for name, inst := range setup.Multipoolers {
		socketDir := filepath.Join(inst.Pgctld.PoolerDir, "pg_sockets")
		db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%d user=postgres dbname=postgres sslmode=disable", socketDir, inst.Pgctld.PgPort))
		if err != nil {
			t.Logf("WAL state %s: connect error: %v", name, err)
			continue
		}
		queryCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		if name == primaryName {
			var lsn, walfile string
			row := db.QueryRowContext(queryCtx, "SELECT pg_current_wal_lsn()::text, pg_walfile_name(pg_current_wal_lsn())")
			if err := row.Scan(&lsn, &walfile); err == nil {
				t.Logf("WAL state %s (primary): current_lsn=%s walfile=%s", name, lsn, walfile)
			} else {
				t.Logf("WAL state %s (primary): query error: %v", name, err)
			}
		} else {
			var receiveLsn, replayLsn string
			row := db.QueryRowContext(queryCtx, "SELECT COALESCE(pg_last_wal_receive_lsn()::text, ''), COALESCE(pg_last_wal_replay_lsn()::text, '')")
			if err := row.Scan(&receiveLsn, &replayLsn); err == nil {
				t.Logf("WAL state %s (standby): receive_lsn=%s replay_lsn=%s", name, receiveLsn, replayLsn)
			} else {
				t.Logf("WAL state %s (standby): query error: %v", name, err)
			}
		}
		cancel()
		_ = db.Close()
	}
}

// verifyReplicasStreamingOnTerm waits until every multipooler other than
// excludePrimaryName reports REPLICA + PostgresReady + streaming wal_receiver +
// the expected term. Used after each recovery step to confirm the cohort is
// healthy before the next assertion.
func verifyReplicasStreamingOnTerm(t *testing.T, setup *shardsetup.ShardSetup, excludePrimaryName string, expectedTerm int64, timeout time.Duration) {
	t.Helper()

	var replicas []*shardsetup.MultipoolerInstance
	for name, inst := range setup.Multipoolers {
		if name == excludePrimaryName {
			continue
		}
		replicas = append(replicas, inst)
	}
	require.NotEmpty(t, replicas, "expected at least one non-primary multipooler")

	shardsetup.EventuallyPoolerCondition(t, replicas, timeout, 1*time.Second,
		func(r shardsetup.PoolerStatusResult) (bool, string) {
			s := r.Status
			if s.PoolerType != clustermetadatapb.PoolerType_REPLICA {
				return false, fmt.Sprintf("not yet REPLICA (is %v)", s.PoolerType)
			}
			if !s.PostgresReady {
				return false, "postgres not running"
			}
			if s.ReplicationStatus == nil || s.ReplicationStatus.PrimaryConnInfo == nil {
				return false, "replication not configured"
			}
			if s.ReplicationStatus.WalReceiverStatus != "streaming" {
				return false, fmt.Sprintf("not streaming (wal_receiver=%s)", s.ReplicationStatus.WalReceiverStatus)
			}
			termNum := r.ConsensusStatus.GetTermRevocation().GetRevokedBelowTerm()
			if termNum != expectedTerm {
				return false, fmt.Sprintf("wrong term %d, expected %d", termNum, expectedTerm)
			}
			return true, ""
		},
		"replicas did not converge to streaming on term %d", expectedTerm,
	)
}

// verifyReplicasHaveAllIDs queries each non-primary multipooler's postgres
// directly via Unix socket and asserts every id in want is present. Replication
// is async, so it polls until the replica catches up or times out.
func verifyReplicasHaveAllIDs(t *testing.T, setup *shardsetup.ShardSetup, excludePrimaryName, table string, want []int64, timeout time.Duration) {
	t.Helper()

	for name, replica := range setup.Multipoolers {
		if name == excludePrimaryName {
			continue
		}
		replicaSocketDir := filepath.Join(replica.Pgctld.PoolerDir, "pg_sockets")
		replicaDB := connectToPostgresViaSocket(t, replicaSocketDir, replica.Pgctld.PgPort)
		require.Eventually(t, func() bool {
			missing := findMissingIDs(t, replicaDB, table, want)
			if len(missing) > 0 {
				t.Logf("replica %s still missing %d/%d ids", replica.Name, len(missing), len(want))
				return false
			}
			return true
		}, timeout, 1*time.Second,
			"replica %s did not catch up to all %d committed ids", replica.Name, len(want))
		replicaDB.Close()
		t.Logf("Replica %s has all %d committed ids", replica.Name, len(want))
	}
}
