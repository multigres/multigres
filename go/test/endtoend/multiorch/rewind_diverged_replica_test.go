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

package multiorch

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestRewindDivergedReplica tests multiorch's ability to detect a replica with a
// diverged timeline (higher LSN than the primary on a different timeline) and repair
// it using pg_rewind so it can rejoin replication.
//
// This test exercises FixReplicationAction.tryPgRewind(), which is the code path
// taken when fixNotReplicating sets primary_conninfo but the WAL receiver fails
// to start because of a timeline divergence.
//
// Scenario:
//  1. 3-node cluster: primary (P) + 2 standbys (R1, R2)
//  2. Stop orch after shard is ready
//  3. Stop R1's WAL receiver, then promote R1 via pg_promote() — R1 is now on timeline 2
//  4. Write a diverging row to R1 (exists on timeline 2 only)
//  5. Restart R1 as a standby — WAL receiver fails due to timeline divergence
//  6. Restart orch — detects R1 not replicating, tries SetPrimaryConnInfo,
//     WAL receiver fails → tryPgRewind → RewindToSource RPC → pg_rewind runs
//  7. Verify R1 rejoins P with the diverged row absent and baseline data present
func TestRewindDivergedReplica(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestRewindDivergedReplica test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end diverged replica rewind test (no postgres binaries)")
	}

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiOrchCount(1),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
	)
	defer cleanup()

	setup.StartMultiOrchs(t.Context(), t)

	pName := waitForShardReady(t, setup, 2, 30*time.Second)
	t.Logf("Shard ready: primary=%s", pName)

	// Pick one replica to diverge
	var r1Name string
	for name := range setup.Multipoolers {
		if name != pName {
			r1Name = name
			break
		}
	}
	require.NotEmpty(t, r1Name, "should have a replica")
	t.Logf("Replica to diverge: %s", r1Name)

	// Write baseline data to primary and wait for it to replicate to R1
	primaryInst := setup.GetMultipoolerInstance(pName)
	require.NotNil(t, primaryInst, "primary instance should exist")
	primarySocketDir := filepath.Join(primaryInst.Pgctld.PoolerDir, "pg_sockets")
	primaryDB := connectToPostgres(t, primarySocketDir, primaryInst.Pgctld.PgPort)
	defer primaryDB.Close()

	_, err := primaryDB.Exec("CREATE TABLE IF NOT EXISTS rewind_diverged_test (id SERIAL PRIMARY KEY, data TEXT)")
	require.NoError(t, err, "should create test table on primary")

	_, err = primaryDB.Exec("INSERT INTO rewind_diverged_test (data) VALUES ('baseline')")
	require.NoError(t, err, "should write baseline data to primary")

	r1Inst := setup.GetMultipoolerInstance(r1Name)
	require.NotNil(t, r1Inst, "r1 instance should exist")
	r1SocketDir := filepath.Join(r1Inst.Pgctld.PoolerDir, "pg_sockets")

	r1DB := connectToPostgres(t, r1SocketDir, r1Inst.Pgctld.PgPort)
	defer r1DB.Close()

	require.Eventually(t, func() bool {
		row := r1DB.QueryRow("SELECT COUNT(*) FROM rewind_diverged_test WHERE data = 'baseline'")
		var count int
		if err := row.Scan(&count); err != nil {
			return false
		}
		return count == 1
	}, 10*time.Second, 200*time.Millisecond, "baseline data should replicate to R1")
	t.Log("Baseline data verified on R1")

	// Make sure orch is in a clean state (no transient problems from bootstrap).
	setup.RequireRecovery(t, "multiorch", 5*time.Second)

	// Pause recovery so orch doesn't interfere with the manual divergence creation
	setup.DisableRecovery(t, "multiorch")

	// Create R1 multipooler client
	r1Client, err := shardsetup.NewMultipoolerClient(r1Inst.Multipooler.GrpcPort)
	require.NoError(t, err, "should create R1 multipooler client")
	defer r1Client.Close()

	// Stop R1's WAL receiver (postgres stays running)
	_, err = r1Client.Manager.StopReplication(t.Context(), &multipoolermanagerdatapb.StopReplicationRequest{
		Mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_RECEIVER_ONLY,
		Wait: true,
	})
	require.NoError(t, err, "should stop R1 WAL receiver")

	waitForReplicationBroken(t, r1Inst, 10*time.Second)
	t.Log("R1 WAL receiver confirmed stopped")

	// Promote R1 to a standalone primary on timeline 2
	_, err = r1DB.Exec("SELECT pg_promote(); CHECKPOINT")
	require.NoError(t, err, "pg_promote() should succeed on R1")
	t.Log("R1 promoted to timeline 2")

	// Wait for R1 postgres to accept writes (promotion is processed asynchronously)
	require.Eventually(t, func() bool {
		_, execErr := r1DB.Exec("SELECT 1")
		return execErr == nil
	}, 10*time.Second, 200*time.Millisecond, "R1 should be writable after promotion")

	// Write a diverging row to R1 on timeline 2 — this row must not exist on P
	_, err = r1DB.Exec("INSERT INTO rewind_diverged_test (data) VALUES ('diverged_on_r1')")
	require.NoError(t, err, "should write diverging data to R1")
	t.Log("Wrote diverging data to R1 on timeline 2")

	// Close R1 DB connection before stopping postgres
	_ = r1DB.Close()

	// Stop R1's postgres (currently running as a promoted primary on timeline 2).
	// StopPostgres disables auto-restarts before stopping, preventing the monitor
	// from immediately restarting the instance. resumeRestarts re-enables them.
	resumeRestarts := setup.StopPostgres(t, r1Name, "fast")
	t.Log("Stopped R1 postgres")

	r1PgctldClient, err := shardsetup.NewPgctldClient(r1Inst.Pgctld.GrpcPort)
	require.NoError(t, err, "should create R1 pgctld client")
	defer r1PgctldClient.Close()

	// Restart R1 as a standby — creates standby.signal; postgres starts with no
	// primary_conninfo (cleared earlier), so the WAL receiver doesn't start yet.
	// Orch will detect not-replicating, set primary_conninfo, get a timeline
	// divergence error from the WAL receiver, and fall through to tryPgRewind.
	_, err = r1PgctldClient.Restart(t.Context(), &pgctldpb.RestartRequest{
		Mode:      "fast",
		Timeout:   durationpb.New(15 * time.Second),
		AsStandby: true,
	})
	require.NoError(t, err, "should restart R1 as standby")
	t.Log("Restarted R1 as standby (timeline 2 WAL present, no primary_conninfo)")

	// Clear the manual-stop flag set by StopReplication(RECEIVER_ONLY). Without this,
	// SetTermPrimary refuses to write primary_conninfo (walReceiverManuallyStopped check
	// in setPrimaryConnInfoLocked), blocking orch from re-establishing replication.
	_, err = r1Client.Manager.StartReplication(t.Context(), &multipoolermanagerdatapb.StartReplicationRequest{})
	require.NoError(t, err, "should clear manual stop flag on R1")

	// Re-enable postgres restarts so multipooler manages R1 going forward
	resumeRestarts()

	// Block until orch fully repairs R1 via pg_rewind.
	setup.RequireRecovery(t, "multiorch", 30*time.Second)

	// Verify data consistency: baseline present, diverged row absent
	r1DBAfter := connectToPostgres(t, r1SocketDir, r1Inst.Pgctld.PgPort)
	defer r1DBAfter.Close()

	require.Eventually(t, func() bool {
		row := r1DBAfter.QueryRow("SELECT COUNT(*) FROM rewind_diverged_test WHERE data = 'baseline'")
		var count int
		if err := row.Scan(&count); err != nil {
			return false
		}
		return count == 1
	}, 10*time.Second, 500*time.Millisecond, "baseline data should be on R1 after pg_rewind")

	row := r1DBAfter.QueryRow("SELECT COUNT(*) FROM rewind_diverged_test WHERE data = 'diverged_on_r1'")
	var divergedCount int
	err = row.Scan(&divergedCount)
	require.NoError(t, err, "should query R1 for diverged row")
	require.Equal(t, 0, divergedCount, "diverged row should NOT be present on R1 after pg_rewind")
	t.Log("Data consistency verified: baseline present, diverged row absent")

	// Verify R1 is streaming from P and added to P's synchronous standby list
	verifyReplicaReplicating(t, setup, r1Name, pName)

	primaryClient, err := shardsetup.NewMultipoolerClient(primaryInst.Multipooler.GrpcPort)
	require.NoError(t, err, "should create primary multipooler client")
	defer primaryClient.Close()

	require.Eventually(t, func() bool {
		return isReplicaInStandbyList(t, primaryClient, r1Name)
	}, 15*time.Second, 1*time.Second, "R1 should be added to P's synchronous standby list")
	t.Log("R1 is in P's synchronous standby list")

	t.Log("TestRewindDivergedReplica completed successfully")
}

// TODO: TestRewindRejectedByHigherTerm — verify that orch handles a replica that has
// advanced its term revocation beyond the primary's current rule. The old BeginTerm(NO_ACTION)
// API used to set this up has been removed; the new approach would use Recruit() to advance
// R1's RevokedBelowTerm, then verify orch's SetTermPrimary is treated as a no-op and the
// correct recovery path is taken.
