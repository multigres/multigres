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
//  2. Disable orch recovery so it doesn't interfere with divergence creation
//  3. Promote R1 via pg_promote() — R1 is now on a higher timeline
//  4. Write a diverging row to R1 (exists on the new timeline only)
//  5. Restart R1 as a standby — WAL receiver starts, immediately FAILs due to
//     timeline conflict (R1's timeline > P's), enters retry-wait state
//  6. Re-enable orch — detects WAL receiver not streaming, verifyReplicationStarted
//     times out → tryPgRewind → RewindToSource RPC → pg_rewind runs
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
		shardsetup.WithMultiorchCount(1),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
	)
	defer cleanup()

	setup.StartMultiorchs(t.Context(), t)

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
	setup.RequireRecovery(t, "multiorch", shardsetup.RecoveryScenarioInitialSettle)

	// Pause recovery so orch doesn't interfere with the manual divergence creation
	setup.DisableRecovery(t, "multiorch")

	// Promote R1 to a standalone primary on a new timeline.
	// pg_promote() stops the WAL receiver internally as part of the promotion
	// sequence, so we do not need to stop it manually first.
	_, err = r1DB.Exec("SELECT pg_promote(); CHECKPOINT")
	require.NoError(t, err, "pg_promote() should succeed on R1")
	t.Log("R1 promoted to a new timeline")

	// Wait for R1 postgres to accept writes (promotion is processed asynchronously)
	require.Eventually(t, func() bool {
		_, execErr := r1DB.Exec("SELECT 1")
		return execErr == nil
	}, 10*time.Second, 200*time.Millisecond, "R1 should be writable after promotion")

	// Write a diverging row to R1 — this row must not exist on P
	_, err = r1DB.Exec("INSERT INTO rewind_diverged_test (data) VALUES ('diverged_on_r1')")
	require.NoError(t, err, "should write diverging data to R1")
	t.Log("Wrote diverging data to R1 on new timeline")

	// Close R1 DB connection before stopping postgres
	_ = r1DB.Close()

	// Stop R1's postgres (currently running as a promoted primary).
	// StopPostgres disables auto-restarts before stopping, preventing the monitor
	// from immediately restarting the instance. resumeRestarts re-enables them.
	resumeRestarts := setup.StopPostgres(t, r1Name, "fast")
	t.Log("Stopped R1 postgres")

	r1PgctldClient, err := shardsetup.NewPgctldClient(r1Inst.Pgctld.GrpcPort)
	require.NoError(t, err, "should create R1 pgctld client")
	defer r1PgctldClient.Close()

	// Restart R1 as a standby. primary_conninfo is already set (from before the
	// promotion), so the WAL receiver starts immediately and FAILs with a timeline
	// conflict (R1's timeline is higher than P's). verifyReplicaNotReplicating
	// requires both "streaming" status and a non-empty pg_last_wal_receive_lsn(),
	// which stays NULL until WAL is actually written — so the brief "streaming"
	// window during a reconnect attempt is not mistaken for healthy replication,
	// and fixNotReplicating falls through to tryPgRewind.
	_, err = r1PgctldClient.Restart(t.Context(), &pgctldpb.RestartRequest{
		Mode:      "fast",
		Timeout:   durationpb.New(15 * time.Second),
		AsStandby: true,
	})
	require.NoError(t, err, "should restart R1 as standby")
	t.Log("Restarted R1 as standby (diverged timeline, primary_conninfo set)")

	// Re-enable postgres restarts so multipooler manages R1 going forward
	resumeRestarts()

	// Block until orch fully repairs R1 via pg_rewind (fix-replication path).
	setup.RequireRecovery(t, "multiorch", shardsetup.RecoveryScenarioFixReplication)

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
	// Verify R1 is streaming from P and added to P's synchronous standby list
	verifyReplicaReplicating(t, setup, r1Name, pName)

	primaryClient, err := shardsetup.NewMultipoolerClient(primaryInst.Multipooler.GrpcPort)
	require.NoError(t, err, "should create primary multipooler client")
	defer primaryClient.Close()

	require.Eventually(t, func() bool {
		return isReplicaInStandbyList(t, primaryClient, r1Name)
	}, 15*time.Second, 1*time.Second, "R1 should be added to P's synchronous standby list")
	t.Log("R1 is in P's synchronous standby list")
}
