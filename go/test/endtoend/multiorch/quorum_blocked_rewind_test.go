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
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestQuorumBlockedRewindDuringDoubleFailure reproduces a suspected deadlock in
// recovery from a double node loss: the only surviving non-leader cohort member
// must pg_rewind against the newly promoted leader before it can supply the
// synchronous ack the leader's own rule write is blocked on — but the leader
// only advertises rewind-readiness once its own consensus cache reflects its
// new leadership, which (as currently implemented) only happens after that
// same blocked rule write succeeds. See the "rewind readiness" investigation
// this test codifies for the full analysis.
//
// Scenario (3-node cluster, default AT_LEAST_2 durability):
//  1. A is primary; B and C are standbys.
//  2. A is given WAL that never reaches B or C (both paused), then stopped —
//     A is now durably diverged relative to whatever B/C become.
//  3. Orch fails over to whichever of B/C is elected (call it S1); a write
//     through S1 advances the other standby (S2) beyond A.
//  4. S1 is stopped too. Only A (diverged, down) and S2 (most advanced, up)
//     remain of the original cohort.
//  5. A's postgres is restarted (as a standby, per default behavior). Recovery
//     now depends on promoting S2 and having A pg_rewind against it to supply
//     the one ack AT_LEAST_2 requires — with no other cohort member alive to
//     ever supply it instead.
//
// Expected (once fixed): S2 is promoted and A rewinds + rejoins within one
// ordinary recovery round, matching the budget already used by comparable
// rewind-driven scenarios (TestRewindDivergedReplica's
// RecoveryScenarioFixReplication, 30s scaled).
func TestQuorumBlockedRewindDuringDoubleFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestQuorumBlockedRewindDuringDoubleFailure test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end quorum-blocked-rewind test (no postgres binaries)")
	}

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiorchCount(1),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
	)
	defer cleanup()

	setup.StartMultiorchs(t.Context(), t)

	aName := waitForShardReady(t, setup, 2, 30*time.Second)
	t.Logf("Initial primary: %s", aName)

	var standbyNames []string
	for name := range setup.Multipoolers {
		if name != aName {
			standbyNames = append(standbyNames, name)
		}
	}
	require.Len(t, standbyNames, 2, "expected exactly 2 standbys")
	bName, cName := standbyNames[0], standbyNames[1]
	t.Logf("Standbys: %s, %s", bName, cName)

	aInst := setup.GetMultipoolerInstance(aName)
	require.NotNil(t, aInst)
	aSocketDir := filepath.Join(aInst.Pgctld.PoolerDir, "pg_sockets")
	aDB := connectToPostgres(t, aSocketDir, aInst.Pgctld.PgPort)
	defer aDB.Close()

	bInst := setup.GetMultipoolerInstance(bName)
	require.NotNil(t, bInst)
	bSocketDir := filepath.Join(bInst.Pgctld.PoolerDir, "pg_sockets")
	bDB := connectToPostgres(t, bSocketDir, bInst.Pgctld.PgPort)
	defer bDB.Close()

	cInst := setup.GetMultipoolerInstance(cName)
	require.NotNil(t, cInst)
	cSocketDir := filepath.Join(cInst.Pgctld.PoolerDir, "pg_sockets")
	cDB := connectToPostgres(t, cSocketDir, cInst.Pgctld.PgPort)
	defer cDB.Close()

	_, err := aDB.Exec("CREATE TABLE IF NOT EXISTS quorum_test (id SERIAL PRIMARY KEY, data TEXT)")
	require.NoError(t, err, "should create test table on A")
	_, err = aDB.Exec("INSERT INTO quorum_test (data) VALUES ('baseline')")
	require.NoError(t, err, "should write baseline data to A")

	waitForRow := func(db *sql.DB, data string, timeout time.Duration, msg string) {
		require.Eventually(t, func() bool {
			row := db.QueryRow("SELECT COUNT(*) FROM quorum_test WHERE data = $1", data)
			var count int
			if err := row.Scan(&count); err != nil {
				return false
			}
			return count == 1
		}, timeout, 200*time.Millisecond, msg)
	}
	waitForRow(bDB, "baseline", utils.ScaleTimeout(10*time.Second), "baseline should replicate to B")
	waitForRow(cDB, "baseline", utils.ScaleTimeout(10*time.Second), "baseline should replicate to C")
	t.Log("Baseline data verified on B and C")

	setup.RequireRecovery(t, "multiorch", shardsetup.RecoveryScenarioInitialSettle)

	// Pause orch while we manufacture A's divergence and take it down.
	resumeRecovery := setup.DisableRecovery(t, "multiorch")

	// Pause both standbys' WAL receivers so the next write to A can never reach
	// them — this guarantees genuine WAL divergence on A, deterministically,
	// rather than racing on shutdown timing.
	bClient, err := shardsetup.NewMultipoolerClient(bInst.Multipooler.GrpcPort)
	require.NoError(t, err)
	defer bClient.Close()
	cClient, err := shardsetup.NewMultipoolerClient(cInst.Multipooler.GrpcPort)
	require.NoError(t, err)
	defer cClient.Close()

	_, err = bClient.Manager.StopReplication(utils.WithTimeout(t, 10*time.Second), &multipoolermanagerdatapb.StopReplicationRequest{
		Mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_AND_RECEIVER,
	})
	require.NoError(t, err, "should pause replication on B")
	_, err = cClient.Manager.StopReplication(utils.WithTimeout(t, 10*time.Second), &multipoolermanagerdatapb.StopReplicationRequest{
		Mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_AND_RECEIVER,
	})
	require.NoError(t, err, "should pause replication on C")

	// synchronous_commit=local bypasses waiting for a standby ack for this one
	// transaction (both standbys are paused, so a normal write would hang
	// forever) while still durably committing the row on A alone.
	_, err = aDB.Exec("BEGIN; SET LOCAL synchronous_commit TO local; " +
		"INSERT INTO quorum_test (data) VALUES ('diverged_on_a'); COMMIT;")
	require.NoError(t, err, "should write diverging data to A")
	t.Log("Wrote diverging data to A; B and C never received it (paused)")

	// Stop A's postgres (auto-restart disabled) while diverged.
	resumeA := setup.StopPostgres(t, aName, "fast")
	t.Logf("Stopped A (%s) postgres while diverged", aName)

	// Resume B and C so the upcoming failover can proceed normally between them.
	_, err = bClient.Manager.StartReplication(utils.WithTimeout(t, 10*time.Second), &multipoolermanagerdatapb.StartReplicationRequest{})
	require.NoError(t, err, "should resume replication on B")
	_, err = cClient.Manager.StartReplication(utils.WithTimeout(t, 10*time.Second), &multipoolermanagerdatapb.StartReplicationRequest{})
	require.NoError(t, err, "should resume replication on C")

	resumeRecovery()

	// Orch should fail over to whichever of B/C it elects (S1); A is unreachable.
	s1Name := shardsetup.WaitForNewPrimary(t, setup, aName, 30*time.Second)
	require.NotEmpty(t, s1Name, "expected multiorch to elect a new primary from B/C")
	t.Logf("First successor primary: %s", s1Name)

	var s2Name string
	if s1Name == bName {
		s2Name = cName
	} else {
		s2Name = bName
	}

	// Not RequireRecovery here: A is deliberately still down at this point, so
	// "ReplicaNotReplicating@A" never clears and a blanket wait would time out.
	// S2 was already streaming from S1 immediately after the failover (B/C
	// were never diverged from each other), so proceed straight to the write.

	// Write through S1 so S2 advances past A.
	s1Inst := setup.GetMultipoolerInstance(s1Name)
	require.NotNil(t, s1Inst)
	s1SocketDir := filepath.Join(s1Inst.Pgctld.PoolerDir, "pg_sockets")
	s1DB := connectToPostgres(t, s1SocketDir, s1Inst.Pgctld.PgPort)
	_, err = s1DB.Exec("INSERT INTO quorum_test (data) VALUES ('post_failover_1')")
	require.NoError(t, err, "should write through new primary S1")
	_ = s1DB.Close()

	s2Inst := setup.GetMultipoolerInstance(s2Name)
	require.NotNil(t, s2Inst)
	s2SocketDir := filepath.Join(s2Inst.Pgctld.PoolerDir, "pg_sockets")
	s2DB := connectToPostgres(t, s2SocketDir, s2Inst.Pgctld.PgPort)
	waitForRow(s2DB, "post_failover_1", utils.ScaleTimeout(10*time.Second), "post-failover write should replicate to S2")
	_ = s2DB.Close()
	t.Logf("S2 (%s) confirmed ahead of A", s2Name)

	// Stop S1 too — only A (diverged, down) and S2 (advanced, up) remain.
	_ = setup.StopPostgres(t, s1Name, "fast")
	t.Logf("Stopped S1 (%s) postgres", s1Name)

	// Bring A back — restarts as a standby against its stale recorded primary;
	// recovery must promote S2 and get A to rewind + ack.
	resumeA()
	t.Logf("Resumed A (%s) postgres restarts", aName)

	// This is the assertion expected to currently fail (or take far longer than
	// one recovery round): S2 promoted, A rewound and rejoined as its standby.
	waitForNodeToRejoinAsStandby(t, setup, aName, s2Name, 0, utils.ScaleTimeout(30*time.Second))
}
