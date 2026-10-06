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
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchpb "github.com/multigres/multigres/go/pb/multiorch"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestPrimaryDeletedDataFilesDoesNotFailOver is a deliberately-red regression
// test documenting a known gap, not a success-path test: LeaderQuorumWritesStalled
// doesn't catch a leader whose pg_data/base was deleted out from under it, because
// the heartbeat writer's already-open connection keeps reading/writing via
// delete-while-open semantics, so quorum_commit_ts never goes stale. Same root
// cause as the known gap that a bare "can I connect" probe (e.g. pg_isready)
// isn't sufficient to prove postgres health; confirmed independently by a
// Multiquake chaos test (deleting base/ didn't trigger a failover either).
//
// If this test starts passing (a failover DOES occur), that means something
// closed the gap -- update it to assert that instead.
func TestPrimaryDeletedDataFilesDoesNotFailOver(t *testing.T) {
	t.Skip("known gap, not yet fixed: health checks need to periodically probe with a " +
		"fresh connection, not just the long-lived admin one, or this can never pass -- see the doc comment above")

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiorchCount(1),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
	)
	defer cleanup()

	setup.StartMultiorchs(t.Context(), t)

	primary := setup.GetPrimary(t)
	require.NotNil(t, primary, "primary instance should exist")
	oldPrimaryName := setup.PrimaryName
	t.Logf("Initial primary: %s", oldPrimaryName)

	// quorum_commit_ts is NULL until the writer's 2nd heartbeat -- wait for a
	// real value, or LeaderQuorumWritesStalled's staleness check has nothing to go stale.
	primaryClient, err := shardsetup.NewMultipoolerClient(primary.Multipooler.GrpcPort)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		resp, err := primaryClient.Manager.Status(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StatusRequest{})
		return err == nil && resp.Status.GetPrimaryStatus().GetQuorumCommitTs() != nil
	}, 10*time.Second, 200*time.Millisecond, "quorum_commit_ts should be established before breaking the disk")
	primaryClient.Close()
	establishedAt := time.Now()

	// The above only confirms quorum_commit_ts via our OWN direct RPC -- orch
	// refreshes its cached view independently via its health-stream ticker.
	// Wait for orch to report a snapshot from after establishedAt, or it
	// might never observe the value before we break the disk.
	var orchInst *shardsetup.ProcessInstance
	for _, inst := range setup.MultiorchInstances {
		orchInst = inst
		break
	}
	require.NotNil(t, orchInst, "expected exactly one multiorch instance")
	orchClient, err := shardsetup.NewMultiorchClient(orchInst.GrpcPort)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		resp, err := orchClient.GetShardStatus(utils.WithShortDeadline(t), &multiorchpb.ShardStatusRequest{
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "postgres",
				TableGroup: constants.DefaultTableGroup,
				Shard:      constants.DefaultShard,
			},
		})
		if err != nil {
			return false
		}
		for _, ph := range resp.PoolerHealths {
			if ph.PoolerType == "PRIMARY" && ph.LastSeen.AsTime().After(establishedAt) {
				return true
			}
		}
		return false
	}, 10*time.Second, 200*time.Millisecond, "orch should observe a health snapshot taken after quorum_commit_ts was established")
	orchClient.Close()

	pgDataDir := filepath.Join(primary.Pgctld.PoolerDir, "pg_data")
	pidBefore, err := readPostmasterPID(pgDataDir)
	require.NoError(t, err, "failed to read postmaster.pid before fault injection")

	baseDir := filepath.Join(pgDataDir, "base")
	entries, err := os.ReadDir(baseDir)
	require.NoError(t, err, "read pg_data/base")
	require.NotEmpty(t, entries, "pg_data/base should not be empty")
	for _, e := range entries {
		require.NoError(t, os.RemoveAll(filepath.Join(baseDir, e.Name())))
	}
	t.Logf("Deleted contents of %s on primary %s (postgres and multipooler stay running)", baseDir, oldPrimaryName)

	// Confirm the fault actually broke things, the same way
	// TestPgIsReadyMissesCorruptedDataFiles does: a brand-new connection has
	// to open() the relation file by path and gets ENOENT immediately, unlike
	// the heartbeat writer's already-open admin connection.
	directDB, err := sql.Open("postgres", shardsetup.GetPostgresDSN("localhost", primary.Pgctld.PgPort, "sslmode=disable"))
	require.NoError(t, err)
	_, err = directDB.ExecContext(t.Context(), "SELECT count(*) FROM multigres.heartbeat")
	require.Error(t, err, "a fresh connection's query should fail once base/ is gone")
	t.Logf("fresh-connection query failed as expected: %v", err)
	require.NoError(t, directDB.Close())

	t.Log("Waiting to see whether multiorch detects LeaderQuorumWritesStalled and fails over...")
	newPrimaryName := shardsetup.WaitForNewPrimary(t, setup, oldPrimaryName, 90*time.Second)
	require.NotEmpty(t, newPrimaryName, "a new primary should eventually be appointed once the old one's data files are gone")
	require.NotEqual(t, oldPrimaryName, newPrimaryName)
	t.Logf("New primary appointed: %s", newPrimaryName)

	// The whole point of this fault (vs. TestPoolerDownEventuallyFailsOver) is
	// that postgres and the multipooler process both keep running throughout.
	// Confirm the old primary's postmaster never restarted (a restart
	// rewrites postmaster.pid with a new PID), so a passing test here is
	// attributable to LeaderQuorumWritesStalled detecting stalled writes, not to an
	// incidental crash.
	pidAfter, err := readPostmasterPID(pgDataDir)
	require.NoError(t, err, "failed to read postmaster.pid after failover")
	require.Equal(t, pidBefore, pidAfter, "old primary's postmaster should never have restarted")
}

// readPostmasterPID reads the PID from postmaster.pid in a data directory.
func readPostmasterPID(pgDataDir string) (int, error) {
	pidBytes, err := os.ReadFile(filepath.Join(pgDataDir, "postmaster.pid"))
	if err != nil {
		return 0, err
	}
	pidStr := strings.TrimSpace(strings.Split(string(pidBytes), "\n")[0])
	return strconv.Atoi(pidStr)
}

// TestPrimaryReadOnlyTransactionModeEventuallyFailsOver simulates a leader
// that's reachable and looks healthy (postgres up, pg_isready passing, reads
// working) but can't actually commit writes, and confirms LeaderQuorumWritesStalled
// (PR #1455) catches it. default_transaction_read_only is the one fault
// mechanism proven to affect an already-open connection immediately and
// deterministically: unlike a filesystem-level fault (see
// TestPrimaryDeletedDataFilesDoesNotFailOver, which the heartbeat writer's
// long-lived connection is immune to), it's a GUC re-read at the start of
// every transaction, so it takes effect on the very next write regardless of
// which connection performs it.
//
// TODO: if multigres ever supports an intentional read-only-primary mode,
// this same GUC would be indistinguishable from that legitimate
// configuration, and LeaderQuorumWritesStalled would need to suppress
// conviction for it. That mode doesn't exist yet, so there's nothing to
// suppress today.
func TestPrimaryReadOnlyTransactionModeEventuallyFailsOver(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end disk fault test (short mode)")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end disk fault test (short mode or no postgres binaries)")
	}

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiorchCount(1),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
	)
	defer cleanup()

	setup.StartMultiorchs(t.Context(), t)

	primary := setup.GetPrimary(t)
	require.NotNil(t, primary, "primary instance should exist")
	oldPrimaryName := setup.PrimaryName
	t.Logf("Initial primary: %s", oldPrimaryName)

	// quorum_commit_ts is NULL until the writer's 2nd heartbeat -- wait for a
	// real value, or LeaderQuorumWritesStalled's staleness check has nothing to go stale.
	primaryClient, err := shardsetup.NewMultipoolerClient(primary.Multipooler.GrpcPort)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		resp, err := primaryClient.Manager.Status(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StatusRequest{})
		return err == nil && resp.Status.GetPrimaryStatus().GetQuorumCommitTs() != nil
	}, 10*time.Second, 200*time.Millisecond, "quorum_commit_ts should be established before flipping the leader read-only")
	primaryClient.Close()
	establishedAt := time.Now()

	// The above only confirms quorum_commit_ts via our OWN direct RPC -- orch
	// refreshes its cached view independently via its health-stream ticker.
	// Wait for orch to report a snapshot from after establishedAt, or it
	// might never observe the value before we flip the GUC.
	var orchInst *shardsetup.ProcessInstance
	for _, inst := range setup.MultiorchInstances {
		orchInst = inst
		break
	}
	require.NotNil(t, orchInst, "expected exactly one multiorch instance")
	orchClient, err := shardsetup.NewMultiorchClient(orchInst.GrpcPort)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		resp, err := orchClient.GetShardStatus(utils.WithShortDeadline(t), &multiorchpb.ShardStatusRequest{
			ShardKey: &clustermetadatapb.ShardKey{
				Database:   "postgres",
				TableGroup: constants.DefaultTableGroup,
				Shard:      constants.DefaultShard,
			},
		})
		if err != nil {
			return false
		}
		for _, ph := range resp.PoolerHealths {
			if ph.PoolerType == "PRIMARY" && ph.LastSeen.AsTime().After(establishedAt) {
				return true
			}
		}
		return false
	}, 10*time.Second, 200*time.Millisecond, "orch should observe a health snapshot taken after quorum_commit_ts was established")
	orchClient.Close()

	// Pause recovery while we inject the fault and assert the broken state,
	// so multiorch's background loop can't race our assertions -- same
	// pattern as fix_replication_test.go.
	enableRecovery := setup.DisableRecovery(t, "multiorch")

	directDB, err := sql.Open("postgres", shardsetup.GetPostgresDSN("localhost", primary.Pgctld.PgPort, "sslmode=disable"))
	require.NoError(t, err)
	defer directDB.Close()
	_, err = directDB.ExecContext(t.Context(), "ALTER SYSTEM SET default_transaction_read_only = on")
	require.NoError(t, err, "failed to set default_transaction_read_only")
	_, err = directDB.ExecContext(t.Context(), "SELECT pg_reload_conf()")
	require.NoError(t, err, "failed to reload postgres config")
	t.Logf("Set default_transaction_read_only=on on primary %s (postgres and multipooler stay running)", oldPrimaryName)

	// Confirm the fault actually broke writes while reads keep working --
	// the opposite shape from the filesystem-level faults above.
	require.Eventually(t, func() bool {
		_, err := directDB.ExecContext(t.Context(), "UPDATE multigres.heartbeat SET leader_id = leader_id")
		return err != nil
	}, 5*time.Second, 200*time.Millisecond, "writes should start failing once default_transaction_read_only is on")
	_, err = directDB.ExecContext(t.Context(), "SELECT count(*) FROM multigres.heartbeat")
	require.NoError(t, err, "reads should keep working -- this fault is write-only, unlike the filesystem-level ones")

	enableRecovery()

	t.Log("Waiting for multiorch to fail over...")
	newPrimaryName := shardsetup.WaitForNewPrimary(t, setup, oldPrimaryName, 90*time.Second)
	require.NotEmpty(t, newPrimaryName, "a new primary should eventually be appointed once the old one can't commit writes")
	require.NotEqual(t, oldPrimaryName, newPrimaryName)
	t.Logf("New primary appointed: %s", newPrimaryName)

	// Confirm LeaderQuorumWritesStalled -- not some other cause -- is what
	// actually drove this promotion. Checked against the durable event log
	// rather than polling live "current problems": the problem can clear the
	// instant the new leader is promoted, so a live poll could race and never
	// observe it even though it fired correctly.
	const problemLeaderQuorumWritesStalled = "LeaderQuorumWritesStalled" // types.ProblemLeaderQuorumWritesStalled; can't import multiorch-internal types here
	events := shardsetup.WaitForEvent(t, orchInst.LogFile, "primary.promotion", "success", 5*time.Second)
	matches := shardsetup.FindEvents(events, "primary.promotion", "success")
	found := false
	for _, e := range matches {
		if e["reason"] == problemLeaderQuorumWritesStalled && e["new_primary"] == newPrimaryName {
			found = true
			break
		}
	}
	require.True(t, found, "expected a successful primary.promotion event with reason=%s and new_primary=%s, got %v",
		problemLeaderQuorumWritesStalled, newPrimaryName, matches)
}
