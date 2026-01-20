// Copyright 2025 Supabase, Inc.
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

// Package endtoend contains integration tests for multigres components.
//
// Leader reelection tests:
//   - TestDeadPrimaryRecovery: Verifies multiorch detects primary failure and
//     automatically elects a new leader from remaining standbys.
package multiorch

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestDeadPrimaryRecovery tests multiorch's ability to detect a primary failure
// and elect a new primary from the standbys.
func TestDeadPrimaryRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestDeadPrimaryRecovery test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end dead primary recovery test (short mode or no postgres binaries)")
	}

	// Create an isolated shard for this test
	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiOrchCount(1),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
	)
	defer cleanup()

	setup.StartMultiOrchs(t)

	// Get the primary
	primary := setup.GetPrimary(t)
	require.NotNil(t, primary, "primary instance should exist")
	oldPrimaryName := setup.PrimaryName
	t.Logf("Initial primary: %s", oldPrimaryName)

	// Verify standbys are replicating from the primary
	t.Logf("Verifying standbys are replicating from primary...")
	for name, inst := range setup.Multipoolers {
		if name == oldPrimaryName {
			continue
		}
		client, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
		require.NoError(t, err)

		resp, err := client.Manager.Status(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StatusRequest{})
		client.Close()

		require.NoError(t, err)
		require.NotNil(t, resp.Status.ReplicationStatus, "Standby %s should have replication status", name)
		require.NotNil(t, resp.Status.ReplicationStatus.PrimaryConnInfo, "Standby %s should have PrimaryConnInfo", name)
		t.Logf("Standby %s is replicating from %s:%d", name,
			resp.Status.ReplicationStatus.PrimaryConnInfo.Host,
			resp.Status.ReplicationStatus.PrimaryConnInfo.Port)
	}

	// Disable monitoring on all nodes so multiorch handles recovery (not postgres monitor)
	t.Logf("Disabling monitoring on all nodes...")
	disableMonitoringOnAllNodes(t, setup)

	// Create write validator pointing to initial primary (single table throughout test)
	primaryClient, err := shardsetup.NewMultipoolerClient(primary.Multipooler.GrpcPort)
	require.NoError(t, err)
	defer primaryClient.Close()

	validator, validatorCleanup, err := shardsetup.NewWriterValidator(t, primaryClient.Pooler,
		shardsetup.WithWorkerCount(4),
		shardsetup.WithWriteInterval(10*time.Millisecond),
	)
	require.NoError(t, err)
	t.Cleanup(validatorCleanup)

	t.Logf("Starting continuous writes to initial primary (table: %s)...", validator.TableName())
	validator.Start(t)

	// Let writes accumulate before first failover
	time.Sleep(200 * time.Millisecond)
	preFailoverSuccess, preFailoverFailed := validator.Stats()
	t.Logf("Pre-failover writes: %d successful, %d failed", preFailoverSuccess, preFailoverFailed)

	// Perform 3 consecutive failovers
	for i := range 3 {
		t.Logf("=== Failover iteration %d ===", i+1)

		// Refresh and get current primary (queries cluster to find actual primary)
		currentPrimary := setup.RefreshPrimary(t)
		require.NotNil(t, currentPrimary, "current primary should exist")
		currentPrimaryName := currentPrimary.Name

		// Kill postgres on the primary (multipooler stays running to report unhealthy status)
		t.Logf("Killing postgres on primary node %s to simulate database crash", currentPrimaryName)
		setup.KillPostgres(t, currentPrimaryName)

		// Wait for multiorch to detect failure and elect new primary
		t.Logf("Waiting for multiorch to detect primary failure and elect new leader...")
		newPrimaryName := waitForNewPrimary(t, setup, currentPrimaryName, 10*time.Second)
		require.NotEmpty(t, newPrimaryName, "Expected multiorch to elect new primary automatically")
		t.Logf("New primary elected: %s", newPrimaryName)

		// Stop the validator (pointing to old primary)
		validator.Stop()
		successWrites, failedWrites := validator.Stats()
		t.Logf("Iteration %d: stopped validator (%d successful, %d failed writes so far)", i+1, successWrites, failedWrites)

		// Wait for killed node to rejoin as standby (always wait, even on last iteration)
		waitForNodeToRejoinAsStandby(t, setup, currentPrimaryName, 30*time.Second)

		// Ensure monitoring is disabled on all nodes (multiorch recovery might have re-enabled it)
		t.Logf("Re-disabling monitoring on all nodes after failover %d...", i+1)
		disableMonitoringOnAllNodes(t, setup)

		// Point validator to new primary and restart writes
		newPrimaryInst := setup.GetMultipoolerInstance(newPrimaryName)
		require.NotNil(t, newPrimaryInst, "new primary instance should exist")

		newPrimaryClient, err := shardsetup.NewMultipoolerClient(newPrimaryInst.Multipooler.GrpcPort)
		require.NoError(t, err)

		validator.SetPooler(newPrimaryClient.Pooler)
		newPrimaryClient.Close() // Close immediately after setting pooler

		t.Logf("Restarting continuous writes to new primary %s...", newPrimaryName)
		validator.Start(t)
		time.Sleep(200 * time.Millisecond) // Let writes accumulate before next failover
	}

	// Stop writes after all failovers
	validator.Stop()
	successfulWrites, failedWrites := validator.Stats()
	t.Logf("Post-failover writes: %d successful, %d failed", successfulWrites, failedWrites)

	// Refresh primary one final time to ensure we have the correct final primary for verification
	finalPrimary := setup.RefreshPrimary(t)
	require.NotNil(t, finalPrimary, "final primary should exist")
	t.Logf("Final primary after all failovers: %s", finalPrimary.Name)

	// Verify final primary is functional
	t.Run("verify final primary is functional", func(t *testing.T) {
		finalPrimaryName := setup.PrimaryName
		finalPrimaryInst := setup.GetMultipoolerInstance(finalPrimaryName)
		require.NotNil(t, finalPrimaryInst, "final primary instance should exist")

		client, err := shardsetup.NewMultipoolerClient(finalPrimaryInst.Multipooler.GrpcPort)
		require.NoError(t, err)
		defer client.Close()

		resp, err := client.Manager.Status(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StatusRequest{})
		require.NoError(t, err)
		require.True(t, resp.Status.IsInitialized, "Final primary should be initialized")
		require.Equal(t, clustermetadatapb.PoolerType_PRIMARY, resp.Status.PoolerType, "Final leader should have PRIMARY pooler type")

		// Verify we can connect and query
		socketDir := filepath.Join(finalPrimaryInst.Pgctld.DataDir, "pg_sockets")
		db := connectToPostgres(t, socketDir, finalPrimaryInst.Pgctld.PgPort)
		defer db.Close()

		var result int
		err = db.QueryRow("SELECT 1").Scan(&result)
		require.NoError(t, err, "Should be able to query new primary")
		assert.Equal(t, 1, result)
	})

	// Verify sync replication is configured on the final primary
	t.Run("verify sync replication configured on final primary", func(t *testing.T) {
		finalPrimaryName := setup.PrimaryName
		finalPrimaryInst := setup.GetMultipoolerInstance(finalPrimaryName)
		require.NotNil(t, finalPrimaryInst, "final primary instance should exist")

		socketDir := filepath.Join(finalPrimaryInst.Pgctld.DataDir, "pg_sockets")
		db := connectToPostgres(t, socketDir, finalPrimaryInst.Pgctld.PgPort)
		defer db.Close()

		var syncStandbyNames string
		err := db.QueryRow("SHOW synchronous_standby_names").Scan(&syncStandbyNames)
		require.NoError(t, err, "Should be able to query synchronous_standby_names")
		require.NotEmpty(t, syncStandbyNames, "Final primary should have synchronous_standby_names configured after failovers")
		t.Logf("Final primary synchronous_standby_names: %s", syncStandbyNames)
	})

	// Verify leadership_history records all failovers
	t.Run("verify leadership_history after failovers", func(t *testing.T) {
		finalPrimaryName := setup.PrimaryName
		finalPrimaryInst := setup.GetMultipoolerInstance(finalPrimaryName)
		require.NotNil(t, finalPrimaryInst, "final primary instance should exist")

		socketDir := filepath.Join(finalPrimaryInst.Pgctld.DataDir, "pg_sockets")
		db := connectToPostgres(t, socketDir, finalPrimaryInst.Pgctld.PgPort)
		defer db.Close()

		// Query the leadership_history table for the latest record
		query := `SELECT term_number, leader_id, coordinator_id, wal_position, reason,
				  cohort_members, accepted_members, created_at
				  FROM multigres.leadership_history
				  ORDER BY term_number DESC
				  LIMIT 1`

		var termNumber int64
		var leaderID, coordinatorID, walPosition, reason string
		var cohortMembersJSON, acceptedMembersJSON string
		var createdAt time.Time

		err := db.QueryRow(query).Scan(&termNumber, &leaderID, &coordinatorID, &walPosition,
			&reason, &cohortMembersJSON, &acceptedMembersJSON, &createdAt)
		require.NoError(t, err, "Should be able to query leadership_history")

		// Assertions - after 3 failovers, term_number should be >= 3
		assert.GreaterOrEqual(t, termNumber, int64(3), "term_number should be >= 3 after 3 failovers")
		assert.Contains(t, leaderID, finalPrimaryName, "leader_id should contain final primary name")
		// Verify coordinator_id matches the multiorch's cell_name format
		// The coordinator ID uses ClusterIDString which returns cell_name format
		expectedCoordinatorID := setup.CellName + "_multiorch"
		assert.Equal(t, expectedCoordinatorID, coordinatorID, "coordinator_id should match multiorch's cell_name format")
		assert.NotEmpty(t, walPosition, "wal_position should not be empty")
		assert.Contains(t, reason, "PrimaryIsDead", "reason should indicate primary failure")

		// Verify cohort_members and accepted_members are valid JSON arrays
		var cohortMembers, acceptedMembers []string
		err = json.Unmarshal([]byte(cohortMembersJSON), &cohortMembers)
		require.NoError(t, err, "cohort_members should be valid JSON array")
		err = json.Unmarshal([]byte(acceptedMembersJSON), &acceptedMembers)
		require.NoError(t, err, "accepted_members should be valid JSON array")

		assert.NotEmpty(t, cohortMembers, "cohort_members should not be empty")
		assert.NotEmpty(t, acceptedMembers, "accepted_members should not be empty")
		assert.LessOrEqual(t, len(acceptedMembers), len(cohortMembers),
			"accepted_members should not exceed cohort_members")

		t.Logf("Leadership history verified: term=%d, leader=%s, coordinator=%s, reason=%s",
			termNumber, leaderID, coordinatorID, reason)
	})

	// Verify all successful writes are present on all nodes (all should be healthy now)
	t.Run("verify writes durability after failovers", func(t *testing.T) {
		finalPrimaryName := setup.PrimaryName
		finalPrimaryInst := setup.GetMultipoolerInstance(finalPrimaryName)
		require.NotNil(t, finalPrimaryInst)

		finalPrimaryClient, err := shardsetup.NewMultipoolerClient(finalPrimaryInst.Multipooler.GrpcPort)
		require.NoError(t, err)
		defer finalPrimaryClient.Close()

		// Get the final primary's LSN position
		primaryPosResp, err := finalPrimaryClient.Manager.PrimaryPosition(utils.WithShortDeadline(t), &multipoolermanagerdatapb.PrimaryPositionRequest{})
		require.NoError(t, err, "Should be able to get final primary position")
		primaryLSN := primaryPosResp.LsnPosition
		t.Logf("Final primary LSN: %s", primaryLSN)

		// Collect pooler clients for all nodes (primary + standbys) and wait for replicas to catch up
		var poolers []*shardsetup.MultiPoolerTestClient
		poolers = append(poolers, finalPrimaryClient.Pooler)

		for name, inst := range setup.Multipoolers {
			if name == finalPrimaryName {
				continue // Already added
			}
			client, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
			require.NoError(t, err)
			defer client.Close()

			// Wait for replica to catch up to the final primary's LSN
			_, err = client.Manager.WaitForLSN(utils.WithTimeout(t, 2*time.Second), &multipoolermanagerdatapb.WaitForLSNRequest{
				TargetLsn: primaryLSN,
			})
			require.NoError(t, err, "Replica %s should catch up to final primary LSN", name)
			t.Logf("Replica %s caught up to LSN %s", name, primaryLSN)

			poolers = append(poolers, client.Pooler)
		}

		require.Len(t, poolers, 3, "should have 3 healthy poolers (all nodes rejoined)")

		// Verify writes - deterministic now that replication has caught up
		err = validator.Verify(t, poolers)
		require.NoError(t, err, "all successful writes should be present on all nodes")

		t.Logf("Write durability verified: %d successful writes present on all nodes", successfulWrites)
	})

	// Capture table name before subtest to avoid closure issues
	validatorTableName := validator.TableName()
	t.Logf("Validator table for consistency check: %s", validatorTableName)

	// Verify data consistency across all nodes
	t.Run("verify data consistency across all nodes", func(t *testing.T) {
		finalPrimaryName := setup.PrimaryName
		finalPrimaryInst := setup.GetMultipoolerInstance(finalPrimaryName)
		require.NotNil(t, finalPrimaryInst)

		// Use the captured table name
		t.Logf("Using validator table: %s", validatorTableName)

		// Connect to primary and get row count and checksum
		primarySocketDir := filepath.Join(finalPrimaryInst.Pgctld.DataDir, "pg_sockets")
		primaryDB := connectToPostgres(t, primarySocketDir, finalPrimaryInst.Pgctld.PgPort)
		defer primaryDB.Close()

		var primaryRowCount int
		countQuery := "SELECT COUNT(*) FROM " + validatorTableName
		err := primaryDB.QueryRow(countQuery).Scan(&primaryRowCount)
		require.NoError(t, err, "Should be able to count rows on primary")
		t.Logf("Primary %s has %d rows", finalPrimaryName, primaryRowCount)

		// Get a checksum of all data on primary for consistency verification
		var primaryChecksum string
		checksumQuery := "SELECT md5(string_agg(id::text, '' ORDER BY id)) FROM " + validatorTableName
		err = primaryDB.QueryRow(checksumQuery).Scan(&primaryChecksum)
		require.NoError(t, err, "Should be able to compute checksum on primary")
		t.Logf("Primary %s data checksum: %s", finalPrimaryName, primaryChecksum)

		// Verify all standbys have identical data
		for name, inst := range setup.Multipoolers {
			if name == finalPrimaryName {
				continue // Already checked primary
			}

			standbySocketDir := filepath.Join(inst.Pgctld.DataDir, "pg_sockets")
			standbyDB := connectToPostgres(t, standbySocketDir, inst.Pgctld.PgPort)
			defer standbyDB.Close()

			// Check row count matches
			var standbyRowCount int
			err = standbyDB.QueryRow(countQuery).Scan(&standbyRowCount)
			require.NoError(t, err, "Should be able to count rows on standby %s", name)
			assert.Equal(t, primaryRowCount, standbyRowCount, "Standby %s should have same row count as primary", name)
			t.Logf("Standby %s has %d rows (matches primary)", name, standbyRowCount)

			// Check data checksum matches
			var standbyChecksum string
			err = standbyDB.QueryRow(checksumQuery).Scan(&standbyChecksum)
			require.NoError(t, err, "Should be able to compute checksum on standby %s", name)
			assert.Equal(t, primaryChecksum, standbyChecksum, "Standby %s should have identical data to primary", name)
			t.Logf("Standby %s data checksum: %s (matches primary)", name, standbyChecksum)
		}

		t.Logf("Data consistency verified: all %d nodes have identical data (%d rows, checksum %s)",
			len(setup.Multipoolers), primaryRowCount, primaryChecksum)
	})
}

// disableMonitoringOnAllNodes disables postgres monitoring on all multipooler nodes.
func disableMonitoringOnAllNodes(t *testing.T, setup *shardsetup.ShardSetup) {
	t.Helper()
	for name, inst := range setup.Multipoolers {
		client, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
		require.NoError(t, err)
		_, err = client.Manager.SetMonitor(t.Context(), &multipoolermanagerdatapb.SetMonitorRequest{Enabled: false})
		require.NoError(t, err)
		client.Close()
		t.Logf("Monitoring disabled on %s", name)
	}
}

// waitForNewPrimary waits for a new primary (different from oldPrimaryName) to be elected.
func waitForNewPrimary(t *testing.T, setup *shardsetup.ShardSetup, oldPrimaryName string, timeout time.Duration) string {
	t.Helper()

	checkPrimary := func() string {
		for name, inst := range setup.Multipoolers {
			if name == oldPrimaryName {
				continue
			}

			client, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
			if err != nil {
				continue
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			resp, err := client.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
			cancel()
			client.Close()

			if err != nil {
				continue
			}

			if resp.Status.IsInitialized && resp.Status.PoolerType == clustermetadatapb.PoolerType_PRIMARY {
				t.Logf("New primary elected: %s (pooler_type=%s)", name, resp.Status.PoolerType)
				return name
			}
		}
		return ""
	}

	// Check immediately
	if name := checkPrimary(); name != "" {
		return name
	}

	checkInterval := 2 * time.Second
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	timeoutCh := time.After(timeout)

	for {
		select {
		case <-ticker.C:
			if name := checkPrimary(); name != "" {
				return name
			}
			t.Logf("Waiting for new primary election... (sleeping %v)", checkInterval)

		case <-timeoutCh:
			t.Fatalf("Timeout: new primary not elected within %v", timeout)
			return ""
		}
	}
}

// waitForNodeToRejoinAsStandby waits for a killed node to be restarted by multiorch
// and rejoin the cluster as a standby replica.
func waitForNodeToRejoinAsStandby(t *testing.T, setup *shardsetup.ShardSetup, nodeName string, timeout time.Duration) {
	t.Helper()
	t.Logf("Waiting for multiorch to restart %s and rejoin as standby...", nodeName)

	inst := setup.GetMultipoolerInstance(nodeName)
	require.NotNil(t, inst, "node %s should exist", nodeName)

	checkRejoin := func() bool {
		client, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
		if err != nil {
			return false
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		status, err := client.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
		cancel()
		client.Close()

		if err != nil {
			return false
		}

		// Check if node is back up as a replica with replication configured
		if status.Status.IsInitialized &&
			status.Status.PoolerType == clustermetadatapb.PoolerType_REPLICA &&
			status.Status.ReplicationStatus != nil &&
			status.Status.ReplicationStatus.PrimaryConnInfo != nil {
			t.Logf("Node %s successfully rejoined as standby (pooler_type=%s)", nodeName, status.Status.PoolerType)
			return true
		}

		t.Logf("Node %s not yet rejoined (PostgresRunning=%v, PoolerType=%v), waiting...",
			nodeName, status.Status.PostgresRunning, status.Status.PoolerType)
		return false
	}

	// Check immediately
	if checkRejoin() {
		return
	}

	checkInterval := 1 * time.Second
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	timeoutCh := time.After(timeout)

	for {
		select {
		case <-ticker.C:
			if checkRejoin() {
				return
			}

		case <-timeoutCh:
			t.Fatalf("Timeout: node %s did not rejoin as standby within %v", nodeName, timeout)
		}
	}
}

// TestPoolerDownNoFailover verifies that multiorch does NOT trigger a failover when the
// primary's multipooler process is down but Postgres is still running and replicas are
// still connected to it.
func TestPoolerDownNoFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end pooler down test (short mode)")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end pooler down test (short mode or no postgres binaries)")
	}

	// Create an isolated shard for this test
	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiOrchCount(1),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
	)
	defer cleanup()

	setup.StartMultiOrchs(t)

	primary := setup.GetPrimary(t)
	require.NotNil(t, primary, "primary instance should exist")
	t.Logf("Initial primary: %s", setup.PrimaryName)

	// Kill the multipooler process on the primary (but leave Postgres running)
	t.Logf("Killing multipooler on primary node %s (postgres stays running)", setup.PrimaryName)
	killMultipooler(t, primary)

	// Wait for multiorch to detect the pooler is down and run several recovery cycles.
	t.Logf("Waiting for multiorch to process the pooler down state...")
	time.Sleep(3 * time.Second)

	// Verify that NO failover occurred - standbys should still be replicas
	t.Run("verify no failover occurred", func(t *testing.T) {
		for name, inst := range setup.Multipoolers {
			if name == setup.PrimaryName {
				continue // Skip the primary (its pooler is down)
			}

			client, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
			require.NoError(t, err)
			defer client.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			resp, err := client.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
			cancel()

			require.NoError(t, err)
			require.True(t, resp.Status.IsInitialized, "Node %s should be initialized", name)
			require.Equal(t, clustermetadatapb.PoolerType_REPLICA, resp.Status.PoolerType,
				"Node %s should still be REPLICA (no failover should have occurred)", name)
			require.NotNil(t, resp.Status.ReplicationStatus, "Standby %s should have replication status", name)
			require.NotNil(t, resp.Status.ReplicationStatus.PrimaryConnInfo, "Standby %s should have PrimaryConnInfo", name)
			t.Logf("Standby %s is still replicating (type=%s), no failover triggered", name, resp.Status.PoolerType)
		}
	})

	// Verify we can still query postgres on the primary directly (it's still running)
	t.Run("verify primary postgres is still running", func(t *testing.T) {
		socketDir := filepath.Join(primary.Pgctld.DataDir, "pg_sockets")
		db := connectToPostgres(t, socketDir, primary.Pgctld.PgPort)
		defer db.Close()

		var result int
		err := db.QueryRow("SELECT 1").Scan(&result)
		require.NoError(t, err, "Should be able to query primary postgres directly (it's still running)")
		assert.Equal(t, 1, result)
		t.Logf("Primary postgres is still running and queryable")
	})

	// Verify we can query the standbys (they're still replicating)
	t.Run("verify standbys are still queryable", func(t *testing.T) {
		for name, inst := range setup.Multipoolers {
			if name == setup.PrimaryName {
				continue
			}
			socketDir := filepath.Join(inst.Pgctld.DataDir, "pg_sockets")
			db := connectToPostgres(t, socketDir, inst.Pgctld.PgPort)
			defer db.Close()

			var result int
			err := db.QueryRow("SELECT 1").Scan(&result)
			require.NoError(t, err, "Should be able to query standby %s", name)
			assert.Equal(t, 1, result)
		}
		t.Logf("All standbys are still queryable")
	})
}

// killMultipooler terminates the multipooler process for a node (simulates pooler crash)
func killMultipooler(t *testing.T, node *shardsetup.MultipoolerInstance) {
	t.Helper()

	if node.Multipooler == nil || node.Multipooler.Process == nil || node.Multipooler.Process.Process == nil {
		t.Fatalf("Multipooler process not found for node %s", node.Name)
	}

	pid := node.Multipooler.Process.Process.Pid
	t.Logf("Killing multipooler (PID %d) on node %s", pid, node.Name)

	err := node.Multipooler.Process.Process.Kill()
	require.NoError(t, err, "Failed to kill multipooler process")

	// Wait for the process to actually terminate
	_ = node.Multipooler.Process.Wait()

	t.Logf("Multipooler killed on %s - postgres should still be running", node.Name)
}
