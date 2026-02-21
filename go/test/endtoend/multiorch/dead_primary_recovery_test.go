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
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
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
		shardsetup.WithMultiOrchCount(3),
		shardsetup.WithMultigateway(),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
		shardsetup.WithPrimaryFailoverGracePeriod("8s", "4s"),
	)
	defer cleanup()

	setup.StartMultiOrchs(t.Context(), t)
	setup.WaitForMultigatewayQueryServing(t)

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

	// Disable monitoring so multiorch orchestrates recovery instead
	// of each multipooler's local postgres monitor. This could create races for
	// this test.
	disableMonitoringOnAllNodes(t, setup)

	// Connect to multigateway for continuous writes (automatically routes to current primary)
	connStr := fmt.Sprintf("host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable connect_timeout=5",
		setup.MultigatewayPgPort, shardsetup.TestPostgresPassword)
	gatewayDB, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	defer gatewayDB.Close()

	err = gatewayDB.Ping()
	require.NoError(t, err, "failed to ping multigateway")

	// Create write validator pointing to multigateway (automatically routes through failovers)
	validator, validatorCleanup, err := shardsetup.NewWriterValidator(t, gatewayDB,
		shardsetup.WithWorkerCount(4),
		shardsetup.WithWriteInterval(10*time.Millisecond),
	)
	require.NoError(t, err)
	t.Cleanup(validatorCleanup)

	t.Logf("Starting continuous writes via multigateway (table: %s)...", validator.TableName())
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
		t.Logf("Killing postgres on primary multipooler %s to simulate database crash", currentPrimaryName)
		setup.KillPostgres(t, currentPrimaryName)

		// Wait for multiorch to detect failure and elect new primary
		t.Logf("Waiting for multiorch to detect primary failure and elect new leader...")
		newPrimaryName := waitForNewPrimary(t, setup, currentPrimaryName, 15*time.Second)
		require.NotEmpty(t, newPrimaryName, "Expected multiorch to elect new primary automatically")
		t.Logf("New primary elected: %s", newPrimaryName)

		// Get the new primary's consensus term to verify rejoining nodes are on correct term
		newPrimary := setup.GetMultipoolerInstance(newPrimaryName)
		require.NotNil(t, newPrimary, "new primary instance should exist")
		newPrimaryClient, err := shardsetup.NewMultipoolerClient(newPrimary.Multipooler.GrpcPort)
		require.NoError(t, err)
		status, err := newPrimaryClient.Manager.Status(utils.WithTimeout(t, 5*time.Second), &multipoolermanagerdatapb.StatusRequest{})
		newPrimaryClient.Close()
		require.NoError(t, err, "should be able to get status from new primary")
		newPrimaryTerm := status.Status.ConsensusTerm.TermNumber
		newPrimaryTermActual := status.Status.ConsensusTerm.PrimaryTerm
		t.Logf("New primary %s is on term %d, primary_term=%d", newPrimaryName, newPrimaryTerm, newPrimaryTermActual)

		// Verify primary_term is set and matches the consensus term
		require.NotZero(t, newPrimaryTermActual, "Primary term must be non-zero for new primary %s", newPrimaryName)
		assert.Equal(t, newPrimaryTerm, newPrimaryTermActual,
			"Primary term should match consensus term for new primary %s (term=%d)", newPrimaryName, newPrimaryTerm)

		// Wait for killed multipooler to rejoin as standby (always wait, even on last iteration)
		waitForNodeToRejoinAsStandby(t, setup, currentPrimaryName, newPrimaryName, newPrimaryTerm, 30*time.Second)

		// Ensure monitoring is disabled on all multipoolers (multiorch recovery might have re-enabled it)
		t.Logf("Re-disabling monitoring on all multipoolers after failover %d...", i+1)
		disableMonitoringOnAllNodes(t, setup)

		// No need to restart validator or switch connections - multigateway automatically routes to new primary
		// We track failed writes just for debugging purposes, but during a failover it is expected
		// that we will see some failures (at least until we get to buffering)
		successWrites, failedWrites := validator.Stats()
		t.Logf("Iteration %d: %d successful, %d failed writes so far (multigateway auto-routing to %s)",
			i+1, successWrites, failedWrites, newPrimaryName)
		time.Sleep(200 * time.Millisecond) // Let writes accumulate before next failover
	}

	// Perform one more failover using BeginTerm to trigger emergency demotion
	t.Logf("=== Failover iteration 4 (via BeginTerm emergency demotion) ===")

	// Refresh and get current primary
	currentPrimary := setup.RefreshPrimary(t)
	require.NotNil(t, currentPrimary, "current primary should exist")
	currentPrimaryName := currentPrimary.Name

	// Get the current primary's term
	primaryClient, err := shardsetup.NewMultipoolerClient(currentPrimary.Multipooler.GrpcPort)
	require.NoError(t, err)
	statusResp, err := primaryClient.Manager.Status(utils.WithTimeout(t, 5*time.Second), &multipoolermanagerdatapb.StatusRequest{})
	require.NoError(t, err)
	oldPrimaryTerm := statusResp.Status.ConsensusTerm.TermNumber
	t.Logf("Primary %s is on term %d", currentPrimaryName, oldPrimaryTerm)

	// Call BeginTerm with a higher term to trigger emergency demotion
	beginTermTerm := oldPrimaryTerm + 1
	t.Logf("Calling BeginTerm on primary %s with term %d to trigger emergency demotion", currentPrimaryName, beginTermTerm)

	beginTermReq := &consensusdatapb.BeginTermRequest{
		Term: beginTermTerm,
		CandidateId: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIORCH,
			Cell:      setup.CellName,
			Name:      "test-coordinator",
		},
		Action: consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_REVOKE,
	}
	beginTermResp, err := primaryClient.Consensus.BeginTerm(utils.WithTimeout(t, 10*time.Second), beginTermReq)
	primaryClient.Close()

	require.NoError(t, err, "BeginTerm should succeed")
	require.True(t, beginTermResp.Accepted, "Primary should accept BeginTerm with higher term")
	t.Logf("BeginTerm accepted by primary, emergency demotion triggered (current_lsn=%s)", beginTermResp.WalPosition.GetCurrentLsn())

	// Wait for multiorch to detect failure and elect new primary
	t.Logf("Waiting for multiorch to detect emergency demotion and elect new leader...")
	newPrimaryName := waitForNewPrimary(t, setup, currentPrimaryName, 20*time.Second)
	require.NotEmpty(t, newPrimaryName, "Expected multiorch to elect new primary after emergency demotion")
	t.Logf("New primary elected: %s", newPrimaryName)

	// Get the new primary's consensus term
	newPrimary := setup.GetMultipoolerInstance(newPrimaryName)
	require.NotNil(t, newPrimary, "new primary instance should exist")
	newPrimaryClient, err := shardsetup.NewMultipoolerClient(newPrimary.Multipooler.GrpcPort)
	require.NoError(t, err)
	newStatusResp, err := newPrimaryClient.Manager.Status(utils.WithTimeout(t, 5*time.Second), &multipoolermanagerdatapb.StatusRequest{})
	newPrimaryClient.Close()
	require.NoError(t, err, "should be able to get status from new primary")
	newPrimaryTerm := newStatusResp.Status.ConsensusTerm.TermNumber
	t.Logf("New primary %s is on term %d", newPrimaryName, newPrimaryTerm)

	// Verify new primary's term is higher than the old primary's original term
	require.Greater(t, newPrimaryTerm, oldPrimaryTerm, "New primary should have higher term than old primary's original term")

	// Wait for emergency demoted primary to rejoin as standby
	waitForNodeToRejoinAsStandby(t, setup, currentPrimaryName, newPrimaryName, newPrimaryTerm, 30*time.Second)

	// Ensure monitoring is disabled on all multipoolers
	t.Logf("Re-disabling monitoring on all multipoolers after failover 4...")
	disableMonitoringOnAllNodes(t, setup)

	successWrites, failedWrites := validator.Stats()
	t.Logf("Iteration 4: %d successful, %d failed writes so far (multigateway auto-routing to %s)",
		successWrites, failedWrites, newPrimaryName)
	time.Sleep(200 * time.Millisecond) // Let writes accumulate

	// Stop writes after all failovers
	validator.Stop()
	successfulWrites, failedWrites := validator.Stats()
	t.Logf("Post-failover writes: %d successful, %d failed", successfulWrites, failedWrites)

	// Refresh primary one final time to ensure we have the correct final primary for verification
	finalPrimary := setup.RefreshPrimary(t)
	require.NotNil(t, finalPrimary, "final primary should exist")
	t.Logf("Final primary after all failovers: %s", finalPrimary.Name)

	// Wait for all multipoolers to be healthy before verification
	t.Logf("Waiting for all multipoolers to be healthy before verification...")
	for name, inst := range setup.Multipoolers {
		require.Eventually(t, func() bool {
			client, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
			if err != nil {
				t.Logf("Multipooler %s: failed to create client: %v", name, err)
				return false
			}
			defer client.Close()

			status, err := client.Manager.Status(utils.WithTimeout(t, 5*time.Second), &multipoolermanagerdatapb.StatusRequest{})
			if err != nil {
				t.Logf("Multipooler %s: Status() call failed: %v", name, err)
				return false
			}

			// Check if postgres is running
			if !status.Status.PostgresRunning {
				t.Logf("Multipooler %s: postgres not running", name)
				return false
			}

			// For replicas, check replication is configured
			if status.Status.PoolerType == clustermetadatapb.PoolerType_REPLICA {
				if status.Status.ReplicationStatus == nil || status.Status.ReplicationStatus.PrimaryConnInfo == nil {
					t.Logf("Multipooler %s: replication status missing (ReplicationStatus=%v, PrimaryConnInfo=%v)",
						name,
						status.Status.ReplicationStatus != nil,
						status.Status.ReplicationStatus != nil && status.Status.ReplicationStatus.PrimaryConnInfo != nil)
					return false
				}
			}

			return true
		}, 10*time.Second, 500*time.Millisecond, "Multipooler %s should be healthy", name)
	}

	// Verify replicas have primary_term = 0 (never been primary)
	t.Logf("Verifying replicas have primary_term = 0...")
	for name, inst := range setup.Multipoolers {
		if name == setup.PrimaryName {
			continue // Skip primary
		}

		client, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
		require.NoError(t, err)

		status, err := client.Manager.Status(utils.WithTimeout(t, 5*time.Second), &multipoolermanagerdatapb.StatusRequest{})
		client.Close()

		require.NoError(t, err)
		if status.Status.PoolerType == clustermetadatapb.PoolerType_REPLICA {
			require.NotNil(t, status.Status.ConsensusTerm, "Replica %s should have consensus term", name)
			assert.Equal(t, int64(0), status.Status.ConsensusTerm.PrimaryTerm,
				"Replica %s should have primary_term=0 (never been primary)", name)
		}
	}

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

		var syncCommit string
		err = db.QueryRow("SHOW synchronous_commit").Scan(&syncCommit)
		require.NoError(t, err, "Should be able to query synchronous_commit")
		assert.Equal(t, "on", syncCommit, "synchronous_commit should be 'on' after failover")
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
		// Verify coordinator_id matches the multiorch's cell_name format (with multiple multiorchs, any could be coordinator)
		expectedCoordinatorPrefix := setup.CellName + "_multiorch"
		assert.Contains(t, coordinatorID, expectedCoordinatorPrefix, "coordinator_id should start with cell_name_multiorch")
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

	// Verify all successful writes are present on all multipoolers (all should be healthy now)
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

		// Collect multipooler test clients for all multipoolers (primary + standbys) and wait for replicas to catch up
		var poolerClients []*shardsetup.MultiPoolerTestClient

		// Add primary's pooler client
		primaryPoolerClient, err := shardsetup.NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", finalPrimaryInst.Multipooler.GrpcPort))
		require.NoError(t, err)
		poolerClients = append(poolerClients, primaryPoolerClient)

		for name, inst := range setup.Multipoolers {
			if name == finalPrimaryName {
				continue // Already added
			}
			client, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
			require.NoError(t, err)

			// Wait for replica to catch up to the final primary's LSN
			_, err = client.Consensus.WaitForLSN(utils.WithTimeout(t, 2*time.Second), &consensusdatapb.WaitForLSNRequest{
				TargetLsn: primaryLSN,
			})
			require.NoError(t, err, "Replica %s should catch up to final primary LSN", name)
			poolerClients = append(poolerClients, client.Pooler)
		}

		// Defer cleanup of all pooler clients after collection
		defer func() {
			for _, client := range poolerClients {
				client.Close()
			}
		}()

		require.Len(t, poolerClients, 3, "should have 3 multipooler clients (all multipoolers rejoined)")

		// Verify writes - deterministic now that replication has caught up
		err = validator.Verify(t, poolerClients)
		require.NoError(t, err, "all successful writes should be present on all multipoolers")
		t.Logf("Write durability verified: %d successful writes present on all multipoolers", successfulWrites)
	})

	validatorTableName := validator.TableName()
	t.Logf("Validator table for consistency check: %s", validatorTableName)

	// Verify data consistency across all multipoolers
	t.Run("verify data consistency across all multipoolers", func(t *testing.T) {
		finalPrimaryName := setup.PrimaryName
		finalPrimaryInst := setup.GetMultipoolerInstance(finalPrimaryName)
		require.NotNil(t, finalPrimaryInst)

		// Connect to primary and get row count and checksum
		primarySocketDir := filepath.Join(finalPrimaryInst.Pgctld.DataDir, "pg_sockets")
		primaryDB := connectToPostgres(t, primarySocketDir, finalPrimaryInst.Pgctld.PgPort)
		defer primaryDB.Close()

		var primaryRowCount int
		countQuery := "SELECT COUNT(*) FROM " + validatorTableName
		err := primaryDB.QueryRow(countQuery).Scan(&primaryRowCount)
		require.NoError(t, err, "Should be able to count rows on primary")

		// Get a checksum of all data on primary for consistency verification
		var primaryChecksum string
		checksumQuery := "SELECT md5(string_agg(id::text, '' ORDER BY id)) FROM " + validatorTableName
		err = primaryDB.QueryRow(checksumQuery).Scan(&primaryChecksum)
		require.NoError(t, err, "Should be able to compute checksum on primary")

		// Verify all standbys have identical data
		for name, inst := range setup.Multipoolers {
			if name == finalPrimaryName {
				continue // Already checked primary
			}
			verifyStandbyDataConsistency(t, name, inst, countQuery, checksumQuery, primaryRowCount, primaryChecksum)
		}

		t.Logf("Data consistency verified: all %d multipoolers have identical data (%d rows, checksum %s)",
			len(setup.Multipoolers), primaryRowCount, primaryChecksum)
	})
}

// verifyStandbyDataConsistency checks that a standby has the same data as the primary.
func verifyStandbyDataConsistency(t *testing.T, name string, inst *shardsetup.MultipoolerInstance, countQuery, checksumQuery string, expectedRowCount int, expectedChecksum string) {
	t.Helper()
	standbySocketDir := filepath.Join(inst.Pgctld.DataDir, "pg_sockets")
	standbyDB := connectToPostgres(t, standbySocketDir, inst.Pgctld.PgPort)
	defer standbyDB.Close()

	// Check row count matches
	var standbyRowCount int
	err := standbyDB.QueryRow(countQuery).Scan(&standbyRowCount)
	require.NoError(t, err, "Should be able to count rows on standby %s", name)
	assert.Equal(t, expectedRowCount, standbyRowCount, "Standby %s should have same row count as primary", name)

	// Check data checksum matches
	var standbyChecksum string
	err = standbyDB.QueryRow(checksumQuery).Scan(&standbyChecksum)
	require.NoError(t, err, "Should be able to compute checksum on standby %s", name)
	assert.Equal(t, expectedChecksum, standbyChecksum, "Standby %s should have identical data to primary", name)
}

// disableMultipoolerMonitoring disables postgres monitoring on a single multipooler.
func disableMultipoolerMonitoring(t *testing.T, name string, inst *shardsetup.MultipoolerInstance) {
	t.Helper()
	client, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
	require.NoError(t, err)
	defer client.Close()

	_, err = client.Manager.SetMonitor(t.Context(), &multipoolermanagerdatapb.SetMonitorRequest{Enabled: false})
	require.NoError(t, err)
}

// disableMonitoringOnAllNodes disables postgres monitoring on all multipoolers.
func disableMonitoringOnAllNodes(t *testing.T, setup *shardsetup.ShardSetup) {
	t.Helper()
	for name, inst := range setup.Multipoolers {
		disableMultipoolerMonitoring(t, name, inst)
	}
}

// checkPrimary checks if a specific multipooler is the primary.
// Returns the multipooler name if it's a primary, empty string otherwise.
func checkPrimary(t *testing.T, name string, inst *shardsetup.MultipoolerInstance, oldPrimaryName string) string {
	t.Helper()
	if name == oldPrimaryName {
		return ""
	}

	client, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
	if err != nil {
		return ""
	}
	defer client.Close()

	resp, err := client.Manager.Status(utils.WithTimeout(t, 5*time.Second), &multipoolermanagerdatapb.StatusRequest{})
	if err != nil {
		return ""
	}

	if resp.Status.IsInitialized && resp.Status.PoolerType == clustermetadatapb.PoolerType_PRIMARY {
		t.Logf("New primary elected: %s (pooler_type=%s)", name, resp.Status.PoolerType)
		return name
	}

	return ""
}

// waitForNewPrimary waits for a new primary (different from oldPrimaryName) to be elected.
func waitForNewPrimary(t *testing.T, setup *shardsetup.ShardSetup, oldPrimaryName string, timeout time.Duration) string {
	t.Helper()

	checkForNewPrimary := func() string {
		for name, inst := range setup.Multipoolers {
			if primaryName := checkPrimary(t, name, inst, oldPrimaryName); primaryName != "" {
				return primaryName
			}
		}
		return ""
	}

	// Check immediately
	if name := checkForNewPrimary(); name != "" {
		return name
	}

	checkInterval := 2 * time.Second
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	timeoutCh := time.After(timeout)

	for {
		select {
		case <-ticker.C:
			if name := checkForNewPrimary(); name != "" {
				return name
			}
			t.Logf("Waiting for new primary election... (sleeping %v)", checkInterval)

		case <-timeoutCh:
			t.Fatalf("Timeout: new primary not elected within %v", timeout)
			return ""
		}
	}
}

// checkReplicaIsHealthy verifies that a replica is healthy and properly configured.
// It checks that postgres is running, replication is streaming, and the node is on the correct term.
func checkReplicaIsHealthy(t *testing.T, multipoolerName string, inst *shardsetup.MultipoolerInstance, expectedPrimaryName string, expectedTerm int64) bool {
	t.Helper()
	client, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
	if err != nil {
		return false
	}
	defer client.Close()

	status, err := client.Manager.Status(utils.WithTimeout(t, 5*time.Second), &multipoolermanagerdatapb.StatusRequest{})
	if err != nil {
		return false
	}

	// Must be a replica with postgres running
	if status.Status.PoolerType != clustermetadatapb.PoolerType_REPLICA {
		t.Logf("Multipooler %s not yet rejoined (PoolerType=%v, expected REPLICA)", multipoolerName, status.Status.PoolerType)
		return false
	}

	if !status.Status.PostgresRunning {
		t.Logf("Multipooler %s postgres not running yet", multipoolerName)
		return false
	}

	// Must have replication configured and streaming
	if status.Status.ReplicationStatus == nil || status.Status.ReplicationStatus.PrimaryConnInfo == nil {
		t.Logf("Multipooler %s replication not configured yet", multipoolerName)
		return false
	}

	// Verify on the correct consensus term (must match the new primary's term exactly)
	if status.Status.ConsensusTerm == nil || status.Status.ConsensusTerm.TermNumber != expectedTerm {
		termNum := int64(0)
		if status.Status.ConsensusTerm != nil {
			termNum = status.Status.ConsensusTerm.TermNumber
		}
		t.Logf("Multipooler %s on wrong term %d (expected %d)", multipoolerName, termNum, expectedTerm)
		return false
	}

	// Verify replication is healthy (streaming state)
	if status.Status.ReplicationStatus.WalReceiverStatus != "streaming" {
		t.Logf("Multipooler %s replication not streaming yet (state=%s)", multipoolerName, status.Status.ReplicationStatus.WalReceiverStatus)
		return false
	}

	return true
}

// checkReplicaInPrimaryStandbyList verifies that a replica appears in the primary's synchronous_standby_names list.
func checkReplicaInPrimaryStandbyList(t *testing.T, setup *shardsetup.ShardSetup, multipoolerName string, expectedPrimaryName string) bool {
	t.Helper()

	primaryInst := setup.GetMultipoolerInstance(expectedPrimaryName)
	if primaryInst == nil {
		t.Logf("Primary %s not found", expectedPrimaryName)
		return false
	}

	primaryClient, err := shardsetup.NewMultipoolerClient(primaryInst.Multipooler.GrpcPort)
	if err != nil {
		t.Logf("Failed to connect to primary %s: %v", expectedPrimaryName, err)
		return false
	}
	defer primaryClient.Close()

	primaryStatus, err := primaryClient.Manager.Status(utils.WithTimeout(t, 5*time.Second), &multipoolermanagerdatapb.StatusRequest{})
	if err != nil {
		t.Logf("Failed to get status from primary %s: %v", expectedPrimaryName, err)
		return false
	}

	if primaryStatus.Status.PrimaryStatus == nil || primaryStatus.Status.PrimaryStatus.SyncReplicationConfig == nil {
		t.Logf("Primary %s has no synchronous replication config yet", expectedPrimaryName)
		return false
	}

	// Check if replica is in the primary's standby list
	for _, standby := range primaryStatus.Status.PrimaryStatus.SyncReplicationConfig.StandbyIds {
		if standby.Name == multipoolerName {
			return true
		}
	}

	t.Logf("Multipooler %s not yet in primary %s's standby list", multipoolerName, expectedPrimaryName)
	return false
}

// checkRejoin checks if a multipooler has rejoined the cluster as a standby replica.
// It verifies the node is healthy, properly configured, and appears in the primary's standby list.
func checkRejoin(t *testing.T, setup *shardsetup.ShardSetup, multipoolerName string, inst *shardsetup.MultipoolerInstance, expectedPrimaryName string, expectedTerm int64) bool {
	t.Helper()

	// First check if replica itself is healthy
	if !checkReplicaIsHealthy(t, multipoolerName, inst, expectedPrimaryName, expectedTerm) {
		return false
	}

	// Then check if replica is in the primary's standby list
	if !checkReplicaInPrimaryStandbyList(t, setup, multipoolerName, expectedPrimaryName) {
		return false
	}

	t.Logf("Multipooler %s successfully rejoined (term=%d, replicating from %s, in standby list)",
		multipoolerName, expectedTerm, expectedPrimaryName)
	return true
}

// waitForNodeToRejoinAsStandby waits for a killed multipooler to be restarted by multiorch
// and rejoin the cluster as a standby replica. It verifies the node is on the correct term
// and replicating from the expected primary.
func waitForNodeToRejoinAsStandby(t *testing.T, setup *shardsetup.ShardSetup, multipoolerName string, expectedPrimaryName string, expectedTerm int64, timeout time.Duration) {
	t.Helper()
	t.Logf("Waiting for multiorch to restart %s and rejoin as standby (expected primary: %s, term: %d)...",
		multipoolerName, expectedPrimaryName, expectedTerm)

	inst := setup.GetMultipoolerInstance(multipoolerName)
	require.NotNil(t, inst, "multipooler %s should exist", multipoolerName)

	// Check immediately
	if checkRejoin(t, setup, multipoolerName, inst, expectedPrimaryName, expectedTerm) {
		return
	}

	checkInterval := 1 * time.Second
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	timeoutCh := time.After(timeout)

	for {
		select {
		case <-ticker.C:
			if checkRejoin(t, setup, multipoolerName, inst, expectedPrimaryName, expectedTerm) {
				return
			}

		case <-timeoutCh:
			t.Fatalf("Timeout: multipooler %s did not rejoin as standby within %v", multipoolerName, timeout)
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

	setup.StartMultiOrchs(t.Context(), t)

	primary := setup.GetPrimary(t)
	require.NotNil(t, primary, "primary instance should exist")
	t.Logf("Initial primary: %s", setup.PrimaryName)

	// Kill the multipooler process on the primary (but leave Postgres running)
	t.Logf("Killing multipooler on primary %s (postgres stays running)", setup.PrimaryName)
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
			verifyStandbyIsStillReplica(t, name, inst)
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
			verifyStandbyIsQueryable(t, name, inst)
		}
		t.Logf("All standbys are still queryable")
	})
}

// verifyStandbyIsQueryable checks that a standby postgres is queryable.
func verifyStandbyIsQueryable(t *testing.T, name string, inst *shardsetup.MultipoolerInstance) {
	t.Helper()
	socketDir := filepath.Join(inst.Pgctld.DataDir, "pg_sockets")
	db := connectToPostgres(t, socketDir, inst.Pgctld.PgPort)
	defer db.Close()

	var result int
	err := db.QueryRow("SELECT 1").Scan(&result)
	require.NoError(t, err, "Should be able to query standby %s", name)
	assert.Equal(t, 1, result)
}

// verifyStandbyIsStillReplica checks that a standby is still a replica (no failover occurred).
func verifyStandbyIsStillReplica(t *testing.T, name string, inst *shardsetup.MultipoolerInstance) {
	t.Helper()
	client, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
	require.NoError(t, err)
	defer client.Close()

	resp, err := client.Manager.Status(utils.WithTimeout(t, 5*time.Second), &multipoolermanagerdatapb.StatusRequest{})
	require.NoError(t, err)
	require.True(t, resp.Status.IsInitialized, "Multipooler %s should be initialized", name)
	require.Equal(t, clustermetadatapb.PoolerType_REPLICA, resp.Status.PoolerType,
		"Multipooler %s should still be REPLICA (no failover should have occurred)", name)
	require.NotNil(t, resp.Status.ReplicationStatus, "Standby %s should have replication status", name)
	require.NotNil(t, resp.Status.ReplicationStatus.PrimaryConnInfo, "Standby %s should have PrimaryConnInfo", name)
	t.Logf("Standby %s is still replicating (type=%s), no failover triggered", name, resp.Status.PoolerType)
}

// killMultipooler terminates the multipooler process (simulates pooler crash)
func killMultipooler(t *testing.T, multipooler *shardsetup.MultipoolerInstance) {
	t.Helper()

	if multipooler.Multipooler == nil || multipooler.Multipooler.Process == nil || multipooler.Multipooler.Process.Process == nil {
		t.Fatalf("Multipooler process not found for %s", multipooler.Name)
	}

	pid := multipooler.Multipooler.Process.Process.Pid
	t.Logf("Killing multipooler (PID %d) on %s", pid, multipooler.Name)

	err := multipooler.Multipooler.Process.Process.Kill()
	require.NoError(t, err, "Failed to kill multipooler process")

	// Wait for the process to actually terminate
	_ = multipooler.Multipooler.Process.Wait()

	t.Logf("Multipooler killed on %s - postgres should still be running", multipooler.Name)
}
