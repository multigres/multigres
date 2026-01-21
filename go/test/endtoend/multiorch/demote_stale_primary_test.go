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

package multiorch

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestDemoteStalePrimary tests multiorch's ability to detect a stale primary
// after failover and demote it, then repair its diverged timeline using pg_rewind.
//
// Scenario:
// 1. 3-node cluster: primary (P1) + 2 standbys (S1, S2) with ANY_2 durability
// 2. Kill P1's postgres to trigger failover
// 3. Wait for multiorch to elect new primary (S1 becomes P2)
// 4. Write data to new primary to ensure timeline has diverged
// 5. Leave P1's postgres stopped (it's still marked PRIMARY in topology with old term)
// 6. Multiorch detects stale primary (both PRIMARY in topology, P1 has lower term)
// 7. Multiorch calls DemoteStalePrimary which starts postgres, runs pg_rewind, restarts as standby
// 8. Multiorch configures replication from P2
// 9. Verify P1 rejoins as a replica of P2
//
// This test verifies the complete stale primary detection and timeline divergence repair flow:
// 1. StalePrimaryAnalyzer detects when old primary comes back with a lower consensus term
// 2. DemoteStalePrimaryAction demotes the stale primary using the correct primary's term
// 3. NotReplicatingAnalyzer detects the replica is not replicating
// 4. FixReplicationAction detects timeline divergence via pg_rewind and repairs the replica
func TestDemoteStalePrimary(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestDemoteStalePrimary test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end stale primary demotion test (no postgres binaries)")
	}

	// Create 3-node cluster with multiorch
	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiOrchCount(1),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
	)
	defer cleanup()

	setup.StartMultiOrchs(t)

	// Get initial primary
	oldPrimary := setup.GetPrimary(t)
	require.NotNil(t, oldPrimary, "primary should exist")
	oldPrimaryName := setup.PrimaryName
	t.Logf("Initial primary: %s", oldPrimaryName)

	// Disable monitoring on old primary so postgres is not restarted by pooler
	oldPrimaryClient, err := shardsetup.NewMultipoolerClient(oldPrimary.Multipooler.GrpcPort)
	require.NoError(t, err)
	defer oldPrimaryClient.Close()

	_, err = oldPrimaryClient.Manager.SetMonitor(t.Context(), &multipoolermanagerdatapb.SetMonitorRequest{Enabled: false})
	require.NoError(t, err)
	defer func() {
		_, _ = oldPrimaryClient.Manager.SetMonitor(t.Context(), &multipoolermanagerdatapb.SetMonitorRequest{Enabled: true})
	}()

	// Step 1: Kill postgres on primary to trigger failover
	t.Log("Killing postgres on primary to trigger failover...")
	setup.KillPostgres(t, oldPrimaryName)

	// Step 2: Wait for new primary election
	t.Log("Waiting for new primary election...")
	newPrimaryName := waitForNewPrimary(t, setup, oldPrimaryName, 30*time.Second)
	require.NotEmpty(t, newPrimaryName, "new primary should be elected")
	t.Logf("New primary elected: %s", newPrimaryName)

	// Step 3: Write data to new primary to ensure timeline has diverged
	t.Log("Writing data to new primary to ensure timeline divergence...")
	writeDataToNewPrimary(t, setup, newPrimaryName)

	// Step 4: Leave postgres stopped on old primary
	// Multiorch will detect split-brain based on topology (both PRIMARY) and consensus terms,
	// then call DemoteStalePrimary which will handle starting postgres, pg_rewind, and restart
	t.Log("Leaving postgres stopped on old primary - multiorch will handle restart...")

	// Step 5: Wait for multiorch to detect and repair divergence
	// This may take longer because multiorch needs to:
	// 1. Detect the split-brain (both PRIMARY in topology, different terms)
	// 2. Call DemoteStalePrimary RPC on the stale primary (lower term)
	// 3. DemoteStalePrimary starts postgres, runs pg_rewind, restarts as standby
	// 4. Configure replication to the new primary
	// 5. Start WAL receiver
	t.Log("Waiting for multiorch to detect stale primary, run pg_rewind, and configure replication...")
	waitForDivergenceRepaired(t, setup, oldPrimaryName, newPrimaryName, 120*time.Second)

	// Step 6: Verify old primary is now replicating from new primary
	t.Log("Verifying old primary is now a replica...")
	verifyReplicaReplicating(t, setup, oldPrimaryName, newPrimaryName)

	// Step 7: Verify replication is actually working by writing data and checking it replicates
	t.Log("Verifying data replication works after pg_rewind...")
	verifyDataReplication(t, setup, oldPrimaryName, newPrimaryName)

	t.Log("TestDemoteStalePrimary completed successfully")
}

// writeDataToNewPrimary writes data to the new primary to ensure timeline divergence
func writeDataToNewPrimary(t *testing.T, setup *shardsetup.ShardSetup, primaryName string) {
	t.Helper()

	primary := setup.GetMultipoolerInstance(primaryName)
	require.NotNil(t, primary, "primary instance should exist")

	socketDir := filepath.Join(primary.Pgctld.DataDir, "pg_sockets")
	db := connectToPostgres(t, socketDir, primary.Pgctld.PgPort)
	require.NotNil(t, db, "should connect to new primary")
	defer db.Close()

	// Write data that ensures the timelines have diverged
	_, err := db.Exec("CREATE TABLE IF NOT EXISTS timeline_test (id SERIAL PRIMARY KEY, data TEXT)")
	require.NoError(t, err, "should create test table")

	_, err = db.Exec("INSERT INTO timeline_test (data) VALUES ('new_primary_data')")
	require.NoError(t, err, "should insert data on new primary")

	t.Log("Wrote data to new primary to ensure timeline divergence")
}

// waitForDivergenceRepaired waits for multiorch to repair the diverged node
func waitForDivergenceRepaired(t *testing.T, setup *shardsetup.ShardSetup, oldPrimaryName, newPrimaryName string, timeout time.Duration) {
	t.Helper()

	oldPrimary := setup.GetMultipoolerInstance(oldPrimaryName)
	require.NotNil(t, oldPrimary, "old primary instance should exist")

	require.Eventually(t, func() bool {
		client, err := shardsetup.NewMultipoolerClient(oldPrimary.Multipooler.GrpcPort)
		if err != nil {
			t.Logf("Cannot connect to old primary: %v", err)
			return false
		}
		defer client.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		resp, err := client.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
		if err != nil {
			t.Logf("Status call failed: %v", err)
			return false
		}

		// Check if it's now a REPLICA and replicating
		if resp.Status.PoolerType != clustermetadatapb.PoolerType_REPLICA {
			t.Logf("Old primary type is %s, waiting for REPLICA...", resp.Status.PoolerType)
			return false
		}

		if resp.Status.ReplicationStatus == nil || resp.Status.ReplicationStatus.PrimaryConnInfo == nil {
			t.Logf("Old primary not yet configured for replication")
			return false
		}

		t.Logf("Old primary is now REPLICA, replicating from %s",
			resp.Status.ReplicationStatus.PrimaryConnInfo.Host)
		return true
	}, timeout, 2*time.Second, "old primary should become replica after pg_rewind")
}

// verifyReplicaReplicating verifies the replica is actively streaming from the new primary
func verifyReplicaReplicating(t *testing.T, setup *shardsetup.ShardSetup, replicaName, primaryName string) {
	t.Helper()

	replica := setup.GetMultipoolerInstance(replicaName)
	require.NotNil(t, replica, "replica instance should exist")

	client, err := shardsetup.NewMultipoolerClient(replica.Multipooler.GrpcPort)
	require.NoError(t, err, "should connect to replica")
	defer client.Close()

	// Use Eventually to allow time for WAL receiver to start streaming after pg_rewind
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		resp, err := client.Manager.StandbyReplicationStatus(ctx, &multipoolermanagerdatapb.StandbyReplicationStatusRequest{})
		if err != nil {
			t.Logf("Failed to get replication status: %v", err)
			return false
		}
		if resp.Status == nil {
			t.Logf("Replication status is nil")
			return false
		}
		if resp.Status.PrimaryConnInfo == nil {
			t.Logf("Primary conninfo is nil")
			return false
		}
		if resp.Status.LastReceiveLsn == "" {
			t.Logf("Last receive LSN is empty")
			return false
		}
		if resp.Status.WalReceiverStatus != "streaming" {
			t.Logf("WAL receiver status is %q, waiting for 'streaming'", resp.Status.WalReceiverStatus)
			return false
		}

		t.Logf("Replica %s is streaming from %s:%d, last_receive_lsn=%s, wal_receiver_status=%s",
			replicaName,
			resp.Status.PrimaryConnInfo.Host,
			resp.Status.PrimaryConnInfo.Port,
			resp.Status.LastReceiveLsn,
			resp.Status.WalReceiverStatus)
		return true
	}, 30*time.Second, 1*time.Second, "Replication should be streaming after pg_rewind")
}

// verifyDataReplication writes data to the new primary and verifies it replicates to the old primary
func verifyDataReplication(t *testing.T, setup *shardsetup.ShardSetup, replicaName, primaryName string) {
	t.Helper()

	primary := setup.GetMultipoolerInstance(primaryName)
	require.NotNil(t, primary, "primary instance should exist")

	replica := setup.GetMultipoolerInstance(replicaName)
	require.NotNil(t, replica, "replica instance should exist")

	// Get primary and replica clients
	primaryClient, err := shardsetup.NewMultipoolerClient(primary.Multipooler.GrpcPort)
	require.NoError(t, err, "should connect to primary client")
	defer primaryClient.Close()

	replicaClient, err := shardsetup.NewMultipoolerClient(replica.Multipooler.GrpcPort)
	require.NoError(t, err, "should connect to replica client")
	defer replicaClient.Close()

	// Write a unique test value to the primary
	testValue := fmt.Sprintf("replication_test_%d", time.Now().Unix())
	_, err = primaryClient.Pooler.ExecuteQuery(context.Background(),
		fmt.Sprintf("INSERT INTO timeline_test (data) VALUES ('%s')", testValue), 0)
	require.NoError(t, err, "should insert test data on primary")
	t.Logf("Wrote test data to primary: %s", testValue)

	// Get primary's current LSN
	primaryPosResp, err := primaryClient.Manager.PrimaryPosition(utils.WithShortDeadline(t), &multipoolermanagerdatapb.PrimaryPositionRequest{})
	require.NoError(t, err, "should get primary LSN position")
	primaryLSN := primaryPosResp.LsnPosition
	t.Logf("Primary LSN after insert: %s", primaryLSN)

	// Wait for replica PostgreSQL to be ready after pg_rewind and restart
	t.Logf("Waiting for replica %s PostgreSQL to be ready...", replicaName)
	require.Eventually(t, func() bool {
		statusResp, err := replicaClient.Manager.StandbyReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StandbyReplicationStatusRequest{})
		return err == nil && statusResp.Status != nil && statusResp.Status.PrimaryConnInfo != nil
	}, 10*time.Second, 500*time.Millisecond, "replica should be ready after pg_rewind")
	t.Logf("Replica PostgreSQL is ready")

	// Wait for replica to catch up to primary's LSN
	t.Logf("Waiting for replica %s to catch up to primary LSN %s...", replicaName, primaryLSN)
	_, err = replicaClient.Manager.WaitForLSN(utils.WithTimeout(t, 10*time.Second), &multipoolermanagerdatapb.WaitForLSNRequest{
		TargetLsn: primaryLSN,
	})
	require.NoError(t, err, "replica should catch up to primary LSN")
	t.Logf("Replica caught up to primary LSN")

	// Verify the data is present on the replica
	result, err := replicaClient.Pooler.ExecuteQuery(context.Background(),
		fmt.Sprintf("SELECT COUNT(*) FROM timeline_test WHERE data = '%s'", testValue), 1)
	require.NoError(t, err, "should be able to query replica")
	require.Len(t, result.Rows, 1, "should have one result row")
	rowCount := string(result.Rows[0].Values[0])
	require.Equal(t, "1", rowCount, "replicated data should be found on replica")

	t.Logf("Verified data replicated successfully: %s", testValue)
}
