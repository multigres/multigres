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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestFixReplication tests multiorch's ability to detect a replica with broken
// replication and automatically fix it by configuring primary_conninfo.
//
// The test:
// 1. Sets up a 3-node cluster with primary and 2 replicas
// 2. Breaks replication by stopping replication and clearing primary_conninfo
// 3. Verifies multiorch detects the problem and fixes replication
func TestFixReplication(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestFixReplication test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end fix replication test (short mode or no postgres binaries)")
	}

	// Setup test cluster (3 nodes: primary + 2 replicas)
	// We need 3 nodes because the "remove from standby list" test requires
	// at least one standby remaining after removal (list can't be empty).
	// Note: multiorch is NOT started yet - we'll start it after breaking replication
	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiOrchCount(1),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
	)
	defer cleanup()

	t.Logf("Test cluster ready in directory: %s", setup.TempDir)
	t.Logf("Identified primary: %s", setup.PrimaryName)

	// Find the replica name
	var replicaName string
	for name := range setup.Multipoolers {
		if name != setup.PrimaryName {
			replicaName = name
			break
		}
	}
	require.NotEmpty(t, replicaName, "should have a replica")
	t.Logf("Identified replica: %s", replicaName)

	// Create test clients for primary and replica
	primaryClient := setup.NewPrimaryClient(t)
	defer primaryClient.Close()

	replicaInst := setup.GetMultipoolerInstance(replicaName)
	require.NotNil(t, replicaInst, "replica instance should exist")

	replicaClient, err := shardsetup.NewMultipoolerClient(replicaInst.Multipooler.GrpcPort)
	require.NoError(t, err, "should be able to create replica client")
	defer replicaClient.Close()

	// Create a test table on primary
	t.Log("Creating test table on primary...")
	_, err = primaryClient.Pooler.ExecuteQuery(context.Background(), "CREATE TABLE IF NOT EXISTS fix_replication_test (id SERIAL PRIMARY KEY, data TEXT)", 0)
	require.NoError(t, err, "should be able to create test table")

	// Verify replication is currently working (before we break it)
	t.Log("Verifying replication is working before breaking it...")
	verifyReplicationStreaming(t, replicaClient)

	// Wait for CREATE TABLE to be replicated to replica before breaking replication.
	t.Log("Waiting for table to be replicated to replica...")
	require.Eventually(t, func() bool {
		_, err := replicaClient.Pooler.ExecuteQuery(context.Background(), "SELECT 1 FROM fix_replication_test LIMIT 0", 0)
		if err != nil {
			t.Logf("Table not yet on replica: %v", err)
			return false
		}
		return true
	}, 10*time.Second, 100*time.Millisecond, "table should be replicated to replica")
	t.Log("Table verified on replica")

	// Break replication using RPC (while multiorch is NOT running)
	t.Logf("Breaking replication on %s via RPC...", replicaName)
	breakReplication(t, replicaClient)

	// Insert data on primary while replication is broken
	t.Log("Inserting data on primary while replication is broken...")
	_, err = primaryClient.Pooler.ExecuteQuery(context.Background(), "INSERT INTO fix_replication_test (data) VALUES ('inserted_while_broken')", 0)
	require.NoError(t, err, "should be able to insert data on primary")

	// Verify data is NOT visible on replica (replication is broken)
	t.Log("Verifying data is NOT yet visible on replica...")
	result, err := replicaClient.Pooler.ExecuteQuery(context.Background(), "SELECT COUNT(*) FROM fix_replication_test WHERE data = 'inserted_while_broken'", 1)
	require.NoError(t, err, "should be able to query replica")
	require.Equal(t, "0", string(result.Rows[0].Values[0]), "data should NOT be visible on replica while replication is broken")

	// NOW start multiorch - it should detect and fix the broken replication
	t.Log("Starting multiorch to detect and fix replication...")
	setup.StartMultiOrchs(t)

	// Wait for multiorch to detect and fix the replication
	t.Log("Waiting for multiorch to detect and fix replication...")
	waitForReplicationFixed(t, replicaClient, 10*time.Second)

	// Verify data IS now visible on replica after fix
	t.Log("Verifying data IS now visible on replica after fix...")
	require.Eventually(t, func() bool {
		result, err := replicaClient.Pooler.ExecuteQuery(context.Background(), "SELECT COUNT(*) FROM fix_replication_test WHERE data = 'inserted_while_broken'", 1)
		if err != nil {
			t.Logf("Error querying replica: %v", err)
			return false
		}
		count := string(result.Rows[0].Values[0])
		if count != "1" {
			t.Logf("Waiting for data to replicate... count=%s", count)
			return false
		}
		return true
	}, 5*time.Second, 500*time.Millisecond, "data should replicate to replica after fix")

	// Verify replica was added to primary's synchronous standby list
	t.Log("Verifying replica is in primary's synchronous standby list...")
	require.Eventually(t, func() bool {
		return isReplicaInStandbyList(t, primaryClient, replicaName)
	}, 10*time.Second, 1*time.Second, "replica should be added to primary's synchronous standby list")

	t.Log("First fix completed successfully, breaking replication again...")

	// Stop multiorch before breaking replication again
	t.Log("Stopping multiorch before breaking replication again...")
	multiorch := setup.GetMultiOrch("multiorch")
	require.NotNil(t, multiorch, "multiorch instance should exist")
	multiorch.Stop()

	// Break replication a second time to verify multiorch can fix it repeatedly
	t.Logf("Breaking replication on %s via RPC (second time)...", replicaName)
	breakReplication(t, replicaClient)

	// Insert more data on primary while replication is broken again
	t.Log("Inserting more data on primary while replication is broken (second time)...")
	_, err = primaryClient.Pooler.ExecuteQuery(context.Background(), "INSERT INTO fix_replication_test (data) VALUES ('inserted_while_broken_2')", 0)
	require.NoError(t, err, "should be able to insert data on primary")

	// Verify new data is NOT visible on replica
	t.Log("Verifying new data is NOT yet visible on replica...")
	result, err = replicaClient.Pooler.ExecuteQuery(context.Background(), "SELECT COUNT(*) FROM fix_replication_test WHERE data = 'inserted_while_broken_2'", 1)
	require.NoError(t, err, "should be able to query replica")
	require.Equal(t, "0", string(result.Rows[0].Values[0]), "new data should NOT be visible on replica while replication is broken")

	// Start multiorch again - it should detect and fix the broken replication
	t.Log("Starting multiorch to detect and fix replication (second time)...")
	err = multiorch.Start(t)
	require.NoError(t, err, "should be able to restart multiorch")

	// Wait for multiorch to detect and fix the replication again
	t.Log("Waiting for multiorch to detect and fix replication (second time)...")
	waitForReplicationFixed(t, replicaClient, 60*time.Second)

	// Verify new data IS now visible on replica after second fix
	t.Log("Verifying new data IS now visible on replica after second fix...")
	require.Eventually(t, func() bool {
		result, err := replicaClient.Pooler.ExecuteQuery(context.Background(), "SELECT COUNT(*) FROM fix_replication_test WHERE data = 'inserted_while_broken_2'", 1)
		if err != nil {
			t.Logf("Error querying replica: %v", err)
			return false
		}
		count := string(result.Rows[0].Values[0])
		if count != "1" {
			t.Logf("Waiting for data to replicate... count=%s", count)
			return false
		}
		return true
	}, 5*time.Second, 500*time.Millisecond, "new data should replicate to replica after second fix")

	// Verify replica is still in primary's synchronous standby list after second fix
	t.Log("Verifying replica is in primary's synchronous standby list (after second fix)...")
	require.Eventually(t, func() bool {
		return isReplicaInStandbyList(t, primaryClient, replicaName)
	}, 10*time.Second, 1*time.Second, "replica should be in primary's synchronous standby list after second fix")

	// Test case: Replica not in standby list (but replication is working)
	// This tests the ReplicaNotInStandbyListAnalyzer
	t.Log("Testing fix for replica not in standby list...")
	// Start multiorch again - it should detect and fix the broken replication
	multiorch.Stop()

	// Remove replica from standby list (without breaking replication)
	// Note: multiorch may re-add it very quickly, so we just verify:
	// 1. The removal RPC succeeds
	// 2. Multiorch ensures the replica is back in the list
	// 3. Replication continues working throughout
	t.Logf("Removing replica %s from primary's standby list...", replicaName)
	removeReplicaFromStandbyList(t, primaryClient, replicaName)

	// Verify replication is still working (primary_conninfo should still be configured)
	t.Log("Verifying replication is still working after standby list removal...")
	verifyReplicationStreaming(t, replicaClient)

	t.Log("Starting multiorch to detect and fix replication (second time)...")
	err = multiorch.Start(t)
	require.NoError(t, err, "should be able to restart multiorch")

	// Multiorch should detect the missing standby and add it back
	// (it may already be back due to fast detection)
	t.Log("Verifying multiorch maintains replica in standby list...")
	require.Eventually(t, func() bool {
		return isReplicaInStandbyList(t, primaryClient, replicaName)
	}, 5*time.Second, 1*time.Second, "multiorch should maintain replica in primary's synchronous standby list")

	// Verify replication is still working after fix
	t.Log("Verifying replication is still working after standby list fix...")
	verifyReplicationStreaming(t, replicaClient)

	t.Log("TestFixReplication completed successfully")
}

// verifyReplicationStreaming checks that the replica has replication configured and is receiving WAL
func verifyReplicationStreaming(t *testing.T, client *shardsetup.MultipoolerClient) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Manager.StandbyReplicationStatus(ctx, &multipoolermanagerdatapb.StandbyReplicationStatusRequest{})
	require.NoError(t, err, "StandbyReplicationStatus should succeed")
	require.NotNil(t, resp.Status, "Status should not be nil")

	// Verify primary_conninfo is configured
	require.NotNil(t, resp.Status.PrimaryConnInfo, "PrimaryConnInfo should be set")
	require.NotEmpty(t, resp.Status.PrimaryConnInfo.Host, "PrimaryConnInfo.Host should not be empty")

	// Verify we're receiving WAL (LastReceiveLsn is set when streaming)
	require.NotEmpty(t, resp.Status.LastReceiveLsn, "LastReceiveLsn should not be empty when streaming")

	t.Logf("Replication verified: primary_host=%s, last_receive_lsn=%s",
		resp.Status.PrimaryConnInfo.Host, resp.Status.LastReceiveLsn)
}

// breakReplication stops replication and clears primary_conninfo using the RPC API.
// It waits until the replication is confirmed broken before returning.
func breakReplication(t *testing.T, client *shardsetup.MultipoolerClient) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Clear primary_conninfo by setting it to empty host
	// Use StopReplicationBefore=true to stop WAL receiver first
	_, err := client.Manager.SetPrimaryConnInfo(ctx, &multipoolermanagerdatapb.SetPrimaryConnInfoRequest{
		PrimaryPoolerId:       "test-primary-id",
		Host:                  "",
		Port:                  0,
		StopReplicationBefore: true,
		StartReplicationAfter: false,
		Force:                 true, // Force to bypass term check
	})
	require.NoError(t, err, "SetPrimaryConnInfo (clear) should succeed")
	t.Log("Cleared primary_conninfo via RPC")

	// Wait until replication is confirmed broken
	require.Eventually(t, func() bool {
		return isReplicationBroken(t, client)
	}, 10*time.Second, 500*time.Millisecond, "replication should be broken after clearing primary_conninfo")
}

// isReplicationBroken checks if replication is no longer configured/streaming
func isReplicationBroken(t *testing.T, client *shardsetup.MultipoolerClient) bool {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Manager.StandbyReplicationStatus(ctx, &multipoolermanagerdatapb.StandbyReplicationStatusRequest{})
	if err != nil {
		t.Logf("StandbyReplicationStatus failed: %v", err)
		return false
	}
	if resp.Status == nil {
		t.Log("Waiting for replication status...")
		return false
	}

	// Verify primary_conninfo is cleared (host should be empty)
	if resp.Status.PrimaryConnInfo != nil && resp.Status.PrimaryConnInfo.Host != "" {
		t.Logf("Waiting for primary_conninfo to be cleared, current host: %s", resp.Status.PrimaryConnInfo.Host)
		return false
	}

	t.Log("Confirmed replication is broken (primary_conninfo cleared)")
	return true
}

// isReplicaInStandbyList checks if the replica is in the primary's synchronous standby list
func isReplicaInStandbyList(t *testing.T, primaryClient *shardsetup.MultipoolerClient, replicaName string) bool {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := primaryClient.Manager.PrimaryStatus(ctx, &multipoolermanagerdatapb.PrimaryStatusRequest{})
	if err != nil {
		t.Logf("PrimaryStatus failed: %v", err)
		return false
	}

	if resp.Status == nil || resp.Status.SyncReplicationConfig == nil {
		t.Log("Waiting for sync replication config...")
		return false
	}

	// Look for the replica in the standby list by Name
	// In shardsetup, standbys are identified by Name (e.g., "pooler-1", "pooler-2")
	for _, standbyID := range resp.Status.SyncReplicationConfig.StandbyIds {
		if standbyID.Name == replicaName {
			t.Logf("Found replica %s in standby list", replicaName)
			return true
		}
	}

	t.Logf("Replica %s not yet in standby list, current standbys: %v",
		replicaName, resp.Status.SyncReplicationConfig.StandbyIds)
	return false
}

// removeReplicaFromStandbyList removes the replica from the primary's synchronous standby list.
// It verifies the replica is no longer in the list before returning.
func removeReplicaFromStandbyList(t *testing.T, primaryClient *shardsetup.MultipoolerClient, replicaName string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Use UpdateSynchronousStandbyList to remove the replica
	// In shardsetup, the ID uses Cell="test-cell" and Name=replicaName
	_, err := primaryClient.Manager.UpdateSynchronousStandbyList(ctx, &multipoolermanagerdatapb.UpdateSynchronousStandbyListRequest{
		Operation: multipoolermanagerdatapb.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_REMOVE,
		StandbyIds: []*clustermetadatapb.ID{{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "test-cell",
			Name:      replicaName,
		}},
		ReloadConfig: true,
		Force:        true, // Force to bypass term check
	})
	require.NoError(t, err, "UpdateSynchronousStandbyList (remove) should succeed")
	t.Logf("Removed replica %s from standby list via RPC", replicaName)

	// Verify the replica is no longer in the standby list
	require.Eventually(t, func() bool {
		return !isReplicaInStandbyList(t, primaryClient, replicaName)
	}, 5*time.Second, 1*time.Second, "replica should not be in standby list after removal")
}

// waitForReplicationFixed polls until multiorch fixes the replication
func waitForReplicationFixed(t *testing.T, client *shardsetup.MultipoolerClient, timeout time.Duration) {
	t.Helper()

	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		resp, err := client.Manager.StandbyReplicationStatus(ctx, &multipoolermanagerdatapb.StandbyReplicationStatusRequest{})
		if err != nil {
			t.Logf("Error checking replication status: %v", err)
			return false
		}

		if resp.Status == nil {
			t.Log("Waiting for replication fix... (status is nil)")
			return false
		}

		// Check if primary_conninfo is configured again
		if resp.Status.PrimaryConnInfo == nil || resp.Status.PrimaryConnInfo.Host == "" {
			t.Log("Waiting for replication fix... (primary_conninfo not yet configured)")
			return false
		}

		// Check if we're receiving WAL again
		if resp.Status.LastReceiveLsn == "" {
			t.Logf("Waiting for replication fix... (primary_conninfo=%s, but not yet receiving WAL)",
				resp.Status.PrimaryConnInfo.Host)
			return false
		}

		t.Logf("Replication fixed: primary_host=%s, last_receive_lsn=%s",
			resp.Status.PrimaryConnInfo.Host, resp.Status.LastReceiveLsn)
		return true
	}, timeout, 2*time.Second, "Multiorch should fix replication within timeout")
}
