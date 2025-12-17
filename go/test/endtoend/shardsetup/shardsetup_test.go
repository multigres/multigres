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

package shardsetup

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// TestShardSetup_ThreeNodeCluster validates that a 3-node cluster is created correctly
// with proper wiring: one primary and two standbys all replicating.
func TestShardSetup_ThreeNodeCluster(t *testing.T) {
	setup := getSharedSetup(t)
	setup.SetupTest(t)

	// Verify we have 3 multipooler instances
	require.Len(t, setup.Multipoolers, 3, "should have 3 multipooler instances")

	// Verify naming convention
	require.NotNil(t, setup.GetMultipoolerInstance("primary"), "should have 'primary' instance")
	require.NotNil(t, setup.GetMultipoolerInstance("standby"), "should have 'standby' instance")
	require.NotNil(t, setup.GetMultipoolerInstance("standby2"), "should have 'standby2' instance")

	// Verify convenience methods work
	require.NotNil(t, setup.PrimaryMultipooler(), "PrimaryMultipooler() should return the primary")
	require.NotNil(t, setup.StandbyMultipooler(), "StandbyMultipooler() should return the standby")
	require.Equal(t, setup.GetMultipooler("primary"), setup.PrimaryMultipooler())
	require.Equal(t, setup.GetMultipooler("standby"), setup.StandbyMultipooler())

	ctx := context.Background()

	// Verify primary is not in recovery (is actually a primary)
	primaryClient := setup.GetPrimaryClient(t)
	defer primaryClient.Close()

	inRecovery, err := QueryStringValue(ctx, primaryClient.Pooler, "SELECT pg_is_in_recovery()")
	require.NoError(t, err)
	assert.Equal(t, "f", inRecovery, "primary should NOT be in recovery")

	// Verify primary has correct pooler type
	err = ValidatePoolerType(ctx, primaryClient.Manager, clustermetadatapb.PoolerType_PRIMARY, "primary")
	require.NoError(t, err)

	// Verify both standbys are in recovery and replicating
	for _, standbyName := range []string{"standby", "standby2"} {
		standbyClient := setup.GetClient(t, standbyName)

		// Verify in recovery mode
		inRecovery, err := QueryStringValue(ctx, standbyClient.Pooler, "SELECT pg_is_in_recovery()")
		require.NoError(t, err, "%s should respond to query", standbyName)
		assert.Equal(t, "t", inRecovery, "%s should be in recovery", standbyName)

		// Verify pooler type is REPLICA
		err = ValidatePoolerType(ctx, standbyClient.Manager, clustermetadatapb.PoolerType_REPLICA, standbyName)
		require.NoError(t, err)

		// Verify replication is streaming (SetupTest configures replication)
		resp, err := standbyClient.Pooler.ExecuteQuery(ctx, "SELECT status FROM pg_stat_wal_receiver", 1)
		require.NoError(t, err, "%s should query pg_stat_wal_receiver", standbyName)
		require.Len(t, resp.Rows, 1, "%s should have WAL receiver", standbyName)
		assert.Equal(t, "streaming", string(resp.Rows[0].Values[0]), "%s should be streaming", standbyName)

		standbyClient.Close()
	}
}

// TestShardSetup_DemoteAndReset validates that after demoting the primary,
// ResetToCleanState correctly restores the cluster.
func TestShardSetup_DemoteAndReset(t *testing.T) {
	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := context.Background()

	// Verify initial state - primary is not in recovery
	primaryClient := setup.GetPrimaryClient(t)
	inRecovery, err := QueryStringValue(ctx, primaryClient.Pooler, "SELECT pg_is_in_recovery()")
	require.NoError(t, err)
	require.Equal(t, "f", inRecovery, "primary should start as primary (not in recovery)")
	primaryClient.Close()

	// Demote the primary
	setup.DemotePrimary(t)

	// Verify primary is now in recovery (demoted)
	primaryClient = setup.GetPrimaryClient(t)
	inRecovery, err = QueryStringValue(ctx, primaryClient.Pooler, "SELECT pg_is_in_recovery()")
	require.NoError(t, err)
	require.Equal(t, "t", inRecovery, "primary should be in recovery after demotion")
	primaryClient.Close()

	// Manually call reset
	t.Log("Calling ResetToCleanState...")
	setup.ResetToCleanState(t)

	// Verify primary is restored
	primaryClient = setup.GetPrimaryClient(t)
	defer primaryClient.Close()

	require.Eventually(t, func() bool {
		val, err := QueryStringValue(ctx, primaryClient.Pooler, "SELECT pg_is_in_recovery()")
		return err == nil && val == "f"
	}, 10*time.Second, 100*time.Millisecond, "primary should be restored to primary state")

	// Verify pooler type is PRIMARY
	err = ValidatePoolerType(ctx, primaryClient.Manager, clustermetadatapb.PoolerType_PRIMARY, "primary")
	require.NoError(t, err)

	// Verify term is reset to 1
	err = ValidateTerm(ctx, primaryClient.Consensus, 1, "primary")
	require.NoError(t, err)

	t.Log("Primary restored successfully")

	// Verify standbys are still standbys with correct state
	for _, standbyName := range []string{"standby", "standby2"} {
		standbyClient := setup.GetClient(t, standbyName)

		// Should be in recovery
		inRecovery, err := QueryStringValue(ctx, standbyClient.Pooler, "SELECT pg_is_in_recovery()")
		require.NoError(t, err)
		assert.Equal(t, "t", inRecovery, "%s should be in recovery", standbyName)

		// Term should be 1
		err = ValidateTerm(ctx, standbyClient.Consensus, 1, standbyName)
		require.NoError(t, err)

		// Pooler type should be REPLICA
		err = ValidatePoolerType(ctx, standbyClient.Manager, clustermetadatapb.PoolerType_REPLICA, standbyName)
		require.NoError(t, err)

		standbyClient.Close()
	}

	// Verify clean state validation passes
	err = setup.ValidateCleanState()
	require.NoError(t, err, "ValidateCleanState should pass after reset")
}

// TestShardSetup_ReplicationWorks validates that data written to primary replicates to standbys.
func TestShardSetup_ReplicationWorks(t *testing.T) {
	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := context.Background()

	// Create a test table and insert data on primary
	primaryClient := setup.GetPrimaryClient(t)
	defer primaryClient.Close()

	_, err := primaryClient.Pooler.ExecuteQuery(ctx, "CREATE TABLE IF NOT EXISTS test_replication (id INT PRIMARY KEY, value TEXT)", 0)
	require.NoError(t, err)

	_, err = primaryClient.Pooler.ExecuteQuery(ctx, "INSERT INTO test_replication (id, value) VALUES (1, 'test_data') ON CONFLICT (id) DO UPDATE SET value = 'test_data'", 0)
	require.NoError(t, err)

	// Verify data replicates to both standbys
	for _, standbyName := range []string{"standby", "standby2"} {
		standbyClient := setup.GetClient(t, standbyName)

		// Wait for replication to catch up
		require.Eventually(t, func() bool {
			resp, err := standbyClient.Pooler.ExecuteQuery(ctx, "SELECT value FROM test_replication WHERE id = 1", 1)
			if err != nil || len(resp.Rows) == 0 {
				return false
			}
			return string(resp.Rows[0].Values[0]) == "test_data"
		}, 5*time.Second, 100*time.Millisecond, "data should replicate to %s", standbyName)

		standbyClient.Close()
	}

	// Cleanup: drop the test table
	_, _ = primaryClient.Pooler.ExecuteQuery(ctx, "DROP TABLE IF EXISTS test_replication", 0)
}

// TestShardSetup_GUCModificationAndReset tests that GUC modifications are properly reset.
// This is a self-contained test that modifies GUCs, calls reset, and verifies reset worked.
func TestShardSetup_GUCModificationAndReset(t *testing.T) {
	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := context.Background()

	// Verify initial clean state on primary
	primaryClient := setup.GetPrimaryClient(t)
	defer primaryClient.Close()

	initialSyncStandby, err := QueryStringValue(ctx, primaryClient.Pooler, "SHOW synchronous_standby_names")
	require.NoError(t, err)
	require.Equal(t, "", initialSyncStandby, "synchronous_standby_names should start empty")

	initialSyncCommit, err := QueryStringValue(ctx, primaryClient.Pooler, "SHOW synchronous_commit")
	require.NoError(t, err)
	require.Equal(t, "on", initialSyncCommit, "synchronous_commit should start as 'on'")

	// Modify GUCs on primary
	_, err = primaryClient.Pooler.ExecuteQuery(ctx, "ALTER SYSTEM SET synchronous_standby_names = 'teststandby'", 0)
	require.NoError(t, err)
	_, err = primaryClient.Pooler.ExecuteQuery(ctx, "ALTER SYSTEM SET synchronous_commit = 'remote_write'", 0)
	require.NoError(t, err)
	_, err = primaryClient.Pooler.ExecuteQuery(ctx, "SELECT pg_reload_conf()", 0)
	require.NoError(t, err)

	// Wait for GUCs to converge on primary
	require.Eventually(t, func() bool {
		val, err := QueryStringValue(ctx, primaryClient.Pooler, "SHOW synchronous_standby_names")
		return err == nil && val == "teststandby"
	}, 5*time.Second, 100*time.Millisecond, "synchronous_standby_names should be modified")

	require.Eventually(t, func() bool {
		val, err := QueryStringValue(ctx, primaryClient.Pooler, "SHOW synchronous_commit")
		return err == nil && val == "remote_write"
	}, 5*time.Second, 100*time.Millisecond, "synchronous_commit should be modified")

	t.Log("Primary GUCs modified successfully")

	// Modify GUCs on standbys
	for _, standbyName := range []string{"standby", "standby2"} {
		standbyClient := setup.GetClient(t, standbyName)

		// Record current primary_conninfo (set by SetupTest)
		currentConnInfo, err := QueryStringValue(ctx, standbyClient.Pooler, "SHOW primary_conninfo")
		require.NoError(t, err)
		require.NotEmpty(t, currentConnInfo, "%s primary_conninfo should be set", standbyName)
		require.Contains(t, currentConnInfo, "localhost", "%s should be replicating from localhost", standbyName)

		// Modify primary_conninfo
		_, err = standbyClient.Pooler.ExecuteQuery(ctx, "ALTER SYSTEM SET primary_conninfo = 'host=modified_host'", 0)
		require.NoError(t, err)
		_, err = standbyClient.Pooler.ExecuteQuery(ctx, "SELECT pg_reload_conf()", 0)
		require.NoError(t, err)

		// Wait for primary_conninfo to converge
		require.Eventually(t, func() bool {
			val, err := QueryStringValue(ctx, standbyClient.Pooler, "SHOW primary_conninfo")
			return err == nil && val == "host=modified_host"
		}, 5*time.Second, 100*time.Millisecond, "%s primary_conninfo should be modified", standbyName)

		t.Logf("%s GUCs modified successfully", standbyName)
		standbyClient.Close()
	}

	// Manually call reset
	t.Log("Calling ResetToCleanState...")
	setup.ResetToCleanState(t)

	// Verify GUCs were reset on primary
	require.Eventually(t, func() bool {
		val, err := QueryStringValue(ctx, primaryClient.Pooler, "SHOW synchronous_standby_names")
		return err == nil && val == ""
	}, 5*time.Second, 100*time.Millisecond, "synchronous_standby_names should be reset to empty")

	require.Eventually(t, func() bool {
		val, err := QueryStringValue(ctx, primaryClient.Pooler, "SHOW synchronous_commit")
		return err == nil && val == "on"
	}, 5*time.Second, 100*time.Millisecond, "synchronous_commit should be reset to 'on'")

	t.Log("Primary GUCs reset successfully")

	// Verify GUCs were reset on standbys
	for _, standbyName := range []string{"standby", "standby2"} {
		standbyClient := setup.GetClient(t, standbyName)

		// primary_conninfo should be reset (empty in clean state)
		require.Eventually(t, func() bool {
			val, err := QueryStringValue(ctx, standbyClient.Pooler, "SHOW primary_conninfo")
			return err == nil && val == ""
		}, 5*time.Second, 100*time.Millisecond, "%s primary_conninfo should be reset to empty", standbyName)

		t.Logf("%s GUCs reset successfully", standbyName)
		standbyClient.Close()
	}

	// Verify clean state validation passes
	err = setup.ValidateCleanState()
	require.NoError(t, err, "ValidateCleanState should pass after reset")
}
