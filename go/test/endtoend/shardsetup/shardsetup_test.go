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
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/test/utils"
)

func skipIfShort(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("Skipping end-to-end test (short mode)")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end test (no postgres binaries)")
	}
}

// TestShardSetup_ThreeNodeCluster validates that a 3-node cluster is created correctly
// with proper wiring: one primary and two standbys all replicating.
func TestShardSetup_ThreeNodeCluster(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	setup.SetupTest(t)

	// Verify we have 3 multipooler instances
	require.Len(t, setup.Multipoolers, 3, "should have 3 multipooler instances")

	// Verify naming convention (generic names since multiorch decides primary)
	require.NotNil(t, setup.GetMultipoolerInstance("pooler-1"), "should have 'pooler-1' instance")
	require.NotNil(t, setup.GetMultipoolerInstance("pooler-2"), "should have 'pooler-2' instance")
	require.NotNil(t, setup.GetMultipoolerInstance("pooler-3"), "should have 'pooler-3' instance")

	// Verify primary was elected and convenience methods work
	require.NotEmpty(t, setup.PrimaryName, "PrimaryName should be set after bootstrap")
	primary := setup.GetPrimary(t)
	require.NotNil(t, primary, "GetPrimary() should return the elected primary")
	require.Equal(t, setup.GetMultipoolerInstance(setup.PrimaryName), primary)

	// Verify standbys (all non-primary nodes)
	standbys := setup.GetStandbys()
	require.Len(t, standbys, 2, "should have 2 standbys")

	ctx := t.Context()

	// Verify primary is not in recovery (is actually a primary)
	primaryClient := setup.NewPrimaryClient(t)
	defer primaryClient.Close()

	inRecovery, err := QueryStringValue(ctx, primaryClient.Pooler, "SELECT pg_is_in_recovery()")
	require.NoError(t, err)
	assert.Equal(t, "f", inRecovery, "primary should NOT be in recovery")

	// Verify primary has correct pooler type
	err = ValidatePoolerType(ctx, primaryClient.Manager, clustermetadatapb.PoolerType_PRIMARY, setup.PrimaryName)
	require.NoError(t, err)

	// Verify all standbys are in recovery and replicating
	for _, standby := range standbys {
		standbyClient := setup.NewClient(t, standby.Name)

		// Verify in recovery mode
		inRecovery, err := QueryStringValue(ctx, standbyClient.Pooler, "SELECT pg_is_in_recovery()")
		require.NoError(t, err, "%s should respond to query", standby.Name)
		assert.Equal(t, "t", inRecovery, "%s should be in recovery", standby.Name)

		// Verify pooler type is REPLICA
		err = ValidatePoolerType(ctx, standbyClient.Manager, clustermetadatapb.PoolerType_REPLICA, standby.Name)
		require.NoError(t, err)

		// Verify replication is streaming (SetupTest configures replication)
		resp, err := standbyClient.Pooler.ExecuteQuery(ctx, "SELECT status FROM pg_stat_wal_receiver", 1)
		require.NoError(t, err, "%s should query pg_stat_wal_receiver", standby.Name)
		require.Len(t, resp.Rows, 1, "%s should have WAL receiver", standby.Name)
		assert.Equal(t, "streaming", string(resp.Rows[0].Values[0]), "%s should be streaming", standby.Name)

		standbyClient.Close()
	}
}

// TestShardSetup_DemoteAndReset validates that after demoting the primary,
// ResetToCleanState correctly restores the cluster.
func TestShardSetup_DemoteAndReset(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := t.Context()

	// Verify initial state - primary is not in recovery
	primaryClient := setup.NewPrimaryClient(t)
	inRecovery, err := QueryStringValue(ctx, primaryClient.Pooler, "SELECT pg_is_in_recovery()")
	require.NoError(t, err)
	require.Equal(t, "f", inRecovery, "primary should start as primary (not in recovery)")
	primaryClient.Close()

	// Demote the primary
	setup.DemotePrimary(t)

	// Verify primary is now in recovery (demoted)
	primaryClient = setup.NewPrimaryClient(t)
	inRecovery, err = QueryStringValue(ctx, primaryClient.Pooler, "SELECT pg_is_in_recovery()")
	require.NoError(t, err)
	require.Equal(t, "t", inRecovery, "primary should be in recovery after demotion")
	primaryClient.Close()

	// Manually call reset
	t.Log("Calling ResetToCleanState...")
	setup.ResetToCleanState(t)

	// Verify primary is restored
	primaryClient = setup.NewPrimaryClient(t)
	defer primaryClient.Close()

	require.Eventually(t, func() bool {
		val, err := QueryStringValue(ctx, primaryClient.Pooler, "SELECT pg_is_in_recovery()")
		return err == nil && val == "f"
	}, 10*time.Second, 100*time.Millisecond, "primary should be restored to primary state")

	// Verify pooler type is PRIMARY
	err = ValidatePoolerType(ctx, primaryClient.Manager, clustermetadatapb.PoolerType_PRIMARY, setup.PrimaryName)
	require.NoError(t, err)

	// Verify term is reset to 1
	err = ValidateTerm(ctx, primaryClient.Consensus, 1, setup.PrimaryName)
	require.NoError(t, err)

	t.Log("Primary restored successfully")

	// Verify standbys are still standbys with correct state
	for _, standby := range setup.GetStandbys() {
		standbyClient := setup.NewClient(t, standby.Name)

		// Should be in recovery
		inRecovery, err := QueryStringValue(ctx, standbyClient.Pooler, "SELECT pg_is_in_recovery()")
		require.NoError(t, err)
		assert.Equal(t, "t", inRecovery, "%s should be in recovery", standby.Name)

		// Term should be 1
		err = ValidateTerm(ctx, standbyClient.Consensus, 1, standby.Name)
		require.NoError(t, err)

		// Pooler type should be REPLICA
		err = ValidatePoolerType(ctx, standbyClient.Manager, clustermetadatapb.PoolerType_REPLICA, standby.Name)
		require.NoError(t, err)

		standbyClient.Close()
	}

	// Verify clean state validation passes
	err = setup.ValidateCleanState()
	require.NoError(t, err, "ValidateCleanState should pass after reset")
}

// TestShardSetup_ReplicationWorks validates that data written to primary replicates to standbys.
func TestShardSetup_ReplicationWorks(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := t.Context()

	// Create a test table and insert data on primary
	primaryClient := setup.NewPrimaryClient(t)
	defer primaryClient.Close()

	_, err := primaryClient.Pooler.ExecuteQuery(ctx, "CREATE TABLE IF NOT EXISTS test_replication (id INT PRIMARY KEY, value TEXT)", 0)
	require.NoError(t, err)

	_, err = primaryClient.Pooler.ExecuteQuery(ctx, "INSERT INTO test_replication (id, value) VALUES (1, 'test_data') ON CONFLICT (id) DO UPDATE SET value = 'test_data'", 0)
	require.NoError(t, err)

	// Verify data replicates to all standbys
	for _, standby := range setup.GetStandbys() {
		standbyClient := setup.NewClient(t, standby.Name)

		// Wait for replication to catch up
		require.Eventually(t, func() bool {
			resp, err := standbyClient.Pooler.ExecuteQuery(ctx, "SELECT value FROM test_replication WHERE id = 1", 1)
			if err != nil || len(resp.Rows) == 0 {
				return false
			}
			return string(resp.Rows[0].Values[0]) == "test_data"
		}, 5*time.Second, 100*time.Millisecond, "data should replicate to %s", standby.Name)

		standbyClient.Close()
	}

	// Cleanup: drop the test table
	_, _ = primaryClient.Pooler.ExecuteQuery(ctx, "DROP TABLE IF EXISTS test_replication", 0)
}

// TestShardSetup_WithoutReplication verifies that WithoutReplication() leaves replication unconfigured.
// Standbys should have empty primary_conninfo and no WAL receiver.
func TestShardSetup_WithoutReplication(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	setup.SetupTest(t, WithoutReplication())

	ctx := t.Context()

	// Verify standbys have no replication configured
	for _, standby := range setup.GetStandbys() {
		standbyClient := setup.NewClient(t, standby.Name)

		// Verify primary_conninfo is empty
		connInfo, err := QueryStringValue(ctx, standbyClient.Pooler, "SHOW primary_conninfo")
		require.NoError(t, err, "%s should respond to SHOW primary_conninfo", standby.Name)
		assert.Equal(t, "", connInfo, "%s primary_conninfo should be empty with WithoutReplication()", standby.Name)

		// Verify no WAL receiver (no replication streaming)
		resp, err := standbyClient.Pooler.ExecuteQuery(ctx, "SELECT status FROM pg_stat_wal_receiver", 1)
		require.NoError(t, err, "%s should respond to pg_stat_wal_receiver query", standby.Name)
		assert.Empty(t, resp.Rows, "%s should have no WAL receiver with WithoutReplication()", standby.Name)

		standbyClient.Close()
	}

	// Verify primary has no synchronous_standby_names configured (clean state)
	primaryClient := setup.NewPrimaryClient(t)
	defer primaryClient.Close()

	syncStandby, err := QueryStringValue(ctx, primaryClient.Pooler, "SHOW synchronous_standby_names")
	require.NoError(t, err)
	assert.Equal(t, "", syncStandby, "synchronous_standby_names should be empty with WithoutReplication()")

	// Write data to primary - it should succeed (no sync replication configured)
	_, err = primaryClient.Pooler.ExecuteQuery(ctx, "CREATE TABLE IF NOT EXISTS test_no_repl (id INT PRIMARY KEY)", 0)
	require.NoError(t, err, "Should be able to create table on primary")

	_, err = primaryClient.Pooler.ExecuteQuery(ctx, "INSERT INTO test_no_repl (id) VALUES (1) ON CONFLICT DO NOTHING", 0)
	require.NoError(t, err, "Should be able to insert on primary without replication")

	// Data should NOT appear on standbys (no replication)
	for _, standby := range setup.GetStandbys() {
		standbyClient := setup.NewClient(t, standby.Name)

		// Table might not exist or be empty - either way, data didn't replicate
		resp, err := standbyClient.Pooler.ExecuteQuery(ctx, "SELECT id FROM test_no_repl WHERE id = 1", 1)
		if err == nil {
			assert.Empty(t, resp.Rows, "%s should not have replicated data with WithoutReplication()", standby.Name)
		}
		// If error (table doesn't exist), that's also fine - no replication happened

		standbyClient.Close()
	}

	// Cleanup
	_, _ = primaryClient.Pooler.ExecuteQuery(ctx, "DROP TABLE IF EXISTS test_no_repl", 0)
}

// TestShardSetup_GUCModificationAndReset tests that GUC modifications are properly reset.
// Saves baseline GUCs, modifies them, calls reset, and verifies restoration to baseline.
func TestShardSetup_GUCModificationAndReset(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := t.Context()
	gucNames := []string{"synchronous_standby_names", "synchronous_commit", "primary_conninfo"}

	// Save baseline GUCs for all nodes
	baseline := make(map[string]map[string]string)
	for name, inst := range setup.Multipoolers {
		client, err := NewMultipoolerClient(inst.Multipooler.GrpcPort)
		require.NoError(t, err)
		baseline[name] = SaveGUCs(ctx, client.Pooler, gucNames)
		t.Logf("%s baseline GUCs: %v", name, baseline[name])
		client.Close()
	}

	// Modify GUCs on all nodes
	for name, inst := range setup.Multipoolers {
		client, err := NewMultipoolerClient(inst.Multipooler.GrpcPort)
		require.NoError(t, err)

		if name == setup.PrimaryName {
			_, _ = client.Pooler.ExecuteQuery(ctx, "ALTER SYSTEM SET synchronous_standby_names = 'teststandby'", 0)
			_, _ = client.Pooler.ExecuteQuery(ctx, "ALTER SYSTEM SET synchronous_commit = 'local'", 0)
		} else {
			_, _ = client.Pooler.ExecuteQuery(ctx, "ALTER SYSTEM SET primary_conninfo = 'host=modified_host'", 0)
		}
		_, _ = client.Pooler.ExecuteQuery(ctx, "SELECT pg_reload_conf()", 0)
		client.Close()
		t.Logf("%s GUCs modified", name)
	}

	// Call reset
	t.Log("Calling ResetToCleanState...")
	setup.ResetToCleanState(t)

	// Verify GUCs restored to baseline on all nodes
	for name, inst := range setup.Multipoolers {
		client, err := NewMultipoolerClient(inst.Multipooler.GrpcPort)
		require.NoError(t, err)

		for gucName, expectedValue := range baseline[name] {
			require.Eventually(t, func() bool {
				val, err := QueryStringValue(ctx, client.Pooler, "SHOW "+gucName)
				return err == nil && val == expectedValue
			}, 5*time.Second, 100*time.Millisecond, "%s %s should be reset to %q", name, gucName, expectedValue)
		}
		client.Close()
		t.Logf("%s GUCs restored to baseline", name)
	}

	// Verify clean state validation passes
	err := setup.ValidateCleanState()
	require.NoError(t, err, "ValidateCleanState should pass after reset")
}

// TestShardSetup_WriterValidator validates that the WriterValidator utility works correctly.
// Writes to the primary and verifies all successful writes replicate to standbys.
func TestShardSetup_WriterValidator(t *testing.T) {
	skipIfShort(t)
	setup := getSharedSetup(t)
	setup.SetupTest(t)

	// Connect to multigateway for writes (realistic client path)
	require.NotNil(t, setup.Multigateway, "multigateway should be available in shared setup")
	gatewayConnStr := fmt.Sprintf("host=localhost port=%d user=postgres password=postgres dbname=postgres sslmode=disable connect_timeout=5",
		setup.MultigatewayPgPort)
	gatewayDB, err := sql.Open("postgres", gatewayConnStr)
	require.NoError(t, err)
	defer gatewayDB.Close()

	validator, cleanup, err := NewWriterValidator(t, gatewayDB,
		WithWorkerCount(4),
		WithWriteInterval(10*time.Millisecond),
	)
	require.NoError(t, err)
	t.Cleanup(cleanup)

	// Start writes
	validator.Start(t)

	// Let writes accumulate
	time.Sleep(200 * time.Millisecond)

	// Stop writes
	validator.Stop()

	successful, failed := validator.Stats()
	t.Logf("WriterValidator stats: %d successful, %d failed", successful, failed)
	require.Greater(t, successful, 0, "should have some successful writes")

	// Collect all multipooler test clients for verification
	var poolerClients []*MultiPoolerTestClient

	// Add primary's multipooler client
	primaryInst := setup.GetPrimary(t)
	primaryPoolerClient, err := NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", primaryInst.Multipooler.GrpcPort))
	require.NoError(t, err)
	defer primaryPoolerClient.Close()
	poolerClients = append(poolerClients, primaryPoolerClient)

	// Add standbys' multipooler clients
	for _, standby := range setup.GetStandbys() {
		standbyPoolerClient, err := NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", standby.Multipooler.GrpcPort))
		require.NoError(t, err)
		defer standbyPoolerClient.Close()
		poolerClients = append(poolerClients, standbyPoolerClient)
	}

	// Wait for replication to catch up, then verify
	require.Eventually(t, func() bool {
		err := validator.Verify(t, poolerClients)
		return err == nil
	}, 5*time.Second, 100*time.Millisecond, "all successful writes should be present across poolers")
}
