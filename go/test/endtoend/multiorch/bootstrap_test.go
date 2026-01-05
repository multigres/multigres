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
// Bootstrap test:
//   - TestBootstrapInitialization: Verifies multiorch automatically detects and bootstraps
//     uninitialized shards without manual intervention, including synchronous replication setup.
package multiorch

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

func TestBootstrapInitialization(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end bootstrap test (short mode)")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end bootstrap test (no postgres binaries)")
	}

	// Create an isolated, uninitialized 3-node cluster using shardsetup
	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithoutInitialization(),
		shardsetup.WithDurabilityPolicy("ANY_2"),
	)
	defer cleanup()

	// Verify nodes are completely uninitialized (no data directory, no postgres running)
	// Multiorch will detect these as needing bootstrap and call InitializeEmptyPrimary
	for name, inst := range setup.Multipoolers {
		func() {
			client, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
			require.NoError(t, err, "should connect to %s", name)
			defer client.Close()

			ctx := utils.WithTimeout(t, 5*time.Second)
			status, err := client.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
			require.NoError(t, err, "should get status from %s", name)

			t.Logf("Node %s Status: IsInitialized=%v, HasDataDirectory=%v, PostgresRunning=%v, PostgresRole=%s, PoolerType=%s",
				name, status.Status.IsInitialized, status.Status.HasDataDirectory,
				status.Status.PostgresRunning, status.Status.PostgresRole, status.Status.PoolerType)

			// Nodes should be completely uninitialized (no data directory at all)
			require.False(t, status.Status.IsInitialized, "Node %s should not be initialized yet", name)
			require.False(t, status.Status.HasDataDirectory, "Node %s should not have data directory yet", name)
			require.False(t, status.Status.PostgresRunning, "Node %s should not have postgres running yet", name)
			t.Logf("Node %s ready for bootstrap (no data directory, postgres not running)", name)
		}()
	}

	// Create and start multiorch to trigger bootstrap
	watchTargets := []string{"postgres/default/0-inf"}
	mo, moCleanup := setup.CreateMultiOrchInstance(t, "test-multiorch", setup.CellName, watchTargets)
	require.NoError(t, mo.Start(t), "should start multiorch")
	t.Cleanup(moCleanup)

	// Wait for multiorch to detect uninitialized shard and bootstrap it automatically
	t.Log("Waiting for multiorch to detect and bootstrap the shard...")
	primaryName := waitForShardPrimary(t, setup, 30*time.Second)
	require.NotEmpty(t, primaryName, "Expected multiorch to bootstrap shard automatically")
	setup.PrimaryName = primaryName

	// Wait for both standbys to initialize and sync replication to be configured
	// (waitForStandbysInitialized includes sync replication check)
	t.Log("Waiting for standbys to complete initialization...")
	waitForStandbysInitialized(t, setup, 2, 30*time.Second)

	// Get primary instance for verification tests
	primary := setup.GetMultipoolerInstance(setup.PrimaryName)
	require.NotNil(t, primary, "Primary instance should exist")

	// Verify bootstrap results
	t.Run("verify primary initialized", func(t *testing.T) {
		t.Logf("Primary node: %s", setup.PrimaryName)

		primaryClient := setup.NewPrimaryClient(t)
		defer primaryClient.Close()

		ctx := t.Context()

		// Verify multigres schema exists
		resp, err := primaryClient.Pooler.ExecuteQuery(ctx,
			"SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = 'multigres')", 1)
		require.NoError(t, err)
		require.Len(t, resp.Rows, 1)
		assert.Equal(t, "t", string(resp.Rows[0].Values[0]), "multigres schema should exist")

		// Verify durability_policy table exists
		resp, err = primaryClient.Pooler.ExecuteQuery(ctx,
			"SELECT EXISTS(SELECT 1 FROM pg_tables WHERE schemaname = 'multigres' AND tablename = 'durability_policy')", 1)
		require.NoError(t, err)
		require.Len(t, resp.Rows, 1)
		assert.Equal(t, "t", string(resp.Rows[0].Values[0]), "durability_policy table should exist")

		// Query the durability policy
		resp, err = primaryClient.Pooler.ExecuteQuery(ctx, `
			SELECT policy_name, policy_version, quorum_rule::text, is_active
			FROM multigres.durability_policy
			WHERE policy_name = 'ANY_2'
		`, 4)
		require.NoError(t, err, "Should find ANY_2 policy")
		require.Len(t, resp.Rows, 1)

		policyName := string(resp.Rows[0].Values[0])
		policyVersion := string(resp.Rows[0].Values[1])
		quorumRuleJSON := string(resp.Rows[0].Values[2])
		isActive := string(resp.Rows[0].Values[3])

		// Verify policy fields
		assert.Equal(t, "ANY_2", policyName)
		assert.Equal(t, "1", policyVersion)
		assert.Equal(t, "t", isActive)

		// Parse and verify JSONB structure
		var quorumRule map[string]any
		err = json.Unmarshal([]byte(quorumRuleJSON), &quorumRule)
		require.NoError(t, err, "Should parse quorum_rule JSON")

		// Verify QuorumType (protojson uses camelCase field names)
		quorumType, ok := quorumRule["quorumType"].(float64)
		require.True(t, ok, "quorumType should be a number")
		assert.Equal(t, float64(clustermetadatapb.QuorumType_QUORUM_TYPE_ANY_N), quorumType)

		// Verify RequiredCount
		requiredCount, ok := quorumRule["requiredCount"].(float64)
		require.True(t, ok, "requiredCount should be a number")
		assert.Equal(t, float64(2), requiredCount)

		// Verify Description
		description, ok := quorumRule["description"].(string)
		require.True(t, ok, "description should be a string")
		assert.Equal(t, "Any 2 nodes must acknowledge", description)

		t.Logf("Verified durability policy: policy_name=%s, quorum_type=ANY_N, required_count=%d",
			policyName, int(requiredCount))
	})

	t.Run("verify standbys initialized", func(t *testing.T) {
		standbyCount := 0
		for name, inst := range setup.Multipoolers {
			if name == setup.PrimaryName {
				continue
			}
			client, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
			require.NoError(t, err)

			ctx := utils.WithTimeout(t, 5*time.Second)
			status, err := client.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
			client.Close()

			require.NoError(t, err)
			if status.Status.IsInitialized && status.Status.PoolerType == clustermetadatapb.PoolerType_REPLICA {
				standbyCount++
				t.Logf("Standby node: %s (pooler_type=%s)", name, status.Status.PoolerType)
			}
		}
		// Should have at least 1 standby
		assert.GreaterOrEqual(t, standbyCount, 1, "Should have at least one standby")
	})

	t.Run("verify multigres internal tables exist", func(t *testing.T) {
		// Verify tables exist on all initialized nodes
		for name, inst := range setup.Multipoolers {
			verifyMultigresTables(t, name, inst.Multipooler.GrpcPort)
		}
	})

	t.Run("verify consensus term", func(t *testing.T) {
		// All initialized nodes should have consensus term = 1
		for name, inst := range setup.Multipoolers {
			client, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
			require.NoError(t, err)

			ctx := utils.WithTimeout(t, 5*time.Second)
			status, err := client.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
			client.Close()

			require.NoError(t, err)
			if status.Status.IsInitialized {
				assert.Equal(t, int64(1), status.Status.ConsensusTerm, "Node %s should have consensus term 1", name)
			}
		}
	})

	t.Run("verify leadership history", func(t *testing.T) {
		primaryClient := setup.NewPrimaryClient(t)
		defer primaryClient.Close()

		ctx := t.Context()

		// Query leadership_history table
		resp, err := primaryClient.Pooler.ExecuteQuery(ctx, `
			SELECT term_number, leader_id, coordinator_id, wal_position, reason,
			       cohort_members, accepted_members
			FROM multigres.leadership_history
			ORDER BY term_number DESC
			LIMIT 1
		`, 7)
		require.NoError(t, err, "should query leadership_history")
		require.Len(t, resp.Rows, 1, "should have exactly one leadership history record")

		row := resp.Rows[0]
		termNumber := string(row.Values[0])
		leaderID := string(row.Values[1])
		coordinatorID := string(row.Values[2])
		walPosition := string(row.Values[3])
		reason := string(row.Values[4])
		cohortMembersJSON := string(row.Values[5])
		acceptedMembersJSON := string(row.Values[6])

		// Verify term_number is 1
		assert.Equal(t, "1", termNumber, "term_number should be 1 for initial bootstrap")

		// Verify leader_id matches primary name (format: cell_name)
		expectedLeaderID := fmt.Sprintf("%s_%s", setup.CellName, setup.PrimaryName)
		assert.Equal(t, expectedLeaderID, leaderID, "leader_id should match primary")

		// Verify coordinator_id matches the multiorch's cell_name format
		// The coordinator ID uses ClusterIDString which returns cell_name format
		expectedCoordinatorID := fmt.Sprintf("%s_test-multiorch", setup.CellName)
		assert.Equal(t, expectedCoordinatorID, coordinatorID, "coordinator_id should match multiorch's cell_name format")

		// Verify WAL position is non-empty
		assert.NotEmpty(t, walPosition, "wal_position should be non-empty")

		// Verify reason is ShardNeedsBootstrap
		assert.Equal(t, "ShardNeedsBootstrap", reason, "reason should be 'ShardNeedsBootstrap'")

		// Parse and verify cohort_members is a valid JSON array
		var cohortMembers []string
		err = json.Unmarshal([]byte(cohortMembersJSON), &cohortMembers)
		require.NoError(t, err, "cohort_members should be valid JSON array")
		assert.Contains(t, cohortMembers, expectedLeaderID, "cohort_members should contain leader")

		// Parse and verify accepted_members is a valid JSON array
		var acceptedMembers []string
		err = json.Unmarshal([]byte(acceptedMembersJSON), &acceptedMembers)
		require.NoError(t, err, "accepted_members should be valid JSON array")
		assert.Contains(t, acceptedMembers, expectedLeaderID, "accepted_members should contain leader")

		t.Logf("Leadership history verified: term=%s, leader=%s, coordinator=%s, reason=%s",
			termNumber, leaderID, coordinatorID, reason)
	})

	t.Run("verify sync replication configured", func(t *testing.T) {
		primaryClient := setup.NewPrimaryClient(t)
		defer primaryClient.Close()

		ctx := t.Context()
		syncStandbyNames, err := shardsetup.QueryStringValue(ctx, primaryClient.Pooler, "SHOW synchronous_standby_names")
		require.NoError(t, err, "should query synchronous_standby_names")

		assert.NotEmpty(t, syncStandbyNames,
			"synchronous_standby_names should be configured for ANY_2 policy")
		t.Logf("synchronous_standby_names = %s", syncStandbyNames)

		// Verify it contains ANY keyword (for ANY_2 policy)
		assert.Contains(t, strings.ToUpper(syncStandbyNames), "ANY",
			"should use ANY method for ANY_2 policy")
	})

	t.Run("verify auto-restore on startup", func(t *testing.T) {
		// Find an initialized standby
		var standbyName string
		var standbyInst *shardsetup.MultipoolerInstance
		for name, inst := range setup.Multipoolers {
			if name == setup.PrimaryName {
				continue
			}
			client, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
			require.NoError(t, err)

			ctx := utils.WithTimeout(t, 5*time.Second)
			status, err := client.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
			client.Close()

			if err == nil && status.Status.IsInitialized && status.Status.PoolerType == clustermetadatapb.PoolerType_REPLICA {
				standbyName = name
				standbyInst = inst
				break
			}
		}
		require.NotEmpty(t, standbyName, "Should have at least one initialized standby")
		t.Logf("Selected standby for auto-restore test: %s", standbyName)

		// Stop multipooler
		standbyInst.Multipooler.Stop()

		// Stop postgres via pgctld
		standbyInst.Pgctld.StopPostgres(t)

		// Remove pg_data directory to simulate complete data loss
		pgDataDir := filepath.Join(standbyInst.Pgctld.DataDir, "pg_data")
		err := os.RemoveAll(pgDataDir)
		require.NoError(t, err, "Should remove pg_data directory")
		t.Logf("Removed pg_data directory: %s", pgDataDir)

		// Restart multipooler (should auto-restore from backup)
		require.NoError(t, standbyInst.Multipooler.Start(t), "Should restart multipooler")
		t.Logf("Restarted multipooler for %s (should auto-restore)", standbyName)

		// Wait for multipooler to be ready
		shardsetup.WaitForManagerReady(t, standbyInst.Multipooler)

		// Wait for auto-restore to complete
		require.Eventually(t, func() bool {
			client, err := shardsetup.NewMultipoolerClient(standbyInst.Multipooler.GrpcPort)
			if err != nil {
				return false
			}
			defer client.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			status, err := client.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
			if err != nil {
				return false
			}
			return status.Status.IsInitialized && status.Status.PostgresRunning
		}, 90*time.Second, 1*time.Second, "Auto-restore should complete within timeout")

		// Verify final state
		client, err := shardsetup.NewMultipoolerClient(standbyInst.Multipooler.GrpcPort)
		require.NoError(t, err)
		defer client.Close()

		ctx := utils.WithTimeout(t, 5*time.Second)
		status, err := client.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
		require.NoError(t, err)

		assert.True(t, status.Status.IsInitialized, "Standby should be initialized after auto-restore")
		assert.True(t, status.Status.HasDataDirectory, "Standby should have data directory after auto-restore")
		assert.True(t, status.Status.PostgresRunning, "PostgreSQL should be running after auto-restore")
		assert.True(t, status.Status.ConsensusTerm > 0, "Consensus term should be greater than 0 after auto-restore")
		assert.Equal(t, "standby", status.Status.PostgresRole, "Should be in standby role after auto-restore")

		t.Logf("Auto-restore succeeded: IsInitialized=%v, HasDataDirectory=%v, PostgresRunning=%v, Role=%s",
			status.Status.IsInitialized, status.Status.HasDataDirectory,
			status.Status.PostgresRunning, status.Status.PostgresRole)
	})
}

// verifyMultigresTables checks that multigres internal tables exist on an initialized node.
// Skips uninitialized nodes.
func verifyMultigresTables(t *testing.T, name string, grpcPort int) {
	t.Helper()

	client, err := shardsetup.NewMultipoolerClient(grpcPort)
	if err != nil {
		return
	}
	defer client.Close()

	ctx := utils.WithShortDeadline(t)

	status, err := client.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
	if err != nil || !status.Status.IsInitialized {
		return
	}

	// Check heartbeat table exists
	resp, err := client.Pooler.ExecuteQuery(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.tables
			WHERE table_schema = 'multigres' AND table_name = 'heartbeat'
		)
	`, 1)
	if err != nil {
		t.Errorf("Failed to query heartbeat table on %s: %v", name, err)
		return
	}
	if string(resp.Rows[0].Values[0]) != "t" {
		t.Errorf("Heartbeat table should exist on %s", name)
	}

	// Check durability_policy table exists
	resp, err = client.Pooler.ExecuteQuery(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.tables
			WHERE table_schema = 'multigres' AND table_name = 'durability_policy'
		)
	`, 1)
	if err != nil {
		t.Errorf("Failed to query durability_policy table on %s: %v", name, err)
		return
	}
	if string(resp.Rows[0].Values[0]) != "t" {
		t.Errorf("Durability policy table should exist on %s", name)
	}

	t.Logf("Verified multigres tables exist on %s (pooler_type=%s)", name, status.Status.PoolerType)
}
