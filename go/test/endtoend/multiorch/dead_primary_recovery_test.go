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
//   - TestMultiOrchLeaderReelection: Verifies multiorch detects primary failure and
//     automatically elects a new leader from remaining standbys.
package multiorch

import (
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/test/utils"
)

// TestDeadPrimaryRecovery tests multiorch's ability to detect a primary failure
// and elect a new primary from the standbys.
//
// This test previously failed due to a multiorch bootstrap coordination race condition.
// Multiple nodes detected "ShardNeedsBootstrap" simultaneously and attempted to bootstrap concurrently.
// The second bootstrap attempt restarted postgres on the primary while the first was still completing,
// creating a second postgres instance that masked the test's intentional SIGKILL.
// This prevented multiorch from detecting the primary failure and electing a new leader.
// The race typically occurred ~7 seconds after initial bootstrap completion.
// Fixed: Distributed locking now prevents concurrent bootstrap attempts.
func TestDeadPrimaryRecovery(t *testing.T) {
	skip, err := utils.ShouldSkipRealPostgres()
	if skip {
		t.Skip("Skipping end-to-end leader reelection test (short mode)")
	}
	require.NoError(t, err, "postgres binaries must be available")

	_, err = exec.LookPath("etcd")
	require.NoError(t, err, "etcd binary must be available in PATH")

	// Setup test environment
	env := setupMultiOrchTestEnv(t, testEnvConfig{
		tempDirPrefix:    "lrtest*",
		cellName:         "test-cell",
		database:         "postgres",
		shardID:          "test-shard-reelect",
		tableGroup:       "test",
		durabilityPolicy: "ANY_2",
		stanzaName:       "reelect-test",
	})

	// Create 3 nodes
	nodes := env.createNodes(3)
	t.Logf("Created 3 empty nodes")

	// Setup pgbackrest
	env.setupPgBackRest()

	// Register all nodes in topology
	env.registerNodes()

	// Start multiorch
	env.startMultiOrch()

	// Wait for initial bootstrap
	t.Logf("Waiting for initial bootstrap...")
	primaryNode := waitForShardPrimary(t, nodes, 60*time.Second)
	require.NotNil(t, primaryNode)
	t.Logf("Initial primary: %s", primaryNode.name)

	// Wait for standbys to complete initialization before killing primary
	t.Logf("Waiting for standbys to complete initialization...")
	waitForStandbysInitialized(t, nodes, primaryNode.name, len(nodes)-1, 60*time.Second)

	// Kill postgres on the primary (multipooler stays running to report unhealthy status)
	t.Logf("Killing postgres on primary node %s to simulate database crash", primaryNode.name)
	killPostgres(t, primaryNode)

	// Wait for multiorch to detect failure and elect new primary
	t.Logf("Waiting for multiorch to detect primary failure and elect new leader...")

	newPrimaryNode := waitForNewPrimaryElected(t, nodes, primaryNode.name, 60*time.Second)
	require.NotNil(t, newPrimaryNode, "Expected multiorch to elect new primary automatically")
	t.Logf("New primary elected: %s", newPrimaryNode.name)

	// Verify new primary is functional
	t.Run("verify new primary is functional", func(t *testing.T) {
		status := checkInitializationStatus(t, newPrimaryNode)
		require.True(t, status.IsInitialized, "New primary should be initialized")
		require.Equal(t, clustermetadatapb.PoolerType_PRIMARY, status.PoolerType, "New leader should have PRIMARY pooler type")

		// Verify we can connect and query
		socketDir := filepath.Join(newPrimaryNode.dataDir, "pg_sockets")
		db := connectToPostgres(t, socketDir, newPrimaryNode.pgPort)
		defer db.Close()

		var result int
		err := db.QueryRow("SELECT 1").Scan(&result)
		require.NoError(t, err, "Should be able to query new primary")
		assert.Equal(t, 1, result)
	})
}

// TestPoolerDownNoFailover verifies that multiorch does NOT trigger a failover when the
// primary's multipooler process is down but Postgres is still running and replicas are
// still connected to it.
//
// This tests the design decision that when only the pooler process crashes (not Postgres),
// the operator should restart the pooler rather than triggering an automatic failover.
// This avoids unnecessary failovers for OOM or process crashes that don't affect the database.
func TestPoolerDownNoFailover(t *testing.T) {
	skip, err := utils.ShouldSkipRealPostgres()
	if skip {
		t.Skip("Skipping end-to-end pooler down test (short mode)")
	}
	require.NoError(t, err, "postgres binaries must be available")

	_, err = exec.LookPath("etcd")
	require.NoError(t, err, "etcd binary must be available in PATH")

	// Setup test environment
	env := setupMultiOrchTestEnv(t, testEnvConfig{
		tempDirPrefix:    "poolerdown*",
		cellName:         "test-cell",
		database:         "postgres",
		shardID:          "test-shard-poolerdown",
		tableGroup:       "test",
		durabilityPolicy: "ANY_2",
		stanzaName:       "poolerdown-test",
	})

	// Create 3 nodes
	nodes := env.createNodes(3)
	t.Logf("Created 3 empty nodes")

	// Setup pgbackrest
	env.setupPgBackRest()

	// Register all nodes in topology
	env.registerNodes()

	// Start multiorch
	env.startMultiOrch()

	// Wait for initial bootstrap
	t.Logf("Waiting for initial bootstrap...")
	primaryNode := waitForShardPrimary(t, nodes, 60*time.Second)
	require.NotNil(t, primaryNode)
	t.Logf("Initial primary: %s", primaryNode.name)

	// Wait for standbys to complete initialization
	t.Logf("Waiting for standbys to complete initialization...")
	waitForStandbysInitialized(t, nodes, primaryNode.name, len(nodes)-1, 60*time.Second)

	// Verify standbys are replicating from the primary
	t.Logf("Verifying standbys are replicating from primary...")
	for _, node := range nodes {
		if node.name == primaryNode.name {
			continue
		}
		status := checkInitializationStatus(t, node)
		require.NotNil(t, status.ReplicationStatus, "Standby %s should have replication status", node.name)
		require.NotNil(t, status.ReplicationStatus.PrimaryConnInfo, "Standby %s should have PrimaryConnInfo", node.name)
		t.Logf("Standby %s is replicating from %s:%d",
			node.name,
			status.ReplicationStatus.PrimaryConnInfo.Host,
			status.ReplicationStatus.PrimaryConnInfo.Port)
	}

	// Kill the multipooler process on the primary (but leave Postgres running)
	t.Logf("Killing multipooler on primary node %s (postgres stays running)", primaryNode.name)
	killMultipooler(t, primaryNode)

	// Wait for multiorch to detect the pooler is down and run several recovery cycles.
	// Tests configure pooler-health-check-interval and recovery-cycle-interval to 500ms,
	// so 3 seconds allows ~6 cycles which is plenty to detect and process the state.
	t.Logf("Waiting for multiorch to process the pooler down state...")
	time.Sleep(3 * time.Second)

	// Verify that NO failover occurred - the original primary should still be the only PRIMARY
	// by checking that standbys are still replicas and connected to the original primary postgres
	t.Run("verify no failover occurred", func(t *testing.T) {
		for _, node := range nodes {
			if node.name == primaryNode.name {
				continue // Skip the primary (its pooler is down)
			}

			status := checkInitializationStatus(t, node)
			require.True(t, status.IsInitialized, "Node %s should be initialized", node.name)

			// Verify node is still a replica, NOT promoted to primary
			require.Equal(t, clustermetadatapb.PoolerType_REPLICA, status.PoolerType,
				"Node %s should still be REPLICA (no failover should have occurred)", node.name)

			// Verify replica is still connected to the original primary postgres
			require.NotNil(t, status.ReplicationStatus, "Standby %s should have replication status", node.name)
			require.NotNil(t, status.ReplicationStatus.PrimaryConnInfo, "Standby %s should have PrimaryConnInfo", node.name)
			t.Logf("Standby %s is still replicating (type=%s), no failover triggered", node.name, status.PoolerType)
		}
	})

	// Verify we can still query postgres on the primary directly (it's still running)
	t.Run("verify primary postgres is still running", func(t *testing.T) {
		socketDir := filepath.Join(primaryNode.dataDir, "pg_sockets")
		db := connectToPostgres(t, socketDir, primaryNode.pgPort)
		defer db.Close()

		var result int
		err := db.QueryRow("SELECT 1").Scan(&result)
		require.NoError(t, err, "Should be able to query primary postgres directly (it's still running)")
		assert.Equal(t, 1, result)
		t.Logf("Primary postgres is still running and queryable")
	})

	// Verify we can query the standbys (they're still replicating)
	t.Run("verify standbys are still queryable", func(t *testing.T) {
		for _, node := range nodes {
			if node.name == primaryNode.name {
				continue
			}
			socketDir := filepath.Join(node.dataDir, "pg_sockets")
			db := connectToPostgres(t, socketDir, node.pgPort)
			defer db.Close()

			var result int
			err := db.QueryRow("SELECT 1").Scan(&result)
			require.NoError(t, err, "Should be able to query standby %s", node.name)
			assert.Equal(t, 1, result)
		}
		t.Logf("All standbys are still queryable")
	})
}

// killMultipooler terminates the multipooler process for a node (simulates pooler crash)
func killMultipooler(t *testing.T, node *nodeInstance) {
	t.Helper()

	if node.multipoolerCmd == nil || node.multipoolerCmd.Process == nil {
		t.Fatalf("Multipooler process not found for node %s", node.name)
	}

	pid := node.multipoolerCmd.Process.Pid
	t.Logf("Killing multipooler (PID %d) on node %s", pid, node.name)

	err := node.multipoolerCmd.Process.Kill()
	require.NoError(t, err, "Failed to kill multipooler process")

	// Wait for the process to actually terminate
	_ = node.multipoolerCmd.Wait()

	t.Logf("Multipooler killed on %s - postgres should still be running", node.name)
}
