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
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
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

	// Configure replication on all standbys
	setup.SetupTest(t, shardsetup.WithoutCleanup())

	// Get the primary
	primary := setup.GetPrimary(t)
	require.NotNil(t, primary, "primary instance should exist")
	t.Logf("Initial primary: %s", primary.Name)

	// Kill postgres on the primary (multipooler stays running to report unhealthy status)
	t.Logf("Killing postgres on primary node %s to simulate database crash", primary.Name)
	setup.KillPostgres(t, primary.Name)

	// Wait for multiorch to detect failure and elect new primary
	t.Logf("Waiting for multiorch to detect primary failure and elect new leader...")

	newPrimaryName := waitForNewPrimary(t, setup, primary.Name, 10*time.Second)
	require.NotEmpty(t, newPrimaryName, "Expected multiorch to elect new primary automatically")
	t.Logf("New primary elected: %s", newPrimaryName)

	// Verify new primary is functional
	t.Run("verify new primary is functional", func(t *testing.T) {
		newPrimaryInst := setup.GetMultipoolerInstance(newPrimaryName)
		require.NotNil(t, newPrimaryInst, "new primary instance should exist")

		status := checkInitializationStatus(t, &nodeInstance{
			name:           newPrimaryName,
			grpcPort:       newPrimaryInst.Multipooler.GrpcPort,
			pgctldGrpcPort: newPrimaryInst.Pgctld.GrpcPort,
		})
		require.True(t, status.IsInitialized, "New primary should be initialized")
		require.Equal(t, clustermetadatapb.PoolerType_PRIMARY, status.PoolerType, "New leader should have PRIMARY pooler type")

		// Verify we can connect and query
		socketDir := filepath.Join(newPrimaryInst.Pgctld.DataDir, "pg_sockets")
		db := connectToPostgres(t, socketDir, newPrimaryInst.Pgctld.PgPort)
		defer db.Close()

		var result int
		err := db.QueryRow("SELECT 1").Scan(&result)
		require.NoError(t, err, "Should be able to query new primary")
		assert.Equal(t, 1, result)
	})
}

// waitForNewPrimary waits for a new primary (different from oldPrimaryName) to be elected.
func waitForNewPrimary(t *testing.T, setup *shardsetup.ShardSetup, oldPrimaryName string, timeout time.Duration) string {
	t.Helper()

	deadline := time.Now().Add(timeout)
	checkInterval := 2 * time.Second

	for time.Now().Before(deadline) {
		for name, inst := range setup.Multipoolers {
			if name == oldPrimaryName {
				continue
			}

			status := checkInitializationStatus(t, &nodeInstance{
				name:           name,
				grpcPort:       inst.Multipooler.GrpcPort,
				pgctldGrpcPort: inst.Pgctld.GrpcPort,
			})

			if status.IsInitialized && status.PoolerType == clustermetadatapb.PoolerType_PRIMARY {
				t.Logf("New primary elected: %s (pooler_type=%s)", name, status.PoolerType)
				return name
			}
		}
		t.Logf("Waiting for new primary election... (sleeping %v)", checkInterval)
		time.Sleep(checkInterval)
	}

	t.Fatalf("Timeout: new primary not elected within %v", timeout)
	return ""
}

// TestPoolerDownNoFailover verifies that multiorch does NOT trigger a failover when the
// primary's multipooler process is down but Postgres is still running and replicas are
// still connected to it.
//
// This tests the design decision that when only the pooler process crashes (not Postgres),
// the operator should restart the pooler rather than triggering an automatic failover.
// This avoids unnecessary failovers for OOM or process crashes that don't affect the database.
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

	// Configure replication on all standbys
	setup.SetupTest(t, shardsetup.WithoutCleanup())
	// Get the primary
	primary := setup.GetMultipoolerInstance(setup.PrimaryName)
	require.NotNil(t, primary, "primary instance should exist")
	t.Logf("Initial primary: %s", setup.PrimaryName)

	// Kill the multipooler process on the primary (but leave Postgres running)
	t.Logf("Killing multipooler on primary node %s (postgres stays running)", primary.Name)
	killMultipooler(t, primary)

	// Wait for multiorch to detect the pooler is down and run several recovery cycles.
	// Tests configure pooler-health-check-interval and recovery-cycle-interval to 500ms,
	// so 3 seconds allows ~6 cycles which is plenty to detect and process the state.
	t.Logf("Waiting for multiorch to process the pooler down state...")
	time.Sleep(3 * time.Second)

	// Verify that NO failover occurred - the original primary should still be the only PRIMARY
	// by checking that standbys are still replicas and connected to the original primary postgres
	t.Run("verify no failover occurred", func(t *testing.T) {
		for name, inst := range setup.Multipoolers {
			if name == setup.PrimaryName {
				continue // Skip the primary (its pooler is down)
			}

			status := checkInitializationStatus(t, &nodeInstance{
				name:           name,
				grpcPort:       inst.Multipooler.GrpcPort,
				pgctldGrpcPort: inst.Pgctld.GrpcPort,
			})
			require.True(t, status.IsInitialized, "Node %s should be initialized", name)

			// Verify node is still a replica, NOT promoted to primary
			require.Equal(t, clustermetadatapb.PoolerType_REPLICA, status.PoolerType,
				"Node %s should still be REPLICA (no failover should have occurred)", name)

			// Verify replica is still connected to the original primary postgres
			require.NotNil(t, status.ReplicationStatus, "Standby %s should have replication status", name)
			require.NotNil(t, status.ReplicationStatus.PrimaryConnInfo, "Standby %s should have PrimaryConnInfo", name)
			t.Logf("Standby %s is still replicating (type=%s), no failover triggered", name, status.PoolerType)
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
