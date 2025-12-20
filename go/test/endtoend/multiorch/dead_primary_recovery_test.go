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

	// Kill postgres on the primary (multipooler stays running to report unhealthy status)
	t.Logf("Killing postgres on primary node %s to simulate database crash", oldPrimaryName)
	setup.KillPostgres(t, oldPrimaryName)

	// Wait for multiorch to detect failure and elect new primary
	t.Logf("Waiting for multiorch to detect primary failure and elect new leader...")

	newPrimaryName := waitForNewPrimary(t, setup, oldPrimaryName, 10*time.Second)
	require.NotEmpty(t, newPrimaryName, "Expected multiorch to elect new primary automatically")
	t.Logf("New primary elected: %s", newPrimaryName)

	// Verify new primary is functional
	t.Run("verify new primary is functional", func(t *testing.T) {
		newPrimaryInst := setup.GetMultipoolerInstance(newPrimaryName)
		require.NotNil(t, newPrimaryInst, "new primary instance should exist")

		client, err := shardsetup.NewMultipoolerClient(newPrimaryInst.Multipooler.GrpcPort)
		require.NoError(t, err)
		defer client.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		resp, err := client.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
		require.NoError(t, err)
		require.True(t, resp.Status.IsInitialized, "New primary should be initialized")
		require.Equal(t, clustermetadatapb.PoolerType_PRIMARY, resp.Status.PoolerType, "New leader should have PRIMARY pooler type")

		// Verify we can connect and query
		socketDir := filepath.Join(newPrimaryInst.Pgctld.DataDir, "pg_sockets")
		db := connectToPostgres(t, socketDir, newPrimaryInst.Pgctld.PgPort)
		defer db.Close()

		var result int
		err = db.QueryRow("SELECT 1").Scan(&result)
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
		t.Logf("Waiting for new primary election... (sleeping %v)", checkInterval)
		time.Sleep(checkInterval)
	}

	t.Fatalf("Timeout: new primary not elected within %v", timeout)
	return ""
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
