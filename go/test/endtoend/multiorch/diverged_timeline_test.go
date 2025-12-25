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
	"github.com/multigres/multigres/go/test/endtoend"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestDivergedTimelineRepair tests multiorch's ability to detect a diverged timeline
// after failover and repair it using pg_rewind.
//
// Scenario:
// 1. 3-node cluster: primary (P1) + 2 standbys (S1, S2) with ANY_2 durability
// 2. Kill P1's postgres to trigger failover
// 3. Wait for multiorch to elect new primary (S1 becomes P2)
// 4. Write data to new primary to ensure timeline has diverged
// 5. Restart old P1's postgres (it comes back as a primary with diverged WAL)
// 6. Multiorch should detect split-brain and demote old primary
// 7. When configuring replication, multiorch should detect timeline divergence
// 8. Multiorch should run pg_rewind to repair P1
// 9. Verify P1 rejoins as a replica of P2
//
// NOTE: This test is currently skipped because multiorch does not yet handle the
// split-brain scenario where the old primary comes back online as a PRIMARY.
// Timeline divergence detection and pg_rewind support are now implemented, but
// multiorch needs a split-brain analyzer to detect and demote the stale primary.
func TestDivergedTimelineRepair(t *testing.T) {
	t.Skip("Skipping: Split-brain resolution not yet implemented - old primary comes back as PRIMARY and multiorch doesn't demote it")

	if testing.Short() {
		t.Skip("skipping TestDivergedTimelineRepair test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end diverged timeline test (no postgres binaries)")
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

	// Step 4: Restart old primary's postgres
	t.Log("Restarting old primary's postgres...")
	restartPostgres(t, oldPrimary)

	// Step 5: Wait for multiorch to detect and repair divergence
	// This may take longer because multiorch needs to:
	// 1. Detect the split-brain (two primaries)
	// 2. Demote the old primary
	// 3. Detect timeline divergence when configuring replication
	// 4. Run pg_rewind to repair
	// 5. Configure replication and start WAL receiver
	t.Log("Waiting for multiorch to repair diverged timeline...")
	waitForDivergenceRepaired(t, setup, oldPrimaryName, newPrimaryName, 120*time.Second)

	// Step 6: Verify old primary is now replicating from new primary
	t.Log("Verifying old primary is now a replica...")
	verifyReplicaReplicating(t, setup, oldPrimaryName, newPrimaryName)

	t.Log("TestDivergedTimelineRepair completed successfully")
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

// restartPostgres restarts postgres on a node via pgctld
func restartPostgres(t *testing.T, node *shardsetup.MultipoolerInstance) {
	t.Helper()

	grpcAddr := fmt.Sprintf("localhost:%d", node.Pgctld.GrpcPort)
	err := endtoend.StartPostgreSQL(t, grpcAddr)
	require.NoError(t, err, "should start postgres via pgctld")

	// Wait for postgres to be ready
	socketDir := filepath.Join(node.Pgctld.DataDir, "pg_sockets")
	require.Eventually(t, func() bool {
		db := connectToPostgres(t, socketDir, node.Pgctld.PgPort)
		if db == nil {
			return false
		}
		defer db.Close()
		var result int
		err := db.QueryRow("SELECT 1").Scan(&result)
		return err == nil
	}, 30*time.Second, 1*time.Second, "postgres should start")

	t.Logf("Postgres restarted successfully on %s", node.Name)
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Manager.StandbyReplicationStatus(ctx, &multipoolermanagerdatapb.StandbyReplicationStatusRequest{})
	require.NoError(t, err, "should get replication status")
	require.NotNil(t, resp.Status, "status should not be nil")
	require.NotNil(t, resp.Status.PrimaryConnInfo, "should have primary_conninfo")
	require.NotEmpty(t, resp.Status.LastReceiveLsn, "should be receiving WAL")

	t.Logf("Replica %s is streaming from %s:%d, last_receive_lsn=%s",
		replicaName,
		resp.Status.PrimaryConnInfo.Host,
		resp.Status.PrimaryConnInfo.Port,
		resp.Status.LastReceiveLsn)
}
