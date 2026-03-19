// Copyright 2026 Supabase, Inc.
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
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestQuorumLoss kills both standbys to lose quorum (AT_LEAST_2 → NumSync=1),
// verifies sync commits block, then confirms writes resume after recovery.
func TestQuorumLoss(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestQuorumLoss in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end quorum loss test (short mode or no postgres binaries)")
	}

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

	primary := setup.GetPrimary(t)
	require.NotNil(t, primary, "primary instance should exist")
	primaryName := setup.PrimaryName
	t.Logf("Initial primary: %s", primaryName)

	// Disable monitoring so multiorch orchestrates recovery
	disableMonitoringOnAllNodes(t, setup)

	var standbyNames []string
	for name := range setup.Multipoolers {
		if name != primaryName {
			standbyNames = append(standbyNames, name)
		}
	}
	require.Len(t, standbyNames, 2, "should have exactly 2 standbys")
	t.Logf("Standbys: %v", standbyNames)

	// Start continuous writes so we can verify durability later
	connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")
	gatewayDB, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	defer gatewayDB.Close()
	require.NoError(t, gatewayDB.Ping(), "failed to ping multigateway")

	validator, validatorCleanup, err := shardsetup.NewWriterValidator(t, gatewayDB,
		shardsetup.WithWorkerCount(4),
		shardsetup.WithWriteInterval(10*time.Millisecond),
	)
	require.NoError(t, err)
	t.Cleanup(validatorCleanup)

	t.Logf("Starting continuous writes via multigateway (table: %s)...", validator.TableName())
	validator.Start(t)
	time.Sleep(200 * time.Millisecond)

	preKillSuccess, preKillFailed := validator.Stats()
	t.Logf("Pre-kill writes: %d successful, %d failed", preKillSuccess, preKillFailed)
	require.Greater(t, preKillSuccess, int64(0), "should have some successful writes before killing standbys")

	// Kill both standbys to break quorum
	t.Logf("Killing postgres on both standbys: %s, %s", standbyNames[0], standbyNames[1])
	setup.KillPostgres(t, standbyNames[0])
	setup.KillPostgres(t, standbyNames[1])

	// Give the primary time to detect the standby disconnections
	time.Sleep(2 * time.Second)

	t.Run("verify writes blocked during quorum loss", func(t *testing.T) {
		primaryInst := setup.GetMultipoolerInstance(primaryName)
		require.NotNil(t, primaryInst)
		socketDir := filepath.Join(primaryInst.Pgctld.DataDir, "pg_sockets")
		directDB := connectToPostgres(t, socketDir, primaryInst.Pgctld.PgPort)
		defer directDB.Close()

		// Set a short statement_timeout so the sync commit doesn't hang forever
		_, err := directDB.Exec("SET statement_timeout = '2s'")
		require.NoError(t, err)

		// DDL may slip through, but the INSERT needs a sync standby ack to commit.
		_, err = directDB.Exec("CREATE TABLE IF NOT EXISTS quorum_loss_test (id serial PRIMARY KEY, val text)")
		if err == nil {
			_, err = directDB.Exec("INSERT INTO quorum_loss_test (val) VALUES ('should_block')")
		}
		require.Error(t, err, "write should timeout with no sync standby to ack")
		t.Logf("Write correctly blocked during quorum loss: %v", err)
	})

	// Let multiorch recover at least one standby to restore quorum
	t.Logf("Waiting for multiorch to recover at least one standby...")
	// Re-enable monitoring so multiorch can restart the killed postgres instances
	for name, inst := range setup.Multipoolers {
		if name == primaryName {
			continue
		}
		client, clientErr := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
		if clientErr != nil {
			t.Logf("Cannot connect to %s yet (expected): %v", name, clientErr)
			continue
		}
		_, _ = client.Manager.SetMonitor(t.Context(), &multipoolermanagerdatapb.SetMonitorRequest{Enabled: true})
		client.Close()
	}

	// Wait for at least one standby to rejoin as streaming replica
	var recoveredStandby string
	require.Eventually(t, func() bool {
		for _, name := range standbyNames {
			inst := setup.GetMultipoolerInstance(name)
			client, clientErr := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
			if clientErr != nil {
				continue
			}
			resp, statusErr := client.Manager.Status(utils.WithTimeout(t, 5*time.Second),
				&multipoolermanagerdatapb.StatusRequest{})
			client.Close()
			if statusErr != nil {
				continue
			}
			if resp.Status.PostgresRunning &&
				resp.Status.PoolerType == clustermetadatapb.PoolerType_REPLICA &&
				resp.Status.ReplicationStatus != nil &&
				resp.Status.ReplicationStatus.WalReceiverStatus == "streaming" {
				recoveredStandby = name
				return true
			}
		}
		return false
	}, 30*time.Second, 1*time.Second, "at least one standby should recover and rejoin within 30s")
	t.Logf("Standby %s recovered and streaming", recoveredStandby)

	// Once a standby is back, sync commits should work again
	t.Run("verify writes resume after quorum restored", func(t *testing.T) {
		primaryInst := setup.GetMultipoolerInstance(primaryName)
		require.NotNil(t, primaryInst)
		socketDir := filepath.Join(primaryInst.Pgctld.DataDir, "pg_sockets")
		directDB := connectToPostgres(t, socketDir, primaryInst.Pgctld.PgPort)
		defer directDB.Close()

		_, err := directDB.Exec("SET statement_timeout = '5s'")
		require.NoError(t, err)

		_, err = directDB.Exec("CREATE TABLE IF NOT EXISTS quorum_loss_test (id serial PRIMARY KEY, val text)")
		require.NoError(t, err, "DDL should succeed after quorum restored")

		_, err = directDB.Exec("INSERT INTO quorum_loss_test (val) VALUES ('quorum_restored')")
		require.NoError(t, err, "sync commit should succeed after standby rejoined")
		t.Logf("Write succeeded after quorum restored")
	})

	// Stop the continuous writer
	validator.Stop()
	successfulWrites, failedWrites := validator.Stats()
	t.Logf("Final writes: %d successful, %d failed", successfulWrites, failedWrites)

	// Wait for the full cluster to recover, then check data integrity
	t.Logf("Waiting for all multipoolers to be healthy...")
	var allInstances []*shardsetup.MultipoolerInstance
	for _, inst := range setup.Multipoolers {
		allInstances = append(allInstances, inst)
	}
	shardsetup.EventuallyPoolerCondition(t, allInstances, 30*time.Second, 500*time.Millisecond,
		func(name string, s *multipoolermanagerdatapb.Status) (bool, string) {
			if !s.PostgresRunning {
				return false, "postgres not running"
			}
			if s.PoolerType == clustermetadatapb.PoolerType_REPLICA {
				if s.ReplicationStatus == nil || s.ReplicationStatus.PrimaryConnInfo == nil {
					return false, "replication not configured"
				}
			}
			return true, ""
		},
		"all multipoolers should be healthy",
	)

	// Verify exactly one primary after recovery
	t.Run("verify exactly one primary", func(t *testing.T) {
		primaryCount := 0
		for name, inst := range setup.Multipoolers {
			client, clientErr := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
			require.NoError(t, clientErr, "should connect to %s", name)
			resp, statusErr := client.Manager.Status(utils.WithTimeout(t, 5*time.Second),
				&multipoolermanagerdatapb.StatusRequest{})
			client.Close()
			require.NoError(t, statusErr)
			if resp.Status.PoolerType == clustermetadatapb.PoolerType_PRIMARY {
				primaryCount++
			}
		}
		assert.Equal(t, 1, primaryCount, "exactly one primary should exist after recovery")
	})

	// Verify data consistency via md5 checksum
	t.Run("verify data consistency across all multipoolers", func(t *testing.T) {
		// Get primary LSN and wait for replicas to catch up
		primaryInst := setup.GetMultipoolerInstance(primaryName)
		require.NotNil(t, primaryInst)

		primaryClient, clientErr := shardsetup.NewMultipoolerClient(primaryInst.Multipooler.GrpcPort)
		require.NoError(t, clientErr)
		defer primaryClient.Close()

		primaryPosResp, posErr := primaryClient.Manager.PrimaryPosition(
			utils.WithShortDeadline(t), &multipoolermanagerdatapb.PrimaryPositionRequest{})
		require.NoError(t, posErr)
		primaryLSN := primaryPosResp.LsnPosition

		for name, inst := range setup.Multipoolers {
			if name == primaryName {
				continue
			}
			client, clientErr := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
			require.NoError(t, clientErr)
			_, waitErr := client.Manager.WaitForLSN(utils.WithTimeout(t, 5*time.Second),
				&multipoolermanagerdatapb.WaitForLSNRequest{TargetLsn: primaryLSN})
			require.NoError(t, waitErr, "replica %s should catch up to primary LSN", name)
			client.Close()
		}

		// Collect pooler clients for verification
		var poolerClients []*shardsetup.MultiPoolerTestClient
		for _, inst := range setup.Multipoolers {
			client, clientErr := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
			require.NoError(t, clientErr)
			poolerClients = append(poolerClients, client.Pooler)
		}
		defer func() {
			for _, c := range poolerClients {
				c.Close()
			}
		}()

		require.Len(t, poolerClients, 3, "should have 3 multipooler clients")
		verifyErr := validator.Verify(t, poolerClients)
		require.NoError(t, verifyErr, "all successful writes should be present on all multipoolers")
		t.Logf("Data consistency verified: %d successful writes present on all multipoolers", successfulWrites)
	})

	// Verify md5 checksum across all nodes
	t.Run("verify md5 checksum consistency", func(t *testing.T) {
		validatorTableName := validator.TableName()
		primaryInst := setup.GetMultipoolerInstance(primaryName)
		require.NotNil(t, primaryInst)

		primarySocketDir := filepath.Join(primaryInst.Pgctld.DataDir, "pg_sockets")
		primaryDB := connectToPostgres(t, primarySocketDir, primaryInst.Pgctld.PgPort)
		defer primaryDB.Close()

		var primaryRowCount int
		countQuery := "SELECT COUNT(*) FROM " + validatorTableName
		require.NoError(t, primaryDB.QueryRow(countQuery).Scan(&primaryRowCount))

		var primaryChecksum string
		checksumQuery := "SELECT md5(string_agg(id::text, '' ORDER BY id)) FROM " + validatorTableName
		require.NoError(t, primaryDB.QueryRow(checksumQuery).Scan(&primaryChecksum))

		for name, inst := range setup.Multipoolers {
			if name == primaryName {
				continue
			}
			verifyStandbyDataConsistency(t, name, inst, countQuery, checksumQuery, primaryRowCount, primaryChecksum)
		}

		t.Logf("md5 checksum verified: all 3 nodes have %d rows, checksum %s", primaryRowCount, primaryChecksum)
	})
}
