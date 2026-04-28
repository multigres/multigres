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

package multipooler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// TestReplicationAPIs tests the replication-related API functionality (SetPrimaryConnInfo, WaitForLSN, etc.)
func TestReplicationAPIs(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)

	// Wait for both managers to be ready before running tests
	waitForManagerReady(t, setup, setup.PrimaryMultipooler)
	waitForManagerReady(t, setup, setup.StandbyMultipooler)

	// Create shared clients for all subtests
	primaryClient, err := shardsetup.NewMultipoolerClient(setup.PrimaryMultipooler.GrpcPort)
	require.NoError(t, err)
	t.Cleanup(func() { primaryClient.Close() })
	primaryManagerClient := primaryClient.Manager
	primaryPoolerClient := primaryClient.Pooler

	standbyClient, err := shardsetup.NewMultipoolerClient(setup.StandbyMultipooler.GrpcPort)
	require.NoError(t, err)
	t.Cleanup(func() { standbyClient.Close() })
	standbyManagerClient := standbyClient.Manager
	standbyPoolerClient := standbyClient.Pooler

	t.Run("ConfigureReplicationAndValidate", func(t *testing.T) {
		setupPoolerTest(t, setup, WithoutReplication(), WithDropTables("test_replication"))

		t.Log("Creating table and inserting data in primary...")
		_, err = primaryPoolerClient.ExecuteQuery(context.Background(), "CREATE TABLE IF NOT EXISTS test_replication (id SERIAL PRIMARY KEY, data TEXT)", 0)
		require.NoError(t, err, "Should be able to create table in primary")

		_, err = primaryPoolerClient.ExecuteQuery(context.Background(), "INSERT INTO test_replication (data) VALUES ('test data')", 0)
		require.NoError(t, err, "Should be able to insert data in primary")

		// Get LSN from primary using consensus Status RPC
		ctx := utils.WithTimeout(t, 1*time.Second)

		primaryPosResp, err := primaryClient.Consensus.Status(ctx, &consensusdatapb.StatusRequest{})
		require.NoError(t, err)
		primaryLSN := primaryPosResp.GetConsensusStatus().GetCurrentPosition().GetLsn()
		t.Logf("Primary LSN after insert: %s", primaryLSN)

		// Validate data is NOT in standby yet (no replication configured)
		t.Log("Validating data is NOT in standby (replication not configured)...")

		// Use WaitForLSN to verify standby cannot reach primary's LSN without replication
		// This should timeout since replication is not configured
		ctx = utils.WithTimeout(t, 1*time.Second)

		waitReq := &multipoolermanagerdatapb.WaitForLSNRequest{
			TargetLsn: primaryLSN,
		}
		_, err = standbyManagerClient.WaitForLSN(ctx, waitReq)
		require.Error(t, err, "WaitForLSN should timeout without replication configured")
		// Check that the error is a timeout (gRPC code DEADLINE_EXCEEDED or CANCELED)
		st, ok := status.FromError(err)
		require.True(t, ok, "Error should be a gRPC status error")
		assert.Contains(t, []string{"DeadlineExceeded", "Canceled"}, st.Code().String(),
			"Error should be a timeout error code, got: %s", st.Code().String())
		t.Log("Confirmed: standby cannot reach primary LSN without replication configured")

		// Verify table doesn't exist in standby
		queryResp, err := standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT COUNT(*) FROM pg_tables WHERE tablename = 'test_replication'", 1)
		require.NoError(t, err)
		require.Len(t, queryResp.Rows, 1)
		tableCount := string(queryResp.Rows[0].Values[0])
		assert.Equal(t, "0", tableCount, "Table should not exist in standby yet")

		// Configure replication using SetPrimaryConnInfo RPC
		t.Log("Configuring replication via SetPrimaryConnInfo RPC...")

		ctx = utils.WithTimeout(t, 1*time.Second)

		// Call SetPrimaryConnInfo with StartReplicationAfter=true
		// Use Force=true since we're testing replication functionality, not term validation
		primary := &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "test-cell",
				Name:      setup.PrimaryMultipooler.Name,
			},
			Hostname: "localhost",
			PortMap:  map[string]int32{"postgres": int32(setup.PrimaryPgctld.PgPort)},
		}
		setPrimaryReq := &multipoolermanagerdatapb.SetPrimaryConnInfoRequest{
			Primary:               primary,
			StartReplicationAfter: true,
			StopReplicationBefore: false,
			CurrentTerm:           0, // Ignored when Force=true
			Force:                 true,
		}
		_, err = standbyClient.Consensus.SetPrimaryConnInfo(ctx, setPrimaryReq)
		require.NoError(t, err, "SetPrimaryConnInfo should succeed")
		t.Log("Replication configured successfully")

		// Wait for standby to catch up to primary's LSN using WaitForLSN API
		t.Logf("Waiting for standby to catch up to primary LSN: %s", primaryLSN)

		ctx = utils.WithTimeout(t, 1*time.Second)

		waitReq = &multipoolermanagerdatapb.WaitForLSNRequest{
			TargetLsn: primaryLSN,
		}
		_, err = standbyManagerClient.WaitForLSN(ctx, waitReq)
		require.NoError(t, err, "Standby should catch up to primary LSN after replication is configured")

		// Verify the table now exists in standby
		require.Eventually(t, func() bool {
			resp, err := standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT COUNT(*) FROM pg_tables WHERE tablename = 'test_replication'", 1)
			if err != nil || len(resp.Rows) == 0 {
				return false
			}
			tableCount := string(resp.Rows[0].Values[0])
			return tableCount == "1"
		}, 15*time.Second, 500*time.Millisecond, "Table should exist in standby after replication")

		// Verify the data replicated
		dataResp, err := standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT COUNT(*) FROM test_replication", 1)
		require.NoError(t, err)
		require.Len(t, dataResp.Rows, 1)
		rowCount := string(dataResp.Rows[0].Values[0])
		assert.Equal(t, "1", rowCount, "Should have 1 row in standby")

		t.Log("Replication is working! Data successfully replicated from primary to standby")
	})

	t.Run("TermMismatchRejected", func(t *testing.T) {
		setupPoolerTest(t, setup)

		ctx := utils.WithTimeout(t, 1*time.Second)

		// Try to set primary conn info with stale term (current term is 1, we'll try with 0)
		primary := &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "test-cell",
				Name:      setup.PrimaryMultipooler.Name,
			},
			Hostname: "localhost",
			PortMap:  map[string]int32{"postgres": int32(setup.PrimaryPgctld.PgPort)},
		}
		setPrimaryReq := &multipoolermanagerdatapb.SetPrimaryConnInfoRequest{
			Primary:               primary,
			StartReplicationAfter: true,
			StopReplicationBefore: false,
			CurrentTerm:           0, // Stale term (lower than current term 1)
			Force:                 false,
		}
		_, err = standbyClient.Consensus.SetPrimaryConnInfo(ctx, setPrimaryReq)
		require.Error(t, err, "SetPrimaryConnInfo should fail with stale term")
		assert.Contains(t, err.Error(), "consensus term too old", "Error should mention term is too old")

		// Try again with force=true, should succeed
		setPrimaryReq.Force = true
		_, err = standbyClient.Consensus.SetPrimaryConnInfo(ctx, setPrimaryReq)
		require.NoError(t, err, "SetPrimaryConnInfo should succeed with force=true")
	})

	t.Run("StopReplicationBeforeFlag", func(t *testing.T) {
		// This test verifies that StopReplicationBefore flag stops replication before setting primary_conninfo

		// Setup cleanup - default behavior starts replication
		setupPoolerTest(t, setup)

		// First ensure replication is running by checking pg_stat_wal_receiver
		t.Log("Verifying replication is running...")
		queryResp, err := standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT status FROM pg_stat_wal_receiver", 1)
		require.NoError(t, err)
		require.Len(t, queryResp.Rows, 1)
		initialStatus := string(queryResp.Rows[0].Values[0])
		t.Logf("Initial WAL receiver status: %s", initialStatus)
		assert.Equal(t, "streaming", initialStatus, "Replication should be streaming")

		// Check if WAL replay is not paused
		queryResp, err = standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT pg_is_wal_replay_paused()", 1)
		require.NoError(t, err)
		require.Len(t, queryResp.Rows, 1)
		isPaused := string(queryResp.Rows[0].Values[0])
		// PostgreSQL wire protocol returns boolean as 't' or 'f' in text format
		assert.Equal(t, "f", isPaused, "WAL replay should not be paused initially")

		// Get current term
		consensusStatus, err := standbyClient.Consensus.Status(utils.WithShortDeadline(t), &consensusdatapb.StatusRequest{})
		require.NoError(t, err)
		currentTerm := consensusStatus.GetConsensusStatus().GetTermRevocation().GetRevokedBelowTerm()

		// Call SetPrimaryConnInfo with StopReplicationBefore=true and StartReplicationAfter=false
		ctx := utils.WithTimeout(t, 1*time.Second)

		t.Log("Calling SetPrimaryConnInfo with StopReplicationBefore=true, StartReplicationAfter=false...")
		primary := &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "test-cell",
				Name:      setup.PrimaryMultipooler.Name,
			},
			Hostname: "localhost",
			PortMap:  map[string]int32{"postgres": int32(setup.PrimaryPgctld.PgPort)},
		}
		setPrimaryReq := &multipoolermanagerdatapb.SetPrimaryConnInfoRequest{
			Primary:               primary,
			StartReplicationAfter: false, // Don't start after
			StopReplicationBefore: true,  // Stop before
			CurrentTerm:           currentTerm,
			Force:                 false,
		}
		_, err = standbyClient.Consensus.SetPrimaryConnInfo(ctx, setPrimaryReq)
		require.NoError(t, err, "SetPrimaryConnInfo should succeed")

		// Verify that WAL replay is now paused
		t.Log("Verifying replication is paused after StopReplicationBefore...")
		queryResp, err = standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT pg_is_wal_replay_paused()", 1)
		require.NoError(t, err)
		require.Len(t, queryResp.Rows, 1)
		isPaused = string(queryResp.Rows[0].Values[0])
		// PostgreSQL wire protocol returns boolean as 't' or 'f' in text format
		assert.Equal(t, "t", isPaused, "WAL replay should be paused after StopReplicationBefore=true")

		t.Log("Replication successfully stopped with StopReplicationBefore flag")
	})

	t.Run("StartReplicationAfterFlag", func(t *testing.T) {
		setupPoolerTest(t, setup)

		// This test verifies that replication only starts if StartReplicationAfter=true

		// Stop replication using StopReplication RPC
		t.Log("Stopping replication using StopReplication RPC...")
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		stopReq := &multipoolermanagerdatapb.StopReplicationRequest{
			Mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_ONLY,
			Wait: true,
		}
		_, err = standbyManagerClient.StopReplication(ctx, stopReq)
		require.NoError(t, err)
		cancel()

		// Verify replication is paused
		queryResp, err := standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT pg_is_wal_replay_paused()", 1)
		require.NoError(t, err)
		require.Len(t, queryResp.Rows, 1)
		isPaused := string(queryResp.Rows[0].Values[0])
		// PostgreSQL wire protocol returns boolean as 't' or 'f' in text format
		assert.Equal(t, "t", isPaused, "WAL replay should be paused")

		// Call SetPrimaryConnInfo with StartReplicationAfter=false
		// Use Force=true since we're testing replication functionality, not term validation
		ctx = utils.WithTimeout(t, 1*time.Second)

		t.Log("Calling SetPrimaryConnInfo with StartReplicationAfter=false...")
		primary := &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "test-cell",
				Name:      setup.PrimaryMultipooler.Name,
			},
			Hostname: "localhost",
			PortMap:  map[string]int32{"postgres": int32(setup.PrimaryPgctld.PgPort)},
		}
		setPrimaryReq := &multipoolermanagerdatapb.SetPrimaryConnInfoRequest{
			Primary:               primary,
			StartReplicationAfter: false, // Don't start after
			StopReplicationBefore: false,
			CurrentTerm:           0, // Ignored when Force=true
			Force:                 true,
		}
		_, err = standbyClient.Consensus.SetPrimaryConnInfo(ctx, setPrimaryReq)
		require.NoError(t, err, "SetPrimaryConnInfo should succeed")

		// Verify replication is still paused (not started)
		t.Log("Verifying replication remains paused when StartReplicationAfter=false...")
		queryResp, err = standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT pg_is_wal_replay_paused()", 1)
		require.NoError(t, err)
		require.Len(t, queryResp.Rows, 1)
		isPaused = string(queryResp.Rows[0].Values[0])
		// PostgreSQL wire protocol returns boolean as 't' or 'f' in text format
		assert.Equal(t, "t", isPaused, "WAL replay should still be paused when StartReplicationAfter=false")

		// Now call again with StartReplicationAfter=true
		t.Log("Calling SetPrimaryConnInfo with StartReplicationAfter=true...")
		setPrimaryReq.StartReplicationAfter = true
		_, err = standbyClient.Consensus.SetPrimaryConnInfo(ctx, setPrimaryReq)
		require.NoError(t, err, "SetPrimaryConnInfo should succeed")

		// Verify replication is now running
		t.Log("Verifying replication started when StartReplicationAfter=true...")
		queryResp, err = standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT pg_is_wal_replay_paused()", 1)
		require.NoError(t, err)
		require.Len(t, queryResp.Rows, 1)
		isPaused = string(queryResp.Rows[0].Values[0])
		// PostgreSQL wire protocol returns boolean as 't' or 'f' in text format
		assert.Equal(t, "f", isPaused, "WAL replay should be running after StartReplicationAfter=true")

		t.Log("Replication successfully started with StartReplicationAfter flag")
	})

	t.Run("WaitForLSN_Standby_Success", func(t *testing.T) {
		setupPoolerTest(t, setup, WithDropTables("test_wait_lsn"))

		// Insert data on primary to generate a new LSN
		t.Log("Creating table and inserting data on primary...")
		_, err = primaryPoolerClient.ExecuteQuery(context.Background(), "CREATE TABLE IF NOT EXISTS test_wait_lsn (id SERIAL PRIMARY KEY, data TEXT)", 0)
		require.NoError(t, err, "Should be able to create table in primary")

		_, err = primaryPoolerClient.ExecuteQuery(context.Background(), "INSERT INTO test_wait_lsn (data) VALUES ('test data for wait lsn')", 0)
		require.NoError(t, err, "Should be able to insert data in primary")

		// Get LSN from primary using consensus Status RPC
		ctx := utils.WithTimeout(t, 1*time.Second)

		primaryPosResp, err := primaryClient.Consensus.Status(ctx, &consensusdatapb.StatusRequest{})
		require.NoError(t, err)
		targetLSN := primaryPosResp.GetConsensusStatus().GetCurrentPosition().GetLsn()
		t.Logf("Target LSN from primary: %s", targetLSN)

		// Wait for standby to reach the target LSN
		t.Log("Waiting for standby to reach target LSN...")
		ctx = utils.WithTimeout(t, 1*time.Second)

		waitReq := &multipoolermanagerdatapb.WaitForLSNRequest{
			TargetLsn: targetLSN,
		}
		_, err = standbyManagerClient.WaitForLSN(ctx, waitReq)
		require.NoError(t, err, "WaitForLSN should succeed on standby")

		t.Log("Standby successfully reached target LSN")

		// Verify the data replicated
		queryResp, err := standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT COUNT(*) FROM test_wait_lsn", 1)
		require.NoError(t, err)
		require.Len(t, queryResp.Rows, 1)
		rowCount := string(queryResp.Rows[0].Values[0])
		assert.Equal(t, "1", rowCount, "Should have 1 row in standby")
	})

	t.Run("WaitForLSN_Primary_Fails", func(t *testing.T) {
		setupPoolerTest(t, setup)

		// WaitForLSN should fail on PRIMARY pooler type
		ctx := utils.WithTimeout(t, 1*time.Second)

		waitReq := &multipoolermanagerdatapb.WaitForLSNRequest{
			TargetLsn: "0/1000000",
		}
		_, err = primaryManagerClient.WaitForLSN(ctx, waitReq)
		require.Error(t, err, "WaitForLSN should fail on primary")
		assert.Contains(t, err.Error(), "operation not allowed", "Error should indicate operation not allowed on PRIMARY")
	})

	t.Run("WaitForLSN_Timeout", func(t *testing.T) {
		setupPoolerTest(t, setup)

		// Test timeout behavior by waiting for a very high LSN that won't be reached
		t.Log("Testing timeout with unreachable LSN...")

		// Use a very high LSN that won't be reached in the timeout period
		unreachableLSN := "FF/FFFFFFFF"

		ctx := utils.WithTimeout(t, 1*time.Second)

		waitReq := &multipoolermanagerdatapb.WaitForLSNRequest{
			TargetLsn: unreachableLSN,
		}
		_, err = standbyManagerClient.WaitForLSN(ctx, waitReq)
		st, ok := status.FromError(err)
		require.True(t, ok, "Error should be a gRPC status error")
		assert.Contains(t, []string{"DeadlineExceeded", "Canceled"}, st.Code().String(),
			"Error should be a timeout error code, got: %s", st.Code().String())
		t.Log("WaitForLSN correctly timed out")
	})

	t.Run("StartReplication_Success", func(t *testing.T) {
		setupPoolerTest(t, setup)

		// This test verifies that StartReplication successfully resumes WAL replay on standby

		// First stop replication using StopReplication RPC
		t.Log("Stopping replication using StopReplication RPC...")
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		stopReq := &multipoolermanagerdatapb.StopReplicationRequest{
			Mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_ONLY,
			Wait: true,
		}
		_, err = standbyManagerClient.StopReplication(ctx, stopReq)
		require.NoError(t, err)
		cancel()

		// Verify replication is paused
		queryResp, err := standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT pg_is_wal_replay_paused()", 1)
		require.NoError(t, err)
		require.Len(t, queryResp.Rows, 1)
		isPaused := string(queryResp.Rows[0].Values[0])
		// PostgreSQL wire protocol returns boolean as 't' or 'f' in text format
		assert.Equal(t, "t", isPaused, "WAL replay should be paused")
		t.Log("Confirmed: WAL replay is paused")

		// Call StartReplication RPC
		t.Log("Calling StartReplication RPC...")
		ctx = utils.WithTimeout(t, 1*time.Second)

		startReq := &multipoolermanagerdatapb.StartReplicationRequest{}
		_, err = standbyManagerClient.StartReplication(ctx, startReq)
		require.NoError(t, err, "StartReplication should succeed on standby")

		// Verify replication is now running
		t.Log("Verifying replication is running after StartReplication...")
		queryResp, err = standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT pg_is_wal_replay_paused()", 1)
		require.NoError(t, err)
		require.Len(t, queryResp.Rows, 1)
		isPaused = string(queryResp.Rows[0].Values[0])
		// PostgreSQL wire protocol returns boolean as 't' or 'f' in text format
		assert.Equal(t, "f", isPaused, "WAL replay should be running after StartReplication")

		t.Log("StartReplication successfully resumed WAL replay")
	})

	t.Run("StartReplication_Primary_Fails", func(t *testing.T) {
		setupPoolerTest(t, setup)

		// StartReplication should fail on PRIMARY pooler type
		t.Log("Testing StartReplication on PRIMARY pooler (should fail)...")

		ctx := utils.WithTimeout(t, 1*time.Second)

		startReq := &multipoolermanagerdatapb.StartReplicationRequest{}
		_, err = primaryManagerClient.StartReplication(ctx, startReq)
		require.Error(t, err, "StartReplication should fail on primary")
		assert.Contains(t, err.Error(), "operation not allowed", "Error should indicate operation not allowed on PRIMARY")
		t.Log("Confirmed: StartReplication correctly rejected on PRIMARY pooler")
	})

	t.Run("StopReplication_Success", func(t *testing.T) {
		// This test verifies that StopReplication successfully pauses WAL replay on standby

		// Setup cleanup - default behavior starts replication
		setupPoolerTest(t, setup)

		// First ensure replication is running
		t.Log("Ensuring replication is running...")
		queryResp, err := standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT pg_is_wal_replay_paused()", 1)
		require.NoError(t, err)
		require.Len(t, queryResp.Rows, 1)
		isPaused := string(queryResp.Rows[0].Values[0])
		// PostgreSQL wire protocol returns boolean as 't' or 'f' in text format
		if isPaused == "t" {
			// Resume it first using StartReplication RPC
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			startReq := &multipoolermanagerdatapb.StartReplicationRequest{}
			_, err = standbyManagerClient.StartReplication(ctx, startReq)
			require.NoError(t, err)
			cancel()
		}
		t.Log("Confirmed: WAL replay is running")

		// Call StopReplication RPC
		// StopReplication waits internally for the pause to complete before returning
		t.Log("Calling StopReplication RPC...")
		ctx := utils.WithTimeout(t, 1*time.Second)

		stopReq := &multipoolermanagerdatapb.StopReplicationRequest{
			Mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_ONLY,
			Wait: true,
		}
		_, err = standbyManagerClient.StopReplication(ctx, stopReq)
		require.NoError(t, err, "StopReplication should succeed on standby")

		// Verify replication is now paused (should be immediate since StopReplication waits)
		t.Log("Verifying replication is paused after StopReplication...")
		queryResp, err = standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT pg_is_wal_replay_paused()", 1)
		require.NoError(t, err)
		require.Len(t, queryResp.Rows, 1)
		isPaused = string(queryResp.Rows[0].Values[0])
		// PostgreSQL wire protocol returns boolean as 't' or 'f' in text format
		assert.Equal(t, "t", isPaused, "WAL replay should be paused after StopReplication")

		t.Log("StopReplication successfully paused WAL replay")
	})

	t.Run("StopReplication_Primary_Fails", func(t *testing.T) {
		setupPoolerTest(t, setup)

		// StopReplication should fail on PRIMARY pooler type
		t.Log("Testing StopReplication on PRIMARY pooler (should fail)...")

		ctx := utils.WithTimeout(t, 1*time.Second)

		stopReq := &multipoolermanagerdatapb.StopReplicationRequest{
			Mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_ONLY,
			Wait: true,
		}
		_, err = primaryManagerClient.StopReplication(ctx, stopReq)
		require.Error(t, err, "StopReplication should fail on primary")
		assert.Contains(t, err.Error(), "operation not allowed", "Error should indicate operation not allowed on PRIMARY")
		t.Log("Confirmed: StopReplication correctly rejected on PRIMARY pooler")
	})

	t.Run("StopReplication_ReceiverOnly_Wait", func(t *testing.T) {
		// This test verifies that RECEIVER_ONLY mode with wait=true:
		// 1. Clears primary_conninfo and disconnects the WAL receiver
		// 2. Does NOT pause WAL replay (replay continues)
		// 3. Waits for receiver to fully disconnect before returning

		// Use async replication for this test since it disconnects the standby and then
		// writes to the primary. With sync replication, writes would hang waiting for the
		// disconnected standby.
		setupPoolerTest(t, setup, WithDropTables("test_receiver_only"), WithResetGuc("synchronous_commit"))
		_, err := primaryPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "ALTER SYSTEM SET synchronous_commit = 'local'", 0)
		require.NoError(t, err, "Failed to set synchronous_commit to local")
		shardsetup.ReloadConfig(context.Background(), t, primaryPoolerClient, "primary")

		// Verify replication is working by checking pg_stat_wal_receiver
		t.Log("Verifying replication is streaming...")
		require.Eventually(t, func() bool {
			queryResp, err := standbyPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "SELECT COUNT(*) FROM pg_stat_wal_receiver WHERE status = 'streaming'", 1)
			if err != nil || len(queryResp.Rows) == 0 {
				return false
			}
			count := string(queryResp.Rows[0].Values[0])
			return count == "1"
		}, 10*time.Second, 500*time.Millisecond, "Replication should be streaming")

		// Verify WAL replay is NOT paused initially
		queryResp, err := standbyPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "SELECT pg_is_wal_replay_paused()", 1)
		require.NoError(t, err)
		require.Len(t, queryResp.Rows, 1)
		isPaused := string(queryResp.Rows[0].Values[0])
		// PostgreSQL wire protocol returns boolean as 't' or 'f' in text format
		assert.Equal(t, "f", isPaused, "WAL replay should not be paused initially")
		t.Log("Confirmed: Replication is streaming and replay is running")

		// Create a test table and insert data on primary before stopping receiver
		t.Log("Creating test table and inserting data on primary...")
		_, err = primaryPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "CREATE TABLE IF NOT EXISTS test_receiver_only (id SERIAL PRIMARY KEY, data TEXT)", 0)
		require.NoError(t, err)
		_, err = primaryPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "INSERT INTO test_receiver_only (data) VALUES ('before_stop')", 0)
		require.NoError(t, err)

		// Wait for data to replicate to standby
		t.Log("Waiting for data to replicate to standby...")
		require.Eventually(t, func() bool {
			resp, err := standbyPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "SELECT COUNT(*) FROM test_receiver_only WHERE data = 'before_stop'", 1)
			if err != nil || len(resp.Rows) == 0 {
				return false
			}
			count := string(resp.Rows[0].Values[0])
			return count == "1"
		}, 10*time.Second, 500*time.Millisecond, "Data should replicate to standby")
		t.Log("Confirmed: Data replicated successfully before stopping receiver")

		// Call StopReplication with RECEIVER_ONLY mode and wait=true
		t.Log("Calling StopReplication with RECEIVER_ONLY mode and wait=true...")

		stopReq := &multipoolermanagerdatapb.StopReplicationRequest{
			Mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_RECEIVER_ONLY,
			Wait: true,
		}
		_, err = standbyManagerClient.StopReplication(utils.WithShortDeadline(t), stopReq)
		require.NoError(t, err, "StopReplication with RECEIVER_ONLY should succeed")

		// Since wait=true, receiver should already be disconnected when the call returns
		t.Log("Verifying receiver is disconnected (should be immediate with wait=true)...")
		queryResp, err = standbyPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "SELECT COUNT(*) FROM pg_stat_wal_receiver", 1)
		require.NoError(t, err)
		require.Len(t, queryResp.Rows, 1)
		receiverCount := string(queryResp.Rows[0].Values[0])
		assert.Equal(t, "0", receiverCount, "WAL receiver should be disconnected after RECEIVER_ONLY with wait=true")

		// Verify WAL replay is still NOT paused (RECEIVER_ONLY shouldn't pause replay)
		queryResp, err = standbyPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "SELECT pg_is_wal_replay_paused()", 1)
		require.NoError(t, err)
		require.Len(t, queryResp.Rows, 1)
		isPaused = string(queryResp.Rows[0].Values[0])
		// PostgreSQL wire protocol returns boolean as 't' or 'f' in text format
		assert.Equal(t, "f", isPaused, "WAL replay should still be running after RECEIVER_ONLY mode")

		// Verify that data inserted before stopping receiver is still visible (replay continues on buffered WAL)
		t.Log("Verifying that previously replicated data is still visible...")
		dataResp, err := standbyPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "SELECT COUNT(*) FROM test_receiver_only WHERE data = 'before_stop'", 1)
		require.NoError(t, err)
		require.Len(t, dataResp.Rows, 1)
		count := string(dataResp.Rows[0].Values[0])
		assert.Equal(t, "1", count, "Previously replicated data should still be visible")

		// Insert new data on primary after receiver is disconnected
		t.Log("Inserting new data on primary after receiver disconnect...")
		_, err = primaryPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "INSERT INTO test_receiver_only (data) VALUES ('after_stop')", 0)
		require.NoError(t, err)

		// Verify that new data does NOT appear on standby (receiver is disconnected)
		t.Log("Verifying that new data does not replicate (receiver disconnected)...")
		time.Sleep(2 * time.Second) // Give it time to potentially replicate (it shouldn't)
		newDataResp, err := standbyPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "SELECT COUNT(*) FROM test_receiver_only WHERE data = 'after_stop'", 1)
		require.NoError(t, err)
		require.Len(t, newDataResp.Rows, 1)
		newCount := string(newDataResp.Rows[0].Values[0])
		assert.Equal(t, "0", newCount, "New data should NOT replicate after receiver disconnect")

		t.Log("Confirmed: Receiver disconnected, replay still running, and new data does not replicate")
	})

	t.Run("StopReplication_ReceiverOnly_NoWait", func(t *testing.T) {
		// This test verifies that RECEIVER_ONLY mode with wait=false:
		// 1. Returns immediately without waiting for receiver to disconnect
		// 2. Receiver eventually disconnects asynchronously
		// 3. Does NOT pause WAL replay

		setupPoolerTest(t, setup, WithDropTables("test_receiver_only_nowait"))
		// Verify replication is working
		t.Log("Verifying replication is streaming...")
		require.Eventually(t, func() bool {
			queryResp, err := standbyPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "SELECT COUNT(*) FROM pg_stat_wal_receiver WHERE status = 'streaming'", 1)
			if err != nil || len(queryResp.Rows) == 0 {
				return false
			}
			count := string(queryResp.Rows[0].Values[0])
			return count == "1"
		}, 10*time.Second, 500*time.Millisecond, "Replication should be streaming")

		// Create a test table and insert data on primary before stopping receiver
		t.Log("Creating test table and inserting data on primary...")
		_, err = primaryPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "CREATE TABLE IF NOT EXISTS test_receiver_only_nowait (id SERIAL PRIMARY KEY, data TEXT)", 0)
		require.NoError(t, err)
		_, err = primaryPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "INSERT INTO test_receiver_only_nowait (data) VALUES ('before_stop')", 0)
		require.NoError(t, err)

		// Wait for data to replicate to standby
		t.Log("Waiting for data to replicate to standby...")
		require.Eventually(t, func() bool {
			resp, err := standbyPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "SELECT COUNT(*) FROM test_receiver_only_nowait WHERE data = 'before_stop'", 1)
			if err != nil || len(resp.Rows) == 0 {
				return false
			}
			count := string(resp.Rows[0].Values[0])
			return count == "1"
		}, 10*time.Second, 500*time.Millisecond, "Data should replicate to standby")
		t.Log("Confirmed: Data replicated successfully before stopping receiver")

		// Call StopReplication with RECEIVER_ONLY mode and wait=false
		t.Log("Calling StopReplication with RECEIVER_ONLY mode and wait=false (should return immediately)...")

		stopReq := &multipoolermanagerdatapb.StopReplicationRequest{
			Mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_RECEIVER_ONLY,
			Wait: false,
		}
		_, err := standbyManagerClient.StopReplication(utils.WithShortDeadline(t), stopReq)
		require.NoError(t, err, "StopReplication with RECEIVER_ONLY and wait=false should succeed")

		// Since wait=false, the call returns immediately, but receiver should eventually disconnect
		t.Log("Verifying receiver eventually disconnects asynchronously...")
		require.Eventually(t, func() bool {
			queryResp, err := standbyPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "SELECT COUNT(*) FROM pg_stat_wal_receiver", 1)
			if err != nil || len(queryResp.Rows) == 0 {
				return false
			}
			count := string(queryResp.Rows[0].Values[0])
			return count == "0"
		}, 10*time.Second, 500*time.Millisecond, "WAL receiver should eventually disconnect")

		// Verify WAL replay is still NOT paused
		queryResp, err := standbyPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "SELECT pg_is_wal_replay_paused()", 1)
		require.NoError(t, err)
		require.Len(t, queryResp.Rows, 1)
		isPaused := string(queryResp.Rows[0].Values[0])
		// PostgreSQL wire protocol returns boolean as 't' or 'f' in text format
		assert.Equal(t, "f", isPaused, "WAL replay should still be running after RECEIVER_ONLY mode")

		// Verify that data inserted before stopping receiver is still visible
		t.Log("Verifying that previously replicated data is still visible...")
		dataResp, err := standbyPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "SELECT COUNT(*) FROM test_receiver_only_nowait WHERE data = 'before_stop'", 1)
		require.NoError(t, err)
		require.Len(t, dataResp.Rows, 1)
		count := string(dataResp.Rows[0].Values[0])
		assert.Equal(t, "1", count, "Previously replicated data should still be visible")

		t.Log("Confirmed: Receiver eventually disconnected, replay still running, and data visible")
	})

	t.Run("StopReplication_ReplayAndReceiver_Wait", func(t *testing.T) {
		// This test verifies that REPLAY_AND_RECEIVER mode with wait=true:
		// 1. Pauses WAL replay
		// 2. Clears primary_conninfo and disconnects the WAL receiver
		// 3. Waits for both to complete before returning

		// Use async replication since this test disconnects the standby and then writes to the primary.
		setupPoolerTest(t, setup, WithDropTables("test_replay_and_receiver"), WithResetGuc("synchronous_commit"))
		_, err := primaryPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "ALTER SYSTEM SET synchronous_commit = 'local'", 0)
		require.NoError(t, err, "Failed to set synchronous_commit to local")
		shardsetup.ReloadConfig(context.Background(), t, primaryPoolerClient, "primary")
		// Verify replication is working
		t.Log("Verifying replication is streaming...")
		require.Eventually(t, func() bool {
			queryResp, err := standbyPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "SELECT COUNT(*) FROM pg_stat_wal_receiver WHERE status = 'streaming'", 1)
			if err != nil || len(queryResp.Rows) == 0 {
				return false
			}
			count := string(queryResp.Rows[0].Values[0])
			return count == "1"
		}, 10*time.Second, 500*time.Millisecond, "Replication should be streaming")

		// Verify WAL replay is NOT paused initially
		queryResp, err := standbyPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "SELECT pg_is_wal_replay_paused()", 1)
		require.NoError(t, err)
		require.Len(t, queryResp.Rows, 1)
		isPaused := string(queryResp.Rows[0].Values[0])
		// PostgreSQL wire protocol returns boolean as 't' or 'f' in text format
		assert.Equal(t, "f", isPaused, "WAL replay should not be paused initially")

		// Create a test table and insert data on primary before pausing
		t.Log("Creating test table and inserting initial data on primary...")
		_, err = primaryPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "CREATE TABLE IF NOT EXISTS test_replay_and_receiver (id SERIAL PRIMARY KEY, data TEXT)", 0)
		require.NoError(t, err)
		_, err = primaryPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "INSERT INTO test_replay_and_receiver (data) VALUES ('before_pause')", 0)
		require.NoError(t, err)

		// Wait for data to replicate to standby
		t.Log("Waiting for initial data to replicate to standby...")
		require.Eventually(t, func() bool {
			resp, err := standbyPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "SELECT COUNT(*) FROM test_replay_and_receiver WHERE data = 'before_pause'", 1)
			if err != nil || len(resp.Rows) == 0 {
				return false
			}
			count := string(resp.Rows[0].Values[0])
			return count == "1"
		}, 10*time.Second, 500*time.Millisecond, "Initial data should replicate to standby")
		t.Log("Confirmed: Initial data replicated successfully")

		// Call StopReplication with REPLAY_AND_RECEIVER mode and wait=true
		t.Log("Calling StopReplication with REPLAY_AND_RECEIVER mode and wait=true...")

		stopReq := &multipoolermanagerdatapb.StopReplicationRequest{
			Mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_AND_RECEIVER,
			Wait: true,
		}
		_, err = standbyManagerClient.StopReplication(utils.WithShortDeadline(t), stopReq)
		require.NoError(t, err, "StopReplication with REPLAY_AND_RECEIVER should succeed")

		// Since wait=true, both replay and receiver should be stopped when the call returns
		t.Log("Verifying replay is paused (should be immediate with wait=true)...")
		queryResp, err = standbyPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "SELECT pg_is_wal_replay_paused()", 1)
		require.NoError(t, err)
		require.Len(t, queryResp.Rows, 1)
		isPaused = string(queryResp.Rows[0].Values[0])
		// PostgreSQL wire protocol returns boolean as 't' or 'f' in text format
		assert.Equal(t, "t", isPaused, "WAL replay should be paused after REPLAY_AND_RECEIVER with wait=true")

		t.Log("Verifying receiver is disconnected (should be immediate with wait=true)...")
		queryResp, err = standbyPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "SELECT COUNT(*) FROM pg_stat_wal_receiver", 1)
		require.NoError(t, err)
		require.Len(t, queryResp.Rows, 1)
		receiverCount := string(queryResp.Rows[0].Values[0])
		assert.Equal(t, "0", receiverCount, "WAL receiver should be disconnected after REPLAY_AND_RECEIVER with wait=true")

		// Insert new data on primary after stopping replication
		t.Log("Inserting new data on primary after stopping replication...")
		_, err = primaryPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "INSERT INTO test_replay_and_receiver (data) VALUES ('after_pause')", 0)
		require.NoError(t, err)

		// Verify that new data does NOT appear on standby (both receiver disconnected and replay paused)
		t.Log("Verifying that new data does not appear on standby...")
		time.Sleep(2 * time.Second) // Give it time to potentially replicate (it shouldn't)
		newDataResp, err := standbyPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "SELECT COUNT(*) FROM test_replay_and_receiver WHERE data = 'after_pause'", 1)
		require.NoError(t, err)
		require.Len(t, newDataResp.Rows, 1)
		newCount := string(newDataResp.Rows[0].Values[0])
		assert.Equal(t, "0", newCount, "New data should NOT appear on standby after REPLAY_AND_RECEIVER stop")

		// Verify old data is still visible
		oldDataResp, err := standbyPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "SELECT COUNT(*) FROM test_replay_and_receiver WHERE data = 'before_pause'", 1)
		require.NoError(t, err)
		require.Len(t, oldDataResp.Rows, 1)
		oldCount := string(oldDataResp.Rows[0].Values[0])
		assert.Equal(t, "1", oldCount, "Old data should still be visible")

		t.Log("Confirmed: Both replay paused and receiver disconnected, new data does not replicate")
	})

	t.Run("StopReplication_ReplayAndReceiver_NoWait", func(t *testing.T) {
		// This test verifies that REPLAY_AND_RECEIVER mode with wait=false:
		// 1. Returns immediately without waiting
		// 2. Replay and receiver eventually stop asynchronously

		setupPoolerTest(t, setup, WithDropTables("test_replay_and_receiver_nowait"))
		// Verify replication is working
		t.Log("Verifying replication is streaming...")
		require.Eventually(t, func() bool {
			queryResp, err := standbyPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "SELECT COUNT(*) FROM pg_stat_wal_receiver WHERE status = 'streaming'", 1)
			if err != nil || len(queryResp.Rows) == 0 {
				return false
			}
			count := string(queryResp.Rows[0].Values[0])
			return count == "1"
		}, 10*time.Second, 500*time.Millisecond, "Replication should be streaming")

		// Create a test table and insert data on primary before pausing
		t.Log("Creating test table and inserting initial data on primary...")
		_, err = primaryPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "CREATE TABLE IF NOT EXISTS test_replay_and_receiver_nowait (id SERIAL PRIMARY KEY, data TEXT)", 0)
		require.NoError(t, err)
		_, err = primaryPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "INSERT INTO test_replay_and_receiver_nowait (data) VALUES ('before_pause')", 0)
		require.NoError(t, err)

		// Wait for data to replicate to standby
		t.Log("Waiting for initial data to replicate to standby...")
		require.Eventually(t, func() bool {
			resp, err := standbyPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "SELECT COUNT(*) FROM test_replay_and_receiver_nowait WHERE data = 'before_pause'", 1)
			if err != nil || len(resp.Rows) == 0 {
				return false
			}
			count := string(resp.Rows[0].Values[0])
			return count == "1"
		}, 10*time.Second, 500*time.Millisecond, "Initial data should replicate to standby")
		t.Log("Confirmed: Initial data replicated successfully")

		// Call StopReplication with REPLAY_AND_RECEIVER mode and wait=false
		t.Log("Calling StopReplication with REPLAY_AND_RECEIVER mode and wait=false (should return immediately)...")

		stopReq := &multipoolermanagerdatapb.StopReplicationRequest{
			Mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_AND_RECEIVER,
			Wait: false,
		}
		_, err := standbyManagerClient.StopReplication(utils.WithShortDeadline(t), stopReq)
		require.NoError(t, err, "StopReplication with REPLAY_AND_RECEIVER and wait=false should succeed")

		// Since wait=false, the call returns immediately, but eventually both should stop
		t.Log("Verifying replay eventually pauses asynchronously...")
		require.Eventually(t, func() bool {
			queryResp, err := standbyPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "SELECT pg_is_wal_replay_paused()", 1)
			if err != nil || len(queryResp.Rows) == 0 {
				return false
			}
			isPaused := string(queryResp.Rows[0].Values[0])
			// PostgreSQL wire protocol returns boolean as 't' or 'f' in text format
			return isPaused == "t"
		}, 10*time.Second, 500*time.Millisecond, "WAL replay should eventually pause")

		t.Log("Verifying receiver eventually disconnects asynchronously...")
		require.Eventually(t, func() bool {
			queryResp, err := standbyPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "SELECT COUNT(*) FROM pg_stat_wal_receiver", 1)
			if err != nil || len(queryResp.Rows) == 0 {
				return false
			}
			count := string(queryResp.Rows[0].Values[0])
			return count == "0"
		}, 10*time.Second, 500*time.Millisecond, "WAL receiver should eventually disconnect")

		// Verify that old data is still visible
		t.Log("Verifying that previously replicated data is still visible...")
		oldDataResp, err := standbyPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "SELECT COUNT(*) FROM test_replay_and_receiver_nowait WHERE data = 'before_pause'", 1)
		require.NoError(t, err)
		require.Len(t, oldDataResp.Rows, 1)
		oldCount := string(oldDataResp.Rows[0].Values[0])
		assert.Equal(t, "1", oldCount, "Previously replicated data should still be visible")

		t.Log("Confirmed: Both replay paused and receiver disconnected eventually, old data visible")
	})
}

func TestReplicationStatus(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)

	// Wait for both managers to be ready before running tests
	waitForManagerReady(t, setup, setup.PrimaryMultipooler)
	waitForManagerReady(t, setup, setup.StandbyMultipooler)

	// Create shared clients for all subtests
	primaryConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { primaryConn.Close() })
	primaryManagerClient := multipoolermanagerpb.NewMultiPoolerManagerClient(primaryConn)

	standbyConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { standbyConn.Close() })
	standbyManagerClient := multipoolermanagerpb.NewMultiPoolerManagerClient(standbyConn)

	t.Run("ReplicationStatus_PRIMARY_returns_primary_status", func(t *testing.T) {
		setupPoolerTest(t, setup, WithoutReplication())

		t.Log("Testing ReplicationStatus on PRIMARY pooler...")

		// Call ReplicationStatus on PRIMARY
		statusResp, err := primaryManagerClient.Status(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StatusRequest{})
		require.NoError(t, err, "ReplicationStatus should succeed on PRIMARY")
		require.NotNil(t, statusResp.Status, "Status should not be nil")

		// Verify pooler type
		assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, statusResp.Status.PoolerType, "PoolerType should be PRIMARY")

		// Verify PrimaryStatus is populated
		assert.NotNil(t, statusResp.Status.PrimaryStatus, "PrimaryStatus should be populated for PRIMARY pooler")
		assert.Nil(t, statusResp.Status.ReplicationStatus, "ReplicationStatus should be nil for PRIMARY pooler")

		// Verify PrimaryStatus fields
		assert.NotEmpty(t, statusResp.Status.PrimaryStatus.Lsn, "LSN should be present")
		assert.Regexp(t, `^[0-9A-F]+/[0-9A-F]+$`, statusResp.Status.PrimaryStatus.Lsn, "LSN should be in PostgreSQL format")
		assert.True(t, statusResp.Status.PrimaryStatus.Ready, "Primary should be ready")
		assert.NotNil(t, statusResp.Status.PrimaryStatus.SyncReplicationConfig, "Sync replication config should be present")

		t.Logf("ReplicationStatus on PRIMARY verified: LSN=%s", statusResp.Status.PrimaryStatus.Lsn)
	})

	t.Run("ReplicationStatus_REPLICA_returns_replication_status", func(t *testing.T) {
		setupPoolerTest(t, setup)

		t.Log("Testing ReplicationStatus on REPLICA pooler...")

		// Ensure standby is connected and replicating (default setup behavior)
		require.Eventually(t, func() bool {
			statusResp, err := standbyManagerClient.Status(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StatusRequest{})
			if err != nil {
				return false
			}
			return statusResp.Status != nil &&
				statusResp.Status.ReplicationStatus != nil &&
				statusResp.Status.ReplicationStatus.PrimaryConnInfo != nil
		}, 10*time.Second, 500*time.Millisecond, "Standby should be connected (from default setup)")

		// Call ReplicationStatus on REPLICA
		statusResp, err := standbyManagerClient.Status(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StatusRequest{})
		require.NoError(t, err, "ReplicationStatus should succeed on REPLICA")
		require.NotNil(t, statusResp.Status, "Status should not be nil")

		// Verify pooler type
		assert.Equal(t, clustermetadatapb.PoolerType_REPLICA, statusResp.Status.PoolerType, "PoolerType should be REPLICA")

		// Verify ReplicationStatus is populated
		assert.Nil(t, statusResp.Status.PrimaryStatus, "PrimaryStatus should be nil for REPLICA pooler")
		assert.NotNil(t, statusResp.Status.ReplicationStatus, "ReplicationStatus should be populated for REPLICA pooler")

		// Verify ReplicationStatus fields
		assert.NotEmpty(t, statusResp.Status.ReplicationStatus.LastReplayLsn, "LastReplayLsn should be present")
		assert.NotEmpty(t, statusResp.Status.ReplicationStatus.LastReceiveLsn, "LastReceiveLsn should be present")
		assert.NotNil(t, statusResp.Status.ReplicationStatus.PrimaryConnInfo, "PrimaryConnInfo should be present")
		assert.NotEmpty(t, statusResp.Status.ReplicationStatus.PrimaryConnInfo.Host, "Primary host should be present")

		t.Logf("ReplicationStatus on REPLICA verified: LastReplayLSN=%s, PrimaryHost=%s",
			statusResp.Status.ReplicationStatus.LastReplayLsn,
			statusResp.Status.ReplicationStatus.PrimaryConnInfo.Host)
	})

	t.Run("ReplicationStatus_unified_API_works_for_both", func(t *testing.T) {
		setupPoolerTest(t, setup)

		t.Log("Testing unified ReplicationStatus API works for both PRIMARY and REPLICA...")

		// Call the same RPC on both PRIMARY and REPLICA
		primaryStatusResp, err := primaryManagerClient.Status(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StatusRequest{})
		require.NoError(t, err, "ReplicationStatus should succeed on PRIMARY")

		standbyStatusResp, err := standbyManagerClient.Status(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StatusRequest{})
		require.NoError(t, err, "ReplicationStatus should succeed on REPLICA")

		// Verify each returns the appropriate status
		assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, primaryStatusResp.Status.PoolerType)
		assert.NotNil(t, primaryStatusResp.Status.PrimaryStatus, "PRIMARY should return PrimaryStatus")
		assert.Nil(t, primaryStatusResp.Status.ReplicationStatus, "PRIMARY should not return ReplicationStatus")

		assert.Equal(t, clustermetadatapb.PoolerType_REPLICA, standbyStatusResp.Status.PoolerType)
		assert.Nil(t, standbyStatusResp.Status.PrimaryStatus, "REPLICA should not return PrimaryStatus")
		assert.NotNil(t, standbyStatusResp.Status.ReplicationStatus, "REPLICA should return ReplicationStatus")

		t.Log("Verified: Same ReplicationStatus RPC works correctly for both PRIMARY and REPLICA")
	})
}
