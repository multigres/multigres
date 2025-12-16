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

	"github.com/multigres/multigres/go/test/endtoend"
	"github.com/multigres/multigres/go/test/utils"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// TestReplicationAPIs tests the replication-related API functionality (SetPrimaryConnInfo, WaitForLSN, etc.)
func TestReplicationAPIs(t *testing.T) {
	skip, err := utils.ShouldSkipRealPostgres()
	if skip {
		t.Skip("Skipping end-to-end tests (short mode)")
	}
	require.NoError(t, err, "postgres binaries must be available")

	setup := getSharedTestSetup(t)

	// Wait for both managers to be ready before running tests
	waitForManagerReady(t, setup, setup.PrimaryMultipooler)
	waitForManagerReady(t, setup, setup.StandbyMultipooler)

	// Create shared clients for all subtests
	primaryPoolerClient, err := endtoend.NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort))
	require.NoError(t, err)
	t.Cleanup(func() { primaryPoolerClient.Close() })

	standbyPoolerClient, err := endtoend.NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort))
	require.NoError(t, err)
	t.Cleanup(func() { standbyPoolerClient.Close() })

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

	t.Run("ConfigureReplicationAndValidate", func(t *testing.T) {
		setupPoolerTest(t, setup, WithoutReplication(), WithDropTables("test_replication"))

		t.Log("Creating table and inserting data in primary...")
		_, err = primaryPoolerClient.ExecuteQuery(context.Background(), "CREATE TABLE IF NOT EXISTS test_replication (id SERIAL PRIMARY KEY, data TEXT)", 0)
		require.NoError(t, err, "Should be able to create table in primary")

		_, err = primaryPoolerClient.ExecuteQuery(context.Background(), "INSERT INTO test_replication (data) VALUES ('test data')", 0)
		require.NoError(t, err, "Should be able to insert data in primary")

		// Get LSN from primary using PrimaryPosition RPC
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		primaryPosResp, err := primaryManagerClient.PrimaryPosition(ctx, &multipoolermanagerdatapb.PrimaryPositionRequest{})
		require.NoError(t, err)
		primaryLSN := primaryPosResp.LsnPosition
		t.Logf("Primary LSN after insert: %s", primaryLSN)

		// Validate data is NOT in standby yet (no replication configured)
		t.Log("Validating data is NOT in standby (replication not configured)...")

		// Use WaitForLSN to verify standby cannot reach primary's LSN without replication
		// This should timeout since replication is not configured
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

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

		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		// Call SetPrimaryConnInfo with StartReplicationAfter=true
		_, err = standbyManagerClient.SetTerm(utils.WithShortDeadline(t), &multipoolermanagerdatapb.SetTermRequest{
			Term: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber: 1,
			},
		})
		require.NoError(t, err, "SetTerm should succeed on standby")

		setPrimaryReq := &multipoolermanagerdatapb.SetPrimaryConnInfoRequest{
			Host:                  "localhost",
			Port:                  int32(setup.PrimaryPgctld.PgPort),
			StartReplicationAfter: true,
			StopReplicationBefore: false,
			CurrentTerm:           1,
			Force:                 false,
		}
		_, err = standbyManagerClient.SetPrimaryConnInfo(ctx, setPrimaryReq)
		require.NoError(t, err, "SetPrimaryConnInfo should succeed")
		t.Log("Replication configured successfully")

		// Wait for standby to catch up to primary's LSN using WaitForLSN API
		t.Logf("Waiting for standby to catch up to primary LSN: %s", primaryLSN)

		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

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

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		// Try to set primary conn info with stale term (current term is 1, we'll try with 0)
		setPrimaryReq := &multipoolermanagerdatapb.SetPrimaryConnInfoRequest{
			Host:                  "localhost",
			Port:                  int32(setup.PrimaryPgctld.PgPort),
			StartReplicationAfter: true,
			StopReplicationBefore: false,
			CurrentTerm:           0, // Stale term (lower than current term 1)
			Force:                 false,
		}
		_, err = standbyManagerClient.SetPrimaryConnInfo(ctx, setPrimaryReq)
		require.Error(t, err, "SetPrimaryConnInfo should fail with stale term")
		assert.Contains(t, err.Error(), "consensus term too old", "Error should mention term is too old")

		// Try again with force=true, should succeed
		setPrimaryReq.Force = true
		_, err = standbyManagerClient.SetPrimaryConnInfo(ctx, setPrimaryReq)
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

		// Call SetPrimaryConnInfo with StopReplicationBefore=true and StartReplicationAfter=false
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		t.Log("Calling SetPrimaryConnInfo with StopReplicationBefore=true, StartReplicationAfter=false...")
		setPrimaryReq := &multipoolermanagerdatapb.SetPrimaryConnInfoRequest{
			Host:                  "localhost",
			Port:                  int32(setup.PrimaryPgctld.PgPort),
			StartReplicationAfter: false, // Don't start after
			StopReplicationBefore: true,  // Stop before
			CurrentTerm:           1,
			Force:                 false,
		}
		_, err = standbyManagerClient.SetPrimaryConnInfo(ctx, setPrimaryReq)
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
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		t.Log("Calling SetPrimaryConnInfo with StartReplicationAfter=false...")
		_, err = standbyManagerClient.SetTerm(utils.WithShortDeadline(t), &multipoolermanagerdatapb.SetTermRequest{
			Term: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber: 1,
			},
		})
		require.NoError(t, err, "SetTerm should succeed on standby")

		setPrimaryReq := &multipoolermanagerdatapb.SetPrimaryConnInfoRequest{
			Host:                  "localhost",
			Port:                  int32(setup.PrimaryPgctld.PgPort),
			StartReplicationAfter: false, // Don't start after
			StopReplicationBefore: false,
			CurrentTerm:           1,
			Force:                 false,
		}
		_, err = standbyManagerClient.SetPrimaryConnInfo(ctx, setPrimaryReq)
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
		_, err = standbyManagerClient.SetPrimaryConnInfo(ctx, setPrimaryReq)
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

		// Get LSN from primary
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		primaryPosResp, err := primaryManagerClient.PrimaryPosition(ctx, &multipoolermanagerdatapb.PrimaryPositionRequest{})
		require.NoError(t, err)
		targetLSN := primaryPosResp.LsnPosition
		t.Logf("Target LSN from primary: %s", targetLSN)

		// Wait for standby to reach the target LSN
		t.Log("Waiting for standby to reach target LSN...")
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

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
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

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

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

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
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

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

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

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
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

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

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

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

		setupPoolerTest(t, setup, WithDropTables("test_receiver_only"))

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

		setupPoolerTest(t, setup, WithDropTables("test_replay_and_receiver"))
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

	t.Run("ResetReplication_Success", func(t *testing.T) {
		setupPoolerTest(t, setup, WithDropTables("test_reset_replication"))

		// This test verifies that ResetReplication successfully disconnects the standby from the primary
		// and that data inserted after reset does not replicate until replication is re-enabled

		// First ensure replication is configured
		t.Log("Ensuring replication is configured...")
		_, err = standbyManagerClient.SetTerm(utils.WithShortDeadline(t), &multipoolermanagerdatapb.SetTermRequest{
			Term: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber: 1,
			},
		})
		require.NoError(t, err, "SetTerm should succeed on standby")
		setPrimaryReq := &multipoolermanagerdatapb.SetPrimaryConnInfoRequest{
			Host:                  "localhost",
			Port:                  int32(setup.PrimaryPgctld.PgPort),
			StartReplicationAfter: true,
			StopReplicationBefore: false,
			CurrentTerm:           1,
			Force:                 false,
		}
		_, err = standbyManagerClient.SetPrimaryConnInfo(utils.WithShortDeadline(t), setPrimaryReq)
		require.NoError(t, err, "SetPrimaryConnInfo should succeed")

		// Verify replication is working by checking pg_stat_wal_receiver
		t.Log("Verifying replication is working...")
		require.Eventually(t, func() bool {
			queryResp, err := standbyPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "SELECT COUNT(*) FROM pg_stat_wal_receiver WHERE status = 'streaming'", 1)
			if err != nil || len(queryResp.Rows) == 0 {
				return false
			}
			count := string(queryResp.Rows[0].Values[0])
			return count == "1"
		}, 10*time.Second, 500*time.Millisecond, "Replication should be streaming")
		t.Log("Confirmed: Replication is streaming")

		// Call ResetReplication RPC (it waits for receiver to disconnect)
		t.Log("Calling ResetReplication RPC...")

		resetReq := &multipoolermanagerdatapb.ResetReplicationRequest{}
		_, err = standbyManagerClient.ResetReplication(utils.WithShortDeadline(t), resetReq)
		require.NoError(t, err, "ResetReplication should succeed on standby")

		// Verify that primary_conninfo is cleared by checking pg_stat_wal_receiver
		// ResetReplication now waits for disconnect, so it should be immediate
		t.Log("Verifying replication is disconnected after ResetReplication...")
		queryResp, err := standbyPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "SELECT COUNT(*) FROM pg_stat_wal_receiver", 1)
		require.NoError(t, err, "Query should succeed")
		require.NotEmpty(t, queryResp.Rows, "Query should return a row")
		count := string(queryResp.Rows[0].Values[0])
		require.Equal(t, "0", count, "WAL receiver should be disconnected after ResetReplication")

		t.Log("ResetReplication successfully disconnected standby from primary")

		// Sanity check: Insert a row on primary, verify it does NOT replicate to standby
		t.Log("Sanity check: Inserting data on primary after ResetReplication...")
		_, err = primaryPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "CREATE TABLE IF NOT EXISTS test_reset_replication (id SERIAL PRIMARY KEY, data TEXT)", 0)
		require.NoError(t, err, "Should be able to create table on primary")

		_, err = primaryPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "INSERT INTO test_reset_replication (data) VALUES ('should not replicate')", 0)
		require.NoError(t, err, "Should be able to insert data on primary")

		// Get LSN from primary after the insert
		primaryPosResp, err := primaryManagerClient.PrimaryPosition(utils.WithShortDeadline(t), &multipoolermanagerdatapb.PrimaryPositionRequest{})
		require.NoError(t, err)
		primaryLSNAfterInsert := primaryPosResp.LsnPosition
		t.Logf("Primary LSN after insert: %s", primaryLSNAfterInsert)

		// Verify standby CANNOT reach the primary LSN (replication is disconnected)
		t.Log("Verifying standby cannot reach primary LSN (replication disconnected)...")
		waitReq := &multipoolermanagerdatapb.WaitForLSNRequest{
			TargetLsn: primaryLSNAfterInsert,
		}
		_, err = standbyManagerClient.WaitForLSN(utils.WithShortDeadline(t), waitReq)
		require.Error(t, err, "WaitForLSN should timeout since replication is disconnected")
		t.Log("Confirmed: Standby cannot reach primary LSN (data did NOT replicate)")

		// Re-enable replication using SetPrimaryConnInfo
		t.Log("Re-enabling replication...")
		setPrimaryReq = &multipoolermanagerdatapb.SetPrimaryConnInfoRequest{
			Host:                  "localhost",
			Port:                  int32(setup.PrimaryPgctld.PgPort),
			StartReplicationAfter: true,
			StopReplicationBefore: false,
			CurrentTerm:           1,
			Force:                 false,
		}
		_, err = standbyManagerClient.SetPrimaryConnInfo(utils.WithShortDeadline(t), setPrimaryReq)
		require.NoError(t, err, "SetPrimaryConnInfo should succeed")

		// Wait for standby to catch up to primary's LSN
		t.Logf("Waiting for standby to catch up to primary LSN: %s", primaryLSNAfterInsert)

		waitReq = &multipoolermanagerdatapb.WaitForLSNRequest{
			TargetLsn: primaryLSNAfterInsert,
		}
		_, err = standbyManagerClient.WaitForLSN(utils.WithShortDeadline(t), waitReq)
		require.NoError(t, err, "Standby should catch up after re-enabling replication")

		// Verify the table now exists on standby
		t.Log("Verifying data replicated after re-enabling replication...")
		dataResp, err := standbyPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "SELECT COUNT(*) FROM test_reset_replication", 1)
		require.NoError(t, err)
		require.Len(t, dataResp.Rows, 1)
		rowCount := string(dataResp.Rows[0].Values[0])
		assert.Equal(t, "1", rowCount, "Should have 1 row on standby after re-enabling replication")

		t.Log("Confirmed: Data successfully replicated after re-enabling replication")
	})

	t.Run("ResetReplication_Primary_Fails", func(t *testing.T) {
		setupPoolerTest(t, setup)

		// ResetReplication should fail on PRIMARY pooler type
		t.Log("Testing ResetReplication on PRIMARY pooler (should fail)...")

		resetReq := &multipoolermanagerdatapb.ResetReplicationRequest{}
		_, err = primaryManagerClient.ResetReplication(utils.WithShortDeadline(t), resetReq)
		require.Error(t, err, "ResetReplication should fail on primary")
		assert.Contains(t, err.Error(), "operation not allowed", "Error should indicate operation not allowed on PRIMARY")
		t.Log("Confirmed: ResetReplication correctly rejected on PRIMARY pooler")
	})
}

// TestReplicationStatus tests the ReplicationStatus API
func TestStandbyReplicationStatus(t *testing.T) {
	skip, err := utils.ShouldSkipRealPostgres()
	if skip {
		t.Skip("Skipping end-to-end tests (short mode)")
	}
	require.NoError(t, err, "postgres binaries must be available")

	setup := getSharedTestSetup(t)

	// Connect to manager clients
	primaryManagerConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { primaryManagerConn.Close() })

	primaryManagerClient := multipoolermanagerpb.NewMultiPoolerManagerClient(primaryManagerConn)

	standbyManagerConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { standbyManagerConn.Close() })
	standbyManagerClient := multipoolermanagerpb.NewMultiPoolerManagerClient(standbyManagerConn)

	// Ensure managers are ready
	waitForManagerReady(t, setup, setup.PrimaryMultipooler)
	waitForManagerReady(t, setup, setup.StandbyMultipooler)

	t.Run("StandbyReplicationStatus_Primary_Fails", func(t *testing.T) {
		setupPoolerTest(t, setup)

		// StandbyReplicationStatus should fail on PRIMARY pooler type
		t.Log("Testing StandbyReplicationStatus on PRIMARY (should fail)...")

		_, err := primaryManagerClient.StandbyReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StandbyReplicationStatusRequest{})
		require.Error(t, err, "StandbyReplicationStatus should fail on PRIMARY")
		assert.Contains(t, err.Error(), "operation not allowed", "Error should indicate operation not allowed on PRIMARY")
		t.Log("Confirmed: StandbyReplicationStatus correctly rejected on PRIMARY pooler")
	})

	t.Run("StandbyReplicationStatus_Standby_NoReplication", func(t *testing.T) {
		setupPoolerTest(t, setup)

		// Test StandbyReplicationStatus on standby when replication is not configured
		t.Log("Testing StandbyReplicationStatus on standby with no replication configured...")

		// First, ensure replication is stopped
		_, err := standbyManagerClient.ResetReplication(utils.WithShortDeadline(t), &multipoolermanagerdatapb.ResetReplicationRequest{})
		require.NoError(t, err, "ResetReplication should succeed")

		// Wait for config to take effect (pg_reload_conf is async)
		t.Log("Waiting for primary_conninfo to be cleared...")
		require.Eventually(t, func() bool {
			statusResp, err := standbyManagerClient.StandbyReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StandbyReplicationStatusRequest{})
			if err != nil {
				t.Logf("ReplicationStatus error: %v", err)
				return false
			}
			// Config cleared when PrimaryConnInfo is nil or Host is empty
			return statusResp.Status.PrimaryConnInfo == nil ||
				statusResp.Status.PrimaryConnInfo.Host == ""
		}, 5*time.Second, 200*time.Millisecond, "primary_conninfo should be cleared after ResetReplication")

		// Get final status
		statusResp, err := standbyManagerClient.StandbyReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StandbyReplicationStatusRequest{})
		require.NoError(t, err, "ReplicationStatus should succeed on standby")
		require.NotNil(t, statusResp.Status, "Status should not be nil")

		// Verify fields
		assert.NotEmpty(t, statusResp.Status.LastReplayLsn, "Last replay LSN should not be empty")
		assert.NotEmpty(t, statusResp.Status.LastReceiveLsn, "Last receive LSN should not be empty")
		assert.False(t, statusResp.Status.IsWalReplayPaused, "WAL replay should not be paused by default")

		// PrimaryConnInfo should be nil or empty when no replication is configured
		if statusResp.Status.PrimaryConnInfo != nil {
			assert.Empty(t, statusResp.Status.PrimaryConnInfo.Host, "Host should be empty when no replication configured")
		}
	})

	t.Run("ReplicationStatus_Standby_WithReplication", func(t *testing.T) {
		setupPoolerTest(t, setup)

		// Configure replication
		t.Log("Configuring replication on standby...")
		setPrimaryReq := &multipoolermanagerdatapb.SetPrimaryConnInfoRequest{
			Host:                  "localhost",
			Port:                  int32(setup.PrimaryPgctld.PgPort),
			StartReplicationAfter: true,
			StopReplicationBefore: false,
			CurrentTerm:           1,
			Force:                 false,
		}
		_, err := standbyManagerClient.SetPrimaryConnInfo(utils.WithShortDeadline(t), setPrimaryReq)
		require.NoError(t, err, "SetPrimaryConnInfo should succeed")

		// Wait for config to take effect (pg_reload_conf is async)
		t.Log("Waiting for primary_conninfo to be set...")
		require.Eventually(t, func() bool {
			statusResp, err := standbyManagerClient.StandbyReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StandbyReplicationStatusRequest{})
			if err != nil {
				t.Logf("ReplicationStatus error: %v", err)
				return false
			}
			// Config set when PrimaryConnInfo is not nil and Host is populated
			return statusResp.Status.PrimaryConnInfo != nil &&
				statusResp.Status.PrimaryConnInfo.Host != ""
		}, 5*time.Second, 200*time.Millisecond, "primary_conninfo should be set after SetPrimaryConnInfo")

		// Get final status
		statusResp, err := standbyManagerClient.StandbyReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StandbyReplicationStatusRequest{})
		require.NoError(t, err, "ReplicationStatus should succeed")
		require.NotNil(t, statusResp.Status, "Status should not be nil")

		// Verify LSN
		assert.NotEmpty(t, statusResp.Status.LastReplayLsn, "Last replay LSN should not be empty")
		assert.Contains(t, statusResp.Status.LastReplayLsn, "/", "Last replay LSN should be in PostgreSQL format (e.g., 0/1234ABCD)")
		assert.NotEmpty(t, statusResp.Status.LastReceiveLsn, "Last receive LSN should not be empty")
		assert.Contains(t, statusResp.Status.LastReceiveLsn, "/", "Last receive LSN should be in PostgreSQL format (e.g., 0/1234ABCD)")

		// Verify replication is not paused
		assert.False(t, statusResp.Status.IsWalReplayPaused, "WAL replay should not be paused")

		// Verify PrimaryConnInfo is populated
		require.NotNil(t, statusResp.Status.PrimaryConnInfo, "PrimaryConnInfo should not be nil")
		assert.Equal(t, "localhost", statusResp.Status.PrimaryConnInfo.Host, "Host should match")
		assert.Equal(t, int32(setup.PrimaryPgctld.PgPort), statusResp.Status.PrimaryConnInfo.Port, "Port should match")
		assert.NotEmpty(t, statusResp.Status.PrimaryConnInfo.Raw, "Raw connection string should not be empty")
	})

	t.Run("ReplicationStatus_Standby_PausedReplication", func(t *testing.T) {
		setupPoolerTest(t, setup)

		// Configure replication but stop it
		t.Log("Configuring replication and then stopping it...")
		_, err := standbyManagerClient.SetTerm(utils.WithShortDeadline(t), &multipoolermanagerdatapb.SetTermRequest{
			Term: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber: 1,
			},
		})
		require.NoError(t, err, "SetTerm should succeed on standby")

		setPrimaryReq := &multipoolermanagerdatapb.SetPrimaryConnInfoRequest{
			Host:                  "localhost",
			Port:                  int32(setup.PrimaryPgctld.PgPort),
			StartReplicationAfter: false,
			StopReplicationBefore: true,
			CurrentTerm:           1,
			Force:                 false,
		}
		_, err = standbyManagerClient.SetPrimaryConnInfo(utils.WithShortDeadline(t), setPrimaryReq)
		require.NoError(t, err, "SetPrimaryConnInfo should succeed")

		// Wait for config to take effect and WAL replay to be paused
		t.Log("Waiting for WAL replay to be paused...")
		require.Eventually(t, func() bool {
			statusResp, err := standbyManagerClient.StandbyReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StandbyReplicationStatusRequest{})
			if err != nil {
				t.Logf("ReplicationStatus error: %v", err)
				return false
			}
			return statusResp.Status.IsWalReplayPaused
		}, 5*time.Second, 200*time.Millisecond, "WAL replay should be paused after SetPrimaryConnInfo with StopReplicationBefore")

		// Get final status
		statusResp, err := standbyManagerClient.StandbyReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StandbyReplicationStatusRequest{})
		require.NoError(t, err, "ReplicationStatus should succeed")
		require.NotNil(t, statusResp.Status, "Status should not be nil")

		// Verify WAL replay is paused
		assert.True(t, statusResp.Status.IsWalReplayPaused, "WAL replay should be paused")
		assert.NotEmpty(t, statusResp.Status.WalReplayPauseState, "Pause state should not be empty")
		// Clean up: resume replication
		startReq := &multipoolermanagerdatapb.StartReplicationRequest{}
		_, err = standbyManagerClient.StartReplication(utils.WithShortDeadline(t), startReq)
		require.NoError(t, err, "StartReplication should succeed")
	})
}

// TestStopReplicationAndGetStatus tests the StopReplicationAndGetStatus API
func TestStopReplicationAndGetStatus(t *testing.T) {
	skip, err := utils.ShouldSkipRealPostgres()
	if skip {
		t.Skip("Skipping end-to-end tests (short mode)")
	}
	require.NoError(t, err, "postgres binaries must be available")

	setup := getSharedTestSetup(t)

	// Connect to manager clients
	primaryManagerConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { primaryManagerConn.Close() })

	primaryManagerClient := multipoolermanagerpb.NewMultiPoolerManagerClient(primaryManagerConn)

	standbyManagerConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { standbyManagerConn.Close() })
	standbyManagerClient := multipoolermanagerpb.NewMultiPoolerManagerClient(standbyManagerConn)

	// Ensure managers are ready
	waitForManagerReady(t, setup, setup.PrimaryMultipooler)
	waitForManagerReady(t, setup, setup.StandbyMultipooler)

	t.Run("StopReplicationAndGetStatus_Primary_Fails", func(t *testing.T) {
		setupPoolerTest(t, setup)

		// StopReplicationAndGetStatus should fail on PRIMARY pooler type
		t.Log("Testing StopReplicationAndGetStatus on PRIMARY (should fail)...")

		_, err := primaryManagerClient.StopReplicationAndGetStatus(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StopReplicationAndGetStatusRequest{
			Mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_ONLY,
			Wait: true,
		})
		require.Error(t, err, "StopReplicationAndGetStatus should fail on PRIMARY")
		assert.Contains(t, err.Error(), "operation not allowed", "Error should indicate operation not allowed on PRIMARY")
		t.Log("Confirmed: StopReplicationAndGetStatus correctly rejected on PRIMARY pooler")
	})

	t.Run("StopReplicationAndGetStatus_Standby_Success", func(t *testing.T) {
		// This test verifies that StopReplicationAndGetStatus stops replication and returns correct status
		t.Log("Testing StopReplicationAndGetStatus on standby with running replication...")

		// Setup cleanup to restore replication state after test
		setupPoolerTest(t, setup, WithoutReplication(), WithDropTables("stop_repl_test"))

		// Connect to primary pooler to write test data
		primaryPoolerClient, err := endtoend.NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort))
		require.NoError(t, err)
		defer primaryPoolerClient.Close()

		// First, configure and start replication
		_, err = standbyManagerClient.SetTerm(utils.WithShortDeadline(t), &multipoolermanagerdatapb.SetTermRequest{
			Term: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber: 1,
			},
		})
		require.NoError(t, err, "SetTerm should succeed on standby")

		t.Log("Configuring replication on standby...")
		setPrimaryReq := &multipoolermanagerdatapb.SetPrimaryConnInfoRequest{
			Host:                  "localhost",
			Port:                  int32(setup.PrimaryPgctld.PgPort),
			StartReplicationAfter: true,
			StopReplicationBefore: false,
			CurrentTerm:           1,
			Force:                 false,
		}
		_, err = standbyManagerClient.SetPrimaryConnInfo(utils.WithShortDeadline(t), setPrimaryReq)
		require.NoError(t, err, "SetPrimaryConnInfo should succeed")

		// Wait for replication to be running
		t.Log("Waiting for replication to be running...")
		require.Eventually(t, func() bool {
			statusResp, err := standbyManagerClient.StandbyReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StandbyReplicationStatusRequest{})
			if err != nil {
				return false
			}
			return statusResp.Status.PrimaryConnInfo != nil &&
				statusResp.Status.PrimaryConnInfo.Host != "" &&
				!statusResp.Status.IsWalReplayPaused
		}, 5*time.Second, 200*time.Millisecond, "Replication should be running")

		// Call StopReplicationAndGetStatus
		// Note: This method waits internally for pause to complete, so status is guaranteed to be paused when it returns
		t.Log("Calling StopReplicationAndGetStatus...")
		stopResp, err := standbyManagerClient.StopReplicationAndGetStatus(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StopReplicationAndGetStatusRequest{
			Mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_ONLY,
			Wait: true,
		})
		require.NoError(t, err, "StopReplicationAndGetStatus should succeed on standby")
		require.NotNil(t, stopResp, "Response should not be nil")
		require.NotNil(t, stopResp.Status, "Status should not be nil")

		t.Log("Verifying status shows replication is paused...")
		assert.True(t, stopResp.Status.IsWalReplayPaused, "WAL replay should be paused after StopReplicationAndGetStatus")
		assert.NotEmpty(t, stopResp.Status.WalReplayPauseState, "Pause state should not be empty")

		// Verify LSN is populated
		assert.NotEmpty(t, stopResp.Status.LastReplayLsn, "Last replay LSN should not be empty")
		assert.Contains(t, stopResp.Status.LastReplayLsn, "/", "Last replay LSN should be in PostgreSQL format")
		assert.NotEmpty(t, stopResp.Status.LastReceiveLsn, "Last receive LSN should not be empty")
		assert.Contains(t, stopResp.Status.LastReceiveLsn, "/", "Last receive LSN should be in PostgreSQL format")

		// Verify PrimaryConnInfo is populated (should still be set even though replication is paused)
		require.NotNil(t, stopResp.Status.PrimaryConnInfo, "PrimaryConnInfo should not be nil")
		assert.Equal(t, "localhost", stopResp.Status.PrimaryConnInfo.Host, "Host should match")
		assert.Equal(t, int32(setup.PrimaryPgctld.PgPort), stopResp.Status.PrimaryConnInfo.Port, "Port should match")
		assert.NotEmpty(t, stopResp.Status.PrimaryConnInfo.Raw, "Raw connection string should not be empty")

		// Store the LSN after stopping
		lsnAfterStop := stopResp.Status.LastReplayLsn
		t.Logf("Standby LSN after stopping replication: %s", lsnAfterStop)

		// Write data to primary (this should NOT replicate to standby since replication is stopped)
		t.Log("Writing data to primary after stopping standby replication...")
		_, err = primaryPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "CREATE TABLE IF NOT EXISTS stop_repl_test (id SERIAL PRIMARY KEY, value TEXT)", 1)
		require.NoError(t, err, "Should be able to create test table on primary")

		_, err = primaryPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "INSERT INTO stop_repl_test (value) VALUES ('test_row_1')", 1)
		require.NoError(t, err, "Should be able to write to primary")

		_, err = primaryPoolerClient.ExecuteQuery(utils.WithShortDeadline(t), "INSERT INTO stop_repl_test (value) VALUES ('test_row_2')", 1)
		require.NoError(t, err, "Should be able to write to primary")

		// Wait a bit to ensure writes would have replicated if replication was running
		time.Sleep(500 * time.Millisecond)

		// Verify standby LSN hasn't advanced (replication is truly stopped)
		t.Log("Verifying standby LSN hasn't advanced...")
		statusAfterWrite, err := standbyManagerClient.StandbyReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StandbyReplicationStatusRequest{})
		require.NoError(t, err, "ReplicationStatus should succeed")
		lsnAfterWrite := statusAfterWrite.Status.LastReplayLsn
		t.Logf("Standby LSN after writes to primary: %s", lsnAfterWrite)

		assert.Equal(t, lsnAfterStop, lsnAfterWrite, "Standby replay LSN should not have advanced after primary writes (replication is stopped)")
		t.Log("Confirmed: Standby LSN did not advance, replication is truly stopped")

		t.Log("StopReplicationAndGetStatus successfully stopped replication and returned correct status")
	})

	t.Run("StopReplicationAndGetStatus_Standby_AlreadyPaused", func(t *testing.T) {
		// This test verifies that StopReplicationAndGetStatus works even when replication is already paused
		t.Log("Testing StopReplicationAndGetStatus when replication is already paused...")

		// Setup cleanup to restore replication state after test
		setupPoolerTest(t, setup, WithoutReplication())

		// First, stop replication
		// StopReplication now waits internally for the pause to complete, so no manual wait needed
		t.Log("Stopping replication first...")
		_, err := standbyManagerClient.StopReplication(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StopReplicationRequest{
			Mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_ONLY,
			Wait: true,
		})
		require.NoError(t, err, "StopReplication should succeed")

		// Call StopReplicationAndGetStatus (should succeed even though already paused)
		// Note: This method waits internally for pause to complete, so status is guaranteed to be paused when it returns
		t.Log("Calling StopReplicationAndGetStatus on already paused replication...")
		stopResp, err := standbyManagerClient.StopReplicationAndGetStatus(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StopReplicationAndGetStatusRequest{
			Mode: multipoolermanagerdatapb.ReplicationPauseMode_REPLICATION_PAUSE_MODE_REPLAY_ONLY,
			Wait: true,
		})
		require.NoError(t, err, "StopReplicationAndGetStatus should succeed even when already paused")
		require.NotNil(t, stopResp, "Response should not be nil")
		require.NotNil(t, stopResp.Status, "Status should not be nil")

		assert.True(t, stopResp.Status.IsWalReplayPaused, "WAL replay should be paused")
		assert.NotEmpty(t, stopResp.Status.LastReplayLsn, "Last replay LSN should not be empty")
		assert.NotEmpty(t, stopResp.Status.LastReceiveLsn, "Last receive LSN should not be empty")

		t.Log("StopReplicationAndGetStatus successfully handled already-paused replication")
	})
}

// TestConfigureSynchronousReplication tests the ConfigureSynchronousReplication API
func TestConfigureSynchronousReplication(t *testing.T) {
	skip, err := utils.ShouldSkipRealPostgres()
	if skip {
		t.Skip("Skipping end-to-end tests (short mode)")
	}
	require.NoError(t, err, "postgres binaries must be available")

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

	primaryPoolerClient, err := endtoend.NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort))
	require.NoError(t, err)
	t.Cleanup(func() { primaryPoolerClient.Close() })

	t.Run("ConfigureSynchronousReplication_Primary_Success", func(t *testing.T) {
		// Register cleanup to reset replication config
		setupPoolerTest(t, setup, WithoutReplication())

		// This test verifies that ConfigureSynchronousReplication successfully configures
		// synchronous replication on the primary
		t.Log("Testing ConfigureSynchronousReplication on PRIMARY...")

		// The application_name used by standby is: {cell}_{name}
		// For test purposes, we'll use a simple standby name
		standbyAppName := "test-standby"

		// Configure synchronous replication with FIRST method
		req := &multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           1,
			StandbyIds:        []*clustermetadatapb.ID{makeMultipoolerID("test-cell", "test-standby")},
			ReloadConfig:      true,
		}
		_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), req)
		require.NoError(t, err, "ConfigureSynchronousReplication should succeed on primary")

		t.Log("ConfigureSynchronousReplication completed successfully")

		// Wait for configuration to converge and verify using PrimaryStatus API
		t.Log("Waiting for configuration to converge...")
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) bool {
			return config != nil &&
				config.SynchronousCommit == multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON &&
				config.SynchronousMethod == multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST &&
				config.NumSync == 1 &&
				containsStandbyIDInConfig(config, "test-cell", standbyAppName)
		}, "Configuration should converge to expected values")

		t.Log("Synchronous replication configured and verified successfully")
	})

	t.Run("ConfigureSynchronousReplication_Primary_AnyMethod", func(t *testing.T) {
		// Register cleanup to reset replication config
		setupPoolerTest(t, setup, WithoutReplication())

		// This test verifies that ConfigureSynchronousReplication works with ANY method
		t.Log("Testing ConfigureSynchronousReplication with ANY method on PRIMARY...")

		// Use multiple standby IDs to test the ANY method with multiple standbys
		standbyIDs := []*clustermetadatapb.ID{
			makeMultipoolerID("test-cell", "test-standby-1"),
			makeMultipoolerID("test-cell", "test-standby-2"),
		}

		// Configure synchronous replication with ANY method
		req := &multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_APPLY,
			SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
			NumSync:           1,
			StandbyIds:        standbyIDs,
			ReloadConfig:      true,
		}
		_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), req)
		require.NoError(t, err, "ConfigureSynchronousReplication should succeed on primary")

		t.Log("ConfigureSynchronousReplication with ANY method completed successfully")

		// Wait for configuration to converge and verify using PrimaryStatus API
		t.Log("Waiting for configuration to converge...")
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) bool {
			return config != nil &&
				config.SynchronousCommit == multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_APPLY &&
				config.SynchronousMethod == multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY &&
				config.NumSync == 1 &&
				len(config.StandbyIds) == 2
		}, "Configuration should converge to expected values")

		t.Log("Synchronous replication with ANY method configured and verified successfully")
	})

	t.Run("ConfigureSynchronousReplication_AllCommitLevels", func(t *testing.T) {
		setupPoolerTest(t, setup, WithoutReplication())
		// This test verifies that all SynchronousCommitLevel values work correctly
		t.Log("Testing ConfigureSynchronousReplication with all commit levels...")

		testCases := []struct {
			level multipoolermanagerdatapb.SynchronousCommitLevel
		}{
			{
				level: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_OFF,
			},
			{
				level: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_LOCAL,
			},
			{
				level: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_WRITE,
			},
			{
				level: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			},
			{
				level: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_APPLY,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.level.String(), func(t *testing.T) {
				// Configure with this commit level
				req := &multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest{
					SynchronousCommit: tc.level,
					SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
					NumSync:           1,
					StandbyIds:        []*clustermetadatapb.ID{makeMultipoolerID("test-cell", "test-standby")},
					ReloadConfig:      true,
				}
				_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), req)
				require.NoError(t, err, "ConfigureSynchronousReplication should succeed for %s", tc.level.String())

				t.Logf("ConfigureSynchronousReplication with %s completed successfully", tc.level.String())

				// Wait for configuration to converge and verify using PrimaryStatus API
				t.Logf("Waiting for configuration to converge to %s...", tc.level.String())
				waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) bool {
					return config != nil && config.SynchronousCommit == tc.level
				}, "Configuration should converge to expected commit level")

				// Verify the configuration using PrimaryStatus
				status := getPrimaryStatusFromClient(t, primaryManagerClient)
				assert.Equal(t, tc.level, status.SyncReplicationConfig.SynchronousCommit, "synchronous_commit should be %s", tc.level.String())

				t.Logf("Successfully verified synchronous_commit level: %s", tc.level.String())
			})
		}
	})

	t.Run("ConfigureSynchronousReplication_AllSynchronousMethods", func(t *testing.T) {
		setupPoolerTest(t, setup, WithoutReplication())
		// This test verifies that FIRST and ANY methods work correctly with different num_sync values
		t.Log("Testing ConfigureSynchronousReplication with all synchronous methods...")

		testCases := []struct {
			name       string
			method     multipoolermanagerdatapb.SynchronousMethod
			numSync    int32
			standbyIDs []*clustermetadatapb.ID
		}{
			{
				name:    "FIRST_1_SingleStandby",
				method:  multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
				numSync: 1,
				standbyIDs: []*clustermetadatapb.ID{
					makeMultipoolerID("test-cell", "standby-1"),
				},
			},
			{
				name:    "FIRST_1_MultipleStandbys",
				method:  multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
				numSync: 1,
				standbyIDs: []*clustermetadatapb.ID{
					makeMultipoolerID("test-cell", "standby-1"),
					makeMultipoolerID("test-cell", "standby-2"),
					makeMultipoolerID("test-cell", "standby-3"),
				},
			},
			{
				name:    "FIRST_2_MultipleStandbys",
				method:  multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
				numSync: 2,
				standbyIDs: []*clustermetadatapb.ID{
					makeMultipoolerID("test-cell", "standby-1"),
					makeMultipoolerID("test-cell", "standby-2"),
					makeMultipoolerID("test-cell", "standby-3"),
				},
			},
			{
				name:    "FIRST_3_MultipleStandbys",
				method:  multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
				numSync: 3,
				standbyIDs: []*clustermetadatapb.ID{
					makeMultipoolerID("test-cell", "standby-1"),
					makeMultipoolerID("test-cell", "standby-2"),
					makeMultipoolerID("test-cell", "standby-3"),
					makeMultipoolerID("test-cell", "standby-4"),
				},
			},
			{
				name:    "ANY_1_SingleStandby",
				method:  multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
				numSync: 1,
				standbyIDs: []*clustermetadatapb.ID{
					makeMultipoolerID("test-cell", "standby-1"),
				},
			},
			{
				name:    "ANY_1_MultipleStandbys",
				method:  multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
				numSync: 1,
				standbyIDs: []*clustermetadatapb.ID{
					makeMultipoolerID("test-cell", "standby-1"),
					makeMultipoolerID("test-cell", "standby-2"),
					makeMultipoolerID("test-cell", "standby-3"),
				},
			},
			{
				name:    "ANY_2_MultipleStandbys",
				method:  multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
				numSync: 2,
				standbyIDs: []*clustermetadatapb.ID{
					makeMultipoolerID("test-cell", "standby-1"),
					makeMultipoolerID("test-cell", "standby-2"),
					makeMultipoolerID("test-cell", "standby-3"),
				},
			},
			{
				name:    "ANY_3_MultipleStandbys",
				method:  multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
				numSync: 3,
				standbyIDs: []*clustermetadatapb.ID{
					makeMultipoolerID("test-cell", "standby-1"),
					makeMultipoolerID("test-cell", "standby-2"),
					makeMultipoolerID("test-cell", "standby-3"),
					makeMultipoolerID("test-cell", "standby-4"),
				},
			},
		}

		setupPoolerTest(t, setup, WithoutReplication())
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Configure with this synchronous method
				req := &multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest{
					SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
					SynchronousMethod: tc.method,
					NumSync:           tc.numSync,
					StandbyIds:        tc.standbyIDs,
					ReloadConfig:      true,
				}
				_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), req)
				require.NoError(t, err, "ConfigureSynchronousReplication should succeed for %s", tc.name)

				t.Logf("ConfigureSynchronousReplication with %s completed successfully", tc.name)

				// Wait for configuration to converge and verify using PrimaryStatus API
				t.Logf("Waiting for configuration to converge for %s...", tc.name)
				waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) bool {
					return config != nil &&
						config.SynchronousMethod == tc.method &&
						config.NumSync == tc.numSync &&
						len(config.StandbyIds) == len(tc.standbyIDs)
				}, "Configuration should converge to expected values")

				// Verify the configuration using PrimaryStatus
				status := getPrimaryStatusFromClient(t, primaryManagerClient)
				assert.Equal(t, tc.method, status.SyncReplicationConfig.SynchronousMethod, "synchronous_method should match")
				assert.Equal(t, tc.numSync, status.SyncReplicationConfig.NumSync, "num_sync should match")
				assert.Len(t, status.SyncReplicationConfig.StandbyIds, len(tc.standbyIDs), "should have correct number of standbys")

				t.Logf("Successfully verified synchronous method configuration: %s", tc.name)
			})
		}
	})

	t.Run("ConfigureSynchronousReplication_EndToEnd_WithRealStandby", func(t *testing.T) {
		setupPoolerTest(t, setup, WithoutReplication(), WithDropTables("test_sync_repl"))
		// This test validates the complete synchronous replication flow:
		// 1. Configure primary with remote_apply and the actual standby name
		// 2. Ensure standby is connected and replicating
		// 3. Verify writes succeed (synchronous replication satisfied)
		// 4. Disconnect standby using ResetReplication
		// 5. Verify writes timeout (synchronous replication cannot be satisfied)
		t.Log("Testing end-to-end synchronous replication with real standby...")

		// The standby's application_name is constructed as: {cell}_{name}
		// Use the ServiceID from the setup which is the multipooler name
		standbyID := makeMultipoolerID("test-cell", setup.StandbyMultipooler.ServiceID)
		standbyAppName := fmt.Sprintf("test-cell_%s", setup.StandbyMultipooler.ServiceID)
		t.Logf("Using standby application_name from setup: %s", standbyAppName)

		// Configure synchronous replication on primary with remote_apply and actual standby
		t.Log("Configuring synchronous replication on primary with remote_apply...")
		configReq := &multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_APPLY,
			SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           1,
			StandbyIds:        []*clustermetadatapb.ID{standbyID},
			ReloadConfig:      true,
		}
		_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), configReq)
		require.NoError(t, err, "ConfigureSynchronousReplication should succeed on primary")

		// Wait for synchronous replication configuration to take effect
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) bool {
			return config != nil &&
				config.SynchronousCommit == multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_APPLY &&
				config.SynchronousMethod == multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST &&
				config.NumSync == 1 &&
				len(config.StandbyIds) == 1
		}, "Synchronous replication configuration should converge")

		_, err = standbyManagerClient.SetTerm(utils.WithShortDeadline(t), &multipoolermanagerdatapb.SetTermRequest{
			Term: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber: 1,
			},
		})
		require.NoError(t, err, "SetTerm should succeed")

		t.Log("Ensuring standby is connected to primary and replicating...")
		setPrimaryReq := &multipoolermanagerdatapb.SetPrimaryConnInfoRequest{
			Host:                  "localhost",
			Port:                  int32(setup.PrimaryPgctld.PgPort),
			StartReplicationAfter: true,
			StopReplicationBefore: false,
			CurrentTerm:           1,
			Force:                 false,
		}
		_, err = standbyManagerClient.SetPrimaryConnInfo(utils.WithShortDeadline(t), setPrimaryReq)
		require.NoError(t, err, "SetPrimaryConnInfo should succeed")

		// Wait for config to take effect and replication to establish (pg_reload_conf is async)
		t.Log("Waiting for replication to be configured...")
		require.Eventually(t, func() bool {
			statusResp, err := standbyManagerClient.StandbyReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StandbyReplicationStatusRequest{})
			if err != nil {
				return false
			}
			return statusResp.Status.PrimaryConnInfo != nil &&
				statusResp.Status.PrimaryConnInfo.Host != "" &&
				!statusResp.Status.IsWalReplayPaused
		}, 5*time.Second, 200*time.Millisecond, "Replication should be configured and active")

		// Verify standby is connected and replicating using ReplicationStatus API
		t.Log("Verifying standby is connected and replicating...")
		statusResp, err := standbyManagerClient.StandbyReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StandbyReplicationStatusRequest{})
		require.NoError(t, err, "ReplicationStatus should succeed")
		require.NotNil(t, statusResp.Status, "Status should not be nil")
		require.NotNil(t, statusResp.Status.PrimaryConnInfo, "PrimaryConnInfo should not be nil")
		t.Logf("Standby replication status: replay_lsn=%s, receive_lsn=%s, is_paused=%v, pause_state=%s, primary_conn_info=%s",
			statusResp.Status.LastReplayLsn, statusResp.Status.LastReceiveLsn, statusResp.Status.IsWalReplayPaused, statusResp.Status.WalReplayPauseState, statusResp.Status.PrimaryConnInfo.Raw)

		// Verify standby is not paused
		require.False(t, statusResp.Status.IsWalReplayPaused, "Standby should be actively replicating (not paused)")

		// Verify primary_conninfo contains the expected application_name
		require.Equal(t, standbyAppName, statusResp.Status.PrimaryConnInfo.ApplicationName,
			"PrimaryConnInfo.ApplicationName should match expected standby application name")

		// Test write with synchronous replication enabled
		t.Log("Testing write with synchronous replication enabled...")

		// Create a test table and insert data - this should succeed because standby is available
		_, err = primaryPoolerClient.ExecuteQuery(context.Background(), "CREATE TABLE IF NOT EXISTS test_sync_repl (id SERIAL PRIMARY KEY, data TEXT)", 0)
		require.NoError(t, err, "Table creation should succeed with standby available")

		_, err = primaryPoolerClient.ExecuteQuery(context.Background(), "INSERT INTO test_sync_repl (data) VALUES ('test-with-standby')", 0)
		require.NoError(t, err, "Insert should succeed with standby connected and replicating")
		t.Log("Write succeeded with synchronous replication enabled")

		// Verify standby caught up to primary after the successful write
		t.Log("Verifying standby caught up to primary after successful write...")
		primaryPosResp, err := primaryManagerClient.PrimaryPosition(utils.WithShortDeadline(t), &multipoolermanagerdatapb.PrimaryPositionRequest{})
		require.NoError(t, err, "Should be able to get primary position")
		primaryLSN := primaryPosResp.LsnPosition
		t.Logf("Primary LSN after write: %s", primaryLSN)

		waitReq := &multipoolermanagerdatapb.WaitForLSNRequest{
			TargetLsn: primaryLSN,
		}
		_, err = standbyManagerClient.WaitForLSN(utils.WithShortDeadline(t), waitReq)
		require.NoError(t, err, "Standby should have caught up to primary after successful write")
		t.Log("Standby successfully caught up to primary")

		// Disconnect standby using ResetReplication
		t.Log("Disconnecting standby using ResetReplication...")
		_, err = standbyManagerClient.ResetReplication(utils.WithShortDeadline(t), &multipoolermanagerdatapb.ResetReplicationRequest{})
		require.NoError(t, err, "ResetReplication should succeed")

		// Wait for standby to fully disconnect
		t.Log("Waiting for standby to disconnect...")
		require.Eventually(t, func() bool {
			standbyPoolerClient, err := endtoend.NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort))
			if err != nil {
				return false
			}
			defer standbyPoolerClient.Close()
			queryResp, err := standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT COUNT(*) FROM pg_stat_wal_receiver", 1)
			if err != nil || len(queryResp.Rows) == 0 {
				return false
			}
			count := string(queryResp.Rows[0].Values[0])
			return count == "0"
		}, 10*time.Second, 500*time.Millisecond, "Standby should disconnect after ResetReplication")
		t.Log("Standby disconnected successfully")

		// Get standby LSN before attempting the write
		t.Log("Getting standby LSN before failed write attempt...")
		standbyStatusBefore, err := standbyManagerClient.StandbyReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StandbyReplicationStatusRequest{})
		require.NoError(t, err, "Should be able to get standby status")
		standbyLSNBefore := standbyStatusBefore.Status.LastReplayLsn
		t.Logf("Standby replay LSN before write attempt: %s", standbyLSNBefore)

		// Test write timeout without standby
		t.Log("Testing write timeout without standby available...")
		// Create a new connection for this test
		primaryPoolerClient.Close()
		primaryPoolerClient, err = endtoend.NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort))
		require.NoError(t, err)
		t.Cleanup(func() { primaryPoolerClient.Close() })

		// This insert should timeout because synchronous_commit=remote_apply requires standby confirmation
		// Use a 3-second context timeout so the test doesn't wait too long
		timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer timeoutCancel()
		_, err = primaryPoolerClient.ExecuteQuery(timeoutCtx, "INSERT INTO test_sync_repl (data) VALUES ('test-without-standby')", 0)
		require.Error(t, err, "Insert should timeout without standby available")
		assert.Contains(t, err.Error(), "DeadlineExceeded", "Error should indicate a deadline exceeded")
		t.Log("Write correctly timed out without standby available")

		// Verify standby LSN did not advance
		t.Log("Verifying standby LSN did not advance after failed write...")
		standbyStatusAfter, err := standbyManagerClient.StandbyReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StandbyReplicationStatusRequest{})
		require.NoError(t, err, "Should be able to get standby status")
		standbyLSNAfter := standbyStatusAfter.Status.LastReplayLsn
		t.Logf("Standby LSN after failed write: %s", standbyLSNAfter)
		assert.Equal(t, standbyLSNBefore, standbyLSNAfter, "Standby LSN should not have advanced since replication is disconnected and write failed")

		t.Log("End-to-end synchronous replication test completed successfully")
	})

	t.Run("ConfigureSynchronousReplication_ClearConfig", func(t *testing.T) {
		setupPoolerTest(t, setup)

		// This test verifies that ConfigureSynchronousReplication can clear the configuration
		// by providing an empty standby list
		t.Log("Testing ConfigureSynchronousReplication can clear configuration...")

		// First, configure synchronous replication with some standbys
		configReq := &multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           1,
			StandbyIds: []*clustermetadatapb.ID{
				makeMultipoolerID("test-cell", "standby1"),
				makeMultipoolerID("test-cell", "standby2"),
			},
			ReloadConfig: true,
		}
		_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), configReq)
		require.NoError(t, err, "ConfigureSynchronousReplication should succeed")

		// Wait for initial configuration to converge and verify using PrimaryStatus API
		t.Log("Waiting for initial configuration to converge...")
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) bool {
			return config != nil &&
				config.SynchronousMethod == multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST &&
				config.NumSync == 1 &&
				len(config.StandbyIds) == 2
		}, "Initial configuration should converge")

		t.Log("Initial configuration verified")

		// Now clear the configuration by providing empty standby list
		t.Log("Clearing synchronous_standby_names configuration...")
		clearReq := &multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           0,
			StandbyIds:        []*clustermetadatapb.ID{},
			ReloadConfig:      true,
		}
		_, err = primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), clearReq)
		require.NoError(t, err, "ConfigureSynchronousReplication should succeed with empty config")

		// Wait for cleared configuration to converge and verify using PrimaryStatus API
		t.Log("Waiting for configuration to be cleared...")
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) bool {
			return config != nil &&
				config.NumSync == 0 &&
				len(config.StandbyIds) == 0
		}, "Configuration should be cleared")

		t.Log("Successfully verified synchronous_standby_names is cleared")
	})

	t.Run("ConfigureSynchronousReplication_Standby_Fails", func(t *testing.T) {
		setupPoolerTest(t, setup)

		// ConfigureSynchronousReplication should fail on REPLICA pooler type
		t.Log("Testing ConfigureSynchronousReplication on REPLICA pooler (should fail)...")

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		req := &multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           1,
			StandbyIds: []*clustermetadatapb.ID{
				makeMultipoolerID("test-cell", "standby1"),
			},
			ReloadConfig: true,
		}
		_, err := standbyManagerClient.ConfigureSynchronousReplication(ctx, req)
		require.Error(t, err, "ConfigureSynchronousReplication should fail on standby")
		assert.Contains(t, err.Error(), "operation not allowed", "Error should indicate operation not allowed on REPLICA")
		t.Log("Confirmed: ConfigureSynchronousReplication correctly rejected on REPLICA pooler")
	})
}

func TestUpdateSynchronousStandbyList(t *testing.T) {
	skip, err := utils.ShouldSkipRealPostgres()
	if skip {
		t.Skip("Skipping end-to-end tests (short mode)")
	}
	require.NoError(t, err, "postgres binaries must be available")

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

	primaryPoolerClient, err := endtoend.NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort))
	require.NoError(t, err)
	t.Cleanup(func() { primaryPoolerClient.Close() })

	t.Run("UpdateSynchronousStandbyList_Add_Success", func(t *testing.T) {
		setupPoolerTest(t, setup, WithoutReplication())
		t.Log("Testing UpdateSynchronousStandbyList ADD operation...")

		// First, configure initial synchronous replication with one standby
		configReq := &multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           1,
			StandbyIds:        []*clustermetadatapb.ID{makeMultipoolerID("test-cell", "standby1")},
			ReloadConfig:      true,
		}
		_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), configReq)
		require.NoError(t, err, "Initial configuration should succeed")

		// Wait for config to converge
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) bool {
			return config != nil && len(config.StandbyIds) == 1 && containsStandbyIDInConfig(config, "test-cell", "standby1")
		}, "Initial config should converge")

		// Verify initial configuration
		status := getPrimaryStatusFromClient(t, primaryManagerClient)
		assert.Equal(t, multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST, status.SyncReplicationConfig.SynchronousMethod)
		assert.Equal(t, int32(1), status.SyncReplicationConfig.NumSync)
		assert.Len(t, status.SyncReplicationConfig.StandbyIds, 1)
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby1"))
		t.Log("Initial configuration verified")

		_, err = primaryManagerClient.SetTerm(utils.WithShortDeadline(t), &multipoolermanagerdatapb.SetTermRequest{
			Term: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber: 1,
			},
		})
		require.NoError(t, err, "SetTerm should succeed on primary")

		updateReq := &multipoolermanagerdatapb.UpdateSynchronousStandbyListRequest{
			Operation:     multipoolermanagerdatapb.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_ADD,
			StandbyIds:    []*clustermetadatapb.ID{makeMultipoolerID("test-cell", "standby2")},
			ReloadConfig:  true,
			ConsensusTerm: 1,
			Force:         false,
		}
		_, err = primaryManagerClient.UpdateSynchronousStandbyList(utils.WithShortDeadline(t), updateReq)
		require.NoError(t, err, "ADD should succeed")

		require.Eventually(t, func() bool {
			numStandbys := getPrimaryStatusFromClient(t, primaryManagerClient).SyncReplicationConfig.StandbyIds
			return len(numStandbys) == 2
		}, 5*time.Second, 50*time.Millisecond)

		// Verify both standbys are now in the list
		status = getPrimaryStatusFromClient(t, primaryManagerClient)
		assert.Equal(t, multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST, status.SyncReplicationConfig.SynchronousMethod)
		assert.Equal(t, int32(1), status.SyncReplicationConfig.NumSync)
		assert.Len(t, status.SyncReplicationConfig.StandbyIds, 2)
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby1"))
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby2"))

		t.Log("ADD operation verified successfully")
	})

	t.Run("UpdateSynchronousStandbyList_Add_Idempotent", func(t *testing.T) {
		setupPoolerTest(t, setup, WithoutReplication())
		t.Log("Testing UpdateSynchronousStandbyList ADD operation is idempotent...")

		// Configure with two standbys
		configReq := &multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           1,
			StandbyIds: []*clustermetadatapb.ID{
				makeMultipoolerID("test-cell", "standby1"),
				makeMultipoolerID("test-cell", "standby2"),
			},
			ReloadConfig: true,
		}
		_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), configReq)
		require.NoError(t, err)

		// Wait for config to converge
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) bool {
			return config != nil && len(config.StandbyIds) == 2 &&
				containsStandbyIDInConfig(config, "test-cell", "standby1") &&
				containsStandbyIDInConfig(config, "test-cell", "standby2")
		}, "Initial config should converge")

		initialStatus := getPrimaryStatusFromClient(t, primaryManagerClient)
		t.Log("Initial configuration verified")

		// Try to ADD a standby that already exists
		updateReq := &multipoolermanagerdatapb.UpdateSynchronousStandbyListRequest{
			Operation:     multipoolermanagerdatapb.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_ADD,
			StandbyIds:    []*clustermetadatapb.ID{makeMultipoolerID("test-cell", "standby1")},
			ReloadConfig:  true,
			ConsensusTerm: 0,
			Force:         true,
		}
		_, err = primaryManagerClient.UpdateSynchronousStandbyList(utils.WithShortDeadline(t), updateReq)
		require.NoError(t, err, "ADD should be idempotent")

		// Wait for config to settle - should remain unchanged (idempotent)
		time.Sleep(1 * time.Second)
		// Configuration should be unchanged (idempotent)
		afterStatus := getPrimaryStatusFromClient(t, primaryManagerClient)
		assert.Equal(t, len(initialStatus.SyncReplicationConfig.StandbyIds), len(afterStatus.SyncReplicationConfig.StandbyIds), "Standby count should be unchanged")
		assert.True(t, containsStandbyIDInConfig(afterStatus.SyncReplicationConfig, "test-cell", "standby1"))
		assert.True(t, containsStandbyIDInConfig(afterStatus.SyncReplicationConfig, "test-cell", "standby2"))
	})

	t.Run("UpdateSynchronousStandbyList_Add_MixedExistingAndNew", func(t *testing.T) {
		setupPoolerTest(t, setup, WithoutReplication())
		t.Log("Testing UpdateSynchronousStandbyList ADD with both existing and new standbys...")

		// Configure with two standbys
		configReq := &multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           1,
			StandbyIds: []*clustermetadatapb.ID{
				makeMultipoolerID("test-cell", "standby1"),
				makeMultipoolerID("test-cell", "standby2"),
			},
			ReloadConfig: true,
		}
		_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), configReq)
		require.NoError(t, err)

		// Wait for config to converge
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) bool {
			return config != nil && len(config.StandbyIds) == 2
		}, "Initial config with 2 standbys should converge")

		t.Log("Initial configuration verified")

		// ADD with mix: standby2 already exists, standby3 and standby4 are new
		updateReq := &multipoolermanagerdatapb.UpdateSynchronousStandbyListRequest{
			Operation: multipoolermanagerdatapb.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_ADD,
			StandbyIds: []*clustermetadatapb.ID{
				makeMultipoolerID("test-cell", "standby2"), // already exists
				makeMultipoolerID("test-cell", "standby3"), // new
				makeMultipoolerID("test-cell", "standby4"), // new
			},
			ReloadConfig:  true,
			ConsensusTerm: 0,
			Force:         true,
		}
		_, err = primaryManagerClient.UpdateSynchronousStandbyList(utils.WithShortDeadline(t), updateReq)
		require.NoError(t, err, "ADD with mixed existing and new standbys should succeed")

		// Wait for config to converge with all 4 standbys
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) bool {
			return config != nil && len(config.StandbyIds) == 4 &&
				containsStandbyIDInConfig(config, "test-cell", "standby1") &&
				containsStandbyIDInConfig(config, "test-cell", "standby2") &&
				containsStandbyIDInConfig(config, "test-cell", "standby3") &&
				containsStandbyIDInConfig(config, "test-cell", "standby4")
		}, "Config should have all 4 standbys")
	})

	t.Run("UpdateSynchronousStandbyList_Add_Then_Remove_Sequence", func(t *testing.T) {
		setupPoolerTest(t, setup, WithoutReplication())
		t.Log("Testing UpdateSynchronousStandbyList ADD followed by REMOVE in sequence...")

		// Configure with initial standbys
		configReq := &multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
			NumSync:           2,
			StandbyIds: []*clustermetadatapb.ID{
				makeMultipoolerID("test-cell", "standby1"),
				makeMultipoolerID("test-cell", "standby2"),
			},
			ReloadConfig: true,
		}
		_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), configReq)
		require.NoError(t, err)

		// Wait for config to converge
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) bool {
			return config != nil && len(config.StandbyIds) == 2
		}, "Initial config with 2 standbys should converge")

		t.Log("Initial configuration verified")

		// ADD new standbys
		addReq := &multipoolermanagerdatapb.UpdateSynchronousStandbyListRequest{
			Operation: multipoolermanagerdatapb.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_ADD,
			StandbyIds: []*clustermetadatapb.ID{
				makeMultipoolerID("test-cell", "standby3"),
				makeMultipoolerID("test-cell", "standby4"),
			},
			ReloadConfig:  true,
			ConsensusTerm: 0,
			Force:         true,
		}
		_, err = primaryManagerClient.UpdateSynchronousStandbyList(utils.WithShortDeadline(t), addReq)
		require.NoError(t, err, "ADD operation should succeed")

		// Wait for config to converge with all 4 standbys
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) bool {
			return config != nil && len(config.StandbyIds) == 4
		}, "Config should have 4 standbys after ADD")

		afterAddStatus := getPrimaryStatusFromClient(t, primaryManagerClient)
		assert.Len(t, afterAddStatus.SyncReplicationConfig.StandbyIds, 4, "Should have 4 standbys after ADD")
		t.Log("ADD operation verified")

		// Now REMOVE some standbys (including one that was just added and one original)
		removeReq := &multipoolermanagerdatapb.UpdateSynchronousStandbyListRequest{
			Operation: multipoolermanagerdatapb.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_REMOVE,
			StandbyIds: []*clustermetadatapb.ID{
				makeMultipoolerID("test-cell", "standby2"), // original
				makeMultipoolerID("test-cell", "standby4"), // just added
			},
			ReloadConfig:  true,
			ConsensusTerm: 0,
			Force:         true,
		}
		_, err = primaryManagerClient.UpdateSynchronousStandbyList(utils.WithShortDeadline(t), removeReq)
		require.NoError(t, err, "REMOVE operation should succeed")

		// Wait for config to converge with 2 remaining standbys
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) bool {
			return config != nil && len(config.StandbyIds) == 2 &&
				containsStandbyIDInConfig(config, "test-cell", "standby1") &&
				containsStandbyIDInConfig(config, "test-cell", "standby3")
		}, "Config should have only standby1 and standby3 after REMOVE")

		// Verify final state
		finalStatus := getPrimaryStatusFromClient(t, primaryManagerClient)
		assert.Len(t, finalStatus.SyncReplicationConfig.StandbyIds, 2, "Should have 2 standbys after REMOVE")
		assert.True(t, containsStandbyIDInConfig(finalStatus.SyncReplicationConfig, "test-cell", "standby1"))
		assert.True(t, containsStandbyIDInConfig(finalStatus.SyncReplicationConfig, "test-cell", "standby3"))
		assert.False(t, containsStandbyIDInConfig(finalStatus.SyncReplicationConfig, "test-cell", "standby2"))
		assert.False(t, containsStandbyIDInConfig(finalStatus.SyncReplicationConfig, "test-cell", "standby4"))
	})

	t.Run("UpdateSynchronousStandbyList_Remove_Success", func(t *testing.T) {
		setupPoolerTest(t, setup, WithoutReplication())
		t.Log("Testing UpdateSynchronousStandbyList REMOVE operation...")

		// Configure with three standbys
		configReq := &multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
			NumSync:           2,
			StandbyIds: []*clustermetadatapb.ID{
				makeMultipoolerID("test-cell", "standby1"),
				makeMultipoolerID("test-cell", "standby2"),
				makeMultipoolerID("test-cell", "standby3"),
			},
			ReloadConfig: true,
		}
		_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), configReq)
		require.NoError(t, err)

		// Wait for config to converge
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) bool {
			return config != nil && len(config.StandbyIds) == 3 && containsStandbyIDInConfig(config, "test-cell", "standby2")
		}, "Initial config with 3 standbys should converge")

		// REMOVE standby2
		updateReq := &multipoolermanagerdatapb.UpdateSynchronousStandbyListRequest{
			Operation:     multipoolermanagerdatapb.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_REMOVE,
			StandbyIds:    []*clustermetadatapb.ID{makeMultipoolerID("test-cell", "standby2")},
			ReloadConfig:  true,
			ConsensusTerm: 0,
			Force:         true,
		}
		_, err = primaryManagerClient.UpdateSynchronousStandbyList(utils.WithShortDeadline(t), updateReq)
		require.NoError(t, err, "REMOVE operation should succeed")

		// Wait for config to converge without standby2
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) bool {
			return config != nil && len(config.StandbyIds) == 2 &&
				!containsStandbyIDInConfig(config, "test-cell", "standby2") &&
				containsStandbyIDInConfig(config, "test-cell", "standby1") &&
				containsStandbyIDInConfig(config, "test-cell", "standby3")
		}, "Config should converge without standby2")

		// Verify standby2 was removed
		status := getPrimaryStatusFromClient(t, primaryManagerClient)
		assert.Equal(t, multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_ANY, status.SyncReplicationConfig.SynchronousMethod)
		assert.Equal(t, int32(2), status.SyncReplicationConfig.NumSync)
		assert.Len(t, status.SyncReplicationConfig.StandbyIds, 2)
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby1"))
		assert.False(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby2"))
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby3"))

		t.Log("REMOVE operation verified successfully")
	})

	t.Run("UpdateSynchronousStandbyList_Remove_NonExistent_Idempotent", func(t *testing.T) {
		setupPoolerTest(t, setup, WithoutReplication())
		t.Log("Testing UpdateSynchronousStandbyList REMOVE operation with non-existent standby (idempotency)...")

		// Configure with two standbys
		configReq := &multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           1,
			StandbyIds: []*clustermetadatapb.ID{
				makeMultipoolerID("test-cell", "standby1"),
				makeMultipoolerID("test-cell", "standby2"),
			},
			ReloadConfig: true,
		}
		_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), configReq)
		require.NoError(t, err)

		// Wait for config to converge
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) bool {
			return config != nil && len(config.StandbyIds) == 2
		}, "Initial config with 2 standbys should converge")

		// Get initial state
		initialStatus := getPrimaryStatusFromClient(t, primaryManagerClient)
		require.Len(t, initialStatus.SyncReplicationConfig.StandbyIds, 2, "Should start with 2 standbys")

		// Try to REMOVE a standby that doesn't exist - should be idempotent
		updateReq := &multipoolermanagerdatapb.UpdateSynchronousStandbyListRequest{
			Operation: multipoolermanagerdatapb.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_REMOVE,
			StandbyIds: []*clustermetadatapb.ID{
				makeMultipoolerID("test-cell", "standby-does-not-exist"),
				makeMultipoolerID("test-cell", "another-does-not-exist-standby"),
			},
			ReloadConfig:  true,
			ConsensusTerm: 0,
			Force:         true,
		}
		_, err = primaryManagerClient.UpdateSynchronousStandbyList(utils.WithShortDeadline(t), updateReq)
		require.NoError(t, err, "REMOVE operation should succeed even with non-existent standbys")

		// Verify configuration remains unchanged (idempotent)
		finalStatus := getPrimaryStatusFromClient(t, primaryManagerClient)
		assert.Len(t, finalStatus.SyncReplicationConfig.StandbyIds, 2, "Should still have 2 standbys")
		assert.True(t, containsStandbyIDInConfig(finalStatus.SyncReplicationConfig, "test-cell", "standby1"), "standby1 should still be present")
		assert.True(t, containsStandbyIDInConfig(finalStatus.SyncReplicationConfig, "test-cell", "standby2"), "standby2 should still be present")
		assert.False(t, containsStandbyIDInConfig(finalStatus.SyncReplicationConfig, "test-cell", "standby-does-not-exist"), "non-existent standby should not be present")
		assert.False(t, containsStandbyIDInConfig(finalStatus.SyncReplicationConfig, "test-cell", "another-does-not-exist-standby"), "another non-existent standby should not be present")

		t.Log("REMOVE operation with non-existent standby verified as idempotent")
	})

	t.Run("UpdateSynchronousStandbyList_Replace_Success", func(t *testing.T) {
		setupPoolerTest(t, setup, WithoutReplication())
		t.Log("Testing UpdateSynchronousStandbyList REPLACE operation...")

		// Configure initial set
		configReq := &multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           1,
			StandbyIds: []*clustermetadatapb.ID{
				makeMultipoolerID("test-cell", "standby1"),
				makeMultipoolerID("test-cell", "standby2"),
			},
			ReloadConfig: true,
		}
		_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), configReq)
		require.NoError(t, err)

		// Wait for config to converge
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) bool {
			return config != nil && len(config.StandbyIds) == 2 &&
				containsStandbyIDInConfig(config, "test-cell", "standby1") &&
				containsStandbyIDInConfig(config, "test-cell", "standby2")
		}, "Initial config should converge")

		t.Log("Initial configuration verified")

		// REPLACE with completely different set
		updateReq := &multipoolermanagerdatapb.UpdateSynchronousStandbyListRequest{
			Operation: multipoolermanagerdatapb.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_REPLACE,
			StandbyIds: []*clustermetadatapb.ID{
				makeMultipoolerID("test-cell", "standby3"),
				makeMultipoolerID("test-cell", "standby4"),
			},
			ReloadConfig:  true,
			ConsensusTerm: 0,
			Force:         true,
		}
		_, err = primaryManagerClient.UpdateSynchronousStandbyList(utils.WithShortDeadline(t), updateReq)
		require.NoError(t, err, "REPLACE operation should succeed")

		// Wait for config to converge with new standbys
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) bool {
			return config != nil && len(config.StandbyIds) == 2 &&
				containsStandbyIDInConfig(config, "test-cell", "standby3") &&
				containsStandbyIDInConfig(config, "test-cell", "standby4") &&
				!containsStandbyIDInConfig(config, "test-cell", "standby1") &&
				!containsStandbyIDInConfig(config, "test-cell", "standby2")
		}, "Config should converge with replaced standbys")

		// Verify list was completely replaced
		status := getPrimaryStatusFromClient(t, primaryManagerClient)
		assert.Equal(t, multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST, status.SyncReplicationConfig.SynchronousMethod)
		assert.Equal(t, int32(1), status.SyncReplicationConfig.NumSync)
		assert.Len(t, status.SyncReplicationConfig.StandbyIds, 2)
		assert.False(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby1"))
		assert.False(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby2"))
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby3"))
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby4"))

		t.Log("REPLACE operation verified successfully")
	})

	t.Run("UpdateSynchronousStandbyList_NoSyncReplication_Fails", func(t *testing.T) {
		setupPoolerTest(t, setup)

		t.Log("Testing UpdateSynchronousStandbyList fails when sync replication not configured...")

		// Ensure synchronous replication is not configured
		resetReq := &multipoolermanagerdatapb.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdatapb.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdatapb.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           0,
			StandbyIds:        []*clustermetadatapb.ID{},
			ReloadConfig:      true,
		}
		_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), resetReq)
		require.NoError(t, err)

		// Wait for config to be cleared
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) bool {
			return config != nil && len(config.StandbyIds) == 0
		}, "Config should be cleared")

		// Try to update when no sync replication is configured
		updateReq := &multipoolermanagerdatapb.UpdateSynchronousStandbyListRequest{
			Operation:     multipoolermanagerdatapb.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_ADD,
			StandbyIds:    []*clustermetadatapb.ID{makeMultipoolerID("test-cell", "standby1")},
			ReloadConfig:  true,
			ConsensusTerm: 0,
			Force:         true,
		}
		_, err = primaryManagerClient.UpdateSynchronousStandbyList(utils.WithShortDeadline(t), updateReq)
		require.Error(t, err, "Should fail when sync replication not configured")
		assert.Contains(t, err.Error(), "not configured", "Error should mention sync replication not configured")

		t.Log("Verified: UpdateSynchronousStandbyList correctly fails when sync replication not configured")
	})

	t.Run("UpdateSynchronousStandbyList_Standby_Fails", func(t *testing.T) {
		setupPoolerTest(t, setup)

		t.Log("Testing UpdateSynchronousStandbyList on REPLICA pooler (should fail)...")

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		updateReq := &multipoolermanagerdatapb.UpdateSynchronousStandbyListRequest{
			Operation:     multipoolermanagerdatapb.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_ADD,
			StandbyIds:    []*clustermetadatapb.ID{makeMultipoolerID("test-cell", "standby1")},
			ReloadConfig:  true,
			ConsensusTerm: 0,
			Force:         true,
		}
		_, err := standbyManagerClient.UpdateSynchronousStandbyList(ctx, updateReq)
		require.Error(t, err, "UpdateSynchronousStandbyList should fail on standby")
		assert.Contains(t, err.Error(), "operation not allowed", "Error should indicate operation not allowed on REPLICA")

		t.Log("Confirmed: UpdateSynchronousStandbyList correctly rejected on REPLICA pooler")
	})
}

func TestReplicationStatus(t *testing.T) {
	skip, err := utils.ShouldSkipRealPostgres()
	if skip {
		t.Skip("Skipping end-to-end tests (short mode)")
	}
	require.NoError(t, err, "postgres binaries must be available")

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
			statusResp, err := standbyManagerClient.StandbyReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StandbyReplicationStatusRequest{})
			if err != nil {
				return false
			}
			return statusResp.Status != nil && statusResp.Status.PrimaryConnInfo != nil
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
