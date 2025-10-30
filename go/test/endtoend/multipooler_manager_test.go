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

package endtoend

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

	"github.com/multigres/multigres/go/test/utils"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	multipoolermanagerdata "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// TestMultipoolerPrimaryPosition tests the replication API functionality
func TestMultipoolerPrimaryPosition(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)

	// Wait for both managers to be ready before running tests
	waitForManagerReady(t, setup, setup.PrimaryMultipooler)
	waitForManagerReady(t, setup, setup.StandbyMultipooler)

	t.Run("PrimaryPosition_Primary", func(t *testing.T) {
		// Connect to primary multipooler
		conn, err := grpc.NewClient(
			fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		require.NoError(t, err)
		defer conn.Close()

		client := multipoolermanagerpb.NewMultiPoolerManagerClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		req := &multipoolermanagerdata.PrimaryPositionRequest{}
		resp, err := client.PrimaryPosition(ctx, req)
		if err != nil {
			st, ok := status.FromError(err)
			if ok && st.Message() == "unknown service multipoolermanager.MultiPoolerManager" {
				t.Logf("Got 'unknown service' error - checking multipooler logs:")
				setup.PrimaryMultipooler.logRecentOutput(t, "Debug - unknown service error")
			}
		}

		// Assert that it succeeds and returns a valid LSN
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.NotEmpty(t, resp.LsnPosition, "LSN should not be empty")

		// PostgreSQL LSN format is typically like "0/1234ABCD"
		assert.Contains(t, resp.LsnPosition, "/", "LSN should be in PostgreSQL format (e.g., 0/1234ABCD)")
	})

	t.Run("PrimaryPosition_Standby", func(t *testing.T) {
		// Connect to standby multipooler
		conn, err := grpc.NewClient(
			fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		require.NoError(t, err)
		defer conn.Close()

		client := multipoolermanagerpb.NewMultiPoolerManagerClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		req := &multipoolermanagerdata.PrimaryPositionRequest{}
		_, err = client.PrimaryPosition(ctx, req)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "operation not allowed")
	})
}

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
	primaryPoolerClient, err := NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort))
	require.NoError(t, err)
	t.Cleanup(func() { primaryPoolerClient.Close() })

	standbyPoolerClient, err := NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort))
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
		t.Log("Creating table and inserting data in primary...")
		_, err = primaryPoolerClient.ExecuteQuery(context.Background(), "CREATE TABLE IF NOT EXISTS test_replication (id SERIAL PRIMARY KEY, data TEXT)", 0)
		require.NoError(t, err, "Should be able to create table in primary")

		_, err = primaryPoolerClient.ExecuteQuery(context.Background(), "INSERT INTO test_replication (data) VALUES ('test data')", 0)
		require.NoError(t, err, "Should be able to insert data in primary")

		// Get LSN from primary using PrimaryPosition RPC
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		primaryPosResp, err := primaryManagerClient.PrimaryPosition(ctx, &multipoolermanagerdata.PrimaryPositionRequest{})
		require.NoError(t, err)
		primaryLSN := primaryPosResp.LsnPosition
		t.Logf("Primary LSN after insert: %s", primaryLSN)

		// Validate data is NOT in standby yet (no replication configured)
		t.Log("Validating data is NOT in standby (replication not configured)...")

		// Use WaitForLSN to verify standby cannot reach primary's LSN without replication
		// This should timeout since replication is not configured
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		waitReq := &multipoolermanagerdata.WaitForLSNRequest{
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
		setPrimaryReq := &multipoolermanagerdata.SetPrimaryConnInfoRequest{
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

		waitReq = &multipoolermanagerdata.WaitForLSNRequest{
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

		// Cleanup: Drop the test table from primary
		_, err = primaryPoolerClient.ExecuteQuery(context.Background(), "DROP TABLE IF EXISTS test_replication", 0)
		require.NoError(t, err)
	})

	t.Run("TermMismatchRejected", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		// Try to set primary conn info with stale term (current term is 1, we'll try with 0)
		setPrimaryReq := &multipoolermanagerdata.SetPrimaryConnInfoRequest{
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
		assert.Equal(t, "false", isPaused, "WAL replay should not be paused initially")

		// Call SetPrimaryConnInfo with StopReplicationBefore=true and StartReplicationAfter=false
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		t.Log("Calling SetPrimaryConnInfo with StopReplicationBefore=true, StartReplicationAfter=false...")
		setPrimaryReq := &multipoolermanagerdata.SetPrimaryConnInfoRequest{
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
		assert.Equal(t, "true", isPaused, "WAL replay should be paused after StopReplicationBefore=true")

		t.Log("Replication successfully stopped with StopReplicationBefore flag")

		// Resume replication for cleanup using StartReplication RPC
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		startReq := &multipoolermanagerdata.StartReplicationRequest{}
		_, err = standbyManagerClient.StartReplication(ctx, startReq)
		require.NoError(t, err)
	})

	t.Run("StartReplicationAfterFlag", func(t *testing.T) {
		// This test verifies that replication only starts if StartReplicationAfter=true

		// Stop replication using StopReplication RPC
		t.Log("Stopping replication using StopReplication RPC...")
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		stopReq := &multipoolermanagerdata.StopReplicationRequest{}
		_, err = standbyManagerClient.StopReplication(ctx, stopReq)
		require.NoError(t, err)
		cancel()

		// Verify replication is paused
		queryResp, err := standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT pg_is_wal_replay_paused()", 1)
		require.NoError(t, err)
		require.Len(t, queryResp.Rows, 1)
		isPaused := string(queryResp.Rows[0].Values[0])
		assert.Equal(t, "true", isPaused, "WAL replay should be paused")

		// Call SetPrimaryConnInfo with StartReplicationAfter=false
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		t.Log("Calling SetPrimaryConnInfo with StartReplicationAfter=false...")
		setPrimaryReq := &multipoolermanagerdata.SetPrimaryConnInfoRequest{
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
		assert.Equal(t, "true", isPaused, "WAL replay should still be paused when StartReplicationAfter=false")

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
		assert.Equal(t, "false", isPaused, "WAL replay should be running after StartReplicationAfter=true")

		t.Log("Replication successfully started with StartReplicationAfter flag")
	})

	t.Run("WaitForLSN_Standby_Success", func(t *testing.T) {
		// Insert data on primary to generate a new LSN
		t.Log("Creating table and inserting data on primary...")
		_, err = primaryPoolerClient.ExecuteQuery(context.Background(), "CREATE TABLE IF NOT EXISTS test_wait_lsn (id SERIAL PRIMARY KEY, data TEXT)", 0)
		require.NoError(t, err, "Should be able to create table in primary")

		_, err = primaryPoolerClient.ExecuteQuery(context.Background(), "INSERT INTO test_wait_lsn (data) VALUES ('test data for wait lsn')", 0)
		require.NoError(t, err, "Should be able to insert data in primary")

		// Get LSN from primary
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		primaryPosResp, err := primaryManagerClient.PrimaryPosition(ctx, &multipoolermanagerdata.PrimaryPositionRequest{})
		require.NoError(t, err)
		targetLSN := primaryPosResp.LsnPosition
		t.Logf("Target LSN from primary: %s", targetLSN)

		// Wait for standby to reach the target LSN
		t.Log("Waiting for standby to reach target LSN...")
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		waitReq := &multipoolermanagerdata.WaitForLSNRequest{
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

		// Cleanup
		_, err = primaryPoolerClient.ExecuteQuery(context.Background(), "DROP TABLE IF EXISTS test_wait_lsn", 0)
		require.NoError(t, err)
	})

	t.Run("WaitForLSN_Primary_Fails", func(t *testing.T) {
		// WaitForLSN should fail on PRIMARY pooler type
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		waitReq := &multipoolermanagerdata.WaitForLSNRequest{
			TargetLsn: "0/1000000",
		}
		_, err = primaryManagerClient.WaitForLSN(ctx, waitReq)
		require.Error(t, err, "WaitForLSN should fail on primary")
		assert.Contains(t, err.Error(), "operation not allowed", "Error should indicate operation not allowed on PRIMARY")
	})

	t.Run("WaitForLSN_Timeout", func(t *testing.T) {
		// Test timeout behavior by waiting for a very high LSN that won't be reached
		t.Log("Testing timeout with unreachable LSN...")

		// Use a very high LSN that won't be reached in the timeout period
		unreachableLSN := "FF/FFFFFFFF"

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		waitReq := &multipoolermanagerdata.WaitForLSNRequest{
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
		// This test verifies that StartReplication successfully resumes WAL replay on standby

		// First stop replication using StopReplication RPC
		t.Log("Stopping replication using StopReplication RPC...")
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		stopReq := &multipoolermanagerdata.StopReplicationRequest{}
		_, err = standbyManagerClient.StopReplication(ctx, stopReq)
		require.NoError(t, err)
		cancel()

		// Verify replication is paused
		queryResp, err := standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT pg_is_wal_replay_paused()", 1)
		require.NoError(t, err)
		require.Len(t, queryResp.Rows, 1)
		isPaused := string(queryResp.Rows[0].Values[0])
		assert.Equal(t, "true", isPaused, "WAL replay should be paused")
		t.Log("Confirmed: WAL replay is paused")

		// Call StartReplication RPC
		t.Log("Calling StartReplication RPC...")
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		startReq := &multipoolermanagerdata.StartReplicationRequest{}
		_, err = standbyManagerClient.StartReplication(ctx, startReq)
		require.NoError(t, err, "StartReplication should succeed on standby")

		// Verify replication is now running
		t.Log("Verifying replication is running after StartReplication...")
		queryResp, err = standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT pg_is_wal_replay_paused()", 1)
		require.NoError(t, err)
		require.Len(t, queryResp.Rows, 1)
		isPaused = string(queryResp.Rows[0].Values[0])
		assert.Equal(t, "false", isPaused, "WAL replay should be running after StartReplication")

		t.Log("StartReplication successfully resumed WAL replay")
	})

	t.Run("StartReplication_Primary_Fails", func(t *testing.T) {
		// StartReplication should fail on PRIMARY pooler type
		t.Log("Testing StartReplication on PRIMARY pooler (should fail)...")

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		startReq := &multipoolermanagerdata.StartReplicationRequest{}
		_, err = primaryManagerClient.StartReplication(ctx, startReq)
		require.Error(t, err, "StartReplication should fail on primary")
		assert.Contains(t, err.Error(), "operation not allowed", "Error should indicate operation not allowed on PRIMARY")
		t.Log("Confirmed: StartReplication correctly rejected on PRIMARY pooler")
	})

	t.Run("StopReplication_Success", func(t *testing.T) {
		// This test verifies that StopReplication successfully pauses WAL replay on standby

		// First ensure replication is running
		t.Log("Ensuring replication is running...")
		queryResp, err := standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT pg_is_wal_replay_paused()", 1)
		require.NoError(t, err)
		require.Len(t, queryResp.Rows, 1)
		isPaused := string(queryResp.Rows[0].Values[0])
		if isPaused == "true" {
			// Resume it first using StartReplication RPC
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			startReq := &multipoolermanagerdata.StartReplicationRequest{}
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

		stopReq := &multipoolermanagerdata.StopReplicationRequest{}
		_, err = standbyManagerClient.StopReplication(ctx, stopReq)
		require.NoError(t, err, "StopReplication should succeed on standby")

		// Verify replication is now paused (should be immediate since StopReplication waits)
		t.Log("Verifying replication is paused after StopReplication...")
		queryResp, err = standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT pg_is_wal_replay_paused()", 1)
		require.NoError(t, err)
		require.Len(t, queryResp.Rows, 1)
		isPaused = string(queryResp.Rows[0].Values[0])
		assert.Equal(t, "true", isPaused, "WAL replay should be paused after StopReplication")

		t.Log("StopReplication successfully paused WAL replay")

		// Resume replication for cleanup using StartReplication RPC
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		startReq := &multipoolermanagerdata.StartReplicationRequest{}
		_, err = standbyManagerClient.StartReplication(ctx, startReq)
		require.NoError(t, err)
	})

	t.Run("StopReplication_Primary_Fails", func(t *testing.T) {
		// StopReplication should fail on PRIMARY pooler type
		t.Log("Testing StopReplication on PRIMARY pooler (should fail)...")

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		stopReq := &multipoolermanagerdata.StopReplicationRequest{}
		_, err = primaryManagerClient.StopReplication(ctx, stopReq)
		require.Error(t, err, "StopReplication should fail on primary")
		assert.Contains(t, err.Error(), "operation not allowed", "Error should indicate operation not allowed on PRIMARY")
		t.Log("Confirmed: StopReplication correctly rejected on PRIMARY pooler")
	})

	t.Run("ResetReplication_Success", func(t *testing.T) {
		// This test verifies that ResetReplication successfully disconnects the standby from the primary
		// and that data inserted after reset does not replicate until replication is re-enabled

		// First ensure replication is configured
		t.Log("Ensuring replication is configured...")
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		setPrimaryReq := &multipoolermanagerdata.SetPrimaryConnInfoRequest{
			Host:                  "localhost",
			Port:                  int32(setup.PrimaryPgctld.PgPort),
			StartReplicationAfter: true,
			StopReplicationBefore: false,
			CurrentTerm:           1,
			Force:                 false,
		}
		_, err = standbyManagerClient.SetPrimaryConnInfo(ctx, setPrimaryReq)
		require.NoError(t, err, "SetPrimaryConnInfo should succeed")
		cancel()

		// Verify replication is working by checking pg_stat_wal_receiver
		t.Log("Verifying replication is working...")
		require.Eventually(t, func() bool {
			queryResp, err := standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT COUNT(*) FROM pg_stat_wal_receiver WHERE status = 'streaming'", 1)
			if err != nil || len(queryResp.Rows) == 0 {
				return false
			}
			count := string(queryResp.Rows[0].Values[0])
			return count == "1"
		}, 10*time.Second, 500*time.Millisecond, "Replication should be streaming")
		t.Log("Confirmed: Replication is streaming")

		// Call ResetReplication RPC
		t.Log("Calling ResetReplication RPC...")
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		resetReq := &multipoolermanagerdata.ResetReplicationRequest{}
		_, err = standbyManagerClient.ResetReplication(ctx, resetReq)
		require.NoError(t, err, "ResetReplication should succeed on standby")

		// Verify that primary_conninfo is cleared by checking pg_stat_wal_receiver
		// After resetting, the WAL receiver should eventually disconnect
		t.Log("Verifying replication is disconnected after ResetReplication...")
		require.Eventually(t, func() bool {
			queryResp, err := standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT COUNT(*) FROM pg_stat_wal_receiver", 1)
			if err != nil || len(queryResp.Rows) == 0 {
				return false
			}
			count := string(queryResp.Rows[0].Values[0])
			return count == "0"
		}, 10*time.Second, 500*time.Millisecond, "WAL receiver should disconnect after ResetReplication")

		t.Log("ResetReplication successfully disconnected standby from primary")

		// Sanity check: Insert a row on primary, verify it does NOT replicate to standby
		t.Log("Sanity check: Inserting data on primary after ResetReplication...")
		_, err = primaryPoolerClient.ExecuteQuery(context.Background(), "CREATE TABLE IF NOT EXISTS test_reset_replication (id SERIAL PRIMARY KEY, data TEXT)", 0)
		require.NoError(t, err, "Should be able to create table on primary")

		_, err = primaryPoolerClient.ExecuteQuery(context.Background(), "INSERT INTO test_reset_replication (data) VALUES ('should not replicate')", 0)
		require.NoError(t, err, "Should be able to insert data on primary")

		// Get LSN from primary after the insert
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		primaryPosResp, err := primaryManagerClient.PrimaryPosition(ctx, &multipoolermanagerdata.PrimaryPositionRequest{})
		require.NoError(t, err)
		primaryLSNAfterInsert := primaryPosResp.LsnPosition
		t.Logf("Primary LSN after insert: %s", primaryLSNAfterInsert)
		cancel()

		// Verify standby CANNOT reach the primary LSN (replication is disconnected)
		t.Log("Verifying standby cannot reach primary LSN (replication disconnected)...")
		waitReq := &multipoolermanagerdata.WaitForLSNRequest{
			TargetLsn: primaryLSNAfterInsert,
		}
		_, err = standbyManagerClient.WaitForLSN(utils.WithShortDeadline(t), waitReq)
		require.Error(t, err, "WaitForLSN should timeout since replication is disconnected")
		t.Log("Confirmed: Standby cannot reach primary LSN (data did NOT replicate)")

		// Re-enable replication using SetPrimaryConnInfo
		t.Log("Re-enabling replication...")
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		setPrimaryReq = &multipoolermanagerdata.SetPrimaryConnInfoRequest{
			Host:                  "localhost",
			Port:                  int32(setup.PrimaryPgctld.PgPort),
			StartReplicationAfter: true,
			StopReplicationBefore: false,
			CurrentTerm:           1,
			Force:                 false,
		}
		_, err = standbyManagerClient.SetPrimaryConnInfo(ctx, setPrimaryReq)
		require.NoError(t, err, "SetPrimaryConnInfo should succeed")
		cancel()

		// Wait for standby to catch up to primary's LSN
		t.Logf("Waiting for standby to catch up to primary LSN: %s", primaryLSNAfterInsert)
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		waitReq = &multipoolermanagerdata.WaitForLSNRequest{
			TargetLsn: primaryLSNAfterInsert,
		}
		_, err = standbyManagerClient.WaitForLSN(ctx, waitReq)
		require.NoError(t, err, "Standby should catch up after re-enabling replication")

		// Verify the table now exists on standby
		t.Log("Verifying data replicated after re-enabling replication...")
		dataResp, err := standbyPoolerClient.ExecuteQuery(context.Background(), "SELECT COUNT(*) FROM test_reset_replication", 1)
		require.NoError(t, err)
		require.Len(t, dataResp.Rows, 1)
		rowCount := string(dataResp.Rows[0].Values[0])
		assert.Equal(t, "1", rowCount, "Should have 1 row on standby after re-enabling replication")

		t.Log("Confirmed: Data successfully replicated after re-enabling replication")

		// Cleanup: Drop the test table from primary
		_, err = primaryPoolerClient.ExecuteQuery(context.Background(), "DROP TABLE IF EXISTS test_reset_replication", 0)
		require.NoError(t, err)
	})

	t.Run("ResetReplication_Primary_Fails", func(t *testing.T) {
		// ResetReplication should fail on PRIMARY pooler type
		t.Log("Testing ResetReplication on PRIMARY pooler (should fail)...")

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		resetReq := &multipoolermanagerdata.ResetReplicationRequest{}
		_, err = primaryManagerClient.ResetReplication(ctx, resetReq)
		require.Error(t, err, "ResetReplication should fail on primary")
		assert.Contains(t, err.Error(), "operation not allowed", "Error should indicate operation not allowed on PRIMARY")
		t.Log("Confirmed: ResetReplication correctly rejected on PRIMARY pooler")
	})
}

// TestReplicationStatus tests the ReplicationStatus API
func TestReplicationStatus(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

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

	t.Run("ReplicationStatus_Primary_Fails", func(t *testing.T) {
		// ReplicationStatus should fail on PRIMARY pooler type
		t.Log("Testing ReplicationStatus on PRIMARY (should fail)...")

		_, err := primaryManagerClient.ReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.ReplicationStatusRequest{})
		require.Error(t, err, "ReplicationStatus should fail on PRIMARY")
		assert.Contains(t, err.Error(), "operation not allowed", "Error should indicate operation not allowed on PRIMARY")
		t.Log("Confirmed: ReplicationStatus correctly rejected on PRIMARY pooler")
	})

	t.Run("ReplicationStatus_Standby_NoReplication", func(t *testing.T) {
		// Test ReplicationStatus on standby when replication is not configured
		t.Log("Testing ReplicationStatus on standby with no replication configured...")

		// First, ensure replication is stopped
		_, err := standbyManagerClient.ResetReplication(utils.WithShortDeadline(t), &multipoolermanagerdata.ResetReplicationRequest{})
		require.NoError(t, err, "ResetReplication should succeed")

		// Wait for config to take effect (pg_reload_conf is async)
		t.Log("Waiting for primary_conninfo to be cleared...")
		require.Eventually(t, func() bool {
			statusResp, err := standbyManagerClient.ReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.ReplicationStatusRequest{})
			if err != nil {
				t.Logf("ReplicationStatus error: %v", err)
				return false
			}
			// Config cleared when PrimaryConnInfo is nil or Host is empty
			return statusResp.Status.PrimaryConnInfo == nil ||
				statusResp.Status.PrimaryConnInfo.Host == ""
		}, 5*time.Second, 200*time.Millisecond, "primary_conninfo should be cleared after ResetReplication")

		// Get final status
		statusResp, err := standbyManagerClient.ReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.ReplicationStatusRequest{})
		require.NoError(t, err, "ReplicationStatus should succeed on standby")
		require.NotNil(t, statusResp.Status, "Status should not be nil")

		// Verify fields
		assert.NotEmpty(t, statusResp.Status.Lsn, "LSN should not be empty")
		assert.False(t, statusResp.Status.IsWalReplayPaused, "WAL replay should not be paused by default")

		// PrimaryConnInfo should be nil or empty when no replication is configured
		if statusResp.Status.PrimaryConnInfo != nil {
			assert.Empty(t, statusResp.Status.PrimaryConnInfo.Host, "Host should be empty when no replication configured")
		}
	})

	t.Run("ReplicationStatus_Standby_WithReplication", func(t *testing.T) {
		// Configure replication
		t.Log("Configuring replication on standby...")
		setPrimaryReq := &multipoolermanagerdata.SetPrimaryConnInfoRequest{
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
			statusResp, err := standbyManagerClient.ReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.ReplicationStatusRequest{})
			if err != nil {
				t.Logf("ReplicationStatus error: %v", err)
				return false
			}
			// Config set when PrimaryConnInfo is not nil and Host is populated
			return statusResp.Status.PrimaryConnInfo != nil &&
				statusResp.Status.PrimaryConnInfo.Host != ""
		}, 5*time.Second, 200*time.Millisecond, "primary_conninfo should be set after SetPrimaryConnInfo")

		// Get final status
		statusResp, err := standbyManagerClient.ReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.ReplicationStatusRequest{})
		require.NoError(t, err, "ReplicationStatus should succeed")
		require.NotNil(t, statusResp.Status, "Status should not be nil")

		// Verify LSN
		assert.NotEmpty(t, statusResp.Status.Lsn, "LSN should not be empty")
		assert.Contains(t, statusResp.Status.Lsn, "/", "LSN should be in PostgreSQL format (e.g., 0/1234ABCD)")

		// Verify replication is not paused
		assert.False(t, statusResp.Status.IsWalReplayPaused, "WAL replay should not be paused")

		// Verify PrimaryConnInfo is populated
		require.NotNil(t, statusResp.Status.PrimaryConnInfo, "PrimaryConnInfo should not be nil")
		assert.Equal(t, "localhost", statusResp.Status.PrimaryConnInfo.Host, "Host should match")
		assert.Equal(t, int32(setup.PrimaryPgctld.PgPort), statusResp.Status.PrimaryConnInfo.Port, "Port should match")
		assert.NotEmpty(t, statusResp.Status.PrimaryConnInfo.Raw, "Raw connection string should not be empty")
	})

	t.Run("ReplicationStatus_Standby_PausedReplication", func(t *testing.T) {
		// Configure replication but stop it
		t.Log("Configuring replication and then stopping it...")
		setPrimaryReq := &multipoolermanagerdata.SetPrimaryConnInfoRequest{
			Host:                  "localhost",
			Port:                  int32(setup.PrimaryPgctld.PgPort),
			StartReplicationAfter: false,
			StopReplicationBefore: true,
			CurrentTerm:           1,
			Force:                 false,
		}
		_, err := standbyManagerClient.SetPrimaryConnInfo(utils.WithShortDeadline(t), setPrimaryReq)
		require.NoError(t, err, "SetPrimaryConnInfo should succeed")

		// Wait for config to take effect and WAL replay to be paused
		t.Log("Waiting for WAL replay to be paused...")
		require.Eventually(t, func() bool {
			statusResp, err := standbyManagerClient.ReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.ReplicationStatusRequest{})
			if err != nil {
				t.Logf("ReplicationStatus error: %v", err)
				return false
			}
			return statusResp.Status.IsWalReplayPaused
		}, 5*time.Second, 200*time.Millisecond, "WAL replay should be paused after SetPrimaryConnInfo with StopReplicationBefore")

		// Get final status
		statusResp, err := standbyManagerClient.ReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.ReplicationStatusRequest{})
		require.NoError(t, err, "ReplicationStatus should succeed")
		require.NotNil(t, statusResp.Status, "Status should not be nil")

		// Verify WAL replay is paused
		assert.True(t, statusResp.Status.IsWalReplayPaused, "WAL replay should be paused")
		assert.NotEmpty(t, statusResp.Status.WalReplayPauseState, "Pause state should not be empty")
		// Clean up: resume replication
		startReq := &multipoolermanagerdata.StartReplicationRequest{}
		_, err = standbyManagerClient.StartReplication(utils.WithShortDeadline(t), startReq)
		require.NoError(t, err, "StartReplication should succeed")
	})
}

// TestStopReplicationAndGetStatus tests the StopReplicationAndGetStatus API
func TestStopReplicationAndGetStatus(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

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
		// StopReplicationAndGetStatus should fail on PRIMARY pooler type
		t.Log("Testing StopReplicationAndGetStatus on PRIMARY (should fail)...")

		_, err := primaryManagerClient.StopReplicationAndGetStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.StopReplicationAndGetStatusRequest{})
		require.Error(t, err, "StopReplicationAndGetStatus should fail on PRIMARY")
		assert.Contains(t, err.Error(), "operation not allowed", "Error should indicate operation not allowed on PRIMARY")
		t.Log("Confirmed: StopReplicationAndGetStatus correctly rejected on PRIMARY pooler")
	})

	t.Run("StopReplicationAndGetStatus_Standby_Success", func(t *testing.T) {
		// This test verifies that StopReplicationAndGetStatus stops replication and returns correct status
		t.Log("Testing StopReplicationAndGetStatus on standby with running replication...")

		// Connect to primary pooler to write test data
		primaryPoolerClient, err := NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort))
		require.NoError(t, err)
		defer primaryPoolerClient.Close()

		// First, configure and start replication
		t.Log("Configuring replication on standby...")
		setPrimaryReq := &multipoolermanagerdata.SetPrimaryConnInfoRequest{
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
			statusResp, err := standbyManagerClient.ReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.ReplicationStatusRequest{})
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
		stopResp, err := standbyManagerClient.StopReplicationAndGetStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.StopReplicationAndGetStatusRequest{})
		require.NoError(t, err, "StopReplicationAndGetStatus should succeed on standby")
		require.NotNil(t, stopResp, "Response should not be nil")
		require.NotNil(t, stopResp.Status, "Status should not be nil")

		t.Log("Verifying status shows replication is paused...")
		assert.True(t, stopResp.Status.IsWalReplayPaused, "WAL replay should be paused after StopReplicationAndGetStatus")
		assert.NotEmpty(t, stopResp.Status.WalReplayPauseState, "Pause state should not be empty")

		// Verify LSN is populated
		assert.NotEmpty(t, stopResp.Status.Lsn, "LSN should not be empty")
		assert.Contains(t, stopResp.Status.Lsn, "/", "LSN should be in PostgreSQL format")

		// Verify PrimaryConnInfo is populated (should still be set even though replication is paused)
		require.NotNil(t, stopResp.Status.PrimaryConnInfo, "PrimaryConnInfo should not be nil")
		assert.Equal(t, "localhost", stopResp.Status.PrimaryConnInfo.Host, "Host should match")
		assert.Equal(t, int32(setup.PrimaryPgctld.PgPort), stopResp.Status.PrimaryConnInfo.Port, "Port should match")
		assert.NotEmpty(t, stopResp.Status.PrimaryConnInfo.Raw, "Raw connection string should not be empty")

		// Store the LSN after stopping
		lsnAfterStop := stopResp.Status.Lsn
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
		statusAfterWrite, err := standbyManagerClient.ReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.ReplicationStatusRequest{})
		require.NoError(t, err, "ReplicationStatus should succeed")
		lsnAfterWrite := statusAfterWrite.Status.Lsn
		t.Logf("Standby LSN after writes to primary: %s", lsnAfterWrite)

		assert.Equal(t, lsnAfterStop, lsnAfterWrite, "Standby LSN should not have advanced after primary writes (replication is stopped)")
		t.Log("Confirmed: Standby LSN did not advance, replication is truly stopped")

		t.Log("StopReplicationAndGetStatus successfully stopped replication and returned correct status")

		// Clean up: resume replication for next tests
		startReq := &multipoolermanagerdata.StartReplicationRequest{}
		_, err = standbyManagerClient.StartReplication(utils.WithShortDeadline(t), startReq)
		require.NoError(t, err, "StartReplication should succeed during cleanup")

		// Wait for replication to actually resume (pg_wal_replay_resume is also async)
		t.Log("Waiting for WAL replay to resume after cleanup...")
		require.Eventually(t, func() bool {
			statusResp, err := standbyManagerClient.ReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.ReplicationStatusRequest{})
			if err != nil {
				return false
			}
			return !statusResp.Status.IsWalReplayPaused
		}, 5*time.Second, 100*time.Millisecond, "WAL replay should resume after StartReplication")
	})

	t.Run("StopReplicationAndGetStatus_Standby_AlreadyPaused", func(t *testing.T) {
		// This test verifies that StopReplicationAndGetStatus works even when replication is already paused
		t.Log("Testing StopReplicationAndGetStatus when replication is already paused...")

		// First, stop replication
		// StopReplication now waits internally for the pause to complete, so no manual wait needed
		t.Log("Stopping replication first...")
		_, err := standbyManagerClient.StopReplication(utils.WithShortDeadline(t), &multipoolermanagerdata.StopReplicationRequest{})
		require.NoError(t, err, "StopReplication should succeed")

		// Call StopReplicationAndGetStatus (should succeed even though already paused)
		// Note: This method waits internally for pause to complete, so status is guaranteed to be paused when it returns
		t.Log("Calling StopReplicationAndGetStatus on already paused replication...")
		stopResp, err := standbyManagerClient.StopReplicationAndGetStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.StopReplicationAndGetStatusRequest{})
		require.NoError(t, err, "StopReplicationAndGetStatus should succeed even when already paused")
		require.NotNil(t, stopResp, "Response should not be nil")
		require.NotNil(t, stopResp.Status, "Status should not be nil")

		assert.True(t, stopResp.Status.IsWalReplayPaused, "WAL replay should be paused")
		assert.NotEmpty(t, stopResp.Status.Lsn, "LSN should not be empty")

		t.Log("StopReplicationAndGetStatus successfully handled already-paused replication")

		// Clean up: resume replication
		startReq := &multipoolermanagerdata.StartReplicationRequest{}
		_, err = standbyManagerClient.StartReplication(utils.WithShortDeadline(t), startReq)
		require.NoError(t, err, "StartReplication should succeed during cleanup")

		// Wait for replication to actually resume (pg_wal_replay_resume is also async)
		t.Log("Waiting for WAL replay to resume...")
		require.Eventually(t, func() bool {
			statusResp, err := standbyManagerClient.ReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.ReplicationStatusRequest{})
			if err != nil {
				return false
			}
			return !statusResp.Status.IsWalReplayPaused
		}, 5*time.Second, 100*time.Millisecond, "WAL replay should resume after StartReplication")
	})
}

// TestConfigureSynchronousReplication tests the ConfigureSynchronousReplication API
func TestConfigureSynchronousReplication(t *testing.T) {
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

	primaryPoolerClient, err := NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort))
	require.NoError(t, err)
	t.Cleanup(func() { primaryPoolerClient.Close() })

	t.Run("ConfigureSynchronousReplication_Primary_Success", func(t *testing.T) {
		// Register cleanup to reset replication config
		setupReplicationTestCleanup(t, setup)

		// This test verifies that ConfigureSynchronousReplication successfully configures
		// synchronous replication on the primary
		t.Log("Testing ConfigureSynchronousReplication on PRIMARY...")

		// The application_name used by standby is: {cell}_{name}
		// For test purposes, we'll use a simple standby name
		standbyAppName := "test-standby"

		// Configure synchronous replication with FIRST method
		req := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           1,
			StandbyIds:        []*clustermetadatapb.ID{makeMultipoolerID("test-cell", "test-standby")},
			ReloadConfig:      true,
		}
		_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), req)
		require.NoError(t, err, "ConfigureSynchronousReplication should succeed on primary")

		t.Log("ConfigureSynchronousReplication completed successfully")

		// Wait for configuration to converge and verify using PrimaryStatus API
		t.Log("Waiting for configuration to converge...")
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
			return config != nil &&
				config.SynchronousCommit == multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON &&
				config.SynchronousMethod == multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST &&
				config.NumSync == 1 &&
				containsStandbyIDInConfig(config, "test-cell", standbyAppName)
		}, "Configuration should converge to expected values")

		t.Log("Synchronous replication configured and verified successfully")
	})

	t.Run("ConfigureSynchronousReplication_Primary_AnyMethod", func(t *testing.T) {
		// Register cleanup to reset replication config
		setupReplicationTestCleanup(t, setup)

		// This test verifies that ConfigureSynchronousReplication works with ANY method
		t.Log("Testing ConfigureSynchronousReplication with ANY method on PRIMARY...")

		// Use multiple standby IDs to test the ANY method with multiple standbys
		standbyIDs := []*clustermetadatapb.ID{
			makeMultipoolerID("test-cell", "test-standby-1"),
			makeMultipoolerID("test-cell", "test-standby-2"),
		}

		// Configure synchronous replication with ANY method
		req := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_APPLY,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
			NumSync:           1,
			StandbyIds:        standbyIDs,
			ReloadConfig:      true,
		}
		_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), req)
		require.NoError(t, err, "ConfigureSynchronousReplication should succeed on primary")

		t.Log("ConfigureSynchronousReplication with ANY method completed successfully")

		// Wait for configuration to converge and verify using PrimaryStatus API
		t.Log("Waiting for configuration to converge...")
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
			return config != nil &&
				config.SynchronousCommit == multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_APPLY &&
				config.SynchronousMethod == multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_ANY &&
				config.NumSync == 1 &&
				len(config.StandbyIds) == 2
		}, "Configuration should converge to expected values")

		t.Log("Synchronous replication with ANY method configured and verified successfully")
	})

	t.Run("ConfigureSynchronousReplication_AllCommitLevels", func(t *testing.T) {
		setupReplicationTestCleanup(t, setup)
		// This test verifies that all SynchronousCommitLevel values work correctly
		t.Log("Testing ConfigureSynchronousReplication with all commit levels...")

		testCases := []struct {
			level multipoolermanagerdata.SynchronousCommitLevel
		}{
			{
				level: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_OFF,
			},
			{
				level: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_LOCAL,
			},
			{
				level: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_WRITE,
			},
			{
				level: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			},
			{
				level: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_APPLY,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.level.String(), func(t *testing.T) {
				// Configure with this commit level
				req := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
					SynchronousCommit: tc.level,
					SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
					NumSync:           1,
					StandbyIds:        []*clustermetadatapb.ID{makeMultipoolerID("test-cell", "test-standby")},
					ReloadConfig:      true,
				}
				_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), req)
				require.NoError(t, err, "ConfigureSynchronousReplication should succeed for %s", tc.level.String())

				t.Logf("ConfigureSynchronousReplication with %s completed successfully", tc.level.String())

				// Wait for configuration to converge and verify using PrimaryStatus API
				t.Logf("Waiting for configuration to converge to %s...", tc.level.String())
				waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
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
		setupReplicationTestCleanup(t, setup)
		// This test verifies that FIRST and ANY methods work correctly with different num_sync values
		t.Log("Testing ConfigureSynchronousReplication with all synchronous methods...")

		testCases := []struct {
			name       string
			method     multipoolermanagerdata.SynchronousMethod
			numSync    int32
			standbyIDs []*clustermetadatapb.ID
		}{
			{
				name:    "FIRST_1_SingleStandby",
				method:  multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
				numSync: 1,
				standbyIDs: []*clustermetadatapb.ID{
					makeMultipoolerID("test-cell", "standby-1"),
				},
			},
			{
				name:    "FIRST_1_MultipleStandbys",
				method:  multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
				numSync: 1,
				standbyIDs: []*clustermetadatapb.ID{
					makeMultipoolerID("test-cell", "standby-1"),
					makeMultipoolerID("test-cell", "standby-2"),
					makeMultipoolerID("test-cell", "standby-3"),
				},
			},
			{
				name:    "FIRST_2_MultipleStandbys",
				method:  multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
				numSync: 2,
				standbyIDs: []*clustermetadatapb.ID{
					makeMultipoolerID("test-cell", "standby-1"),
					makeMultipoolerID("test-cell", "standby-2"),
					makeMultipoolerID("test-cell", "standby-3"),
				},
			},
			{
				name:    "FIRST_3_MultipleStandbys",
				method:  multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
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
				method:  multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
				numSync: 1,
				standbyIDs: []*clustermetadatapb.ID{
					makeMultipoolerID("test-cell", "standby-1"),
				},
			},
			{
				name:    "ANY_1_MultipleStandbys",
				method:  multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
				numSync: 1,
				standbyIDs: []*clustermetadatapb.ID{
					makeMultipoolerID("test-cell", "standby-1"),
					makeMultipoolerID("test-cell", "standby-2"),
					makeMultipoolerID("test-cell", "standby-3"),
				},
			},
			{
				name:    "ANY_2_MultipleStandbys",
				method:  multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
				numSync: 2,
				standbyIDs: []*clustermetadatapb.ID{
					makeMultipoolerID("test-cell", "standby-1"),
					makeMultipoolerID("test-cell", "standby-2"),
					makeMultipoolerID("test-cell", "standby-3"),
				},
			},
			{
				name:    "ANY_3_MultipleStandbys",
				method:  multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
				numSync: 3,
				standbyIDs: []*clustermetadatapb.ID{
					makeMultipoolerID("test-cell", "standby-1"),
					makeMultipoolerID("test-cell", "standby-2"),
					makeMultipoolerID("test-cell", "standby-3"),
					makeMultipoolerID("test-cell", "standby-4"),
				},
			},
		}

		setupReplicationTestCleanup(t, setup)
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Configure with this synchronous method
				req := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
					SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
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
				waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
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
		setupReplicationTestCleanup(t, setup)
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
		configReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_APPLY,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           1,
			StandbyIds:        []*clustermetadatapb.ID{standbyID},
			ReloadConfig:      true,
		}
		_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), configReq)
		require.NoError(t, err, "ConfigureSynchronousReplication should succeed on primary")

		// Ensure standby is connected and replicating
		t.Log("Ensuring standby is connected to primary and replicating...")
		setPrimaryReq := &multipoolermanagerdata.SetPrimaryConnInfoRequest{
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
			statusResp, err := standbyManagerClient.ReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.ReplicationStatusRequest{})
			if err != nil {
				return false
			}
			return statusResp.Status.PrimaryConnInfo != nil &&
				statusResp.Status.PrimaryConnInfo.Host != "" &&
				!statusResp.Status.IsWalReplayPaused
		}, 5*time.Second, 200*time.Millisecond, "Replication should be configured and active")

		// Verify standby is connected and replicating using ReplicationStatus API
		t.Log("Verifying standby is connected and replicating...")
		statusResp, err := standbyManagerClient.ReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.ReplicationStatusRequest{})
		require.NoError(t, err, "ReplicationStatus should succeed")
		require.NotNil(t, statusResp.Status, "Status should not be nil")
		require.NotNil(t, statusResp.Status.PrimaryConnInfo, "PrimaryConnInfo should not be nil")
		t.Logf("Standby replication status: LSN=%s, is_paused=%v, pause_state=%s, primary_conn_info=%s",
			statusResp.Status.Lsn, statusResp.Status.IsWalReplayPaused, statusResp.Status.WalReplayPauseState, statusResp.Status.PrimaryConnInfo.Raw)

		// Verify standby is not paused
		require.False(t, statusResp.Status.IsWalReplayPaused, "Standby should be actively replicating (not paused)")

		// Verify primary_conninfo contains the expected application_name
		require.Equal(t, standbyAppName, statusResp.Status.PrimaryConnInfo.ApplicationName,
			"PrimaryConnInfo.ApplicationName should match expected standby application name")

		// Test write with synchronous replication enabled
		t.Log("Testing write with synchronous replication enabled...")

		// Reconnect to pick up the new synchronous_standby_names configuration
		primaryPoolerClient.Close()
		primaryPoolerClient, err = NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort))
		require.NoError(t, err)
		t.Cleanup(func() { primaryPoolerClient.Close() })

		// Create a test table and insert data - this should succeed because standby is available
		_, err = primaryPoolerClient.ExecuteQuery(context.Background(), "CREATE TABLE IF NOT EXISTS test_sync_repl (id SERIAL PRIMARY KEY, data TEXT)", 0)
		require.NoError(t, err, "Table creation should succeed with standby available")

		_, err = primaryPoolerClient.ExecuteQuery(context.Background(), "INSERT INTO test_sync_repl (data) VALUES ('test-with-standby')", 0)
		require.NoError(t, err, "Insert should succeed with standby connected and replicating")
		t.Log("Write succeeded with synchronous replication enabled")

		// Verify standby caught up to primary after the successful write
		t.Log("Verifying standby caught up to primary after successful write...")
		primaryPosResp, err := primaryManagerClient.PrimaryPosition(utils.WithShortDeadline(t), &multipoolermanagerdata.PrimaryPositionRequest{})
		require.NoError(t, err, "Should be able to get primary position")
		primaryLSN := primaryPosResp.LsnPosition
		t.Logf("Primary LSN after write: %s", primaryLSN)

		waitReq := &multipoolermanagerdata.WaitForLSNRequest{
			TargetLsn: primaryLSN,
		}
		_, err = standbyManagerClient.WaitForLSN(utils.WithShortDeadline(t), waitReq)
		require.NoError(t, err, "Standby should have caught up to primary after successful write")
		t.Log("Standby successfully caught up to primary")

		// Disconnect standby using ResetReplication
		t.Log("Disconnecting standby using ResetReplication...")
		_, err = standbyManagerClient.ResetReplication(utils.WithShortDeadline(t), &multipoolermanagerdata.ResetReplicationRequest{})
		require.NoError(t, err, "ResetReplication should succeed")

		// Wait for standby to fully disconnect
		t.Log("Waiting for standby to disconnect...")
		require.Eventually(t, func() bool {
			standbyPoolerClient, err := NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort))
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
		standbyStatusBefore, err := standbyManagerClient.ReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.ReplicationStatusRequest{})
		require.NoError(t, err, "Should be able to get standby status")
		standbyLSNBefore := standbyStatusBefore.Status.Lsn
		t.Logf("Standby LSN before write attempt: %s", standbyLSNBefore)

		// Test write timeout without standby
		t.Log("Testing write timeout without standby available...")
		// Create a new connection for this test
		primaryPoolerClient.Close()
		primaryPoolerClient, err = NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort))
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
		standbyStatusAfter, err := standbyManagerClient.ReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.ReplicationStatusRequest{})
		require.NoError(t, err, "Should be able to get standby status")
		standbyLSNAfter := standbyStatusAfter.Status.Lsn
		t.Logf("Standby LSN after failed write: %s", standbyLSNAfter)
		assert.Equal(t, standbyLSNBefore, standbyLSNAfter, "Standby LSN should not have advanced since replication is disconnected and write failed")

		// Reset synchronous replication to defaults
		resetReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           0,
			StandbyIds:        []*clustermetadatapb.ID{},
			ReloadConfig:      true,
		}
		_, err = primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), resetReq)
		require.NoError(t, err, "Reset configuration should succeed")

		// Drop test table
		primaryPoolerClient.Close()
		primaryPoolerClient, err = NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort))
		require.NoError(t, err)
		t.Cleanup(func() { primaryPoolerClient.Close() })
		_, err = primaryPoolerClient.ExecuteQuery(context.Background(), "DROP TABLE IF EXISTS test_sync_repl", 0)
		require.NoError(t, err)

		t.Log("End-to-end synchronous replication test completed successfully")
	})

	t.Run("ConfigureSynchronousReplication_ClearConfig", func(t *testing.T) {
		// This test verifies that ConfigureSynchronousReplication can clear the configuration
		// by providing an empty standby list
		t.Log("Testing ConfigureSynchronousReplication can clear configuration...")

		// First, configure synchronous replication with some standbys
		configReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
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
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
			return config != nil &&
				config.SynchronousMethod == multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST &&
				config.NumSync == 1 &&
				len(config.StandbyIds) == 2
		}, "Initial configuration should converge")

		t.Log("Initial configuration verified")

		// Now clear the configuration by providing empty standby list
		t.Log("Clearing synchronous_standby_names configuration...")
		clearReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           0,
			StandbyIds:        []*clustermetadatapb.ID{},
			ReloadConfig:      true,
		}
		_, err = primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), clearReq)
		require.NoError(t, err, "ConfigureSynchronousReplication should succeed with empty config")

		// Wait for cleared configuration to converge and verify using PrimaryStatus API
		t.Log("Waiting for configuration to be cleared...")
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
			return config != nil &&
				config.NumSync == 0 &&
				len(config.StandbyIds) == 0
		}, "Configuration should be cleared")

		t.Log("Successfully verified synchronous_standby_names is cleared")
	})

	t.Run("ConfigureSynchronousReplication_Standby_Fails", func(t *testing.T) {
		// ConfigureSynchronousReplication should fail on REPLICA pooler type
		t.Log("Testing ConfigureSynchronousReplication on REPLICA pooler (should fail)...")

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		req := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
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

	primaryPoolerClient, err := NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort))
	require.NoError(t, err)
	t.Cleanup(func() { primaryPoolerClient.Close() })

	t.Run("UpdateSynchronousStandbyList_Add_Success", func(t *testing.T) {
		setupReplicationTestCleanup(t, setup)
		t.Log("Testing UpdateSynchronousStandbyList ADD operation...")

		// First, configure initial synchronous replication with one standby
		configReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           1,
			StandbyIds:        []*clustermetadatapb.ID{makeMultipoolerID("test-cell", "standby1")},
			ReloadConfig:      true,
		}
		_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), configReq)
		require.NoError(t, err, "Initial configuration should succeed")

		// Wait for config to converge
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
			return config != nil && len(config.StandbyIds) == 1 && containsStandbyIDInConfig(config, "test-cell", "standby1")
		}, "Initial config should converge")

		// Verify initial configuration
		status := getPrimaryStatusFromClient(t, primaryManagerClient)
		assert.Equal(t, multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST, status.SyncReplicationConfig.SynchronousMethod)
		assert.Equal(t, int32(1), status.SyncReplicationConfig.NumSync)
		assert.Len(t, status.SyncReplicationConfig.StandbyIds, 1)
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby1"))
		t.Log("Initial configuration verified")

		updateReq := &multipoolermanagerdata.UpdateSynchronousStandbyListRequest{
			Operation:     multipoolermanagerdata.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_ADD,
			StandbyIds:    []*clustermetadatapb.ID{makeMultipoolerID("test-cell", "standby2")},
			ReloadConfig:  true,
			ConsensusTerm: 1,
			Force:         false,
		}
		_, err = primaryManagerClient.UpdateSynchronousStandbyList(utils.WithShortDeadline(t), updateReq)
		require.NoError(t, err, "ADD should succeed")

		// Verify both standbys are now in the list
		status = getPrimaryStatusFromClient(t, primaryManagerClient)
		assert.Equal(t, multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST, status.SyncReplicationConfig.SynchronousMethod)
		assert.Equal(t, int32(1), status.SyncReplicationConfig.NumSync)
		assert.Len(t, status.SyncReplicationConfig.StandbyIds, 2)
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby1"))
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby2"))

		t.Log("ADD operation verified successfully")
	})

	t.Run("UpdateSynchronousStandbyList_Add_Idempotent", func(t *testing.T) {
		setupReplicationTestCleanup(t, setup)
		t.Log("Testing UpdateSynchronousStandbyList ADD operation is idempotent...")

		// Configure with two standbys
		configReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
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
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
			return config != nil && len(config.StandbyIds) == 2 &&
				containsStandbyIDInConfig(config, "test-cell", "standby1") &&
				containsStandbyIDInConfig(config, "test-cell", "standby2")
		}, "Initial config should converge")

		initialStatus := getPrimaryStatusFromClient(t, primaryManagerClient)
		t.Log("Initial configuration verified")

		// Try to ADD a standby that already exists
		updateReq := &multipoolermanagerdata.UpdateSynchronousStandbyListRequest{
			Operation:     multipoolermanagerdata.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_ADD,
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
		setupReplicationTestCleanup(t, setup)
		t.Log("Testing UpdateSynchronousStandbyList ADD with both existing and new standbys...")

		// Configure with two standbys
		configReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
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
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
			return config != nil && len(config.StandbyIds) == 2
		}, "Initial config with 2 standbys should converge")

		t.Log("Initial configuration verified")

		// ADD with mix: standby2 already exists, standby3 and standby4 are new
		updateReq := &multipoolermanagerdata.UpdateSynchronousStandbyListRequest{
			Operation: multipoolermanagerdata.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_ADD,
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
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
			return config != nil && len(config.StandbyIds) == 4 &&
				containsStandbyIDInConfig(config, "test-cell", "standby1") &&
				containsStandbyIDInConfig(config, "test-cell", "standby2") &&
				containsStandbyIDInConfig(config, "test-cell", "standby3") &&
				containsStandbyIDInConfig(config, "test-cell", "standby4")
		}, "Config should have all 4 standbys")
	})

	t.Run("UpdateSynchronousStandbyList_Add_Then_Remove_Sequence", func(t *testing.T) {
		setupReplicationTestCleanup(t, setup)
		t.Log("Testing UpdateSynchronousStandbyList ADD followed by REMOVE in sequence...")

		// Configure with initial standbys
		configReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
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
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
			return config != nil && len(config.StandbyIds) == 2
		}, "Initial config with 2 standbys should converge")

		t.Log("Initial configuration verified")

		// ADD new standbys
		addReq := &multipoolermanagerdata.UpdateSynchronousStandbyListRequest{
			Operation: multipoolermanagerdata.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_ADD,
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
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
			return config != nil && len(config.StandbyIds) == 4
		}, "Config should have 4 standbys after ADD")

		afterAddStatus := getPrimaryStatusFromClient(t, primaryManagerClient)
		assert.Len(t, afterAddStatus.SyncReplicationConfig.StandbyIds, 4, "Should have 4 standbys after ADD")
		t.Log("ADD operation verified")

		// Now REMOVE some standbys (including one that was just added and one original)
		removeReq := &multipoolermanagerdata.UpdateSynchronousStandbyListRequest{
			Operation: multipoolermanagerdata.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_REMOVE,
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
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
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
		setupReplicationTestCleanup(t, setup)
		t.Log("Testing UpdateSynchronousStandbyList REMOVE operation...")

		// Configure with three standbys
		configReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
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
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
			return config != nil && len(config.StandbyIds) == 3 && containsStandbyIDInConfig(config, "test-cell", "standby2")
		}, "Initial config with 3 standbys should converge")

		// REMOVE standby2
		updateReq := &multipoolermanagerdata.UpdateSynchronousStandbyListRequest{
			Operation:     multipoolermanagerdata.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_REMOVE,
			StandbyIds:    []*clustermetadatapb.ID{makeMultipoolerID("test-cell", "standby2")},
			ReloadConfig:  true,
			ConsensusTerm: 0,
			Force:         true,
		}
		_, err = primaryManagerClient.UpdateSynchronousStandbyList(utils.WithShortDeadline(t), updateReq)
		require.NoError(t, err, "REMOVE operation should succeed")

		// Wait for config to converge without standby2
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
			return config != nil && len(config.StandbyIds) == 2 &&
				!containsStandbyIDInConfig(config, "test-cell", "standby2") &&
				containsStandbyIDInConfig(config, "test-cell", "standby1") &&
				containsStandbyIDInConfig(config, "test-cell", "standby3")
		}, "Config should converge without standby2")

		// Verify standby2 was removed
		status := getPrimaryStatusFromClient(t, primaryManagerClient)
		assert.Equal(t, multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_ANY, status.SyncReplicationConfig.SynchronousMethod)
		assert.Equal(t, int32(2), status.SyncReplicationConfig.NumSync)
		assert.Len(t, status.SyncReplicationConfig.StandbyIds, 2)
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby1"))
		assert.False(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby2"))
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby3"))

		t.Log("REMOVE operation verified successfully")
	})

	t.Run("UpdateSynchronousStandbyList_Remove_NonExistent_Idempotent", func(t *testing.T) {
		setupReplicationTestCleanup(t, setup)
		t.Log("Testing UpdateSynchronousStandbyList REMOVE operation with non-existent standby (idempotency)...")

		// Configure with two standbys
		configReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
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
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
			return config != nil && len(config.StandbyIds) == 2
		}, "Initial config with 2 standbys should converge")

		// Get initial state
		initialStatus := getPrimaryStatusFromClient(t, primaryManagerClient)
		require.Len(t, initialStatus.SyncReplicationConfig.StandbyIds, 2, "Should start with 2 standbys")

		// Try to REMOVE a standby that doesn't exist - should be idempotent
		updateReq := &multipoolermanagerdata.UpdateSynchronousStandbyListRequest{
			Operation: multipoolermanagerdata.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_REMOVE,
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
		setupReplicationTestCleanup(t, setup)
		t.Log("Testing UpdateSynchronousStandbyList REPLACE operation...")

		// Configure initial set
		configReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
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
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
			return config != nil && len(config.StandbyIds) == 2 &&
				containsStandbyIDInConfig(config, "test-cell", "standby1") &&
				containsStandbyIDInConfig(config, "test-cell", "standby2")
		}, "Initial config should converge")

		t.Log("Initial configuration verified")

		// REPLACE with completely different set
		updateReq := &multipoolermanagerdata.UpdateSynchronousStandbyListRequest{
			Operation: multipoolermanagerdata.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_REPLACE,
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
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
			return config != nil && len(config.StandbyIds) == 2 &&
				containsStandbyIDInConfig(config, "test-cell", "standby3") &&
				containsStandbyIDInConfig(config, "test-cell", "standby4") &&
				!containsStandbyIDInConfig(config, "test-cell", "standby1") &&
				!containsStandbyIDInConfig(config, "test-cell", "standby2")
		}, "Config should converge with replaced standbys")

		// Verify list was completely replaced
		status := getPrimaryStatusFromClient(t, primaryManagerClient)
		assert.Equal(t, multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST, status.SyncReplicationConfig.SynchronousMethod)
		assert.Equal(t, int32(1), status.SyncReplicationConfig.NumSync)
		assert.Len(t, status.SyncReplicationConfig.StandbyIds, 2)
		assert.False(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby1"))
		assert.False(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby2"))
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby3"))
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby4"))

		t.Log("REPLACE operation verified successfully")
	})

	t.Run("UpdateSynchronousStandbyList_NoSyncReplication_Fails", func(t *testing.T) {
		t.Log("Testing UpdateSynchronousStandbyList fails when sync replication not configured...")

		// Ensure synchronous replication is not configured
		resetReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           0,
			StandbyIds:        []*clustermetadatapb.ID{},
			ReloadConfig:      true,
		}
		_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), resetReq)
		require.NoError(t, err)

		// Wait for config to be cleared
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
			return config != nil && len(config.StandbyIds) == 0
		}, "Config should be cleared")

		// Try to update when no sync replication is configured
		updateReq := &multipoolermanagerdata.UpdateSynchronousStandbyListRequest{
			Operation:     multipoolermanagerdata.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_ADD,
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
		t.Log("Testing UpdateSynchronousStandbyList on REPLICA pooler (should fail)...")

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		updateReq := &multipoolermanagerdata.UpdateSynchronousStandbyListRequest{
			Operation:     multipoolermanagerdata.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_ADD,
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

func TestPrimaryStatus(t *testing.T) {
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

	t.Run("PrimaryStatus_NoSyncReplication", func(t *testing.T) {
		t.Log("Testing PrimaryStatus without synchronous replication configured...")

		// Clear any existing sync replication configuration
		clearReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           0,
			StandbyIds:        []*clustermetadatapb.ID{},
			ReloadConfig:      true,
		}
		_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), clearReq)
		require.NoError(t, err)

		// Get primary status
		statusResp, err := primaryManagerClient.PrimaryStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.PrimaryStatusRequest{})
		require.NoError(t, err, "PrimaryStatus should succeed")
		require.NotNil(t, statusResp.Status, "Status should not be nil")

		// Verify LSN is present and valid format
		assert.NotEmpty(t, statusResp.Status.Lsn, "LSN should be present")
		assert.Regexp(t, `^[0-9A-F]+/[0-9A-F]+$`, statusResp.Status.Lsn, "LSN should be in PostgreSQL format (X/XXXXXXXX)")
		t.Logf("Primary LSN: %s", statusResp.Status.Lsn)

		// Verify ready status
		assert.True(t, statusResp.Status.Ready, "Primary should be ready")

		// Verify sync replication config is present but with empty standby list
		require.NotNil(t, statusResp.Status.SyncReplicationConfig, "Sync replication config should be present")
		assert.Equal(t, multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			statusResp.Status.SyncReplicationConfig.SynchronousCommit, "Should have synchronous_commit level")
		assert.Empty(t, statusResp.Status.SyncReplicationConfig.StandbyIds, "StandbyIds should be empty when not configured")
		assert.Equal(t, int32(0), statusResp.Status.SyncReplicationConfig.NumSync, "NumSync should be 0")

		t.Log("PrimaryStatus without sync replication verified successfully")
	})

	t.Run("PrimaryStatus_WithSyncReplication", func(t *testing.T) {
		t.Log("Testing PrimaryStatus with synchronous replication configured...")

		// Configure synchronous replication
		standbyIDs := []*clustermetadatapb.ID{
			makeMultipoolerID("test-cell", "standby1"),
			makeMultipoolerID("test-cell", "standby2"),
		}
		configReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_APPLY,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
			NumSync:           2,
			StandbyIds:        standbyIDs,
			ReloadConfig:      true,
		}
		_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), configReq)
		require.NoError(t, err)

		// Wait for configuration to converge - pg_reload_conf() is asynchronous
		t.Log("Waiting for configuration to converge...")
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
			return config != nil &&
				config.SynchronousCommit == multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_APPLY &&
				config.SynchronousMethod == multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_ANY &&
				config.NumSync == 2 &&
				len(config.StandbyIds) == 2
		}, "Configuration should converge to expected values")

		// Get primary status and verify
		statusResp, err := primaryManagerClient.PrimaryStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.PrimaryStatusRequest{})
		require.NoError(t, err, "PrimaryStatus should succeed")
		require.NotNil(t, statusResp.Status, "Status should not be nil")

		// Verify sync replication config is present and correct
		require.NotNil(t, statusResp.Status.SyncReplicationConfig, "Sync replication config should be present")
		assert.Equal(t, multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_REMOTE_APPLY,
			statusResp.Status.SyncReplicationConfig.SynchronousCommit, "Synchronous commit level should match")
		assert.Equal(t, multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_ANY,
			statusResp.Status.SyncReplicationConfig.SynchronousMethod, "Synchronous method should match")
		assert.Equal(t, int32(2), statusResp.Status.SyncReplicationConfig.NumSync, "NumSync should match")
		assert.Len(t, statusResp.Status.SyncReplicationConfig.StandbyIds, 2, "Should have 2 standby IDs")

		// Verify standby IDs
		standbyIDMap := make(map[string]bool)
		for _, id := range statusResp.Status.SyncReplicationConfig.StandbyIds {
			key := fmt.Sprintf("%s_%s", id.Cell, id.Name)
			standbyIDMap[key] = true
		}
		assert.True(t, standbyIDMap["test-cell_standby1"], "standby1 should be in the list")
		assert.True(t, standbyIDMap["test-cell_standby2"], "standby2 should be in the list")

		t.Log("PrimaryStatus with sync replication verified successfully")

		// Cleanup
		clearReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           0,
			StandbyIds:        []*clustermetadatapb.ID{},
			ReloadConfig:      true,
		}
		_, err = primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), clearReq)
		require.NoError(t, err)
	})

	t.Run("PrimaryStatus_WithConnectedFollower", func(t *testing.T) {
		t.Log("Testing PrimaryStatus with connected follower...")

		// Ensure standby is connected and replicating
		setPrimaryReq := &multipoolermanagerdata.SetPrimaryConnInfoRequest{
			Host:                  "localhost",
			Port:                  int32(setup.PrimaryPgctld.PgPort),
			StartReplicationAfter: true,
			StopReplicationBefore: false,
			CurrentTerm:           1,
			Force:                 false,
		}
		_, err := standbyManagerClient.SetPrimaryConnInfo(utils.WithShortDeadline(t), setPrimaryReq)
		require.NoError(t, err)

		// Wait for replication to be established
		require.Eventually(t, func() bool {
			statusResp, err := standbyManagerClient.ReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.ReplicationStatusRequest{})
			if err != nil {
				return false
			}
			return statusResp.Status.PrimaryConnInfo != nil &&
				statusResp.Status.PrimaryConnInfo.Host != "" &&
				!statusResp.Status.IsWalReplayPaused
		}, 5*time.Second, 200*time.Millisecond, "Replication should be established")

		// Wait for primary to register the follower in pg_stat_replication
		// There can be a delay between standby connection and primary's view update
		t.Log("Waiting for primary to register the follower...")
		var statusResp *multipoolermanagerdata.PrimaryStatusResponse
		require.Eventually(t, func() bool {
			var err error
			statusResp, err = primaryManagerClient.PrimaryStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.PrimaryStatusRequest{})
			if err != nil {
				t.Logf("PrimaryStatus error: %v", err)
				return false
			}
			return statusResp.Status != nil && len(statusResp.Status.ConnectedFollowers) > 0
		}, 10*time.Second, 200*time.Millisecond, "Primary should register the follower")

		// Verify followers list contains the standby
		require.NotEmpty(t, statusResp.Status.ConnectedFollowers, "Should have at least one follower")

		// Find our standby in the followers list
		expectedAppName := fmt.Sprintf("test-cell_%s", setup.StandbyMultipooler.ServiceID)
		foundStandby := false
		for _, follower := range statusResp.Status.ConnectedFollowers {
			if follower.Cell == "test-cell" && follower.Name == setup.StandbyMultipooler.ServiceID {
				foundStandby = true
				break
			}
		}
		assert.True(t, foundStandby, "Standby should be in followers list with application_name: %s", expectedAppName)
		t.Logf("Found %d connected follower(s)", len(statusResp.Status.ConnectedFollowers))

		t.Log("PrimaryStatus with connected follower verified successfully")
	})

	t.Run("PrimaryStatus_Standby_Fails", func(t *testing.T) {
		t.Log("Testing PrimaryStatus on REPLICA pooler (should fail)...")

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		_, err := standbyManagerClient.PrimaryStatus(ctx, &multipoolermanagerdata.PrimaryStatusRequest{})
		require.Error(t, err, "PrimaryStatus should fail on standby")
		assert.Contains(t, err.Error(), "operation not allowed", "Error should indicate operation not allowed on REPLICA")

		t.Log("Confirmed: PrimaryStatus correctly rejected on REPLICA pooler")
	})
}

func TestGetFollowers(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)

	// Wait for both managers to be ready before running tests
	waitForManagerReady(t, setup, setup.PrimaryMultipooler)
	waitForManagerReady(t, setup, setup.StandbyMultipooler)

	// Register cleanup to reset replication config after all subtests
	setupReplicationTestCleanup(t, setup)

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

	t.Run("GetFollowers_NoSyncReplication", func(t *testing.T) {
		t.Log("Testing GetFollowers without synchronous replication configured...")

		// Clear any existing sync replication configuration
		clearReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           0,
			StandbyIds:        []*clustermetadatapb.ID{},
			ReloadConfig:      true,
		}
		_, err := primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), clearReq)
		require.NoError(t, err)

		// Get followers
		followersResp, err := primaryManagerClient.GetFollowers(utils.WithShortDeadline(t), &multipoolermanagerdata.GetFollowersRequest{})
		require.NoError(t, err, "GetFollowers should succeed")
		require.NotNil(t, followersResp, "Response should not be nil")

		// Verify empty followers list since no sync replication is configured
		assert.Empty(t, followersResp.Followers, "Followers list should be empty when no sync replication configured")

		// Verify sync config is present
		require.NotNil(t, followersResp.SyncConfig, "Sync config should be present")
		assert.Empty(t, followersResp.SyncConfig.StandbyIds, "StandbyIds should be empty")

		t.Log("GetFollowers without sync replication verified successfully")
	})

	t.Run("GetFollowers_WithConnectedFollower", func(t *testing.T) {
		t.Log("Testing GetFollowers with connected follower...")

		// Configure standby to connect to primary
		setPrimaryReq := &multipoolermanagerdata.SetPrimaryConnInfoRequest{
			Host:                  "localhost",
			Port:                  int32(setup.PrimaryPgctld.PgPort),
			StartReplicationAfter: true,
			StopReplicationBefore: false,
			CurrentTerm:           1,
			Force:                 false,
		}
		_, err := standbyManagerClient.SetPrimaryConnInfo(utils.WithShortDeadline(t), setPrimaryReq)
		require.NoError(t, err)

		// Wait for replication to be established
		require.Eventually(t, func() bool {
			statusResp, err := standbyManagerClient.ReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.ReplicationStatusRequest{})
			if err != nil {
				return false
			}
			return statusResp.Status != nil && statusResp.Status.PrimaryConnInfo != nil
		}, 10*time.Second, 500*time.Millisecond, "Standby should establish replication")

		// Configure synchronous replication with the standby
		standbyID := makeMultipoolerID("test-cell", setup.StandbyMultipooler.ServiceID)
		configReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           1,
			StandbyIds:        []*clustermetadatapb.ID{standbyID},
			ReloadConfig:      true,
		}
		_, err = primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), configReq)
		require.NoError(t, err)

		// Wait for the standby to actually connect and appear in pg_stat_replication
		t.Log("Waiting for standby to connect to primary...")
		require.Eventually(t, func() bool {
			statusResp, err := primaryManagerClient.PrimaryStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.PrimaryStatusRequest{})
			if err != nil {
				return false
			}
			// Check if any followers are connected
			return statusResp.Status != nil && len(statusResp.Status.ConnectedFollowers) > 0
		}, 10*time.Second, 500*time.Millisecond, "Standby should connect to primary")

		// Get followers
		followersResp, err := primaryManagerClient.GetFollowers(utils.WithShortDeadline(t), &multipoolermanagerdata.GetFollowersRequest{})
		require.NoError(t, err, "GetFollowers should succeed")
		require.NotNil(t, followersResp, "Response should not be nil")

		// Verify followers list
		require.Len(t, followersResp.Followers, 1, "Should have exactly 1 follower configured")

		follower := followersResp.Followers[0]
		assert.Equal(t, "test-cell", follower.FollowerId.Cell, "Follower cell should match")
		assert.Equal(t, setup.StandbyMultipooler.ServiceID, follower.FollowerId.Name, "Follower name should match")
		assert.True(t, follower.IsConnected, "Follower should be connected")
		assert.NotEmpty(t, follower.ApplicationName, "Application name should be set")

		// Verify replication stats are present
		require.NotNil(t, follower.ReplicationStats, "Replication stats should be present for connected follower")
		assert.NotZero(t, follower.ReplicationStats.Pid, "PID should be set")
		assert.NotEmpty(t, follower.ReplicationStats.State, "State should be set")
		assert.NotEmpty(t, follower.ReplicationStats.SyncState, "Sync state should be set")
		assert.NotEmpty(t, follower.ReplicationStats.SentLsn, "Sent LSN should be set")
		assert.NotEmpty(t, follower.ReplicationStats.WriteLsn, "Write LSN should be set")
		assert.NotEmpty(t, follower.ReplicationStats.FlushLsn, "Flush LSN should be set")
		assert.NotEmpty(t, follower.ReplicationStats.ReplayLsn, "Replay LSN should be set")

		t.Logf("Follower stats: PID=%d, State=%s, SyncState=%s, SentLSN=%s",
			follower.ReplicationStats.Pid,
			follower.ReplicationStats.State,
			follower.ReplicationStats.SyncState,
			follower.ReplicationStats.SentLsn)
	})

	t.Run("GetFollowers_FollowerDisconnects", func(t *testing.T) {
		t.Log("Testing GetFollowers when connected follower disconnects...")

		// Configure standby to connect to primary
		setPrimaryReq := &multipoolermanagerdata.SetPrimaryConnInfoRequest{
			Host:                  "localhost",
			Port:                  int32(setup.PrimaryPgctld.PgPort),
			StartReplicationAfter: true,
			StopReplicationBefore: false,
			CurrentTerm:           1,
			Force:                 false,
		}
		_, err := standbyManagerClient.SetPrimaryConnInfo(utils.WithShortDeadline(t), setPrimaryReq)
		require.NoError(t, err)

		// Wait for replication to be established
		require.Eventually(t, func() bool {
			statusResp, err := standbyManagerClient.ReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.ReplicationStatusRequest{})
			if err != nil {
				return false
			}
			return statusResp.Status != nil && statusResp.Status.PrimaryConnInfo != nil
		}, 10*time.Second, 500*time.Millisecond, "Standby should establish replication")

		// Configure synchronous replication with the standby
		standbyID := makeMultipoolerID("test-cell", setup.StandbyMultipooler.ServiceID)
		configReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           1,
			StandbyIds:        []*clustermetadatapb.ID{standbyID},
			ReloadConfig:      true,
		}
		_, err = primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), configReq)
		require.NoError(t, err)

		// Wait for the standby to actually connect and appear in pg_stat_replication
		t.Log("Waiting for standby to connect to primary...")
		require.Eventually(t, func() bool {
			statusResp, err := primaryManagerClient.PrimaryStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.PrimaryStatusRequest{})
			if err != nil {
				return false
			}
			// Check if any followers are connected
			return statusResp.Status != nil && len(statusResp.Status.ConnectedFollowers) > 0
		}, 10*time.Second, 500*time.Millisecond, "Standby should connect to primary")

		// Verify follower is connected
		followersResp, err := primaryManagerClient.GetFollowers(utils.WithShortDeadline(t), &multipoolermanagerdata.GetFollowersRequest{})
		require.NoError(t, err, "GetFollowers should succeed")
		require.Len(t, followersResp.Followers, 1, "Should have exactly 1 follower configured")

		follower := followersResp.Followers[0]
		assert.True(t, follower.IsConnected, "Follower should be connected initially")
		assert.NotNil(t, follower.ReplicationStats, "Replication stats should be present initially")
		t.Logf("Initial state: Follower connected with PID=%d, State=%s",
			follower.ReplicationStats.Pid, follower.ReplicationStats.State)

		// Now reset replication on the standby to disconnect it
		t.Log("Resetting replication on standby to disconnect...")
		_, err = standbyManagerClient.ResetReplication(utils.WithShortDeadline(t), &multipoolermanagerdata.ResetReplicationRequest{})
		require.NoError(t, err, "ResetReplication should succeed")

		// Wait for the disconnection to be reflected in pg_stat_replication
		// The replication connection should close within a few seconds
		require.Eventually(t, func() bool {
			followersResp, err := primaryManagerClient.GetFollowers(utils.WithShortDeadline(t), &multipoolermanagerdata.GetFollowersRequest{})
			if err != nil {
				t.Logf("GetFollowers failed: %v", err)
				return false
			}
			if len(followersResp.Followers) != 1 {
				t.Logf("Expected 1 follower, got %d", len(followersResp.Followers))
				return false
			}
			// Check if follower is now disconnected
			return !followersResp.Followers[0].IsConnected
		}, 10*time.Second, 500*time.Millisecond, "Follower should be marked as disconnected after ResetReplication")

		// Verify the final state
		followersResp, err = primaryManagerClient.GetFollowers(utils.WithShortDeadline(t), &multipoolermanagerdata.GetFollowersRequest{})
		require.NoError(t, err, "GetFollowers should succeed")
		require.Len(t, followersResp.Followers, 1, "Should still have 1 follower configured")

		follower = followersResp.Followers[0]
		assert.False(t, follower.IsConnected, "Follower should be disconnected")
		assert.Nil(t, follower.ReplicationStats, "Replication stats should be nil for disconnected follower")
		assert.Equal(t, "test-cell", follower.FollowerId.Cell, "Follower cell should still match")
		assert.Equal(t, setup.StandbyMultipooler.ServiceID, follower.FollowerId.Name, "Follower name should still match")

		t.Log("Verified: Follower disconnect is correctly reflected in GetFollowers response")
	})

	t.Run("GetFollowers_MixedConnectedDisconnected", func(t *testing.T) {
		t.Log("Testing GetFollowers with mix of connected and disconnected followers...")

		// Ensure standby is connected
		setPrimaryReq := &multipoolermanagerdata.SetPrimaryConnInfoRequest{
			Host:                  "localhost",
			Port:                  int32(setup.PrimaryPgctld.PgPort),
			StartReplicationAfter: true,
			StopReplicationBefore: false,
			CurrentTerm:           1,
			Force:                 false,
		}
		_, err := standbyManagerClient.SetPrimaryConnInfo(utils.WithShortDeadline(t), setPrimaryReq)
		require.NoError(t, err)

		// Wait for replication to be established
		require.Eventually(t, func() bool {
			statusResp, err := standbyManagerClient.ReplicationStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.ReplicationStatusRequest{})
			if err != nil {
				return false
			}
			return statusResp.Status != nil && statusResp.Status.PrimaryConnInfo != nil
		}, 10*time.Second, 500*time.Millisecond, "Standby should establish replication")

		// Configure synchronous replication with real standby + fake standby
		connectedID := makeMultipoolerID("test-cell", setup.StandbyMultipooler.ServiceID)
		disconnectedID := makeMultipoolerID("test-cell", "missing-standby")
		configReq := &multipoolermanagerdata.ConfigureSynchronousReplicationRequest{
			SynchronousCommit: multipoolermanagerdata.SynchronousCommitLevel_SYNCHRONOUS_COMMIT_ON,
			SynchronousMethod: multipoolermanagerdata.SynchronousMethod_SYNCHRONOUS_METHOD_FIRST,
			NumSync:           2,
			StandbyIds:        []*clustermetadatapb.ID{connectedID, disconnectedID},
			ReloadConfig:      true,
		}
		_, err = primaryManagerClient.ConfigureSynchronousReplication(utils.WithShortDeadline(t), configReq)
		require.NoError(t, err)

		// Wait for configuration to converge
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient, func(config *multipoolermanagerdata.SynchronousReplicationConfiguration) bool {
			return config != nil && len(config.StandbyIds) == 2
		}, "Configuration should converge")

		// Wait for the real standby to actually connect (the fake one won't)
		t.Log("Waiting for real standby to connect to primary...")
		require.Eventually(t, func() bool {
			statusResp, err := primaryManagerClient.PrimaryStatus(utils.WithShortDeadline(t), &multipoolermanagerdata.PrimaryStatusRequest{})
			if err != nil {
				return false
			}
			// Check if any followers are connected
			return statusResp.Status != nil && len(statusResp.Status.ConnectedFollowers) > 0
		}, 10*time.Second, 500*time.Millisecond, "Real standby should connect to primary")

		// Get followers
		followersResp, err := primaryManagerClient.GetFollowers(utils.WithShortDeadline(t), &multipoolermanagerdata.GetFollowersRequest{})
		require.NoError(t, err, "GetFollowers should succeed")
		require.NotNil(t, followersResp, "Response should not be nil")

		// Verify followers list
		require.Len(t, followersResp.Followers, 2, "Should have exactly 2 followers configured")

		// Count connected and disconnected
		connectedCount := 0
		disconnectedCount := 0
		for _, follower := range followersResp.Followers {
			if follower.IsConnected {
				connectedCount++
				assert.NotNil(t, follower.ReplicationStats, "Connected follower should have stats")
				assert.Equal(t, setup.StandbyMultipooler.ServiceID, follower.FollowerId.Name, "Connected follower name should match")
			} else {
				disconnectedCount++
				assert.Nil(t, follower.ReplicationStats, "Disconnected follower should not have stats")
				assert.Equal(t, "missing-standby", follower.FollowerId.Name, "Disconnected follower name should match")
			}
		}

		assert.Equal(t, 1, connectedCount, "Should have 1 connected follower")
		assert.Equal(t, 1, disconnectedCount, "Should have 1 disconnected follower")

		t.Log("GetFollowers with mixed connected/disconnected followers verified successfully")
	})

	t.Run("GetFollowers_Standby_Fails", func(t *testing.T) {
		t.Log("Testing GetFollowers on REPLICA pooler (should fail)...")

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		_, err := standbyManagerClient.GetFollowers(ctx, &multipoolermanagerdata.GetFollowersRequest{})
		require.Error(t, err, "GetFollowers should fail on standby")
		assert.Contains(t, err.Error(), "operation not allowed", "Error should indicate operation not allowed on REPLICA")

		t.Log("Confirmed: GetFollowers correctly rejected on REPLICA pooler")
	})
}

// TestDemoteAndPromote tests the full Demote and Promote cycle
// This ensures that demoting a primary and promoting a standby work together correctly,
// and that we can restore the original state at the end
func TestDemoteAndPromote(t *testing.T) {
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

	t.Run("FullCycle_DemoteAndPromote", func(t *testing.T) {
		t.Log("=== Testing full Demote/Promote cycle ===")

		// Demote the original primary
		t.Log("Demoting original primary...")

		// Set term on primary
		setTermReq := &multipoolermanagerdata.SetTermRequest{
			Term: &pgctldpb.ConsensusTerm{
				CurrentTerm: 1,
			},
		}
		_, err := primaryManagerClient.SetTerm(utils.WithShortDeadline(t), setTermReq)
		require.NoError(t, err, "SetTerm should succeed on primary")

		// Get LSN before demotion
		posReq := &multipoolermanagerdata.PrimaryPositionRequest{}
		posResp, err := primaryManagerClient.PrimaryPosition(utils.WithShortDeadline(t), posReq)
		require.NoError(t, err, "PrimaryPosition should succeed before demotion")
		lsnBeforeDemotion := posResp.LsnPosition
		t.Logf("LSN before demotion: %s", lsnBeforeDemotion)

		// Perform demotion
		demoteReq := &multipoolermanagerdata.DemoteRequest{
			ConsensusTerm: 1,
			DrainTimeout:  nil,
			Force:         false,
		}
		demoteResp, err := primaryManagerClient.Demote(utils.WithTimeout(t, 10*time.Second), demoteReq)
		require.NoError(t, err, "Demote should succeed")
		require.NotNil(t, demoteResp)

		assert.False(t, demoteResp.WasAlreadyDemoted, "Should not have been already demoted")
		assert.Equal(t, int64(1), demoteResp.ConsensusTerm)
		assert.NotEmpty(t, demoteResp.LsnPosition)
		t.Logf("Demotion complete. LSN: %s, connections terminated: %d",
			demoteResp.LsnPosition, demoteResp.ConnectionsTerminated)

		// Verify primary operations no longer work
		_, err = primaryManagerClient.PrimaryPosition(utils.WithShortDeadline(t), posReq)
		require.Error(t, err, "PrimaryPosition should fail after demotion")

		t.Log("Promoting original standby to primary...")

		// Set term on standby
		setTermReq2 := &multipoolermanagerdata.SetTermRequest{
			Term: &pgctldpb.ConsensusTerm{
				CurrentTerm: 2,
			},
		}
		_, err = standbyManagerClient.SetTerm(utils.WithShortDeadline(t), setTermReq2)
		require.NoError(t, err, "SetTerm should succeed on standby")

		// Stop replication to freeze LSN
		stopReq := &multipoolermanagerdata.StopReplicationRequest{}
		_, err = standbyManagerClient.StopReplication(utils.WithShortDeadline(t), stopReq)
		require.NoError(t, err, "StopReplication should succeed")

		// Get current LSN
		statusReq := &multipoolermanagerdata.ReplicationStatusRequest{}
		statusResp, err := standbyManagerClient.ReplicationStatus(utils.WithShortDeadline(t), statusReq)
		require.NoError(t, err, "ReplicationStatus should succeed")
		currentLSN := statusResp.Status.Lsn
		t.Logf("Current LSN before promotion: %s", currentLSN)

		// Perform promotion
		promoteReq := &multipoolermanagerdata.PromoteRequest{
			ConsensusTerm:         2,
			ExpectedLsn:           currentLSN,
			SyncReplicationConfig: nil, // Don't configure sync replication for now
			Force:                 false,
		}
		promoteResp, err := standbyManagerClient.Promote(utils.WithTimeout(t, 10*time.Second), promoteReq)
		require.NoError(t, err, "Promote should succeed")
		require.NotNil(t, promoteResp)

		assert.False(t, promoteResp.WasAlreadyPrimary, "Should not have been already primary")
		assert.Equal(t, int64(2), promoteResp.ConsensusTerm)
		assert.NotEmpty(t, promoteResp.LsnPosition)
		t.Logf("Promotion complete. LSN: %s", promoteResp.LsnPosition)

		// Verify new primary works
		posResp2, err := standbyManagerClient.PrimaryPosition(utils.WithShortDeadline(t), posReq)
		require.NoError(t, err, "PrimaryPosition should work on new primary")
		assert.NotEmpty(t, posResp2.LsnPosition)

		t.Log("Original standby is now primary")

		t.Log("Restoring original state...")

		// Demote the new primary (original standby)
		setTermReq3 := &multipoolermanagerdata.SetTermRequest{
			Term: &pgctldpb.ConsensusTerm{
				CurrentTerm: 3,
			},
		}
		_, err = standbyManagerClient.SetTerm(utils.WithShortDeadline(t), setTermReq3)
		require.NoError(t, err, "SetTerm should succeed")

		demoteReq2 := &multipoolermanagerdata.DemoteRequest{
			ConsensusTerm: 3,
			DrainTimeout:  nil,
			Force:         false,
		}
		demoteResp2, err := standbyManagerClient.Demote(utils.WithTimeout(t, 10*time.Second), demoteReq2)
		require.NoError(t, err, "Demote should succeed on new primary")
		assert.False(t, demoteResp2.WasAlreadyDemoted)
		t.Logf("New primary demoted. LSN: %s", demoteResp2.LsnPosition)

		// Promote the original primary back
		setTermReq4 := &multipoolermanagerdata.SetTermRequest{
			Term: &pgctldpb.ConsensusTerm{
				CurrentTerm: 4,
			},
		}
		_, err = primaryManagerClient.SetTerm(utils.WithShortDeadline(t), setTermReq4)
		require.NoError(t, err, "SetTerm should succeed")

		// Stop replication on original primary
		stopReq2 := &multipoolermanagerdata.StopReplicationRequest{}
		_, err = primaryManagerClient.StopReplication(utils.WithShortDeadline(t), stopReq2)
		require.NoError(t, err, "StopReplication should succeed")

		// Get LSN
		statusReq2 := &multipoolermanagerdata.ReplicationStatusRequest{}
		statusResp2, err := primaryManagerClient.ReplicationStatus(utils.WithShortDeadline(t), statusReq2)
		require.NoError(t, err, "ReplicationStatus should succeed")
		currentLSN2 := statusResp2.Status.Lsn

		// Promote original primary back
		promoteReq2 := &multipoolermanagerdata.PromoteRequest{
			ConsensusTerm:         4,
			ExpectedLsn:           currentLSN2,
			SyncReplicationConfig: nil,
			Force:                 false,
		}
		promoteResp2, err := primaryManagerClient.Promote(utils.WithTimeout(t, 10*time.Second), promoteReq2)
		require.NoError(t, err, "Promote should succeed")
		assert.False(t, promoteResp2.WasAlreadyPrimary)
		t.Logf("Original primary restored. LSN: %s", promoteResp2.LsnPosition)

		// Verify original primary works again
		posResp3, err := primaryManagerClient.PrimaryPosition(utils.WithShortDeadline(t), posReq)
		require.NoError(t, err, "PrimaryPosition should work on restored primary")
		assert.NotEmpty(t, posResp3.LsnPosition)

		t.Log("Original state restored - primary is primary, standby is standby")
	})

	t.Run("Idempotency_Demote", func(t *testing.T) {
		t.Log("Testing that Demote cannot be called twice after completion...")
		// TODO: This test needs to be hardened to actually
		// test that a promote that fail halfhway through
		// can be retried and successfully completes
		// in an idempotent way.

		// Set term
		setTermReq := &multipoolermanagerdata.SetTermRequest{
			Term: &pgctldpb.ConsensusTerm{
				CurrentTerm: 5,
			},
		}
		_, err := primaryManagerClient.SetTerm(utils.WithShortDeadline(t), setTermReq)
		require.NoError(t, err)

		// First demotion
		demoteReq := &multipoolermanagerdata.DemoteRequest{
			ConsensusTerm: 5,
			DrainTimeout:  nil,
			Force:         false,
		}
		demoteResp1, err := primaryManagerClient.Demote(utils.WithTimeout(t, 20*time.Second), demoteReq)
		require.NoError(t, err, "First demote should succeed")
		assert.False(t, demoteResp1.WasAlreadyDemoted)

		// Second demotion should fail with guard rail error (server is now REPLICA in topology)
		_, err = primaryManagerClient.Demote(utils.WithTimeout(t, 10*time.Second), demoteReq)
		require.Error(t, err, "Second demote should fail - cannot demote a REPLICA")
		assert.Contains(t, err.Error(), "pooler type is REPLICA")

		t.Log("Demote guard rail verified - cannot demote a REPLICA")
	})

	t.Run("Idempotency_Promote", func(t *testing.T) {
		// TODO: This test needs to be hardened to actually
		// test that a promote that fail halfhway through
		// can be retried and successfully completes
		// in an idempotent way.
		t.Log("Testing Promote idempotency...")
		// Promote original primary back (it's currently demoted)
		setTermReq := &multipoolermanagerdata.SetTermRequest{
			Term: &pgctldpb.ConsensusTerm{
				CurrentTerm: 6,
			},
		}
		_, err := primaryManagerClient.SetTerm(utils.WithShortDeadline(t), setTermReq)
		require.NoError(t, err)

		stopReq := &multipoolermanagerdata.StopReplicationRequest{}
		_, err = primaryManagerClient.StopReplication(utils.WithShortDeadline(t), stopReq)
		require.NoError(t, err)

		statusReq := &multipoolermanagerdata.ReplicationStatusRequest{}
		statusResp, err := primaryManagerClient.ReplicationStatus(utils.WithShortDeadline(t), statusReq)
		require.NoError(t, err)
		currentLSN := statusResp.Status.Lsn

		// First promotion
		promoteReq := &multipoolermanagerdata.PromoteRequest{
			ConsensusTerm:         6,
			ExpectedLsn:           currentLSN,
			SyncReplicationConfig: nil,
			Force:                 false,
		}
		promoteResp1, err := primaryManagerClient.Promote(utils.WithTimeout(t, 10*time.Second), promoteReq)
		require.NoError(t, err, "First promote should succeed")
		assert.False(t, promoteResp1.WasAlreadyPrimary)

		// Second promotion should fail with guard rail error (server is now PRIMARY in topology)
		_, err = primaryManagerClient.Promote(utils.WithTimeout(t, 10*time.Second), promoteReq)
		require.Error(t, err, "Second promote should fail - cannot promote a PRIMARY")
		assert.Contains(t, err.Error(), "pooler type is PRIMARY")

		t.Log("Promote guard rail verified - cannot promote a PRIMARY")
	})

	t.Run("TermValidation_Demote", func(t *testing.T) {
		t.Log("Testing Demote term validation...")

		setTermReq := &multipoolermanagerdata.SetTermRequest{
			Term: &pgctldpb.ConsensusTerm{
				CurrentTerm: 7,
			},
		}
		_, err := primaryManagerClient.SetTerm(utils.WithShortDeadline(t), setTermReq)
		require.NoError(t, err)

		// Try with stale term (should fail)
		demoteReq := &multipoolermanagerdata.DemoteRequest{
			ConsensusTerm: 5, // Less than current term (7)
			DrainTimeout:  nil,
			Force:         false,
		}
		_, err = primaryManagerClient.Demote(utils.WithTimeout(t, 10*time.Second), demoteReq)
		require.Error(t, err, "Demote with stale term should fail")
		assert.Contains(t, err.Error(), "term")

		// Try with force flag (should succeed even with stale term)
		demoteReq.Force = true
		_, err = primaryManagerClient.Demote(utils.WithTimeout(t, 10*time.Second), demoteReq)
		require.NoError(t, err, "Demote with force should succeed")

		t.Log("Demote term validation verified")
	})

	t.Run("TermValidation_Promote", func(t *testing.T) {
		t.Log("Testing Promote term validation...")

		// Promote back to restore state
		setTermReq := &multipoolermanagerdata.SetTermRequest{
			Term: &pgctldpb.ConsensusTerm{
				CurrentTerm: 8,
			},
		}
		_, err := primaryManagerClient.SetTerm(utils.WithShortDeadline(t), setTermReq)
		require.NoError(t, err)

		stopReq := &multipoolermanagerdata.StopReplicationRequest{}
		_, err = primaryManagerClient.StopReplication(utils.WithShortDeadline(t), stopReq)
		require.NoError(t, err)

		// Try with wrong term (should fail)
		promoteReq := &multipoolermanagerdata.PromoteRequest{
			ConsensusTerm:         999,
			ExpectedLsn:           "",
			SyncReplicationConfig: nil,
			Force:                 false,
		}
		_, err = primaryManagerClient.Promote(utils.WithTimeout(t, 10*time.Second), promoteReq)
		require.Error(t, err, "Promote with wrong term should fail")
		assert.Contains(t, err.Error(), "term")

		// Try with force flag (should succeed)
		promoteReq.Force = true
		_, err = primaryManagerClient.Promote(utils.WithTimeout(t, 10*time.Second), promoteReq)
		require.NoError(t, err, "Promote with force should succeed")

		t.Log("Promote term validation verified")
	})

	t.Run("LSNValidation_Promote", func(t *testing.T) {
		t.Log("Testing Promote LSN validation...")

		// Demote primary first
		setTermReq := &multipoolermanagerdata.SetTermRequest{
			Term: &pgctldpb.ConsensusTerm{
				CurrentTerm: 9,
			},
		}
		_, err := primaryManagerClient.SetTerm(utils.WithShortDeadline(t), setTermReq)
		require.NoError(t, err)

		demoteReq := &multipoolermanagerdata.DemoteRequest{
			ConsensusTerm: 9,
			DrainTimeout:  nil,
			Force:         false,
		}
		_, err = primaryManagerClient.Demote(utils.WithTimeout(t, 10*time.Second), demoteReq)
		require.NoError(t, err)

		// Now test LSN validation during promote
		setTermReq2 := &multipoolermanagerdata.SetTermRequest{
			Term: &pgctldpb.ConsensusTerm{
				CurrentTerm: 10,
			},
		}
		_, err = primaryManagerClient.SetTerm(utils.WithShortDeadline(t), setTermReq2)
		require.NoError(t, err)

		stopReq := &multipoolermanagerdata.StopReplicationRequest{}
		_, err = primaryManagerClient.StopReplication(utils.WithShortDeadline(t), stopReq)
		require.NoError(t, err)

		statusReq := &multipoolermanagerdata.ReplicationStatusRequest{}
		statusResp, err := primaryManagerClient.ReplicationStatus(utils.WithShortDeadline(t), statusReq)
		require.NoError(t, err)
		currentLSN := statusResp.Status.Lsn

		// Try with wrong LSN (should fail)
		promoteReq := &multipoolermanagerdata.PromoteRequest{
			ConsensusTerm:         10,
			ExpectedLsn:           "FF/FFFFFFFF",
			SyncReplicationConfig: nil,
			Force:                 false,
		}
		_, err = primaryManagerClient.Promote(utils.WithTimeout(t, 10*time.Second), promoteReq)
		require.Error(t, err, "Promote with wrong LSN should fail")
		assert.Contains(t, err.Error(), "LSN")

		// Try with correct LSN (should succeed)
		promoteReq.ExpectedLsn = currentLSN
		_, err = primaryManagerClient.Promote(utils.WithTimeout(t, 10*time.Second), promoteReq)
		require.NoError(t, err, "Promote with correct LSN should succeed")

		t.Log("Promote LSN validation verified")
	})

	t.Run("ErrorCases_DemoteOnStandby", func(t *testing.T) {
		t.Log("Testing Demote on standby (should fail)...")

		setTermReq := &multipoolermanagerdata.SetTermRequest{
			Term: &pgctldpb.ConsensusTerm{
				CurrentTerm: 11,
			},
		}
		_, err := standbyManagerClient.SetTerm(utils.WithShortDeadline(t), setTermReq)
		require.NoError(t, err)

		demoteReq := &multipoolermanagerdata.DemoteRequest{
			ConsensusTerm: 11,
			DrainTimeout:  nil,
			Force:         false,
		}
		_, err = standbyManagerClient.Demote(context.Background(), demoteReq)
		require.Error(t, err, "Demote should fail on standby")
		assert.Contains(t, err.Error(), "pooler type is REPLICA, must be PRIMARY")

		t.Log("Confirmed: Demote correctly rejected on standby")
	})

	t.Log("=== All Demote/Promote tests passed, servers restored to original state ===")
}
