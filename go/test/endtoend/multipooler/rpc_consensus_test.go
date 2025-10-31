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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/multigres/multigres/go/test/utils"

	consensuspb "github.com/multigres/multigres/go/pb/consensus"
	consensusdata "github.com/multigres/multigres/go/pb/consensusdata"
)

func TestConsensus_Status(t *testing.T) {
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
	primaryConsensusClient := consensuspb.NewMultiPoolerConsensusClient(primaryConn)

	standbyConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { standbyConn.Close() })
	standbyConsensusClient := consensuspb.NewMultiPoolerConsensusClient(standbyConn)

	t.Run("Status_Primary", func(t *testing.T) {
		t.Log("Testing Status on primary multipooler...")

		req := &consensusdata.StatusRequest{
			ShardId: "test-shard",
		}
		resp, err := primaryConsensusClient.Status(utils.WithShortDeadline(t), req)
		require.NoError(t, err, "Status should succeed on primary")
		require.NotNil(t, resp, "Response should not be nil")

		// Verify node ID
		assert.Equal(t, "primary-multipooler", resp.PoolerId, "PoolerId should match")

		// Verify cell
		assert.Equal(t, "test-cell", resp.Cell, "Cell should match")

		// Verify role (should be primary)
		assert.Equal(t, "primary", resp.Role, "Role should be primary")

		// Verify term (should be 1 from setup)
		assert.Equal(t, int64(1), resp.CurrentTerm, "CurrentTerm should be 1")

		// Verify health (should be healthy with database connection)
		assert.True(t, resp.IsHealthy, "Primary should be healthy")

		// Verify eligibility
		assert.True(t, resp.IsEligible, "Primary should be eligible")

		// Verify WAL position is present
		require.NotNil(t, resp.WalPosition, "WAL position should be present")
		assert.NotEmpty(t, resp.WalPosition.CurrentLsn, "CurrentLsn should not be empty on primary")
		assert.Regexp(t, `^[0-9A-F]+/[0-9A-F]+$`, resp.WalPosition.CurrentLsn, "CurrentLsn should be in PostgreSQL format")

		t.Logf("Primary node status verified: role=%s, healthy=%v, CurrentLSN=%s",
			resp.Role, resp.IsHealthy, resp.WalPosition.CurrentLsn)
	})

	t.Run("Status_Standby", func(t *testing.T) {
		t.Log("Testing Status on standby multipooler...")

		req := &consensusdata.StatusRequest{
			ShardId: "test-shard",
		}
		resp, err := standbyConsensusClient.Status(utils.WithShortDeadline(t), req)
		require.NoError(t, err, "Status should succeed on standby")
		require.NotNil(t, resp, "Response should not be nil")

		// Verify node ID
		assert.Equal(t, "standby-multipooler", resp.PoolerId, "PoolerId should match")

		// Verify cell
		assert.Equal(t, "test-cell", resp.Cell, "Cell should match")

		// Verify role (should be replica)
		assert.Equal(t, "replica", resp.Role, "Role should be replica")

		// Verify health
		assert.True(t, resp.IsHealthy, "Standby should be healthy")

		// Verify eligibility
		assert.True(t, resp.IsEligible, "Standby should be eligible")

		t.Logf("Standby node status verified: role=%s, healthy=%v", resp.Role, resp.IsHealthy)
	})
}

func TestConsensus_BeginTerm(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)

	// Wait for both managers to be ready before running tests
	waitForManagerReady(t, setup, setup.PrimaryMultipooler)
	waitForManagerReady(t, setup, setup.StandbyMultipooler)

	// Create clients
	primaryConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { primaryConn.Close() })
	primaryConsensusClient := consensuspb.NewMultiPoolerConsensusClient(primaryConn)

	standbyConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { standbyConn.Close() })
	standbyConsensusClient := consensuspb.NewMultiPoolerConsensusClient(standbyConn)

	t.Run("BeginTerm_OldTerm_Rejected", func(t *testing.T) {
		t.Log("Testing BeginTerm with old term (should be rejected)...")

		// Attempt to begin term 0 (older than current term 1)
		req := &consensusdata.BeginTermRequest{
			Term:        0,
			CandidateId: "test-candidate",
			ShardId:     "test-shard",
		}

		resp, err := standbyConsensusClient.BeginTerm(utils.WithShortDeadline(t), req)
		require.NoError(t, err, "BeginTerm RPC should succeed")
		require.NotNil(t, resp, "Response should not be nil")

		// Term should be rejected because it is too old
		assert.False(t, resp.Accepted, "Old term should not be accepted")
		assert.Equal(t, int64(1), resp.Term, "Response term should be current term (1)")
		assert.Equal(t, "standby-multipooler", resp.PoolerId, "PoolerId should match")

		t.Log("BeginTerm correctly rejected old term")
	})

	t.Run("BeginTerm_NewTerm_Accepted", func(t *testing.T) {
		t.Log("Testing BeginTerm with new term (should be accepted)...")

		// Begin term 2 (newer than current term 1)
		req := &consensusdata.BeginTermRequest{
			Term:        2,
			CandidateId: "new-leader-candidate",
			ShardId:     "test-shard",
		}

		resp, err := primaryConsensusClient.BeginTerm(utils.WithShortDeadline(t), req)
		require.NoError(t, err, "BeginTerm RPC should succeed")
		require.NotNil(t, resp, "Response should not be nil")

		// Term 2 should be accepted because:
		// 1. Term is newer (2 > 1)
		// 3. Haven't accepted any other leader yet in this term
		assert.True(t, resp.Accepted, "New term should be accepted")
		assert.Equal(t, int64(2), resp.Term, "Response term should be updated to new term")
		assert.Equal(t, "primary-multipooler", resp.PoolerId, "PoolerId should match")

		t.Log("BeginTerm correctly granted for new term")
	})

	t.Run("BeginTerm_SameTerm_AlreadyAccepted", func(t *testing.T) {
		t.Log("Testing BeginTerm for same term after already accepting (should be rejected)...")

		// Begin term 2 again but different candidate
		req := &consensusdata.BeginTermRequest{
			Term:        2,
			CandidateId: "different-candidate",
			ShardId:     "test-shard",
		}

		resp, err := primaryConsensusClient.BeginTerm(utils.WithShortDeadline(t), req)
		require.NoError(t, err, "BeginTerm RPC should succeed")
		require.NotNil(t, resp, "Response should not be nil")

		// Candidate should be rejected because already accepted another leader in this term
		assert.False(t, resp.Accepted, "BeginTerm should not be accepted when already accepted this term for another leader")
		assert.Equal(t, int64(2), resp.Term, "Response term should remain 2")

		t.Log("BeginTerm correctly rejected when already accepted a leader in term")
	})
}

func TestConsensus_GetLeadershipView(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)

	// Wait for both managers to be ready before running tests
	waitForManagerReady(t, setup, setup.PrimaryMultipooler)
	waitForManagerReady(t, setup, setup.StandbyMultipooler)

	t.Log("Manager primary-multipooler is ready")
	t.Log("Manager standby-multipooler is ready")

	// Create clients
	primaryConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { primaryConn.Close() })
	primaryConsensusClient := consensuspb.NewMultiPoolerConsensusClient(primaryConn)

	standbyConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { standbyConn.Close() })
	standbyConsensusClient := consensuspb.NewMultiPoolerConsensusClient(standbyConn)

	t.Run("GetLeadershipView_FromPrimary", func(t *testing.T) {
		t.Log("Testing GetLeadershipView from primary...")

		req := &consensusdata.LeadershipViewRequest{
			ShardId: "test-shard",
		}

		resp, err := primaryConsensusClient.GetLeadershipView(utils.WithShortDeadline(t), req)
		require.NoError(t, err, "GetLeadershipView RPC should succeed")
		require.NotNil(t, resp, "Response should not be nil")

		// Verify leader_id is set (should be primary-multipooler)
		assert.NotEmpty(t, resp.LeaderId, "LeaderId should not be empty")
		assert.Equal(t, "primary-multipooler", resp.LeaderId, "LeaderId should be primary-multipooler")

		// Verify leader_term is set (should be 1 from test setup)
		assert.Equal(t, int64(1), resp.LeaderTerm, "LeaderTerm should be 1")

		// Verify last_heartbeat is set and recent
		require.NotNil(t, resp.LastHeartbeat, "LastHeartbeat should not be nil")
		assert.True(t, resp.LastHeartbeat.IsValid(), "LastHeartbeat should be a valid timestamp")

		// Heartbeat should be recent (within last 30 seconds)
		heartbeatTime := resp.LastHeartbeat.AsTime()
		timeSinceHeartbeat := time.Since(heartbeatTime)
		assert.Less(t, timeSinceHeartbeat, 30*time.Second,
			"LastHeartbeat should be recent (within 30 seconds)")

		// Verify replication_lag_ns is set (should be 0 or small for primary)
		assert.GreaterOrEqual(t, resp.ReplicationLagNs, int64(0),
			"ReplicationLagNs should be non-negative")

		t.Logf("Leadership view: leader_id=%s, term=%d, lag=%dns",
			resp.LeaderId, resp.LeaderTerm, resp.ReplicationLagNs)
		t.Log("GetLeadershipView returns valid data from primary")
	})

	t.Run("GetLeadershipView_FromStandby", func(t *testing.T) {
		t.Log("Testing GetLeadershipView from standby...")

		req := &consensusdata.LeadershipViewRequest{
			ShardId: "test-shard",
		}

		resp, err := standbyConsensusClient.GetLeadershipView(utils.WithShortDeadline(t), req)
		require.NoError(t, err, "GetLeadershipView RPC should succeed")
		require.NotNil(t, resp, "Response should not be nil")

		// Standby should also see the same leader information
		assert.NotEmpty(t, resp.LeaderId, "LeaderId should not be empty")
		assert.Equal(t, "primary-multipooler", resp.LeaderId, "LeaderId should be primary-multipooler")
		assert.Equal(t, int64(1), resp.LeaderTerm, "LeaderTerm should be 1")

		require.NotNil(t, resp.LastHeartbeat, "LastHeartbeat should not be nil")
		assert.True(t, resp.LastHeartbeat.IsValid(), "LastHeartbeat should be a valid timestamp")

		// Replication lag on standby might be higher than on primary
		assert.GreaterOrEqual(t, resp.ReplicationLagNs, int64(0),
			"ReplicationLagNs should be non-negative")

		t.Log("GetLeadershipView returns valid data from standby")
	})
}

func TestConsensus_CanReachPrimary(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)

	// Wait for both managers to be ready before running tests
	waitForManagerReady(t, setup, setup.PrimaryMultipooler)
	waitForManagerReady(t, setup, setup.StandbyMultipooler)

	t.Log("Manager primary-multipooler is ready")
	t.Log("Manager standby-multipooler is ready")

	// Create clients
	standbyConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { standbyConn.Close() })
	standbyConsensusClient := consensuspb.NewMultiPoolerConsensusClient(standbyConn)

	primaryConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { primaryConn.Close() })
	primaryConsensusClient := consensuspb.NewMultiPoolerConsensusClient(primaryConn)

	// TODO: Add test case that verifies host/port matching when WAL receiver is active.
	// This requires waiting for streaming replication to be fully established or having
	// code that explicitly establishes primary/standby relationships. Currently the WAL
	// receiver is not active immediately after setup, so we cannot test the host/port
	// comparison logic.

	t.Run("Standby_CanReachPrimary", func(t *testing.T) {
		t.Log("Testing CanReachPrimary from standby (should detect active WAL receiver)...")

		req := &consensusdata.CanReachPrimaryRequest{
			PrimaryHost: "localhost",
			PrimaryPort: int32(setup.PrimaryPgctld.PgPort),
		}

		resp, err := standbyConsensusClient.CanReachPrimary(utils.WithShortDeadline(t), req)
		require.NoError(t, err, "CanReachPrimary RPC should succeed")
		require.NotNil(t, resp, "Response should not be nil")

		// Standby should be able to reach primary if WAL receiver is active
		// However, WAL receiver might not be active immediately after setup
		if resp.Reachable {
			assert.Empty(t, resp.ErrorMessage, "Should have no error message when reachable")
			t.Log("Standby can reach primary (WAL receiver active)")
		} else {
			// Acceptable failure reasons: WAL receiver not active yet
			assert.Contains(t, resp.ErrorMessage, "no active WAL receiver",
				"Error message should indicate no active WAL receiver")
			t.Logf("Note: WAL receiver not yet active (%s)", resp.ErrorMessage)
		}
	})

	t.Run("Primary_CannotReachPrimary", func(t *testing.T) {
		t.Log("Testing CanReachPrimary from primary (should return false - no WAL receiver)...")

		req := &consensusdata.CanReachPrimaryRequest{
			PrimaryHost: "localhost",
			PrimaryPort: int32(setup.PrimaryPgctld.PgPort),
		}

		resp, err := primaryConsensusClient.CanReachPrimary(utils.WithShortDeadline(t), req)
		require.NoError(t, err, "CanReachPrimary RPC should succeed")
		require.NotNil(t, resp, "Response should not be nil")

		// Primary should NOT have an active WAL receiver
		assert.False(t, resp.Reachable, "Primary should not be able to reach itself via WAL receiver")
		assert.NotEmpty(t, resp.ErrorMessage, "Should have error message explaining why")
		assert.Contains(t, resp.ErrorMessage, "no active WAL receiver",
			"Error message should indicate no WAL receiver")

		t.Log("Primary correctly reports no WAL receiver")
	})

	t.Run("InvalidHost_CannotReach", func(t *testing.T) {
		t.Log("Testing CanReachPrimary with invalid host (should return false)...")

		req := &consensusdata.CanReachPrimaryRequest{
			PrimaryHost: "invalid-host",
			PrimaryPort: 12345,
		}

		resp, err := standbyConsensusClient.CanReachPrimary(utils.WithShortDeadline(t), req)
		require.NoError(t, err, "CanReachPrimary RPC should succeed (even if host is invalid)")
		require.NotNil(t, resp, "Response should not be nil")

		// Note: The implementation checks pg_stat_wal_receiver and compares conninfo host/port
		// with the requested host/port. However, since WAL receiver is not active immediately
		// after setup, this test cannot verify the host/port mismatch detection.
		// TODO: fix after implementing full cluster initialization
		t.Logf("Response: reachable=%v, error=%s", resp.Reachable, resp.ErrorMessage)
	})
}
