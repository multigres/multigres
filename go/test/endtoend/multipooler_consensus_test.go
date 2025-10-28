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

func TestConsensus_GetNodeStatus(t *testing.T) {
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

	t.Run("GetNodeStatus_Primary", func(t *testing.T) {
		t.Log("Testing GetNodeStatus on primary multipooler...")

		req := &consensusdata.NodeStatusRequest{
			ShardId: "test-shard",
		}
		resp, err := primaryConsensusClient.GetNodeStatus(utils.WithShortDeadline(t), req)
		require.NoError(t, err, "GetNodeStatus should succeed on primary")
		require.NotNil(t, resp, "Response should not be nil")

		// Verify node ID
		assert.Equal(t, "primary-multipooler", resp.NodeId, "NodeId should match")

		// Verify zone/cell
		assert.Equal(t, "test-cell", resp.Zone, "Zone should match")

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
		assert.NotEmpty(t, resp.WalPosition.Lsn, "LSN should not be empty")
		assert.Regexp(t, `^[0-9A-F]+/[0-9A-F]+$`, resp.WalPosition.Lsn, "LSN should be in PostgreSQL format")
		assert.Greater(t, resp.WalPosition.LogIndex, int64(0), "LogIndex should be greater than 0")
		assert.Equal(t, int64(1), resp.WalPosition.LogTerm, "LogTerm should be 1")

		t.Logf("Primary node status verified: role=%s, healthy=%v, LSN=%s",
			resp.Role, resp.IsHealthy, resp.WalPosition.Lsn)
	})

	t.Run("GetNodeStatus_Standby", func(t *testing.T) {
		t.Log("Testing GetNodeStatus on standby multipooler...")

		req := &consensusdata.NodeStatusRequest{
			ShardId: "test-shard",
		}
		resp, err := standbyConsensusClient.GetNodeStatus(utils.WithShortDeadline(t), req)
		require.NoError(t, err, "GetNodeStatus should succeed on standby")
		require.NotNil(t, resp, "Response should not be nil")

		// Verify node ID
		assert.Equal(t, "standby-multipooler", resp.NodeId, "NodeId should match")

		// Verify zone/cell
		assert.Equal(t, "test-cell", resp.Zone, "Zone should match")

		// Verify role (should be replica)
		assert.Equal(t, "replica", resp.Role, "Role should be replica")

		// Verify health
		assert.True(t, resp.IsHealthy, "Standby should be healthy")

		// Verify eligibility
		assert.True(t, resp.IsEligible, "Standby should be eligible")

		t.Logf("Standby node status verified: role=%s, healthy=%v", resp.Role, resp.IsHealthy)
	})
}

func TestConsensus_GetWALPosition(t *testing.T) {
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

	t.Run("GetWALPosition_Primary", func(t *testing.T) {
		t.Log("Testing GetWALPosition on primary multipooler...")

		req := &consensusdata.GetWALPositionRequest{}
		resp, err := primaryConsensusClient.GetWALPosition(utils.WithShortDeadline(t), req)
		require.NoError(t, err, "GetWALPosition should succeed on primary")
		require.NotNil(t, resp, "Response should not be nil")
		require.NotNil(t, resp.WalPosition, "WAL position should be present")

		// Verify LSN format
		assert.NotEmpty(t, resp.WalPosition.Lsn, "LSN should not be empty")
		assert.Regexp(t, `^[0-9A-F]+/[0-9A-F]+$`, resp.WalPosition.Lsn, "LSN should be in PostgreSQL format (X/XXXXXXXX)")

		// Verify LogIndex (should be monotonic integer derived from LSN)
		assert.Greater(t, resp.WalPosition.LogIndex, int64(0), "LogIndex should be greater than 0")

		// Verify LogTerm matches consensus term
		assert.Equal(t, int64(1), resp.WalPosition.LogTerm, "LogTerm should be 1")

		// Verify timestamp is recent
		require.NotNil(t, resp.WalPosition.Timestamp, "Timestamp should be present")
		assert.WithinDuration(t, time.Now(), resp.WalPosition.Timestamp.AsTime(), 5*time.Second, "Timestamp should be recent")

		t.Logf("Primary WAL position: LSN=%s, LogIndex=%d, LogTerm=%d",
			resp.WalPosition.Lsn, resp.WalPosition.LogIndex, resp.WalPosition.LogTerm)
	})

	t.Run("GetWALPosition_Standby", func(t *testing.T) {
		t.Log("Testing GetWALPosition on standby multipooler...")

		req := &consensusdata.GetWALPositionRequest{}
		resp, err := standbyConsensusClient.GetWALPosition(utils.WithShortDeadline(t), req)
		require.NoError(t, err, "GetWALPosition should succeed on standby")
		require.NotNil(t, resp, "Response should not be nil")
		require.NotNil(t, resp.WalPosition, "WAL position should be present")

		// Verify LSN format
		assert.NotEmpty(t, resp.WalPosition.Lsn, "LSN should not be empty")

		// Verify LogIndex
		assert.GreaterOrEqual(t, resp.WalPosition.LogIndex, int64(0), "LogIndex should be non-negative")

		t.Logf("Standby WAL position: LSN=%s, LogIndex=%d",
			resp.WalPosition.Lsn, resp.WalPosition.LogIndex)
	})
}

func TestConsensus_RequestVote(t *testing.T) {
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

	t.Run("RequestVote_OldTerm_Rejected", func(t *testing.T) {
		t.Log("Testing RequestVote with old term (should be rejected)...")

		// Get current WAL position from standby
		walResp, err := standbyConsensusClient.GetWALPosition(utils.WithShortDeadline(t), &consensusdata.GetWALPositionRequest{})
		require.NoError(t, err)

		// Request vote with term 0 (older than current term 1)
		req := &consensusdata.RequestVoteRequest{
			Term:         0,
			CandidateId:  "test-candidate",
			ShardId:      "test-shard",
			LastLogIndex: walResp.WalPosition.LogIndex,
			LastLogTerm:  0,
		}

		resp, err := standbyConsensusClient.RequestVote(utils.WithShortDeadline(t), req)
		require.NoError(t, err, "RequestVote RPC should succeed")
		require.NotNil(t, resp, "Response should not be nil")

		// Vote should be rejected because term is old
		assert.False(t, resp.VoteGranted, "Vote should not be granted for old term")
		assert.Equal(t, int64(1), resp.Term, "Response term should be current term (1)")
		assert.Equal(t, "standby-multipooler", resp.NodeId, "NodeId should match")

		t.Log("Confirmed: RequestVote correctly rejected old term")
	})

	t.Run("RequestVote_NewTerm_Granted", func(t *testing.T) {
		t.Log("Testing RequestVote with new term (should be granted)...")

		// Get current WAL position from primary
		walResp, err := primaryConsensusClient.GetWALPosition(utils.WithShortDeadline(t), &consensusdata.GetWALPositionRequest{})
		require.NoError(t, err)

		// Request vote with term 2 (newer than current term 1)
		req := &consensusdata.RequestVoteRequest{
			Term:         2,
			CandidateId:  "new-leader-candidate",
			ShardId:      "test-shard",
			LastLogIndex: walResp.WalPosition.LogIndex,
			LastLogTerm:  1,
		}

		resp, err := primaryConsensusClient.RequestVote(utils.WithShortDeadline(t), req)
		require.NoError(t, err, "RequestVote RPC should succeed")
		require.NotNil(t, resp, "Response should not be nil")

		// Vote should be granted because:
		// 1. Term is newer (2 > 1)
		// 2. WAL position is up-to-date
		// 3. Haven't voted yet in this term
		assert.True(t, resp.VoteGranted, "Vote should be granted for new term with up-to-date WAL")
		assert.Equal(t, int64(2), resp.Term, "Response term should be updated to new term")
		assert.Equal(t, "primary-multipooler", resp.NodeId, "NodeId should match")

		t.Log("Confirmed: RequestVote correctly granted for new term")
	})

	t.Run("RequestVote_SameTerm_AlreadyVoted", func(t *testing.T) {
		t.Log("Testing RequestVote for same term after already voting (should be rejected)...")

		// Get current WAL position from primary
		walResp, err := primaryConsensusClient.GetWALPosition(utils.WithShortDeadline(t), &consensusdata.GetWALPositionRequest{})
		require.NoError(t, err)

		// Request vote with term 2 again but different candidate
		req := &consensusdata.RequestVoteRequest{
			Term:         2,
			CandidateId:  "different-candidate",
			ShardId:      "test-shard",
			LastLogIndex: walResp.WalPosition.LogIndex,
			LastLogTerm:  2,
		}

		resp, err := primaryConsensusClient.RequestVote(utils.WithShortDeadline(t), req)
		require.NoError(t, err, "RequestVote RPC should succeed")
		require.NotNil(t, resp, "Response should not be nil")

		// Vote should be rejected because already voted for someone else in this term
		assert.False(t, resp.VoteGranted, "Vote should not be granted when already voted in this term")
		assert.Equal(t, int64(2), resp.Term, "Response term should remain 2")

		t.Log("Confirmed: RequestVote correctly rejected when already voted in term")
	})

	t.Run("RequestVote_StaleWAL_Rejected", func(t *testing.T) {
		t.Log("Testing RequestVote with stale WAL position (should be rejected)...")

		// Request vote with term 3 but very old WAL position
		req := &consensusdata.RequestVoteRequest{
			Term:         3,
			CandidateId:  "stale-candidate",
			ShardId:      "test-shard",
			LastLogIndex: 0, // Very old position
			LastLogTerm:  0, // Very old term
		}

		resp, err := standbyConsensusClient.RequestVote(utils.WithShortDeadline(t), req)
		require.NoError(t, err, "RequestVote RPC should succeed")
		require.NotNil(t, resp, "Response should not be nil")

		// Vote should be rejected because WAL position is not up-to-date
		assert.False(t, resp.VoteGranted, "Vote should not be granted for stale WAL position")

		t.Log("Confirmed: RequestVote correctly rejected stale WAL position")
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

	// Create clients
	primaryConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { primaryConn.Close() })
	primaryConsensusClient := consensuspb.NewMultiPoolerConsensusClient(primaryConn)

	t.Run("GetLeadershipView_WithoutReplicationTracker", func(t *testing.T) {
		t.Log("Testing GetLeadershipView without replication tracker (should fail)...")

		req := &consensusdata.LeadershipViewRequest{
			ShardId: "test-shard",
		}

		resp, err := primaryConsensusClient.GetLeadershipView(utils.WithShortDeadline(t), req)

		// Should fail because replication tracker is not initialized
		// (replication tracker would normally be initialized by heartbeat writer)
		require.Error(t, err, "GetLeadershipView should fail without replication tracker")
		assert.Nil(t, resp, "Response should be nil")
		assert.Contains(t, err.Error(), "replication tracker not initialized", "Error should mention replication tracker")

		t.Log("Confirmed: GetLeadershipView correctly fails without replication tracker")
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

	// Create clients
	standbyConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { standbyConn.Close() })
	standbyConsensusClient := consensuspb.NewMultiPoolerConsensusClient(standbyConn)

	t.Run("CanReachPrimary_Placeholder", func(t *testing.T) {
		t.Log("Testing CanReachPrimary placeholder implementation...")

		req := &consensusdata.CanReachPrimaryRequest{
			PrimaryHost: "localhost",
			PrimaryPort: int32(setup.PrimaryPgctld.PgPort),
		}

		resp, err := standbyConsensusClient.CanReachPrimary(utils.WithShortDeadline(t), req)
		require.NoError(t, err, "CanReachPrimary should succeed")
		require.NotNil(t, resp, "Response should not be nil")

		// Placeholder implementation always returns true
		assert.True(t, resp.Reachable, "Placeholder should return reachable=true")
		assert.Empty(t, resp.ErrorMessage, "Placeholder should have no error message")

		t.Log("Confirmed: CanReachPrimary placeholder works as expected")
	})
}
