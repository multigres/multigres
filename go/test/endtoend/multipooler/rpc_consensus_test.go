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

	"github.com/multigres/multigres/go/test/endtoend"
	"github.com/multigres/multigres/go/test/utils"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensuspb "github.com/multigres/multigres/go/pb/consensus"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
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

		req := &consensusdatapb.StatusRequest{
			ShardId: "test-shard",
		}
		resp, err := primaryConsensusClient.Status(utils.WithShortDeadline(t), req)
		require.NoError(t, err, "Status should succeed on primary")
		require.NotNil(t, resp, "Response should not be nil")

		// Verify node ID
		assert.Equal(t, setup.PrimaryMultipooler.Name, resp.PoolerId, "PoolerId should match")

		// Verify cell
		assert.Equal(t, "test-cell", resp.Cell, "Cell should match")

		// Verify role (should be primary)
		assert.Equal(t, "primary", resp.Role, "Role should be primary")

		// Verify term (should be 1 from setup)
		assert.Equal(t, int64(1), resp.CurrentTerm, "TermNumber should be 1")

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

		req := &consensusdatapb.StatusRequest{
			ShardId: "test-shard",
		}
		resp, err := standbyConsensusClient.Status(utils.WithShortDeadline(t), req)
		require.NoError(t, err, "Status should succeed on standby")
		require.NotNil(t, resp, "Response should not be nil")

		// Verify node ID
		assert.Equal(t, setup.StandbyMultipooler.Name, resp.PoolerId, "PoolerId should match")

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

	// Setup test cleanup - this will restore term to 1 after all subtests complete
	setupPoolerTest(t, setup, WithoutReplication())

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
		req := &consensusdatapb.BeginTermRequest{
			Term: 0,
			CandidateId: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "test-candidate",
			},
			ShardId: "test-shard",
		}

		resp, err := standbyConsensusClient.BeginTerm(utils.WithShortDeadline(t), req)
		require.NoError(t, err, "BeginTerm RPC should succeed")
		require.NotNil(t, resp, "Response should not be nil")

		// Term should be rejected because it is too old
		assert.False(t, resp.Accepted, "Old term should not be accepted")
		assert.Equal(t, int64(1), resp.Term, "Response term should be current term (1)")
		assert.Equal(t, setup.StandbyMultipooler.Name, resp.PoolerId, "PoolerId should match")

		t.Log("BeginTerm correctly rejected old term")
	})

	t.Run("BeginTerm_NewTerm_Accepted", func(t *testing.T) {
		t.Log("Testing BeginTerm with new term (should be accepted)...")

		// Begin term 2 (newer than current term 1)
		req := &consensusdatapb.BeginTermRequest{
			Term: 2,
			CandidateId: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "new-leader-candidate",
			},
			ShardId: "test-shard",
		}

		resp, err := primaryConsensusClient.BeginTerm(utils.WithShortDeadline(t), req)
		require.NoError(t, err, "BeginTerm RPC should succeed")
		require.NotNil(t, resp, "Response should not be nil")

		// Term 2 should be accepted because:
		// 1. Term is newer (2 > 1)
		// 3. Haven't accepted any other leader yet in this term
		assert.True(t, resp.Accepted, "New term should be accepted")
		assert.Equal(t, int64(2), resp.Term, "Response term should be updated to new term")
		assert.Equal(t, setup.PrimaryMultipooler.Name, resp.PoolerId, "PoolerId should match")

		t.Log("BeginTerm correctly granted for new term")
	})

	t.Run("BeginTerm_SameTerm_AlreadyAccepted", func(t *testing.T) {
		t.Log("Testing BeginTerm for same term after already accepting (should be rejected)...")

		// Begin term 2 again but different candidate
		req := &consensusdatapb.BeginTermRequest{
			Term: 2,
			CandidateId: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "different-candidate",
			},
			ShardId: "test-shard",
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

		req := &consensusdatapb.LeadershipViewRequest{
			ShardId: "test-shard",
		}

		resp, err := primaryConsensusClient.GetLeadershipView(utils.WithShortDeadline(t), req)
		require.NoError(t, err, "GetLeadershipView RPC should succeed")
		require.NotNil(t, resp, "Response should not be nil")

		// Verify leader_id is set (should be the primary multipooler)
		assert.NotEmpty(t, resp.LeaderId, "LeaderId should not be empty")
		assert.Equal(t, setup.PrimaryName, resp.LeaderId, "LeaderId should be primary")

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

		t.Logf("Leadership view: leader_id=%s, lag=%dns",
			resp.LeaderId, resp.ReplicationLagNs)
		t.Log("GetLeadershipView returns valid data from primary")
	})

	t.Run("GetLeadershipView_FromStandby", func(t *testing.T) {
		t.Log("Testing GetLeadershipView from standby...")

		req := &consensusdatapb.LeadershipViewRequest{
			ShardId: "test-shard",
		}

		resp, err := standbyConsensusClient.GetLeadershipView(utils.WithShortDeadline(t), req)
		require.NoError(t, err, "GetLeadershipView RPC should succeed")
		require.NotNil(t, resp, "Response should not be nil")

		// Standby should also see the same leader information
		assert.NotEmpty(t, resp.LeaderId, "LeaderId should not be empty")
		assert.Equal(t, setup.PrimaryMultipooler.Name, resp.LeaderId, "LeaderId should be primary-multipooler")
		// LeaderTerm is deprecated and always 0 now (stored only in consensus state file)

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

		req := &consensusdatapb.CanReachPrimaryRequest{
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

		req := &consensusdatapb.CanReachPrimaryRequest{
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

		req := &consensusdatapb.CanReachPrimaryRequest{
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

// TestBeginTermDemotesPrimary verifies that when a primary accepts a BeginTerm
// for a higher term, it automatically demotes itself to prevent split-brain.
// The response includes the demote_lsn (final LSN before demotion).
func TestBeginTermDemotesPrimary(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)

	// Wait for both managers to be ready before running tests
	waitForManagerReady(t, setup, setup.PrimaryMultipooler)
	waitForManagerReady(t, setup, setup.StandbyMultipooler)

	// Create shared clients
	primaryConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { primaryConn.Close() })
	primaryConsensusClient := consensuspb.NewMultiPoolerConsensusClient(primaryConn)
	primaryManagerClient := multipoolermanagerpb.NewMultiPoolerManagerClient(primaryConn)

	standbyConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { standbyConn.Close() })
	standbyManagerClient := multipoolermanagerpb.NewMultiPoolerManagerClient(standbyConn)

	t.Run("BeginTerm_AutoDemotesPrimary", func(t *testing.T) {
		setupPoolerTest(t, setup)

		t.Log("=== Testing BeginTerm auto-demotion of primary ===")

		// Get current term and verify primary is actually primary
		statusReq := &consensusdatapb.StatusRequest{}
		statusResp, err := primaryConsensusClient.Status(utils.WithShortDeadline(t), statusReq)
		require.NoError(t, err)
		require.Equal(t, "primary", statusResp.Role, "Node should be primary before test")
		currentTerm := statusResp.CurrentTerm
		t.Logf("Current term: %d, role: %s", currentTerm, statusResp.Role)

		// Verify PostgreSQL is actually running as primary (not in recovery)
		primaryPoolerClient, err := endtoend.NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort))
		require.NoError(t, err)
		defer primaryPoolerClient.Close()

		resp, err := primaryPoolerClient.ExecuteQuery(context.Background(), "SELECT pg_is_in_recovery()", 1)
		require.NoError(t, err)
		// PostgreSQL wire protocol returns boolean as 't' or 'f' in text format
		require.Equal(t, "f", string(resp.Rows[0].Values[0]), "PostgreSQL should NOT be in recovery (should be primary)")
		t.Log("Confirmed PostgreSQL is running as primary")

		// Send BeginTerm with a higher term to the primary
		newTerm := currentTerm + 100 // Use a high term to avoid conflicts with other tests
		fakeCoordinatorID := &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIORCH,
			Cell:      "test-cell",
			Name:      "fake-coordinator-for-demote-test",
		}

		beginTermReq := &consensusdatapb.BeginTermRequest{
			Term:        newTerm,
			CandidateId: fakeCoordinatorID,
		}

		t.Logf("Sending BeginTerm with term %d to primary...", newTerm)
		beginTermResp, err := primaryConsensusClient.BeginTerm(utils.WithTimeout(t, 30*time.Second), beginTermReq)
		require.NoError(t, err, "BeginTerm RPC should succeed")

		// Verify response
		assert.True(t, beginTermResp.Accepted, "Primary should accept the higher term")
		assert.Equal(t, newTerm, beginTermResp.Term, "Response term should be the new term")
		assert.NotEmpty(t, beginTermResp.DemoteLsn, "Primary should include demote_lsn after auto-demotion")
		t.Logf("BeginTerm response: accepted=%v, term=%d, demote_lsn=%s",
			beginTermResp.Accepted, beginTermResp.Term, beginTermResp.DemoteLsn)

		// Wait for PostgreSQL to restart as standby
		time.Sleep(2 * time.Second)

		// Verify PostgreSQL is now in recovery mode (demoted)
		resp, err = primaryPoolerClient.ExecuteQuery(context.Background(), "SELECT pg_is_in_recovery()", 1)
		require.NoError(t, err)
		// PostgreSQL wire protocol returns boolean as 't' or 'f' in text format
		assert.Equal(t, "t", string(resp.Rows[0].Values[0]),
			"PostgreSQL should be in recovery after auto-demotion")
		t.Log("SUCCESS: PostgreSQL is now in recovery mode (demoted)")

		// Verify writes are rejected
		_, err = primaryPoolerClient.ExecuteQuery(context.Background(),
			"CREATE TABLE IF NOT EXISTS beginterm_demote_test (id serial PRIMARY KEY)", 1)
		require.Error(t, err, "Writes should be rejected after demotion")
		assert.Contains(t, err.Error(), "read-only", "Error should indicate read-only mode")
		t.Logf("SUCCESS: Writes correctly rejected: %v", err)

		// === RESTORE STATE ===
		// We need to restore the primary back to its original state for other tests
		t.Log("Restoring original state...")

		// Configure demoted primary to replicate from standby
		setPrimaryConnInfoReq := &multipoolermanagerdatapb.SetPrimaryConnInfoRequest{
			PrimaryPoolerId:       setup.StandbyMultipooler.Name,
			Host:                  "localhost",
			Port:                  int32(setup.StandbyMultipooler.PgPort),
			StopReplicationBefore: false,
			StartReplicationAfter: true,
			CurrentTerm:           newTerm,
			Force:                 true,
		}
		_, err = primaryManagerClient.SetPrimaryConnInfo(utils.WithShortDeadline(t), setPrimaryConnInfoReq)
		require.NoError(t, err, "SetPrimaryConnInfo should succeed after demotion")

		// Set term on standby for promotion
		promoteTerm := newTerm + 1
		setTermReq := &multipoolermanagerdatapb.SetTermRequest{
			Term: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber: promoteTerm,
			},
		}
		_, err = standbyManagerClient.SetTerm(utils.WithShortDeadline(t), setTermReq)
		require.NoError(t, err, "SetTerm should succeed on standby")

		// Stop replication on standby
		_, err = standbyManagerClient.StopReplication(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StopReplicationRequest{})
		require.NoError(t, err, "StopReplication should succeed")

		// Get current LSN
		standbyStatusReq := &multipoolermanagerdatapb.StandbyReplicationStatusRequest{}
		standbyStatusResp, err := standbyManagerClient.StandbyReplicationStatus(utils.WithShortDeadline(t), standbyStatusReq)
		require.NoError(t, err)
		standbyLSN := standbyStatusResp.Status.LastReplayLsn

		// Promote standby to primary
		promoteReq := &multipoolermanagerdatapb.PromoteRequest{
			ConsensusTerm: promoteTerm,
			ExpectedLsn:   standbyLSN,
			Force:         false,
		}
		_, err = standbyManagerClient.Promote(utils.WithTimeout(t, 10*time.Second), promoteReq)
		require.NoError(t, err, "Promote should succeed on standby")
		t.Log("Standby promoted to primary")

		// Now demote the new primary (standby) and promote original primary back
		demoteTerm := promoteTerm + 1
		setTermReq2 := &multipoolermanagerdatapb.SetTermRequest{
			Term: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber: demoteTerm,
			},
		}
		_, err = standbyManagerClient.SetTerm(utils.WithShortDeadline(t), setTermReq2)
		require.NoError(t, err)

		demoteReq := &multipoolermanagerdatapb.DemoteRequest{
			ConsensusTerm: demoteTerm,
			Force:         false,
		}
		_, err = standbyManagerClient.Demote(utils.WithTimeout(t, 10*time.Second), demoteReq)
		require.NoError(t, err, "Demote should succeed on new primary")
		t.Log("New primary (original standby) demoted")

		// Promote original primary back
		restoreTerm := demoteTerm + 1
		setTermReq3 := &multipoolermanagerdatapb.SetTermRequest{
			Term: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber: restoreTerm,
			},
		}
		_, err = primaryManagerClient.SetTerm(utils.WithShortDeadline(t), setTermReq3)
		require.NoError(t, err)

		_, err = primaryManagerClient.StopReplication(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StopReplicationRequest{})
		require.NoError(t, err)

		primaryStatusReq := &multipoolermanagerdatapb.StandbyReplicationStatusRequest{}
		primaryStatusResp, err := primaryManagerClient.StandbyReplicationStatus(utils.WithShortDeadline(t), primaryStatusReq)
		require.NoError(t, err)
		primaryLSN := primaryStatusResp.Status.LastReplayLsn

		promoteReq2 := &multipoolermanagerdatapb.PromoteRequest{
			ConsensusTerm: restoreTerm,
			ExpectedLsn:   primaryLSN,
			Force:         false,
		}
		_, err = primaryManagerClient.Promote(utils.WithTimeout(t, 10*time.Second), promoteReq2)
		require.NoError(t, err, "Promote should succeed on original primary")

		// Verify original primary is primary again
		resp, err = primaryPoolerClient.ExecuteQuery(context.Background(), "SELECT pg_is_in_recovery()", 1)
		require.NoError(t, err)
		// PostgreSQL wire protocol returns boolean as 't' or 'f' in text format
		require.Equal(t, "f", string(resp.Rows[0].Values[0]), "Original primary should be primary again")

		t.Log("Original state restored - primary is primary, standby is standby")
	})
}
