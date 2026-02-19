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

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensuspb "github.com/multigres/multigres/go/pb/consensus"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

func TestConsensus_Status(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)

	// Wait for both managers to be ready before running tests
	waitForManagerReady(t, setup, setup.PrimaryMultipooler)
	waitForManagerReady(t, setup, setup.StandbyMultipooler)

	setupPoolerTest(t, setup, WithoutReplication())

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

	// Create manager clients for status validation
	primaryManagerClient := multipoolermanagerpb.NewMultiPoolerManagerClient(primaryConn)
	standbyManagerClient := multipoolermanagerpb.NewMultiPoolerManagerClient(standbyConn)

	// Get initial term and track it throughout the test
	consensusStatusReq := &consensusdatapb.StatusRequest{}
	consensusStatusResp, err := standbyConsensusClient.Status(utils.WithShortDeadline(t), consensusStatusReq)
	require.NoError(t, err)
	expectedTerm := consensusStatusResp.CurrentTerm
	t.Logf("Initial term: %d", expectedTerm)

	// Run NO_ACTION tests first to verify they don't disrupt the system
	t.Run("BeginTerm_NO_ACTION_Primary", func(t *testing.T) {
		t.Log("Testing BeginTerm with NO_ACTION on primary...")

		// Send BeginTerm with NO_ACTION using next term
		expectedTerm++
		req := &consensusdatapb.BeginTermRequest{
			Term: expectedTerm,
			CandidateId: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIORCH,
				Cell:      "zone1",
				Name:      "test-coordinator",
			},
			ShardId: "test-shard",
			Action:  consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_NO_ACTION,
		}

		resp, err := primaryConsensusClient.BeginTerm(utils.WithShortDeadline(t), req)
		require.NoError(t, err, "BeginTerm RPC should succeed")
		require.NotNil(t, resp, "Response should not be nil")

		// Verify acceptance without revoke
		assert.True(t, resp.Accepted, "Primary should accept BeginTerm with NO_ACTION")
		assert.Equal(t, expectedTerm, resp.Term, "Response term should match expected term")
		assert.Nil(t, resp.WalPosition, "NO_ACTION should not return WAL position")

		// Verify primary is still primary and postgres is running
		managerStatusReq := &multipoolermanagerdatapb.StatusRequest{}
		managerStatusResp, err := primaryManagerClient.Status(utils.WithShortDeadline(t), managerStatusReq)
		require.NoError(t, err)

		assert.Equal(t, clustermetadatapb.PoolerType_PRIMARY, managerStatusResp.Status.PoolerType, "Should still be PRIMARY")
		assert.Equal(t, "primary", managerStatusResp.Status.PostgresRole, "PostgreSQL should still be primary")
		assert.True(t, managerStatusResp.Status.PostgresRunning, "PostgreSQL should still be running")

		t.Logf("BeginTerm NO_ACTION on primary: term=%d, still primary with postgres running", expectedTerm)
	})

	t.Run("BeginTerm_NO_ACTION_Standby", func(t *testing.T) {
		t.Log("Testing BeginTerm with NO_ACTION on standby...")

		// First, set up replication so we can verify it's preserved after NO_ACTION
		setPrimaryReq := &consensusdatapb.SetPrimaryConnInfoRequest{
			Primary: &clustermetadatapb.MultiPooler{
				Id: &clustermetadatapb.ID{
					Component: clustermetadatapb.ID_MULTIPOOLER,
					Cell:      "zone1",
					Name:      "test-primary",
				},
				Hostname: "test-primary-host",
				PortMap:  map[string]int32{"postgres": 5432},
			},
			CurrentTerm:           expectedTerm,
			StartReplicationAfter: true,
		}
		_, err = standbyConsensusClient.SetPrimaryConnInfo(utils.WithShortDeadline(t), setPrimaryReq)
		require.NoError(t, err, "SetPrimaryConnInfo should succeed")

		// Wait for replication config to converge
		t.Log("Waiting for replication config to converge...")
		require.Eventually(t, func() bool {
			statusReq := &multipoolermanagerdatapb.StatusRequest{}
			statusResp, err := standbyManagerClient.Status(utils.WithShortDeadline(t), statusReq)
			if err != nil {
				return false
			}
			return statusResp.Status.ReplicationStatus != nil &&
				statusResp.Status.ReplicationStatus.PrimaryConnInfo != nil &&
				statusResp.Status.ReplicationStatus.PrimaryConnInfo.Host == "test-primary-host"
		}, 5*time.Second, 200*time.Millisecond, "Replication config should converge")

		// Send BeginTerm with NO_ACTION using next term
		expectedTerm++
		req := &consensusdatapb.BeginTermRequest{
			Term: expectedTerm,
			CandidateId: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIORCH,
				Cell:      "zone1",
				Name:      "test-coordinator",
			},
			ShardId: "test-shard",
			Action:  consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_NO_ACTION,
		}

		resp, err := standbyConsensusClient.BeginTerm(utils.WithShortDeadline(t), req)
		require.NoError(t, err, "BeginTerm RPC should succeed")
		require.NotNil(t, resp, "Response should not be nil")

		// Verify acceptance without revoke
		assert.True(t, resp.Accepted, "Standby should accept BeginTerm with NO_ACTION")
		assert.Equal(t, expectedTerm, resp.Term, "Response term should match expected term")
		assert.Nil(t, resp.WalPosition, "NO_ACTION should not return WAL position")

		// Verify standby still has replication configured (NO_ACTION should not clear it)
		managerStatusReq := &multipoolermanagerdatapb.StatusRequest{}
		managerStatusResp, err := standbyManagerClient.Status(utils.WithShortDeadline(t), managerStatusReq)
		require.NoError(t, err)

		assert.Equal(t, clustermetadatapb.PoolerType_REPLICA, managerStatusResp.Status.PoolerType, "Should still be REPLICA")
		assert.Equal(t, "standby", managerStatusResp.Status.PostgresRole, "PostgreSQL should still be standby")
		assert.True(t, managerStatusResp.Status.PostgresRunning, "PostgreSQL should still be running")
		assert.NotNil(t, managerStatusResp.Status.ReplicationStatus, "Replication status should be populated")
		assert.NotNil(t, managerStatusResp.Status.ReplicationStatus.PrimaryConnInfo, "Primary connection info should be preserved")
		assert.Equal(t, "test-primary-host", managerStatusResp.Status.ReplicationStatus.PrimaryConnInfo.Host,
			"Primary host should not change after NO_ACTION")

		t.Logf("BeginTerm NO_ACTION on standby: term=%d, still standby with replication preserved", expectedTerm)
	})

	t.Run("BeginTerm_OldTerm_Rejected", func(t *testing.T) {
		t.Log("Testing BeginTerm with old term (should be rejected)...")

		// Attempt to begin with a term older than current
		oldTerm := expectedTerm - 1
		req := &consensusdatapb.BeginTermRequest{
			Term: oldTerm,
			CandidateId: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "test-candidate",
			},
			ShardId: "test-shard",
			Action:  consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_REVOKE,
		}

		resp, err := standbyConsensusClient.BeginTerm(utils.WithShortDeadline(t), req)
		require.NoError(t, err, "BeginTerm RPC should succeed")
		require.NotNil(t, resp, "Response should not be nil")

		// Term should be rejected because it is too old
		assert.False(t, resp.Accepted, "Old term should not be accepted")
		assert.Equal(t, expectedTerm, resp.Term, "Response term should be current term")
		assert.Equal(t, setup.StandbyMultipooler.Name, resp.PoolerId, "PoolerId should match")

		t.Logf("BeginTerm correctly rejected old term %d (current: %d)", oldTerm, expectedTerm)
	})

	t.Run("BeginTerm_NewTerm_Accepted", func(t *testing.T) {
		t.Log("Testing BeginTerm with new term (should be accepted)...")

		// Register cleanup early so it runs even if assertions fail
		t.Cleanup(func() {
			restoreAfterEmergencyDemotion(t, setup, setup.PrimaryPgctld, setup.PrimaryMultipooler, setup.PrimaryMultipooler.Name)
		})

		// Begin with a newer term
		expectedTerm++
		req := &consensusdatapb.BeginTermRequest{
			Term: expectedTerm,
			CandidateId: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "new-leader-candidate",
			},
			ShardId: "test-shard",
			Action:  consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_REVOKE,
		}

		resp, err := primaryConsensusClient.BeginTerm(utils.WithShortDeadline(t), req)
		require.NoError(t, err, "BeginTerm RPC should succeed")
		require.NotNil(t, resp, "Response should not be nil")

		// New term should be accepted
		assert.True(t, resp.Accepted, "New term should be accepted")
		assert.Equal(t, expectedTerm, resp.Term, "Response term should be updated to new term")
		assert.Equal(t, setup.PrimaryMultipooler.Name, resp.PoolerId, "PoolerId should match")

		// Verify PostgreSQL is stopped (emergency demotion stops postgres)
		pgctldClient, err := shardsetup.NewPgctldClient(setup.PrimaryPgctld.GrpcPort)
		require.NoError(t, err)
		defer pgctldClient.Close()

		require.Eventually(t, func() bool {
			statusResp, err := pgctldClient.Status(context.Background(), &pgctldpb.StatusRequest{})
			return err == nil && statusResp.Status == pgctldpb.ServerStatus_STOPPED
		}, 10*time.Second, 1*time.Second, "PostgreSQL should be stopped after emergency demotion from BeginTerm on pooler: %s", setup.PrimaryMultipooler.Name)
		t.Log("Confirmed: PostgreSQL stopped after emergency demotion")

		t.Logf("BeginTerm correctly granted for new term %d", expectedTerm)
	})

	t.Run("BeginTerm_SameTerm_AlreadyAccepted", func(t *testing.T) {
		t.Log("Testing BeginTerm for same term after already accepting (should be rejected)...")

		// Try to begin same term again but with different candidate
		req := &consensusdatapb.BeginTermRequest{
			Term: expectedTerm,
			CandidateId: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "zone1",
				Name:      "different-candidate",
			},
			ShardId: "test-shard",
			Action:  consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_REVOKE,
		}

		resp, err := primaryConsensusClient.BeginTerm(utils.WithShortDeadline(t), req)
		require.NoError(t, err, "BeginTerm RPC should succeed")
		require.NotNil(t, resp, "Response should not be nil")

		// Candidate should be rejected because already accepted another leader in this term
		assert.False(t, resp.Accepted, "BeginTerm should not be accepted when already accepted this term for another leader")
		assert.Equal(t, expectedTerm, resp.Term, "Response term should remain at current term")

		t.Logf("BeginTerm correctly rejected different candidate for already-accepted term %d", expectedTerm)
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

	setupPoolerTest(t, setup, WithoutReplication())

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

	setupPoolerTest(t, setup, WithoutReplication())

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

// TestBeginTermEmergencyDemotesPrimary verifies that when a primary accepts a BeginTerm
// for a higher term, it automatically performs an emergency demotion to prevent split-brain.
// The response includes the WAL position with the final LSN before emergency demotion.
func TestBeginTermEmergencyDemotesPrimary(t *testing.T) {
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
	standbyConsensusClient := consensuspb.NewMultiPoolerConsensusClient(standbyConn)
	standbyManagerClient := multipoolermanagerpb.NewMultiPoolerManagerClient(standbyConn)

	t.Run("BeginTerm_RevokeStandby_ReplayCatchesUp", func(t *testing.T) {
		setupPoolerTest(t, setup)

		// Verify standby is replicating
		statusResp, err := standbyConsensusClient.Status(utils.WithShortDeadline(t), &consensusdatapb.StatusRequest{})
		require.NoError(t, err)
		require.Equal(t, "replica", statusResp.Role)
		currentTerm := statusResp.CurrentTerm

		// Send BeginTerm REVOKE to standby
		newTerm := currentTerm + 100
		resp, err := standbyConsensusClient.BeginTerm(utils.WithTimeout(t, 30*time.Second), &consensusdatapb.BeginTermRequest{
			Term: newTerm,
			CandidateId: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIORCH,
				Cell:      "test-cell",
				Name:      "test-coordinator-standby-revoke",
			},
			ShardId: "test-shard",
			Action:  consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_REVOKE,
		})
		require.NoError(t, err, "BeginTerm REVOKE on standby should succeed")
		require.NotNil(t, resp)

		assert.True(t, resp.Accepted, "Standby should accept the higher term")
		assert.Equal(t, newTerm, resp.Term)

		// Verify WAL positions are present after revoke
		require.NotNil(t, resp.WalPosition, "Standby should include WAL position after revoke")
		assert.NotEmpty(t, resp.WalPosition.LastReceiveLsn, "LastReceiveLsn should not be empty")
		assert.NotEmpty(t, resp.WalPosition.LastReplayLsn, "LastReplayLsn should not be empty")
	})

	t.Run("BeginTerm_AutoEmergencyDemotesPrimary", func(t *testing.T) {
		setupPoolerTest(t, setup)

		t.Log("=== Testing BeginTerm auto emergency demotion of primary ===")

		// Get current term and verify primary is actually primary
		statusReq := &consensusdatapb.StatusRequest{}
		statusResp, err := primaryConsensusClient.Status(utils.WithShortDeadline(t), statusReq)
		require.NoError(t, err)
		require.Equal(t, "primary", statusResp.Role, "Node should be primary before test")
		currentTerm := statusResp.CurrentTerm
		t.Logf("Current term: %d, role: %s", currentTerm, statusResp.Role)

		// Verify PostgreSQL is actually running as primary (not in recovery)
		primaryPoolerClient, err := shardsetup.NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort))
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
			Action:      consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_REVOKE,
		}

		t.Logf("Sending BeginTerm with term %d to primary...", newTerm)
		beginTermResp, err := primaryConsensusClient.BeginTerm(utils.WithTimeout(t, 30*time.Second), beginTermReq)
		require.NoError(t, err, "BeginTerm RPC should succeed")

		// Verify response
		assert.True(t, beginTermResp.Accepted, "Primary should accept the higher term")
		assert.Equal(t, newTerm, beginTermResp.Term, "Response term should be the new term")
		require.NotNil(t, beginTermResp.WalPosition, "Primary should include WAL position after auto-demotion")
		assert.NotEmpty(t, beginTermResp.WalPosition.CurrentLsn, "Primary should include current_lsn after auto-demotion")
		t.Logf("BeginTerm response: accepted=%v, term=%d, current_lsn=%s",
			beginTermResp.Accepted, beginTermResp.Term, beginTermResp.WalPosition.CurrentLsn)

		// Verify PostgreSQL is stopped (emergency demotion stops postgres, doesn't restart as standby)
		pgctldClient, err := shardsetup.NewPgctldClient(setup.PrimaryPgctld.GrpcPort)
		require.NoError(t, err)
		defer pgctldClient.Close()

		require.Eventually(t, func() bool {
			statusResp, err := pgctldClient.Status(context.Background(), &pgctldpb.StatusRequest{})
			return err == nil && statusResp.Status == pgctldpb.ServerStatus_STOPPED
		}, 10*time.Second, 1*time.Second, "PostgreSQL should be stopped after emergency demotion on pooler: %s", setup.PrimaryMultipooler.Name)
		t.Log("SUCCESS: PostgreSQL is stopped after emergency demotion")

		// === RESTORE STATE ===
		// We need to restore the primary back to its original state for other tests
		t.Log("Restoring original state...")

		// Restore demoted primary to working state
		restoreAfterEmergencyDemotion(t, setup, setup.PrimaryPgctld, setup.PrimaryMultipooler, setup.PrimaryMultipooler.Name)

		// Step 4: Configure demoted primary to replicate from standby
		primary := &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      setup.CellName,
				Name:      setup.StandbyMultipooler.Name,
			},
			Hostname: "localhost",
			PortMap:  map[string]int32{"postgres": int32(setup.StandbyMultipooler.PgPort)},
		}
		setPrimaryConnInfoReq := &consensusdatapb.SetPrimaryConnInfoRequest{
			Primary:               primary,
			StopReplicationBefore: false,
			StartReplicationAfter: true,
			CurrentTerm:           newTerm,
			Force:                 true,
		}
		_, err = primaryConsensusClient.SetPrimaryConnInfo(utils.WithShortDeadline(t), setPrimaryConnInfoReq)
		require.NoError(t, err, "SetPrimaryConnInfo should succeed after demotion")

		// Stop replication on standby to prepare for promotion
		_, err = standbyManagerClient.StopReplication(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StopReplicationRequest{})
		require.NoError(t, err, "StopReplication should succeed")

		// Get current LSN
		standbyStatusReq := &multipoolermanagerdatapb.StandbyReplicationStatusRequest{}
		standbyStatusResp, err := standbyManagerClient.StandbyReplicationStatus(utils.WithShortDeadline(t), standbyStatusReq)
		require.NoError(t, err)
		standbyLSN := standbyStatusResp.Status.LastReplayLsn

		// Promote standby to primary
		// Use Force=true since we're testing BeginTerm auto-demote, not term validation
		promoteReq := &consensusdatapb.PromoteRequest{
			ConsensusTerm: 0, // Ignored when Force=true
			ExpectedLsn:   standbyLSN,
			Force:         true,
		}
		_, err = standbyConsensusClient.Promote(utils.WithTimeout(t, 10*time.Second), promoteReq)
		require.NoError(t, err, "Promote should succeed on standby")
		t.Log("Standby promoted to primary")

		// Now demote the new primary (standby) and promote original primary back
		// Use Force=true since we're testing BeginTerm auto-demote, not term validation
		demoteReq := &multipoolermanagerdatapb.EmergencyDemoteRequest{
			ConsensusTerm: 0, // Ignored when Force=true
			Force:         true,
		}
		_, err = standbyManagerClient.EmergencyDemote(utils.WithTimeout(t, 10*time.Second), demoteReq)
		require.NoError(t, err, "Demote should succeed on new primary")
		t.Log("New primary (original standby) demoted")

		// Restore demoted standby to working state
		restoreAfterEmergencyDemotion(t, setup, setup.StandbyPgctld, setup.StandbyMultipooler, setup.StandbyMultipooler.Name)

		// Promote original primary back
		// Use Force=true since we're testing BeginTerm auto-demote, not term validation
		_, err = primaryManagerClient.StopReplication(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StopReplicationRequest{})
		require.NoError(t, err)

		primaryStatusReq := &multipoolermanagerdatapb.StandbyReplicationStatusRequest{}
		primaryStatusResp, err := primaryManagerClient.StandbyReplicationStatus(utils.WithShortDeadline(t), primaryStatusReq)
		require.NoError(t, err)
		primaryLSN := primaryStatusResp.Status.LastReplayLsn

		promoteReq2 := &consensusdatapb.PromoteRequest{
			ConsensusTerm: 0, // Ignored when Force=true
			ExpectedLsn:   primaryLSN,
			Force:         true,
		}
		_, err = primaryConsensusClient.Promote(utils.WithTimeout(t, 10*time.Second), promoteReq2)
		require.NoError(t, err, "Promote should succeed on original primary")

		// Verify original primary is primary again
		resp, err = primaryPoolerClient.ExecuteQuery(context.Background(), "SELECT pg_is_in_recovery()", 1)
		require.NoError(t, err)
		// PostgreSQL wire protocol returns boolean as 't' or 'f' in text format
		require.Equal(t, "f", string(resp.Rows[0].Values[0]), "Original primary should be primary again")

		t.Log("Original state restored - primary is primary, standby is standby")
	})
}
