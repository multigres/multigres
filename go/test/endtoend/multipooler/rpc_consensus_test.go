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

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"

	"github.com/multigres/multigres/go/common/consensus"

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
		assert.Equal(t, setup.PrimaryMultipooler.Name, resp.GetId().GetName(), "PoolerId should match")

		// Verify cell
		assert.Equal(t, "test-cell", resp.GetId().GetCell(), "Cell should match")

		// Verify term (should be 1 from setup)
		assert.Equal(t, int64(1), resp.GetConsensusStatus().GetTermRevocation().GetRevokedBelowTerm(), "TermNumber should be 1")

		// Verify this node is the consensus primary
		assert.True(t, consensus.IsPrimary(resp.GetConsensusStatus()), "Primary should be consensus primary")

		// Verify WAL position is present
		assert.NotEmpty(t, resp.GetConsensusStatus().GetCurrentPosition().GetLsn(), "CurrentLsn should not be empty on primary")
		assert.Regexp(t, `^[0-9A-F]+/[0-9A-F]+$`, resp.GetConsensusStatus().GetCurrentPosition().GetLsn(), "CurrentLsn should be in PostgreSQL format")

		t.Logf("Primary node status verified: CurrentLSN=%s", resp.GetConsensusStatus().GetCurrentPosition().GetLsn())
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
		assert.Equal(t, setup.StandbyMultipooler.Name, resp.GetId().GetName(), "PoolerId should match")

		// Verify cell
		assert.Equal(t, "test-cell", resp.GetId().GetCell(), "Cell should match")

		// Verify this node is not the consensus primary
		assert.False(t, consensus.IsPrimary(resp.GetConsensusStatus()), "Standby should not be consensus primary")

		t.Logf("Standby node status verified")
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

	// Get initial term from primary (primary can have a higher term than standby when
	// previous runs performed REVOKE on primary but not standby, so we must use
	// max(primary, standby) to avoid collision with an already-accepted term).
	consensusStatusReq := &consensusdatapb.StatusRequest{}
	primaryStatusResp, err := primaryConsensusClient.Status(utils.WithShortDeadline(t), consensusStatusReq)
	require.NoError(t, err)
	standbyStatusResp, err := standbyConsensusClient.Status(utils.WithShortDeadline(t), consensusStatusReq)
	require.NoError(t, err)
	primaryTerm := primaryStatusResp.GetConsensusStatus().GetTermRevocation().GetRevokedBelowTerm()
	standbyTerm := standbyStatusResp.GetConsensusStatus().GetTermRevocation().GetRevokedBelowTerm()
	expectedTerm := max(primaryTerm, standbyTerm)
	t.Logf("Initial term: %d (primary=%d, standby=%d)", expectedTerm, primaryTerm, standbyTerm)

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
		assert.True(t, managerStatusResp.Status.PostgresReady, "PostgreSQL should still be running")

		t.Logf("BeginTerm NO_ACTION on primary: term=%d, still primary with postgres running", expectedTerm)
	})

	t.Run("BeginTerm_NO_ACTION_Standby", func(t *testing.T) {
		t.Log("Testing BeginTerm with NO_ACTION on standby...")

		// First, set up replication so we can verify it's preserved after NO_ACTION
		setPrimaryReq := &multipoolermanagerdatapb.SetPrimaryConnInfoRequest{
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
		assert.True(t, managerStatusResp.Status.PostgresReady, "PostgreSQL should still be running")
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

		// Verify PostgreSQL is running as standby (emergency demotion restarts as standby)
		primaryPoolerClient, err := shardsetup.NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort))
		require.NoError(t, err)
		t.Cleanup(func() { primaryPoolerClient.Close() })
		require.Eventually(t, func() bool {
			inRecovery, err := postgresIsInRecovery(t, primaryPoolerClient)
			return err == nil && inRecovery
		}, 15*time.Second, 1*time.Second, "PostgreSQL should be running as standby after emergency demotion from BeginTerm on pooler: %s", setup.PrimaryMultipooler.Name)
		t.Log("Confirmed: PostgreSQL running as standby after emergency demotion")

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

		// Verify standby is replicating and not the consensus primary
		statusResp, err := standbyConsensusClient.Status(utils.WithShortDeadline(t), &consensusdatapb.StatusRequest{})
		require.NoError(t, err)
		assert.False(t, consensus.IsPrimary(statusResp.GetConsensusStatus()), "Standby should not be consensus primary before REVOKE")
		currentTerm := statusResp.GetConsensusStatus().GetTermRevocation().GetRevokedBelowTerm()

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
		assert.True(t, consensus.IsPrimary(statusResp.GetConsensusStatus()), "Node should be consensus primary before test")
		currentTerm := statusResp.GetConsensusStatus().GetTermRevocation().GetRevokedBelowTerm()
		t.Logf("Current term: %d", currentTerm)

		// Verify PostgreSQL is actually running as primary (not in recovery)
		primaryPoolerClient, err := shardsetup.NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort))
		require.NoError(t, err)
		defer primaryPoolerClient.Close()

		inRecovery, err := postgresIsInRecovery(t, primaryPoolerClient)
		require.NoError(t, err)
		require.False(t, inRecovery, "PostgreSQL should NOT be in recovery (should be primary)")
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

		// Verify PostgreSQL is running as standby (emergency demotion restarts as standby)
		require.Eventually(t, func() bool {
			inRecovery, err := postgresIsInRecovery(t, primaryPoolerClient)
			return err == nil && inRecovery
		}, 15*time.Second, 1*time.Second, "PostgreSQL should be running as standby after emergency demotion on pooler: %s", setup.PrimaryMultipooler.Name)
		t.Log("SUCCESS: PostgreSQL is running as standby after emergency demotion")

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
		setPrimaryConnInfoReq := &multipoolermanagerdatapb.SetPrimaryConnInfoRequest{
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
		standbyStatusResp, err := standbyManagerClient.Status(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StatusRequest{})
		require.NoError(t, err)
		require.NotNil(t, standbyStatusResp.Status.ReplicationStatus, "standby should have replication status")
		standbyLSN := standbyStatusResp.Status.ReplicationStatus.LastReplayLsn

		// Promote standby to primary
		// Use Force=true since we're testing BeginTerm auto-demote, not term validation
		promoteReq := &multipoolermanagerdatapb.PromoteRequest{
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
		_, err = standbyConsensusClient.EmergencyDemote(utils.WithTimeout(t, 10*time.Second), demoteReq)
		require.NoError(t, err, "Demote should succeed on new primary")
		t.Log("New primary (original standby) demoted")

		// Restore demoted standby to working state
		restoreAfterEmergencyDemotion(t, setup, setup.StandbyPgctld, setup.StandbyMultipooler, setup.StandbyMultipooler.Name)

		// Promote original primary back
		// Use Force=true since we're testing BeginTerm auto-demote, not term validation
		_, err = primaryManagerClient.StopReplication(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StopReplicationRequest{})
		require.NoError(t, err)

		primaryStatusResp, err := primaryManagerClient.Status(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StatusRequest{})
		require.NoError(t, err)
		require.NotNil(t, primaryStatusResp.Status.ReplicationStatus, "demoted primary should have replication status")
		primaryLSN := primaryStatusResp.Status.ReplicationStatus.LastReplayLsn

		promoteReq2 := &multipoolermanagerdatapb.PromoteRequest{
			ConsensusTerm: 0, // Ignored when Force=true
			ExpectedLsn:   primaryLSN,
			Force:         true,
		}
		_, err = primaryConsensusClient.Promote(utils.WithTimeout(t, 10*time.Second), promoteReq2)
		require.NoError(t, err, "Promote should succeed on original primary")

		// Verify original primary is primary again
		inRecovery, err = postgresIsInRecovery(t, primaryPoolerClient)
		require.NoError(t, err)
		require.False(t, inRecovery, "Original primary should be primary again")

		t.Log("Original state restored - primary is primary, standby is standby")
	})
}

// TestUpdateConsensusRule tests the UpdateConsensusRule API on the consensus service.
// UpdateConsensusRule was previously UpdateSynchronousStandbyList on the manager service.
func TestUpdateConsensusRule(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)

	waitForManagerReady(t, setup, setup.PrimaryMultipooler)
	waitForManagerReady(t, setup, setup.StandbyMultipooler)

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

	primaryPoolerClient, err := shardsetup.NewMultiPoolerTestClient(fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort))
	require.NoError(t, err)
	t.Cleanup(func() { primaryPoolerClient.Close() })

	// resetStandbys atomically replaces the standby list with the given IDs using ADD + REMOVE.
	// It requires the list to be non-empty on entry (each subtest starts from the shared cluster
	// state, which always has at least one real standby configured).
	resetStandbys := func(t *testing.T, ids ...*clustermetadatapb.ID) {
		t.Helper()

		// ADD all desired standbys first (keeps list non-empty throughout).
		_, err := primaryConsensusClient.UpdateConsensusRule(utils.WithShortDeadline(t),
			&multipoolermanagerdatapb.UpdateSynchronousStandbyListRequest{
				Operation:    multipoolermanagerdatapb.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_ADD,
				StandbyIds:   ids,
				ReloadConfig: true,
				Force:        true,
			})
		require.NoError(t, err, "ADD setup should succeed")

		// REMOVE any standbys that are currently in the list but not in the desired set.
		status := getPrimaryStatusFromClient(t, primaryManagerClient)
		var toRemove []*clustermetadatapb.ID
		for _, existing := range status.SyncReplicationConfig.GetStandbyIds() {
			wanted := false
			for _, id := range ids {
				if existing.Cell == id.Cell && existing.Name == id.Name {
					wanted = true
					break
				}
			}
			if !wanted {
				toRemove = append(toRemove, existing)
			}
		}
		if len(toRemove) > 0 {
			_, err = primaryConsensusClient.UpdateConsensusRule(utils.WithShortDeadline(t),
				&multipoolermanagerdatapb.UpdateSynchronousStandbyListRequest{
					Operation:    multipoolermanagerdatapb.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_REMOVE,
					StandbyIds:   toRemove,
					ReloadConfig: true,
					Force:        true,
				})
			require.NoError(t, err, "REMOVE cleanup should succeed")
		}

		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient,
			func(config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) bool {
				return config != nil && len(config.StandbyIds) == len(ids)
			}, "resetStandbys should converge")
	}

	t.Run("ADD_Success", func(t *testing.T) {
		setupPoolerTest(t, setup)
		t.Log("Testing UpdateConsensusRule ADD operation...")

		resetStandbys(t, makeMultipoolerID("test-cell", "standby1"))

		_, err := primaryConsensusClient.UpdateConsensusRule(utils.WithShortDeadline(t),
			&multipoolermanagerdatapb.UpdateSynchronousStandbyListRequest{
				Operation:    multipoolermanagerdatapb.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_ADD,
				StandbyIds:   []*clustermetadatapb.ID{makeMultipoolerID("test-cell", "standby2")},
				ReloadConfig: true,
				Force:        true,
			})
		require.NoError(t, err, "ADD should succeed")

		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient,
			func(config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) bool {
				return config != nil && len(config.StandbyIds) == 2 &&
					containsStandbyIDInConfig(config, "test-cell", "standby2")
			}, "ADD should converge")

		status := getPrimaryStatusFromClient(t, primaryManagerClient)
		assert.Len(t, status.SyncReplicationConfig.StandbyIds, 2)
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby1"))
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby2"))

		guc, err := shardsetup.QueryStringValue(utils.WithShortDeadline(t), primaryPoolerClient, "SHOW synchronous_standby_names")
		require.NoError(t, err)
		assert.Equal(t, `ANY 1 ("test-cell_standby1", "test-cell_standby2")`, guc, "GUC should reflect standby list after ADD")
		t.Log("ADD operation verified successfully")
	})

	t.Run("ADD_Idempotent", func(t *testing.T) {
		setupPoolerTest(t, setup)
		t.Log("Testing UpdateConsensusRule ADD is idempotent...")

		resetStandbys(t, makeMultipoolerID("test-cell", "standby1"), makeMultipoolerID("test-cell", "standby2"))
		initialStatus := getPrimaryStatusFromClient(t, primaryManagerClient)

		// ADD a standby that already exists
		_, err := primaryConsensusClient.UpdateConsensusRule(utils.WithShortDeadline(t),
			&multipoolermanagerdatapb.UpdateSynchronousStandbyListRequest{
				Operation:    multipoolermanagerdatapb.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_ADD,
				StandbyIds:   []*clustermetadatapb.ID{makeMultipoolerID("test-cell", "standby1")},
				ReloadConfig: true,
				Force:        true,
			})
		require.NoError(t, err, "ADD should be idempotent")

		time.Sleep(500 * time.Millisecond)
		afterStatus := getPrimaryStatusFromClient(t, primaryManagerClient)
		assert.Len(t, afterStatus.SyncReplicationConfig.StandbyIds, len(initialStatus.SyncReplicationConfig.StandbyIds),
			"Standby count should be unchanged after idempotent ADD")
		t.Log("ADD idempotency verified")
	})

	t.Run("REMOVE_Success", func(t *testing.T) {
		setupPoolerTest(t, setup)
		t.Log("Testing UpdateConsensusRule REMOVE operation...")

		resetStandbys(t,
			makeMultipoolerID("test-cell", "standby1"),
			makeMultipoolerID("test-cell", "standby2"),
			makeMultipoolerID("test-cell", "standby3"),
		)

		_, err := primaryConsensusClient.UpdateConsensusRule(utils.WithShortDeadline(t),
			&multipoolermanagerdatapb.UpdateSynchronousStandbyListRequest{
				Operation:    multipoolermanagerdatapb.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_REMOVE,
				StandbyIds:   []*clustermetadatapb.ID{makeMultipoolerID("test-cell", "standby2")},
				ReloadConfig: true,
				Force:        true,
			})
		require.NoError(t, err, "REMOVE should succeed")

		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient,
			func(config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) bool {
				return config != nil && len(config.StandbyIds) == 2 &&
					!containsStandbyIDInConfig(config, "test-cell", "standby2")
			}, "REMOVE should converge")

		status := getPrimaryStatusFromClient(t, primaryManagerClient)
		assert.Len(t, status.SyncReplicationConfig.StandbyIds, 2)
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby1"))
		assert.False(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby2"))
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby3"))

		guc, err := shardsetup.QueryStringValue(utils.WithShortDeadline(t), primaryPoolerClient, "SHOW synchronous_standby_names")
		require.NoError(t, err)
		assert.Equal(t, `ANY 1 ("test-cell_standby1", "test-cell_standby3")`, guc, "GUC should reflect standby list after REMOVE")
		t.Log("REMOVE operation verified successfully")
	})

	t.Run("REMOVE_NonExistent_Idempotent", func(t *testing.T) {
		setupPoolerTest(t, setup)
		t.Log("Testing UpdateConsensusRule REMOVE with non-existent standby (idempotency)...")

		resetStandbys(t, makeMultipoolerID("test-cell", "standby1"), makeMultipoolerID("test-cell", "standby2"))

		_, err := primaryConsensusClient.UpdateConsensusRule(utils.WithShortDeadline(t),
			&multipoolermanagerdatapb.UpdateSynchronousStandbyListRequest{
				Operation: multipoolermanagerdatapb.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_REMOVE,
				StandbyIds: []*clustermetadatapb.ID{
					makeMultipoolerID("test-cell", "does-not-exist"),
				},
				ReloadConfig: true,
				Force:        true,
			})
		require.NoError(t, err, "REMOVE of non-existent standby should succeed")

		status := getPrimaryStatusFromClient(t, primaryManagerClient)
		assert.Len(t, status.SyncReplicationConfig.StandbyIds, 2, "List should be unchanged")
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby1"))
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby2"))
		t.Log("REMOVE idempotency verified")
	})

	t.Run("ADD_Then_REMOVE_Sequence", func(t *testing.T) {
		setupPoolerTest(t, setup)
		t.Log("Testing UpdateConsensusRule ADD followed by REMOVE...")

		resetStandbys(t, makeMultipoolerID("test-cell", "standby1"), makeMultipoolerID("test-cell", "standby2"))

		// ADD two more
		_, err := primaryConsensusClient.UpdateConsensusRule(utils.WithShortDeadline(t),
			&multipoolermanagerdatapb.UpdateSynchronousStandbyListRequest{
				Operation: multipoolermanagerdatapb.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_ADD,
				StandbyIds: []*clustermetadatapb.ID{
					makeMultipoolerID("test-cell", "standby3"),
					makeMultipoolerID("test-cell", "standby4"),
				},
				ReloadConfig: true,
				Force:        true,
			})
		require.NoError(t, err, "ADD should succeed")

		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient,
			func(config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) bool {
				return config != nil && len(config.StandbyIds) == 4
			}, "ADD should converge to 4 standbys")

		// REMOVE two
		_, err = primaryConsensusClient.UpdateConsensusRule(utils.WithShortDeadline(t),
			&multipoolermanagerdatapb.UpdateSynchronousStandbyListRequest{
				Operation: multipoolermanagerdatapb.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_REMOVE,
				StandbyIds: []*clustermetadatapb.ID{
					makeMultipoolerID("test-cell", "standby2"),
					makeMultipoolerID("test-cell", "standby4"),
				},
				ReloadConfig: true,
				Force:        true,
			})
		require.NoError(t, err, "REMOVE should succeed")

		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient,
			func(config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) bool {
				return config != nil && len(config.StandbyIds) == 2 &&
					containsStandbyIDInConfig(config, "test-cell", "standby1") &&
					containsStandbyIDInConfig(config, "test-cell", "standby3")
			}, "REMOVE should converge")

		status := getPrimaryStatusFromClient(t, primaryManagerClient)
		assert.Len(t, status.SyncReplicationConfig.StandbyIds, 2)
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby1"))
		assert.False(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby2"))
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby3"))
		assert.False(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby4"))

		guc, err := shardsetup.QueryStringValue(utils.WithShortDeadline(t), primaryPoolerClient, "SHOW synchronous_standby_names")
		require.NoError(t, err)
		assert.Equal(t, `ANY 1 ("test-cell_standby1", "test-cell_standby3")`, guc, "GUC should reflect final standby list after ADD+REMOVE sequence")
		t.Log("ADD then REMOVE sequence verified successfully")
	})

	t.Run("Standby_Fails", func(t *testing.T) {
		setupPoolerTest(t, setup)
		t.Log("Testing UpdateConsensusRule fails on REPLICA pooler...")

		_, err := standbyConsensusClient.UpdateConsensusRule(utils.WithTimeout(t, 1*time.Second),
			&multipoolermanagerdatapb.UpdateSynchronousStandbyListRequest{
				Operation:    multipoolermanagerdatapb.StandbyUpdateOperation_STANDBY_UPDATE_OPERATION_ADD,
				StandbyIds:   []*clustermetadatapb.ID{makeMultipoolerID("test-cell", "standby1")},
				ReloadConfig: true,
				Force:        true,
			})
		require.Error(t, err, "UpdateConsensusRule should fail on standby")
		assert.Contains(t, err.Error(), "operation not allowed", "Error should indicate operation not allowed on REPLICA")
		t.Log("Confirmed: UpdateConsensusRule correctly rejected on REPLICA pooler")
	})
}
