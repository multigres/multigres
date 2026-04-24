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
	"os"
	"path/filepath"
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
)

// TestEmergencyDemoteAndPromote tests the full EmergencyDemote and Promote cycle
// This ensures that emergency demoting a primary and promoting a standby work together correctly,
// and that we can restore the original state at the end
func TestEmergencyDemoteAndPromote(t *testing.T) {
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
	primaryConsensusClient := consensuspb.NewMultiPoolerConsensusClient(primaryConn)

	standbyConn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", setup.StandbyMultipooler.GrpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { standbyConn.Close() })
	standbyManagerClient := multipoolermanagerpb.NewMultiPoolerManagerClient(standbyConn)
	standbyConsensusClient := consensuspb.NewMultiPoolerConsensusClient(standbyConn)

	t.Run("FullCycle_EmergencyDemoteAndPromote", func(t *testing.T) {
		setupPoolerTest(t, setup)

		t.Log("=== Testing full EmergencyDemote/Promote cycle ===")

		// Get current terms - tests use Force=true so actual term values don't matter,
		// but we use them for consistency in responses
		ctx := utils.WithShortDeadline(t)
		primaryTerm := shardsetup.MustGetCurrentTerm(t, ctx, primaryConsensusClient)
		t.Logf("Starting test with primary term: %d", primaryTerm)

		// Demote the original primary
		t.Log("Demoting original primary...")

		// Get LSN before demotion
		primaryStatusBefore, err := primaryConsensusClient.Status(utils.WithShortDeadline(t), &consensusdatapb.StatusRequest{})
		require.NoError(t, err, "Status should succeed before demotion")
		lsnBeforeDemotion := primaryStatusBefore.WalPosition.CurrentLsn
		t.Logf("LSN before demotion: %s", lsnBeforeDemotion)

		// Perform demotion with Force=true (testing demote functionality, not term validation)
		demoteReq := &multipoolermanagerdatapb.EmergencyDemoteRequest{
			Claim:        utils.NewTestClaim(primaryTerm),
			DrainTimeout: nil,
			Force:        true,
		}
		demoteResp, err := primaryConsensusClient.EmergencyDemote(utils.WithTimeout(t, 10*time.Second), demoteReq)
		require.NoError(t, err, "Demote should succeed")
		require.NotNil(t, demoteResp)

		assert.False(t, demoteResp.WasAlreadyDemoted, "Should not have been already demoted")
		assert.NotEmpty(t, demoteResp.LsnPosition)
		t.Logf("Demotion complete. LSN: %s, connections terminated: %d",
			demoteResp.LsnPosition, demoteResp.ConnectionsTerminated)

		// Restore demoted primary to working state (emergency demotion stops postgres)
		restoreAfterEmergencyDemotion(t, setup, setup.PrimaryPgctld, setup.PrimaryMultipooler, setup.PrimaryMultipooler.Name)

		// Now configure the demoted server to replicate from the standby (which will be promoted)
		t.Log("Configuring demoted primary to replicate from standby...")
		primary := &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "test-cell",
				Name:      setup.StandbyMultipooler.Name,
			},
			Hostname: "localhost",
			PortMap:  map[string]int32{"postgres": int32(setup.StandbyMultipooler.PgPort)},
		}
		setPrimaryConnInfoReq := &multipoolermanagerdatapb.SetPrimaryConnInfoRequest{
			Primary:               primary,
			StopReplicationBefore: false,
			StartReplicationAfter: true, // Start replication immediately
			Claim:                 nil,  // Ignored when Force=true
			Force:                 true,
		}
		_, err = primaryConsensusClient.SetPrimaryConnInfo(utils.WithShortDeadline(t), setPrimaryConnInfoReq)
		require.NoError(t, err, "SetPrimaryConnInfo should succeed after demotion")

		// Verify standby.signal exists after demotion and replication config
		t.Log("Verifying standby.signal exists after demotion...")
		primaryStandbySignalPath := filepath.Join(setup.PrimaryPgctld.PoolerDir, "pg_data", "standby.signal")
		_, statErr := os.Stat(primaryStandbySignalPath)
		assert.NoError(t, statErr, "standby.signal should exist after demotion")

		// Verify node is now in replica role after demotion
		primaryStatusAfter, err := primaryConsensusClient.Status(utils.WithShortDeadline(t), &consensusdatapb.StatusRequest{})
		require.NoError(t, err)
		assert.Equal(t, consensusdatapb.PostgresRole_POSTGRES_ROLE_REPLICA, primaryStatusAfter.Role,
			"Demoted primary should be in replica role after demotion and SetPrimaryConnInfo")

		t.Log("Promoting original standby to primary...")

		// Stop replication to freeze LSN
		stopReq := &multipoolermanagerdatapb.StopReplicationRequest{}
		_, err = standbyManagerClient.StopReplication(utils.WithShortDeadline(t), stopReq)
		require.NoError(t, err, "StopReplication should succeed")

		// Get current LSN
		standbyStatusResp, err := standbyManagerClient.Status(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StatusRequest{})
		require.NoError(t, err, "Status should succeed")
		require.NotNil(t, standbyStatusResp.Status.ReplicationStatus, "standby should have replication status")
		currentLSN := standbyStatusResp.Status.ReplicationStatus.LastReplayLsn
		t.Logf("Current LSN before promotion: %s", currentLSN)

		// Perform promotion with Force=true (testing promote functionality, not term validation)
		promoteReq := &multipoolermanagerdatapb.PromoteRequest{
			Claim:                 nil, // Ignored when Force=true
			ExpectedLsn:           currentLSN,
			SyncReplicationConfig: nil, // Don't configure sync replication for now
			Force:                 true,
		}
		promoteResp, err := standbyConsensusClient.Promote(utils.WithTimeout(t, 10*time.Second), promoteReq)
		require.NoError(t, err, "Promote should succeed")
		require.NotNil(t, promoteResp)

		assert.False(t, promoteResp.WasAlreadyPrimary, "Should not have been already primary")
		assert.NotEmpty(t, promoteResp.LsnPosition)
		t.Logf("Promotion complete. LSN: %s", promoteResp.LsnPosition)

		// Verify signal files are removed after promotion
		t.Log("Verifying signal files removed from newly promoted primary...")
		standbySignalPath := filepath.Join(setup.StandbyPgctld.PoolerDir, "pg_data", "standby.signal")
		recoverySignalPath := filepath.Join(setup.StandbyPgctld.PoolerDir, "pg_data", "recovery.signal")

		_, standbyStatErr := os.Stat(standbySignalPath)
		assert.True(t, os.IsNotExist(standbyStatErr), "standby.signal should not exist after promotion")

		_, recoveryStatErr := os.Stat(recoverySignalPath)
		assert.True(t, os.IsNotExist(recoveryStatErr), "recovery.signal should not exist after promotion")

		// Verify new primary works
		standbyNowPrimaryStatus, err := standbyConsensusClient.Status(utils.WithShortDeadline(t), &consensusdatapb.StatusRequest{})
		require.NoError(t, err, "Status should work on new primary")
		assert.NotEmpty(t, standbyNowPrimaryStatus.WalPosition.GetCurrentLsn())

		t.Log("Original standby is now primary")

		t.Log("Restoring original state...")

		// Demote the new primary (original standby) with Force=true
		demoteReq2 := &multipoolermanagerdatapb.EmergencyDemoteRequest{
			Claim:        nil, // Ignored when Force=true
			DrainTimeout: nil,
			Force:        true,
		}
		demoteResp2, err := standbyConsensusClient.EmergencyDemote(utils.WithTimeout(t, 10*time.Second), demoteReq2)
		require.NoError(t, err, "Demote should succeed on new primary")
		assert.False(t, demoteResp2.WasAlreadyDemoted)
		t.Logf("New primary demoted. LSN: %s", demoteResp2.LsnPosition)

		// Restore demoted standby to working state (emergency demotion stops postgres)
		restoreAfterEmergencyDemotion(t, setup, setup.StandbyPgctld, setup.StandbyMultipooler, setup.StandbyMultipooler.Name)

		// Verify standby.signal exists after second demotion
		t.Log("Verifying standby.signal exists after second demotion...")
		standbyStandbySignalPath := filepath.Join(setup.StandbyPgctld.PoolerDir, "pg_data", "standby.signal")
		_, statErr2 := os.Stat(standbyStandbySignalPath)
		assert.NoError(t, statErr2, "standby.signal should exist after demotion")

		// Stop replication on original primary
		stopReq2 := &multipoolermanagerdatapb.StopReplicationRequest{}
		_, err = primaryManagerClient.StopReplication(utils.WithShortDeadline(t), stopReq2)
		require.NoError(t, err, "StopReplication should succeed")

		// Get LSN
		primaryNowReplicaStatus, err := primaryManagerClient.Status(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StatusRequest{})
		require.NoError(t, err, "Status should succeed")
		require.NotNil(t, primaryNowReplicaStatus.Status.ReplicationStatus, "primary (now replica) should have replication status")
		currentLSN2 := primaryNowReplicaStatus.Status.ReplicationStatus.LastReplayLsn

		// Promote original primary back with Force=true
		promoteReq2 := &multipoolermanagerdatapb.PromoteRequest{
			Claim:                 nil, // Ignored when Force=true
			ExpectedLsn:           currentLSN2,
			SyncReplicationConfig: nil,
			Force:                 true,
		}
		promoteResp2, err := primaryConsensusClient.Promote(utils.WithTimeout(t, 10*time.Second), promoteReq2)
		require.NoError(t, err, "Promote should succeed")
		assert.False(t, promoteResp2.WasAlreadyPrimary)
		t.Logf("Original primary restored. LSN: %s", promoteResp2.LsnPosition)

		// Verify signal files are removed after restoring original primary
		t.Log("Verifying signal files removed from restored primary...")
		primaryStandbySignalPath = filepath.Join(setup.PrimaryPgctld.PoolerDir, "pg_data", "standby.signal")
		primaryRecoverySignalPath := filepath.Join(setup.PrimaryPgctld.PoolerDir, "pg_data", "recovery.signal")

		_, primaryStandbyStatErr := os.Stat(primaryStandbySignalPath)
		assert.True(t, os.IsNotExist(primaryStandbyStatErr), "standby.signal should not exist after promotion")

		_, primaryRecoveryStatErr := os.Stat(primaryRecoverySignalPath)
		assert.True(t, os.IsNotExist(primaryRecoveryStatErr), "recovery.signal should not exist after promotion")

		// Verify original primary works again
		restoredPrimaryStatus, err := primaryConsensusClient.Status(utils.WithShortDeadline(t), &consensusdatapb.StatusRequest{})
		require.NoError(t, err, "Status should work on restored primary")
		assert.NotEmpty(t, restoredPrimaryStatus.WalPosition.GetCurrentLsn())

		t.Log("Original state restored - primary is primary, standby is standby")
	})

	t.Run("Idempotency_EmergencyDemote", func(t *testing.T) {
		setupPoolerTest(t, setup)

		t.Log("Testing that EmergencyDemote cannot be called twice after completion...")
		// TODO: This test needs to be hardened to actually
		// test that a promote that fail halfhway through
		// can be retried and successfully completes
		// in an idempotent way.

		// First demotion with Force=true (testing demote behavior, not term validation)
		demoteReq := &multipoolermanagerdatapb.EmergencyDemoteRequest{
			Claim:        nil, // Ignored when Force=true
			DrainTimeout: nil,
			Force:        true,
		}
		demoteResp1, err := primaryConsensusClient.EmergencyDemote(utils.WithTimeout(t, 20*time.Second), demoteReq)
		require.NoError(t, err, "First demote should succeed")
		assert.False(t, demoteResp1.WasAlreadyDemoted)

		// Restore demoted primary to working state (emergency demotion stops postgres)
		restoreAfterEmergencyDemotion(t, setup, setup.PrimaryPgctld, setup.PrimaryMultipooler, setup.PrimaryMultipooler.Name)

		// Configure demoted primary to replicate from standby
		primary := &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "test-cell",
				Name:      setup.StandbyMultipooler.Name,
			},
			Hostname: "localhost",
			PortMap:  map[string]int32{"postgres": int32(setup.StandbyMultipooler.PgPort)},
		}
		setPrimaryConnInfoReq := &multipoolermanagerdatapb.SetPrimaryConnInfoRequest{
			Primary:               primary,
			StopReplicationBefore: false,
			StartReplicationAfter: true,
			Claim:                 nil, // Ignored when Force=true
			Force:                 true,
		}
		_, err = primaryConsensusClient.SetPrimaryConnInfo(utils.WithShortDeadline(t), setPrimaryConnInfoReq)
		require.NoError(t, err)

		// Second demotion should fail with guard rail error (server is now a standby in PostgreSQL)
		_, err = primaryConsensusClient.EmergencyDemote(utils.WithTimeout(t, 10*time.Second), demoteReq)
		require.Error(t, err, "Second emergency demote should fail - cannot demote a standby")
		assert.Contains(t, err.Error(), "standby mode")

		t.Log("EmergencyDemote guard rail verified - cannot demote a standby")
	})

	t.Run("Idempotency_Promote", func(t *testing.T) {
		setupPoolerTest(t, setup)

		t.Log("Testing Promote idempotency...")

		// First demote the primary so we can test promote idempotency
		demoteReq := &multipoolermanagerdatapb.EmergencyDemoteRequest{
			Claim:        nil, // Ignored when Force=true
			DrainTimeout: nil,
			Force:        true,
		}
		_, err = primaryConsensusClient.EmergencyDemote(utils.WithTimeout(t, 10*time.Second), demoteReq)
		require.NoError(t, err, "Demote should succeed")

		// Restore demoted primary to working state (emergency demotion stops postgres)
		restoreAfterEmergencyDemotion(t, setup, setup.PrimaryPgctld, setup.PrimaryMultipooler, setup.PrimaryMultipooler.Name)

		// Configure demoted primary to replicate from standby
		primary := &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "test-cell",
				Name:      setup.StandbyMultipooler.Name,
			},
			Hostname: "localhost",
			PortMap:  map[string]int32{"postgres": int32(setup.StandbyMultipooler.PgPort)},
		}
		setPrimaryConnInfoReq := &multipoolermanagerdatapb.SetPrimaryConnInfoRequest{
			Primary:               primary,
			StopReplicationBefore: false,
			StartReplicationAfter: true,
			Claim:                 nil, // Ignored when Force=true
			Force:                 true,
		}
		_, err = primaryConsensusClient.SetPrimaryConnInfo(utils.WithShortDeadline(t), setPrimaryConnInfoReq)
		require.NoError(t, err)

		// Now test promote idempotency
		stopReq := &multipoolermanagerdatapb.StopReplicationRequest{}
		_, err = primaryManagerClient.StopReplication(utils.WithShortDeadline(t), stopReq)
		require.NoError(t, err)

		idempotencyStatusResp, err := primaryManagerClient.Status(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StatusRequest{})
		require.NoError(t, err)
		require.NotNil(t, idempotencyStatusResp.Status.ReplicationStatus)
		currentLSN := idempotencyStatusResp.Status.ReplicationStatus.LastReplayLsn

		// First promotion with Force=true
		promoteReq := &multipoolermanagerdatapb.PromoteRequest{
			Claim:                 nil, // Ignored when Force=true
			ExpectedLsn:           currentLSN,
			SyncReplicationConfig: nil,
			Force:                 true,
		}
		promoteResp1, err := primaryConsensusClient.Promote(utils.WithTimeout(t, 10*time.Second), promoteReq)
		require.NoError(t, err, "First promote should succeed")
		assert.False(t, promoteResp1.WasAlreadyPrimary)

		// Second promotion should SUCCEED with idempotent behavior (server is now PRIMARY in topology)
		// The new guard rail logic detects that everything is already complete and returns success
		promoteResp2, err := primaryConsensusClient.Promote(utils.WithTimeout(t, 10*time.Second), promoteReq)
		require.NoError(t, err, "Second promote should succeed - idempotent operation")
		assert.True(t, promoteResp2.WasAlreadyPrimary, "Should report as already primary")

		t.Log("Promote idempotency verified - second call succeeds and reports WasAlreadyPrimary=true")
	})

	t.Run("TermValidation_EmergencyDemote", func(t *testing.T) {
		setupPoolerTest(t, setup)

		t.Log("Testing EmergencyDemote term validation...")

		// Get current term to test relative term validation
		ctx := utils.WithShortDeadline(t)
		currentTerm := shardsetup.MustGetCurrentTerm(t, ctx, primaryConsensusClient)
		t.Logf("Current term: %d", currentTerm)

		// Calculate stale term - if term is too low, skip this test
		// (term bumping via demote would leave node in REPLICA state)
		staleTerm := currentTerm - 2
		if staleTerm < 1 {
			t.Skipf("Skipping test: current term %d is too low for stale term validation (need at least 3)", currentTerm)
		}

		demoteReq := &multipoolermanagerdatapb.EmergencyDemoteRequest{
			Claim:        utils.NewTestClaim(staleTerm), // Less than current term
			DrainTimeout: nil,
			Force:        false,
		}
		_, err = primaryConsensusClient.EmergencyDemote(utils.WithTimeout(t, 10*time.Second), demoteReq)
		require.Error(t, err, "EmergencyDemote with stale term should fail")
		assert.Contains(t, err.Error(), "term")

		// Try with force flag (should succeed even with stale term)
		demoteReq.Force = true
		_, err = primaryConsensusClient.EmergencyDemote(utils.WithTimeout(t, 10*time.Second), demoteReq)
		require.NoError(t, err, "EmergencyDemote with force should succeed")

		// Restore demoted primary to working state (emergency demotion stops postgres)
		restoreAfterEmergencyDemotion(t, setup, setup.PrimaryPgctld, setup.PrimaryMultipooler, setup.PrimaryMultipooler.Name)

		t.Log("EmergencyDemote term validation verified")
	})

	t.Run("TermValidation_Promote", func(t *testing.T) {
		setupPoolerTest(t, setup)

		t.Log("Testing Promote term validation...")

		// Get current term for relative term values
		ctx := utils.WithShortDeadline(t)
		currentTerm := shardsetup.MustGetCurrentTerm(t, ctx, primaryConsensusClient)
		t.Logf("Current term: %d", currentTerm)

		// First demote the primary so we can test promote term validation
		// Use Force=true since we're testing promote validation, not demote
		demoteReq := &multipoolermanagerdatapb.EmergencyDemoteRequest{
			Claim:        nil, // Ignored when Force=true
			DrainTimeout: nil,
			Force:        true,
		}
		_, err = primaryConsensusClient.EmergencyDemote(utils.WithTimeout(t, 10*time.Second), demoteReq)
		require.NoError(t, err, "Demote should succeed")

		// Restore demoted primary to working state (emergency demotion stops postgres)
		restoreAfterEmergencyDemotion(t, setup, setup.PrimaryPgctld, setup.PrimaryMultipooler, setup.PrimaryMultipooler.Name)

		// Configure demoted primary to replicate from standby
		primary := &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "test-cell",
				Name:      setup.StandbyMultipooler.Name,
			},
			Hostname: "localhost",
			PortMap:  map[string]int32{"postgres": int32(setup.StandbyMultipooler.PgPort)},
		}
		setPrimaryConnInfoReq := &multipoolermanagerdatapb.SetPrimaryConnInfoRequest{
			Primary:               primary,
			StopReplicationBefore: false,
			StartReplicationAfter: true,
			Claim:                 nil, // Ignored when Force=true
			Force:                 true,
		}
		_, err = primaryConsensusClient.SetPrimaryConnInfo(utils.WithShortDeadline(t), setPrimaryConnInfoReq)
		require.NoError(t, err)

		// Now test promote term validation
		stopReq := &multipoolermanagerdatapb.StopReplicationRequest{}
		_, err = primaryManagerClient.StopReplication(utils.WithShortDeadline(t), stopReq)
		require.NoError(t, err)

		// Get the updated term after demote (term increases with operations)
		ctx = utils.WithShortDeadline(t)
		updatedTerm := shardsetup.MustGetCurrentTerm(t, ctx, primaryConsensusClient)
		staleTerm := max(updatedTerm-2, 0)
		t.Logf("Testing promote with stale term %d (current: %d)", staleTerm, updatedTerm)

		// Try with stale term (should fail)
		promoteReq := &multipoolermanagerdatapb.PromoteRequest{
			Claim:                 utils.NewTestClaim(staleTerm),
			ExpectedLsn:           "",
			SyncReplicationConfig: nil,
			Force:                 false,
		}
		_, err = primaryConsensusClient.Promote(utils.WithTimeout(t, 10*time.Second), promoteReq)
		require.Error(t, err, "Promote with stale term should fail")
		assert.Contains(t, err.Error(), "term")

		// Try with force flag (should succeed)
		promoteReq.Force = true
		_, err = primaryConsensusClient.Promote(utils.WithTimeout(t, 10*time.Second), promoteReq)
		require.NoError(t, err, "Promote with force should succeed")

		t.Log("Promote term validation verified")
	})

	t.Run("LSNValidation_Promote", func(t *testing.T) {
		setupPoolerTest(t, setup)

		t.Log("Testing Promote LSN validation...")

		// Demote primary first - use Force=true since we're testing LSN validation, not term
		demoteReq := &multipoolermanagerdatapb.EmergencyDemoteRequest{
			Claim:        nil, // Ignored when Force=true
			DrainTimeout: nil,
			Force:        true,
		}
		_, err = primaryConsensusClient.EmergencyDemote(utils.WithTimeout(t, 10*time.Second), demoteReq)
		require.NoError(t, err)

		// Restore demoted primary to working state (emergency demotion stops postgres)
		restoreAfterEmergencyDemotion(t, setup, setup.PrimaryPgctld, setup.PrimaryMultipooler, setup.PrimaryMultipooler.Name)

		// Configure the demoted server to replicate from the standby
		t.Log("Configuring demoted primary to replicate from standby...")
		primary := &clustermetadatapb.MultiPooler{
			Id: &clustermetadatapb.ID{
				Component: clustermetadatapb.ID_MULTIPOOLER,
				Cell:      "test-cell",
				Name:      setup.StandbyMultipooler.Name,
			},
			Hostname: "localhost",
			PortMap:  map[string]int32{"postgres": int32(setup.StandbyMultipooler.PgPort)},
		}
		setPrimaryConnInfoReq := &multipoolermanagerdatapb.SetPrimaryConnInfoRequest{
			Primary:               primary,
			StopReplicationBefore: false,
			StartReplicationAfter: true,
			Claim:                 nil, // Ignored when Force=true
			Force:                 true,
		}
		_, err = primaryConsensusClient.SetPrimaryConnInfo(utils.WithShortDeadline(t), setPrimaryConnInfoReq)
		require.NoError(t, err, "SetPrimaryConnInfo should succeed after demotion")

		// Now test LSN validation during promote
		stopReq := &multipoolermanagerdatapb.StopReplicationRequest{}
		_, err = primaryManagerClient.StopReplication(utils.WithShortDeadline(t), stopReq)
		require.NoError(t, err)

		lsnValidationStatusResp, err := primaryManagerClient.Status(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StatusRequest{})
		require.NoError(t, err)
		require.NotNil(t, lsnValidationStatusResp.Status.ReplicationStatus)
		currentLSN := lsnValidationStatusResp.Status.ReplicationStatus.LastReplayLsn

		// Get current term for the promote request
		ctx := utils.WithShortDeadline(t)
		currentTerm := shardsetup.MustGetCurrentTerm(t, ctx, primaryConsensusClient)

		// Try with wrong LSN (should fail) - use correct term so only LSN validation triggers
		promoteReq := &multipoolermanagerdatapb.PromoteRequest{
			Claim:                 utils.NewTestClaim(currentTerm),
			ExpectedLsn:           "FF/FFFFFFFF",
			SyncReplicationConfig: nil,
			Force:                 false,
		}
		_, err = primaryConsensusClient.Promote(utils.WithTimeout(t, 10*time.Second), promoteReq)
		require.Error(t, err, "Promote with wrong LSN should fail")
		assert.Contains(t, err.Error(), "LSN")

		// Try with correct LSN (should succeed)
		promoteReq.ExpectedLsn = currentLSN
		_, err = primaryConsensusClient.Promote(utils.WithTimeout(t, 10*time.Second), promoteReq)
		require.NoError(t, err, "Promote with correct LSN should succeed")

		t.Log("Promote LSN validation verified")
	})

	t.Run("ErrorCases_EmergencyDemoteOnStandby", func(t *testing.T) {
		setupPoolerTest(t, setup)

		t.Log("Testing EmergencyDemote on standby (should fail)...")

		// Use Force=true since we're testing error behavior for demote on standby,
		// not term validation. The demote should fail because PostgreSQL is in standby mode.
		demoteReq := &multipoolermanagerdatapb.EmergencyDemoteRequest{
			Claim:        nil, // Ignored when Force=true
			DrainTimeout: nil,
			Force:        true,
		}
		_, err = standbyConsensusClient.EmergencyDemote(context.Background(), demoteReq)
		require.Error(t, err, "EmergencyDemote should fail on standby")
		assert.Contains(t, err.Error(), "standby mode")

		t.Log("Confirmed: EmergencyDemote correctly rejected on standby")
	})
}
