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

	"github.com/multigres/multigres/go/test/utils"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

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
		setupPoolerTest(t, setup)

		t.Log("=== Testing full Demote/Promote cycle ===")

		// Demote the original primary
		t.Log("Demoting original primary...")

		// Set term on primary
		setTermReq := &multipoolermanagerdatapb.SetTermRequest{
			Term: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber: 1,
			},
		}
		_, err := primaryManagerClient.SetTerm(utils.WithShortDeadline(t), setTermReq)
		require.NoError(t, err, "SetTerm should succeed on primary")

		// Get LSN before demotion
		posReq := &multipoolermanagerdatapb.PrimaryPositionRequest{}
		posResp, err := primaryManagerClient.PrimaryPosition(utils.WithShortDeadline(t), posReq)
		require.NoError(t, err, "PrimaryPosition should succeed before demotion")
		lsnBeforeDemotion := posResp.LsnPosition
		t.Logf("LSN before demotion: %s", lsnBeforeDemotion)

		// Perform demotion
		demoteReq := &multipoolermanagerdatapb.DemoteRequest{
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
			CurrentTerm:           1,
			Force:                 false,
		}
		_, err = primaryManagerClient.SetPrimaryConnInfo(utils.WithShortDeadline(t), setPrimaryConnInfoReq)
		require.NoError(t, err, "SetPrimaryConnInfo should succeed after demotion")

		// Verify primary operations no longer work
		_, err = primaryManagerClient.PrimaryPosition(utils.WithShortDeadline(t), posReq)
		require.Error(t, err, "PrimaryPosition should fail after demotion")

		t.Log("Promoting original standby to primary...")

		// Set term on standby
		setTermReq2 := &multipoolermanagerdatapb.SetTermRequest{
			Term: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber: 2,
			},
		}
		_, err = standbyManagerClient.SetTerm(utils.WithShortDeadline(t), setTermReq2)
		require.NoError(t, err, "SetTerm should succeed on standby")

		// Stop replication to freeze LSN
		stopReq := &multipoolermanagerdatapb.StopReplicationRequest{}
		_, err = standbyManagerClient.StopReplication(utils.WithShortDeadline(t), stopReq)
		require.NoError(t, err, "StopReplication should succeed")

		// Get current LSN
		statusReq := &multipoolermanagerdatapb.StandbyReplicationStatusRequest{}
		statusResp, err := standbyManagerClient.StandbyReplicationStatus(utils.WithShortDeadline(t), statusReq)
		require.NoError(t, err, "ReplicationStatus should succeed")
		currentLSN := statusResp.Status.LastReplayLsn
		t.Logf("Current LSN before promotion: %s", currentLSN)

		// Perform promotion
		promoteReq := &multipoolermanagerdatapb.PromoteRequest{
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
		setTermReq3 := &multipoolermanagerdatapb.SetTermRequest{
			Term: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber: 3,
			},
		}
		_, err = standbyManagerClient.SetTerm(utils.WithShortDeadline(t), setTermReq3)
		require.NoError(t, err, "SetTerm should succeed")

		demoteReq2 := &multipoolermanagerdatapb.DemoteRequest{
			ConsensusTerm: 3,
			DrainTimeout:  nil,
			Force:         false,
		}
		demoteResp2, err := standbyManagerClient.Demote(utils.WithTimeout(t, 10*time.Second), demoteReq2)
		require.NoError(t, err, "Demote should succeed on new primary")
		assert.False(t, demoteResp2.WasAlreadyDemoted)
		t.Logf("New primary demoted. LSN: %s", demoteResp2.LsnPosition)

		// Promote the original primary back
		setTermReq4 := &multipoolermanagerdatapb.SetTermRequest{
			Term: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber: 4,
			},
		}
		_, err = primaryManagerClient.SetTerm(utils.WithShortDeadline(t), setTermReq4)
		require.NoError(t, err, "SetTerm should succeed")

		// Stop replication on original primary
		stopReq2 := &multipoolermanagerdatapb.StopReplicationRequest{}
		_, err = primaryManagerClient.StopReplication(utils.WithShortDeadline(t), stopReq2)
		require.NoError(t, err, "StopReplication should succeed")

		// Get LSN
		statusReq2 := &multipoolermanagerdatapb.StandbyReplicationStatusRequest{}
		statusResp2, err := primaryManagerClient.StandbyReplicationStatus(utils.WithShortDeadline(t), statusReq2)
		require.NoError(t, err, "ReplicationStatus should succeed")
		currentLSN2 := statusResp2.Status.LastReplayLsn

		// Promote original primary back
		promoteReq2 := &multipoolermanagerdatapb.PromoteRequest{
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
		setupPoolerTest(t, setup)

		t.Log("Testing that Demote cannot be called twice after completion...")
		// TODO: This test needs to be hardened to actually
		// test that a promote that fail halfhway through
		// can be retried and successfully completes
		// in an idempotent way.

		// Set term
		setTermReq := &multipoolermanagerdatapb.SetTermRequest{
			Term: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber: 5,
			},
		}
		_, err := primaryManagerClient.SetTerm(utils.WithShortDeadline(t), setTermReq)
		require.NoError(t, err)

		// First demotion
		demoteReq := &multipoolermanagerdatapb.DemoteRequest{
			ConsensusTerm: 5,
			DrainTimeout:  nil,
			Force:         false,
		}
		demoteResp1, err := primaryManagerClient.Demote(utils.WithTimeout(t, 20*time.Second), demoteReq)
		require.NoError(t, err, "First demote should succeed")
		assert.False(t, demoteResp1.WasAlreadyDemoted)

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
			CurrentTerm:           5,
			Force:                 false,
		}
		_, err = primaryManagerClient.SetPrimaryConnInfo(utils.WithShortDeadline(t), setPrimaryConnInfoReq)
		require.NoError(t, err)

		// Second demotion should fail with guard rail error (server is now REPLICA in topology)
		_, err = primaryManagerClient.Demote(utils.WithTimeout(t, 10*time.Second), demoteReq)
		require.Error(t, err, "Second demote should fail - cannot demote a REPLICA")
		assert.Contains(t, err.Error(), "pooler type is REPLICA")

		t.Log("Demote guard rail verified - cannot demote a REPLICA")
	})

	t.Run("Idempotency_Promote", func(t *testing.T) {
		setupPoolerTest(t, setup)

		t.Log("Testing Promote idempotency...")

		// First demote the primary so we can test promote idempotency
		setTermReq := &multipoolermanagerdatapb.SetTermRequest{
			Term: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber: 6,
			},
		}
		_, err := primaryManagerClient.SetTerm(utils.WithShortDeadline(t), setTermReq)
		require.NoError(t, err)

		demoteReq := &multipoolermanagerdatapb.DemoteRequest{
			ConsensusTerm: 6,
			DrainTimeout:  nil,
			Force:         false,
		}
		_, err = primaryManagerClient.Demote(utils.WithTimeout(t, 10*time.Second), demoteReq)
		require.NoError(t, err, "Demote should succeed")

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
			CurrentTerm:           6,
			Force:                 false,
		}
		_, err = primaryManagerClient.SetPrimaryConnInfo(utils.WithShortDeadline(t), setPrimaryConnInfoReq)
		require.NoError(t, err)

		// Now test promote idempotency
		stopReq := &multipoolermanagerdatapb.StopReplicationRequest{}
		_, err = primaryManagerClient.StopReplication(utils.WithShortDeadline(t), stopReq)
		require.NoError(t, err)

		statusReq := &multipoolermanagerdatapb.StandbyReplicationStatusRequest{}
		statusResp, err := primaryManagerClient.StandbyReplicationStatus(utils.WithShortDeadline(t), statusReq)
		require.NoError(t, err)
		currentLSN := statusResp.Status.LastReplayLsn

		// First promotion
		promoteReq := &multipoolermanagerdatapb.PromoteRequest{
			ConsensusTerm:         6,
			ExpectedLsn:           currentLSN,
			SyncReplicationConfig: nil,
			Force:                 false,
		}
		promoteResp1, err := primaryManagerClient.Promote(utils.WithTimeout(t, 10*time.Second), promoteReq)
		require.NoError(t, err, "First promote should succeed")
		assert.False(t, promoteResp1.WasAlreadyPrimary)

		// Second promotion should SUCCEED with idempotent behavior (server is now PRIMARY in topology)
		// The new guard rail logic detects that everything is already complete and returns success
		promoteResp2, err := primaryManagerClient.Promote(utils.WithTimeout(t, 10*time.Second), promoteReq)
		require.NoError(t, err, "Second promote should succeed - idempotent operation")
		assert.True(t, promoteResp2.WasAlreadyPrimary, "Should report as already primary")
		assert.Equal(t, int64(6), promoteResp2.ConsensusTerm)

		t.Log("Promote idempotency verified - second call succeeds and reports WasAlreadyPrimary=true")
	})

	t.Run("TermValidation_Demote", func(t *testing.T) {
		setupPoolerTest(t, setup)

		t.Log("Testing Demote term validation...")

		setTermReq := &multipoolermanagerdatapb.SetTermRequest{
			Term: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber: 7,
			},
		}
		_, err := primaryManagerClient.SetTerm(utils.WithShortDeadline(t), setTermReq)
		require.NoError(t, err)

		// Try with stale term (should fail)
		demoteReq := &multipoolermanagerdatapb.DemoteRequest{
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
		setupPoolerTest(t, setup)

		t.Log("Testing Promote term validation...")

		// First demote the primary so we can test promote term validation
		setTermReq := &multipoolermanagerdatapb.SetTermRequest{
			Term: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber: 8,
			},
		}
		_, err := primaryManagerClient.SetTerm(utils.WithShortDeadline(t), setTermReq)
		require.NoError(t, err)

		demoteReq := &multipoolermanagerdatapb.DemoteRequest{
			ConsensusTerm: 8,
			DrainTimeout:  nil,
			Force:         false,
		}
		_, err = primaryManagerClient.Demote(utils.WithTimeout(t, 10*time.Second), demoteReq)
		require.NoError(t, err, "Demote should succeed")

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
			CurrentTerm:           8,
			Force:                 false,
		}
		_, err = primaryManagerClient.SetPrimaryConnInfo(utils.WithShortDeadline(t), setPrimaryConnInfoReq)
		require.NoError(t, err)

		// Now test promote term validation
		stopReq := &multipoolermanagerdatapb.StopReplicationRequest{}
		_, err = primaryManagerClient.StopReplication(utils.WithShortDeadline(t), stopReq)
		require.NoError(t, err)

		// Try with wrong term (should fail)
		promoteReq := &multipoolermanagerdatapb.PromoteRequest{
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
		setupPoolerTest(t, setup)

		t.Log("Testing Promote LSN validation...")

		// Demote primary first
		setTermReq := &multipoolermanagerdatapb.SetTermRequest{
			Term: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber: 9,
			},
		}
		_, err := primaryManagerClient.SetTerm(utils.WithShortDeadline(t), setTermReq)
		require.NoError(t, err)

		demoteReq := &multipoolermanagerdatapb.DemoteRequest{
			ConsensusTerm: 9,
			DrainTimeout:  nil,
			Force:         false,
		}
		_, err = primaryManagerClient.Demote(utils.WithTimeout(t, 10*time.Second), demoteReq)
		require.NoError(t, err)

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
			CurrentTerm:           9,
			Force:                 false,
		}
		_, err = primaryManagerClient.SetPrimaryConnInfo(utils.WithShortDeadline(t), setPrimaryConnInfoReq)
		require.NoError(t, err, "SetPrimaryConnInfo should succeed after demotion")

		// Now test LSN validation during promote
		setTermReq2 := &multipoolermanagerdatapb.SetTermRequest{
			Term: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber: 10,
			},
		}
		_, err = primaryManagerClient.SetTerm(utils.WithShortDeadline(t), setTermReq2)
		require.NoError(t, err)

		stopReq := &multipoolermanagerdatapb.StopReplicationRequest{}
		_, err = primaryManagerClient.StopReplication(utils.WithShortDeadline(t), stopReq)
		require.NoError(t, err)

		statusReq := &multipoolermanagerdatapb.StandbyReplicationStatusRequest{}
		statusResp, err := primaryManagerClient.StandbyReplicationStatus(utils.WithShortDeadline(t), statusReq)
		require.NoError(t, err)
		currentLSN := statusResp.Status.LastReplayLsn

		// Try with wrong LSN (should fail)
		promoteReq := &multipoolermanagerdatapb.PromoteRequest{
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
		setupPoolerTest(t, setup)

		t.Log("Testing Demote on standby (should fail)...")

		setTermReq := &multipoolermanagerdatapb.SetTermRequest{
			Term: &multipoolermanagerdatapb.ConsensusTerm{
				TermNumber: 11,
			},
		}
		_, err := standbyManagerClient.SetTerm(utils.WithShortDeadline(t), setTermReq)
		require.NoError(t, err)

		demoteReq := &multipoolermanagerdatapb.DemoteRequest{
			ConsensusTerm: 11,
			DrainTimeout:  nil,
			Force:         false,
		}
		_, err = standbyManagerClient.Demote(context.Background(), demoteReq)
		require.Error(t, err, "Demote should fail on standby")
		assert.Contains(t, err.Error(), "pooler type is REPLICA, must be PRIMARY")

		t.Log("Confirmed: Demote correctly rejected on standby")
	})
}
