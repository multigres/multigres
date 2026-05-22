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
		assert.True(t, consensus.IsLeader(resp.GetConsensusStatus()), "Primary should be consensus primary")

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
		assert.False(t, consensus.IsLeader(resp.GetConsensusStatus()), "Standby should not be consensus primary")

		t.Logf("Standby node status verified")
	})
}

// TestUpdateConsensusRule tests the UpdateConsensusRule API on the consensus service.
// UpdateConsensusRule was previously UpdateConsensusRule on the manager service.
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

	// realStandbyID is the actual standby pooler in the test setup. It must
	// stay in the synchronous standby list throughout each subtest so that
	// rule_history writes (which require sync ack from the cohort) can
	// acknowledge through it. With only fake-named standbys in the cohort,
	// every subsequent UpdateConsensusRule's rule_history INSERT would block
	// until the caller's deadline.
	//
	// Derive the name dynamically: multiorch elects either pooler-1 or
	// pooler-2 as primary, so the actual standby's name is whichever was
	// not elected.
	realStandbyName := setup.GetStandbys()[0].Name
	realStandbyID := makeMultipoolerID(setup.CellName, realStandbyName)
	// leaderID is the primary pooler. The rule store cohort always includes the leader, so
	// resetStandbys must never try to remove it.
	leaderID := makeMultipoolerID(setup.CellName, setup.PrimaryName)

	// resetStandbys atomically replaces the standby list using ADD + REMOVE.
	// realStandbyID is always present in the cohort so subsequent sync writes
	// can ack; it is implicitly added to ids if the caller did not include it.
	resetStandbys := func(t *testing.T, ids ...*clustermetadatapb.ID) {
		t.Helper()

		// Build the desired list with realStandbyID guaranteed to be present.
		desired := []*clustermetadatapb.ID{realStandbyID}
		for _, id := range ids {
			if id.Cell == realStandbyID.Cell && id.Name == realStandbyID.Name {
				continue
			}
			desired = append(desired, id)
		}

		// ADD all desired standbys first (keeps list non-empty throughout).
		_, err := primaryConsensusClient.UpdateConsensusRule(utils.WithTimeout(t, 10*time.Second),
			&multipoolermanagerdatapb.UpdateConsensusRuleRequest{
				Operation:            multipoolermanagerdatapb.CohortUpdateOperation_COHORT_UPDATE_OPERATION_ADD,
				StandbyIds:           desired,
				ExpectedOutgoingRule: currentRuleNumberFromClient(t, primaryManagerClient),
			})
		require.NoError(t, err, "ADD setup should succeed")

		// Read current cohort from rule store (not GUC) to find stale members to remove.
		// GUC may differ from rule store if setupPoolerTest cleanup restored it to baseline.
		statusResp, err := primaryManagerClient.Status(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StatusRequest{})
		require.NoError(t, err, "Status should succeed to read current cohort")
		var toRemove []*clustermetadatapb.ID
		for _, existing := range statusResp.Status.GetCohortMembers() {
			// The rule store cohort includes the leader; never try to remove it.
			if existing.Cell == leaderID.Cell && existing.Name == leaderID.Name {
				continue
			}
			wanted := false
			for _, id := range desired {
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
			_, err = primaryConsensusClient.UpdateConsensusRule(utils.WithTimeout(t, 10*time.Second),
				&multipoolermanagerdatapb.UpdateConsensusRuleRequest{
					Operation:            multipoolermanagerdatapb.CohortUpdateOperation_COHORT_UPDATE_OPERATION_REMOVE,
					StandbyIds:           toRemove,
					ExpectedOutgoingRule: currentRuleNumberFromClient(t, primaryManagerClient),
				})
			require.NoError(t, err, "REMOVE cleanup should succeed")
		}

		// GUC includes all cohort members: leader + desired standbys.
		// BuildSyncReplicationConfig intentionally includes the leader in SyncStandbyIDs.
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient,
			func(config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) bool {
				return config != nil && len(config.StandbyIds) == len(desired)+1
			}, "resetStandbys should converge")
	}

	t.Run("ADD_Success", func(t *testing.T) {
		setupPoolerTest(t, setup)
		t.Log("Testing UpdateConsensusRule ADD operation...")

		resetStandbys(t, makeMultipoolerID("test-cell", "standby1"))

		_, err := primaryConsensusClient.UpdateConsensusRule(utils.WithTimeout(t, 10*time.Second),
			&multipoolermanagerdatapb.UpdateConsensusRuleRequest{
				Operation:            multipoolermanagerdatapb.CohortUpdateOperation_COHORT_UPDATE_OPERATION_ADD,
				StandbyIds:           []*clustermetadatapb.ID{makeMultipoolerID("test-cell", "standby2")},
				ExpectedOutgoingRule: currentRuleNumberFromClient(t, primaryManagerClient),
			})
		require.NoError(t, err, "ADD should succeed")

		// 4 entries: leader + realStandby + standby1 + standby2
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient,
			func(config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) bool {
				return config != nil && len(config.StandbyIds) == 4 &&
					containsStandbyIDInConfig(config, "test-cell", "standby2")
			}, "ADD should converge")

		// Cohort: leader + realStandbyID + standby1 (from resetStandbys) + standby2.
		status := getPrimaryStatusFromClient(t, primaryManagerClient)
		assert.Len(t, status.SyncReplicationConfig.StandbyIds, 4)
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, setup.CellName, setup.PrimaryName))
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, setup.CellName, realStandbyName))
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby1"))
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby2"))

		guc, err := shardsetup.QueryStringValue(utils.WithShortDeadline(t), primaryPoolerClient, "SHOW synchronous_standby_names")
		require.NoError(t, err)
		assert.Contains(t, guc, fmt.Sprintf(`"%s_%s"`, setup.CellName, setup.PrimaryName), "GUC should include leader")
		assert.Contains(t, guc, fmt.Sprintf(`"%s_%s"`, setup.CellName, realStandbyName), "GUC should include real standby")
		assert.Contains(t, guc, `"test-cell_standby1"`, "GUC should include standby1")
		assert.Contains(t, guc, `"test-cell_standby2"`, "GUC should include standby2")
		t.Log("ADD operation verified successfully")
	})

	t.Run("ADD_Idempotent", func(t *testing.T) {
		setupPoolerTest(t, setup)
		t.Log("Testing UpdateConsensusRule ADD is idempotent...")

		resetStandbys(t, makeMultipoolerID("test-cell", "standby1"), makeMultipoolerID("test-cell", "standby2"))
		initialStatus := getPrimaryStatusFromClient(t, primaryManagerClient)

		// ADD a standby that already exists
		_, err := primaryConsensusClient.UpdateConsensusRule(utils.WithTimeout(t, 10*time.Second),
			&multipoolermanagerdatapb.UpdateConsensusRuleRequest{
				Operation:            multipoolermanagerdatapb.CohortUpdateOperation_COHORT_UPDATE_OPERATION_ADD,
				StandbyIds:           []*clustermetadatapb.ID{makeMultipoolerID("test-cell", "standby1")},
				ExpectedOutgoingRule: currentRuleNumberFromClient(t, primaryManagerClient),
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

		_, err := primaryConsensusClient.UpdateConsensusRule(utils.WithTimeout(t, 10*time.Second),
			&multipoolermanagerdatapb.UpdateConsensusRuleRequest{
				Operation:            multipoolermanagerdatapb.CohortUpdateOperation_COHORT_UPDATE_OPERATION_REMOVE,
				StandbyIds:           []*clustermetadatapb.ID{makeMultipoolerID("test-cell", "standby2")},
				ExpectedOutgoingRule: currentRuleNumberFromClient(t, primaryManagerClient),
			})
		require.NoError(t, err, "REMOVE should succeed")

		// 4 entries: leader + realStandby + standby1 + standby3
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient,
			func(config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) bool {
				return config != nil && len(config.StandbyIds) == 4 &&
					!containsStandbyIDInConfig(config, "test-cell", "standby2")
			}, "REMOVE should converge")

		// Cohort: leader + realStandbyID + standby1 + standby3 (standby2 removed).
		status := getPrimaryStatusFromClient(t, primaryManagerClient)
		assert.Len(t, status.SyncReplicationConfig.StandbyIds, 4)
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, setup.CellName, setup.PrimaryName))
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, setup.CellName, realStandbyName))
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby1"))
		assert.False(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby2"))
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby3"))

		guc, err := shardsetup.QueryStringValue(utils.WithShortDeadline(t), primaryPoolerClient, "SHOW synchronous_standby_names")
		require.NoError(t, err)
		assert.Contains(t, guc, fmt.Sprintf(`"%s_%s"`, setup.CellName, setup.PrimaryName), "GUC should include leader")
		assert.Contains(t, guc, fmt.Sprintf(`"%s_%s"`, setup.CellName, realStandbyName), "GUC should include real standby")
		assert.Contains(t, guc, `"test-cell_standby1"`, "GUC should include standby1")
		assert.Contains(t, guc, `"test-cell_standby3"`, "GUC should include standby3")
		t.Log("REMOVE operation verified successfully")
	})

	t.Run("REMOVE_NonExistent_Idempotent", func(t *testing.T) {
		setupPoolerTest(t, setup)
		t.Log("Testing UpdateConsensusRule REMOVE with non-existent standby (idempotency)...")

		resetStandbys(t, makeMultipoolerID("test-cell", "standby1"), makeMultipoolerID("test-cell", "standby2"))

		_, err := primaryConsensusClient.UpdateConsensusRule(utils.WithTimeout(t, 10*time.Second),
			&multipoolermanagerdatapb.UpdateConsensusRuleRequest{
				Operation: multipoolermanagerdatapb.CohortUpdateOperation_COHORT_UPDATE_OPERATION_REMOVE,
				StandbyIds: []*clustermetadatapb.ID{
					makeMultipoolerID("test-cell", "does-not-exist"),
				},
				ExpectedOutgoingRule: currentRuleNumberFromClient(t, primaryManagerClient),
			})
		require.NoError(t, err, "REMOVE of non-existent standby should succeed")

		// Cohort: leader + realStandbyID + standby1 + standby2.
		status := getPrimaryStatusFromClient(t, primaryManagerClient)
		assert.Len(t, status.SyncReplicationConfig.StandbyIds, 4, "List should be unchanged")
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, setup.CellName, setup.PrimaryName))
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, setup.CellName, realStandbyName))
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby1"))
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby2"))
		t.Log("REMOVE idempotency verified")
	})

	t.Run("ADD_Then_REMOVE_Sequence", func(t *testing.T) {
		setupPoolerTest(t, setup)
		t.Log("Testing UpdateConsensusRule ADD followed by REMOVE...")

		resetStandbys(t, makeMultipoolerID("test-cell", "standby1"), makeMultipoolerID("test-cell", "standby2"))

		// ADD two more
		_, err := primaryConsensusClient.UpdateConsensusRule(utils.WithTimeout(t, 10*time.Second),
			&multipoolermanagerdatapb.UpdateConsensusRuleRequest{
				Operation: multipoolermanagerdatapb.CohortUpdateOperation_COHORT_UPDATE_OPERATION_ADD,
				StandbyIds: []*clustermetadatapb.ID{
					makeMultipoolerID("test-cell", "standby3"),
					makeMultipoolerID("test-cell", "standby4"),
				},
				ExpectedOutgoingRule: currentRuleNumberFromClient(t, primaryManagerClient),
			})
		require.NoError(t, err, "ADD should succeed")

		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient,
			func(config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) bool {
				return config != nil && len(config.StandbyIds) == 6
			}, "ADD should converge to 6 standbys (leader + realStandby + standby1..4)")

		// REMOVE two
		_, err = primaryConsensusClient.UpdateConsensusRule(utils.WithTimeout(t, 10*time.Second),
			&multipoolermanagerdatapb.UpdateConsensusRuleRequest{
				Operation: multipoolermanagerdatapb.CohortUpdateOperation_COHORT_UPDATE_OPERATION_REMOVE,
				StandbyIds: []*clustermetadatapb.ID{
					makeMultipoolerID("test-cell", "standby2"),
					makeMultipoolerID("test-cell", "standby4"),
				},
				ExpectedOutgoingRule: currentRuleNumberFromClient(t, primaryManagerClient),
			})
		require.NoError(t, err, "REMOVE should succeed")

		// 4 entries: leader + realStandby + standby1 + standby3
		waitForSyncConfigConvergenceWithClient(t, primaryManagerClient,
			func(config *multipoolermanagerdatapb.SynchronousReplicationConfiguration) bool {
				return config != nil && len(config.StandbyIds) == 4 &&
					containsStandbyIDInConfig(config, "test-cell", "standby1") &&
					containsStandbyIDInConfig(config, "test-cell", "standby3")
			}, "REMOVE should converge")

		// Cohort: leader + realStandbyID + standby1 + standby3.
		status := getPrimaryStatusFromClient(t, primaryManagerClient)
		assert.Len(t, status.SyncReplicationConfig.StandbyIds, 4)
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, setup.CellName, setup.PrimaryName))
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, setup.CellName, realStandbyName))
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby1"))
		assert.False(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby2"))
		assert.True(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby3"))
		assert.False(t, containsStandbyIDInConfig(status.SyncReplicationConfig, "test-cell", "standby4"))

		guc, err := shardsetup.QueryStringValue(utils.WithShortDeadline(t), primaryPoolerClient, "SHOW synchronous_standby_names")
		require.NoError(t, err)
		assert.Contains(t, guc, fmt.Sprintf(`"%s_%s"`, setup.CellName, setup.PrimaryName), "GUC should include leader")
		assert.Contains(t, guc, fmt.Sprintf(`"%s_%s"`, setup.CellName, realStandbyName), "GUC should include real standby")
		assert.Contains(t, guc, `"test-cell_standby1"`, "GUC should include standby1")
		assert.Contains(t, guc, `"test-cell_standby3"`, "GUC should include standby3")
		t.Log("ADD then REMOVE sequence verified successfully")
	})

	t.Run("REMOVE_All_Fails", func(t *testing.T) {
		setupPoolerTest(t, setup)
		t.Log("Testing that removing all standbys fails with a clear error...")

		resetStandbys(t) // Set cohort to just realStandbyID

		_, err := primaryConsensusClient.UpdateConsensusRule(utils.WithTimeout(t, 10*time.Second),
			&multipoolermanagerdatapb.UpdateConsensusRuleRequest{
				Operation:            multipoolermanagerdatapb.CohortUpdateOperation_COHORT_UPDATE_OPERATION_REMOVE,
				StandbyIds:           []*clustermetadatapb.ID{realStandbyID},
				ExpectedOutgoingRule: currentRuleNumberFromClient(t, primaryManagerClient),
			})
		require.Error(t, err, "Removing all standbys should fail")
		assert.Contains(t, err.Error(), "durability not achievable", "Error should indicate cohort cannot satisfy durability policy")
		t.Log("Confirmed: removing all standbys correctly rejected")
	})

	t.Run("Standby_Fails", func(t *testing.T) {
		setupPoolerTest(t, setup)
		t.Log("Testing UpdateConsensusRule fails on REPLICA pooler...")

		_, err := standbyConsensusClient.UpdateConsensusRule(utils.WithTimeout(t, 1*time.Second),
			&multipoolermanagerdatapb.UpdateConsensusRuleRequest{
				Operation:  multipoolermanagerdatapb.CohortUpdateOperation_COHORT_UPDATE_OPERATION_ADD,
				StandbyIds: []*clustermetadatapb.ID{makeMultipoolerID("test-cell", "standby1")},
				// Expected rule is a placeholder; the call should fail at the
				// REPLICA guardrail before the CAS check would matter.
				ExpectedOutgoingRule: &clustermetadatapb.RuleNumber{},
			})
		require.Error(t, err, "UpdateConsensusRule should fail on standby")
		assert.Contains(t, err.Error(), "operation not allowed", "Error should indicate operation not allowed on REPLICA")
		t.Log("Confirmed: UpdateConsensusRule correctly rejected on REPLICA pooler")
	})
}
