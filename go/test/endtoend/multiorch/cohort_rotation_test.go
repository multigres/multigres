// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package multiorch

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// fetchLeaderCohort calls Status on the current shard leader and returns the
// names of recorded cohort members. Returns nil on any RPC failure so callers
// can retry inside polling loops without a hard test failure.
func fetchLeaderCohort(t *testing.T, setup *shardsetup.ShardSetup) []string {
	t.Helper()
	leader := setup.RefreshPrimary(t)
	if leader == nil {
		return nil
	}
	client, err := shardsetup.NewMultipoolerClient(leader.Multipooler.GrpcPort)
	if err != nil {
		return nil
	}
	defer client.Close()
	resp, err := client.Manager.Status(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StatusRequest{})
	if err != nil || resp == nil || resp.Status == nil {
		return nil
	}
	names := make([]string, 0, len(resp.Status.CohortMembers))
	for _, m := range resp.Status.CohortMembers {
		names = append(names, m.Name)
	}
	return names
}

// waitForCohortMembership blocks until the leader's cohort matches the
// expected set of pooler names (order-insensitive), or the timeout fires.
// Compares by Name because each test setup uses a single cell with
// uniquely-named poolers.
func waitForCohortMembership(t *testing.T, setup *shardsetup.ShardSetup, expected []string, timeout time.Duration) {
	t.Helper()
	expectedSet := make(map[string]struct{}, len(expected))
	for _, name := range expected {
		expectedSet[name] = struct{}{}
	}
	require.Eventually(t, func() bool {
		members := fetchLeaderCohort(t, setup)
		if len(members) != len(expected) {
			return false
		}
		for _, m := range members {
			if _, ok := expectedSet[m]; !ok {
				return false
			}
		}
		return true
	}, timeout, 500*time.Millisecond,
		"cohort never converged on %v", expected)
}

// TestCohortRotation_FullReplacement exercises a full rotation of the cohort:
// expand from 3 to 5 poolers, then gracefully drain all 3 originals (primary
// last). Verifies that writes through the multigateway continue to succeed
// throughout, and that the final cohort consists only of the new poolers.
//
// This is the first e2e test of the cohort-INELIGIBLE-on-graceful-shutdown
// path (CohortMismatchAnalyzer → ReconcileCohortAction REMOVE) and of
// mid-cluster pooler join (CohortMismatchAnalyzer → ReconcileCohortAction
// ADD), both of which have unit coverage but were not previously exercised
// end-to-end.
//
// Why primary last: each follower drain doesn't trigger leader failover, so
// chaining them is cheap. Draining the primary is the single failover event
// in the test — running it after the cohort has already grown to 5 means
// LeaderResignedAnalyzer can pick from the two new poolers as recruitment
// candidates and the replacement is fast.
func TestCohortRotation_FullReplacement(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping integration test (no postgres binaries)")
	}

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiOrchCount(1),
		shardsetup.WithMultigateway(),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
	)
	defer cleanup()

	setup.StartMultiOrchs(t.Context(), t)
	setup.RequireRecovery(t, "multiorch", 30*time.Second)
	setup.WaitForHealthStreamsEstablished(t, "multiorch", 30*time.Second)
	setup.WaitForMultigatewayQueryServing(t)

	initialPrimary := setup.PrimaryName
	require.NotEmpty(t, initialPrimary, "initial primary must be elected")

	originals := make([]string, 0, 3)
	for name := range setup.Multipoolers {
		originals = append(originals, name)
	}
	require.Len(t, originals, 3)
	waitForCohortMembership(t, setup, originals, 30*time.Second)
	t.Logf("Initial cohort established: primary=%s, members=%v", initialPrimary, originals)

	// Continuous writer through the multigateway. The gateway re-routes on
	// failover, so the writer sees the rotation as (mostly) transparent
	// availability.
	connStr := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")
	gatewayDB, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	defer gatewayDB.Close()
	require.NoError(t, gatewayDB.Ping(), "failed to ping multigateway")

	validator, validatorCleanup, err := shardsetup.NewWriterValidator(t, gatewayDB,
		shardsetup.WithWorkerCount(4),
		shardsetup.WithWriteInterval(10*time.Millisecond),
	)
	require.NoError(t, err)
	t.Cleanup(validatorCleanup)
	validator.Start(t)
	t.Logf("Continuous writer started (table=%s)", validator.TableName())

	// Phase 1: expand cohort from 3 to 5.
	newPoolerNames := []string{"new-0", "new-1"}
	for _, name := range newPoolerNames {
		grpcPort := utils.GetFreePort(t)
		pgPort := utils.GetFreePort(t)
		multipoolerPort := utils.GetFreePort(t)
		inst := setup.CreateMultipoolerInstance(t, name, grpcPort, pgPort, multipoolerPort)

		ctx := t.Context()
		require.NoError(t, inst.Pgctld.Start(ctx, t),
			"failed to start pgctld for %s", name)
		require.NoError(t, inst.Multipooler.Start(ctx, t),
			"failed to start multipooler for %s", name)
		shardsetup.WaitForManagerReady(t, inst.Multipooler, nil)
		t.Logf("New pooler %s started; awaiting cohort admission", name)
	}

	expectedAfterExpand := append(append([]string{}, originals...), newPoolerNames...)
	// Generous timeout: the new pooler join path runs fix-replication
	// (configure standby + base backup) and then cohort-add — both gated by
	// multiorch's analyzer tick, plus actual WAL streaming setup time.
	waitForCohortMembership(t, setup, expectedAfterExpand, 30*time.Second)
	t.Logf("Cohort expanded to %v", expectedAfterExpand)

	expandSuccess, expandFailed := validator.Stats()
	t.Logf("After expand: %d successful, %d failed writes", expandSuccess, expandFailed)
	require.Positive(t, expandSuccess, "writes should have accumulated during expand")

	// Phase 2: drain originals, primary last.
	drainOrder := make([]string, 0, len(originals))
	for _, name := range originals {
		if name != initialPrimary {
			drainOrder = append(drainOrder, name)
		}
	}
	drainOrder = append(drainOrder, initialPrimary)

	remaining := append([]string{}, expectedAfterExpand...)
	for _, name := range drainOrder {
		t.Logf("Draining %s gracefully", name)
		terminateMultipoolerGracefully(t, setup.Multipoolers[name], 20*time.Second)

		// Remove the drained pooler from the expected set, then wait for the
		// leader's cohort view to converge.
		next := make([]string, 0, len(remaining)-1)
		for _, candidate := range remaining {
			if candidate != name {
				next = append(next, candidate)
			}
		}
		remaining = next
		waitForCohortMembership(t, setup, remaining, 30*time.Second)
		t.Logf("Cohort after draining %s: %v", name, remaining)
	}

	// Final cohort should be exactly the two new poolers.
	require.ElementsMatch(t, newPoolerNames, remaining,
		"final cohort should be exactly the new poolers")

	// And the leader must be one of them — we drained the original primary
	// last, so a successful failover puts a new pooler in the leader slot.
	finalLeader := setup.RefreshPrimary(t)
	require.NotNil(t, finalLeader, "a primary must remain after rotation")
	require.Contains(t, newPoolerNames, finalLeader.Name,
		"final primary should be one of the new poolers, got %s", finalLeader.Name)
	t.Logf("Final primary: %s; final cohort: %v", finalLeader.Name, remaining)

	// Stop writes and verify continuity. Some failures during the single
	// leader-failover window are expected (writes in flight when the old
	// primary is killed will fail), but a clean rotation should keep the
	// failure rate well below 25%. A higher rate indicates the cohort
	// dropped below quorum or the gateway took too long to reroute.
	validator.Stop()
	successful, failed := validator.Stats()
	t.Logf("Final write stats: %d successful, %d failed", successful, failed)
	require.Positive(t, successful, "expected successful writes throughout rotation")

	total := successful + failed
	failureRate := float64(failed) / float64(total)
	assert.Less(t, failureRate, 0.25,
		"write failure rate %.1f%% during rotation is too high; errors: %v",
		failureRate*100, validator.FailedErrors())

	// Sanity: the writes that the validator recorded as successful must be
	// readable from the new cohort. Read from each surviving pooler since the
	// validator records only what the gateway acknowledged.
	finalClients := make([]*shardsetup.MultiPoolerTestClient, 0, len(newPoolerNames))
	for _, name := range newPoolerNames {
		addr := fmt.Sprintf("localhost:%d", setup.Multipoolers[name].Multipooler.GrpcPort)
		client, err := shardsetup.NewMultiPoolerTestClient(addr)
		require.NoError(t, err)
		t.Cleanup(func() { client.Close() })
		finalClients = append(finalClients, client)
	}
	require.NoError(t, validator.Verify(t, finalClients),
		"successful writes must be readable from the new cohort")
}
