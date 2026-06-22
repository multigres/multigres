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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/testtiming"
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
	start := time.Now()
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
	testtiming.Record(t, "cohort convergence", time.Since(start), timeout)
}

// TestCohortRotation_FullReplacement exercises a full rotation of the cohort:
// expand from 3 to 5 poolers, then gracefully drain all 3 originals (primary
// last). Verifies that membership transitions converge correctly at each step
// and that the final cohort consists only of the new poolers, with one of
// them elected as the new leader.
//
// This is the first e2e test of the cohort-INELIGIBLE-on-graceful-shutdown
// path (CohortMismatchAnalyzer → ReconcileCohortAction REMOVE) and of
// mid-cluster pooler join (CohortMismatchAnalyzer → ReconcileCohortAction
// ADD), both of which have unit coverage but were not previously exercised
// end-to-end. Write-buffering during failover is covered by
// TestDeadPrimaryRecovery and intentionally out of scope here.
//
// Why primary last: each follower drain doesn't trigger leader failover, so
// chaining them is cheap. Draining the primary is the single failover event
// in the test — running it after the cohort has already grown to 5 means
// LeaderResignedAnalyzer can pick from the two new poolers as recruitment
// candidates and the replacement is fast.
//
// Safety note (precarious cohort sizes): the test drains down to a 2-member
// final cohort [new-0, new-1]. Under common durability policies (e.g.
// AT_LEAST_N with N=2) that cohort cannot tolerate a further concurrent
// failure — a single death of either pooler would leave consensus unable
// to recruit a quorum. This is by design here: the test asserts that
// CohortMismatchAnalyzer removes EXPLICITLY-DEAD members (cache
// tombstones) unconditionally, even when the result is operationally
// fragile, because keeping dead members in the cohort record doesn't help
// (they aren't contributing anyway) and clutters consensus state. The
// less-confident "missing-from-cache without a tombstone" path is gated
// by commonconsensus.IsCohortMemberRemovalSafe and would not shrink past
// the policy's safety margin.
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
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
	)
	defer cleanup()

	setup.StartMultiOrchs(t.Context(), t)
	setup.RequireRecovery(t, "multiorch", 30*time.Second)
	setup.WaitForHealthStreamsEstablished(t, "multiorch", 30*time.Second)

	initialPrimary := setup.PrimaryName
	require.NotEmpty(t, initialPrimary, "initial primary must be elected")

	originals := make([]string, 0, 3)
	for name := range setup.Multipoolers {
		originals = append(originals, name)
	}
	require.Len(t, originals, 3)
	waitForCohortMembership(t, setup, originals, 30*time.Second)
	t.Logf("Initial cohort established: primary=%s, members=%v", initialPrimary, originals)

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
		shardsetup.WaitForManagerReady(t, inst.Multipooler)
		t.Logf("New pooler %s started; awaiting cohort admission", name)
	}

	expectedAfterExpand := append(append([]string{}, originals...), newPoolerNames...)
	// The new pooler join path runs fix-replication (configure standby + base
	// backup) and then cohort-add — both gated by multiorch's analyzer tick,
	// plus actual WAL streaming setup time.
	waitForCohortMembership(t, setup, expectedAfterExpand, 60*time.Second)
	t.Logf("Cohort expanded to %v", expectedAfterExpand)

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
		isPrimary := name == initialPrimary
		terminateMultipoolerGracefully(t, setup.Multipoolers[name], 20*time.Second)

		// Draining the primary triggers failover. Wait for the new leader to
		// be elected before polling cohort membership, otherwise the cohort
		// poll loop spends its budget seeing "no primary found" and times out
		// before failover completes on slower runners.
		if isPrimary {
			newPrimary := shardsetup.WaitForNewPrimary(t, setup, name, 90*time.Second)
			t.Logf("New primary elected after draining %s: %s", name, newPrimary)
		}

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
}
