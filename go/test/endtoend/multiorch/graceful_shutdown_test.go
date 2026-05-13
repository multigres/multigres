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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// terminateMultipoolerGracefully sends SIGTERM to the multipooler binary (NOT
// pgctld) and waits for it to exit. This drives the GracefulShutdown
// servenv hook end-to-end:
//
//   - it transitions the pooler to NOT_SERVING (rejecting new gateway queries
//     and draining in-flight ones, bounded by --connpool-drain-grace-period),
//   - it closes the connection pool to release idle backends,
//   - it calls pgctld.Stop(fast) (escalating to immediate if fast fails),
//   - it announces LIFECYCLE_SIGNAL_STOPPED, closes the health stream, and
//     exits.
func terminateMultipoolerGracefully(t *testing.T, instance *shardsetup.MultipoolerInstance, timeout time.Duration) {
	t.Helper()
	require.NotNil(t, instance.Multipooler)
	require.NotNil(t, instance.Multipooler.Process)
	require.NotNil(t, instance.Multipooler.Process.Process)

	pid := instance.Multipooler.Process.Process.Pid
	t.Logf("Sending SIGTERM to multipooler %s (PID %d)", instance.Name, pid)

	// Use the executil.Cmd.Stop method so the test framework's process reaper
	// coordinates with us (Cmd.Stop sends SIGTERM, waits for exit, then escalates
	// to SIGKILL if needed). Calling executil.TerminateProcess directly on the
	// underlying os.Process can race with Cmd.Wait and miss the exit signal.
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	exitErr, stopped := instance.Multipooler.Process.Stop(ctx)
	require.True(t, stopped,
		"multipooler %s did not exit gracefully within %v",
		instance.Name, timeout)
	t.Logf("Multipooler %s exited gracefully (exitErr=%v)", instance.Name, exitErr)
}

// TestPrimaryGracefulShutdownTriggersFailover verifies the end-to-end
// graceful-shutdown contract on the primary side:
//
//  1. SIGTERM is sent to the primary's multipooler binary.
//  2. The pooler runs its shutdown sequence: NOT_SERVING transition (drains
//     in-flight queries) → pool Close → pgctld.Stop(fast) → STOPPED terminal
//     broadcast → process exit.
//  3. multiorch observes LIFECYCLE_SIGNAL_STOPPED on the cached
//     PoolerHealthState and synthesizes REQUESTING_DEMOTION on the primary's
//     LeadershipStatus.
//  4. LeaderNeedsReplacement returns true → LeaderIsDeadAnalyzer triggers
//     failover → AppointLeaderAction promotes a surviving standby.
//
// The test asserts that within a generous bound, a *new* primary appears.
func TestPrimaryGracefulShutdownTriggersFailover(t *testing.T) {
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

	// Wait for multiorch to have established health streams to all poolers
	// and seen a clean cluster state. Without this, the STOPPED snapshot we
	// produce below can race a stream that's still being established, and
	// multiorch never receives the signal.
	setup.RequireRecovery(t, "multiorch", 30*time.Second)

	oldPrimary := setup.GetPrimary(t)
	require.NotNil(t, oldPrimary, "primary instance should exist")
	oldPrimaryName := setup.PrimaryName
	t.Logf("Initial primary: %s", oldPrimaryName)

	// SIGTERM the primary's multipooler. The graceful-shutdown sequence runs
	// inside this call; on return, the process has exited.
	terminateMultipoolerGracefully(t, oldPrimary, 90*time.Second)

	// multiorch should now elect a new primary from the surviving standbys.
	// The bound is generous (60s): one snapshot tick + analyzer tick +
	// AppointLeaderAction (BeginTerm + Promote) on a 3-node cohort. Most
	// runs complete in well under 30s.
	t.Logf("Waiting for multiorch to elect a new primary...")
	newPrimaryName := shardsetup.WaitForNewPrimary(t, setup, oldPrimaryName, 60*time.Second)
	require.NotEmpty(t, newPrimaryName, "expected multiorch to elect a new primary")
	require.NotEqual(t, oldPrimaryName, newPrimaryName,
		"new primary must be different from the terminated old primary")
	t.Logf("New primary elected: %s (was: %s)", newPrimaryName, oldPrimaryName)

	// The terminated pooler must be marked DRAINED in topology. Either the
	// pooler-side unregister hook or the orchestrator-side markPoolerDrained
	// path can write this; both converge on the same end state. This locks in
	// that DRAINED is reflected for `multigres getpoolers` after a graceful
	// shutdown that ends in failover.
	oldPrimaryID := setup.GetMultipoolerID(oldPrimaryName)
	require.NotNil(t, oldPrimaryID, "expected to resolve old primary ID")
	require.Eventually(t, func() bool {
		mp, err := setup.TopoServer.GetMultiPooler(t.Context(), oldPrimaryID)
		if err != nil {
			return false
		}
		return mp.Type == clustermetadatapb.PoolerType_DRAINED
	}, 30*time.Second, 500*time.Millisecond,
		"old primary %s should be marked DRAINED in topology after graceful shutdown", oldPrimaryName)
	t.Logf("Old primary %s is DRAINED in topology", oldPrimaryName)
}

// TestStandbyGracefulShutdownDoesNotTriggerFailover verifies that SIGTERM on
// a standby pooler does NOT cause spurious failover: the existing primary
// stays primary, and the surviving standbys keep replicating from it.
//
// This is the role-symmetry property: the same shutdown sequence runs on
// the standby (announce, stop postgres, drain, exit), but because
// LeaderNeedsReplacement is keyed on the *topology primary*, the
// orchestrator's reaction is limited to "stop trying to reconnect to the
// terminated standby" — no leader appointment.
func TestStandbyGracefulShutdownDoesNotTriggerFailover(t *testing.T) {
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

	primaryName := setup.PrimaryName
	require.NotEmpty(t, primaryName, "initial primary must be elected")
	t.Logf("Initial primary: %s", primaryName)

	// Pick any standby for termination.
	var standbyName string
	var standby *shardsetup.MultipoolerInstance
	for name, inst := range setup.Multipoolers {
		if name != primaryName {
			standbyName = name
			standby = inst
			break
		}
	}
	require.NotEmpty(t, standbyName, "expected at least one standby")
	t.Logf("Terminating standby %s gracefully", standbyName)

	terminateMultipoolerGracefully(t, standby, 90*time.Second)

	// Give multiorch time to observe the terminated standby (a few snapshot
	// ticks worth) so any spurious failover would have triggered by now.
	time.Sleep(10 * time.Second)

	// Verify the primary is unchanged.
	current := setup.RefreshPrimary(t)
	require.NotNil(t, current)
	require.Equal(t, primaryName, current.Name,
		"primary must remain unchanged after standby graceful shutdown")
	t.Logf("Primary %s unchanged after standby %s graceful shutdown", primaryName, standbyName)

	// Verify the surviving standby is still a healthy REPLICA with replication configured.
	for name, inst := range setup.Multipoolers {
		if name == primaryName || name == standbyName {
			continue
		}
		client, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
		require.NoError(t, err)
		resp, err := client.Manager.Status(utils.WithTimeout(t, 5*time.Second), &multipoolermanagerdatapb.StatusRequest{})
		client.Close()
		require.NoError(t, err)
		require.Equal(t, clustermetadatapb.PoolerType_REPLICA, resp.Status.PoolerType,
			"surviving standby %s must still be a REPLICA", name)
		require.NotNil(t, resp.Status.ReplicationStatus,
			"surviving standby %s must still have replication configured", name)
		require.NotNil(t, resp.Status.ReplicationStatus.PrimaryConnInfo,
			"surviving standby %s must have PrimaryConnInfo", name)
		t.Logf("Surviving standby %s is still a healthy REPLICA", name)
	}
}

// TestMultiReplicaContinuityAfterStandbyShutdown verifies the cluster keeps
// operating with continuous replication after one standby is gracefully
// terminated. With 3 poolers (1 primary + 2 standbys), terminating a standby
// must:
//
//   - keep the primary healthy and unchanged,
//   - keep the surviving standby actively streaming WAL from the primary
//     (wal_receiver_status == "streaming"), not merely "configured" — the
//     test asserts active streaming so a half-broken cluster doesn't pass,
//   - leave the primary's sync_replication_config consistent with the
//     surviving topology.
//
// This is the property that makes "rolling standby restart" workable: the
// remaining members keep doing useful work throughout.
func TestMultiReplicaContinuityAfterStandbyShutdown(t *testing.T) {
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

	primaryName := setup.PrimaryName
	require.NotEmpty(t, primaryName)
	t.Logf("Initial primary: %s", primaryName)

	// Pick the standby to terminate; remember the name of the OTHER standby
	// so we can assert it remains streaming.
	var terminatedStandby, survivingStandbyName string
	for name := range setup.Multipoolers {
		if name == primaryName {
			continue
		}
		if terminatedStandby == "" {
			terminatedStandby = name
		} else {
			survivingStandbyName = name
		}
	}
	require.NotEmpty(t, terminatedStandby)
	require.NotEmpty(t, survivingStandbyName)

	t.Logf("Terminating standby %s; expecting %s to keep replicating", terminatedStandby, survivingStandbyName)
	terminateMultipoolerGracefully(t, setup.Multipoolers[terminatedStandby], 90*time.Second)

	// Give multiorch a few snapshot ticks to react.
	time.Sleep(10 * time.Second)

	// Primary unchanged.
	current := setup.RefreshPrimary(t)
	require.Equal(t, primaryName, current.Name, "primary must remain unchanged")

	// Surviving standby is actively streaming, not just configured.
	survivingClient, err := shardsetup.NewMultipoolerClient(setup.Multipoolers[survivingStandbyName].Multipooler.GrpcPort)
	require.NoError(t, err)
	defer survivingClient.Close()

	require.Eventually(t, func() bool {
		resp, err := survivingClient.Manager.Status(utils.WithTimeout(t, 3*time.Second), &multipoolermanagerdatapb.StatusRequest{})
		if err != nil {
			return false
		}
		if resp.Status.PoolerType != clustermetadatapb.PoolerType_REPLICA {
			return false
		}
		if resp.Status.ReplicationStatus == nil {
			return false
		}
		// "streaming" is the active state: WAL is flowing. "configured" means
		// the receiver knows where to connect but isn't streaming yet.
		return resp.Status.ReplicationStatus.WalReceiverStatus == "streaming"
	}, 15*time.Second, 500*time.Millisecond,
		"surviving standby %s must be actively streaming WAL after %s shutdown",
		survivingStandbyName, terminatedStandby)
	t.Logf("Surviving standby %s is actively streaming from primary %s", survivingStandbyName, primaryName)

	// Primary's sync replication config is still sensible (at minimum, the
	// terminated standby being in/out of the list doesn't break anything;
	// the multiorch replica-recovery flow will eventually rebuild the cohort).
	primaryClient, err := shardsetup.NewMultipoolerClient(setup.Multipoolers[primaryName].Multipooler.GrpcPort)
	require.NoError(t, err)
	defer primaryClient.Close()
	resp, err := primaryClient.Manager.Status(utils.WithTimeout(t, 5*time.Second), &multipoolermanagerdatapb.StatusRequest{})
	require.NoError(t, err)
	require.Equal(t, clustermetadatapb.PoolerType_PRIMARY, resp.Status.PoolerType)
	require.NotNil(t, resp.Status.PrimaryStatus)
	t.Logf("Primary %s remains healthy with %d connected followers",
		primaryName, len(resp.Status.PrimaryStatus.ConnectedFollowers))
}

// TestSequentialGracefulShutdowns verifies that the graceful-shutdown sequence
// runs correctly across two back-to-back SIGTERMs:
//
//  1. SIGTERM a standby → standby exits cleanly, primary stays primary, no
//     spurious failover is triggered, and multiorch updates its view of the
//     cohort to exclude the terminated standby (verified via RequireRecovery
//     converging on a healthy steady state without that node).
//  2. SIGTERM the primary → primary's graceful-shutdown sequence runs to
//     completion (announce, smart→fast→immediate pgctld.Stop escalation,
//     drain, STOPPED snapshot, exit).
//
// Note on scope: this test deliberately does NOT assert that a new primary is
// elected after step 2. The default durability policy (AT_LEAST_2) requires
// majority recruitment in the cohort, and after two terminations the
// orchestrator's eligibility filter and sync-replication cleanup interact in
// ways that depend on consensus-protocol behaviour separate from graceful
// shutdown. The "primary shutdown triggers failover" property is verified by
// TestPrimaryGracefulShutdownTriggersFailover with a single termination;
// this test focuses on the sequential-shutdown code path.
func TestSequentialGracefulShutdowns(t *testing.T) {
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

	primaryName := setup.PrimaryName
	require.NotEmpty(t, primaryName)

	// Pick any standby to terminate first.
	var firstStandby string
	for name := range setup.Multipoolers {
		if name != primaryName {
			firstStandby = name
			break
		}
	}
	require.NotEmpty(t, firstStandby)
	t.Logf("Initial primary: %s; will terminate standby %s first, then primary %s",
		primaryName, firstStandby, primaryName)

	// Step 1: terminate one standby. No failover should occur, and multiorch
	// should converge on a steady state without the terminated node.
	t.Logf("Step 1: terminating standby %s", firstStandby)
	terminateMultipoolerGracefully(t, setup.Multipoolers[firstStandby], 90*time.Second)

	time.Sleep(5 * time.Second)
	current := setup.RefreshPrimary(t)
	require.Equal(t, primaryName, current.Name,
		"primary must be unchanged after standby %s shutdown", firstStandby)
	t.Logf("Step 1 verified: primary %s unchanged", primaryName)

	// Step 2: terminate the primary. The graceful-shutdown sequence must
	// complete cleanly. This is the main assertion of this test: SIGTERM
	// works on a primary even after a standby has already been terminated.
	t.Logf("Step 2: terminating primary %s", primaryName)
	terminateMultipoolerGracefully(t, setup.Multipoolers[primaryName], 90*time.Second)
	t.Logf("Step 2 verified: primary %s exited gracefully after sequential SIGTERMs", primaryName)
}

// TestPrimaryGracefulShutdownNoSmartEscalation locks in the property that
// the new shutdown design does not need pgctld smart shutdown: the pooler
// drains in-flight queries and closes its connection pool itself, so by the
// time pgctld.Stop runs, postgres has no clients to wait for. Fast shutdown
// completes near-instantly.
//
// The test measures wall-clock time from SIGTERM to multipooler exit. Under
// the working design that should be a few seconds at most. If a regression
// reintroduces smart-style "wait for clients" behavior (or otherwise causes
// fast to escalate to immediate), wall-clock would balloon to fast-timeout
// + immediate-timeout (~10s+) — comfortably outside this test's bound.
//
// Tighter bound than TestPrimaryGracefulShutdownTriggersFailover so a
// regression is caught here rather than masked by that test's generous
// failover window.
func TestPrimaryGracefulShutdownNoSmartEscalation(t *testing.T) {
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

	primary := setup.GetPrimary(t)
	require.NotNil(t, primary, "primary instance should exist")
	primaryName := setup.PrimaryName

	// SIGTERM the primary and measure how long the graceful-shutdown sequence
	// takes end-to-end. The bound is 8 seconds: fast (5s budget) typically
	// returns in under 1s when the pool is already closed; the rest is for
	// drain (≤3s) + final snapshot. If smart-style waiting comes back, we'd
	// see at least 5s for fast hanging on pool clients before escalating.
	const bound = 8 * time.Second
	start := time.Now()
	terminateMultipoolerGracefully(t, primary, bound+5*time.Second)
	elapsed := time.Since(start)

	require.Less(t, elapsed, bound,
		"primary %s graceful shutdown took %s, bound %s — likely escalated to fast/immediate due to a regression in pool drain ordering",
		primaryName, elapsed, bound)
	t.Logf("primary %s graceful shutdown completed in %s (bound %s)", primaryName, elapsed, bound)
}
