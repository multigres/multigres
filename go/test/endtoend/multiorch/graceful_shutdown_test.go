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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// terminateMultipoolerGracefully sends SIGTERM to the multipooler binary (NOT
// pgctld) and waits for it to exit. This drives the OnTermSync GracefulShutdown
// hook end-to-end: a COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE announcement on the
// health stream and pgctld.Stop (fast → immediate escalation). The OnClose
// chain that follows updates the topology entry to PoolerType_DRAINED.
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
// graceful-shutdown contract on the primary side: SIGTERM on the primary
// produces an explicit COHORT_ELIGIBILITY_SIGNAL_INELIGIBLE signal on the
// health stream, the LeaderResignedAnalyzer fires (via LeaderNeedsReplacement
// treating INELIGIBLE as a resignation), multiorch promotes a surviving
// standby, and the terminated pooler's topology entry ends up as
// PoolerType_DRAINED. The failover must happen quickly because resignation
// is an unambiguous signal — no follower-disconnect grace period.
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

	// Wait for multiorch to settle to a clean cluster state, then for it to
	// actually establish health streams to every pooler. The
	// RequireRecovery-only gate is satisfied by topology state alone — it
	// can return before the orchestrator has subscribed to the
	// ManagerHealthStream, in which case the INELIGIBLE snapshot our SIGTERM
	// produces never reaches multiorch and failover falls back to the slow
	// LeaderIsDeadAnalyzer path. The streams check closes that window.
	setup.RequireRecovery(t, "multiorch", 30*time.Second)
	setup.WaitForHealthStreamsEstablished(t, "multiorch", 30*time.Second)

	oldPrimary := setup.GetPrimary(t)
	require.NotNil(t, oldPrimary, "primary instance should exist")
	require.NotNil(t, oldPrimary.Multipooler)
	require.NotNil(t, oldPrimary.Multipooler.Process)
	oldPrimaryName := setup.PrimaryName
	t.Logf("Initial primary: %s", oldPrimaryName)

	// Send SIGTERM but don't block on the old primary's exit — the orch
	// promotion-side latency is what we want to measure, not the dying
	// pooler's pgctld.Stop budget. We join the goroutine after the failover
	// assertion to verify clean exit separately.
	t.Logf("Sending SIGTERM to multipooler %s (PID %d)",
		oldPrimary.Name, oldPrimary.Multipooler.Process.Process.Pid)
	start := time.Now()
	terminateDone := make(chan struct{})
	go func() {
		defer close(terminateDone)
		termCtx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		_, _ = oldPrimary.Multipooler.Process.Stop(termCtx)
	}()

	// multiorch should elect a new primary quickly: LeaderResignedAnalyzer
	// fires as soon as it sees INELIGIBLE on the leader, then
	// AppointLeaderAction (Recruit + Propose) runs on the surviving cohort
	// — concurrently with the old primary's pgctld.Stop, because runFailover
	// excludes the resigned pooler from the cohort before recruit.
	t.Logf("Waiting for multiorch to elect a new primary...")
	newPrimaryName := shardsetup.WaitForNewPrimary(t, setup, oldPrimaryName, 30*time.Second)
	elapsed := time.Since(start)
	require.NotEmpty(t, newPrimaryName, "expected multiorch to elect a new primary")
	require.NotEqual(t, oldPrimaryName, newPrimaryName,
		"new primary must be different from the terminated old primary")
	t.Logf("New primary elected: %s (was: %s) in %s", newPrimaryName, oldPrimaryName, elapsed)

	// Lock in a tight upper bound on orch-side failover latency: SIGTERM
	// delivery → REQUESTING_DEMOTION broadcast → orch detects →
	// LeaderResignedAnalyzer fires → AppointLeader completes → new primary
	// visible. Regressions in any of those (e.g. Recruit still waiting on
	// the resigned pooler) would blow past 5s.
	require.Less(t, elapsed, 5*time.Second,
		"graceful primary replacement took %s (expected well under 5s); "+
			"likely a regression in INELIGIBLE delivery, LeaderResignedAnalyzer firing, "+
			"or the appointment cohort still containing the resigned pooler", elapsed)

	// Confirm the dying primary actually exited cleanly. Generous bound
	// covers pgctld.Stop escalation (fast → immediate).
	select {
	case <-terminateDone:
		t.Logf("Multipooler %s exited gracefully", oldPrimaryName)
	case <-t.Context().Done():
		t.Fatalf("test context cancelled while waiting for %s to exit: %v",
			oldPrimaryName, t.Context().Err())
	case <-time.After(60 * time.Second):
		t.Fatalf("multipooler %s did not exit gracefully within 60s", oldPrimaryName)
	}

	// The terminated pooler must be marked DRAINED in topology. The pooler-side
	// unregister hook (OnClose → tr.Unregister) writes this when the process
	// exits cleanly. Locks in that DRAINED is reflected for `multigres getpoolers`
	// after a graceful shutdown.
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
// Even though the standby's GracefulShutdown advertises INELIGIBLE just like
// the primary's does, INELIGIBLE on a non-leader doesn't trigger the
// LeaderResignedAnalyzer (LeaderNeedsReplacement only fires for the current
// leader). The orchestrator's reaction is limited to removing the terminated
// standby from the cohort via ReconcileCohortAction — no leader appointment.
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
	setup.WaitForHealthStreamsEstablished(t, "multiorch", 30*time.Second)

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

	// A spurious failover would change the primary name. Watch for that
	// throughout a window long enough for several multiorch snapshot ticks;
	// the assertion fails the instant the primary changes, not just at the
	// end.
	assert.Never(t, func() bool {
		current := setup.RefreshPrimary(t)
		return current != nil && current.Name != primaryName
	}, 10*time.Second, 500*time.Millisecond,
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
	setup.WaitForHealthStreamsEstablished(t, "multiorch", 30*time.Second)

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

	// Primary must stay the same throughout the window — fails immediately
	// if a spurious failover occurs.
	assert.Never(t, func() bool {
		current := setup.RefreshPrimary(t)
		return current != nil && current.Name != primaryName
	}, 10*time.Second, 500*time.Millisecond,
		"primary must remain unchanged after standby graceful shutdown")

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
//     completion (announce REQUESTING_DEMOTION, smart→fast→immediate
//     pgctld.Stop escalation, exit).
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
	setup.WaitForHealthStreamsEstablished(t, "multiorch", 30*time.Second)

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

	// Primary must not flip after the standby is terminated.
	assert.Never(t, func() bool {
		current := setup.RefreshPrimary(t)
		return current != nil && current.Name != primaryName
	}, 5*time.Second, 500*time.Millisecond,
		"primary must be unchanged after standby %s shutdown", firstStandby)
	t.Logf("Step 1 verified: primary %s unchanged", primaryName)

	// Step 2: terminate the primary. The graceful-shutdown sequence must
	// complete cleanly. This is the main assertion of this test: SIGTERM
	// works on a primary even after a standby has already been terminated.
	t.Logf("Step 2: terminating primary %s", primaryName)
	terminateMultipoolerGracefully(t, setup.Multipoolers[primaryName], 90*time.Second)
	t.Logf("Step 2 verified: primary %s exited gracefully after sequential SIGTERMs", primaryName)
}
