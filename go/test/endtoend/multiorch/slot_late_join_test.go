// Copyright 2026 Supabase, Inc.
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

package multiorch

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestSlotBasedReplicationLateStandbyJoins is the regression test for the
// bootstrap slot-creation deadlock (§2.6). It reproduces the exact case the
// deadlock hits — a standby that joins AFTER the primary was promoted — with
// slot-based replication enabled, and asserts the late standby still joins the
// cohort.
//
// With the flag on, a standby sets primary_slot_name = mg_<self> and its WAL
// receiver cannot stream until that physical slot exists on the primary. Before
// the fix, the primary only created a follower's slot at the promotion hook (for
// the committed cohort) or COHORT_ADD (which itself requires the standby to
// already be streaming). A late-joining standby therefore deadlocked: it could
// not stream, so it was never admitted to the cohort, so its slot was never
// created. The discovery-driven ReconcileFollowers reconcile creates the slot as
// soon as multiorch discovers the standby — ahead of streaming — so it can
// stream and be admitted. Without the fix, the final waitForCohortMembership
// times out.
func TestSlotBasedReplicationLateStandbyJoins(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end late-standby-join test (short mode)")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end late-standby-join test (no postgres binaries)")
	}

	// Start with one primary + one standby, slot-based replication on.
	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(2),
		shardsetup.WithMultiorchCount(1),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
		shardsetup.WithDurabilityPolicy("AT_LEAST_2"),
		shardsetup.WithMultipoolerExtraArgs("--enable-slot-based-replication=true"),
	)
	defer cleanup()

	setup.StartMultiorchs(t.Context(), t)
	setup.RequireRecovery(t, "multiorch", shardsetup.RecoveryScenarioInitialSettle)
	setup.WaitForHealthStreamsEstablished(t, "multiorch", 30*time.Second)
	require.NotEmpty(t, setup.PrimaryName, "initial primary must be elected")

	initial := make([]string, 0, 2)
	for name := range setup.Multipoolers {
		initial = append(initial, name)
	}
	require.Len(t, initial, 2, "expected one primary + one standby to start")
	waitForCohortMembership(t, setup, initial, 60*time.Second)
	t.Logf("Initial cohort established: primary=%s, members=%v", setup.PrimaryName, initial)

	// Add a standby AFTER the primary is established (the deadlock case). Late
	// poolers created via CreateMultipoolerInstance do not inherit the setup-wide
	// extra args, so set the flag explicitly.
	const lateName = "late-standby"
	inst := setup.CreateMultipoolerInstance(t, lateName, utils.GetFreePort(t), utils.GetFreePort(t), utils.GetFreePort(t))
	inst.Multipooler.ExtraArgs = append(inst.Multipooler.ExtraArgs, "--enable-slot-based-replication=true")

	ctx := t.Context()
	require.NoError(t, inst.Pgctld.Start(ctx, t), "failed to start pgctld for %s", lateName)
	require.NoError(t, inst.Multipooler.Start(ctx, t), "failed to start multipooler for %s", lateName)
	shardsetup.WaitForManagerReady(t, inst.Multipooler)
	t.Logf("Late standby %s started after promotion; awaiting cohort admission", lateName)

	// The regression assertion: the late standby must join the cohort. Before the
	// §2.6 fix this deadlocks and the wait times out.
	expected := append(append([]string{}, initial...), lateName)
	waitForCohortMembership(t, setup, expected, 90*time.Second)
	t.Logf("Late standby joined the cohort without a manual slot workaround: %v", expected)
}
