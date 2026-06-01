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
	"path/filepath"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// TestPromotionElectsExactlyOneLeader verifies that after a primary failure
// exactly one leader election fires — no cascade of re-elections.
//
// The bug this guards against: if the multipooler prematurely clears
// POSTGRES_STATUS_PROMOTING before postgres is ready (e.g. due to a timeout
// in waitForPromotionComplete), multiorch sees a dead leader and triggers
// another election, creating a cascading loop.
//
// The fix: waitForPromotionComplete now uses the caller's context timeout
// rather than a hardcoded 30 s inner deadline, so PROMOTING stays set until
// pg_isready actually succeeds. The rule_history row count is the observable
// invariant: one failover == one promotion record.
func TestPromotionElectsExactlyOneLeader(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestPromotionElectsExactlyOneLeader in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: no real postgres binaries available")
	}

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiOrchCount(1),
		shardsetup.WithMultigateway(),
		shardsetup.WithLeaderFailoverGracePeriod("2s", "0s"),
	)
	defer cleanup()

	setup.StartMultiOrchs(t.Context(), t)
	setup.WaitForMultigatewayQueryServing(t)

	primary := setup.GetPrimary(t)
	require.NotNil(t, primary)
	oldPrimaryName := setup.PrimaryName

	// Record the promotion count before the failover so we can assert the delta.
	socketDir := filepath.Join(primary.Pgctld.PoolerDir, "pg_sockets")
	db := connectToPostgres(t, socketDir, primary.Pgctld.PgPort)
	defer db.Close()

	var promotionsBefore int
	err := db.QueryRow(
		`SELECT COUNT(*) FROM multigres.rule_history WHERE event_type = 'promotion'`,
	).Scan(&promotionsBefore)
	require.NoError(t, err, "should be able to query rule_history before failover")

	// Disable postgres auto-restarts before killing postgres. Without this, pgctld
	// would restart postgres within seconds of the SIGKILL — before the new election
	// completes and emergencyDemoteLocked sets rewindPending. A premature restart
	// would bring the old primary back up at term=1, creating split-brain and
	// preventing LeaderIsDead from ever firing.
	primaryManagerClient, err := shardsetup.NewMultipoolerClient(primary.Multipooler.GrpcPort)
	require.NoError(t, err)
	_, err = primaryManagerClient.Manager.SetPostgresRestartsEnabled(
		utils.WithShortDeadline(t),
		&multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: false},
	)
	require.NoError(t, err)
	primaryManagerClient.Close()

	// Kill postgres on the primary to trigger a failover.
	setup.KillPostgres(t, oldPrimaryName)

	// Wait for a new primary to be elected.
	newPrimaryName := shardsetup.WaitForNewPrimary(t, setup, oldPrimaryName, 30*time.Second)
	require.NotEmpty(t, newPrimaryName, "multiorch should elect a new primary after primary death")

	// Re-enable postgres restarts now that the new election is complete.
	// emergencyDemoteLocked has set rewindPending on the old primary, so when
	// postgres restarts it will pg_rewind against the new primary rather than
	// coming back up as a competing primary. This is also required for stale-leader
	// demotion: SetTermPrimary needs postgres running to write the rule.
	primaryManagerClient2, err := shardsetup.NewMultipoolerClient(primary.Multipooler.GrpcPort)
	require.NoError(t, err)
	_, err = primaryManagerClient2.Manager.SetPostgresRestartsEnabled(
		utils.WithShortDeadline(t),
		&multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: true},
	)
	require.NoError(t, err)
	primaryManagerClient2.Close()

	// Let multiorch finish all follow-up work (stale-primary demotion, etc.)
	setup.RequireRecovery(t, "multiorch", 45*time.Second)

	// Count promotions on the new primary.
	newPrimary := setup.GetMultipoolerInstance(newPrimaryName)
	require.NotNil(t, newPrimary)
	newSocketDir := filepath.Join(newPrimary.Pgctld.PoolerDir, "pg_sockets")
	newDB := connectToPostgres(t, newSocketDir, newPrimary.Pgctld.PgPort)
	defer newDB.Close()

	var promotionsAfter int
	err = newDB.QueryRow(
		`SELECT COUNT(*) FROM multigres.rule_history WHERE event_type = 'promotion'`,
	).Scan(&promotionsAfter)
	require.NoError(t, err, "should be able to query rule_history after failover")

	// Exactly one new promotion should have been recorded. A cascade of
	// re-elections would produce two or more rows.
	assert.Equal(t, promotionsBefore+1, promotionsAfter,
		"exactly one promotion should fire per failover (cascade would produce more)")
}

// TestPromotingStatusClearedOnlyWhenReady verifies that a newly promoted node
// reports POSTGRES_STATUS_PROMOTING until its postgres process is accepting
// connections, and transitions to a non-PROMOTING status only after that.
//
// This is the direct observable invariant of the fix: if PROMOTING clears
// before PostgresReady=true, LeaderIsDeadAnalyzer would fire a spurious
// re-election.
func TestPromotingStatusClearedOnlyWhenReady(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping TestPromotingStatusClearedOnlyWhenReady in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: no real postgres binaries available")
	}

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiOrchCount(1),
		shardsetup.WithMultigateway(),
		shardsetup.WithLeaderFailoverGracePeriod("2s", "0s"),
	)
	defer cleanup()

	setup.StartMultiOrchs(t.Context(), t)
	setup.WaitForMultigatewayQueryServing(t)

	oldPrimaryName := setup.PrimaryName
	primary := setup.GetPrimary(t)
	require.NotNil(t, primary)

	// Disable auto-restarts before the kill so pgctld doesn't bounce postgres
	// before LeaderIsDead fires and the new election completes (see comment in
	// TestPromotionElectsExactlyOneLeader for the full split-brain explanation).
	primaryManagerClient, err := shardsetup.NewMultipoolerClient(primary.Multipooler.GrpcPort)
	require.NoError(t, err)
	_, err = primaryManagerClient.Manager.SetPostgresRestartsEnabled(
		utils.WithShortDeadline(t),
		&multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: false},
	)
	require.NoError(t, err)
	primaryManagerClient.Close()

	setup.KillPostgres(t, oldPrimaryName)

	newPrimaryName := shardsetup.WaitForNewPrimary(t, setup, oldPrimaryName, 30*time.Second)
	require.NotEmpty(t, newPrimaryName)

	// Poll the new primary's status. The invariant: whenever PROMOTING clears,
	// PostgresReady must already be true. We sample repeatedly to catch any
	// window where PROMOTING=false but Ready=false.
	newPrimary := setup.GetMultipoolerInstance(newPrimaryName)
	require.NotNil(t, newPrimary)

	newClient, err := shardsetup.NewMultipoolerClient(newPrimary.Multipooler.GrpcPort)
	require.NoError(t, err)
	defer newClient.Close()

	require.Eventually(t, func() bool {
		resp, err := newClient.Manager.Status(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StatusRequest{})
		if err != nil {
			return false
		}
		promoting := resp.Status.PostgresStatus == multipoolermanagerdatapb.PostgresStatus_POSTGRES_STATUS_PROMOTING
		ready := resp.Status.PostgresReady
		// Once PROMOTING clears, postgres must be ready.
		if !promoting {
			assert.True(t, ready,
				"PROMOTING status cleared before PostgresReady=true — LeaderIsDeadAnalyzer would fire a spurious re-election")
			return true
		}
		return false
	}, 30*time.Second, 200*time.Millisecond,
		"new primary should leave PROMOTING state within 30s")
}
