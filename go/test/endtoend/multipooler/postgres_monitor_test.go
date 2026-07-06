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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// TestPostgresMonitorControl tests the complete postgres monitoring control flow:
// 1. Auto-restart when monitoring is enabled (default)
// 2. No auto-restart when monitoring is disabled
// 3. Auto-restart resumes when monitoring is re-enabled
func TestPostgresMonitorControl(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)
	setupPoolerTest(t, setup)

	// Wait for primary manager to be ready
	waitForManagerReady(t, setup, setup.PrimaryMultipooler)

	// Create client to primary multipooler
	primaryClient := setup.NewPrimaryClient(t)
	defer primaryClient.Close()

	t.Run("1. verify postgres is initially running", func(t *testing.T) {
		ctx := utils.WithShortDeadline(t)
		status, err := primaryClient.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
		require.NoError(t, err, "Should get status from primary")
		require.True(t, status.Status.PostgresReady, "Postgres should be running initially")
		t.Logf("Postgres is running initially")
	})

	t.Run("2. kill postgres and verify auto-restart with monitoring enabled", func(t *testing.T) {
		// Kill postgres process
		t.Logf("Killing postgres on primary node %s", setup.PrimaryName)
		setup.KillPostgres(t, setup.PrimaryName)

		// Wait a moment for monitoring to detect the failure
		time.Sleep(2 * time.Second)

		// Verify postgres was automatically restarted by monitoring
		t.Logf("Waiting for postgres to be automatically restarted...")
		require.Eventually(t, func() bool {
			ctx := utils.WithShortDeadline(t)
			status, err := primaryClient.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
			if err != nil {
				t.Logf("Status check failed: %v", err)
				return false
			}
			return status.Status.PostgresReady
		}, 30*time.Second, 500*time.Millisecond, "Postgres should be automatically restarted by monitoring")

		t.Logf("Postgres was successfully auto-restarted")
	})

	t.Run("3. disable postgres restarts", func(t *testing.T) {
		ctx := utils.WithShortDeadline(t)
		_, err := primaryClient.Manager.SetPostgresRestartsEnabled(ctx, &multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: false})
		require.NoError(t, err, "Should disable postgres restarts successfully")
		t.Logf("Postgres restarts disabled on primary")
	})

	t.Run("4. kill postgres and verify it stays down", func(t *testing.T) {
		// Kill postgres process
		t.Logf("Killing postgres on primary node %s", setup.PrimaryName)
		setup.KillPostgres(t, setup.PrimaryName)

		// The monitor runs every 5s; verify postgres does not restart over ~3 cycles.
		require.Never(t, func() bool {
			ctx := utils.WithShortDeadline(t)
			status, err := primaryClient.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
			if err != nil {
				return false
			}
			return status.Status.PostgresReady
		}, 15*time.Second, 500*time.Millisecond, "Postgres should NOT be restarted when restarts are disabled")
		t.Logf("Confirmed: postgres stayed down while restarts disabled")
	})

	t.Run("5. re-enable restarts and verify postgres restarts", func(t *testing.T) {
		ctx := utils.WithShortDeadline(t)
		_, err := primaryClient.Manager.SetPostgresRestartsEnabled(ctx, &multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: true})
		require.NoError(t, err, "Should re-enable postgres restarts successfully")
		t.Logf("Postgres restarts re-enabled on primary")

		// Wait for postgres to be restarted by monitoring
		t.Logf("Waiting for postgres to be automatically restarted...")
		require.Eventually(t, func() bool {
			ctx := utils.WithShortDeadline(t)
			status, err := primaryClient.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
			if err != nil {
				t.Logf("Status check failed: %v", err)
				return false
			}
			return status.Status.PostgresReady
		}, 30*time.Second, 500*time.Millisecond, "Postgres should restart after re-enabling restarts")

		t.Logf("Postgres was successfully restarted after re-enabling restarts")
	})
}

// TestGUCSelfHealing verifies that the pooler's postgres monitor detects and repairs
// externally-corrupted synchronous_standby_names without any orchestrator intervention.
// This test exercises the remedialActionReconcileGUC path in the monitor loop.
func TestGUCSelfHealing(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)
	setupPoolerTest(t, setup)
	waitForManagerReady(t, setup, setup.PrimaryMultipooler)

	pgClient, err := shardsetup.NewMultipoolerTestClient(fmt.Sprintf("localhost:%d", setup.PrimaryMultipooler.GrpcPort))
	require.NoError(t, err)
	defer pgClient.Close()

	ctx := utils.WithTimeout(t, 10*time.Second)

	correctValue, err := shardsetup.QueryStringValue(ctx, pgClient, "SHOW synchronous_standby_names")
	require.NoError(t, err)
	if correctValue == "" {
		t.Skip("synchronous_standby_names not configured")
	}
	t.Logf("Correct synchronous_standby_names: %q", correctValue)

	// Corrupt the GUC. We use 'ANY 1 (*)' rather than a completely unknown name
	// so that sync commits can still complete (the wildcard matches the real standby),
	// allowing reconcileGUC's SELECT FOR UPDATE to commit without blocking.
	_, err = pgClient.ExecuteQuery(ctx, "ALTER SYSTEM SET synchronous_standby_names = 'ANY 1 (*)'", 1)
	require.NoError(t, err)
	shardsetup.ReloadConfig(ctx, t, pgClient, setup.PrimaryName)

	require.Eventually(t, func() bool {
		val, qerr := shardsetup.QueryStringValue(utils.WithShortDeadline(t), pgClient, "SHOW synchronous_standby_names")
		return qerr == nil && val != correctValue
	}, 5*time.Second, 50*time.Millisecond, "corrupted GUC should be visible after reload")

	// The monitor runs every 5s, so allow up to one full tick plus
	// action-lock acquisition and remediation latency for drift to be healed.
	require.Eventually(t, func() bool {
		val, qerr := shardsetup.QueryStringValue(utils.WithShortDeadline(t), pgClient, "SHOW synchronous_standby_names")
		return qerr == nil && val == correctValue
	}, 10*time.Second, 100*time.Millisecond, "postgres monitor should heal synchronous_standby_names drift")

	t.Logf("GUC self-healing confirmed: synchronous_standby_names restored to %q", correctValue)
}

// TestPostgresMonitor_FixesPrimaryConnInfoDrift exercises the MonitorPostgres
// self-heal of primary_conninfo end-to-end. With orch out of the picture
// (the shared multipooler setup runs no multiorch), this proves the
// pooler-local loop converges on its own:
//
//  1. Establish replication on the standby (which populates the recorded
//     ReplicationPrimary).
//  2. Externally clobber primary_conninfo on the standby's postgres to a
//     wrong host/port so it no longer matches what the pooler has on file.
//  3. Wait for the monitor (5s tick) to detect drift via
//     primaryConnInfoDiffersFromRecorded and rewrite primary_conninfo back
//     to the recorded primary.
//
// The asymmetry that matters: an orch-driven fix (FixReplicationAction)
// requires the replica to look unhealthy from orch's perspective; the
// pooler-local fix triggers on configuration drift regardless of whether
// replication is currently failing. This test isolates the pooler-local
// path.
func TestPostgresMonitor_FixesPrimaryConnInfoDrift(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	setup := getSharedTestSetup(t)
	// Use WithoutReplication so this test owns the SetPrimaryConnInfo call
	// below — that's how we both establish replication AND populate the
	// recorded ReplicationPrimary the monitor reads from.
	setupPoolerTest(t, setup, WithoutReplication())

	waitForManagerReady(t, setup, setup.StandbyMultipooler)

	standbyClient, err := shardsetup.NewMultipoolerClient(setup.StandbyMultipooler.GrpcPort)
	require.NoError(t, err)
	t.Cleanup(func() { standbyClient.Close() })

	// Configure replication AND record the (rule, primary) tuple. SetPrimary
	// populates ReplicationPrimary, which is what the monitor reads.
	primaryID := &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      setup.CellName,
		Name:      setup.PrimaryMultipooler.Name,
	}
	// Use a high coordinator term so the supplied rule is strictly higher than
	// whatever the standby has observed, forcing SetPrimary's standby
	// branch to apply.
	_, err = standbyClient.Consensus.SetPrimary(t.Context(), &consensusdatapb.SetPrimaryRequest{
		ReplicationPrimary: &clustermetadatapb.ReplicationPrimary{
			Rule: &clustermetadatapb.ShardRule{
				RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: 1 << 30},
				LeaderId:   primaryID,
			},
			Primary: &clustermetadatapb.PoolerAddress{
				Id:           primaryID,
				Host:         "localhost",
				PostgresPort: int32(setup.PrimaryPgctld.PgPort),
			},
		},
	})
	require.NoError(t, err, "SetPrimary should succeed on standby")

	// Snapshot the well-formed conninfo for the post-heal comparison. Read it
	// directly from postgres rather than from the pooler's record so the
	// assertion below verifies the monitor actually rewrote postgres state.
	standbyDB := connectToPostgresViaSocket(t,
		getPostgresSocketPath(setup.StandbyPgctld.PoolerDir),
		setup.StandbyPgctld.PgPort)
	t.Cleanup(func() { standbyDB.Close() })

	readConnInfo := func() string {
		var connInfo string
		err := standbyDB.QueryRow(`SELECT current_setting('primary_conninfo', true)`).Scan(&connInfo)
		require.NoError(t, err, "should read primary_conninfo")
		return connInfo
	}
	originalConnInfo := readConnInfo()
	require.NotEmpty(t, originalConnInfo, "primary_conninfo must be set after SetPrimaryConnInfo")
	require.Contains(t, originalConnInfo, "host=localhost",
		"sanity check: original conninfo should point at the real primary host")

	// Clobber primary_conninfo with a different, unreachable host. The string
	// is intentionally parsable so primaryConnInfoDiffersFromRecorded takes
	// its host-mismatch branch (not the unparsable fallback). ALTER SYSTEM
	// does not accept query parameters, so the value is inlined; pg_reload_conf
	// makes the new value visible to subsequent reads.
	//
	// We don't re-read primary_conninfo to confirm the clobber landed: doing so
	// would race the monitor (which can heal within its 5s tick) and the
	// successful return of ALTER SYSTEM + pg_reload_conf is sufficient evidence
	// the GUC was rewritten. The brief sleep below absorbs any reload latency.
	_, err = standbyDB.Exec(`ALTER SYSTEM SET primary_conninfo = 'host=bogus.invalid port=9999 user=replicator'`)
	require.NoError(t, err, "ALTER SYSTEM SET primary_conninfo should succeed")
	_, err = standbyDB.Exec(`SELECT pg_reload_conf()`)
	require.NoError(t, err, "pg_reload_conf should succeed")
	time.Sleep(10 * time.Millisecond)

	// Monitor ticks every 5s; allow up to ~30s for the heal to converge over
	// 4-5 cycles even with scheduling jitter.
	require.Eventually(t, func() bool {
		current := readConnInfo()
		// The fully reconstructed conninfo carries other fields too
		// (application_name, sslmode, etc.); the load-bearing assertion is
		// that the clobber is gone and the recorded primary host is back.
		return !strings.Contains(current, "bogus.invalid") &&
			strings.Contains(current, "host=localhost")
	}, 30*time.Second, 500*time.Millisecond, "monitor should self-heal primary_conninfo back to recorded primary")

	t.Logf("MonitorPostgres restored primary_conninfo from the bogus override back to the recorded primary")
}
