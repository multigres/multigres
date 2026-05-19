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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
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

	// Configure replication AND record the (rule, primary) tuple. With the
	// new flow this happens via SetTermPrimary; this test uses the legacy
	// SetPrimaryConnInfo since the shared setup doesn't yet run multiorch.
	// Both paths populate ReplicationPrimary, which is what the monitor reads.
	primaryPooler := &clustermetadatapb.MultiPooler{
		Id: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      setup.CellName,
			Name:      setup.PrimaryMultipooler.Name,
		},
		Hostname: "localhost",
		PortMap:  map[string]int32{"postgres": int32(setup.PrimaryPgctld.PgPort)},
	}
	_, err = standbyClient.Consensus.SetPrimaryConnInfo(t.Context(), &multipoolermanagerdatapb.SetPrimaryConnInfoRequest{
		Primary:               primaryPooler,
		StopReplicationBefore: false,
		StartReplicationAfter: true,
		Force:                 true,
	})
	require.NoError(t, err, "SetPrimaryConnInfo should succeed on standby")

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
