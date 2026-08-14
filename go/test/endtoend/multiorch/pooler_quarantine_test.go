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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestStandbyQuarantinesOnUnrecoverablePostgres verifies the Layer-1
// unrecoverable-FATAL-loop detection end to end with real postgres: a standby
// whose postgres can no longer start (the classic genuinely-diverged case,
// simulated here by corrupting its control file) must quarantine itself rather
// than retrying forever, flipping its topology record to LIFECYCLE_QUARANTINED.
//
// Uses NewIsolated because the fault (corrupting a data directory) is
// destructive and must not poison the shared fixture.
func TestStandbyQuarantinesOnUnrecoverablePostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real-postgres e2e in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("real postgres binaries not available")
	}

	setup, cleanup := shardsetup.NewIsolated(
		t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiorchCount(1),
		// Quarantine quickly: a 15s continuous-failure budget (plus the built-in
		// min-attempts floor, reached in ~15s at the 5s monitor tick) versus the
		// production default of OFF.
		shardsetup.WithMultipoolerExtraArgs("--postgres-unrecoverable-timeout=15s"),
	)
	defer cleanup()

	setup.StartMultiorchs(t.Context(), t)
	setup.RequireRecovery(t, "multiorch", shardsetup.RecoveryScenarioInitialSettle)
	setup.WaitForHealthStreamsEstablished(t, "multiorch", 30*time.Second)

	// Pick a standby — never the primary. Corrupting the primary would trigger a
	// failover, which is a different scenario; we want to observe a standby
	// self-quarantining.
	setup.RefreshPrimary(t)
	standbys := setup.GetStandbys()
	require.NotEmpty(t, standbys, "expected at least one standby")
	standby := standbys[0]
	standbyName := standby.Name
	t.Logf("Inducing unrecoverable postgres on standby %s (primary=%s)", standbyName, setup.PrimaryName)

	// Make postgres persistently fail to start:
	//  1. Stop it. StopPostgres disables the monitor's auto-restart first, so we
	//     get a clean window to corrupt the data directory.
	//  2. Truncate global/pg_control — postgres FATALs on every subsequent start
	//     ("could not read control file"). We leave PG_VERSION intact so pgctld
	//     still reports the directory as initialized and the monitor keeps
	//     choosing StartPostgres (which fails) instead of restore-from-backup.
	//  3. Re-enable restarts. From here every 5s monitor tick attempts a start,
	//     fails, and increments the unrecoverable streak.
	resume := setup.StopPostgres(t, standbyName, "immediate")
	pgControl := filepath.Join(standby.Pgctld.PoolerDir, "pg_data", "global", "pg_control")
	require.FileExists(t, pgControl, "pg_control should exist before corruption")
	require.NoError(t, os.Truncate(pgControl, 0), "failed to truncate pg_control")
	resume()

	// The pooler quarantines itself in topology.
	standbyID := setup.GetMultipoolerID(standbyName)
	require.Eventually(t, func() bool {
		mp, err := setup.TopoServer.GetMultipooler(t.Context(), standbyID)
		if err != nil {
			return false
		}
		return mp.GetLifecycleStatus().GetStatus() == clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_QUARANTINED
	}, utils.ScaleTimeout(60*time.Second), 500*time.Millisecond,
		"standby %s should quarantine itself once postgres is unrecoverable", standbyName)
}
