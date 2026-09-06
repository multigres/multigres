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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestInterruptedRewindQuarantinesStandby exercises the interrupted-pg_rewind
// safety net end to end with real postgres: a standby left in the state an
// interrupted rewind produces must be driven to the terminal QUARANTINED verdict
// (so the operator replaces it) rather than started on the half-rewound directory
// and left to spin forever waiting for WAL it can never fetch.
//
// The fault reproduces the interrupted-rewind end-state directly, rather than by
// racing a real rewind:
//   - a rewind sentinel on disk (the durable marker restartAsStandbyLocked writes
//     before the mutating pg_rewind and removes only after a verified-healthy
//     standby returns), and
//   - a truncated global/pg_control (an interrupted pg_rewind can truncate the
//     control file; per PostgreSQL guidance such a directory is unrecoverable).
//
// This is what the monitor finds on the next tick — or, after a pod was SIGKILLed
// mid-rewind, on the replacement pod's first tick. The sentinel forces the
// rewind-repair path (re-arming the in-memory-only suspectedDivergence flag lost
// across the restart) instead of a blind start; because the directory is
// unrecoverable, repair keeps failing and the unrecoverable classifier quarantines
// the node.
//
// Distinct from TestStandbyQuarantinesOnUnrecoverablePostgres, which reaches
// quarantine via plain start-failure and does not exercise the rewind-sentinel
// path. (The orchestrator follow-through — dropping a quarantined member from the
// sync cohort — is durability-policy dependent: shrinking a minimal cohort by a
// dead member is not always safe and multiorch may instead surface ShardAtRisk
// for the operator, so it is intentionally not asserted here.)
//
// Uses NewIsolated because the fault is destructive to a data directory and must
// not poison the shared fixture.
func TestInterruptedRewindQuarantinesStandby(t *testing.T) {
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
		// Quarantine quickly once repair keeps failing: a 15s continuous-failure
		// budget (plus the built-in min-attempts floor) versus the production
		// default of OFF.
		shardsetup.WithMultipoolerExtraArgs("--postgres-unrecoverable-timeout=15s"),
	)
	defer cleanup()

	setup.StartMultiorchs(t.Context(), t)
	setup.RequireRecovery(t, "multiorch", shardsetup.RecoveryScenarioInitialSettle)
	setup.WaitForHealthStreamsEstablished(t, "multiorch", 30*time.Second)

	// Pick a standby to model the interrupted rewind on — never the primary.
	setup.RefreshPrimary(t)
	standbys := setup.GetStandbys()
	require.NotEmpty(t, standbys, "expected at least one standby")
	victim := standbys[0]
	victimName := victim.Name
	t.Logf("Simulating interrupted rewind on standby %s (primary=%s)", victimName, setup.PrimaryName)

	// Reproduce the interrupted-rewind end-state:
	//  1. Stop postgres. StopPostgres disables the monitor's auto-restart first, so
	//     we get a clean window to plant the fault.
	//  2. Write the rewind sentinel — the durable marker a real interrupted rewind
	//     leaves behind, which forces the monitor onto the repair path instead of a
	//     blind start.
	//  3. Truncate global/pg_control — the corruption an interrupted pg_rewind can
	//     leave, so repair cannot succeed and the node must be quarantined.
	//     PG_VERSION is left intact so pgctld still reports the directory
	//     initialized.
	//  4. Re-enable restarts. Every monitor tick now re-arms divergence, attempts a
	//     held start / repair, fails, and advances the unrecoverable streak.
	resume := setup.StopPostgres(t, victimName, "immediate")
	sentinelPath := filepath.Join(victim.Pgctld.PoolerDir, constants.RewindSentinelFile)
	require.NoError(t, os.WriteFile(sentinelPath, []byte("pg_rewind in progress\n"), 0o644),
		"failed to plant rewind sentinel")
	pgControl := filepath.Join(victim.Pgctld.PoolerDir, "pg_data", "global", "pg_control")
	require.FileExists(t, pgControl, "pg_control should exist before corruption")
	require.NoError(t, os.Truncate(pgControl, 0), "failed to truncate pg_control")
	resume()

	// The pooler reaches the terminal QUARANTINED verdict rather than spinning on
	// the half-rewound directory — the operator-facing signal to replace the node.
	victimID := setup.GetMultipoolerID(victimName)
	require.Eventually(t, func() bool {
		mp, err := setup.TopoServer.GetMultipooler(t.Context(), victimID)
		if err != nil {
			return false
		}
		return mp.GetLifecycleStatus().GetStatus() == clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_QUARANTINED
	}, utils.ScaleTimeout(60*time.Second), 500*time.Millisecond,
		"standby %s should quarantine itself after an unrecoverable interrupted rewind", victimName)
}
