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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestInterruptedRewindTaintVerdict exercises the on-disk evidence the pooler uses
// to tell an interrupted pg_rewind that modified the data directory from one that
// left it untouched, end to end with real postgres.
//
// Both standbys are left in the state a pooler restart mid-rewind produces: the
// rewind sentinel, and the attempt marker written just before the mutating
// pg_rewind call. They differ only in whether anything in the data directory was
// modified after the attempt marker:
//   - the tainted standby had a file rewritten, as pg_rewind does once it starts
//     copying. The pooler must record the durable verdict and quarantine it
//     straight away, without starting postgres on the directory or retrying the
//     rewind, so no unrecoverable-postgres timeout is configured here.
//   - the clean standby is untouched. The attempt marker must be consumed without
//     a verdict, leaving the node on the normal repair path.
//
// Uses NewIsolated because the fault is destructive to a data directory and must
// not poison the shared fixture.
func TestInterruptedRewindTaintVerdict(t *testing.T) {
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
	)
	defer cleanup()

	setup.StartMultiorchs(t.Context(), t)
	setup.RequireRecovery(t, "multiorch", shardsetup.RecoveryScenarioInitialSettle)
	setup.WaitForHealthStreamsEstablished(t, "multiorch", 30*time.Second)

	setup.RefreshPrimary(t)
	standbys := setup.GetStandbys()
	require.GreaterOrEqual(t, len(standbys), 2, "expected two standbys")
	tainted, clean := standbys[0], standbys[1]
	t.Logf("tainted=%s clean=%s primary=%s", tainted.Name, clean.Name, setup.PrimaryName)

	// Leave a standby in the state an interrupted rewind produces. Postgres is
	// stopped first (StopPostgres disables the monitor's restart, giving a clean
	// window) and stays down until restarts are re-enabled, so the pooler settles
	// the attempt before anything starts postgres on the directory, as it does after
	// a real pod restart.
	plant := func(name string, poolerDir string, modify bool) (resume func()) {
		resume = setup.StopPostgres(t, name, "immediate")
		require.NoError(t, os.WriteFile(filepath.Join(poolerDir, constants.RewindSentinelFile), []byte("pg_rewind in progress\n"), 0o644))
		require.NoError(t, os.WriteFile(filepath.Join(poolerDir, constants.RewindAttemptFile), []byte("pg_rewind run launched\n"), 0o644))
		if modify {
			// Rewrite a file after the attempt marker, as pg_rewind does when it
			// overwrites a relation file or the control file.
			pgControl := filepath.Join(poolerDir, "pg_data", "global", "pg_control")
			content, err := os.ReadFile(pgControl)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(pgControl, content, 0o600))
		}
		return resume
	}
	resumeTainted := plant(tainted.Name, tainted.Pgctld.PoolerDir, true)
	resumeClean := plant(clean.Name, clean.Pgctld.PoolerDir, false)
	resumeTainted()
	resumeClean()

	lifecycle := func(name string) clustermetadatapb.PoolerLifecycleStatus {
		mp, err := setup.TopoServer.GetMultipooler(t.Context(), setup.GetMultipoolerID(name))
		if err != nil {
			return clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_UNKNOWN
		}
		return mp.GetLifecycleStatus().GetStatus()
	}
	exists := func(path string) bool {
		_, err := os.Stat(path)
		return err == nil
	}

	// The tainted standby is quarantined and its verdict is durable.
	require.Eventually(t, func() bool {
		return lifecycle(tainted.Name) == clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_QUARANTINED
	}, utils.ScaleTimeout(60*time.Second), 500*time.Millisecond,
		"standby %s modified by an interrupted rewind should be quarantined", tainted.Name)
	assert.FileExists(t, filepath.Join(tainted.Pgctld.PoolerDir, "pg_data", constants.RewindTaintedFile),
		"the verdict lives in the data directory it describes")
	assert.False(t, exists(filepath.Join(tainted.Pgctld.PoolerDir, constants.RewindAttemptFile)),
		"the attempt marker is consumed by the verdict")

	// The clean standby's attempt marker is consumed without a verdict and the
	// node is not quarantined.
	require.Eventually(t, func() bool {
		return !exists(filepath.Join(clean.Pgctld.PoolerDir, constants.RewindAttemptFile))
	}, utils.ScaleTimeout(60*time.Second), 500*time.Millisecond,
		"the untouched standby %s should have its attempt marker consumed", clean.Name)
	assert.False(t, exists(filepath.Join(clean.Pgctld.PoolerDir, "pg_data", constants.RewindTaintedFile)),
		"an untouched data directory must not be tainted")
	assert.NotEqual(t, clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_QUARANTINED, lifecycle(clean.Name),
		"an untouched data directory stays on the repair path")
}

// TestFailedDryRunAfterCrashRecoveryIsRetryable checks end to end that a rewind
// failing before pg_rewind copies anything stays retryable. A standby stopped
// with an immediate shutdown is crash-recovered by pgctld, which writes to its
// data directory, and with the leader down the rewind dry-run then fails. That
// failure must not be read as a modification: no taint verdict, no quarantine,
// and the node rejoins once the leader is back.
//
// In this flow the monitor usually recovers the node by starting it before the
// rewind, so the crash recovery is not always inside the dry-run call itself; the
// unit tests cover the case where it is.
func TestFailedDryRunAfterCrashRecoveryIsRetryable(t *testing.T) {
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
	)
	defer cleanup()

	setup.StartMultiorchs(t.Context(), t)
	setup.RequireRecovery(t, "multiorch", shardsetup.RecoveryScenarioInitialSettle)
	setup.WaitForHealthStreamsEstablished(t, "multiorch", 30*time.Second)
	// Keep the orchestrator from failing over while the leader is down.
	setup.DisableRecovery(t, "multiorch")

	setup.RefreshPrimary(t)
	standbys := setup.GetStandbys()
	require.NotEmpty(t, standbys)
	victim := standbys[0]
	leaderName := setup.PrimaryName
	victimDir := victim.Pgctld.PoolerDir
	t.Logf("victim=%s leader=%s", victim.Name, leaderName)

	// Leave the standby unclean, with the rewind sentinel that routes the monitor
	// onto the rewind path, and take the leader down so the dry-run cannot connect.
	resumeVictim := setup.StopPostgres(t, victim.Name, "immediate")
	resumeLeader := setup.StopPostgres(t, leaderName, "fast")
	defer resumeLeader()
	require.NoError(t, os.WriteFile(filepath.Join(victimDir, constants.RewindSentinelFile), []byte("pg_rewind in progress\n"), 0o644))
	resumeVictim()

	logPath := victim.Multipooler.LogFile
	require.Eventually(t, func() bool {
		b, err := os.ReadFile(logPath)
		return err == nil && strings.Contains(string(b), "pg_rewind dry-run failed")
	}, utils.ScaleTimeout(90*time.Second), time.Second, "the dry-run should have failed with the leader down")

	_, err := os.Stat(filepath.Join(victimDir, "pg_data", constants.RewindTaintedFile))
	require.True(t, os.IsNotExist(err), "a failed dry-run must not taint the data directory")
	_, err = os.Stat(filepath.Join(victimDir, constants.RewindAttemptFile))
	require.True(t, os.IsNotExist(err), "no attempt marker is written before the mutating call")
	mp, err := setup.TopoServer.GetMultipooler(t.Context(), setup.GetMultipoolerID(victim.Name))
	require.NoError(t, err)
	require.NotEqual(t, clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_QUARANTINED, mp.GetLifecycleStatus().GetStatus())

	// With the leader back the repair completes and the sentinel is cleared.
	resumeLeader()
	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(victimDir, constants.RewindSentinelFile))
		return os.IsNotExist(err)
	}, utils.ScaleTimeout(120*time.Second), time.Second, "the standby should complete its rewind once the leader is back")
	_, err = os.Stat(filepath.Join(victimDir, "pg_data", constants.RewindTaintedFile))
	require.True(t, os.IsNotExist(err))
}
