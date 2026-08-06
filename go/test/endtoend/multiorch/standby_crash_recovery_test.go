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
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/testtiming"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// TestStandbyRecoversAfterPostmasterKill verifies that a crashed standby is
// brought back automatically instead of getting wedged in a restart loop.
//
// A healthy, caught-up standby whose postmaster is SIGKILLed must be brought
// back automatically by the pooler's postgres monitor. Before the fix it was
// not: pgctld's crash-recovery path ran single-user (`postgres --single`)
// recovery unconditionally for any not-cleanly-stopped standby. Single-user
// recovery runs in primary mode and does not follow timeline-history switches,
// so it finalized the standby on its old timeline past the leader's fork point;
// the ensuing standby start then FATAL-looped "requested timeline N is not a
// child of this server's history", and the monitor retried it forever while the
// pod kept reporting Ready.
//
// The test establishes that the standby is genuinely non-diverged before the
// kill (its replay has caught up to a fixed post-fork primary position on the
// current timeline, and its walreceiver is streaming), so the only correct
// outcome is that ordinary standby-mode crash recovery brings it back.
//
// The primary analog — SIGKILL the primary's postmaster and assert the monitor
// auto-restarts it — is TestPostgresMonitorControl in
// go/test/endtoend/multipooler/postgres_monitor_test.go.
func TestStandbyRecoversAfterPostmasterKill(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping end-to-end test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end test (short mode or no postgres binaries)")
	}

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(2), // primary + 1 standby
		shardsetup.WithMultiorchCount(1),
		shardsetup.WithDatabase("postgres"),
		shardsetup.WithCellName("test-cell"),
	)
	defer cleanup()

	// A live multiorch watches the shard, as in production.
	setup.StartMultiorchs(t.Context(), t)
	setup.WaitForHealthStreamsEstablished(t, "multiorch", 60*time.Second)

	primary := setup.GetPrimary(t)
	standby := setup.GetStandbys()[0]
	t.Logf("primary=%s standby=%s", primary.Name, standby.Name)

	primaryClient, err := shardsetup.NewMultipoolerClient(primary.Multipooler.GrpcPort)
	require.NoError(t, err, "connect to primary")
	defer primaryClient.Close()
	standbyClient, err := shardsetup.NewMultipoolerClient(standby.Multipooler.GrpcPort)
	require.NoError(t, err, "connect to standby")
	defer standbyClient.Close()

	// Generate WAL on the primary, past the timeline fork.
	generateCtx := utils.WithTimeout(t, 10*time.Second)
	_, err = primaryClient.Pooler.ExecuteQuery(generateCtx, "CREATE TABLE IF NOT EXISTS crash_recovery_probe(id int)", 0)
	require.NoError(t, err, "create probe table")
	_, err = primaryClient.Pooler.ExecuteQuery(generateCtx, "INSERT INTO crash_recovery_probe SELECT generate_series(1,1000)", 0)
	require.NoError(t, err, "insert probe rows")

	// Pin the primary's current WAL position (from its Status, no raw SQL) and wait
	// for the standby to replay up to it via the WaitForLSN RPC. Reaching a
	// post-fork position on the current timeline is only possible if the standby
	// followed the switch cleanly — i.e. it is not timeline-diverged.
	primaryStatus, err := primaryClient.Manager.Status(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StatusRequest{})
	require.NoError(t, err, "primary Status")
	targetLSN := primaryStatus.GetStatus().GetPrimaryStatus().GetLsn()
	require.NotEmpty(t, targetLSN, "primary should report a current WAL LSN")
	t.Logf("waiting for standby to replay up to primary LSN %s", targetLSN)

	_, err = standbyClient.Manager.WaitForLSN(utils.WithTimeout(t, 60*time.Second), &multipoolermanagerdatapb.WaitForLSNRequest{
		TargetLsn: targetLSN,
		Timeout:   durationpb.New(45 * time.Second),
	})
	require.NoError(t, err, "standby should replay up to primary LSN %s (cleanly following the current timeline)", targetLSN)

	// Confirm the walreceiver is streaming — with the caught-up replay position
	// this establishes a non-diverged standby on the current timeline.
	status, err := standbyClient.Manager.Status(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StatusRequest{})
	require.NoError(t, err, "standby Status before kill")
	require.True(t, status.GetStatus().GetPostgresReady(), "standby postgres should be ready before kill")
	require.Equal(t, "streaming", status.GetStatus().GetReplicationStatus().GetWalReceiverStatus(),
		"standby walreceiver should be streaming (non-diverged) before kill")

	// SIGKILL the standby's postmaster (a real crash, bypassing clean shutdown).
	setup.KillPostgres(t, standby.Name)
	killedAt := time.Now()

	// The pooler monitor must auto-restart postgres via ordinary standby-mode
	// crash recovery. Record how long that takes so it lands in the e2e timing
	// summary alongside bootstrap/recovery/failover.
	restartLimit := utils.ScaleTimeout(60 * time.Second)
	require.Eventually(t, func() bool {
		status, err := standbyClient.Manager.Status(utils.WithShortDeadline(t), &multipoolermanagerdatapb.StatusRequest{})
		return err == nil && status.GetStatus().GetPostgresReady()
	}, restartLimit, 2*time.Second,
		"SIGKILLed standby postmaster should be auto-restarted by the pooler monitor")

	restartDuration := time.Since(killedAt)
	testtiming.Record(t, "standby postgres restart after kill", restartDuration, restartLimit)
	t.Logf("standby postgres recovered %s after SIGKILL", restartDuration.Round(time.Second))
}
