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

package queryserving

import (
	"context"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/clustersetup"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
)

// restartedPoolerDiscoveryBudget is how long after a killed pooler serves again
// the gateway may take to see it serving. A retry the gateway makes when it sees
// the pooler's record rewritten takes well under a second. The gateway's own
// backoff after a crash is seconds to a minute.
const restartedPoolerDiscoveryBudget = 3 * time.Second

// restartedPoolerDownTime is how long the killed pooler stays down.
const restartedPoolerDownTime = 15 * time.Second

// TestMultigateway_SeesARestartedPoolerAtOnce: a pooler is killed with SIGKILL, so
// it never writes SHUTDOWN to its record and the gateway keeps its connection, with
// the health stream backing off from the crash. When the pooler comes back and
// registers again, the gateway must see it serving without waiting out the
// backoff.
//
// For each run the standby's multipooler is killed and, once the gateway has been
// backing off for restartedPoolerDownTime, started again. The time measured is from the
// pooler answering queries directly to the pooler showing as serving in the
// gateway's own view of its poolers (multigres.multipoolers).
// MULTIGRES_E2E_RESTART_RUNS sets the number of runs (default 3).
//
// The gateway's backoff doubles with every failed attempt and draws its delay at
// random up to that, so a single run can be fast by luck; the longer the pooler is
// down the more often it is not.
func TestMultigateway_SeesARestartedPoolerAtOnce(t *testing.T) {
	clustersetup.RequireMultigresTopology(t, "the test kills one of two poolers")
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	cluster := getSharedSetup(t)
	setup, ok := cluster.(*shardsetup.ShardSetup)
	require.True(t, ok, "the test needs a Multigres cluster")
	setup.SetupTest(t)
	ctx := t.Context()

	runs := 3
	if v := os.Getenv("MULTIGRES_E2E_RESTART_RUNS"); v != "" {
		n, err := strconv.Atoi(v)
		require.NoError(t, err)
		runs = n
	}

	standbys := setup.GetStandbys()
	require.NotEmpty(t, standbys, "the test needs a standby")
	standby := standbys[0]

	view, err := pgx.Connect(ctx, shardsetup.GetTestUserDSN("localhost", setup.ClientPort(), "sslmode=disable", "connect_timeout=5"))
	require.NoError(t, err)
	defer view.Close(context.Background())

	// gatewayServes reports whether the gateway's view of the standby says it serves.
	gatewayServes := func() bool {
		qctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		var status string
		var connected bool
		err := view.QueryRow(qctx, `SELECT serving_status, connected FROM multigres.multipoolers WHERE name = $1`, standby.Name).Scan(&status, &connected)
		return err == nil && connected && strings.EqualFold(status, "serving")
	}
	require.Eventually(t, gatewayServes, 30*time.Second, 100*time.Millisecond, "the gateway never saw the standby serve")

	var delays []time.Duration
	for run := 1; run <= runs; run++ {
		// SIGKILL: the record is left as it was, in place.
		killCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, stopped := standby.Multipooler.Process.Kill(killCtx)
		cancel()
		require.True(t, stopped, "multipooler %s should exit after SIGKILL", standby.Name)
		require.Eventually(t, func() bool { return !gatewayServes() }, 30*time.Second, 50*time.Millisecond,
			"the gateway kept the killed pooler as serving")

		// While it is down the gateway's health stream fails and backs off.
		time.Sleep(restartedPoolerDownTime)

		// The process outlives the test: it is part of the shared cluster.
		require.NoError(t, standby.Multipooler.Start(context.Background(), t))
		client, err := clustersetup.NewMultipoolerClient(standby.Multipooler.GrpcPort)
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			qctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			_, err := client.Pooler.ExecuteQuery(qctx, "SELECT 1", 1)
			return err == nil
		}, 60*time.Second, 10*time.Millisecond, "the restarted pooler never served")
		servingAgain := time.Now()
		_ = client.Close()

		require.Eventually(t, gatewayServes, 120*time.Second, 10*time.Millisecond,
			"the gateway never saw the restarted pooler serve")
		delay := time.Since(servingAgain)
		delays = append(delays, delay)
		t.Logf("run %d: the gateway saw the restarted pooler serve %v after it answered queries", run, delay.Round(time.Millisecond))

		// Let the pooler settle before the next kill.
		shardsetup.WaitForManagerReady(t, standby.Multipooler)
	}

	slices.Sort(delays)
	t.Logf("delays: min %v, median %v, max %v over %d runs",
		delays[0].Round(time.Millisecond), delays[len(delays)/2].Round(time.Millisecond), delays[len(delays)-1].Round(time.Millisecond), len(delays))
	require.Less(t, delays[len(delays)-1], restartedPoolerDiscoveryBudget,
		"the gateway waited out its health stream backoff instead of retrying when the pooler registered again")
}
