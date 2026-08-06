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

package multipooler

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"

	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// readyStatusCode issues a GET against the multipooler's /ready endpoint and
// returns the HTTP status code, or 0 if the request could not be made.
func readyStatusCode(t *testing.T, httpPort int) int {
	t.Helper()
	url := fmt.Sprintf("http://localhost:%d/ready", httpPort)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Logf("GET %s failed: %v", url, err)
		return 0
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

// TestMultipoolerReadinessReflectsPostmaster verifies the MUL-1009 readiness
// gap fix: when this pod's PostgreSQL postmaster is dead, the multipooler's
// HTTP /ready endpoint returns 503 (so Kubernetes marks the pod NotReady)
// instead of the previous always-200 behavior that hid a crashed/down node.
// It then confirms /ready recovers to 200 once postgres is back.
//
// Uses an isolated single-node shard rather than the shared fixture: killing a
// postmaster is destructive, and a single node with no peer and no live
// multiorch cannot trigger a failover, so the kill can't poison other tests.
func TestMultipoolerReadinessReflectsPostmaster(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: PostgreSQL binaries not found")
	}

	// Single pooler, no multiorch. Deferred start so the pooler self-bootstraps
	// into a serving primary via its monitor loop once we start it.
	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(1),
		shardsetup.WithDeferredMultipoolerStart(),
	)
	defer cleanup()

	require.Len(t, setup.Multipoolers, 1)
	var inst *shardsetup.MultipoolerInstance
	for _, v := range setup.Multipoolers {
		inst = v
	}

	require.NoError(t, inst.Multipooler.Start(t.Context(), t))
	shardsetup.WaitForManagerReady(t, inst.Multipooler)

	httpPort := inst.Multipooler.HttpPort
	require.NotZero(t, httpPort, "multipooler HTTP port should be set")

	client, err := shardsetup.NewMultipoolerClient(inst.Multipooler.GrpcPort)
	require.NoError(t, err)
	defer client.Close()

	// Baseline: once postgres has self-bootstrapped and is accepting, /ready is 200.
	require.Eventually(t, func() bool {
		return readyStatusCode(t, httpPort) == http.StatusOK
	}, 90*time.Second, 1*time.Second, "/ready should return 200 once postgres is up")

	// Hold postgres down by disabling the monitor's auto-restart before killing
	// it, so the NotReady window is observable deterministically.
	disableCtx := utils.WithShortDeadline(t)
	_, err = client.Manager.SetPostgresRestartsEnabled(disableCtx,
		&multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: false})
	require.NoError(t, err, "should disable postgres restarts")

	t.Logf("Killing postgres on %s", inst.Name)
	setup.KillPostgres(t, inst.Name)

	// With the postmaster dead, /ready must flip to 503.
	require.Eventually(t, func() bool {
		return readyStatusCode(t, httpPort) == http.StatusServiceUnavailable
	}, 30*time.Second, 500*time.Millisecond, "/ready should return 503 once the postmaster is dead")

	// Re-enable auto-restart; the monitor brings postgres back (crash recovery
	// for the SIGKILLed node) and /ready recovers to 200.
	enableCtx := utils.WithShortDeadline(t)
	_, err = client.Manager.SetPostgresRestartsEnabled(enableCtx,
		&multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: true})
	require.NoError(t, err, "should re-enable postgres restarts")

	require.Eventually(t, func() bool {
		return readyStatusCode(t, httpPort) == http.StatusOK
	}, 60*time.Second, 500*time.Millisecond, "/ready should return 200 again after postgres recovers")
}
