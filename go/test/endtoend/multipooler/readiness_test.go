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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
func TestMultipoolerReadinessReflectsPostmaster(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: PostgreSQL binaries not found")
	}

	setup := getSharedTestSetup(t)
	setupPoolerTest(t, setup)

	waitForManagerReady(t, setup, setup.PrimaryMultipooler)
	httpPort := setup.PrimaryMultipooler.HttpPort
	require.NotZero(t, httpPort, "primary multipooler HTTP port should be set")

	primaryClient := setup.NewPrimaryClient(t)
	defer primaryClient.Close()

	// Baseline: a healthy pooler with postgres accepting connections is ready.
	require.Eventually(t, func() bool {
		return readyStatusCode(t, httpPort) == http.StatusOK
	}, 30*time.Second, 500*time.Millisecond, "/ready should return 200 while postgres is healthy")

	// Hold postgres down by disabling the monitor's auto-restart before killing
	// it, so we can observe the NotReady window deterministically. The deferred
	// re-enable is a best-effort safety net for the shared fixture in case an
	// assertion below fails early; the happy path re-enables inline and then
	// asserts recovery.
	restartsReEnabled := false
	reEnableRestarts := func() {
		if restartsReEnabled {
			return
		}
		ctx := utils.WithShortDeadline(t)
		if _, err := primaryClient.Manager.SetPostgresRestartsEnabled(ctx,
			&multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: true}); err != nil {
			t.Logf("failed to re-enable postgres restarts: %v", err)
			return
		}
		restartsReEnabled = true
	}
	defer reEnableRestarts()

	disableCtx := utils.WithShortDeadline(t)
	_, err := primaryClient.Manager.SetPostgresRestartsEnabled(disableCtx,
		&multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: false})
	require.NoError(t, err, "should disable postgres restarts")

	t.Logf("Killing postgres on primary node %s", setup.PrimaryName)
	setup.KillPostgres(t, setup.PrimaryName)

	// With the postmaster dead, /ready must flip to 503.
	require.Eventually(t, func() bool {
		return readyStatusCode(t, httpPort) == http.StatusServiceUnavailable
	}, 30*time.Second, 500*time.Millisecond, "/ready should return 503 once the postmaster is dead")

	// Re-enable auto-restart; the monitor brings postgres back (crash recovery
	// for the SIGKILLed primary) and /ready recovers to 200.
	reEnableRestarts()
	require.True(t, restartsReEnabled, "postgres restarts should have been re-enabled")

	require.Eventually(t, func() bool {
		return readyStatusCode(t, httpPort) == http.StatusOK
	}, 60*time.Second, 500*time.Millisecond, "/ready should return 200 again after postgres recovers")

	// Sanity: the manager also reports postgres as ready again.
	require.Eventually(t, func() bool {
		ctx := utils.WithShortDeadline(t)
		status, err := primaryClient.Manager.Status(ctx, &multipoolermanagerdatapb.StatusRequest{})
		if err != nil {
			return false
		}
		return status.GetStatus().GetPostgresReady()
	}, 30*time.Second, 500*time.Millisecond, "manager should report postgres ready after recovery")

	assert.Equal(t, http.StatusOK, readyStatusCode(t, httpPort), "/ready should be 200 at test end")
}
