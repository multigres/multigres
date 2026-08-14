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

// TestMultipoolerReadinessReflectsGRPCNotPostgres verifies that the pooler's
// /ready endpoint reflects only whether its gRPC control plane is serving — and
// is deliberately decoupled from postgres. A pooler whose postgres is dead must
// stay Ready (200) so Kubernetes keeps its Service endpoint/DNS and the pod
// remains reachable for control RPCs and its health stream. This is the
// property that avoids coupling pod reachability to postgres availability.
//
// Uses an isolated single-node shard: killing a postmaster is destructive, and
// a single node with no peer and no live multiorch cannot trigger a failover,
// so the kill can't poison other tests.
func TestMultipoolerReadinessReflectsGRPCNotPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: PostgreSQL binaries not found")
	}

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

	// The gRPC server is up, so /ready is 200.
	require.Eventually(t, func() bool {
		return readyStatusCode(t, httpPort) == http.StatusOK
	}, 60*time.Second, 500*time.Millisecond, "/ready should return 200 while the gRPC server is up")

	// Kill postgres and hold it down (disable the monitor's auto-restart).
	disableCtx := utils.WithShortDeadline(t)
	_, err = client.Manager.SetPostgresRestartsEnabled(disableCtx,
		&multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: false})
	require.NoError(t, err, "should disable postgres restarts")

	t.Logf("Killing postgres on %s (readiness must stay 200)", inst.Name)
	setup.KillPostgres(t, inst.Name)

	// The decoupling property: with postgres dead but the gRPC server still
	// serving, /ready must remain 200 the whole time. If readiness were coupled
	// to postgres it would flip to 503 here.
	require.Never(t, func() bool {
		return readyStatusCode(t, httpPort) != http.StatusOK
	}, 15*time.Second, 1*time.Second, "/ready must stay 200 while postgres is down (readiness reflects gRPC, not postgres)")

	// Re-enable restarts so the node returns to a healthy steady state before teardown.
	enableCtx := utils.WithShortDeadline(t)
	_, err = client.Manager.SetPostgresRestartsEnabled(enableCtx,
		&multipoolermanagerdatapb.SetPostgresRestartsEnabledRequest{Enabled: true})
	require.NoError(t, err, "should re-enable postgres restarts")
}
