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

package multiadmin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/executil"
)

// newAdminClient dials the multiadmin gRPC server on localhost. Caller must
// Close() the returned connection.
func newAdminClient(t *testing.T, grpcPort int) (multiadminpb.MultiAdminServiceClient, *grpc.ClientConn) {
	t.Helper()
	addr := fmt.Sprintf("localhost:%d", grpcPort)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err, "failed to dial multiadmin at %s", addr)
	return multiadminpb.NewMultiAdminServiceClient(conn), conn
}

// TestMultiadminProcessRunning smoke-tests that the multiadmin process is up
// and that the harness recorded its ports.
func TestMultiadminProcessRunning(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: PostgreSQL binaries not found")
	}

	setup := getSharedSetup(t)

	require.NotNil(t, setup.Multiadmin, "WithMultiadmin() should have created a multiadmin instance")
	require.True(t, setup.Multiadmin.IsRunning(), "multiadmin process should be running")
	require.NotZero(t, setup.MultiadminHttpPort, "MultiadminHttpPort should be set")
	require.NotZero(t, setup.MultiadminGrpcPort, "MultiadminGrpcPort should be set")
}

// TestMultiadminLiveReady verifies that the /live and /ready HTTP endpoints
// served by the embedded servenv mux return 200.
func TestMultiadminLiveReady(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: PostgreSQL binaries not found")
	}

	setup := getSharedSetup(t)
	base := fmt.Sprintf("http://localhost:%d", setup.MultiadminHttpPort)

	for _, path := range []string{"/live", "/ready"} {
		t.Run(path, func(t *testing.T) {
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, base+path, nil)
			require.NoError(t, err)
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err, "GET %s failed", path)
			defer resp.Body.Close()
			assert.Equal(t, http.StatusOK, resp.StatusCode, "GET %s should return 200", path)
		})
	}
}

// TestMultiadminGRPC exercises the main read-only gRPC RPCs against the
// shared cluster. These are the same RPCs the multigres CLI calls.
func TestMultiadminGRPC(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: PostgreSQL binaries not found")
	}

	setup := getSharedSetup(t)
	client, conn := newAdminClient(t, setup.MultiadminGrpcPort)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	t.Run("GetCellNames", func(t *testing.T) {
		resp, err := client.GetCellNames(ctx, &multiadminpb.GetCellNamesRequest{})
		require.NoError(t, err)
		assert.Contains(t, resp.GetNames(), setup.CellName, "expected configured cell to be returned")
	})

	t.Run("GetDatabaseNames", func(t *testing.T) {
		resp, err := client.GetDatabaseNames(ctx, &multiadminpb.GetDatabaseNamesRequest{})
		require.NoError(t, err)
		assert.NotEmpty(t, resp.GetNames(), "expected at least one database in the topology")
	})

	t.Run("GetCell", func(t *testing.T) {
		resp, err := client.GetCell(ctx, &multiadminpb.GetCellRequest{Name: setup.CellName})
		require.NoError(t, err)
		require.NotNil(t, resp.GetCell(), "expected cell info")
		assert.NotEmpty(t, resp.GetCell().GetServerAddresses(), "cell should have etcd server addresses")
	})

	t.Run("GetPoolers", func(t *testing.T) {
		resp, err := client.GetPoolers(ctx, &multiadminpb.GetPoolersRequest{Cells: []string{setup.CellName}})
		require.NoError(t, err)
		// The shared cluster runs MultipoolerCount=2 poolers in the configured cell.
		assert.Len(t, resp.GetPoolers(), 2, "expected both multipoolers to be reported")
	})

	t.Run("GetGateways", func(t *testing.T) {
		resp, err := client.GetGateways(ctx, &multiadminpb.GetGatewaysRequest{Cells: []string{setup.CellName}})
		require.NoError(t, err)
		assert.Len(t, resp.GetGateways(), 1, "expected the single multigateway to be reported")
	})
}

// TestMultiadminHTTPAPI hits the grpc-gateway REST endpoints exposed under
// /api/v1/ on the multiadmin HTTP port — this is what the Next.js UI talks to.
func TestMultiadminHTTPAPI(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: PostgreSQL binaries not found")
	}

	setup := getSharedSetup(t)
	base := fmt.Sprintf("http://localhost:%d/api/v1", setup.MultiadminHttpPort)

	getJSON := func(t *testing.T, path string) map[string]any {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, base+path, nil)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err, "GET %s failed", path)
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode, "GET %s status=%d body=%s", path, resp.StatusCode, string(body))
		var out map[string]any
		require.NoError(t, json.Unmarshal(body, &out), "invalid JSON from %s: %s", path, string(body))
		return out
	}

	t.Run("cells", func(t *testing.T) {
		out := getJSON(t, "/cells")
		names, ok := out["names"].([]any)
		require.True(t, ok, "expected 'names' array, got %v", out)
		assert.Contains(t, names, setup.CellName)
	})

	t.Run("databases", func(t *testing.T) {
		out := getJSON(t, "/databases")
		names, ok := out["names"].([]any)
		require.True(t, ok, "expected 'names' array, got %v", out)
		assert.NotEmpty(t, names)
	})

	t.Run("poolers", func(t *testing.T) {
		out := getJSON(t, "/poolers?cells="+setup.CellName)
		poolers, ok := out["poolers"].([]any)
		require.True(t, ok, "expected 'poolers' array, got %v", out)
		assert.Len(t, poolers, 2)
	})

	t.Run("gateways", func(t *testing.T) {
		out := getJSON(t, "/gateways?cells="+setup.CellName)
		gateways, ok := out["gateways"].([]any)
		require.True(t, ok, "expected 'gateways' array, got %v", out)
		assert.Len(t, gateways, 1)
	})
}

// TestMultiadminCLI exercises the multigres CLI against the running
// multiadmin server via --admin-server. This is the integration path users
// actually take (and the one the localprovisioner test only covered when an
// end-to-end "multigres up" cluster was provisioned).
func TestMultiadminCLI(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: PostgreSQL binaries not found")
	}

	setup := getSharedSetup(t)
	adminServer := fmt.Sprintf("localhost:%d", setup.MultiadminGrpcPort)

	run := func(t *testing.T, subcmd string, extraArgs ...string) string {
		t.Helper()
		args := append([]string{subcmd, "--admin-server", adminServer}, extraArgs...)
		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		defer cancel()
		out, err := executil.Command(ctx, "multigres", args...).CombinedOutput()
		require.NoError(t, err, "multigres %s failed: %s", subcmd, string(out))
		return string(out)
	}

	t.Run("getcellnames", func(t *testing.T) {
		out := run(t, "getcellnames")
		assert.Contains(t, out, setup.CellName, "getcellnames should include the configured cell")
	})

	t.Run("getdatabasenames", func(t *testing.T) {
		out := run(t, "getdatabasenames")
		// Output is JSON: {"names": [...]}. We don't pin the exact name, just
		// require non-empty names.
		assert.Contains(t, out, "\"names\"")
	})

	t.Run("getpoolers", func(t *testing.T) {
		out := run(t, "getpoolers", "--cells", setup.CellName)
		assert.Contains(t, out, "\"poolers\"", "getpoolers output should be a JSON object with a 'poolers' field")
		// Both pooler service IDs should appear in the response payload.
		for name := range setup.Multipoolers {
			assert.Contains(t, out, name, "getpoolers output should mention pooler %q", name)
		}
	})

	t.Run("getgateways", func(t *testing.T) {
		out := run(t, "getgateways", "--cells", setup.CellName)
		assert.Contains(t, out, "\"gateways\"")
	})
}
