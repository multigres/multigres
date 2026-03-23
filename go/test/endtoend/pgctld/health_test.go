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

package pgctld

import (
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/executil"
)

// TestPgctldLiveEndpoint verifies that pgctld exposes an HTTP /live endpoint
// that returns 200 OK when the process is running.
func TestPgctldLiveEndpoint(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	if !utils.HasPostgreSQLBinaries() {
		t.Fatal("PostgreSQL binaries not found")
	}

	tempDir, cleanup := testutil.TempDir(t, "pgctld_health_test")
	defer cleanup()

	grpcPort := utils.GetFreePort(t)
	httpPort := utils.GetFreePort(t)
	pgPort := utils.GetFreePort(t)

	// Start pgctld server with HTTP port
	cmd := executil.Command(t.Context(), "pgctld",
		"server",
		"--pooler-dir", tempDir,
		"--grpc-port", strconv.Itoa(grpcPort),
		"--http-port", strconv.Itoa(httpPort),
		"--pg-port", strconv.Itoa(pgPort),
		"--timeout", "30",
	)
	setupTestEnv(cmd, tempDir)

	err := cmd.Start()
	require.NoError(t, err, "pgctld should start")
	t.Cleanup(func() {
		_ = cmd.Wait()
	})

	// Wait for HTTP /live endpoint to return 200
	liveURL := fmt.Sprintf("http://localhost:%d/live", httpPort)
	require.Eventually(t, func() bool {
		resp, err := http.Get(liveURL) //nolint:gosec // Test code, URL is constructed from local port
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 10*time.Second, 200*time.Millisecond, "/live endpoint should return 200")

	// Verify a second request also succeeds (not a one-time thing)
	resp, err := http.Get(liveURL) //nolint:gosec // Test code
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}
