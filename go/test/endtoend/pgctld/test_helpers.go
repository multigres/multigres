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
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/executil"
)

// mockBinEnv returns the base environment for tests that use mock PostgreSQL
// binaries. It includes PATH pointing at binDir, PGDATA, POSTGRES_PASSWORD and
// PGPASSWORD so pgctld init/start/stop subprocesses can satisfy the scram-sha-256
// requirement without needing a real PostgreSQL instance.
func mockBinEnv(binDir, pgDataDir string) []string {
	env := append(utils.BaseTestEnv(),
		"PATH="+binDir+":"+os.Getenv("PATH"),
		constants.PgDataDirEnvVar+"="+pgDataDir,
		constants.PgPasswordEnvVar+"=test-password",
		"PGPASSWORD=test-password",
	)
	if runtime.GOOS == "darwin" {
		env = append(env, "LC_ALL=en_US.UTF-8")
	}
	return env
}

// setupTestEnv sets common environment variables for pgctld subprocesses.
// Includes a default POSTGRES_PASSWORD because pgctld now refuses to init or
// serve without one (pg_hba.conf requires scram-sha-256 for all connections).
// Also sets PGPASSWORD so psql subprocesses can authenticate.
//
// Uses utils.BaseTestEnv() as the base environment to strip container-specific
// variables (POSTGRES_USER=supabase_admin, POSTGRES_INITDB_SQL_FILES) that would
// otherwise be inherited from the Supabase Postgres container and corrupt the
// test cluster. POSTGRES_USER=postgres is set by BaseTestEnv().
func setupTestEnv(cmd *executil.Cmd, poolerDir string) {
	cmd.SetEnv(utils.BaseTestEnv())
	cmd.AddEnv(
		"PGCONNECT_TIMEOUT=5",
		constants.PgDataDirEnvVar+"="+filepath.Join(poolerDir, "pg_data"),
		constants.PgPasswordEnvVar+"=test-password",
		"PGPASSWORD=test-password",
	)
	if runtime.GOOS == "darwin" {
		// Required to avoid "postmaster became multithreaded during startup" on macOS.
		cmd.AddEnv("LC_ALL=en_US.UTF-8")
	}
}

// pgCtldServer holds the ports and process handle for a running pgctld server.
type pgCtldServer struct {
	GrpcPort int
	HttpPort int
	PgPort   int
	Cmd      *executil.Cmd
}

// startPgCtldServer starts a pgctld server subprocess and waits until its gRPC
// port is accepting connections. poolerDir is passed as --pooler-dir.
// configFile is optional; if non-empty it is passed as --config-file.
// extraEnv may be used to inject additional environment variables such as
// POSTGRES_PASSWORD or a custom PATH.
//
// The server is stopped automatically when the test ends via t.Cleanup.
// The returned Cmd may also be used directly (e.g. to kill the process
// mid-test for orphan-detection tests).
func startPgCtldServer(t *testing.T, poolerDir, configFile string, extraEnv ...string) *pgCtldServer {
	t.Helper()

	grpcPort := utils.GetFreePort(t)
	httpPort := utils.GetFreePort(t)
	pgPort := utils.GetFreePort(t)

	args := []string{
		"server",
		"--pooler-dir", poolerDir,
		"--grpc-port", strconv.Itoa(grpcPort),
		"--http-port", strconv.Itoa(httpPort),
		"--pg-port", strconv.Itoa(pgPort),
		"--timeout", "30",
	}
	if configFile != "" {
		args = append(args, "--config-file", configFile)
	}

	cmd := executil.Command(t.Context(), "pgctld", args...).WithProcessGroup()
	cmd.AddEnv("MULTIGRES_TESTDATA_DIR=" + poolerDir)
	setupTestEnv(cmd, poolerDir)
	for _, e := range extraEnv {
		cmd.AddEnv(e)
	}

	require.NoError(t, cmd.Start(), "pgctld server should start")
	t.Cleanup(func() { _ = cmd.Wait() })

	// Wait for gRPC port to accept connections.
	require.Eventually(t, func() bool {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("localhost:%d", grpcPort), 100*time.Millisecond)
		if err != nil {
			return false
		}
		conn.Close()
		return true
	}, 20*time.Second, 100*time.Millisecond, "pgctld gRPC server should be ready on port %d", grpcPort)

	return &pgCtldServer{
		GrpcPort: grpcPort,
		HttpPort: httpPort,
		PgPort:   pgPort,
		Cmd:      cmd,
	}
}
