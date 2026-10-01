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

package clustersetup

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/lib/pq" // registers the "postgres" driver used by WaitForQueryServingOnPort
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/test/utils"
)

// CreatePgctldInstance creates a new pgctld process instance configuration.
// Follows the pattern from multipooler/setup_test.go:createPgctldInstance.
func CreatePgctldInstance(t *testing.T, name, baseDir string, grpcPort, pgPort, httpPort, pgbackrestPort int, pgbackrestCertDir string, backupLocation *clustermetadatapb.BackupLocation) *ProcessInstance {
	t.Helper()

	dataDir := filepath.Join(baseDir, name, "data")
	logFile := filepath.Join(baseDir, name, "pgctld.log")

	// Create data directory
	err := os.MkdirAll(filepath.Dir(logFile), 0o755)
	require.NoError(t, err)

	return &ProcessInstance{
		Name:              name,
		PoolerDir:         dataDir,
		LogFile:           logFile,
		GrpcPort:          grpcPort,
		HttpPort:          httpPort,
		PgPort:            pgPort,
		Binary:            "pgctld",
		PgBackRestPort:    pgbackrestPort,
		PgBackRestCertDir: pgbackrestCertDir,
		BackupLocation:    backupLocation,
		Environment:       append(utils.BaseTestEnv(), "PGCONNECT_TIMEOUT=5", "LC_ALL=en_US.UTF-8", "POSTGRES_PASSWORD="+TestPostgresPassword, constants.PgDataDirEnvVar+"="+filepath.Join(dataDir, "pg_data")),
	}
}

// WaitForQueryServingOnPort waits until a write succeeds through the gateway's
// PostgreSQL port. A write, not just SELECT 1, confirms that the gateway routes
// to the primary. withTLS makes the probe connect with sslmode=require, which
// also satisfies a gateway started with --pg-require-ssl.
func WaitForQueryServingOnPort(t *testing.T, pgPort int, withTLS bool) {
	t.Helper()

	// When TLS is configured on the gateway, use sslmode=require so this
	// readiness probe still works under --pg-require-ssl=true. The probe
	// only needs an encrypted transport, not certificate verification, so
	// sslmode=require is sufficient regardless of cert chain.
	sslMode := "sslmode=disable"
	if withTLS {
		sslMode = "sslmode=require"
	}
	connStr := GetTestUserDSN("localhost", pgPort, sslMode, "connect_timeout=2")

	ctx := utils.WithTimeout(t, 60*time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	startTime := time.Now()
	for {
		select {
		case <-ctx.Done():
			elapsed := time.Since(startTime)
			t.Fatalf("timeout waiting for multigateway to execute queries after %v (multigateway may not have discovered poolers from topology yet)", elapsed)
		case <-ticker.C:
			db, err := sql.Open("postgres", connStr)
			if err != nil {
				continue
			}

			// Verify both read and write paths work. SELECT 1 may succeed
			// via a REPLICA before multigateway learns about the PRIMARY.
			// CREATE TABLE forces routing to PRIMARY, confirming that
			// multigateway has discovered the primary pooler.
			queryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			_, err = db.ExecContext(queryCtx, "CREATE TABLE IF NOT EXISTS _mgw_ready_check (x int); DROP TABLE IF EXISTS _mgw_ready_check")
			cancel()
			db.Close()

			if err == nil {
				elapsed := time.Since(startTime)
				t.Logf("Multigateway can execute queries (ready after %v)", elapsed)
				return
			}
		}
	}
}

// PrintLogLocation prints the temp directory location for debugging.
// If TEST_PRINT_LOGS env var is set, also prints all log contents from the temp directory.
func PrintLogLocation(tempDir string) {
	println("\n" + "=" + "=== TEST LOGS PRESERVED ===" + "=")
	println("Logs available at: " + tempDir)

	// Only print log contents if TEST_PRINT_LOGS is set
	if os.Getenv("TEST_PRINT_LOGS") == "" {
		println("Set TEST_PRINT_LOGS=1 to print log contents")
		println("=" + "=========================" + "=")
		return
	}

	// Print all .log files found in the temp directory
	println("\n" + "=" + "=== SERVICE LOGS (test failure) ===" + "=")
	err := filepath.Walk(tempDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || filepath.Ext(path) != ".log" {
			return nil
		}

		println("\n--- " + path + " ---")
		// #nosec G122 -- walking the test's own temp dir to print logs on failure; no untrusted symlink TOCTOU.
		content, readErr := os.ReadFile(path)
		if readErr != nil {
			println("  [error reading log: " + readErr.Error() + "]")
			return nil //nolint:nilerr // Continue walking even if one file fails
		}
		if len(content) == 0 {
			println("  [empty log file]")
			return nil
		}
		println(string(content))
		return nil
	})
	if err != nil {
		println("  [error walking log directory: " + err.Error() + "]")
	}

	println("\n" + "=" + "=== END SERVICE LOGS ===" + "=")
}
