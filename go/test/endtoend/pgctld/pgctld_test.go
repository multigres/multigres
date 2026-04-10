// Copyright 2025 Supabase, Inc.
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
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/multigres/multigres/go/cmd/pgctld/command"
	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	"github.com/multigres/multigres/go/common/constants"
	pb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/executil"
	"github.com/multigres/multigres/go/tools/pathutil"
)

// TestMain sets up the test environment for pgctld tests
func TestMain(m *testing.M) {
	// Add bin/ to PATH so pgctld binary can be found
	if err := pathutil.PrependBinToPath(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to add bin/ to PATH: %v\n", err)
		os.Exit(1) //nolint:forbidigo
	}

	os.Exit(m.Run()) //nolint:forbidigo
}

// setupTestEnv sets up environment variables for PostgreSQL tests.
// poolerDir is the pooler directory; PGDATA is set to poolerDir/pg_data.

// TestEndToEndWithRealPostgreSQL tests pgctld with real PostgreSQL binaries
// This test requires PostgreSQL to be installed on the system
func TestEndToEndWithRealPostgreSQL(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	// Check if PostgreSQL binaries are available
	if !utils.HasPostgreSQLBinaries() {
		t.Fatal("PostgreSQL binaries not found, make sure to install PostgreSQL and add it to the PATH")
	}

	tempDir, cleanup := testutil.TempDir(t, "pgctld_e2e_test")
	defer cleanup()

	dataDir := filepath.Join(tempDir, "data")

	// Create a pgctld config file to avoid config file errors
	pgctldConfigFile := filepath.Join(tempDir, ".pgctld.yaml")
	err := os.WriteFile(pgctldConfigFile, []byte(`
# Test pgctld configuration for e2e tests
log-level: info
timeout: 30
`), 0o644)
	require.NoError(t, err)

	t.Run("basic_commands_with_real_postgresql", func(t *testing.T) {
		// Step 1: Initialize the database first
		initCmd := executil.Command(t.Context(), "pgctld", "init", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
		setupTestEnv(initCmd, dataDir)
		initOutput, err := initCmd.CombinedOutput()
		if err != nil {
			t.Logf("pgctld init failed with output: %s", string(initOutput))
		}
		require.NoError(t, err)

		// Step 1.5: Verify data checksums are enabled
		pgDataDir := filepath.Join(dataDir, "pg_data")
		controldataCmd := exec.Command("pg_controldata", pgDataDir)
		controldataOutput, err := controldataCmd.CombinedOutput()
		require.NoError(t, err, "pg_controldata should succeed, output: %s", string(controldataOutput))

		outputStr := string(controldataOutput)
		assert.Contains(t, outputStr, "Data page checksum version:", "should report checksum version")
		assert.NotContains(t, outputStr, "Data page checksum version:                  0",
			"checksums should be enabled (non-zero version)")
		t.Log("Verified: data checksums are enabled")

		// Step 2: Check status - should show stopped after init
		statusCmd := executil.Command(t.Context(), "pgctld", "status", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
		setupTestEnv(statusCmd, dataDir)
		output, err := statusCmd.CombinedOutput()
		if err != nil {
			t.Logf("pgctld status failed with output: %s", string(output))
		}
		require.NoError(t, err)
		assert.Contains(t, string(output), "Stopped")

		// Step 3: Test help commands work
		helpCmd := exec.Command("pgctld", "--help")
		helpOutput, err := helpCmd.Output()
		require.NoError(t, err)
		assert.Contains(t, string(helpOutput), "pgctld")

		// Step 4: Test that real PostgreSQL binaries are detected
		versionCmd := exec.Command("postgres", "--version")
		versionOutput, err := versionCmd.Output()
		require.NoError(t, err)
		t.Logf("PostgreSQL version: %s", string(versionOutput))
		assert.Contains(t, string(versionOutput), "postgres")

		// Step 5: Test initialization works with real PostgreSQL
		initdbCmd := exec.Command("initdb", "--help")
		initdbOutput, err := initdbCmd.Output()
		require.NoError(t, err)
		assert.Contains(t, string(initdbOutput), "initdb")

		t.Log("End-to-end test environment validated successfully")
	})
}

// TestEndToEndGRPCWithRealPostgreSQL tests the gRPC interface with real PostgreSQL
func TestEndToEndGRPCWithRealPostgreSQL(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	// Check if PostgreSQL binaries are available
	if !utils.HasPostgreSQLBinaries() {
		t.Fatal("PostgreSQL binaries not found, make sure to install PostgreSQL and add it to the PATH")
	}

	tempDir, cleanup := testutil.TempDir(t, "pgctld_grpc_e2e_test")
	defer cleanup()

	dataDir := filepath.Join(tempDir, "data")

	// Create a pgctld config file to avoid config file errors
	pgctldConfigFile := filepath.Join(tempDir, ".pgctld.yaml")
	err := os.WriteFile(pgctldConfigFile, []byte(`
# Test pgctld configuration for gRPC e2e tests
log-level: info
timeout: 30
`), 0o644)
	require.NoError(t, err)

	t.Run("grpc_server_with_real_postgresql", func(t *testing.T) {
		srv := startPgCtldServer(t, dataDir, pgctldConfigFile)
		t.Logf("gRPC test using ports - gRPC: %d, PostgreSQL: %d", srv.GrpcPort, srv.PgPort)
	})
}

// TestEndToEndPerformance tests performance characteristics
func TestEndToEndPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance tests in short mode")
	}

	// Check if PostgreSQL binaries are available
	if !utils.HasPostgreSQLBinaries() {
		t.Fatal("PostgreSQL binaries not found, make sure to install PostgreSQL and add it to the PATH")
	}

	tempDir, cleanup := testutil.TempDir(t, "pgctld_perf_test")
	defer cleanup()

	dataDir := filepath.Join(tempDir, "data")

	// Create a pgctld config file to avoid config file errors
	pgctldConfigFile := filepath.Join(tempDir, ".pgctld.yaml")
	err := os.WriteFile(pgctldConfigFile, []byte(`
# Test pgctld configuration for performance tests
log-level: info
timeout: 30
`), 0o644)
	require.NoError(t, err)

	// Use cached pgctld binary for testing

	// Get free port for these tests (shared across subtests since they share the same dataDir)
	perfTestPort := utils.GetFreePort(t)

	t.Run("startup_performance", func(t *testing.T) {
		t.Logf("Performance test using port: %d", perfTestPort)

		// Measure time to start PostgreSQL
		startTime := time.Now()
		initCmd := executil.Command(t.Context(), "pgctld", "init", "--pooler-dir", dataDir, "--pg-port", strconv.Itoa(perfTestPort), "--config-file", pgctldConfigFile)
		setupTestEnv(initCmd, dataDir)
		startOutput, err := initCmd.CombinedOutput()
		if err != nil {
			t.Logf("pgctld start failed with output: %s", string(startOutput))
		}

		require.NoError(t, err)

		startCmd := executil.Command(t.Context(), "pgctld", "start", "--pooler-dir", dataDir, "--pg-port", strconv.Itoa(perfTestPort), "--config-file", pgctldConfigFile)
		setupTestEnv(startCmd, dataDir)
		startOutput, err = startCmd.CombinedOutput()
		if err != nil {
			t.Logf("pgctld start failed with output: %s", string(startOutput))
		}
		require.NoError(t, err)

		startupDuration := time.Since(startTime)
		t.Logf("PostgreSQL startup took: %v", startupDuration)

		// Startup should typically complete within 30 seconds
		assert.Less(t, startupDuration, 30*time.Second, "PostgreSQL startup took too long")

		// Clean shutdown
		stopCmd := executil.Command(t.Context(), "pgctld", "stop", "--pooler-dir", dataDir, "--pg-port", strconv.Itoa(perfTestPort), "--config-file", pgctldConfigFile)
		setupTestEnv(stopCmd, dataDir)
		err = stopCmd.Run()
		require.NoError(t, err)
	})

	t.Run("multiple_rapid_operations", func(t *testing.T) {
		// Test rapid start/stop cycles using the same port as startup_performance
		for i := range 3 {
			t.Logf("Cycle %d", i+1)

			// Start
			startCmd := executil.Command(t.Context(), "pgctld", "start", "--pooler-dir", dataDir, "--pg-port", strconv.Itoa(perfTestPort), "--config-file", pgctldConfigFile)
			setupTestEnv(startCmd, dataDir)
			startOutput, err := startCmd.CombinedOutput()
			if err != nil {
				t.Logf("pgctld start failed with error: %v, output: %s", err, string(startOutput))
			}
			require.NoError(t, err)

			// Brief wait
			time.Sleep(1 * time.Second)

			// Stop
			stopCmd := executil.Command(t.Context(), "pgctld", "stop", "--pooler-dir", dataDir, "--pg-port", strconv.Itoa(perfTestPort), "--mode", "fast", "--config-file", pgctldConfigFile)
			setupTestEnv(stopCmd, dataDir)
			err = stopCmd.Run()
			require.NoError(t, err)

			// Brief wait before next cycle
			time.Sleep(500 * time.Millisecond)
		}
	})
}

// TestEndToEndSystemIntegration tests integration with system PostgreSQL
func TestEndToEndSystemIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping system integration tests in short mode")
	}

	// Check if PostgreSQL binaries are available
	if !utils.HasPostgreSQLBinaries() {
		t.Fatal("PostgreSQL binaries not found, make sure to install PostgreSQL and add it to the PATH")
	}

	tempDir, cleanup := testutil.TempDir(t, "pgctld_system_test")
	defer cleanup()

	dataDir := filepath.Join(tempDir, "data")

	// Create a pgctld config file to avoid config file errors
	pgctldConfigFile := filepath.Join(tempDir, ".pgctld.yaml")
	err := os.WriteFile(pgctldConfigFile, []byte(`
# Test pgctld configuration for system integration tests
log-level: info
timeout: 30
`), 0o644)
	require.NoError(t, err)

	// Use cached pgctld binary for testing

	t.Run("version_compatibility", func(t *testing.T) {
		// Get free port for this test
		testPort := utils.GetFreePort(t)
		t.Logf("Using port: %d", testPort)

		// Check PostgreSQL version compatibility
		versionCmd := exec.Command("postgres", "--version")
		output, err := versionCmd.Output()
		require.NoError(t, err)
		t.Logf("PostgreSQL version: %s", string(output))

		initCmd := executil.Command(t.Context(), "pgctld", "init", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile, "--pg-port", strconv.Itoa(testPort))
		setupTestEnv(initCmd, dataDir)
		initOutput, err := initCmd.CombinedOutput()
		if err != nil {
			t.Logf("pgctld init failed with output: %s", string(initOutput))
		}
		require.NoError(t, err)

		// Start PostgreSQL to test compatibility
		startCmd := executil.Command(t.Context(), "pgctld", "start", "--pooler-dir", dataDir, "--pg-port", strconv.Itoa(testPort), "--config-file", pgctldConfigFile)
		setupTestEnv(startCmd, dataDir)
		startOutput, err := startCmd.CombinedOutput()
		if err != nil {
			t.Logf("pgctld start failed with output: %s", string(startOutput))
		}
		require.NoError(t, err)

		// Get version info through pgctld
		statusCmd := executil.Command(t.Context(), "pgctld", "status", "--pooler-dir", dataDir, "--pg-port", strconv.Itoa(testPort), "--config-file", pgctldConfigFile)
		setupTestEnv(statusCmd, dataDir)
		output, err = statusCmd.Output()
		require.NoError(t, err)
		t.Logf("pgctld status output: %s", string(output))

		// Clean shutdown
		stopCmd := executil.Command(t.Context(), "pgctld", "stop", "--pooler-dir", dataDir, "--pg-port", strconv.Itoa(testPort), "--config-file", pgctldConfigFile)
		setupTestEnv(stopCmd, dataDir)
		err = stopCmd.Run()
		require.NoError(t, err)
	})
}

// TestRestartAsStandbyWithRealPostgreSQL tests restart --as-standby with real PostgreSQL
func TestRestartAsStandbyWithRealPostgreSQL(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	// Check if PostgreSQL binaries are available
	if !utils.HasPostgreSQLBinaries() {
		t.Skip("PostgreSQL binaries not found, skipping real PostgreSQL test")
	}

	tempDir, cleanup := testutil.TempDir(t, "pgctld_restart_standby_real_test")
	defer cleanup()

	dataDir := filepath.Join(tempDir, "data")

	// Create a pgctld config file
	pgctldConfigFile := filepath.Join(tempDir, ".pgctld.yaml")
	err := os.WriteFile(pgctldConfigFile, []byte(`
# Test pgctld configuration
log-level: info
timeout: 30
`), 0o644)
	require.NoError(t, err)

	// Get free port for this test
	testPort := utils.GetFreePort(t)
	t.Logf("Using port: %d", testPort)

	// Initialize database
	initCmd := executil.Command(t.Context(), "pgctld", "init", "--pooler-dir", dataDir, "--pg-port", strconv.Itoa(testPort), "--config-file", pgctldConfigFile)
	setupTestEnv(initCmd, dataDir)
	initOutput, err := initCmd.CombinedOutput()
	if err != nil {
		t.Logf("pgctld init failed with output: %s", string(initOutput))
	}
	require.NoError(t, err)

	// Start PostgreSQL
	startCmd := executil.Command(t.Context(), "pgctld", "start", "--pooler-dir", dataDir, "--pg-port", strconv.Itoa(testPort), "--config-file", pgctldConfigFile)
	setupTestEnv(startCmd, dataDir)
	startOutput, err := startCmd.CombinedOutput()
	if err != nil {
		t.Logf("pgctld start failed with output: %s", string(startOutput))
	}
	require.NoError(t, err)

	// Verify it's running
	statusCmd := executil.Command(t.Context(), "pgctld", "status", "--pooler-dir", dataDir, "--pg-port", strconv.Itoa(testPort), "--config-file", pgctldConfigFile)
	setupTestEnv(statusCmd, dataDir)
	output, err := statusCmd.Output()
	require.NoError(t, err)
	assert.Contains(t, string(output), "Running")

	// Restart as standby
	restartCmd := executil.Command(t.Context(), "pgctld", "restart", "--pooler-dir", dataDir, "--pg-port", strconv.Itoa(testPort), "--as-standby", "--config-file", pgctldConfigFile)
	setupTestEnv(restartCmd, dataDir)
	output, err = restartCmd.CombinedOutput()
	if err != nil {
		t.Logf("restart --as-standby output: %s", string(output))
	}
	require.NoError(t, err)
	assert.Contains(t, string(output), "restarted as standby successfully")

	// Verify standby.signal was created
	standbySignalPath := filepath.Join(dataDir, "pg_data", "standby.signal")
	_, err = os.Stat(standbySignalPath)
	assert.NoError(t, err, "standby.signal file should exist after restart --as-standby")

	// Verify server is still running
	statusCmd = executil.Command(t.Context(), "pgctld", "status", "--pooler-dir", dataDir, "--pg-port", strconv.Itoa(testPort), "--config-file", pgctldConfigFile)
	setupTestEnv(statusCmd, dataDir)
	output, err = statusCmd.Output()
	require.NoError(t, err)
	assert.Contains(t, string(output), "Running")

	// Verify PostgreSQL is in recovery mode (standby mode) by querying pg_is_in_recovery()
	t.Logf("Verifying PostgreSQL is in recovery mode")
	socketDir := filepath.Join(dataDir, "pg_sockets")
	recoveryCheckCmd := executil.Command(t.Context(), "psql",
		"-h", socketDir,
		"-p", strconv.Itoa(testPort),
		"-U", "postgres",
		"-d", "postgres",
		"-t", "-c", "SELECT pg_is_in_recovery();",
	)
	setupTestEnv(recoveryCheckCmd, dataDir)
	output, err = recoveryCheckCmd.CombinedOutput()
	require.NoError(t, err, "Recovery check should succeed, output: %s", string(output))
	t.Logf("Recovery mode check result: %s", string(output))
	// The output should contain 't' for true indicating recovery mode
	assert.Contains(t, strings.TrimSpace(string(output)), "t", "PostgreSQL should be in recovery mode after restart --as-standby")

	// Clean stop
	stopCmd := executil.Command(t.Context(), "pgctld", "stop", "--pooler-dir", dataDir, "--pg-port", strconv.Itoa(testPort), "--config-file", pgctldConfigFile)
	setupTestEnv(stopCmd, dataDir)
	err = stopCmd.Run()
	require.NoError(t, err)
}

// TestPostgreSQLAuthentication tests PostgreSQL authentication with POSTGRES_PASSWORD
func TestPostgreSQLAuthentication(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping authentication tests in short mode")
	}

	// Check if PostgreSQL binaries are available
	if !utils.HasPostgreSQLBinaries() {
		t.Fatal("PostgreSQL binaries not found, skipping authentication test")
	}

	t.Run("pgpassword_authentication", func(t *testing.T) {
		// Set up temporary directory
		baseDir, cleanup := testutil.TempDir(t, "pgctld_auth_test")
		defer cleanup()

		t.Logf("Base directory: %s", baseDir)
		t.Logf("Base directory is absolute: %v", filepath.IsAbs(baseDir))

		// Use cached pgctld binary for testing

		// Get available port for PostgreSQL
		port := utils.GetFreePort(t)
		t.Logf("Authentication test using port: %d", port)

		// Test password
		testPassword := "secure_test_password_123"

		// Initialize with POSTGRES_PASSWORD
		t.Logf("Initializing PostgreSQL with POSTGRES_PASSWORD")
		initCmd := exec.Command("pgctld", "init", "--pooler-dir", baseDir, "--pg-port", strconv.Itoa(port))

		initCmd.Env = append(os.Environ(),
			"PGCONNECT_TIMEOUT=5",
			"POSTGRES_PASSWORD="+testPassword,
			constants.PgDataDirEnvVar+"="+filepath.Join(baseDir, "pg_data"),
		)
		// Required to avoid "postmaster became multithreaded during startup" on macOS
		if runtime.GOOS == "darwin" {
			initCmd.Env = append(initCmd.Env, "LC_ALL=en_US.UTF-8")
		}

		output, err := initCmd.CombinedOutput()
		require.NoError(t, err, "pgctld init should succeed, output: %s", string(output))
		assert.Contains(t, string(output), "\"password_source\":\"POSTGRES_PASSWORD environment variable\"", "Should use POSTGRES_PASSWORD")

		// Start the PostgreSQL server
		t.Logf("Starting PostgreSQL server")
		startCmd := exec.Command("pgctld", "start", "--pooler-dir", baseDir, "--pg-port", strconv.Itoa(port))
		startCmd.Env = append(os.Environ(),
			"PGCONNECT_TIMEOUT=5",
			"POSTGRES_PASSWORD="+testPassword,
			constants.PgDataDirEnvVar+"="+filepath.Join(baseDir, "pg_data"),
		)

		// Required to avoid "postmaster became multithreaded during startup" on macOS
		if runtime.GOOS == "darwin" {
			startCmd.Env = append(startCmd.Env, "LC_ALL=en_US.UTF-8")
		}

		output, err = startCmd.CombinedOutput()
		require.NoError(t, err, "pgctld start should succeed, output: %s", string(output))

		// Give the server a moment to be fully ready
		time.Sleep(2 * time.Second)

		// Test socket connection (should work without password)
		t.Logf("Testing Unix socket connection (no password required)")
		socketDir := filepath.Join(baseDir, "pg_sockets")
		t.Logf("Socket directory path: %s", socketDir)
		t.Logf("Socket directory absolute path: %s", filepath.Join(socketDir))

		socketCmd := exec.Command("psql",
			"-h", socketDir,
			"-p", strconv.Itoa(port), // Need to specify port even for socket connections
			"-U", "postgres",
			"-d", "postgres",
			"-c", "SELECT current_user, current_database();",
		)
		t.Logf("psql command: %v", socketCmd.Args)
		output, err = socketCmd.CombinedOutput()
		require.NoError(t, err, "Socket connection should succeed, output: %s", string(output))
		assert.Contains(t, string(output), "postgres", "Should connect as postgres user")

		// Get the actual port from the status output
		statusCmd := exec.Command("pgctld", "status", "--pooler-dir", baseDir)
		statusCmd.Env = append(os.Environ(), constants.PgDataDirEnvVar+"="+filepath.Join(baseDir, "pg_data"))
		statusOutput, err := statusCmd.CombinedOutput()
		require.NoError(t, err, "pgctld status should succeed")
		t.Logf("Status output: %s", string(statusOutput))

		// Test TCP connection with correct password
		t.Logf("Testing TCP connection with correct password")
		tcpCmd := exec.Command("psql",
			"-h", "localhost",
			"-p", strconv.Itoa(port), // Use the same port that was configured
			"-U", "postgres",
			"-d", "postgres",
			"-c", "SELECT current_user, current_database();",
		)
		tcpCmd.Env = append(os.Environ(), "PGPASSWORD="+testPassword)
		output, err = tcpCmd.CombinedOutput()
		require.NoError(t, err, "TCP connection with correct password should succeed, output: %s", string(output))
		assert.Contains(t, string(output), "postgres", "Should connect as postgres user")

		// Test TCP connection with wrong password (should fail)
		t.Logf("Testing TCP connection with wrong password")
		wrongPasswordCmd := exec.Command("psql",
			"-h", "localhost",
			"-p", strconv.Itoa(port),
			"-U", "postgres",
			"-d", "postgres",
			"-c", "SELECT 1;",
		)
		wrongPasswordCmd.Env = append(os.Environ(), "PGPASSWORD=wrong_password")
		output, err = wrongPasswordCmd.CombinedOutput()
		assert.Error(t, err, "TCP connection with wrong password should fail")
		assert.Contains(t, string(output), "password authentication failed", "Should fail with authentication error")

		// Verify role and database exist via socket connection
		t.Logf("Verifying postgres role and database exist")

		// Check that postgres role exists
		roleCheckCmd := exec.Command("psql",
			"-h", socketDir,
			"-p", strconv.Itoa(port),
			"-U", "postgres",
			"-d", "postgres",
			"-t", "-c", "SELECT rolname FROM pg_roles WHERE rolname = 'postgres';",
		)
		output, err = roleCheckCmd.CombinedOutput()
		require.NoError(t, err, "Role check should succeed, output: %s", string(output))
		assert.Contains(t, string(output), "postgres", "postgres role should exist")

		// Check that postgres database exists
		dbCheckCmd := exec.Command("psql",
			"-h", socketDir,
			"-p", strconv.Itoa(port),
			"-U", "postgres",
			"-d", "postgres",
			"-t", "-c", "SELECT datname FROM pg_database WHERE datname = 'postgres';",
		)
		output, err = dbCheckCmd.CombinedOutput()
		require.NoError(t, err, "Database check should succeed, output: %s", string(output))
		assert.Contains(t, string(output), "postgres", "postgres database should exist")

		// Check role privileges
		privilegeCheckCmd := exec.Command("psql",
			"-h", socketDir,
			"-p", strconv.Itoa(port),
			"-U", "postgres",
			"-d", "postgres",
			"-t", "-c", "SELECT rolsuper FROM pg_roles WHERE rolname = 'postgres';",
		)
		output, err = privilegeCheckCmd.CombinedOutput()
		require.NoError(t, err, "Privilege check should succeed, output: %s", string(output))
		assert.Contains(t, string(output), "t", "postgres role should have superuser privileges")

		// Clean shutdown
		t.Logf("Shutting down PostgreSQL")
		stopCmd := exec.Command("pgctld", "stop", "--pooler-dir", baseDir)
		stopCmd.Env = append(os.Environ(), "PGCONNECT_TIMEOUT=5", constants.PgDataDirEnvVar+"="+filepath.Join(baseDir, "pg_data"))
		err = stopCmd.Run()
		require.NoError(t, err, "pgctld stop should succeed")
	})
}

// TestCustomDatabaseCreation verifies that pgctld init creates a non-default database
// when POSTGRES_DB (or --pg-database) names a database other than "postgres".
// This mirrors the behaviour of the docker-library/postgres entrypoint's docker_setup_db().
func TestCustomDatabaseCreation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping custom database creation tests in short mode")
	}

	if !utils.HasPostgreSQLBinaries() {
		t.Fatal("PostgreSQL binaries not found, skipping custom database creation test")
	}

	// runDatabaseCreationTest is shared by both sub-tests below.
	// It initialises pgctld with the given extra env vars / CLI args, starts PostgreSQL,
	// then verifies that targetDB exists and is connectable.
	runDatabaseCreationTest := func(t *testing.T, targetDB string, extraEnv []string, extraInitArgs []string) {
		t.Helper()

		baseDir, cleanup := testutil.TempDir(t, "pgctld_customdb_test")
		defer cleanup()

		port := utils.GetFreePort(t)
		socketDir := filepath.Join(baseDir, "pg_sockets")

		baseEnv := append(os.Environ(), "PGCONNECT_TIMEOUT=5", constants.PgDataDirEnvVar+"="+filepath.Join(baseDir, "pg_data"))
		if runtime.GOOS == "darwin" {
			baseEnv = append(baseEnv, "LC_ALL=en_US.UTF-8")
		}

		// -- init --
		initArgs := []string{"init", "--pooler-dir", baseDir, "--pg-port", strconv.Itoa(port)}
		initArgs = append(initArgs, extraInitArgs...)
		initCmd := exec.Command("pgctld", initArgs...)
		initCmd.Env = append(baseEnv, extraEnv...)
		out, err := initCmd.CombinedOutput()
		require.NoError(t, err, "pgctld init should succeed, output: %s", string(out))

		// -- start --
		startCmd := exec.Command("pgctld", "start", "--pooler-dir", baseDir, "--pg-port", strconv.Itoa(port))
		startCmd.Env = append(baseEnv, extraEnv...)
		out, err = startCmd.CombinedOutput()
		require.NoError(t, err, "pgctld start should succeed, output: %s", string(out))

		defer func() {
			stopCmd := exec.Command("pgctld", "stop", "--pooler-dir", baseDir)
			stopCmd.Env = baseEnv
			_ = stopCmd.Run()
		}()

		// -- verify: targetDB exists in pg_database --
		dbCheckCmd := exec.Command("psql",
			"-h", socketDir,
			"-p", strconv.Itoa(port),
			"-U", "postgres",
			"-d", "postgres",
			"-Atc", fmt.Sprintf("SELECT datname FROM pg_database WHERE datname = '%s'",
				strings.ReplaceAll(targetDB, "'", "''")),
		)
		out, err = dbCheckCmd.CombinedOutput()
		require.NoError(t, err, "pg_database query should succeed, output: %s", string(out))
		assert.Contains(t, strings.TrimSpace(string(out)), targetDB,
			"database %q should appear in pg_database after init", targetDB)

		// -- verify: we can connect directly to targetDB --
		connCheckCmd := exec.Command("psql",
			"-h", socketDir,
			"-p", strconv.Itoa(port),
			"-U", "postgres",
			"-d", targetDB,
			"-Atc", "SELECT current_database();",
		)
		out, err = connCheckCmd.CombinedOutput()
		require.NoError(t, err, "connecting to %q should succeed, output: %s", targetDB, string(out))
		assert.Contains(t, strings.TrimSpace(string(out)), targetDB,
			"current_database() should return %q when connected to it", targetDB)
	}

	t.Run("via_cli_flag", func(t *testing.T) {
		// --pg-database flag explicitly names the custom database.
		runDatabaseCreationTest(t, "mydb", nil, []string{"--pg-database", "mydb"})
	})

	t.Run("via_postgres_db_env_var", func(t *testing.T) {
		// POSTGRES_DB env var — the standard Docker postgres convention.
		runDatabaseCreationTest(t, "mydb", []string{"POSTGRES_DB=mydb"}, nil)
	})

	t.Run("default_database_unchanged", func(t *testing.T) {
		// When the target database is the default "postgres", no transient start
		// should occur and the standard postgres database must still be accessible.
		runDatabaseCreationTest(t, "postgres", nil, nil)
	})
}

// TestPostgreSQLLifecycleIntegration tests the complete PostgreSQL lifecycle using CLI
func TestPostgreSQLLifecycleIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	tempDir, cleanup := testutil.TempDir(t, "pgctld_integration_test")
	defer cleanup()

	dataDir := filepath.Join(tempDir, "data")
	configFile := filepath.Join(tempDir, "postgresql.conf")

	// Create a test configuration file
	err := os.WriteFile(configFile, []byte(`
# Test PostgreSQL configuration
port = 5433
max_connections = 100
shared_buffers = 128MB
log_statement = 'all'
`), 0o644)
	require.NoError(t, err)

	// Create a pgctld config file to avoid config file errors
	pgctldConfigFile := filepath.Join(tempDir, ".pgctld.yaml")
	err = os.WriteFile(pgctldConfigFile, []byte(`
# Test pgctld configuration
log-level: info
timeout: 30
`), 0o644)
	require.NoError(t, err)

	// Setup mock PostgreSQL binaries
	binDir := filepath.Join(tempDir, "bin")
	err = os.MkdirAll(binDir, 0o755)
	require.NoError(t, err)
	testutil.CreateMockPostgreSQLBinaries(t, binDir)

	t.Run("complete_lifecycle_via_cli", func(t *testing.T) {
		// Step 1: Initialize the database first
		initCmd := exec.Command("pgctld", "init", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
		initCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"),
			constants.PgDataDirEnvVar+"="+filepath.Join(dataDir, "pg_data"))
		initOutput, err := initCmd.CombinedOutput()
		if err != nil {
			t.Logf("initCmd.Output() error: %v, output: %s", err, string(initOutput))
		}
		require.NoError(t, err)

		// Step 2: Check status - should be stopped after init
		statusCmd := exec.Command("pgctld", "status", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
		statusCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"),
			constants.PgDataDirEnvVar+"="+filepath.Join(dataDir, "pg_data"))
		output, err := statusCmd.CombinedOutput()
		if err != nil {
			t.Logf("statusCmd.Output() error: %v, output: %s", err, string(output))
		}
		require.NoError(t, err)
		assert.Contains(t, string(output), "Stopped")

		// Step 3: Start PostgreSQL
		startCmd := exec.Command("pgctld", "start", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
		startCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"),
			constants.PgDataDirEnvVar+"="+filepath.Join(dataDir, "pg_data"))
		err = startCmd.Run()
		require.NoError(t, err)

		// Step 4: Check status - should be running
		statusCmd = exec.Command("pgctld", "status", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
		statusCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"),
			constants.PgDataDirEnvVar+"="+filepath.Join(dataDir, "pg_data"))
		output, err = statusCmd.Output()
		require.NoError(t, err)
		assert.Contains(t, string(output), "Running")

		// Step 5: Reload configuration
		reloadCmd := exec.Command("pgctld", "reload-config", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
		reloadCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"),
			constants.PgDataDirEnvVar+"="+filepath.Join(dataDir, "pg_data"))
		err = reloadCmd.Run()
		require.NoError(t, err)

		// Step 6: Restart PostgreSQL
		restartCmd := exec.Command("pgctld", "restart", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
		restartCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"),
			constants.PgDataDirEnvVar+"="+filepath.Join(dataDir, "pg_data"))
		err = restartCmd.Run()
		require.NoError(t, err)

		// Step 7: Check status again - should still be running
		statusCmd = exec.Command("pgctld", "status", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
		statusCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"),
			constants.PgDataDirEnvVar+"="+filepath.Join(dataDir, "pg_data"))
		output, err = statusCmd.Output()
		require.NoError(t, err)
		assert.Contains(t, string(output), "Running")

		// Step 8: Stop PostgreSQL
		stopCmd := exec.Command("pgctld", "stop", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
		stopCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"),
			constants.PgDataDirEnvVar+"="+filepath.Join(dataDir, "pg_data"))
		err = stopCmd.Run()
		require.NoError(t, err)

		// Step 9: Final status check - should be stopped
		statusCmd = exec.Command("pgctld", "status", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
		statusCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"),
			constants.PgDataDirEnvVar+"="+filepath.Join(dataDir, "pg_data"))
		output, err = statusCmd.Output()
		require.NoError(t, err)
		assert.Contains(t, string(output), "Stopped")
	})
}

// TestMultipleStartStopCycles tests multiple start/stop cycles
func TestMultipleStartStopCycles(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	tempDir, cleanup := testutil.TempDir(t, "pgctld_cycles_test")
	defer cleanup()

	dataDir := filepath.Join(tempDir, "data")

	// Create a pgctld config file to avoid config file errors
	pgctldConfigFile := filepath.Join(tempDir, ".pgctld.yaml")
	err := os.WriteFile(pgctldConfigFile, []byte(`
# Test pgctld configuration
log-level: info
timeout: 30
`), 0o644)
	require.NoError(t, err)

	// Setup mock PostgreSQL binaries
	binDir := filepath.Join(tempDir, "bin")
	err = os.MkdirAll(binDir, 0o755)
	require.NoError(t, err)
	testutil.CreateMockPostgreSQLBinaries(t, binDir)

	// Initialize database first
	initCmd := exec.Command("pgctld", "init", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
	initCmd.Env = append(os.Environ(),
		"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"),
		constants.PgDataDirEnvVar+"="+filepath.Join(dataDir, "pg_data"))
	err = initCmd.Run()
	require.NoError(t, err)

	// Start PostgreSQL for the first time
	startCmd := exec.Command("pgctld", "start", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
	startCmd.Env = append(os.Environ(),
		"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"),
		constants.PgDataDirEnvVar+"="+filepath.Join(dataDir, "pg_data"))
	err = startCmd.Run()
	require.NoError(t, err)

	// Stop initial start
	stopCmd := exec.Command("pgctld", "stop", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
	stopCmd.Env = append(os.Environ(),
		"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"),
		constants.PgDataDirEnvVar+"="+filepath.Join(dataDir, "pg_data"))
	err = stopCmd.Run()
	require.NoError(t, err)

	// Test multiple start/stop cycles
	for i := range 3 {
		t.Run(fmt.Sprintf("cycle_%d", i+1), func(t *testing.T) {
			// Start PostgreSQL
			startCmd := exec.Command("pgctld", "start", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
			startCmd.Env = append(os.Environ(),
				"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"),
				constants.PgDataDirEnvVar+"="+filepath.Join(dataDir, "pg_data"))
			err := startCmd.Run()
			require.NoError(t, err)

			// Verify running
			statusCmd := exec.Command("pgctld", "status", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
			statusCmd.Env = append(os.Environ(),
				"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"),
				constants.PgDataDirEnvVar+"="+filepath.Join(dataDir, "pg_data"))
			output, err := statusCmd.Output()
			require.NoError(t, err)
			assert.Contains(t, string(output), "Running")

			// Stop PostgreSQL
			stopCmd := exec.Command("pgctld", "stop", "--pooler-dir", dataDir, "--mode", "fast", "--config-file", pgctldConfigFile)
			stopCmd.Env = append(os.Environ(),
				"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"),
				constants.PgDataDirEnvVar+"="+filepath.Join(dataDir, "pg_data"))
			err = stopCmd.Run()
			require.NoError(t, err)

			// Verify stopped
			statusCmd = exec.Command("pgctld", "status", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
			statusCmd.Env = append(os.Environ(),
				"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"),
				constants.PgDataDirEnvVar+"="+filepath.Join(dataDir, "pg_data"))
			output, err = statusCmd.Output()
			require.NoError(t, err)
			assert.Contains(t, string(output), "Stopped")
		})
	}
}

// TestConfigurationChanges tests configuration reload functionality
func TestConfigurationChanges(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	tempDir, cleanup := testutil.TempDir(t, "pgctld_config_test")
	defer cleanup()

	dataDir := filepath.Join(tempDir, "data")
	configFile := filepath.Join(dataDir, "postgresql.conf")

	// Create a pgctld config file to avoid config file errors
	pgctldConfigFile := filepath.Join(tempDir, ".pgctld.yaml")
	err := os.WriteFile(pgctldConfigFile, []byte(`
# Test pgctld configuration
log-level: info
timeout: 30
`), 0o644)
	require.NoError(t, err)

	// Setup mock PostgreSQL binaries
	binDir := filepath.Join(tempDir, "bin")
	err = os.MkdirAll(binDir, 0o755)
	require.NoError(t, err)
	testutil.CreateMockPostgreSQLBinaries(t, binDir)

	// Initialize database first
	initCmd := exec.Command("pgctld", "init", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
	initCmd.Env = append(os.Environ(),
		"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"),
		constants.PgDataDirEnvVar+"="+filepath.Join(dataDir, "pg_data"))
	err = initCmd.Run()
	require.NoError(t, err)

	// Start PostgreSQL
	startCmd := exec.Command("pgctld", "start", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
	startCmd.Env = append(os.Environ(),
		"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"),
		constants.PgDataDirEnvVar+"="+filepath.Join(dataDir, "pg_data"))
	err = startCmd.Run()
	require.NoError(t, err)

	defer func() {
		stopCmd := exec.Command("pgctld", "stop", "--pooler-dir", dataDir, "--mode", "fast", "--config-file", pgctldConfigFile)
		stopCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"),
			constants.PgDataDirEnvVar+"="+filepath.Join(dataDir, "pg_data"))
		_ = stopCmd.Run()
	}()

	t.Run("reload_configuration", func(t *testing.T) {
		// Update configuration file
		err := os.WriteFile(configFile, []byte(`
# Updated configuration
max_connections = 200
shared_buffers = 256MB
log_min_messages = info
`), 0o644)
		require.NoError(t, err)

		// Reload configuration
		reloadCmd := exec.Command("pgctld", "reload-config", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
		reloadCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"),
			constants.PgDataDirEnvVar+"="+filepath.Join(dataDir, "pg_data"))
		err = reloadCmd.Run()
		require.NoError(t, err)

		// Server should still be running
		statusCmd := exec.Command("pgctld", "status", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
		statusCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"),
			constants.PgDataDirEnvVar+"="+filepath.Join(dataDir, "pg_data"))
		output, err := statusCmd.Output()
		require.NoError(t, err)
		assert.Contains(t, string(output), "Running")
	})
}

// TestErrorRecovery tests recovery from various error states
func TestErrorRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	tempDir, cleanup := testutil.TempDir(t, "pgctld_recovery_test")
	defer cleanup()

	dataDir := filepath.Join(tempDir, "data")

	// Create a pgctld config file to avoid config file errors
	pgctldConfigFile := filepath.Join(tempDir, ".pgctld.yaml")
	err := os.WriteFile(pgctldConfigFile, []byte(`
# Test pgctld configuration
log-level: info
timeout: 30
`), 0o644)
	require.NoError(t, err)

	// Setup mock PostgreSQL binaries
	binDir := filepath.Join(tempDir, "bin")
	err = os.MkdirAll(binDir, 0o755)
	require.NoError(t, err)
	testutil.CreateMockPostgreSQLBinaries(t, binDir)

	t.Run("start_with_nonexistent_data_dir", func(t *testing.T) {
		nonexistentDir := filepath.Join(tempDir, "nonexistent")

		// Try to start with non-existent directory - should fail requiring init first
		startCmd := exec.Command("pgctld", "start", "--pooler-dir", nonexistentDir, "--config-file", pgctldConfigFile)
		startCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"),
			constants.PgDataDirEnvVar+"="+filepath.Join(nonexistentDir, "pg_data"))
		err := startCmd.Run()
		require.Error(t, err, "Start should fail when data directory is not initialized")

		// Initialize first, then start should work
		initCmd := exec.Command("pgctld", "init", "--pooler-dir", nonexistentDir, "--config-file", pgctldConfigFile)
		initCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"),
			constants.PgDataDirEnvVar+"="+filepath.Join(nonexistentDir, "pg_data"))
		err = initCmd.Run()
		require.NoError(t, err)

		// Now start should work
		startCmd = exec.Command("pgctld", "start", "--pooler-dir", nonexistentDir, "--config-file", pgctldConfigFile)
		startCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"),
			constants.PgDataDirEnvVar+"="+filepath.Join(nonexistentDir, "pg_data"))
		err = startCmd.Run()
		require.NoError(t, err)

		// Clean stop
		stopCmd := exec.Command("pgctld", "stop", "--pooler-dir", nonexistentDir, "--mode", "immediate", "--config-file", pgctldConfigFile)
		stopCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"),
			constants.PgDataDirEnvVar+"="+filepath.Join(nonexistentDir, "pg_data"))
		_ = stopCmd.Run()
	})

	t.Run("double_start_attempt", func(t *testing.T) {
		// Initialize data directory first
		initCmd := exec.Command("pgctld", "init", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
		initCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"),
			constants.PgDataDirEnvVar+"="+filepath.Join(dataDir, "pg_data"))
		err := initCmd.Run()
		require.NoError(t, err)

		// Start PostgreSQL
		startCmd := exec.Command("pgctld", "start", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
		startCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"),
			constants.PgDataDirEnvVar+"="+filepath.Join(dataDir, "pg_data"))
		err = startCmd.Run()
		require.NoError(t, err)

		// Try to start again - should handle gracefully
		startCmd2 := exec.Command("pgctld", "start", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
		startCmd2.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"),
			constants.PgDataDirEnvVar+"="+filepath.Join(dataDir, "pg_data"))
		output, err := startCmd2.CombinedOutput()
		// Should either succeed or fail gracefully with appropriate message
		if err != nil {
			assert.Contains(t, strings.ToLower(string(output)), "already")
		}

		// Clean stop
		stopCmd := exec.Command("pgctld", "stop", "--pooler-dir", dataDir, "--mode", "immediate", "--config-file", pgctldConfigFile)
		stopCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"),
			constants.PgDataDirEnvVar+"="+filepath.Join(dataDir, "pg_data"))
		_ = stopCmd.Run()
	})
}

// TestOrphanDetectionWithRealPostgreSQL tests that orphan detection stops PostgreSQL
// when the parent pgctld server process dies unexpectedly
func TestOrphanDetectionWithRealPostgreSQL(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping orphan detection tests in short mode")
	}

	if !utils.HasPostgreSQLBinaries() {
		t.Fatal("PostgreSQL binaries not found")
	}

	tempDir, cleanup := testutil.TempDir(t, "pgctld_orphan_test")
	defer cleanup()

	dataDir := filepath.Join(tempDir, "data")
	pgctldConfigFile := filepath.Join(tempDir, ".pgctld.yaml")
	err := os.WriteFile(pgctldConfigFile, []byte("log-level: info\ntimeout: 30\n"), 0o644)
	require.NoError(t, err)

	// Add the endtoend directory to PATH so run_command_if_parent_dies.sh can be found.
	endtoendDir, err := filepath.Abs(".")
	require.NoError(t, err)

	// Initialize data directory
	initCmd := executil.Command(t.Context(), "pgctld", "init", "--pooler-dir", dataDir, "--pg-port", strconv.Itoa(utils.GetFreePort(t)), "--config-file", pgctldConfigFile)
	setupTestEnv(initCmd, dataDir)
	require.NoError(t, initCmd.Run())

	// Start pgctld server subprocess with orphan detection enabled.
	// Pass the extended PATH so the orphan-detection helper script is found.
	srv := startPgCtldServer(t, dataDir, pgctldConfigFile,
		"PATH="+endtoendDir+":"+os.Getenv("PATH"))

	// Start postgres via gRPC (this will use server's orphan detection setting)
	grpcAddr := fmt.Sprintf("localhost:%d", srv.GrpcPort)
	err = InitAndStartPostgreSQL(t, grpcAddr)
	require.NoError(t, err)

	// Wait for postgres PID
	var pgPID int
	require.Eventually(t, func() bool {
		pid, err := readPostmasterPID(filepath.Join(dataDir, "pg_data"))
		if err == nil {
			pgPID = pid
			return true
		}
		return false
	}, 10*time.Second, 100*time.Millisecond)

	// Verify postgres is running
	pgProcess, err := os.FindProcess(pgPID)
	require.NoError(t, err)
	require.NoError(t, pgProcess.Signal(syscall.Signal(0)))

	// Kill the pgctld server subprocess abruptly
	_, killed := srv.Cmd.Kill(utils.WithShortDeadline(t))
	require.True(t, killed, "pgctld server should have been killed within deadline")

	// TODO(dweitzman): Start a process using sleep command and use that PID for orphan detection

	// Delete the temp directory, triggering orphan detection
	os.RemoveAll(tempDir)

	// Wait for orphan detection to stop postgres
	time.Sleep(2 * time.Second)

	// Verify postgres is stopped
	require.Eventually(t, func() bool {
		err = pgProcess.Signal(syscall.Signal(0))
		return err == nil
	}, 5*time.Second, 100*time.Millisecond)
}

func readPostmasterPID(dataDir string) (int, error) {
	pidFile := filepath.Join(dataDir, "postmaster.pid")
	content, err := os.ReadFile(pidFile)
	if err != nil {
		return 0, err
	}

	lines := strings.Split(string(content), "\n")
	if len(lines) == 0 {
		return 0, errors.New("empty postmaster.pid file")
	}

	pid, err := strconv.Atoi(strings.TrimSpace(lines[0]))
	if err != nil {
		return 0, fmt.Errorf("invalid PID in postmaster.pid: %s", lines[0])
	}

	return pid, nil
}

// TestPgRewind_AfterCrash tests that PgRewind RPC automatically handles crash recovery
// when PostgreSQL was killed ungracefully (SIGKILL) and left in an unclean state
func TestPgRewind_AfterCrash(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	if !utils.HasPostgreSQLBinaries() {
		t.Skip("PostgreSQL binaries not found, skipping test")
	}

	tempDir, cleanup := testutil.TempDir(t, "pgctld_pgrewind_crash_test")
	defer cleanup()

	// Set up two PostgreSQL instances via pgctld:
	// - Primary: source server for pg_rewind
	// - Standby: the server we'll crash and then rewind

	// ===== PRIMARY SETUP =====
	primaryDir := filepath.Join(tempDir, "primary")
	primaryConfigFile := filepath.Join(tempDir, "primary.pgctld.yaml")
	err := os.WriteFile(primaryConfigFile, []byte("log-level: info\ntimeout: 30\n"), 0o644)
	require.NoError(t, err)

	testPassword := "test_pg_rewind_password_123" //nolint:gosec // Test password, not a real credential
	err = os.MkdirAll(primaryDir, 0o755)
	require.NoError(t, err)

	primary := startPgCtldServer(t, primaryDir, primaryConfigFile,
		"POSTGRES_PASSWORD="+testPassword)
	t.Logf("Primary ports - gRPC: %d, PostgreSQL: %d", primary.GrpcPort, primary.PgPort)

	// Initialize and start primary PostgreSQL
	primaryGrpcAddr := fmt.Sprintf("localhost:%d", primary.GrpcPort)
	err = InitAndStartPostgreSQL(t, primaryGrpcAddr)
	require.NoError(t, err)
	t.Log("Primary PostgreSQL started successfully")

	// ===== STANDBY SETUP =====
	standbyDir := filepath.Join(tempDir, "standby")
	standbyConfigFile := filepath.Join(tempDir, "standby.pgctld.yaml")
	err = os.WriteFile(standbyConfigFile, []byte("log-level: info\ntimeout: 30\n"), 0o644)
	require.NoError(t, err)

	err = os.MkdirAll(standbyDir, 0o755)
	require.NoError(t, err)

	standby := startPgCtldServer(t, standbyDir, standbyConfigFile,
		"POSTGRES_PASSWORD="+testPassword)
	t.Logf("Standby ports - gRPC: %d, PostgreSQL: %d", standby.GrpcPort, standby.PgPort)

	// Initialize and start standby PostgreSQL
	standbyGrpcAddr := fmt.Sprintf("localhost:%d", standby.GrpcPort)
	err = InitAndStartPostgreSQL(t, standbyGrpcAddr)
	require.NoError(t, err)
	t.Log("Standby PostgreSQL started successfully")

	// ===== CRASH STANDBY =====
	// Get standby PostgreSQL PID
	standbyPgDataDir := filepath.Join(standbyDir, "pg_data")
	standbyPgPID, err := readPostmasterPID(standbyPgDataDir)
	require.NoError(t, err)
	t.Logf("Standby PostgreSQL PID: %d", standbyPgPID)

	// Kill standby PostgreSQL with SIGKILL to simulate crash
	t.Log("Killing standby PostgreSQL with SIGKILL to simulate crash")
	killCtx, killCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer killCancel()
	standbyPgProcess, err := os.FindProcess(standbyPgPID)
	require.NoError(t, err)
	_, _ = executil.KillPID(killCtx, standbyPgPID)

	// Wait for process to terminate (poll instead of sleep)
	require.Eventually(t, func() bool {
		// Signal 0 checks if process exists without actually sending a signal
		err := standbyPgProcess.Signal(syscall.Signal(0))
		// On macOS/Linux, if the process is gone, we get "no such process" error
		return err != nil
	}, 5*time.Second, 100*time.Millisecond, "Standby postgres should terminate after SIGKILL")

	// Verify standby is in an unclean state (not a clean shutdown) so crash recovery is needed.
	// Since postgres always starts as standby, the cluster state after SIGKILL is
	// "in archive recovery" rather than "in production".
	t.Log("Verifying standby is in an unclean state after crash")
	controldataCmd := exec.Command("pg_controldata", standbyPgDataDir)
	output, err := controldataCmd.CombinedOutput()
	require.NoError(t, err)
	outputStr := string(output)
	assert.Contains(t, outputStr, "Database cluster state:", "Should show cluster state")
	clusterState := command.ExtractClusterState(outputStr)
	assert.False(t, command.IsCleanClusterState(clusterState),
		"Cluster state should be unclean after SIGKILL (crash recovery needed), got: %q", clusterState)
	t.Logf("Confirmed: standby is in state %q after SIGKILL (unclean, crash recovery needed)", clusterState)

	// ===== CALL PGREWIND RPC =====
	// Connect to standby gRPC and call PgRewind with dry_run=true
	// This should automatically run crash recovery before attempting pg_rewind
	standbyConn, err := grpc.NewClient(
		standbyGrpcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer standbyConn.Close()

	standbyClient := pb.NewPgCtldClient(standbyConn)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	t.Log("Calling PgRewind RPC with dry_run=true (should trigger automatic crash recovery)")
	rewindResp, err := standbyClient.PgRewind(ctx, &pb.PgRewindRequest{
		SourceHost: "localhost",
		SourcePort: int32(primary.PgPort),
		DryRun:     true,
	})
	if err != nil {
		t.Logf("PgRewind RPC returned error: %v", err)
		// Even if pg_rewind failed (e.g., connection issues), crash recovery should have run
		// We'll verify this by checking the database state below
	} else {
		t.Logf("PgRewind RPC succeeded: %s", rewindResp.Message)
		t.Logf("PgRewind output: %s", rewindResp.Output)
	}

	// ===== VERIFY CRASH RECOVERY SUCCEEDED =====
	// After PgRewind RPC, crash recovery should have run automatically,
	// leaving the database in "shut down" or "shut down in recovery" state
	t.Log("Verifying standby is now cleanly stopped after crash recovery")
	controldataCmd = exec.Command("pg_controldata", standbyPgDataDir)
	output, err = controldataCmd.CombinedOutput()
	require.NoError(t, err)
	outputStr = string(output)

	assert.Contains(t, outputStr, "Database cluster state:", "Should show cluster state")
	// After crash recovery, should be cleanly stopped
	isCleanlyStoppedState := strings.Contains(outputStr, "shut down") || strings.Contains(outputStr, "shut down in recovery")
	assert.True(t, isCleanlyStoppedState,
		"Database should be in 'shut down' or 'shut down in recovery' state after crash recovery, got:\n%s", outputStr)
	t.Log("Success: standby is now cleanly stopped, proving crash recovery succeeded")
}
