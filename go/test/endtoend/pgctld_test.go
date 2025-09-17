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

package endtoend

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	"github.com/multigres/multigres/go/pgctld"
)

var (
	// cachedPgctldBinary holds the cached pgctld binary path
	cachedPgctldBinary string
	// pgctldBuildOnce ensures pgctld binary is built only once
	pgctldBuildOnce sync.Once
	// pgctldBuildError holds any error from building the pgctld binary
	pgctldBuildError error
)

// getCachedPgctldBinary builds the pgctld binary once and returns the cached path
func getCachedPgctldBinary(t *testing.T) string {
	t.Helper()
	pgctldBuildOnce.Do(func() {
		// Create a temporary directory for the cached binary
		tempDir, err := os.MkdirTemp("", "pgctld_cache_*")
		if err != nil {
			pgctldBuildError = fmt.Errorf("failed to create temp dir: %w", err)
			return
		}

		cachedPgctldBinary = filepath.Join(tempDir, "pgctld")
		buildCmd := exec.Command("go", "build", "-o", cachedPgctldBinary, "../../cmd/pgctld")
		buildOutput, err := buildCmd.CombinedOutput()
		if err != nil {
			pgctldBuildError = fmt.Errorf("failed to build pgctld binary: %v\nOutput: %s", err, string(buildOutput))
			return
		}
		t.Logf("Built cached pgctld binary at: %s", cachedPgctldBinary)
	})

	if pgctldBuildError != nil {
		t.Fatalf("Failed to build pgctld binary: %v", pgctldBuildError)
	}

	return cachedPgctldBinary
}

// setupTestEnv sets up environment variables for PostgreSQL tests
func setupTestEnv(cmd *exec.Cmd) {
	cmd.Env = append(os.Environ(),
		"PGCONNECT_TIMEOUT=5", // Shorter timeout for tests
	)
}

// TestEndToEndWithRealPostgreSQL tests pgctld with real PostgreSQL binaries
// This test requires PostgreSQL to be installed on the system
func TestEndToEndWithRealPostgreSQL(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}

	// Check if PostgreSQL binaries are available
	if !hasPostgreSQLBinaries() {
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

	// Use cached pgctld binary for testing
	pgctldBinary := getCachedPgctldBinary(t)

	t.Run("basic_commands_with_real_postgresql", func(t *testing.T) {
		// Step 1: Initialize the database first
		initCmd := exec.Command(pgctldBinary, "init", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
		setupTestEnv(initCmd)
		initOutput, err := initCmd.CombinedOutput()
		if err != nil {
			t.Logf("pgctld init failed with output: %s", string(initOutput))
		}
		require.NoError(t, err)

		// Step 2: Check status - should show stopped after init
		statusCmd := exec.Command(pgctldBinary, "status", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
		setupTestEnv(statusCmd)
		output, err := statusCmd.CombinedOutput()
		if err != nil {
			t.Logf("pgctld status failed with output: %s", string(output))
		}
		require.NoError(t, err)
		assert.Contains(t, string(output), "Stopped")

		// Step 3: Test help commands work
		helpCmd := exec.Command(pgctldBinary, "--help")
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
	if !hasPostgreSQLBinaries() {
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

	// Use cached pgctld binary for testing
	pgctldBinary := getCachedPgctldBinary(t)

	t.Run("grpc_server_with_real_postgresql", func(t *testing.T) {
		// Generate random ports for this test
		grpcPort := testutil.GenerateRandomPort()
		pgPort := testutil.GenerateRandomPort()
		t.Logf("gRPC test using ports - gRPC: %d, PostgreSQL: %d", grpcPort, pgPort)

		// Start gRPC server in background
		serverCmd := exec.Command(pgctldBinary, "server",
			"--pooler-dir", dataDir,
			"--grpc-port", strconv.Itoa(grpcPort),
			"--pg-port", strconv.Itoa(pgPort),
			"--config-file", pgctldConfigFile)
		setupTestEnv(serverCmd) // Use system PATH for real PostgreSQL binaries, trust auth for tests

		err := serverCmd.Start()
		require.NoError(t, err)
		defer func() {
			if serverCmd.Process != nil {
				_ = serverCmd.Process.Kill()
				_ = serverCmd.Wait()
			}
		}()

		deadline := time.Now().Add(20 * time.Second)
		serverStarted := false

		for time.Now().Before(deadline) {
			// Check if server process is still running (not crashed)
			if serverCmd.Process != nil {
				// Check if process is still alive by checking if ProcessState is nil
				// If the process has exited, ProcessState will be non-nil
				require.Nil(t, serverCmd.ProcessState, "gRPC server process died: exit code %d", serverCmd.ProcessState.ExitCode())

				// Test basic gRPC connectivity by checking if the server is listening
				// Try to connect to the gRPC port to verify it's listening
				conn, err := net.DialTimeout("tcp", fmt.Sprintf("localhost:%d", grpcPort), 100*time.Millisecond)
				if err == nil {
					conn.Close()
					serverStarted = true
					break
				}
			}
			time.Sleep(100 * time.Millisecond)
		}

		require.True(t, serverStarted, "timeout: gRPC server failed to start listening")
	})
}

// TestEndToEndPerformance tests performance characteristics
func TestEndToEndPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance tests in short mode")
	}

	// Check if PostgreSQL binaries are available
	if !hasPostgreSQLBinaries() {
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
	pgctldBinary := getCachedPgctldBinary(t)

	t.Run("startup_performance", func(t *testing.T) {
		// Generate random port for this test
		perfTestPort := testutil.GenerateRandomPort()
		t.Logf("Performance test using port: %d", perfTestPort)

		// Measure time to start PostgreSQL
		startTime := time.Now()
		initCmd := exec.Command(pgctldBinary, "init", "--pooler-dir", dataDir, "--pg-port", strconv.Itoa(perfTestPort), "--config-file", pgctldConfigFile)
		setupTestEnv(initCmd)
		startOutput, err := initCmd.CombinedOutput()
		if err != nil {
			t.Logf("pgctld start failed with output: %s", string(startOutput))
		}

		require.NoError(t, err)

		startCmd := exec.Command(pgctldBinary, "start", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
		setupTestEnv(startCmd)
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
		stopCmd := exec.Command(pgctldBinary, "stop", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
		setupTestEnv(stopCmd)
		err = stopCmd.Run()
		require.NoError(t, err)
	})

	t.Run("multiple_rapid_operations", func(t *testing.T) {
		// Test rapid start/stop cycles
		for i := range 3 {
			t.Logf("Cycle %d", i+1)

			// Start
			startCmd := exec.Command(pgctldBinary, "start", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
			setupTestEnv(startCmd)
			startOutput, err := startCmd.CombinedOutput()
			if err != nil {
				t.Logf("pgctld start failed with error: %v, output: %s", err, string(startOutput))
			}
			require.NoError(t, err)

			// Brief wait
			time.Sleep(1 * time.Second)

			// Stop
			stopCmd := exec.Command(pgctldBinary, "stop", "--pooler-dir", dataDir, "--mode", "fast", "--config-file", pgctldConfigFile)
			setupTestEnv(stopCmd)
			err = stopCmd.Run()
			require.NoError(t, err)

			// Brief wait before next cycle
			time.Sleep(500 * time.Millisecond)
		}
	})
}

// hasPostgreSQLBinaries checks if PostgreSQL binaries are available in PATH
func hasPostgreSQLBinaries() bool {
	requiredBinaries := []string{"initdb", "postgres", "pg_ctl", "pg_isready"}

	for _, binary := range requiredBinaries {
		_, err := exec.LookPath(binary)
		if err != nil {
			return false
		}
	}

	return true
}

// TestEndToEndSystemIntegration tests integration with system PostgreSQL
func TestEndToEndSystemIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping system integration tests in short mode")
	}

	// Check if PostgreSQL binaries are available
	if !hasPostgreSQLBinaries() {
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
	pgctldBinary := getCachedPgctldBinary(t)

	t.Run("version_compatibility", func(t *testing.T) {
		// Generate random port for this test
		testPort := testutil.GenerateRandomPort()
		t.Logf("Using port: %d", testPort)

		// Check PostgreSQL version compatibility
		versionCmd := exec.Command("postgres", "--version")
		output, err := versionCmd.Output()
		require.NoError(t, err)
		t.Logf("PostgreSQL version: %s", string(output))

		initCmd := exec.Command(pgctldBinary, "init", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile, "--pg-port", strconv.Itoa(testPort))
		setupTestEnv(initCmd)
		initOutput, err := initCmd.CombinedOutput()
		if err != nil {
			t.Logf("pgctld init failed with output: %s", string(initOutput))
		}
		require.NoError(t, err)

		// Start PostgreSQL to test compatibility
		startCmd := exec.Command(pgctldBinary, "start", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
		setupTestEnv(startCmd)
		startOutput, err := startCmd.CombinedOutput()
		if err != nil {
			t.Logf("pgctld start failed with output: %s", string(startOutput))
		}
		require.NoError(t, err)

		// Get version info through pgctld
		statusCmd := exec.Command(pgctldBinary, "status", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
		setupTestEnv(statusCmd)
		output, err = statusCmd.Output()
		require.NoError(t, err)
		t.Logf("pgctld status output: %s", string(output))

		// Clean shutdown
		stopCmd := exec.Command(pgctldBinary, "stop", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
		setupTestEnv(stopCmd)
		err = stopCmd.Run()
		require.NoError(t, err)
	})
}

// TestPostgreSQLAuthentication tests PostgreSQL authentication with PGPASSWORD
func TestPostgreSQLAuthentication(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping authentication tests in short mode")
	}

	// Check if PostgreSQL binaries are available
	if !hasPostgreSQLBinaries() {
		t.Fatal("PostgreSQL binaries not found, skipping authentication test")
	}

	t.Run("pgpassword_authentication", func(t *testing.T) {
		// Set up temporary directory
		baseDir, cleanup := testutil.TempDir(t, "pgctld_auth_test")
		defer cleanup()

		t.Logf("Base directory: %s", baseDir)
		t.Logf("Base directory is absolute: %v", filepath.IsAbs(baseDir))

		// Use cached pgctld binary for testing
		pgctldBinary := getCachedPgctldBinary(t)

		// Get available port for PostgreSQL
		port := testutil.GenerateRandomPort()
		t.Logf("Authentication test using port: %d", port)

		// Test password
		testPassword := "secure_test_password_123"

		// Initialize with PGPASSWORD
		t.Logf("Initializing PostgreSQL with PGPASSWORD")
		initCmd := exec.Command(pgctldBinary, "init", "--pooler-dir", baseDir, "--pg-port", strconv.Itoa(port))

		initCmd.Env = append(os.Environ(),
			"PGCONNECT_TIMEOUT=5",
			fmt.Sprintf("PGPASSWORD=%s", testPassword),
		)
		output, err := initCmd.CombinedOutput()
		require.NoError(t, err, "pgctld init should succeed, output: %s", string(output))
		assert.Contains(t, string(output), "password_source=\"PGPASSWORD environment variable\"", "Should use PGPASSWORD")

		// Start the PostgreSQL server
		t.Logf("Starting PostgreSQL server")
		startCmd := exec.Command(pgctldBinary, "start", "--pooler-dir", baseDir)
		startCmd.Env = append(os.Environ(),
			"PGCONNECT_TIMEOUT=5",
			fmt.Sprintf("PGPASSWORD=%s", testPassword),
		)
		output, err = startCmd.CombinedOutput()
		require.NoError(t, err, "pgctld start should succeed, output: %s", string(output))

		// Give the server a moment to be fully ready
		time.Sleep(2 * time.Second)

		// Test socket connection (should work without password)
		t.Logf("Testing Unix socket connection (no password required)")
		socketDir := pgctld.PostgresSocketDir(baseDir)
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
		statusCmd := exec.Command(pgctldBinary, "status", "--pooler-dir", baseDir)
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
		tcpCmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", testPassword))
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
		stopCmd := exec.Command(pgctldBinary, "stop", "--pooler-dir", baseDir)
		stopCmd.Env = append(os.Environ(), "PGCONNECT_TIMEOUT=5")
		err = stopCmd.Run()
		require.NoError(t, err, "pgctld stop should succeed")
	})

	t.Run("password_file_authentication", func(t *testing.T) {
		// Set up temporary directory
		baseDir, cleanup := testutil.TempDir(t, "pgctld_pwfile_test")
		defer cleanup()

		// Use cached pgctld binary for testing
		pgctldBinary := getCachedPgctldBinary(t)

		// Get available port for PostgreSQL
		port := testutil.GenerateRandomPort()
		t.Logf("Password file test using port: %d", port)

		// Test password
		testPassword := "file_password_secure_456"

		// Create password file
		pwfile := filepath.Join(baseDir, "password.txt")
		err := os.WriteFile(pwfile, []byte(testPassword), 0o600)
		require.NoError(t, err, "Should create password file")

		// Initialize with password file
		t.Logf("Initializing PostgreSQL with password file")
		initCmd := exec.Command(pgctldBinary, "init", "--pooler-dir", baseDir, "--pg-pwfile", pwfile, "--pg-port", strconv.Itoa(port))
		initCmd.Env = append(os.Environ(), "PGCONNECT_TIMEOUT=5")
		output, err := initCmd.CombinedOutput()
		require.NoError(t, err, "pgctld init should succeed, output: %s", string(output))
		assert.Contains(t, string(output), "password_source=\"password file\"", "Should use password file")

		// Start the PostgreSQL server
		t.Logf("Starting PostgreSQL server")
		startCmd := exec.Command(pgctldBinary, "start", "--pooler-dir", baseDir, "--pg-pwfile", pwfile)
		startCmd.Env = append(os.Environ(), "PGCONNECT_TIMEOUT=5")
		output, err = startCmd.CombinedOutput()
		require.NoError(t, err, "pgctld start should succeed, output: %s", string(output))

		// Give the server a moment to be fully ready
		time.Sleep(2 * time.Second)

		// Test TCP connection with password from file
		t.Logf("Testing TCP connection with password from file")
		tcpCmd := exec.Command("psql",
			"-h", "localhost",
			"-p", strconv.Itoa(port),
			"-U", "postgres",
			"-d", "postgres",
			"-c", "SELECT 'Password file authentication works!' as result;",
		)
		tcpCmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", testPassword))
		output, err = tcpCmd.CombinedOutput()
		require.NoError(t, err, "TCP connection with password from file should succeed, output: %s", string(output))
		assert.Contains(t, string(output), "Password file authentication works!", "Should connect successfully")

		// Clean shutdown
		t.Logf("Shutting down PostgreSQL")
		stopCmd := exec.Command(pgctldBinary, "stop", "--pooler-dir", baseDir, "--pg-pwfile", pwfile)
		stopCmd.Env = append(os.Environ(), "PGCONNECT_TIMEOUT=5")
		err = stopCmd.Run()
		require.NoError(t, err, "pgctld stop should succeed")
	})

	t.Run("password_source_conflict", func(t *testing.T) {
		// Set up temporary directory
		baseDir, cleanup := testutil.TempDir(t, "pgctld_conflict_test")
		defer cleanup()

		// Use cached pgctld binary for testing
		pgctldBinary := getCachedPgctldBinary(t)

		// Create password file
		pwfile := filepath.Join(baseDir, "password.txt")
		err := os.WriteFile(pwfile, []byte("file_password"), 0o600)
		require.NoError(t, err, "Should create password file")

		// Try to initialize with both PGPASSWORD and password file (should fail)
		t.Logf("Testing conflict between PGPASSWORD and password file")
		initCmd := exec.Command(pgctldBinary, "init", "--pooler-dir", baseDir, "--pg-pwfile", pwfile)
		initCmd.Env = append(os.Environ(),
			"PGCONNECT_TIMEOUT=5",
			"PGPASSWORD=env_password",
		)
		output, err := initCmd.CombinedOutput()
		assert.Error(t, err, "pgctld init should fail with both password sources")
		assert.Contains(t, string(output), "both --pg-pwfile flag and PGPASSWORD environment variable are set", "Should show conflict error")
	})
}

// TestPostgreSQLLifecycleIntegration tests the complete PostgreSQL lifecycle using CLI
func TestPostgreSQLLifecycleIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	pgctldBinary := getCachedPgctldBinary(t)
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
		initCmd := exec.Command(pgctldBinary, "init", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
		initCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
		initOutput, err := initCmd.CombinedOutput()
		if err != nil {
			t.Logf("initCmd.Output() error: %v, output: %s", err, string(initOutput))
		}
		require.NoError(t, err)

		// Step 2: Check status - should be stopped after init
		statusCmd := exec.Command(pgctldBinary, "status", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
		statusCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
		output, err := statusCmd.CombinedOutput()
		if err != nil {
			t.Logf("statusCmd.Output() error: %v, output: %s", err, string(output))
		}
		require.NoError(t, err)
		assert.Contains(t, string(output), "Stopped")

		// Step 3: Start PostgreSQL
		startCmd := exec.Command(pgctldBinary, "start", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
		startCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
		err = startCmd.Run()
		require.NoError(t, err)

		// Step 4: Check status - should be running
		statusCmd = exec.Command(pgctldBinary, "status", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
		statusCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
		output, err = statusCmd.Output()
		require.NoError(t, err)
		assert.Contains(t, string(output), "Running")

		// Step 5: Reload configuration
		reloadCmd := exec.Command(pgctldBinary, "reload-config", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
		reloadCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
		err = reloadCmd.Run()
		require.NoError(t, err)

		// Step 6: Restart PostgreSQL
		restartCmd := exec.Command(pgctldBinary, "restart", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
		restartCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
		err = restartCmd.Run()
		require.NoError(t, err)

		// Step 7: Check status again - should still be running
		statusCmd = exec.Command(pgctldBinary, "status", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
		statusCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
		output, err = statusCmd.Output()
		require.NoError(t, err)
		assert.Contains(t, string(output), "Running")

		// Step 8: Stop PostgreSQL
		stopCmd := exec.Command(pgctldBinary, "stop", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
		stopCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
		err = stopCmd.Run()
		require.NoError(t, err)

		// Step 9: Final status check - should be stopped
		statusCmd = exec.Command(pgctldBinary, "status", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
		statusCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
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

	pgctldBinary := getCachedPgctldBinary(t)
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
	initCmd := exec.Command(pgctldBinary, "init", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
	initCmd.Env = append(os.Environ(),
		"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
	err = initCmd.Run()
	require.NoError(t, err)

	// Start PostgreSQL for the first time
	startCmd := exec.Command(pgctldBinary, "start", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
	startCmd.Env = append(os.Environ(),
		"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
	err = startCmd.Run()
	require.NoError(t, err)

	// Stop initial start
	stopCmd := exec.Command(pgctldBinary, "stop", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
	stopCmd.Env = append(os.Environ(),
		"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
	err = stopCmd.Run()
	require.NoError(t, err)

	// Test multiple start/stop cycles
	for i := 0; i < 3; i++ {
		t.Run(fmt.Sprintf("cycle_%d", i+1), func(t *testing.T) {
			// Start PostgreSQL
			startCmd := exec.Command(pgctldBinary, "start", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
			startCmd.Env = append(os.Environ(),
				"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
			err := startCmd.Run()
			require.NoError(t, err)

			// Verify running
			statusCmd := exec.Command(pgctldBinary, "status", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
			statusCmd.Env = append(os.Environ(),
				"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
			output, err := statusCmd.Output()
			require.NoError(t, err)
			assert.Contains(t, string(output), "Running")

			// Stop PostgreSQL
			stopCmd := exec.Command(pgctldBinary, "stop", "--pooler-dir", dataDir, "--mode", "fast", "--config-file", pgctldConfigFile)
			stopCmd.Env = append(os.Environ(),
				"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
			err = stopCmd.Run()
			require.NoError(t, err)

			// Verify stopped
			statusCmd = exec.Command(pgctldBinary, "status", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
			statusCmd.Env = append(os.Environ(),
				"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
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

	pgctldBinary := getCachedPgctldBinary(t)
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
	initCmd := exec.Command(pgctldBinary, "init", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
	initCmd.Env = append(os.Environ(),
		"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
	err = initCmd.Run()
	require.NoError(t, err)

	// Start PostgreSQL
	startCmd := exec.Command(pgctldBinary, "start", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
	startCmd.Env = append(os.Environ(),
		"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
	err = startCmd.Run()
	require.NoError(t, err)

	defer func() {
		stopCmd := exec.Command(pgctldBinary, "stop", "--pooler-dir", dataDir, "--mode", "fast", "--config-file", pgctldConfigFile)
		stopCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
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
		reloadCmd := exec.Command(pgctldBinary, "reload-config", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
		reloadCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
		err = reloadCmd.Run()
		require.NoError(t, err)

		// Server should still be running
		statusCmd := exec.Command(pgctldBinary, "status", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
		statusCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
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

	pgctldBinary := getCachedPgctldBinary(t)
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
		startCmd := exec.Command(pgctldBinary, "start", "--pooler-dir", nonexistentDir, "--config-file", pgctldConfigFile)
		startCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
		err := startCmd.Run()
		require.Error(t, err, "Start should fail when data directory is not initialized")

		// Initialize first, then start should work
		initCmd := exec.Command(pgctldBinary, "init", "--pooler-dir", nonexistentDir, "--config-file", pgctldConfigFile)
		initCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
		err = initCmd.Run()
		require.NoError(t, err)

		// Now start should work
		startCmd = exec.Command(pgctldBinary, "start", "--pooler-dir", nonexistentDir, "--config-file", pgctldConfigFile)
		startCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
		err = startCmd.Run()
		require.NoError(t, err)

		// Clean stop
		stopCmd := exec.Command(pgctldBinary, "stop", "--pooler-dir", nonexistentDir, "--mode", "immediate", "--config-file", pgctldConfigFile)
		stopCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
		_ = stopCmd.Run()
	})

	t.Run("double_start_attempt", func(t *testing.T) {
		// Initialize data directory first
		initCmd := exec.Command(pgctldBinary, "init", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
		initCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
		err := initCmd.Run()
		require.NoError(t, err)

		// Start PostgreSQL
		startCmd := exec.Command(pgctldBinary, "start", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
		startCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
		err = startCmd.Run()
		require.NoError(t, err)

		// Try to start again - should handle gracefully
		startCmd2 := exec.Command(pgctldBinary, "start", "--pooler-dir", dataDir, "--config-file", pgctldConfigFile)
		startCmd2.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
		output, err := startCmd2.CombinedOutput()
		// Should either succeed or fail gracefully with appropriate message
		if err != nil {
			assert.Contains(t, strings.ToLower(string(output)), "already")
		}

		// Clean stop
		stopCmd := exec.Command(pgctldBinary, "stop", "--pooler-dir", dataDir, "--mode", "immediate", "--config-file", pgctldConfigFile)
		stopCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
		_ = stopCmd.Run()
	})
}
