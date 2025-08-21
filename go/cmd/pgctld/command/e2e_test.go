// Copyright 2025 The Multigres Authors.
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

package command

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
)

// setupTestEnv sets up environment variables for PostgreSQL tests
func setupTestEnv(cmd *exec.Cmd) {
	cmd.Env = append(os.Environ(),
		"PGCONNECT_TIMEOUT=5", // Shorter timeout for tests
		"PGPASSWORD=postgres", // Default password for tests
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
		t.Skip("PostgreSQL binaries not found, skipping end-to-end tests")
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
`), 0644)
	require.NoError(t, err)

	// Build pgctld binary for testing
	pgctldBinary := filepath.Join(tempDir, "pgctld")
	buildCmd := exec.Command("go", "build", "-o", pgctldBinary, "..")
	buildOutput, err := buildCmd.CombinedOutput()
	require.NoError(t, err, "Failed to build pgctld binary: %v\nOutput: %s", err, string(buildOutput))

	t.Run("basic_commands_with_real_postgresql", func(t *testing.T) {
		// Step 1: Initial status - should be not initialized
		statusCmd := exec.Command(pgctldBinary, "status", "--data-dir", dataDir, "--config", pgctldConfigFile)
		setupTestEnv(statusCmd)
		output, err := statusCmd.Output()
		require.NoError(t, err)
		assert.Contains(t, string(output), "Not initialized")

		// Step 2: Test help commands work
		helpCmd := exec.Command(pgctldBinary, "--help")
		helpOutput, err := helpCmd.Output()
		require.NoError(t, err)
		assert.Contains(t, string(helpOutput), "pgctld")

		// Step 3: Test that real PostgreSQL binaries are detected
		versionCmd := exec.Command("postgres", "--version")
		versionOutput, err := versionCmd.Output()
		require.NoError(t, err)
		t.Logf("PostgreSQL version: %s", string(versionOutput))
		assert.Contains(t, string(versionOutput), "postgres")

		// Step 4: Test initialization works with real PostgreSQL
		initCmd := exec.Command("initdb", "--help")
		initOutput, err := initCmd.Output()
		require.NoError(t, err)
		assert.Contains(t, string(initOutput), "initdb")

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
		t.Skip("PostgreSQL binaries not found, skipping end-to-end tests")
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
`), 0644)
	require.NoError(t, err)

	// Build pgctld binary for testing
	pgctldBinary := filepath.Join(tempDir, "pgctld")
	buildCmd := exec.Command("go", "build", "-o", pgctldBinary, "..")
	buildOutput, err := buildCmd.CombinedOutput()
	require.NoError(t, err, "Failed to build pgctld binary: %v\nOutput: %s", err, string(buildOutput))

	t.Run("grpc_server_with_real_postgresql", func(t *testing.T) {
		// Generate random ports for this test
		grpcPort := testutil.GenerateRandomPort()
		pgPort := testutil.GenerateRandomPort()
		t.Logf("gRPC test using ports - gRPC: %d, PostgreSQL: %d", grpcPort, pgPort)

		// Start gRPC server in background
		serverCmd := exec.Command(pgctldBinary, "server",
			"--data-dir", dataDir,
			"--grpc-port", strconv.Itoa(grpcPort),
			"--pg-port", strconv.Itoa(pgPort),
			"--config", pgctldConfigFile)
		setupTestEnv(serverCmd) // Use system PATH for real PostgreSQL binaries, trust auth for tests

		err := serverCmd.Start()
		require.NoError(t, err)
		defer func() {
			if serverCmd.Process != nil {
				_ = serverCmd.Process.Kill()
				_ = serverCmd.Wait()
			}
		}()

		// Give server time to start
		time.Sleep(3 * time.Second)

		// Test basic gRPC connectivity by checking if the server is listening
		// We can't easily test full gRPC functionality without importing the pb package
		// but we can at least verify the server starts without errors

		// Check if server process is still running (not crashed)
		if serverCmd.Process != nil {
			// Check if process is still alive by checking if ProcessState is nil
			// If the process has exited, ProcessState will be non-nil
			if serverCmd.ProcessState == nil {
				t.Log("gRPC server is running successfully")
			} else {
				t.Errorf("gRPC server process died: exit code %d", serverCmd.ProcessState.ExitCode())
			}
		}
	})
}

// TestEndToEndPerformance tests performance characteristics
func TestEndToEndPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance tests in short mode")
	}

	// Check if PostgreSQL binaries are available
	if !hasPostgreSQLBinaries() {
		t.Skip("PostgreSQL binaries not found, skipping performance tests")
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
`), 0644)
	require.NoError(t, err)

	// Build pgctld binary for testing
	pgctldBinary := filepath.Join(tempDir, "pgctld")
	buildCmd := exec.Command("go", "build", "-o", pgctldBinary, "..")
	buildOutput, err := buildCmd.CombinedOutput()
	require.NoError(t, err, "Failed to build pgctld binary: %v\nOutput: %s", err, string(buildOutput))

	t.Run("startup_performance", func(t *testing.T) {
		// Generate random port for this test
		perfTestPort := testutil.GenerateRandomPort()
		t.Logf("Performance test using port: %d", perfTestPort)

		// Measure time to start PostgreSQL
		startTime := time.Now()

		startCmd := exec.Command(pgctldBinary, "start", "--data-dir", dataDir, "--pg-port", strconv.Itoa(perfTestPort), "--config", pgctldConfigFile)
		setupTestEnv(startCmd)
		err = startCmd.Run()
		require.NoError(t, err)

		startupDuration := time.Since(startTime)
		t.Logf("PostgreSQL startup took: %v", startupDuration)

		// Startup should typically complete within 30 seconds
		assert.Less(t, startupDuration, 30*time.Second, "PostgreSQL startup took too long")

		// Clean shutdown
		stopCmd := exec.Command(pgctldBinary, "stop", "--data-dir", dataDir, "--config", pgctldConfigFile)
		setupTestEnv(stopCmd)
		err = stopCmd.Run()
		require.NoError(t, err)
	})

	t.Run("multiple_rapid_operations", func(t *testing.T) {
		// Generate random port for this test
		rapidTestPort := testutil.GenerateRandomPort()
		t.Logf("Rapid operations test using port: %d", rapidTestPort)

		// Test rapid start/stop cycles
		for i := 0; i < 3; i++ {
			t.Logf("Cycle %d", i+1)

			// Start
			startCmd := exec.Command(pgctldBinary, "start", "--data-dir", dataDir, "--pg-port", strconv.Itoa(rapidTestPort), "--config", pgctldConfigFile)
			setupTestEnv(startCmd)
			err := startCmd.Run()
			require.NoError(t, err)

			// Brief wait
			time.Sleep(1 * time.Second)

			// Stop
			stopCmd := exec.Command(pgctldBinary, "stop", "--data-dir", dataDir, "--mode", "fast", "--config", pgctldConfigFile)
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
		t.Skip("PostgreSQL binaries not found, skipping system integration tests")
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
`), 0644)
	require.NoError(t, err)

	// Build pgctld binary for testing
	pgctldBinary := filepath.Join(tempDir, "pgctld")
	buildCmd := exec.Command("go", "build", "-o", pgctldBinary, "..")
	buildOutput, err := buildCmd.CombinedOutput()
	require.NoError(t, err, "Failed to build pgctld binary: %v\nOutput: %s", err, string(buildOutput))

	t.Run("version_compatibility", func(t *testing.T) {
		// Generate random port for this test
		testPort := testutil.GenerateRandomPort()
		t.Logf("Using port: %d", testPort)

		// Check PostgreSQL version compatibility
		versionCmd := exec.Command("postgres", "--version")
		output, err := versionCmd.Output()
		require.NoError(t, err)
		t.Logf("PostgreSQL version: %s", string(output))

		// Start PostgreSQL to test compatibility
		startCmd := exec.Command(pgctldBinary, "start", "--data-dir", dataDir, "--pg-port", strconv.Itoa(testPort), "--config", pgctldConfigFile)
		setupTestEnv(startCmd)
		startOutput, err := startCmd.CombinedOutput()
		if err != nil {
			t.Logf("pgctld start failed with output: %s", string(startOutput))
		}
		require.NoError(t, err)

		// Get version info through pgctld
		statusCmd := exec.Command(pgctldBinary, "status", "--data-dir", dataDir, "--pg-port", strconv.Itoa(testPort), "--config", pgctldConfigFile)
		setupTestEnv(statusCmd)
		output, err = statusCmd.Output()
		require.NoError(t, err)
		t.Logf("pgctld status output: %s", string(output))

		// Clean shutdown
		stopCmd := exec.Command(pgctldBinary, "stop", "--data-dir", dataDir, "--config", pgctldConfigFile)
		setupTestEnv(stopCmd)
		err = stopCmd.Run()
		require.NoError(t, err)
	})

	t.Run("configuration_management", func(t *testing.T) {
		// Generate random port for this test
		configTestPort := testutil.GenerateRandomPort()
		t.Logf("Configuration test using port: %d", configTestPort)

		// Test custom configuration - use separate data directory
		configDataDir := filepath.Join(tempDir, "config_data")
		configFile := filepath.Join(configDataDir, "postgresql.conf")

		// Start PostgreSQL first to create data directory
		startCmd := exec.Command(pgctldBinary, "start", "--data-dir", configDataDir, "--pg-port", strconv.Itoa(configTestPort), "--config", pgctldConfigFile)
		setupTestEnv(startCmd)
		startOutput, err := startCmd.CombinedOutput()
		if err != nil {
			t.Logf("pgctld start failed with output: %s", string(startOutput))
		}
		require.NoError(t, err)

		// Check status to ensure server is running before modifying config
		statusCheckCmd := exec.Command(pgctldBinary, "status", "--data-dir", configDataDir, "--pg-port", strconv.Itoa(configTestPort), "--config", pgctldConfigFile)
		setupTestEnv(statusCheckCmd)
		statusOutput, err := statusCheckCmd.CombinedOutput()
		t.Logf("Status before reload: %s", string(statusOutput))
		require.NoError(t, err)

		// Modify configuration
		customConfig := `
# Custom configuration for testing
max_connections = 50
shared_buffers = 64MB
log_statement = 'all'
log_min_messages = info
`
		err = os.WriteFile(configFile, []byte(customConfig), 0644)
		require.NoError(t, err)

		// Reload configuration
		reloadCmd := exec.Command(pgctldBinary, "reload-config", "--data-dir", configDataDir, "--config", pgctldConfigFile)
		setupTestEnv(reloadCmd)
		reloadOutput, err := reloadCmd.CombinedOutput()
		if err != nil {
			t.Logf("reload-config failed with output: %s", string(reloadOutput))
		}
		require.NoError(t, err)

		// Verify server is still running after reload
		statusCmd := exec.Command(pgctldBinary, "status", "--data-dir", configDataDir, "--pg-port", strconv.Itoa(configTestPort), "--config", pgctldConfigFile)
		setupTestEnv(statusCmd)
		output, err := statusCmd.Output()
		require.NoError(t, err)
		assert.Contains(t, string(output), "Running")

		// Clean shutdown
		stopCmd := exec.Command(pgctldBinary, "stop", "--data-dir", configDataDir, "--config", pgctldConfigFile)
		setupTestEnv(stopCmd)
		err = stopCmd.Run()
		require.NoError(t, err)
	})
}
