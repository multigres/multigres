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
	"fmt"
	"net"
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

	// Build pgctld binary for testing
	pgctldBinary := filepath.Join(tempDir, "pgctld")
	buildCmd := exec.Command("go", "build", "-o", pgctldBinary, "..")
	buildOutput, err := buildCmd.CombinedOutput()
	require.NoError(t, err, "Failed to build pgctld binary: %v\nOutput: %s", err, string(buildOutput))

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
