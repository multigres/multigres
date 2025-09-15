// Copyright 2025 The Supabase, Inc.
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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
)

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

	// Build pgctld binary for testing
	pgctldBinary := filepath.Join(tempDir, "pgctld")
	buildCmd := exec.Command("go", "build", "-o", pgctldBinary, "..")
	buildOutput, err := buildCmd.CombinedOutput()
	require.NoError(t, err, "Failed to build pgctld binary: %v\nOutput: %s", err, string(buildOutput))

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

	// Build pgctld binary for testing
	pgctldBinary := filepath.Join(tempDir, "pgctld")
	buildCmd := exec.Command("go", "build", "-o", pgctldBinary, "..")
	err = buildCmd.Run()
	require.NoError(t, err, "Failed to build pgctld binary")

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

	// Build pgctld binary for testing
	pgctldBinary := filepath.Join(tempDir, "pgctld")
	buildCmd := exec.Command("go", "build", "-o", pgctldBinary, "..")
	err = buildCmd.Run()
	require.NoError(t, err, "Failed to build pgctld binary")

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

	// Build pgctld binary for testing
	pgctldBinary := filepath.Join(tempDir, "pgctld")
	buildCmd := exec.Command("go", "build", "-o", pgctldBinary, "..")
	err = buildCmd.Run()
	require.NoError(t, err, "Failed to build pgctld binary")

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
