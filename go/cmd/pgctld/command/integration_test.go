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
`), 0644)
	require.NoError(t, err)

	// Create a pgctld config file to avoid config file errors
	pgctldConfigFile := filepath.Join(tempDir, ".pgctld.yaml")
	err = os.WriteFile(pgctldConfigFile, []byte(`
# Test pgctld configuration
log-level: info
timeout: 30
`), 0644)
	require.NoError(t, err)

	// Setup mock PostgreSQL binaries
	binDir := filepath.Join(tempDir, "bin")
	err = os.MkdirAll(binDir, 0755)
	require.NoError(t, err)
	testutil.CreateMockPostgreSQLBinaries(t, binDir)

	// Build pgctld binary for testing
	pgctldBinary := filepath.Join(tempDir, "pgctld")
	buildCmd := exec.Command("go", "build", "-o", pgctldBinary, "..")
	buildOutput, err := buildCmd.CombinedOutput()
	require.NoError(t, err, "Failed to build pgctld binary: %v\nOutput: %s", err, string(buildOutput))

	t.Run("complete_lifecycle_via_cli", func(t *testing.T) {
		// Step 1: Initial status - should be not initialized
		statusCmd := exec.Command(pgctldBinary, "status", "--pg-data-dir", dataDir, "--config", pgctldConfigFile)
		statusCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
		output, err := statusCmd.CombinedOutput()
		if err != nil {
			t.Logf("statusCmd.Output() error: %v, output: %s", err, string(output))
		}
		require.NoError(t, err)
		assert.Contains(t, string(output), "Not initialized")

		// Step 2: Start PostgreSQL (should initialize and start)
		startCmd := exec.Command(pgctldBinary, "start", "--pg-data-dir", dataDir, "--config", pgctldConfigFile)
		startCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
		err = startCmd.Run()
		require.NoError(t, err)

		// Step 3: Check status - should be running
		statusCmd = exec.Command(pgctldBinary, "status", "--pg-data-dir", dataDir, "--config", pgctldConfigFile)
		statusCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
		output, err = statusCmd.Output()
		require.NoError(t, err)
		assert.Contains(t, string(output), "Running")

		// Step 4: Reload configuration
		reloadCmd := exec.Command(pgctldBinary, "reload-config", "--pg-data-dir", dataDir, "--config", pgctldConfigFile)
		reloadCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
		err = reloadCmd.Run()
		require.NoError(t, err)

		// Step 5: Restart PostgreSQL
		restartCmd := exec.Command(pgctldBinary, "restart", "--pg-data-dir", dataDir, "--config", pgctldConfigFile)
		restartCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
		err = restartCmd.Run()
		require.NoError(t, err)

		// Step 6: Check status again - should still be running
		statusCmd = exec.Command(pgctldBinary, "status", "--pg-data-dir", dataDir, "--config", pgctldConfigFile)
		statusCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
		output, err = statusCmd.Output()
		require.NoError(t, err)
		assert.Contains(t, string(output), "Running")

		// Step 7: Stop PostgreSQL
		stopCmd := exec.Command(pgctldBinary, "stop", "--pg-data-dir", dataDir, "--config", pgctldConfigFile)
		stopCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
		err = stopCmd.Run()
		require.NoError(t, err)

		// Step 8: Final status check - should be stopped
		statusCmd = exec.Command(pgctldBinary, "status", "--pg-data-dir", dataDir, "--config", pgctldConfigFile)
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
`), 0644)
	require.NoError(t, err)

	// Setup mock PostgreSQL binaries
	binDir := filepath.Join(tempDir, "bin")
	err = os.MkdirAll(binDir, 0755)
	require.NoError(t, err)
	testutil.CreateMockPostgreSQLBinaries(t, binDir)

	// Build pgctld binary for testing
	pgctldBinary := filepath.Join(tempDir, "pgctld")
	buildCmd := exec.Command("go", "build", "-o", pgctldBinary, "..")
	err = buildCmd.Run()
	require.NoError(t, err, "Failed to build pgctld binary")

	// Initialize once via CLI
	startCmd := exec.Command(pgctldBinary, "start", "--pg-data-dir", dataDir, "--config", pgctldConfigFile)
	startCmd.Env = append(os.Environ(),
		"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
	err = startCmd.Run()
	require.NoError(t, err)

	// Stop initial start
	stopCmd := exec.Command(pgctldBinary, "stop", "--pg-data-dir", dataDir, "--config", pgctldConfigFile)
	stopCmd.Env = append(os.Environ(),
		"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
	err = stopCmd.Run()
	require.NoError(t, err)

	// Test multiple start/stop cycles
	for i := 0; i < 3; i++ {
		t.Run(fmt.Sprintf("cycle_%d", i+1), func(t *testing.T) {
			// Start PostgreSQL
			startCmd := exec.Command(pgctldBinary, "start", "--pg-data-dir", dataDir, "--config", pgctldConfigFile)
			startCmd.Env = append(os.Environ(),
				"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
			err := startCmd.Run()
			require.NoError(t, err)

			// Verify running
			statusCmd := exec.Command(pgctldBinary, "status", "--pg-data-dir", dataDir, "--config", pgctldConfigFile)
			statusCmd.Env = append(os.Environ(),
				"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
			output, err := statusCmd.Output()
			require.NoError(t, err)
			assert.Contains(t, string(output), "Running")

			// Stop PostgreSQL
			stopCmd := exec.Command(pgctldBinary, "stop", "--pg-data-dir", dataDir, "--mode", "fast", "--config", pgctldConfigFile)
			stopCmd.Env = append(os.Environ(),
				"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
			err = stopCmd.Run()
			require.NoError(t, err)

			// Verify stopped
			statusCmd = exec.Command(pgctldBinary, "status", "--pg-data-dir", dataDir, "--config", pgctldConfigFile)
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
`), 0644)
	require.NoError(t, err)

	// Setup mock PostgreSQL binaries
	binDir := filepath.Join(tempDir, "bin")
	err = os.MkdirAll(binDir, 0755)
	require.NoError(t, err)
	testutil.CreateMockPostgreSQLBinaries(t, binDir)

	// Build pgctld binary for testing
	pgctldBinary := filepath.Join(tempDir, "pgctld")
	buildCmd := exec.Command("go", "build", "-o", pgctldBinary, "..")
	err = buildCmd.Run()
	require.NoError(t, err, "Failed to build pgctld binary")

	// Initialize and start PostgreSQL
	startCmd := exec.Command(pgctldBinary, "start", "--pg-data-dir", dataDir, "--config", pgctldConfigFile)
	startCmd.Env = append(os.Environ(),
		"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
	err = startCmd.Run()
	require.NoError(t, err)

	defer func() {
		stopCmd := exec.Command(pgctldBinary, "stop", "--pg-data-dir", dataDir, "--mode", "fast", "--config", pgctldConfigFile)
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
`), 0644)
		require.NoError(t, err)

		// Reload configuration
		reloadCmd := exec.Command(pgctldBinary, "reload-config", "--pg-data-dir", dataDir, "--config", pgctldConfigFile)
		reloadCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
		err = reloadCmd.Run()
		require.NoError(t, err)

		// Server should still be running
		statusCmd := exec.Command(pgctldBinary, "status", "--pg-data-dir", dataDir, "--config", pgctldConfigFile)
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
`), 0644)
	require.NoError(t, err)

	// Setup mock PostgreSQL binaries
	binDir := filepath.Join(tempDir, "bin")
	err = os.MkdirAll(binDir, 0755)
	require.NoError(t, err)
	testutil.CreateMockPostgreSQLBinaries(t, binDir)

	// Build pgctld binary for testing
	pgctldBinary := filepath.Join(tempDir, "pgctld")
	buildCmd := exec.Command("go", "build", "-o", pgctldBinary, "..")
	err = buildCmd.Run()
	require.NoError(t, err, "Failed to build pgctld binary")

	t.Run("start_with_nonexistent_data_dir", func(t *testing.T) {
		nonexistentDir := filepath.Join(tempDir, "nonexistent")

		// Try to start with non-existent directory - should initialize and start
		startCmd := exec.Command(pgctldBinary, "start", "--pg-data-dir", nonexistentDir, "--config", pgctldConfigFile)
		startCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
		err := startCmd.Run()
		require.NoError(t, err)

		// Clean stop
		stopCmd := exec.Command(pgctldBinary, "stop", "--pg-data-dir", nonexistentDir, "--mode", "immediate", "--config", pgctldConfigFile)
		stopCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
		_ = stopCmd.Run()
	})

	t.Run("double_start_attempt", func(t *testing.T) {
		// Start PostgreSQL
		startCmd := exec.Command(pgctldBinary, "start", "--pg-data-dir", dataDir, "--config", pgctldConfigFile)
		startCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
		err := startCmd.Run()
		require.NoError(t, err)

		// Try to start again - should handle gracefully
		startCmd2 := exec.Command(pgctldBinary, "start", "--pg-data-dir", dataDir, "--config", pgctldConfigFile)
		startCmd2.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
		output, err := startCmd2.CombinedOutput()
		// Should either succeed or fail gracefully with appropriate message
		if err != nil {
			assert.Contains(t, strings.ToLower(string(output)), "already")
		}

		// Clean stop
		stopCmd := exec.Command(pgctldBinary, "stop", "--pg-data-dir", dataDir, "--mode", "immediate", "--config", pgctldConfigFile)
		stopCmd.Env = append(os.Environ(),
			"PATH="+filepath.Join(tempDir, "bin")+":"+os.Getenv("PATH"))
		_ = stopCmd.Run()
	})
}
