// Copyright 2025 Supabase, Inc.
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

package command

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	"github.com/multigres/multigres/go/pgctld"
)

func TestRunStatus(t *testing.T) {
	baseDir, cleanup := testutil.TempDir(t, "pgctld_status_test")
	defer cleanup()

	// Setup mock binaries
	binDir := filepath.Join(baseDir, "bin")
	require.NoError(t, os.MkdirAll(binDir, 0o755))
	testutil.CreateMockPostgreSQLBinaries(t, binDir)

	originalPath := os.Getenv("PATH")
	os.Setenv("PATH", binDir+":"+originalPath)
	defer os.Setenv("PATH", originalPath)

	// Helper function to capture command output
	runStatusCommand := func() (string, error) {
		oldStdout := os.Stdout
		r, w, _ := os.Pipe()
		os.Stdout = w

		cmd := Root
		cmd.SetArgs([]string{"status", "--pooler-dir", baseDir})
		err := cmd.Execute()

		w.Close()
		os.Stdout = oldStdout

		var buf bytes.Buffer
		_, readErr := io.Copy(&buf, r)
		require.NoError(t, readErr)
		return buf.String(), err
	}

	// Test 1: Not initialized (no data directory exists yet)
	t.Run("not_initialized", func(t *testing.T) {
		_, err := runStatusCommand()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "data directory not initialized")
	})

	// Test 2: Stopped (initialized but no PID file)
	t.Run("stopped", func(t *testing.T) {
		testutil.CreateDataDir(t, baseDir, true)
		output, err := runStatusCommand()
		require.NoError(t, err)
		assert.Contains(t, output, "Status: Stopped")
	})

	// Test 3: Running (initialized with PID file)
	t.Run("running", func(t *testing.T) {
		// Generate PostgreSQL config and create PID file to simulate running
		dataDir := testutil.CreateDataDir(t, baseDir, true)
		testutil.CreatePIDFile(t, dataDir, 12345)

		output, err := runStatusCommand()
		t.Logf("output: %s", output)
		require.NoError(t, err)
		assert.Contains(t, output, "Status: Running")
		assert.Contains(t, output, "PID:")
		assert.Contains(t, output, "Port: 5432")
	})
}

func TestRunStatusNoPoolerDir(t *testing.T) {
	// Don't set pooler directory - should get an error
	cmd := Root
	cmd.SetArgs([]string{"status", "--pooler-dir", ""})
	err := cmd.Execute()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "pooler-dir needs to be set")
}

func TestIsServerReady(t *testing.T) {
	tests := []struct {
		name        string
		setupBinary func(string)
		isReady     bool
	}{
		{
			name: "server is ready",
			setupBinary: func(binDir string) {
				testutil.CreateMockPostgreSQLBinaries(t, binDir)
			},
			isReady: true,
		},
		{
			name: "server not ready",
			setupBinary: func(binDir string) {
				// Create pg_isready that fails
				testutil.MockBinary(t, binDir, "pg_isready", "exit 1")
			},
			isReady: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseDir, cleanup := testutil.TempDir(t, "pgctld_ready_test")
			defer cleanup()

			binDir := filepath.Join(baseDir, "bin")
			require.NoError(t, os.MkdirAll(binDir, 0o755))
			tt.setupBinary(binDir)

			originalPath := os.Getenv("PATH")
			os.Setenv("PATH", binDir+":"+originalPath)
			defer os.Setenv("PATH", originalPath)

			// Create initialized data directory with postgresql.conf
			testutil.CreateDataDir(t, baseDir, true)

			// Create config directly for the test
			config, err := pgctld.NewPostgresCtlConfig(
				5432, "postgres", "postgres", "",
				30, pgctld.PostgresDataDir(baseDir), pgctld.PostgresConfigFile(baseDir), baseDir)
			require.NoError(t, err)

			result := isServerReadyWithConfig(config)
			assert.Equal(t, tt.isReady, result)
		})
	}
}

func TestGetServerVersion(t *testing.T) {
	tests := []struct {
		name           string
		setupBinary    func(string)
		expectedOutput string
	}{
		{
			name: "successful version retrieval",
			setupBinary: func(binDir string) {
				testutil.CreateMockPostgreSQLBinaries(t, binDir)
			},
			expectedOutput: "PostgreSQL 15.0",
		},
		{
			name: "version command fails",
			setupBinary: func(binDir string) {
				testutil.MockBinary(t, binDir, "psql", "exit 1")
			},
			expectedOutput: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseDir, cleanup := testutil.TempDir(t, "pgctld_version_test")
			defer cleanup()

			binDir := filepath.Join(baseDir, "bin")
			require.NoError(t, os.MkdirAll(binDir, 0o755))
			tt.setupBinary(binDir)

			// Create initialized data directory with postgresql.conf
			testutil.CreateDataDir(t, baseDir, true)

			originalPath := os.Getenv("PATH")
			os.Setenv("PATH", binDir+":"+originalPath)
			defer os.Setenv("PATH", originalPath)

			// Create config directly for the test
			config, err := pgctld.NewPostgresCtlConfig(
				5432, "postgres", "postgres", "",
				30, pgctld.PostgresDataDir(baseDir), pgctld.PostgresConfigFile(baseDir), baseDir)
			require.NoError(t, err)

			result := getServerVersionWithConfig(config)
			if tt.expectedOutput != "" {
				assert.Contains(t, result, tt.expectedOutput)
			} else {
				assert.Empty(t, result)
			}
		})
	}
}

func TestGetServerUptime(t *testing.T) {
	t.Run("calculates uptime from PID file", func(t *testing.T) {
		baseDir, cleanup := testutil.TempDir(t, "pgctld_uptime_test")
		defer cleanup()

		dataDir := testutil.CreateDataDir(t, baseDir, true)
		testutil.CreatePIDFile(t, dataDir, 12345)

		result := getServerUptime(dataDir)

		// Should return some uptime value (exact value depends on timing)
		assert.NotEmpty(t, result)
		assert.Contains(t, result, "minute") // Should show some time unit
	})

	t.Run("returns empty for missing PID file", func(t *testing.T) {
		baseDir, cleanup := testutil.TempDir(t, "pgctld_uptime_test")
		defer cleanup()

		dataDir := testutil.CreateDataDir(t, baseDir, true)
		// No PID file created

		result := getServerUptime(dataDir)
		assert.Empty(t, result)
	})
}
