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

package command

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	"github.com/multigres/multigres/go/pgctld"
)

func TestRunStart(t *testing.T) {
	tests := []struct {
		name          string
		setupDataDir  func(string) string // Now takes postgres data dir, not base dir
		setupBinaries bool
		expectError   bool
		errorContains string
	}{
		{
			name: "start with uninitialized data dir fails",
			setupDataDir: func(pgDataDir string) string {
				return testutil.CreateDataDir(t, pgDataDir, false) // uninitialized
			},
			setupBinaries: true,
			expectError:   true,
		},
		{
			name: "successful start with initialized data dir",
			setupDataDir: func(pgDataDir string) string {
				dataDir := testutil.CreateDataDir(t, pgDataDir, true) // initialized
				return dataDir
			},
			setupBinaries: true,
			expectError:   false,
		},
		{
			name: "server already running",
			setupDataDir: func(pgDataDir string) string {
				dataDir := testutil.CreateDataDir(t, pgDataDir, true)
				// Create PID file to simulate running server
				testutil.CreatePIDFile(t, dataDir, 12345)
				return dataDir
			},
			setupBinaries: true,
			expectError:   false, // Should succeed but report already running
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup temporary directories
			baseDir, cleanup := testutil.TempDir(t, "pgctld_start_test")
			defer cleanup()

			tt.setupDataDir(baseDir)

			// Setup mock binaries if needed
			if tt.setupBinaries {
				binDir := filepath.Join(baseDir, "bin")
				require.NoError(t, os.MkdirAll(binDir, 0o755))
				testutil.CreateMockPostgreSQLBinaries(t, binDir)

				// Add to PATH for test
				originalPath := os.Getenv("PATH")
				os.Setenv("PATH", binDir+":"+originalPath)
				defer os.Setenv("PATH", originalPath)
			}

			cmd := Root

			// Set up the command arguments
			args := []string{"start", "--pooler-dir", baseDir}
			cmd.SetArgs(args)

			err := cmd.Execute()

			if tt.expectError {
				require.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestIsDataDirInitialized(t *testing.T) {
	tests := []struct {
		name        string
		setupDir    func(string) string
		initialized bool
	}{
		{
			name: "uninitialized directory",
			setupDir: func(baseDir string) string {
				return testutil.CreateDataDir(t, baseDir, false)
			},
			initialized: false,
		},
		{
			name: "initialized directory",
			setupDir: func(baseDir string) string {
				return testutil.CreateDataDir(t, baseDir, true)
			},
			initialized: true,
		},
		{
			name: "non-existent directory",
			setupDir: func(baseDir string) string {
				return filepath.Join(baseDir, "nonexistent")
			},
			initialized: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseDir, cleanup := testutil.TempDir(t, "pgctld_init_test")
			defer cleanup()

			tt.setupDir(baseDir)
			result := pgctld.IsDataDirInitialized(baseDir)
			assert.Equal(t, tt.initialized, result)
		})
	}
}

func TestIsPostgreSQLRunning(t *testing.T) {
	tests := []struct {
		name      string
		setupDir  func(string) string
		isRunning bool
	}{
		{
			name: "server running with PID file",
			setupDir: func(baseDir string) string {
				dataDir := testutil.CreateDataDir(t, baseDir, true)
				testutil.CreatePIDFile(t, dataDir, 12345)
				return dataDir
			},
			isRunning: true,
		},
		{
			name: "server not running",
			setupDir: func(baseDir string) string {
				return testutil.CreateDataDir(t, baseDir, true)
			},
			isRunning: false,
		},
		{
			name: "uninitialized directory",
			setupDir: func(baseDir string) string {
				return testutil.CreateDataDir(t, baseDir, false)
			},
			isRunning: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseDir, cleanup := testutil.TempDir(t, "pgctld_running_test")
			defer cleanup()

			dataDir := tt.setupDir(baseDir)
			result := isPostgreSQLRunning(dataDir)
			assert.Equal(t, tt.isRunning, result)
		})
	}
}

func TestInitializeDataDir(t *testing.T) {
	t.Run("successful initialization", func(t *testing.T) {
		baseDir, cleanup := testutil.TempDir(t, "pgctld_initdb_test")
		defer cleanup()

		dataDir := filepath.Join(baseDir, "data")

		// Setup mock initdb binary
		binDir := filepath.Join(baseDir, "bin")
		require.NoError(t, os.MkdirAll(binDir, 0o755))
		testutil.CreateMockPostgreSQLBinaries(t, binDir)

		// Add to PATH for test
		originalPath := os.Getenv("PATH")
		os.Setenv("PATH", binDir+":"+originalPath)
		defer os.Setenv("PATH", originalPath)

		err := initializeDataDir(dataDir)
		require.NoError(t, err)

		// Verify directory was created
		assert.DirExists(t, dataDir)

		// Verify PG_VERSION file exists (created by mock)
		assert.FileExists(t, filepath.Join(dataDir, "PG_VERSION"))
	})

	t.Run("fails with invalid directory permissions", func(t *testing.T) {
		// Try to create data dir in a read-only location
		dataDir := "/root/impossible_dir"

		err := initializeDataDir(dataDir)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create data directory")
	})
}

func TestWaitForPostgreSQL(t *testing.T) {
	t.Run("server becomes ready immediately", func(t *testing.T) {
		baseDir, cleanup := testutil.TempDir(t, "pgctld_wait_test")
		defer cleanup()

		// Create initialized data directory with postgresql.conf
		testutil.CreateDataDir(t, baseDir, true)

		// Setup mock pg_isready that succeeds
		binDir := filepath.Join(baseDir, "bin")
		require.NoError(t, os.MkdirAll(binDir, 0o755))
		testutil.CreateMockPostgreSQLBinaries(t, binDir)

		originalPath := os.Getenv("PATH")
		os.Setenv("PATH", binDir+":"+originalPath)
		defer os.Setenv("PATH", originalPath)

		// Create config that matches the test setup
		config, err := pgctld.NewPostgresCtlConfig(
			"localhost",
			5432,
			"postgres",
			"postgres",
			"",
			30, // timeout
			pgctld.PostgresDataDir(baseDir),
			pgctld.PostgresConfigFile(baseDir),
			baseDir,
		)
		require.NoError(t, err)

		err = waitForPostgreSQLWithConfig(config)
		assert.NoError(t, err)
	})

	t.Run("timeout waiting for server", func(t *testing.T) {
		baseDir, cleanup := testutil.TempDir(t, "pgctld_timeout_test")
		defer cleanup()

		// Create initialized data directory with postgresql.conf
		testutil.CreateDataDir(t, baseDir, true)

		// Create mock pg_isready that always fails
		binDir := filepath.Join(baseDir, "bin")
		require.NoError(t, os.MkdirAll(binDir, 0o755))
		testutil.MockBinary(t, binDir, "pg_isready", "exit 1")

		originalPath := os.Getenv("PATH")
		os.Setenv("PATH", binDir+":"+originalPath)
		defer os.Setenv("PATH", originalPath)

		// Create config with short timeout for test
		config, err := pgctld.NewPostgresCtlConfig(
			"localhost",
			5432,
			"postgres",
			"postgres",
			"",
			1, // 1 second timeout
			pgctld.PostgresDataDir(baseDir),
			pgctld.PostgresConfigFile(baseDir),
			baseDir,
		)
		require.NoError(t, err)

		err = waitForPostgreSQLWithConfig(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "did not become ready")
	})
}
