/*
Copyright 2025 The Multigres Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package command

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
)

func TestStopPostgreSQLWithResult(t *testing.T) {
	tests := []struct {
		name           string
		setupDataDir   func(string) string
		setupBinaries  bool
		mode           string
		config         func(*PostgresConfig) *PostgresConfig
		expectError    bool
		errorContains  string
		expectedResult func(*StopResult)
	}{
		{
			name: "successful stop with fast mode",
			setupDataDir: func(baseDir string) string {
				dataDir := testutil.CreateDataDir(t, baseDir, true)
				testutil.CreatePIDFile(t, dataDir, 12345)
				return dataDir
			},
			setupBinaries: true,
			mode:          "fast",
			config: func(config *PostgresConfig) *PostgresConfig {
				return config
			},
			expectError: false,
			expectedResult: func(result *StopResult) {
				assert.True(t, result.WasRunning)
				assert.Equal(t, "PostgreSQL server stopped successfully", result.Message)
			},
		},
		{
			name: "successful stop with smart mode",
			setupDataDir: func(baseDir string) string {
				dataDir := testutil.CreateDataDir(t, baseDir, true)
				testutil.CreatePIDFile(t, dataDir, 12345)
				return dataDir
			},
			setupBinaries: true,
			mode:          "smart",
			config: func(config *PostgresConfig) *PostgresConfig {
				return config
			},
			expectError: false,
			expectedResult: func(result *StopResult) {
				assert.True(t, result.WasRunning)
				assert.Equal(t, "PostgreSQL server stopped successfully", result.Message)
			},
		},
		{
			name: "successful stop with immediate mode",
			setupDataDir: func(baseDir string) string {
				dataDir := testutil.CreateDataDir(t, baseDir, true)
				testutil.CreatePIDFile(t, dataDir, 12345)
				return dataDir
			},
			setupBinaries: true,
			mode:          "immediate",
			config: func(config *PostgresConfig) *PostgresConfig {
				return config
			},
			expectError: false,
			expectedResult: func(result *StopResult) {
				assert.True(t, result.WasRunning)
				assert.Equal(t, "PostgreSQL server stopped successfully", result.Message)
			},
		},
		{
			name: "stop when PostgreSQL is not running",
			setupDataDir: func(baseDir string) string {
				// Create data dir without PID file (not running)
				return testutil.CreateDataDir(t, baseDir, true)
			},
			setupBinaries: false,
			mode:          "fast",
			config: func(config *PostgresConfig) *PostgresConfig {
				return config
			},
			expectError: false,
			expectedResult: func(result *StopResult) {
				assert.False(t, result.WasRunning)
				assert.Equal(t, "PostgreSQL is not running", result.Message)
			},
		},
		{
			name:         "error when data-dir is empty",
			setupDataDir: func(baseDir string) string { return "" },
			mode:         "fast",
			config: func(config *PostgresConfig) *PostgresConfig {
				config.DataDir = ""
				return config
			},
			expectError:   true,
			errorContains: "pg-data-dir is required",
		},
		{
			name: "default mode when empty string provided",
			setupDataDir: func(baseDir string) string {
				dataDir := testutil.CreateDataDir(t, baseDir, true)
				testutil.CreatePIDFile(t, dataDir, 12345)
				return dataDir
			},
			setupBinaries: true,
			mode:          "", // Empty mode should default to "fast"
			config: func(config *PostgresConfig) *PostgresConfig {
				return config
			},
			expectError: false,
			expectedResult: func(result *StopResult) {
				assert.True(t, result.WasRunning)
				assert.Equal(t, "PostgreSQL server stopped successfully", result.Message)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseDir, cleanup := testutil.TempDir(t, "pgctld_stop_test")
			defer cleanup()

			dataDir := tt.setupDataDir(baseDir)

			if tt.setupBinaries {
				binDir := filepath.Join(baseDir, "bin")
				require.NoError(t, os.MkdirAll(binDir, 0o755))
				testutil.CreateMockPostgreSQLBinaries(t, binDir)

				originalPath := os.Getenv("PATH")
				os.Setenv("PATH", binDir+":"+originalPath)
				defer os.Setenv("PATH", originalPath)
			}

			config := &PostgresConfig{
				DataDir:  dataDir,
				Host:     "localhost",
				Port:     5432,
				User:     "postgres",
				Database: "postgres",
				Timeout:  30,
			}
			config = tt.config(config)

			result, err := StopPostgreSQLWithResult(config, tt.mode)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, result)
				if tt.expectedResult != nil {
					tt.expectedResult(result)
				}
			}
		})
	}
}

func TestRunStop(t *testing.T) {
	tests := []struct {
		name          string
		setupDataDir  func(string) string
		setupBinaries bool
		mode          string
		expectError   bool
		errorContains string
	}{
		{
			name: "successful stop command",
			setupDataDir: func(baseDir string) string {
				dataDir := testutil.CreateDataDir(t, baseDir, true)
				testutil.CreatePIDFile(t, dataDir, 12345)
				return dataDir
			},
			setupBinaries: true,
			mode:          "fast",
			expectError:   false,
		},
		{
			name: "stop when not running",
			setupDataDir: func(baseDir string) string {
				return testutil.CreateDataDir(t, baseDir, true)
			},
			setupBinaries: false,
			mode:          "fast",
			expectError:   false,
		},
		{
			name: "stop with smart mode",
			setupDataDir: func(baseDir string) string {
				dataDir := testutil.CreateDataDir(t, baseDir, true)
				testutil.CreatePIDFile(t, dataDir, 12345)
				return dataDir
			},
			setupBinaries: true,
			mode:          "smart",
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseDir, cleanup := testutil.TempDir(t, "pgctld_run_stop_test")
			defer cleanup()

			// Setup cleanup for cobra command execution
			cleanupViper := SetupTestPgCtldCleanup(t)
			defer cleanupViper()

			dataDir := tt.setupDataDir(baseDir)

			if tt.setupBinaries {
				binDir := filepath.Join(baseDir, "bin")
				require.NoError(t, os.MkdirAll(binDir, 0o755))
				testutil.CreateMockPostgreSQLBinaries(t, binDir)

				originalPath := os.Getenv("PATH")
				os.Setenv("PATH", binDir+":"+originalPath)
				defer os.Setenv("PATH", originalPath)
			}

			cmd := Root

			// Set up the command arguments
			args := []string{"stop", "--mode", tt.mode}
			if dataDir != "" {
				args = append(args, "--pg-data-dir", dataDir)
			}
			cmd.SetArgs(args)

			err := cmd.Execute()

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestStopPostgreSQLWithConfig(t *testing.T) {
	tests := []struct {
		name          string
		setupDataDir  func(string) string
		setupBinaries bool
		mode          string
		expectError   bool
	}{
		{
			name: "successful stop via config wrapper",
			setupDataDir: func(baseDir string) string {
				dataDir := testutil.CreateDataDir(t, baseDir, true)
				testutil.CreatePIDFile(t, dataDir, 12345)
				return dataDir
			},
			setupBinaries: true,
			mode:          "fast",
			expectError:   false,
		},
		{
			name: "stop when not running via config wrapper",
			setupDataDir: func(baseDir string) string {
				return testutil.CreateDataDir(t, baseDir, true)
			},
			setupBinaries: false,
			mode:          "fast",
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseDir, cleanup := testutil.TempDir(t, "pgctld_stop_config_test")
			defer cleanup()

			dataDir := tt.setupDataDir(baseDir)

			if tt.setupBinaries {
				binDir := filepath.Join(baseDir, "bin")
				require.NoError(t, os.MkdirAll(binDir, 0o755))
				testutil.CreateMockPostgreSQLBinaries(t, binDir)

				originalPath := os.Getenv("PATH")
				os.Setenv("PATH", binDir+":"+originalPath)
				defer os.Setenv("PATH", originalPath)
			}

			config := &PostgresConfig{
				DataDir:  dataDir,
				Host:     "localhost",
				Port:     5432,
				User:     "postgres",
				Database: "postgres",
				Timeout:  30,
			}

			err := StopPostgreSQLWithConfig(config, tt.mode)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestTakeCheckpoint(t *testing.T) {
	tests := []struct {
		name          string
		setupBinaries bool
		config        *PostgresConfig
		expectError   bool
		errorContains string
	}{
		{
			name:          "successful checkpoint",
			setupBinaries: true,
			config: &PostgresConfig{
				Host:     "localhost",
				Port:     5432,
				User:     "postgres",
				Database: "postgres",
				DataDir:  "/tmp/test",
			},
			expectError: false,
		},
		{
			name:          "checkpoint with password",
			setupBinaries: true,
			config: &PostgresConfig{
				Host:     "localhost",
				Port:     5432,
				User:     "postgres",
				Database: "postgres",
				Password: "secret",
				DataDir:  "/tmp/test",
			},
			expectError: false,
		},
		{
			name:          "checkpoint failure - psql command fails",
			setupBinaries: true, // Create failing psql binary
			config: &PostgresConfig{
				Host:     "localhost",
				Port:     5432,
				User:     "postgres",
				Database: "postgres",
				DataDir:  "/tmp/test",
			},
			expectError:   true,
			errorContains: "checkpoint command failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseDir, cleanup := testutil.TempDir(t, "pgctld_checkpoint_test")
			defer cleanup()

			if tt.setupBinaries {
				binDir := filepath.Join(baseDir, "bin")
				require.NoError(t, os.MkdirAll(binDir, 0o755))

				if tt.name == "checkpoint failure - psql command fails" {
					// Create a psql that always fails
					testutil.MockBinary(t, binDir, "psql", "exit 1")
				} else {
					testutil.CreateMockPostgreSQLBinaries(t, binDir)
				}

				originalPath := os.Getenv("PATH")
				os.Setenv("PATH", binDir+":"+originalPath)
				defer os.Setenv("PATH", originalPath)
			}

			err := takeCheckpoint(tt.config)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestStopResult(t *testing.T) {
	t.Run("StopResult struct creation and validation", func(t *testing.T) {
		// Test StopResult struct
		result := &StopResult{
			WasRunning: true,
			Message:    "Test message",
		}

		assert.True(t, result.WasRunning)
		assert.Equal(t, "Test message", result.Message)

		// Test empty result
		emptyResult := &StopResult{}
		assert.False(t, emptyResult.WasRunning)
		assert.Empty(t, emptyResult.Message)
	})
}
