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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	"github.com/multigres/multigres/go/pgctld"
)

func TestStopPostgreSQLWithResult(t *testing.T) {
	tests := []struct {
		name           string
		setupBinaries  bool
		createPIDFile  bool
		mode           string
		expectError    bool
		errorContains  string
		expectedResult func(*StopResult)
	}{
		{
			name:          "successful stop with fast mode",
			setupBinaries: true,
			createPIDFile: true,
			mode:          "fast",
			expectError:   false,
			expectedResult: func(result *StopResult) {
				assert.True(t, result.WasRunning)
				assert.Equal(t, "PostgreSQL server stopped successfully\n", result.Message)
			},
		},
		{
			name:          "successful stop with smart mode",
			setupBinaries: true,
			createPIDFile: true,
			mode:          "smart",
			expectError:   false,
			expectedResult: func(result *StopResult) {
				assert.True(t, result.WasRunning)
				assert.Equal(t, "PostgreSQL server stopped successfully\n", result.Message)
			},
		},
		{
			name:          "successful stop with immediate mode",
			setupBinaries: true,
			createPIDFile: true,
			mode:          "immediate",
			expectError:   false,
			expectedResult: func(result *StopResult) {
				assert.True(t, result.WasRunning)
				assert.Equal(t, "PostgreSQL server stopped successfully\n", result.Message)
			},
		},
		{
			name:          "stop when PostgreSQL is not running",
			setupBinaries: false,
			createPIDFile: false,
			mode:          "fast",
			expectError:   false,
			expectedResult: func(result *StopResult) {
				assert.False(t, result.WasRunning)
				assert.Equal(t, "PostgreSQL is not running", result.Message)
			},
		},
		{
			name:          "default mode when empty string provided",
			setupBinaries: true,
			createPIDFile: true,
			mode:          "", // Empty mode should default to "fast"
			expectError:   false,
			expectedResult: func(result *StopResult) {
				assert.True(t, result.WasRunning)
				assert.Equal(t, "PostgreSQL server stopped successfully\n", result.Message)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseDir, cleanup := testutil.TempDir(t, "pgctld_stop_test")
			defer cleanup()

			if tt.setupBinaries {
				binDir := filepath.Join(baseDir, "bin")
				require.NoError(t, os.MkdirAll(binDir, 0o755))
				testutil.CreateMockPostgreSQLBinaries(t, binDir)

				originalPath := os.Getenv("PATH")
				os.Setenv("PATH", binDir+":"+originalPath)
				defer os.Setenv("PATH", originalPath)
			}

			// Always create data directory
			dataDir := testutil.CreateDataDir(t, baseDir, true)

			// Conditionally create PID file to simulate running/not running
			if tt.createPIDFile {
				testutil.CreatePIDFile(t, dataDir, 12345)
			}

			poolerDir := baseDir
			pgDataDir := pgctld.PostgresDataDir(poolerDir)
			pgConfigFile := pgctld.PostgresConfigFile(poolerDir)
			config, err := pgctld.NewPostgresCtlConfig(5432, "postgres", "postgres", 30, pgDataDir, pgConfigFile, poolerDir)
			require.NoError(t, err)

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

func TestStopPostgreSQLWithResult_EmptyPoolerDir(t *testing.T) {
	// Create a mock PostgreSQL server config without setting pooler dir
	_, err := pgctld.GeneratePostgresServerConfig("", 5432, "postgres")

	// Should get an error about pooler-dir not being set
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pooler-dir needs to be set")
}

func TestRunStop(t *testing.T) {
	tests := []struct {
		name           string
		setupPoolerDir func(string) string
		setupBinaries  bool
		mode           string
		expectError    bool
		errorContains  string
	}{
		{
			name: "successful stop command",
			setupPoolerDir: func(baseDir string) string {
				testutil.CreateDataDir(t, baseDir, true)
				testutil.CreatePIDFile(t, baseDir, 12345)
				return baseDir
			},
			setupBinaries: true,
			mode:          "fast",
			expectError:   false,
		},
		{
			name: "stop when not running",
			setupPoolerDir: func(baseDir string) string {
				testutil.CreateDataDir(t, baseDir, true)
				// Don't create PID file (not running)
				return baseDir
			},
			setupBinaries: false,
			mode:          "fast",
			expectError:   false,
		},
		{
			name: "stop with smart mode",
			setupPoolerDir: func(baseDir string) string {
				testutil.CreateDataDir(t, baseDir, true)
				testutil.CreatePIDFile(t, baseDir, 12345)
				return baseDir
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

			poolerDir := tt.setupPoolerDir(baseDir)

			if tt.setupBinaries {
				binDir := filepath.Join(baseDir, "bin")
				require.NoError(t, os.MkdirAll(binDir, 0o755))
				testutil.CreateMockPostgreSQLBinaries(t, binDir)

				originalPath := os.Getenv("PATH")
				os.Setenv("PATH", binDir+":"+originalPath)
				defer os.Setenv("PATH", originalPath)
			}

			// Create a fresh root command for each test
			cmd := GetRootCommand()

			// Set up the command arguments
			args := []string{"stop", "--mode", tt.mode}
			args = append(args, "--pooler-dir", poolerDir)
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
		setupBinaries bool
		createPIDFile bool
		mode          string
		expectError   bool
	}{
		{
			name:          "successful stop via config wrapper",
			setupBinaries: true,
			createPIDFile: true,
			mode:          "fast",
			expectError:   false,
		},
		{
			name:          "stop when not running via config wrapper",
			setupBinaries: false,
			createPIDFile: false,
			mode:          "fast",
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseDir, cleanup := testutil.TempDir(t, "pgctld_stop_config_test")
			defer cleanup()

			if tt.setupBinaries {
				binDir := filepath.Join(baseDir, "bin")
				require.NoError(t, os.MkdirAll(binDir, 0o755))
				testutil.CreateMockPostgreSQLBinaries(t, binDir)

				originalPath := os.Getenv("PATH")
				os.Setenv("PATH", binDir+":"+originalPath)
				defer os.Setenv("PATH", originalPath)
			}

			// Create a mock PostgreSQL server config
			pgConfig, err := pgctld.GeneratePostgresServerConfig(baseDir, 5432, "postgres")
			require.NoError(t, err)

			// Always create data directory
			testutil.CreateDataDir(t, baseDir, true)

			// Conditionally create PID file to simulate running/not running
			if tt.createPIDFile {
				testutil.CreatePIDFile(t, pgConfig.DataDir, 12345)
			}

			poolerDir := baseDir
			pgDataDir := pgctld.PostgresDataDir(poolerDir)
			pgConfigFile := pgctld.PostgresConfigFile(poolerDir)

			config, err := pgctld.NewPostgresCtlConfig(5432, "postgres", "postgres", 30, pgDataDir, pgConfigFile, poolerDir)
			require.NoError(t, err)

			err = StopPostgreSQLWithConfig(config, tt.mode)

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
		config        func(baseDir string) *pgctld.PostgresCtlConfig
		expectError   bool
		errorContains string
	}{
		{
			name:          "successful checkpoint",
			setupBinaries: true,
			config: func(baseDir string) *pgctld.PostgresCtlConfig {
				pgDataDir := pgctld.PostgresDataDir(baseDir)
				pgConfigFile := pgctld.PostgresConfigFile(baseDir)
				config, err := pgctld.NewPostgresCtlConfig(5432, "postgres", "postgres", 30, pgDataDir, pgConfigFile, baseDir)
				require.NoError(t, err)
				return config
			},
			expectError: false,
		},
		{
			name:          "checkpoint failure - psql command fails",
			setupBinaries: true, // Create failing psql binary
			config: func(baseDir string) *pgctld.PostgresCtlConfig {
				pgDataDir := pgctld.PostgresDataDir(baseDir)
				pgConfigFile := pgctld.PostgresConfigFile(baseDir)
				config, err := pgctld.NewPostgresCtlConfig(5432, "postgres", "postgres", 30, pgDataDir, pgConfigFile, baseDir)
				require.NoError(t, err)
				return config
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

			err := takeCheckpoint(tt.config(baseDir))

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
