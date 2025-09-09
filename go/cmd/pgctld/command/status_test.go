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
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
)

func TestRunStatus(t *testing.T) {
	tests := []struct {
		name           string
		setupDataDir   func(string) string
		setupBinaries  bool
		expectError    bool
		errorContains  string
		outputContains []string
	}{
		{
			name: "status not initialized",
			setupDataDir: func(baseDir string) string {
				return testutil.CreateDataDir(t, baseDir, false) // uninitialized
			},
			setupBinaries:  false,
			expectError:    false,
			outputContains: []string{"Status: Not initialized"},
		},
		{
			name: "status stopped",
			setupDataDir: func(baseDir string) string {
				return testutil.CreateDataDir(t, baseDir, true) // initialized but not running
			},
			setupBinaries:  false,
			expectError:    false,
			outputContains: []string{"Status: Stopped"},
		},
		{
			name: "status running",
			setupDataDir: func(baseDir string) string {
				dataDir := testutil.CreateDataDir(t, baseDir, true)
				testutil.CreatePIDFile(t, dataDir, 12345) // create PID file
				return dataDir
			},
			setupBinaries:  true,
			expectError:    false,
			outputContains: []string{"Status: Running", "PID:", "Port: 5432"},
		},
		{
			name: "no data dir specified",
			setupDataDir: func(baseDir string) string {
				return "" // empty data dir
			},
			setupBinaries: false,
			expectError:   true,
			errorContains: "pg-data-dir is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup temporary directories
			baseDir, cleanup := testutil.TempDir(t, "pgctld_status_test")
			defer cleanup()

			// Setup cleanup for cobra command execution
			cleanupViper := SetupTestPgCtldCleanup(t)
			defer cleanupViper()

			dataDir := tt.setupDataDir(baseDir)

			// Setup mock binaries if needed
			if tt.setupBinaries {
				binDir := filepath.Join(baseDir, "bin")
				require.NoError(t, os.MkdirAll(binDir, 0o755))
				testutil.CreateMockPostgreSQLBinaries(t, binDir)

				originalPath := os.Getenv("PATH")
				os.Setenv("PATH", binDir+":"+originalPath)
				defer os.Setenv("PATH", originalPath)
			}

			// Capture output
			oldStdout := os.Stdout
			r, w, _ := os.Pipe()
			os.Stdout = w

			// Use Root command to get all persistent flags and execute status subcommand
			cmd := Root
			args := []string{"status"}
			if dataDir != "" {
				args = append(args, "--pg-data-dir", dataDir)
			}
			cmd.SetArgs(args)

			err := cmd.Execute()
			// Restore stdout and read captured output
			w.Close()
			os.Stdout = oldStdout

			var buf bytes.Buffer
			_, readErr := io.Copy(&buf, r)
			require.NoError(t, readErr)
			output := buf.String()

			if tt.expectError {
				require.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				require.NoError(t, err)

				// Check output contains expected strings
				for _, expectedStr := range tt.outputContains {
					assert.Contains(t, output, expectedStr, "Output should contain: %s", expectedStr)
				}
			}
		})
	}
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

			cleanupViper := SetupTestPgCtldCleanup(t)
			defer cleanupViper()

			result := isServerReady()
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

			originalPath := os.Getenv("PATH")
			os.Setenv("PATH", binDir+":"+originalPath)
			defer os.Setenv("PATH", originalPath)

			cleanupViper := SetupTestPgCtldCleanup(t)
			defer cleanupViper()

			result := getServerVersion()
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
