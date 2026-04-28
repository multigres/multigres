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
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/services/pgctld"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
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

			// Create a fresh root command for each test
			cmd, _ := GetRootCommand()

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
				nonExistent := filepath.Join(baseDir, "nonexistent")
				t.Setenv(constants.PgDataDirEnvVar, nonExistent)
				return nonExistent
			},
			initialized: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseDir, cleanup := testutil.TempDir(t, "pgctld_init_test")
			defer cleanup()

			tt.setupDir(baseDir)
			result := pgctld.IsDataDirInitialized()
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

		// baseDir serves as poolerDir; dataDir will be poolerDir/pg_data
		poolerDir := baseDir
		t.Setenv(constants.PgDataDirEnvVar, filepath.Join(poolerDir, "pg_data"))

		// Setup mock initdb binary
		binDir := filepath.Join(baseDir, "bin")
		require.NoError(t, os.MkdirAll(binDir, 0o755))
		testutil.CreateMockPostgreSQLBinaries(t, binDir)

		// Add to PATH for test
		originalPath := os.Getenv("PATH")
		os.Setenv("PATH", binDir+":"+originalPath)
		defer os.Setenv("PATH", originalPath)

		logger := slog.New(slog.DiscardHandler)
		cfg := PgCtldServiceConfig{
			User:     constants.DefaultPostgresUser,
			Password: shardsetup.TestPostgresPassword,
		}
		err := initializeDataDir(logger, cfg)
		require.NoError(t, err)

		// Verify directory was created (dataDir is poolerDir/pg_data)
		dataDir := filepath.Join(poolerDir, "pg_data")
		assert.DirExists(t, dataDir)

		// Verify PG_VERSION file exists (created by mock)
		assert.FileExists(t, filepath.Join(dataDir, "PG_VERSION"))
	})

	t.Run("fails with invalid directory permissions", func(t *testing.T) {
		// Try to create data dir in a read-only location (simulate permission
		// error). The function initializeDataDir reads the PGDATA env var to
		// determine where to initialize, so we set it to a location that should
		// fail.
		t.Setenv(constants.PgDataDirEnvVar, "/root/impossible_dir")
		logger := slog.New(slog.DiscardHandler)
		cfg := PgCtldServiceConfig{
			User:     constants.DefaultPostgresUser,
			Password: shardsetup.TestPostgresPassword,
		}
		err := initializeDataDir(logger, cfg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "initdb failed")
	})

	t.Run("extra initdb args are forwarded to initdb", func(t *testing.T) {
		baseDir, cleanup := testutil.TempDir(t, "pgctld_initdb_args_test")
		defer cleanup()

		argsFile := filepath.Join(baseDir, "initdb-args.txt")

		// Mock initdb that records its arguments to a file so we can assert on them.
		binDir := filepath.Join(baseDir, "bin")
		require.NoError(t, os.MkdirAll(binDir, 0o755))
		testutil.MockBinary(t, binDir, "initdb", `
echo "$@" > `+argsFile+`
mkdir -p "$2/base"
echo "15.0" > "$2/PG_VERSION"
touch "$2/postgresql.conf"
touch "$2/pg_hba.conf"
`)

		t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))
		t.Setenv(constants.PgDataDirEnvVar, filepath.Join(baseDir, "pg_data"))

		logger := slog.New(slog.DiscardHandler)
		cfg := PgCtldServiceConfig{
			User:       constants.DefaultPostgresUser,
			InitdbArgs: "--locale-provider=icu --icu-locale=en_US.UTF-8",
		}
		err := initializeDataDir(logger, cfg)
		require.NoError(t, err)

		argsBytes, err := os.ReadFile(argsFile)
		require.NoError(t, err)
		argsOut := string(argsBytes)
		assert.Contains(t, argsOut, "--locale-provider=icu")
		assert.Contains(t, argsOut, "--icu-locale=en_US.UTF-8")
	})
}

func TestRunInitDbSQLFiles(t *testing.T) {
	t.Run("executes each file in order with expected psql args", func(t *testing.T) {
		baseDir, cleanup := testutil.TempDir(t, "pgctld_init_sql_test")
		defer cleanup()

		argsLog := filepath.Join(baseDir, "psql-args.log")

		binDir := filepath.Join(baseDir, "bin")
		require.NoError(t, os.MkdirAll(binDir, 0o755))
		testutil.MockBinary(t, binDir, "psql", `echo "$@" >> `+argsLog)
		t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))

		fileA := filepath.Join(baseDir, "a.sql")
		fileB := filepath.Join(baseDir, "b.sql")
		require.NoError(t, os.WriteFile(fileA, []byte("CREATE TABLE a();"), 0o644))
		require.NoError(t, os.WriteFile(fileB, []byte("CREATE TABLE b();"), 0o644))

		logger := slog.New(slog.DiscardHandler)
		pg := &pgInstance{
			socketDir: "/tmp",
			port:      5432,
			user:      "postgres",
			logger:    logger,
		}
		err := runInitDbSQLFiles(logger, pg, "mydb", []string{fileA, fileB})
		require.NoError(t, err)

		logBytes, err := os.ReadFile(argsLog)
		require.NoError(t, err)
		lines := strings.Split(strings.TrimSpace(string(logBytes)), "\n")
		require.Len(t, lines, 2, "expected one psql invocation per file")

		assert.Contains(t, lines[0], fileA, "first invocation should reference a.sql")
		assert.Contains(t, lines[1], fileB, "second invocation should reference b.sql")
		for _, line := range lines {
			assert.Contains(t, line, "ON_ERROR_STOP=1")
			assert.Contains(t, line, "-d mydb")
		}
	})

	t.Run("returns error on missing file without invoking psql", func(t *testing.T) {
		logger := slog.New(slog.DiscardHandler)
		// nil pgInstance is safe here: os.Stat runs before the first psql call.
		err := runInitDbSQLFiles(logger, nil, "mydb", []string{"/nonexistent/file.sql"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not accessible")
	})

	t.Run("aborts on first file failure and skips remaining", func(t *testing.T) {
		baseDir, cleanup := testutil.TempDir(t, "pgctld_init_sql_fail_test")
		defer cleanup()

		runLog := filepath.Join(baseDir, "psql-runs.log")

		binDir := filepath.Join(baseDir, "bin")
		require.NoError(t, os.MkdirAll(binDir, 0o755))
		testutil.MockBinary(t, binDir, "psql", `
echo "$@" >> `+runLog+`
if [[ "$*" == *"fail.sql"* ]]; then
    echo "mock psql: simulated failure" >&2
    exit 1
fi
`)
		t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))

		failFile := filepath.Join(baseDir, "fail.sql")
		nextFile := filepath.Join(baseDir, "next.sql")
		require.NoError(t, os.WriteFile(failFile, []byte("BAD"), 0o644))
		require.NoError(t, os.WriteFile(nextFile, []byte("OK"), 0o644))

		logger := slog.New(slog.DiscardHandler)
		pg := &pgInstance{
			socketDir: "/tmp",
			port:      5432,
			user:      "postgres",
			logger:    logger,
		}
		err := runInitDbSQLFiles(logger, pg, "mydb", []string{failFile, nextFile})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "fail.sql")

		logBytes, err := os.ReadFile(runLog)
		require.NoError(t, err)
		got := string(logBytes)
		assert.Contains(t, got, "fail.sql", "first file should have been invoked")
		assert.NotContains(t, got, "next.sql", "remaining files must not be invoked after a failure")
	})
}

func TestReadLogTail(t *testing.T) {
	tests := []struct {
		name     string
		content  string
		lines    int
		expected string
	}{
		{
			name:     "normal case with trailing newline",
			content:  "line1\nline2\nline3\nline4\nline5\n",
			lines:    3,
			expected: "line3\nline4\nline5",
		},
		{
			name:     "fewer lines than requested",
			content:  "line1\nline2\n",
			lines:    5,
			expected: "line1\nline2",
		},
		{
			name:     "empty file",
			content:  "",
			lines:    5,
			expected: "(empty log file)",
		},
		{
			name:     "whitespace only",
			content:  "  \n\n  \n",
			lines:    5,
			expected: "(empty log file)",
		},
		{
			name:     "exact number of lines",
			content:  "line1\nline2\nline3",
			lines:    3,
			expected: "line1\nline2\nline3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temp file with content
			tmpDir, cleanup := testutil.TempDir(t, "log_tail_test")
			defer cleanup()

			logPath := filepath.Join(tmpDir, "test.log")
			err := os.WriteFile(logPath, []byte(tt.content), 0o644)
			require.NoError(t, err)

			result := readLogTail(logPath, tt.lines)
			assert.Equal(t, tt.expected, result)
		})
	}

	t.Run("file not found", func(t *testing.T) {
		result := readLogTail("/nonexistent/path/log.txt", 10)
		assert.Contains(t, result, "failed to read log")
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
			5432,
			constants.DefaultPostgresUser,
			constants.DefaultPostgresDatabase,
			30, // timeout
			pgctld.PostgresDataDir(),
			pgctld.PostgresConfigFile(),
			baseDir,
			"localhost",
			pgctld.PostgresSocketDir(baseDir),
		)
		require.NoError(t, err)

		logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
		err = waitForPostgreSQLWithConfig(logger, config)
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
			5432,
			"postgres",
			"postgres",
			1, // 1 second timeout
			pgctld.PostgresDataDir(),
			pgctld.PostgresConfigFile(),
			baseDir,
			"localhost",
			pgctld.PostgresSocketDir(baseDir),
		)
		require.NoError(t, err)

		logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
		err = waitForPostgreSQLWithConfig(logger, config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "did not become ready")
	})
}

func TestWaitForPostgreSQLCrashDetection(t *testing.T) {
	t.Run("detects crashed process", func(t *testing.T) {
		baseDir, cleanup := testutil.TempDir(t, "pgctld_crash_test")
		defer cleanup()

		// Create initialized data directory
		dataDir := testutil.CreateDataDir(t, baseDir, true)

		// Create PID file with non-existent PID (simulates crashed process)
		testutil.CreateDeadPIDFile(t, dataDir, 999999)

		// Create mock pg_isready that always fails
		binDir := filepath.Join(baseDir, "bin")
		require.NoError(t, os.MkdirAll(binDir, 0o755))
		testutil.MockBinary(t, binDir, "pg_isready", "exit 1")

		originalPath := os.Getenv("PATH")
		os.Setenv("PATH", binDir+":"+originalPath)
		defer os.Setenv("PATH", originalPath)

		config, err := pgctld.NewPostgresCtlConfig(
			5432,
			"postgres",
			"postgres",
			5, // 5 second timeout
			pgctld.PostgresDataDir(),
			pgctld.PostgresConfigFile(),
			baseDir,
			"localhost",
			pgctld.PostgresSocketDir(baseDir),
		)
		require.NoError(t, err)

		logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
		err = waitForPostgreSQLWithConfig(logger, config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "crashed")
	})
}
