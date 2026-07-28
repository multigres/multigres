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
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/cmd/pgctld/testutil"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/services/pgctld"
)

func TestPgIsReadyTimeoutSecs(t *testing.T) {
	tests := []struct {
		name     string
		ctx      func() context.Context
		expected int
	}{
		{
			name:     "no deadline uses default",
			ctx:      context.Background,
			expected: int(pgIsReadyDefaultTimeout.Seconds()), // 3
		},
		{
			name: "deadline well beyond default returns default",
			ctx: func() context.Context {
				ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
				t.Cleanup(cancel)
				return ctx
			},
			expected: int(pgIsReadyDefaultTimeout.Seconds()), // 3
		},
		{
			name: "deadline tighter than default uses remaining minus buffer",
			ctx: func() context.Context {
				// 4.5 s remaining: 4.5 - 1 (buffer) = 3.5 → truncated to 3
				ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(4*time.Second+500*time.Millisecond))
				t.Cleanup(cancel)
				return ctx
			},
			expected: 3,
		},
		{
			name: "very tight deadline is clamped to minimum 1",
			ctx: func() context.Context {
				// 1.5 s remaining: 1.5 - 1 (buffer) = 0.5 → truncated to 0 → clamped to 1
				ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(1*time.Second+500*time.Millisecond))
				t.Cleanup(cancel)
				return ctx
			},
			expected: 1,
		},
		{
			name: "deadline already at buffer boundary is clamped to minimum 1",
			ctx: func() context.Context {
				// 1.1 s remaining: 1.1 - 1 (buffer) = 0.1 s → truncated to 0 → clamped to 1
				ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(1*time.Second+100*time.Millisecond))
				t.Cleanup(cancel)
				return ctx
			},
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := pgIsReadyTimeoutSecs(tt.ctx())
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestRunStatus(t *testing.T) {
	baseDir, cleanup := testutil.TempDir(t, "pgctld_status_test")
	defer cleanup()

	// Set PGDATA to the expected data directory location (may not exist yet for not_initialized test)
	t.Setenv(constants.PgDataDirEnvVar, filepath.Join(baseDir, "pg_data"))

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

		// Create a fresh root command for each test
		cmd, _ := GetRootCommand()
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

	// Test 2: Stopped (initialized, no PID file). A readable PGDATA with no
	// postmaster.pid is an authoritative "not running", so this must report
	// Stopped without falling back to a connectivity probe.
	//
	// The probe is deliberately not a tiebreaker here: pg_isready -h <socketDir>
	// only proves that *something* answers on that socket directory and port, not
	// that the postmaster for *this* PGDATA is up. A leftover or co-located
	// instance sharing the socket dir would make it a false positive, which is
	// why the readable-PGDATA verdict wins outright.
	t.Run("stopped", func(t *testing.T) {
		testutil.CreateDataDir(t, baseDir, true)
		output, err := runStatusCommand()
		require.NoError(t, err)
		assert.Contains(t, output, "Status: Stopped")
	})

	// Test 2b: Reachable but the PID file cannot be read. This is the part of the
	// #628 case that reaches GetStatusWithResult: PGDATA is traversable (so
	// IsDataDirInitialized succeeds and the callers do not short-circuit to
	// NOT_INITIALIZED), but postmaster.pid is owned by postgres and unreadable,
	// so the local process check is inconclusive while pg_isready still accepts
	// connections. Connectivity is the only evidence available there, so the
	// server must be reported Running rather than Stopped.
	//
	// The other half of #628 — a PGDATA pgctld cannot traverse at all — is
	// intercepted upstream by that gate and never reaches this code.
	t.Run("reachable_with_unreadable_pidfile", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root bypasses file permissions, so an unreadable pidfile cannot be simulated")
		}

		// Cleanup matters here (unlike the equivalent case in TestPostgresLiveness):
		// TestRunStatus shares one baseDir across subtests, so an unreadable
		// postmaster.pid left behind would change what later subtests observe.
		dataDir := testutil.CreateDataDir(t, baseDir, true)
		pidFile := filepath.Join(dataDir, "postmaster.pid")
		require.NoError(t, os.WriteFile(pidFile, []byte("12345\n"), 0o000))
		t.Cleanup(func() { _ = os.Remove(pidFile) })

		// The default mock pg_isready accepts connections.
		output, err := runStatusCommand()
		require.NoError(t, err)
		assert.Contains(t, output, "Status: Running")
	})

	// Test 2c: Inconclusive local check AND nothing answering. The pidfile is
	// unreadable so the process check cannot decide, and pg_isready fails, so the
	// probe's "no" is what settles it. This is the branch that reports Stopped
	// after falling back — distinct from test 2, which never probes at all.
	t.Run("unreachable_with_unreadable_pidfile", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root bypasses file permissions, so an unreadable pidfile cannot be simulated")
		}

		downBinDir := filepath.Join(baseDir, "bin_down")
		require.NoError(t, os.MkdirAll(downBinDir, 0o755))
		testutil.CreateMockPostgreSQLBinaries(t, downBinDir)
		testutil.MockBinary(t, downBinDir, "pg_isready", "exit 1")

		originalPath := os.Getenv("PATH")
		os.Setenv("PATH", downBinDir+":"+originalPath)
		defer os.Setenv("PATH", originalPath)

		dataDir := testutil.CreateDataDir(t, baseDir, true)
		pidFile := filepath.Join(dataDir, "postmaster.pid")
		require.NoError(t, os.WriteFile(pidFile, []byte("12345\n"), 0o000))
		t.Cleanup(func() { _ = os.Remove(pidFile) })

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

	// Test 4: Process running but pg_isready fails (simulates SIGSTOP / cgroup freeze)
	t.Run("running_but_unresponsive", func(t *testing.T) {
		// Override pg_isready to fail so that the process-exists-but-frozen path is exercised
		unresponsiveBinDir := filepath.Join(baseDir, "bin_unresponsive")
		require.NoError(t, os.MkdirAll(unresponsiveBinDir, 0o755))
		testutil.CreateMockPostgreSQLBinaries(t, unresponsiveBinDir)
		testutil.MockBinary(t, unresponsiveBinDir, "pg_isready", "exit 1")

		originalPath := os.Getenv("PATH")
		os.Setenv("PATH", unresponsiveBinDir+":"+originalPath)
		defer os.Setenv("PATH", originalPath)

		dataDir := testutil.CreateDataDir(t, baseDir, true)
		testutil.CreatePIDFile(t, dataDir, 0 /* pid unused, real pid is used internally */)

		output, err := runStatusCommand()
		t.Logf("output: %s", output)
		require.NoError(t, err)
		assert.Contains(t, output, "Status: Stopped")
	})

	// Test 5: No pooler directory - should get an error
	t.Run("no_pooler_dir", func(t *testing.T) {
		cmd, _ := GetRootCommand()
		cmd.SetArgs([]string{"status", "--pooler-dir", ""})
		err := cmd.Execute()

		require.Error(t, err)
		assert.Contains(t, err.Error(), "pooler-dir needs to be set")
	})
}

// TestNoteCrossUserStatus covers the dedup contract: quiet while the condition
// persists, but re-armed once it clears — so a transient malformed postmaster.pid
// during startup cannot permanently silence a real cross-user misconfiguration
// that appears later.
func TestNoteCrossUserStatus(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	warnings := func() int { return strings.Count(buf.String(), "local process check was inconclusive") }

	crossUserStatusReported.Store(false)
	t.Cleanup(func() { crossUserStatusReported.Store(false) })

	noteCrossUserStatus(t.Context(), logger, "/pgdata", true)
	require.Equal(t, 1, warnings(), "first occurrence should warn")

	for range 5 {
		noteCrossUserStatus(t.Context(), logger, "/pgdata", true)
	}
	assert.Equal(t, 1, warnings(), "should stay quiet while the condition persists")

	noteCrossUserStatus(t.Context(), logger, "/pgdata", false)
	assert.Equal(t, 1, warnings(), "clearing the condition should not warn")

	noteCrossUserStatus(t.Context(), logger, "/pgdata", true)
	assert.Equal(t, 2, warnings(), "recurrence after clearing should warn again")
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
				5432,
				"postgres",
				"postgres",
				30,
				pgctld.PostgresDataDir(),
				pgctld.PostgresConfigFile(),
				baseDir,
				"localhost",
				pgctld.PostgresSocketDir(baseDir),
			)
			require.NoError(t, err)

			result := isServerReadyWithConfig(t.Context(), config)
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
				5432,
				"postgres",
				"postgres",
				30,
				pgctld.PostgresDataDir(),
				pgctld.PostgresConfigFile(),
				baseDir,
				"localhost",
				pgctld.PostgresSocketDir(baseDir),
			)
			require.NoError(t, err)

			result := getServerVersionWithConfig(t.Context(), config)
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
