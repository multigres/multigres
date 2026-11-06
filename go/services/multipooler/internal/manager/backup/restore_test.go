// Copyright 2026 Supabase, Inc.
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

package backup

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/tools/telemetry"
)

// TestShellQuoteSingle round-trips values through an actual shell (not just
// string comparison against a hand-computed expectation), since a bug in the
// escaping logic could just as easily be mirrored in a hardcoded expectation.
func TestShellQuoteSingle(t *testing.T) {
	tests := []string{
		"/tmp/pooler/restore_command.pid",
		"/tmp/pooler's dir/restore_command.pid",
		"'leading and trailing quotes'",
		"has spaces and\ttabs",
	}
	for _, raw := range tests {
		t.Run(raw, func(t *testing.T) {
			out, err := exec.Command("sh", "-c", "printf '%s' "+shellQuoteSingle(raw)).Output()
			require.NoError(t, err)
			assert.Equal(t, raw, string(out))
		})
	}
}

// TestRestore_ExecPaths drives Restore against a stubbed pgbackrest, covering
// the success and failure paths both with and without an explicit backup ID.
func TestRestore_ExecPaths(t *testing.T) {
	tests := []struct {
		name        string
		exitCode    int
		backupID    string
		wantErr     bool
		errContains string
	}{
		{name: "success with backup id", exitCode: 0, backupID: "20250104-100000F"},
		{name: "success without backup id", exitCode: 0, backupID: ""},
		{name: "failure on nonzero exit", exitCode: 1, backupID: "20250104-100000F", wantErr: true, errContains: "pgbackrest restore failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stubPgbackrest(t, fmt.Sprintf("#!/bin/bash\nexit %d\n", tt.exitCode))
			poolerDir := t.TempDir()
			e, _ := newTestEngine(t, poolerDir, "", "", "/tmp/backups")
			e.SetConfigPath(setupMockPgBackRestConfig(t, poolerDir))

			err := e.Restore(context.Background(), tt.backupID, poolerDir)
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errContains)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestRestore_ErrorsWhenConfigPathMissing(t *testing.T) {
	poolerDir := t.TempDir()
	e, _ := newTestEngine(t, poolerDir, "", "", "/tmp/backups")
	err := e.Restore(context.Background(), "20250104-100000F", poolerDir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pgbackrest config not found")
	assert.True(t, e.Health().Snapshot().RestoreInProgressStart.IsZero())
}

// Observe the real execution path while the command is blocked, then verify
// that every outcome clears the gauges. Repeated collection must not consume
// the timer, and restore activity must not affect the backup timer.
func TestRestore_InProgressMetrics(t *testing.T) {
	for _, tt := range []struct {
		name     string
		exitCode int
		cancel   bool
	}{
		{name: "success"},
		{name: "failure", exitCode: 1},
		{name: "cancellation", cancel: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			setup := telemetry.SetupTestTelemetry(t)
			require.NoError(t, setup.Telemetry.InitTelemetry(t.Context(), "test-service"))
			t.Cleanup(func() { _ = setup.Telemetry.ShutdownTelemetry(context.Background()) })
			reader := setup.MetricReader
			poolerDir := t.TempDir()
			started := filepath.Join(poolerDir, "started")
			release := filepath.Join(poolerDir, "release")
			t.Setenv("RESTORE_TEST_STARTED", started)
			t.Setenv("RESTORE_TEST_RELEASE", release)
			stubPgbackrest(t, fmt.Sprintf(`#!/bin/sh
: > "$RESTORE_TEST_STARTED"
while [ ! -f "$RESTORE_TEST_RELEASE" ]; do sleep 0.01; done
exit %d
`, tt.exitCode))
			e, _ := newTestEngine(t, poolerDir, "", "", "/tmp/backups")
			e.SetConfigPath(setupMockPgBackRestConfig(t, poolerDir))

			assertIdle := func() {
				t.Helper()
				active, ok := gaugeInt64(t, reader, "pgbackrest.restore.in_progress")
				require.True(t, ok)
				assert.Zero(t, active)
				elapsed, ok := gaugeFloat64(t, reader, "pgbackrest.restore.in_progress_duration_seconds")
				require.True(t, ok)
				assert.Zero(t, elapsed)
			}
			assertIdle()

			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			finished := make(chan struct{})
			go func() {
				defer close(finished)
				done <- e.Restore(ctx, "20250104-100000F", poolerDir)
			}()
			t.Cleanup(func() {
				cancel()
				select {
				case <-finished:
				case <-time.After(5 * time.Second):
					t.Error("restore command did not stop")
				}
			})
			require.Eventually(t, func() bool {
				_, err := os.Stat(started)
				return err == nil
			}, 5*time.Second, 5*time.Millisecond)

			active, ok := gaugeInt64(t, reader, "pgbackrest.restore.in_progress")
			require.True(t, ok)
			assert.Equal(t, int64(1), active)
			elapsed, ok := gaugeFloat64(t, reader, "pgbackrest.restore.in_progress_duration_seconds")
			require.True(t, ok)
			assert.Positive(t, elapsed)
			nextElapsed, ok := gaugeFloat64(t, reader, "pgbackrest.restore.in_progress_duration_seconds")
			require.True(t, ok)
			assert.GreaterOrEqual(t, nextElapsed, elapsed, "collection must not clear the timer")
			backupActive, ok := gaugeInt64(t, reader, "pgbackrest.backup.in_progress")
			require.True(t, ok)
			assert.Zero(t, backupActive)

			if tt.cancel {
				cancel()
			} else {
				require.NoError(t, os.WriteFile(release, nil, 0o600))
			}
			select {
			case err := <-done:
				if tt.cancel || tt.exitCode != 0 {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("restore did not return")
			}
			assert.True(t, e.Health().Snapshot().RestoreInProgressStart.IsZero())
			assertIdle()
		})
	}
}

func TestRestoreCommand(t *testing.T) {
	e := NewEngine(slog.New(slog.DiscardHandler), nil, nil, Settings{})

	_, err := e.RestoreCommand("/pooler")
	require.Error(t, err, "no pgbackrest config yet")

	e.SetConfigPath("/etc/pgbackrest.conf")
	cmd, err := e.RestoreCommand("/pooler")
	require.NoError(t, err)
	assert.Equal(t,
		`pgctld restore-wrapper '/pooler/restore_command.pid' -- pgbackrest --stanza='`+stanzaName+`' --config='/etc/pgbackrest.conf' archive-get %f "%p"`,
		cmd)
}
