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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStanzaCreate_ExecPaths drives StanzaCreate against a stubbed pgbackrest,
// covering the success (exit 0) and failure (non-zero) paths.
func TestStanzaCreate_ExecPaths(t *testing.T) {
	tests := []struct {
		name        string
		exitCode    int
		wantErr     bool
		errContains string
	}{
		{name: "success on exit 0", exitCode: 0},
		{name: "failure on nonzero exit", exitCode: 1, wantErr: true, errContains: "pgbackrest stanza-create failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stubPgbackrest(t, fmt.Sprintf("#!/bin/bash\nexit %d\n", tt.exitCode))
			poolerDir := t.TempDir()
			e, _ := newTestEngine(t, poolerDir, "", "", "/tmp/backups")
			e.SetConfigPath(setupMockPgBackRestConfig(t, poolerDir))

			err := e.StanzaCreate(context.Background())
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errContains)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestStanzaDelete_ExecPaths checks the command sequence and that the stanza is
// started again after a failed delete.
func TestStanzaDelete_ExecPaths(t *testing.T) {
	tests := []struct {
		name        string
		failOn      string
		wantErr     bool
		errContains string
		wantCalls   []string
	}{
		{
			name:      "stops, deletes with --force, then starts",
			wantCalls: []string{"stop", "--force_stanza-delete", "start"},
		},
		{
			name:        "starts the stanza again when the delete fails",
			failOn:      "stanza-delete",
			wantErr:     true,
			errContains: "pgbackrest stanza-delete failed",
			wantCalls:   []string{"stop", "--force_stanza-delete", "start"},
		},
		{
			name:        "does not delete when stop fails",
			failOn:      "stop",
			wantErr:     true,
			errContains: "pgbackrest stop failed",
			wantCalls:   []string{"stop"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			callLog := filepath.Join(t.TempDir(), "calls")
			stubPgbackrest(t, fmt.Sprintf(`#!/bin/bash
args=("$@")
last="${args[${#args[@]}-1]}"
case "$*" in
  *--force*stanza-delete*) echo "--force stanza-delete" >> %[1]s ;;
  *) echo "$last" >> %[1]s ;;
esac
if [ "$last" = %[2]q ]; then exit 1; fi
exit 0
`, callLog, tt.failOn))
			poolerDir := t.TempDir()
			e, _ := newTestEngine(t, poolerDir, "", "", "/tmp/backups")
			e.SetConfigPath(setupMockPgBackRestConfig(t, poolerDir))

			err := e.StanzaDelete(context.Background())
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errContains)
			} else {
				require.NoError(t, err)
			}

			data, readErr := os.ReadFile(callLog)
			require.NoError(t, readErr)
			require.Equal(t, tt.wantCalls, strings.Fields(strings.ReplaceAll(strings.TrimSpace(string(data)), "--force stanza-delete", "--force_stanza-delete")))
		})
	}
}

// TestStanzaDelete_IOTimeoutOnlyOnRepoCommands checks stanza-delete gets
// --io-timeout while stop and start, which reject it, do not.
func TestStanzaDelete_IOTimeoutOnlyOnRepoCommands(t *testing.T) {
	callLog := filepath.Join(t.TempDir(), "calls")
	stubPgbackrest(t, fmt.Sprintf("#!/bin/bash\necho \"$*\" >> %s\n", callLog))
	poolerDir := t.TempDir()
	e, _ := newTestEngine(t, poolerDir, "", "", "/tmp/backups")
	e.SetConfigPath(setupMockPgBackRestConfig(t, poolerDir))

	require.NoError(t, e.StanzaDelete(context.Background()))

	data, err := os.ReadFile(callLog)
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	require.Len(t, lines, 3)
	assert.NotContains(t, lines[0], "--io-timeout")
	assert.True(t, strings.HasPrefix(lines[1], "--io-timeout=19 "), lines[1])
	assert.Contains(t, lines[1], "stanza-delete")
	assert.NotContains(t, lines[2], "--io-timeout")
}

func TestStanzaDelete_StartFailureAfterSuccessfulDeleteIsReturned(t *testing.T) {
	stubPgbackrest(t, "#!/bin/bash\nlast=\"${@: -1}\"\nif [ \"$last\" = start ]; then exit 1; fi\nexit 0\n")
	poolerDir := t.TempDir()
	e, _ := newTestEngine(t, poolerDir, "", "", "/tmp/backups")
	e.SetConfigPath(setupMockPgBackRestConfig(t, poolerDir))

	err := e.StanzaDelete(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pgbackrest start failed")
}

func TestStanzaStart(t *testing.T) {
	callLog := filepath.Join(t.TempDir(), "calls")
	stubPgbackrest(t, fmt.Sprintf("#!/bin/bash\necho \"$*\" >> %s\n", callLog))
	poolerDir := t.TempDir()
	e, _ := newTestEngine(t, poolerDir, "", "", "/tmp/backups")
	e.SetConfigPath(setupMockPgBackRestConfig(t, poolerDir))

	require.NoError(t, e.StanzaStart(context.Background()))

	data, err := os.ReadFile(callLog)
	require.NoError(t, err)
	line := strings.TrimSpace(string(data))
	assert.True(t, strings.HasSuffix(line, " start"), line)
	assert.NotContains(t, line, "--io-timeout", "start rejects --io-timeout")
}

func TestStanzaStart_ErrorOnFailure(t *testing.T) {
	stubPgbackrest(t, "#!/bin/bash\nexit 1\n")
	poolerDir := t.TempDir()
	e, _ := newTestEngine(t, poolerDir, "", "", "/tmp/backups")
	e.SetConfigPath(setupMockPgBackRestConfig(t, poolerDir))

	err := e.StanzaStart(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pgbackrest start failed")
}

func TestIsStaleStanzaError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "info files do not match", err: errors.New("stanza-create failed\nERROR: [028]: backup and archive info files exist but do not match the database"), want: true},
		{name: "info file missing", err: errors.New("stanza-create failed\nERROR: [055]: unable to open missing file backup.info"), want: true},
		{name: "stop file present", err: errors.New("ERROR: [062]: stop file exists for stanza multigres"), want: false},
		{name: "cannot connect", err: errors.New("ERROR: [039]: unable to connect"), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsStaleStanzaError(tt.err))
		})
	}
}

func TestStanzaDelete_ErrorsWhenConfigPathMissing(t *testing.T) {
	e, _ := newTestEngine(t, t.TempDir(), "", "", "/tmp/backups")
	err := e.StanzaDelete(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not yet generated")
}

func TestStanzaCreate_ErrorsWhenConfigPathMissing(t *testing.T) {
	e, _ := newTestEngine(t, t.TempDir(), "", "", "/tmp/backups")
	err := e.StanzaCreate(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not yet generated")
}

// TestSetPgpassPath_PropagatesToCommand verifies SetPgpassPath exports
// PGPASSFILE to pgbackrest's environment. The stub echoes the variable and
// fails, so the error output reveals what the command saw.
func TestSetPgpassPath_PropagatesToCommand(t *testing.T) {
	stubPgbackrest(t, "#!/bin/bash\necho \"PGPASSFILE=$PGPASSFILE\"\nexit 1\n")
	poolerDir := t.TempDir()
	e, _ := newTestEngine(t, poolerDir, "", "", "/tmp/backups")
	e.SetConfigPath(setupMockPgBackRestConfig(t, poolerDir))
	e.SetPgpassPath("/tmp/test.pgpass")

	err := e.StanzaCreate(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "PGPASSFILE=/tmp/test.pgpass")
}

// TestStanzaDelete_StartRunsAfterContextIsCancelled checks the stanza is started
// again even if the caller's context is cancelled during the delete.
func TestStanzaDelete_StartRunsAfterContextIsCancelled(t *testing.T) {
	dir := t.TempDir()
	callLog := filepath.Join(dir, "calls")
	deleteStarted := filepath.Join(dir, "delete-started")
	stubPgbackrest(t, fmt.Sprintf(`#!/bin/bash
last="${@: -1}"
echo "$last" >> %[1]s
if [ "$last" = stanza-delete ]; then
  touch %[2]s
  exec sleep 30
fi
exit 0
`, callLog, deleteStarted))
	poolerDir := t.TempDir()
	e, _ := newTestEngine(t, poolerDir, "", "", "/tmp/backups")
	e.SetConfigPath(setupMockPgBackRestConfig(t, poolerDir))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- e.StanzaDelete(ctx) }()

	require.Eventually(t, func() bool {
		_, err := os.Stat(deleteStarted)
		return err == nil
	}, 10*time.Second, 20*time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "pgbackrest stanza-delete failed")
	case <-time.After(10 * time.Second):
		t.Fatal("StanzaDelete did not return after its context was cancelled")
	}

	data, err := os.ReadFile(callLog)
	require.NoError(t, err)
	assert.Equal(t, []string{"stop", "stanza-delete", "start"}, strings.Fields(string(data)))
}

func TestStanzaDelete_DeleteErrorWinsWhenStartAlsoFails(t *testing.T) {
	stubPgbackrest(t, "#!/bin/bash\nlast=\"${@: -1}\"\nif [ \"$last\" = stanza-delete ] || [ \"$last\" = start ]; then exit 1; fi\nexit 0\n")
	poolerDir := t.TempDir()
	e, _ := newTestEngine(t, poolerDir, "", "", "/tmp/backups")
	e.SetConfigPath(setupMockPgBackRestConfig(t, poolerDir))

	err := e.StanzaDelete(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pgbackrest stanza-delete failed")
	assert.NotContains(t, err.Error(), "pgbackrest start failed")
}

func TestStanzaStart_ErrorsWhenConfigPathMissing(t *testing.T) {
	e, _ := newTestEngine(t, t.TempDir(), "", "", "/tmp/backups")
	err := e.StanzaStart(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not yet generated")
}
