// Copyright 2026 Supabase, Inc.
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

package testutil

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/tools/executil"
)

func TestTempDirCleanupPreservesUnrelatedProcess(t *testing.T) {
	// This child belongs to the test, but is deliberately not registered as a mock.
	// Using a child keeps a regression from killing the test driver itself.
	child := executil.Command(t.Context(), "sleep", "3600")
	require.NoError(t, child.Start())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = child.Stop(ctx)
	})
	go func() { _ = child.Wait() }()

	dir, cleanup := TempDir(t, "pgctld_unrelated_pid_test")
	defer cleanup()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "postmaster.pid"),
		[]byte(strconv.Itoa(child.Process.Pid)+"\n"), 0o600))

	cleanup()

	require.NoError(t, child.Process.Signal(syscall.Signal(0)), "cleanup must not signal an unregistered process")
	_, err := os.Stat(dir)
	require.True(t, os.IsNotExist(err), "cleanup should still remove the directory")
}

func TestTempDirCleanupTracksMockChildren(t *testing.T) {
	for _, action := range []string{"overwrite", "remove"} {
		t.Run(action, func(t *testing.T) {
			dir, cleanup := TempDir(t, "pgctld_mock_pid_test")
			defer cleanup()
			binDir := filepath.Join(dir, "bin")
			require.NoError(t, os.Mkdir(binDir, 0o700))
			CreateMockPostgreSQLBinaries(t, binDir)
			dataDir := CreateDataDir(t, dir, true)

			// Track both helper-created and script-created children, including
			// repeated starts that overwrite postmaster.pid.
			CreatePIDFile(t, dataDir, 0)
			for range 2 {
				cmd := executil.Command(t.Context(), filepath.Join(binDir, "pg_ctl"), "start", "-D", dataDir)
				require.NoError(t, cmd.Run())
			}
			content, err := os.ReadFile(filepath.Join(dataDir, mockPIDFile))
			require.NoError(t, err)
			pids := strings.Fields(string(content))
			require.Len(t, pids, 3)
			processes := make([]*os.Process, 0, len(pids))
			for _, text := range pids {
				pid, err := strconv.Atoi(text)
				require.NoError(t, err)
				process, err := os.FindProcess(pid)
				require.NoError(t, err)
				require.NoError(t, process.Signal(syscall.Signal(0)))
				processes = append(processes, process)
			}

			pidFile := filepath.Join(dataDir, "postmaster.pid")
			if action == "overwrite" {
				require.NoError(t, os.WriteFile(pidFile, []byte(strconv.Itoa(DeadPID)+"\n"), 0o600))
			} else {
				require.NoError(t, os.Remove(pidFile))
			}
			cleanup()
			for _, process := range processes {
				require.Eventually(t, func() bool {
					return process.Signal(syscall.Signal(0)) != nil
				}, 5*time.Second, 10*time.Millisecond, "tracked mock %d must exit", process.Pid)
				require.NoError(t, process.Release())
			}
		})
	}
}
