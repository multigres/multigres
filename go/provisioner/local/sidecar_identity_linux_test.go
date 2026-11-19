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

package local

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/tools/executil"
)

// fakeProcFor builds a HOST_PROC tree gopsutil will read for pid: copies of
// the process's own files, plus the three inputs gopsutil estimates boot time
// from. self/cgroup marks the host as a container guest, where the estimate is
// "now minus uptime" truncated to a second; outside a container it is btime.
func fakeProcFor(t *testing.T, pid int, uptime string, btime int64) string {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, strconv.Itoa(pid))
	require.NoError(t, os.MkdirAll(dir, 0o755))
	for _, name := range []string{"stat", "cmdline", "status"} {
		b, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), name))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), b, 0o644))
	}
	require.NoError(t, os.MkdirAll(filepath.Join(root, "self"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "self", "cgroup"), []byte("0::/docker/fixture\n"), 0o644))
	setBootTimeInputs(t, root, uptime, btime)
	return root
}

func setBootTimeInputs(t *testing.T, root, uptime string, btime int64) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(root, "uptime"), []byte(uptime+" 0.00\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "stat"), fmt.Appendf(nil, "cpu  0 0 0 0 0 0 0 0 0 0\nbtime %d\n", btime), 0o644))
}

// A recorded sidecar must still be recognised after the host's idea of its
// boot time moves. Boot time is not stored by the kernel: it is estimated from
// the wall clock, so it moves when the clock is stepped or slewed, and in a
// container the estimate "now minus uptime" flips between two seconds whenever
// its fractional part sits at a second boundary. An identity derived from it
// would make a live, unchanged sidecar look like a recycled PID.
func TestSidecarIdentityDoesNotDependOnBootTime(t *testing.T) {
	pid := startSleeper(t)

	root := fakeProcFor(t, pid, "1000.00", 1_700_000_000)
	t.Setenv("HOST_PROC", root)
	owner, err := readSidecarIdentity(pid)
	require.NoError(t, err)

	// One second of difference in either estimate, as a stepped clock or a
	// fraction straddling the boundary produces.
	setBootTimeInputs(t, root, "998.50", 1_700_000_001)
	again, err := readSidecarIdentity(pid)
	require.NoError(t, err)
	require.Equal(t, owner.StartTime, again.StartTime, "the same process must keep its identity")
	alive, err := owner.alive()
	require.NoError(t, err, "a live sidecar must not be reported as a recycled PID")
	require.True(t, alive)
}

func TestParseStatStartTime(t *testing.T) {
	tail := " S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 4242 20 21\n"
	for name, comm := range map[string]string{
		"plain":       "(sleep)",
		"spaces":      "(a b c)",
		"parentheses": "(evil) R 9 9 9 (x)",
	} {
		t.Run(name, func(t *testing.T) {
			got, err := parseStatStartTime([]byte("77 " + comm + tail))
			require.NoError(t, err)
			assert.Equal(t, int64(4242), got)
		})
	}
	for name, line := range map[string]string{
		"empty":       "",
		"no comm":     "77 sleep S 1 2 3",
		"truncated":   "77 (sleep) S 1 2 3",
		"not numeric": "77 (sleep) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 soon 20",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parseStatStartTime([]byte(line))
			require.Error(t, err)
		})
	}
}

// The value is the kernel's field, read as it stands: it must equal what
// /proc shows for a process whose name makes the line awkward to split.
func TestProcessStartTime_AwkwardCommandName(t *testing.T) {
	sleep, err := exec.LookPath("sleep")
	require.NoError(t, err)
	bin := filepath.Join(t.TempDir(), "a b) (c")
	raw, err := os.ReadFile(sleep)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(bin, raw, 0o755))
	cmd := executil.Command(context.Background(), bin, "300")
	require.NoError(t, cmd.Start())
	t.Cleanup(func() { _, _ = cmd.Stop(context.Background()); _ = cmd.Wait() })

	got, err := processStartTime(cmd.Process.Pid)
	require.NoError(t, err)
	self, err := processStartTime(os.Getpid())
	require.NoError(t, err)
	assert.Greater(t, got, self, "the child started after this test process")
	stat, err := os.ReadFile("/proc/" + strconv.Itoa(cmd.Process.Pid) + "/stat")
	require.NoError(t, err)
	assert.Contains(t, string(stat), " "+strconv.FormatInt(got, 10)+" ")
}
