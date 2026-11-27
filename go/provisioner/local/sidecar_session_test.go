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
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const sidecarLauncherDirEnv = "MULTIGRES_TEST_SIDECAR_LAUNCHER_DIR"

// TestSidecarLauncherHelper is not a test. TestSidecarIsOutOfReachOfTheLaunchersTerminal
// re-execs the test binary into it to get a launcher process with a process
// group of its own, the way a shell runs `multigres cluster start`. It starts a
// sidecar through the real launch path and then stays busy, as a provisioner
// does while it brings up the rest of the cluster.
func TestSidecarLauncherHelper(t *testing.T) {
	dir := os.Getenv(sidecarLauncherDirEnv)
	if dir == "" {
		t.Skip("helper process for TestSidecarIsOutOfReachOfTheLaunchersTerminal")
	}
	paths := NewPgBackRestdPaths(filepath.Join(dir, "pooler"))
	binary, err := os.Executable()
	require.NoError(t, err)
	lock, err := paths.lockOwner()
	require.NoError(t, err)
	pid, err := paths.ensureSidecar(context.Background(), binary, fixtureArgs(paths))
	require.NoError(t, err)
	require.NoError(t, lock.Close())
	require.NoError(t, os.WriteFile(filepath.Join(dir, "sidecar.pid"), []byte(strconv.Itoa(pid)), 0o600))
	time.Sleep(time.Minute)
}

// A Ctrl-C in the terminal that runs `multigres cluster start` sends SIGINT to
// every process of the foreground process group. pgbackrestd must not be one
// of them: interrupted while it starts a pgBackRest command it can be left
// unable to prove its workers stopped, and it then refuses to start again. The
// launcher therefore starts it in a session of its own.
//
// The sidecar here is a test binary, which exits on SIGINT like any Go program
// that does not handle it.
func TestSidecarIsOutOfReachOfTheLaunchersTerminal(t *testing.T) {
	paths := ownerTestPaths(t)
	dir := filepath.Dir(paths.Dir)
	self, err := os.Executable()
	require.NoError(t, err)

	launcher := exec.Command(self, "-test.run=^TestSidecarLauncherHelper$")
	launcher.Env = append(os.Environ(), sidecarLauncherDirEnv+"="+dir)
	launcher.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	launcher.Stdout, launcher.Stderr = os.Stdout, os.Stderr
	require.NoError(t, launcher.Start())
	launcherExited := make(chan struct{})
	go func() { _ = launcher.Wait(); close(launcherExited) }()
	t.Cleanup(func() {
		_ = syscall.Kill(-launcher.Process.Pid, syscall.SIGKILL)
		_ = paths.stopOwned(context.Background())
	})

	var sidecar int
	require.Eventually(t, func() bool {
		b, err := os.ReadFile(filepath.Join(dir, "sidecar.pid"))
		if err != nil {
			return false
		}
		sidecar, err = strconv.Atoi(strings.TrimSpace(string(b)))
		return err == nil
	}, 60*time.Second, 20*time.Millisecond, "the launcher did not start a sidecar")

	pgid, err := syscall.Getpgid(sidecar)
	require.NoError(t, err)
	require.Equal(t, sidecar, pgid, "the sidecar must lead a process group of its own")
	require.NotEqual(t, launcher.Process.Pid, pgid, "the sidecar must not be in the launcher's process group")

	// Ctrl-C.
	require.NoError(t, syscall.Kill(-launcher.Process.Pid, syscall.SIGINT))
	select {
	case <-launcherExited:
	case <-time.After(10 * time.Second):
		t.Fatal("the launcher did not exit on SIGINT")
	}

	owner, err := paths.loadOwner()
	require.NoError(t, err)
	require.NotNil(t, owner)
	require.Equal(t, sidecar, owner.PID)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		alive, err := owner.alive()
		require.NoError(t, err)
		require.True(t, alive, "the SIGINT sent to the launcher's process group reached the sidecar")
		time.Sleep(50 * time.Millisecond)
	}
}
