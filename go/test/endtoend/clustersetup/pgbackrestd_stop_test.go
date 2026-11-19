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

package clustersetup

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/pgbackreststate"
	"github.com/multigres/multigres/go/provisioner/local"
	"github.com/multigres/multigres/go/tools/executil"
)

// fakeSidecar is a PgBackRestdInstance around a shell command, wired the way
// Start wires the real one.
func fakeSidecar(t *testing.T, script string) *PgBackRestdInstance {
	t.Helper()
	i := &PgBackRestdInstance{Name: "fake", exited: make(chan struct{})}
	i.Process = executil.Command(context.Background(), "/bin/sh", "-c", script).WithProcessGroup()
	require.NoError(t, i.Process.Start())
	go func() {
		defer close(i.exited)
		_ = i.Process.Wait()
	}()
	t.Cleanup(func() { _, _ = i.Process.Kill(context.Background()) })
	return i
}

// A caller wipes the sidecar's state as soon as Stop returns nil, and may
// start it again, so by then the process must be gone and IsRunning false:
// Start does nothing while IsRunning is true.
func TestPgBackRestdStop_ReturnsOnlyAfterTheProcessIsGone(t *testing.T) {
	for _, script := range []string{
		"sleep 300",
		// Ignores SIGTERM, so only the SIGKILL escalation ends it.
		"trap '' TERM; while :; do sleep 1; done",
	} {
		for range 5 {
			i := fakeSidecar(t, script)
			pid := i.Process.Process.Pid
			require.True(t, i.IsRunning())
			// Long enough for the shell to install its trap.
			time.Sleep(100 * time.Millisecond)
			require.NoError(t, i.Stop(200*time.Millisecond))
			require.False(t, i.IsRunning(), "Stop returned nil while IsRunning was still true")
			require.Error(t, i.Process.Process.Signal(syscall.Signal(0)), "PID %d still exists after Stop", pid)
		}
	}
	// Stopping a sidecar that never ran, or already stopped, is not an error.
	require.NoError(t, (&PgBackRestdInstance{}).Stop(time.Second))
}

// stoppedSidecarState gives i the directories of a sidecar that started once
// and has exited: a state directory whose instance lock carries the
// supervision stamp and that nobody holds.
func stoppedSidecarState(t *testing.T, i *PgBackRestdInstance) {
	t.Helper()
	i.Paths = local.NewPgBackRestdPaths(t.TempDir())
	require.NoError(t, os.MkdirAll(i.Paths.StateDir, 0o700))
	lock, err := os.OpenFile(filepath.Join(i.Paths.StateDir, pgbackreststate.InstanceLockName), os.O_CREATE|os.O_RDWR, 0o600)
	require.NoError(t, err)
	require.NoError(t, pgbackreststate.RecordSupervision(i.Paths.StateDir, lock))
	require.NoError(t, lock.Close())
}

// A sidecar that exited may have left a pgBackRest worker running: it keeps
// the worker's marker when it cannot prove the worker stopped. Deleting the
// directory would delete that evidence and let the next sidecar start beside
// the worker, so the reset is refused and nothing is removed.
func TestPgBackRestdStopAndRemoveState_KeepsTheDirectoryWhileAWorkerIsUnsettled(t *testing.T) {
	i := fakeSidecar(t, "sleep 300")
	stoppedSidecarState(t, i)
	marker := filepath.Join(i.Paths.StateDir, "worker-1.pending")
	require.NoError(t, os.WriteFile(marker, []byte("{}"), 0o600))
	fixture := filepath.Join(i.Paths.Dir, "fixture")
	require.NoError(t, os.WriteFile(fixture, []byte("kept"), 0o600))

	err := i.StopAndRemoveState(200 * time.Millisecond)
	require.ErrorIs(t, err, pgbackreststate.ErrWorkersUnsettled)
	require.False(t, i.IsRunning())
	require.FileExists(t, marker)
	require.FileExists(t, fixture)

	// Once the marker is gone the same call removes the directory.
	require.NoError(t, os.Remove(marker))
	require.NoError(t, i.StopAndRemoveState(200*time.Millisecond))
	require.NoDirExists(t, i.Paths.Dir)
}

// A process that still holds the instance lock (a sidecar, or a supervisor
// that outlived it) owns the state, whatever markers are present.
func TestPgBackRestdStopAndRemoveState_KeepsTheDirectoryWhileTheLockIsHeld(t *testing.T) {
	i := fakeSidecar(t, "sleep 300")
	stoppedSidecarState(t, i)
	held, err := os.OpenFile(filepath.Join(i.Paths.StateDir, pgbackreststate.InstanceLockName), os.O_RDWR, 0)
	require.NoError(t, err)
	defer held.Close()
	require.NoError(t, syscall.Flock(int(held.Fd()), syscall.LOCK_EX|syscall.LOCK_NB))

	err = i.StopAndRemoveState(200 * time.Millisecond)
	require.ErrorIs(t, err, pgbackreststate.ErrStateInUse)
	require.DirExists(t, i.Paths.StateDir)
}

// A sidecar that never started has no state directory and nothing to settle.
func TestPgBackRestdStopAndRemoveState_RemovesADirectoryThatHoldsNoState(t *testing.T) {
	i := &PgBackRestdInstance{Paths: local.NewPgBackRestdPaths(t.TempDir())}
	require.NoError(t, os.MkdirAll(i.Paths.Dir, 0o700))
	require.NoError(t, i.StopAndRemoveState(time.Second))
	require.NoDirExists(t, i.Paths.Dir)
}
