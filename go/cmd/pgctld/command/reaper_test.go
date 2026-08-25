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

package command

import (
	"context"
	"log/slog"
	"os/exec"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/tools/executil"
)

func quietReaper() *childReaper {
	return newChildReaper(slog.New(slog.DiscardHandler))
}

func (r *childReaper) isTracked(pid int) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.tracked[pid]
	return ok
}

// TestChildReaper_ReapsTrackedChild verifies the reaper wait()s on a tracked,
// un-waited child once it exits — exactly the postmaster case, where pgctld is
// the reparented parent and nothing else calls wait().
func TestChildReaper_ReapsTrackedChild(t *testing.T) {
	r := quietReaper()

	// Start a short-lived child and deliberately never call cmd.Wait(), so the
	// reaper is the only thing that can reap it (as with the reparented postmaster).
	cmd := exec.Command("sleep", "0.3")
	require.NoError(t, cmd.Start())
	pid := cmd.Process.Pid

	// Track attempts an immediate reap; the child is still running, so it stays tracked.
	r.TrackPID(pid)
	require.True(t, r.isTracked(pid))

	// After the child exits, reaping drops it from the tracked set.
	require.Eventually(t, func() bool {
		r.Reap()
		return !r.isTracked(pid)
	}, 5*time.Second, 20*time.Millisecond)

	// The child has genuinely been reaped: it is no longer waitable.
	_, err := syscall.Wait4(pid, nil, syscall.WNOHANG, nil)
	require.ErrorIs(t, err, syscall.ECHILD)
}

// TestChildReaper_IgnoresUntrackedPID verifies the reaper leaves a running child
// it was never told about completely alone.
func TestChildReaper_IgnoresUntrackedPID(t *testing.T) {
	r := quietReaper()

	// Use executil.Cmd so cleanup can Stop() it: Stop reaps via the command's own
	// Wait(), whereas signalling a bare *os.Process would leave an unreaped zombie
	// that a liveness poll never sees exit.
	cmd := executil.Command(context.Background(), "sleep", "5")
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		_, _ = cmd.Stop(context.Background())
	})

	// Reaping with nothing tracked must not touch the running child.
	r.Reap()
	require.False(t, r.isTracked(cmd.Process.Pid))

	// The child is still alive and still ours to wait on.
	require.NoError(t, cmd.Process.Signal(syscall.Signal(0)))
}

// TestChildReaper_DoesNotStealOsExecChildren is the regression guard for the
// exit-status theft. With the running reaper installed (SIGCHLD handler active),
// many concurrent os/exec children that Wait() for themselves must all observe their true exit
// status. The old Wait4(-1) reaper raced these waits and intermittently made
// Run() fail with "waitid: no child processes"; tracking exact PIDs cannot.
func TestChildReaper_DoesNotStealOsExecChildren(t *testing.T) {
	r := quietReaper()
	go r.Run()
	t.Cleanup(r.Stop)

	// Let the SIGCHLD handler install before spawning children.
	time.Sleep(50 * time.Millisecond)

	var wg sync.WaitGroup
	for range 40 {
		wg.Go(func() {
			for range 5 {
				// A trivial, fast command that os/exec both starts and waits on.
				assert.NoError(t, exec.Command("true").Run())
			}
		})
	}
	wg.Wait()
}
