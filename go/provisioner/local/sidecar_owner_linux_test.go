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
	"io"
	"os"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/shirou/gopsutil/v4/process"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/tools/executil"
)

func TestSidecarExitProcessFixture(t *testing.T) {
	if len(os.Args) == 0 || os.Args[len(os.Args)-1] != "--owner-exit-fixture" {
		return
	}
	// A pipe handshake proves exec and runtime initialization have finished;
	// these exit tests must not conflate pre-exec argv with exit-time argv.
	_, err := os.Stdout.Write([]byte{1})
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, os.Stdin)
	require.NoError(t, err)
}

func startOwnerExitFixture(t *testing.T) (*executil.Cmd, io.WriteCloser, *sidecarOwner) {
	t.Helper()
	binary, err := os.Executable()
	require.NoError(t, err)
	cmd := executil.Command(context.Background(), binary, "-test.run=^TestSidecarExitProcessFixture$", "--", "--owner-exit-fixture")
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		_ = stdin.Close()
		_, _ = cmd.Stop(context.Background())
		_ = cmd.Wait()
	})
	var ready [1]byte
	_, err = io.ReadFull(stdout, ready[:])
	require.NoError(t, err)
	require.Equal(t, byte(1), ready[0])
	owner, err := readSidecarIdentity(cmd.Process.Pid)
	require.NoError(t, err)
	require.Equal(t, cmd.Args, owner.Argv)
	return cmd, stdin, owner
}

func TestSidecarOwnerLinuxExitStates(t *testing.T) {
	cmd, stdin, owner := startOwnerExitFixture(t)
	alive, err := owner.alive()
	require.NoError(t, err)
	require.True(t, alive)
	require.NoError(t, stdin.Close())
	proc, err := process.NewProcess(int32(owner.PID))
	require.NoError(t, err)
	// Deliberately do not Wait yet: an unreaped zombie retains its PID but
	// has no argv. This is positive exit evidence, not an unknown live owner.
	require.Eventually(t, func() bool {
		status, err := proc.Status()
		return err == nil && slices.Contains(status, process.Zombie)
	}, 5*time.Second, time.Millisecond)
	exists, err := process.PidExists(int32(owner.PID))
	require.NoError(t, err)
	require.True(t, exists)
	_, err = readSidecarIdentity(owner.PID)
	require.ErrorContains(t, err, "incomplete sidecar process identity")
	alive, err = owner.alive()
	require.NoError(t, err)
	require.False(t, alive)
	require.NoError(t, owner.stop(t.Context()))
	require.NoError(t, cmd.Wait())
	alive, err = owner.alive()
	require.NoError(t, err)
	require.False(t, alive)
}

func TestSidecarOwnerLinuxRejectsLiveIdentityMismatch(t *testing.T) {
	cmd, _, owner := startOwnerExitFixture(t)
	for _, field := range []string{"birth", "argv"} {
		t.Run(field, func(t *testing.T) {
			wrong := *owner
			if field == "birth" {
				wrong.Created--
			} else {
				wrong.Argv = []string{"unverified"}
			}
			alive, err := wrong.alive()
			require.ErrorIs(t, err, errSidecarIdentityChanged)
			require.False(t, alive)
			require.ErrorIs(t, wrong.stop(t.Context()), errSidecarIdentityChanged)
			require.NoError(t, cmd.Process.Signal(syscall.Signal(0)))
			alive, err = owner.alive()
			require.NoError(t, err)
			require.True(t, alive, "rejected owner must not signal the live process")
		})
	}
}

func TestSidecarOwnerLinuxStopExitRace(t *testing.T) {
	for range 100 {
		cmd, _, owner := startOwnerExitFixture(t)
		// Reaping races the liveness reads just as in the failing CI test.
		done := make(chan struct{})
		go func() {
			_ = cmd.Wait()
			close(done)
		}()
		require.NoError(t, owner.stop(t.Context()))
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("stopped fixture was not reaped")
		}
	}
}
