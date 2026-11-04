//go:build !linux

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

package pgbackrestd

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// killSupervisorAt makes the next supervisors die of SIGKILL at stage (see
// testHookSupervisor) and returns the PIDs it killed.
func killSupervisorAt(t *testing.T, stage string) *[]int {
	t.Helper()
	var killed []int
	testHookSupervisor = func(at string, pid int) {
		if at == stage {
			killed = append(killed, pid)
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	}
	t.Cleanup(func() { testHookSupervisor = nil })
	return &killed
}

func pendingMarkers(t *testing.T, s *Server) []string {
	t.Helper()
	markers, err := filepath.Glob(s.cfg.stateSub("worker-*.pending"))
	require.NoError(t, err)
	return markers
}

// A supervisor killed after it was forked and before its spec was written has
// started no worker. The sidecar must see that as settled: the command fails,
// nothing stays latched, the marker is gone, Close releases the instance lock,
// and a new sidecar starts on the same state directory. A signal sent to the
// sidecar's process group while it forks a supervisor kills the supervisor in
// exactly this window.
func TestSupervisorKilledBeforeSpecDeliveryIsSettled(t *testing.T) {
	s := newTestServer(t, nil)
	runLog := fakeServerBin(t, s, "exit 0")
	killed := killSupervisorAt(t, "started")

	_, err := s.runChild(t.Context(), KindS3Command, cmdStart, nil, io.Discard, io.Discard)
	require.Error(t, err, "the command did not run and must say so")
	require.Len(t, *killed, 1)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	require.NoError(t, s.quiesceWorkers(ctx), "a supervisor that accepted nothing leaves nothing to settle")
	assert.Empty(t, pendingMarkers(t, s), "its marker records no worker")
	assert.Zero(t, countRuns(t, runLog), "no worker may have run")

	// The same sidecar goes on working.
	testHookSupervisor = nil
	code, err := s.runChild(t.Context(), KindS3Command, cmdStart, nil, io.Discard, io.Discard)
	require.NoError(t, err)
	require.Zero(t, code)
	require.Equal(t, 1, countRuns(t, runLog))

	// And a restart needs no repair.
	require.NoError(t, s.Close())
	next, err := newForTest(s.cfg)
	require.NoError(t, err, "a new sidecar must start on the same state directory")
	require.NoError(t, next.Close())
}

// The same death during shutdown: the sidecar is closing, the supervisor is
// killed before its spec, and Close must still succeed.
func TestSupervisorKilledBeforeSpecDeliveryDoesNotBlockClose(t *testing.T) {
	s := newTestServer(t, nil)
	fakeServerBin(t, s, "exit 0")
	killSupervisorAt(t, "started")
	_, err := s.runChild(t.Context(), KindS3Command, cmdStart, nil, io.Discard, io.Discard)
	require.Error(t, err)
	require.NoError(t, s.Close())
	assert.Empty(t, pendingMarkers(t, s))
	next, err := newForTest(s.cfg)
	require.NoError(t, err)
	require.NoError(t, next.Close())
}

// A supervisor that accepted its spec and then died may have started the
// worker. Nothing proves it did not, so the sidecar stays latched: the marker
// stays, Close keeps the instance lock, and a new sidecar refuses to start.
func TestSupervisorKilledAfterAcceptingStaysUnsettled(t *testing.T) {
	s := newTestServer(t, nil)
	workerPID := filepath.Join(t.TempDir(), "worker.pid")
	fakeServerBin(t, s, "echo $$ > "+workerPID+"; exec sleep 60")
	t.Cleanup(func() {
		if b, err := os.ReadFile(workerPID); err == nil {
			if pid, err := strconv.Atoi(strings.TrimSpace(string(b))); err == nil {
				_ = syscall.Kill(pid, syscall.SIGKILL)
			}
		}
	})
	killed := killSupervisorAt(t, "accepted")

	_, err := s.runChild(t.Context(), KindS3Command, cmdStart, nil, io.Discard, io.Discard)
	require.Error(t, err)
	require.Len(t, *killed, 1)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	require.Error(t, s.quiesceWorkers(ctx), "a supervisor that accepted its spec and died proves nothing")
	require.Len(t, pendingMarkers(t, s), 1, "the marker must stay")
	require.ErrorContains(t, s.Close(), "retaining instance lock")

	_, err = newForTest(s.cfg)
	require.ErrorContains(t, err, "still own", "the latched sidecar keeps the instance lock")
	// The sidecar process exits. No production path releases the lock this way.
	require.NoError(t, s.instanceLock.Close())
	if s.anchor != nil {
		s.anchor.Close()
	}
	_, err = newForTest(s.cfg)
	require.ErrorContains(t, err, "quiescence was not recorded")
}

// The supervisor writes Accepted before it starts the worker, and starts
// nothing if the event cannot be written. That order is what lets the sidecar
// read "no Accepted event" as "no worker": here the event pipe has no reader,
// and the worker must never run.
func TestSupervisorStartsNothingWithoutAcceptedEvent(t *testing.T) {
	dir := t.TempDir()
	ran := filepath.Join(dir, "ran")
	marker := filepath.Join(dir, "worker-test.pending")
	require.NoError(t, os.WriteFile(marker, []byte("pending"), 0o600))

	controlR, controlW, err := os.Pipe()
	require.NoError(t, err)
	defer controlW.Close()
	eventsR, eventsW, err := os.Pipe()
	require.NoError(t, err)
	lock, err := os.Create(filepath.Join(dir, "lock"))
	require.NoError(t, err)
	defer lock.Close()
	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	require.NoError(t, err)
	defer devNull.Close()

	self, err := os.Executable()
	require.NoError(t, err)
	cmd := exec.Command(self, SupervisorSubcommand)
	cmd.SysProcAttr = supervisorSysProcAttr()
	cmd.ExtraFiles = []*os.File{controlR, eventsW, lock, devNull, devNull}
	require.NoError(t, cmd.Start())
	controlR.Close()
	eventsW.Close()
	// Nobody will ever read an event.
	eventsR.Close()
	require.NoError(t, json.NewEncoder(controlW).Encode(workerSpec{
		Argv:   []string{"/bin/sh", "-c", "echo ran > " + ran},
		Marker: marker,
	}))

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		require.Error(t, err, "a supervisor that cannot report Accepted must fail")
	case <-time.After(10 * time.Second):
		// The deferred close of the control pipe ends it.
		t.Fatal("the supervisor did not exit")
	}
	// Give a wrongly started worker time to leave its trace.
	time.Sleep(300 * time.Millisecond)
	require.NoFileExists(t, ran, "the supervisor started a worker without having reported Accepted")
	// The sidecar that would have retired the marker is the reader that is
	// gone, so the supervisor, which knows it started nothing, retires it.
	require.NoFileExists(t, marker, "a supervisor that started nothing must not leave its marker")
}

// The sidecar re-executes its own path for every supervisor, and that file can
// be replaced while the sidecar runs. A build from before the Accepted event
// starts its worker first and reports afterwards; if it is killed in between,
// the sidecar reads no event at all, which it takes as proof that no worker was
// started. The subcommand name carries the protocol version so that such a
// binary cannot be given a spec: it does not know the name.
//
// The helper here is that earlier build: under the earlier name it starts the
// worker and exits without a word. Given a name it does not know, the real
// binary prints its usage to stdout and exits 0 (the root command takes no
// arguments and has no Run); a script that fails instead is covered too, so
// that the decision is shown not to depend on the exit code.
func TestSupervisorOfAnotherProtocolVersionStartsNothing(t *testing.T) {
	for name, unknown := range map[string]string{
		"prints usage and exits 0": "echo \"Usage: pgbackrestd [command]\"\nexit 0\n",
		"exits 1":                  "echo \"unknown command $1\" >&2\nexit 1\n",
	} {
		t.Run(name, func(t *testing.T) {
			s := newTestServer(t, nil)
			fakeServerBin(t, s, "exit 0")
			dir := t.TempDir()
			ran := filepath.Join(dir, "ran")
			helper := filepath.Join(dir, "pgbackrestd")
			script := "#!/bin/sh\n" +
				"if [ \"$1\" = worker-supervisor ]; then echo ran > " + ran + "; exit 0; fi\n" +
				unknown
			require.NoError(t, os.WriteFile(helper, []byte(script), 0o700))
			s.self = helper

			_, err := s.runChild(t.Context(), KindS3Command, cmdStart, nil, io.Discard, io.Discard)
			require.Error(t, err, "a helper that does not speak the protocol cannot run the command")
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			require.NoError(t, s.quiesceWorkers(ctx))
			time.Sleep(300 * time.Millisecond)
			require.NoFileExists(t, ran, "a helper of another protocol version was handed a worker to start")
			assert.Empty(t, pendingMarkers(t, s))
		})
	}
}
