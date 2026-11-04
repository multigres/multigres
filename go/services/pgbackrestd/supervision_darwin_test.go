//go:build darwin

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
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPortableAnchorHelper(t *testing.T) {
	other := os.Getenv("TEST_PORTABLE_OTHER_GROUP")
	if other == "" {
		t.Skip("subprocess helper")
	}
	require.NoError(t, becomeSubreaper())
	root := exec.Command("/bin/sh", "-c", "sleep 60 & exit 0")
	root.SysProcAttr = supervisedChildSysProcAttr()
	require.NoError(t, root.Run()) // deterministic boundary: foreground reaped
	require.False(t, portableAnchorAlone(), "reparented child must remain visible after foreground reaping")
	require.NoError(t, signalOwnedChildren(root.Process.Pid, syscall.SIGTERM))
	require.Eventually(t, portableAnchorAlone, 5*time.Second, time.Millisecond, "kernel must retain only the anchor after reparented child exits")
	unrelated, err := strconv.Atoi(other)
	require.NoError(t, err)
	// Even if the stale root number now named this other (test-owned) group,
	// signalling must use our live anchor, never that number.
	require.NoError(t, signalOwnedChildren(unrelated, syscall.SIGTERM))
	require.True(t, portableGroupGone(unrelated))
}

func TestPortableIdentityReleaseBoundary(t *testing.T) {
	unrelated := exec.Command("/bin/sleep", "60")
	unrelated.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	require.NoError(t, unrelated.Start())
	defer func() {
		//nolint:gocritic // this test owns exec.Cmd and reaps it with Wait; PID helpers are for externally owned processes
		if err := unrelated.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			t.Errorf("kill test-owned unrelated group: %v", err)
		}
		err := unrelated.Wait()
		var exitErr *exec.ExitError
		if err != nil && !errors.As(err, &exitErr) {
			t.Errorf("reap test-owned unrelated group: %v", err)
		}
	}()
	self, err := os.Executable()
	require.NoError(t, err)
	helper := exec.Command(self, "-test.run=^TestPortableAnchorHelper$")
	helper.Env = append(os.Environ(), "TEST_PORTABLE_OTHER_GROUP="+strconv.Itoa(unrelated.Process.Pid))
	helper.SysProcAttr = supervisorSysProcAttr()
	out, err := helper.CombinedOutput()
	require.NoError(t, err, string(out))
	require.NoError(t, syscall.Kill(unrelated.Process.Pid, 0), "unrelated group must not receive TERM")
}

func TestPortableRealSynchronousCommand(t *testing.T) {
	bin, err := exec.LookPath("pgbackrest")
	if err != nil {
		t.Skip("pgbackrest not installed")
	}
	s := newTestServer(t, nil)
	s.bin = bin
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	code, err := s.runChild(ctx, KindS3Command, cmdStart, nil, io.Discard, io.Discard)
	require.NoError(t, err)
	require.Zero(t, code)
	require.NoError(t, s.Close())
}

func TestPortableEscalationNeverAcknowledgesReleasedAnchor(t *testing.T) {
	s := newTestServer(t, nil)
	ready := filepath.Join(t.TempDir(), "ready")
	fakeServerBin(t, s, "trap '' TERM; echo ready > "+ready+"; exec sleep 600")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := s.runChild(ctx, KindS3Command, cmdStart, nil, io.Discard, io.Discard); done <- err }()
	require.Eventually(t, func() bool { _, err := os.Stat(ready); return err == nil }, 5*time.Second, time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("escalation did not return")
	}
	require.Error(t, s.Close(), "destroyed anchor cannot certify settlement")
	markers, err := filepath.Glob(s.cfg.stateSub("worker-*.pending"))
	require.NoError(t, err)
	require.Len(t, markers, 1)
	// Model the owner's exit, after the complete private group was SIGKILLed.
	// No production path uses this forced test-only release.
	s.instanceLock.Close()
	if s.anchor != nil {
		s.anchor.Close()
	}
	_, err = newForTest(s.cfg)
	require.ErrorContains(t, err, "quiescence was not recorded")
}
