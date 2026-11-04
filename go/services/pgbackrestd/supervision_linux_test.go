//go:build linux

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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/pgbackreststate"
	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
)

func detachedProbe(t *testing.T, s *Server) int {
	t.Helper()
	ready := filepath.Join(t.TempDir(), "daemon.pid")
	script := filepath.Join(t.TempDir(), "pgbackrest")
	// setsid plus background exit models pgBackRest's fork/detach/reexec. The
	// daemon explicitly ignores TERM, so a stop ACK requires escalation/reaping.
	require.NoError(t, os.WriteFile(script, []byte("#!/bin/sh\nsetsid sh -c 'trap \"\" TERM; echo $$ > \"$1\"; exec sleep 600' sh "+ready+" </dev/null >/dev/null 2>&1 &\nexit 0\n"), 0o700))
	s.bin = script
	code, err := s.runChild(t.Context(), KindS3Command, cmdArchivePush, nil, io.Discard, io.Discard)
	require.NoError(t, err)
	require.Zero(t, code)
	var pid int
	require.Eventually(t, func() bool {
		b, err := os.ReadFile(ready)
		if err != nil {
			return false
		}
		pid, err = strconv.Atoi(strings.TrimSpace(string(b)))
		return err == nil
	}, 5*time.Second, 5*time.Millisecond)
	require.NoError(t, syscall.Kill(pid, 0), "foreground completion must preserve async work")
	return pid
}

func TestOwnedWorkerDetachedIgnoredTERM(t *testing.T) {
	s := newTestServer(t, nil)
	pid := detachedProbe(t, s)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.NoError(t, s.quiesceWorkers(ctx))
	require.ErrorIs(t, syscall.Kill(pid, 0), syscall.ESRCH, "ACK is after reaping, not after signal")
	markers, err := filepath.Glob(s.cfg.stateSub("worker-*.pending"))
	require.NoError(t, err)
	require.Empty(t, markers)
}

func TestOwnedWorkerTimeoutAndShutdown(t *testing.T) {
	s := newTestServer(t, nil)
	pid := detachedProbe(t, s)
	// A zero budget cannot authorize deletion; the supervisor continues to own
	// and reap the process after the request has failed closed.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.Error(t, s.quiesceWorkers(ctx))
	require.NoError(t, s.Close())
	require.ErrorIs(t, syscall.Kill(pid, 0), syscall.ESRCH)
}

func TestOwnershipLockAndDirtyMarker(t *testing.T) {
	s := newTestServer(t, nil)
	_, err := newServer(s.cfg, s.decision)
	require.ErrorContains(t, err, "still own")
	require.NoError(t, s.Close())
	require.NoError(t, privateWrite(s.cfg.stateSub("worker-dead.pending"), []byte("unsettled")))
	_, err = newServer(s.cfg, s.decision)
	require.ErrorContains(t, err, "quiescence was not recorded")
}

// This helper is a separate sidecar process, not a goroutine in the test.
func TestHelperOwnedParent(t *testing.T) {
	raw := os.Getenv("OWNED_PARENT_CONFIG")
	if raw == "" {
		t.Skip("helper")
	}
	stopCtx, stop := signal.NotifyContext(t.Context(), syscall.SIGTERM)
	defer stop()
	var cfg Config
	require.NoError(t, json.Unmarshal([]byte(raw), &cfg))
	s, err := newForTest(cfg)
	require.NoError(t, err)
	s.bin = os.Getenv("OWNED_PARENT_SCRIPT")
	code, err := s.runChild(t.Context(), KindS3Command, cmdArchivePush, nil, io.Discard, io.Discard)
	require.NoError(t, err)
	require.Zero(t, code)
	supervisor := 0
	s.children.Range(func(k, _ any) bool { supervisor = k.(int); return false })
	fmt.Println(supervisor)
	if os.Getenv("OWNED_PARENT_GROUP_STOP") == "1" {
		<-stopCtx.Done()
		require.NoError(t, s.Close(), "graceful daemon group-stop must settle its supervised workers")
		return
	}
	select {}
}

func TestOwnedWorkerSurvivesParentOnlyUntilQuiescence(t *testing.T) {
	if os.Getenv("OWNED_SUBREAPER_TEST") != "1" {
		self, err := os.Executable()
		require.NoError(t, err)
		fixture := exec.Command(self, "-test.run=^TestOwnedWorkerSurvivesParentOnlyUntilQuiescence$")
		fixture.Env = append(os.Environ(), "OWNED_SUBREAPER_TEST=1")
		output, err := fixture.CombinedOutput()
		require.NoError(t, err, string(output))
		return
	}
	// Isolate subreaper state from other tests. When the owner dies, adopt its
	// stopped supervisor in the same session. Otherwise Linux can orphan the
	// stopped process group and send SIGHUP + SIGCONT, killing the supervisor
	// instead of testing its inherited lock while it cannot make progress.
	require.NoError(t, becomeSubreaper())
	template := newTestServer(t, nil)
	cfg := template.cfg
	require.NoError(t, template.Close())
	ready := filepath.Join(t.TempDir(), "daemon.pid")
	script := filepath.Join(t.TempDir(), "pgbackrest")
	require.NoError(t, os.WriteFile(script, []byte("#!/bin/sh\nsetsid sh -c 'trap \"\" TERM; echo $$ > \"$1\"; exec sleep 600' sh "+ready+" </dev/null >/dev/null 2>&1 &\nexit 0\n"), 0o700))
	raw, err := json.Marshal(cfg)
	require.NoError(t, err)
	self, err := os.Executable()
	require.NoError(t, err)
	parent := exec.Command(self, "-test.run=^TestHelperOwnedParent$")
	parent.Env = append(os.Environ(), "OWNED_PARENT_CONFIG="+string(raw), "OWNED_PARENT_SCRIPT="+script)
	pipe, err := parent.StdoutPipe()
	require.NoError(t, err)
	parent.Stderr = os.Stderr
	require.NoError(t, parent.Start())
	t.Cleanup(func() {
		//nolint:gocritic // deliberate parent-death fixture; this test owns and reaps its exec.Cmd
		if err := parent.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			t.Errorf("kill parent fixture: %v", err)
		}
		// Wait may already have run at the crash boundary below.
		if parent.ProcessState == nil {
			if _, err := parent.Process.Wait(); err != nil {
				t.Errorf("reap parent fixture: %v", err)
			}
		}
	})
	line, err := bufio.NewReader(pipe).ReadString('\n')
	require.NoError(t, err)
	supervisor, err := strconv.Atoi(strings.TrimSpace(line))
	require.NoError(t, err)
	require.Positive(t, supervisor)
	require.Eventually(t, func() bool { _, err := os.Stat(ready); return err == nil }, 5*time.Second, 5*time.Millisecond)
	b, err := os.ReadFile(ready)
	require.NoError(t, err)
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	require.NoError(t, err)
	// Freeze the owner, not the worker: restart exclusion is kernel-enforced
	// even when no progress towards shutdown can be made. No timing guess.
	require.NoError(t, syscall.Kill(supervisor, syscall.SIGSTOP))
	t.Cleanup(func() {
		_ = syscall.Kill(supervisor, syscall.SIGCONT)
		_ = syscall.Kill(pid, syscall.SIGKILL)
		if parent.ProcessState == nil {
			//nolint:gocritic // reap the fixture owner before its adopted supervisor, including on early assertion failure
			_ = parent.Process.Kill()
			_ = parent.Wait()
		}
		// The isolated subreaper owns the adopted supervisor after parent.Wait.
		var status syscall.WaitStatus
		_, err := syscall.Wait4(supervisor, &status, 0, nil)
		require.NoError(t, err)
	})
	require.Eventually(t, func() bool {
		status, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", supervisor))
		return err == nil && strings.Contains(string(status), "State:\tT (stopped)")
	}, 5*time.Second, time.Millisecond, "supervisor must be stopped before owner death")
	//nolint:gocritic // abrupt sidecar death is the tested event; graceful stop would not exercise ownership inheritance
	require.NoError(t, parent.Process.Kill())
	_ = parent.Wait()
	guard, gerr := pgbackreststate.AcquireCleanupGuard(cfg.StateDir)
	require.Nil(t, guard)
	require.ErrorIs(t, gerr, pgbackreststate.ErrStateInUse, "supervisor retains cleanup exclusion after owner death")
	_, err = newForTest(cfg)
	require.ErrorContains(t, err, "still own")
	require.NoError(t, syscall.Kill(pid, 0))
	require.NoError(t, syscall.Kill(supervisor, syscall.SIGCONT))
	require.Eventually(t, func() bool { return syscall.Kill(pid, 0) == syscall.ESRCH }, 5*time.Second, 5*time.Millisecond)
	var next *Server
	require.Eventually(t, func() bool { var err error; next, err = newForTest(cfg); return err == nil }, 5*time.Second, 5*time.Millisecond)
	require.NoError(t, next.Close())
	guard, gerr = pgbackreststate.AcquireCleanupGuard(cfg.StateDir)
	require.NoError(t, gerr)
	require.NoError(t, guard.Close())
}

func TestRestoreRealStopWaitsForOwnedDaemon(t *testing.T) {
	pgbr, err := exec.LookPath("pgbackrest")
	if err != nil {
		t.Skip("pgbackrest not installed")
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 required for flock probe")
	}
	s := newTestServer(t, nil)
	_, err = s.configure(&pb.ConfigureRequest{Revision: 1, Repos: repos(1, 1)})
	require.NoError(t, err)
	dir := t.TempDir()
	ready := filepath.Join(dir, "ready")
	lock := s.cfg.stateSub("lock/multigres-archive.lock")
	daemon := filepath.Join(dir, "daemon.py")
	require.NoError(t, os.WriteFile(daemon, []byte(`import os,sys,signal,fcntl,json,time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
f=open(sys.argv[1], 'w')
fcntl.flock(f,fcntl.LOCK_EX)
f.write(json.dumps({'execId':'test','pid':os.getpid()})); f.flush()
open(sys.argv[2],'w').write(str(os.getpid()))
while True: time.sleep(1)
`), 0o600))
	script := filepath.Join(dir, "pgbackrest")
	require.NoError(t, os.WriteFile(script, fmt.Appendf(nil, "#!/bin/sh\nsetsid %q %q %q %q </dev/null >/dev/null 2>&1 &\nexit 0\n", python, daemon, lock, ready), 0o700))
	s.bin = script
	code, err := s.runChild(t.Context(), KindS3Command, cmdArchivePush, nil, io.Discard, io.Discard)
	require.NoError(t, err)
	require.Zero(t, code)
	require.Eventually(t, func() bool { _, err := os.Stat(ready); return err == nil }, 5*time.Second, 5*time.Millisecond)
	b, err := os.ReadFile(ready)
	require.NoError(t, err)
	pid, err := strconv.Atoi(string(b))
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(s.cfg.PGData, 0o700))
	partial := filepath.Join(s.cfg.PGData, "partial")
	require.NoError(t, os.WriteFile(partial, []byte("keep until quiescent"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(filepath.Dir(s.cfg.PGData), constants.RestoreSentinelFile), []byte("interrupted"), 0o600))
	// Exercise the actual pinned stop/start commands. stop unlinks and TERM's
	// the daemon but cannot terminate it; the sidecar must supply the barrier.
	proxy := filepath.Join(dir, "proxy")
	require.NoError(t, os.WriteFile(proxy, fmt.Appendf(nil, `#!/bin/sh
for a in "$@"; do case "$a" in --*) ;; *) cmd=$a; break;; esac; done
case "$cmd" in
start|stop) exec %q "$@" ;;
restore) if kill -0 %d 2>/dev/null; then exit 42; fi
         if [ -e %q ]; then exit 43; fi ;;
esac
exit 0
`, pgbr, pid, partial), 0o700))
	s.bin = proxy
	out := newStreamSender(discardRestoreStream{})
	defer func() { require.NoError(t, out.finish(nil)) }()
	req := &pb.RunRequest{Command: pb.Command_COMMAND_RESTORE, Restore: &pb.RestoreOperation{Id: uuid.NewString(), Sequence: 1, Incarnation: s.incarnation, Cleanup: true}}
	code, err = s.runRestoreOperation(t.Context(), req, out)
	require.NoError(t, err)
	require.Zero(t, code)
	require.ErrorIs(t, syscall.Kill(pid, 0), syscall.ESRCH)
	require.NoFileExists(t, partial)
	require.True(t, s.restoreRecord.Succeeded)
}

// A dead supervisor is not a quiescence ACK. Its orphan can still run, so both
// the live sidecar and its replacement must refuse to authorize cleanup.
func TestOwnedSupervisorDeathFailsClosed(t *testing.T) {
	s := newTestServer(t, nil)
	pid := detachedProbe(t, s)
	t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })
	supervisor := 0
	s.children.Range(func(k, _ any) bool { supervisor = k.(int); return false })
	require.Positive(t, supervisor)
	require.NoError(t, syscall.Kill(supervisor, syscall.SIGKILL))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.Error(t, s.quiesceWorkers(ctx))
	require.NoError(t, syscall.Kill(pid, 0), "the orphan is not evidence of quiescence")
	require.Error(t, s.Close())
	_, err := newForTest(s.cfg)
	require.ErrorContains(t, err, "still own", "a live owner must retain the instance lock on failed teardown")
	// Model owner death separately: even once its lock description is gone,
	// the same-namespace orphan marker must still refuse a new process owner.
	require.NoError(t, s.instanceLock.Close())
	guard, gerr := pgbackreststate.AcquireCleanupGuard(s.cfg.StateDir)
	require.Nil(t, guard)
	require.ErrorIs(t, gerr, pgbackreststate.ErrWorkersUnsettled, "free flock must not authorize deletion under an orphan")
	_, err = newForTest(s.cfg)
	require.ErrorContains(t, err, "quiescence was not recorded")
}

func TestOwnedWorkerDrainsOutputAfterForegroundResponse(t *testing.T) {
	old := foregroundDrainWait
	foregroundDrainWait = 20 * time.Millisecond
	t.Cleanup(func() { foregroundDrainWait = old })
	s := newTestServer(t, nil)
	dir := t.TempDir()
	ready, release, emitted := filepath.Join(dir, "ready"), filepath.Join(dir, "release"), filepath.Join(dir, "emitted")
	script := filepath.Join(dir, "pgbackrest")
	// Deliberately retain stdout/stderr across setsid instead of doing
	// pgBackRest's usual stdio close. Write more than a pipe can buffer, only
	// AFTER the foreground call has returned, then acknowledge that it drained.
	require.NoError(t, os.WriteFile(script, fmt.Appendf(nil, `#!/bin/sh
setsid sh -c 'trap "" TERM; echo $$ > "$1"; while [ ! -f "$2" ]; do sleep 0.01; done; head -c 262144 /dev/zero; echo drained > "$3"; exec sleep 600' sh %q %q %q </dev/null &
exit 0
`, ready, release, emitted), 0o700))
	s.bin = script
	var out bytes.Buffer
	code, err := s.runChild(t.Context(), KindS3Command, cmdArchivePush, nil, &out, &out)
	require.NoError(t, err)
	require.Zero(t, code)
	require.Eventually(t, func() bool { _, err := os.Stat(ready); return err == nil }, 5*time.Second, 5*time.Millisecond)
	b, err := os.ReadFile(ready)
	require.NoError(t, err)
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	require.NoError(t, err)
	require.NoError(t, syscall.Kill(pid, 0), "foreground exit must not kill the async worker")
	require.NoError(t, os.WriteFile(release, nil, 0o600))
	require.Eventually(t, func() bool { _, err := os.Stat(emitted); return err == nil }, 5*time.Second, 5*time.Millisecond, "post-response output must drain, not block or SIGPIPE")
	require.Empty(t, out.String(), "worker must no longer retain the caller's output buffer")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.NoError(t, s.quiesceWorkers(ctx))
	require.ErrorIs(t, syscall.Kill(pid, 0), syscall.ESRCH)
}

// executil.Cmd.Stop signals the daemon's whole process group. Ownership helpers
// must not die in that blast radius: the daemon closes their lifetime channels
// and waits for their own subreaper ACK before a same-state replacement starts.
func TestOwnedSupervisorSurvivesDaemonGroupTERM(t *testing.T) {
	template := newTestServer(t, nil)
	cfg := template.cfg
	require.NoError(t, template.Close())
	ready := filepath.Join(t.TempDir(), "daemon.pid")
	script := filepath.Join(t.TempDir(), "pgbackrest")
	require.NoError(t, os.WriteFile(script, fmt.Appendf(nil, "#!/bin/sh\nsetsid sh -c 'trap \"\" TERM; echo $$ > \"$1\"; exec sleep 600' sh %q </dev/null >/dev/null 2>&1 &\nexit 0\n", ready), 0o700))
	raw, err := json.Marshal(cfg)
	require.NoError(t, err)
	self, err := os.Executable()
	require.NoError(t, err)
	parent := exec.Command(self, "-test.run=^TestHelperOwnedParent$")
	parent.Env = append(os.Environ(), "OWNED_PARENT_CONFIG="+string(raw), "OWNED_PARENT_SCRIPT="+script, "OWNED_PARENT_GROUP_STOP=1")
	parent.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdout, err := parent.StdoutPipe()
	require.NoError(t, err)
	var diagnostics bytes.Buffer
	parent.Stderr = &diagnostics
	require.NoError(t, parent.Start())
	t.Cleanup(func() {
		if parent.ProcessState == nil {
			//nolint:gocritic // test owns exec.Cmd; abrupt fallback cleanup must not replace the group-TERM event being tested
			if err := parent.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
				t.Errorf("kill parent fixture: %v", err)
			}
			_ = parent.Wait()
		}
	})
	line, err := bufio.NewReader(stdout).ReadString('\n')
	require.NoError(t, err)
	supervisor, err := strconv.Atoi(strings.TrimSpace(line))
	require.NoError(t, err)
	require.Positive(t, supervisor)
	require.Eventually(t, func() bool { _, err := os.Stat(ready); return err == nil }, 5*time.Second, time.Millisecond)
	pidText, err := os.ReadFile(ready)
	require.NoError(t, err)
	pid, err := strconv.Atoi(strings.TrimSpace(string(pidText)))
	require.NoError(t, err)
	// Cleanup only targets the still-unreaped test-owned parent; the worker is
	// owned by its supervisor and is never signalled using a stale PID here.
	require.NoError(t, syscall.Kill(pid, 0), "worker must be live at the signal boundary")
	require.NoError(t, syscall.Kill(-parent.Process.Pid, syscall.SIGTERM))
	require.NoError(t, parent.Wait(), diagnostics.String())
	require.ErrorIs(t, syscall.Kill(pid, 0), syscall.ESRCH, "successful shutdown is after worker reap")
	markers, err := filepath.Glob(filepath.Join(cfg.StateDir, "worker-*.pending"))
	require.NoError(t, err)
	require.Empty(t, markers)
	next, err := newForTest(cfg)
	require.NoError(t, err, "same-state restart requires durable quiescence")
	require.NoError(t, next.Close())
}
