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

//go:build linux

package pgbackrestd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// backupLockOwner reads the real backup lock's owner and tests the flock on
// that same inode. Missing/partially written records are not readiness.
func backupLockOwner(path string) (int, bool, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if errors.Is(err, os.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	defer f.Close()
	var owner struct {
		PID int `json:"pid"`
	}
	if err := json.NewDecoder(f).Decode(&owner); err != nil {
		return 0, false, nil
	}
	err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if err == nil {
		return owner.PID, false, nil
	} // Close releases our successful probe.
	if errors.Is(err, unix.EWOULDBLOCK) {
		return owner.PID, true, nil
	}
	return 0, false, err
}

func backupRole(pid int, role string) bool {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
	return err == nil && slices.Contains(strings.Split(string(raw), "\x00"), role)
}

func processParent(pid int) int {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(raw)[bytes.LastIndexByte(raw, ')')+1:])
	if len(fields) < 2 {
		return 0
	}
	parent, _ := strconv.Atoi(fields[1])
	return parent
}

type pausedBackupProcess struct{ pid, fd int }

// pidfds keep fixture signals attached to the exact processes observed, even
// if one exits before STOP/CONT. The ownership supervisor is never paused.
func closePausedBackup(processes []pausedBackupProcess, resume bool) error {
	var result error
	for _, p := range processes {
		if resume {
			if err := unix.PidfdSendSignal(p.fd, unix.SIGCONT, nil, 0); err != nil && !errors.Is(err, unix.ESRCH) {
				result = errors.Join(result, err)
			}
		}
		result = errors.Join(result, unix.Close(p.fd))
	}
	return result
}

// A real backup is cancelled mid-flight and the next backup is started at
// once, as multipooler does when it loses its lease. Neither the first run's
// return nor the second run's start may race the dying process group for
// pgBackRest's lock ([050]). The dataset only has to keep the backup busy for
// the second it takes to see its worker; a stub holding a lock would not
// exercise pgBackRest's own local workers or its flock handling.
func TestRunChild_CancelledBackupReleasesLockBeforeNextBackup(t *testing.T) {
	c := newRealCluster(t, 0)
	c.run(t, cmdStart)
	c.run(t, cmdStanzaCreate)
	c.psql(t, "create table big as select i, md5(i::text)||md5((i+1)::text)||md5((i+2)::text) p from generate_series(1,8000000) i")
	c.psql(t, "select pg_switch_wal()")
	require.Eventually(t, func() bool { return len(archivedSegments(t, c.s.cfg.FSRepoPath)) >= 1 }, 60*time.Second, 500*time.Millisecond)
	c.run(t, cmdCheck)
	lockPath := c.s.cfg.stateSub(dirLock + "/" + Stanza + "-backup-1.lock")
	ctx, cancel := context.WithCancel(t.Context())
	type backupResult struct {
		err       error
		code      int
		stillLive []int
	}
	first := make(chan backupResult, 1)
	firstDone := make(chan struct{})
	inspectAtReturn := make(chan []int, 1)
	var output bytes.Buffer
	go func() {
		code, err := c.s.runChild(ctx, KindS3Command, cmdBackup, []string{"--type=full"}, &output, &output)
		result := backupResult{code: code, err: err}
		select {
		case pids := <-inspectAtReturn:
			for _, pid := range pids {
				if pidAlive(pid) {
					result.stillLive = append(result.stillLive, pid)
				}
			}
		default:
		}
		first <- result
		close(firstDone)
	}()
	var paused []pausedBackupProcess
	defer func() {
		// Even failed readiness uses owned cancellation to kill/reap stopped tasks;
		// never resume work or kill the supervisor as a cleanup shortcut.
		cancel()
		select {
		case <-firstDone:
		case <-time.After(20 * time.Second):
			t.Error("owned backup cleanup did not settle")
		}
		require.NoError(t, closePausedBackup(paused, false))
	}()
	var readinessErr error
	require.Eventually(t, func() bool {
		select {
		case <-firstDone:
			readinessErr = errors.New("backup completed before cancellation readiness")
			return true
		default:
		}
		if len(paused) > 0 {
			for _, p := range paused {
				state := procState(p.pid)
				if state == "" || state == "Z" || state == "X" {
					readinessErr = closePausedBackup(paused, true)
					paused = nil
					return readinessErr != nil
				}
				if state != "T" {
					return false
				}
			}
			pid, held, err := backupLockOwner(lockPath)
			if err != nil {
				readinessErr = err
				return true
			}
			if held && pid == paused[0].pid {
				return true
			}
			readinessErr = closePausedBackup(paused, true)
			paused = nil
			return readinessErr != nil
		}
		root, held, err := backupLockOwner(lockPath)
		if err != nil {
			readinessErr = err
			return true
		}
		if !held || !backupRole(root, "backup") {
			return false
		}
		// The lock owner must be a foreground of one of THIS server's supervisors,
		// not an arbitrary retained start/check/archive invocation in children.
		if _, ok := c.s.children.Load(processParent(root)); !ok {
			return false
		}
		children, err := os.ReadFile(fmt.Sprintf("/proc/%d/task/%d/children", root, root))
		if err != nil {
			return false
		}
		pids := []int{root}
		for field := range strings.FieldsSeq(string(children)) {
			pid, _ := strconv.Atoi(field)
			if processParent(pid) == root && backupRole(pid, "backup:local") {
				pids = append(pids, pid)
			}
		}
		if len(pids) < 2 {
			return false
		}
		for _, pid := range pids {
			fd, err := unix.PidfdOpen(pid, 0)
			if errors.Is(err, unix.ESRCH) {
				readinessErr = closePausedBackup(paused, false)
				paused = nil
				return readinessErr != nil
			}
			if err != nil {
				readinessErr = err
				return true
			}
			paused = append(paused, pausedBackupProcess{pid: pid, fd: fd})
		}
		// Revalidate AFTER opening pidfds. A reused numeric PID cannot make
		// us stop an unrelated process; signals target the pinned handles.
		_, ours := c.s.children.Load(processParent(root))
		valid := ours && backupRole(root, "backup")
		for _, p := range paused[1:] {
			valid = valid && processParent(p.pid) == root && backupRole(p.pid, "backup:local")
		}
		if !valid {
			readinessErr = closePausedBackup(paused, false)
			paused = nil
			return readinessErr != nil
		}
		for _, p := range paused {
			if err := unix.PidfdSendSignal(p.fd, unix.SIGSTOP, nil, 0); err != nil && !errors.Is(err, unix.ESRCH) {
				readinessErr = err
				return true
			}
		}
		// Next observation must see stopped root AND real local worker(s), held
		// flock with matching owner, and an unfinished invocation simultaneously.
		return false
	}, 60*time.Second, 10*time.Millisecond, "actual backup root/local worker and held flock must be frozen before cancellation")
	require.NoError(t, readinessErr)
	require.GreaterOrEqual(t, len(paused), 2)
	pids := make([]int, 0, len(paused))
	for _, p := range paused {
		pids = append(pids, p.pid)
	}
	inspectAtReturn <- pids
	root, held, err := backupLockOwner(lockPath)
	require.NoError(t, err)
	require.True(t, held, "frozen real backup must still hold its lock")
	require.Equal(t, paused[0].pid, root)
	select {
	case <-firstDone:
		t.Fatal("backup finished before cancellation")
	default:
	}
	cancel()
	// No wait for the first run: simulate the client's immediate successor.
	var second bytes.Buffer
	code, err := c.s.runChild(t.Context(), KindS3Command, cmdBackup, []string{"--type=full"}, &second, &second)
	require.NoError(t, err)
	assert.Zero(t, code, "the second backup must not hit [050]:\n%s", second.String())
	assert.NotContains(t, second.String(), "[050]")
	result := <-first
	require.ErrorIs(t, result.err, context.Canceled)
	assert.Empty(t, result.stillLive, "cancelled run returned before actual backup processes were reaped")
}

// fakeServerPgbackrest installs a fake pgbackrest whose server command runs
// serverBody and whose other commands record a start in the returned file and
// block.
func fakeServerPgbackrest(t *testing.T, serverBody string) (logFile string) {
	t.Helper()
	dir := t.TempDir()
	logFile = filepath.Join(dir, "starts")
	script := "#!/bin/sh\nfor a; do last=$a; done\ncase \"$last\" in\n" +
		"version) echo 'pgBackRest 2.60.0'; exit 0;;\n" +
		"server)\n" + serverBody + "\n;;\n" +
		"*) echo started >> " + logFile + "; sleep 300;;\nesac\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "pgbackrest"), []byte(script), 0o700))
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return logFile
}

// startServer runs the 8432 server child through runChild, as serve does, so
// the wiring that publishes its pid is the code under test. It returns once
// the pid is published.
func startServer(t *testing.T, s *Server) (pid int) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		var out bytes.Buffer
		_, _ = s.runChild(ctx, KindServer, "", nil, &out, &out)
	}()
	t.Cleanup(func() { cancel(); <-done })
	require.Eventually(t, func() bool { pid = int(s.serverPID.Load()); return pid != 0 }, 10*time.Second, 10*time.Millisecond)
	return pid
}

func runBackup(s *Server) (cancel func(), wait func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		var out bytes.Buffer
		_, _ = s.runChild(ctx, KindS3Command, cmdBackup, nil, &out, &out)
	}()
	return cancel, func() { <-done }
}

// A standby backup run by another pooler reaches this sidecar through the 8432
// server, and the worker it forks here holds the backup lock until the dropped
// connection reaches it. A backup that starts in that moment must wait for the
// worker, not collide with its lock.
func TestRunChild_BackupWaitsForServerWorkers(t *testing.T) {
	// The worker outlives the first half second of the test; the server itself
	// stays up afterwards, as the real one does.
	logFile := fakeServerPgbackrest(t, "sleep 1.5 &\nwait\nexec sleep 300")
	s := newTestServer(t, nil)
	logs := captureLogs(s)
	pid := startServer(t, s)
	require.Eventually(t, func() bool { return descendantsAlive(pid) }, 5*time.Second, 10*time.Millisecond)

	began := time.Now()
	cancel, wait := runBackup(s)
	defer func() { cancel(); wait() }()
	time.Sleep(500 * time.Millisecond)
	assert.Zero(t, startCount(logFile), "the backup must not start while a server worker lives")
	require.Eventually(t, func() bool { return startCount(logFile) == 1 }, 10*time.Second, 10*time.Millisecond)
	assert.GreaterOrEqual(t, time.Since(began), time.Second, "it started once the worker was gone")
	assert.Contains(t, logs.String(), "workers exited", "the wait is visible in the log")
}

// A live server with no workers costs nothing.
func TestRunChild_BackupDoesNotWaitForAServerWithNoWorkers(t *testing.T) {
	logFile := fakeServerPgbackrest(t, "exec sleep 300")
	s := newTestServer(t, nil)
	logs := captureLogs(s)
	pid := startServer(t, s)
	require.False(t, descendantsAlive(pid))

	began := time.Now()
	cancel, wait := runBackup(s)
	defer func() { cancel(); wait() }()
	require.Eventually(t, func() bool { return startCount(logFile) == 1 }, 5*time.Second, 10*time.Millisecond)
	assert.Less(t, time.Since(began), time.Second)
	assert.NotContains(t, logs.String(), "workers")
}

// A server worker that outlives the bound is reported, and the command starts.
func TestRunChild_ServerWorkerWaitIsBoundedAndWarns(t *testing.T) {
	old := serverWorkersWait
	serverWorkersWait = 300 * time.Millisecond
	t.Cleanup(func() { serverWorkersWait = old })
	logFile := fakeServerPgbackrest(t, "sleep 300 &\nwait")
	s := newTestServer(t, nil)
	logs := captureLogs(s)
	pid := startServer(t, s)
	require.Eventually(t, func() bool { return descendantsAlive(pid) }, 5*time.Second, 10*time.Millisecond)

	cancel, wait := runBackup(s)
	defer func() { cancel(); wait() }()
	require.Eventually(t, func() bool { return startCount(logFile) == 1 }, 5*time.Second, 10*time.Millisecond)
	assert.Contains(t, logs.String(), "still running")
}
