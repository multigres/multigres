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
	"bytes"
	"context"
	"errors"
	"maps"
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
	requireServerWorker(t, pid)

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
	alive, err := serverWorkersAlive(pid, 0)
	require.NoError(t, err)
	require.False(t, alive)

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
	requireServerWorker(t, pid)

	cancel, wait := runBackup(s)
	defer func() { cancel(); wait() }()
	require.Eventually(t, func() bool { return startCount(logFile) == 1 }, 5*time.Second, 10*time.Millisecond)
	assert.Contains(t, logs.String(), "still running")
}

func requireServerWorker(t *testing.T, pid int) {
	t.Helper()
	require.Eventually(t, func() bool {
		alive, err := serverWorkersAlive(pid, 0)
		return err == nil && alive
	}, 5*time.Second, 10*time.Millisecond, "the fake server never had a worker")
}

// When the process table cannot be read the wait cannot conclude the server
// has no workers: it runs to its bound, says why, and then starts the command.
func TestRunChild_UnreadableProcessTableWaitsTheBound(t *testing.T) {
	oldWait, oldRead := serverWorkersWait, readProcessTableFn
	serverWorkersWait = 400 * time.Millisecond
	t.Cleanup(func() { serverWorkersWait, readProcessTableFn = oldWait, oldRead })
	logFile := fakeServerPgbackrest(t, "exec sleep 300")
	s := newTestServer(t, nil)
	logs := captureLogs(s)
	startServer(t, s)
	readProcessTableFn = func() (processTable, error) { return processTable{}, errors.New("table unreadable") }

	began := time.Now()
	cancel, wait := runBackup(s)
	defer func() { cancel(); wait() }()
	require.Eventually(t, func() bool { return startCount(logFile) == 1 }, 5*time.Second, 10*time.Millisecond)
	assert.GreaterOrEqual(t, time.Since(began), 400*time.Millisecond, "an unreadable table must not read as no workers")
	assert.Contains(t, logs.String(), "cannot tell whether pgbackrest server workers are running")
	assert.Contains(t, logs.String(), "table unreadable")
}

// startTree runs script under sh in its own process group and returns the
// shell's pid. The whole group is killed when the test ends.
func startTree(t *testing.T, script string) int {
	t.Helper()
	cmd := exec.Command("sh", "-c", script)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
	})
	return cmd.Process.Pid
}

func requireDescendants(t *testing.T, root int, want bool, msg string) {
	t.Helper()
	require.Eventually(t, func() bool {
		alive, err := serverWorkersAlive(root, 0)
		return err == nil && alive == want
	}, 10*time.Second, 10*time.Millisecond, msg)
}

// The server's workers are its children, and whatever a worker starts is a
// grandchild; both count, on every platform the sidecar runs on.
func TestDescendantsAlive_SeesChildrenAndGrandchildren(t *testing.T) {
	if _, err := readProcessTable(); err != nil {
		t.Skipf("no process table on this platform: %v", err)
	}
	child := startTree(t, "sleep 300 & wait")
	requireDescendants(t, child, true, "a direct child must count")

	grandchild := startTree(t, "(sleep 300 & wait) & wait")
	require.Eventually(t, func() bool {
		table, err := readProcessTable()
		if err != nil {
			return false
		}
		for _, p := range table.procs {
			if p.parent != grandchild && table.procs[p.parent].parent == grandchild {
				return true
			}
		}
		return false
	}, 10*time.Second, 10*time.Millisecond, "the tree never grew a grandchild")
	requireDescendants(t, grandchild, true, "a grandchild must count")

	alive, err := serverWorkersAlive(os.Getpid(), 0)
	require.NoError(t, err)
	assert.True(t, alive, "this test's own children are alive")
}

func TestDescendantsAlive_FalseOnceTheyExit(t *testing.T) {
	if _, err := readProcessTable(); err != nil {
		t.Skipf("no process table on this platform: %v", err)
	}
	// The shell stays; its only child exits after a moment and is reaped.
	root := startTree(t, "sleep 0.5 & wait; exec sleep 300")
	requireDescendants(t, root, true, "the short-lived child must be seen while it runs")
	requireDescendants(t, root, false, "an exited child must stop counting")
}

// A worker that has exited but is not yet reaped holds no lock, so it must not
// keep a backup waiting. exec turns the shell into a sleep that never reaps
// the child it started.
func TestDescendantsAlive_IgnoresZombies(t *testing.T) {
	if _, err := readProcessTable(); err != nil {
		t.Skipf("no process table on this platform: %v", err)
	}
	root := startTree(t, "sleep 0.5 & exec sleep 300")
	requireDescendants(t, root, true, "the child must be seen while it runs")
	requireDescendants(t, root, false, "a zombie child must not count as alive")
}

func TestProcessTable_HasWorker(t *testing.T) {
	const supervisor, server = 10, 11
	base := map[int]procEntry{
		1:          {parent: 0, group: 1},
		supervisor: {parent: 1, group: supervisor},
		server:     {parent: supervisor, group: server},
		50:         {parent: 1, group: 50},
	}
	with := func(pid int, e procEntry) processTable {
		procs := maps.Clone(base)
		procs[pid] = e
		return processTable{procs: procs}
	}
	assert.False(t, processTable{procs: base}.hasWorker(supervisor, server), "the supervisor and the server are not workers")
	assert.True(t, with(20, procEntry{parent: server, group: server}).hasWorker(supervisor, server), "a child of the server")
	assert.False(t, with(21, procEntry{parent: 20, group: 99}).hasWorker(supervisor, server), "a process whose parent is not in the table and whose group is foreign")
	assert.True(t, with(22, procEntry{parent: supervisor, group: server}).hasWorker(supervisor, server), "an orphan reparented to the subreaper")
	assert.True(t, with(23, procEntry{parent: 1, group: server}).hasWorker(supervisor, server), "an orphan reparented to init, still in the server's group")
	assert.True(t, with(24, procEntry{parent: 1, group: supervisor}).hasWorker(supervisor, server), "an orphan in the supervisor's group")
	assert.False(t, with(25, procEntry{parent: 50, group: 50}).hasWorker(supervisor, server), "an unrelated process")
	assert.True(t, processTable{procs: base}.hasWorker(supervisor, 0), "until the server's pid is reported, the supervisor's one child counts")
	noServer := processTable{procs: map[int]procEntry{1: {parent: 0, group: 1}, supervisor: {parent: 1, group: supervisor}, 26: {parent: 1, group: 0}}}
	assert.False(t, noServer.hasWorker(supervisor, 0), "group 0 does not match an unknown server")
	assert.False(t, processTable{procs: map[int]procEntry{5: {parent: 6}, 6: {parent: 5}}}.hasWorker(7, 0), "a cycle from pid reuse ends")
}

// A table with a row that could not be read can still prove a worker exists,
// but not that none does.
func TestServerWorkersAlive_IncompleteTable(t *testing.T) {
	old := readProcessTableFn
	t.Cleanup(func() { readProcessTableFn = old })
	unread := errors.New("pid 77: permission denied")

	readProcessTableFn = func() (processTable, error) {
		return processTable{procs: map[int]procEntry{10: {parent: 1, group: 10}}, unread: unread}, nil
	}
	alive, err := serverWorkersAlive(10, 0)
	require.ErrorIs(t, err, unread, "no worker in sight is not proof when a row is missing")
	assert.False(t, alive)

	readProcessTableFn = func() (processTable, error) {
		return processTable{procs: map[int]procEntry{10: {parent: 1, group: 10}, 20: {parent: 10, group: 10}}, unread: unread}, nil
	}
	alive, err = serverWorkersAlive(10, 0)
	require.NoError(t, err, "a worker that is in sight is proof enough")
	assert.True(t, alive)
}

// The server can die while one of its workers is still in the middle of a
// backup. The worker is then no descendant of the server any more, but it
// holds the backup lock until it exits, so a backup must not start before it
// is gone. What becomes of the orphan differs by platform (on Linux the
// supervisor adopts it and lets it finish, elsewhere the supervisor ends its
// process group), so this asserts only the order: worker gone, then backup.
func TestRunChild_BackupStartsOnlyAfterAnOrphanedServerWorkerIsGone(t *testing.T) {
	workerPID := filepath.Join(t.TempDir(), "worker.pid")
	// The server starts one worker and exits at once.
	logFile := fakeServerPgbackrest(t, "sleep 1.5 &\necho $! > "+workerPID+"\nexit 1")
	s := newTestServer(t, nil)
	pid := startServer(t, s)
	require.Eventually(t, func() bool { return syscall.Kill(pid, 0) != nil }, 5*time.Second, time.Millisecond, "the fake server never exited")
	raw, err := os.ReadFile(workerPID)
	require.NoError(t, err)
	worker, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	require.NoError(t, err)

	cancel, wait := runBackup(s)
	defer func() { cancel(); wait() }()
	for deadline := time.Now().Add(10 * time.Second); startCount(logFile) == 0; {
		require.True(t, time.Now().Before(deadline), "the backup never started")
		time.Sleep(time.Millisecond)
	}
	assert.Error(t, syscall.Kill(worker, 0), "the backup started while the orphaned worker %d was alive", worker)
}
