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

// familyOf returns every live pid whose process group is pgid or that descends
// from root, which finds workers that setsid out of the group too.
func familyOf(root, pgid int) []int {
	entries, _ := os.ReadDir("/proc")
	parent := map[int]int{}
	group := map[int]int{}
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		b, err := os.ReadFile("/proc/" + e.Name() + "/stat")
		if err != nil {
			continue
		}
		s := string(b)
		f := strings.Fields(s[strings.LastIndexByte(s, ')')+1:])
		pp, _ := strconv.Atoi(f[1])
		pg, _ := strconv.Atoi(f[2])
		parent[pid], group[pid] = pp, pg
	}
	in := map[int]bool{root: true}
	for changed := true; changed; {
		changed = false
		for pid, pp := range parent {
			if !in[pid] && (in[pp] || group[pid] == pgid) {
				in[pid], changed = true, true
			}
		}
	}
	var out []int
	for pid := range in {
		if procState(pid) != "" {
			out = append(out, pid)
		}
	}
	return out
}

// lockFree reports whether every lock file in dir can be flocked, which is how
// pgBackRest takes them.
func lockFree(t *testing.T, dir string) bool {
	t.Helper()
	files, _ := filepath.Glob(filepath.Join(dir, "*.lock"))
	for _, p := range files {
		f, err := os.OpenFile(p, os.O_RDWR, 0)
		if err != nil {
			continue
		}
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		f.Close()
		if err != nil {
			return false
		}
	}
	return true
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
	lockDir := c.s.cfg.stateSub(dirLock)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var (
		out       bytes.Buffer
		family    []int
		stillLive []int
	)
	first := make(chan struct{})
	go func() {
		defer close(first)
		_, _ = c.s.runChild(ctx, KindS3Command, cmdBackup, []string{"--type=full"}, &out, &out)
		for _, p := range family {
			if pidAlive(p) {
				stillLive = append(stillLive, p)
			}
		}
	}()

	var pid int
	require.Eventually(t, func() bool {
		c.s.children.Range(func(k, _ any) bool { pid = k.(int); return false })
		return pid != 0 && len(familyOf(pid, pid)) >= 2
	}, 60*time.Second, 20*time.Millisecond, "the backup must have forked its local worker")
	family = familyOf(pid, pid)
	require.False(t, lockFree(t, lockDir), "the running backup must hold the lock")

	cancel()
	// No wait for the first run: the client of a cancelled Run does not wait.
	var second bytes.Buffer
	code, err := c.s.runChild(context.Background(), KindS3Command, cmdBackup, []string{"--type=full"}, &second, &second)
	require.NoError(t, err)
	assert.Zero(t, code, "the second backup must not hit [050]:\n%s", second.String())
	assert.NotContains(t, second.String(), "[050]")

	<-first
	assert.Empty(t, stillLive, "the first run returned while its backup or worker was alive (pids %v)", family)
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

// reapGroup is what keeps runChild from returning while a SIGKILLed worker is
// still in its exit holding a lock. Here nothing has signalled the group, so
// only reapGroup's own kill can empty it before the bound.
func TestReapGroup_EmptiesTheGroupBeforeTheBound(t *testing.T) {
	old := killGroupWait
	killGroupWait = 2 * time.Second
	t.Cleanup(func() { killGroupWait = old })
	s := newTestServer(t, nil)

	cmd := exec.Command("sh", "-c", "sleep 300 & echo $!; wait")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	line := make([]byte, 32)
	n, err := stdout.Read(line)
	require.NoError(t, err)
	worker, err := strconv.Atoi(strings.TrimSpace(string(line[:n])))
	require.NoError(t, err)
	require.False(t, groupGone(cmd.Process.Pid))

	began := time.Now()
	s.reapGroup(cmd.Process.Pid, "backup")
	assert.Less(t, time.Since(began), time.Second, "reapGroup must end the wait by killing, not by timing out")
	assert.False(t, pidAlive(worker), "the worker must be gone")
	assert.True(t, groupGone(cmd.Process.Pid))
	_ = cmd.Wait()
}
