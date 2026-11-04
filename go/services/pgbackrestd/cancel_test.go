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
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
)

// procState returns the state letter of a pid from /proc, or "" when it is gone.
func procState(pid int) string {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return ""
	}
	s := string(b)
	return strings.Fields(s[strings.LastIndexByte(s, ')')+1:])[0]
}

// pidAlive is kill(pid, 0) that does not count a zombie: a reparented child
// whose new parent has not reaped it yet holds nothing.
func pidAlive(pid int) bool {
	return syscall.Kill(pid, 0) == nil && procState(pid) != "Z"
}

// loggingPgbackrest installs a fake pgbackrest that records each start in
// logFile and then blocks, with a grandchild in the same group.
func loggingPgbackrest(t *testing.T) (logFile string) {
	t.Helper()
	dir := t.TempDir()
	logFile = filepath.Join(dir, "starts")
	script := "#!/bin/sh\ncase \"$*\" in *version*) echo 'pgBackRest 2.60.0'; exit 0;; esac\necho started >> " + logFile + "\nsleep 300 &\nsleep 300\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "pgbackrest"), []byte(script), 0o700))
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return logFile
}

func startCount(logFile string) int {
	b, _ := os.ReadFile(logFile)
	return strings.Count(string(b), "started")
}

// A cancelled run must not report completion while any process of its group is
// alive: the caller treats the return as "the lock is free".
func TestRunChild_CancelReturnsOnlyAfterGroupIsGone(t *testing.T) {
	pidFile, readyFile := fakePgbackrest(t)
	s := newTestServer(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		var out bytes.Buffer
		_, _ = s.runChild(ctx, KindS3Command, "info", nil, &out, &out)
	}()
	require.Eventually(t, func() bool { _, err := os.Stat(readyFile); return err == nil }, 10*time.Second, 20*time.Millisecond)
	raw, err := os.ReadFile(pidFile)
	require.NoError(t, err)
	grandchild, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	require.NoError(t, err)

	cancel()
	<-done
	assert.False(t, pidAlive(grandchild), "the group's worker must be gone when runChild returns")
}

// The client of a cancelled Run returns at once, so the next Run can arrive
// while the first child is still dying. It must wait, not start a command that
// would collide with the first one's lock.
func TestRunChild_NextRunWaitsForACancelledOne(t *testing.T) {
	logFile := loggingPgbackrest(t)
	s := newTestServer(t, nil)

	release := make(chan struct{})
	settleHook = func(string) { <-release }
	t.Cleanup(func() { settleHook = nil })

	ctx1, cancel1 := context.WithCancel(context.Background())
	first := make(chan struct{})
	go func() {
		defer close(first)
		var out bytes.Buffer
		_, _ = s.runChild(ctx1, KindS3Command, "backup", nil, &out, &out)
	}()
	require.Eventually(t, func() bool { return startCount(logFile) == 1 }, 10*time.Second, 10*time.Millisecond)

	cancel1()
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	second := make(chan struct{})
	go func() {
		defer close(second)
		var out bytes.Buffer
		_, _ = s.runChild(ctx2, KindS3Command, "backup", nil, &out, &out)
	}()

	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, 1, startCount(logFile), "the second run must not start while the cancelled child is still being killed")
	select {
	case <-first:
		t.Fatal("the first run returned before its kill settled")
	default:
	}

	close(release)
	<-first
	require.Eventually(t, func() bool { return startCount(logFile) == 2 }, 10*time.Second, 10*time.Millisecond,
		"the second run starts once the first is gone")
	cancel2()
	<-second
}

// Liveness: waiting is only for children already cancelled. A child that is
// running normally must not block admission (pgBackRest's lock refuses a real
// concurrent command), and a caller whose own request is cancelled while it
// waits gets out.
func TestRunChild_WaitIsOnlyForCancelledChildrenAndIsInterruptible(t *testing.T) {
	logFile := loggingPgbackrest(t)
	s := newTestServer(t, nil)

	var wg sync.WaitGroup
	ctxA, cancelA := context.WithCancel(context.Background())
	ctxB, cancelB := context.WithCancel(context.Background())
	for _, ctx := range []context.Context{ctxA, ctxB} {
		wg.Go(func() {
			var out bytes.Buffer
			_, _ = s.runChild(ctx, KindS3Command, "backup", nil, &out, &out)
		})
	}
	require.Eventually(t, func() bool { return startCount(logFile) == 2 }, 10*time.Second, 10*time.Millisecond,
		"a running child must not hold up another admission")

	release := make(chan struct{})
	settleHook = func(string) { <-release }
	t.Cleanup(func() { settleHook = nil })
	cancelA()
	ctxC, cancelC := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancelC()
	var out bytes.Buffer
	code, err := s.runChild(ctxC, KindS3Command, "backup", nil, &out, &out)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, -1, code)
	assert.Equal(t, 2, startCount(logFile), "an abandoned waiter must not start a child")

	close(release)
	cancelB()
	wg.Wait()
}

// Status.active_commands is how a client knows a cancelled Run is finished: it
// lists the command while it runs and keeps listing it until the group is gone.
func TestStatus_ActiveCommandsCoverRunningAndDyingRuns(t *testing.T) {
	logFile := loggingPgbackrest(t)
	h := newControlHarness(t, nil)
	require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)
	active := func() []pb.Command {
		resp, err := h.client.Status(t.Context(), connect.NewRequest(&pb.StatusRequest{}))
		require.NoError(t, err)
		return resp.Msg.GetActiveCommands()
	}
	require.Empty(t, active())

	release := make(chan struct{})
	settleHook = func(string) { <-release }
	t.Cleanup(func() { settleHook = nil })

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = h.run(ctx, pb.Command_COMMAND_BACKUP)
	}()
	require.Eventually(t, func() bool { return startCount(logFile) == 1 }, 10*time.Second, 10*time.Millisecond)
	assert.Equal(t, []pb.Command{pb.Command_COMMAND_BACKUP}, active(), "a running command is listed")

	cancel()
	// The kill has been issued and the leader reaped, but the settle hook
	// holds the run open: it must still be listed.
	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, []pb.Command{pb.Command_COMMAND_BACKUP}, active(), "a dying command is listed until it is reaped")

	close(release)
	require.Eventually(t, func() bool { return len(active()) == 0 }, 10*time.Second, 10*time.Millisecond,
		"the entry goes once the group is gone")
	<-done
}

// syncBuffer is a log sink that tests read while the server still writes.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// captureLogs points the server's logger at a buffer. Call it before any
// goroutine uses the server.
func captureLogs(s *Server) *syncBuffer {
	b := &syncBuffer{}
	s.logger = slog.New(slog.NewTextHandler(b, nil))
	return b
}

// Liveness: a cancelled child that never finishes dying (a leader in
// uninterruptible sleep keeps cmd.Wait from returning) must not hold every
// later admission, WAL push and fetch included, until the waiter's own context
// ends.
func TestRunChild_AdmissionStopsWaitingForAStuckCancelledChild(t *testing.T) {
	old := cancelledEntryWait
	cancelledEntryWait = 300 * time.Millisecond
	t.Cleanup(func() { cancelledEntryWait = old })
	logFile := loggingPgbackrest(t)
	s := newTestServer(t, nil)
	logs := captureLogs(s)

	stuck := make(chan struct{})
	settleHook = func(string) { <-stuck }
	t.Cleanup(func() { settleHook = nil })

	ctx1, cancel1 := context.WithCancel(context.Background())
	first := make(chan struct{})
	go func() {
		defer close(first)
		var out bytes.Buffer
		_, _ = s.runChild(ctx1, KindS3Command, "backup", nil, &out, &out)
	}()
	require.Eventually(t, func() bool { return startCount(logFile) == 1 }, 10*time.Second, 10*time.Millisecond)
	cancel1()

	ctx2, cancel2 := context.WithCancel(context.Background())
	second := make(chan struct{})
	go func() {
		defer close(second)
		var out bytes.Buffer
		_, _ = s.runChild(ctx2, KindS3Command, "backup", nil, &out, &out)
	}()
	require.Eventually(t, func() bool { return startCount(logFile) == 2 }, 10*time.Second, 10*time.Millisecond,
		"the second run must start although the cancelled child never leaves")
	assert.Contains(t, logs.String(), "has not exited")

	// The hook is global, so the second run's own settle blocks on it too.
	cancel2()
	close(stuck)
	<-second
	<-first
}
