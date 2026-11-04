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
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
	"github.com/multigres/multigres/go/pb/pgbackrestdservice/pgbackrestdserviceconnect"
	"github.com/multigres/multigres/go/tools/testpoll"
)

const (
	infoMissingStanza = `[{"name":"multigres","backup":[],"repo":[{"key":1,"status":{"code":1,"message":"missing stanza path"}}]}]`
	infoEmptyStanza   = `[{"name":"multigres","backup":[],"repo":[{"key":1,"status":{"code":2,"message":"no valid backups"}}]}]`
	infoWithBackup    = `[{"name":"multigres","backup":[{"label":"20261116-100000F","type":"full","error":false}],"repo":[{"key":1,"status":{"code":0,"message":"ok"}}]}]`
	infoUnreachable   = `[{"name":"multigres","backup":[],"repo":[{"key":1,"status":{"code":99,"message":"unable to reach the repository"}}]}]`
)

// exclusiveHarness is a configured sidecar over a fake pgbackrest that records
// the server pause and resume next to the commands.
func newExclusiveHarness(t *testing.T, o fakeOpts) (*controlHarness, *fakeBackrest) {
	t.Helper()
	fake := newFakeBackrestOpts(t, o)
	h := newControlHarness(t, nil)
	h.s.pauseFn = pauseRecorder(t, fake)
	// A latch would stop admission and end the serve loop, which this harness
	// does not run: tests that look at the aftermath keep the gate usable.
	h.s.latchExit = func() {}
	require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)
	return h, fake
}

func (h *controlHarness) gateOpen() bool {
	release, ok := h.s.gate.TryEnter()
	if ok {
		release()
	}
	return ok
}

func (h *controlHarness) requireGateOpen(t *testing.T) {
	t.Helper()
	require.True(t, h.gateOpen(), "the gate reopens")
}

func shortTimes(t *testing.T) {
	t.Helper()
	oldStep, oldInfo, oldDrain, oldDelay := resetStepTimeout, preflightInfoTimeout, resetDrainWait, startRetryDelay
	resetStepTimeout, preflightInfoTimeout, resetDrainWait = 5*time.Second, 3*time.Second, 200*time.Millisecond
	t.Cleanup(func() {
		resetStepTimeout, preflightInfoTimeout, resetDrainWait, startRetryDelay = oldStep, oldInfo, oldDrain, oldDelay
	})
}

func deleteStanza(h *controlHarness, ctx context.Context) (runResult, error) {
	return h.run(ctx, pb.Command_COMMAND_STANZA_DELETE)
}

func TestStanzaDelete_RunsTheSequence(t *testing.T) {
	for name, info := range map[string]string{
		"stanza missing":          infoMissingStanza,
		"no backups, valid state": infoEmptyStanza,
	} {
		t.Run(name, func(t *testing.T) {
			h, fake := newExclusiveHarness(t, fakeOpts{infoJSON: info})
			spool := h.s.cfg.stateSub(dirSpool + "/archive/multigres/out")
			require.NoError(t, os.MkdirAll(spool, 0o700))
			require.NoError(t, os.WriteFile(filepath.Join(spool, "000000010000000000000001.ok"), nil, 0o600))

			res, err := deleteStanza(h, t.Context())
			require.NoError(t, err)
			assert.Equal(t, int32(0), res.exit)
			assert.Equal(t, append(configurePause, "start", "stop", "info", "stanza-delete", "start", "RESUME"), shortened(fake.lines(t)))
			assert.Contains(t, fake.lines(t), "KILL", "stop --force took effect")
			_, statErr := os.Stat(h.s.cfg.stateSub(dirSpool + "/archive"))
			assert.True(t, os.IsNotExist(statErr), "the push spool is cleared")
			_, statErr = os.Stat(fake.stop)
			assert.True(t, os.IsNotExist(statErr), "archiving is started again")
			h.requireGateOpen(t)
			assert.Empty(t, h.s.activeCommands())
		})
	}
}

func TestStanzaDelete_PreflightRefusals(t *testing.T) {
	cases := map[string]fakeOpts{
		"a backup exists":        {infoJSON: infoWithBackup},
		"status 99 with exit 0":  {infoJSON: infoUnreachable},
		"malformed JSON":         {infoJSON: `[{"name":`},
		"empty document":         {infoJSON: `[]`},
		"info exits nonzero":     {infoJSON: infoEmptyStanza, failing: []string{"info"}},
		"info hangs":             {hang: []string{"info"}},
		"truncated, too large":   {infoBytes: 4096},
		"not json at all":        {},
		"json null, no stanzas ": {infoJSON: `null`},
	}
	for name, o := range cases {
		t.Run(name, func(t *testing.T) {
			shortTimes(t)
			old := maxPreflightInfoBytes
			maxPreflightInfoBytes = 1024
			t.Cleanup(func() { maxPreflightInfoBytes = old })
			h, fake := newExclusiveHarness(t, o)

			_, err := deleteStanza(h, t.Context())
			require.Error(t, err)
			cmds := shortened(fake.lines(t))
			assert.NotContains(t, cmds, "stanza-delete", "nothing is deleted on a refusal")
			assert.Equal(t, append(configurePause, "start", "stop", "info", "start", "RESUME"), cmds, "the unwind still runs")
			h.requireGateOpen(t)
			assert.Empty(t, h.s.activeCommands())
		})
	}
}

func TestStanzaDelete_FailedDeleteReportsExitAndUnwinds(t *testing.T) {
	h, fake := newExclusiveHarness(t, fakeOpts{infoJSON: infoEmptyStanza, failing: []string{"stanza-delete"}})
	spool := h.s.cfg.stateSub(dirSpool + "/archive")
	require.NoError(t, os.MkdirAll(spool, 0o700))

	res, err := deleteStanza(h, t.Context())
	require.NoError(t, err)
	assert.Equal(t, int32(7), res.exit)
	assert.Contains(t, res.stderr, "boom stanza-delete")
	assert.Equal(t, append(configurePause, "start", "stop", "info", "stanza-delete", "start", "RESUME"), shortened(fake.lines(t)))
	_, statErr := os.Stat(spool)
	assert.NoError(t, statErr, "the spool is only cleared after a delete that worked")
	h.requireGateOpen(t)
}

func TestStanzaDelete_RefusesTwoRepositoriesBeforeQuiescing(t *testing.T) {
	fake := newFakeBackrestOpts(t, fakeOpts{infoJSON: infoEmptyStanza})
	h := newControlHarness(t, nil)
	h.s.pauseFn = pauseRecorder(t, fake)
	require.True(t, h.configure(t, 1, repos(1, 1, 2, 2)).Accepted)

	_, err := deleteStanza(h, t.Context())
	require.Error(t, err)
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
	assert.Equal(t, configurePause[:2], shortened(fake.lines(t)), "no pause, no child: the gate was never closed")
	h.requireGateOpen(t)
}

func TestStanzaDelete_NeedsConfigure(t *testing.T) {
	fake := newFakeBackrestOpts(t, fakeOpts{})
	h := newControlHarness(t, nil)
	_, err := deleteStanza(h, t.Context())
	require.Error(t, err)
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
	assert.Empty(t, fake.lines(t))
}

func TestStanzaDelete_RejectsOptions(t *testing.T) {
	h, fake := newExclusiveHarness(t, fakeOpts{infoJSON: infoEmptyStanza})
	for _, arg := range []string{"--force", "--repo=1", "--io-timeout=3"} {
		_, err := h.run(t.Context(), pb.Command_COMMAND_STANZA_DELETE, arg)
		require.Error(t, err, arg)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), arg)
	}
	assert.Equal(t, configurePause[:2], shortened(fake.lines(t)))
}

// A caller that goes away must not leave a delete running: the child is never
// spawned or is killed and reaped, and the unwind runs regardless.
func TestStanzaDelete_CancelledCallerNeverLeavesADelete(t *testing.T) {
	for _, step := range []string{"stop", "info", "stanza-delete"} {
		t.Run("during "+step, func(t *testing.T) {
			h, fake := newExclusiveHarness(t, fakeOpts{infoJSON: infoEmptyStanza, hang: []string{step}})
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() { _, err := deleteStanza(h, ctx); done <- err }()
			require.Eventually(t, func() bool { return slices.Contains(shortened(fake.lines(t)), step) }, 10*time.Second, 5*time.Millisecond)
			cancel()
			require.Error(t, <-done)

			require.Eventually(t, func() bool {
				l := shortened(fake.lines(t))
				return len(l) > 0 && l[len(l)-1] == "RESUME" && h.gateOpen()
			}, 10*time.Second, 5*time.Millisecond, "the unwind completes: %v", fake.lines(t))
			cmds := shortened(fake.lines(t))
			switch step {
			case "stop":
				assert.NotContains(t, cmds, "info")
				assert.NotContains(t, cmds, "stanza-delete")
			case "info":
				assert.NotContains(t, cmds, "stanza-delete")
			}
			assert.Equal(t, "start", cmds[len(cmds)-2], "archiving is started again")
			require.Eventually(t, func() bool { return len(h.s.activeCommands()) == 0 }, 10*time.Second, 5*time.Millisecond)
			_, statErr := os.Stat(fake.stop)
			assert.True(t, os.IsNotExist(statErr))
		})
	}
}

// Cancelled before the sidecar admits it, nothing runs at all.
func TestStanzaDelete_CancelledBeforeAdmission(t *testing.T) {
	h, fake := newExclusiveHarness(t, fakeOpts{infoJSON: infoEmptyStanza})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, spawned, err := h.s.runExclusive(ctx, &pb.RunRequest{Command: pb.Command_COMMAND_STANZA_DELETE, ExpectedInstanceId: h.s.incarnation}, newStreamSender(discardStream{}))
	assert.False(t, spawned)
	require.Error(t, err)
	assert.Equal(t, configurePause[:2], shortened(fake.lines(t)))
	h.requireGateOpen(t)
	assert.Empty(t, h.s.activeCommands())
}

type discardStream struct{}

func (discardStream) Send(*pb.RunOutput) error { return nil }

// The caller's lease protects the delete, not the unwind: the command leaves
// active_commands once the destructive child is reaped, while the unwind may
// still be retrying start with the gate closed.
func TestStanzaDelete_LeavesActiveCommandsBeforeTheUnwindEnds(t *testing.T) {
	startRetryDelayOld := startRetryDelay
	startRetryDelay = 300 * time.Millisecond
	t.Cleanup(func() { startRetryDelay = startRetryDelayOld })
	// Call 1 is the start before stop; 2 to 4 are the unwind's.
	h, _ := newExclusiveHarness(t, fakeOpts{infoJSON: infoEmptyStanza, failStartCalls: []int{2, 3, 4}})

	done := make(chan error, 1)
	go func() { _, err := deleteStanza(h, t.Context()); done <- err }()
	require.Eventually(t, func() bool {
		return !h.gateOpen() && len(h.s.activeCommands()) == 0 && h.s.startPending.Load() == 0
	}, 10*time.Second, 5*time.Millisecond, "the delete left the list while the unwind still held the gate")
	require.Error(t, <-done)
}

func TestExclusive_StartFailureFailsTheRunAndRetriesInTheBackground(t *testing.T) {
	for _, cmd := range []pb.Command{pb.Command_COMMAND_STANZA_DELETE, pb.Command_COMMAND_ARCHIVE_SPOOL_RESET} {
		t.Run(cmd.String(), func(t *testing.T) {
			// Call 1 is the start before stop; 2 to 4 fail; 5 is the retry.
			h, fake := newExclusiveHarness(t, fakeOpts{infoJSON: infoEmptyStanza, failStartCalls: []int{2, 3, 4}})
			_, err := h.run(t.Context(), cmd)
			require.Error(t, err, "a delete that worked still fails when archiving stays stopped")
			assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
			assert.Contains(t, err.Error(), "WAL archiving is stopped")
			h.requireGateOpen(t)
			require.Eventually(t, func() bool { _, e := os.Stat(fake.stop); return os.IsNotExist(e) }, 10*time.Second, 5*time.Millisecond,
				"the background retry cleared the stop file")
		})
	}
}

// An older retry still asleep (a restore whose start failed) does not decide
// the outcome of a later reset.
func TestExclusive_OlderStartRetryDoesNotFailTheRun(t *testing.T) {
	h, _ := newExclusiveHarness(t, fakeOpts{infoJSON: infoEmptyStanza})
	h.s.startPending.Add(1)
	t.Cleanup(func() { h.s.startPending.Add(-1) })
	for _, cmd := range []pb.Command{pb.Command_COMMAND_STANZA_DELETE, pb.Command_COMMAND_ARCHIVE_SPOOL_RESET} {
		res, err := h.run(t.Context(), cmd)
		require.NoError(t, err, cmd.String())
		assert.Equal(t, int32(0), res.exit)
	}
}

func TestArchiveSpoolReset_NeedsNoConfigure(t *testing.T) {
	fake := newFakeBackrestOpts(t, fakeOpts{})
	h := newControlHarness(t, nil)
	h.s.pauseFn = pauseRecorder(t, fake)
	require.False(t, h.s.configured())

	res, err := h.run(t.Context(), pb.Command_COMMAND_ARCHIVE_SPOOL_RESET)
	require.NoError(t, err)
	assert.Equal(t, int32(0), res.exit)
	assert.Equal(t, []string{"PAUSE", "start", "stop", "start", "RESUME"}, shortened(fake.lines(t)))
	h.requireGateOpen(t)
}

// Clearing goes through safefs: a planted link is removed, never followed, and
// an absent spool is fine.
func TestArchiveSpoolReset_ClearsThroughSafefs(t *testing.T) {
	h, _ := newExclusiveHarness(t, fakeOpts{})
	outside := t.TempDir()
	victim := filepath.Join(outside, "keep")
	require.NoError(t, os.WriteFile(victim, []byte("x"), 0o600))
	spool := h.s.cfg.stateSub(dirSpool)
	out := filepath.Join(spool, "archive", Stanza, "out")
	require.NoError(t, os.MkdirAll(out, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(out, "000000010000000000000001.ok"), nil, 0o600))
	require.NoError(t, os.Symlink(outside, filepath.Join(spool, "archive", Stanza, "link")))
	other := filepath.Join(spool, "restore-keep")
	require.NoError(t, os.WriteFile(other, nil, 0o600))

	_, err := h.run(t.Context(), pb.Command_COMMAND_ARCHIVE_SPOOL_RESET)
	require.NoError(t, err)
	_, statErr := os.Lstat(filepath.Join(spool, "archive"))
	assert.True(t, os.IsNotExist(statErr))
	assert.FileExists(t, victim, "a link was not followed")
	assert.FileExists(t, other, "only spool/archive goes")

	_, err = h.run(t.Context(), pb.Command_COMMAND_ARCHIVE_SPOOL_RESET)
	require.NoError(t, err, "an absent spool is not an error")
}

// A command that needs the gate keeps a reset out: the drain times out, nothing
// runs, and the gate opens again.
func TestExclusive_DrainTimeoutRunsNothing(t *testing.T) {
	for _, cmd := range []pb.Command{pb.Command_COMMAND_STANZA_DELETE, pb.Command_COMMAND_ARCHIVE_SPOOL_RESET} {
		t.Run(cmd.String(), func(t *testing.T) {
			shortTimes(t)
			h, fake := newExclusiveHarness(t, fakeOpts{infoJSON: infoEmptyStanza})
			busy, ok := h.s.gate.TryEnter() // a running backup, or a WAL request
			require.True(t, ok)

			_, err := h.run(t.Context(), cmd)
			require.Error(t, err)
			assert.Equal(t, connect.CodeUnavailable, connect.CodeOf(err))
			assert.Equal(t, configurePause[:2], shortened(fake.lines(t)), "nothing ran and the server was not paused")
			busy()
			h.requireGateOpen(t)
			assert.Empty(t, h.s.activeCommands())
		})
	}
}

func TestExclusive_SecondExclusiveOperationIsUnavailableAtOnce(t *testing.T) {
	h, fake := newExclusiveHarness(t, fakeOpts{infoJSON: infoEmptyStanza})
	reopen, err := h.s.gate.Close(t.Context(), time.Second) // a restore, a delete or a reset in progress
	require.NoError(t, err)

	for _, cmd := range []pb.Command{pb.Command_COMMAND_STANZA_DELETE, pb.Command_COMMAND_ARCHIVE_SPOOL_RESET, pb.Command_COMMAND_RESTORE} {
		start := time.Now()
		_, err := h.run(t.Context(), cmd)
		require.Error(t, err, cmd.String())
		assert.Equal(t, connect.CodeUnavailable, connect.CodeOf(err), cmd.String())
		assert.Less(t, time.Since(start), 5*time.Second)
	}
	assert.Equal(t, configurePause[:2], shortened(fake.lines(t)))
	reopen()
	h.requireGateOpen(t)
}

func TestExclusive_ConfigureIsRefusedWhileQuiesced(t *testing.T) {
	fake := newFakeBackrestOpts(t, fakeOpts{infoJSON: infoEmptyStanza, hang: []string{"stop"}})
	h := newControlHarness(t, nil)
	require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { _, err := deleteStanza(h, ctx); done <- err }()
	require.Eventually(t, func() bool { return slices.Contains(shortened(fake.lines(t)), "stop") }, 10*time.Second, 5*time.Millisecond)

	_, err := h.client.Configure(t.Context(), connect.NewRequest(&pb.ConfigureRequest{LocationFingerprint: h.s.cfg.locationFingerprint(), Revision: 2, Repos: repos(1, 1)}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeUnavailable, connect.CodeOf(err))
	assert.Contains(t, err.Error(), "the repository is quiesced")
	cancel()
	<-done
}

func TestExclusive_DoesNotTouchRestoreState(t *testing.T) {
	h, _ := newExclusiveHarness(t, fakeOpts{infoJSON: infoEmptyStanza})
	h.s.lastRestoreOK.Store(true)
	h.s.restoreMu.Lock()
	rec := h.s.restoreRecord
	h.s.restoreMu.Unlock()

	for _, cmd := range []pb.Command{pb.Command_COMMAND_STANZA_DELETE, pb.Command_COMMAND_ARCHIVE_SPOOL_RESET} {
		_, err := h.run(t.Context(), cmd)
		require.NoError(t, err)
	}
	assert.True(t, h.s.lastRestoreOK.Load())
	assert.Zero(t, h.s.restoreActive.Load())
	h.s.restoreMu.Lock()
	assert.Same(t, rec, h.s.restoreRecord)
	h.s.restoreMu.Unlock()
}

var ioTimeoutFlag = regexp.MustCompile(`--io-timeout=(\d+)`)

func ioTimeoutOf(line string) string {
	if m := ioTimeoutFlag.FindStringSubmatch(line); m != nil {
		return m[1]
	}
	return ""
}

// linesFor returns the logged argv lines whose pgbackrest command is cmd.
func linesFor(t *testing.T, fake *fakeBackrest, cmd string) []string {
	t.Helper()
	var out []string
	for _, l := range fake.lines(t) {
		if got := shortened([]string{l}); len(got) == 1 && got[0] == cmd {
			out = append(out, l)
		}
	}
	return out
}

func TestIOTimeout_OnlyForRepositoryCommandsAndFromTheirContext(t *testing.T) {
	h, fake := newExclusiveHarness(t, fakeOpts{infoJSON: infoEmptyStanza})
	withDeadline := func(d time.Duration) context.Context {
		ctx, cancel := context.WithTimeout(t.Context(), d)
		t.Cleanup(cancel)
		return ctx
	}

	_, err := h.run(withDeadline(30*time.Second), pb.Command_COMMAND_INFO, "--output=json")
	require.NoError(t, err)
	_, err = h.run(withDeadline(30*time.Second), pb.Command_COMMAND_STANZA_CREATE)
	require.NoError(t, err)
	_, err = h.runReq(withDeadline(30*time.Second), &pb.RunRequest{Command: pb.Command_COMMAND_REPO_LS, Args: []string{"--output=json"}})
	require.NoError(t, err)
	_, err = h.run(withDeadline(30*time.Second), pb.Command_COMMAND_CHECK)
	require.NoError(t, err)

	info, create, ls, check := linesFor(t, fake, "info"), linesFor(t, fake, "stanza-create"), linesFor(t, fake, "repo-ls"), linesFor(t, fake, "check")
	require.Len(t, info, 1)
	require.Len(t, create, 1)
	require.Len(t, ls, 1)
	require.Len(t, check, 1)
	for name, l := range map[string]string{"info": info[0], "stanza-create": create[0], "repo-ls": ls[0]} {
		got := ioTimeoutOf(l)
		assert.Contains(t, []string{"19", "18"}, got, "%s: two thirds of the client's 30s: %s", name, l)
	}
	assert.Empty(t, ioTimeoutOf(check[0]), "check does not take the option")

	// No deadline, and a deadline of 90s or more, leave pgBackRest's default.
	_, err = h.run(t.Context(), pb.Command_COMMAND_INFO, "--output=json")
	require.NoError(t, err)
	_, err = h.run(withDeadline(5*time.Minute), pb.Command_COMMAND_INFO, "--output=json")
	require.NoError(t, err)
	info = linesFor(t, fake, "info")
	require.Len(t, info, 3)
	assert.Empty(t, ioTimeoutOf(info[1]))
	assert.Empty(t, ioTimeoutOf(info[2]))
}

// The delete's own children get the option from the step they run under (a
// delete's 30s step gives 19s), never from the request, and stop and start
// never get it: pgBackRest rejects it there.
func TestIOTimeout_InternalStepsUseTheirOwnContext(t *testing.T) {
	h, fake := newExclusiveHarness(t, fakeOpts{infoJSON: infoEmptyStanza})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	_, err := deleteStanza(h, ctx)
	require.NoError(t, err)

	for _, cmd := range []string{"info", "stanza-delete"} {
		ls := linesFor(t, fake, cmd)
		require.Len(t, ls, 1, cmd)
		assert.Contains(t, []string{"19", "18"}, ioTimeoutOf(ls[0]), "%s: %s", cmd, ls[0])
	}
	for _, cmd := range []string{"stop", "start"} {
		for _, l := range linesFor(t, fake, cmd) {
			assert.Empty(t, ioTimeoutOf(l), l)
		}
	}
}

func TestRepoLs_ArgvIsBuiltBySidecar(t *testing.T) {
	h, fake := newExclusiveHarness(t, fakeOpts{})
	for _, req := range []*pb.RunRequest{
		{Command: pb.Command_COMMAND_REPO_LS, Args: []string{"--output=json", "--log-level-console=off"}},
		{Command: pb.Command_COMMAND_REPO_LS, Args: []string{"--output=json"}, ArchiveId: "17-1"},
		{Command: pb.Command_COMMAND_REPO_LS, Args: []string{"--output=json"}, ArchiveId: "17-1", WalDir: "000000010000000A"},
	} {
		_, err := h.runReq(t.Context(), req)
		require.NoError(t, err)
	}
	ls := linesFor(t, fake, "repo-ls")
	require.Len(t, ls, 3)
	for _, l := range ls {
		assert.NotContains(t, l, "--lock-path", "repo-ls rejects the option")
		assert.Contains(t, l, "--stanza=multigres")
	}
	assert.True(t, strings.HasSuffix(ls[0], " repo-ls --output=json --log-level-console=off archive/multigres"), ls[0])
	assert.True(t, strings.HasSuffix(ls[1], " repo-ls --output=json archive/multigres/17-1"), ls[1])
	assert.True(t, strings.HasSuffix(ls[2], " repo-ls --output=json archive/multigres/17-1/000000010000000A"), ls[2])
}

func TestRepoLs_RejectsCallerPaths(t *testing.T) {
	h, fake := newExclusiveHarness(t, fakeOpts{})
	for _, req := range []*pb.RunRequest{
		{Command: pb.Command_COMMAND_REPO_LS, Args: []string{"--output=json", "../"}},
		{Command: pb.Command_COMMAND_REPO_LS, Args: []string{"--output=json"}, ArchiveId: "../.."},
		{Command: pb.Command_COMMAND_REPO_LS, Args: []string{"--output=json"}, WalDir: "000000010000000A"},
		{Command: pb.Command_COMMAND_INFO, Args: []string{"--output=json"}, ArchiveId: "17-1"},
	} {
		_, err := h.runReq(t.Context(), req)
		require.Error(t, err)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	}
	assert.Empty(t, linesFor(t, fake, "repo-ls"))
}

// The delete stays listed in active_commands while its child is being killed,
// and leaves only once the child is reaped. A caller that waits for it to leave
// holds its backup lease until then.
func TestStanzaDelete_StaysListedUntilTheChildIsReaped(t *testing.T) {
	h, fake := newExclusiveHarness(t, fakeOpts{infoJSON: infoEmptyStanza, hang: []string{"stanza-delete"}})
	var listedAtSettle atomic.Bool
	var settled atomic.Bool
	oldHook := settleHook
	settleHook = func(command string) {
		if command == cmdStanzaDelete {
			listedAtSettle.Store(slices.Contains(h.s.activeCommands(), pb.Command_COMMAND_STANZA_DELETE))
			settled.Store(true)
		}
	}
	t.Cleanup(func() { settleHook = oldHook })

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { _, err := deleteStanza(h, ctx); done <- err }()
	require.Eventually(t, func() bool { return slices.Contains(shortened(fake.lines(t)), "stanza-delete") }, 10*time.Second, 5*time.Millisecond)
	cancel()
	require.Error(t, <-done)

	require.Eventually(t, settled.Load, 10*time.Second, 5*time.Millisecond, "the delete child was reaped")
	assert.True(t, listedAtSettle.Load(), "the delete was listed when its child had just been reaped, not before")
	require.Eventually(t, func() bool { return len(h.s.activeCommands()) == 0 }, 10*time.Second, 5*time.Millisecond,
		"and leaves once its worker is positively settled")
}

// A supervisor that does not acknowledge the cancel leaves the delete unproven:
// it stays listed for as long as the worker is unsettled and leaves only after
// the supervisor has reaped it. The sidecar never kills the supervisor itself.
func TestStanzaDelete_StaysListedWhileTheSupervisorIsStopped(t *testing.T) {
	oldWait := workerStopWait
	workerStopWait = 3 * time.Second
	t.Cleanup(func() { workerStopWait = oldWait })
	h, fake := newExclusiveHarness(t, fakeOpts{infoJSON: infoEmptyStanza, hang: []string{"stanza-delete"}})

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { _, err := deleteStanza(h, ctx); done <- err }()
	require.Eventually(t, func() bool { return slices.Contains(shortened(fake.lines(t)), "stanza-delete") }, 10*time.Second, 5*time.Millisecond)
	require.Eventually(t, func() bool { _, err := os.Stat(fake.stop + ".pid.stanza-delete"); return err == nil }, 10*time.Second, 5*time.Millisecond)
	// The supervisor is the parent of the fake pgbackrest the delete runs.
	raw, err := os.ReadFile(fake.stop + ".pid.stanza-delete")
	require.NoError(t, err)
	out, err := exec.Command("ps", "-o", "ppid=", "-p", strings.TrimSpace(string(raw))).Output()
	require.NoError(t, err)
	supervisor, err := strconv.Atoi(strings.TrimSpace(string(out)))
	require.NoError(t, err)
	_, owned := h.s.children.Load(supervisor)
	require.True(t, owned, "the parent of the delete is a supervisor this sidecar owns")
	require.NoError(t, syscall.Kill(supervisor, syscall.SIGSTOP))
	resumed := false
	resume := func() {
		if !resumed {
			resumed = true
			_ = syscall.Kill(supervisor, syscall.SIGCONT)
		}
	}
	t.Cleanup(resume)

	cancel()
	require.Error(t, <-done, "the Run ends; the delete is not proven dead")
	require.Eventually(t, h.s.deleteLatched, 10*time.Second, 5*time.Millisecond)
	require.Eventually(t, h.gateOpen, 10*time.Second, 5*time.Millisecond, "the unwind completed")
	_, statErr := os.Stat(fake.stop)
	assert.NoError(t, statErr, "archiving stays stopped while the delete is unproven")
	assert.Equal(t, 1, strings.Count(strings.Join(shortened(fake.lines(t)), " "), "start"), "the unwind did not start archiving")
	testpoll.Never(t, func() bool { return !slices.Contains(h.s.activeCommands(), pb.Command_COMMAND_STANZA_DELETE) },
		4*time.Second, 20*time.Millisecond, "the delete stays listed while its supervisor is stopped")

	resume()
	require.Eventually(t, func() bool { return !slices.Contains(h.s.activeCommands(), pb.Command_COMMAND_STANZA_DELETE) },
		20*time.Second, 10*time.Millisecond, "it leaves once the supervisor has reaped the delete")
}

// A worker that latches (settlement failed) keeps its delete listed for good.
func TestStanzaDelete_StaysListedForTheLifeOfALatchedWorker(t *testing.T) {
	deleteSettle := &ownedWorker{done: make(chan struct{}), err: errors.New("supervisor exited without quiescence acknowledgement")}
	close(deleteSettle.done)
	h, _ := newExclusiveHarness(t, fakeOpts{})
	lc := h.s.trackRun(t.Context(), pb.Command_COMMAND_STANZA_DELETE, "r")
	var once sync.Once
	h.s.settleDeleteEntry(deleteSettle, func() { once.Do(func() { h.s.untrackChild(lc) }) })
	assert.Equal(t, []pb.Command{pb.Command_COMMAND_STANZA_DELETE}, h.s.activeCommands())

	settled := &ownedWorker{done: make(chan struct{})}
	close(settled.done)
	h.s.settleDeleteEntry(settled, func() { once.Do(func() { h.s.untrackChild(lc) }) })
	assert.Empty(t, h.s.activeCommands())
}

// stop failing under a live context (a non-zero exit, or no answer in time)
// ends the sequence: no info, no delete, no spool removal; the unwind still
// starts archiving, resumes the server and reopens the gate, and the Run fails
// with stop's result.
func TestExclusive_StopFailureEndsTheSequence(t *testing.T) {
	for _, cmd := range []pb.Command{pb.Command_COMMAND_STANZA_DELETE, pb.Command_COMMAND_ARCHIVE_SPOOL_RESET} {
		for _, mode := range []string{"exit", "timeout"} {
			t.Run(cmd.String()+"/"+mode, func(t *testing.T) {
				o := fakeOpts{infoJSON: infoEmptyStanza}
				if mode == "exit" {
					o.failing = []string{"stop"}
				} else {
					o.hang = []string{"stop"}
					old := resetStepTimeout
					resetStepTimeout = 1500 * time.Millisecond
					t.Cleanup(func() { resetStepTimeout = old })
				}
				h, fake := newExclusiveHarness(t, o)
				spool := h.s.cfg.stateSub(dirSpool + "/archive/multigres/out")
				require.NoError(t, os.MkdirAll(spool, 0o700))

				res, err := h.run(t.Context(), cmd)
				if mode == "exit" {
					require.NoError(t, err)
					assert.Equal(t, int32(7), res.exit, "the Run reports stop's result")
				} else {
					require.Error(t, err)
					assert.Equal(t, connect.CodeDeadlineExceeded, connect.CodeOf(err))
				}
				assert.Equal(t, append(configurePause, "start", "stop", "start", "RESUME"), shortened(fake.lines(t)),
					"no info, no stanza-delete; the unwind starts archiving and resumes the server")
				assert.DirExists(t, spool, "nothing is removed from the spool")
				h.requireGateOpen(t)
				assert.Empty(t, h.s.activeCommands())
			})
		}
	}
}

// A delete that exits non-zero while start also fails keeps the delete's result
// in the error the Run fails with.
func TestStanzaDelete_DeleteFailureIsKeptWhenStartAlsoFails(t *testing.T) {
	// Call 1 is the start before stop; 2 to 4 are the unwind's.
	h, _ := newExclusiveHarness(t, fakeOpts{infoJSON: infoEmptyStanza, failing: []string{"stanza-delete"}, failStartCalls: []int{2, 3, 4}})
	_, err := deleteStanza(h, t.Context())
	require.Error(t, err)
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
	assert.Contains(t, err.Error(), "WAL archiving is stopped")
	assert.Contains(t, err.Error(), "exit status 7", "the delete's exit status is not lost")
}

// What a waiting client sees: polling the Status RPC, as the multipooler's
// settlement wait does, the delete is listed until its child has exited, so a
// caller that returns when it leaves has not abandoned a running delete.
func TestStanzaDelete_StatusRPCListsTheDeleteUntilItsChildHasExited(t *testing.T) {
	h, fake := newExclusiveHarness(t, fakeOpts{infoJSON: infoEmptyStanza, hang: []string{"stanza-delete"}})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { _, err := deleteStanza(h, ctx); done <- err }()
	pidFile := fake.stop + ".pid.stanza-delete"
	require.Eventually(t, func() bool { _, err := os.Stat(pidFile); return err == nil }, 10*time.Second, 5*time.Millisecond)
	raw, err := os.ReadFile(pidFile)
	require.NoError(t, err)
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	require.NoError(t, err)
	require.NoError(t, syscall.Kill(pid, 0), "the delete's child is running")

	cancel()
	require.Error(t, <-done)
	listed := func() bool {
		resp, err := h.client.Status(t.Context(), connect.NewRequest(&pb.StatusRequest{}))
		require.NoError(t, err)
		return slices.Contains(resp.Msg.ActiveCommands, pb.Command_COMMAND_STANZA_DELETE)
	}
	require.Eventually(t, func() bool { return !listed() }, 20*time.Second, 5*time.Millisecond)
	assert.ErrorIs(t, syscall.Kill(pid, 0), syscall.ESRCH, "by the time Status stops listing the delete, its child has exited")
}

// supervisorOf returns the pid of the supervisor that owns the pid in a
// fake child's pid file.
func supervisorOf(t *testing.T, h *controlHarness, pidFile string) (child, supervisor int) {
	t.Helper()
	require.Eventually(t, func() bool { _, err := os.Stat(pidFile); return err == nil }, 10*time.Second, 5*time.Millisecond)
	raw, err := os.ReadFile(pidFile)
	require.NoError(t, err)
	child, err = strconv.Atoi(strings.TrimSpace(string(raw)))
	require.NoError(t, err)
	out, err := exec.Command("ps", "-o", "ppid=", "-p", strconv.Itoa(child)).Output()
	require.NoError(t, err)
	supervisor, err = strconv.Atoi(strings.TrimSpace(string(out)))
	require.NoError(t, err)
	_, owned := h.s.children.Load(supervisor)
	require.True(t, owned, "the parent of the delete is a supervisor this sidecar owns")
	return child, supervisor
}

// A supervisor that is killed outright latches its worker: the delete's entry
// stays listed, the next barrier fails, the stop file stays, and the next reset
// refuses without starting archiving.
// killLatch starts a delete that hangs, kills its supervisor outright and returns
// the Run's result once the sidecar has latched.
func killLatch(t *testing.T, h *controlHarness, fake *fakeBackrest) (runResult, error) {
	t.Helper()
	type result struct {
		res runResult
		err error
	}
	done := make(chan result, 1)
	go func() { res, err := deleteStanza(h, t.Context()); done <- result{res, err} }()
	child, supervisor := supervisorOf(t, h, fake.stop+".pid.stanza-delete")
	t.Cleanup(func() { _ = syscall.Kill(child, syscall.SIGKILL) })
	require.NoError(t, syscall.Kill(supervisor, syscall.SIGKILL))
	r := <-done
	require.Eventually(t, h.gateOpen, 10*time.Second, 5*time.Millisecond)
	return r.res, r.err
}

func (h *controlHarness) requireStopFile(t *testing.T, fake *fakeBackrest, msg string) {
	t.Helper()
	_, err := os.Stat(fake.stop)
	require.NoError(t, err, msg)
}

// A supervisor that is killed outright latches its worker: the delete's entry
// stays listed, the next barrier fails, the stop file stays, and every later
// destructive sequence refuses without starting anything.
func TestStanzaDelete_KilledSupervisorLatchesAndRefusesLaterResets(t *testing.T) {
	h, fake := newExclusiveHarness(t, fakeOpts{infoJSON: infoEmptyStanza, hang: []string{"stanza-delete"}})
	res, err := killLatch(t, h, fake)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not proven settled")
	assert.Equal(t, "1", res.spawned, "a delete child was started, so the caller must wait on its run")
	assert.Equal(t, []pb.Command{pb.Command_COMMAND_STANZA_DELETE}, h.s.activeCommands(), "the entry stays listed for the life of the latch")
	require.Error(t, h.s.quiesceStep(t.Context()), "the next barrier fails on the latched worker")
	h.requireStopFile(t, fake, "the stop file stays")

	for _, cmd := range []pb.Command{pb.Command_COMMAND_STANZA_DELETE, pb.Command_COMMAND_ARCHIVE_SPOOL_RESET} {
		t.Run(cmd.String(), func(t *testing.T) {
			before := len(fake.lines(t))
			_, err := h.run(t.Context(), cmd)
			require.Error(t, err)
			assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
			assert.Contains(t, err.Error(), "not proven settled")
			assert.Len(t, fake.lines(t), before, "the refused sequence runs nothing")
			h.requireStopFile(t, fake, "archiving is not started")
		})
	}
}

// A restore, whether it arrives after the latch or was waiting for the gate
// when it was set, does not run its start and leaves the stop file.
func TestStanzaDelete_LatchRefusesRestoreAndKeepsTheStopFile(t *testing.T) {
	h, fake := newExclusiveHarness(t, fakeOpts{infoJSON: infoEmptyStanza, hang: []string{"stanza-delete"}})
	_, err := killLatch(t, h, fake)
	require.Error(t, err)

	before := len(fake.lines(t))
	_, err = h.run(t.Context(), pb.Command_COMMAND_RESTORE)
	require.Error(t, err)
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
	assert.Contains(t, err.Error(), "not proven settled")
	assert.Len(t, fake.lines(t), before, "no start, stop or restore ran")
	h.requireStopFile(t, fake, "the restore did not lift the stop file")
	h.requireGateOpen(t)

	t.Run("latched while waiting for the gate", func(t *testing.T) {
		h2, fake2 := newExclusiveHarness(t, fakeOpts{infoJSON: infoEmptyStanza})
		busy, ok := h2.s.gate.TryEnter()
		require.True(t, ok)
		oldDrain := restoreDrainWait
		restoreDrainWait = 10 * time.Second
		t.Cleanup(func() { restoreDrainWait = oldDrain })
		done := make(chan error, 1)
		go func() { _, err := h2.run(t.Context(), pb.Command_COMMAND_RESTORE); done <- err }()
		require.Eventually(t, func() bool { return h2.s.restoreActive.Load() > 0 }, 10*time.Second, 5*time.Millisecond)
		h2.s.latchDeleteUnproven(t.Context(), "r", errors.New("test"))
		busy()
		err := <-done
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not proven settled")
		assert.Equal(t, configurePause[:2], shortened(fake2.lines(t)), "the restore ran nothing")
	})
}

// A start retry that was asleep before the latch wakes into it, abandons without
// running start and leaves the stop file.
func TestStanzaDelete_LatchStopsAnOlderStartRetry(t *testing.T) {
	h, fake := newExclusiveHarness(t, fakeOpts{infoJSON: infoEmptyStanza, hang: []string{"stanza-delete"}})
	wake := make(chan struct{})
	var calls atomic.Int32
	h.s.pg.sleep = func(ctx context.Context, _ time.Duration) bool {
		if calls.Add(1) > 1 {
			// Every later wait ends at once: a retry that does not give up spins.
			time.Sleep(time.Millisecond)
			return ctx.Err() == nil
		}
		select {
		case <-wake:
			return true
		case <-ctx.Done():
			return false
		}
	}
	retried := make(chan struct{})
	go func() { h.s.retryStart(t.Context()); close(retried) }()

	_, err := killLatch(t, h, fake)
	require.Error(t, err)
	before := len(fake.lines(t))
	close(wake)
	select {
	case <-retried:
	case <-time.After(10 * time.Second):
		t.Fatal("the retry did not give up")
	}
	assert.Len(t, fake.lines(t), before, "the retry ran no start")
	h.requireStopFile(t, fake, "the older retry did not lift the stop file")
}

// restartRepository does not start, and does not leave a background retry
// pending, while latched.
func TestRestartRepository_RefusesWhileLatched(t *testing.T) {
	h, fake := newExclusiveHarness(t, fakeOpts{})
	h.s.latchDeleteUnproven(t.Context(), "r", errors.New("test"))
	before := len(fake.lines(t))
	err := h.s.restartRepository(t.Context(), newStreamSender(discardStream{}), time.Second)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not proven settled")
	assert.Zero(t, h.s.startPending.Load())
	assert.Len(t, fake.lines(t), before)

	// The one place every start passes through refuses too, whoever calls it.
	assert.False(t, h.s.runStart(t.Context()))
	_, err = h.s.runChild(t.Context(), KindS3Command, cmdStart, nil, io.Discard, io.Discard)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not proven settled")
	assert.Len(t, fake.lines(t), before, "no start ran")
}

// The latch is visible in Status, with its reason, while the run is listed and
// after a late positive settlement has removed the run.
func TestStatus_ShowsTheDeleteLatch(t *testing.T) {
	oldWait := workerStopWait
	workerStopWait = 3 * time.Second
	t.Cleanup(func() { workerStopWait = oldWait })
	h, fake := newExclusiveHarness(t, fakeOpts{infoJSON: infoEmptyStanza, hang: []string{"stanza-delete"}})
	status := func() *pb.StatusResponse {
		resp, err := h.client.Status(t.Context(), connect.NewRequest(&pb.StatusRequest{}))
		require.NoError(t, err)
		return resp.Msg
	}
	assert.False(t, status().DeleteUnproven)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { _, err := deleteStanza(h, ctx); done <- err }()
	child, supervisor := supervisorOf(t, h, fake.stop+".pid.stanza-delete")
	t.Cleanup(func() { _ = syscall.Kill(child, syscall.SIGKILL) })
	require.NoError(t, syscall.Kill(supervisor, syscall.SIGSTOP))
	t.Cleanup(func() { _ = syscall.Kill(supervisor, syscall.SIGCONT) })
	cancel()
	<-done
	require.Eventually(t, func() bool { return status().DeleteUnproven }, 10*time.Second, 5*time.Millisecond)
	st := status()
	assert.NotEmpty(t, st.ActiveRuns, "the run is still listed")
	assert.Contains(t, st.DeleteUnprovenReason, "not proven settled")

	require.NoError(t, syscall.Kill(supervisor, syscall.SIGCONT))
	require.Eventually(t, func() bool { return len(status().ActiveRuns) == 0 }, 20*time.Second, 10*time.Millisecond, "a late settlement removes the run")
	st = status()
	assert.True(t, st.DeleteUnproven, "the latch stays until the sidecar restarts")
	assert.NotEmpty(t, st.DeleteUnprovenReason)
	h.requireStopFile(t, fake, "archiving stays stopped")
}

// A request cancelled after the delete child started still reports it, at the
// function, where no trailer can reach the gone caller. A healthy supervisor
// settles, so the cancelled caller leaves no latch behind.
func TestStanzaDelete_CancelledAfterTheChildStartedReportsSpawned(t *testing.T) {
	h, fake := newExclusiveHarness(t, fakeOpts{infoJSON: infoEmptyStanza, hang: []string{"stanza-delete"}})
	ctx, cancel := context.WithCancel(t.Context())
	type result struct {
		spawned bool
		err     error
	}
	done := make(chan result, 1)
	go func() {
		_, spawned, err := h.s.runExclusive(ctx, &pb.RunRequest{Command: pb.Command_COMMAND_STANZA_DELETE, ExpectedInstanceId: h.s.incarnation, RunId: "r"}, newStreamSender(discardStream{}))
		done <- result{spawned, err}
	}()
	require.Eventually(t, func() bool { _, err := os.Stat(fake.stop + ".pid.stanza-delete"); return err == nil }, 10*time.Second, 5*time.Millisecond)
	cancel()
	r := <-done
	require.Error(t, r.err)
	assert.True(t, r.spawned)
	assert.False(t, h.s.deleteLatched(), "a healthy supervisor settles on the detached barrier, so a cancelled caller does not latch")
	require.Eventually(t, func() bool { _, err := os.Stat(fake.stop); return os.IsNotExist(err) }, 10*time.Second, 5*time.Millisecond, "archiving starts again")
}

func TestStanzaDelete_BindsToTheInstanceItWasDecidedOn(t *testing.T) {
	for name, id := range map[string]string{"empty": "", "another instance": "00000000-0000-0000-0000-000000000000"} {
		t.Run(name, func(t *testing.T) {
			h, fake := newExclusiveHarness(t, fakeOpts{infoJSON: infoEmptyStanza})
			res, err := h.runRaw(t.Context(), &pb.RunRequest{Command: pb.Command_COMMAND_STANZA_DELETE, RunId: "r1", ExpectedInstanceId: id})
			require.Error(t, err)
			assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
			assert.Equal(t, "0", res.spawned)
			assert.Equal(t, configurePause[:2], shortened(fake.lines(t)), "nothing ran")
			h.requireGateOpen(t)
		})
	}
}

func TestStanzaDelete_ReportsWhetherAChildWasSpawned(t *testing.T) {
	t.Run("deleted", func(t *testing.T) {
		h, _ := newExclusiveHarness(t, fakeOpts{infoJSON: infoEmptyStanza})
		res, err := deleteStanza(h, t.Context())
		require.NoError(t, err)
		assert.Equal(t, "1", res.spawned)
	})
	t.Run("non-zero exit", func(t *testing.T) {
		h, _ := newExclusiveHarness(t, fakeOpts{infoJSON: infoEmptyStanza, failing: []string{"stanza-delete"}})
		res, err := deleteStanza(h, t.Context())
		require.NoError(t, err)
		assert.Equal(t, "1", res.spawned)
	})
	t.Run("preflight refusal", func(t *testing.T) {
		h, _ := newExclusiveHarness(t, fakeOpts{infoJSON: infoWithBackup})
		res, err := deleteStanza(h, t.Context())
		require.Error(t, err)
		assert.Equal(t, "0", res.spawned)
	})
	t.Run("stop failure", func(t *testing.T) {
		h, _ := newExclusiveHarness(t, fakeOpts{infoJSON: infoEmptyStanza, failing: []string{"stop"}})
		res, err := deleteStanza(h, t.Context())
		require.NoError(t, err)
		assert.Equal(t, "0", res.spawned)
	})
}

func TestStatus_ListsActiveRunsWithTheirIds(t *testing.T) {
	h, fake := newExclusiveHarness(t, fakeOpts{infoJSON: infoEmptyStanza, hang: []string{"stanza-delete"}})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := h.runReq(ctx, &pb.RunRequest{Command: pb.Command_COMMAND_STANZA_DELETE, RunId: "run-a"})
		done <- err
	}()
	require.Eventually(t, func() bool { return slices.Contains(shortened(fake.lines(t)), "stanza-delete") }, 10*time.Second, 5*time.Millisecond)
	resp, err := h.client.Status(t.Context(), connect.NewRequest(&pb.StatusRequest{}))
	require.NoError(t, err)
	require.Len(t, resp.Msg.ActiveRuns, 1)
	assert.Equal(t, "run-a", resp.Msg.ActiveRuns[0].RunId)
	assert.Equal(t, pb.Command_COMMAND_STANZA_DELETE, resp.Msg.ActiveRuns[0].Command)
	assert.Equal(t, h.s.incarnation, resp.Msg.InstanceId)
	cancel()
	<-done
}

// A supervisor that acknowledges only after the delete's own cancellation wait
// has ended, but within the barrier's, settles without latching: the barrier
// runs on its own bound, not on the cancelled request's context.
func TestStanzaDelete_AcknowledgementWithinTheBarrierDoesNotLatch(t *testing.T) {
	oldWait := workerStopWait
	workerStopWait = 3 * time.Second
	t.Cleanup(func() { workerStopWait = oldWait })
	h, fake := newExclusiveHarness(t, fakeOpts{infoJSON: infoEmptyStanza, hang: []string{"stanza-delete"}})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { _, err := deleteStanza(h, ctx); done <- err }()
	child, supervisor := supervisorOf(t, h, fake.stop+".pid.stanza-delete")
	t.Cleanup(func() { _ = syscall.Kill(child, syscall.SIGKILL) })
	require.NoError(t, syscall.Kill(supervisor, syscall.SIGSTOP))
	t.Cleanup(func() { _ = syscall.Kill(supervisor, syscall.SIGCONT) })

	cancel()
	// The child's own cancellation wait ends after workerStopWait; the barrier
	// then waits another workerStopWait, and the supervisor wakes in the middle.
	time.Sleep(workerStopWait + workerStopWait/4)
	require.NoError(t, syscall.Kill(supervisor, syscall.SIGCONT))
	require.Error(t, <-done)
	assert.False(t, h.s.deleteLatched(), "the barrier saw the acknowledgement")
	require.Eventually(t, func() bool { _, err := os.Stat(fake.stop); return os.IsNotExist(err) }, 10*time.Second, 5*time.Millisecond, "archiving starts again")
	assert.Empty(t, h.s.activeCommands())
}

// A worker that is still alive when the sequence starts, and that the first
// barrier cannot prove stopped, ends the sequence before the preflight, the
// delete and the spool removal. The unwind still starts archiving again.
func TestExclusive_LiveWorkerEndsTheSequenceBeforeAnyDestructiveStep(t *testing.T) {
	for _, cmd := range []pb.Command{pb.Command_COMMAND_STANZA_DELETE, pb.Command_COMMAND_ARCHIVE_SPOOL_RESET} {
		t.Run(cmd.String(), func(t *testing.T) {
			oldWait := workerStopWait
			workerStopWait = 300 * time.Millisecond
			t.Cleanup(func() { workerStopWait = oldWait })
			h, fake := newExclusiveHarness(t, fakeOpts{infoJSON: infoEmptyStanza})
			spool := h.s.cfg.stateSub(dirSpool + "/archive/multigres/out")
			require.NoError(t, os.MkdirAll(spool, 0o700))

			w := &ownedWorker{done: make(chan struct{})}
			h.s.workersMu.Lock()
			h.s.workers = map[*ownedWorker]struct{}{w: {}}
			h.s.workersMu.Unlock()
			t.Cleanup(func() {
				h.s.workersMu.Lock()
				delete(h.s.workers, w)
				h.s.workersMu.Unlock()
			})

			res, err := h.run(t.Context(), cmd)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "quiescence unproven")
			assert.Equal(t, append(configurePause, "start", "stop", "start", "RESUME"), shortened(fake.lines(t)),
				"no info and no stanza-delete; the unwind starts archiving and resumes the server")
			assert.DirExists(t, spool, "the spool is intact")
			if cmd == pb.Command_COMMAND_STANZA_DELETE {
				assert.Equal(t, "0", res.spawned)
			}
			h.requireGateOpen(t)
		})
	}
}

// A latch makes the sidecar shut itself down once, from the handler that set
// it, and a client that is waiting on that run is released by the new instance
// id, which the manager tests cover.
func TestStanzaDelete_LatchShutsTheSidecarDownOnce(t *testing.T) {
	h, fake := newExclusiveHarness(t, fakeOpts{infoJSON: infoEmptyStanza, hang: []string{"stanza-delete"}})
	var exits atomic.Int32
	h.s.latchExit = func() { exits.Add(1) }
	require.False(t, h.s.deleteLatched())
	_, err := killLatch(t, h, fake)
	require.Error(t, err, "the client still gets the Run's failure")
	require.Eventually(t, func() bool { return exits.Load() == 1 }, 10*time.Second, 5*time.Millisecond)

	// Later refused runs do not shut it down again.
	_, err = h.run(t.Context(), pb.Command_COMMAND_ARCHIVE_SPOOL_RESET)
	require.Error(t, err)
	h.s.exitAfterLatch()
	assert.Equal(t, int32(1), exits.Load())
}

// runRealServer runs the real Server.Run on real sockets with no exit seam and
// returns a harness whose client dials its control socket.
func runRealServer(t *testing.T, fake *fakeBackrest) (*controlHarness, <-chan error, context.CancelFunc) {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "sc-latch-")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })
	s := newTestServer(t, func(c *Config) {
		c.StateDir = filepath.Join(dir, "state")
		c.WALSocket = filepath.Join(dir, "wal", "wal.sock")
		c.ControlSocket = filepath.Join(dir, "ctl", "control.sock")
	})
	s.pauseFn = pauseRecorder(t, fake)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", s.cfg.ControlSocket)
	}}
	t.Cleanup(transport.CloseIdleConnections)
	client := pgbackrestdserviceconnect.NewPgBackRestdClient(&http.Client{Transport: transport}, "http://unix")
	require.Eventually(t, func() bool { s.serveMu.Lock(); defer s.serveMu.Unlock(); return s.serveCancel != nil },
		10*time.Second, time.Millisecond)
	h := &controlHarness{s: s, client: client}
	require.True(t, h.configure(t, 1, repos(1, 1)).Accepted)
	return h, done, cancel
}

// The real serve loop, with no test seam: a latch delivers the failing Run with
// its trailer, ends the serve loop with errDeleteUnprovenExit, leaves the stop
// file and runs no start.
func TestStanzaDelete_LatchEndsTheServeLoop(t *testing.T) {
	oldWait := workerStopWait
	workerStopWait = 2 * time.Second
	t.Cleanup(func() { workerStopWait = oldWait })
	fake := newFakeBackrestOpts(t, fakeOpts{infoJSON: infoEmptyStanza, hang: []string{"stanza-delete"}})
	h, done, _ := runRealServer(t, fake)

	type result struct {
		res runResult
		err error
	}
	rdone := make(chan result, 1)
	go func() { res, err := deleteStanza(h, t.Context()); rdone <- result{res, err} }()
	child, supervisor := supervisorOf(t, h, fake.stop+".pid.stanza-delete")
	t.Cleanup(func() { _ = syscall.Kill(child, syscall.SIGKILL) })
	require.NoError(t, syscall.Kill(supervisor, syscall.SIGKILL))

	var r result
	select {
	case r = <-rdone:
	case <-time.After(60 * time.Second):
		t.Fatal("the Run never answered")
	}
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(r.err), "the response was delivered")
	assert.Equal(t, "1", r.res.spawned, "with its trailer")
	select {
	case err := <-done:
		require.ErrorIs(t, err, errDeleteUnprovenExit)
	case <-time.After(60 * time.Second):
		t.Fatal("the sidecar never exited after the latch")
	}
	h.requireStopFile(t, fake, "the stop file is still there after the exit")
	lines := shortened(fake.lines(t))
	for i := len(lines) - 1; i >= 0 && lines[i] != "stanza-delete"; i-- {
		assert.NotEqual(t, "start", lines[i], "no start after the delete")
	}
}

// A supervisor that never answers does not keep the sidecar from exiting: the
// exit is bounded by the server's grace and the worker waits.
func TestStanzaDelete_LatchExitIsBoundedWhenAWorkerNeverAnswers(t *testing.T) {
	oldWait := workerStopWait
	workerStopWait = 2 * time.Second
	t.Cleanup(func() { workerStopWait = oldWait })
	fake := newFakeBackrestOpts(t, fakeOpts{infoJSON: infoEmptyStanza, hang: []string{"stanza-delete"}})
	h, done, _ := runRealServer(t, fake)

	ctx, cancel := context.WithCancel(t.Context())
	rdone := make(chan error, 1)
	go func() { _, err := deleteStanza(h, ctx); rdone <- err }()
	child, supervisor := supervisorOf(t, h, fake.stop+".pid.stanza-delete")
	t.Cleanup(func() { _ = syscall.Kill(child, syscall.SIGKILL) })
	require.NoError(t, syscall.Kill(supervisor, syscall.SIGSTOP))
	t.Cleanup(func() { _ = syscall.Kill(supervisor, syscall.SIGCONT) })
	began := time.Now()
	cancel()
	<-rdone

	select {
	case err := <-done:
		require.ErrorIs(t, err, errDeleteUnprovenExit)
		assert.Less(t, time.Since(began), shutdownGrace+6*workerStopWait+10*time.Second, "bounded")
	case <-time.After(90 * time.Second):
		t.Fatal("the sidecar did not exit with a worker that never answers")
	}
	h.requireStopFile(t, fake, "the stop file stays")
}

// A delete that latches while a signal-driven shutdown is already draining
// handlers still adds errDeleteUnprovenExit to what Run returns: the latch is
// read after the servers have drained.
func TestStanzaDelete_LatchDuringShutdownStillReportsTheExit(t *testing.T) {
	oldWait := workerStopWait
	workerStopWait = 2 * time.Second
	t.Cleanup(func() { workerStopWait = oldWait })
	fake := newFakeBackrestOpts(t, fakeOpts{infoJSON: infoEmptyStanza, hang: []string{"stanza-delete"}})
	h, done, stop := runRealServer(t, fake)

	rdone := make(chan error, 1)
	go func() { _, err := deleteStanza(h, t.Context()); rdone <- err }()
	child, supervisor := supervisorOf(t, h, fake.stop+".pid.stanza-delete")
	t.Cleanup(func() { _ = syscall.Kill(child, syscall.SIGKILL) })

	// The signal: the serve context ends while the delete handler is running, so
	// Run is draining it. Only then does the delete's supervisor die and the
	// handler latch.
	stop()
	require.Eventually(t, func() bool { return !h.gateOpen() }, 10*time.Second, time.Millisecond)
	require.False(t, h.s.deleteLatched(), "nothing has latched yet")
	require.NoError(t, syscall.Kill(supervisor, syscall.SIGKILL))

	select {
	case err := <-done:
		require.ErrorIs(t, err, errDeleteUnprovenExit)
		assert.True(t, h.s.deleteLatched())
	case <-time.After(60 * time.Second):
		t.Fatal("the sidecar did not exit")
	}
	select {
	case <-rdone:
	case <-time.After(10 * time.Second):
		t.Fatal("the delete's Run never ended")
	}
}
