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

package manager

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	backupCommon "github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/etcdtopo"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
	backupengine "github.com/multigres/multigres/go/services/multipooler/internal/manager/backup"
)

func (f *fakeSidecarServer) setActive(cmds ...pb.Command) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status.ActiveCommands = cmds
	f.status.ActiveRuns = nil
}

// setRuns lists runs with their ids, the way the sidecar's Status does.
func (f *fakeSidecarServer) setRuns(runs ...*pb.ActiveRun) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status.ActiveRuns = runs
	f.status.ActiveCommands = nil
	for _, r := range runs {
		f.status.ActiveCommands = append(f.status.ActiveCommands, r.GetCommand())
	}
}

// blockingRun registers cmd as active, signals started, and holds the stream
// open until the client goes away. The command then stays "active" for dying,
// the way the sidecar's child does while it is killed and reaped, after the
// handler has already returned.
func blockingRun(f *fakeSidecarServer, cmd pb.Command, started chan<- struct{}, dying time.Duration, cleared *atomic.Int64) func(context.Context, *pb.RunRequest, *connect.ServerStream[pb.RunOutput]) error {
	return func(ctx context.Context, req *pb.RunRequest, _ *connect.ServerStream[pb.RunOutput]) error {
		f.setRuns(&pb.ActiveRun{Command: cmd, RunId: req.GetRunId()})
		started <- struct{}{}
		<-ctx.Done()
		go func() {
			time.Sleep(dying)
			cleared.Store(time.Now().UnixNano())
			f.setActive()
		}()
		return ctx.Err()
	}
}

func TestSidecarClient_CancelReturnsOnlyOnceTheCommandIsGone(t *testing.T) {
	f := &fakeSidecarServer{status: &pb.StatusResponse{}}
	started := make(chan struct{}, 1)
	var cleared atomic.Int64
	f.run = blockingRun(f, pb.Command_COMMAND_BACKUP, started, 400*time.Millisecond, &cleared)
	c := newSidecarClient(serveFakeSidecar(t, f))
	t.Cleanup(c.Close)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := c.Run(ctx, &pb.RunRequest{Command: pb.Command_COMMAND_BACKUP, Args: nil}, io.Discard, io.Discard)
		done <- err
	}()
	<-started
	cancel()
	err := <-done
	returned := time.Now().UnixNano()
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
	require.NotZero(t, cleared.Load(), "Run returned while the sidecar still listed the command")
	assert.GreaterOrEqual(t, returned, cleared.Load())
}

func TestSidecarClient_CancelTimesOutWithAClearError(t *testing.T) {
	f := &fakeSidecarServer{status: &pb.StatusResponse{}}
	started := make(chan struct{}, 1)
	f.run = func(ctx context.Context, _ *pb.RunRequest, _ *connect.ServerStream[pb.RunOutput]) error {
		f.setActive(pb.Command_COMMAND_BACKUP)
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err() // the command never leaves Status
	}
	c := newSidecarClient(serveFakeSidecar(t, f))
	c.stopTimeout = 300 * time.Millisecond
	t.Cleanup(c.Close)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := c.Run(ctx, &pb.RunRequest{Command: pb.Command_COMMAND_BACKUP, Args: nil}, io.Discard, io.Discard)
		done <- err
	}()
	<-started
	cancel()
	err := <-done
	require.Error(t, err)
	assert.Contains(t, err.Error(), "did not stop")
	assert.ErrorIs(t, err, context.Canceled, "the original cancel stays visible")
}

func TestSidecarClient_CancelWaitEndsOnShutdown(t *testing.T) {
	f := &fakeSidecarServer{status: &pb.StatusResponse{}}
	started := make(chan struct{}, 1)
	f.run = func(ctx context.Context, _ *pb.RunRequest, _ *connect.ServerStream[pb.RunOutput]) error {
		f.setActive(pb.Command_COMMAND_BACKUP)
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}
	c := newSidecarClient(serveFakeSidecar(t, f))
	t.Cleanup(c.Close)
	shutdown, stop := context.WithCancel(t.Context())
	c.shutdown = shutdown

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := c.Run(ctx, &pb.RunRequest{Command: pb.Command_COMMAND_BACKUP, Args: nil}, io.Discard, io.Discard)
		done <- err
	}()
	<-started
	cancel()
	time.AfterFunc(200*time.Millisecond, stop)
	select {
	case err := <-done:
		assert.Contains(t, err.Error(), "shutting down")
	case <-time.After(10 * time.Second):
		t.Fatal("the wait ignored manager shutdown")
	}
}

// A restore is not cancelled by its client, so its Run does not wait.
func TestSidecarClient_RestoreCancelDoesNotWait(t *testing.T) {
	f := &fakeSidecarServer{status: &pb.StatusResponse{}}
	started := make(chan struct{}, 1)
	f.run = func(ctx context.Context, _ *pb.RunRequest, _ *connect.ServerStream[pb.RunOutput]) error {
		f.setActive(pb.Command_COMMAND_RESTORE)
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}
	c := newSidecarClient(serveFakeSidecar(t, f))
	t.Cleanup(c.Close)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := c.Run(ctx, &pb.RunRequest{Command: pb.Command_COMMAND_RESTORE, Args: nil}, io.Discard, io.Discard)
		done <- err
	}()
	<-started
	cancel()
	select {
	case err := <-done:
		require.Error(t, err)
		assert.NotContains(t, err.Error(), "did not stop")
	case <-time.After(2 * time.Second):
		t.Fatal("a cancelled restore Run waited for the restore")
	}
}

// Liveness: the wait is for this caller's command only. Another run of the
// same command that this client itself started must not hold the cancel up,
// and an undelivered Run has nothing to wait for.
func TestSidecarClient_CancelIgnoresItsOwnOtherRuns(t *testing.T) {
	f := &fakeSidecarServer{status: &pb.StatusResponse{}}
	started := make(chan struct{}, 2)
	f.run = func(ctx context.Context, _ *pb.RunRequest, _ *connect.ServerStream[pb.RunOutput]) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}
	c := newSidecarClient(serveFakeSidecar(t, f))
	t.Cleanup(c.Close)

	other, stopOther := context.WithCancel(t.Context())
	otherDone := make(chan struct{})
	go func() {
		defer close(otherDone)
		_, _ = c.Run(other, &pb.RunRequest{Command: pb.Command_COMMAND_BACKUP, Args: nil}, io.Discard, io.Discard)
	}()
	// The goroutine must end inside this test: it reads the package timing
	// variables other tests write.
	t.Cleanup(func() {
		f.setActive()
		stopOther()
		<-otherDone
	})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := c.Run(ctx, &pb.RunRequest{Command: pb.Command_COMMAND_BACKUP, Args: nil}, io.Discard, io.Discard)
		done <- err
	}()
	<-started
	<-started
	// The sidecar lists both runs, then only the survivor once the cancelled one is reaped.
	f.setActive(pb.Command_COMMAND_BACKUP, pb.Command_COMMAND_BACKUP)
	time.AfterFunc(200*time.Millisecond, func() { f.setActive(pb.Command_COMMAND_BACKUP) })
	cancel()
	select {
	case err := <-done:
		require.Error(t, err)
		assert.NotContains(t, err.Error(), "did not stop")
	case <-time.After(5 * time.Second):
		t.Fatal("the wait counted this client's other run")
	}

	gone := newSidecarClient(t.TempDir() + "/nobody")
	_, err := gone.Run(t.Context(), &pb.RunRequest{Command: pb.Command_COMMAND_BACKUP, Args: nil}, io.Discard, io.Discard)
	assert.True(t, errors.Is(err, backupengine.ErrRunNotDelivered), "got %v", err)
}

// Liveness: a run of the same command that starts during the wait is
// legitimate and must not hold the wait to its bound.
func TestSidecarClient_CancelWaitRecountsOtherRunsEveryPoll(t *testing.T) {
	f := &fakeSidecarServer{status: &pb.StatusResponse{}}
	started := make(chan struct{}, 2)
	polling := make(chan struct{})
	var firstPoll sync.Once
	f.statusHook = func() { firstPoll.Do(func() { close(polling) }) }
	f.run = func(ctx context.Context, _ *pb.RunRequest, _ *connect.ServerStream[pb.RunOutput]) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}
	c := newSidecarClient(serveFakeSidecar(t, f))
	c.stopTimeout = 5 * time.Second
	t.Cleanup(c.Close)

	ctxA, cancelA := context.WithCancel(t.Context())
	doneA := make(chan error, 1)
	go func() {
		_, err := c.Run(ctxA, &pb.RunRequest{Command: pb.Command_COMMAND_BACKUP, Args: nil}, io.Discard, io.Discard)
		doneA <- err
	}()
	<-started
	f.setActive(pb.Command_COMMAND_BACKUP)
	cancelA()
	// B starts only once A's wait is already polling, so a count taken when the
	// wait began would not include it.
	<-polling

	ctxB, cancelB := context.WithCancel(t.Context())
	doneB := make(chan struct{})
	go func() {
		defer close(doneB)
		_, _ = c.Run(ctxB, &pb.RunRequest{Command: pb.Command_COMMAND_BACKUP, Args: nil}, io.Discard, io.Discard)
	}()
	t.Cleanup(func() {
		f.setActive()
		cancelB()
		<-doneB
	})
	<-started
	// A is still listed while B starts; then A is reaped and only B remains.
	f.setActive(pb.Command_COMMAND_BACKUP, pb.Command_COMMAND_BACKUP)
	time.Sleep(200 * time.Millisecond)
	f.setActive(pb.Command_COMMAND_BACKUP)

	select {
	case err := <-doneA:
		require.Error(t, err)
		assert.NotContains(t, err.Error(), "did not stop")
	case <-time.After(3 * time.Second):
		t.Fatal("a run started during the wait held it to the bound")
	}
}

// A stanza delete cancelled mid-flight (the caller timed out or lost its lease)
// does not return from the engine until the sidecar no longer lists it. This
// test has no lease: it pins that the engine call outlives the cancel, which is
// what keeps a lease that is still held from being released early.
func TestStanzaDelete_CancelledCallerWaitsForSettlementBeforeReturning(t *testing.T) {
	f := &fakeSidecarServer{status: &pb.StatusResponse{InstanceId: "inst-1"}}
	started := make(chan struct{}, 1)
	var cleared atomic.Int64
	f.run = blockingRun(f, pb.Command_COMMAND_STANZA_DELETE, started, 400*time.Millisecond, &cleared)
	c := newSidecarClient(serveFakeSidecar(t, f))
	t.Cleanup(c.Close)
	pm := createTestManager(t, t.TempDir(), "", "", clustermetadatapb.PoolerType_REPLICA)
	e := newStubEngine(pm.logger, pm.record, backupengine.Settings{Sidecar: c})

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- e.StanzaDelete(ctx) }()
	<-started
	cancel()
	err := <-done
	returned := time.Now().UnixNano()
	require.Error(t, err)
	require.NotZero(t, cleared.Load(), "the engine returned while the sidecar still listed the delete")
	assert.GreaterOrEqual(t, returned, cleared.Load())
}

// A delete's settlement wait has no timeout: returning (and so releasing a lease
// that is still held) before a sidecar says the delete is gone could let the
// delete meet the next lease holder's work. It logs while it waits, including
// the sidecar's latch reason when Status reports one, and ends at manager
// shutdown with an error. This test acquires no lease.
func TestStanzaDelete_SettlementWaitHasNoBoundShortOfShutdown(t *testing.T) {
	f := &fakeSidecarServer{status: &pb.StatusResponse{InstanceId: "inst-1", DeleteUnproven: true, DeleteUnprovenReason: "worker settlement failed"}}
	started := make(chan struct{}, 1)
	f.run = func(ctx context.Context, req *pb.RunRequest, _ *connect.ServerStream[pb.RunOutput]) error {
		f.setRuns(&pb.ActiveRun{Command: pb.Command_COMMAND_STANZA_DELETE, RunId: req.GetRunId()})
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err() // the delete never leaves Status
	}
	c := newSidecarClient(serveFakeSidecar(t, f))
	c.stopTimeout = 100 * time.Millisecond
	c.settleWarnEvery = 150 * time.Millisecond
	var logs syncBuffer
	c.logger = slog.New(slog.NewTextHandler(&logs, nil))
	shutdown, stop := context.WithCancel(t.Context())
	c.shutdown = shutdown
	t.Cleanup(c.Close)
	pm := createTestManager(t, t.TempDir(), "", "", clustermetadatapb.PoolerType_REPLICA)
	e := newStubEngine(pm.logger, pm.record, backupengine.Settings{Sidecar: c})

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- e.StanzaDelete(ctx) }()
	<-started
	cancel()
	select {
	case err := <-done:
		t.Fatalf("returned while the delete was still listed: %v", err)
	case <-time.After(10 * c.stopTimeout):
	}
	require.Eventually(t, func() bool {
		out := logs.String()
		return strings.Contains(out, "level=WARN") && strings.Contains(out, "STANZA_DELETE") && strings.Contains(out, "still waiting") &&
			strings.Contains(out, "worker settlement failed") && !strings.Contains(out, "lease stays held")
	}, 5*time.Second, 20*time.Millisecond, "the wait says what it is waiting for")

	stop()
	err := <-done
	require.Error(t, err)
	assert.Contains(t, err.Error(), "shutting down")
}

// A sidecar that came back as a different instance (its container restarted and
// killed every process in it) cannot still be running the delete.
func TestStanzaDelete_SettlementEndsWhenTheSidecarInstanceChanges(t *testing.T) {
	f := &fakeSidecarServer{status: &pb.StatusResponse{InstanceId: "inst-1"}}
	started := make(chan struct{}, 1)
	f.run = func(ctx context.Context, req *pb.RunRequest, _ *connect.ServerStream[pb.RunOutput]) error {
		f.setRuns(&pb.ActiveRun{Command: pb.Command_COMMAND_STANZA_DELETE, RunId: req.GetRunId()})
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}
	c := newSidecarClient(serveFakeSidecar(t, f))
	t.Cleanup(c.Close)
	pm := createTestManager(t, t.TempDir(), "", "", clustermetadatapb.PoolerType_REPLICA)
	e := newStubEngine(pm.logger, pm.record, backupengine.Settings{Sidecar: c})

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- e.StanzaDelete(ctx) }()
	<-started
	cancel()
	select {
	case err := <-done:
		t.Fatalf("returned while the same instance still listed the delete: %v", err)
	case <-time.After(300 * time.Millisecond):
	}

	f.mu.Lock()
	f.status.InstanceId = "inst-2" // the run is still listed: only the instance ends the wait
	f.mu.Unlock()
	select {
	case err := <-done:
		require.Error(t, err, "the Run itself still failed")
		assert.Contains(t, err.Error(), "context canceled")
	case <-time.After(5 * time.Second):
		t.Fatal("the wait did not end when the instance changed")
	}
}

// Without knowing the instance the delete cannot be tied to one, so the Run is
// not sent at all.
func TestStanzaDelete_NotSentWhenTheInstanceCannotBeRead(t *testing.T) {
	f := &fakeSidecarServer{status: &pb.StatusResponse{}, statusErr: errors.New("status unavailable")}
	var sent atomic.Bool
	f.run = func(context.Context, *pb.RunRequest, *connect.ServerStream[pb.RunOutput]) error {
		sent.Store(true)
		return nil
	}
	c := newSidecarClient(serveFakeSidecar(t, f))
	t.Cleanup(c.Close)
	_, err := c.Run(t.Context(), &pb.RunRequest{Command: pb.Command_COMMAND_STANZA_DELETE}, io.Discard, io.Discard)
	require.ErrorIs(t, err, backupengine.ErrRunNotDelivered)
	assert.False(t, sent.Load())

	t.Run("an empty instance id", func(t *testing.T) {
		f := &fakeSidecarServer{status: &pb.StatusResponse{}}
		var sent atomic.Bool
		f.run = func(context.Context, *pb.RunRequest, *connect.ServerStream[pb.RunOutput]) error {
			sent.Store(true)
			return nil
		}
		c := newSidecarClient(serveFakeSidecar(t, f))
		t.Cleanup(c.Close)
		_, err := c.Run(t.Context(), &pb.RunRequest{Command: pb.Command_COMMAND_STANZA_DELETE}, io.Discard, io.Discard)
		require.ErrorIs(t, err, backupengine.ErrRunNotDelivered)
		assert.False(t, sent.Load())
	})
}

// startDeleteWait starts a delete against a fake sidecar that lists the run and
// never answers it, cancels it, and returns the channel the Run's result arrives on.
func startDeleteWait(t *testing.T, f *fakeSidecarServer, c *sidecarClient) <-chan error {
	t.Helper()
	started := make(chan struct{}, 1)
	f.run = func(ctx context.Context, req *pb.RunRequest, _ *connect.ServerStream[pb.RunOutput]) error {
		assert.NotEmpty(t, req.GetRunId())
		f.setRuns(&pb.ActiveRun{Command: pb.Command_COMMAND_STANZA_DELETE, RunId: req.GetRunId()})
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := c.Run(ctx, &pb.RunRequest{Command: pb.Command_COMMAND_STANZA_DELETE}, io.Discard, io.Discard)
		done <- err
	}()
	<-started
	cancel()
	return done
}

// A Status that fails for several polls does not end the wait: only a Status that
// answers and no longer lists the run does.
func TestStanzaDelete_StatusFailuresDoNotEndTheWait(t *testing.T) {
	f := &fakeSidecarServer{status: &pb.StatusResponse{InstanceId: "inst-1"}}
	c := newSidecarClient(serveFakeSidecar(t, f))
	c.stopPoll = 10 * time.Millisecond
	t.Cleanup(c.Close)
	done := startDeleteWait(t, f, c)

	f.mu.Lock()
	f.statusErr = errors.New("status unavailable")
	f.mu.Unlock()
	select {
	case err := <-done:
		t.Fatalf("the wait ended while Status was failing: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	f.mu.Lock()
	f.statusErr = nil
	f.mu.Unlock()
	select {
	case err := <-done:
		t.Fatalf("the wait ended while the run was still listed: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	f.setRuns()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the wait did not end once the run left Status")
	}
}

// Only this Run's id holds the wait: another delete's run (a second caller, or
// a later one) does not, and a different run listed under the same command does
// not keep this one waiting.
func TestStanzaDelete_WaitsForItsOwnRunOnly(t *testing.T) {
	f := &fakeSidecarServer{status: &pb.StatusResponse{InstanceId: "inst-1"}}
	c := newSidecarClient(serveFakeSidecar(t, f))
	c.stopPoll = 10 * time.Millisecond
	t.Cleanup(c.Close)
	done := startDeleteWait(t, f, c)

	f.setRuns(&pb.ActiveRun{Command: pb.Command_COMMAND_STANZA_DELETE, RunId: "someone-else"})
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("another run of the same command held the wait")
	}
}

// What the Run says about spawning: a refusal that never started a child needs
// no wait, even though the same sidecar lists another delete.
func TestStanzaDelete_NoChildSpawnedMeansNoWait(t *testing.T) {
	f := &fakeSidecarServer{status: &pb.StatusResponse{InstanceId: "inst-1"}}
	f.setRuns(&pb.ActiveRun{Command: pb.Command_COMMAND_STANZA_DELETE, RunId: "x"})
	f.run = func(_ context.Context, req *pb.RunRequest, stream *connect.ServerStream[pb.RunOutput]) error {
		assert.Equal(t, "inst-1", req.GetExpectedInstanceId())
		// Listed under its own id, the way a refused delete is for a moment.
		f.setRuns(&pb.ActiveRun{Command: pb.Command_COMMAND_STANZA_DELETE, RunId: req.GetRunId()})
		stream.ResponseTrailer().Set(backupCommon.ChildSpawnedTrailer, backupCommon.ChildSpawnedNone)
		return connect.NewError(connect.CodeFailedPrecondition, errors.New("refusing to delete the stanza: the repository holds 1 backups"))
	}
	c := newSidecarClient(serveFakeSidecar(t, f))
	c.stopTimeout = 100 * time.Millisecond
	t.Cleanup(c.Close)

	finished := make(chan error, 1)
	go func() {
		_, err := c.Run(t.Context(), &pb.RunRequest{Command: pb.Command_COMMAND_STANZA_DELETE}, io.Discard, io.Discard)
		finished <- err
	}()
	select {
	case err := <-finished:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "holds 1 backups")
	case <-time.After(3 * time.Second):
		t.Fatal("waited for a delete that was never started")
	}
}

// A response that never arrives (the stream broke) carries no trailer, so the
// delete may have been started and its own run is waited for.
func TestStanzaDelete_LostResponseMeansWaitOnTheRun(t *testing.T) {
	f := &fakeSidecarServer{status: &pb.StatusResponse{InstanceId: "inst-1"}}
	c := newSidecarClient(serveFakeSidecar(t, f))
	c.stopPoll = 10 * time.Millisecond
	t.Cleanup(c.Close)
	done := startDeleteWait(t, f, c)
	select {
	case err := <-done:
		t.Fatalf("returned while the run was listed: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	f.setRuns()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("did not return once the run left Status")
	}
}

// The delete is bound to the instance the client read: a sidecar that is
// another instance by the time the Run arrives refuses it.
func TestStanzaDelete_InstanceChangedBetweenStatusAndAdmissionIsRefused(t *testing.T) {
	f := &fakeSidecarServer{status: &pb.StatusResponse{InstanceId: "inst-1"}}
	var seen atomic.Value
	f.run = func(_ context.Context, req *pb.RunRequest, stream *connect.ServerStream[pb.RunOutput]) error {
		seen.Store(req.GetExpectedInstanceId())
		// The replacement instance answers: it is not the one the client asked.
		stream.ResponseTrailer().Set(backupCommon.ChildSpawnedTrailer, backupCommon.ChildSpawnedNone)
		return connect.NewError(connect.CodeFailedPrecondition, errors.New("expected_instance_id is empty or names a different sidecar instance"))
	}
	f.statusHook = func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.statusCalls++; f.statusCalls == 1 {
			return
		}
		f.status.InstanceId = "inst-2"
	}
	c := newSidecarClient(serveFakeSidecar(t, f))
	t.Cleanup(c.Close)
	_, err := c.Run(t.Context(), &pb.RunRequest{Command: pb.Command_COMMAND_STANZA_DELETE}, io.Discard, io.Discard)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "different sidecar instance")
	assert.Equal(t, "inst-1", seen.Load(), "the client sent the instance it had read")
}

// A delete whose start could not be lifted fails the Run in-band; the engine
// reports it and never waits on a status counter.
func TestStanzaDelete_StartFailureReportedByTheRun(t *testing.T) {
	f := &fakeSidecarServer{status: &pb.StatusResponse{InstanceId: "inst-1"}}
	f.run = func(context.Context, *pb.RunRequest, *connect.ServerStream[pb.RunOutput]) error {
		return connect.NewError(connect.CodeFailedPrecondition, errors.New("pgbackrest start failed; WAL archiving is stopped"))
	}
	c := newSidecarClient(serveFakeSidecar(t, f))
	t.Cleanup(c.Close)
	pm := createTestManager(t, t.TempDir(), "", "", clustermetadatapb.PoolerType_REPLICA)
	e := newStubEngine(pm.logger, pm.record, backupengine.Settings{Sidecar: c})

	err := e.StanzaDelete(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WAL archiving is stopped")
}

// The repo-ls fields travel to the sidecar as typed fields, not as options.
func TestSidecarClient_ArchiveFieldsReachTheSidecar(t *testing.T) {
	f := &fakeSidecarServer{status: &pb.StatusResponse{}}
	var got atomic.Pointer[pb.RunRequest]
	f.run = func(_ context.Context, req *pb.RunRequest, stream *connect.ServerStream[pb.RunOutput]) error {
		got.Store(req)
		return stream.Send(&pb.RunOutput{Chunk: &pb.RunOutput_ExitCode{ExitCode: 0}})
	}
	c := newSidecarClient(serveFakeSidecar(t, f))
	t.Cleanup(c.Close)

	_, err := c.Run(t.Context(), &pb.RunRequest{Command: pb.Command_COMMAND_REPO_LS, Args: []string{"--output=json"}, ArchiveId: "17-1", WalDir: "000000010000000A"}, io.Discard, io.Discard)
	require.NoError(t, err)
	req := got.Load()
	require.NotNil(t, req)
	assert.Equal(t, pb.Command_COMMAND_REPO_LS, req.GetCommand())
	assert.Equal(t, []string{"--output=json"}, req.GetArgs())
	assert.Equal(t, "17-1", req.GetArchiveId())
	assert.Equal(t, "000000010000000A", req.GetWalDir())
}

// Only commands that take the backup lock wait after a cancel; every member of
// the enum is classified so a new command has to be decided on.
func TestSidecarClient_OnlyLockTakingCommandsWaitAfterACancel(t *testing.T) {
	waits := map[pb.Command]bool{
		pb.Command_COMMAND_BACKUP:        true,
		pb.Command_COMMAND_EXPIRE:        true,
		pb.Command_COMMAND_STANZA_CREATE: true,
		// A delete a cancelled caller leaves running could remove the stanza
		// the next lease holder creates, so the call waits for it.
		pb.Command_COMMAND_STANZA_DELETE: true,
	}
	for v, name := range pb.Command_name {
		cmd := pb.Command(v)
		t.Run(name, func(t *testing.T) {
			f := &fakeSidecarServer{status: &pb.StatusResponse{InstanceId: "inst-1"}}
			started := make(chan struct{}, 1)
			f.run = func(ctx context.Context, req *pb.RunRequest, _ *connect.ServerStream[pb.RunOutput]) error {
				f.setRuns(&pb.ActiveRun{Command: cmd, RunId: req.GetRunId()})
				started <- struct{}{}
				<-ctx.Done()
				return ctx.Err() // stays listed, so a waiting Run cannot return early
			}
			c := newSidecarClient(serveFakeSidecar(t, f))
			c.stopTimeout = 5 * time.Second
			t.Cleanup(c.Close)

			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() {
				_, err := c.Run(ctx, &pb.RunRequest{Command: cmd, Args: nil}, io.Discard, io.Discard)
				done <- err
			}()
			<-started
			cancel()
			if waits[cmd] {
				select {
				case err := <-done:
					t.Fatalf("returned while the command was still listed: %v", err)
				case <-time.After(500 * time.Millisecond):
				}
				f.setActive()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("did not return once the command was gone")
				}
				return
			}
			select {
			case err := <-done:
				require.Error(t, err)
				assert.NotContains(t, err.Error(), "did not stop")
			case <-time.After(400 * time.Millisecond):
				t.Fatal("a lock-free command waited after a cancel")
			}
		})
	}
}

// The wait runs inside an action lock that GracefulShutdown needs, so it must
// end at the top of shutdown, not at its end.
func TestGracefulShutdown_EndsASidecarCancelWait(t *testing.T) {
	f := &fakeSidecarServer{status: &pb.StatusResponse{}}
	started := make(chan struct{}, 1)
	f.run = func(ctx context.Context, _ *pb.RunRequest, _ *connect.ServerStream[pb.RunOutput]) error {
		f.setActive(pb.Command_COMMAND_BACKUP)
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err() // never leaves Status
	}
	fx := newAsyncRestoreFixtureWith(t, serveFakeSidecar(t, f))
	require.NotNil(t, fx.pm.sidecar)

	cancelRun := make(chan struct{})
	runDone := make(chan error, 1)
	go func() {
		// Like a backup: the action lock is held for the whole run, wait included.
		lockCtx, err := fx.pm.actionLock.Acquire(t.Context(), "test-backup")
		if err != nil {
			runDone <- err
			return
		}
		defer fx.pm.actionLock.Release(lockCtx)
		ctx, cancel := context.WithCancel(lockCtx)
		go func() { <-cancelRun; cancel() }()
		_, err = fx.pm.sidecar.Run(ctx, &pb.RunRequest{Command: pb.Command_COMMAND_BACKUP, Args: nil}, io.Discard, io.Discard)
		runDone <- err
	}()
	<-started
	close(cancelRun)
	time.Sleep(300 * time.Millisecond) // into the wait

	shutdownDone := make(chan struct{})
	go func() {
		fx.pm.GracefulShutdown(t.Context())
		close(shutdownDone)
	}()
	select {
	case err := <-runDone:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "shutting down")
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown did not end a backup's cancel wait, which holds the lock it needs")
	}
	select {
	case <-shutdownDone:
	case <-time.After(30 * time.Second):
		t.Fatal("GracefulShutdown never got the action lock")
	}
}

// Manager shutdown while a stanza delete is unsettled must not hand a still
// valid lease to the next holder: the callback returns an abandon request, the
// lease is not unlocked, and it expires on its own after its TTL. Real etcd, the
// manager's own GracefulShutdown, and the bootstrap's own stanza preparation as
// the lease callback, so the request has to survive its wraps, at each of the
// two places it deletes a stanza.
func TestStanzaDelete_ShutdownAbandonsTheLeaseUntilItExpires(t *testing.T) {
	if testing.Short() {
		t.Skip("starts etcd")
	}
	for name, site := range map[string]struct {
		info      string
		createErr string
	}{
		"the empty stanza is deleted first":   {info: stanzaInfoJSON(2)},
		"the retry after stanza-create fails": {info: stanzaInfoJSON(1), createErr: "ERROR: [028]: backup and archive info files exist but do not match the database"},
	} {
		t.Run(name, func(t *testing.T) {
			testShutdownAbandonsTheLease(t, site.info, site.createErr)
		})
	}
}

func testShutdownAbandonsTheLease(t *testing.T, infoJSON, createErr string) {
	const (
		key = "locks/stanza-delete-shutdown"
		ttl = 3 * time.Second
	)
	f := &fakeSidecarServer{status: &pb.StatusResponse{InstanceId: "inst-1"}}
	started := make(chan struct{}, 1)
	reply := func(stream *connect.ServerStream[pb.RunOutput], stdout, stderr string, code int32) error {
		if stdout != "" {
			if err := stream.Send(&pb.RunOutput{Chunk: &pb.RunOutput_Stdout{Stdout: []byte(stdout)}}); err != nil {
				return err
			}
		}
		if stderr != "" {
			if err := stream.Send(&pb.RunOutput{Chunk: &pb.RunOutput_Stderr{Stderr: []byte(stderr)}}); err != nil {
				return err
			}
		}
		return stream.Send(&pb.RunOutput{Chunk: &pb.RunOutput_ExitCode{ExitCode: code}})
	}
	f.run = func(ctx context.Context, req *pb.RunRequest, stream *connect.ServerStream[pb.RunOutput]) error {
		switch req.GetCommand() {
		case pb.Command_COMMAND_INFO:
			return reply(stream, infoJSON, "", 0)
		case pb.Command_COMMAND_REPO_LS:
			return reply(stream, `{".":{"type":"path"}}`, "", 0)
		case pb.Command_COMMAND_STANZA_CREATE:
			if createErr != "" {
				return reply(stream, "", createErr, 1)
			}
			return reply(stream, "", "", 0)
		case pb.Command_COMMAND_STANZA_DELETE:
			f.setRuns(&pb.ActiveRun{Command: pb.Command_COMMAND_STANZA_DELETE, RunId: req.GetRunId()})
			started <- struct{}{}
			<-ctx.Done()
			return ctx.Err() // the run stays listed
		}
		return connect.NewError(connect.CodeUnimplemented, errors.New("not scripted"))
	}
	fx := newAsyncRestoreFixtureWith(t, serveFakeSidecar(t, f))
	require.NoError(t, fx.pm.topoClient.MarkBootstrapInProgress(t.Context(), fx.pm.shardRef(), "earlier-attempt"))

	clientAddr, _, _ := etcdtopo.StartEtcd(t)
	newConn := func() topoclient.Conn {
		conn, err := etcdtopo.Factory{}.Create("global", "/topo", []string{clientAddr})
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })
		return conn
	}
	holder, competitor := newConn(), newConn()
	canAcquire := func() bool {
		d, err := competitor.TryLockWithLease(t.Context(), key, "competitor", ttl)
		if err != nil {
			return false
		}
		_ = d.Unlock(t.Context())
		return true
	}

	var desc topoclient.LockDescriptor
	acquire := func(ctx context.Context, action string) (context.Context, func(*error), error) {
		actx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		d, err := holder.TryLockWithLease(actx, key, action, ttl)
		if err != nil {
			return nil, nil, err
		}
		desc = d
		return ctx, func(*error) { _ = d.Unlock(context.WithoutCancel(ctx)) }, nil
	}
	revoke := func(context.Context) error { return errors.New("no steal") }
	check := func(ctx context.Context) error { return desc.Check(ctx) }

	callerCtx, cancelCaller := context.WithCancel(t.Context())
	leaseDone := make(chan struct{})
	var leaseErr error
	// Teardown that does not depend on the behaviour under test: the settlement
	// wait ignores the caller's cancellation by design, so the cleanup ends it by
	// making the sidecar stop listing the run, then waits for the lease goroutine.
	t.Cleanup(func() {
		f.setActive()
		cancelCaller()
		select {
		case <-leaseDone:
		case <-time.After(15 * time.Second):
			t.Error("the lease goroutine did not return in cleanup")
		}
	})
	go func() {
		defer close(leaseDone)
		// Like the bootstrap: the action lock is held for the lease callback.
		lockCtx, err := fx.pm.actionLock.Acquire(t.Context(), "test-bootstrap")
		if err != nil {
			leaseErr = err
			return
		}
		defer fx.pm.actionLock.Release(lockCtx)
		leaseErr = topoclient.WithLease(callerCtx, "bootstrap", acquire, revoke, check, func(leaseCtx context.Context) error {
			_, err := fx.pm.prepareStanzaLocked(leaseCtx)
			return err
		}, topoclient.WithLeaseCheckInterval(200*time.Millisecond), topoclient.WithLeaseTTL(ttl))
	}()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("the delete never started")
	}
	cancelCaller() // the caller goes away; the settlement wait keeps the lease
	time.Sleep(500 * time.Millisecond)
	require.False(t, canAcquire(), "held while the delete is settling")

	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		fx.pm.GracefulShutdown(t.Context())
	}()
	select {
	case <-leaseDone:
	case <-time.After(15 * time.Second):
		t.Fatal("shutdown did not end the settlement wait")
	}
	returned := time.Now()
	require.Error(t, leaseErr)
	assert.True(t, topoclient.IsLeaseAbandon(leaseErr), "the bootstrap's error asks for the lease to be abandoned: %v", leaseErr)
	assert.False(t, canAcquire(), "the still valid lease was not released at shutdown")
	select {
	case <-shutdownDone:
	case <-time.After(30 * time.Second):
		t.Error("GracefulShutdown did not return")
	}

	require.Eventually(t, canAcquire, ttl+5*time.Second, 100*time.Millisecond, "it expires on its own")
	assert.GreaterOrEqual(t, time.Since(returned), ttl-time.Second, "and only after about its TTL")
}
