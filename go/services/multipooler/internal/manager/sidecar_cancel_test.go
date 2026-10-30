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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "github.com/multigres/multigres/go/pb/pgbackrestdservice"
	backupengine "github.com/multigres/multigres/go/services/multipooler/internal/manager/backup"
)

func (f *fakeSidecarServer) setActive(cmds ...pb.Command) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status.ActiveCommands = cmds
}

// blockingRun registers cmd as active, signals started, and holds the stream
// open until the client goes away. The command then stays "active" for dying,
// the way the sidecar's child does while it is killed and reaped, after the
// handler has already returned.
func blockingRun(f *fakeSidecarServer, cmd pb.Command, started chan<- struct{}, dying time.Duration, cleared *atomic.Int64) func(context.Context, *pb.RunRequest, *connect.ServerStream[pb.RunOutput]) error {
	return func(ctx context.Context, _ *pb.RunRequest, _ *connect.ServerStream[pb.RunOutput]) error {
		f.setActive(cmd)
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
		_, err := c.Run(ctx, pb.Command_COMMAND_BACKUP, nil, io.Discard, io.Discard)
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
	old := sidecarStopTimeout
	sidecarStopTimeout = 300 * time.Millisecond
	t.Cleanup(func() { sidecarStopTimeout = old })

	f := &fakeSidecarServer{status: &pb.StatusResponse{}}
	started := make(chan struct{}, 1)
	f.run = func(ctx context.Context, _ *pb.RunRequest, _ *connect.ServerStream[pb.RunOutput]) error {
		f.setActive(pb.Command_COMMAND_BACKUP)
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err() // the command never leaves Status
	}
	c := newSidecarClient(serveFakeSidecar(t, f))
	t.Cleanup(c.Close)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := c.Run(ctx, pb.Command_COMMAND_BACKUP, nil, io.Discard, io.Discard)
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
		_, err := c.Run(ctx, pb.Command_COMMAND_BACKUP, nil, io.Discard, io.Discard)
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
		_, err := c.Run(ctx, pb.Command_COMMAND_RESTORE, nil, io.Discard, io.Discard)
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
		_, _ = c.Run(other, pb.Command_COMMAND_BACKUP, nil, io.Discard, io.Discard)
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
		_, err := c.Run(ctx, pb.Command_COMMAND_BACKUP, nil, io.Discard, io.Discard)
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
	_, err := gone.Run(t.Context(), pb.Command_COMMAND_BACKUP, nil, io.Discard, io.Discard)
	assert.True(t, errors.Is(err, backupengine.ErrRunNotDelivered), "got %v", err)
}

// Liveness: a run of the same command that starts during the wait is
// legitimate and must not hold the wait to its bound.
func TestSidecarClient_CancelWaitRecountsOtherRunsEveryPoll(t *testing.T) {
	old := sidecarStopTimeout
	sidecarStopTimeout = 5 * time.Second
	t.Cleanup(func() { sidecarStopTimeout = old })

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
	t.Cleanup(c.Close)

	ctxA, cancelA := context.WithCancel(t.Context())
	doneA := make(chan error, 1)
	go func() {
		_, err := c.Run(ctxA, pb.Command_COMMAND_BACKUP, nil, io.Discard, io.Discard)
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
		_, _ = c.Run(ctxB, pb.Command_COMMAND_BACKUP, nil, io.Discard, io.Discard)
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

// Only commands that take the backup lock wait after a cancel; every member of
// the enum is classified so a new command has to be decided on.
func TestSidecarClient_OnlyLockTakingCommandsWaitAfterACancel(t *testing.T) {
	old := sidecarStopTimeout
	sidecarStopTimeout = 5 * time.Second
	t.Cleanup(func() { sidecarStopTimeout = old })

	waits := map[pb.Command]bool{
		pb.Command_COMMAND_BACKUP:        true,
		pb.Command_COMMAND_EXPIRE:        true,
		pb.Command_COMMAND_STANZA_CREATE: true,
	}
	for v, name := range pb.Command_name {
		cmd := pb.Command(v)
		t.Run(name, func(t *testing.T) {
			f := &fakeSidecarServer{status: &pb.StatusResponse{}}
			started := make(chan struct{}, 1)
			f.run = func(ctx context.Context, _ *pb.RunRequest, _ *connect.ServerStream[pb.RunOutput]) error {
				f.setActive(cmd)
				started <- struct{}{}
				<-ctx.Done()
				return ctx.Err() // stays listed, so a waiting Run cannot return early
			}
			c := newSidecarClient(serveFakeSidecar(t, f))
			t.Cleanup(c.Close)

			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() {
				_, err := c.Run(ctx, cmd, nil, io.Discard, io.Discard)
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
		_, err = fx.pm.sidecar.Run(ctx, pb.Command_COMMAND_BACKUP, nil, io.Discard, io.Discard)
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
