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
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/multigres/multigres/go/common/pgbackreststate"

	"golang.org/x/sys/unix"
)

// SupervisorSubcommand is routed before command parsing, including in test binaries.
const SupervisorSubcommand = "worker-supervisor"

var (
	workerStopWait = 15 * time.Second
	// Allow buffered foreground output to drain. This is an output truncation
	// bound, never a process-quiescence test; tests shorten it.
	foregroundDrainWait = killWaitDelay
)

type workerSpec struct {
	Argv   []string
	Marker string
}
type workerEvent struct {
	Spawned int    `json:",omitempty"`
	Exit    *int   `json:",omitempty"`
	Quiet   bool   `json:",omitempty"`
	Error   string `json:",omitempty"`
}

type ownedWorker struct {
	control    *os.File
	once       sync.Once
	done       chan struct{}
	foreground chan workerEvent
	err        error // published by closing done
}

func (w *ownedWorker) stop() {
	w.once.Do(func() {
		if w.control != nil {
			_ = w.control.Close()
		}
	})
}

func (w *ownedWorker) wait(ctx context.Context) error {
	select {
	case <-w.done:
		return w.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// acquireOwnership runs before any socket replacement or startup cleanup. The
// lock inode is NEVER unlinked. Supervisors inherit its open file description.
// Unsettled same-domain markers fail closed even with a free lock. Only the
// narrow serialized PID-1 container-replacement contract can retire prior-domain
// markers; process identity changes alone are never evidence of quiescence.
func (s *Server) acquireOwnership() error {
	fd, err := unix.Open(s.cfg.stateSub(pgbackreststate.InstanceLockName), unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0o600)
	if err != nil {
		return err
	}
	f := os.NewFile(uintptr(fd), "instance.lock")
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		f.Close()
		return fmt.Errorf("another sidecar or its workers still own this state directory: %w", err)
	}
	if err := pgbackreststate.ValidateServingProtocol(f); err != nil {
		f.Close()
		return fmt.Errorf("unrecognized supervision protocol: %w", err)
	}
	owner, err := currentWorkerOwner()
	if err != nil {
		f.Close()
		return fmt.Errorf("establish worker process domain: %w", err)
	}
	s.workerOwner = owner
	if err := s.recoverWorkerMarkers(); err != nil {
		f.Close()
		return err
	}
	if err := pgbackreststate.RecordSupervision(s.cfg.StateDir, f); err != nil {
		f.Close()
		return fmt.Errorf("record supervision protocol: %w", err)
	}
	s.instanceLock = f
	return nil
}

// runOwned separates foreground completion from lifetime ownership. An async
// archive command can return while its supervisor stays in workers. Only a
// positive Quiet event AND successful supervisor exit release that entry.
func (s *Server) runOwned(ctx context.Context, child *exec.Cmd, stdout, stderr io.Writer, kind Kind, command string) (exit int, runErr error) {
	if err := ctx.Err(); err != nil {
		return -1, err
	}
	s.workersMu.Lock()
	if s.workersClosing {
		s.workersMu.Unlock()
		return -1, errors.New("worker admission closed")
	}
	marker := s.cfg.stateSub("worker-" + uuid.NewString() + ".pending")
	record, err := s.workerMarkerBytes()
	if err != nil {
		s.workersMu.Unlock()
		return -1, err
	}
	spawned := false
	defer func() {
		// Start returning an error proves no supervisor was created. After a
		// successful Start, only the supervisor may retire the marker.
		if !spawned {
			runErr = errors.Join(runErr, retireWorkerMarker(marker))
		}
	}()
	if err := privateWrite(marker, record); err != nil {
		s.workersMu.Unlock()
		return -1, err
	}
	controlR, controlW, err := os.Pipe()
	if err != nil {
		s.workersMu.Unlock()
		return -1, err
	}
	eventsR, eventsW, err := os.Pipe()
	if err != nil {
		controlR.Close()
		controlW.Close()
		s.workersMu.Unlock()
		return -1, err
	}
	outR, outW, err := os.Pipe()
	if err != nil {
		controlR.Close()
		controlW.Close()
		eventsR.Close()
		eventsW.Close()
		s.workersMu.Unlock()
		return -1, err
	}
	errR, errW, err := os.Pipe()
	if err != nil {
		controlR.Close()
		controlW.Close()
		eventsR.Close()
		eventsW.Close()
		outR.Close()
		outW.Close()
		s.workersMu.Unlock()
		return -1, err
	}
	// The instance lock belongs only to the supervisor, never to the sandboxed
	// pgBackRest process. The supervisor must survive sidecar death to reap it.
	// Explicit lifetime pipe, not context-driven process-group cancellation.
	cmd := exec.Command(s.self, SupervisorSubcommand)
	cmd.SysProcAttr = supervisorSysProcAttr()
	cmd.Env = child.Env
	cmd.ExtraFiles = []*os.File{controlR, eventsW, s.instanceLock, outW, errW}
	cmd.Stderr = os.Stderr
	err = cmd.Start()
	controlR.Close()
	eventsW.Close()
	outW.Close()
	errW.Close()
	if err != nil {
		controlW.Close()
		eventsR.Close()
		outR.Close()
		errR.Close()
		s.workersMu.Unlock()
		return -1, err
	}
	spawned = true
	s.children.Store(cmd.Process.Pid, struct{}{})
	w := &ownedWorker{control: controlW, done: make(chan struct{}), foreground: make(chan workerEvent, 1)}
	if s.workers == nil {
		s.workers = map[*ownedWorker]struct{}{}
	}
	s.workers[w] = struct{}{}
	go func() {
		defer eventsR.Close()
		decoder := json.NewDecoder(eventsR)
		quiet := false
		var failure error
		for {
			var event workerEvent
			if err := decoder.Decode(&event); err != nil {
				if !errors.Is(err, io.EOF) {
					failure = err
				}
				break
			}
			if event.Spawned != 0 && kind == KindServer {
				s.serverPID.Store(int64(event.Spawned))
			}
			if event.Exit != nil {
				w.foreground <- event
			}
			if event.Error != "" && event.Exit == nil {
				failure = errors.New(event.Error)
			}
			if event.Quiet {
				quiet = true
			}
		}
		if err := cmd.Wait(); err != nil {
			failure = err
		}
		if !quiet && failure == nil {
			failure = errors.New("supervisor exited without quiescence acknowledgement")
		}
		s.children.Delete(cmd.Process.Pid)
		if kind == KindServer {
			s.serverPID.Store(0)
		}
		w.err = failure
		w.stop()
		s.workersMu.Lock()
		// Failed entries stay latched: no subsequent restore may ignore them.
		if failure == nil {
			delete(s.workers, w)
		}
		close(w.done)
		s.workersMu.Unlock()
	}()

	var outputMu sync.Mutex
	outRelay := newWorkerOutput(&outputMu, stdout)
	errRelay := newWorkerOutput(&outputMu, stderr)
	outDone, errDone := make(chan error, 1), make(chan error, 1)
	go func() { _, err := io.Copy(outRelay, outR); outR.Close(); outDone <- err }()
	go func() { _, err := io.Copy(errRelay, errR); errR.Close(); errDone <- err }()
	copies := []chan error{outDone, errDone}
	truncated := false
	defer func() { detachWorkerOutputs(truncated, outRelay, errRelay) }()

	// Publish the whole spec before shutdown can close this registered worker's
	// lifetime pipe. Otherwise a normal shutdown could interrupt its initial
	// frame and leave an unnecessarily ambiguous pending marker.
	// A stopped or broken supervisor cannot hold the admission mutex forever.
	// A failed/partial handoff is ambiguous and deliberately keeps its marker.
	handoffErr := controlW.SetWriteDeadline(time.Now().Add(workerStopWait))
	if handoffErr == nil {
		handoffErr = json.NewEncoder(controlW).Encode(workerSpec{Argv: child.Args, Marker: marker})
		handoffErr = errors.Join(handoffErr, controlW.SetWriteDeadline(time.Time{}))
	}
	s.workersMu.Unlock()
	if handoffErr != nil {
		w.stop()
		return -1, handoffErr
	}
	var event workerEvent
	select {
	case event = <-w.foreground:
	case <-ctx.Done():
		w.stop()
		waitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), workerStopWait)
		defer cancel()
		if err := w.wait(waitCtx); err != nil {
			return -1, fmt.Errorf("cancel quiescence: %w", err)
		}
		return -1, ctx.Err()
	case <-w.done:
		select {
		case event = <-w.foreground:
		default:
			return -1, fmt.Errorf("worker failed before foreground result: %w", w.err)
		}
	}
	if ctx.Err() != nil {
		w.stop()
		waitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), workerStopWait)
		defer cancel()
		if err := w.wait(waitCtx); err != nil {
			return -1, err
		}
		return -1, ctx.Err()
	}

	// Normally pgBackRest's daemon closes stdio. If a descendant retains it,
	// foreground completion must still be independent of lifetime ownership.
	// After this bound, detach response sinks and continue draining to discard.
	drainCtx, cancel := context.WithTimeout(ctx, foregroundDrainWait)
	defer cancel()
	for _, done := range copies {
		select {
		case err := <-done:
			if err != nil {
				w.stop()
				return -1, err
			}
		case <-drainCtx.Done():
			truncated = true
			if ctx.Err() != nil {
				w.stop()
				return -1, ctx.Err()
			}
			// Wire sinks report truncation explicitly. WAL operations only consume
			// the foreground exit code. Never let a raw parser trust partial output.
			if (outRelay.cancel == nil || errRelay.cancel == nil) && command != cmdArchivePush && command != cmdArchiveGet {
				return -1, errors.New("worker retained foreground output pipes; output truncated")
			}
			return *event.Exit, nil
		}
	}

	if event.Error != "" {
		return -1, errors.New(event.Error)
	}
	return *event.Exit, nil
}

func (s *Server) quiesceWorkers(ctx context.Context) error {
	s.workersMu.Lock()
	workers := make([]*ownedWorker, 0, len(s.workers))
	for w := range s.workers {
		workers = append(workers, w)
		w.stop()
	}
	s.workersMu.Unlock()
	for _, w := range workers {
		if err := w.wait(ctx); err != nil {
			return fmt.Errorf("worker quiescence unproven: %w", err)
		}
	}
	return nil
}

// SupervisorMain owns exactly one invocation and every descendant. This helper
// is deliberately outside Landlock; only its child receives the child policy.
// No control, status or instance-lock descriptor is passed to that child.
func SupervisorMain() error {
	// Inherited ExtraFiles are not automatically CLOEXEC in a Go re-exec.
	// Without this, a daemon can keep the lifetime/event/lock/output FDs open
	// even after redirecting its stdio, defeating both EOF and output draining.
	for fd := 3; fd <= 7; fd++ {
		syscall.CloseOnExec(fd)
	}
	control := os.NewFile(3, "lifetime")
	events := os.NewFile(4, "events")
	lock := os.NewFile(5, "instance-lock")
	defer lock.Close()
	stdout, stderr := os.NewFile(6, "stdout"), os.NewFile(7, "stderr")
	defer stdout.Close()
	defer stderr.Close()
	var spec workerSpec
	decoder := json.NewDecoder(control)
	if err := decoder.Decode(&spec); err != nil {
		return err
	}
	if len(spec.Argv) == 0 {
		return errors.New("missing worker argv")
	}
	if err := becomeSubreaper(); err != nil {
		return err
	}
	// Decode may leave encoder whitespace buffered or still in the pipe.
	// Only EOF/read failure ends ownership, never a successful one-byte read.
	stopping := watchWorkerLifetime(io.MultiReader(decoder.Buffered(), control))
	encoder := json.NewEncoder(events)
	neverSpawned := func(cause error) error {
		code := -1
		_ = encoder.Encode(workerEvent{Exit: &code, Error: cause.Error()})
		if err := retireWorkerMarker(spec.Marker); err != nil {
			return err
		}
		return encoder.Encode(workerEvent{Quiet: true})
	}
	// This live supervisor has decoded the complete spec and knows it has not
	// spawned a child. This is not recovery after an unknown supervisor death.
	select {
	case <-stopping:
		return neverSpawned(errors.New("sidecar stopped before worker spawn"))
	default:
	}
	// This process owns all reaping through Wait4, not executil.Wait.
	child := exec.Command(spec.Argv[0], spec.Argv[1:]...)
	child.Env = os.Environ()
	child.Stdout, child.Stderr = stdout, stderr
	child.SysProcAttr = supervisedChildSysProcAttr()
	if err := child.Start(); err != nil {
		return neverSpawned(err)
	}
	pid := child.Process.Pid
	if err := encoder.Encode(workerEvent{Spawned: pid}); err != nil {
		//nolint:gocritic // exclusive Wait4 reaper below must retain PID ownership; stop helpers reap the child
		_ = child.Process.Kill()
	}
	_ = child.Process.Release() // Wait4 below owns ALL reaping, including the root.
	stdout.Close()
	stderr.Close()
	rootDone := false
	var stopAt time.Time
	for {
		select {
		case <-stopping:
			if stopAt.IsZero() {
				stopAt = time.Now()
			}
		default:
		}
		if !stopAt.IsZero() {
			sig := syscall.SIGTERM
			if time.Since(stopAt) >= time.Second {
				sig = syscall.SIGKILL
			}
			if err := signalOwnedChildren(pid, sig); err != nil {
				return err
			}
		}
		var status syscall.WaitStatus
		reaped, err := syscall.Wait4(-1, &status, syscall.WNOHANG, nil)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if errors.Is(err, syscall.ECHILD) {
			if !rootDone {
				return errors.New("lost foreground child")
			}
			if !portableGroupGone(pid) {
				if stopAt.IsZero() {
					stopAt = time.Now()
				}
				time.Sleep(5 * time.Millisecond)
				continue
			}
			// The supervisor retains the portable group anchor through this ACK.
			if err := retireWorkerMarker(spec.Marker); err != nil {
				return err
			}
			return encoder.Encode(workerEvent{Quiet: true})
		}
		if err != nil {
			return err
		}
		if reaped == pid {
			rootDone = true
			code := status.ExitStatus()
			if status.Signaled() {
				code = 128 + int(status.Signal())
			}
			if err := encoder.Encode(workerEvent{Exit: &code}); err != nil {
				if stopAt.IsZero() {
					stopAt = time.Now()
				}
			}
		}
		if reaped == 0 {
			time.Sleep(5 * time.Millisecond)
		}
	}
}

type lockedWorkerOutput struct {
	mu     *sync.Mutex
	w      io.Writer
	cancel func(bool)
}

func newWorkerOutput(mu *sync.Mutex, w io.Writer) *lockedWorkerOutput {
	relay := &lockedWorkerOutput{mu: mu, w: w}
	if sink, ok := w.(interface{ detachOutput(bool) }); ok {
		relay.cancel = sink.detachOutput
	}
	return relay
}

func (w *lockedWorkerOutput) interrupt(truncated bool) {
	if w.cancel != nil {
		w.cancel(truncated)
	}
}

func (w *lockedWorkerOutput) Write(b []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.w == nil {
		return len(b), nil
	}
	return w.w.Write(b)
}

// Interrupt BOTH sinks before taking the shared lock: one may be inside a
// backpressured Write while holding it. After detachment the drain goroutines
// retain neither a Connect stream nor caller-owned buffers, only nil sinks.
func detachWorkerOutputs(truncated bool, outputs ...*lockedWorkerOutput) {
	for _, output := range outputs {
		output.interrupt(truncated)
	}
	mu := outputs[0].mu
	mu.Lock()
	defer mu.Unlock()
	for _, output := range outputs {
		output.w = nil
		output.cancel = nil
	}
}

// watchWorkerLifetime drains framing leftovers without mistaking them for a
// termination message. The private protocol has one initial JSON frame and EOF
// as its only lifetime signal; it deliberately has no byte-valued stop command.
func watchWorkerLifetime(reader io.Reader) <-chan struct{} {
	ended := make(chan struct{})
	go func() { _, _ = io.Copy(io.Discard, reader); close(ended) }()
	return ended
}

// retireWorkerMarker is only for positively never-spawned or proven settled
// invocations. A missing marker is safe here; a failed directory sync is not.
func retireWorkerMarker(marker string) error {
	if err := os.Remove(marker); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove worker marker: %w", err)
	}
	dir, err := os.Open(filepath.Dir(marker))
	if err != nil {
		return err
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync worker marker: %w", err)
	}
	return nil
}
