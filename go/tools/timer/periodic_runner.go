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

// Package timer provides PeriodicRunner for running callbacks at regular intervals.
package timer

import (
	"context"
	"sync"
	"time"
)

// state represents the lifecycle state of the PeriodicRunner.
type state int

const (
	stopped  state = iota // not running, can be started
	running               // actively running, callbacks may be scheduled
	stopping              // Stop() called, waiting for in-flight callbacks to complete
)

// PeriodicRunner runs a callback at regular intervals with lifecycle management.
//
// Key behaviors:
//   - Callback receives a context derived from the parent context
//   - Stop() cancels the context and waits for in-flight callbacks
//   - Next callback scheduled only after current completes (backpressure)
//   - Supports Start/Stop/Start cycles (reopening)
//
// Example usage:
//
//	runner := timer.NewPeriodicRunner(ctx, 1*time.Second)
//	runner.Start(func(ctx context.Context) {
//	    // periodic work here
//	})
//	// later...
//	runner.Stop() // waits for any in-flight callback
type PeriodicRunner struct {
	parentCtx context.Context
	interval  time.Duration

	mu       sync.Mutex
	cond     *sync.Cond      // for waiting on state transitions
	state    state           // current lifecycle state
	ctx      context.Context // child context, created on Start, cancelled on Stop
	cancel   context.CancelFunc
	timer    *time.Timer
	wg       sync.WaitGroup
	callback func(ctx context.Context)
}

// NewPeriodicRunner creates a PeriodicRunner with the given parent context and interval.
// The parent context is used to derive child contexts on each Start() call.
// Callers should typically pass a detached context (e.g., ctxutil.Detach()) to avoid
// the runner being cancelled when request contexts complete.
func NewPeriodicRunner(ctx context.Context, interval time.Duration) *PeriodicRunner {
	pr := &PeriodicRunner{
		parentCtx: ctx,
		interval:  interval,
		state:     stopped,
	}
	pr.cond = sync.NewCond(&pr.mu)
	return pr
}

// Start begins running the callback at regular intervals.
// The callback receives a context that is cancelled when Stop() is called.
// If onStart is non-nil, it is called exactly once when actually starting
// (not when already running), before any callback can execute.
// Returns true if the runner was started, false if it was already running.
func (r *PeriodicRunner) Start(callback func(ctx context.Context), onStart func()) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Wait for any in-progress Stop() to complete
	for r.state == stopping {
		r.cond.Wait()
	}

	if r.state == running {
		return false
	}

	r.state = running
	r.callback = callback
	r.ctx, r.cancel = context.WithCancel(r.parentCtx)

	if onStart != nil {
		onStart()
	}

	r.scheduleNext()
	return true
}

// Stop cancels the context and waits for any in-flight callback to complete.
// After Stop returns, no more callbacks will run. Can be restarted with Start().
// Stop is idempotent - calling it when already stopped has no effect.
func (r *PeriodicRunner) Stop() {
	r.mu.Lock()

	if r.state != running {
		// Already stopped or another Stop() is in progress
		r.mu.Unlock()
		return
	}

	// Transition to stopping - from this point, this thread owns the stopping->stopped transition
	r.state = stopping

	// Cancel context to unblock any in-flight callback
	if r.cancel != nil {
		r.cancel()
	}

	// Stop the timer to prevent new callbacks from being scheduled
	if r.timer != nil {
		r.timer.Stop()
		r.timer = nil
	}

	r.ctx = nil
	r.cancel = nil
	r.callback = nil

	r.mu.Unlock()

	// Wait for any in-flight callback to complete (outside lock to avoid deadlock)
	r.wg.Wait()

	// Transition to stopped and wake any waiting Start() calls
	r.mu.Lock()
	// Because this goroutine owns the transition from stopping -> stopped,
	// nothing should have changed with that while we released the lock to Wait().
	if r.state != stopping {
		panic("PeriodicRunner reached an impossible state")
	}
	r.state = stopped
	r.cond.Broadcast()
	r.mu.Unlock()
}

// Running returns true if the runner is currently running.
// This includes the stopping state (while waiting for in-flight callbacks).
func (r *PeriodicRunner) Running() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state != stopped
}

// scheduleNext schedules the next callback execution.
// Must be called while holding r.mu.
func (r *PeriodicRunner) scheduleNext() {
	r.timer = time.AfterFunc(r.interval, r.execute)
}

// execute runs the callback and schedules the next execution.
func (r *PeriodicRunner) execute() {
	r.mu.Lock()

	if r.state != running || r.ctx == nil {
		r.mu.Unlock()
		return
	}

	// Opportunistically stop if parent context is cancelled
	if r.ctx.Err() != nil {
		r.mu.Unlock()
		r.Stop()
		return
	}

	// Track this execution so Stop() can wait for it to complete
	r.wg.Add(1)
	defer r.wg.Done()

	// Capture callback and context while holding the lock
	callback := r.callback
	ctx := r.ctx

	// Release lock during callback execution to avoid blocking Stop()
	r.mu.Unlock()

	callback(ctx)

	// Re-acquire lock to schedule next execution
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != running {
		return
	}

	// Schedule next execution only after this one completes (backpressure)
	r.scheduleNext()
}
