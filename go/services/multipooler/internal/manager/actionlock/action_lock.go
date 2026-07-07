// Copyright 2025 Supabase, Inc.
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

package actionlock

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/multigres/multigres/go/common/mterrors"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// actionLockKey is the context key for storing action lock information
type actionLockKey struct{}

// actionLockValue contains information about a lock acquisition
type actionLockValue struct {
	lockID    uint64
	operation string
	released  *atomic.Bool
}

// ActionLock is a wrapper around a semaphore that tracks ownership via Context.
// It ensures that only one goroutine can hold the lock at a time and provides
// mechanisms to assert that a given context holds the lock.
type ActionLock struct {
	sema      *semaphore.Weighted
	mu        sync.Mutex
	currentID uint64 // ID of current lock holder (0 if unlocked)
	nextID    uint64 // Counter for generating unique IDs

	// currentCancel cancels the derived context returned to the current
	// holder. Every acquisition (urgent or not) gets one, so any future
	// UrgentAcquire call can ask the current holder to wind down, regardless
	// of how it was acquired. nil when unlocked. Cleared on Release.
	currentCancel context.CancelFunc

	// urgentWaiting counts in-flight UrgentAcquire calls that haven't yet
	// acquired or given up. urgentDone is closed whenever urgentWaiting
	// drops back to 0 (the last one out closes it) and replaced with a
	// fresh channel on the next 0->1 transition. A normal Acquire call
	// waits on it instead of racing the semaphore while urgent work is in
	// flight — see acquire and UrgentAcquire.
	urgentWaiting int
	urgentDone    chan struct{}

	// activeAction and activeActionStartedAt track the current postgres action
	// in progress. Protected by mu. Cleared automatically on Release.
	activeAction          multipoolermanagerdatapb.PostgresAction
	activeActionStartedAt time.Time
}

// NewActionLock creates a new ActionLock.
func NewActionLock() *ActionLock {
	done := make(chan struct{})
	close(done) // no urgent request in flight initially
	return &ActionLock{
		sema:       semaphore.NewWeighted(1),
		nextID:     1, // Start at 1 so 0 can represent "unlocked"
		urgentDone: done,
	}
}

// Acquire acquires the action lock and returns a new context that proves
// ownership. The operation string is used for debugging/tracking purposes.
// Returns an error if the lock cannot be acquired (e.g., context cancelled)
// or if the provided context already holds the lock. While an UrgentAcquire
// call is in flight, Acquire waits for it to finish rather than race it for
// the semaphore (see UrgentAcquire) — a caller whose own ctx expires first
// still gets an error, same as any other wait.
func (al *ActionLock) Acquire(ctx context.Context, operation string) (context.Context, error) {
	return al.acquire(ctx, operation, false)
}

// UrgentAcquire acquires the action lock like Acquire, but first cancels the
// current holder's context so it has a chance to notice and release promptly
// instead of running to completion. It does not itself force the current
// holder to release — code inside the critical section still has to observe
// ctx.Done() (directly, or via something like a query path wired to respect
// it) and unwind on its own; canceling only signals that it should.
//
// It does not need to actively preempt anyone else waiting for the lock: a
// normal Acquire call waits for urgentWaiting to reach 0 before it ever
// tries the semaphore, and rechecks after winning it too (see acquire), so
// even one that was already blocked when this call started gives the lock
// straight back rather than keep it — at which point
// golang.org/x/sync/semaphore's own Release/notifyWaiters hands it to the
// next queued waiter (this call) atomically under its own internal lock.
// There's no gap in that hand-off for a third caller to steal, so nothing
// here needs to track or cancel other waiters itself.
func (al *ActionLock) UrgentAcquire(ctx context.Context, operation string) (context.Context, error) {
	al.mu.Lock()
	if al.urgentWaiting == 0 {
		al.urgentDone = make(chan struct{})
	}
	al.urgentWaiting++
	if al.currentCancel != nil {
		al.currentCancel()
	}
	al.mu.Unlock()

	defer func() {
		al.mu.Lock()
		al.urgentWaiting--
		if al.urgentWaiting == 0 {
			close(al.urgentDone)
		}
		al.mu.Unlock()
	}()

	return al.acquire(ctx, operation, true)
}

func (al *ActionLock) acquire(ctx context.Context, operation string, urgent bool) (context.Context, error) {
	// Check if this context already holds the lock
	if val, ok := ctx.Value(actionLockKey{}).(*actionLockValue); ok {
		if !val.released.Load() {
			return ctx, fmt.Errorf("context already holds the action lock (operation: %s)", val.operation)
		}
	}

	for {
		if !urgent {
			// Wait for any in-flight urgent request(s) to finish rather than
			// race them for the semaphore.
			if err := al.waitForNoUrgent(ctx); err != nil {
				return ctx, mterrors.Wrap(err, "failed to acquire action lock")
			}
		}

		if err := al.sema.Acquire(ctx, 1); err != nil {
			return ctx, mterrors.Wrap(err, "failed to acquire action lock")
		}

		if urgent || !al.hasUrgentWaiting() {
			break
		}

		// Granted despite an UrgentAcquire call arriving after the wait
		// above but before we won the semaphore. Rather than actively track
		// and cancel other waiters, just check again now, and hand it
		// straight back — golang.org/x/sync/semaphore's own
		// Release/notifyWaiters then atomically hands it to the next queued
		// waiter (presumably that urgent request) under its own internal
		// lock, so there's no window for a third caller to steal it in
		// between. Loop back to wait for it to finish too, rather than fail
		// outright.
		al.sema.Release(1)
	}

	return al.grant(ctx, operation), nil
}

// waitForNoUrgent blocks until no UrgentAcquire call is in flight, or ctx is
// done. Loops because urgentDone firing only means the generation waited on
// ended — another urgent request may have started in the meantime.
func (al *ActionLock) waitForNoUrgent(ctx context.Context) error {
	for {
		al.mu.Lock()
		waiting := al.urgentWaiting > 0
		done := al.urgentDone
		al.mu.Unlock()
		if !waiting {
			return nil
		}
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// hasUrgentWaiting reports whether an UrgentAcquire call is currently
// in-flight (waiting on or about to try the semaphore).
func (al *ActionLock) hasUrgentWaiting() bool {
	al.mu.Lock()
	defer al.mu.Unlock()
	return al.urgentWaiting > 0
}

// grant marks the lock held by a new owner derived from ctx and returns the
// context to give them.
func (al *ActionLock) grant(ctx context.Context, operation string) context.Context {
	// Derive a cancelable context so a future UrgentAcquire call can signal
	// this holder to wind down. lockCtx (not ctx) is what gets returned and
	// stored below, so AssertActionLockHeld/Release see the same context a
	// caller would later cancel out from under themselves.
	lockCtx, cancel := context.WithCancel(ctx)

	al.mu.Lock()
	lockID := al.nextID
	al.nextID++
	al.currentID = lockID
	al.currentCancel = cancel
	al.mu.Unlock()

	val := &actionLockValue{
		lockID:    lockID,
		operation: operation,
		released:  &atomic.Bool{},
	}
	return context.WithValue(lockCtx, actionLockKey{}, val)
}

// Release releases the action lock. It validates that the provided context
// holds the lock and panics if it doesn't (which indicates a programming error).
// After releasing, the context is marked as invalid so future assertions will fail.
func (al *ActionLock) Release(ctx context.Context) {
	val, ok := ctx.Value(actionLockKey{}).(*actionLockValue)
	if !ok {
		panic("Release called with context that has no action lock info")
	}

	// Check if already released
	if val.released.Load() {
		panic(fmt.Sprintf("Release called twice with same context (operation: %s)", val.operation))
	}

	// Verify this context holds the current lock
	al.mu.Lock()
	currentID := al.currentID
	al.mu.Unlock()

	if val.lockID != currentID {
		panic(fmt.Sprintf("Release called with context that doesn't hold the lock (operation: %s, lockID: %d, currentID: %d)",
			val.operation, val.lockID, currentID))
	}

	// Mark as released BEFORE actually releasing the semaphore
	// This ensures assertions fail immediately
	val.released.Store(true)

	al.mu.Lock()
	al.currentID = 0
	// Deliberately not calling the stored cancel func here: the context
	// returned to callers is allowed to outlive Release (e.g. reused as the
	// base for a later Acquire, once released — see
	// TestActionLock_AcquireAfterRelease), so canceling it now would leave
	// that context permanently unusable. Just drop the reference; the
	// derived context becomes unreachable and is garbage collected once
	// nothing (goroutines or child contexts) still holds it.
	al.currentCancel = nil
	al.activeAction = multipoolermanagerdatapb.PostgresAction_POSTGRES_ACTION_UNSPECIFIED
	al.activeActionStartedAt = time.Time{}
	al.mu.Unlock()

	al.sema.Release(1)
}

// SetAction records the postgres action currently being performed.
// Must be called while holding the lock (enforced via AssertActionLockHeld).
func (al *ActionLock) SetAction(ctx context.Context, action multipoolermanagerdatapb.PostgresAction) error {
	if err := AssertActionLockHeld(ctx); err != nil {
		return err
	}
	al.mu.Lock()
	defer al.mu.Unlock()
	al.activeAction = action
	al.activeActionStartedAt = time.Now()
	return nil
}

// ActiveAction returns the current postgres action and how long it has been running.
// Returns UNSPECIFIED and zero duration when no action is in progress.
// Safe to call without holding the action lock.
func (al *ActionLock) ActiveAction() (multipoolermanagerdatapb.PostgresAction, time.Duration) {
	al.mu.Lock()
	defer al.mu.Unlock()
	if al.activeAction == multipoolermanagerdatapb.PostgresAction_POSTGRES_ACTION_UNSPECIFIED {
		return multipoolermanagerdatapb.PostgresAction_POSTGRES_ACTION_UNSPECIFIED, 0
	}
	return al.activeAction, time.Since(al.activeActionStartedAt)
}

// AssertActionLockHeld returns an error if the provided context does not hold
// an action lock or if the lock has been released. This is a global function
// that doesn't require a reference to the ActionLock.
//
// TODO: build this on a shared assertion helper in go/tools (e.g. tools/assert)
// for "can't happen" invariants. That helper would (a) increment a metric when
// an assertion fires so we can alert on logic errors that are supposed to be
// impossible, and (b) panic by default under test builds — except for the unit
// tests that explicitly opt out to exercise the lock-not-held path — while
// returning an error in production. AssertActionLockHeld would then be one such
// assertion, giving us loud failures in tests and observable-but-non-fatal
// behavior in production.
func AssertActionLockHeld(ctx context.Context) error {
	val, ok := ctx.Value(actionLockKey{}).(*actionLockValue)
	if !ok {
		return errors.New("context does not hold an action lock")
	}

	if val.released.Load() {
		return errors.New("context's action lock has been released")
	}

	return nil
}
