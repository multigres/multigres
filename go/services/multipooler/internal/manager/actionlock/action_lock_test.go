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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestActionLock_AcquireAndRelease(t *testing.T) {
	lock := NewActionLock()
	ctx := context.Background()

	// Acquire the lock
	newCtx, err := lock.Acquire(ctx, "test-operation")
	require.NoError(t, err)

	// Context should be different
	assert.NotEqual(t, ctx, newCtx)

	// Release the lock
	lock.Release(newCtx)
}

func TestAssertActionLockHeld_WhenHeld(t *testing.T) {
	lock := NewActionLock()
	ctx := context.Background()

	newCtx, err := lock.Acquire(ctx, "test-operation")
	require.NoError(t, err)
	defer lock.Release(newCtx)

	// Assertion should pass
	assert.NoError(t, AssertActionLockHeld(newCtx))
}

func TestAssertActionLockHeld_NoLockInfo(t *testing.T) {
	ctx := context.Background()

	// Assertion should fail for context without lock info
	assert.Error(t, AssertActionLockHeld(ctx))
}

func TestAssertActionLockHeld_AfterRelease(t *testing.T) {
	lock := NewActionLock()
	ctx := context.Background()

	newCtx, err := lock.Acquire(ctx, "test-operation")
	require.NoError(t, err)

	// Release the lock
	lock.Release(newCtx)

	// Assertion should fail after release
	assert.Error(t, AssertActionLockHeld(newCtx))
}

func TestActionLock_ReleasePanicsWithoutLockInfo(t *testing.T) {
	lock := NewActionLock()
	ctx := context.Background()

	// Should panic when releasing context without lock info
	assert.Panics(t, func() {
		lock.Release(ctx)
	})
}

func TestActionLock_ReleasePanicsTwice(t *testing.T) {
	lock := NewActionLock()
	ctx := context.Background()

	newCtx, err := lock.Acquire(ctx, "test-operation")
	require.NoError(t, err)

	// First release should succeed
	lock.Release(newCtx)

	// Second release should panic
	assert.Panics(t, func() {
		lock.Release(newCtx)
	})
}

func TestActionLock_AcquireFailsWhenAlreadyHeld(t *testing.T) {
	lock := NewActionLock()
	ctx := context.Background()

	// First acquire
	newCtx, err := lock.Acquire(ctx, "test-operation-1")
	require.NoError(t, err)
	defer lock.Release(newCtx)

	// Try to acquire again with the context that already holds it
	_, err = lock.Acquire(newCtx, "test-operation-2")
	assert.Error(t, err)
}

func TestActionLock_ContextCancellation(t *testing.T) {
	lock := NewActionLock()
	ctx, cancel := context.WithCancel(context.Background())

	// First acquire the lock with another goroutine
	firstCtx, err := lock.Acquire(context.Background(), "blocking-operation")
	require.NoError(t, err)

	// Try to acquire with cancelled context (should fail immediately)
	cancel()
	_, err = lock.Acquire(ctx, "test-operation")
	assert.Error(t, err)

	// Clean up
	lock.Release(firstCtx)
}

func TestActionLock_ConcurrentAcquire(t *testing.T) {
	lock := NewActionLock()
	const numGoroutines = 10

	var wg sync.WaitGroup
	var mu sync.Mutex
	successCount := 0
	var order []int

	for i := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			ctx := context.Background()
			newCtx, err := lock.Acquire(ctx, fmt.Sprintf("operation-%d", id))
			if !assert.NoError(t, err) {
				return
			}

			// Critical section - should only be accessed by one goroutine at a time
			mu.Lock()
			successCount++
			order = append(order, id)
			mu.Unlock()

			// Hold the lock briefly
			time.Sleep(10 * time.Millisecond)

			lock.Release(newCtx)
		}(i)
	}

	wg.Wait()

	// All goroutines should have successfully acquired the lock
	assert.Equal(t, numGoroutines, successCount)

	// All goroutines should have executed
	assert.Len(t, order, numGoroutines)
}

func TestActionLock_AcquireAfterRelease(t *testing.T) {
	lock := NewActionLock()
	ctx := context.Background()

	// First acquisition
	ctx1, err := lock.Acquire(ctx, "operation-1")
	require.NoError(t, err)
	lock.Release(ctx1)

	// Should be able to acquire again after release
	ctx2, err := lock.Acquire(ctx, "operation-2")
	require.NoError(t, err)
	lock.Release(ctx2)

	// Should be able to acquire again after release
	ctx3, err := lock.Acquire(ctx1, "operation-3")
	require.NoError(t, err)
	lock.Release(ctx3)
}

func TestActionLock_ReleasePanicsWithWrongContext(t *testing.T) {
	lock := NewActionLock()
	ctx := context.Background()

	// Acquire with one operation
	ctx1, err := lock.Acquire(ctx, "operation-1")
	require.NoError(t, err)

	// Acquire again after releasing (to get a different lock ID)
	lock.Release(ctx1)

	ctx2, err := lock.Acquire(ctx, "operation-2")
	require.NoError(t, err)
	defer lock.Release(ctx2)

	// Try to release with old context (should panic)
	assert.Panics(t, func() {
		lock.Release(ctx1)
	})
}

func TestActionLock_UrgentAcquire_LockFree(t *testing.T) {
	lock := NewActionLock()
	ctx := context.Background()

	// With no current holder, UrgentAcquire behaves exactly like Acquire.
	newCtx, err := lock.UrgentAcquire(ctx, "urgent-operation")
	require.NoError(t, err)
	assert.NoError(t, AssertActionLockHeld(newCtx))
	lock.Release(newCtx)
}

func TestActionLock_UrgentAcquire_CancelsCurrentHolder(t *testing.T) {
	lock := NewActionLock()

	// Acquire normally; the returned context is the one UrgentAcquire should cancel.
	holderCtx, err := lock.Acquire(context.Background(), "slow-operation")
	require.NoError(t, err)

	select {
	case <-holderCtx.Done():
		t.Fatal("holder context should not be canceled before an urgent request arrives")
	default:
	}

	// UrgentAcquire blocks until the holder releases, so run it in a
	// goroutine and release the holder shortly after to unblock it.
	urgentDone := make(chan struct{})
	var urgentCtx context.Context
	var urgentErr error
	go func() {
		defer close(urgentDone)
		urgentCtx, urgentErr = lock.UrgentAcquire(context.Background(), "urgent-operation")
	}()

	// The holder's context should be canceled promptly, even though the
	// holder hasn't released yet — that's the signal for it to wind down.
	select {
	case <-holderCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("UrgentAcquire did not cancel the current holder's context")
	}
	assert.Equal(t, context.Canceled, holderCtx.Err())

	// The urgent caller should still be blocked: canceling only signals the
	// holder, it doesn't force the release.
	select {
	case <-urgentDone:
		t.Fatal("UrgentAcquire should still be waiting for the holder to release")
	case <-time.After(50 * time.Millisecond):
	}

	// Once the (canceled) holder actually releases, the urgent caller proceeds.
	lock.Release(holderCtx)

	select {
	case <-urgentDone:
	case <-time.After(time.Second):
		t.Fatal("UrgentAcquire did not proceed after the holder released")
	}
	require.NoError(t, urgentErr)
	assert.NoError(t, AssertActionLockHeld(urgentCtx))
	lock.Release(urgentCtx)
}

func TestActionLock_UrgentAcquire_PreemptsAlreadyWaitingNormalAcquire(t *testing.T) {
	lock := NewActionLock()

	// Hold the lock so a normal Acquire has to wait.
	holderCtx, err := lock.Acquire(context.Background(), "slow-operation")
	require.NoError(t, err)

	// Start a normal Acquire that blocks, well before any urgent request exists.
	waiterStarted := make(chan struct{})
	waiterDone := make(chan struct{})
	var waiterErr error
	go func() {
		close(waiterStarted)
		_, waiterErr = lock.Acquire(context.Background(), "already-waiting-operation")
		close(waiterDone)
	}()
	<-waiterStarted
	time.Sleep(10 * time.Millisecond) // let it actually reach sema.Acquire

	// Now an urgent request arrives. It cancels the current holder, and
	// queues behind the already-waiting normal Acquire above (still ahead
	// of it in the semaphore's own FIFO).
	urgentDone := make(chan struct{})
	var urgentCtx context.Context
	var urgentErr error
	go func() {
		urgentCtx, urgentErr = lock.UrgentAcquire(context.Background(), "urgent-operation")
		close(urgentDone)
	}()

	// The holder's context should have been canceled.
	select {
	case <-holderCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("UrgentAcquire did not cancel the current holder's context")
	}

	// Neither waiter can proceed until the (canceled) holder actually
	// releases — cancellation only signals it to wind down, it doesn't
	// force an immediate release.
	select {
	case <-waiterDone:
		t.Fatal("already-waiting Acquire should still be blocked on the semaphore")
	case <-urgentDone:
		t.Fatal("UrgentAcquire should still be blocked on the semaphore")
	case <-time.After(50 * time.Millisecond):
	}

	lock.Release(holderCtx)

	// The already-waiting normal Acquire wins the semaphore's FIFO first
	// (it queued before the urgent request did), notices the pending
	// urgent request, gives the lock straight back, and loops around to
	// wait for the urgent request to finish entirely — so it does not
	// resolve yet.
	select {
	case <-waiterDone:
		t.Fatal("already-waiting Acquire should have looped back to wait for the urgent request, not finished")
	case <-time.After(50 * time.Millisecond):
	}

	// The urgent caller then gets the lock via the semaphore's own
	// Release/notifyWaiters hand-off — not a resurrected version of the
	// waiter that just gave it back.
	select {
	case <-urgentDone:
	case <-time.After(time.Second):
		t.Fatal("UrgentAcquire did not proceed after the waiter gave the lock back")
	}
	require.NoError(t, urgentErr)
	assert.NoError(t, AssertActionLockHeld(urgentCtx))

	// The waiter is still parked, waiting for the urgent request to finish.
	select {
	case <-waiterDone:
		t.Fatal("already-waiting Acquire should still be waiting for the urgent request to finish")
	case <-time.After(50 * time.Millisecond):
	}

	// Once the urgent caller releases, the waiter finally proceeds.
	lock.Release(urgentCtx)
	select {
	case <-waiterDone:
	case <-time.After(time.Second):
		t.Fatal("already-waiting Acquire did not proceed after the urgent request finished")
	}
	assert.NoError(t, waiterErr)
}

func TestActionLock_Acquire_WaitsThenSucceedsAfterUrgentAcquireFinishes(t *testing.T) {
	lock := NewActionLock()

	// Hold the lock so UrgentAcquire has to wait.
	holderCtx, err := lock.Acquire(context.Background(), "slow-operation")
	require.NoError(t, err)

	urgentStarted := make(chan struct{})
	urgentAcquired := make(chan context.Context, 1)
	go func() {
		close(urgentStarted)
		urgentCtx, err := lock.UrgentAcquire(context.Background(), "urgent-operation")
		require.NoError(t, err)
		urgentAcquired <- urgentCtx
	}()
	<-urgentStarted

	// Give UrgentAcquire a moment to register itself as pending before we
	// race a normal Acquire against it.
	select {
	case <-holderCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("UrgentAcquire never canceled the holder; urgentWaiting may not have been set")
	}

	// A plain Acquire arriving now should wait rather than fail or race the
	// urgent request for the semaphore.
	lateStarted := make(chan struct{})
	lateDone := make(chan struct{})
	var lateErr error
	go func() {
		close(lateStarted)
		_, lateErr = lock.Acquire(context.Background(), "late-normal-operation")
		close(lateDone)
	}()
	<-lateStarted

	select {
	case <-lateDone:
		t.Fatal("Acquire should still be waiting for the urgent request to finish")
	case <-time.After(50 * time.Millisecond):
	}

	// Let the urgent request through; the late normal Acquire is still
	// waiting on urgentWaiting to drop to 0, not on the semaphore directly.
	lock.Release(holderCtx)
	var urgentCtx context.Context
	select {
	case urgentCtx = <-urgentAcquired:
	case <-time.After(time.Second):
		t.Fatal("UrgentAcquire did not proceed after the holder released")
	}

	select {
	case <-lateDone:
		t.Fatal("Acquire should still be waiting while the urgent request holds the lock")
	case <-time.After(50 * time.Millisecond):
	}

	// Once the urgent caller finishes, the waiting normal Acquire proceeds.
	lock.Release(urgentCtx)
	select {
	case <-lateDone:
	case <-time.After(time.Second):
		t.Fatal("Acquire did not proceed after UrgentAcquire finished")
	}
	require.NoError(t, lateErr)
}

func TestActionLock_Acquire_CtxExpiresWhileWaitingForUrgentAcquire(t *testing.T) {
	lock := NewActionLock()

	holderCtx, err := lock.Acquire(context.Background(), "slow-operation")
	require.NoError(t, err)

	urgentStarted := make(chan struct{})
	go func() {
		close(urgentStarted)
		_, _ = lock.UrgentAcquire(context.Background(), "urgent-operation")
	}()
	<-urgentStarted

	select {
	case <-holderCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("UrgentAcquire never canceled the holder; urgentWaiting may not have been set")
	}

	// A normal Acquire whose own ctx expires while waiting for the urgent
	// request to finish should fail with that ctx's error, not hang.
	shortCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = lock.Acquire(shortCtx, "impatient-operation")
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	lock.Release(holderCtx)
}

func TestActionLock_UrgentAcquire_ThenUrgentAcquireAgainCancelsIt(t *testing.T) {
	lock := NewActionLock()

	firstCtx, err := lock.UrgentAcquire(context.Background(), "first-urgent")
	require.NoError(t, err)

	// A second UrgentAcquire cancels whoever currently holds the lock too,
	// even if that holder itself got in via UrgentAcquire.
	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone)
		secondCtx, err := lock.UrgentAcquire(context.Background(), "second-urgent")
		require.NoError(t, err)
		lock.Release(secondCtx)
	}()

	select {
	case <-firstCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("second UrgentAcquire did not cancel the first holder's context")
	}

	lock.Release(firstCtx)

	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("second UrgentAcquire did not proceed after the first holder released")
	}
}

// TestActionLock_UrgentAcquire_RunsBeforeManyQueuedNormalWaiters simulates a
// realistic scenario end to end: a holder that ignores cancellation for a
// while (like a real postgres write that doesn't respect ctx.Done()
// promptly), an UrgentAcquire call trying to preempt it, and a batch of
// ordinary Acquire calls piling up behind the pending urgent request. It
// checks two things at once: none of the queued normal callers ever error
// out (they wait and eventually succeed, per
// TestActionLock_Acquire_WaitsThenSucceedsAfterUrgentAcquireFinishes), and
// the urgent caller is the very first to actually hold the lock once the
// stuck holder finally releases — not merely "not errored", but strictly
// ordered first.
func TestActionLock_UrgentAcquire_RunsBeforeManyQueuedNormalWaiters(t *testing.T) {
	lock := NewActionLock()

	// Holder that will ignore its own canceled context for a while before
	// finally releasing — standing in for a real action whose underlying
	// query doesn't abort promptly on cancellation.
	holderCtx, err := lock.Acquire(context.Background(), "stuck-operation")
	require.NoError(t, err)

	var orderMu sync.Mutex
	var order []string
	recordOrder := func(name string) {
		orderMu.Lock()
		order = append(order, name)
		orderMu.Unlock()
	}

	urgentDone := make(chan struct{})
	go func() {
		defer close(urgentDone)
		urgentCtx, err := lock.UrgentAcquire(context.Background(), "urgent-operation")
		require.NoError(t, err)
		recordOrder("urgent")
		lock.Release(urgentCtx)
	}()

	// Wait for the urgent request to register (it cancels the holder as
	// soon as it starts) before piling on the normal waiters below.
	select {
	case <-holderCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("UrgentAcquire did not cancel the holder")
	}

	const numWaiters = 5
	var wg sync.WaitGroup
	waiterErrs := make([]error, numWaiters)
	for i := range numWaiters {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			waiterCtx, err := lock.Acquire(context.Background(), fmt.Sprintf("normal-operation-%d", i))
			waiterErrs[i] = err
			if err == nil {
				recordOrder(fmt.Sprintf("normal-%d", i))
				lock.Release(waiterCtx)
			}
		}(i)
	}

	// Give every waiter above time to actually reach (and block in) their
	// wait for the urgent request to clear, simulating the stuck holder
	// having "noticed" cancellation but not yet acted on it.
	time.Sleep(20 * time.Millisecond)

	// The holder finally "notices" and releases.
	lock.Release(holderCtx)

	wg.Wait()
	select {
	case <-urgentDone:
	case <-time.After(time.Second):
		t.Fatal("UrgentAcquire never finished")
	}

	for i, err := range waiterErrs {
		assert.NoError(t, err, "normal-operation-%d should have waited and succeeded, not errored", i)
	}
	require.Len(t, order, numWaiters+1)
	assert.Equal(t, "urgent", order[0], "the urgent request must run before every queued normal waiter")
}
