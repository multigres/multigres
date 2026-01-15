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

package timer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPeriodicRunnerStartStop(t *testing.T) {
	called := make(chan struct{}, 10)

	runner := NewPeriodicRunner(t.Context(), 1*time.Millisecond)
	assert.False(t, runner.Running())

	runner.Start(func(_ context.Context) {
		select {
		case called <- struct{}{}:
		default:
		}
	}, nil)
	assert.True(t, runner.Running())

	// Wait for at least one execution
	<-called

	runner.Stop()
	assert.False(t, runner.Running())
}

func TestPeriodicRunnerStopWaitsForInFlight(t *testing.T) {
	callbackStarted := make(chan struct{})
	callbackCanProceed := make(chan struct{})
	callbackDone := make(chan struct{})

	runner := NewPeriodicRunner(t.Context(), 1*time.Millisecond)
	runner.Start(func(_ context.Context) {
		select {
		case <-callbackStarted:
			// Already signaled
		default:
			close(callbackStarted)
		}
		<-callbackCanProceed
		select {
		case <-callbackDone:
			// Already signaled
		default:
			close(callbackDone)
		}
	}, nil)

	// Wait for callback to start
	<-callbackStarted

	// Start Stop in background - it should block until callback completes
	stopDone := make(chan struct{})
	go func() {
		runner.Stop()
		close(stopDone)
	}()

	// Verify Stop is blocked (callback hasn't completed yet)
	select {
	case <-stopDone:
		t.Fatal("Stop returned before callback completed")
	default:
		// Good - Stop is waiting
	}

	// Let callback proceed
	close(callbackCanProceed)

	// Wait for callback to complete
	<-callbackDone

	// Now Stop should return
	<-stopDone
}

func TestPeriodicRunnerContextCancellation(t *testing.T) {
	callbackStarted := make(chan struct{})
	ctxDone := make(chan struct{})

	runner := NewPeriodicRunner(t.Context(), 1*time.Millisecond)
	runner.Start(func(ctx context.Context) {
		select {
		case <-callbackStarted:
		default:
			close(callbackStarted)
		}
		// Wait for context to be cancelled
		<-ctx.Done()
		select {
		case <-ctxDone:
		default:
			close(ctxDone)
		}
	}, nil)

	// Wait for callback to start
	<-callbackStarted

	// Stop should cancel the context, allowing callback to complete
	runner.Stop()

	// Context should have been cancelled
	<-ctxDone
}

func TestPeriodicRunnerIdempotentStart(t *testing.T) {
	firstCallbackCalled := make(chan struct{})
	secondCallbackCalled := make(chan struct{})

	runner := NewPeriodicRunner(t.Context(), 1*time.Millisecond)

	runner.Start(func(_ context.Context) {
		select {
		case <-firstCallbackCalled:
		default:
			close(firstCallbackCalled)
		}
	}, nil)

	// Second Start should be a no-op
	runner.Start(func(_ context.Context) {
		select {
		case <-secondCallbackCalled:
		default:
			close(secondCallbackCalled)
		}
	}, nil)

	// Wait for first callback
	<-firstCallbackCalled

	runner.Stop()

	// Second callback should never have been called
	select {
	case <-secondCallbackCalled:
		t.Fatal("second Start should have been ignored")
	default:
		// Good
	}
}

func TestPeriodicRunnerIdempotentStop(t *testing.T) {
	runner := NewPeriodicRunner(t.Context(), 1*time.Millisecond)
	runner.Start(func(_ context.Context) {}, nil)

	// Multiple stops should not panic
	runner.Stop()
	runner.Stop()
	runner.Stop()

	assert.False(t, runner.Running())
}

func TestPeriodicRunnerRestart(t *testing.T) {
	var calls atomic.Int64
	called := make(chan struct{}, 100)

	runner := NewPeriodicRunner(t.Context(), 1*time.Millisecond)

	// First run
	runner.Start(func(_ context.Context) {
		calls.Add(1)
		select {
		case called <- struct{}{}:
		default:
		}
	}, nil)

	// Wait for at least one call
	<-called
	runner.Stop()

	firstRunCalls := calls.Load()
	require.GreaterOrEqual(t, firstRunCalls, int64(1))

	// Restart
	runner.Start(func(_ context.Context) {
		calls.Add(1)
		select {
		case called <- struct{}{}:
		default:
		}
	}, nil)

	// Wait for at least one more call
	<-called
	runner.Stop()

	// Should have more calls after restart
	assert.Greater(t, calls.Load(), firstRunCalls)
}

func TestPeriodicRunnerBackpressure(t *testing.T) {
	var concurrency atomic.Int32
	var maxConcurrency atomic.Int32
	executed := make(chan struct{}, 100)
	canProceed := make(chan struct{})

	runner := NewPeriodicRunner(t.Context(), 1*time.Millisecond)
	runner.Start(func(ctx context.Context) {
		current := concurrency.Add(1)
		// Track max concurrency
		for {
			old := maxConcurrency.Load()
			if current <= old || maxConcurrency.CompareAndSwap(old, current) {
				break
			}
		}

		select {
		case executed <- struct{}{}:
		default:
		}

		// Wait until test allows us to proceed or context cancelled
		select {
		case <-canProceed:
		case <-ctx.Done():
		}

		concurrency.Add(-1)
	}, nil)

	// Wait for first callback to start
	<-executed

	// Signal second callback would be ready but should be blocked due to backpressure
	// Let first callback complete
	close(canProceed)

	// Wait for a couple more executions
	<-executed
	<-executed

	runner.Stop()

	// With backpressure, max concurrency should be 1
	assert.Equal(t, int32(1), maxConcurrency.Load(), "callbacks should not run concurrently")
}

func TestPeriodicRunnerStopWithoutStart(t *testing.T) {
	runner := NewPeriodicRunner(t.Context(), 1*time.Millisecond)

	// Stop without Start should not panic
	runner.Stop()

	assert.False(t, runner.Running())
}

func TestPeriodicRunnerStartReturnValue(t *testing.T) {
	called := make(chan struct{}, 10)
	runner := NewPeriodicRunner(t.Context(), 1*time.Millisecond)

	// First Start should return true
	started := runner.Start(func(_ context.Context) {
		select {
		case called <- struct{}{}:
		default:
		}
	}, nil)
	assert.True(t, started, "first Start should return true")

	// Wait for callback to execute to ensure runner is running
	<-called

	// Second Start should return false (already running)
	started = runner.Start(func(_ context.Context) {}, nil)
	assert.False(t, started, "second Start should return false")

	runner.Stop()
}

func TestPeriodicRunnerOnStartCallback(t *testing.T) {
	onStartCalled := make(chan struct{})
	callbackStarted := make(chan struct{})

	runner := NewPeriodicRunner(t.Context(), 1*time.Millisecond)

	runner.Start(func(_ context.Context) {
		select {
		case <-callbackStarted:
		default:
			close(callbackStarted)
		}
	}, func() {
		close(onStartCalled)
	})

	// onStart should be called before any callback executes
	select {
	case <-onStartCalled:
		// Good - onStart was called
	case <-callbackStarted:
		t.Fatal("callback executed before onStart")
	}

	// Wait for callback to also execute
	<-callbackStarted

	runner.Stop()
}

func TestPeriodicRunnerOnStartNotCalledWhenAlreadyRunning(t *testing.T) {
	called := make(chan struct{}, 10)
	var onStartCount atomic.Int32

	runner := NewPeriodicRunner(t.Context(), 1*time.Millisecond)

	// First Start with onStart
	runner.Start(func(_ context.Context) {
		select {
		case called <- struct{}{}:
		default:
		}
	}, func() {
		onStartCount.Add(1)
	})

	// Wait for callback
	<-called

	// Second Start should not call onStart
	runner.Start(func(_ context.Context) {}, func() {
		onStartCount.Add(1)
	})

	runner.Stop()

	assert.Equal(t, int32(1), onStartCount.Load(), "onStart should only be called once")
}

// TestPeriodicRunnerConcurrentStartStop tests that Start() called during Stop() waits correctly.
// This test exercises the race condition where Start() might be called while Stop() is waiting
// for in-flight callbacks, which could cause WaitGroup violations if not handled correctly.
func TestPeriodicRunnerConcurrentStartStop(t *testing.T) {
	callbackStarted := make(chan struct{})
	callbackCanProceed := make(chan struct{})

	runner := NewPeriodicRunner(t.Context(), 1*time.Millisecond)

	// Start runner with a callback that blocks
	runner.Start(func(_ context.Context) {
		select {
		case <-callbackStarted:
		default:
			close(callbackStarted)
		}
		<-callbackCanProceed
	}, nil)

	// Wait for callback to start
	<-callbackStarted

	// Start Stop() in background - it will block on wg.Wait()
	stopDone := make(chan struct{})
	go func() {
		runner.Stop()
		close(stopDone)
	}()

	// Give Stop() time to enter wg.Wait()
	time.Sleep(10 * time.Millisecond)

	// Try to Start() while Stop() is waiting - this should block until Stop() completes
	startDone := make(chan struct{})
	secondCallbackCalled := make(chan struct{}, 1)
	go func() {
		runner.Start(func(_ context.Context) {
			select {
			case secondCallbackCalled <- struct{}{}:
			default:
			}
		}, nil)
		close(startDone)
	}()

	// Verify Start() is blocked (Stop() hasn't completed yet)
	select {
	case <-startDone:
		t.Fatal("Start should be blocked while Stop is in progress")
	case <-time.After(10 * time.Millisecond):
		// Good - Start is waiting
	}

	// Let first callback complete
	close(callbackCanProceed)

	// Wait for Stop to complete
	<-stopDone

	// Now Start should complete
	<-startDone

	// Verify the second callback runs
	<-secondCallbackCalled

	runner.Stop()
}

// TestPeriodicRunnerStopsOnParentContextCancellation verifies that the runner
// automatically stops when the parent context is cancelled.
func TestPeriodicRunnerStopsOnParentContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	firstCallbackStarted := make(chan struct{})
	firstCallbackCanFinish := make(chan struct{})
	firstCallbackDone := make(chan struct{})
	secondCallbackStarted := make(chan struct{})

	var callCount atomic.Int32

	runner := NewPeriodicRunner(ctx, 1*time.Millisecond)
	runner.Start(func(_ context.Context) {
		count := callCount.Add(1)
		if count == 1 {
			// First callback - signal that we've started
			close(firstCallbackStarted)
			// Cancel the parent context while callback is running
			cancel()
			// Wait for test to let us proceed
			<-firstCallbackCanFinish
			close(firstCallbackDone)
		} else {
			// Second or later callback - should not happen
			select {
			case secondCallbackStarted <- struct{}{}:
			default:
			}
		}
	}, nil)

	// Wait for first callback to start
	<-firstCallbackStarted
	require.True(t, runner.Running())

	// Let first callback complete
	close(firstCallbackCanFinish)
	<-firstCallbackDone

	// Runner should stop automatically after first callback completes
	require.Eventually(t, func() bool {
		return !runner.Running()
	}, 100*time.Millisecond, 1*time.Millisecond, "runner should stop when parent context is cancelled")

	// Verify only one callback ran
	assert.Equal(t, int32(1), callCount.Load(), "only the first callback should have run")

	// Verify no second callback was scheduled
	select {
	case <-secondCallbackStarted:
		t.Fatal("second callback should not have been scheduled after context cancellation")
	case <-time.After(20 * time.Millisecond):
		// Good - no second callback
	}
}
