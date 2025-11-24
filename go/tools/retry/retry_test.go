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

package retry

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test infrastructure

// fakeTimer is a deterministic timer for testing that completes immediately.
type fakeTimer struct {
	delays []time.Duration
}

func (f *fakeTimer) After(d time.Duration) <-chan time.Time {
	f.delays = append(f.delays, d)
	ch := make(chan time.Time, 1)
	ch <- time.Now() // Complete immediately
	return ch
}

// fakeBackoff returns predetermined delays for testing backoff logic in isolation.
type fakeBackoff struct {
	delays       []time.Duration
	attempt      int
	nextDelayNum int   // Count of nextDelay() calls
	resetsAt     []int // Track after which nextDelay call reset() was called
}

func (f *fakeBackoff) nextDelay() time.Duration {
	var delay time.Duration
	if f.attempt < len(f.delays) {
		delay = f.delays[f.attempt]
	} else if len(f.delays) > 0 {
		// Return the last delay for attempts beyond the predetermined list
		delay = f.delays[len(f.delays)-1]
	} else {
		// Default fallback
		delay = 1 * time.Second
	}
	f.attempt++
	f.nextDelayNum++
	return delay
}

func (f *fakeBackoff) reset() {
	// Record that reset was called after this many nextDelay() calls
	f.resetsAt = append(f.resetsAt, f.nextDelayNum)
	f.attempt = 0
}

// withBackoff is a test-only option to set a custom backoff strategy.
func withBackoff(b backoff) Option {
	return func(c *retryConfig) { c.backoff = b }
}

// newRetryWithFakeBackoff creates a retry with predetermined backoff delays.
func newRetryWithFakeBackoff(delays []time.Duration, opts ...Option) (*Retry, *fakeTimer, *fakeBackoff) {
	fb := &fakeBackoff{delays: delays}
	allOpts := append([]Option{withBackoff(fb)}, opts...)
	// Use dummy values for baseDelay/maxDelay - they're only used for validation
	r := New(1*time.Millisecond, 1*time.Minute, allOpts...)
	ft := &fakeTimer{}
	r.timer = ft
	return r, ft, fb
}

// Tests for New constructor

func TestNew_CreatesRetry(t *testing.T) {
	r := New(500*time.Millisecond, time.Minute)
	assert.Equal(t, 500*time.Millisecond, r.cfg.BaseDelay)
	assert.Equal(t, time.Minute, r.cfg.MaxDelay)
	assert.NotNil(t, r.cfg.backoff, "backoff strategy should be set")
	assert.IsType(t, &exponentialFullJitterBackoff{}, r.cfg.backoff, "should use exponential full jitter by default")
	assert.Equal(t, 0, r.attempt, "should start at attempt 0")
}

func TestNew_PanicsOnInvalidConfig(t *testing.T) {
	tests := []struct {
		name      string
		baseDelay time.Duration
		maxDelay  time.Duration
		opts      []Option
		panics    bool
	}{
		{
			name:      "negative BaseDelay",
			baseDelay: -1 * time.Second,
			maxDelay:  time.Minute,
			panics:    true,
		},
		{
			name:      "zero BaseDelay",
			baseDelay: 0,
			maxDelay:  time.Minute,
			panics:    true,
		},
		{
			name:      "negative MaxDelay",
			baseDelay: time.Second,
			maxDelay:  -1 * time.Minute,
			panics:    true,
		},
		{
			name:      "zero MaxDelay",
			baseDelay: time.Second,
			maxDelay:  0,
			panics:    true,
		},
		{
			name:      "BaseDelay greater than MaxDelay",
			baseDelay: time.Minute,
			maxDelay:  time.Second,
			panics:    true,
		},
		{
			name:      "valid config with InitialDelay",
			baseDelay: time.Second,
			maxDelay:  time.Minute,
			opts:      []Option{WithInitialDelay()},
			panics:    false,
		},
		{
			name:      "valid basic config",
			baseDelay: time.Second,
			maxDelay:  time.Minute,
			panics:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.panics {
				assert.Panics(t, func() {
					New(tt.baseDelay, tt.maxDelay, tt.opts...)
				})
			} else {
				assert.NotPanics(t, func() {
					r := New(tt.baseDelay, tt.maxDelay, tt.opts...)
					assert.NotNil(t, r)
				})
			}
		})
	}
}

// Tests for startAttempt method (internal)

func TestRetry_startAttempt_FirstAttemptNoDelay(t *testing.T) {
	delays := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond}
	r, ft, _ := newRetryWithFakeBackoff(delays)

	// First call should return immediately without waiting
	err := r.startAttempt(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 1, r.attempt)
	assert.Empty(t, ft.delays, "first attempt should not wait")

	// Second call should wait with backoff
	err = r.startAttempt(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 2, r.attempt)
	require.Len(t, ft.delays, 1)
	assert.Equal(t, delays[0], ft.delays[0])

	// Third call should wait with larger backoff
	err = r.startAttempt(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 3, r.attempt)
	require.Len(t, ft.delays, 2)
	assert.Equal(t, delays[1], ft.delays[1])
}

func TestRetry_startAttempt_WithInitialDelay(t *testing.T) {
	delays := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond}
	r, ft, _ := newRetryWithFakeBackoff(delays, WithInitialDelay())

	// First call should wait when WithInitialDelay is set
	err := r.startAttempt(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 1, r.attempt)
	require.Len(t, ft.delays, 1)
	assert.Equal(t, delays[0], ft.delays[0])

	// Second call should also wait
	err = r.startAttempt(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 2, r.attempt)
	require.Len(t, ft.delays, 2)
	assert.Equal(t, delays[1], ft.delays[1])
}

func TestRetry_startAttempt_ContextCancelled(t *testing.T) {
	delays := []time.Duration{10 * time.Millisecond}
	r, _, _ := newRetryWithFakeBackoff(delays)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// First attempt should check context and return error
	err := r.startAttempt(ctx)
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 0, r.attempt, "should not increment attempt on context error")
}

func TestRetry_startAttempt_ContextCancelledDuringWait(t *testing.T) {
	// Use real timer for this test since we need actual timing
	r := New(10*time.Millisecond, time.Minute, withBackoff(newExponentialBackoffNoJitter(10*time.Millisecond, time.Minute)))

	ctx, cancel := context.WithCancel(context.Background())

	// First attempt succeeds
	err := r.startAttempt(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, r.attempt)

	// Cancel context then try second attempt
	cancel()
	err = r.startAttempt(ctx)
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	// Attempt should still be 1 since we didn't complete the second attempt
	assert.Equal(t, 1, r.attempt)
}

// Tests for Reset method

func TestRetry_Reset(t *testing.T) {
	delays := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 10 * time.Millisecond}
	r, _, fb := newRetryWithFakeBackoff(delays)
	ctx := context.Background()

	// Make a few attempts
	require.NoError(t, r.startAttempt(ctx)) // No wait (first attempt), attempt 1
	require.NoError(t, r.startAttempt(ctx)) // Wait 10ms (calls nextDelay #1), attempt 2
	require.NoError(t, r.startAttempt(ctx)) // Wait 20ms (calls nextDelay #2), attempt 3

	// Reset backoff
	r.Reset()

	// Next attempt should use first delay again
	require.NoError(t, r.startAttempt(ctx)) // Wait 10ms (calls nextDelay #3, after reset), attempt 4

	// Verify reset was called after 2 nextDelay() calls
	// (nextDelay called for attempts 2 and 3, then reset, then nextDelay called for attempt 4)
	require.Len(t, fb.resetsAt, 1)
	assert.Equal(t, 2, fb.resetsAt[0], "reset called after 2nd nextDelay()")

	// Verify Attempt() counter is NOT reset (monotonic)
	assert.Equal(t, 4, r.attempt)
}

// Integration tests

func TestRetry_IntegrationExample(t *testing.T) {
	r := New(100*time.Millisecond, 30*time.Second, withBackoff(newExponentialBackoffNoJitter(100*time.Millisecond, 30*time.Second)))
	ctx := context.Background()

	for attempt, err := range r.Attempts(ctx) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Simulate success on 3rd attempt
		if attempt == 3 {
			break
		}
	}

	assert.Equal(t, 3, r.attempt)
}

func TestRetry_IntegrationWithContextTimeout(t *testing.T) {
	r := New(10*time.Millisecond, time.Second, withBackoff(newExponentialBackoffNoJitter(10*time.Millisecond, time.Second)))
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	var lastErr error
	for _, err := range r.Attempts(ctx) {
		if err != nil {
			lastErr = err
			break
		}

		// Simulate operation that always fails
		// Will eventually timeout
	}

	assert.Error(t, lastErr)
	assert.True(t, errors.Is(lastErr, context.DeadlineExceeded) || errors.Is(lastErr, context.Canceled))
	assert.Greater(t, r.attempt, 0, "should have made at least one attempt")
}

// Example demonstrates basic backoff usage with exponential backoff and full jitter.
func Example() {
	r := New(500*time.Millisecond, 30*time.Second)
	ctx := context.Background()

	for _, err := range r.Attempts(ctx) {
		if err != nil {
			// Handle context cancellation/timeout
			return
		}

		// Your operation here
		result, err := makeAPICall()
		if err == nil {
			// Success!
			_ = result
			return
		}

		// Will retry with exponential backoff on next iteration
	}
}

// Example_withTimeout shows time-bounded retry with context timeout.
func Example_withTimeout() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	r := New(100*time.Millisecond, 5*time.Second)

	for _, err := range r.Attempts(ctx) {
		if err != nil {
			// Timeout reached or context cancelled
			if errors.Is(err, context.DeadlineExceeded) {
				// Handle timeout
			}
			return
		}

		// Operation will retry until success or 30s timeout
		_, err := makeAPICall()
		if err == nil {
			return // Success
		}
	}
}

// Mock function for examples
func makeAPICall() (any, error) {
	return nil, nil
}

func TestRetry_Attempts_SuccessfulAttempts(t *testing.T) {
	r := New(10*time.Millisecond, 100*time.Millisecond)
	r.timer = &fakeTimer{}

	ctx := context.Background()
	attemptCount := 0

	for attempt, err := range r.Attempts(ctx) {
		if err != nil {
			t.Fatalf("Expected nil error, got %v", err)
		}

		attemptCount++
		assert.Equal(t, attemptCount, attempt, "Attempt number should match iteration count")

		if attemptCount == 3 {
			break // Success after 3 attempts
		}
	}

	assert.Equal(t, 3, attemptCount, "Should have completed 3 attempts")
}

func TestRetry_Attempts_ContextCancelled(t *testing.T) {
	r := New(10*time.Millisecond, 100*time.Millisecond)
	r.timer = &fakeTimer{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for attempt, err := range r.Attempts(ctx) {
		if attempt == 3 {
			cancel() // Cancel after 3 attempts
		}

		if err != nil {
			assert.ErrorIs(t, err, context.Canceled, "Expected context.Canceled error")
			assert.Equal(t, 3, attempt, "Should have completed 3 attempts")
			return
		}
	}

	t.Fatal("Expected context cancellation error, but loop ended without error")
}

func TestRetry_Attempts_ContextTimeout(t *testing.T) {
	r := New(10*time.Millisecond, 100*time.Millisecond)
	r.timer = &fakeTimer{}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	var lastAttempt int

	for attempt, err := range r.Attempts(ctx) {
		lastAttempt = attempt

		if err != nil {
			assert.ErrorIs(t, err, context.DeadlineExceeded, "Expected context.DeadlineExceeded error")
			assert.Greater(t, attempt, 0, "Should have at least one attempt")
			return
		}

		// Simulate some work
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("Expected timeout error after %d attempts, but loop ended without error", lastAttempt)
}

func TestRetry_Attempts_EarlyBreak(t *testing.T) {
	ft := &fakeTimer{}
	r := New(10*time.Millisecond, 100*time.Millisecond)
	r.timer = ft

	ctx := context.Background()
	attemptCount := 0

	for _, err := range r.Attempts(ctx) {
		if err != nil {
			t.Fatalf("Expected nil error, got %v", err)
		}

		attemptCount++
		if attemptCount == 2 {
			break // Early exit
		}
	}

	assert.Equal(t, 2, attemptCount, "Should stop after 2 attempts")
	assert.Len(t, ft.delays, 1, "Should have 1 delay (first attempt has no delay)")
}

func TestRetry_Attempts_WithInitialDelay(t *testing.T) {
	ft := &fakeTimer{}
	r := New(10*time.Millisecond, 100*time.Millisecond, WithInitialDelay())
	r.timer = ft

	ctx := context.Background()
	attemptCount := 0

	for _, err := range r.Attempts(ctx) {
		if err != nil {
			t.Fatalf("Expected nil error, got %v", err)
		}

		attemptCount++
		if attemptCount == 2 {
			break
		}
	}

	// With InitialDelay, even the first attempt should have a delay
	assert.Len(t, ft.delays, 2, "Should have delays for both attempts including the first")
}
