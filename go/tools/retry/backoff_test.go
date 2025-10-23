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
	"math/rand/v2"
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
	return func(c *backoffConfig) { c.backoff = b }
}

// newBackoffWithFakeBackoff creates a backoff with predetermined backoff delays.
func newBackoffWithFakeBackoff(delays []time.Duration, opts ...Option) (*Backoff, *fakeTimer, *fakeBackoff) {
	fb := &fakeBackoff{delays: delays}
	allOpts := append([]Option{withBackoff(fb)}, opts...)
	// Use dummy values for baseDelay/maxDelay - they're only used for validation
	b := New(1*time.Millisecond, 1*time.Minute, allOpts...)
	ft := &fakeTimer{}
	b.timer = ft
	return b, ft, fb
}

// testSeed represents a pair of seed values for deterministic random number generation.
type testSeed struct {
	s1, s2 uint64
}

// Test seeds for deterministic jitter testing.
var (
	// seed1x1 produces Float64() ≈ 0.340286, giving ~34% of max delay
	seed1x1 = testSeed{1, 1}
	// seed2x2 produces Float64() ≈ 0.078291, giving ~8% of max delay (low value)
	seed2x2 = testSeed{2, 2}
)

// multiDelay calls nextDelay() on the backoff n+1 times to advance to attempt n,
// returning the delay for attempt n.
func multiDelay(b backoff, attempt int) time.Duration {
	var delay time.Duration
	for i := 0; i <= attempt; i++ {
		delay = b.nextDelay()
	}
	return delay
}

// Jitter test values
const (
	jitter_seed1x1_10ms   = 3402859 * time.Nanosecond
	jitter_seed1x1_100ms  = 34028597 * time.Nanosecond
	jitter_seed2x2_100ms  = 7829106 * time.Nanosecond
	jitter_seed1x1_200ms  = 181991587 * time.Nanosecond
	jitter_seed1x1_200ms2 = 165756971 * time.Nanosecond
	jitter_seed1x1_150ms  = 142925804 * time.Nanosecond
)

// Tests for New constructor

func TestNew_CreatesBackoff(t *testing.T) {
	b := New(500*time.Millisecond, time.Minute)
	assert.Equal(t, 500*time.Millisecond, b.cfg.BaseDelay)
	assert.Equal(t, time.Minute, b.cfg.MaxDelay)
	assert.NotNil(t, b.cfg.backoff, "backoff strategy should be set")
	assert.IsType(t, &exponentialFullJitterBackoff{}, b.cfg.backoff, "should use exponential full jitter by default")
	assert.Equal(t, 0, b.Attempt(), "should start at attempt 0")
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
					b := New(tt.baseDelay, tt.maxDelay, tt.opts...)
					assert.NotNil(t, b)
				})
			}
		})
	}
}

// Tests for StartAttempt method

func TestBackoff_StartAttempt_FirstAttemptNoDelay(t *testing.T) {
	delays := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond}
	b, ft, _ := newBackoffWithFakeBackoff(delays)

	// First call should return immediately without waiting
	err := b.StartAttempt(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 1, b.Attempt())
	assert.Empty(t, ft.delays, "first attempt should not wait")

	// Second call should wait with backoff
	err = b.StartAttempt(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 2, b.Attempt())
	require.Len(t, ft.delays, 1)
	assert.Equal(t, delays[0], ft.delays[0])

	// Third call should wait with larger backoff
	err = b.StartAttempt(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 3, b.Attempt())
	require.Len(t, ft.delays, 2)
	assert.Equal(t, delays[1], ft.delays[1])
}

func TestBackoff_StartAttempt_WithInitialDelay(t *testing.T) {
	delays := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond}
	b, ft, _ := newBackoffWithFakeBackoff(delays, WithInitialDelay())

	// First call should wait when WithInitialDelay is set
	err := b.StartAttempt(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 1, b.Attempt())
	require.Len(t, ft.delays, 1)
	assert.Equal(t, delays[0], ft.delays[0])

	// Second call should also wait
	err = b.StartAttempt(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 2, b.Attempt())
	require.Len(t, ft.delays, 2)
	assert.Equal(t, delays[1], ft.delays[1])
}

func TestBackoff_StartAttempt_ContextCancelled(t *testing.T) {
	delays := []time.Duration{10 * time.Millisecond}
	b, _, _ := newBackoffWithFakeBackoff(delays)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// First attempt should check context and return error
	err := b.StartAttempt(ctx)
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 0, b.Attempt(), "should not increment attempt on context error")
}

func TestBackoff_StartAttempt_ContextCancelledDuringWait(t *testing.T) {
	// Use real timer for this test since we need actual timing
	b := New(10*time.Millisecond, time.Minute, withBackoff(newExponentialBackoffNoJitter(10*time.Millisecond, time.Minute)))

	ctx, cancel := context.WithCancel(context.Background())

	// First attempt succeeds
	err := b.StartAttempt(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, b.Attempt())

	// Cancel context then try second attempt (which would wait)
	cancel()
	err = b.StartAttempt(ctx)
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	// Attempt should still be 1 since we didn't complete the second attempt
	assert.Equal(t, 1, b.Attempt())
}

func TestBackoff_StartAttempt_ContextTimeout(t *testing.T) {
	delays := []time.Duration{10 * time.Millisecond}
	b, _, _ := newBackoffWithFakeBackoff(delays)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	// Manually expire the context
	cancel()

	err := b.StartAttempt(ctx)
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 0, b.Attempt())
}

// Tests for Reset method

func TestBackoff_Reset(t *testing.T) {
	delays := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 10 * time.Millisecond}
	b, ft, fb := newBackoffWithFakeBackoff(delays)
	ctx := context.Background()

	// Make a few attempts
	require.NoError(t, b.StartAttempt(ctx)) // No wait (first attempt), attempt 1
	require.NoError(t, b.StartAttempt(ctx)) // Wait 10ms (calls nextDelay #1), attempt 2
	require.NoError(t, b.StartAttempt(ctx)) // Wait 20ms (calls nextDelay #2), attempt 3

	require.Len(t, ft.delays, 2)
	assert.Equal(t, delays[0], ft.delays[0])
	assert.Equal(t, delays[1], ft.delays[1])

	// Reset backoff
	b.Reset()

	// Next attempt should use first delay again
	require.NoError(t, b.StartAttempt(ctx)) // Wait 10ms (calls nextDelay #3, after reset), attempt 4
	require.Len(t, ft.delays, 3)
	assert.Equal(t, delays[2], ft.delays[2], "after reset, should use first delay")

	// Verify reset was called after 2 nextDelay() calls
	// (nextDelay called for attempts 2 and 3, then reset, then nextDelay called for attempt 4)
	require.Len(t, fb.resetsAt, 1)
	assert.Equal(t, 2, fb.resetsAt[0], "reset called after 2nd nextDelay()")

	// Verify Attempt() counter is NOT reset (monotonic)
	assert.Equal(t, 4, b.Attempt())
}

// Tests for Attempt method

func TestBackoff_Attempt(t *testing.T) {
	b := New(100*time.Millisecond, 30*time.Second)
	ctx := context.Background()

	assert.Equal(t, 0, b.Attempt(), "should start at 0")

	require.NoError(t, b.StartAttempt(ctx))
	assert.Equal(t, 1, b.Attempt())

	require.NoError(t, b.StartAttempt(ctx))
	assert.Equal(t, 2, b.Attempt())

	b.Reset()
	assert.Equal(t, 2, b.Attempt(), "Reset() should not affect Attempt() counter")

	require.NoError(t, b.StartAttempt(ctx))
	assert.Equal(t, 3, b.Attempt())
}

// Tests for backoff strategy implementations

func TestCalculateDelay(t *testing.T) {
	tests := []struct {
		name       string
		baseDelay  time.Duration
		maxDelay   time.Duration
		attempt    int
		withJitter bool
		seed       testSeed
		expected   time.Duration
	}{
		{
			name:       "first attempt no jitter",
			baseDelay:  10 * time.Millisecond,
			maxDelay:   time.Minute,
			attempt:    0,
			withJitter: false,
			expected:   10 * time.Millisecond,
		},
		{
			name:       "second attempt no jitter",
			baseDelay:  10 * time.Millisecond,
			maxDelay:   time.Minute,
			attempt:    1,
			withJitter: false,
			expected:   20 * time.Millisecond,
		},
		{
			name:       "third attempt no jitter",
			baseDelay:  10 * time.Millisecond,
			maxDelay:   time.Minute,
			attempt:    2,
			withJitter: false,
			expected:   40 * time.Millisecond,
		},
		{
			name:       "with max delay cap",
			baseDelay:  10 * time.Millisecond,
			maxDelay:   30 * time.Millisecond,
			attempt:    5,
			withJitter: false,
			expected:   30 * time.Millisecond,
		},
		{
			name:       "with full jitter seed1x1",
			baseDelay:  100 * time.Millisecond,
			maxDelay:   time.Minute,
			attempt:    0,
			withJitter: true,
			seed:       seed1x1,
			expected:   jitter_seed1x1_100ms,
		},
		{
			name:       "with full jitter seed2x2 (low value)",
			baseDelay:  100 * time.Millisecond,
			maxDelay:   time.Minute,
			attempt:    0,
			withJitter: true,
			seed:       seed2x2,
			expected:   jitter_seed2x2_100ms,
		},
		{
			name:       "jitter on second attempt",
			baseDelay:  100 * time.Millisecond,
			maxDelay:   time.Minute,
			attempt:    1,
			withJitter: true,
			seed:       seed1x1,
			expected:   jitter_seed1x1_200ms, // 100ms * 2^1 = 200ms base
		},
		{
			name:       "jitter on third attempt",
			baseDelay:  50 * time.Millisecond,
			maxDelay:   time.Minute,
			attempt:    2,
			withJitter: true,
			seed:       seed1x1,
			expected:   jitter_seed1x1_200ms2, // 50ms * 2^2 = 200ms base
		},
		{
			name:       "jitter with max delay cap",
			baseDelay:  100 * time.Millisecond,
			maxDelay:   150 * time.Millisecond,
			attempt:    5,
			withJitter: true,
			seed:       seed1x1,
			expected:   jitter_seed1x1_150ms, // 100ms * 2^5 = 3200ms, capped to 150ms
		},
		{
			name:       "jitter with small delays",
			baseDelay:  10 * time.Millisecond,
			maxDelay:   time.Minute,
			attempt:    0,
			withJitter: true,
			seed:       seed1x1,
			expected:   jitter_seed1x1_10ms,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var b backoff
			if tt.withJitter {
				b = newExponentialFullJitterBackoffWithRNG(tt.baseDelay, tt.maxDelay, rand.New(rand.NewPCG(tt.seed.s1, tt.seed.s2)))
			} else {
				b = newExponentialBackoffNoJitter(tt.baseDelay, tt.maxDelay)
			}

			delay := multiDelay(b, tt.attempt)
			assert.Equal(t, tt.expected, delay)
		})
	}
}

func TestCalculateDelay_ExtremeAttemptCounts(t *testing.T) {
	tests := []struct {
		name          string
		baseDelay     time.Duration
		maxDelay      time.Duration
		attempts      int
		expectedDelay time.Duration
	}{
		{
			name:          "attempt 100 with 1s min, 1m max - should cap at max",
			baseDelay:     time.Second,
			maxDelay:      time.Minute,
			attempts:      100,
			expectedDelay: time.Minute,
		},
		{
			name:          "attempt 1000 with 1s min, 1m max - should cap at max",
			baseDelay:     time.Second,
			maxDelay:      time.Minute,
			attempts:      1000,
			expectedDelay: time.Minute,
		},
		{
			name:          "attempt 50 with 1ms min, 1h max - should cap due to overflow protection",
			baseDelay:     time.Millisecond,
			maxDelay:      time.Hour,
			attempts:      50,
			expectedDelay: time.Hour,
		},
		{
			name:          "attempt 10 with 1s min, 1h max - no overflow, precise calculation",
			baseDelay:     time.Second,
			maxDelay:      time.Hour,
			attempts:      10,
			expectedDelay: 1024 * time.Second, // 2^10 = 1024
		},
		{
			name:          "attempt 63 triggers overflow protection cap",
			baseDelay:     time.Second,
			maxDelay:      time.Hour,
			attempts:      63,
			expectedDelay: time.Hour,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := newExponentialBackoffNoJitter(tt.baseDelay, tt.maxDelay)

			var delay time.Duration
			assert.NotPanics(t, func() {
				delay = multiDelay(b, tt.attempts)
			})

			assert.Equal(t, tt.expectedDelay, delay)
			assert.GreaterOrEqual(t, delay, time.Duration(0))
			assert.LessOrEqual(t, delay, tt.maxDelay)
		})
	}
}

func TestCalculateDelay_JitterVariesAroundTarget(t *testing.T) {
	tests := []struct {
		name        string
		baseDelay   time.Duration
		maxDelay    time.Duration
		attempts    int
		expectedMin time.Duration
		expectedMax time.Duration
	}{
		{
			name:        "full jitter at MinDelay",
			baseDelay:   100 * time.Millisecond,
			maxDelay:    time.Minute,
			attempts:    0,
			expectedMin: 0,
			expectedMax: 100 * time.Millisecond,
		},
		{
			name:        "full jitter at MaxDelay cap",
			baseDelay:   10 * time.Millisecond,
			maxDelay:    50 * time.Millisecond,
			attempts:    3, // 10 * 2^3 = 80ms, capped to 50ms
			expectedMin: 0,
			expectedMax: 50 * time.Millisecond,
		},
		{
			name:        "full jitter at high attempts",
			baseDelay:   time.Second,
			maxDelay:    10 * time.Second,
			attempts:    5, // Would be 32s without cap, capped to 10s
			expectedMin: 0,
			expectedMax: 10 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := newExponentialFullJitterBackoff(tt.baseDelay, tt.maxDelay)

			delay := multiDelay(b, tt.attempts)
			assert.GreaterOrEqual(t, delay, tt.expectedMin)
			assert.LessOrEqual(t, delay, tt.expectedMax)
		})
	}
}

func TestBackoff_Reset_OnBackoffStrategy(t *testing.T) {
	b := newExponentialBackoffNoJitter(10*time.Millisecond, time.Minute)

	delay1 := b.nextDelay()
	assert.Equal(t, 10*time.Millisecond, delay1)

	delay2 := b.nextDelay()
	assert.Equal(t, 20*time.Millisecond, delay2)

	b.reset()

	delay3 := b.nextDelay()
	assert.Equal(t, 10*time.Millisecond, delay3)

	delay4 := b.nextDelay()
	assert.Equal(t, 20*time.Millisecond, delay4)
}

// Integration tests

func TestBackoff_IntegrationExample(t *testing.T) {
	b := New(100*time.Millisecond, 30*time.Second, withBackoff(newExponentialBackoffNoJitter(100*time.Millisecond, 30*time.Second)))
	ctx := context.Background()

	attempts := 0
	for {
		if err := b.StartAttempt(ctx); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		attempts++

		// Simulate success on 3rd attempt
		if attempts == 3 {
			break
		}
	}

	assert.Equal(t, 3, attempts)
	assert.Equal(t, 3, b.Attempt())
}

func TestBackoff_IntegrationWithContextTimeout(t *testing.T) {
	b := New(10*time.Millisecond, time.Second, withBackoff(newExponentialBackoffNoJitter(10*time.Millisecond, time.Second)))
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	attempts := 0
	var lastErr error
	for {
		if err := b.StartAttempt(ctx); err != nil {
			lastErr = err
			break
		}

		attempts++

		// Simulate operation that always fails
		// Will eventually timeout
	}

	assert.Error(t, lastErr)
	assert.True(t, errors.Is(lastErr, context.DeadlineExceeded) || errors.Is(lastErr, context.Canceled))
	assert.Greater(t, attempts, 0, "should have made at least one attempt")
}

// Example demonstrates basic backoff usage with exponential backoff and full jitter.
func Example() {
	b := New(500*time.Millisecond, 30*time.Second)
	ctx := context.Background()

	for {
		if err := b.StartAttempt(ctx); err != nil {
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

	b := New(100*time.Millisecond, 5*time.Second)

	for {
		if err := b.StartAttempt(ctx); err != nil {
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
func makeAPICall() (interface{}, error) {
	return nil, nil
}
