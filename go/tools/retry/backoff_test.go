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
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

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
