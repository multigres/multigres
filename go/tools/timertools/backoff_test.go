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

package timertools

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewBackoffTicker_InvalidParams(t *testing.T) {
	// Test zero initial interval
	assert.Panics(t, func() {
		NewBackoffTicker(0, time.Second)
	}, "Should panic with zero initial interval")

	// Test negative initial interval
	assert.Panics(t, func() {
		NewBackoffTicker(-time.Millisecond, time.Second)
	}, "Should panic with negative initial interval")

	// Test max interval less than initial interval
	assert.Panics(t, func() {
		NewBackoffTicker(time.Second, 500*time.Millisecond)
	}, "Should panic when max < initial interval")
}

func TestNewBackoffTicker_ValidParams(t *testing.T) {
	initial := 10 * time.Millisecond
	max := 100 * time.Millisecond

	ticker := NewBackoffTicker(initial, max)
	require.NotNil(t, ticker, "Ticker should be created")
	assert.NotNil(t, ticker.C, "Channel should be initialized")
	assert.Equal(t, initial, ticker.CurrentInterval(), "Should start with initial interval")

	ticker.Stop()
}

func TestBackoffTicker_BasicTicking(t *testing.T) {
	initial := 5 * time.Millisecond
	max := 50 * time.Millisecond
	ticker := NewBackoffTicker(initial, max)
	defer ticker.Stop()

	// Wait for first tick
	select {
	case tick := <-ticker.C:
		assert.WithinDuration(t, time.Now(), tick, 20*time.Millisecond, "First tick should arrive quickly")
	case <-time.After(50 * time.Millisecond):
		require.Fail(t, "First tick should arrive within 50ms")
	}

	// Verify interval has doubled (within jitter bounds)
	currentInterval := ticker.CurrentInterval()
	expectedMin := time.Duration(float64(initial*2) * 0.9) // Account for jitter
	expectedMax := time.Duration(float64(initial*2) * 1.1)
	assert.True(t, currentInterval >= expectedMin && currentInterval <= expectedMax,
		"Interval should have doubled with jitter: got %v, expected range [%v, %v]",
		currentInterval, expectedMin, expectedMax)
}

func TestBackoffTicker_ExponentialBackoff(t *testing.T) {
	initial := 2 * time.Millisecond
	max := 32 * time.Millisecond
	ticker := NewBackoffTicker(initial, max)
	defer ticker.Stop()

	expectedIntervals := []time.Duration{
		2 * time.Millisecond,  // Initial
		4 * time.Millisecond,  // 2 * 2
		8 * time.Millisecond,  // 4 * 2
		16 * time.Millisecond, // 8 * 2
		32 * time.Millisecond, // 16 * 2 (capped at max)
		32 * time.Millisecond, // Should stay at max
	}

	// Check initial interval
	assert.Equal(t, expectedIntervals[0], ticker.CurrentInterval(), "Should start with initial interval")

	// Wait for several ticks and verify exponential growth
	for i := 1; i < len(expectedIntervals); i++ {
		select {
		case <-ticker.C:
			currentInterval := ticker.CurrentInterval()
			expected := expectedIntervals[i]

			// Allow for 10% jitter in our expectations
			minExpected := time.Duration(float64(expected) * 0.9)
			maxExpected := time.Duration(float64(expected) * 1.1)

			assert.True(t, currentInterval >= minExpected && currentInterval <= maxExpected,
				"Tick %d: interval should be ~%v (with jitter), got %v", i, expected, currentInterval)
		case <-time.After(100 * time.Millisecond):
			require.Fail(t, "Tick %d should arrive within 100ms", i)
		}
	}
}

func TestBackoffTicker_MaxInterval(t *testing.T) {
	initial := 1 * time.Millisecond
	max := 10 * time.Millisecond
	ticker := NewBackoffTicker(initial, max)
	defer ticker.Stop()

	// Wait for enough ticks to exceed max interval
	for i := range 8 {
		select {
		case <-ticker.C:
			currentInterval := ticker.CurrentInterval()
			maxWithJitter := time.Duration(float64(max) * 1.1) // Account for jitter
			assert.LessOrEqual(t, currentInterval, maxWithJitter,
				"Interval should never exceed max (with jitter): got %v, max %v", currentInterval, maxWithJitter)
		case <-time.After(50 * time.Millisecond):
			require.Fail(t, "Tick %d should arrive within 50ms", i)
		}
	}
}

func TestBackoffTicker_Jitter(t *testing.T) {
	initial := 50 * time.Millisecond // Increased for more measurable jitter
	max := 500 * time.Millisecond

	// Create multiple tickers and measure their first tick timing
	numTickers := 20 // Increased number of tickers
	tickTimes := make([]time.Duration, numTickers)
	var wg sync.WaitGroup
	startTime := time.Now()

	for i := range numTickers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ticker := NewBackoffTicker(initial, max)
			defer ticker.Stop()

			select {
			case tickTime := <-ticker.C:
				tickTimes[idx] = tickTime.Sub(startTime)
			case <-time.After(200 * time.Millisecond):
				// Timeout case
			}
		}(i)
	}

	// Wait for all tickers to complete
	wg.Wait()

	// Verify that we have jitter by checking the range of tick times
	validTimes := make([]time.Duration, 0, numTickers)
	for _, tickTime := range tickTimes {
		if tickTime > 0 {
			validTimes = append(validTimes, tickTime)
		}
	}

	require.Greater(t, len(validTimes), numTickers/2, "Should have received most ticks")

	// Calculate min and max tick times
	if len(validTimes) > 1 {
		minTime := validTimes[0]
		maxTime := validTimes[0]
		for _, t := range validTimes {
			if t < minTime {
				minTime = t
			}
			if t > maxTime {
				maxTime = t
			}
		}

		// With 10% jitter, we should see at least 5ms variation for 50ms base interval
		timeDiff := maxTime - minTime
		expectedMinVariation := 5 * time.Millisecond
		assert.GreaterOrEqual(t, timeDiff, expectedMinVariation,
			"Jitter should cause at least %v variation in tick timing, got %v (min: %v, max: %v)",
			expectedMinVariation, timeDiff, minTime, maxTime)
	}
}

func TestBackoffTicker_Stop(t *testing.T) {
	initial := 5 * time.Millisecond
	max := 50 * time.Millisecond
	ticker := NewBackoffTicker(initial, max)

	// Wait for first tick
	select {
	case <-ticker.C:
		// Good, got first tick
	case <-time.After(50 * time.Millisecond):
		require.Fail(t, "Should receive first tick")
	}

	// Stop the ticker
	ticker.Stop()

	// Wait and verify no more ticks arrive
	select {
	case <-ticker.C:
		t.Error("Should not receive tick after Stop()")
	case <-time.After(100 * time.Millisecond):
		// Good, no tick received
	}

	// Multiple calls to Stop() should be safe
	ticker.Stop()
}

func TestBackoffTicker_Reset(t *testing.T) {
	initial := 2 * time.Millisecond
	max := 20 * time.Millisecond
	ticker := NewBackoffTicker(initial, max)
	defer ticker.Stop()

	// Wait for a few ticks to increase the interval
	for i := range 3 {
		select {
		case <-ticker.C:
		case <-time.After(50 * time.Millisecond):
			require.Fail(t, "Should receive tick %d", i)
		}
	}

	// Interval should be larger than initial now
	intervalBeforeReset := ticker.CurrentInterval()
	expectedMinBeforeReset := time.Duration(float64(initial*4) * 0.9) // After 3 ticks: 2->4->8->16
	assert.GreaterOrEqual(t, intervalBeforeReset, expectedMinBeforeReset,
		"Interval should have grown before reset")

	// Reset the ticker
	ticker.Reset()

	// Interval should be back to initial
	intervalAfterReset := ticker.CurrentInterval()
	assert.Equal(t, initial, intervalAfterReset, "Interval should reset to initial value")
}

func TestBackoffTicker_ResetAfterStop(t *testing.T) {
	initial := 5 * time.Millisecond
	max := 50 * time.Millisecond
	ticker := NewBackoffTicker(initial, max)

	ticker.Stop()

	// Reset after stop should not panic and should not restart ticking
	ticker.Reset()

	// Verify no ticks arrive
	select {
	case <-ticker.C:
		t.Error("Should not receive tick after stop and reset")
	case <-time.After(50 * time.Millisecond):
		// Good, no tick received
	}
}

func TestBackoffTicker_ChannelBuffering(t *testing.T) {
	initial := 1 * time.Millisecond
	max := 10 * time.Millisecond
	ticker := NewBackoffTicker(initial, max)
	defer ticker.Stop()

	// Don't read from channel immediately, let ticks accumulate
	time.Sleep(20 * time.Millisecond)

	// Channel should only have one tick (it's buffered with size 1)
	// The ticker implementation uses a non-blocking send, so if the channel
	// is full, subsequent ticks are dropped
	tickCount := 0

	// Drain the channel quickly
	for {
		select {
		case <-ticker.C:
			tickCount++
		default:
			// No more ticks available immediately
			goto done
		}
	}
done:

	// Should have at least 1 tick buffered, but due to the non-blocking nature
	// and timing variations, we might see 1-2 ticks depending on timing
	assert.GreaterOrEqual(t, tickCount, 1, "Channel should have at least 1 tick")
	assert.LessOrEqual(t, tickCount, 2, "Channel should not accumulate many ticks due to non-blocking send")
}
