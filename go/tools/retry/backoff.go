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
	"math"
	"math/rand/v2"
	"sync"
	"time"
)

// backoff calculates retry delays and manages backoff state.
// Implementations determine the backoff strategy (exponential, linear, constant, etc.).
// Each strategy manages its own configuration and state internally.
//
// Implementations must be thread-safe as reset() may be called from a different
// goroutine than nextDelay().
type backoff interface {
	// nextDelay calculates and returns the next delay, then advances the internal state.
	// Must be thread-safe.
	nextDelay() time.Duration

	// reset resets the backoff state to initial values.
	// Must be thread-safe and safe to call concurrently with nextDelay().
	reset()
}

// exponentialFullJitterBackoff implements exponential backoff with Full Jitter.
//
// This implements the "Full Jitter" algorithm recommended by AWS:
// sleep = random_between(0, min(cap, base * 2^attempt))
//
// Full Jitter provides maximum randomization to prevent thundering herd problems
// where multiple clients retry at the same time, causing synchronized load spikes.
//
// Reference: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
//
// The algorithm works as follows:
//  1. Calculate exponential delay: baseDelay * 2^attempt
//  2. Cap at maxDelay to prevent unbounded growth
//  3. Apply Full Jitter: randomize between 0 and computed delay
//
// Pros: Maximum load spreading, best thundering herd protection
// Cons: Can produce very short delays (close to 0), which may cause rapid retries
//
// Note: For use cases requiring guaranteed minimum delays, consider implementing
// a fractionalJitter strategy in the future.
type exponentialFullJitterBackoff struct {
	baseDelay     time.Duration
	maxDelay      time.Duration
	rng           *rand.Rand
	disableJitter bool // For deterministic testing

	mu      sync.Mutex
	attempt int // Current attempt number (0-indexed), protected by mu
}

// newExponentialFullJitterBackoff creates a new exponential backoff with full jitter.
func newExponentialFullJitterBackoff(baseDelay, maxDelay time.Duration) *exponentialFullJitterBackoff {
	return &exponentialFullJitterBackoff{
		baseDelay: baseDelay,
		maxDelay:  maxDelay,
		rng:       rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(time.Now().UnixNano()))),
	}
}

// newExponentialFullJitterBackoffWithRNG creates a backoff with a specific RNG (for testing).
func newExponentialFullJitterBackoffWithRNG(baseDelay, maxDelay time.Duration, rng *rand.Rand) *exponentialFullJitterBackoff {
	return &exponentialFullJitterBackoff{
		baseDelay: baseDelay,
		maxDelay:  maxDelay,
		rng:       rng,
	}
}

// newExponentialBackoffNoJitter creates a backoff without jitter (for testing).
func newExponentialBackoffNoJitter(baseDelay, maxDelay time.Duration) *exponentialFullJitterBackoff {
	return &exponentialFullJitterBackoff{
		baseDelay:     baseDelay,
		maxDelay:      maxDelay,
		disableJitter: true,
	}
}

// nextDelay calculates the next delay using exponential backoff with full jitter,
// then increments the internal attempt counter.
// Thread-safe: can be called concurrently with reset().
func (e *exponentialFullJitterBackoff) nextDelay() time.Duration {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Exponential backoff: baseDelay * 2^attempt
	// Use bit shifting for precise integer math and overflow protection

	attempt := e.attempt

	// Cap attempt count to prevent overflow (shifting more than 62 bits would overflow int64)
	if attempt > 62 {
		attempt = 62
	}

	// Calculate delay = baseDelay * (1 << attempt)
	// time.Duration is int64, so we can work with it directly
	multiplier := int64(1 << attempt)
	baseDelayInt := int64(e.baseDelay)

	var delay time.Duration
	if baseDelayInt > 0 && multiplier > math.MaxInt64/baseDelayInt {
		// Would overflow, use maxDelay
		delay = e.maxDelay
	} else {
		delay = time.Duration(baseDelayInt * multiplier)
		// Apply max delay cap
		if delay > e.maxDelay {
			delay = e.maxDelay
		}
	}

	// Apply Full Jitter: randomize between 0 and computed delay
	// This prevents synchronized retries by spreading retries across time
	// Note: rand.Rand is not thread-safe, so we call it while holding the mutex
	if !e.disableJitter {
		// random_between(0, delay)
		// rng.Float64() returns [0.0, 1.0)
		delay = time.Duration(float64(delay) * e.rng.Float64())
	}

	// Increment attempt counter for next call
	e.attempt++

	return delay
}

// reset resets the backoff state to initial values.
// Thread-safe: can be called concurrently with nextDelay().
func (e *exponentialFullJitterBackoff) reset() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.attempt = 0
}
