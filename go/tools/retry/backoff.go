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

// ExponentialBackoff exposes the package's exponential-backoff-with-full-jitter
// calculation for callers that manage their own timing — e.g. rate-limiting an
// action across independent scheduling cycles — rather than driving a Retry loop.
// Call NextDelay each time you act to get the next delay (it advances the
// sequence), and Reset after a success to start over. Safe for use by a single
// owner; NextDelay/Reset are individually thread-safe.
type ExponentialBackoff struct {
	inner *exponentialFullJitterBackoff
}

// NewExponentialBackoff returns an ExponentialBackoff with the given base and max
// delays. Delays are random in [0, min(maxDelay, baseDelay*2^attempt)].
//
// TODO: support configurable jitter strategies (e.g. equal/fractional jitter)
// rather than only full jitter. Full jitter can return a near-zero delay, so
// callers that need a guaranteed minimum gap between attempts currently have to
// floor the result themselves (see recordDivergenceRewindAttempt). An "equal
// jitter" option (delay/2 + random(0, delay/2)) would give that floor natively.
func NewExponentialBackoff(baseDelay, maxDelay time.Duration) *ExponentialBackoff {
	return &ExponentialBackoff{inner: newExponentialFullJitterBackoff(baseDelay, maxDelay)}
}

// NextDelay returns the next delay and advances the backoff state.
func (b *ExponentialBackoff) NextDelay() time.Duration { return b.inner.nextDelay() }

// Reset restarts the backoff sequence (e.g. after a successful action).
func (b *ExponentialBackoff) Reset() { b.inner.reset() }

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

	delay := ExponentialBackoff(e.baseDelay, e.maxDelay, e.attempt)

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

// ExponentialBackoff returns the exponential backoff magnitude baseDelay *
// 2^attempt, clamped to maxDelay, with overflow protection. attempt is
// zero-indexed (attempt 0 → baseDelay). It applies no jitter: callers layer their
// own jitter strategy on top — Full Jitter here (nextDelay), or the deterministic
// fractional jitter in go/common/ha, which needs a guaranteed minimum delay that
// Full Jitter cannot provide.
func ExponentialBackoff(baseDelay, maxDelay time.Duration, attempt int) time.Duration {
	// Cap attempt to prevent overflow (shifting more than 62 bits overflows int64).
	multiplier := int64(1 << min(attempt, 62))
	baseDelayInt := int64(baseDelay)
	if baseDelayInt > 0 && multiplier > math.MaxInt64/baseDelayInt {
		return maxDelay
	}
	return min(time.Duration(baseDelayInt*multiplier), maxDelay)
}

// reset resets the backoff state to initial values.
// Thread-safe: can be called concurrently with nextDelay().
func (e *exponentialFullJitterBackoff) reset() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.attempt = 0
}
