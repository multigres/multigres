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
	"math"
	"math/rand/v2"
	"sync"
	"time"
)

// Backoff manages exponential backoff state for retry loops.
// Use the iterator-style StartAttempt method to implement retry logic.
//
// Example usage:
//
//	b := retry.New(100*time.Millisecond, 30*time.Second)
//	for {
//	    if err := b.StartAttempt(ctx); err != nil {
//	        return err // Context cancelled or timed out
//	    }
//	    result, err := makeAPICall()
//	    if err == nil {
//	        return result // Success!
//	    }
//	    // Will backoff before next attempt
//	}
type Backoff struct {
	cfg     backoffConfig
	attempt int
	timer   Timer
}

// backoffConfig holds the configuration for backoff behavior.
type backoffConfig struct {
	// BaseDelay is the base delay for exponential backoff (delay = baseDelay × 2^attempt).
	// With Full Jitter, actual delays range from 0 to the computed delay.
	// Each retry doubles the delay up to MaxDelay.
	// Required.
	BaseDelay time.Duration

	// MaxDelay is the maximum delay between retry attempts.
	// Computed delays exceeding this value will be capped.
	// Required.
	MaxDelay time.Duration

	// InitialDelay adds a delay before the first attempt (attempt 0).
	// Useful when you've already tried once before calling StartAttempt().
	// Default: false (call operation immediately)
	InitialDelay bool

	// backoff strategy for calculating delays between retries.
	// Defaults to exponential backoff with full jitter.
	// Not exported in public API, but accessible for testing.
	backoff backoff
}

// Option is a functional option for configuring a Backoff.
type Option func(*backoffConfig)

// WithInitialDelay configures the backoff to add a delay before the first attempt.
// Use this when you've already tried once before calling StartAttempt().
func WithInitialDelay() Option {
	return func(c *backoffConfig) { c.InitialDelay = true }
}

// New creates a new Backoff with the given baseDelay and maxDelay, plus optional configuration.
// Panics if the parameters are invalid (represents a coding error).
//
// The default backoff strategy is exponential backoff with full jitter, which provides
// maximum randomization to prevent thundering herd problems.
//
// Parameters:
//   - baseDelay: Base delay for exponential backoff (delay = baseDelay × 2^attempt)
//   - maxDelay: Maximum delay cap to prevent unbounded growth
func New(baseDelay, maxDelay time.Duration, opts ...Option) *Backoff {
	// Validate required parameters (panic on coding errors)
	if baseDelay <= 0 {
		panic("retry: BaseDelay must be positive")
	}
	if maxDelay <= 0 {
		panic("retry: MaxDelay must be positive")
	}
	if baseDelay > maxDelay {
		panic("retry: BaseDelay cannot be greater than MaxDelay")
	}

	// Build config with defaults
	cfg := backoffConfig{
		BaseDelay: baseDelay,
		MaxDelay:  maxDelay,
		backoff:   newExponentialFullJitterBackoff(baseDelay, maxDelay),
	}

	// Apply optional configuration
	for _, opt := range opts {
		opt(&cfg)
	}

	return &Backoff{
		cfg:   cfg,
		timer: realTimer{},
	}
}

// StartAttempt prepares for the next retry attempt by waiting for the backoff delay.
// On the first call (attempt 0), it returns immediately unless WithInitialDelay was configured.
// On subsequent calls, it waits for the exponentially increasing backoff delay.
//
// Returns:
//   - nil if the caller should proceed with the next attempt
//   - ctx.Err() if the context was cancelled or timed out during the wait
//
// Usage pattern:
//
//	b := retry.New(100*time.Millisecond, 30*time.Second)
//	for {
//	    if err := b.StartAttempt(ctx); err != nil {
//	        return err // Context error
//	    }
//	    // Perform operation
//	    if success {
//	        return nil
//	    }
//	    // continue will backoff on next iteration
//	}
func (b *Backoff) StartAttempt(ctx context.Context) error {
	// Check context first
	if err := ctx.Err(); err != nil {
		return err
	}

	// Determine if we should wait before this attempt
	shouldWait := b.attempt > 0 || b.cfg.InitialDelay

	if shouldWait {
		// Calculate delay with backoff strategy
		delay := b.cfg.backoff.nextDelay()

		// Wait for the delay or context cancellation
		select {
		case <-b.timer.After(delay):
			// Delay completed
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Increment attempt counter
	b.attempt++

	return nil
}

// Attempt returns the current attempt number (1-indexed after first StartAttempt call).
// Returns 0 before the first call to StartAttempt.
func (b *Backoff) Attempt() int {
	return b.attempt
}

// Reset resets the backoff state to the initial delay.
// Use this when you've determined the system is healthy and future errors
// should start from the minimum backoff.
//
// This is useful for infinite retry loops where a long-running stable connection
// indicates system health. For example, after successfully establishing a watch
// that remains stable for some time, you can reset the backoff so that if a new
// error occurs later, it starts with minimal delay rather than a large backoff.
//
// Note: Reset only affects the backoff calculation. The attempt counter returned
// by Attempt() is never reset and continues to increment monotonically.
//
// Example:
//
//	b := retry.New(500*time.Millisecond, 30*time.Second)
//	for {
//	    if err := b.StartAttempt(ctx); err != nil {
//	        return err
//	    }
//	    watcher, err := establishWatch()
//	    if err != nil {
//	        continue // Will retry with backoff
//	    }
//
//	    // Start a timer to reset backoff after watch is stable
//	    resetTimer := time.AfterFunc(30*time.Second, func() {
//	        b.Reset() // Reset if watch stays healthy for 30s
//	    })
//	    defer resetTimer.Stop()
//
//	    // Run the watch loop
//	    if err := runWatchLoop(watcher); err != nil {
//	        continue // Will retry with backoff
//	    }
//	}
func (b *Backoff) Reset() {
	b.cfg.backoff.reset()
}

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

// Future backoff strategies to consider implementing:
//
// 1. decorrelatedJitterBackoff:
//    Formula: sleep = min(cap, random_between(base, prev_sleep * 3))
//    Best for: When you want some randomization but prefer smoother retry patterns
//    Examples:
//    - User-facing retries where UX benefits from more predictable timing
//    - Scenarios where previous delay provides useful signal about system state
//    Pros: Maintains some dependency on previous delay, can feel more "natural"
//    Cons: Has clamping issues that reduce jitter effectiveness over time
//    Note: Would require a stateful interface or passing prevDelay parameter
//
// 2. fractionalJitterBackoff (also called "Equal Jitter" when fraction=0.5):
//    Formula: sleep = delay * (1-fraction) + random_between(0, delay * fraction)
//    Examples: fraction=0.5: sleep = delay/2 + random_between(0, delay/2)
//              fraction=0.2: sleep = 80% of delay + random_between(0, 20% of delay)
//    Best for: Latency-sensitive scenarios where delays must never get too short
//    Examples:
//    - OLTP query retries where sub-millisecond retries could overwhelm the database
//    - Rate limiting scenarios where you need guaranteed minimum spacing between requests
//    - Retrying latency-sensitive operations where very short delays are counterproductive
//    Pros: Configurable minimum delay (1-fraction), more predictable latency bounds
//    Cons: Less randomization than Full Jitter, less effective at preventing thundering herd
//
// 3. constantBackoff:
//    Formula: sleep = baseDelay (always)
//    Best for: Simple scenarios where exponential growth isn't needed
//
// 4. linearBackoff:
//    Formula: sleep = min(cap, base * attempt)
//    Best for: Gradual increase without exponential growth
