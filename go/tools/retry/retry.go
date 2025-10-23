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
	"time"
)

// Retry manages exponential backoff state for retry loops.
// Use the iterator-style StartAttempt method to implement retry logic.
//
// Example usage:
//
//	r := retry.New(100*time.Millisecond, 30*time.Second)
//	for {
//	    if err := r.StartAttempt(ctx); err != nil {
//	        return err // Context cancelled or timed out
//	    }
//	    result, err := makeAPICall()
//	    if err == nil {
//	        return result // Success!
//	    }
//	    // Will backoff before next attempt
//	}
type Retry struct {
	cfg     retryConfig
	attempt int
	timer   Timer
}

// retryConfig holds the configuration for retry behavior.
type retryConfig struct {
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
	backoff backoff
}

// Option is a functional option for configuring a Retry.
type Option func(*retryConfig)

// WithInitialDelay configures the retry to add a delay before the first attempt.
// Use this when you've already tried once before calling StartAttempt().
func WithInitialDelay() Option {
	return func(c *retryConfig) { c.InitialDelay = true }
}

// New creates a new Retry with the given baseDelay and maxDelay, plus optional configuration.
// Panics if the parameters are invalid (represents a coding error).
//
// The default backoff strategy is exponential backoff with full jitter, which provides
// maximum randomization to prevent thundering herd problems.
//
// Parameters:
//   - baseDelay: Base delay for exponential backoff (delay = baseDelay × 2^attempt)
//   - maxDelay: Maximum delay cap to prevent unbounded growth
func New(baseDelay, maxDelay time.Duration, opts ...Option) *Retry {
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
	cfg := retryConfig{
		BaseDelay: baseDelay,
		MaxDelay:  maxDelay,
		backoff:   newExponentialFullJitterBackoff(baseDelay, maxDelay),
	}

	// Apply optional configuration
	for _, opt := range opts {
		opt(&cfg)
	}

	return &Retry{
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
//	r := retry.New(100*time.Millisecond, 30*time.Second)
//	for {
//	    if err := r.StartAttempt(ctx); err != nil {
//	        return err // Context error
//	    }
//	    // Perform operation
//	    if success {
//	        return nil
//	    }
//	    // continue will backoff on next iteration
//	}
func (r *Retry) StartAttempt(ctx context.Context) error {
	// Check context first
	if err := ctx.Err(); err != nil {
		return err
	}

	// Determine if we should wait before this attempt
	shouldWait := r.attempt > 0 || r.cfg.InitialDelay

	if shouldWait {
		// Calculate delay with backoff strategy
		delay := r.cfg.backoff.nextDelay()

		// Wait for the delay or context cancellation
		select {
		case <-r.timer.After(delay):
			// Delay completed
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Increment attempt counter
	r.attempt++

	return nil
}

// Attempt returns the current attempt number (1-indexed after first StartAttempt call).
// Returns 0 before the first call to StartAttempt.
func (r *Retry) Attempt() int {
	return r.attempt
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
//	r := retry.New(500*time.Millisecond, 30*time.Second)
//	for {
//	    if err := r.StartAttempt(ctx); err != nil {
//	        return err
//	    }
//	    watcher, err := establishWatch()
//	    if err != nil {
//	        continue // Will retry with backoff
//	    }
//
//	    // Start a timer to reset backoff after watch is stable
//	    resetTimer := time.AfterFunc(30*time.Second, func() {
//	        r.Reset() // Reset if watch stays healthy for 30s
//	    })
//	    defer resetTimer.Stop()
//
//	    // Run the watch loop
//	    if err := runWatchLoop(watcher); err != nil {
//	        continue // Will retry with backoff
//	    }
//	}
func (r *Retry) Reset() {
	r.cfg.backoff.reset()
}

// Attempts returns an iterator for range-based retry loops (Go 1.23+).
// Yields (attempt number, error) pairs where error is nil for each retry attempt,
// or non-nil when the context is cancelled/timed out (final iteration).
//
// Example usage:
//
//	r := retry.New(100*time.Millisecond, 30*time.Second)
//	for attempt, err := range r.Attempts(ctx) {
//	    if err != nil {
//	        return fmt.Errorf("retry failed after %d attempts: %w", attempt, err)
//	    }
//
//	    result, err := makeAPICall()
//	    if err == nil {
//	        return result // Success!
//	    }
//	    log.Printf("Attempt %d failed: %v", attempt, err)
//	}
//
// The range form is equivalent to the explicit form:
//
//	for {
//	    if err := r.StartAttempt(ctx); err != nil {
//	        return fmt.Errorf("retry failed after %d attempts: %w", r.Attempt(), err)
//	    }
//	    result, err := makeAPICall()
//	    if err == nil {
//	        return result
//	    }
//	}
func (r *Retry) Attempts(ctx context.Context) func(yield func(int, error) bool) {
	return func(yield func(int, error) bool) {
		for {
			err := r.StartAttempt(ctx)
			if !yield(r.attempt, err) {
				return
			}
		}
	}
}
