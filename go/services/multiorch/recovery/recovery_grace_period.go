// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package recovery

import (
	"math/rand/v2"
	"sync"
	"time"

	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

const (
	// maxAllowedJitter is the maximum jitter duration we'll allow, regardless of config.
	// This prevents misconfiguration and keeps jitter calculations simple.
	maxAllowedJitter = 1 * time.Minute
)

// problemTiming tracks the deadline and jitter for a specific problem.
type problemTiming struct {
	deadline       time.Time     // When action can execute if problem persists
	jitterDuration time.Duration // Calculated once, reused on each reset
}

// RecoveryGracePeriodTracker tracks grace periods for recovery actions.
// It implements a deadline-based model where:
// - While healthy: deadline continuously resets to now + (base + jitter)
// - Problem detected: deadline stops updating, counts down to expiry
// - Action executes only after deadline expires
//
// Thread safety: All methods are safe for concurrent use. The internal rand.Rand
// is protected by the mutex and only accessed while holding a write lock.
type RecoveryGracePeriodTracker struct {
	config *config.Config

	mu        sync.Mutex
	deadlines map[types.ProblemCode]*problemTiming
	rng       *rand.Rand // Protected by mu - only accessed during Lock()
}

// RecoveryGracePeriodTrackerOption configures the deadline tracker.
type RecoveryGracePeriodTrackerOption func(*RecoveryGracePeriodTracker)

// WithRand sets a custom random generator for jitter generation.
// Useful for deterministic testing with a fixed seed.
func WithRand(rng *rand.Rand) RecoveryGracePeriodTrackerOption {
	return func(dt *RecoveryGracePeriodTracker) {
		dt.rng = rng
	}
}

// NewRecoveryGracePeriodTracker creates a new deadline tracker.
// By default, uses a random seed for jitter generation.
func NewRecoveryGracePeriodTracker(config *config.Config, opts ...RecoveryGracePeriodTrackerOption) *RecoveryGracePeriodTracker {
	dt := &RecoveryGracePeriodTracker{
		config:    config,
		deadlines: make(map[types.ProblemCode]*problemTiming),
		rng:       rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(time.Now().UnixNano()))),
	}

	for _, opt := range opts {
		opt(dt)
	}

	return dt
}

// Observe records the health state of a problem type.
// This should be called every recovery cycle for tracked problem types.
//
// If isHealthy is true: resets the deadline to now + (base + jitter)
// If isHealthy is false: freezes the deadline (countdown continues)
//
// On first call, calculates and stores jitter for this problem type.
// If the problem type doesn't require deadline tracking, this is a noop.
func (dt *RecoveryGracePeriodTracker) Observe(code types.ProblemCode, isHealthy bool) {
	// Only track deadlines for PrimaryIsDead initially
	if code != types.ProblemPrimaryIsDead {
		return
	}

	dt.mu.Lock()
	defer dt.mu.Unlock()

	timing, exists := dt.deadlines[code]
	if !exists {
		// First time seeing this problem type - calculate jitter once
		maxJitter := dt.config.GetPrimaryElectionTimeoutMaxJitter()

		// Clamp to reasonable bounds
		maxJitter = max(0, min(maxJitter, maxAllowedJitter))

		var jitter time.Duration
		if maxJitter > 0 {
			// Use [0, maxJitter) range (exclusive upper bound)
			jitter = time.Duration(dt.rng.Int64N(int64(maxJitter)))
		}

		timing = &problemTiming{
			jitterDuration: jitter,
		}
		dt.deadlines[code] = timing
	}

	// If healthy, reset deadline. If unhealthy, freeze (deadline unchanged).
	if isHealthy {
		base := dt.config.GetPrimaryElectionTimeoutBase()
		timing.deadline = time.Now().Add(base + timing.jitterDuration)
	}
}

// ShouldExecute checks if recovery action should execute for this problem.
// Returns true if action should execute (deadline expired or no deadline tracking).
// Returns false if still within deadline window (should wait longer).
//
// This assumes Observe() has already been called for the problem type.
// If the problem type doesn't require deadline tracking, returns true (execute immediately).
func (dt *RecoveryGracePeriodTracker) ShouldExecute(problem types.Problem) bool {
	// Only check deadlines for PrimaryIsDead - other problems execute immediately
	if problem.Code != types.ProblemPrimaryIsDead {
		return true
	}

	dt.mu.Lock()
	defer dt.mu.Unlock()

	timing, exists := dt.deadlines[problem.Code]
	if !exists {
		// No deadline tracked yet - allow immediate action
		// (This shouldn't happen in normal flow, but be safe)
		return true
	}

	// Check if deadline has expired
	return time.Now().After(timing.deadline) || time.Now().Equal(timing.deadline)
}

// Delete removes the deadline entry for a specific problem type.
// Typically not needed as we have a small, bounded number of problem types.
func (dt *RecoveryGracePeriodTracker) Delete(code types.ProblemCode) {
	dt.mu.Lock()
	defer dt.mu.Unlock()

	delete(dt.deadlines, code)
}
