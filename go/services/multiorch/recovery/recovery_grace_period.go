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
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

const (
	// maxAllowedJitter is the maximum jitter duration we'll allow, regardless of config.
	// This prevents misconfiguration and keeps jitter calculations simple.
	maxAllowedJitter = 1 * time.Minute
)

// gracePeriodKey uniquely identifies a grace period tracking entry.
// Uses (ProblemCode, PoolerID) to track independently per pooler.
type gracePeriodKey struct {
	code     types.ProblemCode
	poolerID string
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
	ctx    context.Context
	config *config.Config
	logger *slog.Logger

	mu        sync.Mutex
	deadlines map[gracePeriodKey]time.Time
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

// WithLogger sets a custom logger for the tracker.
func WithLogger(logger *slog.Logger) RecoveryGracePeriodTrackerOption {
	return func(dt *RecoveryGracePeriodTracker) {
		dt.logger = logger
	}
}

// NewRecoveryGracePeriodTracker creates a new deadline tracker.
// By default, uses a random seed for jitter generation and slog.Default() for logging.
func NewRecoveryGracePeriodTracker(ctx context.Context, config *config.Config, opts ...RecoveryGracePeriodTrackerOption) *RecoveryGracePeriodTracker {
	dt := &RecoveryGracePeriodTracker{
		ctx:       ctx,
		config:    config,
		logger:    slog.Default(),
		deadlines: make(map[gracePeriodKey]time.Time),
		rng:       rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(time.Now().UnixNano()))),
	}

	for _, opt := range opts {
		opt(dt)
	}

	return dt
}

// calculateDeadline computes a new deadline with base + jitter for the given grace period config.
// Must be called while holding dt.mu lock.
func (dt *RecoveryGracePeriodTracker) calculateDeadline(cfg types.GracePeriodConfig) time.Time {
	base := cfg.BaseDelay
	maxJitter := cfg.MaxJitter

	// Clamp to reasonable bounds
	maxJitter = max(0, min(maxJitter, maxAllowedJitter))

	var jitter time.Duration
	if maxJitter > 0 {
		// Use [0, maxJitter) range (exclusive upper bound)
		jitter = time.Duration(dt.rng.Int64N(int64(maxJitter)))
	}

	return time.Now().Add(base + jitter)
}

// Observe records the health state of a problem type for a specific pooler.
// This should be called every recovery cycle for each (pooler, analyzer) combination.
//
// If isHealthy is true: resets the deadline to now + (base + jitter), with fresh jitter
// If isHealthy is false: freezes the deadline (countdown continues)
//
// If the action doesn't require grace period tracking, this is a noop.
func (dt *RecoveryGracePeriodTracker) Observe(code types.ProblemCode, poolerID string, action types.RecoveryAction, isHealthy bool) {
	dt.mu.Lock()
	defer dt.mu.Unlock()

	// Get grace period config from the action
	gracePeriodCfg := action.GracePeriod()
	if gracePeriodCfg == nil {
		// Action doesn't require grace period tracking
		return
	}

	key := gracePeriodKey{code: code, poolerID: poolerID}
	_, exists := dt.deadlines[key]

	if isHealthy {
		// Reset deadline with fresh jitter
		dt.deadlines[key] = dt.calculateDeadline(*gracePeriodCfg)
	} else if !exists {
		// First time seeing this problem unhealthy - initialize deadline with base + jitter
		dt.deadlines[key] = dt.calculateDeadline(*gracePeriodCfg)
	}
	// If unhealthy and exists, freeze (do nothing - deadline unchanged)
}

// ShouldExecute checks if recovery action should execute for this problem.
// Returns true if action should execute (deadline expired or no grace period needed).
// Returns false if still within grace period window (should wait longer).
//
// This assumes Observe() has already been called for the (problem type, pooler) combination.
// If the action doesn't require grace period tracking, returns true (execute immediately).
func (dt *RecoveryGracePeriodTracker) ShouldExecute(problem types.Problem) bool {
	dt.mu.Lock()
	defer dt.mu.Unlock()

	// Get grace period config from the action
	gracePeriodCfg := problem.RecoveryAction.GracePeriod()
	if gracePeriodCfg == nil {
		// Action doesn't require grace period tracking - execute immediately
		return true
	}

	// Use pooler ID from the problem to look up the deadline
	if problem.PoolerID == nil {
		dt.logger.WarnContext(dt.ctx, "Cannot check grace period: problem missing pooler ID",
			"problem_code", problem.Code)
		return false
	}
	poolerID := topoclient.MultiPoolerIDString(problem.PoolerID)

	key := gracePeriodKey{code: problem.Code, poolerID: poolerID}
	deadline, exists := dt.deadlines[key]
	if !exists {
		// Problem has grace period but no deadline - this is unexpected
		// Observe() should have been called before ShouldExecute()
		dt.logger.WarnContext(dt.ctx, "Grace period deadline not found, skipping recovery",
			"problem_code", problem.Code,
			"pooler_id", poolerID)
		return false
	}

	// Check if deadline has expired
	now := time.Now()
	if now.After(deadline) || now.Equal(deadline) {
		return true
	}

	// Deadline not reached yet - log that we're deferring
	timeRemaining := deadline.Sub(now)
	dt.logger.InfoContext(dt.ctx, "Deferring recovery action, waiting for grace period to expire",
		"problem_code", problem.Code,
		"time_remaining_seconds", timeRemaining.Seconds(),
		"deadline", deadline,
	)
	return false
}
