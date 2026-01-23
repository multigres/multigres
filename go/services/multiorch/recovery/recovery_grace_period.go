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

	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

const (
	// maxAllowedJitter is the maximum jitter duration we'll allow, regardless of config.
	// This prevents misconfiguration and keeps jitter calculations simple.
	maxAllowedJitter = 1 * time.Minute
)

// GracePeriodConfig holds grace period settings for a specific problem type.
type GracePeriodConfig struct {
	BaseDelay time.Duration
	MaxJitter time.Duration
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

	mu                 sync.Mutex
	deadlines          map[types.ProblemCode]time.Time
	rng                *rand.Rand // Protected by mu - only accessed during Lock()
	gracePeriodConfigs map[types.ProblemCode]GracePeriodConfig
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

// WithGracePeriodConfig configures grace period settings for a specific problem type.
func WithGracePeriodConfig(code types.ProblemCode, cfg GracePeriodConfig) RecoveryGracePeriodTrackerOption {
	return func(dt *RecoveryGracePeriodTracker) {
		dt.gracePeriodConfigs[code] = cfg
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
// Use WithGracePeriodConfig to configure which problem types require grace period tracking.
func NewRecoveryGracePeriodTracker(ctx context.Context, config *config.Config, opts ...RecoveryGracePeriodTrackerOption) *RecoveryGracePeriodTracker {
	dt := &RecoveryGracePeriodTracker{
		ctx:                ctx,
		config:             config,
		logger:             slog.Default(),
		deadlines:          make(map[types.ProblemCode]time.Time),
		rng:                rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(time.Now().UnixNano()))),
		gracePeriodConfigs: make(map[types.ProblemCode]GracePeriodConfig),
	}

	for _, opt := range opts {
		opt(dt)
	}

	return dt
}

// calculateDeadline computes a new deadline with base + jitter for the given grace period config.
// Must be called while holding dt.mu lock.
func (dt *RecoveryGracePeriodTracker) calculateDeadline(cfg GracePeriodConfig) time.Time {
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

// Observe records the health state of a problem type.
// This should be called every recovery cycle for tracked problem types.
//
// If isHealthy is true: resets the deadline to now + (base + jitter), with fresh jitter
// If isHealthy is false: freezes the deadline (countdown continues)
//
// If the problem type doesn't require deadline tracking, this is a noop.
func (dt *RecoveryGracePeriodTracker) Observe(code types.ProblemCode, isHealthy bool) {
	dt.mu.Lock()
	defer dt.mu.Unlock()

	// Check if this problem type requires grace period tracking
	cfg, tracked := dt.gracePeriodConfigs[code]
	if !tracked {
		return
	}

	_, exists := dt.deadlines[code]

	if isHealthy {
		// Reset deadline with fresh jitter
		dt.deadlines[code] = dt.calculateDeadline(cfg)
	} else if !exists {
		// First time seeing this problem unhealthy - initialize deadline with base + jitter
		dt.deadlines[code] = dt.calculateDeadline(cfg)
	}
	// If unhealthy and exists, freeze (do nothing - deadline unchanged)
}

// ShouldExecute checks if recovery action should execute for this problem.
// Returns true if action should execute (deadline expired or no deadline tracking).
// Returns false if still within deadline window (should wait longer).
//
// This assumes Observe() has already been called for the problem type.
// If the problem type doesn't require deadline tracking, returns true (execute immediately).
func (dt *RecoveryGracePeriodTracker) ShouldExecute(problem types.Problem) bool {
	dt.mu.Lock()
	defer dt.mu.Unlock()

	// Check if this problem type requires grace period tracking
	_, tracked := dt.gracePeriodConfigs[problem.Code]
	if !tracked {
		// Not tracked - execute immediately
		return true
	}

	deadline, exists := dt.deadlines[problem.Code]
	if !exists {
		// Problem is configured to be tracked but has no deadline - this is unexpected
		// Observe() should have been called before ShouldExecute()
		dt.logger.WarnContext(dt.ctx, "Grace period tracked problem has no deadline", "problem_code", problem.Code)
		return false
	}

	// Check if deadline has expired
	return time.Now().After(deadline) || time.Now().Equal(deadline)
}
