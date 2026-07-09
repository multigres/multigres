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

// gracePeriodKey uniquely identifies a grace period tracking entry.
// entityID is either a pooler ID string (for pooler-scoped problems) or a
// shard key string (for shard-scoped problems).
type gracePeriodKey struct {
	code     types.ProblemCode
	entityID string
}

// RecoveryGracePeriodTracker tracks grace periods for recovery actions.
// It implements a deadline-based model where:
// - First detection of a problem: starts a countdown at now + (base + jitter)
// - Still detected: deadline is frozen, counting down to expiry
// - No longer detected: the problem is resolved and its deadline is dropped
// - Action executes only after the deadline expires
//
// Callers drive it by calling Reconcile once per recovery cycle with the full
// set of detected problems; a problem's absence from that set is what marks it
// resolved, so the tracker never needs to know the universe of possible problems.
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

// Reconcile updates grace-period deadlines against the full set of problems
// detected in one recovery cycle. It must be called exactly once per cycle,
// after every analyzer has run, with all detected problems:
//
//   - A newly detected problem (no existing deadline) starts its countdown at
//     now + base + jitter.
//   - A problem detected again keeps its existing deadline: the countdown is
//     frozen, not restarted.
//   - A tracked problem absent from detected is treated as resolved and its
//     deadline is dropped, so a later recurrence starts a fresh countdown.
//
// Because resolution is inferred from absence, the tracker needs neither the
// health of individual entities nor the set of codes an analyzer might emit —
// only what was actually detected this cycle. Problems whose recovery action
// has no grace period are not tracked.
func (dt *RecoveryGracePeriodTracker) Reconcile(detected []types.Problem) {
	dt.mu.Lock()
	defer dt.mu.Unlock()

	stillActive := make(map[gracePeriodKey]struct{}, len(detected))
	for _, p := range detected {
		gracePeriodCfg := p.RecoveryAction.GracePeriod()
		if gracePeriodCfg == nil {
			// Action doesn't require grace period tracking.
			continue
		}
		key := gracePeriodKey{code: p.Code, entityID: p.EntityID()}
		stillActive[key] = struct{}{}
		if _, exists := dt.deadlines[key]; !exists {
			// First detection - start the countdown with base + jitter.
			dt.deadlines[key] = dt.calculateDeadline(*gracePeriodCfg)
		}
		// Already tracked - freeze (leave the deadline unchanged).
	}

	// Drop deadlines for problems no longer detected this cycle: they resolved.
	for key := range dt.deadlines {
		if _, ok := stillActive[key]; !ok {
			delete(dt.deadlines, key)
		}
	}
}

// ForceExpireAll immediately expires all tracked grace period deadlines.
// After this call, ShouldExecute returns true for all tracked problems regardless
// of their original deadline.
//
// Intended for use in TriggerRecoveryNow so that an explicit operator request
// can bypass the normal grace period wait and act on detected problems immediately.
func (dt *RecoveryGracePeriodTracker) ForceExpireAll() {
	dt.mu.Lock()
	defer dt.mu.Unlock()

	past := time.Time{} // zero value is before all real timestamps
	for key := range dt.deadlines {
		dt.deadlines[key] = past
	}
}

// ShouldExecute checks if recovery action should execute for this problem.
// Returns true if action should execute (deadline expired or no grace period needed).
// Returns false if still within grace period window (should wait longer).
//
// This assumes Reconcile() has already run this cycle for the detected set that
// includes this problem. If the action doesn't require grace period tracking,
// returns true (execute immediately).
func (dt *RecoveryGracePeriodTracker) ShouldExecute(problem types.Problem) bool {
	dt.mu.Lock()
	defer dt.mu.Unlock()

	// Get grace period config from the action
	gracePeriodCfg := problem.RecoveryAction.GracePeriod()
	if gracePeriodCfg == nil {
		// Action doesn't require grace period tracking - execute immediately
		return true
	}

	entityID := problem.EntityID()

	key := gracePeriodKey{code: problem.Code, entityID: entityID}
	deadline, exists := dt.deadlines[key]
	if !exists {
		// Problem has grace period but no deadline - this is unexpected
		// Reconcile() should have run for this cycle's detected set before ShouldExecute()
		dt.logger.WarnContext(dt.ctx, "Grace period deadline not found, skipping recovery",
			"problem_code", problem.Code,
			"entity_id", entityID)
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
