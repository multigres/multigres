// Copyright 2026 Supabase, Inc.
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
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/tools/retry"
)

// retryBackoffState is the failure history for one (problem code, entity).
type retryBackoffState struct {
	failures int
	// notBefore is the earliest time the action may execute again.
	notBefore time.Time
	// lastSeen is the last time the problem was detected, used to drop state
	// for problems that stay resolved.
	lastSeen time.Time
	// retention is how long the state outlives the problem's last detection.
	retention time.Duration
}

// RecoveryRetryBackoffTracker throttles re-execution of recovery actions that
// keep failing against the same problem and entity.
//
// The recovery loop re-detects problems every cycle from the health stream, so
// skipping Execute while a problem is backing off does not stop it from being
// re-checked: the problem still clears the moment it is no longer detected.
// Only actions that set RecoveryMetadata.RetryBackoff are tracked, and failover
// problems are never tracked (they are gated on collective recruitment backoff).
//
// State is cleared by RecordSuccess, or by Reconcile once the problem has been
// absent from the detected set for longer than the action's MaxDelay. The
// retention window keeps a problem that flaps in and out of detection from
// resetting its backoff on every flap.
//
// Thread safety: all methods are safe for concurrent use.
type RecoveryRetryBackoffTracker struct {
	logger *slog.Logger
	now    func() time.Time

	mu     sync.Mutex
	states map[gracePeriodKey]*retryBackoffState
	rng    *rand.Rand // Protected by mu
}

// NewRecoveryRetryBackoffTracker creates a new retry backoff tracker.
func NewRecoveryRetryBackoffTracker(logger *slog.Logger) *RecoveryRetryBackoffTracker {
	return &RecoveryRetryBackoffTracker{
		logger: logger,
		now:    time.Now,
		states: make(map[gracePeriodKey]*retryBackoffState),
		rng:    rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(time.Now().UnixNano()))),
	}
}

// retryBackoffConfig returns the problem's backoff config, or nil if the
// problem is not subject to retry backoff.
func retryBackoffConfig(problem types.Problem) *types.RetryBackoffConfig {
	if problem.Code.IsFailoverProblem() {
		return nil
	}
	return problem.RecoveryAction.Metadata().RetryBackoff
}

// Reconcile refreshes the last-seen time of tracked problems that are still
// detected and drops state for problems absent for longer than their
// retention window. Call once per recovery cycle with the full detected set.
func (t *RecoveryRetryBackoffTracker) Reconcile(detected []types.Problem) {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.now()
	for _, p := range detected {
		if st, ok := t.states[gracePeriodKey{code: p.Code, entityID: p.EntityID()}]; ok {
			st.lastSeen = now
		}
	}
	for key, st := range t.states {
		if now.Sub(st.lastSeen) > st.retention {
			delete(t.states, key)
		}
	}
}

// ShouldExecute reports whether the problem's action may run now, and the
// earliest time it may (zero if immediately).
func (t *RecoveryRetryBackoffTracker) ShouldExecute(problem types.Problem) (notBefore time.Time, ready bool) {
	if retryBackoffConfig(problem) == nil {
		return time.Time{}, true
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	st, ok := t.states[gracePeriodKey{code: problem.Code, entityID: problem.EntityID()}]
	if !ok || !t.now().Before(st.notBefore) {
		return time.Time{}, true
	}
	return st.notBefore, false
}

// RecordFailure registers a failed attempt and schedules the next permitted one.
func (t *RecoveryRetryBackoffTracker) RecordFailure(ctx context.Context, problem types.Problem) {
	cfg := retryBackoffConfig(problem)
	if cfg == nil {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	key := gracePeriodKey{code: problem.Code, entityID: problem.EntityID()}
	st, ok := t.states[key]
	if !ok {
		st = &retryBackoffState{}
		t.states[key] = st
	}
	st.failures++

	maxDelay := cfg.MaxDelay
	if maxDelay <= 0 {
		maxDelay = math.MaxInt64
	}
	delay := retry.ExponentialBackoffMagnitude(cfg.BaseDelay, maxDelay, st.failures-1)
	if cfg.JitterFraction > 0 {
		delay += time.Duration(t.rng.Float64() * cfg.JitterFraction * float64(delay))
	}

	now := t.now()
	st.notBefore = now.Add(delay)
	st.lastSeen = now
	st.retention = max(cfg.MaxDelay, delay)

	t.logger.InfoContext(ctx, "recovery action failed, backing off before retrying",
		"problem_code", problem.Code,
		"entity_id", problem.EntityID(),
		"consecutive_failures", st.failures,
		"retry_after", delay,
	)
}

// RecordSuccess clears the problem's failure history.
func (t *RecoveryRetryBackoffTracker) RecordSuccess(problem types.Problem) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.states, gracePeriodKey{code: problem.Code, entityID: problem.EntityID()})
}

// Reset clears all backoff state so the next cycle acts on every detected
// problem immediately. Used by TriggerRecoveryNow so an explicit operator
// request bypasses backoff.
func (t *RecoveryRetryBackoffTracker) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	clear(t.states)
}
