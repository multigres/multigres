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
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	commontypes "github.com/multigres/multigres/go/common/types"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/tools/retry"
)

// retryBackoffCeiling caps the delay when an action leaves MaxDelay unset.
const retryBackoffCeiling = time.Hour

// retryBackoffKey identifies a problem for retry backoff tracking. entityID is
// a pooler ID string for pooler-scoped problems or a shard string for
// shard-scoped ones.
type retryBackoffKey struct {
	code     types.ProblemCode
	entityID string
}

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
	// shard and epoch record the shard and its rule when the failure was
	// recorded; state from an earlier epoch no longer applies.
	shard string
	epoch string
	// provisional marks a state whose last attempt succeeded: it is cleared once
	// the problem is seen to be gone, and kept if the problem is still detected.
	provisional bool
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
// State is dropped when:
//   - the shard's rule changes (for example a new leader), because the failures
//     were against a different target;
//   - a successful attempt is followed by a cycle that no longer detects the
//     problem (a success that leaves the problem detected keeps the history);
//   - the problem has been absent for longer than the delay it was enforcing,
//     which still lets a problem that flaps in and out of detection keep its
//     backoff.
//
// Thread safety: all methods are safe for concurrent use.
type RecoveryRetryBackoffTracker struct {
	logger *slog.Logger
	now    func() time.Time

	// bypass counts active Bypass calls; while positive nothing is gated.
	bypass atomic.Int32

	mu     sync.Mutex
	states map[retryBackoffKey]*retryBackoffState
	// epochs holds each shard's current rule as of the last Reconcile.
	epochs map[string]string
	rng    *rand.Rand // Protected by mu
}

// NewRecoveryRetryBackoffTracker creates a new retry backoff tracker.
func NewRecoveryRetryBackoffTracker(logger *slog.Logger) *RecoveryRetryBackoffTracker {
	return &RecoveryRetryBackoffTracker{
		logger: logger,
		now:    time.Now,
		states: make(map[retryBackoffKey]*retryBackoffState),
		rng:    rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(time.Now().UnixNano()))),
	}
}

// retryBackoffConfig returns the problem's backoff config, or nil if the
// problem is not subject to retry backoff.
func retryBackoffConfig(problem types.Problem) *types.RetryBackoffConfig {
	if problem.RecoveryAction == nil || problem.Code.IsFailoverProblem() {
		return nil
	}
	return problem.RecoveryAction.Metadata().RetryBackoff
}

func retryBackoffKeyOf(problem types.Problem) retryBackoffKey {
	return retryBackoffKey{code: problem.Code, entityID: problem.EntityID()}
}

func shardKeyOf(problem types.Problem) string {
	return string(commontypes.FormatShardRef(problem.ShardRef))
}

// Reconcile applies this cycle's detected problems and each shard's current
// rule (keyed by formatted shard ref) to the tracked state, dropping what no
// longer applies. Call once per recovery cycle, after all analyzers, with the
// full detected set.
func (t *RecoveryRetryBackoffTracker) Reconcile(detected []types.Problem, epochs map[string]string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.epochs = epochs
	now := t.now()

	seen := make(map[retryBackoffKey]struct{}, len(detected))
	for _, p := range detected {
		seen[retryBackoffKeyOf(p)] = struct{}{}
	}

	for key, st := range t.states {
		if st.epoch != t.epochs[st.shard] {
			delete(t.states, key)
			continue
		}
		if _, ok := seen[key]; ok {
			st.lastSeen = now
			// A success that leaves the problem detected did not resolve it.
			st.provisional = false
			continue
		}
		if st.provisional || now.Sub(st.lastSeen) > st.retention {
			delete(t.states, key)
		}
	}
}

// ShouldExecute reports whether the problem's action may run now, and the
// earliest time it may (zero if immediately).
func (t *RecoveryRetryBackoffTracker) ShouldExecute(problem types.Problem) (notBefore time.Time, ready bool) {
	if retryBackoffConfig(problem) == nil || t.bypass.Load() > 0 {
		return time.Time{}, true
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	st, ok := t.states[retryBackoffKeyOf(problem)]
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

	key := retryBackoffKeyOf(problem)
	st, ok := t.states[key]
	if !ok {
		st = &retryBackoffState{}
		t.states[key] = st
	}
	st.failures++
	st.provisional = false
	st.shard = shardKeyOf(problem)
	st.epoch = t.epochs[st.shard]

	maxDelay := cfg.MaxDelay
	if maxDelay <= 0 {
		maxDelay = retryBackoffCeiling
	}
	delay := retry.ExponentialBackoffMagnitude(cfg.BaseDelay, maxDelay, st.failures-1)
	if cfg.JitterFraction > 0 {
		delay += time.Duration(t.rng.Float64() * cfg.JitterFraction * float64(delay))
	}

	now := t.now()
	st.notBefore = now.Add(delay)
	st.lastSeen = now
	st.retention = delay

	t.logger.InfoContext(ctx, "recovery action failed, backing off before retrying",
		"problem_code", problem.Code,
		"entity_id", problem.EntityID(),
		"consecutive_failures", st.failures,
		"retry_after", delay,
	)
}

// RecordSuccess notes a successful attempt. The failure history is not cleared
// yet: the next Reconcile drops it if the problem is gone and keeps it if the
// problem is still detected, since a success does not by itself mean the
// problem was resolved.
func (t *RecoveryRetryBackoffTracker) RecordSuccess(problem types.Problem) {
	if retryBackoffConfig(problem) == nil {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if st, ok := t.states[retryBackoffKeyOf(problem)]; ok {
		st.provisional = true
		st.notBefore = time.Time{}
	}
}

// Bypass turns off gating until the returned release function is called, and
// clears all state both now and on release. Used by TriggerRecoveryNow so an
// explicit operator request acts on every cycle of the call and leaves no
// backoff behind.
func (t *RecoveryRetryBackoffTracker) Bypass() (release func()) {
	t.bypass.Add(1)
	t.reset()

	var once sync.Once
	return func() {
		once.Do(func() {
			t.bypass.Add(-1)
			t.reset()
		})
	}
}

func (t *RecoveryRetryBackoffTracker) reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	clear(t.states)
}
