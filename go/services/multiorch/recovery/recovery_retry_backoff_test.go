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
	"errors"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/recovery/analysis"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

// fakeClock is a manually advanced clock for the retry backoff tracker.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newTestRetryBackoffTracker() (*RecoveryRetryBackoffTracker, *fakeClock) {
	clock := newFakeClock()
	tracker := NewRecoveryRetryBackoffTracker(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))
	tracker.now = clock.Now
	return tracker, clock
}

func backoffAction(cfg *types.RetryBackoffConfig) *mockRecoveryAction {
	return &mockRecoveryAction{
		metadata: types.RecoveryMetadata{
			Name:         "BackoffAction",
			Timeout:      time.Second,
			RetryBackoff: cfg,
		},
	}
}

var testShardRef = &clustermetadatapb.ShardRef{Database: "testdb", TableGroup: "default", Shard: "0"}

// shardProblem builds a shard-scoped problem for the given code and action.
func shardProblem(code types.ProblemCode, action types.RecoveryAction) types.Problem {
	return types.Problem{
		Code:           code,
		Scope:          types.ScopeShard,
		ShardRef:       testShardRef,
		RecoveryAction: action,
	}
}

// poolerProblem builds a pooler-scoped problem for the given code and action.
func poolerProblem(code types.ProblemCode, name string, action types.RecoveryAction) types.Problem {
	return types.Problem{
		Code:           code,
		Scope:          types.ScopePooler,
		ShardRef:       testShardRef,
		PoolerID:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: name},
		RecoveryAction: action,
	}
}

var testRetryBackoff = &types.RetryBackoffConfig{BaseDelay: 10 * time.Second, MaxDelay: time.Minute}

func retryBackoffFailures(tracker *RecoveryRetryBackoffTracker, problem types.Problem) (failures int, ok bool) {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	st, ok := tracker.states[retryBackoffKeyOf(problem)]
	if !ok {
		return 0, false
	}
	return st.failures, true
}

func TestRetryBackoff_ThrottlesRepeatedFailures(t *testing.T) {
	tracker, clock := newTestRetryBackoffTracker()
	problem := poolerProblem(types.ProblemReplicaNotReplicating, "replica1", backoffAction(testRetryBackoff))

	_, ready := tracker.ShouldExecute(problem)
	assert.True(t, ready, "first attempt is never throttled")

	// Delays double per consecutive failure and are capped at MaxDelay.
	for _, delay := range []time.Duration{10 * time.Second, 20 * time.Second, 40 * time.Second, time.Minute, time.Minute} {
		tracker.RecordFailure(t.Context(), problem)

		notBefore, ready := tracker.ShouldExecute(problem)
		assert.False(t, ready)
		assert.Equal(t, clock.Now().Add(delay), notBefore)

		clock.Advance(delay - time.Nanosecond)
		_, ready = tracker.ShouldExecute(problem)
		assert.False(t, ready, "still backing off just before the deadline")

		clock.Advance(time.Nanosecond)
		_, ready = tracker.ShouldExecute(problem)
		assert.True(t, ready, "ready once the delay elapses")
	}
}

func TestRetryBackoff_JitterOnlyLengthensDelay(t *testing.T) {
	tracker, clock := newTestRetryBackoffTracker()
	cfg := &types.RetryBackoffConfig{BaseDelay: 10 * time.Second, MaxDelay: time.Minute, JitterFraction: 0.25}
	problem := poolerProblem(types.ProblemReplicaNotReplicating, "replica1", backoffAction(cfg))

	tracker.RecordFailure(t.Context(), problem)

	notBefore, ready := tracker.ShouldExecute(problem)
	assert.False(t, ready)
	delay := notBefore.Sub(clock.Now())
	assert.GreaterOrEqual(t, delay, 10*time.Second)
	assert.LessOrEqual(t, delay, 12500*time.Millisecond)
}

func TestRetryBackoff_UnsetMaxDelayIsCappedAndNeverOverflows(t *testing.T) {
	tracker, clock := newTestRetryBackoffTracker()
	cfg := &types.RetryBackoffConfig{BaseDelay: time.Second, JitterFraction: 0.25}
	problem := poolerProblem(types.ProblemReplicaNotReplicating, "replica1", backoffAction(cfg))

	for range 200 {
		tracker.RecordFailure(t.Context(), problem)
	}

	notBefore, ready := tracker.ShouldExecute(problem)
	assert.False(t, ready, "a long failure run must not wrap the delay into the past")
	delay := notBefore.Sub(clock.Now())
	assert.GreaterOrEqual(t, delay, retryBackoffCeiling)
	assert.LessOrEqual(t, delay, retryBackoffCeiling+retryBackoffCeiling/4)
}

func TestRetryBackoff_SuccessClearsStateOnceTheProblemIsGone(t *testing.T) {
	tracker, _ := newTestRetryBackoffTracker()
	problem := poolerProblem(types.ProblemReplicaNotReplicating, "replica1", backoffAction(testRetryBackoff))

	tracker.RecordFailure(t.Context(), problem)
	tracker.RecordFailure(t.Context(), problem)
	_, ready := tracker.ShouldExecute(problem)
	require.False(t, ready)

	tracker.RecordSuccess(problem)
	_, ready = tracker.ShouldExecute(problem)
	assert.True(t, ready, "a success lets the next attempt run immediately")

	tracker.Reconcile(nil, nil)
	_, ok := retryBackoffFailures(tracker, problem)
	assert.False(t, ok, "the history is dropped once the problem is no longer detected")

	// The next failure starts again from the base delay, not from the old count.
	tracker.RecordFailure(t.Context(), problem)
	failures, _ := retryBackoffFailures(tracker, problem)
	assert.Equal(t, 1, failures)
}

func TestRetryBackoff_SuccessThatLeavesTheProblemDetectedKeepsHistory(t *testing.T) {
	tracker, clock := newTestRetryBackoffTracker()
	problem := poolerProblem(types.ProblemReplicaNotReplicating, "replica1", backoffAction(testRetryBackoff))

	tracker.RecordFailure(t.Context(), problem)
	tracker.RecordFailure(t.Context(), problem)
	tracker.RecordSuccess(problem)

	// The next cycle still detects the problem, so the success did not resolve it.
	tracker.Reconcile([]types.Problem{problem}, nil)
	failures, ok := retryBackoffFailures(tracker, problem)
	require.True(t, ok)
	assert.Equal(t, 2, failures)

	// The following failure continues the escalation instead of restarting it.
	tracker.RecordFailure(t.Context(), problem)
	notBefore, _ := tracker.ShouldExecute(problem)
	assert.Equal(t, clock.Now().Add(40*time.Second), notBefore)
}

func TestRetryBackoff_RecordSuccessIgnoresActionsWithoutConfig(t *testing.T) {
	tracker, _ := newTestRetryBackoffTracker()
	problem := poolerProblem(types.ProblemReplicaNotReplicating, "replica1", backoffAction(nil))

	tracker.RecordSuccess(problem)

	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	assert.Empty(t, tracker.states)
}

func TestRetryBackoff_PerEntity(t *testing.T) {
	tracker, _ := newTestRetryBackoffTracker()
	action := backoffAction(testRetryBackoff)
	failing := poolerProblem(types.ProblemReplicaNotReplicating, "replica1", action)
	other := poolerProblem(types.ProblemReplicaNotReplicating, "replica2", action)

	tracker.RecordFailure(t.Context(), failing)

	_, ready := tracker.ShouldExecute(failing)
	assert.False(t, ready)
	_, ready = tracker.ShouldExecute(other)
	assert.True(t, ready, "another replica is not throttled by this one's failures")
}

func TestRetryBackoff_IgnoresActionsWithoutConfigAndFailoverProblems(t *testing.T) {
	tracker, _ := newTestRetryBackoffTracker()

	noConfig := poolerProblem(types.ProblemReplicaNotReplicating, "replica1", backoffAction(nil))
	tracker.RecordFailure(t.Context(), noConfig)
	_, ready := tracker.ShouldExecute(noConfig)
	assert.True(t, ready, "actions that do not opt in are never throttled")

	failover := shardProblem(types.ProblemLeaderUnhealthy, backoffAction(testRetryBackoff))
	tracker.RecordFailure(t.Context(), failover)
	_, ready = tracker.ShouldExecute(failover)
	assert.True(t, ready, "failover problems are gated on recruitment backoff instead")
}

func TestRetryBackoff_ReconcileRetainsStateAcrossFlapsThenDropsIt(t *testing.T) {
	tracker, clock := newTestRetryBackoffTracker()
	problem := poolerProblem(types.ProblemReplicaNotReplicating, "replica1", backoffAction(testRetryBackoff))

	tracker.RecordFailure(t.Context(), problem) // 10s delay, so a 10s retention

	// Absent for less than the delay being enforced: a flap keeps the state.
	clock.Advance(5 * time.Second)
	tracker.Reconcile(nil, nil)
	_, ok := retryBackoffFailures(tracker, problem)
	assert.True(t, ok)

	// Detected again: the retention window restarts.
	tracker.Reconcile([]types.Problem{problem}, nil)
	clock.Advance(8 * time.Second)
	tracker.Reconcile(nil, nil)
	_, ok = retryBackoffFailures(tracker, problem)
	assert.True(t, ok)

	// Absent for longer than the delay: the problem is treated as resolved.
	clock.Advance(3 * time.Second)
	tracker.Reconcile(nil, nil)
	_, ok = retryBackoffFailures(tracker, problem)
	assert.False(t, ok)
}

func TestRetryBackoff_RuleChangeDropsState(t *testing.T) {
	tracker, _ := newTestRetryBackoffTracker()
	problem := poolerProblem(types.ProblemReplicaNotReplicating, "replica1", backoffAction(testRetryBackoff))
	shard := shardKeyOf(problem)
	detected := []types.Problem{problem}

	tracker.Reconcile(detected, map[string]string{shard: "rule-1"})
	tracker.RecordFailure(t.Context(), problem)
	tracker.RecordFailure(t.Context(), problem)
	_, ready := tracker.ShouldExecute(problem)
	require.False(t, ready)

	tracker.Reconcile(detected, map[string]string{shard: "rule-1"})
	_, ready = tracker.ShouldExecute(problem)
	assert.False(t, ready, "an unchanged rule keeps the backoff")

	// A new leader means the earlier failures were against a different target.
	tracker.Reconcile(detected, map[string]string{shard: "rule-2"})
	_, ready = tracker.ShouldExecute(problem)
	assert.True(t, ready, "a rule change must not delay delivering the new leader")
	_, ok := retryBackoffFailures(tracker, problem)
	assert.False(t, ok)
}

func TestRetryBackoff_BypassDisablesGatingAndClearsState(t *testing.T) {
	tracker, _ := newTestRetryBackoffTracker()
	problem := poolerProblem(types.ProblemReplicaNotReplicating, "replica1", backoffAction(testRetryBackoff))

	tracker.RecordFailure(t.Context(), problem)
	_, ready := tracker.ShouldExecute(problem)
	require.False(t, ready)

	release := tracker.Bypass()
	_, ready = tracker.ShouldExecute(problem)
	assert.True(t, ready, "bypass clears what was already backing off")

	// Failures during the bypass do not gate, however many there are.
	for range 5 {
		tracker.RecordFailure(t.Context(), problem)
		_, ready = tracker.ShouldExecute(problem)
		assert.True(t, ready)
	}

	release()
	release() // releasing twice must not unbalance the counter
	assert.Equal(t, int32(0), tracker.bypass.Load())
	_, ready = tracker.ShouldExecute(problem)
	assert.True(t, ready, "backoff built up during the bypass is not left behind")
}

// retryBackoffHarness is an engine with one replica whose problem is detected
// while detected is true, and a counter of action executions.
type retryBackoffHarness struct {
	engine      *Engine
	clock       *fakeClock
	attempts    func() int
	setDetected func(bool)
	setRule     func(term int64)
}

func newRetryBackoffHarness(t *testing.T, executeFn func(attempt int) error) *retryBackoffHarness {
	t.Helper()
	ctx := t.Context()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	t.Cleanup(func() { ts.Close() })

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := config.NewTestConfig(config.WithCell("cell1"), config.WithRecoveryCycleInterval(20*time.Millisecond))
	fakeClient := rpcclient.NewFakeClient()
	engine := NewEngine(ts, logger, cfg, []config.WatchTarget{}, fakeClient, newTestCoordinator(ts, fakeClient, "cell1"))

	clock := newFakeClock()
	engine.recoveryRetryBackoffTracker.now = clock.Now

	replicaID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica-pooler"}
	poolerState := func(term int64) *store.Pooler {
		return store.NewPooler(&multiorchdatapb.PoolerHealthState{
			Multipooler: &clustermetadatapb.Multipooler{
				Id:       replicaID,
				ShardRef: &clustermetadatapb.ShardRef{Database: "db1", TableGroup: "tg1", Shard: "0"},
				Type:     clustermetadatapb.PoolerType_REPLICA,
				Hostname: "replica-host",
			},
			ConsensusStatus: &clustermetadatapb.ConsensusStatus{
				CurrentPosition: &clustermetadatapb.PoolerPosition{
					Position: &clustermetadatapb.RulePosition{
						Decision: &clustermetadatapb.ShardRule{
							RuleNumber: &clustermetadatapb.RuleNumber{CoordinatorTerm: term},
						},
					},
				},
			},
			LastSeen: timestamppb.Now(),
		}, nil)
	}
	poolerID := store.SeedCache(t, engine.poolerCache, poolerState(1))

	var mu sync.Mutex
	attempts := 0
	action := &mockRecoveryAction{
		metadata: types.RecoveryMetadata{
			Name:         "BackoffAction",
			Timeout:      10 * time.Second,
			RetryBackoff: &types.RetryBackoffConfig{BaseDelay: 10 * time.Second, MaxDelay: time.Minute},
		},
		executeFn: func(ctx context.Context, problem types.Problem) error {
			mu.Lock()
			attempts++
			n := attempts
			mu.Unlock()
			return executeFn(n)
		},
	}

	var detected atomic.Bool
	detected.Store(true)
	code := types.ProblemCode("TestRetryBackoffProblem")
	analysis.SetTestAnalyzers([]analysis.Analyzer{&customAnalyzer{
		analyzeFn: func(p *multiorchdatapb.PoolerHealthState) *types.Problem {
			if !detected.Load() {
				return nil
			}
			return &types.Problem{
				Code:           code,
				CheckName:      "TestRetryBackoffAnalyzer",
				PoolerID:       p.GetMultipooler().GetId(),
				ShardRef:       p.GetMultipooler().GetShardRef(),
				Priority:       types.PriorityNormal,
				Scope:          types.ScopePooler,
				RecoveryAction: action,
				DetectedAt:     time.Now(),
			}
		},
		name:           "TestRetryBackoffAnalyzer",
		problemCode:    code,
		recoveryAction: action,
	}})
	t.Cleanup(analysis.ResetAnalyzers)

	return &retryBackoffHarness{
		engine: engine,
		clock:  clock,
		attempts: func() int {
			mu.Lock()
			defer mu.Unlock()
			return attempts
		},
		setDetected: detected.Store,
		setRule: func(term int64) {
			engine.poolerCache.DoUpdate(poolerID, func(*store.Pooler) *store.Pooler { return poolerState(term) })
		},
	}
}

func (h *retryBackoffHarness) cycle(t *testing.T) {
	t.Helper()
	h.engine.performRecoveryCycle(t.Context())
}

func TestRecoveryLoop_FailingActionIsThrottled(t *testing.T) {
	h := newRetryBackoffHarness(t, func(int) error { return errors.New("replica will not stream") })

	// Without backoff this would run the action once per cycle.
	for range 20 {
		h.cycle(t)
	}
	assert.Equal(t, 1, h.attempts(), "a failing action must not re-execute every cycle")

	// Once the backoff elapses the action is retried, then throttled again with a longer delay.
	h.clock.Advance(10 * time.Second)
	for range 20 {
		h.cycle(t)
	}
	assert.Equal(t, 2, h.attempts())

	h.clock.Advance(10 * time.Second)
	h.cycle(t)
	assert.Equal(t, 2, h.attempts(), "second failure backs off for 20s, not 10s")

	h.clock.Advance(10 * time.Second)
	h.cycle(t)
	assert.Equal(t, 3, h.attempts())
}

func TestRecoveryLoop_SuccessfulFixClearsBackoffOnceTheProblemIsGone(t *testing.T) {
	// Attempt 1 fails, attempt 2 succeeds, later attempts fail again.
	h := newRetryBackoffHarness(t, func(attempt int) error {
		if attempt == 2 {
			return nil
		}
		return errors.New("replica will not stream")
	})

	h.cycle(t)
	require.Equal(t, 1, h.attempts())
	h.cycle(t)
	require.Equal(t, 1, h.attempts(), "throttled after the failure")

	h.clock.Advance(10 * time.Second)
	h.cycle(t)
	require.Equal(t, 2, h.attempts(), "retried once the backoff elapsed, and succeeded")

	// The fix worked: the problem is gone, then recurs and fails.
	h.setDetected(false)
	h.cycle(t)
	h.setDetected(true)
	h.cycle(t)
	require.Equal(t, 3, h.attempts(), "a recurrence is acted on immediately")

	// The history was cleared, so this failure backs off for the base delay again.
	h.clock.Advance(10 * time.Second)
	h.cycle(t)
	assert.Equal(t, 4, h.attempts(), "backoff restarted at 10s, not 20s")
}

func TestRecoveryLoop_SuccessThatLeavesTheProblemDetectedKeepsBackingOff(t *testing.T) {
	h := newRetryBackoffHarness(t, func(attempt int) error {
		if attempt == 2 {
			return nil
		}
		return errors.New("replica will not stream")
	})

	h.cycle(t)
	h.clock.Advance(10 * time.Second)
	h.cycle(t)
	require.Equal(t, 2, h.attempts(), "attempt 2 succeeds")

	// The problem is still detected, so the success resolved nothing and the
	// failure count carries on.
	h.cycle(t)
	require.Equal(t, 3, h.attempts(), "the next attempt is not delayed by a success")

	h.clock.Advance(10 * time.Second)
	h.cycle(t)
	assert.Equal(t, 3, h.attempts(), "the second failure backs off for 20s")
	h.clock.Advance(10 * time.Second)
	h.cycle(t)
	assert.Equal(t, 4, h.attempts())
}

func TestRecoveryLoop_RuleChangeResetsBackoff(t *testing.T) {
	h := newRetryBackoffHarness(t, func(int) error { return errors.New("replica will not stream") })

	h.cycle(t)
	h.cycle(t)
	require.Equal(t, 1, h.attempts(), "backing off after the first failure")

	// A new leader changes the shard's rule: the replica must be told promptly,
	// not after the backoff built up against the previous leader.
	h.setRule(2)
	h.cycle(t)
	assert.Equal(t, 2, h.attempts())
}

func TestTriggerRecoveryNow_BypassesBackoffForTheWholeCall(t *testing.T) {
	h := newRetryBackoffHarness(t, func(int) error { return errors.New("replica will not stream") })

	h.cycle(t)
	h.cycle(t)
	require.Equal(t, 1, h.attempts(), "backing off before the explicit trigger")

	// The problem never resolves, so the call polls until its timeout, running a
	// cycle every 20ms. With the fake clock frozen, any gating would hold the
	// attempts at one.
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, err := h.engine.TriggerRecoveryNow(ctx, 0)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.GreaterOrEqual(t, h.attempts(), 4, "every cycle of the call acted despite the failures")

	tracker := h.engine.recoveryRetryBackoffTracker
	assert.Equal(t, int32(0), tracker.bypass.Load(), "the bypass is released on return")
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	assert.Empty(t, tracker.states, "no backoff is left behind by the forced attempts")
}
