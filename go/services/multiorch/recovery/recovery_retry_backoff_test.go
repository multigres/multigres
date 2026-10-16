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

var testRetryBackoff = &types.RetryBackoffConfig{BaseDelay: 10 * time.Second, MaxDelay: time.Minute}

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

func TestRetryBackoff_SuccessClearsState(t *testing.T) {
	tracker, _ := newTestRetryBackoffTracker()
	problem := poolerProblem(types.ProblemReplicaNotReplicating, "replica1", backoffAction(testRetryBackoff))

	tracker.RecordFailure(t.Context(), problem)
	tracker.RecordFailure(t.Context(), problem)
	_, ready := tracker.ShouldExecute(problem)
	require.False(t, ready)

	tracker.RecordSuccess(problem)

	_, ready = tracker.ShouldExecute(problem)
	assert.True(t, ready, "success clears the backoff")

	// The next failure starts again from the base delay, not from the old count.
	tracker.RecordFailure(t.Context(), problem)
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	assert.Equal(t, 1, tracker.states[gracePeriodKey{code: problem.Code, entityID: problem.EntityID()}].failures)
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
	key := gracePeriodKey{code: problem.Code, entityID: problem.EntityID()}
	hasState := func() bool {
		tracker.mu.Lock()
		defer tracker.mu.Unlock()
		_, ok := tracker.states[key]
		return ok
	}

	tracker.RecordFailure(t.Context(), problem)

	// Absent for less than the retention window (MaxDelay): a flap keeps the state.
	clock.Advance(30 * time.Second)
	tracker.Reconcile(nil)
	assert.True(t, hasState())

	// Detected again: the retention window restarts.
	tracker.Reconcile([]types.Problem{problem})
	clock.Advance(45 * time.Second)
	tracker.Reconcile(nil)
	assert.True(t, hasState())

	// Absent for longer than MaxDelay: the problem is treated as resolved.
	clock.Advance(30 * time.Second)
	tracker.Reconcile(nil)
	assert.False(t, hasState())
}

func TestRetryBackoff_Reset(t *testing.T) {
	tracker, _ := newTestRetryBackoffTracker()
	problem := poolerProblem(types.ProblemReplicaNotReplicating, "replica1", backoffAction(testRetryBackoff))

	tracker.RecordFailure(t.Context(), problem)
	tracker.Reset()

	_, ready := tracker.ShouldExecute(problem)
	assert.True(t, ready)
}

// newRetryBackoffEngine builds an engine with one replica whose problem is
// always detected, and returns a counter of action executions.
func newRetryBackoffEngine(t *testing.T, executeFn func(attempt int) error) (*Engine, *fakeClock, *mockRecoveryAction, func() int) {
	t.Helper()
	ctx := t.Context()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "cell1")
	t.Cleanup(func() { ts.Close() })

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := config.NewTestConfig(config.WithCell("cell1"))
	fakeClient := rpcclient.NewFakeClient()
	engine := NewEngine(ts, logger, cfg, []config.WatchTarget{}, fakeClient, newTestCoordinator(ts, fakeClient, "cell1"))

	clock := newFakeClock()
	engine.recoveryRetryBackoffTracker.now = clock.Now

	replicaID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: "replica-pooler"}
	store.SeedCache(t, engine.poolerCache, store.NewPooler(&multiorchdatapb.PoolerHealthState{
		Multipooler: &clustermetadatapb.Multipooler{
			Id:       replicaID,
			ShardKey: &clustermetadatapb.ShardKey{Database: "db1", TableGroup: "tg1", Shard: "0"},
			Type:     clustermetadatapb.PoolerType_REPLICA,
			Hostname: "replica-host",
		},
		LastSeen: timestamppb.Now(),
	}, nil))

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

	code := types.ProblemCode("TestRetryBackoffProblem")
	analysis.SetTestAnalyzers([]analysis.Analyzer{&customAnalyzer{
		analyzeFn: func(p *store.Pooler) *types.Problem {
			return &types.Problem{
				Code:           code,
				CheckName:      "TestRetryBackoffAnalyzer",
				PoolerID:       p.Health().GetMultipooler().GetId(),
				ShardKey:       p.Health().GetMultipooler().GetShardKey(),
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

	return engine, clock, action, func() int {
		mu.Lock()
		defer mu.Unlock()
		return attempts
	}
}

func TestRecoveryLoop_FailingActionIsThrottled(t *testing.T) {
	engine, clock, _, attempts := newRetryBackoffEngine(t, func(int) error { return errors.New("replica will not stream") })

	// Without backoff this would run the action once per cycle.
	for range 20 {
		engine.performRecoveryCycle(t.Context())
	}
	assert.Equal(t, 1, attempts(), "a failing action must not re-execute every cycle")

	// Once the backoff elapses the action is retried, then throttled again with a longer delay.
	clock.Advance(10 * time.Second)
	for range 20 {
		engine.performRecoveryCycle(t.Context())
	}
	assert.Equal(t, 2, attempts())

	clock.Advance(10 * time.Second)
	engine.performRecoveryCycle(t.Context())
	assert.Equal(t, 2, attempts(), "second failure backs off for 20s, not 10s")

	clock.Advance(10 * time.Second)
	engine.performRecoveryCycle(t.Context())
	assert.Equal(t, 3, attempts())
}

func TestRecoveryLoop_SuccessfulFixClearsBackoff(t *testing.T) {
	engine, clock, _, attempts := newRetryBackoffEngine(t, func(attempt int) error {
		if attempt == 1 {
			return errors.New("transient failure")
		}
		return nil
	})

	engine.performRecoveryCycle(t.Context())
	require.Equal(t, 1, attempts())
	engine.performRecoveryCycle(t.Context())
	require.Equal(t, 1, attempts(), "throttled after the failure")

	clock.Advance(10 * time.Second)
	engine.performRecoveryCycle(t.Context())
	require.Equal(t, 2, attempts(), "retried once the backoff elapsed, and succeeded")

	// The success cleared the state, so the problem recurring is acted on immediately.
	engine.performRecoveryCycle(t.Context())
	assert.Equal(t, 3, attempts(), "no backoff after a successful fix")
}

func TestRecoveryLoop_TriggerRecoveryBypassesBackoff(t *testing.T) {
	engine, _, _, attempts := newRetryBackoffEngine(t, func(int) error { return errors.New("replica will not stream") })

	engine.performRecoveryCycle(t.Context())
	engine.performRecoveryCycle(t.Context())
	require.Equal(t, 1, attempts())

	engine.recoveryRetryBackoffTracker.Reset()
	engine.performRecoveryCycle(t.Context())
	assert.Equal(t, 2, attempts())
}
