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
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// mockActionWithGracePeriod is a mock action for testing grace period behavior.
type mockActionWithGracePeriod struct {
	gracePeriod *types.GracePeriodConfig
}

func (m *mockActionWithGracePeriod) Execute(ctx context.Context, problem types.Problem) error {
	return nil
}

func (m *mockActionWithGracePeriod) Metadata() types.RecoveryMetadata {
	return types.RecoveryMetadata{Name: "MockAction"}
}

func (m *mockActionWithGracePeriod) RequiresHealthyLeader() bool {
	return false
}

func (m *mockActionWithGracePeriod) GracePeriod() *types.GracePeriodConfig {
	return m.gracePeriod
}

var testGraceShardKey = &clustermetadatapb.ShardKey{Database: "testdb", TableGroup: "default", Shard: "0"}

// shardProblem builds a shard-scoped problem for the given code and action, the
// way the recovery loop would present it to Reconcile.
func shardProblem(code types.ProblemCode, action types.RecoveryAction) types.Problem {
	return types.Problem{
		Code:           code,
		Scope:          types.ScopeShard,
		ShardKey:       testGraceShardKey,
		RecoveryAction: action,
	}
}

// poolerProblem builds a pooler-scoped problem for the given code and action.
func poolerProblem(code types.ProblemCode, name string, action types.RecoveryAction) types.Problem {
	return types.Problem{
		Code:           code,
		Scope:          types.ScopePooler,
		ShardKey:       testGraceShardKey,
		PoolerID:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: name},
		RecoveryAction: action,
	}
}

func (dt *RecoveryGracePeriodTracker) deadlineFor(p types.Problem) (time.Time, bool) {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	deadline, ok := dt.deadlines[gracePeriodKey{code: p.Code, entityID: p.EntityID()}]
	return deadline, ok
}

func TestRecoveryGracePeriod_FirstDetectionStartsCountdown(t *testing.T) {
	cfg := config.NewTestConfig()
	tracker := NewRecoveryGracePeriodTracker(t.Context(), cfg)

	action := &mockActionWithGracePeriod{
		gracePeriod: &types.GracePeriodConfig{BaseDelay: 4 * time.Second, MaxJitter: 8 * time.Second},
	}
	problem := shardProblem(types.ProblemLeaderIsDead, action)

	before := time.Now()
	tracker.Reconcile([]types.Problem{problem})
	after := time.Now()

	deadline, exists := tracker.deadlineFor(problem)
	require.True(t, exists, "deadline should exist after first detection")

	// Deadline should be within [now + base, now + base + maxJitter].
	minDeadline := before.Add(4 * time.Second)
	maxDeadline := after.Add(4*time.Second + 8*time.Second)
	assert.False(t, deadline.Before(minDeadline), "deadline should be at least base in the future")
	assert.False(t, deadline.After(maxDeadline), "deadline should not exceed base + max jitter")
}

func TestRecoveryGracePeriod_StillDetectedFreezesDeadline(t *testing.T) {
	cfg := config.NewTestConfig()
	tracker := NewRecoveryGracePeriodTracker(t.Context(), cfg)

	action := &mockActionWithGracePeriod{
		gracePeriod: &types.GracePeriodConfig{BaseDelay: 10 * time.Second, MaxJitter: 0},
	}
	problem := shardProblem(types.ProblemLeaderIsDead, action)

	tracker.Reconcile([]types.Problem{problem})
	first, ok := tracker.deadlineFor(problem)
	require.True(t, ok)

	time.Sleep(20 * time.Millisecond)

	// Detected again in a later cycle: the countdown must be frozen, not restarted.
	tracker.Reconcile([]types.Problem{problem})
	second, ok := tracker.deadlineFor(problem)
	require.True(t, ok)

	assert.Equal(t, first, second, "deadline should be frozen while the problem stays detected")
}

func TestRecoveryGracePeriod_ResolvedProblemIsEvicted(t *testing.T) {
	cfg := config.NewTestConfig()
	tracker := NewRecoveryGracePeriodTracker(t.Context(), cfg)

	action := &mockActionWithGracePeriod{
		gracePeriod: &types.GracePeriodConfig{BaseDelay: 10 * time.Second, MaxJitter: 0},
	}
	problem := shardProblem(types.ProblemLeaderIsDead, action)

	tracker.Reconcile([]types.Problem{problem})
	_, ok := tracker.deadlineFor(problem)
	require.True(t, ok, "deadline exists while detected")

	// A cycle that no longer detects the problem resolves it.
	tracker.Reconcile(nil)
	_, ok = tracker.deadlineFor(problem)
	assert.False(t, ok, "deadline should be dropped when the problem is no longer detected")
}

func TestRecoveryGracePeriod_RecurrenceStartsFreshCountdown(t *testing.T) {
	cfg := config.NewTestConfig()
	tracker := NewRecoveryGracePeriodTracker(t.Context(), cfg)

	action := &mockActionWithGracePeriod{
		gracePeriod: &types.GracePeriodConfig{BaseDelay: 10 * time.Second, MaxJitter: 0},
	}
	problem := shardProblem(types.ProblemLeaderIsDead, action)

	tracker.Reconcile([]types.Problem{problem})
	first, ok := tracker.deadlineFor(problem)
	require.True(t, ok)

	// Resolve, then let the same problem recur after a gap.
	tracker.Reconcile(nil)
	time.Sleep(20 * time.Millisecond)
	tracker.Reconcile([]types.Problem{problem})
	second, ok := tracker.deadlineFor(problem)
	require.True(t, ok)

	assert.True(t, second.After(first), "a recurrence after resolution should start a fresh, later countdown")
}

func TestRecoveryGracePeriod_ShouldExecuteBeforeDeadline(t *testing.T) {
	cfg := config.NewTestConfig()
	tracker := NewRecoveryGracePeriodTracker(t.Context(), cfg)

	action := &mockActionWithGracePeriod{
		gracePeriod: &types.GracePeriodConfig{BaseDelay: 10 * time.Second, MaxJitter: 0},
	}
	problem := shardProblem(types.ProblemLeaderIsDead, action)

	tracker.Reconcile([]types.Problem{problem})
	assert.False(t, tracker.ShouldExecute(problem), "should not execute before the deadline expires")
}

func TestRecoveryGracePeriod_ShouldExecuteAfterDeadline(t *testing.T) {
	cfg := config.NewTestConfig()
	tracker := NewRecoveryGracePeriodTracker(t.Context(), cfg)

	action := &mockActionWithGracePeriod{
		gracePeriod: &types.GracePeriodConfig{BaseDelay: 100 * time.Millisecond, MaxJitter: 0},
	}
	problem := shardProblem(types.ProblemLeaderIsDead, action)

	tracker.Reconcile([]types.Problem{problem})
	time.Sleep(150 * time.Millisecond)
	assert.True(t, tracker.ShouldExecute(problem), "should execute after the deadline expires")
}

func TestRecoveryGracePeriod_NoGracePeriodExecutesImmediately(t *testing.T) {
	cfg := config.NewTestConfig()
	tracker := NewRecoveryGracePeriodTracker(t.Context(), cfg)

	// Action with no grace period is not tracked and executes immediately.
	action := &mockActionWithGracePeriod{gracePeriod: nil}
	problem := shardProblem(types.ProblemLeaderIsDead, action)

	tracker.Reconcile([]types.Problem{problem})
	_, exists := tracker.deadlineFor(problem)
	assert.False(t, exists, "problems without a grace period should not be tracked")
	assert.True(t, tracker.ShouldExecute(problem), "problems without a grace period should execute immediately")
}

func TestRecoveryGracePeriod_MissingDeadlineDefersExecution(t *testing.T) {
	cfg := config.NewTestConfig()
	tracker := NewRecoveryGracePeriodTracker(t.Context(), cfg)

	action := &mockActionWithGracePeriod{
		gracePeriod: &types.GracePeriodConfig{BaseDelay: 10 * time.Second, MaxJitter: 0},
	}
	problem := shardProblem(types.ProblemLeaderIsDead, action)

	// ShouldExecute without a prior Reconcile has no deadline: defer rather than
	// act blindly.
	assert.False(t, tracker.ShouldExecute(problem), "should defer when no deadline was reconciled for the problem")
}

func TestRecoveryGracePeriod_DistinctProblemsTrackedIndependently(t *testing.T) {
	cfg := config.NewTestConfig()
	tracker := NewRecoveryGracePeriodTracker(t.Context(), cfg)

	action := &mockActionWithGracePeriod{
		gracePeriod: &types.GracePeriodConfig{BaseDelay: 10 * time.Second, MaxJitter: 0},
	}
	leaderProblem := shardProblem(types.ProblemLeaderIsDead, action)
	replicaProblem := poolerProblem(types.ProblemReplicaNotReplicating, "replica-1", action)

	tracker.Reconcile([]types.Problem{leaderProblem, replicaProblem})
	_, leaderOK := tracker.deadlineFor(leaderProblem)
	_, replicaOK := tracker.deadlineFor(replicaProblem)
	require.True(t, leaderOK)
	require.True(t, replicaOK)

	// Resolving one leaves the other untouched.
	tracker.Reconcile([]types.Problem{leaderProblem})
	_, leaderOK = tracker.deadlineFor(leaderProblem)
	_, replicaOK = tracker.deadlineFor(replicaProblem)
	assert.True(t, leaderOK, "still-detected problem should keep its deadline")
	assert.False(t, replicaOK, "resolved problem should be evicted independently")
}

func TestRecoveryGracePeriod_JitterWithinConfiguredBounds(t *testing.T) {
	cfg := config.NewTestConfig()

	// Deterministic RNG so we can assert the exact jitter value.
	rng := rand.New(rand.NewPCG(99999, 88888))
	tracker := NewRecoveryGracePeriodTracker(t.Context(), cfg, WithRand(rng))

	action := &mockActionWithGracePeriod{
		gracePeriod: &types.GracePeriodConfig{BaseDelay: 4 * time.Second, MaxJitter: 8 * time.Second},
	}
	problem := shardProblem(types.ProblemLeaderIsDead, action)

	testRng := rand.New(rand.NewPCG(99999, 88888))
	expectedJitter := time.Duration(testRng.Int64N(int64(8 * time.Second)))

	before := time.Now()
	tracker.Reconcile([]types.Problem{problem})
	after := time.Now()

	deadline, ok := tracker.deadlineFor(problem)
	require.True(t, ok)
	assert.False(t, deadline.Before(before.Add(4*time.Second+expectedJitter)), "deadline should be at least detection time + base + jitter")
	assert.False(t, deadline.After(after.Add(4*time.Second+expectedJitter)), "deadline should be at most detection time + base + jitter")
}

func TestRecoveryGracePeriod_ForceExpireAll(t *testing.T) {
	cfg := config.NewTestConfig()
	tracker := NewRecoveryGracePeriodTracker(t.Context(), cfg)

	action := &mockActionWithGracePeriod{
		gracePeriod: &types.GracePeriodConfig{BaseDelay: 10 * time.Second, MaxJitter: 0},
	}
	problemA := shardProblem(types.ProblemLeaderIsDead, action)
	problemB := poolerProblem(types.ProblemStaleLeader, "pooler-1", action)

	tracker.Reconcile([]types.Problem{problemA, problemB})

	require.False(t, tracker.ShouldExecute(problemA), "should not execute before grace period expires")
	require.False(t, tracker.ShouldExecute(problemB), "should not execute before grace period expires")

	tracker.ForceExpireAll()
	assert.True(t, tracker.ShouldExecute(problemA), "should execute after ForceExpireAll")
	assert.True(t, tracker.ShouldExecute(problemB), "should execute after ForceExpireAll")
}

func TestRecoveryGracePeriod_ConcurrentAccess(t *testing.T) {
	cfg := config.NewTestConfig()
	tracker := NewRecoveryGracePeriodTracker(t.Context(), cfg)

	action := &mockActionWithGracePeriod{
		gracePeriod: &types.GracePeriodConfig{BaseDelay: 4 * time.Second, MaxJitter: 8 * time.Second},
	}
	problem := shardProblem(types.ProblemLeaderIsDead, action)

	done := make(chan bool)
	for range 10 {
		go func() {
			for range 100 {
				tracker.Reconcile([]types.Problem{problem})
				tracker.ShouldExecute(problem)
			}
			done <- true
		}()
	}
	for range 10 {
		<-done
	}

	deadline, exists := tracker.deadlineFor(problem)
	assert.True(t, exists, "deadline should exist after concurrent access")
	assert.False(t, deadline.IsZero(), "deadline should not be zero")
}
