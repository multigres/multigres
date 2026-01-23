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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commontypes "github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

func TestRecoveryActionDeadlineTracker_InitialDeadlineReset(t *testing.T) {
	cfg := config.NewTestConfig(
		config.WithPrimaryElectionTimeoutBase(4*time.Second),
		config.WithPrimaryElectionTimeoutMaxJitter(8*time.Second),
	)

	tracker := NewRecoveryGracePeriodTracker(cfg)

	// First reset - should calculate jitter and set deadline
	before := time.Now()
	tracker.Observe(types.ProblemPrimaryIsDead, true)
	after := time.Now()

	// Verify the deadline was set
	tracker.mu.Lock()
	identity := types.ProblemPrimaryIsDead
	timing, exists := tracker.deadlines[identity]
	tracker.mu.Unlock()

	require.True(t, exists, "deadline entry should exist after reset")
	require.NotNil(t, timing, "timing should not be nil")

	// Deadline should be between now + base and now + base + maxJitter
	minDeadline := before.Add(4 * time.Second)
	maxDeadline := after.Add(4*time.Second + 8*time.Second)
	assert.True(t, timing.deadline.After(minDeadline) || timing.deadline.Equal(minDeadline),
		"deadline should be at least base timeout in the future")
	assert.True(t, timing.deadline.Before(maxDeadline) || timing.deadline.Equal(maxDeadline),
		"deadline should not exceed base + max jitter")

	// Jitter should be within bounds [0, maxJitter]
	assert.True(t, timing.jitterDuration >= 0, "jitter should be non-negative")
	assert.True(t, timing.jitterDuration <= 8*time.Second, "jitter should not exceed max jitter")
}

func TestRecoveryActionDeadlineTracker_ContinuousReset(t *testing.T) {
	cfg := config.NewTestConfig(
		config.WithPrimaryElectionTimeoutBase(4*time.Second),
		config.WithPrimaryElectionTimeoutMaxJitter(8*time.Second),
	)

	tracker := NewRecoveryGracePeriodTracker(cfg)

	// First reset
	tracker.Observe(types.ProblemPrimaryIsDead, true)

	// Get the jitter value
	tracker.mu.Lock()
	identity := types.ProblemPrimaryIsDead
	firstJitter := tracker.deadlines[identity].jitterDuration
	firstDeadline := tracker.deadlines[identity].deadline
	tracker.mu.Unlock()

	// Wait a bit
	time.Sleep(100 * time.Millisecond)

	// Reset again
	tracker.Observe(types.ProblemPrimaryIsDead, true)

	// Verify jitter stayed the same but deadline was updated
	tracker.mu.Lock()
	secondJitter := tracker.deadlines[identity].jitterDuration
	secondDeadline := tracker.deadlines[identity].deadline
	tracker.mu.Unlock()

	assert.Equal(t, firstJitter, secondJitter, "jitter should remain the same across resets")
	assert.True(t, secondDeadline.After(firstDeadline), "deadline should be updated to a later time")
}

func TestRecoveryActionDeadlineTracker_ObserveFreezesDeadline(t *testing.T) {
	cfg := config.NewTestConfig(
		config.WithPrimaryElectionTimeoutBase(10*time.Second),
		config.WithPrimaryElectionTimeoutMaxJitter(0), // No jitter for predictability
	)

	tracker := NewRecoveryGracePeriodTracker(cfg)

	// Observe healthy state - sets deadline
	tracker.Observe(types.ProblemPrimaryIsDead, true)

	tracker.mu.Lock()
	frozenDeadline := tracker.deadlines[types.ProblemPrimaryIsDead].deadline
	tracker.mu.Unlock()

	// Wait a bit
	time.Sleep(100 * time.Millisecond)

	// Observe unhealthy state - should NOT change deadline (freeze it)
	tracker.Observe(types.ProblemPrimaryIsDead, false)

	tracker.mu.Lock()
	afterUnhealthyDeadline := tracker.deadlines[types.ProblemPrimaryIsDead].deadline
	tracker.mu.Unlock()

	// Deadline should be unchanged (frozen)
	assert.Equal(t, frozenDeadline, afterUnhealthyDeadline, "deadline should be frozen when unhealthy")

	// Wait a bit more
	time.Sleep(100 * time.Millisecond)

	// Observe unhealthy again - still frozen
	tracker.Observe(types.ProblemPrimaryIsDead, false)

	tracker.mu.Lock()
	stillFrozenDeadline := tracker.deadlines[types.ProblemPrimaryIsDead].deadline
	tracker.mu.Unlock()

	assert.Equal(t, frozenDeadline, stillFrozenDeadline, "deadline should remain frozen across multiple unhealthy observations")

	// Observe healthy again - should reset deadline
	tracker.Observe(types.ProblemPrimaryIsDead, true)

	tracker.mu.Lock()
	resetDeadline := tracker.deadlines[types.ProblemPrimaryIsDead].deadline
	tracker.mu.Unlock()

	assert.True(t, resetDeadline.After(frozenDeadline), "deadline should be reset when healthy again")
}

func TestRecoveryActionDeadlineTracker_DeadlineNotExpired(t *testing.T) {
	cfg := config.NewTestConfig(
		config.WithPrimaryElectionTimeoutBase(10*time.Second),
		config.WithPrimaryElectionTimeoutMaxJitter(0), // No jitter for predictability
	)

	tracker := NewRecoveryGracePeriodTracker(cfg)

	shardKey := commontypes.ShardKey{
		Database:   "testdb",
		TableGroup: "default",
		Shard:      "0",
	}

	// Reset deadline (now + 10s)
	tracker.Observe(types.ProblemPrimaryIsDead, true)

	// Create a problem
	problem := types.Problem{
		Code:     types.ProblemPrimaryIsDead,
		ShardKey: shardKey,
		PoolerID: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "cell1",
			Name:      "primary-1",
		},
	}

	// Check immediately - should not be expired
	expired := tracker.ShouldExecute(problem)
	assert.False(t, expired, "deadline should not be expired immediately after reset")
}

func TestRecoveryActionDeadlineTracker_DeadlineExpired(t *testing.T) {
	cfg := config.NewTestConfig(
		config.WithPrimaryElectionTimeoutBase(100*time.Millisecond),
		config.WithPrimaryElectionTimeoutMaxJitter(0), // No jitter for predictability
	)

	tracker := NewRecoveryGracePeriodTracker(cfg)

	shardKey := commontypes.ShardKey{
		Database:   "testdb",
		TableGroup: "default",
		Shard:      "0",
	}

	// Reset deadline (now + 100ms)
	tracker.Observe(types.ProblemPrimaryIsDead, true)

	// Create a problem
	problem := types.Problem{
		Code:     types.ProblemPrimaryIsDead,
		ShardKey: shardKey,
		PoolerID: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "cell1",
			Name:      "primary-1",
		},
	}

	// Wait for deadline to expire
	time.Sleep(150 * time.Millisecond)

	// Check - should be expired
	expired := tracker.ShouldExecute(problem)
	assert.True(t, expired, "deadline should be expired after waiting")
}

func TestRecoveryActionDeadlineTracker_NoDeadlineTracked(t *testing.T) {
	cfg := config.NewTestConfig(
		config.WithPrimaryElectionTimeoutBase(4*time.Second),
		config.WithPrimaryElectionTimeoutMaxJitter(8*time.Second),
	)

	tracker := NewRecoveryGracePeriodTracker(cfg)

	shardKey := commontypes.ShardKey{
		Database:   "testdb",
		TableGroup: "default",
		Shard:      "0",
	}

	// Create a problem without resetting deadline first
	problem := types.Problem{
		Code:     types.ProblemPrimaryIsDead,
		ShardKey: shardKey,
		PoolerID: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "cell1",
			Name:      "primary-1",
		},
	}

	// Should allow immediate execution when no deadline tracked
	expired := tracker.ShouldExecute(problem)
	assert.True(t, expired, "should allow immediate execution when no deadline tracked")
}

func TestRecoveryActionDeadlineTracker_JitterReusedAcrossResets(t *testing.T) {
	cfg := config.NewTestConfig(
		config.WithPrimaryElectionTimeoutBase(4*time.Second),
		config.WithPrimaryElectionTimeoutMaxJitter(8*time.Second),
	)

	tracker := NewRecoveryGracePeriodTracker(cfg)

	// Reset multiple times
	var jitterValues []time.Duration
	for range 5 {
		tracker.Observe(types.ProblemPrimaryIsDead, true)

		tracker.mu.Lock()
		identity := types.ProblemPrimaryIsDead
		jitter := tracker.deadlines[identity].jitterDuration
		tracker.mu.Unlock()

		jitterValues = append(jitterValues, jitter)
		time.Sleep(10 * time.Millisecond)
	}

	// All jitter values should be identical
	for i := 1; i < len(jitterValues); i++ {
		assert.Equal(t, jitterValues[0], jitterValues[i],
			"jitter should be consistent across all resets")
	}
}

func TestRecoveryActionDeadlineTracker_DifferentProblemsIndependent(t *testing.T) {
	cfg := config.NewTestConfig(
		config.WithPrimaryElectionTimeoutBase(4*time.Second),
		config.WithPrimaryElectionTimeoutMaxJitter(8*time.Second),
	)

	tracker := NewRecoveryGracePeriodTracker(cfg)

	// Reset deadline (there's only one per problem type, not per shard)
	tracker.Observe(types.ProblemPrimaryIsDead, true)

	// Get jitter value
	tracker.mu.Lock()
	identity := types.ProblemPrimaryIsDead
	jitter := tracker.deadlines[identity].jitterDuration
	tracker.mu.Unlock()

	// Jitter should be within bounds
	assert.True(t, jitter >= 0 && jitter <= 8*time.Second)
}

func TestRecoveryActionDeadlineTracker_NonTrackedProblemTypes(t *testing.T) {
	cfg := config.NewTestConfig(
		config.WithPrimaryElectionTimeoutBase(4*time.Second),
		config.WithPrimaryElectionTimeoutMaxJitter(8*time.Second),
	)

	tracker := NewRecoveryGracePeriodTracker(cfg)

	// Reset should be a noop for non-tracked problem types
	tracker.Observe(types.ProblemReplicaNotReplicating, true)

	// Verify no entry was created
	tracker.mu.Lock()
	identity := types.ProblemReplicaNotReplicating
	_, exists := tracker.deadlines[identity]
	tracker.mu.Unlock()

	assert.False(t, exists, "should not create deadline entry for non-tracked problem types")

	// IsDeadlineExpired should return true (execute immediately)
	problem := types.Problem{
		Code: types.ProblemReplicaNotReplicating,
		ShardKey: commontypes.ShardKey{
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
		},
		PoolerID: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "cell1",
			Name:      "replica-1",
		},
	}

	expired := tracker.ShouldExecute(problem)
	assert.True(t, expired, "non-tracked problem types should execute immediately")
}

func TestRecoveryActionDeadlineTracker_ConcurrentAccess(t *testing.T) {
	cfg := config.NewTestConfig(
		config.WithPrimaryElectionTimeoutBase(4*time.Second),
		config.WithPrimaryElectionTimeoutMaxJitter(8*time.Second),
	)

	tracker := NewRecoveryGracePeriodTracker(cfg)

	problem := types.Problem{
		Code: types.ProblemPrimaryIsDead,
		ShardKey: commontypes.ShardKey{
			Database:   "testdb",
			TableGroup: "default",
			Shard:      "0",
		},
		PoolerID: &clustermetadatapb.ID{
			Component: clustermetadatapb.ID_MULTIPOOLER,
			Cell:      "cell1",
			Name:      "primary-1",
		},
	}

	// Run concurrent operations
	done := make(chan bool)
	for range 10 {
		go func() {
			for range 100 {
				tracker.Observe(types.ProblemPrimaryIsDead, true)
				tracker.ShouldExecute(problem)
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for range 10 {
		<-done
	}

	// Verify state is consistent
	tracker.mu.Lock()
	identity := types.ProblemPrimaryIsDead
	timing, exists := tracker.deadlines[identity]
	tracker.mu.Unlock()

	assert.True(t, exists, "deadline should exist after concurrent access")
	assert.NotNil(t, timing, "timing should not be nil")
}

func TestRecoveryActionDeadlineTracker_DynamicConfigUpdate(t *testing.T) {
	cfg := config.NewTestConfig(
		config.WithPrimaryElectionTimeoutBase(4*time.Second),
		config.WithPrimaryElectionTimeoutMaxJitter(8*time.Second),
	)

	tracker := NewRecoveryGracePeriodTracker(cfg)

	// First reset with original config
	tracker.Observe(types.ProblemPrimaryIsDead, true)

	tracker.mu.Lock()
	identity := types.ProblemPrimaryIsDead
	originalJitter := tracker.deadlines[identity].jitterDuration
	tracker.mu.Unlock()

	// Verify original jitter is within original bounds
	assert.True(t, originalJitter >= 0 && originalJitter <= 8*time.Second,
		"original jitter should be within original bounds")

	// Create a new tracker with different config to verify new problems use new config
	newCfg := config.NewTestConfig(
		config.WithPrimaryElectionTimeoutBase(2*time.Second),
		config.WithPrimaryElectionTimeoutMaxJitter(4*time.Second),
	)
	newTracker := NewRecoveryGracePeriodTracker(newCfg)

	// Reset with new config
	newTracker.Observe(types.ProblemPrimaryIsDead, true)

	newTracker.mu.Lock()
	identity2 := types.ProblemPrimaryIsDead
	newJitter := newTracker.deadlines[identity2].jitterDuration
	newTracker.mu.Unlock()

	// New jitter should be within new bounds
	assert.True(t, newJitter >= 0 && newJitter <= 4*time.Second,
		"new jitter should be within new config bounds")
}
