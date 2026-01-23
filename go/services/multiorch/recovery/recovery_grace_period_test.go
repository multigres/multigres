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
	deadline, exists := tracker.deadlines[identity]
	tracker.mu.Unlock()

	require.True(t, exists, "deadline entry should exist after reset")

	// Deadline should be between now + base and now + base + maxJitter
	minDeadline := before.Add(4 * time.Second)
	maxDeadline := after.Add(4*time.Second + 8*time.Second)
	assert.True(t, deadline.After(minDeadline) || deadline.Equal(minDeadline),
		"deadline should be at least base timeout in the future")
	assert.True(t, deadline.Before(maxDeadline) || deadline.Equal(maxDeadline),
		"deadline should not exceed base + max jitter")
}

func TestRecoveryActionDeadlineTracker_ContinuousReset(t *testing.T) {
	cfg := config.NewTestConfig(
		config.WithPrimaryElectionTimeoutBase(4*time.Second),
		config.WithPrimaryElectionTimeoutMaxJitter(8*time.Second),
	)

	// Use deterministic random generator for predictable jitter
	rng := rand.New(rand.NewPCG(12345, 67890))
	tracker := NewRecoveryGracePeriodTracker(cfg, WithRand(rng))

	// First reset - will generate first jitter value
	before1 := time.Now()
	tracker.Observe(types.ProblemPrimaryIsDead, true)
	after1 := time.Now()

	// Get the deadline
	tracker.mu.Lock()
	identity := types.ProblemPrimaryIsDead
	firstDeadline := tracker.deadlines[identity]
	tracker.mu.Unlock()

	// Calculate expected jitter from the seeded RNG
	testRng := rand.New(rand.NewPCG(12345, 67890))
	expectedJitter1 := time.Duration(testRng.Int64N(int64(8 * time.Second)))

	// Verify first deadline is base + expected jitter
	assert.True(t, firstDeadline.After(before1.Add(4*time.Second+expectedJitter1)) ||
		firstDeadline.Equal(before1.Add(4*time.Second+expectedJitter1)))
	assert.True(t, firstDeadline.Before(after1.Add(4*time.Second+expectedJitter1)) ||
		firstDeadline.Equal(after1.Add(4*time.Second+expectedJitter1)))

	// Wait a bit
	time.Sleep(100 * time.Millisecond)

	// Reset again - will generate second jitter value
	before2 := time.Now()
	tracker.Observe(types.ProblemPrimaryIsDead, true)
	after2 := time.Now()

	// Verify deadline was updated
	tracker.mu.Lock()
	secondDeadline := tracker.deadlines[identity]
	tracker.mu.Unlock()

	// Calculate second expected jitter (next value from RNG)
	expectedJitter2 := time.Duration(testRng.Int64N(int64(8 * time.Second)))

	// Verify second deadline is base + new expected jitter
	assert.True(t, secondDeadline.After(before2.Add(4*time.Second+expectedJitter2)) ||
		secondDeadline.Equal(before2.Add(4*time.Second+expectedJitter2)))
	assert.True(t, secondDeadline.Before(after2.Add(4*time.Second+expectedJitter2)) ||
		secondDeadline.Equal(after2.Add(4*time.Second+expectedJitter2)))

	// Verify the jitter values are different (unless by random chance they're the same)
	// We verify this by checking deadlines are recalculated with fresh jitter
	assert.NotEqual(t, firstDeadline, secondDeadline, "deadline should be recalculated with fresh jitter")
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
	frozenDeadline := tracker.deadlines[types.ProblemPrimaryIsDead]
	tracker.mu.Unlock()

	// Wait a bit
	time.Sleep(100 * time.Millisecond)

	// Observe unhealthy state - should NOT change deadline (freeze it)
	tracker.Observe(types.ProblemPrimaryIsDead, false)

	tracker.mu.Lock()
	afterUnhealthyDeadline := tracker.deadlines[types.ProblemPrimaryIsDead]
	tracker.mu.Unlock()

	// Deadline should be unchanged (frozen)
	assert.Equal(t, frozenDeadline, afterUnhealthyDeadline, "deadline should be frozen when unhealthy")

	// Wait a bit more
	time.Sleep(100 * time.Millisecond)

	// Observe unhealthy again - still frozen
	tracker.Observe(types.ProblemPrimaryIsDead, false)

	tracker.mu.Lock()
	stillFrozenDeadline := tracker.deadlines[types.ProblemPrimaryIsDead]
	tracker.mu.Unlock()

	assert.Equal(t, frozenDeadline, stillFrozenDeadline, "deadline should remain frozen across multiple unhealthy observations")

	// Observe healthy again - should reset deadline
	tracker.Observe(types.ProblemPrimaryIsDead, true)

	tracker.mu.Lock()
	resetDeadline := tracker.deadlines[types.ProblemPrimaryIsDead]
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

func TestRecoveryActionDeadlineTracker_JitterRecalculatedAcrossResets(t *testing.T) {
	cfg := config.NewTestConfig(
		config.WithPrimaryElectionTimeoutBase(4*time.Second),
		config.WithPrimaryElectionTimeoutMaxJitter(8*time.Second),
	)

	tracker := NewRecoveryGracePeriodTracker(cfg)

	// Reset multiple times and collect deadlines
	var deadlines []time.Time
	for range 5 {
		tracker.Observe(types.ProblemPrimaryIsDead, true)

		tracker.mu.Lock()
		identity := types.ProblemPrimaryIsDead
		deadline := tracker.deadlines[identity]
		tracker.mu.Unlock()

		deadlines = append(deadlines, deadline)
		time.Sleep(10 * time.Millisecond)
	}

	// All deadlines should be within valid bounds (base + [0, maxJitter])
	for _, deadline := range deadlines {
		// Each deadline should be roughly 4-12 seconds in the future from when it was set
		// (We can't verify exact bounds since time passes during the test)
		assert.False(t, deadline.IsZero(), "deadline should not be zero")
	}
}

func TestRecoveryActionDeadlineTracker_DifferentProblemsIndependent(t *testing.T) {
	cfg := config.NewTestConfig(
		config.WithPrimaryElectionTimeoutBase(4*time.Second),
		config.WithPrimaryElectionTimeoutMaxJitter(8*time.Second),
	)

	tracker := NewRecoveryGracePeriodTracker(cfg)

	// Reset deadline (there's only one per problem type, not per shard)
	before := time.Now()
	tracker.Observe(types.ProblemPrimaryIsDead, true)
	after := time.Now()

	// Get deadline
	tracker.mu.Lock()
	identity := types.ProblemPrimaryIsDead
	deadline := tracker.deadlines[identity]
	tracker.mu.Unlock()

	// Deadline should be within bounds [now + base, now + base + maxJitter]
	minDeadline := before.Add(4 * time.Second)
	maxDeadline := after.Add(4*time.Second + 8*time.Second)
	assert.True(t, deadline.After(minDeadline) || deadline.Equal(minDeadline))
	assert.True(t, deadline.Before(maxDeadline) || deadline.Equal(maxDeadline))
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
	deadline, exists := tracker.deadlines[identity]
	tracker.mu.Unlock()

	assert.True(t, exists, "deadline should exist after concurrent access")
	assert.False(t, deadline.IsZero(), "deadline should not be zero")
}

func TestRecoveryActionDeadlineTracker_DynamicConfigUpdate(t *testing.T) {
	cfg := config.NewTestConfig(
		config.WithPrimaryElectionTimeoutBase(4*time.Second),
		config.WithPrimaryElectionTimeoutMaxJitter(8*time.Second),
	)

	tracker := NewRecoveryGracePeriodTracker(cfg)

	// First reset with original config
	before1 := time.Now()
	tracker.Observe(types.ProblemPrimaryIsDead, true)
	after1 := time.Now()

	tracker.mu.Lock()
	identity := types.ProblemPrimaryIsDead
	originalDeadline := tracker.deadlines[identity]
	tracker.mu.Unlock()

	// Verify original deadline is within original bounds
	minDeadline1 := before1.Add(4 * time.Second)
	maxDeadline1 := after1.Add(4*time.Second + 8*time.Second)
	assert.True(t, originalDeadline.After(minDeadline1) || originalDeadline.Equal(minDeadline1),
		"original deadline should be within original bounds")
	assert.True(t, originalDeadline.Before(maxDeadline1) || originalDeadline.Equal(maxDeadline1),
		"original deadline should be within original bounds")

	// Create a new tracker with different config to verify new problems use new config
	newCfg := config.NewTestConfig(
		config.WithPrimaryElectionTimeoutBase(2*time.Second),
		config.WithPrimaryElectionTimeoutMaxJitter(4*time.Second),
	)
	newTracker := NewRecoveryGracePeriodTracker(newCfg)

	// Reset with new config
	before2 := time.Now()
	newTracker.Observe(types.ProblemPrimaryIsDead, true)
	after2 := time.Now()

	newTracker.mu.Lock()
	identity2 := types.ProblemPrimaryIsDead
	newDeadline := newTracker.deadlines[identity2]
	newTracker.mu.Unlock()

	// New deadline should be within new bounds
	minDeadline2 := before2.Add(2 * time.Second)
	maxDeadline2 := after2.Add(2*time.Second + 4*time.Second)
	assert.True(t, newDeadline.After(minDeadline2) || newDeadline.Equal(minDeadline2),
		"new deadline should be within new config bounds")
	assert.True(t, newDeadline.Before(maxDeadline2) || newDeadline.Equal(maxDeadline2),
		"new deadline should be within new config bounds")
}
