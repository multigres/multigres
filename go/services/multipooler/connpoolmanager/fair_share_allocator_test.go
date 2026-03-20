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

package connpoolmanager

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFairShareAllocator_New(t *testing.T) {
	alloc := NewFairShareAllocator(100, 1)
	assert.Equal(t, int64(100), alloc.Capacity())
}

func TestFairShareAllocator_EmptyDemands(t *testing.T) {
	alloc := NewFairShareAllocator(100, 1)
	result := alloc.Allocate(map[string]int64{})

	assert.Empty(t, result)
}

func TestFairShareAllocator_SingleUser(t *testing.T) {
	tests := []struct {
		name     string
		capacity int64
		demand   int64
		want     int64
	}{
		{
			name:     "demand below capacity gets full capacity for burst headroom",
			capacity: 400,
			demand:   150,
			want:     400, // Gets full capacity since they're the only user
		},
		{
			name:     "demand equals capacity",
			capacity: 100,
			demand:   100,
			want:     100,
		},
		{
			name:     "demand exceeds capacity",
			capacity: 100,
			demand:   500,
			want:     100, // capped at capacity
		},
		{
			name:     "zero demand gets full capacity for burst headroom",
			capacity: 100,
			demand:   0,
			want:     100, // Single user gets full capacity
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			alloc := NewFairShareAllocator(tc.capacity, 1)
			result := alloc.Allocate(map[string]int64{
				"userA": tc.demand,
			})

			require.Contains(t, result, "userA")
			assert.Equal(t, tc.want, result["userA"])
		})
	}
}

func TestFairShareAllocator_EqualDemand(t *testing.T) {
	// Two users with equal demand that fits within capacity
	// Total demand: 60, capacity: 100, extra: 40 / 2 = 20 each
	alloc := NewFairShareAllocator(100, 1)
	result := alloc.Allocate(map[string]int64{
		"userA": 30,
		"userB": 30,
	})

	// Both get demand + burst headroom: 30 + 20 = 50 each
	assert.Equal(t, int64(50), result["userA"])
	assert.Equal(t, int64(50), result["userB"])
}

func TestFairShareAllocator_MaxMinFairness(t *testing.T) {
	// Classic max-min fairness scenario from the design doc
	// Capacity: 400
	// User A wants 150, User B wants 100, User C wants 80
	// Total demand: 330, extra: 70 / 3 = 23 each
	alloc := NewFairShareAllocator(400, 1)

	result := alloc.Allocate(map[string]int64{
		"userA": 150,
		"userB": 100,
		"userC": 80,
	})

	// All users get demand + burst headroom (23 each)
	assert.Equal(t, int64(173), result["userA"]) // 150 + 23
	assert.Equal(t, int64(123), result["userB"]) // 100 + 23
	assert.Equal(t, int64(103), result["userC"]) // 80 + 23
}

func TestFairShareAllocator_DemandExceedsCapacity(t *testing.T) {
	// Three users each want 50, but only 100 capacity
	// Fair share = 100/3 ≈ 33 each
	alloc := NewFairShareAllocator(100, 1)

	result := alloc.Allocate(map[string]int64{
		"userA": 50,
		"userB": 50,
		"userC": 50,
	})

	// Total should be <= 100
	total := result["userA"] + result["userB"] + result["userC"]

	assert.LessOrEqual(t, total, int64(100))
	// Each user should get at least 1
	assert.GreaterOrEqual(t, result["userA"], int64(1))
	assert.GreaterOrEqual(t, result["userB"], int64(1))
	assert.GreaterOrEqual(t, result["userC"], int64(1))
}

func TestFairShareAllocator_MixedSatisfaction(t *testing.T) {
	// User A wants a lot, User B wants a little
	// B should be fully satisfied, A gets the rest
	alloc := NewFairShareAllocator(100, 1)

	result := alloc.Allocate(map[string]int64{
		"userA": 200, // Wants more than capacity
		"userB": 20,  // Wants little
	})

	// B should get full demand
	assert.Equal(t, int64(20), result["userB"])
	// A should get the rest (80)
	assert.Equal(t, int64(80), result["userA"])
}

func TestFairShareAllocator_ZeroDemandUser(t *testing.T) {
	// User with zero demand should still get minimum 1, plus burst headroom
	// Total effective demand: 50 + 1 = 51, capacity: 100, extra: 49 / 2 = 24 each
	alloc := NewFairShareAllocator(100, 1)

	result := alloc.Allocate(map[string]int64{
		"active": 50,
		"idle":   0,
	})

	assert.Equal(t, int64(74), result["active"]) // 50 + 24
	assert.Equal(t, int64(25), result["idle"])   // 1 + 24
}

func TestFairShareAllocator_TwoAllocatorsForDifferentResources(t *testing.T) {
	// Demonstrate using two allocators for regular and reserved pools
	// This is how it would be used in the rebalancer

	regularAlloc := NewFairShareAllocator(400, 1)  // 80% of 500
	reservedAlloc := NewFairShareAllocator(100, 1) // 20% of 500

	// User A: high regular demand, low reserved
	// User B: low regular demand, high reserved
	regularDemands := map[string]int64{
		"userA": 300,
		"userB": 50,
	}
	reservedDemands := map[string]int64{
		"userA": 10,
		"userB": 80,
	}

	regularResult := regularAlloc.Allocate(regularDemands)
	reservedResult := reservedAlloc.Allocate(reservedDemands)

	// Regular: demand 350 from 400, extra: 50 / 2 = 25 each
	assert.Equal(t, int64(325), regularResult["userA"]) // 300 + 25
	assert.Equal(t, int64(75), regularResult["userB"])  // 50 + 25

	// Reserved: demand 90 from 100, extra: 10 / 2 = 5 each
	assert.Equal(t, int64(15), reservedResult["userA"]) // 10 + 5
	assert.Equal(t, int64(85), reservedResult["userB"]) // 80 + 5
}

// Property-based tests

func TestFairShareAllocator_Property_TotalDoesNotExceedCapacity(t *testing.T) {
	// Property: Total allocation should not exceed capacity
	// (except when minimum 1 per user causes overflow)
	capacity := int64(80)
	alloc := NewFairShareAllocator(capacity, 1)

	for i := range 100 {
		numUsers := rand.IntN(10) + 1
		demands := make(map[string]int64, numUsers)
		for j := range numUsers {
			demands[string(rune('A'+j))] = rand.Int64N(200)
		}

		result := alloc.Allocate(demands)

		var total int64
		for _, v := range result {
			total += v
		}

		// Allow for minimum 1 per user overflow
		maxAllowed := max(capacity, int64(numUsers))

		assert.LessOrEqual(t, total, maxAllowed,
			"iteration %d: total %d exceeds max %d", i, total, maxAllowed)
	}
}

func TestFairShareAllocator_Property_MinimumOneConnection(t *testing.T) {
	// Property: Every user gets at least 1 connection
	alloc := NewFairShareAllocator(100, 1)

	for i := range 100 {
		numUsers := rand.IntN(20) + 1
		demands := make(map[string]int64, numUsers)
		for j := range numUsers {
			demands[string(rune('A'+j))] = rand.Int64N(100)
		}

		result := alloc.Allocate(demands)

		for user, allocation := range result {
			assert.GreaterOrEqual(t, allocation, int64(1),
				"iteration %d: user %s has %d (should be >= 1)", i, user, allocation)
		}
	}
}

func TestFairShareAllocator_Property_AtLeastDemandWhenCapacitySufficient(t *testing.T) {
	// Property: When capacity is sufficient, every user gets at least their demand
	// (plus potential burst headroom from extra capacity)
	alloc := NewFairShareAllocator(1000, 1) // Large capacity

	for i := range 100 {
		numUsers := rand.IntN(10) + 1
		demands := make(map[string]int64, numUsers)
		var totalDemand int64
		for j := range numUsers {
			d := rand.Int64N(50) + 1 // Small demands so total fits in capacity
			demands[string(rune('A'+j))] = d
			totalDemand += d
		}

		// Only test when demand fits
		if totalDemand > 1000 {
			continue
		}

		result := alloc.Allocate(demands)

		for user, allocation := range result {
			demand := demands[user]
			assert.GreaterOrEqual(t, allocation, demand,
				"iteration %d: user %s got %d but demanded %d", i, user, allocation, demand)
		}
	}
}

func TestFairShareAllocator_Property_FullSatisfactionPlusBurstHeadroom(t *testing.T) {
	// Property: If total demand <= capacity, everyone gets at least their full demand
	// plus an equal share of the remaining capacity for burst headroom
	alloc := NewFairShareAllocator(1000, 1)

	for i := range 100 {
		numUsers := rand.IntN(5) + 1
		demands := make(map[string]int64, numUsers)
		var totalDemand int64

		for j := range numUsers {
			demand := rand.Int64N(100) + 1
			demands[string(rune('A'+j))] = demand
			totalDemand += demand
		}

		// Only test when demand fits
		if totalDemand > 1000 {
			continue
		}

		result := alloc.Allocate(demands)

		// Calculate expected burst headroom per user
		remaining := int64(1000) - totalDemand
		extraPerUser := remaining / int64(numUsers)

		for user, allocation := range result {
			demand := demands[user]
			expectedAlloc := demand + extraPerUser
			assert.Equal(t, expectedAlloc, allocation,
				"iteration %d: user %s should get demand + burst headroom", i, user)
		}
	}
}

func TestFairShareAllocator_BurstHeadroom(t *testing.T) {
	// Test that remaining capacity is split evenly for burst headroom
	alloc := NewFairShareAllocator(100, 1)

	// Three users with small demands: 10 + 10 + 10 = 30
	// Remaining: 100 - 30 = 70, split evenly: 70 / 3 = 23 each
	result := alloc.Allocate(map[string]int64{
		"userA": 10,
		"userB": 10,
		"userC": 10,
	})

	// Each user gets 10 + 23 = 33
	assert.Equal(t, int64(33), result["userA"])
	assert.Equal(t, int64(33), result["userB"])
	assert.Equal(t, int64(33), result["userC"])

	// Total should be 99 (1 left over due to integer division)
	total := result["userA"] + result["userB"] + result["userC"]
	assert.Equal(t, int64(99), total)
}

func TestFairShareAllocator_NoBurstHeadroomWhenCapacityExhausted(t *testing.T) {
	// When demand exceeds capacity, no burst headroom is given
	// because all capacity is used satisfying demands
	alloc := NewFairShareAllocator(100, 1)

	// Three users each want 50 (total 150 > capacity 100)
	result := alloc.Allocate(map[string]int64{
		"userA": 50,
		"userB": 50,
		"userC": 50,
	})

	// Total should be exactly capacity (no extra to give)
	total := result["userA"] + result["userB"] + result["userC"]
	assert.Equal(t, int64(100), total)

	// Each user should get ~33 (fair share of capacity)
	for user, alloc := range result {
		assert.GreaterOrEqual(t, alloc, int64(33),
			"user %s should get at least 33", user)
		assert.LessOrEqual(t, alloc, int64(34),
			"user %s should get at most 34", user)
	}
}

// Benchmark

func BenchmarkFairShareAllocator_Allocate(b *testing.B) {
	alloc := NewFairShareAllocator(400, 1)

	// Create 10 users with varying demands
	demands := map[string]int64{
		"user1":  50,
		"user2":  100,
		"user3":  30,
		"user4":  80,
		"user5":  60,
		"user6":  40,
		"user7":  90,
		"user8":  20,
		"user9":  70,
		"user10": 55,
	}

	for b.Loop() {
		_ = alloc.Allocate(demands)
	}
}
