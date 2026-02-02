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
			name:     "demand below capacity",
			capacity: 400,
			demand:   150,
			want:     150,
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
			name:     "zero demand gets minimum",
			capacity: 100,
			demand:   0,
			want:     1,
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
	alloc := NewFairShareAllocator(100, 1)
	result := alloc.Allocate(map[string]int64{
		"userA": 30,
		"userB": 30,
	})

	// Both should get their full demand
	assert.Equal(t, int64(30), result["userA"])
	assert.Equal(t, int64(30), result["userB"])
}

func TestFairShareAllocator_MaxMinFairness(t *testing.T) {
	// Classic max-min fairness scenario from the design doc
	// Capacity: 400
	// User A wants 150, User B wants 100, User C wants 80
	// All should be satisfied since total demand (330) < capacity (400)
	alloc := NewFairShareAllocator(400, 1)

	result := alloc.Allocate(map[string]int64{
		"userA": 150,
		"userB": 100,
		"userC": 80,
	})

	// All users should get their full demand
	assert.Equal(t, int64(150), result["userA"])
	assert.Equal(t, int64(100), result["userB"])
	assert.Equal(t, int64(80), result["userC"])
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
	// User with zero demand should still get minimum 1
	alloc := NewFairShareAllocator(100, 1)

	result := alloc.Allocate(map[string]int64{
		"active": 50,
		"idle":   0,
	})

	assert.Equal(t, int64(50), result["active"])
	assert.Equal(t, int64(1), result["idle"])
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

	// Regular: A wants 300, B wants 50. Both get full demand (total 350 < 400)
	assert.Equal(t, int64(300), regularResult["userA"])
	assert.Equal(t, int64(50), regularResult["userB"])

	// Reserved: A wants 10, B wants 80. Both get full demand (total 90 < 100)
	assert.Equal(t, int64(10), reservedResult["userA"])
	assert.Equal(t, int64(80), reservedResult["userB"])
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

func TestFairShareAllocator_Property_NeverExceedsDemand(t *testing.T) {
	// Property: No user gets more than their demand (treating 0 demand as 1)
	alloc := NewFairShareAllocator(1000, 1) // Large capacity

	for i := range 100 {
		numUsers := rand.IntN(10) + 1
		demands := make(map[string]int64, numUsers)
		for j := range numUsers {
			demands[string(rune('A'+j))] = rand.Int64N(100) + 1 // At least 1 demand
		}

		result := alloc.Allocate(demands)

		for user, allocation := range result {
			demand := demands[user]
			assert.LessOrEqual(t, allocation, demand,
				"iteration %d: user %s got %d but demanded %d", i, user, allocation, demand)
		}
	}
}

func TestFairShareAllocator_Property_FullSatisfactionWhenCapacitySufficient(t *testing.T) {
	// Property: If total demand <= capacity, everyone gets their full demand
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

		for user, allocation := range result {
			demand := demands[user]
			assert.Equal(t, demand, allocation,
				"iteration %d: user %s should get full demand", i, user)
		}
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
