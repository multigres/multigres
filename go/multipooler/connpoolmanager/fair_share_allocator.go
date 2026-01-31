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

// FairShareAllocator distributes connection capacity among users using max-min fairness.
// It is agnostic of resource type - create separate instances for regular and reserved pools.
//
// The algorithm ensures:
//   - Each user gets at least 1 connection (hard floor)
//   - No user gets more than their demand
//   - Total allocation does not exceed capacity
//   - Remaining capacity is distributed fairly among unsatisfied users
type FairShareAllocator struct {
	capacity int64
}

// NewFairShareAllocator creates a new allocator with the given capacity budget.
func NewFairShareAllocator(capacity int64) *FairShareAllocator {
	return &FairShareAllocator{
		capacity: capacity,
	}
}

// Capacity returns the total capacity this allocator manages.
func (a *FairShareAllocator) Capacity() int64 {
	return a.capacity
}

// Allocate distributes capacity among users based on their demands using max-min fairness.
//
// The algorithm (progressive filling):
//  1. Start with all allocations at 0
//  2. Calculate fair share = remaining_capacity / unsatisfied_users
//  3. Give each unsatisfied user min(their_demand, fair_share)
//  4. Users whose demand is met become "satisfied"
//  5. Repeat with remaining capacity among unsatisfied users
func (a *FairShareAllocator) Allocate(demands map[string]int64) map[string]int64 {
	numUsers := len(demands)
	if numUsers == 0 {
		return make(map[string]int64)
	}

	// Initialize allocations to 0
	allocs := make(map[string]int64, numUsers)
	for user := range demands {
		allocs[user] = 0
	}

	// Track which users are still unsatisfied (allocation < demand)
	unsatisfied := make(map[string]bool, numUsers)
	for user := range demands {
		unsatisfied[user] = true
	}

	remaining := a.capacity

	// Progressive filling: keep distributing until no capacity or all satisfied
	for remaining > 0 && len(unsatisfied) > 0 {
		// Calculate fair share of remaining capacity
		fairShare := remaining / int64(len(unsatisfied))
		if fairShare == 0 {
			// Not enough capacity for everyone - give 1 to as many as possible
			fairShare = 1
		}

		// Track how much we actually allocate this round
		allocated := int64(0)
		newlySatisfied := make([]string, 0)

		for user := range unsatisfied {
			demand := demands[user]
			currentAlloc := allocs[user]
			remainingDemand := demand - currentAlloc

			// Ensure minimum 1 connection, even if demand is 0
			if demand <= 0 {
				remainingDemand = 1 - currentAlloc
			}

			if remainingDemand <= 0 {
				// Already satisfied
				newlySatisfied = append(newlySatisfied, user)
				continue
			}

			// Give min(remaining_demand, fair_share, remaining_capacity)
			give := min(remainingDemand, fairShare, remaining-allocated)
			if give <= 0 {
				continue
			}

			allocs[user] += give
			allocated += give

			// Check if now satisfied
			newDemand := demands[user]
			if newDemand <= 0 {
				newDemand = 1 // Minimum demand
			}
			if allocs[user] >= newDemand {
				newlySatisfied = append(newlySatisfied, user)
			}
		}

		// Remove satisfied users from unsatisfied set
		for _, user := range newlySatisfied {
			delete(unsatisfied, user)
		}

		remaining -= allocated

		// Safety: if we allocated nothing this round, break to avoid infinite loop
		if allocated == 0 {
			break
		}
	}

	return allocs
}
