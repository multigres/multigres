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

import "sync/atomic"

// FairShareAllocator distributes connection capacity among users using max-min fairness.
// It is agnostic of resource type - create separate instances for regular and reserved pools.
//
// The algorithm ensures:
//   - Each user gets at least minPerUser connections (floor to handle burst demand)
//   - No user gets more than their demand (unless demand < minPerUser)
//   - Total allocation does not exceed capacity
//   - Remaining capacity is distributed fairly among unsatisfied users
type FairShareAllocator struct {
	capacity   atomic.Int64 // adjusted each rebalance by the class split
	minPerUser int64
}

// NewFairShareAllocator creates a new allocator with the given capacity budget.
// minPerUser sets the minimum allocation per user to handle burst demand that
// point-in-time sampling might miss. This prevents capacity from being reduced
// too aggressively for light users who occasionally need concurrent connections.
func NewFairShareAllocator(capacity int64, minPerUser int64) *FairShareAllocator {
	if minPerUser < 1 {
		minPerUser = 1
	}
	a := &FairShareAllocator{minPerUser: minPerUser}
	a.capacity.Store(capacity)
	return a
}

// Capacity returns the total capacity this allocator manages.
func (a *FairShareAllocator) Capacity() int64 {
	return a.capacity.Load()
}

// SetCapacity changes the capacity budget for subsequent Allocate calls.
func (a *FairShareAllocator) SetCapacity(capacity int64) {
	a.capacity.Store(capacity)
}

// Allocate distributes capacity among users based on their demands using max-min fairness.
//
// The algorithm (progressive filling):
//  1. Start with all allocations at 0
//  2. Calculate fair share = remaining_capacity / unsatisfied_users
//  3. Give each unsatisfied user min(their_demand, fair_share)
//  4. Users whose demand is met become "satisfied"
//  5. Repeat with remaining capacity among unsatisfied users
//  6. If capacity remains after all demands are met, split it evenly for burst headroom
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

	// Track which users are still unsatisfied (allocation < effective demand)
	unsatisfied := make(map[string]bool, numUsers)
	for user := range demands {
		unsatisfied[user] = true
	}

	remaining := a.capacity.Load()

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
			// Effective demand is at least minPerUser to handle burst demand
			demand := max(demands[user], a.minPerUser)
			remainingDemand := demand - allocs[user]

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
			if allocs[user] >= demand {
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

	// If there's remaining capacity after all demands are met, distribute it evenly
	// among all users for burst headroom. This ensures users can handle sudden
	// traffic spikes without waiting for the next rebalance cycle.
	if remaining > 0 {
		extraPerUser := remaining / int64(numUsers)
		if extraPerUser > 0 {
			for user := range allocs {
				allocs[user] += extraPerUser
			}
		}
	}

	return allocs
}

// splitClassCapacity divides one shared budget between the regular and
// reserved pool classes according to demand. reservedRatio only fixes each
// class's nominal target; a class whose demand exceeds its target borrows
// whatever the other class is not using, so either class can reach almost the
// whole budget when the other is idle:
//
//  1. Each class gets min(demand, target).
//  2. Unused capacity is lent to demand above target.
//  3. Capacity nobody demands is split by reservedRatio as burst headroom.
//
// Each class then keeps a floor of min(minCap, its target) slots, where
// minCap is the number of user pools, so the per-user allocator can give
// every user a non-zero allocation in both classes (a zero-capacity pool
// refuses acquisitions instead of waiting). Capping the floor at the nominal
// target means borrowing never leaves a class worse off than the fixed split.
// The result sums to total for any budget >= 2; a budget of 1 overshoots by
// one rather than starve a class.
func splitClassCapacity(total int64, reservedRatio float64, regularDemand, reservedDemand, minCap int64) (regularCap, reservedCap int64) {
	regularTarget := max(int64(float64(total)*(1-reservedRatio)), 1)
	reservedTarget := max(total-regularTarget, 1)

	regularCap = min(max(regularDemand, 0), regularTarget)
	reservedCap = min(max(reservedDemand, 0), reservedTarget)
	spare := total - regularCap - reservedCap

	// Lend spare capacity to whichever class wants more than its target.
	// At most one class can be above target when spare > 0.
	if lend := min(regularDemand-regularCap, spare); lend > 0 {
		regularCap += lend
		spare -= lend
	}
	if lend := min(reservedDemand-reservedCap, spare); lend > 0 {
		reservedCap += lend
		spare -= lend
	}

	// Undemanded capacity is headroom, shared by the configured ratio.
	if spare > 0 {
		extraRegular := int64(float64(spare) * (1 - reservedRatio))
		regularCap += extraRegular
		reservedCap += spare - extraRegular
	}

	// Floor each class, taking the slots from the other class. Floors never
	// exceed targets, and targets sum to total, so this cannot overshoot.
	regularFloor := max(min(minCap, regularTarget), 1)
	reservedFloor := max(min(minCap, reservedTarget), 1)
	if regularCap < regularFloor {
		reservedCap = max(reservedCap-(regularFloor-regularCap), reservedFloor)
		regularCap = regularFloor
	}
	if reservedCap < reservedFloor {
		regularCap = max(regularCap-(reservedFloor-reservedCap), regularFloor)
		reservedCap = reservedFloor
	}
	return regularCap, reservedCap
}
