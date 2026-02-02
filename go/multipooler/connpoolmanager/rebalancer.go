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
	"context"
	"maps"
	"time"
)

// startRebalancer starts the background rebalancer goroutine.
// The rebalancer periodically:
//  1. Collects demand from DemandTrackers
//  2. Computes fair allocations using FairShareAllocator
//  3. Applies new capacities via UserPool.SetCapacity()
//  4. Garbage collects inactive user pools
func (m *Manager) startRebalancer() {
	m.rebalancerWg.Add(1)
	go m.rebalanceLoop()
}

// rebalanceLoop is the main loop for the rebalancer goroutine.
func (m *Manager) rebalanceLoop() {
	defer m.rebalancerWg.Done()

	interval := m.config.RebalanceInterval()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-m.rebalancerCtx.Done():
			return
		case <-ticker.C:
			m.rebalance(m.rebalancerCtx)
		}
	}
}

// rebalance performs one rebalance cycle:
// - Collects demand from all user pools
// - Computes fair allocations
// - Applies new capacities
// - Garbage collects inactive pools
func (m *Manager) rebalance(ctx context.Context) {
	pools := m.userPoolsSnapshot.Load()
	if pools == nil || len(*pools) == 0 {
		return
	}

	// 1. Collect demands from all user pools
	regularDemands := make(map[string]int64, len(*pools))
	reservedDemands := make(map[string]int64, len(*pools))
	var totalRegularDemand, totalReservedDemand int64
	for user, pool := range *pools {
		rd := pool.RegularDemand()
		sd := pool.ReservedDemand()
		regularDemands[user] = rd
		reservedDemands[user] = sd
		totalRegularDemand += rd
		totalReservedDemand += sd
	}

	// 2. Compute fair allocations
	regularAllocs := m.regularAllocator.Allocate(regularDemands)
	reservedAllocs := m.reservedAllocator.Allocate(reservedDemands)

	// 3. Apply new capacities to each pool and collect stats
	var totalRegularCap, totalReservedCap int64
	for user, pool := range *pools {
		regularCap := regularAllocs[user]
		reservedCap := reservedAllocs[user]
		totalRegularCap += regularCap
		totalReservedCap += reservedCap

		if err := pool.SetCapacity(ctx, regularCap, reservedCap); err != nil {
			m.logger.WarnContext(ctx, "failed to set capacity",
				"user", user,
				"regular_cap", regularCap,
				"reserved_cap", reservedCap,
				"error", err)
		}
	}

	// Log rebalance summary at debug level (can be expensive with many users)
	m.logger.DebugContext(ctx, "rebalance complete",
		"user_count", len(*pools),
		"total_regular_demand", totalRegularDemand,
		"total_reserved_demand", totalReservedDemand,
		"total_regular_allocated", totalRegularCap,
		"total_reserved_allocated", totalReservedCap,
		"regular_budget", m.regularAllocator.Capacity(),
		"reserved_budget", m.reservedAllocator.Capacity(),
	)

	// 4. Garbage collect inactive pools
	m.garbageCollectInactivePools(ctx)
}

// garbageCollectInactivePools removes user pools that have been inactive
// longer than the configured timeout.
func (m *Manager) garbageCollectInactivePools(ctx context.Context) {
	inactiveTimeout := m.config.InactiveTimeout()
	if inactiveTimeout <= 0 {
		return
	}

	pools := m.userPoolsSnapshot.Load()
	if pools == nil || len(*pools) == 0 {
		return
	}

	now := time.Now().UnixNano()
	cutoff := now - inactiveTimeout.Nanoseconds()

	// Find inactive pools
	var inactiveUsers []string
	for user, pool := range *pools {
		if pool.LastActivity() < cutoff {
			inactiveUsers = append(inactiveUsers, user)
		}
	}

	if len(inactiveUsers) == 0 {
		return
	}

	// Remove inactive pools using copy-on-write
	m.createMu.Lock()
	defer m.createMu.Unlock()

	// Re-read snapshot with lock held
	pools = m.userPoolsSnapshot.Load()
	if pools == nil {
		return
	}

	// Create new map without inactive pools
	newPools := make(map[string]*UserPool, len(*pools)-len(inactiveUsers))
	maps.Copy(newPools, *pools)

	var closedCount int
	for _, user := range inactiveUsers {
		pool, ok := newPools[user]
		if !ok {
			continue
		}

		// Double-check activity timestamp (may have been updated since first check)
		if pool.LastActivity() >= cutoff {
			continue
		}

		// Close and remove the pool
		pool.Close()
		delete(newPools, user)
		closedCount++

		m.logger.InfoContext(ctx, "garbage collected inactive user pool",
			"user", user,
			"inactive_duration", time.Duration(now-pool.LastActivity()))
	}

	if closedCount > 0 {
		m.userPoolsSnapshot.Store(&newPools)
		m.logger.InfoContext(ctx, "garbage collection complete",
			"removed_pools", closedCount,
			"remaining_pools", len(newPools))
	}
}
