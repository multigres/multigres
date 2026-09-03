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
	for user, pool := range *pools {
		regularDemands[user] = pool.RegularDemand()
		reservedDemands[user] = pool.ReservedDemand()
	}

	// 2. Compute fair allocations
	regularAllocs := m.regularAllocator.Allocate(regularDemands)
	reservedAllocs := m.reservedAllocator.Allocate(reservedDemands)

	// 3. Apply new capacities to each pool
	for user, pool := range *pools {
		regularCap := regularAllocs[user]
		reservedCap := reservedAllocs[user]

		m.logger.DebugContext(ctx, "rebalance user",
			"user", user,
			"regular_demand", regularDemands[user],
			"reserved_demand", reservedDemands[user],
			"regular_cap", regularCap,
			"reserved_cap", reservedCap)

		if err := pool.SetCapacity(ctx, regularCap, reservedCap); err != nil {
			m.logger.WarnContext(ctx, "failed to set capacity",
				"user", user,
				"regular_cap", regularCap,
				"reserved_cap", reservedCap,
				"error", err)
		}
	}

	// 4. Garbage collect inactive pools
	m.garbageCollectInactivePools(ctx)
}

// garbageCollectInactivePools removes user pools that have been inactive
// longer than the configured timeout.
//
// Removal is atomic with respect to acquisitions: UserPool.tryMarkInactive
// decides "idle" and blocks new borrows in one step, the replacement snapshot
// is published before any pool is closed (so a caller retrying
// ErrPoolClosed lands on a fresh pool, not the one being torn down), and
// Close runs outside createMu so it can never stall pool creation.
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

	// Lock-free pre-scan so the common case (nothing to collect) takes no lock.
	var candidates []string
	for user, pool := range *pools {
		if pool.LastActivity() < cutoff && !pool.HasCheckedOutConns() {
			candidates = append(candidates, user)
		}
	}
	if len(candidates) == 0 {
		return
	}

	// Claim and unpublish under createMu (copy-on-write, same as creation).
	m.createMu.Lock()
	pools = m.userPoolsSnapshot.Load() // never nil once Open has run
	newPools := make(map[string]*UserPool, len(*pools))
	maps.Copy(newPools, *pools)

	var removed []*UserPool
	for _, user := range candidates {
		pool, ok := newPools[user]
		if !ok {
			continue
		}
		// Authoritative check: fails if an acquisition is in flight or the
		// pool was touched/borrowed since the pre-scan.
		if !pool.tryMarkInactive(cutoff) {
			continue
		}
		delete(newPools, user)
		removed = append(removed, pool)
	}
	if len(removed) > 0 {
		m.userPoolsSnapshot.Store(&newPools)
	}
	m.createMu.Unlock()

	if len(removed) == 0 {
		return
	}

	// Close outside createMu. The pools are unpublished and refuse new
	// acquisitions, and they hold no checked-out connections, so Close does
	// not wait on drain.
	for _, pool := range removed {
		pool.Close()
		m.logger.InfoContext(ctx, "garbage collected inactive user pool",
			"user", pool.Username(),
			"inactive_duration", time.Duration(now-pool.LastActivity()))
	}
	m.logger.InfoContext(ctx, "garbage collection complete",
		"removed_pools", len(removed),
		"remaining_pools", len(newPools))
}
