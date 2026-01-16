// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestGetFreePort_NoDuplicates verifies that GetFreePort never returns
// the same port twice within a single test, even when called rapidly
// in succession.
func TestGetFreePort_NoDuplicates(t *testing.T) {
	// Allocate many ports rapidly
	const numPorts = 100
	ports := make([]int, numPorts)
	for i := range numPorts {
		ports[i] = GetFreePort(t)
	}

	// Verify all ports are unique
	seen := make(map[int]bool)
	for _, port := range ports {
		require.False(t, seen[port], "duplicate port allocated: %d", port)
		seen[port] = true
	}
}

// TestGetFreePort_Concurrent verifies that GetFreePort is safe to call
// from multiple goroutines simultaneously and never returns duplicate
// ports across concurrent allocations.
func TestGetFreePort_Concurrent(t *testing.T) {
	const numGoroutines = 50
	var wg sync.WaitGroup
	results := make(chan int, numGoroutines)

	// Spawn multiple goroutines allocating ports concurrently
	for range numGoroutines {
		wg.Go(func() {
			results <- GetFreePort(t)
		})
	}

	wg.Wait()
	close(results)

	// Verify no duplicates across all concurrent allocations
	seen := make(map[int]bool)
	for port := range results {
		require.False(t, seen[port], "duplicate port from concurrent allocation: %d", port)
		seen[port] = true
	}
}

// TestGetFreePort_CoordinatorRestart verifies that calling cleanup (shutdown)
// followed by GetFreePort works correctly: the old coordinator stops, and a new
// coordinator starts with a fresh lease table.
func TestGetFreePort_CoordinatorRestart(t *testing.T) {
	sockPath := coordDir() + "/port-coordinator.sock"

	// Start with clean slate - ensure no coordinator from previous tests
	shutdownCoordinator()
	require.Eventually(t, func() bool {
		_, ok := tryRequestPort(t, sockPath)
		return !ok
	}, 2*time.Second, 100*time.Millisecond, "coordinator should be stopped initially")

	// First allocation starts the coordinator
	port1 := GetFreePort(t)
	require.Greater(t, port1, 0)

	// Shut down coordinator (simulating test cleanup)
	shutdownCoordinator()

	// Wait for old coordinator to fully stop
	require.Eventually(t, func() bool {
		_, ok := tryRequestPort(t, sockPath)
		return !ok
	}, 2*time.Second, 100*time.Millisecond, "old coordinator should stop after shutdown")

	// Next allocation should restart the coordinator with fresh lease table
	port2 := GetFreePort(t)
	require.Greater(t, port2, 0)

	// Verify new coordinator is running
	port3, ok := tryRequestPort(t, sockPath)
	require.True(t, ok, "new coordinator should be running after restart")
	require.Greater(t, port3, 0)

	// Verify all ports are unique
	require.NotEqual(t, port1, port2, "ports should be unique")
	require.NotEqual(t, port1, port3, "ports should be unique")
	require.NotEqual(t, port2, port3, "ports should be unique")
}

// TestGetFreePort_ConcurrentShutdownAndAllocate is a stress test that verifies
// the system handles concurrent port allocation and coordinator shutdown/restart
// gracefully without errors or returning invalid ports.
func TestGetFreePort_ConcurrentShutdownAndAllocate(t *testing.T) {
	const testDuration = 2 * time.Second
	const allocateInterval = 50 * time.Millisecond
	const shutdownInterval = 300 * time.Millisecond

	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	// Channel to collect all allocated ports
	portsCh := make(chan int, 1000)

	// Goroutine 1: Request ports every 50ms
	wg.Go(func() {
		ticker := time.NewTicker(allocateInterval)
		defer ticker.Stop()

		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				port := GetFreePort(t)
				portsCh <- port
			}
		}
	})

	// Goroutine 2: Shutdown coordinator every 300ms
	wg.Go(func() {
		ticker := time.NewTicker(shutdownInterval)
		defer ticker.Stop()

		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				shutdownCoordinator()
			}
		}
	})

	// Let it run for the test duration
	time.Sleep(testDuration)
	close(stopCh)
	wg.Wait()
	close(portsCh)

	// Collect and verify all ports
	var ports []int
	for port := range portsCh {
		ports = append(ports, port)
	}

	// Assert we got some ports
	require.NotEmpty(t, ports, "should have allocated at least some ports")

	// Assert all ports are non-zero
	for i, port := range ports {
		require.Greater(t, port, 0, "port at index %d should be greater than 0", i)
	}
}
