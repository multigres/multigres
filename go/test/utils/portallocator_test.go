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

	"github.com/stretchr/testify/require"
)

// TestGetFreePort_NoDuplicates verifies that GetFreePort never returns
// the same port twice within a single test, even when called rapidly
// in succession.
func TestGetFreePort_NoDuplicates(t *testing.T) {
	// Allocate many ports rapidly - this reproduces the scenario where
	// we saw port 41917 allocated twice in cluster_test.go
	const numPorts = 20
	ports := make([]int, numPorts)
	for i := 0; i < numPorts; i++ {
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
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- GetFreePort(t)
		}()
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
