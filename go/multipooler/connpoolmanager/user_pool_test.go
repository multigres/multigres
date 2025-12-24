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
	"log/slog"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUserPoolConcurrentClosedCheck(t *testing.T) {
	pool := &UserPool{
		username: "testuser",
		logger:   slog.Default(),
		closed:   false,
	}

	var wg sync.WaitGroup
	closedCount := 0
	var countMu sync.Mutex

	// Simulate concurrent close attempts
	for range 10 {
		wg.Go(func() {
			pool.mu.Lock()
			if !pool.closed {
				pool.closed = true
				countMu.Lock()
				closedCount++
				countMu.Unlock()
			}
			pool.mu.Unlock()
		})
	}

	wg.Wait()

	// Only one goroutine should have successfully closed
	assert.Equal(t, 1, closedCount)
	assert.True(t, pool.closed)
}

func TestUserPoolMultipleUsernameAccess(t *testing.T) {
	// Test that Username() is safe to call multiple times
	pool := &UserPool{
		username: "concurrent_user",
	}

	var wg sync.WaitGroup
	results := make([]string, 10)

	for i := range 10 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = pool.Username()
		}(i)
	}

	wg.Wait()

	// All results should be the same
	for _, result := range results {
		assert.Equal(t, "concurrent_user", result)
	}
}
