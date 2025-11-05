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

package manager

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestActionLock_AcquireAndRelease(t *testing.T) {
	lock := NewActionLock()
	ctx := context.Background()

	// Acquire the lock
	newCtx, err := lock.Acquire(ctx, "test-operation")
	require.NoError(t, err)

	// Context should be different
	assert.NotEqual(t, ctx, newCtx)

	// Release the lock
	lock.Release(newCtx)
}

func TestAssertActionLockHeld_WhenHeld(t *testing.T) {
	lock := NewActionLock()
	ctx := context.Background()

	newCtx, err := lock.Acquire(ctx, "test-operation")
	require.NoError(t, err)
	defer lock.Release(newCtx)

	// Assertion should pass
	assert.NoError(t, AssertActionLockHeld(newCtx))
}

func TestAssertActionLockHeld_NoLockInfo(t *testing.T) {
	ctx := context.Background()

	// Assertion should fail for context without lock info
	assert.Error(t, AssertActionLockHeld(ctx))
}

func TestAssertActionLockHeld_AfterRelease(t *testing.T) {
	lock := NewActionLock()
	ctx := context.Background()

	newCtx, err := lock.Acquire(ctx, "test-operation")
	require.NoError(t, err)

	// Release the lock
	lock.Release(newCtx)

	// Assertion should fail after release
	assert.Error(t, AssertActionLockHeld(newCtx))
}

func TestActionLock_ReleasePanicsWithoutLockInfo(t *testing.T) {
	lock := NewActionLock()
	ctx := context.Background()

	// Should panic when releasing context without lock info
	assert.Panics(t, func() {
		lock.Release(ctx)
	})
}

func TestActionLock_ReleasePanicsTwice(t *testing.T) {
	lock := NewActionLock()
	ctx := context.Background()

	newCtx, err := lock.Acquire(ctx, "test-operation")
	require.NoError(t, err)

	// First release should succeed
	lock.Release(newCtx)

	// Second release should panic
	assert.Panics(t, func() {
		lock.Release(newCtx)
	})
}

func TestActionLock_AcquireFailsWhenAlreadyHeld(t *testing.T) {
	lock := NewActionLock()
	ctx := context.Background()

	// First acquire
	newCtx, err := lock.Acquire(ctx, "test-operation-1")
	require.NoError(t, err)
	defer lock.Release(newCtx)

	// Try to acquire again with the context that already holds it
	_, err = lock.Acquire(newCtx, "test-operation-2")
	assert.Error(t, err)
}

func TestActionLock_ContextCancellation(t *testing.T) {
	lock := NewActionLock()
	ctx, cancel := context.WithCancel(context.Background())

	// First acquire the lock with another goroutine
	firstCtx, err := lock.Acquire(context.Background(), "blocking-operation")
	require.NoError(t, err)

	// Try to acquire with cancelled context (should fail immediately)
	cancel()
	_, err = lock.Acquire(ctx, "test-operation")
	assert.Error(t, err)

	// Clean up
	lock.Release(firstCtx)
}

func TestActionLock_ConcurrentAcquire(t *testing.T) {
	lock := NewActionLock()
	const numGoroutines = 10

	var wg sync.WaitGroup
	var mu sync.Mutex
	successCount := 0
	var order []int

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			ctx := context.Background()
			newCtx, err := lock.Acquire(ctx, fmt.Sprintf("operation-%d", id))
			if !assert.NoError(t, err) {
				return
			}

			// Critical section - should only be accessed by one goroutine at a time
			mu.Lock()
			successCount++
			order = append(order, id)
			mu.Unlock()

			// Hold the lock briefly
			time.Sleep(10 * time.Millisecond)

			lock.Release(newCtx)
		}(i)
	}

	wg.Wait()

	// All goroutines should have successfully acquired the lock
	assert.Equal(t, numGoroutines, successCount)

	// All goroutines should have executed
	assert.Len(t, order, numGoroutines)
}

func TestActionLock_AcquireAfterRelease(t *testing.T) {
	lock := NewActionLock()
	ctx := context.Background()

	// First acquisition
	ctx1, err := lock.Acquire(ctx, "operation-1")
	require.NoError(t, err)
	lock.Release(ctx1)

	// Should be able to acquire again after release
	ctx2, err := lock.Acquire(ctx, "operation-2")
	require.NoError(t, err)
	lock.Release(ctx2)

	// Should be able to acquire again after release
	ctx3, err := lock.Acquire(ctx1, "operation-3")
	require.NoError(t, err)
	lock.Release(ctx3)
}

func TestActionLock_ReleasePanicsWithWrongContext(t *testing.T) {
	lock := NewActionLock()
	ctx := context.Background()

	// Acquire with one operation
	ctx1, err := lock.Acquire(ctx, "operation-1")
	require.NoError(t, err)

	// Acquire again after releasing (to get a different lock ID)
	lock.Release(ctx1)

	ctx2, err := lock.Acquire(ctx, "operation-2")
	require.NoError(t, err)
	defer lock.Release(ctx2)

	// Try to release with old context (should panic)
	assert.Panics(t, func() {
		lock.Release(ctx1)
	})
}
