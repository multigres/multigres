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

package plancache

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/services/multigateway/engine"
)

// newForTest wraps the exported NewForTest for convenience in tests.
func newForTest(capacity int) *PlanCache {
	return NewForTest(capacity)
}

func makePlan(sql string) *engine.Plan {
	return engine.NewPlan(sql, engine.NewRoute("default", "0", sql, nil))
}

func TestGetPut(t *testing.T) {
	c := newForTest(1000)
	defer c.Close()
	ctx := context.Background()

	plan := makePlan("SELECT * FROM users WHERE id = $1")
	c.Put("SELECT * FROM users WHERE id = $1", plan)

	// theine processes writes asynchronously; give it a moment
	time.Sleep(50 * time.Millisecond)

	got, ok := c.Get(ctx, "SELECT * FROM users WHERE id = $1")
	require.True(t, ok)
	assert.Equal(t, plan, got)

	_, ok = c.Get(ctx, "nonexistent")
	assert.False(t, ok)
}

func TestUpdateExistingEntry(t *testing.T) {
	c := newForTest(1000)
	defer c.Close()
	ctx := context.Background()

	plan1 := makePlan("q1-v1")
	plan2 := makePlan("q1-v2")

	c.Put("q1", plan1)
	time.Sleep(50 * time.Millisecond)

	c.Put("q1", plan2)
	time.Sleep(50 * time.Millisecond)

	got, ok := c.Get(ctx, "q1")
	require.True(t, ok)
	assert.Equal(t, plan2, got)
}

func TestInvalidate(t *testing.T) {
	c := newForTest(1000)
	defer c.Close()
	ctx := context.Background()

	c.Put("q1", makePlan("q1"))
	c.Put("q2", makePlan("q2"))
	time.Sleep(50 * time.Millisecond)

	// Verify entries exist
	_, ok := c.Get(ctx, "q1")
	require.True(t, ok)

	// Invalidate all entries
	c.Invalidate()

	// Previous entries should now be stale (treated as misses)
	_, ok = c.Get(ctx, "q1")
	assert.False(t, ok, "q1 should be stale after Invalidate")

	_, ok = c.Get(ctx, "q2")
	assert.False(t, ok, "q2 should be stale after Invalidate")

	// New entries should work at the new epoch
	c.Put("q3", makePlan("q3"))
	time.Sleep(50 * time.Millisecond)

	_, ok = c.Get(ctx, "q3")
	assert.True(t, ok, "q3 should be found after Invalidate")
}

func TestMultipleInvalidations(t *testing.T) {
	c := newForTest(1000)
	defer c.Close()
	ctx := context.Background()

	c.Put("q1", makePlan("q1"))
	time.Sleep(50 * time.Millisecond)

	_, ok := c.Get(ctx, "q1")
	require.True(t, ok)

	// First invalidation
	c.Invalidate()
	_, ok = c.Get(ctx, "q1")
	assert.False(t, ok)

	// Insert at new epoch
	c.Put("q2", makePlan("q2"))
	time.Sleep(50 * time.Millisecond)

	_, ok = c.Get(ctx, "q2")
	require.True(t, ok)

	// Second invalidation
	c.Invalidate()
	_, ok = c.Get(ctx, "q2")
	assert.False(t, ok)
}

func TestDisabledCache(t *testing.T) {
	c := New(0)
	ctx := context.Background()

	c.Put("q1", makePlan("q1"))
	_, ok := c.Get(ctx, "q1")
	assert.False(t, ok, "disabled cache should always miss")
	assert.Equal(t, 0, c.Len())
}

func TestMetrics(t *testing.T) {
	c := newForTest(1000)
	defer c.Close()
	ctx := context.Background()

	c.Put("q1", makePlan("q1"))
	time.Sleep(50 * time.Millisecond)

	// Hit
	_, ok := c.Get(ctx, "q1")
	require.True(t, ok)

	// Miss
	_, _ = c.Get(ctx, "nonexistent")

	assert.True(t, c.Hits() >= 1, "expected at least 1 hit")
	assert.True(t, c.Misses() >= 1, "expected at least 1 miss")
}

func TestConcurrentAccess(t *testing.T) {
	c := newForTest(10000)
	defer c.Close()
	ctx := context.Background()

	var wg sync.WaitGroup

	// Concurrent writes
	for i := range 50 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("q%d", i)
			c.Put(key, makePlan(key))
		}(i)
	}

	// Concurrent reads
	for range 50 {
		wg.Go(func() {
			c.Get(ctx, "q1")
		})
	}

	// Concurrent invalidation
	wg.Go(func() {
		c.Invalidate()
	})

	wg.Wait()
	// Just verify no panic/race
}
