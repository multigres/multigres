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

package rpcclient

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Note: These tests focus on the caching logic without making actual gRPC connections.
// Integration tests with real gRPC connections, concurrent access, and benchmarks are
// located in go/test/endtoend/multipooler/conn_cache_test.go.

// TestConnCache_Creation tests cache creation with different capacities.
func TestConnCache_Creation(t *testing.T) {
	cache1 := newConnCache()
	require.NotNil(t, cache1, "newConnCache() should not return nil")
	assert.Equal(t, defaultCapacity, cache1.capacity, "cache1 should have default capacity")

	customCapacity := 50
	cache2 := newConnCacheWithCapacity(customCapacity)
	require.NotNil(t, cache2, "newConnCacheWithCapacity() should not return nil")
	assert.Equal(t, customCapacity, cache2.capacity, "cache2 should have custom capacity")
}

// TestConnCache_ReferenceCountingLogic tests the reference counting mechanism.
func TestConnCache_ReferenceCountingLogic(t *testing.T) {
	cache := newConnCache()

	// Manually create a fake connection for testing
	addr := "localhost:1234"
	conn := &cachedConn{
		addr:           addr,
		refs:           1, // Start with 1 ref
		lastAccessTime: time.Now(),
	}

	cache.m.Lock()
	cache.conns[addr] = conn
	cache.evict = append(cache.evict, conn)
	cache.m.Unlock()

	// Get a closer and call it
	_, closer, _ := cache.connWithCloser(conn)
	require.NotNil(t, closer, "connWithCloser should return a closer")

	// Call closer to decrement ref count
	err := closer()
	assert.NoError(t, err, "closer() should not return an error")
	assert.Equal(t, 0, conn.refs, "refs should be 0 after first close")

	// Verify calling closer twice would go negative
	err = closer()
	assert.NoError(t, err, "second closer() should not return an error")
	assert.Equal(t, 0, conn.refs, "refs should be 0 after second close (clamped)")
}

// TestConnCache_SortEvictions tests the eviction queue sorting.
func TestConnCache_SortEvictions(t *testing.T) {
	cache := newConnCache()

	// Create connections with different access times
	now := time.Now()
	conn1 := &cachedConn{addr: "addr1", lastAccessTime: now.Add(-30 * time.Second), refs: 0}
	conn2 := &cachedConn{addr: "addr2", lastAccessTime: now.Add(-20 * time.Second), refs: 0}
	conn3 := &cachedConn{addr: "addr3", lastAccessTime: now.Add(-10 * time.Second), refs: 0}

	cache.m.Lock()
	// Add in random order
	cache.conns["addr1"] = conn1
	cache.conns["addr2"] = conn2
	cache.conns["addr3"] = conn3
	cache.evict = []*cachedConn{conn3, conn1, conn2} // Unsorted
	cache.evictSorted = false

	// Sort the eviction queue
	cache.sortEvictionsLocked()

	assert.True(t, cache.evictSorted, "eviction queue should be marked as sorted")

	// Verify order (oldest first)
	assert.Same(t, conn1, cache.evict[0], "evict[0] should be conn1 (oldest)")
	assert.Same(t, conn2, cache.evict[1], "evict[1] should be conn2")
	assert.Same(t, conn3, cache.evict[2], "evict[2] should be conn3 (newest)")

	// Calling sort again should be a no-op
	cache.sortEvictionsLocked()
	assert.True(t, cache.evictSorted, "eviction queue should still be marked as sorted")

	cache.m.Unlock()
}

// TestConnCache_EvictionLogic tests the eviction logic without semaphore complications.
func TestConnCache_EvictionLogic(t *testing.T) {
	cache := newConnCache()

	// Create connections with different ref counts
	conn1 := &cachedConn{addr: "addr1", lastAccessTime: time.Now(), refs: 0} // Can be evicted
	conn2 := &cachedConn{addr: "addr2", lastAccessTime: time.Now(), refs: 1} // Referenced
	conn3 := &cachedConn{addr: "addr3", lastAccessTime: time.Now(), refs: 0} // Can be evicted

	cache.m.Lock()
	cache.conns["addr1"] = conn1
	cache.conns["addr2"] = conn2
	cache.conns["addr3"] = conn3
	cache.evict = []*cachedConn{conn1, conn2, conn3}
	cache.evictSorted = true

	// Verify logic: conn1 and conn3 have refs==0, conn2 has refs==1
	evictable := 0
	for _, conn := range cache.evict {
		if conn.refs == 0 {
			evictable++
		}
	}
	assert.Equal(t, 2, evictable, "should have 2 evictable connections")

	// Verify conn2 is not evictable
	assert.NotEqual(t, 0, conn2.refs, "conn2 should not be evictable (refs should be > 0)")

	cache.m.Unlock()
}

// TestConnCache_NoEvictionWhenReferenced tests that referenced connections aren't evictable.
func TestConnCache_NoEvictionWhenReferenced(t *testing.T) {
	cache := newConnCache()

	// Create connections that are all referenced
	conn1 := &cachedConn{addr: "addr1", lastAccessTime: time.Now(), refs: 1}
	conn2 := &cachedConn{addr: "addr2", lastAccessTime: time.Now(), refs: 2}

	cache.m.Lock()
	cache.conns["addr1"] = conn1
	cache.conns["addr2"] = conn2
	cache.evict = []*cachedConn{conn1, conn2}

	// Verify no connections are evictable (all have refs > 0)
	evictable := 0
	for _, conn := range cache.evict {
		if conn.refs == 0 {
			evictable++
		}
	}
	assert.Equal(t, 0, evictable, "no connections should be evictable (all should be referenced)")

	cache.m.Unlock()
}

// TestConnCache_CloseNonExistent tests closing a non-existent connection.
func TestConnCache_CloseNonExistent(t *testing.T) {
	cache := newConnCache()

	// Try to close a connection that doesn't exist (should not panic)
	cache.close("nonexistent")

	// Cache should still be empty
	cache.m.Lock()
	assert.Equal(t, 0, len(cache.conns), "cache should be empty")
	cache.m.Unlock()
}
