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

package connstate

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSettingsCacheGetOrCreate(t *testing.T) {
	cache := NewSettingsCache()

	vars := map[string]string{
		"timezone":    "UTC",
		"search_path": "public",
	}

	// First call should create a new Settings
	s1 := cache.GetOrCreate(vars)
	require.NotNil(t, s1)
	assert.NotZero(t, s1.Bucket())

	// Second call with same vars should return the same pointer
	s2 := cache.GetOrCreate(vars)
	assert.Same(t, s1, s2, "same vars should return same pointer")
	assert.Equal(t, s1.Bucket(), s2.Bucket())

	// Cache size should be 1
	assert.Equal(t, 1, cache.Size())
}

func TestSettingsCacheDifferentSettings(t *testing.T) {
	cache := NewSettingsCache()

	vars1 := map[string]string{"timezone": "UTC"}
	vars2 := map[string]string{"timezone": "America/New_York"}

	s1 := cache.GetOrCreate(vars1)
	s2 := cache.GetOrCreate(vars2)

	// Different settings should have different pointers
	assert.NotSame(t, s1, s2)

	// Different buckets
	assert.NotEqual(t, s1.Bucket(), s2.Bucket())

	// Cache size should be 2
	assert.Equal(t, 2, cache.Size())
}

func TestSettingsCacheEmptyVars(t *testing.T) {
	cache := NewSettingsCache()

	// Empty vars should return nil
	assert.Nil(t, cache.GetOrCreate(map[string]string{}))

	// Nil vars should also return nil
	assert.Nil(t, cache.GetOrCreate(nil))

	// Cache size should be 0
	assert.Equal(t, 0, cache.Size())
}

func TestSettingsCacheKeyOrder(t *testing.T) {
	cache := NewSettingsCache()

	// Different order of keys in map should still match
	vars1 := map[string]string{"a": "1", "b": "2", "c": "3"}
	vars2 := map[string]string{"c": "3", "a": "1", "b": "2"}

	s1 := cache.GetOrCreate(vars1)
	s2 := cache.GetOrCreate(vars2)

	// Should return the same pointer since keys are sorted
	assert.Same(t, s1, s2, "key order should not matter")
}

func TestSettingsCacheConcurrent(t *testing.T) {
	cache := NewSettingsCache()
	vars := map[string]string{"timezone": "UTC"}

	var wg sync.WaitGroup
	results := make([]*Settings, 100)

	// Launch 100 concurrent goroutines all requesting the same settings
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = cache.GetOrCreate(vars)
		}(i)
	}

	wg.Wait()

	// All results should be the same pointer
	first := results[0]
	for i, s := range results {
		assert.Same(t, first, s, "result[%d] should be same pointer as result[0]", i)
	}

	// Cache should have exactly 1 entry
	assert.Equal(t, 1, cache.Size())
}

func TestSettingsCacheClear(t *testing.T) {
	cache := NewSettingsCache()

	cache.GetOrCreate(map[string]string{"a": "1"})
	cache.GetOrCreate(map[string]string{"b": "2"})

	assert.Equal(t, 2, cache.Size())

	cache.Clear()

	assert.Equal(t, 0, cache.Size())
	assert.Equal(t, int64(0), cache.Hits())
	assert.Equal(t, int64(0), cache.Misses())
}

func TestSettingsCacheLRUEviction(t *testing.T) {
	cache := NewSettingsCacheWithSize(3)

	// Fill the cache
	s1 := cache.GetOrCreate(map[string]string{"a": "1"})
	s2 := cache.GetOrCreate(map[string]string{"b": "2"})
	s3 := cache.GetOrCreate(map[string]string{"c": "3"})

	assert.Equal(t, 3, cache.Size())

	// Add one more - should evict the oldest (s1)
	s4 := cache.GetOrCreate(map[string]string{"d": "4"})

	assert.Equal(t, 3, cache.Size())

	// s1 should be evicted, getting same vars should create new Settings
	s1New := cache.GetOrCreate(map[string]string{"a": "1"})
	assert.NotSame(t, s1, s1New, "s1 should be evicted")

	// After adding s1New, s2 was evicted (it was the oldest)
	// s3 and s4 should still be the same pointers
	assert.Same(t, s3, cache.GetOrCreate(map[string]string{"c": "3"}))
	assert.Same(t, s4, cache.GetOrCreate(map[string]string{"d": "4"}))

	// s2 was evicted when s1New was added
	s2New := cache.GetOrCreate(map[string]string{"b": "2"})
	assert.NotSame(t, s2, s2New, "s2 should be evicted")
}

func TestSettingsCacheLRUAccess(t *testing.T) {
	cache := NewSettingsCacheWithSize(3)

	// Fill the cache
	s1 := cache.GetOrCreate(map[string]string{"a": "1"})
	cache.GetOrCreate(map[string]string{"b": "2"})
	cache.GetOrCreate(map[string]string{"c": "3"})

	// Access s1 to move it to front
	cache.GetOrCreate(map[string]string{"a": "1"})

	// Add one more - should evict s2 (oldest after s1 was accessed)
	cache.GetOrCreate(map[string]string{"d": "4"})

	// s1 should still be cached because it was recently accessed
	assert.Same(t, s1, cache.GetOrCreate(map[string]string{"a": "1"}))
}

func TestSettingsCacheMetrics(t *testing.T) {
	cache := NewSettingsCache()

	// First access is a miss
	cache.GetOrCreate(map[string]string{"a": "1"})
	assert.Equal(t, int64(1), cache.Misses())
	assert.Equal(t, int64(0), cache.Hits())

	// Second access is a hit
	cache.GetOrCreate(map[string]string{"a": "1"})
	assert.Equal(t, int64(1), cache.Misses())
	assert.Equal(t, int64(1), cache.Hits())

	// Different settings is a miss
	cache.GetOrCreate(map[string]string{"b": "2"})
	assert.Equal(t, int64(2), cache.Misses())

	// Clear should reset metrics
	cache.Clear()
	assert.Equal(t, int64(0), cache.Hits())
	assert.Equal(t, int64(0), cache.Misses())
}

func TestSettingsCacheMaxSize(t *testing.T) {
	cache := NewSettingsCacheWithSize(100)
	assert.Equal(t, 100, cache.MaxSize())

	// Test default size
	defaultCache := NewSettingsCache()
	assert.Equal(t, DefaultSettingsCacheSize, defaultCache.MaxSize())

	// Test invalid size falls back to default
	invalidCache := NewSettingsCacheWithSize(0)
	assert.Equal(t, DefaultSettingsCacheSize, invalidCache.MaxSize())
}

func TestSettingsQueries(t *testing.T) {
	cache := NewSettingsCache()

	vars := map[string]string{
		"timezone":    "UTC",
		"search_path": "public",
	}

	s := cache.GetOrCreate(vars)

	// ApplyQuery should be computed from Vars
	assert.NotEmpty(t, s.ApplyQuery())

	// Should contain both settings (sorted alphabetically)
	expected := "SET SESSION search_path = 'public'; SET SESSION timezone = 'UTC'"
	assert.Equal(t, expected, s.ApplyQuery())

	// ResetQuery should return RESET ALL
	assert.Equal(t, "RESET ALL", s.ResetQuery())
}

func TestSettingsPointerEqualityForPooling(t *testing.T) {
	cache := NewSettingsCache()

	vars := map[string]string{"timezone": "UTC"}

	// Simulate multiple requests with the same settings
	s1 := cache.GetOrCreate(vars)
	s2 := cache.GetOrCreate(vars)

	// Create ConnectionState objects with these settings
	state1 := NewConnectionStateWithSettings(s1)
	state2 := NewConnectionStateWithSettings(s2)

	// With cached settings, pointer equality should work
	assert.Same(t, state1.GetSettings(), state2.GetSettings())

	// Both should have the same bucket
	assert.Equal(t, state1.Bucket(), state2.Bucket())
}

func BenchmarkSettingsCacheGetOrCreate(b *testing.B) {
	cache := NewSettingsCache()
	vars := map[string]string{
		"timezone":    "UTC",
		"search_path": "public",
	}

	// Pre-populate the cache
	cache.GetOrCreate(vars)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cache.GetOrCreate(vars)
		}
	})
}

func BenchmarkSettingsPointerEquality(b *testing.B) {
	cache := NewSettingsCache()
	vars := map[string]string{"timezone": "UTC"}
	s := cache.GetOrCreate(vars)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Pointer comparison
		_ = s == cache.GetOrCreate(vars)
	}
}
