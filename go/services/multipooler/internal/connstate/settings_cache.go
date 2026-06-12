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
	"container/list"
	"sort"
	"strings"
	"sync"
)

// SettingsCacheKey is the type used for cache keys.
type SettingsCacheKey string

// SettingsCache provides a bounded LRU cache for Settings that ensures the same
// settings configuration always returns the same *Settings pointer. This enables:
// 1. Pointer equality for fast settings comparison
// 2. Consistent bucket assignment for same settings
// 3. Reduced memory allocation for repeated settings
//
// This follows the Vitess pattern where settings must be "interned" for optimal
// connection pool performance.
type SettingsCache struct {
	mu      sync.Mutex
	cache   map[SettingsCacheKey]*list.Element
	lru     *list.List
	maxSize int

	// bucketCounter assigns unique bucket numbers to new Settings.
	bucketCounter uint32

	// Metrics
	hits   int64
	misses int64
}

// cacheEntry holds the cached settings and its key for LRU eviction.
type cacheEntry struct {
	key      SettingsCacheKey
	settings *Settings
}

// NewSettingsCache creates a new SettingsCache with the specified max size.
// The maxSize must be > 0; the caller is responsible for providing a valid size.
func NewSettingsCache(maxSize int) *SettingsCache {
	return &SettingsCache{
		cache:   make(map[SettingsCacheKey]*list.Element),
		lru:     list.New(),
		maxSize: maxSize,
	}
}

// GetOrCreate returns a cached Settings for the given variables, or creates
// and caches a new one if it doesn't exist. This ensures that the same
// settings configuration always returns the same *Settings pointer.
func (c *SettingsCache) GetOrCreate(vars map[string]string) *Settings {
	if len(vars) == 0 {
		return nil
	}

	// Generate a deterministic cache key from the vars
	key := c.makeKey(vars)

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if already cached
	if elem, ok := c.cache[key]; ok {
		// Move to front (most recently used)
		c.lru.MoveToFront(elem)
		c.hits++
		return elem.Value.(*cacheEntry).settings
	}

	// Create new Settings with a unique bucket number
	c.misses++
	c.bucketCounter++
	s := NewSettings(vars, c.bucketCounter)

	// Add to cache
	entry := &cacheEntry{key: key, settings: s}
	elem := c.lru.PushFront(entry)
	c.cache[key] = elem

	// Evict oldest if over capacity
	for c.lru.Len() > c.maxSize {
		oldest := c.lru.Back()
		if oldest != nil {
			c.lru.Remove(oldest)
			delete(c.cache, oldest.Value.(*cacheEntry).key)
		}
	}

	return s
}

// makeKey generates a deterministic cache key from the settings variables.
// The key is created by sorting the variable names and concatenating them.
func (c *SettingsCache) makeKey(vars map[string]string) SettingsCacheKey {
	// Sort keys for deterministic ordering
	keys := make([]string, 0, len(vars))
	for k := range vars {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Build key string: "key1=value1;key2=value2;..."
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(';')
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(vars[k])
	}
	return SettingsCacheKey(b.String())
}

// Size returns the number of cached settings.
func (c *SettingsCache) Size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.cache)
}

// MaxSize returns the maximum number of settings that can be cached.
func (c *SettingsCache) MaxSize() int {
	return c.maxSize
}

// Hits returns the number of cache hits.
func (c *SettingsCache) Hits() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hits
}

// Misses returns the number of cache misses.
func (c *SettingsCache) Misses() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.misses
}

// Clear removes all cached settings and resets metrics.
func (c *SettingsCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache = make(map[SettingsCacheKey]*list.Element)
	c.lru.Init()
	c.hits = 0
	c.misses = 0
}
