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

// Package plancache provides a plan cache for query execution plans.
// It uses a W-TinyLFU cache (via theine) with epoch-based invalidation,
// mapping normalized SQL strings to cached Plans.
package plancache

import (
	"context"
	"sync/atomic"

	"github.com/multigres/multigres/go/common/cache/theine"
	"github.com/multigres/multigres/go/services/multigateway/engine"
)

// PlanCache is a plan cache backed by a W-TinyLFU cache with epoch-based invalidation.
//
// Epoch-based invalidation allows O(1) cache clearing: calling Invalidate()
// increments the epoch, and all entries written at a previous epoch are treated
// as misses without physically deleting them. Stale entries are lazily cleaned
// up during eviction.
type PlanCache struct {
	store   *theine.Store[theine.StringKey, *engine.Plan]
	epoch   atomic.Uint32
	metrics *CacheMetrics
}

// New creates a PlanCache with the given maximum memory in bytes.
// If maxMemory is <= 0, the cache is effectively disabled.
// The doorkeeper (bloom filter admission policy) is enabled to prevent
// cache pollution from one-off queries.
func New(maxMemory int) *PlanCache {
	metrics, _ := NewCacheMetrics()
	if maxMemory <= 0 {
		return &PlanCache{metrics: metrics}
	}
	return &PlanCache{
		store:   theine.NewStore[theine.StringKey, *engine.Plan](int64(maxMemory), true),
		metrics: metrics,
	}
}

// Get looks up a cached plan by normalized SQL key.
// Returns the cached plan and true on hit, or nil and false on miss.
// Entries from a previous epoch are treated as misses.
func (c *PlanCache) Get(ctx context.Context, normalizedSQL string) (*engine.Plan, bool) {
	if c.store == nil {
		c.metrics.RecordMiss(ctx)
		return nil, false
	}
	plan, ok := c.store.Get(theine.StringKey(normalizedSQL), c.epoch.Load())
	if ok {
		c.metrics.RecordHit(ctx)
	} else {
		c.metrics.RecordMiss(ctx)
	}
	return plan, ok
}

// Put inserts or updates a cache entry.
// The entry is stamped with the current epoch.
func (c *PlanCache) Put(normalizedSQL string, plan *engine.Plan) {
	if c.store == nil {
		return
	}
	// cost=0 tells theine to call plan.CachedSize() to determine the entry's memory cost.
	c.store.Set(theine.StringKey(normalizedSQL), plan, 0, c.epoch.Load())
}

// Invalidate invalidates all cached plans by incrementing the epoch.
// Existing entries become stale and will be treated as misses on subsequent
// Get calls. Stale entries are lazily cleaned up during eviction.
// This should be called when DDL or other destructive actions change the schema.
func (c *PlanCache) Invalidate() {
	c.epoch.Add(1)
}

// Len returns the number of entries currently in the cache.
// This includes stale entries that haven't been evicted yet.
func (c *PlanCache) Len() int {
	if c.store == nil {
		return 0
	}
	return c.store.Len()
}

// Hits returns the total number of cache hits.
func (c *PlanCache) Hits() int64 {
	if c.store == nil {
		return 0
	}
	return c.store.Metrics.Hits()
}

// Misses returns the total number of cache misses.
func (c *PlanCache) Misses() int64 {
	if c.store == nil {
		return 0
	}
	return c.store.Metrics.Misses()
}

// Evictions returns the total number of cache evictions.
func (c *PlanCache) Evictions() int64 {
	if c.store == nil {
		return 0
	}
	return c.store.Metrics.Evicted()
}

// Close shuts down the cache and stops the background maintenance goroutine.
func (c *PlanCache) Close() {
	if c.store == nil {
		return
	}
	c.store.Close()
}
