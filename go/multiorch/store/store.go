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

package store

import (
	"sync"
)

// Store is a generic thread-safe in-memory key-value store.
// It can be used to store any type of value indexed by any comparable key.
//
// Design rationale:
// While sync.Map could be used here, this custom data structure provides a more
// flexible API tailored to our access patterns. sync.Map is optimized for
// "write-once, read-many" scenarios with minimal write contention. In contrast,
// the recovery engine experiences frequent writes (health check updates, topology
// changes, bookkeeping operations), making sync.Map's optimizations less effective.
//
// This abstraction provides flexibility for future optimizations if contention
// becomes a bottleneck:
// - Range locks or per-key locking for finer-grained concurrency
// - Channel-based write batching to reduce lock contention
type Store[K comparable, V any] struct {
	mu    sync.Mutex
	items map[K]V
}

// NewStore creates a new generic store.
func NewStore[K comparable, V any]() *Store[K, V] {
	return &Store[K, V]{
		items: make(map[K]V),
	}
}

// Get retrieves a value by key. Returns the value and a boolean indicating if the key exists.
func (s *Store[K, V]) Get(key K) (V, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.items[key]
	return v, ok
}

// Set stores a value for the given key. If the key already exists, it will be overwritten.
func (s *Store[K, V]) Set(key K, value V) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items[key] = value
}

// Delete removes a value by key. Returns true if the key existed, false otherwise.
func (s *Store[K, V]) Delete(key K) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, existed := s.items[key]
	delete(s.items, key)
	return existed
}

// Len returns the number of items in the store.
func (s *Store[K, V]) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.items)
}

// Clear removes all items from the store.
func (s *Store[K, V]) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items = make(map[K]V)
}

// Range iterates over all key-value pairs in the store while holding the lock.
// The iteration stops early if the callback function returns false.
//
// Example:
//
//	store.Range(func(key string, value PoolerHealth) bool {
//	    // Process key and value
//	    return true  // continue iteration
//	})
func (s *Store[K, V]) Range(fn func(key K, value V) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for k, v := range s.items {
		if !fn(k, v) {
			return
		}
	}
}
