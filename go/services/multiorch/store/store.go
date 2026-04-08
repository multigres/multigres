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

	"google.golang.org/protobuf/proto"
)

// ProtoStore is a generic thread-safe in-memory key-value store for protobuf messages.
// It automatically clones values on Get and Set to ensure callers always work with
// isolated copies, preventing concurrent mutation of shared state.
//
// Design rationale:
// While sync.Map could be used here, this custom data structure provides a more
// flexible API tailored to our access patterns. sync.Map is optimized for
// "write-once, read-many" scenarios with minimal write contention. In contrast,
// the recovery engine experiences frequent writes (health check updates, topology
// changes, bookkeeping operations), making sync.Map's optimizations less effective.
//
// The automatic cloning on Get/Set ensures that:
// - Callers always get a private copy they can mutate safely
// - The store only ever holds its own canonical copy
// - No explicit DeepCopy calls are needed at call sites
//
// This abstraction provides flexibility for future optimizations if contention
// becomes a bottleneck:
// - Range locks or per-key locking for finer-grained concurrency
// - Channel-based write batching to reduce lock contention
type ProtoStore[K comparable, V proto.Message] struct {
	mu    sync.Mutex
	items map[K]V
}

// NewProtoStore creates a new proto store.
func NewProtoStore[K comparable, V proto.Message]() *ProtoStore[K, V] {
	return &ProtoStore[K, V]{
		items: make(map[K]V),
	}
}

// Get retrieves a value by key. Returns a deep clone of the value and a boolean
// indicating if the key exists. The returned value is safe to mutate without
// affecting the stored copy.
func (s *ProtoStore[K, V]) Get(key K) (V, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	v, ok := s.items[key]
	if !ok {
		var zero V
		return zero, false
	}

	// Return a deep clone so callers get an isolated copy
	cloned := proto.Clone(v).(V)
	return cloned, true
}

// Set stores a deep clone of the value for the given key. If the key already
// exists, it will be overwritten. The store keeps its own copy, so the caller
// can continue to mutate the passed value without affecting the stored copy.
func (s *ProtoStore[K, V]) Set(key K, value V) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Store a deep clone so the canonical copy only lives inside the store
	s.items[key] = proto.Clone(value).(V)
}

// Delete removes a value by key. Returns true if the key existed, false otherwise.
func (s *ProtoStore[K, V]) Delete(key K) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, existed := s.items[key]
	delete(s.items, key)
	return existed
}

// Len returns the number of items in the store.
func (s *ProtoStore[K, V]) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.items)
}

// Clear removes all items from the store.
func (s *ProtoStore[K, V]) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items = make(map[K]V)
}

// Range iterates over all key-value pairs in the store while holding the lock.
// Each value passed to the callback is a deep clone, safe to mutate.
// The iteration stops early if the callback function returns false.
//
// Example:
//
//	store.Range(func(key string, value *PoolerHealthState) bool {
//	    // Process key and value (value is a clone, safe to mutate)
//	    return true  // continue iteration
//	})
func (s *ProtoStore[K, V]) Range(fn func(key K, value V) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for k, v := range s.items {
		// Clone each value so the callback gets an isolated copy
		cloned := proto.Clone(v).(V)
		if !fn(k, cloned) {
			return
		}
	}
}

// DoUpdate performs an atomic read-modify-write operation for a given key.
//
// The provided function receives a pointer to the current value and can modify
// it in place. After the function returns, the updated value is stored back in
// the map. This is useful for cases where you want to update a value based on
// its current state without having to do multiple Get/Set calls. Note that the
// function is not called if the key does not exist, so it won't create new
// entries. This is designed for safely updating existing entries while avoiding
// accidental creation of new ones.
//
// Example:
//
//	store.DoUpdate("pooler1", func(value *PoolerHealthState) *PoolerHealthState {
//	    // Update value based on current state (value is a pointer to the current or zero value)
//	    value.LastSeen = timestamppb.Now()
//	    return value  // return the updated value to store it back in the map
//	})
//
// Note: The update function should return the new value to be stored. If the
// function does not modify the value, it should return the original value to
// ensure it remains in the store.
//
// Right now we are holding a lock on the entire store for the duration of the
// update function, but an improvement for the future could be to implement
// finer-grained locking (e.g., per-key locks) if contention becomes an issue.
func (s *ProtoStore[K, V]) DoUpdate(key K, fn func(value V) V) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Skip the update if the key doesn't exist to avoid accidentally creating
	// new entries.
	if v, ok := s.items[key]; ok {
		s.items[key] = fn(v)
	}
}
