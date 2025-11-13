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

// GetAll returns a snapshot of all values in the store.
// The order is not guaranteed.
func (s *Store[K, V]) GetAll() []V {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := make([]V, 0, len(s.items))
	for _, v := range s.items {
		result = append(result, v)
	}
	return result
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
