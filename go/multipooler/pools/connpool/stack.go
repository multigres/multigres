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

package connpool

import "sync"

// connStack is a mutex-protected stack for pooled connections.
// It is safe to use from multiple goroutines.
//
// # Design Decision: Mutex vs Lock-Free
//
// This implementation uses mutex synchronization rather than lock-free (CAS-based) operations.
// This decision was made based on benchmark analysis at target throughput of ~800k-1M ops/sec:
//
//   - Mutex has better p99 latency at moderate contention (3µs vs 11µs at 4 goroutines)
//   - Mutex has more predictable tail latencies (no CAS retry storms under contention)
//   - Both achieve equivalent throughput at this scale (~850k ops/sec)
//   - Mutex is simpler to debug, profile, and maintain
//
// Lock-free would only be advantageous at much higher contention levels (millions of
// ops/sec with 100+ goroutines) or when hard real-time guarantees are required.
// For connection pools where actual query execution dominates latency, the simpler
// mutex approach is preferred.
//
// See: stack_bench_test.go TestStackLatencyAtThroughput for benchmark details.
type connStack[C Connection] struct {
	mu    sync.Mutex
	top   *Pooled[C]
	count int
}

// Push adds a connection to the top of the stack.
func (s *connStack[C]) Push(conn *Pooled[C]) {
	s.mu.Lock()
	conn.next = s.top
	s.top = conn
	s.count++
	s.mu.Unlock()
}

// Pop removes and returns the connection from the top of the stack.
// Returns nil and false if the stack is empty.
func (s *connStack[C]) Pop() (*Pooled[C], bool) {
	s.mu.Lock()
	if s.top == nil {
		s.mu.Unlock()
		return nil, false
	}
	conn := s.top
	s.top = conn.next
	s.count--
	s.mu.Unlock()
	conn.next = nil
	return conn, true
}

// Len returns the number of connections in the stack.
func (s *connStack[C]) Len() int {
	s.mu.Lock()
	n := s.count
	s.mu.Unlock()
	return n
}

// ForEach iterates over all connections in the stack and calls fn for each.
// The iteration happens under the lock, so fn should be fast and non-blocking.
// If fn returns false, iteration stops early.
func (s *connStack[C]) ForEach(fn func(*Pooled[C]) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for conn := s.top; conn != nil; conn = conn.next {
		if !fn(conn) {
			return
		}
	}
}
