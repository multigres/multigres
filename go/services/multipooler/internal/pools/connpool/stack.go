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

	// onPush is called after each successful Push operation.
	// Used for metrics tracking (e.g., OTel idle count +1).
	onPush func()
	// onPop is called after each successful Pop operation.
	// Used for metrics tracking (e.g., OTel idle count -1).
	onPop func()
}

// Push adds a connection to the top of the stack.
func (s *connStack[C]) Push(conn *Pooled[C]) {
	s.mu.Lock()
	conn.next = s.top
	s.top = conn
	s.count++
	s.mu.Unlock()
	if s.onPush != nil {
		s.onPush()
	}
}

// PopFirst removes and returns the topmost connection for which match
// returns true, leaving every other connection in place and in order, along
// with its depth: the number of connections that were above it. Returns
// nil, 0, false if none matches. match runs under the lock, so it must be
// fast and non-blocking. Unlinking a middle node is safe here because every
// stack operation, including ForEach, holds the mutex.
//
// Pair with InsertAt to put the connection back where it was: pushing a
// middle connection onto the top would promote it into client traffic.
func (s *connStack[C]) PopFirst(match func(*Pooled[C]) bool) (*Pooled[C], int, bool) {
	s.mu.Lock()
	var prev *Pooled[C]
	depth := 0
	for conn := s.top; conn != nil; prev, conn, depth = conn, conn.next, depth+1 {
		if !match(conn) {
			continue
		}
		if prev == nil {
			s.top = conn.next
		} else {
			prev.next = conn.next
		}
		s.count--
		s.mu.Unlock()
		conn.next = nil
		if s.onPop != nil {
			s.onPop()
		}
		return conn, depth, true
	}
	s.mu.Unlock()
	return nil, 0, false
}

// InsertAt adds a connection below the first depth connections of the stack,
// or at the bottom if the stack is shorter than that. InsertAt(conn, 0) is
// Push. Clients may have pushed or popped since a PopFirst measured depth,
// so the position is best effort; it is exact in the case that matters, a
// hot top and a cold tail, and never lifts the connection above one that
// was above it.
func (s *connStack[C]) InsertAt(conn *Pooled[C], depth int) {
	s.mu.Lock()
	if depth <= 0 || s.top == nil {
		conn.next = s.top
		s.top = conn
	} else {
		prev := s.top
		for i := 1; i < depth && prev.next != nil; i++ {
			prev = prev.next
		}
		conn.next = prev.next
		prev.next = conn
	}
	s.count++
	s.mu.Unlock()
	if s.onPush != nil {
		s.onPush()
	}
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
	if s.onPop != nil {
		s.onPop()
	}
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
