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

// Package connstack provides a lock-free stack implementation for connection pooling.
// This implementation uses 128-bit atomic CAS operations (pointer + counter) to prevent
// the ABA problem.
package connstack

import (
	"runtime"
	"sync/atomic"

	"vitess.io/vitess/go/atomic2"
)

// Node represents a node in the stack.
// Each node contains a pointer to the next node for stack chaining.
type Node[T any] interface {
	// NextPtr returns a pointer to the atomic next pointer.
	// This allows the stack to manipulate the next pointer without knowing
	// the concrete type of T.
	NextPtr() *atomic.Pointer[T]
}

// Stack is a lock-free LIFO stack safe for concurrent use.
// The stack uses atomic 128-bit CAS operations on the top pointer and a pop counter
// to prevent the ABA problem. The stack is intrusive - elements must implement Node.
type Stack[T Node[T]] struct {
	// top combines a pointer to the top element and a pop counter.
	// The counter is incremented on every pop to prevent ABA races.
	top atomic2.PointerAndUint64[T]
}

// Push adds an element to the top of the stack.
// This operation is lock-free and safe for concurrent use.
func (s *Stack[T]) Push(elem T) {
	for {
		// Load current top and pop counter
		oldTop, popCount := s.top.Load()

		// Set elem's next to current top
		elem.NextPtr().Store(oldTop)

		// Try to make elem the new top
		// Don't increment counter on push (only on pop, simpler and prevents overflow)
		if s.top.CompareAndSwap(oldTop, popCount, &elem, popCount) {
			return
		}

		// CAS failed due to contention, yield and retry
		runtime.Gosched()
	}
}

// Pop removes and returns the element from the top of the stack.
// Returns nil if the stack is empty.
// This operation is lock-free and safe for concurrent use.
func (s *Stack[T]) Pop() T {
	for {
		// Load current top and pop counter
		oldTop, popCount := s.top.Load()

		// Stack is empty
		if oldTop == nil {
			var zero T
			return zero
		}

		// Load next element
		nextTop := (*oldTop).NextPtr().Load()

		// Try to make next the new top, incrementing pop counter
		// Incrementing the counter prevents ABA problem
		if s.top.CompareAndSwap(oldTop, popCount, nextTop, popCount+1) {
			// Clear the next pointer of popped element to avoid memory leaks
			(*oldTop).NextPtr().Store(nil)
			return *oldTop
		}

		// CAS failed due to contention, yield and retry
		runtime.Gosched()
	}
}

// Peek returns the element at the top of the stack without removing it.
// Returns nil if the stack is empty.
// Note: The returned element may be immediately popped by another goroutine.
func (s *Stack[T]) Peek() T {
	top, _ := s.top.Load()
	if top == nil {
		var zero T
		return zero
	}
	return *top
}

// IsEmpty returns true if the stack is empty.
// Note: The result may be immediately invalidated by concurrent operations.
func (s *Stack[T]) IsEmpty() bool {
	top, _ := s.top.Load()
	return top == nil
}
