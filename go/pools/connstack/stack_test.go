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

package connstack

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testNode is a simple node implementation for testing.
type testNode struct {
	value int
	next  atomic.Pointer[*testNode]
}

func (n *testNode) NextPtr() *atomic.Pointer[*testNode] {
	return &n.next
}

func TestStackBasicOperations(t *testing.T) {
	var s Stack[*testNode]

	// Test empty stack
	assert.True(t, s.IsEmpty(), "new stack should be empty")
	assert.Nil(t, s.Pop(), "pop from empty stack should return nil")

	// Push single element
	node1 := &testNode{value: 1}
	s.Push(node1)
	assert.False(t, s.IsEmpty(), "stack should not be empty after push")

	// Peek should return the element without removing it
	peeked := s.Peek()
	require.NotNil(t, peeked, "peek should return non-nil")
	assert.Equal(t, 1, peeked.value, "peek should return node with value 1")
	assert.False(t, s.IsEmpty(), "stack should not be empty after peek")

	// Pop should return the element
	popped := s.Pop()
	require.NotNil(t, popped, "pop should return non-nil")
	assert.Equal(t, 1, popped.value, "pop should return node with value 1")
	assert.True(t, s.IsEmpty(), "stack should be empty after popping only element")
}

func TestStackLIFOOrder(t *testing.T) {
	var s Stack[*testNode]

	// Push multiple elements
	for i := range 5 {
		s.Push(&testNode{value: i + 1})
	}

	// Pop should return in LIFO order (5, 4, 3, 2, 1)
	for i := 5; i >= 1; i-- {
		popped := s.Pop()
		require.NotNil(t, popped, "pop should return non-nil for value %d", i)
		assert.Equal(t, i, popped.value, "pop should return correct value")
	}

	// Stack should be empty now
	assert.True(t, s.IsEmpty(), "stack should be empty after popping all elements")
}

func TestStackPopAll(t *testing.T) {
	var s Stack[*testNode]

	// Push multiple elements
	for i := range 5 {
		s.Push(&testNode{value: i + 1})
	}

	// Pop all elements
	for range 5 {
		popped := s.Pop()
		assert.NotNil(t, popped, "should pop valid element")
	}

	assert.True(t, s.IsEmpty(), "stack should be empty after popping all")
	assert.Nil(t, s.Pop(), "pop from empty stack should return nil")
}

func TestStackConcurrentPush(t *testing.T) {
	var s Stack[*testNode]
	numGoroutines := 100
	elementsPerGoroutine := 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Concurrent pushes - each goroutine creates new nodes
	for i := range numGoroutines {
		go func(base int) {
			defer wg.Done()
			for j := range elementsPerGoroutine {
				node := &testNode{value: base*1000 + j}
				s.Push(node)
			}
		}(i)
	}

	wg.Wait()

	// Verify all elements are in the stack
	count := 0
	seen := make(map[int]bool)
	for {
		popped := s.Pop()
		if popped == nil {
			break
		}
		count++
		assert.False(t, seen[popped.value], "value %d should not be popped twice", popped.value)
		seen[popped.value] = true
	}

	expected := numGoroutines * elementsPerGoroutine
	assert.Equal(t, expected, count, "should pop all pushed elements")
}

func TestStackConcurrentPop(t *testing.T) {
	var s Stack[*testNode]
	numElements := 10000

	// Pre-populate stack with unique nodes
	for i := range numElements {
		node := &testNode{value: i}
		s.Push(node)
	}

	numGoroutines := 100
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	var successfulPops atomic.Int64
	poppedValues := sync.Map{}

	// Concurrent pops
	for range numGoroutines {
		go func() {
			defer wg.Done()
			for {
				popped := s.Pop()
				if popped == nil {
					return
				}
				successfulPops.Add(1)
				_, exists := poppedValues.LoadOrStore(popped.value, true)
				assert.False(t, exists, "value %d should not be popped twice", popped.value)
			}
		}()
	}

	wg.Wait()

	assert.Equal(t, int64(numElements), successfulPops.Load(), "should pop all elements")
	assert.True(t, s.IsEmpty(), "stack should be empty after all pops")
}

func TestStackConcurrentPushAndPop(t *testing.T) {
	var s Stack[*testNode]
	numGoroutines := 50
	operationsPerGoroutine := 1000

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2) // Half pushers, half poppers

	var pushCount atomic.Int64
	var popCount atomic.Int64
	poppedValues := sync.Map{}

	// Concurrent pushers - each creates unique nodes
	for i := range numGoroutines {
		go func(base int) {
			defer wg.Done()
			for j := range operationsPerGoroutine {
				node := &testNode{value: base*10000 + j}
				s.Push(node)
				pushCount.Add(1)
			}
		}(i)
	}

	// Concurrent poppers
	for range numGoroutines {
		go func() {
			defer wg.Done()
			for range operationsPerGoroutine {
				popped := s.Pop()
				if popped != nil {
					popCount.Add(1)
					_, exists := poppedValues.LoadOrStore(popped.value, true)
					assert.False(t, exists, "value %d should not be popped twice", popped.value)
				}
			}
		}()
	}

	wg.Wait()

	// Pop remaining elements
	remainingCount := int64(0)
	for {
		popped := s.Pop()
		if popped == nil {
			break
		}
		remainingCount++
		_, exists := poppedValues.LoadOrStore(popped.value, true)
		assert.False(t, exists, "value %d should not be popped twice", popped.value)
	}

	totalPopped := popCount.Load() + remainingCount
	totalPushed := pushCount.Load()

	assert.Equal(t, totalPushed, totalPopped, "total pushed should equal total popped")
}

func TestStackNoElementLoss(t *testing.T) {
	var s Stack[*testNode]
	numOperations := 10000

	pushed := make(map[int]bool)
	popped := make(map[int]bool)
	var mu sync.Mutex

	var wg sync.WaitGroup
	wg.Add(2)

	// Pusher goroutine
	go func() {
		defer wg.Done()
		for i := range numOperations {
			node := &testNode{value: i}
			s.Push(node)
			mu.Lock()
			pushed[i] = true
			mu.Unlock()
		}
	}()

	// Popper goroutine
	go func() {
		defer wg.Done()
		for range numOperations {
			node := s.Pop()
			if node != nil {
				mu.Lock()
				if popped[node.value] {
					t.Errorf("element %d should not be popped twice", node.value)
				}
				popped[node.value] = true
				mu.Unlock()
			}
		}
	}()

	wg.Wait()

	// Pop any remaining elements
	for {
		node := s.Pop()
		if node == nil {
			break
		}
		mu.Lock()
		if popped[node.value] {
			t.Errorf("element %d should not be popped twice", node.value)
		}
		popped[node.value] = true
		mu.Unlock()
	}

	// Verify all pushed elements were popped exactly once
	mu.Lock()
	defer mu.Unlock()

	assert.Equal(t, len(pushed), len(popped), "all pushed elements should be popped")

	for val := range pushed {
		assert.True(t, popped[val], "element %d should be popped", val)
	}
}

func TestStackEmptyPop(t *testing.T) {
	var s Stack[*testNode]

	// Popping from empty stack should return nil
	popped := s.Pop()
	assert.Nil(t, popped, "pop from empty stack should return nil")
}

// Benchmark tests
func BenchmarkStackPush(b *testing.B) {
	var s Stack[*testNode]
	nodes := make([]*testNode, b.N)
	for i := range b.N {
		nodes[i] = &testNode{value: i}
	}

	b.ResetTimer()
	for i := range b.N {
		s.Push(nodes[i])
		s.Pop() // Keep stack size constant
	}
}

func BenchmarkStackPop(b *testing.B) {
	var s Stack[*testNode]

	// Pre-populate stack
	for i := range b.N {
		s.Push(&testNode{value: i})
	}

	b.ResetTimer()
	for range b.N {
		s.Pop()
	}
}

func BenchmarkStackConcurrentPushPop(b *testing.B) {
	var s Stack[*testNode]

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			node := &testNode{value: i}
			s.Push(node)
			s.Pop()
			i++
		}
	})
}
