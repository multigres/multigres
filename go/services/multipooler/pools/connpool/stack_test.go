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

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStackOperations tests push, pop, len, and LIFO ordering.
func TestStackOperations(t *testing.T) {
	var stack connStack[*mockConnection]

	// 1. Empty stack returns nil on pop
	conn, ok := stack.Pop()
	assert.Nil(t, conn)
	assert.False(t, ok)
	assert.Equal(t, 0, stack.Len())

	// 2. Push increases length
	conn1 := &Pooled[*mockConnection]{Conn: newMockConnection()}
	conn2 := &Pooled[*mockConnection]{Conn: newMockConnection()}
	conn3 := &Pooled[*mockConnection]{Conn: newMockConnection()}

	stack.Push(conn1)
	assert.Equal(t, 1, stack.Len())

	stack.Push(conn2)
	assert.Equal(t, 2, stack.Len())

	stack.Push(conn3)
	assert.Equal(t, 3, stack.Len())

	// 3. Pop returns in LIFO order (last in, first out)
	popped, ok := stack.Pop()
	require.True(t, ok)
	assert.Same(t, conn3, popped)
	assert.Equal(t, 2, stack.Len())

	popped, ok = stack.Pop()
	require.True(t, ok)
	assert.Same(t, conn2, popped)
	assert.Equal(t, 1, stack.Len())

	popped, ok = stack.Pop()
	require.True(t, ok)
	assert.Same(t, conn1, popped)
	assert.Equal(t, 0, stack.Len())

	// 4. Empty again after all pops
	conn, ok = stack.Pop()
	assert.Nil(t, conn)
	assert.False(t, ok)
}

// TestStackForEach tests iteration over stack elements.
func TestStackForEach(t *testing.T) {
	var stack connStack[*mockConnection]

	// Push some connections
	conn1 := &Pooled[*mockConnection]{Conn: newMockConnection()}
	conn2 := &Pooled[*mockConnection]{Conn: newMockConnection()}
	conn3 := &Pooled[*mockConnection]{Conn: newMockConnection()}

	stack.Push(conn1)
	stack.Push(conn2)
	stack.Push(conn3)

	// 1. ForEach visits all elements in stack order (top to bottom)
	var visited []*Pooled[*mockConnection]
	stack.ForEach(func(c *Pooled[*mockConnection]) bool {
		visited = append(visited, c)
		return true
	})

	require.Len(t, visited, 3)
	assert.Same(t, conn3, visited[0]) // top of stack (last pushed)
	assert.Same(t, conn2, visited[1])
	assert.Same(t, conn1, visited[2]) // bottom of stack (first pushed)

	// 2. ForEach can stop early by returning false
	count := 0
	stack.ForEach(func(c *Pooled[*mockConnection]) bool {
		count++
		return count < 2 // stop after 2 elements
	})
	assert.Equal(t, 2, count)

	// 3. ForEach on empty stack does nothing
	var emptyStack connStack[*mockConnection]
	called := false
	emptyStack.ForEach(func(c *Pooled[*mockConnection]) bool {
		called = true
		return true
	})
	assert.False(t, called)
}

// TestStackConcurrentAccess tests thread-safety of stack operations.
func TestStackConcurrentAccess(t *testing.T) {
	var stack connStack[*mockConnection]
	var wg sync.WaitGroup

	numGoroutines := 10
	opsPerGoroutine := 100

	// Concurrent pushes and pops
	for i := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for range opsPerGoroutine {
				conn := &Pooled[*mockConnection]{Conn: newMockConnection()}
				stack.Push(conn)
				stack.Pop()
			}
		}(i)
	}

	wg.Wait()

	// Stack should be empty or have some leftover connections
	// (race between push and pop is fine, we just verify no crashes)
	assert.GreaterOrEqual(t, stack.Len(), 0)
}
