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

package list

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListNew(t *testing.T) {
	l := New[int]()
	require.NotNil(t, l)
	assert.Equal(t, 0, l.Len())
	assert.Nil(t, l.Front())
	assert.Nil(t, l.Back())
}

func TestListPushFrontAndBack(t *testing.T) {
	l := New[string]()

	// Push to front
	e1 := l.PushFront("first")
	assert.Equal(t, 1, l.Len())
	assert.Equal(t, "first", l.Front().Value)
	assert.Equal(t, "first", l.Back().Value)
	assert.Same(t, e1, l.Front())

	// Push to back
	e2 := l.PushBack("last")
	assert.Equal(t, 2, l.Len())
	assert.Equal(t, "first", l.Front().Value)
	assert.Equal(t, "last", l.Back().Value)
	assert.Same(t, e2, l.Back())

	// Push another to front
	e3 := l.PushFront("new-first")
	assert.Equal(t, 3, l.Len())
	assert.Equal(t, "new-first", l.Front().Value)
	assert.Same(t, e3, l.Front())
}

func TestListTraversal(t *testing.T) {
	l := New[int]()
	values := []int{1, 2, 3, 4, 5}

	for _, v := range values {
		l.PushBack(v)
	}

	// Forward traversal
	var forward []int
	for e := l.Front(); e != nil; e = e.Next() {
		forward = append(forward, e.Value)
	}
	assert.Equal(t, values, forward)

	// Backward traversal
	var backward []int
	for e := l.Back(); e != nil; e = e.Prev() {
		backward = append(backward, e.Value)
	}
	assert.Equal(t, []int{5, 4, 3, 2, 1}, backward)
}

func TestListRemove(t *testing.T) {
	l := New[int]()
	e1 := l.PushBack(1)
	e2 := l.PushBack(2)
	e3 := l.PushBack(3)

	// Remove middle element
	l.Remove(e2)
	assert.Equal(t, 2, l.Len())
	assert.Same(t, e3, e1.Next())
	assert.Same(t, e1, e3.Prev())

	// Remove front
	l.Remove(e1)
	assert.Equal(t, 1, l.Len())
	assert.Same(t, e3, l.Front())
	assert.Same(t, e3, l.Back())

	// Remove last element
	l.Remove(e3)
	assert.Equal(t, 0, l.Len())
	assert.Nil(t, l.Front())
	assert.Nil(t, l.Back())
}

func TestListRemoveFromWrongListPanics(t *testing.T) {
	l1 := New[int]()
	l2 := New[int]()

	e := l1.PushBack(1)

	assert.Panics(t, func() {
		l2.Remove(e)
	})
}

func TestListPushFrontValue(t *testing.T) {
	l := New[int]()

	// Pre-allocate element and push it
	elem := &Element[int]{Value: 42}
	l.PushFrontValue(elem)

	assert.Equal(t, 1, l.Len())
	assert.Same(t, elem, l.Front())
	assert.Equal(t, 42, l.Front().Value)
}

func TestListPushBackValue(t *testing.T) {
	l := New[int]()
	l.PushBack(1)

	// Pre-allocate element and push to back
	elem := &Element[int]{Value: 99}
	l.PushBackValue(elem)

	assert.Equal(t, 2, l.Len())
	assert.Same(t, elem, l.Back())
	assert.Equal(t, 99, l.Back().Value)
}

func TestListInit(t *testing.T) {
	// Init is used to initialize a zero-value list
	var l List[int]
	l.Init()

	assert.Equal(t, 0, l.Len())
	assert.Nil(t, l.Front())
	assert.Nil(t, l.Back())

	// After init, the list should be usable
	l.PushBack(1)
	assert.Equal(t, 1, l.Len())
	assert.Equal(t, 1, l.Front().Value)
}

func TestElementNextPrevBoundaries(t *testing.T) {
	l := New[int]()
	e := l.PushBack(1)

	// Single element: Next and Prev should be nil
	assert.Nil(t, e.Next())
	assert.Nil(t, e.Prev())

	// Add another element
	e2 := l.PushBack(2)
	assert.Same(t, e2, e.Next())
	assert.Nil(t, e.Prev())
	assert.Same(t, e, e2.Prev())
	assert.Nil(t, e2.Next())
}

func TestListWithStructValues(t *testing.T) {
	type item struct {
		id   int
		name string
	}

	l := New[item]()
	l.PushBack(item{id: 1, name: "one"})
	l.PushBack(item{id: 2, name: "two"})

	assert.Equal(t, 2, l.Len())
	assert.Equal(t, 1, l.Front().Value.id)
	assert.Equal(t, "two", l.Back().Value.name)
}

func TestElementRemovedHasNilPointers(t *testing.T) {
	l := New[int]()
	e := l.PushBack(1)
	l.PushBack(2)

	l.Remove(e)

	// Removed element should have nil pointers to avoid memory leaks
	assert.Nil(t, e.next)
	assert.Nil(t, e.prev)
	assert.Nil(t, e.list)
}
