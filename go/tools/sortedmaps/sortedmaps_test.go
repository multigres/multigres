// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sortedmaps_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/multigres/multigres/go/tools/sortedmaps"
)

func TestKeys(t *testing.T) {
	t.Run("string keys returned in sorted order", func(t *testing.T) {
		m := map[string]int{"c": 3, "a": 1, "b": 2}
		assert.Equal(t, []string{"a", "b", "c"}, sortedmaps.Keys(m))
	})

	t.Run("int keys returned in sorted order", func(t *testing.T) {
		m := map[int]string{30: "x", 10: "y", 20: "z"}
		assert.Equal(t, []int{10, 20, 30}, sortedmaps.Keys(m))
	})

	t.Run("empty map returns empty slice", func(t *testing.T) {
		assert.Empty(t, sortedmaps.Keys(map[string]int{}))
	})
}

func TestValues(t *testing.T) {
	t.Run("values returned in sorted key order", func(t *testing.T) {
		m := map[string]int{"c": 3, "a": 1, "b": 2}
		assert.Equal(t, []int{1, 2, 3}, sortedmaps.Values(m))
	})

	t.Run("empty map returns empty slice", func(t *testing.T) {
		assert.Empty(t, sortedmaps.Values(map[string]int{}))
	})
}

func TestAll(t *testing.T) {
	t.Run("iterates in sorted key order", func(t *testing.T) {
		m := map[string]int{"c": 3, "a": 1, "b": 2}
		var keys []string
		var values []int
		for k, v := range sortedmaps.All(m) {
			keys = append(keys, k)
			values = append(values, v)
		}
		assert.Equal(t, []string{"a", "b", "c"}, keys)
		assert.Equal(t, []int{1, 2, 3}, values)
	})

	t.Run("early break stops iteration", func(t *testing.T) {
		m := map[string]int{"c": 3, "a": 1, "b": 2}
		var keys []string
		for k := range sortedmaps.All(m) {
			keys = append(keys, k)
			if k == "b" {
				break
			}
		}
		assert.Equal(t, []string{"a", "b"}, keys)
	})
}

func TestByStr(t *testing.T) {
	type opaque struct{ name string }

	t.Run("iterates in string-representation order", func(t *testing.T) {
		// Keys are pointers, so natural ordering is non-deterministic;
		// ByStr forces a stable order via fmt.Sprintf("%v", key).
		a := &opaque{name: "alpha"}
		b := &opaque{name: "beta"}
		c := &opaque{name: "gamma"}
		m := map[*opaque]string{a: "A", b: "B", c: "C"}

		var values []string
		for _, v := range sortedmaps.ByStr(m) {
			values = append(values, v)
		}

		// Three distinct values present; deterministic order across runs.
		assert.Len(t, values, 3)
		secondRun := make([]string, 0, 3)
		for _, v := range sortedmaps.ByStr(m) {
			secondRun = append(secondRun, v)
		}
		assert.Equal(t, values, secondRun)
	})

	t.Run("early break stops iteration", func(t *testing.T) {
		m := map[string]int{"c": 3, "a": 1, "b": 2}
		count := 0
		for range sortedmaps.ByStr(m) {
			count++
			if count == 2 {
				break
			}
		}
		assert.Equal(t, 2, count)
	})
}
