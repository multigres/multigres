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

// Package sortedmaps provides map iteration helpers that produce deterministic
// output by sorting before iterating. Use these in place of the stdlib "maps"
// package functions (maps.Keys, maps.Values, maps.All) and direct "for range"
// over a map wherever reproducibility across Go runs is required.
package sortedmaps

import (
	"cmp"
	"fmt"
	"iter"
	"slices"
	"strings"
)

// Keys returns the keys of m in sorted order.
func Keys[K cmp.Ordered, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// Values returns the values of m in sorted key order.
func Values[K cmp.Ordered, V any](m map[K]V) []V {
	keys := Keys(m)
	values := make([]V, len(keys))
	for i, k := range keys {
		values[i] = m[k]
	}
	return values
}

// All returns an iterator over the (key, value) pairs of m in sorted key order.
//
// Example:
//
//	for id, info := range sortedmaps.All(myMap) {
//	    // ...
//	}
func All[K cmp.Ordered, V any](m map[K]V) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		for _, k := range Keys(m) {
			if !yield(k, m[k]) {
				return
			}
		}
	}
}

// ByStr returns an iterator over the (key, value) pairs of m sorted by the
// string representation of the key. Use this when K is comparable but not
// cmp.Ordered (e.g., when K is a type parameter constrained only to
// comparable). For ordered key types, prefer All.
func ByStr[K comparable, V any](m map[K]V) iter.Seq2[K, V] {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b K) int {
		return strings.Compare(fmt.Sprintf("%v", a), fmt.Sprintf("%v", b))
	})
	return func(yield func(K, V) bool) {
		for _, k := range keys {
			if !yield(k, m[k]) {
				return
			}
		}
	}
}
