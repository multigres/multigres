// Copyright 2026 Supabase, Inc.
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

package preparedstatement

import (
	"slices"
	"sync"
)

// RelationInvalidationTracker tracks, for each canonical prepared statement
// name, which relations its query depends on, and a per-relation generation
// counter bumped whenever a DDL statement changes that relation's shape.
//
// This exists because a prepared statement's backend-side PostgreSQL plan
// can be reused across many different client sessions once a pooled backend
// connection has it prepared (see PoolerConsolidator), but nothing tells the
// pool when a DDL statement invalidates that plan's result shape until the
// next Bind/Execute revalidates it — and PostgreSQL's Describe does not
// revalidate at all (see multipooler executor.ensurePrepared's callers). A
// caller stamps a cached statement with StatementGeneration at prepare time
// and compares it to a fresh call before trusting the cache, closing that
// gap without needing to parse SQL itself — the gateway already computed the
// dependency information (ast.ExtractTablesUsed / ast.DDLTargetRelations)
// and forwards it on the wire.
//
// Entries are never removed, matching PoolerConsolidator's own reasoning:
// the set of unique canonical statements and relations is bounded by the
// application's query surface.
type RelationInvalidationTracker struct {
	mu sync.Mutex
	// dependencies maps canonical statement name -> relations it depends on
	// (schema-qualified, as produced by ast.ExtractTablesUsed /
	// ast.DDLTargetRelations). Accumulated by union across calls, since a
	// caller that doesn't yet know the relations a statement depends on (for
	// example a forced eager Parse) may register it before one that does.
	dependencies map[string][]string
	// relationGeneration maps relation name -> generation, set from
	// nextGeneration by InvalidateRelations.
	relationGeneration map[string]uint64
	// nextGeneration is a single global counter, incremented once per
	// InvalidateRelations call and stamped onto every relation it touches.
	// A counter per relation, incremented independently, would not compose:
	// two separate DDLs on different relations could each bump their own
	// relation to generation 1, and a statement depending on both would
	// then report the same StatementGeneration after either DDL alone as
	// after both — masking the second invalidation from a caller that
	// re-stamped its cache between the two. A single shared counter gives
	// every invalidation event its own strictly increasing value, so the max
	// across a statement's dependent relations always reflects the most
	// recent DDL that could have affected it.
	nextGeneration uint64
}

// NewRelationInvalidationTracker creates an empty RelationInvalidationTracker.
func NewRelationInvalidationTracker() *RelationInvalidationTracker {
	return &RelationInvalidationTracker{
		dependencies:       make(map[string][]string),
		relationGeneration: make(map[string]uint64),
	}
}

// RecordDependencies unions relations into the set canonicalName depends on.
// A nil or empty relations is a no-op, so a caller that doesn't know a
// statement's dependencies never erases a set recorded by an earlier caller
// that did.
func (t *RelationInvalidationTracker) RecordDependencies(canonicalName string, relations []string) {
	if len(relations) == 0 {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	existing := t.dependencies[canonicalName]
	for _, r := range relations {
		if !slices.Contains(existing, r) {
			existing = append(existing, r)
		}
	}
	t.dependencies[canonicalName] = existing
}

// StatementGeneration returns the current generation for canonicalName: the
// highest generation among the relations it is known to depend on, or 0 if
// it depends on no known relation (never invalidated).
func (t *RelationInvalidationTracker) StatementGeneration(canonicalName string) uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	var max uint64
	for _, r := range t.dependencies[canonicalName] {
		if g := t.relationGeneration[r]; g > max {
			max = g
		}
	}
	return max
}

// InvalidateRelations bumps the generation for each named relation, so any
// canonical statement depending on it reports a StatementGeneration newer
// than whatever a caller stamped on it before this call. Call after a DDL
// statement naming these relations executes successfully.
func (t *RelationInvalidationTracker) InvalidateRelations(relations []string) {
	if len(relations) == 0 {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.nextGeneration++
	for _, r := range relations {
		t.relationGeneration[r] = t.nextGeneration
	}
}
