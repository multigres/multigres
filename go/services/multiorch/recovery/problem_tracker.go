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

package recovery

import (
	"cmp"
	"slices"
	"sync"
	"time"

	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// problemKey identifies one recurring problem: a problem code observed against
// one entity (a pooler or a shard).
type problemKey struct {
	code     types.ProblemCode
	entityID string
}

func keyOf(p types.Problem) problemKey {
	return problemKey{code: p.Code, entityID: p.EntityID()}
}

type problemState struct {
	brokenSince time.Time
	lastKnown   types.Problem
}

// clearedProblem is a problem that is no longer detected, as last seen.
type clearedProblem struct {
	problem  types.Problem
	duration time.Duration
}

// problemTransitions are the changes between two consecutive cycles' detected sets.
type problemTransitions struct {
	appeared []types.Problem
	cleared  []clearedProblem
}

// problemTracker is the single record of which problems are currently detected
// and since when. The engine reconciles it once per cycle, and everything that
// needs to know when a problem started or stopped works from the transitions it
// returns, so there is one place that decides that.
//
// It follows only active problems and forgets one when it clears.
//
// Concurrency: all methods are safe for concurrent use, and no callback runs
// under the lock. reconcile is called by one goroutine at a time, the recovery
// loop: PeriodicRunner schedules a cycle only after the previous one returns,
// and Stop waits for an in-flight cycle, so cycles never overlap. Everything
// else (metrics collection, gRPC handlers) only reads, through activeProblems.
// The engine relies on cycles not overlapping for more than this tracker, so it
// does not add a second guard around reconcile.
type problemTracker struct {
	mu     sync.Mutex
	active map[problemKey]*problemState
}

func newProblemTracker() *problemTracker {
	return &problemTracker{active: make(map[problemKey]*problemState)}
}

// reconcile takes the full set of problems detected in one cycle, once per
// cycle after every analyzer has run. A problem absent from the set counts as
// resolved, so the tracker needs no list of the problems an analyzer might emit.
//
// That also means an analyzer that errors in a cycle makes its problems look
// cleared, and they appear again once it succeeds.
//
// TODO: carry over the problems of shards whose analyzers errored, so a transient
// error does not log a clear and a fresh detection.
func (t *problemTracker) reconcile(detected []types.Problem, now time.Time) problemTransitions {
	t.mu.Lock()
	defer t.mu.Unlock()

	var transitions problemTransitions
	seen := make(map[problemKey]struct{}, len(detected))
	for _, p := range detected {
		key := keyOf(p)
		seen[key] = struct{}{}
		state, known := t.active[key]
		if !known {
			state = &problemState{brokenSince: now}
			t.active[key] = state
			transitions.appeared = append(transitions.appeared, p)
		}
		state.lastKnown = p
	}
	for key, state := range t.active {
		if _, still := seen[key]; !still {
			transitions.cleared = append(transitions.cleared, clearedProblem{
				problem:  state.lastKnown,
				duration: now.Sub(state.brokenSince),
			})
			delete(t.active, key)
		}
	}
	slices.SortFunc(transitions.cleared, func(a, b clearedProblem) int {
		return compareKeys(keyOf(a.problem), keyOf(b.problem))
	})
	return transitions
}

// activeProblems returns the currently detected problems, oldest first.
func (t *problemTracker) activeProblems() []types.Problem {
	t.mu.Lock()
	defer t.mu.Unlock()

	keys := make([]problemKey, 0, len(t.active))
	for key := range t.active {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(a, b problemKey) int {
		return cmp.Or(t.active[a].brokenSince.Compare(t.active[b].brokenSince), compareKeys(a, b))
	})
	problems := make([]types.Problem, 0, len(keys))
	for _, key := range keys {
		problems = append(problems, t.active[key].lastKnown)
	}
	return problems
}

func compareKeys(a, b problemKey) int {
	return cmp.Or(cmp.Compare(a.code, b.code), cmp.Compare(a.entityID, b.entityID))
}
