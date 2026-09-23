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
	"sync"
	"time"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

const (
	// maxTrackedProblems/maxTrackedActions cap the number of distinct problem
	// identities / action+entity identities retained, evicting the
	// least-recently-active first once exceeded. A count cap, not a time
	// cutoff: RecentAttempts is already a bounded ring per key, so the only
	// reason to evict a whole entry is to bound the number of distinct
	// identities tracked, not any single entry's size — a resolved problem
	// or a quiet action's history is kept regardless of age as long as
	// there's room, rather than aging out on a fixed clock. Active problems
	// are never evicted regardless of count.
	maxTrackedProblems = 1000
	maxTrackedActions  = 1000
	// maxRecentAttempts bounds the ring of attempts kept per action+entity.
	maxRecentAttempts = 5
)

// problemKey identifies one recurring problem instance: a specific problem
// code observed against a specific entity (pooler or shard).
type problemKey struct {
	code     types.ProblemCode
	entityID string
}

// ProblemState is the engine's complete record for one problem identity:
// current status plus symptom-level history. There is no separate
// "detected problems" list — "currently active" is the subset of these with
// ResolvedSince.IsZero(). Reflects only what this orch instance has
// observed since it started, not a durable, cluster-wide record: this orch
// may be one of several watching the same shard, and none of this survives
// a restart.
type ProblemState struct {
	LastKnown       types.Problem
	BrokenSince     time.Time
	ResolvedSince   time.Time
	OccurrenceCount int
}

// actionKey identifies one recovery action's track record against one
// entity — deliberately not scoped by problem code, since several different
// codes can drive the same action against the same entity over time (e.g.
// several Leader* codes all drive AppointLeaderAction for a shard).
type actionKey struct {
	actionName string
	entityID   string
}

// AttemptRecord is one recovery-action execution, from start to completion.
// CompletedAt is zero while the attempt is still running — Execute() can
// take up to its action's full timeout, and without this a caller checking
// mid-attempt can't distinguish "actively running" from "hasn't tried in a
// while."
type AttemptRecord struct {
	At             time.Time
	TriggeringCode types.ProblemCode
	CompletedAt    time.Time
	Error          string // only meaningful once CompletedAt is set; empty means it succeeded
}

// ActionAttemptHistory is the engine's attempt history for one recovery
// action against one entity. ShardKey is carried explicitly because
// entityID alone (a bare pooler ID for pooler-scoped actions) doesn't say
// which shard it belongs to.
type ActionAttemptHistory struct {
	ShardKey       *clustermetadatapb.ShardKey
	TotalAttempts  int
	RecentAttempts []AttemptRecord
}

// problemTracker holds the engine's problem-state and action-attempt-history
// maps and implements their reconciliation/eviction/recording logic. The two
// maps have separate mutexes, not one shared lock: reconcile (once per
// cycle) and recordAttemptStart/Complete (once per attempt, potentially from
// several goroutines at once — processShardProblems runs pooler-scoped
// recoveries in parallel) touch different maps and would otherwise contend
// on a single lock even when operating on disjoint keys.
type problemTracker struct {
	problemStatesMu sync.Mutex
	problemStates   map[problemKey]*ProblemState

	actionHistoryMu sync.Mutex
	actionHistory   map[actionKey]*ActionAttemptHistory
}

func newProblemTracker() *problemTracker {
	return &problemTracker{
		problemStates: make(map[problemKey]*ProblemState),
		actionHistory: make(map[actionKey]*ActionAttemptHistory),
	}
}

// reconcile updates problemStates against the full set of problems detected
// in one recovery cycle, then evicts down to maxTrackedProblems if needed.
// Must be called exactly once per cycle, after every analyzer has run —
// mirrors RecoveryGracePeriodTracker.Reconcile's contract, but (unlike that
// tracker) never deletes an entry on resolution; a resolved entry is kept
// until its identity's next fresh episode overwrites it, or until it's
// evicted to make room (see evictProblemStatesLocked).
func (t *problemTracker) reconcile(problems []types.Problem) {
	t.problemStatesMu.Lock()
	defer t.problemStatesMu.Unlock()

	now := time.Now()
	active := make(map[problemKey]struct{}, len(problems))
	for _, p := range problems {
		key := problemKey{code: p.Code, entityID: p.EntityID()}
		active[key] = struct{}{}

		state, exists := t.problemStates[key]
		if !exists || !state.ResolvedSince.IsZero() {
			prevOccurrences := 0
			if exists {
				prevOccurrences = state.OccurrenceCount
			}
			state = &ProblemState{BrokenSince: now, OccurrenceCount: prevOccurrences + 1}
			t.problemStates[key] = state
		}
		state.LastKnown = p
	}

	for key, state := range t.problemStates {
		if _, ok := active[key]; !ok && state.ResolvedSince.IsZero() {
			state.ResolvedSince = now
		}
	}

	t.evictProblemStatesLocked()
}

// evictProblemStatesLocked removes the least-recently-resolved problem
// states once the map exceeds maxTrackedProblems. Active problems
// (ResolvedSince zero) are never evicted, even if that leaves the map over
// the cap — there's nothing safe to drop, since they're still happening.
// Must be called while holding problemStatesMu.
func (t *problemTracker) evictProblemStatesLocked() {
	for len(t.problemStates) > maxTrackedProblems {
		var oldestKey problemKey
		var oldest time.Time
		found := false
		for key, state := range t.problemStates {
			if state.ResolvedSince.IsZero() {
				continue
			}
			if !found || state.ResolvedSince.Before(oldest) {
				oldestKey, oldest, found = key, state.ResolvedSince, true
			}
		}
		if !found {
			return // everything left is active; can't evict further
		}
		delete(t.problemStates, oldestKey)
	}
}

// activeProblems returns the currently-active problems (ResolvedSince
// zero) — the contract existing callers of GetDetectedProblems rely on.
func (t *problemTracker) activeProblems() []types.Problem {
	t.problemStatesMu.Lock()
	defer t.problemStatesMu.Unlock()

	problems := make([]types.Problem, 0, len(t.problemStates))
	for _, state := range t.problemStates {
		if state.ResolvedSince.IsZero() {
			problems = append(problems, state.LastKnown)
		}
	}
	return problems
}

// allStates returns every tracked problem state, active and resolved alike.
func (t *problemTracker) allStates() []ProblemState {
	t.problemStatesMu.Lock()
	defer t.problemStatesMu.Unlock()

	states := make([]ProblemState, 0, len(t.problemStates))
	for _, state := range t.problemStates {
		states = append(states, *state)
	}
	return states
}

// recordAttemptStart appends a new in-progress attempt record (CompletedAt
// zero) to the action's attempt history, trimming RecentAttempts to the
// last maxRecentAttempts and evicting down to maxTrackedActions if needed.
func (t *problemTracker) recordAttemptStart(problem types.Problem, at time.Time) {
	t.actionHistoryMu.Lock()
	defer t.actionHistoryMu.Unlock()

	key := actionKey{actionName: problem.RecoveryAction.Metadata().Name, entityID: problem.EntityID()}
	hist, ok := t.actionHistory[key]
	if !ok {
		hist = &ActionAttemptHistory{ShardKey: problem.ShardKey}
		t.actionHistory[key] = hist
	}
	hist.RecentAttempts = append(hist.RecentAttempts, AttemptRecord{At: at, TriggeringCode: problem.Code})
	if len(hist.RecentAttempts) > maxRecentAttempts {
		hist.RecentAttempts = hist.RecentAttempts[len(hist.RecentAttempts)-maxRecentAttempts:]
	}
	hist.TotalAttempts++

	t.evictActionHistoryLocked()
}

// evictActionHistoryLocked removes the least-recently-attempted action
// histories once the map exceeds maxTrackedActions. Must be called while
// holding actionHistoryMu.
func (t *problemTracker) evictActionHistoryLocked() {
	for len(t.actionHistory) > maxTrackedActions {
		var oldestKey actionKey
		var oldest time.Time
		found := false
		for key, hist := range t.actionHistory {
			if len(hist.RecentAttempts) == 0 {
				continue
			}
			lastAttempt := hist.RecentAttempts[len(hist.RecentAttempts)-1].At
			if !found || lastAttempt.Before(oldest) {
				oldestKey, oldest, found = key, lastAttempt, true
			}
		}
		if !found {
			return
		}
		delete(t.actionHistory, oldestKey)
	}
}

// recordAttemptComplete fills in the outcome of the most recent in-progress
// attempt recorded for this action+entity by recordAttemptStart.
func (t *problemTracker) recordAttemptComplete(problem types.Problem, completedAt time.Time, err error) {
	t.actionHistoryMu.Lock()
	defer t.actionHistoryMu.Unlock()

	hist, ok := t.actionHistory[actionKey{actionName: problem.RecoveryAction.Metadata().Name, entityID: problem.EntityID()}]
	if !ok || len(hist.RecentAttempts) == 0 {
		return
	}
	last := &hist.RecentAttempts[len(hist.RecentAttempts)-1]
	if !last.CompletedAt.IsZero() {
		return // already completed; nothing to fill in
	}
	last.CompletedAt = completedAt
	if err != nil {
		last.Error = err.Error()
	}
}

// actionHistoryFor returns the attempt history recorded for the given
// action name + entity ID, if any.
func (t *problemTracker) actionHistoryFor(actionName, entityID string) (ActionAttemptHistory, bool) {
	t.actionHistoryMu.Lock()
	defer t.actionHistoryMu.Unlock()

	hist, ok := t.actionHistory[actionKey{actionName: actionName, entityID: entityID}]
	if !ok {
		return ActionAttemptHistory{}, false
	}
	return *hist, true
}
