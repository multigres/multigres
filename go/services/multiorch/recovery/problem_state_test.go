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
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// fakeAction is a minimal types.RecoveryAction for tests that only care
// about the action's identity (Metadata().Name), not its actual behavior.
type fakeAction struct {
	name string
}

func (a *fakeAction) Execute(context.Context, types.RecheckedProblem) error { return nil }
func (a *fakeAction) Metadata() types.RecoveryMetadata                      { return types.RecoveryMetadata{Name: a.name} }
func (a *fakeAction) RequiresHealthyLeader() bool                           { return false }
func (a *fakeAction) GracePeriod() *types.GracePeriodConfig                 { return nil }

func testShardKey() *clustermetadatapb.ShardKey {
	return &clustermetadatapb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}
}

func testPoolerID(name string) *clustermetadatapb.ID {
	return &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: name}
}

func testProblem(code types.ProblemCode, actionName string) types.Problem {
	return types.Problem{
		Code:           code,
		CheckName:      "TestAnalyzer",
		PoolerID:       testPoolerID("pooler-1"),
		ShardKey:       testShardKey(),
		Scope:          types.ScopePooler,
		RecoveryAction: &fakeAction{name: actionName},
	}
}

func TestProblemTracker_Reconcile_NewPersistingResolved(t *testing.T) {
	tr := newProblemTracker()
	p := testProblem("ReplicaLagging", "FixReplication")

	// First sighting: starts a fresh episode.
	tr.reconcile([]types.Problem{p})
	states := tr.allStates()
	require.Len(t, states, 1)
	require.False(t, states[0].BrokenSince.IsZero())
	require.True(t, states[0].ResolvedSince.IsZero())
	require.Equal(t, 1, states[0].OccurrenceCount)
	brokenSince := states[0].BrokenSince

	// Still detected next cycle: episode fields unchanged.
	tr.reconcile([]types.Problem{p})
	states = tr.allStates()
	require.Len(t, states, 1)
	require.Equal(t, brokenSince, states[0].BrokenSince)
	require.Equal(t, 1, states[0].OccurrenceCount)
	require.True(t, states[0].ResolvedSince.IsZero())

	// Absent this cycle: resolved, but the entry is NOT deleted (unlike
	// RecoveryGracePeriodTracker) — GetProblemStates must still surface it.
	tr.reconcile(nil)
	states = tr.allStates()
	require.Len(t, states, 1, "a resolved problem state must persist, not vanish")
	require.False(t, states[0].ResolvedSince.IsZero())
	require.Empty(t, tr.activeProblems(), "activeProblems must not include a resolved entry")

	// Recurs: a fresh episode, with OccurrenceCount incremented.
	tr.reconcile([]types.Problem{p})
	states = tr.allStates()
	require.Len(t, states, 1)
	require.True(t, states[0].ResolvedSince.IsZero())
	require.Equal(t, 2, states[0].OccurrenceCount, "a recurrence must increment OccurrenceCount")
	require.True(t, states[0].BrokenSince.After(brokenSince), "a fresh episode must reset BrokenSince")
}

func TestProblemTracker_ActiveProblems_ExcludesResolved(t *testing.T) {
	tr := newProblemTracker()
	p := testProblem("ShardStuck", "AlertOnly")

	tr.reconcile([]types.Problem{p})
	require.Len(t, tr.activeProblems(), 1)

	tr.reconcile(nil)
	require.Empty(t, tr.activeProblems())
}

func TestProblemTracker_AttemptHistory_FragmentationAcrossCodes(t *testing.T) {
	// Two different problem codes driving the SAME action against the SAME
	// entity must accumulate into one shared history, not two — this is the
	// exact gap the (Code, EntityID)-keyed design would have had.
	tr := newProblemTracker()
	p1 := testProblem("LeaderUnhealthy", "AppointLeader")
	p2 := testProblem("LeaderUnsupported", "AppointLeader")

	tr.recordAttemptStart(p1, time.Now())
	tr.recordAttemptComplete(p1, time.Now(), errors.New("boom"))
	tr.recordAttemptStart(p2, time.Now())
	tr.recordAttemptComplete(p2, time.Now(), nil)

	hist, ok := tr.actionHistoryFor("AppointLeader", p1.EntityID())
	require.True(t, ok)
	require.Equal(t, 2, hist.TotalAttempts, "attempts from both codes must accumulate into one history")
	require.Len(t, hist.RecentAttempts, 2)
	require.Equal(t, types.ProblemCode("LeaderUnhealthy"), hist.RecentAttempts[0].TriggeringCode)
	require.Equal(t, "boom", hist.RecentAttempts[0].Error)
	require.Equal(t, types.ProblemCode("LeaderUnsupported"), hist.RecentAttempts[1].TriggeringCode)
	require.Empty(t, hist.RecentAttempts[1].Error)
}

func TestProblemTracker_AttemptHistory_StartThenComplete(t *testing.T) {
	tr := newProblemTracker()
	p := testProblem("LeaderUnhealthy", "AppointLeader")

	start := time.Now()
	tr.recordAttemptStart(p, start)

	hist, ok := tr.actionHistoryFor("AppointLeader", p.EntityID())
	require.True(t, ok)
	require.Len(t, hist.RecentAttempts, 1)
	require.True(t, hist.RecentAttempts[0].CompletedAt.IsZero(), "an attempt must read as in-progress before completion is recorded")

	tr.recordAttemptComplete(p, time.Now(), nil)
	hist, ok = tr.actionHistoryFor("AppointLeader", p.EntityID())
	require.True(t, ok)
	require.False(t, hist.RecentAttempts[0].CompletedAt.IsZero())
	require.Empty(t, hist.RecentAttempts[0].Error)
}

func TestProblemTracker_AttemptHistory_RingBounded(t *testing.T) {
	tr := newProblemTracker()
	p := testProblem("LeaderUnhealthy", "AppointLeader")

	for range maxRecentAttempts + 3 {
		tr.recordAttemptStart(p, time.Now())
		tr.recordAttemptComplete(p, time.Now(), nil)
	}

	hist, ok := tr.actionHistoryFor("AppointLeader", p.EntityID())
	require.True(t, ok)
	require.Len(t, hist.RecentAttempts, maxRecentAttempts, "the ring must trim to maxRecentAttempts")
	require.Equal(t, maxRecentAttempts+3, hist.TotalAttempts, "TotalAttempts must count all-time, not just what fits in the ring")
}

func TestProblemTracker_Eviction_ResolvedProblemsCapped(t *testing.T) {
	tr := newProblemTracker()

	// One problem stays active throughout — it must survive even though the
	// cap is exceeded by the resolved ones, since active problems are never
	// evicted.
	activeProblem := testProblem("LeaderUnhealthy", "AppointLeader")
	tr.reconcile([]types.Problem{activeProblem})

	// Fill past the cap with distinct, already-resolved identities.
	for i := range maxTrackedProblems + 5 {
		p := testProblem(types.ProblemCode(fmt.Sprintf("Code%d", i)), "FixReplication")
		p.PoolerID = testPoolerID(fmt.Sprintf("pooler-%d", i))
		tr.reconcile([]types.Problem{activeProblem, p})
		tr.reconcile([]types.Problem{activeProblem}) // p resolves immediately
	}

	states := tr.allStates()
	require.LessOrEqual(t, len(states), maxTrackedProblems+1, "resolved entries must be capped at maxTrackedProblems (plus the one still-active entry)")
	require.Len(t, tr.activeProblems(), 1, "the still-active problem must never be evicted")
}

// TestProblemTracker_Eviction_AllActiveSkipsEviction covers
// evictProblemStatesLocked's "nothing resolved to evict" guard: without it,
// exceeding maxTrackedProblems with only active (never-resolved) entries
// would loop forever, since the outer loop condition never stops being true.
func TestProblemTracker_Eviction_AllActiveSkipsEviction(t *testing.T) {
	tr := newProblemTracker()

	var problems []types.Problem
	for i := range maxTrackedProblems + 5 {
		p := testProblem(types.ProblemCode(fmt.Sprintf("Code%d", i)), "SomeAction")
		p.PoolerID = testPoolerID(fmt.Sprintf("pooler-%d", i))
		problems = append(problems, p)
	}
	tr.reconcile(problems)

	require.Len(t, tr.allStates(), maxTrackedProblems+5,
		"eviction must not remove active problems even over the cap")
}

// TestProblemTracker_AttemptComplete_UnknownActionIsNoop covers
// recordAttemptComplete's "no history for this action+entity" guard.
func TestProblemTracker_AttemptComplete_UnknownActionIsNoop(t *testing.T) {
	tr := newProblemTracker()
	p := testProblem("LeaderUnhealthy", "NeverStarted")

	require.NotPanics(t, func() {
		tr.recordAttemptComplete(p, time.Now(), nil)
	})
	_, ok := tr.actionHistoryFor("NeverStarted", p.EntityID())
	require.False(t, ok, "completing an attempt that was never started must not create a history entry")
}

// TestProblemTracker_AttemptComplete_AlreadyCompletedIsNoop covers
// recordAttemptComplete's "already completed" guard: a second completion
// call for the same attempt must not overwrite the first outcome.
func TestProblemTracker_AttemptComplete_AlreadyCompletedIsNoop(t *testing.T) {
	tr := newProblemTracker()
	p := testProblem("LeaderUnhealthy", "AppointLeader")

	tr.recordAttemptStart(p, time.Now())
	firstCompletion := time.Now()
	tr.recordAttemptComplete(p, firstCompletion, errors.New("first error"))

	tr.recordAttemptComplete(p, firstCompletion.Add(time.Hour), errors.New("second error"))

	hist, ok := tr.actionHistoryFor("AppointLeader", p.EntityID())
	require.True(t, ok)
	require.Len(t, hist.RecentAttempts, 1)
	require.True(t, hist.RecentAttempts[0].CompletedAt.Equal(firstCompletion),
		"a second completion call must not overwrite the first")
	require.Equal(t, "first error", hist.RecentAttempts[0].Error,
		"a second completion call must not overwrite the first error")
}

func TestProblemTracker_Eviction_ActionHistoryCapped(t *testing.T) {
	tr := newProblemTracker()

	for i := range maxTrackedActions + 5 {
		p := testProblem("ReplicaLagging", fmt.Sprintf("Action%d", i))
		tr.recordAttemptStart(p, time.Now())
		tr.recordAttemptComplete(p, time.Now(), nil)
	}

	// Spot check: the very first action (least recently attempted) must have
	// been evicted to make room, while a recent one survives.
	_, ok := tr.actionHistoryFor("Action0", testProblem("ReplicaLagging", "Action0").EntityID())
	require.False(t, ok, "the least-recently-attempted action history must be evicted once over the cap")
	_, ok = tr.actionHistoryFor(fmt.Sprintf("Action%d", maxTrackedActions+4), testProblem("ReplicaLagging", "x").EntityID())
	require.True(t, ok, "the most recently attempted action history must survive")
}
