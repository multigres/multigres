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
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

var testShardKey = &clustermetadatapb.ShardKey{Database: "testdb", TableGroup: "default", Shard: "0"}

// shardProblem builds a shard-scoped problem for the given code and action.
func shardProblem(code types.ProblemCode, action types.RecoveryAction) types.Problem {
	return types.Problem{Code: code, Scope: types.ScopeShard, ShardKey: testShardKey, RecoveryAction: action}
}

// poolerProblem builds a pooler-scoped problem for the given code and action.
func poolerProblem(code types.ProblemCode, name string, action types.RecoveryAction) types.Problem {
	return types.Problem{
		Code:           code,
		Scope:          types.ScopePooler,
		ShardKey:       testShardKey,
		PoolerID:       &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "cell1", Name: name},
		RecoveryAction: action,
	}
}

func TestProblemTracker_AppearsOnceAndStaysQuietWhilePersisting(t *testing.T) {
	tracker := newProblemTracker()
	problem := shardProblem(types.ProblemShardAtRisk, nil)
	now := time.Now()

	first := tracker.reconcile([]types.Problem{problem}, now)
	require.Len(t, first.appeared, 1)
	assert.Equal(t, types.ProblemShardAtRisk, first.appeared[0].Code)
	assert.Empty(t, first.cleared)

	again := tracker.reconcile([]types.Problem{problem}, now.Add(time.Minute))
	assert.Empty(t, again.appeared)
	assert.Empty(t, again.cleared)
}

func TestProblemTracker_ClearsWithTheTimeItWasDetected(t *testing.T) {
	tracker := newProblemTracker()
	problem := shardProblem(types.ProblemShardAtRisk, nil)
	now := time.Now()
	tracker.reconcile([]types.Problem{problem}, now)
	tracker.reconcile([]types.Problem{problem}, now.Add(10*time.Second))

	cleared := tracker.reconcile(nil, now.Add(30*time.Second))

	assert.Empty(t, cleared.appeared)
	require.Len(t, cleared.cleared, 1)
	assert.Equal(t, types.ProblemShardAtRisk, cleared.cleared[0].problem.Code)
	assert.Equal(t, 30*time.Second, cleared.cleared[0].duration)
	assert.Empty(t, tracker.activeProblems(), "a cleared problem is forgotten")
}

func TestProblemTracker_RecurrenceIsANewEpisode(t *testing.T) {
	tracker := newProblemTracker()
	problem := shardProblem(types.ProblemShardAtRisk, nil)
	now := time.Now()
	tracker.reconcile([]types.Problem{problem}, now)
	tracker.reconcile(nil, now.Add(time.Minute))

	again := tracker.reconcile([]types.Problem{problem}, now.Add(2*time.Minute))
	require.Len(t, again.appeared, 1)

	cleared := tracker.reconcile(nil, now.Add(2*time.Minute+5*time.Second))
	require.Len(t, cleared.cleared, 1)
	assert.Equal(t, 5*time.Second, cleared.cleared[0].duration, "measured from the second episode")
}

func TestProblemTracker_CodeAndEntityEachIdentifyAProblem(t *testing.T) {
	tracker := newProblemTracker()
	a := poolerProblem(types.ProblemReplicaNotReplicating, "a", nil)
	b := poolerProblem(types.ProblemReplicaNotReplicating, "b", nil)
	otherCode := poolerProblem(types.ProblemStaleLeader, "a", nil)
	now := time.Now()

	first := tracker.reconcile([]types.Problem{a, b, otherCode}, now)
	assert.Len(t, first.appeared, 3, "same code on two poolers, and two codes on one pooler, are all distinct")

	cleared := tracker.reconcile([]types.Problem{a}, now.Add(time.Second))
	require.Len(t, cleared.cleared, 2)
	remaining := tracker.activeProblems()
	require.Len(t, remaining, 1)
	assert.Equal(t, "a", remaining[0].PoolerID.Name)
	assert.Equal(t, types.ProblemReplicaNotReplicating, remaining[0].Code)
}

func TestProblemTracker_ActiveProblemsAreOldestFirstWithTheLatestDescription(t *testing.T) {
	tracker := newProblemTracker()
	older := shardProblem(types.ProblemShardStuck, nil)
	newer := shardProblem(types.ProblemShardAtRisk, nil)
	now := time.Now()
	tracker.reconcile([]types.Problem{older}, now)
	tracker.reconcile([]types.Problem{newer, older}, now.Add(time.Second))

	older.Description = "worse now"
	changed := tracker.reconcile([]types.Problem{newer, older}, now.Add(2*time.Second))
	assert.Empty(t, changed.appeared, "a changed description is not a new problem")

	active := tracker.activeProblems()
	require.Len(t, active, 2)
	assert.Equal(t, types.ProblemShardStuck, active[0].Code)
	assert.Equal(t, "worse now", active[0].Description)
	assert.Equal(t, types.ProblemShardAtRisk, active[1].Code)
}

func TestEngineObserveProblems_LogsTransitions(t *testing.T) {
	var logs bytes.Buffer
	engine := &Engine{logger: slog.New(slog.NewTextHandler(&logs, nil)), problems: newProblemTracker()}
	problem := shardProblem(types.ProblemLeaderUnreachableByCohort, nil)

	engine.observeProblems(t.Context(), []types.Problem{problem})
	engine.observeProblems(t.Context(), []types.Problem{problem})
	engine.observeProblems(t.Context(), nil)

	assert.Equal(t, 1, strings.Count(logs.String(), "problem detected"), "logged once while it persisted")
	assert.Equal(t, 1, strings.Count(logs.String(), "problem cleared"))
	assert.Contains(t, logs.String(), "problem_code=LeaderUnreachableByCohort")
	assert.Empty(t, engine.GetDetectedProblems())
}

// The recovery loop reconciles once per cycle while metrics collection and gRPC
// handlers read concurrently; run under -race.
func TestProblemTracker_ReadsAreSafeWhileTheLoopReconciles(t *testing.T) {
	tracker := newProblemTracker()
	known := map[types.ProblemCode]bool{types.ProblemShardAtRisk: true, types.ProblemShardStuck: true}
	sets := [][]types.Problem{
		{shardProblem(types.ProblemShardAtRisk, nil), shardProblem(types.ProblemShardStuck, nil)},
		{shardProblem(types.ProblemShardStuck, nil)},
		nil,
	}

	done := make(chan struct{})
	var readers sync.WaitGroup
	for range 4 {
		readers.Go(func() {
			for {
				select {
				case <-done:
					return
				default:
				}
				for _, p := range tracker.activeProblems() {
					if !known[p.Code] {
						t.Errorf("read a problem the loop never reported: %v", p.Code)
					}
				}
			}
		})
	}

	now := time.Now()
	for i := range 500 {
		tracker.reconcile(sets[i%len(sets)], now.Add(time.Duration(i)*time.Second))
	}
	close(done)
	readers.Wait()
}

// The engine's other goroutines (gRPC handlers, metrics collection) read the
// tracker while the loop's cycle reconciles it.
func TestEngineObserveProblems_IsSafeAlongsideItsReaders(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	engine := &Engine{logger: logger, problems: newProblemTracker()}
	problem := shardProblem(types.ProblemLeaderUnreachableByCohort, nil)

	done := make(chan struct{})
	var readers sync.WaitGroup
	for _, read := range []func(){
		func() { _ = engine.GetDetectedProblems() },
		func() { _ = engine.collectDetectedProblemsData() },
	} {
		readers.Go(func() {
			for {
				select {
				case <-done:
					return
				default:
					read()
				}
			}
		})
	}

	for i := range 300 {
		if i%2 == 0 {
			engine.observeProblems(t.Context(), []types.Problem{problem})
		} else {
			engine.observeProblems(t.Context(), nil)
		}
	}
	close(done)
	readers.Wait()
}
